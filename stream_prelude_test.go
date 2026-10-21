package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOllamaPreludeFallbackEndToEnd(t *testing.T) {
	for _, visible := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-content", true: "after-content"}[visible], func(t *testing.T) {
			circuitBreakers.Delete("ollama|synthetic-worker")
			t.Cleanup(func() { circuitBreakers.Delete("ollama|synthetic-worker") })
			var primaryCalls, backupCalls atomic.Int32
			primaryStream := rolePrelude
			if visible {
				primaryStream += textFrame
			}
			primaryStream += errorFrame
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, primaryStream)
			}))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backupCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, textFrame)
			}))
			defer backup.Close()
			s, err := newProxyServer(config{Providers: map[string]providerConfig{"ollama": {BaseURL: primary.URL, Billing: "subscription"}, "bigmodel": {BaseURL: backup.URL, Billing: "subscription"}}})
			if err != nil {
				t.Fatal(err)
			}
			selected := modelConfig{Requested: "worker", Provider: "ollama", Upstream: "synthetic-worker", Fallbacks: []modelConfig{{Provider: "bigmodel", Upstream: "synthetic-worker"}}}
			resp, err := s.doWithFallbacks(context.Background(), httptest.NewRequest("POST", "/v1/messages", nil), []byte(`{"model":"worker","messages":[]}`), map[string]any{"model": "worker"}, selected)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(resp.resp.Body)
			resp.resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if primaryCalls.Load() != 1 {
				t.Fatal("primary replayed")
			}
			if visible {
				if backupCalls.Load() != 0 || string(got) != primaryStream {
					t.Fatal("visible stream replayed or altered")
				}
			} else if backupCalls.Load() != 1 || resp.providerName != "bigmodel" || string(got) != textFrame {
				t.Fatal("pre-content fallback failed")
			}
		})
	}
}

const rolePrelude = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n"
const errorFrame = "event: error\ndata: {\"error\":{\"type\":\"overloaded_error\"}}\n\n"
const textFrame = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n"

func TestEmptyAnthropicPreludeRejectsTruncation(t *testing.T) {
	start := "data: {\"type\":\"message_start\",\"message\":{\"content\":null}}\n\n"
	for _, tail := range []string{"", "data: {\"type\":\"message_stop\"}\n\n", "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"\"}}\n\n"} {
		body, err := primeSSEBodyWithPrelude(io.NopCloser(strings.NewReader(start+tail)), time.Second, time.Second)
		if body != nil || err == nil {
			t.Fatal("empty/truncated stream committed")
		}
	}
	valid := start + "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
	body, err := primeSSEBodyWithPrelude(io.NopCloser(strings.NewReader(valid)), time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(got) != valid {
		t.Fatal("valid empty response not preserved")
	}
}

func TestPrimeFrameTelemetryDoesNotExposeArbitraryTypes(t *testing.T) {
	if got := primeFrameKind(`{"type":"private arbitrary text"}`); got != "unknown" {
		t.Fatal(got)
	}
	var frames []string
	body, err := primeSSEBodyObserved(io.NopCloser(strings.NewReader(rolePrelude+errorFrame)), time.Second, time.Second, nil, func(kind string) { frames = append(frames, kind) })
	if body != nil || err == nil || len(frames) != 2 || frames[0] != "openai_choices" || frames[1] != "error" {
		t.Fatalf("frames=%v err=%v", frames, err)
	}
}

func TestSmallChainBudgetSharesWithoutStarvation(t *testing.T) {
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{"a": {Billing: "subscription"}, "b": {Billing: "subscription"}, "c": {Billing: "subscription"}}}}
	attempts := []claudeCandidateAttempt{{model: modelConfig{Provider: "a"}}, {model: modelConfig{Provider: "b"}}, {model: modelConfig{Provider: "c"}}}
	for _, seconds := range []int{15, 60, 90} {
		got, skip := s.remainingAttemptHeaderBudget(requestTrace{ChainBudget: time.Duration(seconds) * time.Second}, attempts, 0, "a")
		want := time.Duration(seconds/3) * time.Second
		if seconds == 90 {
			want = 40 * time.Second
		}
		if skip || got != want {
			t.Fatalf("%d: budget=%s skip=%v", seconds, got, skip)
		}
	}
}

func TestPreludeErrorCanFailBeforeCommit(t *testing.T) {
	body, err := primeSSEBodyWithPrelude(io.NopCloser(strings.NewReader(rolePrelude+errorFrame)), time.Second, time.Second)
	var providerErr *initialSSEError
	if body != nil || !errors.As(err, &providerErr) || providerErr.kind != "overloaded_error" {
		t.Fatalf("body=%v err=%v", body, err)
	}
}

func TestPreludeNeverRetriesAfterSemanticFrame(t *testing.T) {
	for _, semantic := range []string{textFrame,
		"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n",
		"data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"id\":\"test\",\"name\":\"test\"}}\n\n",
	} {
		stream := rolePrelude + semantic + errorFrame
		body, err := primeSSEBodyWithPrelude(io.NopCloser(strings.NewReader(stream)), time.Second, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(body)
		body.Close()
		if err != nil || string(got) != stream {
			t.Fatal("stream bytes lost or error consumed after semantic content")
		}
	}
}

func TestPreludeThinkingUsesStreamAllowance(t *testing.T) {
	r, w := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer w.Close()
		io.WriteString(w, rolePrelude)
		time.Sleep(80 * time.Millisecond)
		io.WriteString(w, textFrame)
	}()
	body, err := primeSSEBodyWithPrelude(r, 40*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, body)
	body.Close()
	<-done
}

func TestPreludeIdleTimeoutClosesBlockedReader(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	go func() { io.WriteString(w, rolePrelude) }()
	body, err := primeSSEBodyWithPrelude(r, time.Second, 20*time.Millisecond)
	if body != nil || err == nil {
		t.Fatal("idle prelude did not fail")
	}
}

func TestSubscriptionReserveUsesBillingNotRankingTier(t *testing.T) {
	tier := 100
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{
		"ollama": {Billing: "subscription"}, "bigmodel": {Billing: "subscription"}, "opencode": {Billing: "subscription"}, "paid": {Billing: "metered"},
	}}}
	attempts := []claudeCandidateAttempt{
		{model: modelConfig{Provider: "ollama", Tier: &tier}},
		{model: modelConfig{Provider: "bigmodel", Tier: &tier}},
		{model: modelConfig{Provider: "bigmodel", Tier: &tier}},
		{model: modelConfig{Provider: "opencode", Tier: &tier}},
		{model: modelConfig{Provider: "paid"}},
	}
	trace := requestTrace{ChainBudget: 90 * time.Second}
	budget, skip := s.remainingAttemptHeaderBudget(trace, attempts, 0, "ollama")
	if skip || budget != 40*time.Second {
		t.Fatalf("budget=%s skip=%v", budget, skip)
	}
	trace.ChainBudget = 20 * time.Second
	budget, skip = s.remainingAttemptHeaderBudget(trace, attempts, 1, "bigmodel")
	if skip || budget != 10*time.Second {
		t.Fatalf("bigmodel budget=%s skip=%v", budget, skip)
	}
	budget, skip = s.remainingAttemptHeaderBudget(trace, attempts, 3, "opencode")
	if skip || budget != 20*time.Second {
		t.Fatalf("last subscription budget=%s skip=%v", budget, skip)
	}
}
