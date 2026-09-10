package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPassiveConcurrencyScopesAndRelease(t *testing.T) {
	var c concurrencyTelemetry
	_, r1 := c.begin("claude", "account-a", "model-one", "chain-a", "/v1/messages")
	e, r2 := c.begin("claude", "account-a", "model-two", "chain-b", "/v1/messages")
	if e.ActiveTotal != 2 || e.ActiveAccount != 2 || e.ActiveModel != 1 || e.ActiveChain != 1 {
		t.Fatalf("wrong scopes: %+v", e)
	}
	e, r3 := c.begin("claude", "account-b", "model-two", "chain-b", "/v1/messages")
	if e.ActiveTotal != 3 || e.ActiveAccount != 1 || e.ActiveModel != 1 || e.ActiveChain != 2 {
		t.Fatalf("account isolation: %+v", e)
	}
	r1()
	r1()
	r2()
	r3()
	if s := c.snapshot(); s.Total != 0 || len(s.Scopes) != 0 {
		t.Fatalf("leaked scopes: %+v", s)
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release := c.begin("p", "a", "m", "c", "/v1/messages")
			c.snapshot()
			release()
		}()
	}
	wg.Wait()
	if c.snapshot().Total != 0 {
		t.Fatal("concurrent release leaked")
	}
}

func TestPassiveTelemetryCopiesTiming(t *testing.T) {
	previous := &attemptOutcome{Provider: "first", Status: 503}
	trace := requestTrace{Execution: &attemptExecution{Number: 2, Previous: previous}, Timing: &upstreamTiming{}, HeadersMS: 100}
	trace.Timing.connectionMS.Store(10)
	trace.Timing.tlsMS.Store(8)
	trace.Timing.firstResponseByteMS.Store(90)
	trace.Timing.gotConnection.Store(true)
	trace.Timing.connectionReused.Store(true)
	var sample metricsSample
	applyExecutionTelemetry(&sample, trace)
	previous.Status = 400
	trace.Execution.Number = 99
	trace.Timing.connectionMS.Store(999)
	if sample.Execution.Number != 2 || sample.Execution.Previous.Status != 503 || sample.ConnectionMS != 10 || sample.TLSMS != 8 || sample.ResponseHeadersMS != 100 || sample.Execution.FirstResponseByteMS != 90 || !*sample.Execution.ConnectionReused {
		t.Fatalf("bad snapshot: %+v", sample)
	}
	var skip metricsSample
	applyExecutionTelemetry(&skip, requestTrace{Timing: trace.Timing})
	if skip.Execution != nil || skip.ConnectionMS != 0 {
		t.Fatal("skip inherited timings")
	}
}

func TestPassiveTelemetryFallbackIntegration(t *testing.T) {
	var primaryCalls, secondaryCalls atomic.Int64
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"type":"overloaded_error","message":"synthetic overload"}}`)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryCalls.Add(1)
		io.WriteString(w, `{"id":"synthetic","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer second.Close()
	f := testConfig(first.URL, "anthropic")
	p := f.Providers["endpoint"]
	p.BaseURL = second.URL
	f.Providers["telemetry-next"] = p
	m := f.Models["model"]
	m.Provider = "telemetry-next"
	f.Models["next"] = m
	f.Providers["telemetry-skip"] = p
	m.Provider = "telemetry-skip"
	f.Models["skip"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "skip"}, {Model: "next"}}}
	cfg := compileFixture(t, f)
	s, err := newProxyServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.markProviderBlocked("telemetry-skip", "synthetic_block", time.Minute)
	t.Cleanup(func() {
		s.metrics.close()
		circuitBreakers.Delete(s.circuitKey("endpoint", modelConfig{Upstream: "test-model"}))
	})
	out := httptest.NewRecorder()
	s.handler().ServeHTTP(out, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"synthetic"}]}`)))
	if out.Code != 200 || primaryCalls.Load() != 1 || secondaryCalls.Load() != 1 {
		t.Fatalf("routing changed: status=%d calls=%d/%d", out.Code, primaryCalls.Load(), secondaryCalls.Load())
	}
	var failed, completed, skipped *metricsSample
	for _, sample := range s.metrics.read(0) {
		x := sample
		if sample.RecordKind == "attempt" && sample.Status == 503 {
			failed = &x
		}
		if sample.RecordKind == "completion" && sample.Success {
			completed = &x
		}
		if sample.RecordKind == "skip" && sample.Provider == "telemetry-skip" {
			skipped = &x
		}
	}
	if failed == nil || completed == nil || failed.Execution == nil || completed.Execution == nil {
		t.Fatalf("missing executions: %v", s.metrics.read(0))
	}
	if skipped == nil || skipped.Execution != nil || skipped.ConnectionMS != 0 {
		t.Fatal("policy skip inherited prior attempt telemetry")
	}
	e := completed.Execution
	if e.Number != 2 || e.Previous == nil || e.Previous.Provider != "endpoint" || e.Previous.Status != 503 || e.Previous.Reason == "" || e.Chain != "main" {
		t.Fatalf("bad fallback link: %+v", e)
	}
	if failed.Execution.HTTPVersion == "" || failed.Execution.ConnectionReused == nil || e.HTTPVersion == "" || e.ConnectionReused == nil {
		t.Fatal("missing transport telemetry")
	}
	if e.ActiveTotal != 1 || s.passiveConcurrency.snapshot().Total != 0 {
		t.Fatal("failed attempt/response leaked active count")
	}
}

func TestPassiveTelemetryDoesNotLimitRequestsAndStatusIsReadOnly(t *testing.T) {
	const count = 8
	entered := make(chan struct{}, count)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, `{"id":"synthetic","type":"message","role":"assistant","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer upstream.Close()
	defer unblock()
	cfg := compileFixture(t, testConfig(upstream.URL, "anthropic"))
	s, err := newProxyServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.metrics.close()
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"synthetic"}]}`)))
		}()
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for i := 0; i < count; i++ {
		select {
		case <-entered:
		case <-timer.C:
			unblock()
			wg.Wait()
			t.Fatal("requests unexpectedly limited")
		}
	}
	for i := 0; i < 3; i++ {
		out := httptest.NewRecorder()
		s.status(out, httptest.NewRequest("GET", "/status", nil))
		var status struct {
			Concurrency concurrencySnapshot `json:"upstreamConcurrency"`
		}
		if err := json.Unmarshal(out.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		if status.Concurrency.Total != count {
			t.Fatalf("wrong active count: %+v", status)
		}
	}
	unblock()
	wg.Wait()
	if s.passiveConcurrency.snapshot().Total != 0 {
		t.Fatal("completion leaked active counts")
	}
}

func TestPassiveTelemetryCancellationReleasesAttempt(t *testing.T) {
	entered := make(chan struct{})
	shutdown := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-shutdown:
		}
	}))
	defer upstream.Close()
	defer close(shutdown)
	s, err := newProxyServer(compileFixture(t, testConfig(upstream.URL, "anthropic")))
	if err != nil {
		t.Fatal(err)
	}
	defer s.metrics.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"synthetic"}]}`)).WithContext(ctx))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream not reached")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation blocked")
	}
	if s.passiveConcurrency.snapshot().Total != 0 {
		t.Fatal("cancellation leaked active attempt")
	}
	for _, sample := range s.metrics.read(0) {
		if sample.RecordKind == "attempt" && sample.FailureReason == "client_cancel" && sample.Execution != nil {
			return
		}
	}
	t.Fatal("missing cancellation execution telemetry")
}
