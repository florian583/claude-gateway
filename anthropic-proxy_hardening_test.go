package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type auditRoundTripper func(*http.Request) (*http.Response, error)

func (f auditRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type auditCancelReader struct{ cancel context.CancelFunc }

func (r *auditCancelReader) Read([]byte) (int, error) { r.cancel(); return 0, context.Canceled }
func (r *auditCancelReader) Close() error             { return nil }

func TestFiveHourAuditFailedAttemptsAffectStability(t *testing.T) {
	p := buildRoutePerformance([]metricsSample{
		{Provider: "audit", Upstream: "m", RecordKind: "attempt", FailureReason: "transport", Success: false, LatencyMS: 1000},
		{Provider: "audit", Upstream: "m", RecordKind: "completion", Success: true, LatencyMS: 10},
	})["audit|m"]
	if p.Samples != 2 || p.SuccessRate != .5 {
		t.Fatalf("samples=%d success=%v; want 2 and 0.5", p.Samples, p.SuccessRate)
	}
}

func TestFiveHourAuditEffortIsNotStructuredOutput(t *testing.T) {
	if requestNeedsStructuredOutputCompatibility(map[string]any{"output_config": map[string]any{"effort": "max"}}) {
		t.Fatal("effort-only request incorrectly classified as structured output")
	}
}

func TestFiveHourAuditWarmGateAvoidsUsageIO(t *testing.T) {
	const service = "audit-five-hour-cache"
	const token = "sk-ant-oat-test-audit-only"
	claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: token, fetchedAt: time.Now()})
	defer claudeOAuthCredentials.Delete(service)
	profile := testClaudeProfile(t, "audit-cache", service, "audit-cache")
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"anthropic": {BaseURL: "http://audit.invalid"}}, ClaudeUsage: claudeUsageConfig{Provider: "anthropic", EligibleUpstreams: []string{"claude-opus-4-8"}, FiveHourThresholdPct: 100, SevenDayThresholdPct: 100, CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	s.claudeUse.snapshots = map[string]claudeUsageSnapshot{claudeTokenKey("Bearer " + token): {Profile: profile.Name, FetchedAt: time.Now().Add(-6 * time.Minute), FiveHour: claudeUsageWindow{Utilization: 10}, SevenDay: claudeUsageWindow{Utilization: 10}}}
	var calls atomic.Int32
	s.clients["anthropic"] = &http.Client{Transport: auditRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))}, nil
	})}
	request := withSelectedClaudeProfile(httptest.NewRequest("POST", "http://localhost/v1/messages", nil), profile)
	allowed, _ := s.claudeSubscriptionAllowed(context.Background(), request, modelConfig{Provider: "anthropic", Upstream: "claude-opus-4-8"})
	if !allowed {
		t.Fatal("fixture should remain eligible")
	}
	if calls.Load() != 0 {
		t.Fatalf("warm eligible account caused %d synchronous usage calls", calls.Load())
	}
}

func TestFiveHourAuditCancelDuringPrimeIsNeutral(t *testing.T) {
	const provider = "audit-cancel-prime"
	s, err := newProxyServer(config{DefaultProvider: provider, Providers: map[string]providerConfig{provider: {BaseURL: "http://audit.invalid"}}})
	if err != nil {
		t.Fatal(err)
	}
	m := modelConfig{Requested: "audit", Provider: provider, Upstream: "audit"}
	key := s.circuitKey(provider, m)
	circuitBreakers.Delete(key)
	defer circuitBreakers.Delete(key)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.clients[provider] = &http.Client{Transport: auditRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: &auditCancelReader{cancel: cancel}}, nil
	})}
	request := httptest.NewRequest("POST", "http://localhost/v1/messages", nil).WithContext(ctx)
	_, _ = s.doWithFallbacks(ctx, request, []byte(`{}`), map[string]any{}, m)
	if raw, ok := circuitBreakers.Load(key); ok {
		state := raw.(*circuitState)
		state.mu.Lock()
		failures := state.failures
		state.mu.Unlock()
		if failures > 0 {
			t.Fatalf("client cancellation added %d provider circuit failures", failures)
		}
	}
}

func TestFiveHourAuditOldCompletionKeepsNewBlock(t *testing.T) {
	const provider = "audit-late-completion"
	entered := make(chan struct{})
	finish := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_audit\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"audit\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n")
		w.(http.Flusher).Flush()
		close(entered)
		<-finish
		fmt.Fprint(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	}))
	defer upstream.Close()
	s, err := newProxyServer(config{DefaultProvider: provider, Providers: map[string]providerConfig{provider: {BaseURL: upstream.URL}}, Models: map[string]modelConfig{"audit": {Provider: provider, Upstream: "audit"}}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		s.handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "http://localhost/v1/messages", strings.NewReader(`{"model":"audit","stream":true,"messages":[{"role":"user","content":"test"}]}`)))
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("fixture did not start")
	}
	s.markProviderBlocked(provider, "quota_or_rate_limit", time.Minute)
	close(finish)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fixture did not complete")
	}
	if blocked, _ := s.providerBlocked(provider); !blocked {
		t.Fatal("older in-flight success erased newer quota block")
	}
}

func TestFiveHourAuditEOFIsNotSuccessfulOpenAICompletion(t *testing.T) {
	stream := "data: {\"id\":\"a\",\"choices\":[{\"delta\":{\"content\":\"unfinished\"},\"finish_reason\":null}]}\n\n"
	var out bytes.Buffer
	stats := &streamStats{}
	err := translateOpenAIStreamWithStats(strings.NewReader(stream), &out, "audit", stats)
	if err == nil && !stats.ProtocolError.Load() && !strings.Contains(out.String(), "event: error") {
		t.Fatal("EOF without finish_reason or DONE fabricated successful message_stop")
	}
}

func TestFiveHourAuditInvalidRequestGetsTerminal(t *testing.T) {
	s, err := newProxyServer(config{Metrics: metricsConfig{Path: t.TempDir() + "/metrics.jsonl", MaxSamples: 100}, DefaultProvider: "audit", Providers: map[string]providerConfig{"audit": {BaseURL: "http://audit.invalid"}}, Models: map[string]modelConfig{"known": {Provider: "audit", Upstream: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.metrics.close()
	s.handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "http://localhost/v1/messages", strings.NewReader(`{"model":"not-configured","messages":[]}`)))
	if got := len(s.metrics.read(100)); got != 1 {
		t.Fatalf("invalid request terminal records=%d; want 1", got)
	}
}

func TestFiveHourAuditPrimeRejectsErrorBeforeCommit(t *testing.T) {
	body, err := primeSSEBody(io.NopCloser(strings.NewReader("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"try later\"}}\n\n")), time.Second)
	if body != nil {
		body.Close()
	}
	if err == nil {
		t.Fatal("initial error frame considered viable stream; safe pre-output fallback lost")
	}
}

func TestHardeningOldCircuitProbeCannotReleaseNewOwner(t *testing.T) {
	s := &proxyServer{}
	m := modelConfig{Provider: "lease-probe", Upstream: "m"}
	key := s.circuitKey(m.Provider, m)
	state := &circuitState{openUntil: time.Now().Add(-time.Second)}
	circuitBreakers.Store(key, state)
	t.Cleanup(func() { circuitBreakers.Delete(key) })
	ok, old := s.acquireCircuitAttempt(m.Provider, m, false)
	if !ok || old == nil {
		t.Fatal("missing first probe")
	}
	s.tripCircuit(m.Provider, m)
	state.mu.Lock()
	state.openUntil = time.Now().Add(-time.Second)
	state.mu.Unlock()
	ok, current := s.acquireCircuitAttempt(m.Provider, m, false)
	if !ok || current == nil {
		t.Fatal("missing recovery probe")
	}
	releaseCircuitProbe(old)
	if ok, _ := s.acquireCircuitAttempt(m.Provider, m, true); ok {
		t.Fatal("old lease released newer probe")
	}
	releaseCircuitProbe(current)
	if ok, next := s.acquireCircuitAttempt(m.Provider, m, false); !ok {
		t.Fatal("current probe did not release")
	} else {
		releaseCircuitProbe(next)
	}
}

func TestHardeningOldSuccessCannotClearNewCircuit(t *testing.T) {
	s := &proxyServer{}
	m := modelConfig{Provider: "ordered-circuit", Upstream: "m"}
	key := s.circuitKey(m.Provider, m)
	t.Cleanup(func() { circuitBreakers.Delete(key) })
	started := time.Now().Add(-time.Second)
	s.tripCircuit(m.Provider, m)
	s.tripCircuit(m.Provider, m)
	s.resetCircuitAfter(m.Provider, m, started)
	if !s.circuitOpen(m.Provider, m) {
		t.Fatal("older completion cleared newer failure")
	}
	s.resetCircuitAfter(m.Provider, m, time.Now().Add(time.Second))
	if s.circuitOpen(m.Provider, m) {
		t.Fatal("new successful probe did not recover circuit")
	}
}

type auditFailWriter struct{}

func (auditFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestHardeningOpenAITerminalMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, ending string
		fail         bool
	}{
		{"unexpected-eof", "", true},
		{"done", "data: [DONE]\n\n", false},
		{"finish-eof", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", false},
		{"malformed-json", "data: {oops}\n\n", true},
		{"provider-error", "data: {\"error\":{\"message\":\"failure\"}}\n\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := "data: {\"choices\":[{\"delta\":{\"content\":\"text\"}}]}\n\n" + tc.ending
			var out bytes.Buffer
			stats := &streamStats{}
			err := translateOpenAIStreamWithStats(strings.NewReader(stream), &out, "m", stats)
			if (err != nil) != tc.fail {
				t.Fatalf("err=%v wantFail=%v", err, tc.fail)
			}
			if tc.fail && (!stats.ProtocolError.Load() || strings.Contains(out.String(), "event: message_stop")) {
				t.Fatal("failed stream fabricated success")
			}
			if !tc.fail && strings.Count(out.String(), "event: message_stop") != 1 {
				t.Fatal("missing or repeated terminal")
			}
		})
	}
	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"text\"},\"finish_reason\":\"stop\"}]}\n\n"
	if err := translateOpenAIStream(strings.NewReader(stream), auditFailWriter{}, "m"); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer failure lost: %v", err)
	}
}

func TestHardeningToolArgumentsMustBeObjectsAndBounded(t *testing.T) {
	for _, args := range []string{"[]", "null", "42", `"string"`, "{broken", "{}", `{"x":1}`} {
		t.Run(args, func(t *testing.T) {
			chunk := map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": "call", "function": map[string]any{"name": "tool", "arguments": args}}}}, "finish_reason": "tool_calls"}}}
			raw, _ := json.Marshal(chunk)
			stats := &streamStats{}
			var out bytes.Buffer
			_ = translateOpenAIStreamWithStats(strings.NewReader("data: "+string(raw)+"\n\ndata: [DONE]\n\n"), &out, "m", stats)
			valid := args == "{}" || args == `{"x":1}`
			if stats.ProtocolError.Load() == valid || strings.Contains(out.String(), "event: message_stop") != valid {
				t.Fatalf("argument validation failed: %s", out.String())
			}
		})
	}
	state := &openaiStreamState{tools: map[int]*openAIToolStreamBlock{}}
	call := map[string]any{"index": 0, "id": "call", "function": map[string]any{"name": "tool", "arguments": strings.Repeat("a", maxToolArgumentBytes+1)}}
	if err := processOpenAIToolCallChunk(io.Discard, state, call); err == nil {
		t.Fatal("oversized arguments accepted")
	}
	state = &openaiStreamState{tools: map[int]*openAIToolStreamBlock{}}
	for i := 0; i <= maxToolCalls; i++ {
		err := processOpenAIToolCallChunk(io.Discard, state, map[string]any{"index": i})
		if (err != nil) != (i == maxToolCalls) {
			t.Fatalf("tool bound index=%d err=%v", i, err)
		}
	}
}

func TestHardeningSSEBounds(t *testing.T) {
	if body, err := primeSSEBody(io.NopCloser(strings.NewReader("data: "+strings.Repeat("a", maxInitialSSEBytes+1))), time.Second); err == nil {
		body.Close()
		t.Fatal("oversized initial frame accepted")
	}
	stats := &streamStats{}
	if err := translateOpenAIStreamWithStats(strings.NewReader("data: "+strings.Repeat("a", maxSSEFrameBytes+1)), io.Discard, "m", stats); err == nil || !stats.ProtocolError.Load() {
		t.Fatal("oversized OpenAI frame accepted")
	}
}

func TestHardeningCredentialsExpiryAndCanceledWaiter(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name   string
		expiry time.Time
		want   bool
	}{
		{"future", now.Add(time.Hour), true}, {"expired", now.Add(-time.Second), false}, {"nearly-expired", now.Add(time.Second), false}, {"legacy-unknown", time.Time{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cachedClaudeOAuthCredential{fetchedAt: now, expiresAt: tc.expiry}
			if c.usable(now) != tc.want {
				t.Fatal("wrong expiry eligibility")
			}
		})
	}
	profile := claudeUsageProfile{CredentialsService: "canceled-credential-waiter"}
	done := make(chan struct{})
	claudeCredentialReads.Store(profile.CredentialsService, done)
	defer func() { claudeCredentialReads.Delete(profile.CredentialsService); close(done) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolveClaudeProfileOAuthToken(ctx, profile); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter ignored cancellation: %v", err)
	}
}

func TestHardeningCredentialReadSingleflightAndFailedReconcile(t *testing.T) {
	bin := t.TempDir()
	calls := filepath.Join(bin, "calls")
	script := "#!/bin/sh\nprintf 'read\\n' >> '" + calls + "'\n/bin/sleep 0.03\nprintf '%s' '{\"claudeAiOauth\":{\"accessToken\":\"sk-ant-oat-test-singleflight\",\"expiresAt\":4102444800000}}'\n"
	if err := os.WriteFile(filepath.Join(bin, "security"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	profile := claudeUsageProfile{CredentialsService: "hardening-singleflight"}
	t.Cleanup(func() { claudeOAuthCredentials.Delete(profile.CredentialsService) })
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := resolveClaudeProfileOAuthToken(context.Background(), profile)
			if err != nil || token != "sk-ant-oat-test-singleflight" {
				t.Errorf("token read failed: %v", err)
			}
		}()
	}
	wg.Wait()
	log, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(log), "read") != 1 {
		t.Fatalf("Keychain stampede: calls=%q err=%v", log, err)
	}
	if err := os.WriteFile(filepath.Join(bin, "security"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveClaudeProfileOAuthToken(context.Background(), profile, true); err == nil {
		t.Fatal("failed refresh accepted")
	}
	if token, err := resolveClaudeProfileOAuthToken(context.Background(), profile); err != nil || token != "sk-ant-oat-test-singleflight" {
		t.Fatal("failed reconciliation invalidated valid credential")
	}
}

func TestHardeningCountTokensInvalidResponseDoesNotTryGenerationFallback(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{{200, `{"wrong":1}`}, {200, `{"input_tokens":-1}`}, {200, `{"input_tokens":1.5}`}, {200, `<html>bad</html>`}, {401, `{}`}, {429, `{}`}, {404, `{}`}} {
		t.Run(fmt.Sprint(tc.status, tc.body), func(t *testing.T) {
			var primary, fallback atomic.Int32
			s, err := newProxyServer(config{Providers: map[string]providerConfig{"count-primary": {BaseURL: "http://count.invalid"}, "count-backup": {BaseURL: "http://backup.invalid"}}})
			if err != nil {
				t.Fatal(err)
			}
			s.clients["count-primary"] = &http.Client{Transport: auditRoundTripper(func(*http.Request) (*http.Response, error) {
				primary.Add(1)
				return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			s.clients["count-backup"] = &http.Client{Transport: auditRoundTripper(func(*http.Request) (*http.Response, error) { fallback.Add(1); return nil, errors.New("must not call") })}
			m := modelConfig{Provider: "count-primary", Upstream: "m", Fallbacks: []modelConfig{{Provider: "count-backup", Upstream: "m"}}}
			key := t.Name()
			defer countTokensUnsupported.Delete(key)
			if s.tryUpstreamCountTokens(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/messages/count_tokens", nil), []byte(`{"model":"m"}`), nil, m, key) {
				t.Fatal("invalid count response accepted")
			}
			if primary.Load() != 1 || fallback.Load() != 0 || len(s.providerStates) != 0 {
				t.Fatalf("count affected generation: primary=%d fallback=%d states=%v", primary.Load(), fallback.Load(), s.providerStates)
			}
		})
	}
}

func TestHardeningMenuQuotaSnapshotIsReadOnly(t *testing.T) {
	profile := testClaudeProfile(t, "readonly-status", "missing-keychain-readonly", "readonly-status")
	s := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic", AccountProfiles: map[string]claudeUsageProfile{profile.Name: profile}, FiveHourThresholdPct: 100, SevenDayThresholdPct: 100, StaleTTLSeconds: 1800}}, claudeUse: claudeUsageCache{snapshots: map[string]claudeUsageSnapshot{"cached": {Profile: profile.Name, FetchedAt: time.Now(), FiveHour: claudeUsageWindow{Utilization: 10}, SevenDay: claudeUsageWindow{Utilization: 10}}}}}
	s.providerStates = map[string]providerRuntimeState{}
	s.markProviderBlocked("anthropic@"+profile.Name, "quota_or_rate_limit", time.Minute)
	status := s.claudeUsageStatus()
	raw, _ := json.Marshal(status)
	if !strings.Contains(string(raw), `"routingEligible":false`) || !strings.Contains(string(raw), `"quotaEligible":true`) {
		t.Fatalf("effective and quota eligibility conflated: %s", raw)
	}
	if _, ok := claudeOAuthCredentials.Load(profile.CredentialsService); ok {
		t.Fatal("status read credentials")
	}
}

func TestHardeningURLImageAndUnsupportedDocument(t *testing.T) {
	payload := map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": "https://example.test/image.png"}}}}}}
	raw, err := anthropicToOpenAIRequest(payload, nil)
	if err != nil || !strings.Contains(string(raw), "https://example.test/image.png") {
		t.Fatalf("URL image lost: %s err=%v", raw, err)
	}
	payload["messages"] = []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "document"}}}}
	if _, err := anthropicToOpenAIRequest(payload, nil); err == nil {
		t.Fatal("unsupported document silently discarded")
	}
}

func TestHardeningTelemetryTracksActualUsageAndBackend(t *testing.T) {
	stats := &streamStats{}
	lines := []string{"event: message_start\n", "data: {\"type\":\"message_start\",\"provider\":\"Baseten\",\"message\":{\"usage\":{\"input_tokens\":3,\"cache_read_input_tokens\":10000,\"cache_creation_input_tokens\":500}}}\n"}
	filterSSEEvent(lines, &sseFilterState{}, nil, 99, "m", stats)
	if stats.CacheReadTokens.Load() != 10000 || stats.CacheWriteTokens.Load() != 500 || stats.InputEstimated.Load() {
		t.Fatal("cache and measured input telemetry incorrect")
	}
	if name, _ := stats.ReportedBackend.Load().(string); name != "Baseten" {
		t.Fatal("explicit backend metadata lost")
	}
	stats.LastEventNS.Store(time.Now().Add(-time.Second).UnixNano())
	stats.noteEvent()
	if stats.MaximumGapNS.Load() < int64(time.Second) {
		t.Fatal("event gap not recorded")
	}
	filterSSEEvent([]string{"event: content_block_delta\n", "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n"}, &sseFilterState{}, nil, 0, "m", stats)
	if stats.FirstContentNS.Load() == 0 {
		t.Fatal("first content not recorded")
	}
}

func TestHardeningStatusAndDrain(t *testing.T) {
	s := &proxyServer{cfg: config{SourceHash: "config-hash"}}
	s.activeRequests.Store(2)
	s.draining.Store(true)
	w := httptest.NewRecorder()
	s.status(w, httptest.NewRequest("GET", "/status", nil))
	var status map[string]any
	if json.Unmarshal(w.Body.Bytes(), &status) != nil || status["configHash"] != "config-hash" || status["activeRequests"] != float64(2) || status["draining"] != true {
		t.Fatalf("bad status: %s", w.Body.String())
	}
	w = httptest.NewRecorder()
	s.handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"m"}`)))
	if w.Code != 503 || s.activeRequests.Load() != 2 {
		t.Fatal("drain accepted new work or leaked request count")
	}
}

func TestHardeningClaudePaidGuardMatrix(t *testing.T) {
	for _, model := range []string{"claude-sonnet-5", "claude-opus-4-8", "claude-fable-5-1"} {
		for _, paidProvider := range []string{"vercel-glm53-flash", "vercel-deepseek", "vercel-kimi", "merge-gateway-kimi"} {
			for _, tc := range []struct {
				name         string
				status       int
				kind         string
				sse, allowed bool
			}{
				{"auth", 401, "authentication_error", false, false},
				{"sse-auth", 200, "authentication_error", true, false},
				{"service", 503, "overloaded_error", false, false},
				{"sse-service", 200, "overloaded_error", true, false},
				{"quota", 429, "rate_limit_error", false, true},
				{"sse-quota", 200, "rate_limit_error", true, true},
			} {
				t.Run(model+"/"+paidProvider+"/"+tc.name, func(t *testing.T) {
					key := "anthropic|" + model
					circuitBreakers.Delete(key)
					t.Cleanup(func() { circuitBreakers.Delete(key) })
					var paid atomic.Int32
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/api/oauth/usage" {
							fmt.Fprint(w, `{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`)
							return
						}
						if strings.HasPrefix(r.URL.Path, "/paid/") {
							paid.Add(1)
							fmt.Fprint(w, `{"type":"message"}`)
							return
						}
						if tc.sse {
							w.Header().Set("Content-Type", "text/event-stream")
						}
						w.WriteHeader(tc.status)
						body := fmt.Sprintf(`{"type":"error","error":{"type":%q,"message":"test rejection"}}`, tc.kind)
						if tc.sse {
							fmt.Fprint(w, "event: error\ndata: "+body+"\n\n")
						} else {
							fmt.Fprint(w, body)
						}
					}))
					defer upstream.Close()
					s, err := newProxyServer(config{DefaultProvider: "anthropic", Providers: map[string]providerConfig{"anthropic": {BaseURL: upstream.URL}, paidProvider: {BaseURL: upstream.URL + "/paid"}}, ClaudeUsage: claudeUsageConfig{Provider: "anthropic", EligibleUpstreams: []string{model}, FiveHourThresholdPct: 100, SevenDayThresholdPct: 100, CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000}})
					if err != nil {
						t.Fatal(err)
					}
					r := httptest.NewRequest("POST", "/v1/messages", nil)
					r.Header.Set("Authorization", "Bearer sk-ant-oat-test-matrix")
					runtimeKey := s.circuitKey(s.providerRuntimeKey(r, "anthropic"), modelConfig{Upstream: model})
					circuitBreakers.Delete(runtimeKey)
					t.Cleanup(func() { circuitBreakers.Delete(runtimeKey) })
					m := modelConfig{Requested: model, Provider: "anthropic", Upstream: model, Fallbacks: []modelConfig{{Provider: paidProvider, Upstream: "fallback"}}}
					response, _ := s.doWithFallbacks(context.Background(), r, []byte(`{"messages":[]}`), map[string]any{"messages": []any{}}, m)
					if response.resp != nil {
						response.resp.Body.Close()
					}
					if (paid.Load() == 1) != tc.allowed {
						t.Fatalf("paid attempts=%d allowed=%v", paid.Load(), tc.allowed)
					}
				})
			}
		}
	}
}

func TestHardeningExpiredKeychainTokenIsNeverReturned(t *testing.T) {
	bin := t.TempDir()
	t.Setenv("PATH", bin)
	for _, epoch := range []int64{time.Now().Add(-time.Hour).Unix(), time.Now().Add(-time.Hour).UnixMilli()} {
		profile := claudeUsageProfile{CredentialsService: fmt.Sprint("expired-", epoch)}
		script := fmt.Sprintf("#!/bin/sh\nprintf '%%s' '{\"claudeAiOauth\":{\"accessToken\":\"sk-ant-oat-test-expired\",\"expiresAt\":%d}}'\n", epoch)
		if err := os.WriteFile(filepath.Join(bin, "security"), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		token, err := resolveClaudeProfileOAuthToken(context.Background(), profile)
		claudeOAuthCredentials.Delete(profile.CredentialsService)
		if token != "" || err == nil || !strings.Contains(err.Error(), "expired") {
			t.Fatalf("known-expired credential returned: err=%v", err)
		}
	}
}

func TestHardeningAdaptivePerformanceExpiresOldAndNeutralRecords(t *testing.T) {
	metrics := newMetricsStore(metricsConfig{Path: filepath.Join(t.TempDir(), "metrics.jsonl"), MaxSamples: 100})
	defer metrics.close()
	now := time.Now()
	for _, sample := range []metricsSample{
		{Provider: "p", Upstream: "m", RecordKind: "completion", Timestamp: now.Add(-time.Hour).UTC().Format(time.RFC3339Nano), Success: true},
		{Provider: "p", Upstream: "m", RecordKind: "completion", Timestamp: now.Add(time.Hour).UTC().Format(time.RFC3339Nano), Success: true},
		{Provider: "p", Upstream: "m", RecordKind: "count_tokens", Timestamp: now.UTC().Format(time.RFC3339Nano), Success: true},
		{Provider: "p", Upstream: "m", RecordKind: "attempt", Timestamp: now.UTC().Format(time.RFC3339Nano), FailureReason: "client_cancel"},
		{Provider: "p", Upstream: "m", RecordKind: "attempt", Timestamp: now.UTC().Format(time.RFC3339Nano), FailureReason: "transport"},
		{Provider: "p", Upstream: "m", RecordKind: "completion", Timestamp: now.UTC().Format(time.RFC3339Nano), Success: true},
	} {
		metrics.record(sample)
	}
	s := &proxyServer{metrics: metrics, cfg: config{AdaptiveRouting: adaptiveRoutingConfig{Enabled: true, WindowSamples: 100}}}
	p := s.adaptivePerformance()["p|m"]
	if p.Samples != 2 || p.SuccessRate != .5 {
		t.Fatalf("stale/neutral records affected routing: %+v", p)
	}
}
