package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfiguredQuotaPrecheckReturns429WithoutInference(t *testing.T) {
	var inference atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/oauth/usage" {
			_, _ = w.Write([]byte(`{"five_hour":{"utilization":0},"seven_day":{"utilization":100}}`))
			return
		}
		inference.Add(1)
		w.WriteHeader(500)
	}))
	defer up.Close()
	f := testClaudeConfig(t)
	p := f.Providers["endpoint"]
	p.Usage.Mode = "api"
	f.Providers["endpoint"] = p
	m := f.Models["model"]
	m.Upstream = "claude-haiku-5-5"
	f.Models["model"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}}}
	cfg := compileFixture(t, f)
	provider := cfg.Providers["endpoint"]
	provider.BaseURL = up.URL
	cfg.Providers["endpoint"] = provider
	seedTestCredentials(t, cfg)
	s := testProxy(t, cfg)
	w := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"test"}]}`)
	assertRateLimitResponse(t, w, 60)
	if inference.Load() != 0 {
		t.Fatal("quota precheck sent inference")
	}
	if !strings.Contains(w.Body.String(), "gateway quota precheck") || !strings.Contains(w.Body.String(), "no upstream request sent") {
		t.Fatalf("quota precheck origin unclear: %s", w.Body.String())
	}
}

func TestTransientAnthropicRateLimitIsolation(t *testing.T) {
	s, err := newProxyServer(config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}})
	if err != nil {
		t.Fatal(err)
	}
	haiku := modelConfig{Provider: "anthropic", Upstream: "claude-haiku-5-5"}
	account := "anthropic@fixture-a"
	key := s.providerFailureBlockKey("anthropic", account, haiku, http.Header{}, temporaryRateLimitReason)
	if key != routeHealthKey(account, haiku) {
		t.Fatalf("temporary throttle scope=%q; want account+model", key)
	}
	s.markProviderBlocked(key, temporaryRateLimitReason, time.Minute)
	if blocked, _ := s.providerBlockForCandidate(account, haiku); !blocked {
		t.Fatal("same account/model throttle bypassed")
	}
	for _, upstream := range []string{"claude-opus-5-5", "claude-fable-5-1"} {
		if blocked, _ := s.providerBlockForCandidate(account, modelConfig{Upstream: upstream}); blocked {
			t.Fatalf("temporary Haiku throttle blocked %s", upstream)
		}
	}
	if blocked, _ := s.providerBlockForCandidate("anthropic@fixture-b", haiku); blocked {
		t.Fatal("one account blocked a different account")
	}
	for _, window := range []string{"5h", "7d"} {
		h := http.Header{}
		h.Set("Anthropic-Ratelimit-Unified-"+window+"-Status", "rejected")
		if got := s.providerFailureBlockKey("anthropic", account, haiku, h, temporaryRateLimitReason); got != account {
			t.Fatalf("confirmed shared %s rejection scope=%q; want account", window, got)
		}
	}
	if got := s.providerFailureBlockKey("anthropic", account, haiku, http.Header{}, "quota_or_rate_limit"); got != account {
		t.Fatalf("unknown hard quota must stay conservative: %s", got)
	}
}

func rateLimitFixture(t *testing.T, upstreamURL string) *proxyServer {
	t.Helper()
	s, err := newProxyServer(config{
		DefaultProvider: "anthropic",
		Providers:       map[string]providerConfig{"anthropic": {BaseURL: upstreamURL}},
		ClaudeUsage:     claudeUsageConfig{Provider: "anthropic"},
		Models: map[string]modelConfig{
			"haiku": {Provider: "anthropic", Upstream: "claude-haiku-5-5"},
			"opus":  {Provider: "anthropic", Upstream: "claude-opus-5-5"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func rateLimitRequest(s *proxyServer, model string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"`+model+`","messages":[]}`))
	r.Header.Set("Authorization", "Bearer sk-ant-oat-test-only")
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, r)
	return w
}

func assertRateLimitResponse(t *testing.T, w *httptest.ResponseRecorder, maximum int) {
	t.Helper()
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d; want 429; body=%s", w.Code, w.Body.String())
	}
	retry, err := strconv.Atoi(w.Header().Get("Retry-After"))
	if err != nil || retry < 1 || retry > maximum {
		t.Fatalf("invalid Retry-After=%q; maximum=%d", w.Header().Get("Retry-After"), maximum)
	}
	var body struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Error.Type != "rate_limit_error" {
		t.Fatalf("not an Anthropic rate-limit error: %s", w.Body.String())
	}
}

func TestCachedRateLimitReturns429WithoutProbing(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer up.Close()
	s := rateLimitFixture(t, up.URL)
	r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	r.Header.Set("Authorization", "Bearer sk-ant-oat-test-only")
	s.markProviderBlocked(s.providerRuntimeKey(r, "anthropic"), temporaryRateLimitReason, 90*time.Second)
	assertRateLimitResponse(t, rateLimitRequest(s, "haiku"), 90)
	if calls.Load() != 0 {
		t.Fatal("cached throttle sent an upstream probe")
	}
}

func TestFinalHTTP429IsRememberedWithoutBlockingOpus(t *testing.T) {
	var haikuCalls, opusCalls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if body.Model == "claude-haiku-5-5" {
			haikuCalls.Add(1)
			w.Header().Set("Retry-After", "45")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"fixture throttle"}}`))
			return
		}
		opusCalls.Add(1)
		_, _ = w.Write([]byte(`{"type":"message","model":"claude-opus-5-5","content":[],"stop_reason":"end_turn"}`))
	}))
	defer up.Close()
	s := rateLimitFixture(t, up.URL)
	assertRateLimitResponse(t, rateLimitRequest(s, "haiku"), 45)
	assertRateLimitResponse(t, rateLimitRequest(s, "haiku"), 45)
	if haikuCalls.Load() != 1 {
		t.Fatalf("throttled final leg called %d times", haikuCalls.Load())
	}
	if w := rateLimitRequest(s, "opus"); w.Code != 200 || opusCalls.Load() != 1 {
		t.Fatalf("Opus affected by Haiku throttle: status=%d calls=%d", w.Code, opusCalls.Load())
	}
}

func TestInitialSSEThrottleReturns429AndIsRemembered(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Retry-After", "30")
		_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"fixture throttle\"}}\n\n"))
	}))
	defer up.Close()
	s := rateLimitFixture(t, up.URL)
	assertRateLimitResponse(t, rateLimitRequest(s, "haiku"), 30)
	assertRateLimitResponse(t, rateLimitRequest(s, "haiku"), 30)
	if calls.Load() != 1 {
		t.Fatalf("pre-output throttle replayed %d times", calls.Load())
	}
}

func TestConfirmedSharedQuotaRejectStillBlocksOtherModels(t *testing.T) {
	for _, kind := range []string{"shared-header", "http-usage-limit", "sse-usage-limit"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "45")
				if kind == "sse-usage-limit" {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"weekly usage limit reached\"}}\n\n"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				message := "weekly usage limit reached"
				if kind == "shared-header" {
					w.Header().Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
					message = "fixture throttle"
				}
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]string{"type": "rate_limit_error", "message": message}})
			}))
			defer up.Close()
			s := rateLimitFixture(t, up.URL)
			assertRateLimitResponse(t, rateLimitRequest(s, "haiku"), 45)
			assertRateLimitResponse(t, rateLimitRequest(s, "opus"), 45)
			if calls.Load() != 1 {
				t.Fatalf("confirmed shared quota allowed another model call: %d", calls.Load())
			}
		})
	}
}
