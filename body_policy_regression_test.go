package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBodyFailureKeepsAccountPin(t *testing.T) {
	var mu sync.Mutex
	var accounts []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer sk-ant-oat-test-")
		mu.Lock()
		accounts = append(accounts, account)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if account == "first" {
			w.Header().Set("Content-Length", "1000")
			io.WriteString(w, `{"content":`)
			return
		}
		io.WriteString(w, testMessageResponse)
	}))
	defer upstream.Close()
	cfg := compileFixture(t, testClaudeConfig(t))
	p := cfg.Providers["endpoint"]
	p.BaseURL = upstream.URL
	cfg.Providers["endpoint"] = p
	seedTestCredentials(t, cfg)
	s := testProxy(t, cfg)
	t.Cleanup(func() { s.events.close(); s.usageHistory.close(); _ = s.closeClaudeAccountPins() })
	t.Cleanup(func() { _ = s.closeClaudeAccountPins() })
	payload := map[string]any{"model": "test", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	selected := cfg.Models["test"]
	selected.Requested = "test"
	key := routingStickyKey(selected, payload)
	s.refreshClaudeAccountLease(key, cfg.ClaudeUsage.AccountProfiles["first"])
	out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	mu.Lock()
	got := strings.Join(accounts, ",")
	mu.Unlock()
	if got != "first" || out.Code != 502 {
		t.Fatalf("calls=%s status=%d", got, out.Code)
	}
	s.claudeAccountMu.Lock()
	pin := s.claudeAccounts[key]
	s.claudeAccountMu.Unlock()
	if pin.Profile.Name != "first" {
		t.Fatal("body failure changed account pin")
	}
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	for _, state := range s.providerStates {
		if state.InstabilitySignals != 0 || !state.QuarantinedUntil.IsZero() {
			t.Fatal("body failure penalized account")
		}
	}
}

func TestStalledBodyLegacyPaidPolicy(t *testing.T) {
	for _, status := range []int{200, 400, 413, 402, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var paid atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/oauth/usage" {
					io.WriteString(w, `{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if strings.HasPrefix(r.URL.Path, "/paid/") {
					paid.Add(1)
					io.WriteString(w, testMessageResponse)
					return
				}
				w.WriteHeader(status)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer upstream.Close()
			model := fmt.Sprintf("claude-sonnet-test-%d", status)
			s := testProxy(t, config{DefaultProvider: "anthropic", BodyLimits: bodyLimitsConfig{JSONReadTimeoutMS: 30, ErrorReadTimeoutMS: 30}, Providers: map[string]providerConfig{"anthropic": {BaseURL: upstream.URL}, "vercel-glm53-flash": {BaseURL: upstream.URL + "/paid"}}, ClaudeUsage: claudeUsageConfig{Provider: "anthropic", EligibleUpstreams: []string{model}, FiveHourThresholdPct: 100, SevenDayThresholdPct: 100, CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000}})
			r := httptest.NewRequest("POST", "/v1/messages", nil)
			r.Header.Set("Authorization", "Bearer sk-ant-oat-test-body-policy")
			m := modelConfig{Requested: model, Provider: "anthropic", Upstream: model, Fallbacks: []modelConfig{{Provider: "vercel-glm53-flash", Upstream: "fallback"}}}
			response, err := s.doWithFallbacks(context.Background(), r, []byte(`{"messages":[]}`), map[string]any{"messages": []any{}}, m)
			if response.resp != nil {
				defer response.resp.Body.Close()
			}
			allowed := status == 402 || status == 429
			if (paid.Load() == 1) != allowed {
				t.Fatalf("paid=%d allowed=%v error=%v", paid.Load(), allowed, err)
			}
			if status == 400 || status == 413 {
				if err != nil || response.resp == nil || response.resp.StatusCode != status {
					t.Fatalf("client status lost: response=%v err=%v", response.resp, err)
				}
			}
		})
	}
}

func TestStalledClientRejectionDoesNotReplay(t *testing.T) {
	var backup atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/backup" {
			backup.Add(1)
			io.WriteString(w, testMessageResponse)
			return
		}
		w.WriteHeader(413)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	f := testConfig(upstream.URL, "anthropic")
	p := f.Providers["endpoint"]
	p.MessagesPath = "/backup"
	f.Providers["backup"] = p
	m := f.Models["model"]
	m.Provider = "backup"
	f.Models["backup"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "backup"}}}
	f.BodyLimits.ErrorReadTimeoutMS = 30
	s := testProxy(t, compileFixture(t, f))
	t.Cleanup(func() { s.events.close(); s.usageHistory.close(); _ = s.closeClaudeAccountPins() })
	out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	if out.Code != 413 || backup.Load() != 0 {
		t.Fatalf("status=%d replay=%d", out.Code, backup.Load())
	}
}

func TestBodyCancellationRecordsAttemptAndReleasesReservations(t *testing.T) {
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer upstream.Close()
	s := testProxy(t, compileFixture(t, testConfig(upstream.URL, "anthropic")))
	t.Cleanup(func() { s.events.close(); s.usageHistory.close(); _ = s.closeClaudeAccountPins() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[]}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.handler().ServeHTTP(httptest.NewRecorder(), r) }()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation stalled")
	}
	found := false
	for _, sample := range s.metrics.readPerRoute(100) {
		if sample.RecordKind == "attempt" && sample.FailureReason == "client_cancel" {
			found = true
		}
	}
	if !found {
		t.Fatal("cancelled body attempt missing")
	}
	if s.passiveConcurrency.snapshot().Total != 0 {
		t.Fatal("reservation leaked")
	}
}

func TestStalledBodyConfiguredPaidEvidence(t *testing.T) {
	for _, tc := range []struct {
		status    int
		confirmed bool
		want      int32
	}{{200, false, 0}, {402, false, 1}, {503, false, 0}, {503, true, 1}} {
		t.Run(fmt.Sprintf("%d-confirmed-%v", tc.status, tc.confirmed), func(t *testing.T) {
			for _, key := range []string{"endpoint|test-model", "paid|test-model"} {
				circuitBreakers.Delete(key)
				t.Cleanup(func() { circuitBreakers.Delete(key) })
			}
			var paid atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/paid" {
					paid.Add(1)
					io.WriteString(w, testMessageResponse)
					return
				}
				w.WriteHeader(tc.status)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer upstream.Close()
			f := testConfig("https://example.invalid", "anthropic")
			p := f.Providers["endpoint"]
			p.Billing = "subscription"
			p.Auth = authDefinition{Type: "env", Name: "BODY_POLICY_TEST_KEY"}
			t.Setenv("BODY_POLICY_TEST_KEY", "synthetic-test-key")
			f.Providers["endpoint"] = p
			p.Billing = "metered"
			p.MessagesPath = "/paid"
			f.Providers["paid"] = p
			m := f.Models["model"]
			m.Provider = "paid"
			f.Models["paid"] = m
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "paid"}}, AllowPaidFallback: true, PaidFallbackOn: []string{"quota-exhausted", "confirmed-provider-outage"}}
			f.BodyLimits = bodyLimitsConfig{JSONReadTimeoutMS: 30, ErrorReadTimeoutMS: 30}
			cfg := compileFixture(t, f)
			for name, provider := range cfg.Providers {
				provider.BaseURL = upstream.URL
				cfg.Providers[name] = provider
			}
			s := testProxy(t, cfg)
			t.Cleanup(func() { s.events.close(); s.usageHistory.close(); _ = s.closeClaudeAccountPins() })
			if tc.confirmed {
				s.updateOutageEvidence("endpoint|test-model", "prior-one", 503)
				s.updateOutageEvidence("endpoint|test-model", "prior-two", 503)
			}
			sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
			if paid.Load() != tc.want {
				t.Fatalf("paid=%d want=%d", paid.Load(), tc.want)
			}
		})
	}
}
