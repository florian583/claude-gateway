package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDashboardReadOnlyConfiguredProjection(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer upstream.Close()
	f := testClaudeConfig(t)
	profile := f.Profiles["first"]
	profile.DisplayName = "Primary account"
	f.Profiles["first"] = profile
	p := f.Providers["endpoint"]
	p.DisplayName = "Claude subscription"
	p.BaseURL = "https://api.anthropic.com"
	f.Providers["endpoint"] = p
	f.Providers["paid"] = providerDefinition{Protocol: "openai-chat-completions", BaseURL: "https://provider.example", Billing: "metered", Auth: authDefinition{Type: "env", Name: "UNREAD_SECRET"}}
	cfg := compileFixture(t, f)
	for id, provider := range cfg.Providers {
		provider.BaseURL = upstream.URL
		cfg.Providers[id] = provider
	}
	s := testProxy(t, cfg)
	now := time.Now().UTC()
	s.clockNow = func() time.Time { return now }
	s.claudeUse.snapshots = map[string]claudeUsageSnapshot{"opaque-token-key": {Profile: "first", Source: "oauth-api", FetchedAt: now, TokenKey: "secret-canary", FiveHour: claudeUsageWindow{Utilization: 25, ResetsAt: now.Add(time.Hour)}, SevenDay: claudeUsageWindow{Utilization: 50, ResetsAt: now.Add(24 * time.Hour)}}}
	s.providerStates = map[string]providerRuntimeState{"endpoint@second": {BlockedUntil: now.Add(time.Hour), Reason: "authentication_error"}}
	before, _ := json.Marshal(s.providerStates)
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, httptest.NewRequest("GET", "/dashboard", nil))
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("bad response: %d", w.Code)
	}
	for _, value := range []string{"secret-canary", "opaque-token-key", "credentialsService", "configDir", "UNREAD_SECRET", upstream.URL} {
		if strings.Contains(w.Body.String(), value) {
			t.Fatalf("dashboard exposed forbidden field/value category")
		}
	}
	var out dashboardSnapshot
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Version != 1 || len(out.Accounts) != 3 || len(out.Providers) != 1 || out.Providers[0].State != "UNKNOWN" || out.Providers[0].Billing != "metered" {
		t.Fatalf("unexpected projection: %+v", out)
	}
	if out.Accounts[0].Name != "Primary account" || out.Accounts[0].State != "OK" || out.Accounts[0].Windows[0].UsedPct != 25 || out.Accounts[1].State != "AUTH" {
		t.Fatalf("bad accounts: %+v", out.Accounts)
	}
	if out.Accounts[2].State != "UNKNOWN" || len(out.Accounts[2].Windows) != 0 {
		t.Fatal("unknown must not claim 100% remaining")
	}
	after, _ := json.Marshal(s.providerStates)
	if !reflect.DeepEqual(before, after) || calls.Load() != 0 || len(s.claudeUse.refreshing) != 0 {
		t.Fatal("dashboard mutated state or contacted provider")
	}
	for _, method := range []string{"POST", "PUT", "DELETE"} {
		w = httptest.NewRecorder()
		s.handler().ServeHTTP(w, httptest.NewRequest(method, "/dashboard", nil))
		if w.Code != 405 {
			t.Fatal("dashboard allows mutation method")
		}
	}
}

func TestDashboardFreshnessAndWindowReset(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		used      float64
		at, reset time.Time
		want      string
	}{
		{100, now.Add(-time.Hour), now.Add(time.Hour), "STALE"},
		{100, now, now.Add(-time.Second), "STALE"},
		{100, now, now.Add(time.Hour), "FULL"},
		{90, now, now.Add(time.Hour), "LOW"},
		{10, now.Add(time.Minute), now.Add(time.Hour), "STALE"},
	} {
		got := dashboardCapacity([]dashboardWindow{{Name: "fiveHour", UsedPct: test.used, ResetsAt: &test.reset}}, test.at, now, time.Minute)
		if got != test.want {
			t.Fatalf("got %s want %s", got, test.want)
		}
	}
}

func TestDashboardFableLimitIsModelSpecificAndReadOnly(t *testing.T) {
	f := testClaudeConfig(t)
	m := f.Models["model"]
	m.Upstream = "claude-fable-5-1"
	f.Models["fable"] = m
	s := testProxy(t, compileFixture(t, f))
	now := time.Now()
	s.providerStates["endpoint@first#fable"] = providerRuntimeState{Reason: "fable_quota_rejected", LastFailure: now.Add(-2 * time.Hour), BlockedUntil: now.Add(time.Hour)}
	before, _ := json.Marshal(s.providerStates)
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, httptest.NewRequest("GET", "/dashboard", nil))
	var out dashboardSnapshot
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	for _, row := range out.Accounts {
		if row.ID == "first" && (row.State != "FABLE LIMIT" || row.FableResetAt == nil) {
			t.Fatalf("missing model restriction: %+v", row)
		}
	}
	after, _ := json.Marshal(s.providerStates)
	if string(before) != string(after) {
		t.Fatal("dashboard mutated quota state")
	}
}

func TestDashboardTrafficCompletionAccounting(t *testing.T) {
	s := testProxy(t, compileFixture(t, testClaudeConfig(t)))
	now := time.Now().UTC()
	base := metricsSample{Timestamp: now.Format(time.RFC3339Nano), RequestID: "request-a", Provider: "endpoint", ClaudeProfile: "first", RequestedModel: "worker", Upstream: "test-model", RecordKind: "completion", Status: 200, Success: true, TokensPerSecond: 80, StreamError: "do-not-expose", IsFallback: true}
	s.metrics.record(base)
	attempt := base
	attempt.RecordKind = "attempt"
	attempt.Success = false
	attempt.Status = 503
	s.metrics.record(attempt)
	skip := base
	skip.RequestID = "skip"
	skip.RecordKind = "skip"
	s.metrics.record(skip)
	terminal := base
	terminal.RecordKind = "terminal"
	s.metrics.record(terminal)
	count := base
	count.RecordKind = "count_tokens"
	s.metrics.record(count)
	failed := base
	failed.RequestID = "failed"
	failed.Success = false
	failed.RecordKind = "attempt"
	failed.Status = 503
	failed.TokensPerSecond = 0
	s.metrics.record(failed)
	out := s.dashboardSnapshot(now.Add(time.Second))
	if out.Current == nil || out.Current.Route != "endpoint / first" || len(out.Traffic) != 1 || out.Traffic[0].OK != 1 || out.Traffic[0].Errors != 1 || out.Traffic[0].Fallbacks != 2 || out.Traffic[0].TPS != 80 {
		t.Fatalf("bad accounting: %+v", out)
	}
	encoded, _ := json.Marshal(out)
	if strings.Contains(string(encoded), "do-not-expose") {
		t.Fatal("raw stream error leaked")
	}
	if later := s.dashboardSnapshot(now.Add(31 * time.Minute)); later.Current != nil || len(later.Traffic) != 0 {
		t.Fatal("stale flow shown as current")
	}
}

func TestDashboardConcurrentSnapshots(t *testing.T) {
	s := testProxy(t, compileFixture(t, testClaudeConfig(t)))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = s.dashboardSnapshot(time.Now())
				s.metrics.record(metricsSample{RecordKind: "skip"})
			}
		}()
	}
	wg.Wait()
}
