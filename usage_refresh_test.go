package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduledUsageRefreshDoesNotReuseNearExpiryCache(t *testing.T) {
	var calls atomic.Int32
	var unavailable atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if unavailable.Load() {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte(`{"five_hour":{"utilization":20},"seven_day":{"utilization":30}}`))
	}))
	defer upstream.Close()
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"endpoint": {BaseURL: upstream.URL}}, ClaudeUsage: claudeUsageConfig{Provider: "endpoint", CacheTTLSeconds: 60, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	auth := "Bearer test-scheduled-usage"
	profile := claudeUsageProfile{Name: "work", CachePath: filepath.Join(t.TempDir(), "missing.json")}
	old := time.Now().Add(-59 * time.Second)
	s.claudeUse.snapshots[claudeTokenKey(auth)] = claudeUsageSnapshot{Profile: profile.Name, Source: "oauth-api", FetchedAt: old, FiveHour: claudeUsageWindow{Utilization: 40}}
	cached, err := s.claudeUsageSnapshotWithAuth(context.Background(), auth, profile)
	if err != nil || calls.Load() != 0 || !cached.FetchedAt.Equal(old) {
		t.Fatal("ordinary request should reuse fresh cache")
	}
	fresh, err := s.claudeUsageSnapshotRefresh(context.Background(), auth, profile, true)
	if err != nil || calls.Load() != 1 || !fresh.FetchedAt.After(old) || fresh.FiveHour.Utilization != 20 {
		t.Fatal("scheduled fetch was skipped at TTL boundary")
	}
	unavailable.Store(true)
	stale, err := s.claudeUsageSnapshotRefresh(context.Background(), auth, profile, true)
	if err != nil || calls.Load() != 2 || stale.Source != "oauth-api-stale" || !stale.FetchedAt.Equal(fresh.FetchedAt) {
		t.Fatal("failed refresh must retain original timestamp and stale source")
	}
}

func TestClaudeDashboardAllowsRefreshLatencyButExpiresOldUsage(t *testing.T) {
	s := testProxy(t, compileFixture(t, testClaudeConfig(t)))
	s.cfg.ClaudeUsage.CacheTTLSeconds = 60
	now := time.Now().UTC()
	s.clockNow = func() time.Time { return now }
	for _, tc := range []struct {
		age  time.Duration
		want string
	}{{65 * time.Second, "OK"}, {76 * time.Second, "STALE"}} {
		s.claudeUse.snapshots = map[string]claudeUsageSnapshot{"test": {Profile: "first", Source: "oauth-api", FetchedAt: now.Add(-tc.age), FiveHour: claudeUsageWindow{Utilization: 20, ResetsAt: now.Add(time.Hour)}}}
		w := httptest.NewRecorder()
		s.handler().ServeHTTP(w, httptest.NewRequest("GET", "/dashboard", nil))
		var out dashboardSnapshot
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if out.Accounts[0].State != tc.want {
			t.Fatalf("age=%v state=%s want=%s", tc.age, out.Accounts[0].State, tc.want)
		}
	}
}

func TestClaudeUsageRateLimitCooldown(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"endpoint": {BaseURL: upstream.URL}}, ClaudeUsage: claudeUsageConfig{Provider: "endpoint", CacheTTLSeconds: 60, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	profile := claudeUsageProfile{Name: "work", CachePath: filepath.Join(t.TempDir(), "missing.json")}
	auth := "Bearer rate-limited-usage"
	key := claudeTokenKey(auth)
	old := time.Now().Add(-time.Minute)
	s.claudeUse.snapshots[key] = claudeUsageSnapshot{Profile: profile.Name, Source: "oauth-api", FetchedAt: old}
	for i := 0; i < 3; i++ {
		cached, err := s.claudeUsageSnapshotRefresh(context.Background(), auth, profile, true)
		if err != nil || !cached.FetchedAt.Equal(old) || cached.Source != "oauth-api-stale" {
			t.Fatal("cooldown must preserve stale snapshot", cached, err)
		}
	}
	if calls.Load() != 1 || time.Until(s.claudeUse.retryAt[key]) < 119*time.Second {
		t.Fatal("Retry-After was not respected")
	}
	delete(s.claudeUse.snapshots, key)
	if _, err := s.claudeUsageSnapshotRefresh(context.Background(), auth, profile, true); err == nil || calls.Load() != 1 {
		t.Fatal("missing cache must not bypass cooldown")
	}
	s.claudeUse.retryAt[key] = time.Now().Add(-time.Second)
	s.claudeUsageSnapshotRefresh(context.Background(), auth, profile, true)
	if calls.Load() != 2 {
		t.Fatal("expired cooldown must allow recovery")
	}
	// Usage endpoint throttling must never block inference providers.
	if blocked, _ := s.providerBlocked("endpoint"); blocked {
		t.Fatal("usage throttling blocked inference")
	}
}
