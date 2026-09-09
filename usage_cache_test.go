package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUsageAdmissionAcrossAccountsAndRotatedTokens(t *testing.T) {
	var active, calls atomic.Int32
	var overlap atomic.Bool
	var limited atomic.Bool
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if active.Add(1) > 1 {
			overlap.Store(true)
		}
		defer active.Add(-1)
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		if limited.Load() {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			return
		}
		w.Write([]byte(`{"five_hour":{"utilization":20},"seven_day":{"utilization":30}}`))
	}))
	defer up.Close()
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"a": {BaseURL: up.URL}}, ClaudeUsage: claudeUsageConfig{Provider: "a", RequestTimeoutMS: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, p := range []string{"first", "second"} {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			if _, e := s.fetchClaudeUsageSnapshot(context.Background(), p, p); e != nil {
				t.Error(e)
			}
		}(p)
	}
	wg.Wait()
	if overlap.Load() || calls.Load() != 2 {
		t.Fatal("cross-account usage fetch overlap", calls.Load())
	}
	limited.Store(true)
	s.fetchClaudeUsageSnapshot(context.Background(), "first-old-token", "first")
	if _, err = s.fetchClaudeUsageSnapshot(context.Background(), "first-new-token", "first"); err == nil {
		t.Fatal("rotated token bypassed global cooldown")
	}
	if _, err = s.fetchClaudeUsageSnapshot(context.Background(), "second", "second"); err == nil || calls.Load() != 3 {
		t.Fatal("other account bypassed cooldown", calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if release, e := s.admitClaudeUsageFetch(ctx); e == nil {
		release()
		t.Fatal("canceled admission accepted")
	}
}

func TestUsageCacheRestartAndIdentityIsolation(t *testing.T) {
	cfg := compileFixture(t, testClaudeConfig(t))
	if err := os.MkdirAll(cfg.Definition.StateDir, 0700); err != nil {
		t.Fatal(err)
	}
	s := testProxy(t, cfg)
	at := time.Now().Add(-2 * time.Minute)
	s.claudeUse.snapshots["token-must-not-persist"] = claudeUsageSnapshot{Profile: "first", FetchedAt: at, Source: "oauth-api", TokenKey: "secret", FiveHour: claudeUsageWindow{Utilization: 30}}
	s.claudeUse.globalRetryAt = time.Now().Add(time.Minute)
	s.persistClaudeUsageCache()
	restarted := testProxy(t, cfg)
	v, ok := restarted.claudeUse.snapshots["restored:first"]
	if !ok || !v.FetchedAt.Equal(at) || v.FiveHour.Utilization != 30 || v.TokenKey != "" {
		t.Fatal("restart lost or changed cached quota", v)
	}
	if !restarted.claudeUse.globalRetryAt.After(time.Now()) {
		t.Fatal("restart lost cooldown")
	}
	info, err := os.Stat(s.usageCachePath())
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("cache permissions", err)
	}
	p := cfg.ClaudeUsage.AccountProfiles["first"]
	p.CredentialsService = "changed-account"
	cfg.ClaudeUsage.AccountProfiles["first"] = p
	changed := testProxy(t, cfg)
	if len(changed.claudeUse.snapshots) != 0 {
		t.Fatal("cache reused across changed account identity")
	}
}
