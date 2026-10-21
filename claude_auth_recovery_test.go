package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func authRecoveryServer(t *testing.T, handler http.HandlerFunc) (*proxyServer, claudeUsageProfile) {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)
	s, err := newProxyServer(config{
		Providers:   map[string]providerConfig{"fixture": {BaseURL: upstream.URL}},
		ClaudeUsage: claudeUsageConfig{Provider: "fixture", RequestTimeoutMS: 1000, CacheTTLSeconds: 60, StaleTTLSeconds: 300},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, claudeUsageProfile{Name: "primary"}
}

func quarantineFixture(s *proxyServer, p claudeUsageProfile) {
	for i := 0; i < claudeCredentialQuarantineThreshold; i++ {
		s.noteClaudeCredentialRefreshResult(p, false)
	}
}

func TestUsageAuthRecoveryAfterManualLogin(t *testing.T) {
	s, profile := authRecoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/oauth/usage" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Errorf("unexpected verification request: %s", r.URL.Path)
		}
		w.Write([]byte(`{"five_hour":{"utilization":20},"seven_day":{"utilization":30}}`))
	})
	quarantineFixture(s, profile)
	quarantineFixture(s, claudeUsageProfile{Name: "other"})
	s.markProviderBlocked("fixture@primary#fable", "fable_quota_rejected", time.Hour)
	before := time.Now().Add(-time.Second)
	if _, err := s.claudeUsageSnapshotRefresh(context.Background(), "Bearer fixture-token", profile, true); err != nil {
		t.Fatal(err)
	}
	if blocked, state := s.providerBlocked("fixture@primary"); blocked {
		t.Fatalf("manual-login recovery left account blocked: %+v", state)
	}
	if blocked, _ := s.providerBlocked("fixture@other"); !blocked {
		t.Fatal("recovery cleared another account")
	}
	if blocked, _ := s.providerBlocked("fixture@primary#fable"); !blocked {
		t.Fatal("recovery cleared model quota")
	}
	// An older helper completing late must not undo newer live authentication.
	for i := 0; i < claudeCredentialQuarantineThreshold; i++ {
		s.noteClaudeCredentialRefreshResult(profile, false, before)
	}
	if blocked, _ := s.providerBlocked("fixture@primary"); blocked {
		t.Fatal("older refresh failures re-quarantined recovered account")
	}
	// New failures still count, starting from zero after recovery.
	s.noteClaudeCredentialRefreshResult(profile, false, time.Now())
	if blocked, _ := s.providerBlocked("fixture@primary"); blocked {
		t.Fatal("old failure count survived recovery")
	}
}

func TestUsageAuthRecoveryRejectsCachedOrFailedProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		cached bool
	}{
		{"cache hit", 200, `{}`, true},
		{"unauthorized stale fallback", 401, `{}`, false},
		{"rate limited stale fallback", 429, `{}`, false},
		{"invalid JSON", 200, `not-json`, false},
		{"invalid utilization", 200, `{"five_hour":{"utilization":101}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			s, profile := authRecoveryServer(t, func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			})
			quarantineFixture(s, profile)
			s.claudeUse.snapshots = map[string]claudeUsageSnapshot{
				claudeTokenKey("Bearer fixture-token"): {Profile: profile.Name, Source: "oauth-api", FetchedAt: time.Now()},
			}
			s.claudeUsageSnapshotRefresh(context.Background(), "Bearer fixture-token", profile, !tc.cached)
			if blocked, _ := s.providerBlocked("fixture@primary"); !blocked {
				t.Fatal("unverified or cached data cleared quarantine")
			}
			if tc.cached && calls != 0 {
				t.Fatal("cache hit made an unnecessary verification request")
			}
		})
	}
}

func TestUsageAuthRecoveryPreservesOtherBlocks(t *testing.T) {
	for _, reason := range []string{"quota_or_rate_limit", "auth_exhausted", "auth_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			s, profile := authRecoveryServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
			})
			s.markProviderBlocked("fixture@primary", reason, time.Hour)
			if _, err := s.claudeUsageSnapshotRefresh(context.Background(), "Bearer fixture-token", profile, true); err != nil {
				t.Fatal(err)
			}
			if blocked, state := s.providerBlocked("fixture@primary"); !blocked || state.Reason != reason {
				t.Fatalf("usage authentication overrode a different restriction: %+v", state)
			}
		})
	}
}

func TestUsageAuthRecoveryPreservesNewerFailure(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, profile := authRecoveryServer(t, func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
	})
	quarantineFixture(s, profile)
	done := make(chan error, 1)
	go func() {
		_, err := s.claudeUsageSnapshotRefresh(context.Background(), "Bearer fixture-token", profile, true)
		done <- err
	}()
	<-started
	s.noteClaudeCredentialRefreshResult(profile, false, time.Now())
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if blocked, _ := s.providerBlocked("fixture@primary"); !blocked {
		t.Fatal("older usage response cleared newer auth failure")
	}
}
