package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCredentialRefreshOutcome(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		rotate, expired, verified bool
		scheduled                 bool
		rejected                  bool
		exit                      int
		wantOK                    bool
	}{
		{name: "verified existing token", verified: true, wantOK: true},
		{name: "rotated despite helper error", rotate: true, exit: 1, wantOK: true},
		{name: "unverified unchanged token"},
		{name: "expired token with proof", expired: true, verified: true},
		{name: "rotated expired token", rotate: true, expired: true},
		{name: "failed helper with proof", verified: true, exit: 1},
		{name: "scheduled still valid", scheduled: true, wantOK: true},
		{name: "scheduled failed helper", scheduled: true, exit: 1},
		{name: "scheduled expired", scheduled: true, expired: true},
		{name: "rotation explicitly rejected", rotate: true, rejected: true, exit: 1},
		{name: "scheduled explicitly rejected", scheduled: true, rejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			state := filepath.Join(dir, "credential.json")
			before := `{"claudeAiOauth":{"accessToken":"sk-ant-oat-test-fixture-before","expiresAt":4102444800000}}`
			token := "sk-ant-oat-test-fixture-before"
			if tc.rotate {
				token = "sk-ant-oat-test-fixture-after"
			}
			expiry := int64(4102444800000)
			if tc.expired {
				expiry = 1
			}
			after := fmt.Sprintf(`{"claudeAiOauth":{"accessToken":%q,"expiresAt":%d}}`, token, expiry)
			if err := os.WriteFile(state, []byte(before), 0600); err != nil {
				t.Fatal(err)
			}
			proof := "{}"
			if tc.verified {
				proof = `{"version":1,"authVerified":true}`
			}
			if tc.rejected {
				proof = `{"version":1,"authRejected":true}`
			}
			for name, script := range map[string]string{
				"security": fmt.Sprintf("#!/bin/sh\n/bin/cat %q\n", state),
				"refresh":  fmt.Sprintf("#!/bin/sh\n[ \"$CLAUDE_OAUTH_REFRESH_REASON\" = %q ] || exit 64\nprintf '%%s' '%s' > %q\nprintf '%%s' '%s'\nexit %d\n", map[bool]string{true: "scheduled", false: "auth_error"}[tc.scheduled], after, state, proof, tc.exit),
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("PATH", dir)
			f := testClaudeConfig(t)
			p := f.Profiles["first"]
			p.RefreshCommand = []string{filepath.Join(dir, "refresh")}
			f.Profiles["first"] = p
			cfg := compileFixture(t, f)
			profile := cfg.ClaudeUsage.AccountProfiles["first"]
			t.Cleanup(func() { claudeOAuthCredentials.Delete(profile.CredentialsService) })
			s := testProxy(t, cfg)
			ctx := context.Background()
			if tc.scheduled {
				ctx = context.WithValue(ctx, claudeRefreshReasonKey{}, claudeRefreshScheduled)
			}
			t.Setenv("CLAUDE_OAUTH_REFRESH_REASON", "inherited-invalid")
			err := s.refreshProfileCredentials(ctx, profile)
			if (err == nil) != tc.wantOK {
				t.Fatalf("wantOK=%t err=%v", tc.wantOK, err)
			}
		})
	}
}

func TestDormantProfileCredentialMonitor(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	f := testClaudeConfig(t)
	for name, p := range f.Profiles {
		p.RefreshCommand = []string{"/synthetic/refresh"}
		f.Profiles[name] = p
	}
	cfg := compileFixture(t, f)
	s := testProxy(t, cfg)
	for name, p := range cfg.ClaudeUsage.AccountProfiles {
		expiry := time.Now().Add(time.Hour)
		if name != "first" {
			expiry = time.Now().Add(time.Minute)
			if name == "separate" {
				expiry = time.Time{}
			}
		}
		claudeOAuthCredentials.Store(p.CredentialsService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-dormant", fetchedAt: time.Now(), expiresAt: expiry})
		t.Cleanup(func() { claudeOAuthCredentials.Delete(p.CredentialsService) })
	}
	refreshed := make(chan string, 3)
	s.claudeRefreshRunner = func(ctx context.Context, p claudeUsageProfile) error {
		if ctx.Value(claudeRefreshReasonKey{}) != claudeRefreshScheduled {
			t.Error("dormant monitor used auth-error recovery")
		}
		refreshed <- p.Name
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.monitorProfileCredentials(ctx); close(done) }()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case name := <-refreshed:
			seen[name] = true
		case <-time.After(5 * time.Second):
			t.Fatal("dormant profiles were not refreshed")
		}
	}
	cancel()
	<-done
	if seen["first"] || !seen["second"] || !seen["separate"] {
		t.Fatalf("refreshed=%v", seen)
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.claudeRefreshMu.Lock()
		active := len(s.claudeRefreshInFlight)
		s.claudeRefreshMu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCredentialRefreshCancelledQueue(t *testing.T) {
	f := testClaudeConfig(t)
	p := f.Profiles["first"]
	p.RefreshCommand = []string{"/must-not-execute"}
	f.Profiles["first"] = p
	s := testProxy(t, compileFixture(t, f))
	s.claudeAuthRefreshGate = make(chan struct{}, 1)
	s.claudeAuthRefreshGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.refreshProfileCredentials(ctx, s.cfg.ClaudeUsage.AccountProfiles["first"]); err != context.Canceled {
		t.Fatalf("error=%v", err)
	}
}

func TestScheduledRefreshDoesNotClearRejectedCredential(t *testing.T) {
	s := testProxy(t, compileFixture(t, testClaudeConfig(t)))
	profile := s.cfg.ClaudeUsage.AccountProfiles["first"]
	key := s.cfg.ClaudeUsage.Provider + "@" + profile.Name
	s.markProviderBlocked(key, "auth_quarantined", time.Hour)
	done := make(chan struct{})
	s.claudeRefreshRunner = func(ctx context.Context, _ claudeUsageProfile) error {
		defer close(done)
		if ctx.Value(claudeRefreshReasonKey{}) != claudeRefreshScheduled {
			t.Error("scheduled refresh lost its reason")
		}
		return nil
	}
	s.triggerClaudeOAuthRefreshWithReason(profile, claudeRefreshScheduled)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not run")
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.claudeRefreshMu.Lock()
		active := len(s.claudeRefreshInFlight)
		s.claudeRefreshMu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	if blocked, state := s.providerBlocked(key); !blocked || state.Reason != "auth_quarantined" {
		t.Fatalf("scheduled local check cleared rejection: blocked=%t state=%+v", blocked, state)
	}
}
