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
		exit                      int
		wantOK                    bool
	}{
		{name: "verified existing token", verified: true, wantOK: true},
		{name: "rotated despite helper error", rotate: true, exit: 1, wantOK: true},
		{name: "unverified unchanged token"},
		{name: "expired token with proof", expired: true, verified: true},
		{name: "rotated expired token", rotate: true, expired: true},
		{name: "failed helper with proof", verified: true, exit: 1},
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
			for name, script := range map[string]string{
				"security": fmt.Sprintf("#!/bin/sh\n/bin/cat %q\n", state),
				"refresh":  fmt.Sprintf("#!/bin/sh\nprintf '%%s' '%s' > %q\nprintf '%%s' '%s'\nexit %d\n", after, state, proof, tc.exit),
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
			err := s.refreshProfileCredentials(context.Background(), profile)
			if (err == nil) != tc.wantOK {
				t.Fatalf("wantOK=%t err=%v", tc.wantOK, err)
			}
		})
	}
}

func TestDormantProfileCredentialMonitor(t *testing.T) {
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
	s.claudeRefreshRunner = func(ctx context.Context, p claudeUsageProfile) error { refreshed <- p.Name; return nil }
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
