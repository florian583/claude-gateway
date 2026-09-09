package main

import (
	"context"
	"encoding/json"
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

const testMessageResponse = `{"id":"msg_test","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`

func testProxy(t *testing.T, cfg config) *proxyServer {
	t.Helper()
	s, e := newProxyServer(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		s.metrics.close()
		for _, tr := range s.transports {
			tr.CloseIdleConnections()
		}
	})
	return s
}
func sendTestRequest(s *proxyServer, payload string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(payload)))
	return w
}
func testClaudeConfig(t *testing.T) fileConfig {
	t.Helper()
	f := testConfig("https://api.anthropic.com", "anthropic")
	f.Profiles = map[string]profileDefinition{}
	for _, id := range []string{"first", "second", "separate"} {
		f.Profiles[id] = profileDefinition{Command: []string{"claude"}, ConfigDir: filepath.Join(t.TempDir(), id)}
	}
	f.AccountPools = map[string]accountPoolDefinition{
		"shared":   {Profiles: []string{"first", "second"}, StickySeconds: 90},
		"separate": {Profiles: []string{"separate"}},
	}
	f.Providers["endpoint"] = providerDefinition{Protocol: "anthropic", Variant: "claude-subscription", BaseURL: "https://api.anthropic.com", Billing: "subscription", Auth: authDefinition{Type: "claude-profile-pool", Pool: "shared"}}
	m := f.Models["model"]
	m.Upstream = "claude-test-model"
	f.Models["model"] = m
	return f
}
func seedTestCredentials(t *testing.T, cfg config) {
	t.Helper()
	for name, p := range cfg.ClaudeUsage.AccountProfiles {
		claudeOAuthCredentials.Store(p.CredentialsService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-" + name, fetchedAt: time.Now(), expiresAt: time.Now().Add(time.Hour)})
		t.Cleanup(func() { claudeOAuthCredentials.Delete(p.CredentialsService) })
	}
}

func TestConfiguredNamedAccountPoolSequentialFailover(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	var inFlight, maxInFlight atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		if current > maxInFlight.Load() {
			maxInFlight.Store(current)
		}
		id := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer sk-ant-oat-test-")
		mu.Lock()
		calls = append(calls, id)
		mu.Unlock()
		if id == "first" {
			w.WriteHeader(429)
			io.WriteString(w, `{"error":{"type":"rate_limit_error","message":"quota exceeded"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testMessageResponse)
	}))
	defer upstream.Close()
	cfg := compileFixture(t, testClaudeConfig(t))
	p := cfg.Providers["endpoint"]
	p.BaseURL = upstream.URL
	cfg.Providers["endpoint"] = p
	seedTestCredentials(t, cfg)
	s := testProxy(t, cfg)
	out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	mu.Lock()
	got := strings.Join(calls, ",")
	mu.Unlock()
	if out.Code != 200 || got != "first,second" || maxInFlight.Load() != 1 {
		t.Fatalf("status=%d calls=%s concurrency=%d body=%s", out.Code, got, maxInFlight.Load(), out.Body.String())
	}
	if len(s.claudeUse.snapshots) != 0 {
		t.Fatal("passive inference fetched quota")
	}
	for _, lease := range s.claudeAccounts {
		if time.Until(lease.Until) > 91*time.Second {
			t.Fatal("per-pool stickiness ignored")
		}
	}
}

func TestConfiguredDistinctPoolsSameUpstreamAndQuotaLimits(t *testing.T) {
	f := testClaudeConfig(t)
	m := f.Models["model"]
	m.AccountPool = "separate"
	f.Models["isolated"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "isolated"}}}
	p := f.Profiles["second"]
	p.SevenDayThresholdPct = 95
	f.Profiles["second"] = p
	s := testProxy(t, compileFixture(t, f))
	candidates := s.collectCandidates(s.cfg.Models["test"])
	if len(candidates) != 2 {
		t.Fatal("distinct pools deduplicated")
	}
	attempts := s.expandConfiguredClaudeAttempts(httptest.NewRequest("POST", "/v1/messages", nil), candidates)
	var ids []string
	for _, a := range attempts {
		ids = append(ids, a.profile.Name)
	}
	if strings.Join(ids, ",") != "first,second,separate" {
		t.Fatalf("wrong pool membership: %v", ids)
	}
	snapshot := claudeUsageSnapshot{Profile: "second", Source: "oauth-api"}
	snapshot.FiveHour.Utilization = 99
	snapshot.SevenDay.Utilization = 94
	if !s.snapshotAllowedForCandidate(snapshot, candidates[0]) {
		t.Fatal("5h allowance stopped before 100")
	}
	snapshot.SevenDay.Utilization = 95
	if s.snapshotAllowedForCandidate(snapshot, candidates[0]) {
		t.Fatal("weekly personal reserve ignored")
	}
	snapshot.Source = "unknown"
	if !s.snapshotAllowedForCandidate(snapshot, candidates[0]) {
		t.Fatal("unknown quota blocked inference")
	}
	for _, profile := range s.cfg.ClaudeUsage.AccountProfiles {
		if _, e := s.claudeUsageStatusSnapshot(profile); e == nil {
			t.Fatal("passive mode should return unknown without Keychain")
		}
	}
	data, _ := json.Marshal(s.claudeUsageStatus())
	if strings.Contains(string(data), "waiting for background") || strings.Contains(string(data), `"usageKnown":true`) {
		t.Fatal("passive status fabricated quota")
	}
}

func TestConfiguredPaidFallbackRequiresEveryLegEvidence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		statuses     []int
		expectedPaid int
	}{
		{"quota", []int{429}, 1}, {"authentication", []int{401}, 0}, {"one-outage", []int{503}, 0},
		{"quota-then-auth", []int{429, 401}, 0}, {"quota-then-outage", []int{429, 503}, 0}, {"all-quota", []int{429, 402}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var paid atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/paid":
					paid.Add(1)
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, testMessageResponse)
				default:
					i := 0
					if r.URL.Path == "/second" {
						i = 1
					}
					status := tc.statuses[i]
					w.WriteHeader(status)
					io.WriteString(w, `{"error":{"message":"request rejected"}}`)
				}
			}))
			defer upstream.Close()
			f := testConfig("https://example.invalid", "anthropic")
			p := f.Providers["endpoint"]
			p.Billing = "subscription"
			p.Auth = authDefinition{Type: "env", Name: "PORTABLE_TEST_KEY"}
			p.MessagesPath = "/first"
			f.Providers["endpoint"] = p
			stages := []chainStep{{Model: "model"}}
			if len(tc.statuses) > 1 {
				p.MessagesPath = "/second"
				f.Providers["other"] = p
				m := f.Models["model"]
				m.Provider = "other"
				f.Models["other"] = m
				stages = append(stages, chainStep{Model: "other"})
			}
			p.Billing = "metered"
			p.MessagesPath = "/paid"
			f.Providers["renamed-gateway"] = p
			m := f.Models["model"]
			m.Provider = "renamed-gateway"
			f.Models["paid"] = m
			stages = append(stages, chainStep{Model: "paid"})
			f.Chains["main"] = chainDefinition{Steps: stages, AllowPaidFallback: true, PaidFallbackOn: []string{"quota-exhausted", "confirmed-provider-outage"}}
			t.Setenv("PORTABLE_TEST_KEY", "test-only-token")
			cfg := compileFixture(t, f)
			for id, p := range cfg.Providers {
				p.BaseURL = upstream.URL
				cfg.Providers[id] = p
			}
			s := testProxy(t, cfg)
			sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
			if int(paid.Load()) != tc.expectedPaid {
				t.Fatalf("paid calls=%d want=%d", paid.Load(), tc.expectedPaid)
			}
		})
	}
}

func TestConfiguredPaidPolicyStateAndOutageProof(t *testing.T) {
	m := modelConfig{AllowPaidFallback: true, PaidFallbackOn: []string{"quota-exhausted", "confirmed-provider-outage"}}
	if (paidFallbackEvidence{}).allowed(m) || (paidFallbackEvidence{"quota": "quota-exhausted", "wifi": ""}).allowed(m) {
		t.Fatal("unknown triggered paid fallback")
	}
	s := &proxyServer{}
	if s.updateOutageEvidence("route", "one", 503) || s.updateOutageEvidence("route", "one", 503) || s.updateOutageEvidence("route", "two", 503) {
		t.Fatal("outage needs three separate requests")
	}
	if !s.updateOutageEvidence("route", "three", 503) {
		t.Fatal("confirmed outage unavailable")
	}
	if s.updateOutageEvidence("route", "four", 200) || s.updateOutageEvidence("route", "", 0) {
		t.Fatal("success did not clear outage proof")
	}
}

func TestConfiguredRejectInvalidRequestsAndRedirects(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, testMessageResponse) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	s := testProxy(t, compileFixture(t, testConfig(redirect.URL, "anthropic")))
	for _, b := range []string{"", `{`, `null`, `{}`, `{"model":22}`, `{"model":"missing"}`} {
		if out := sendTestRequest(s, b); out.Code != 400 {
			t.Fatalf("invalid request status=%d", out.Code)
		}
	}
	sendTestRequest(s, `{"model":"test","messages":[]}`)
	if calls.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestConfiguredCatalogNeverExposesOverridesOrAuth(t *testing.T) {
	f := testConfig("http://127.0.0.1:9999", "anthropic")
	m := f.Models["model"]
	m.RequestOverrides = map[string]any{"metadata": map[string]any{"private": "canary-private"}}
	f.Models["model"] = m
	s := testProxy(t, compileFixture(t, f))
	out := httptest.NewRecorder()
	s.catalog(out, httptest.NewRequest("GET", "/catalog", nil))
	if strings.Contains(out.Body.String(), "canary") || strings.Contains(out.Body.String(), "requestOverrides") {
		t.Fatal("catalog leaked arbitrary config options")
	}
}

func TestConfiguredInitDoesNotOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("CLAUDE_PROXY_CONFIG", path)
	if e := runCLI(context.Background(), []string{"init"}, nil, io.Discard, io.Discard); e != nil {
		t.Fatal(e)
	}
	first, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := decodeFileConfig(path, first); e != nil {
		t.Fatal(e)
	}
	if e := runCLI(context.Background(), []string{"init"}, nil, io.Discard, io.Discard); e == nil {
		t.Fatal("init overwrote existing config")
	}
	second, _ := os.ReadFile(path)
	if string(first) != string(second) {
		t.Fatal("existing config changed")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("config permissions not private")
	}
}

func TestConfiguredOllamaAPIMonitorOptIn(t *testing.T) {
	calls := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"limits":{"weekly":{"usage":0.25},"session":{"usage":0.10}}}`)
	}))
	defer upstream.Close()
	f := testConfig(upstream.URL, "anthropic")
	p := f.Providers["endpoint"]
	p.Variant = "ollama-cloud"
	p.Usage = usageDefinition{Mode: "api", PollIntervalSeconds: 30}
	f.Providers["endpoint"] = p
	s := testProxy(t, compileFixture(t, f))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.monitorOllamaUsage(ctx); close(done) }()
	select {
	case path := <-calls:
		if path != "/api/usage" {
			t.Fatal(path)
		}
	case <-time.After(time.Second):
		t.Fatal("opted-in API reader never started")
	}
	cancel()
	<-done
}

func TestConfiguredExampleConfigurations(t *testing.T) {
	paths, e := filepath.Glob("examples/*.json")
	if e != nil || len(paths) != 3 {
		t.Fatalf("examples unavailable: %v", e)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			b, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			if _, e := decodeFileConfig(path, b); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestConfiguredBoundedHelperAndMissingCredentials(t *testing.T) {
	var b limitedBuffer
	b.limit = 4
	if _, e := io.Copy(&b, strings.NewReader("too much output")); e == nil || len(b.Bytes()) > 4 {
		t.Fatal("helper output bound bypassed")
	}
	t.Setenv("PORTABLE_EMPTY_KEY", "")
	p := providerConfig{Variant: "generic", AuthTokenEnv: "PORTABLE_EMPTY_KEY"}
	token, configured, e := providerAuthToken("arbitrary", p)
	if e != nil || !configured || token != "" {
		t.Fatal("missing configured credential treated as anonymous")
	}
}

func TestConfiguredPaidFallbackAfterProactiveQuotaBlock(t *testing.T) {
	var calls atomic.Int32
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testMessageResponse)
	}))
	defer paid.Close()
	f := testConfig("http://127.0.0.1:1", "anthropic")
	p := providerDefinition{Protocol: "anthropic", BaseURL: "https://paid.example", Billing: "metered", Auth: authDefinition{Type: "env", Name: "PORTABLE_PAID_KEY"}}
	f.Providers["paid"] = p
	m := f.Models["model"]
	m.Provider = "paid"
	f.Models["paid"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "paid"}}, AllowPaidFallback: true}
	t.Setenv("PORTABLE_PAID_KEY", "test-token")
	cfg := compileFixture(t, f)
	q := cfg.Providers["paid"]
	q.BaseURL = paid.URL
	cfg.Providers["paid"] = q
	s := testProxy(t, cfg)
	s.markProviderBlocked("endpoint", "ollama_usage_weekly_exhausted", time.Minute)
	out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	if out.Code != 200 || calls.Load() != 1 {
		t.Fatalf("proactive quota did not reach opted-in paid fallback: %d", out.Code)
	}
}

func TestConfiguredModelOptionsDoNotLeakBetweenAttempts(t *testing.T) {
	f := testConfig("http://127.0.0.1:9999", "openai-chat-completions")
	p := f.Providers["endpoint"]
	p.RequestOverrides = map[string]any{"temperature": 0.2}
	f.Providers["endpoint"] = p
	m := f.Models["model"]
	m.RequestOverrides = map[string]any{"temperature": 0.7, "reasoning_effort": "high"}
	f.Models["model"] = m
	s := testProxy(t, compileFixture(t, f))
	body := []byte(`{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	var payload map[string]any
	json.Unmarshal(body, &payload)
	got, e := s.buildAttemptBody(body, payload, s.cfg.Models["test"], "endpoint", "/v1/messages")
	if e != nil {
		t.Fatal(e)
	}
	var adapted map[string]any
	json.Unmarshal(got, &adapted)
	if adapted["temperature"] != 0.7 || adapted["reasoning_effort"] != "high" || payload["temperature"] != nil || payload["model"] != "test" {
		t.Fatal("options ignored or leaked into original request")
	}
}

func TestConfiguredBrowserCacheRetainsBoundedEvidenceOnLoss(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	f := testConfig("http://127.0.0.1:9999", "anthropic")
	p := f.Providers["endpoint"]
	p.Usage = usageDefinition{Mode: "browser", SnapshotPath: path}
	f.Providers["endpoint"] = p
	s := testProxy(t, compileFixture(t, f))
	s.clockNow = func() time.Time { return now }
	used := 1.0
	raw, _ := json.Marshal(browserUsageSnapshot{Version: 1, Providers: map[string]browserUsageProvider{"endpoint": {Status: "ok", FetchedAt: now, Utilization: &used, Windows: map[string]browserUsageWindow{"weekly": {PercentUsed: 100, Reset: "in 1h"}}}}})
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if exhausted, _ := s.browserUsageExhausted("endpoint"); !exhausted {
		t.Fatal("quota not recorded")
	}
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	now = now.Add(6 * time.Second)
	if exhausted, _ := s.browserUsageExhausted("endpoint"); !exhausted {
		t.Fatal("missing collector erased confirmed quota")
	}
	if _, known := s.providerUsageUtilization("endpoint"); known {
		t.Fatal("lost collector presented stale usage as fresh")
	}
	now = now.Add(2 * time.Hour)
	if exhausted, _ := s.browserUsageExhausted("endpoint"); exhausted {
		t.Fatal("quota remained blocked after reset")
	}
}

func TestConfiguredDefaultProfileDoesNotBecomeCustomDirectory(t *testing.T) {
	f := testClaudeConfig(t)
	f.Profiles["first"] = profileDefinition{Command: []string{"claude"}}
	cfg := compileFixture(t, f)
	p := cfg.Definition.Profiles["first"]
	if !p.DefaultDirectory || p.CredentialsService != "Claude Code-credentials" {
		t.Fatal("default profile identity changed")
	}
	for _, e := range profileEnvironment(p) {
		if strings.HasPrefix(e, "CLAUDE_CONFIG_DIR=") {
			t.Fatal("default profile unexpectedly forced to custom directory")
		}
	}
	home, e := os.UserHomeDir()
	if e != nil {
		t.Fatal(e)
	}
	if cfg.ClaudeUsage.AccountProfiles["first"].CachePath != filepath.Join(home, ".claude.json") {
		t.Fatal("default account metadata path incorrect")
	}
}

func TestConfiguredQuotaWindowResetAllowsReevaluation(t *testing.T) {
	now := time.Now()
	s := testProxy(t, compileFixture(t, testClaudeConfig(t)))
	s.clockNow = func() time.Time { return now }
	snap := claudeUsageSnapshot{Profile: "first", Source: "oauth-api", FetchedAt: now.Add(-time.Hour), FiveHour: claudeUsageWindow{Utilization: 100, ResetsAt: now.Add(-time.Minute)}}
	if !s.snapshotAllowedForCandidate(snap, s.cfg.Models["test"]) {
		t.Fatal("expired quota window still blocks routing")
	}
	snap.FiveHour.ResetsAt = now.Add(time.Minute)
	if s.snapshotAllowedForCandidate(snap, s.cfg.Models["test"]) {
		t.Fatal("active quota window ignored")
	}
}

func TestConfiguredRefreshRequiresRotationOrVerifiedRecovery(t *testing.T) {
	for _, tc := range []struct{ rotate, verified bool }{{false, false}, {true, false}, {false, true}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			rotate := tc.rotate
			dir := t.TempDir()
			state := filepath.Join(dir, "credential.json")
			before := `{"claudeAiOauth":{"accessToken":"sk-ant-oat-test-fixture-before","expiresAt":4102444800000}}`
			after := before
			if rotate {
				after = `{"claudeAiOauth":{"accessToken":"sk-ant-oat-test-fixture-after","expiresAt":4102444800000}}`
			}
			if e := os.WriteFile(state, []byte(before), 0600); e != nil {
				t.Fatal(e)
			}
			security := fmt.Sprintf("#!/bin/sh\n/bin/cat %q\n", state)
			helper := fmt.Sprintf("#!/bin/sh\nprintf '%%s' '%s' > %q\n", after, state)
			if tc.verified {
				helper += "printf '%s' '{\"version\":1,\"authVerified\":true}'\n"
			}
			for name, script := range map[string]string{"security": security, "refresh": helper} {
				if e := os.WriteFile(filepath.Join(dir, name), []byte(script), 0700); e != nil {
					t.Fatal(e)
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
			e := s.refreshProfileCredentials(context.Background(), profile)
			if (e == nil) != (rotate || tc.verified) {
				t.Fatalf("rotation=%t error=%v", rotate, e)
			}
		})
	}
}
