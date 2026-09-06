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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testHTTPDoer func(*http.Request) (*http.Response, error)

func (do testHTTPDoer) Do(request *http.Request) (*http.Response, error) {
	return do(request)
}

func testClaudeProfile(t *testing.T, name, service, account string) claudeUsageProfile {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".json")
	body := fmt.Sprintf(`{"oauthAccount":{"accountUuid":%q,"organizationUuid":%q}}`, account, "org-"+account)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return claudeUsageProfile{Name: name, CachePath: path, CredentialsService: service}
}

func TestAPIHelloProbeIsHandledLocally(t *testing.T) {
	server := &proxyServer{providerStates: map[string]providerRuntimeState{}}
	request := httptest.NewRequest(http.MethodHead, "/api/hello", nil)
	response := httptest.NewRecorder()

	server.handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if len(server.providerStates) != 0 {
		t.Fatalf("provider states = %#v, want no probe side effects", server.providerStates)
	}
}

func TestOllamaUsageRouteReservesProviderForDeepSeekAtWeeklyThreshold(t *testing.T) {
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/usage" {
			t.Fatalf("path = %q, want /api/usage", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"limits":{"session":{"usage":0.1},"weekly":{"usage":0.85}}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"ollama": {BaseURL: usage.URL}},
		Models:    map[string]modelConfig{"deepseek": {Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"}},
		OllamaUsage: ollamaUsageConfig{
			Provider:           "ollama",
			WeeklyThresholdPct: 85,
			CacheTTLSeconds:    300,
			EligibleUpstreams:  []string{"glm-5.3-flash:cloud", "deepseek-v4-flash:cloud"},
			ReserveUpstreams:   []string{"deepseek-v4-flash:cloud"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	routed := server.applyOllamaUsageRoute(context.Background(), modelConfig{Requested: "glm[1m]", Provider: "ollama", Upstream: "glm-5.3-flash:cloud"})
	if routed.Upstream != "glm-5.3-flash:cloud" {
		t.Fatalf("upstream = %q, want original route", routed.Upstream)
	}
	if routed.Requested != "glm[1m]" {
		t.Fatalf("requested = %q, want original request", routed.Requested)
	}
	if blocked, state := server.providerBlocked("ollama"); blocked {
		t.Fatalf("provider state = %#v, want provider available for DeepSeek", state)
	}
	if reserved, reason := server.ollamaCandidateReservedOut(routed); !reserved || reason != "weekly" {
		t.Fatalf("GLM reserve state = %v %q, want weekly restriction", reserved, reason)
	}
	if reserved, reason := server.ollamaCandidateReservedOut(modelConfig{Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"}); reserved {
		t.Fatalf("DeepSeek reserve state = %v %q, want available", reserved, reason)
	}
}

func TestOllamaUsageRouteKeepsGLMBelowThreshold(t *testing.T) {
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"limits":{"weekly":{"usage":0.849}}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"ollama": {BaseURL: usage.URL}},
		Models:    map[string]modelConfig{"deepseek": {Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"}},
		OllamaUsage: ollamaUsageConfig{
			Provider:           "ollama",
			WeeklyThresholdPct: 85,
			CacheTTLSeconds:    300,
			EligibleUpstreams:  []string{"glm-5.3-flash:cloud", "deepseek-v4-flash:cloud"},
			ReserveUpstreams:   []string{"deepseek-v4-flash:cloud"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	original := modelConfig{Requested: "glm[1m]", Provider: "ollama", Upstream: "glm-5.3-flash:cloud"}
	routed := server.applyOllamaUsageRoute(context.Background(), original)
	if routed.Upstream != original.Upstream {
		t.Fatalf("upstream = %q, want %q", routed.Upstream, original.Upstream)
	}
	if blocked, _ := server.providerBlocked("ollama"); blocked {
		t.Fatal("ollama blocked below threshold")
	}
	if reserved, _ := server.ollamaCandidateReservedOut(original); reserved {
		t.Fatal("GLM reserved out below threshold")
	}
}

func TestOllamaUsageRouteReservesProviderForDeepSeekAtSessionThreshold(t *testing.T) {
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"limits":{"session":{"usage":0.9},"weekly":{"usage":0.2}}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"ollama": {BaseURL: usage.URL}},
		OllamaUsage: ollamaUsageConfig{
			Provider:            "ollama",
			SessionThresholdPct: 85,
			CacheTTLSeconds:     300,
			EligibleUpstreams:   []string{"glm-5.3-flash:cloud", "deepseek-v4-flash:cloud"},
			ReserveUpstreams:    []string{"deepseek-v4-flash:cloud"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	glm := modelConfig{Requested: "glm[1m]", Provider: "ollama", Upstream: "glm-5.3-flash:cloud"}
	server.applyOllamaUsageRoute(context.Background(), glm)
	if blocked, state := server.providerBlocked("ollama"); blocked {
		t.Fatalf("provider state = %#v, want provider available for DeepSeek", state)
	}
	if reserved, reason := server.ollamaCandidateReservedOut(glm); !reserved || reason != "session" {
		t.Fatalf("GLM reserve state = %v %q, want session restriction", reserved, reason)
	}
	if reserved, reason := server.ollamaCandidateReservedOut(modelConfig{Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"}); reserved {
		t.Fatalf("DeepSeek reserve state = %v %q, want available", reserved, reason)
	}
}

func TestOllamaUsageRouteBlocksWholeProviderAtExhaustion(t *testing.T) {
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"limits":{"session":{"usage":1},"weekly":{"usage":0.9}}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"ollama": {BaseURL: usage.URL}},
		OllamaUsage: ollamaUsageConfig{
			Provider: "ollama", SessionThresholdPct: 85, CacheTTLSeconds: 300,
			EligibleUpstreams: []string{"glm-5.3-flash:cloud", "deepseek-v4-flash:cloud"},
			ReserveUpstreams:  []string{"deepseek-v4-flash:cloud"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	server.applyOllamaUsageRoute(context.Background(), modelConfig{Provider: "ollama", Upstream: "glm-5.3-flash:cloud"})
	blocked, state := server.providerBlocked("ollama")
	if !blocked || state.Reason != "ollama_usage_session_exhausted" {
		t.Fatalf("provider state = %#v, want session exhaustion block", state)
	}
}

func TestClineUsageFeedsHeadroomAndBlocksOnlyAtExhaustion(t *testing.T) {
	usagePct := 75.0
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/users/me/plan/usage-limits" {
			t.Fatalf("path = %q, want Cline usage limits endpoint", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"limits":[{"type":"five_hour","percentUsed":` + strconv.FormatFloat(usagePct, 'f', -1, 64) + `,"resetsAt":"2026-08-25T10:01:15Z"},{"type":"weekly","percentUsed":32,"resetsAt":"2026-08-26T15:13:23Z"}]}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers:  map[string]providerConfig{"clinepass": {BaseURL: usage.URL + "/api"}},
		ClineUsage: clineUsageConfig{Provider: "clinepass", CacheTTLSeconds: 300, BlockThresholdPct: 100},
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := server.fetchClineUsageSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server.clineUse = clineUsageCache{hasValue: true, snapshot: snapshot}
	server.updateClineUsageState(snapshot)
	if got, known := server.providerUsageUtilization("clinepass"); !known || got != 0.75 {
		t.Fatalf("utilization = %.2f known=%v, want 0.75 true", got, known)
	}
	if blocked, state := server.providerBlocked("clinepass"); blocked {
		t.Fatalf("provider state = %#v, want available below 100%%", state)
	}

	usagePct = 100
	snapshot, err = server.fetchClineUsageSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server.updateClineUsageState(snapshot)
	blocked, state := server.providerBlocked("clinepass")
	if !blocked || state.Reason != "cline_usage_five_hour_exhausted" {
		t.Fatalf("provider state = %#v, want five-hour exhaustion block", state)
	}
}

func TestBrowserUsageFeedsHeadroomAndRetainsKnownExhaustion(t *testing.T) {
	now := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
	tempDir := t.TempDir()
	usagePath := filepath.Join(tempDir, "provider-usage-browser.json")
	body := fmt.Sprintf(`{
		"version":1,
		"updatedAt":%q,
		"providers":{
			"opencode-go":{"source":"ego-browser","status":"ok","fetchedAt":%q,"utilization":1,"windows":{"weekly":{"percentUsed":100}}},
			"commandcode":{"source":"ego-browser","status":"ok","fetchedAt":%q,"utilization":0.73,"windows":{"weekly":{"percentUsed":73}}}
		}
	}`, now.Format(time.RFC3339), now.Format(time.RFC3339), now.Format(time.RFC3339))
	if err := os.WriteFile(usagePath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"opencode-go":           {BaseURL: "https://opencode.example"},
			"opencode-go-anthropic": {BaseURL: "https://opencode.example"},
			"commandcode":           {BaseURL: "https://command.example"},
		},
		Metrics: metricsConfig{Path: filepath.Join(tempDir, "proxy-metrics.jsonl")},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.browserUsagePath = usagePath
	server.clockNow = func() time.Time { return now }

	for _, provider := range []string{"opencode-go", "opencode-go-anthropic"} {
		if got, known := server.providerUsageUtilization(provider); !known || got != 1 {
			t.Fatalf("%s utilization = %.2f known=%v, want 1 true", provider, got, known)
		}
		if exhausted, reason := server.browserUsageExhausted(provider); !exhausted || reason != "browser_usage_limit_exhausted" {
			t.Fatalf("%s exhausted = %v %q, want true browser limit", provider, exhausted, reason)
		}
	}
	if got, known := server.providerUsageUtilization("commandcode"); !known || got != 0.73 {
		t.Fatalf("commandcode utilization = %.2f known=%v, want 0.73 true", got, known)
	}

	server.clockNow = func() time.Time { return now.Add(browserUsageFreshDuration + time.Second) }
	if got, known := server.providerUsageUtilization("opencode-go"); known || got != 0 {
		t.Fatalf("stale utilization = %.2f known=%v, want 0 false", got, known)
	}
	if exhausted, reason := server.browserUsageExhausted("opencode-go"); !exhausted || reason != "browser_usage_limit_exhausted_stale" {
		t.Fatalf("stale exhausted usage = %t %q, want retained block", exhausted, reason)
	}
	server.clockNow = func() time.Time { return now.Add(25 * time.Hour) }
	if exhausted, _ := server.browserUsageExhausted("opencode-go"); exhausted {
		t.Fatal("unparseable exhausted usage remained blocked past conservative hold")
	}
}

func TestParseBrowserResetDuration(t *testing.T) {
	for label, want := range map[string]time.Duration{
		"Limit reached · resets in 2d 22h": 70 * time.Hour,
		"Resets in 5 days 3 hours":         123 * time.Hour,
		"Resets in 3 hours 27 minutes":     3*time.Hour + 27*time.Minute,
	} {
		got, ok := parseBrowserResetDuration(label)
		if !ok || got != want {
			t.Fatalf("parse %q = %s ok=%t, want %s", label, got, ok, want)
		}
	}
}

func TestClaudeUsageGateSeparatesOAuthAccountsAndCaches(t *testing.T) {
	usageCalls := map[string]int{}
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/oauth/usage" {
			t.Fatalf("path = %q, want /api/oauth/usage", r.URL.Path)
		}
		auth := r.Header.Get("Authorization")
		usageCalls[auth]++
		if r.Header.Get("anthropic-beta") != "oauth-2025-04-20" {
			t.Fatalf("anthropic-beta = %q", r.Header.Get("anthropic-beta"))
		}
		weekly := 20
		if auth == "Bearer sk-ant-oat-test-teamB-token" {
			weekly = 61
		}
		_, _ = w.Write([]byte(`{"five_hour":{"utilization":9,"resets_at":"2026-08-26T12:49:59Z"},"seven_day":{"utilization":` + strconv.Itoa(weekly) + `,"resets_at":"2026-08-28T09:59:59Z"}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5"},
			FiveHourThresholdPct: 60, SevenDayThresholdPct: 60, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := modelConfig{Provider: "anthropic", Upstream: "claude-sonnet-5"}

	accountA := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	accountA.Header.Set("Authorization", "Bearer sk-ant-oat-test-accountA-token")
	if allowed, reason := server.claudeSubscriptionAllowed(context.Background(), accountA, candidate); !allowed {
		t.Fatalf("AccountA account rejected: %q", reason)
	}
	if allowed, reason := server.claudeSubscriptionAllowed(context.Background(), accountA, candidate); !allowed {
		t.Fatalf("cached AccountA account rejected: %q", reason)
	}
	if usageCalls["Bearer sk-ant-oat-test-accountA-token"] != 1 {
		t.Fatalf("AccountA usage calls = %d, want 1 cached call", usageCalls["Bearer sk-ant-oat-test-accountA-token"])
	}

	teamB := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48107/v1/messages", nil)
	teamB.Header.Set("x-api-key", "sk-ant-oat-test-teamB-token")
	if allowed, reason := server.claudeSubscriptionAllowed(context.Background(), teamB, candidate); allowed || reason != "seven_day_reserve" {
		t.Fatalf("second account gate = %v %q, want weekly reserve", allowed, reason)
	}
	if usageCalls["Bearer sk-ant-oat-test-teamB-token"] != 1 {
		t.Fatalf("second account usage calls = %d, want separate OAuth query", usageCalls["Bearer sk-ant-oat-test-teamB-token"])
	}
}

func TestClaudeAccountAutoSelectionBalancesUnifiedPoolAndSticks(t *testing.T) {
	const (
		accountAService = "test-teamA-account-auto"
		fallbackService = "test-backup-account-auto"
	)
	claudeOAuthCredentials.Store(accountAService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-accountA-auto-token", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-backup-auto-token", fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(accountAService)
		claudeOAuthCredentials.Delete(fallbackService)
	})

	usageByToken := map[string]float64{
		"Bearer sk-ant-oat-test-accountA-auto-token": 54,
		"Bearer sk-ant-oat-test-backup-auto-token":   6,
	}
	var usageMu sync.Mutex
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		usageMu.Lock()
		fiveHour := usageByToken[r.Header.Get("Authorization")]
		usageMu.Unlock()
		_, _ = fmt.Fprintf(w, `{"five_hour":{"utilization":%.1f},"seven_day":{"utilization":20}}`, fiveHour)
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true, AccountStickySeconds: 300,
			EligibleUpstreams: []string{"claude-fable-5"}, FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			AccountProfiles: map[string]claudeUsageProfile{
				"teamA":  testClaudeProfile(t, "teamA", accountAService, "accountA-auto"),
				"backup": testClaudeProfile(t, "backup", fallbackService, "backup-auto"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	profile, snapshot, err := server.autoSelectClaudeProfile(context.Background(), request, "conversation-a")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "backup" || snapshot.FiveHour.Utilization != 6 {
		t.Fatalf("selection = %q %.1f%%, want least-used unified account at 6%%", profile.Name, snapshot.FiveHour.Utilization)
	}

	payload := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	bound, release := server.bindClaudeAccount(context.Background(), request, modelConfig{Provider: "anthropic", Upstream: "claude-fable-5"}, payload, true)
	defer release()
	token, err := server.claudeOAuthBearer(context.Background(), bound)
	if err != nil {
		t.Fatal(err)
	}
	if token != "Bearer sk-ant-oat-test-backup-auto-token" {
		t.Fatalf("selected OAuth token = %q, want least-used account token", token)
	}

	usageMu.Lock()
	usageByToken["Bearer sk-ant-oat-test-accountA-auto-token"] = 1
	usageByToken["Bearer sk-ant-oat-test-backup-auto-token"] = 20
	usageMu.Unlock()
	server.claudeUse.mu.Lock()
	server.claudeUse.snapshots = map[string]claudeUsageSnapshot{}
	server.claudeUse.mu.Unlock()

	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "conversation-a")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "backup" {
		t.Fatalf("sticky selection = %q, want backup", profile.Name)
	}
	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "conversation-b")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "teamA" {
		t.Fatalf("new conversation selection = %q, want listener primary", profile.Name)
	}
}

func TestClaudeAccountStickinessBreaksForMaterialHeadroom(t *testing.T) {
	sticky := claudeUsageProfile{Name: "sticky", CredentialsService: "sticky-service"}
	idle := claudeUsageProfile{Name: "idle", CredentialsService: "idle-service"}
	server := &proxyServer{
		cfg:                   config{ClaudeUsage: claudeUsageConfig{AccountStickySeconds: 300}},
		claudeAccounts:        map[string]claudeAccountSticky{"conversation": {Profile: sticky, Until: time.Now().Add(time.Minute)}},
		claudeAccountInFlight: map[string]int{},
	}
	chosen, _, _ := server.chooseClaudeAccount([]availableClaudeAccount{
		{profile: sticky, snapshot: claudeUsageSnapshot{FiveHour: claudeUsageWindow{Utilization: 72}, SevenDay: claudeUsageWindow{Utilization: 40}}},
		{profile: idle, snapshot: claudeUsageSnapshot{FiveHour: claudeUsageWindow{Utilization: 10}, SevenDay: claudeUsageWindow{Utilization: 20}}},
	}, "conversation", sticky, false)
	if chosen.profile.Name != "idle" {
		t.Fatalf("chosen profile = %q, want materially healthier account", chosen.profile.Name)
	}
	if lease := server.claudeAccounts["conversation"]; lease.Profile.Name != "idle" {
		t.Fatalf("lease = %#v, want switched account", lease)
	}
}

func TestClaudeAccountReservationsSpreadParallelRequestsAndPreserveStickyLease(t *testing.T) {
	const (
		primaryService = "test-claude-primary-account-reservation"
		secondService  = "test-claude-second-account-reservation"
		thirdService   = "test-claude-third-account-reservation"
	)
	claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-primary-reservation", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(secondService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-second-reservation", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(thirdService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-third-reservation", fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(primaryService)
		claudeOAuthCredentials.Delete(secondService)
		claudeOAuthCredentials.Delete(thirdService)
	})

	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		utilization := 5
		if r.Header.Get("Authorization") == "Bearer sk-ant-oat-test-second-reservation" {
			utilization = 20
		} else if r.Header.Get("Authorization") == "Bearer sk-ant-oat-test-third-reservation" {
			utilization = 30
		}
		_, _ = fmt.Fprintf(w, `{"five_hour":{"utilization":%d},"seven_day":{"utilization":20}}`, utilization)
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true, AccountStickySeconds: 300,
			EligibleUpstreams:    []string{"claude-fable-5"},
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			AccountPool: []string{"primary", "backup", "third"},
			AccountProfiles: map[string]claudeUsageProfile{
				"primary": testClaudeProfile(t, "primary", primaryService, "primary-reservation"),
				"backup":  testClaudeProfile(t, "backup", secondService, "second-reservation"),
				"third":   testClaudeProfile(t, "third", thirdService, "third-reservation"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	selected := modelConfig{Provider: "anthropic", Upstream: "claude-fable-5", Requested: "claude-fable-5"}
	payload := map[string]any{"model": "claude-fable-5", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	stickyKey := routingStickyKey(selected, payload)

	first, releaseFirst := server.bindClaudeAccount(context.Background(), request, selected, payload, true)
	firstProfile, ok := selectedClaudeProfile(first)
	if !ok || firstProfile.Name != "primary" {
		t.Fatalf("first profile = %#v ok=%v, want primary", firstProfile, ok)
	}
	second, releaseSecond := server.bindClaudeAccount(context.Background(), request, selected, payload, true)
	secondProfile, ok := selectedClaudeProfile(second)
	if !ok || secondProfile.Name != "backup" {
		t.Fatalf("parallel profile = %#v ok=%v, want idle backup", secondProfile, ok)
	}
	if lease := server.claudeAccounts[stickyKey]; lease.Profile.Name != "primary" {
		t.Fatalf("parallel spill changed sticky lease to %#v, want primary", lease)
	}
	attempts := server.expandClaudeAccountAttempts(first, []modelConfig{selected}, selected)
	if len(attempts) != 3 || attempts[0].profile.Name != "primary" || attempts[1].profile.Name != "third" || attempts[2].profile.Name != "backup" {
		t.Fatalf("replacement order = %#v, want selected primary then idle third then busy backup", attempts)
	}

	releaseSecond()
	releaseFirst()
	third, releaseThird := server.bindClaudeAccount(context.Background(), request, selected, payload, true)
	defer releaseThird()
	thirdProfile, ok := selectedClaudeProfile(third)
	if !ok || thirdProfile.Name != "primary" {
		t.Fatalf("post-release profile = %#v ok=%v, want sticky primary", thirdProfile, ok)
	}
}

func TestClaudeAccountAutoSelectionSkipsBlockedProfile(t *testing.T) {
	const (
		accountAService = "test-teamA-account-blocked"
		fallbackService = "test-backup-account-blocked"
	)
	claudeOAuthCredentials.Store(accountAService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-accountA-blocked-token", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-backup-blocked-token", fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(accountAService)
		claudeOAuthCredentials.Delete(fallbackService)
	})

	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fiveHour := 20
		if r.Header.Get("Authorization") == "Bearer sk-ant-oat-test-backup-blocked-token" {
			fiveHour = 5
		}
		_, _ = fmt.Fprintf(w, `{"five_hour":{"utilization":%d},"seven_day":{"utilization":20}}`, fiveHour)
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true, AccountStickySeconds: 300,
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			AccountProfiles: map[string]claudeUsageProfile{
				"teamA":  testClaudeProfile(t, "teamA", accountAService, "accountA-blocked"),
				"backup": testClaudeProfile(t, "backup", fallbackService, "backup-blocked"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.markProviderBlocked("anthropic@teamA", "test", time.Minute)

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	profile, _, err := server.autoSelectClaudeProfile(context.Background(), request, "conversation")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "backup" {
		t.Fatalf("selection = %q, want paired backup fallback", profile.Name)
	}
}

func TestClaudeAccountAutoSelectionSkipsModelBlockedProfile(t *testing.T) {
	const (
		primaryService  = "test-claude-fable-model-block-primary"
		fallbackService = "test-claude-fable-model-block-fallback"
	)
	claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-fable-model-block-primary", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-fable-model-block-fallback", fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(primaryService)
		claudeOAuthCredentials.Delete(fallbackService)
	})

	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true, AccountStickySeconds: 300,
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			AccountPool: []string{"personal", "backup"},
			AccountProfiles: map[string]claudeUsageProfile{
				"personal": testClaudeProfile(t, "personal", primaryService, "fable-model-block-primary"),
				"backup":   testClaudeProfile(t, "backup", fallbackService, "fable-model-block-fallback"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	selected := modelConfig{Provider: "anthropic", Requested: "claude-fable-5-1", Upstream: "claude-fable-5-1"}
	server.markProviderBlocked("anthropic@personal#fable", "quota_or_rate_limit", time.Hour)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	bound, release := server.bindClaudeAccount(context.Background(), request, selected, map[string]any{"messages": []any{}}, false)
	if release != nil {
		defer release()
	}
	profile, ok := selectedClaudeProfile(bound)
	if !ok || profile.Name != "backup" {
		t.Fatalf("selection = %#v ok=%v, want unblocked backup", profile, ok)
	}
}

func TestClaudeUnifiedPoolUsesAllFourAccountsAcrossListeners(t *testing.T) {
	names := []string{"teamA", "teamB", "personal", "backup"}
	usageByToken := map[string]float64{}
	profiles := map[string]claudeUsageProfile{}
	for index, name := range names {
		service := "test-unified-four-" + name
		token := "sk-ant-oat-test-unified-four-" + name
		claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: token, fetchedAt: time.Now()})
		t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })
		profiles[name] = testClaudeProfile(t, name, service, "account-"+name)
		usageByToken["Bearer "+token] = []float64{60, 40, 5, 15}[index]
	}

	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := usageByToken[r.Header.Get("Authorization")]
		_, _ = fmt.Fprintf(w, `{"five_hour":{"utilization":%.1f},"seven_day":{"utilization":20}}`, value)
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true, AccountPool: names,
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000, AccountProfiles: profiles,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48107/v1/messages", nil)
	profile, _, err := server.autoSelectClaudeProfile(context.Background(), request, "conversation-one")
	if err != nil || profile.Name != "personal" {
		t.Fatalf("first unified selection = %q err=%v, want least-used personal", profile.Name, err)
	}
	server.markProviderBlocked("anthropic@personal", "test", time.Minute)
	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "conversation-two")
	if err != nil || profile.Name != "backup" {
		t.Fatalf("blocked-account failover = %q err=%v, want next least-used backup", profile.Name, err)
	}
}

func TestExplicitClaudeAccountSelectionUsesHealthyUnifiedPoolMember(t *testing.T) {
	for _, tc := range []struct {
		name               string
		primaryUsageStatus int
		wantProfile        string
	}{
		{name: "usage unavailable", primaryUsageStatus: http.StatusTooManyRequests, wantProfile: "backup"},
		{name: "authoritatively exhausted", primaryUsageStatus: http.StatusOK, wantProfile: "backup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primaryService := "test-team-primary-" + tc.name
			fallbackService := "test-max-fallback-" + tc.name
			primaryToken := "sk-ant-oat-test-team-primary-" + tc.name
			fallbackToken := "sk-ant-oat-test-max-fallback-" + tc.name
			claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: primaryToken, fetchedAt: time.Now()})
			claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: fallbackToken, fetchedAt: time.Now()})
			t.Cleanup(func() {
				claudeOAuthCredentials.Delete(primaryService)
				claudeOAuthCredentials.Delete(fallbackService)
			})

			messageCalls := map[string]int{}
			anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth := r.Header.Get("Authorization")
				if r.URL.Path == "/api/oauth/usage" {
					if auth == "Bearer "+primaryToken {
						if tc.primaryUsageStatus != http.StatusOK {
							http.Error(w, "rate limited", tc.primaryUsageStatus)
							return
						}
						_, _ = w.Write([]byte(`{"five_hour":{"utilization":95},"seven_day":{"utilization":20}}`))
						return
					}
					_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":20}}`))
					return
				}
				messageCalls[auth]++
				_, _ = w.Write([]byte(`{"type":"message","model":"claude-fable-5","content":[]}`))
			}))
			defer anthropic.Close()

			server, err := newProxyServer(config{
				Providers: map[string]providerConfig{"anthropic": {BaseURL: anthropic.URL}},
				ClaudeUsage: claudeUsageConfig{
					Provider: "anthropic", AutoSelectAccounts: true,
					EligibleUpstreams: []string{"claude-fable-5"}, FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
					CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
					AccountProfiles: map[string]claudeUsageProfile{
						"teamA":  testClaudeProfile(t, "teamA", primaryService, "team-primary-"+tc.name),
						"backup": testClaudeProfile(t, "backup", fallbackService, "max-fallback-"+tc.name),
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			selected := modelConfig{Provider: "anthropic", Upstream: "claude-fable-5", Requested: "claude-fable-5"}
			body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"hello"}]}`)
			payload := map[string]any{"model": "claude-fable-5", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
			request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
			stickyKey := routingStickyKey(selected, payload)
			bound, release := server.bindClaudeAccount(context.Background(), request, selected, payload, true)
			defer release()
			profile, ok := selectedClaudeProfile(bound)
			if !ok || profile.Name != tc.wantProfile {
				t.Fatalf("bound profile = %#v ok=%v, want %q", profile, ok, tc.wantProfile)
			}
			if lease := server.claudeAccounts[stickyKey]; lease.Profile.Name != tc.wantProfile {
				t.Fatalf("lease = %#v, want %q", lease, tc.wantProfile)
			}

			upstream, err := server.doWithFallbacks(context.Background(), bound, body, payload, selected)
			if err != nil {
				t.Fatal(err)
			}
			defer upstream.resp.Body.Close()
			wantAuth := "Bearer " + primaryToken
			if tc.wantProfile == "backup" {
				wantAuth = "Bearer " + fallbackToken
			}
			if messageCalls[wantAuth] != 1 || len(messageCalls) != 1 {
				t.Fatalf("message calls = %#v, want one inference on selected account", messageCalls)
			}
		})
	}
}

func TestConfiguredClaudeProfilesDeduplicatesSameSubscription(t *testing.T) {
	directory := t.TempDir()
	primaryCache := filepath.Join(directory, "primary.json")
	duplicateCache := filepath.Join(directory, "duplicate.json")
	otherCache := filepath.Join(directory, "other.json")
	duplicateIdentity := []byte(`{"oauthAccount":{"accountUuid":"account-a","organizationUuid":"org-a"}}`)
	if err := os.WriteFile(primaryCache, duplicateIdentity, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(duplicateCache, duplicateIdentity, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(otherCache, []byte(`{"oauthAccount":{"accountUuid":"account-b","organizationUuid":"org-b"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	server := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{
		ListenerProfiles: map[string]claudeUsageProfile{
			"48104": {Name: "primary", CachePath: primaryCache, CredentialsService: "primary-service"},
		},
		AccountProfiles: map[string]claudeUsageProfile{
			"duplicate": {Name: "duplicate", CachePath: duplicateCache, CredentialsService: "duplicate-service"},
			"other":     {Name: "other", CachePath: otherCache, CredentialsService: "other-service"},
		},
	}}}
	profiles := server.configuredClaudeProfiles()
	if len(profiles) != 2 || profiles[0].Name != "primary" || profiles[1].Name != "other" {
		t.Fatalf("profiles = %#v, want primary plus distinct other subscription", profiles)
	}
}

func TestClaudeProfileIdentityCacheRefreshesOnFileChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".claude.json")
	if err := os.WriteFile(path, []byte(`{"oauthAccount":{"accountUuid":"account-a"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &proxyServer{}
	profile := claudeUsageProfile{Name: "profile", CachePath: path}
	if got := server.claudeProfileAccountKey(profile); got != "account:account-a" {
		t.Fatalf("first identity = %q", got)
	}
	if err := os.WriteFile(path, []byte(`{"oauthAccount":{"accountUuid":"account-b"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if got := server.claudeProfileAccountKey(profile); got != "account:account-b" {
		t.Fatalf("refreshed identity = %q, want account-b", got)
	}
}

func TestConfiguredClaudeCredentialProfilesDeduplicateExactDirsAndServices(t *testing.T) {
	profiles := configuredClaudeCredentialProfiles(claudeUsageConfig{
		ListenerProfiles: map[string]claudeUsageProfile{
			"48104": {Name: "primary", CachePath: "/profiles/primary/.claude.json", CredentialsService: "service-primary"},
		},
		AccountProfiles: map[string]claudeUsageProfile{
			"duplicate-dir":     {CachePath: "/profiles/primary/.claude.json", CredentialsService: "service-other"},
			"duplicate-service": {CachePath: "/profiles/other/.claude.json", CredentialsService: "service-primary"},
			"secondary":         {CachePath: "/profiles/secondary/.claude.json", CredentialsService: "service-secondary"},
		},
	})
	if len(profiles) != 2 {
		t.Fatalf("credential profiles = %#v, want two unique profiles", profiles)
	}
	if got := []string{profiles[0].Name, profiles[1].Name}; !reflect.DeepEqual(got, []string{"primary", "secondary"}) {
		t.Fatalf("credential profiles = %#v, want exact dir/service dedupe", profiles)
	}
}

func TestClaudeOAuthRefreshTriggerDeduplicatesExactProfile(t *testing.T) {
	profile := claudeUsageProfile{
		Name: "primary", CachePath: filepath.Join(t.TempDir(), ".claude.json"), CredentialsService: "service-primary",
	}
	started := make(chan claudeUsageProfile, 2)
	release := make(chan struct{})
	server := &proxyServer{
		claudeRefreshInFlight: map[string]bool{},
		claudeRefreshRunner: func(_ context.Context, got claudeUsageProfile) error {
			started <- got
			<-release
			return nil
		},
	}
	server.triggerClaudeOAuthRefresh(profile)
	got := <-started
	server.triggerClaudeOAuthRefresh(profile)
	select {
	case duplicate := <-started:
		t.Fatalf("duplicate refresh started for %#v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
	if !reflect.DeepEqual(got, profile) {
		t.Fatalf("triggered profile = %#v, want %#v", got, profile)
	}
	close(release)
	waitForTestClaudeRefresh(t, server)
}

func waitForTestClaudeRefresh(t *testing.T, server *proxyServer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		server.claudeRefreshMu.Lock()
		active := len(server.claudeRefreshInFlight) > 0
		server.claudeRefreshMu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh worker did not finish before fixture cleanup")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClaudeOAuthRefreshClearsOnlyVerifiedAccountAuthBlock(t *testing.T) {
	for _, tc := range []struct {
		name        string
		reason      string
		wantCleared bool
	}{
		{name: "auth block", reason: "auth_exhausted", wantCleared: true},
		{name: "quota block", reason: "quota_or_rate_limit", wantCleared: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			profile := claudeUsageProfile{
				Name: "primary", CachePath: filepath.Join(directory, ".claude.json"), CredentialsService: "service-primary",
			}
			statusPath := filepath.Join(directory, "status.json")
			server := &proxyServer{
				cfg: config{ClaudeUsage: claudeUsageConfig{
					Provider: "anthropic", AccountProfiles: map[string]claudeUsageProfile{"primary": profile},
				}},
				providerStates:        map[string]providerRuntimeState{},
				claudeRefreshInFlight: map[string]bool{},
				claudeRefreshStatus:   statusPath,
			}
			server.markProviderBlocked("anthropic@primary", tc.reason, time.Hour)
			server.claudeRefreshRunner = func(_ context.Context, _ claudeUsageProfile) error {
				status := fmt.Sprintf(`{"generatedAt":%q,"profiles":{"primary":{"state":"refreshed","remainingSeconds":3600}}}`, time.Now().UTC().Format(time.RFC3339))
				return os.WriteFile(statusPath, []byte(status), 0o600)
			}

			server.triggerClaudeOAuthRefresh(profile)
			waitForTestClaudeRefresh(t, server)
			blocked, state := server.providerBlocked("anthropic@primary")
			if blocked == tc.wantCleared || (!tc.wantCleared && state.Reason != tc.reason) {
				t.Fatalf("blocked=%t state=%#v wantCleared=%t", blocked, state, tc.wantCleared)
			}
		})
	}
}

func TestFableWeeklyQuotaBlockIsModelScoped(t *testing.T) {
	server := &proxyServer{
		cfg:            config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}},
		providerStates: map[string]providerRuntimeState{},
	}
	runtimeKey := "anthropic@primary"
	fable := modelConfig{Provider: "anthropic", Requested: "claude-fable-5-1", Upstream: "claude-fable-5-1"}
	headers := http.Header{
		"Anthropic-Ratelimit-Unified-5h-Status":    []string{"allowed"},
		"Anthropic-Ratelimit-Unified-7d-Status":    []string{"allowed_warning"},
		"Anthropic-Ratelimit-Unified-7d_oi-Status": []string{"rejected"},
	}
	modelKey := server.quotaBlockKeyForResponse(runtimeKey, fable, headers)
	if modelKey != runtimeKey+"#fable" {
		t.Fatalf("Fable quota key = %q, want model-scoped key", modelKey)
	}
	if got := server.quotaBlockKeyForResponse(runtimeKey, fable, http.Header{}); got != runtimeKey+"#fable" {
		t.Fatalf("headerless Fable quota key = %q, want model-scoped key", got)
	}
	server.markProviderBlocked(modelKey, "quota_or_rate_limit", time.Hour)
	if blocked, _ := server.providerBlocked(runtimeKey); blocked {
		t.Fatal("Fable-only quota blocked the whole Claude account")
	}
	if blocked, _ := server.providerBlockForCandidate(runtimeKey, fable); !blocked {
		t.Fatal("Fable candidate ignored its model-scoped quota block")
	}
	sonnet := modelConfig{Provider: "anthropic", Requested: "claude-sonnet-5", Upstream: "claude-sonnet-5"}
	if blocked, _ := server.providerBlockForCandidate(runtimeKey, sonnet); blocked {
		t.Fatal("Fable-only quota blocked Sonnet on the same account")
	}

	headers.Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
	if got := server.quotaBlockKeyForResponse(runtimeKey, fable, headers); got != runtimeKey {
		t.Fatalf("account-wide quota key = %q, want %q", got, runtimeKey)
	}
}

func TestProviderRuntimeKeyUsesSelectedClaudeProfile(t *testing.T) {
	server := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}}}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	selected := modelConfig{Provider: "anthropic", ClaudeProfile: "backup"}
	if got := server.providerRuntimeKeyForCandidate(request, "anthropic", selected); got != "anthropic@backup" {
		t.Fatalf("runtime key = %q, want selected account key", got)
	}
}

func TestAnthropicHeadersProactivelyTrackFableQuota(t *testing.T) {
	server := &proxyServer{
		cfg:            config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}},
		providerStates: map[string]providerRuntimeState{},
	}
	runtimeKey := "anthropic@primary"
	reset := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	server.observeProviderHeaders(runtimeKey, http.Header{
		"Anthropic-Ratelimit-Unified-7d_oi-Status": []string{"rejected"},
		"Anthropic-Ratelimit-Unified-7d_oi-Reset":  []string{reset},
	})
	fable := modelConfig{Provider: "anthropic", Requested: "claude-fable-5-1"}
	if blocked, _ := server.providerBlockForCandidate(runtimeKey, fable); !blocked {
		t.Fatal("Fable quota header did not proactively block Fable")
	}
	sonnet := modelConfig{Provider: "anthropic", Requested: "claude-sonnet-5"}
	if blocked, _ := server.providerBlockForCandidate(runtimeKey, sonnet); blocked {
		t.Fatal("Fable quota header blocked Sonnet")
	}
	server.observeProviderHeaders(runtimeKey, http.Header{
		"Anthropic-Ratelimit-Unified-7d_oi-Status": []string{"allowed"},
	})
	if blocked, _ := server.providerBlockForCandidate(runtimeKey, fable); blocked {
		t.Fatal("allowed Fable quota header did not clear Fable block")
	}
}

func TestClaudeAuthFailureTriggersExactProfileAndPreservesPairedFallback(t *testing.T) {
	const (
		primaryService  = "test-auth-failure-primary"
		fallbackService = "test-auth-failure-fallback"
		primaryToken    = "sk-ant-oat-test-auth-failure-primary"
		fallbackToken   = "sk-ant-oat-test-auth-failure-fallback"
	)
	claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: primaryToken, fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: fallbackToken, fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(primaryService)
		claudeOAuthCredentials.Delete(fallbackService)
	})

	messageCalls := map[string]int{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if r.URL.Path == "/api/oauth/usage" {
			_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
			return
		}
		if got := r.Header.Get("anthropic-beta"); !strings.Contains(got, "oauth-2025-04-20") {
			t.Errorf("anthropic-beta = %q, want OAuth beta", got)
		}
		messageCalls[auth]++
		if auth == "Bearer "+primaryToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"authentication_error","message":"invalid OAuth token"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"type":"message","content":[]}`))
	}))
	defer upstream.Close()

	primary := testClaudeProfile(t, "teamA", primaryService, "auth-primary")
	fallback := testClaudeProfile(t, "backup", fallbackService, "auth-fallback")
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: upstream.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true,
			EligibleUpstreams: []string{"claude-fable-5"}, FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			AccountProfiles: map[string]claudeUsageProfile{"teamA": primary, "backup": fallback},
			AccountPool:     []string{"teamA", "backup"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	triggered := make(chan claudeUsageProfile, 1)
	server.claudeRefreshRunner = func(_ context.Context, profile claudeUsageProfile) error {
		triggered <- profile
		return nil
	}
	selected := modelConfig{Provider: "anthropic", Upstream: "claude-fable-5", Requested: "claude-fable-5"}
	body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"hello"}]}`)
	payload := map[string]any{"model": "claude-fable-5", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", bytes.NewReader(body))
	response, err := server.doWithFallbacks(context.Background(), request, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.resp.Body.Close()
	if response.model.ClaudeProfile != "backup" || messageCalls["Bearer "+primaryToken] != 1 || messageCalls["Bearer "+fallbackToken] != 1 {
		t.Fatalf("response profile=%q calls=%#v, want primary 401 then paired fallback", response.model.ClaudeProfile, messageCalls)
	}
	select {
	case got := <-triggered:
		if !reflect.DeepEqual(got, primary) {
			t.Fatalf("refresh profile = %#v, want exact primary %#v", got, primary)
		}
	case <-time.After(time.Second):
		t.Fatal("auth failure did not trigger refresh")
	}
	if _, cached := claudeOAuthCredentials.Load(primaryService); cached {
		t.Fatal("stale primary OAuth token remained cached")
	}
}

func TestAnthropicAuthFailuresNeverReachPaidFallback(t *testing.T) {
	const (
		primaryService  = "test-paid-guard-primary"
		fallbackService = "test-paid-guard-fallback"
	)
	for service, token := range map[string]string{
		primaryService:  "sk-ant-oat-test-paid-guard-primary",
		fallbackService: "sk-ant-oat-test-paid-guard-fallback",
	} {
		claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: token, fetchedAt: time.Now()})
		t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })
	}

	var anthropicCalls atomic.Int32
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/oauth/usage" {
			_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
			return
		}
		anthropicCalls.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"permission_error","message":"OAuth authentication is currently not allowed for this organization.","details":{"error_code":"oauth_not_allowed_for_organization"}}}`))
	}))
	defer anthropic.Close()

	var paidCalls atomic.Int32
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paidCalls.Add(1)
		_, _ = w.Write([]byte(`{"type":"message","model":"moonshotai/kimi-k3","content":[]}`))
	}))
	defer paid.Close()

	circuitDisabled := false
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"anthropic":   {BaseURL: anthropic.URL, CircuitBreaker: &circuitDisabled},
			"vercel-kimi": {BaseURL: paid.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true,
			EligibleUpstreams: []string{"claude-fable-5"}, FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			PaidFallbackProviders: []string{"vercel-kimi"},
			AccountProfiles: map[string]claudeUsageProfile{
				"teamA":  testClaudeProfile(t, "teamA", primaryService, "paid-guard-primary"),
				"backup": testClaudeProfile(t, "backup", fallbackService, "paid-guard-fallback"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.claudeRefreshRunner = func(context.Context, claudeUsageProfile) error { return nil }
	selected := modelConfig{
		Provider: "anthropic", Upstream: "claude-fable-5", Requested: "claude-fable-5",
		Fallbacks: []modelConfig{{Provider: "vercel-kimi", Upstream: "moonshotai/kimi-k3"}},
	}
	body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"hello"}]}`)
	payload := map[string]any{"model": "claude-fable-5", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)

	if _, err := server.doWithFallbacks(context.Background(), request, body, payload, selected); err == nil {
		t.Fatal("auth failures reached a successful paid fallback")
	}
	if anthropicCalls.Load() != 2 || paidCalls.Load() != 0 {
		t.Fatalf("calls Anthropic=%d paid=%d, want 2/0", anthropicCalls.Load(), paidCalls.Load())
	}
}

func TestClaudeProvider5xxDoesNotTriggerCredentialRefresh(t *testing.T) {
	const service = "test-provider-5xx"
	claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-provider-server-errors", fetchedAt: time.Now()})
	t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/oauth/usage" {
			_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
			return
		}
		http.Error(w, "provider unavailable", http.StatusServiceUnavailable)
	}))
	defer upstream.Close()
	profile := testClaudeProfile(t, "teamA", service, "provider-5xx")
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: upstream.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-fable-5"},
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, CacheTTLSeconds: 300, StaleTTLSeconds: 1800,
			ListenerProfiles: map[string]claudeUsageProfile{"48104": profile},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	triggered := make(chan struct{}, 1)
	server.claudeRefreshRunner = func(context.Context, claudeUsageProfile) error {
		triggered <- struct{}{}
		return nil
	}
	selected := modelConfig{Provider: "anthropic", Upstream: "claude-fable-5", Requested: "claude-fable-5"}
	body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"hello"}]}`)
	payload := map[string]any{"model": "claude-fable-5", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", bytes.NewReader(body))
	_, _ = server.doWithFallbacks(context.Background(), request, body, payload, selected)
	select {
	case <-triggered:
		t.Fatal("provider-wide 5xx triggered credential refresh")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClaudeRefreshRunnerUsesExactForcedProfileAndSanitizedEnvironment(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "helper.log")
	helperPath := filepath.Join(directory, "helper.sh")
	helper := "#!/bin/bash\n" +
		"printf 'cwd=%s\\n' \"$PWD\" > \"$TEST_HELPER_LOG\"\n" +
		"printf 'base=%s auth=%s key=%s config=%s\\n' \"${ANTHROPIC_BASE_URL-unset}\" \"${ANTHROPIC_AUTH_TOKEN-unset}\" \"${ANTHROPIC_API_KEY-unset}\" \"${CLAUDE_CONFIG_DIR-unset}\" >> \"$TEST_HELPER_LOG\"\n" +
		"printf 'arg=%s\\n' \"$@\" >> \"$TEST_HELPER_LOG\"\n"
	if err := os.WriteFile(helperPath, []byte(helper), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_HELPER_LOG", logPath)
	t.Setenv("ANTHROPIC_BASE_URL", "http://127.0.0.1:48104")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "auth-canary")
	t.Setenv("ANTHROPIC_API_KEY", "key-canary")
	t.Setenv("CLAUDE_CONFIG_DIR", "/wrong/profile")
	profile := claudeUsageProfile{Name: "exact", CachePath: filepath.Join(directory, "profile", ".claude.json"), CredentialsService: "service-exact"}
	if err := runClaudeOAuthRefreshHelper(context.Background(), helperPath, profile); err != nil {
		t.Fatal(err)
	}
	logged, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(logged)
	for _, expected := range []string{"base=unset auth=unset key=unset config=unset", "arg=--config-dir", "arg=" + filepath.Dir(profile.CachePath), "arg=--credentials-service", "arg=service-exact", "arg=--auth-error"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("helper log missing %q: %s", expected, text)
		}
	}
	if strings.Contains(text, "auth-canary") || strings.Contains(text, "key-canary") {
		t.Fatalf("helper received credential environment: %s", text)
	}
}

func TestClaudeScheduledRefreshRunnerUsesConfigAndSanitizedEnvironment(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "helper.log")
	helperPath := filepath.Join(directory, "helper.sh")
	configPath := filepath.Join(directory, "proxy.json")
	helper := "#!/bin/bash\n" +
		"printf 'cwd=%s\\n' \"$PWD\" > \"$TEST_HELPER_LOG\"\n" +
		"printf 'base=%s auth=%s key=%s config=%s\\n' \"${ANTHROPIC_BASE_URL-unset}\" \"${ANTHROPIC_AUTH_TOKEN-unset}\" \"${ANTHROPIC_API_KEY-unset}\" \"${CLAUDE_CONFIG_DIR-unset}\" >> \"$TEST_HELPER_LOG\"\n" +
		"printf 'arg=%s\\n' \"$@\" >> \"$TEST_HELPER_LOG\"\n"
	if err := os.WriteFile(helperPath, []byte(helper), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_HELPER_LOG", logPath)
	t.Setenv("ANTHROPIC_BASE_URL", "http://127.0.0.1:48104")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "auth-canary")
	t.Setenv("ANTHROPIC_API_KEY", "key-canary")
	t.Setenv("CLAUDE_CONFIG_DIR", "/wrong/profile")
	if err := runClaudeOAuthScheduledRefreshHelper(context.Background(), helperPath, configPath); err != nil {
		t.Fatal(err)
	}
	logged, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(logged)
	for _, expected := range []string{"cwd=/private/tmp", "base=unset auth=unset key=unset config=unset", "arg=--config", "arg=" + configPath} {
		if !strings.Contains(text, expected) {
			t.Fatalf("helper log missing %q: %s", expected, text)
		}
	}
	if strings.Contains(text, "--auth-error") || strings.Contains(text, "auth-canary") || strings.Contains(text, "key-canary") {
		t.Fatalf("scheduled helper crossed security boundary: %s", text)
	}
}

func TestClaudeScheduledRefreshMonitorReconcilesCredentialCache(t *testing.T) {
	const service = "scheduled-refresh-service"
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "security"), []byte("#!/bin/sh\nprintf '%s' '{\"claudeAiOauth\":{\"accessToken\":\"sk-ant-oat-test-refreshed\"}}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	profile := claudeUsageProfile{Name: "primary", CachePath: filepath.Join(t.TempDir(), ".claude.json"), CredentialsService: service}
	claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-stale", fetchedAt: time.Now()})
	t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })
	ran := make(chan struct{}, 1)
	server := &proxyServer{
		cfg: config{ClaudeUsage: claudeUsageConfig{
			OAuthRefreshEnabled: true, OAuthRefreshInterval: 60, OAuthRefreshTimeout: 120,
			ListenerProfiles: map[string]claudeUsageProfile{"48104": profile},
		}},
		claudeScheduledRunner: func(context.Context) error {
			ran <- struct{}{}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() { defer close(finished); server.monitorClaudeOAuthRefresh(ctx) }()
	defer func() { cancel(); <-finished }()
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("scheduled refresh did not run immediately")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if raw, cached := claudeOAuthCredentials.Load(service); cached && raw.(cachedClaudeOAuthCredential).token == "sk-ant-oat-test-refreshed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduled refresh did not reconcile rotated OAuth token")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClaudeCredentialRefreshHealthAllowsOnlyConfiguredSanitizedFields(t *testing.T) {
	profile := testClaudeProfile(t, "primary", "service-primary-health", "health-primary")
	directory := t.TempDir()
	statusPath := filepath.Join(directory, "status.json")
	status := `{
		"checkedAt":"2026-08-30T12:00:00Z",
		"generatedAt":"2026-08-30T12:00:01Z",
		"accessToken":"top-level-canary",
		"profiles":{
			"primary":{"state":"refreshed","expiresAt":"2026-08-30T13:00:00Z","remainingSeconds":3600,"lastAttemptAt":"2026-08-30T12:00:00Z","lastSuccessAt":"2026-08-30T12:00:00Z","error":"token-canary","refreshToken":"profile-canary"},
			"unknown":{"state":"valid","expiresAt":"2026-08-30T13:00:00Z","remainingSeconds":3600,"error":null}
		}
	}`
	if err := os.WriteFile(statusPath, []byte(status), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &proxyServer{
		cfg:                 config{ClaudeUsage: claudeUsageConfig{ListenerProfiles: map[string]claudeUsageProfile{"48104": profile}}},
		claudeRefreshStatus: statusPath,
	}
	health := server.claudeCredentialRefreshHealth()
	body, err := json.Marshal(health)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(body)
	for _, forbidden := range []string{"top-level-canary", "profile-canary", "token-canary", "unknown", "accessToken", "refreshToken"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("credential refresh health leaked %q: %s", forbidden, encoded)
		}
	}
	entry, ok := health.Profiles["primary"]
	if !ok || entry.State != "refreshed" || entry.RemainingSeconds == nil || *entry.RemainingSeconds != 3600 || entry.Error != nil {
		t.Fatalf("sanitized health = %#v", health)
	}
}

func TestClaudeRefreshOutcomeRequiresFreshUsableProfileStatus(t *testing.T) {
	directory := t.TempDir()
	statusPath := filepath.Join(directory, "status.json")
	primary := testClaudeProfile(t, "primary", "service-primary-outcome", "outcome-primary")
	secondary := testClaudeProfile(t, "secondary", "service-secondary-outcome", "outcome-secondary")
	server := &proxyServer{
		cfg: config{ClaudeUsage: claudeUsageConfig{
			ListenerProfiles: map[string]claudeUsageProfile{"48104": primary},
			AccountProfiles:  map[string]claudeUsageProfile{"secondary": secondary},
		}},
		claudeRefreshStatus: statusPath,
	}
	started := time.Now().UTC().Truncate(time.Second)
	status := fmt.Sprintf(`{"generatedAt":%q,"profiles":{"primary":{"state":"valid","remainingSeconds":3600},"secondary":{"state":"refresh_failed","remainingSeconds":-1,"error":"claude_cli_failed"}}}`, started.Format(time.RFC3339))
	if err := os.WriteFile(statusPath, []byte(status), 0o600); err != nil {
		t.Fatal(err)
	}
	healthy, failed, err := server.validateClaudeScheduledRefreshOutcome(started)
	if err != nil {
		t.Fatal(err)
	}
	if healthy != 1 || !reflect.DeepEqual(failed, []string{"secondary:refresh_failed"}) {
		t.Fatalf("healthy=%d failed=%#v", healthy, failed)
	}
	if err := server.validateClaudeProfileRefreshOutcome(primary, started); err != nil {
		t.Fatal(err)
	}
	if err := server.validateClaudeProfileRefreshOutcome(secondary, started); err == nil {
		t.Fatal("failed profile accepted as refreshed")
	}
	if _, _, err := server.validateClaudeScheduledRefreshOutcome(started.Add(time.Second)); err == nil {
		t.Fatal("stale refresh status accepted")
	}
}

func TestClaudeListenersShareUnifiedPoolAndDuplicateAccountIsIneligible(t *testing.T) {
	directory := t.TempDir()
	profile := func(name, account string) claudeUsageProfile {
		path := filepath.Join(directory, name+".json")
		body := fmt.Sprintf(`{"oauthAccount":{"accountUuid":%q,"organizationUuid":%q}}`, account, "org-"+account)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return claudeUsageProfile{Name: name, CachePath: path, CredentialsService: "service-" + name}
	}

	server := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{
		AutoSelectAccounts: true,
		AccountPool:        []string{"teamA", "teamB", "personal", "backup"},
		AccountProfiles: map[string]claudeUsageProfile{
			"teamA":    profile("teamA", "accountA"),
			"teamB":    profile("teamB", "teamB"),
			"personal": profile("personal", "duplicate-account"),
			"backup":   profile("backup", "duplicate-account"),
		},
	}}}

	accountAEligible := server.eligibleClaudePoolProfiles("48104")
	teamBEligible := server.eligibleClaudePoolProfiles("48107")
	accountANames := make([]string, len(accountAEligible))
	for i, profile := range accountAEligible {
		accountANames[i] = profile.Name
	}
	teamBNames := make([]string, len(teamBEligible))
	for i, profile := range teamBEligible {
		teamBNames[i] = profile.Name
	}
	want := []string{"teamA", "teamB", "personal"}
	if !reflect.DeepEqual(accountANames, want) {
		t.Fatalf("48104 eligible pool = %#v, want unified pool with duplicate backup excluded", accountAEligible)
	}
	if !reflect.DeepEqual(teamBNames, want) {
		t.Fatalf("48107 eligible pool = %#v, want same unified pool", teamBEligible)
	}
}

func TestClaudeAccountLeaseStabilityExpiryAndPrimaryRecovery(t *testing.T) {
	const (
		primaryService  = "test-lease-teamA"
		fallbackService = "test-lease-backup"
	)
	claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-lease-primary", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-lease-fallback", fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(primaryService)
		claudeOAuthCredentials.Delete(fallbackService)
	})

	usageByAuth := map[string]float64{
		"Bearer sk-ant-oat-test-lease-primary":  99,
		"Bearer sk-ant-oat-test-lease-fallback": 10,
	}
	var usageMu sync.Mutex
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		usageMu.Lock()
		value := usageByAuth[r.Header.Get("Authorization")]
		usageMu.Unlock()
		_, _ = fmt.Fprintf(w, `{"five_hour":{"utilization":%.1f},"seven_day":{"utilization":10}}`, value)
	}))
	defer usage.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true,
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			AccountProfiles: map[string]claudeUsageProfile{
				"teamA":  testClaudeProfile(t, "teamA", primaryService, "accountA-lease"),
				"backup": testClaudeProfile(t, "backup", fallbackService, "backup-lease"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	server.clockNow = func() time.Time { return now }
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)

	profile, _, err := server.autoSelectClaudeProfile(context.Background(), request, "leased-conversation")
	if err != nil || profile.Name != "backup" {
		t.Fatalf("initial selection = %q err=%v, want backup while primary exhausted", profile.Name, err)
	}
	lease := server.claudeAccounts["leased-conversation"]
	if got := lease.Until.Sub(now); got != 1800*time.Second {
		t.Fatalf("default lease = %s, want 1800s", got)
	}

	now = now.Add(20 * time.Minute)
	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "leased-conversation")
	if err != nil || profile.Name != "backup" {
		t.Fatalf("active lease reuse = %q err=%v, want backup", profile.Name, err)
	}
	refreshedLease := server.claudeAccounts["leased-conversation"]
	if got := refreshedLease.Until.Sub(now); got != 1800*time.Second || !refreshedLease.Until.After(lease.Until) {
		t.Fatalf("refreshed lease until=%s remaining=%s, want 30m from latest activity", refreshedLease.Until, got)
	}

	usageMu.Lock()
	usageByAuth["Bearer sk-ant-oat-test-lease-primary"] = 5
	usageMu.Unlock()
	server.claudeUse.mu.Lock()
	server.claudeUse.snapshots = map[string]claudeUsageSnapshot{}
	server.claudeUse.mu.Unlock()

	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "new-conversation")
	if err != nil || profile.Name != "teamA" {
		t.Fatalf("new conversation after recovery = %q err=%v, want primary", profile.Name, err)
	}
	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "leased-conversation")
	if err != nil || profile.Name != "backup" {
		t.Fatalf("active lease after primary recovery = %q err=%v, want stable backup", profile.Name, err)
	}

	now = refreshedLease.Until.Add(time.Second)
	profile, _, err = server.autoSelectClaudeProfile(context.Background(), request, "leased-conversation")
	if err != nil || profile.Name != "teamA" {
		t.Fatalf("expired lease selection = %q err=%v, want recovered primary", profile.Name, err)
	}
}

func TestAnthropicAccountLocalExhaustionRetriesPairedAccountBeforeKimi(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		body        string
		blockReason string
	}{
		{name: "quota", status: http.StatusTooManyRequests, body: `{"type":"error","error":{"type":"rate_limit_error","message":"usage limit reached"}}`, blockReason: "quota_or_rate_limit"},
		{name: "auth", status: http.StatusUnauthorized, body: `{"type":"error","error":{"type":"authentication_error","message":"expired login"}}`, blockReason: "auth_exhausted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primaryService := "test-failover-primary-" + tc.name
			fallbackService := "test-failover-fallback-" + tc.name
			primaryToken := "sk-ant-oat-test-failover-primary-" + tc.name
			fallbackToken := "sk-ant-oat-test-failover-fallback-" + tc.name
			claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: primaryToken, fetchedAt: time.Now()})
			claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: fallbackToken, fetchedAt: time.Now()})
			t.Cleanup(func() {
				claudeOAuthCredentials.Delete(primaryService)
				claudeOAuthCredentials.Delete(fallbackService)
			})

			var primaryCalls atomic.Int32
			var fallbackCalls atomic.Int32
			anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/oauth/usage" {
					_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
					return
				}
				switch r.Header.Get("Authorization") {
				case "Bearer " + primaryToken:
					primaryCalls.Add(1)
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				case "Bearer " + fallbackToken:
					fallbackCalls.Add(1)
					_, _ = w.Write([]byte(`{"type":"message","model":"claude-opus-4-8","content":[]}`))
				default:
					t.Errorf("unexpected Anthropic auth %q", r.Header.Get("Authorization"))
					w.WriteHeader(http.StatusUnauthorized)
				}
			}))
			defer anthropic.Close()

			var kimiCalls atomic.Int32
			kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				kimiCalls.Add(1)
				_, _ = w.Write([]byte(`{"type":"message","model":"moonshotai/kimi-k3","content":[]}`))
			}))
			defer kimi.Close()

			circuitDisabled := false
			server, err := newProxyServer(config{
				Providers: map[string]providerConfig{
					"anthropic": {BaseURL: anthropic.URL, CircuitBreaker: &circuitDisabled},
					"kimi":      {BaseURL: kimi.URL},
				},
				ClaudeUsage: claudeUsageConfig{
					Provider: "anthropic", AutoSelectAccounts: true,
					EligibleUpstreams:    []string{"claude-opus-4-8"},
					AccountPool:          []string{"teamA", "backup"},
					FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
					CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
					AccountProfiles: map[string]claudeUsageProfile{
						"teamA":  testClaudeProfile(t, "teamA", primaryService, "accountA-failover-"+tc.name),
						"backup": testClaudeProfile(t, "backup", fallbackService, "backup-failover-"+tc.name),
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			selected := modelConfig{
				Provider: "anthropic", Upstream: "claude-opus-4-8", Requested: "claude-opus-4-8",
				Fallbacks: []modelConfig{{Provider: "kimi", Upstream: "moonshotai/kimi-k3"}},
			}
			body := []byte(`{"model":"claude-opus-4-8","messages":[{"role":"user","content":"hello"}]}`)
			payload := map[string]any{"model": "claude-opus-4-8", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
			request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)

			upstream, err := server.doWithFallbacks(context.Background(), request, body, payload, selected)
			if err != nil {
				t.Fatal(err)
			}
			defer upstream.resp.Body.Close()
			if upstream.providerName != "anthropic" || upstream.model.ClaudeProfile != "backup" {
				t.Fatalf("upstream = %q profile=%q, want paired Anthropic backup", upstream.providerName, upstream.model.ClaudeProfile)
			}
			if primaryCalls.Load() != 1 || fallbackCalls.Load() != 1 || kimiCalls.Load() != 0 {
				t.Fatalf("calls primary=%d fallback=%d kimi=%d, want 1/1/0", primaryCalls.Load(), fallbackCalls.Load(), kimiCalls.Load())
			}
			blocked, state := server.providerBlocked("anthropic@teamA")
			if !blocked || state.Reason != tc.blockReason {
				t.Fatalf("primary state = %#v, want account-local %q block", state, tc.blockReason)
			}
			if blocked, _ := server.providerBlocked("anthropic@backup"); blocked {
				t.Fatal("primary failure leaked into paired account health")
			}
			stickyKey := routingStickyKey(selected, payload)
			if lease := server.claudeAccounts[stickyKey]; lease.Profile.Name != "backup" {
				t.Fatalf("successful failover lease = %#v, want backup", lease)
			}
		})
	}
}

func TestAnthropicProviderWide5xxRequiresConfirmationBeforePaidFallback(t *testing.T) {
	const (
		primaryService  = "test-outage-primary"
		fallbackService = "test-outage-fallback"
	)
	claudeOAuthCredentials.Store(primaryService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-outage-primary", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(fallbackService, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-outage-fallback", fetchedAt: time.Now()})
	t.Cleanup(func() {
		claudeOAuthCredentials.Delete(primaryService)
		claudeOAuthCredentials.Delete(fallbackService)
	})

	var anthropicMessageCalls atomic.Int32
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/oauth/usage" {
			_, _ = w.Write([]byte(`{"five_hour":{"utilization":10},"seven_day":{"utilization":10}}`))
			return
		}
		anthropicMessageCalls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"boom"}}`))
	}))
	defer anthropic.Close()
	var kimiCalls atomic.Int32
	kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kimiCalls.Add(1)
		_, _ = w.Write([]byte(`{"type":"message","model":"moonshotai/kimi-k3","content":[]}`))
	}))
	defer kimi.Close()

	circuitDisabled := false
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"anthropic":   {BaseURL: anthropic.URL, CircuitBreaker: &circuitDisabled},
			"vercel-kimi": {BaseURL: kimi.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true,
			EligibleUpstreams:    []string{"claude-opus-4-8"},
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			PaidFallbackProviders: []string{"vercel-kimi"}, PaidFallbackMinOutageFailures: 3,
			PaidFallbackWindowSeconds: 120, PaidFallbackActiveSeconds: 300,
			AccountProfiles: map[string]claudeUsageProfile{
				"teamA":  testClaudeProfile(t, "teamA", primaryService, "accountA-outage"),
				"backup": testClaudeProfile(t, "backup", fallbackService, "backup-outage"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Provider: "anthropic", Upstream: "claude-opus-4-8", Requested: "claude-opus-4-8",
		Fallbacks: []modelConfig{{Provider: "vercel-kimi", Upstream: "moonshotai/kimi-k3"}},
	}
	body := []byte(`{"model":"claude-opus-4-8","messages":[{"role":"user","content":"hello"}]}`)
	payload := map[string]any{"model": "claude-opus-4-8", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	for attempt := 1; attempt <= 2; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
		if _, err := server.doWithFallbacks(context.Background(), request, body, payload, selected); err == nil {
			t.Fatalf("attempt %d reached paid fallback before outage confirmation", attempt)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	upstream, err := server.doWithFallbacks(context.Background(), request, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.resp.Body.Close()
	if upstream.providerName != "vercel-kimi" || anthropicMessageCalls.Load() != 3 || kimiCalls.Load() != 1 {
		t.Fatalf("provider=%q Anthropic calls=%d Kimi calls=%d, want paid fallback only after three confirmed failures", upstream.providerName, anthropicMessageCalls.Load(), kimiCalls.Load())
	}
}

func TestAnthropicAccountUsageLimitFallsBackToKimi(t *testing.T) {
	var mu sync.Mutex
	anthropicCalls := map[string]int{}
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		anthropicCalls[auth]++
		mu.Unlock()
		if strings.Contains(auth, "accountA-token") {
			w.Header().Set("Retry-After", "3600")
			w.Header().Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"usage limit reached"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"claude-fable-5","content":[]}`))
	}))
	defer anthropic.Close()

	kimiCalls := 0
	kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kimiCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"moonshotai/kimi-k3","content":[]}`))
	}))
	defer kimi.Close()

	circuitDisabled := false
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"anthropic": {BaseURL: anthropic.URL, CircuitBreaker: &circuitDisabled},
			"kimi":      {BaseURL: kimi.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5"},
			FiveHourThresholdPct: 85, SevenDayThresholdPct: 85,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			ListenerProfiles: map[string]claudeUsageProfile{
				"48104": {Name: "accountA"},
				"48107": {Name: "teamB"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Provider: "anthropic", Requested: "claude-fable-5",
		Fallbacks: []modelConfig{{Provider: "kimi", Upstream: "moonshotai/kimi-k3", ResponseAlias: "kimi-k3[1m]"}},
	}
	body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"hello"}]}`)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}

	accountA := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	accountA.Header.Set("Authorization", "Bearer sk-ant-oat-test-accountA-token")
	accountAUpstream, err := server.doWithFallbacks(context.Background(), accountA, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	if accountAUpstream.providerName != "kimi" || accountAUpstream.resp.StatusCode != http.StatusOK {
		t.Fatalf("AccountA upstream = %q status=%d, want Kimi 200", accountAUpstream.providerName, accountAUpstream.resp.StatusCode)
	}
	_ = accountAUpstream.resp.Body.Close()
	accountAKey := server.providerRuntimeKey(accountA, "anthropic")
	if blocked, _ := server.providerBlocked(accountAKey); !blocked {
		t.Fatal("account usage limit did not temporarily block the exhausted account")
	}
	if server.circuitEnabled(accountAKey) {
		t.Fatal("scoped Anthropic key lost base provider circuitBreaker=false")
	}

	teamB := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48107/v1/messages", nil)
	teamB.Header.Set("Authorization", "Bearer sk-ant-oat-test-teamB-token")
	teamBKey := server.providerRuntimeKey(teamB, "anthropic")
	if blocked, _ := server.providerBlocked(teamBKey); blocked {
		t.Fatal("first account rate limit leaked into second account")
	}
	teamBUpstream, err := server.doWithFallbacks(context.Background(), teamB, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	if teamBUpstream.providerName != "anthropic" {
		t.Fatalf("second account provider = %q, want Anthropic", teamBUpstream.providerName)
	}
	_ = teamBUpstream.resp.Body.Close()

	mu.Lock()
	accountACalls := anthropicCalls["Bearer sk-ant-oat-test-accountA-token"]
	teamBCalls := anthropicCalls["Bearer sk-ant-oat-test-teamB-token"]
	mu.Unlock()
	if accountACalls != 1 || teamBCalls != 1 || kimiCalls != 1 {
		t.Fatalf("calls: accountA=%d teamB=%d kimi=%d, want 1/1/1", accountACalls, teamBCalls, kimiCalls)
	}
}

func TestAnthropicServiceWideRateLimitFallsBackForEveryAccount(t *testing.T) {
	var anthropicCalls atomic.Int32
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anthropicCalls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"service temporarily unavailable"}}`))
	}))
	defer anthropic.Close()

	var kimiCalls atomic.Int32
	kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kimiCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"moonshotai/kimi-k3","content":[]}`))
	}))
	defer kimi.Close()

	circuitDisabled := false
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"anthropic": {BaseURL: anthropic.URL, CircuitBreaker: &circuitDisabled},
			"kimi":      {BaseURL: kimi.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5"},
			FiveHourThresholdPct: 85, SevenDayThresholdPct: 85,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			ListenerProfiles: map[string]claudeUsageProfile{
				"48104": {Name: "accountA"},
				"48107": {Name: "teamB"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Provider: "anthropic", Requested: "claude-fable-5",
		Fallbacks: []modelConfig{{Provider: "kimi", Upstream: "moonshotai/kimi-k3", ResponseAlias: "kimi-k3[1m]"}},
	}
	body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"hello"}]}`)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		port  string
		name  string
		token string
	}{
		{port: "48104", name: "accountA", token: "accountA-token"},
		{port: "48107", name: "teamB", token: "teamB-token"},
	} {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:"+tc.port+"/v1/messages", nil)
		req.Header.Set("Authorization", "Bearer sk-ant-oat-"+tc.token)
		upstream, err := server.doWithFallbacks(context.Background(), req, body, payload, selected)
		if err != nil {
			t.Fatalf("%s fallback: %v", tc.name, err)
		}
		if upstream.providerName != "kimi" {
			t.Fatalf("%s provider = %q, want Kimi fallback", tc.name, upstream.providerName)
		}
		_ = upstream.resp.Body.Close()
		if upstream.resp.StatusCode != http.StatusOK {
			t.Fatalf("%s fallback status = %d, want 200", tc.name, upstream.resp.StatusCode)
		}
	}
	if blocked, _ := server.providerBlocked("anthropic"); blocked {
		t.Fatal("service-wide failures created a global Anthropic block")
	}
	if anthropicCalls.Load() != 2 || kimiCalls.Load() != 2 {
		t.Fatalf("calls: anthropic=%d kimi=%d, want 2/2", anthropicCalls.Load(), kimiCalls.Load())
	}
}

func TestConversationOpenerUsesClaudeHeadroomThenOpenSourceHealth(t *testing.T) {
	usagePct := 30
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"five_hour":{"utilization":%d},"seven_day":{"utilization":20}}`, usagePct)))
	}))
	defer usage.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"anthropic": {BaseURL: usage.URL}, "bigmodel": {BaseURL: usage.URL}, "ollama": {BaseURL: usage.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5", "claude-opus-4-8"},
			PreferBelowUsagePct: 40, FiveHourThresholdPct: 85, SevenDayThresholdPct: 85,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
		AdaptiveRouting: adaptiveRoutingConfig{ProviderTiers: map[string]int{"bigmodel": 0, "ollama": 0, "anthropic": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidates := []modelConfig{
		{Provider: "bigmodel", Upstream: "glm-5.3-flash"},
		{Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"},
		{Provider: "anthropic", Upstream: "claude-sonnet-5"},
		{Provider: "anthropic", Upstream: "claude-opus-4-8"},
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer sk-ant-oat-test-headroom")
	if got := server.openingConversationFamily(context.Background(), req, candidates); got != "anthropic" {
		t.Fatalf("30%% consumed opener family = %q, want anthropic", got)
	}

	usagePct = 50
	req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer sk-ant-oat-test-open-source")
	if got := server.openingConversationFamily(context.Background(), req, candidates); got != "glm" {
		t.Fatalf("50%% consumed opener family = %q, want healthy GLM", got)
	}
	server.markProviderBlocked("bigmodel", "test_glm_instability", time.Hour)
	if got := server.openingConversationFamily(context.Background(), req, candidates); got != "deepseek" {
		t.Fatalf("blocked GLM opener family = %q, want DeepSeek", got)
	}
}

func TestExplicitOpusOverridesStaleKimiConversationAffinity(t *testing.T) {
	var anthropicCalls atomic.Int32
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anthropicCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"claude-opus-4-8","content":[]}`))
	}))
	defer anthropic.Close()

	var kimiCalls atomic.Int32
	kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kimiCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"moonshotai/kimi-k3","content":[]}`))
	}))
	defer kimi.Close()

	server, err := newProxyServer(config{
		DefaultProvider: "anthropic",
		Providers: map[string]providerConfig{
			"anthropic": {BaseURL: anthropic.URL},
			"kimi":      {BaseURL: kimi.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-opus-4-8"},
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
		},
		AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, StickySeconds: 300,
			ProviderTiers: map[string]int{"anthropic": 0, "kimi": 0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Provider: "anthropic", Requested: "claude-opus-4-8",
		Fallbacks: []modelConfig{{Provider: "kimi", Upstream: "moonshotai/kimi-k3", ResponseAlias: "kimi-k3[1m]"}},
	}
	body := []byte(`{"model":"claude-opus-4-8","messages":[{"role":"user","content":"hello"}]}`)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	stickyKey := routingStickyKey(selected, payload)
	server.adaptive.sticky[stickyKey] = stickyRoute{
		Key: "kimi|moonshotai/kimi-k3", Family: "kimi",
		Until: time.Now().Add(time.Hour), FamilyUntil: time.Now().Add(24 * time.Hour),
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer sk-ant-oat-test-explicit-primary")
	upstream, err := server.doWithFallbacks(context.Background(), req, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.resp.Body.Close()
	if upstream.providerName != "anthropic" {
		t.Fatalf("provider = %q, want Anthropic primary", upstream.providerName)
	}
	if anthropicCalls.Load() != 1 || kimiCalls.Load() != 0 {
		t.Fatalf("calls: anthropic=%d kimi=%d, want 1/0", anthropicCalls.Load(), kimiCalls.Load())
	}
	if family := server.conversationFamily(stickyKey); family != "anthropic" {
		t.Fatalf("conversation family = %q, want anthropic", family)
	}
}

func TestExplicitClaudePrimaryUsesFiveMinuteFamilyPin(t *testing.T) {
	server := &proxyServer{
		cfg: config{
			DefaultProvider: "anthropic",
			ClaudeUsage: claudeUsageConfig{
				Provider: "anthropic", EligibleUpstreams: []string{"claude-opus-4-8"},
			},
			AdaptiveRouting: adaptiveRoutingConfig{StickySeconds: 300},
		},
		adaptive: adaptiveState{sticky: map[string]stickyRoute{}},
	}
	candidate := modelConfig{Provider: "anthropic", Requested: "claude-opus-4-8"}
	if !server.claudeSubscriptionCandidate(candidate) {
		t.Fatal("direct Opus primary was not recognized as Claude subscription traffic")
	}
	started := time.Now()
	server.forceConversationFamily("conversation", "anthropic")
	remaining := time.Until(server.adaptive.sticky["conversation"].FamilyUntil)
	if remaining < 295*time.Second || remaining > 305*time.Second || time.Since(started) > time.Second {
		t.Fatalf("family affinity remaining = %s, want approximately 5m", remaining)
	}
}

func TestDirectGLMOverridesStaleAnthropicConversationAffinity(t *testing.T) {
	t.Setenv("TEST_ANTHROPIC_TOKEN", "test-token")
	var glmCalls atomic.Int32
	glm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		glmCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"glm-5.3-flash","content":[]}`))
	}))
	defer glm.Close()

	var anthropicCalls atomic.Int32
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anthropicCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","model":"claude-sonnet-5","content":[]}`))
	}))
	defer anthropic.Close()

	server, err := newProxyServer(config{
		DefaultProvider: "glm",
		Providers: map[string]providerConfig{
			"glm":       {BaseURL: glm.URL},
			"anthropic": {BaseURL: anthropic.URL, AuthTokenEnv: "TEST_ANTHROPIC_TOKEN", AuthHeader: "Authorization", AuthPrefix: "Bearer "},
		},
		AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, StickySeconds: 300,
			ProviderTiers: map[string]int{"glm": 0, "anthropic": 0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Provider: "glm", Upstream: "glm-5.3-flash", Requested: "sonnet",
		Fallbacks: []modelConfig{{Provider: "anthropic", Upstream: "claude-sonnet-5"}},
	}
	body := []byte(`{"model":"sonnet","messages":[{"role":"user","content":"hello"}]}`)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	stickyKey := routingStickyKey(selected, payload)
	server.adaptive.sticky[stickyKey] = stickyRoute{
		Key: "anthropic|claude-sonnet-5", Family: "anthropic",
		Until: time.Now().Add(time.Hour), FamilyUntil: time.Now().Add(24 * time.Hour),
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	upstream, err := server.doWithFallbacks(context.Background(), req, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.resp.Body.Close()
	if upstream.providerName != "glm" {
		t.Fatalf("provider = %q, want explicit GLM primary", upstream.providerName)
	}
	if glmCalls.Load() != 1 || anthropicCalls.Load() != 0 {
		t.Fatalf("calls: glm=%d anthropic=%d, want 1/0", glmCalls.Load(), anthropicCalls.Load())
	}
}

func TestRequestOAuthBearerRejectsAPIKeyBilling(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("x-api-key", "sk-ant-api03-test-paid-key")
	if _, err := requestOAuthBearer(req); err == nil {
		t.Fatal("ordinary Anthropic API key accepted as Claude subscription OAuth")
	}
}

func TestConfiguredProviderAuthStripsClientCredentials(t *testing.T) {
	t.Setenv("TEST_PROVIDER_TOKEN", "provider-token")
	server := &proxyServer{cfg: config{Providers: map[string]providerConfig{
		"gateway": {AuthTokenEnv: "TEST_PROVIDER_TOKEN", AuthHeader: "Authorization", AuthPrefix: "Bearer "},
	}}}
	headers := http.Header{
		"Authorization": []string{"Bearer sk-ant-oat-test-client"},
		"x-api-key":     []string{"sk-ant-api03-test-client"},
	}
	if err := server.applyProviderHeaders("gateway", headers); err != nil {
		t.Fatal(err)
	}
	if headers.Get("Authorization") != "Bearer provider-token" || headers.Get("x-api-key") != "" {
		t.Fatalf("provider headers = %#v, want only configured provider auth", headers)
	}
}

func TestClaudeUsageGateFallsBackToFreshCLICache(t *testing.T) {
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer usage.Close()
	cachePath := filepath.Join(t.TempDir(), ".claude.json")
	cached := `{"cachedUsageUtilization":{"fetchedAtMs":` + strconv.FormatInt(time.Now().UnixMilli(), 10) + `,"utilization":{"five_hour":{"utilization":10,"resets_at":"2026-08-26T12:49:59Z"},"seven_day":{"utilization":20,"resets_at":"2026-08-28T09:59:59Z"}}}}`
	if err := os.WriteFile(cachePath, []byte(cached), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5"},
			FiveHourThresholdPct: 60, SevenDayThresholdPct: 60, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
			ListenerProfiles: map[string]claudeUsageProfile{"48104": {Name: "accountA", CachePath: cachePath}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer sk-ant-oat-test-accountA-token")
	allowed, reason := server.claudeSubscriptionAllowed(context.Background(), req, modelConfig{Provider: "anthropic", Upstream: "claude-sonnet-5"})
	if !allowed || reason != "" {
		t.Fatalf("CLI cache gate = %v %q, want allowed", allowed, reason)
	}
	status := server.claudeUsageStatus()
	profiles := status["profiles"].(map[string]any)
	if profiles["accountA"].(map[string]any)["source"] != "claude-cli-cache" {
		t.Fatalf("status = %#v, want CLI cache source", profiles["accountA"])
	}
}

func TestClaudeUsageStatusPrefersFreshOAuthAPIOverCLICache(t *testing.T) {
	const service = "test-api-first-usage"
	claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: "sk-ant-oat-test-api-first", fetchedAt: time.Now()})
	t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })

	var apiCalls atomic.Int32
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		_, _ = w.Write([]byte(`{"five_hour":{"utilization":12},"seven_day":{"utilization":18}}`))
	}))
	defer usage.Close()
	cachePath := filepath.Join(t.TempDir(), ".claude.json")
	cached := `{"cachedUsageUtilization":{"fetchedAtMs":` + strconv.FormatInt(time.Now().UnixMilli(), 10) + `,"utilization":{"five_hour":{"utilization":99},"seven_day":{"utilization":99}}}}`
	if err := os.WriteFile(cachePath, []byte(cached), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := claudeUsageProfile{Name: "teamA", CachePath: cachePath, CredentialsService: service}
	snapshot, err := server.claudeUsageStatusSnapshot(profile)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Source != "oauth-api" || snapshot.FiveHour.Utilization != 12 || snapshot.SevenDay.Utilization != 18 {
		t.Fatalf("snapshot = %#v, want fresh OAuth API values instead of CLI 99%%", snapshot)
	}
	if apiCalls.Load() != 1 {
		t.Fatalf("OAuth API calls = %d, want 1", apiCalls.Load())
	}
}

func TestClaudeUsageStatusUsesNewestTokenSnapshotForSharedProfile(t *testing.T) {
	profileName := "teamA"
	older := claudeUsageSnapshot{
		FetchedAt: time.Now().Add(-time.Minute), Profile: profileName,
		FiveHour: claudeUsageWindow{Utilization: 81}, SevenDay: claudeUsageWindow{Utilization: 82}, Source: "oauth-api",
	}
	newer := claudeUsageSnapshot{
		FetchedAt: time.Now(), Profile: profileName,
		FiveHour: claudeUsageWindow{Utilization: 21}, SevenDay: claudeUsageWindow{Utilization: 22}, Source: "oauth-api",
	}
	server := &proxyServer{
		cfg: config{ClaudeUsage: claudeUsageConfig{FiveHourThresholdPct: 95, SevenDayThresholdPct: 95}},
		claudeUse: claudeUsageCache{snapshots: map[string]claudeUsageSnapshot{
			claudeTokenKey("Bearer rotated-token"): older,
			claudeTokenKey("Bearer current-token"): newer,
		}},
	}

	// Repeated reads make the contract independent of Go map iteration order.
	for iteration := 0; iteration < 64; iteration++ {
		status := server.claudeUsageStatus()
		profiles := status["profiles"].(map[string]any)
		entry := profiles[profileName].(map[string]any)
		if entry["fetchedAt"] != newer.FetchedAt.UTC().Format(time.RFC3339) ||
			entry["fiveHourUsagePct"] != newer.FiveHour.Utilization ||
			entry["sevenDayUsagePct"] != newer.SevenDay.Utilization {
			t.Fatalf("iteration %d profile status = %#v, want newest token snapshot %#v", iteration, entry, newer)
		}
	}
}

func TestClaudeUsageStatusIgnoresExpiredTokenSnapshot(t *testing.T) {
	server := &proxyServer{
		cfg: config{ClaudeUsage: claudeUsageConfig{
			FiveHourThresholdPct: 95, SevenDayThresholdPct: 95, StaleTTLSeconds: 1800,
		}},
		claudeUse: claudeUsageCache{snapshots: map[string]claudeUsageSnapshot{
			claudeTokenKey("Bearer expired-token"): {
				FetchedAt: time.Now().Add(-6 * time.Hour), Profile: "teamB",
				FiveHour: claudeUsageWindow{Utilization: 10}, SevenDay: claudeUsageWindow{Utilization: 20}, Source: "oauth-api",
			},
		}},
	}
	profiles := server.claudeUsageStatus()["profiles"].(map[string]any)
	if _, exists := profiles["teamB"]; exists {
		t.Fatalf("expired token snapshot remained visible: %#v", profiles)
	}
}

func TestClaudeUsageStatusFallsBackToFreshCLICacheAfterOAuthAPIFailure(t *testing.T) {
	const service = "test-api-failure-fresh-cli"
	const token = "sk-ant-oat-test-api-failure-fresh-cli"
	claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: token, fetchedAt: time.Now()})
	t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })

	var apiCalls atomic.Int32
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer usage.Close()
	cachePath := filepath.Join(t.TempDir(), ".claude.json")
	cached := `{"cachedUsageUtilization":{"fetchedAtMs":` + strconv.FormatInt(time.Now().UnixMilli(), 10) + `,"utilization":{"five_hour":{"utilization":14},"seven_day":{"utilization":24}}}}`
	if err := os.WriteFile(cachePath, []byte(cached), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := claudeUsageProfile{Name: "teamA", CachePath: cachePath, CredentialsService: service}
	server.claudeUse.snapshots[claudeTokenKey("Bearer "+token)] = claudeUsageSnapshot{
		FetchedAt: time.Now().Add(-10 * time.Minute), Profile: profile.Name,
		FiveHour: claudeUsageWindow{Utilization: 88}, SevenDay: claudeUsageWindow{Utilization: 89}, Source: "oauth-api",
	}

	snapshot, err := server.claudeUsageStatusSnapshot(profile)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Source != "claude-cli-cache" || snapshot.FiveHour.Utilization != 14 || snapshot.SevenDay.Utilization != 24 {
		t.Fatalf("snapshot = %#v, want fresh CLI values after OAuth API failure", snapshot)
	}
	if apiCalls.Load() != 1 {
		t.Fatalf("OAuth API calls = %d, want 1", apiCalls.Load())
	}
}

func TestClaudeUsageStatusServesLastGoodSnapshotDuringAPIBlip(t *testing.T) {
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary outage", http.StatusServiceUnavailable)
	}))
	defer usage.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	auth := "Bearer sk-ant-oat-test-last-good"
	profile := claudeUsageProfile{Name: "last-good", CachePath: filepath.Join(t.TempDir(), "missing.json")}
	server.claudeUse.snapshots[claudeTokenKey(auth)] = claudeUsageSnapshot{
		FetchedAt: time.Now().Add(-10 * time.Minute), Profile: profile.Name,
		FiveHour: claudeUsageWindow{Utilization: 42}, SevenDay: claudeUsageWindow{Utilization: 51}, Source: "oauth-api",
	}
	snapshot, err := server.claudeUsageSnapshotWithAuth(context.Background(), auth, profile)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Source != "oauth-api-stale" || snapshot.FiveHour.Utilization != 42 {
		t.Fatalf("snapshot = %#v, want stale last-good", snapshot)
	}
}

func TestClaudeUsageNearThresholdUsesShortCacheTTL(t *testing.T) {
	var calls atomic.Int32
	usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"five_hour":{"utilization":20},"seven_day":{"utilization":30}}`))
	}))
	defer usage.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := claudeUsageProfile{Name: "near-limit", CachePath: filepath.Join(t.TempDir(), "missing.json")}
	auth := "Bearer sk-ant-oat-test-near-limit"
	server.claudeUse.snapshots[claudeTokenKey(auth)] = claudeUsageSnapshot{
		FetchedAt: time.Now().Add(-31 * time.Second), Profile: profile.Name,
		FiveHour: claudeUsageWindow{Utilization: 91}, SevenDay: claudeUsageWindow{Utilization: 40}, Source: "oauth-api",
	}
	snapshot, err := server.claudeUsageSnapshotWithAuth(context.Background(), auth, profile)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || snapshot.FiveHour.Utilization != 20 {
		t.Fatalf("calls=%d snapshot=%#v, want near-limit refresh after 30s", calls.Load(), snapshot)
	}
}

func TestClaudeUsageStatusReturnsOAuthAPIErrorWhenCLICacheUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		writeCache bool
	}{
		{name: "missing"},
		{name: "stale", writeCache: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := "test-api-failure-cli-" + tc.name
			claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: "sk-ant-oat-" + tc.name, fetchedAt: time.Now()})
			t.Cleanup(func() { claudeOAuthCredentials.Delete(service) })

			usage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "rate limited", http.StatusTooManyRequests)
			}))
			defer usage.Close()
			cachePath := filepath.Join(t.TempDir(), ".claude.json")
			if tc.writeCache {
				cached := `{"cachedUsageUtilization":{"fetchedAtMs":` + strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10) + `,"utilization":{"five_hour":{"utilization":14},"seven_day":{"utilization":24}}}}`
				if err := os.WriteFile(cachePath, []byte(cached), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			server, err := newProxyServer(config{
				Providers: map[string]providerConfig{"anthropic": {BaseURL: usage.URL}},
				ClaudeUsage: claudeUsageConfig{
					Provider: "anthropic", FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
					CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			profile := claudeUsageProfile{Name: "teamA", CachePath: cachePath, CredentialsService: service}
			snapshot, err := server.claudeUsageStatusSnapshot(profile)
			if err == nil || !strings.Contains(err.Error(), "Claude usage endpoint returned status 429") {
				t.Fatalf("snapshot = %#v error = %v, want authoritative OAuth API 429 error", snapshot, err)
			}
		})
	}
}

func TestDoWithFallbacksSkipsReservedClaudeSubscriptionBeforePayGo(t *testing.T) {
	messageCalls := 0
	claude := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/oauth/usage":
			_, _ = w.Write([]byte(`{"five_hour":{"utilization":20},"seven_day":{"utilization":70}}`))
		case "/v1/messages":
			messageCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"claude"}]}`))
		default:
			t.Fatalf("unexpected Claude path %q", r.URL.Path)
		}
	}))
	defer claude.Close()
	payGoCalls := 0
	payGo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payGoCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"paygo"}]}`))
	}))
	defer payGo.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"anthropic": {BaseURL: claude.URL},
			"paygo":     {BaseURL: payGo.URL},
		},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5"},
			FiveHourThresholdPct: 60, SevenDayThresholdPct: 60, CacheTTLSeconds: 300,
			StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Requested: "worker[1m]", Provider: "anthropic", Upstream: "claude-sonnet-5",
		Fallbacks: []modelConfig{{Provider: "paygo", Upstream: "deepseek"}},
	}
	body := []byte(`{"model":"worker[1m]","messages":[{"role":"user","content":"work"}]}`)
	payload := map[string]any{"model": "worker[1m]", "messages": []any{map[string]any{"role": "user", "content": "work"}}}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer sk-ant-oat-test-accountA-token")
	upstream, err := server.doWithFallbacks(context.Background(), req, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	_ = upstream.resp.Body.Close()
	if messageCalls != 0 || payGoCalls != 1 || upstream.providerName != "paygo" {
		t.Fatalf("Claude calls=%d paygo calls=%d provider=%q, want 0 1 paygo", messageCalls, payGoCalls, upstream.providerName)
	}
}

func TestAdaptiveRoutingKeepsPayGoLast(t *testing.T) {
	server := &proxyServer{
		cfg: config{AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, MinSamples: 4, RefreshSeconds: 60, StickySeconds: 300, ExploreEvery: 1000,
			StabilityWeight: 8, LatencyWeight: 1, TPSWeight: 0.35, UsageWeight: 4, SwitchMargin: 8,
			ProviderTiers: map[string]int{"opencode": 0, "vercel": 100},
			QualityBonus:  map[string]float64{}, UsageCost: map[string]float64{},
		}},
		adaptive: adaptiveState{
			performance: map[string]routePerformance{
				"opencode|deepseek": {Samples: 20, SuccessRate: 0.9, P50HeaderMS: 3000, P50TPS: 25},
				"vercel|deepseek":   {Samples: 20, SuccessRate: 1, P50HeaderMS: 100, P50TPS: 100},
			},
			refreshedAt: time.Now(), sticky: map[string]stickyRoute{},
		},
		providerStates: map[string]providerRuntimeState{},
	}
	candidates := []modelConfig{{Provider: "opencode", Upstream: "deepseek"}, {Provider: "vercel", Upstream: "deepseek"}}
	ranked := server.rankCandidates(modelConfig{Requested: "sonnet"}, candidates)
	if ranked[0].Provider != "opencode" {
		t.Fatalf("first provider = %q, want subscription opencode", ranked[0].Provider)
	}
}

func TestThinkingHistoryDetection(t *testing.T) {
	payload := func(signatures ...string) map[string]any {
		blocks := make([]any, 0, len(signatures))
		for _, signature := range signatures {
			blocks = append(blocks, map[string]any{"type": "thinking", "thinking": "reason", "signature": signature})
		}
		return map[string]any{"messages": []any{map[string]any{"role": "assistant", "content": blocks}}}
	}
	if got := detectThinkingHistory(payload("a646e152f6c5463a82129f0e")); got != thinkingHistoryNonAnthropic {
		t.Fatalf("short signature history = %v, want non-Anthropic", got)
	}
	if got := detectThinkingHistory(payload(strings.Repeat("A", 256))); got != thinkingHistoryAnthropic {
		t.Fatalf("long signature history = %v, want Anthropic", got)
	}
	if got := detectThinkingHistory(payload("short", strings.Repeat("B", 256))); got != thinkingHistoryMixed {
		t.Fatalf("mixed signature history = %v, want mixed", got)
	}
}

func TestBuildAttemptBodySanitizesIncompatibleThinkingAndPreservesTools(t *testing.T) {
	server := &proxyServer{cfg: config{Providers: map[string]providerConfig{
		"anthropic": {}, "gateway": {},
	}}}
	bodyFor := func(signature string) []byte {
		return []byte(fmt.Sprintf(`{"model":"worker","messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"reason","signature":%q},{"type":"tool_use","id":"toolu_1","name":"Read","input":{"path":"a"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"}]}]}`, signature))
	}
	assertBody := func(t *testing.T, body []byte, wantThinking bool) {
		t.Helper()
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(decoded)
		text := string(encoded)
		if strings.Contains(text, `"type":"thinking"`) != wantThinking {
			t.Fatalf("thinking present = %v, want %v: %s", strings.Contains(text, `"type":"thinking"`), wantThinking, text)
		}
		if !strings.Contains(text, `"type":"tool_use"`) || !strings.Contains(text, `"type":"tool_result"`) {
			t.Fatalf("tool state lost: %s", text)
		}
	}

	shortBody := bodyFor("a646e152f6c5463a82129f0e")
	var shortPayload map[string]any
	_ = json.Unmarshal(shortBody, &shortPayload)
	toAnthropic, err := server.buildAttemptBody(shortBody, shortPayload, modelConfig{Upstream: "claude-sonnet-5"}, "anthropic", "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	assertBody(t, toAnthropic, false)

	longBody := bodyFor(strings.Repeat("A", 256))
	var longPayload map[string]any
	_ = json.Unmarshal(longBody, &longPayload)
	toAnthropic, err = server.buildAttemptBody(longBody, longPayload, modelConfig{Upstream: "claude-sonnet-5"}, "anthropic", "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	assertBody(t, toAnthropic, true)
	toGateway, err := server.buildAttemptBody(longBody, longPayload, modelConfig{Upstream: "zai/glm-5.3-flash"}, "gateway", "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	assertBody(t, toGateway, false)
}

func TestNormalizeRequestRepairsCrossModelToolIDs(t *testing.T) {
	server := &proxyServer{cfg: config{Models: map[string]modelConfig{
		"fable": {Provider: "anthropic"},
	}}}
	body := []byte(`{"model":"fable","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"Bash:157","name":"Bash","input":{}},{"type":"server_tool_use","id":"srvtoolu_bad-id","name":"search","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"Bash:157","content":"ok"},{"type":"tool_result","tool_use_id":"srvtoolu_bad-id","content":"ok"}]}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	normalizedBody, payload, _, err := server.normalizeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(normalizedBody, []byte("Bash:157")) || bytes.Contains(normalizedBody, []byte("srvtoolu_bad-id")) {
		t.Fatalf("invalid tool IDs survived normalization: %s", normalizedBody)
	}
	messages := payload["messages"].([]any)
	assistantBlocks := messages[0].(map[string]any)["content"].([]any)
	resultBlocks := messages[1].(map[string]any)["content"].([]any)
	toolID := assistantBlocks[0].(map[string]any)["id"].(string)
	toolResultID := resultBlocks[0].(map[string]any)["tool_use_id"].(string)
	serverToolID := assistantBlocks[1].(map[string]any)["id"].(string)
	serverResultID := resultBlocks[1].(map[string]any)["tool_use_id"].(string)
	if toolID != toolResultID || !validToolUseID(toolID, false) {
		t.Fatalf("tool pair = %q/%q", toolID, toolResultID)
	}
	if serverToolID != serverResultID || !validToolUseID(serverToolID, true) {
		t.Fatalf("server tool pair = %q/%q", serverToolID, serverResultID)
	}
}

func TestProxyRejectsUnknownModelBeforeUpstreamIO(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		DefaultProvider: "provider",
		Providers:       map[string]providerConfig{"provider": {BaseURL: upstream.URL}},
		Models:          map[string]modelConfig{"known": {Provider: "provider", Upstream: "known"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"unknown-model","messages":[]}`))
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "unknown model") {
		t.Fatalf("status=%d body=%s, want local unknown-model 400", recorder.Code, recorder.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("upstream calls=%d, want zero", calls.Load())
	}
}

func TestBuildAttemptBodyDropsTemperatureOnlyForSonnet5(t *testing.T) {
	server := &proxyServer{cfg: config{
		Providers:   map[string]providerConfig{"anthropic": {}},
		ClaudeUsage: claudeUsageConfig{Provider: "anthropic"},
	}}
	body := []byte(`{"model":"sonnet","temperature":0.7,"messages":[]}`)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	encoded, err := server.buildAttemptBody(body, payload, modelConfig{Upstream: "claude-sonnet-5"}, "anthropic", "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if _, exists := got["temperature"]; exists {
		t.Fatalf("Sonnet payload retained temperature: %s", encoded)
	}
	if got["model"] != "claude-sonnet-5" || payload["model"] != "sonnet" {
		t.Fatalf("candidate model=%v original model=%v", got["model"], payload["model"])
	}
}

func TestExplicitDirectFamilyUsesCostPhases(t *testing.T) {
	server := &proxyServer{
		cfg: config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}, AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, MinSamples: 4, RefreshSeconds: 60, ExploreEvery: 1000,
			ProviderTiers: map[string]int{"subscription": 0, "xai-oauth": 90, "paygo": 100, "other-sub": 10},
		}},
		adaptive:       adaptiveState{performance: map[string]routePerformance{}, refreshedAt: time.Now(), sticky: map[string]stickyRoute{}},
		providerStates: map[string]providerRuntimeState{},
	}
	selected := modelConfig{Requested: "deepseek-v4-flash[1m]", Provider: "subscription", Upstream: "deepseek-v4-flash"}
	candidates := []modelConfig{
		{Provider: "paygo", Upstream: "deepseek-v4-flash"},
		{Provider: "other-sub", Upstream: "glm-5.3-flash"},
		{Provider: "xai-oauth", Upstream: "grok-build-0.1"},
		selected,
	}
	ranked := server.rankCandidates(selected, candidates)
	got := []string{ranked[0].Provider, ranked[1].Provider, ranked[2].Provider, ranked[3].Provider}
	want := []string{"subscription", "xai-oauth", "paygo", "other-sub"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("providers=%v, want %v", got, want)
	}
}

func TestRouteIncompatibilityFallsBackWithoutProviderQuarantine(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"temperature is deprecated for this model"}}`))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer fallback.Close()
	server, err := newProxyServer(config{
		Providers:  map[string]providerConfig{"primary": {BaseURL: primary.URL}, "fallback": {BaseURL: fallback.URL}},
		Quarantine: providerQuarantineConfig{Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300, MaxSeconds: 300},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{Requested: "glm", Provider: "primary", Upstream: "glm", Fallbacks: []modelConfig{{Provider: "fallback", Upstream: "glm"}}}
	body := []byte(`{"model":"glm","messages":[]}`)
	response, err := server.doWithFallbacks(context.Background(), httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body)), body, map[string]any{"model": "glm"}, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer response.resp.Body.Close()
	if response.providerName != "fallback" {
		t.Fatalf("provider=%q, want fallback", response.providerName)
	}
	if blocked, state := server.providerBlocked("primary"); blocked || state.InstabilitySignals != 0 {
		t.Fatalf("route incompatibility poisoned provider: %#v", state)
	}
}

func TestRoutingStickyKeyIsConversationScopedAndStable(t *testing.T) {
	selected := modelConfig{Requested: "sonnet"}
	payload := func(first, second string) map[string]any {
		return map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": first},
			map[string]any{"role": "assistant", "content": "ok"},
			map[string]any{"role": "user", "content": second},
		}}
	}
	first := routingStickyKey(selected, payload("mission-a", "continue"))
	if got := routingStickyKey(selected, payload("mission-a", "new turn")); got != first {
		t.Fatalf("conversation key changed across appended turns: %q != %q", got, first)
	}
	if got := routingStickyKey(selected, payload("mission-b", "continue")); got == first {
		t.Fatalf("different conversations shared sticky key %q", got)
	}
}

func TestAdaptiveRoutingKeepsEstablishedModelFamilyBeforePayGoAlternatives(t *testing.T) {
	server := &proxyServer{
		cfg: config{AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, MinSamples: 4, RefreshSeconds: 60, StickySeconds: 300, ExploreEvery: 20,
			StabilityWeight: 8, LatencyWeight: 1, TPSWeight: 0.35, UsageWeight: 4,
			ProviderTiers: map[string]int{"bigmodel": 0, "ollama": 0, "vercel-glm": 100},
		}},
		adaptive: adaptiveState{
			performance: map[string]routePerformance{
				"bigmodel|glm-5.3-flash":         {Samples: 10, SuccessRate: 1},
				"ollama|deepseek-v4-flash:cloud": {Samples: 10, SuccessRate: 1},
				"vercel-glm|zai/glm-5.3-flash":   {Samples: 10, SuccessRate: 1},
			},
			refreshedAt: time.Now(), sticky: map[string]stickyRoute{"conversation": {Family: "glm", Until: time.Now().Add(time.Minute)}},
		},
		providerStates: map[string]providerRuntimeState{},
	}
	candidates := []modelConfig{
		{Provider: "bigmodel", Upstream: "glm-5.3-flash"},
		{Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"},
		{Provider: "vercel-glm", Upstream: "zai/glm-5.3-flash"},
	}
	ranked := server.rankCandidatesForRequest(modelConfig{Requested: "sonnet"}, candidates, "conversation", false, thinkingHistoryNonAnthropic)
	if got := []string{ranked[0].Provider, ranked[1].Provider, ranked[2].Provider}; !reflect.DeepEqual(got, []string{"bigmodel", "vercel-glm", "ollama"}) {
		t.Fatalf("providers = %#v, want same-family GLM chain before DeepSeek", got)
	}
}

func TestSignedHistoryAnchorsSameFamilyOnFirstRequestAfterRestart(t *testing.T) {
	server := &proxyServer{
		cfg: config{AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, MinSamples: 4, RefreshSeconds: 60, StickySeconds: 300, ExploreEvery: 20,
			StabilityWeight: 8, LatencyWeight: 1, TPSWeight: 0.35, UsageWeight: 4,
			ProviderTiers: map[string]int{"bigmodel": 0, "anthropic": 0, "cline": 10, "vercel-glm": 100},
		}},
		adaptive: adaptiveState{
			performance: map[string]routePerformance{
				"bigmodel|glm-5.3-flash":       {Samples: 10, SuccessRate: 1},
				"anthropic|claude-sonnet-5":    {Samples: 10, SuccessRate: 1},
				"cline|deepseek-v4-flash":      {Samples: 10, SuccessRate: 1},
				"vercel-glm|zai/glm-5.3-flash": {Samples: 10, SuccessRate: 1},
			},
			refreshedAt: time.Now(), sticky: map[string]stickyRoute{},
		},
		providerStates: map[string]providerRuntimeState{},
	}
	candidates := []modelConfig{
		{Provider: "bigmodel", Upstream: "glm-5.3-flash"},
		{Provider: "anthropic", Upstream: "claude-sonnet-5"},
		{Provider: "cline", Upstream: "deepseek-v4-flash"},
		{Provider: "vercel-glm", Upstream: "zai/glm-5.3-flash"},
	}
	ranked := server.rankCandidatesForRequest(modelConfig{Requested: "sonnet"}, candidates, "new-after-restart", false, thinkingHistoryNonAnthropic)
	if got := []string{ranked[0].Provider, ranked[1].Provider}; !reflect.DeepEqual(got, []string{"bigmodel", "vercel-glm"}) {
		t.Fatalf("providers = %#v, want immediate same-family GLM fallback", got)
	}
}

func TestThinkingSignatureMismatchFallsBackWithoutQuarantine(t *testing.T) {
	circuitBreakers.Delete("primary|glm")
	t.Cleanup(func() { circuitBreakers.Delete("primary|glm") })
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Invalid signature in thinking block"}}`))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer fallback.Close()
	server, err := newProxyServer(config{
		DefaultProvider: "primary",
		Providers:       map[string]providerConfig{"primary": {BaseURL: primary.URL}, "fallback": {BaseURL: fallback.URL}},
		Quarantine:      providerQuarantineConfig{Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300, MaxSeconds: 300},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{Requested: "worker", Provider: "primary", Upstream: "glm", Fallbacks: []modelConfig{{Provider: "fallback", Upstream: "glm"}}}
	body := []byte(`{"model":"worker","messages":[{"role":"user","content":"work"}]}`)
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	response, err := server.doWithFallbacks(context.Background(), req, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.resp.Body.Close()
	if primaryCalls != 1 || fallbackCalls != 1 || response.providerName != "fallback" {
		t.Fatalf("calls primary=%d fallback=%d winner=%q", primaryCalls, fallbackCalls, response.providerName)
	}
	if blocked, state := server.providerBlocked("primary"); blocked || state.InstabilitySignals != 0 {
		t.Fatalf("signature compatibility error quarantined provider: %#v", state)
	}
}

func TestAdaptiveWorkerCanPreferFasterDeepSeekOverGLMQualityBonus(t *testing.T) {
	server := &proxyServer{
		cfg: config{AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, MinSamples: 4, RefreshSeconds: 60, StickySeconds: 300, ExploreEvery: 1000,
			StabilityWeight: 8, LatencyWeight: 1, TPSWeight: 0.35, UsageWeight: 4, SwitchMargin: 8,
			ProviderTiers: map[string]int{"bigmodel": 0, "opencode": 0},
			QualityBonus:  map[string]float64{"bigmodel|glm": 8},
			UsageCost:     map[string]float64{"bigmodel|glm": 1, "opencode|deepseek": 0.35},
		}},
		adaptive: adaptiveState{
			performance: map[string]routePerformance{
				"bigmodel|glm":      {Samples: 20, SuccessRate: 0.98, P50HeaderMS: 10000, P50TPS: 10},
				"opencode|deepseek": {Samples: 20, SuccessRate: 0.98, P50HeaderMS: 500, P50TPS: 80},
			},
			refreshedAt: time.Now(), sticky: map[string]stickyRoute{},
		},
		providerStates: map[string]providerRuntimeState{},
	}
	candidates := []modelConfig{{Provider: "bigmodel", Upstream: "glm"}, {Provider: "opencode", Upstream: "deepseek"}}
	ranked := server.rankCandidates(modelConfig{Requested: "sonnet"}, candidates)
	if ranked[0].Upstream != "deepseek" {
		t.Fatalf("first upstream = %q, want faster deepseek", ranked[0].Upstream)
	}
}

func TestRequestNeedsStructuredOutputCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]any
		want    bool
	}{
		{name: "nil", payload: nil, want: false},
		{name: "output config", payload: map[string]any{"output_config": map[string]any{"format": "json_schema"}}, want: true},
		{name: "offered tool", payload: map[string]any{"tools": []any{map[string]any{"name": "StructuredOutput"}}}, want: true},
		{name: "enforcement turn", payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "[structured-output-enforce] You MUST call the StructuredOutput tool"},
		}}, want: true},
		{name: "tool reference", payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "content": `[{"type":"tool_reference","tool_name":"StructuredOutput"}]`}}},
		}}, want: true},
		{name: "old history mention ignored", payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "StructuredOutput"},
			map[string]any{"role": "assistant", "content": "done"},
			map[string]any{"role": "user", "content": "continue normal work"},
		}}, want: false},
		{name: "unrelated", payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "summarize this file"},
		}}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := requestNeedsStructuredOutputCompatibility(test.payload); got != test.want {
				t.Fatalf("requestNeedsStructuredOutputCompatibility() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestStructuredOutputCompatibleCandidates(t *testing.T) {
	candidates := []modelConfig{
		{Provider: "ollama", Upstream: "glm-5.1:cloud"},
		{Provider: "bigmodel", Upstream: "glm-5.1"},
		{Provider: "bigmodel", Upstream: "glm-5.3-flash"},
		{Provider: "opencode-go", Upstream: "glm-5.3-flash"},
		{Provider: "ollama", Upstream: "deepseek-v4-flash:cloud"},
		{Provider: "opencode", Upstream: "deepseek-v4-flash"},
		{Provider: "anthropic", Upstream: "claude-sonnet-5"},
		{Provider: "anthropic", Upstream: "claude-opus-4-8"},
	}
	filtered, changed := structuredOutputCompatibleCandidates(candidates)
	if !changed || len(filtered) != 6 {
		t.Fatalf("filtered = %#v changed=%v, want GLM 5.3 Flash, DeepSeek, and native Claude models", filtered, changed)
	}
	for _, candidate := range filtered {
		if !strings.Contains(candidate.Upstream, "glm-5.3-flash") && !strings.Contains(candidate.Upstream, "deepseek") && candidate.Provider != "anthropic" {
			t.Fatalf("incompatible candidate remains: %#v", candidate)
		}
	}

	glmOnly := candidates[:2]
	filtered, changed = structuredOutputCompatibleCandidates(glmOnly)
	if changed || len(filtered) != len(glmOnly) {
		t.Fatalf("GLM-only route changed without a DeepSeek fallback: %#v changed=%v", filtered, changed)
	}
}

func TestDoWithFallbacksRoutesStructuredOutputTurnDirectlyToDeepSeek(t *testing.T) {
	glmCalls := 0
	glm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		glmCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"glm"}],"stop_reason":"end_turn"}`))
	}))
	defer glm.Close()

	deepSeekCalls := 0
	deepSeek := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deepSeekCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"deepseek"}],"stop_reason":"end_turn"}`))
	}))
	defer deepSeek.Close()

	server, err := newProxyServer(config{
		DefaultProvider: "glm",
		Providers: map[string]providerConfig{
			"glm":      {BaseURL: glm.URL},
			"deepseek": {BaseURL: deepSeek.URL},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Requested: "sonnet", Provider: "glm", Upstream: "glm-5.1",
		Fallbacks: []modelConfig{{Provider: "deepseek", Upstream: "deepseek-v4-flash"}},
	}
	payload := map[string]any{
		"model":    "sonnet",
		"messages": []any{map[string]any{"role": "user", "content": "[structured-output-enforce] call StructuredOutput"}},
	}
	body := []byte(`{"model":"sonnet","messages":[{"role":"user","content":"[structured-output-enforce] call StructuredOutput"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
	upstream, err := server.doWithFallbacks(context.Background(), req, body, payload, selected)
	if err != nil {
		t.Fatal(err)
	}
	_ = upstream.resp.Body.Close()
	if glmCalls != 0 || deepSeekCalls != 1 {
		t.Fatalf("glmCalls=%d deepSeekCalls=%d, want 0 and 1", glmCalls, deepSeekCalls)
	}
	if upstream.model.Upstream != "deepseek-v4-flash" {
		t.Fatalf("upstream = %q, want DeepSeek", upstream.model.Upstream)
	}
}

func assertCandidateTiers(t *testing.T, candidates []modelConfig, expected map[string]int) {
	t.Helper()
	if len(candidates) != len(expected) {
		t.Fatalf("candidate count = %d, want %d: %#v", len(candidates), len(expected), candidates)
	}
	for _, candidate := range candidates {
		key := candidate.Provider + "|" + candidate.Upstream
		want, ok := expected[key]
		if !ok {
			t.Fatalf("unexpected candidate %q", key)
		}
		if candidate.Tier == nil || *candidate.Tier != want {
			t.Fatalf("candidate %q tier = %v, want %d", key, candidate.Tier, want)
		}
	}
}

func TestCandidateTierOverridesGlobalProviderTier(t *testing.T) {
	tier := 10
	server := &proxyServer{cfg: config{AdaptiveRouting: adaptiveRoutingConfig{ProviderTiers: map[string]int{"opencode": 0}}}}
	if got := server.candidateTier(modelConfig{Tier: &tier}, "opencode"); got != 10 {
		t.Fatalf("candidate tier = %d, want 10", got)
	}
}

func TestRouteScoreUsesSharedProviderUsageHeadroom(t *testing.T) {
	server := &proxyServer{
		cfg: config{
			OllamaUsage: ollamaUsageConfig{Provider: "ollama"},
			AdaptiveRouting: adaptiveRoutingConfig{
				MinSamples: 4, StabilityWeight: 8, LatencyWeight: 1, TPSWeight: 0.35,
				UsageWeight: 4, HeadroomWeight: 20,
				QualityBonus: map[string]float64{}, UsageCost: map[string]float64{},
			},
		},
		providerStates: map[string]providerRuntimeState{
			"bigmodel": {RateLimitInfo: map[string]string{"Anthropic-Ratelimit-Unified-5h-Utilization": "0.20"}},
		},
	}
	server.ollamaUse = ollamaUsageCache{hasValue: true, snapshot: ollamaUsageSnapshot{
		Session: ollamaUsageWindow{Usage: 0.80}, Weekly: ollamaUsageWindow{Usage: 0.40},
	}}
	perf := routePerformance{Samples: 20, SuccessRate: 1, P50HeaderMS: 2000, P50TPS: 50}
	ollamaScore := server.routeScore("ollama", "ollama|glm", perf)
	bigModelScore := server.routeScore("bigmodel", "bigmodel|glm", perf)
	if bigModelScore <= ollamaScore {
		t.Fatalf("BigModel score %.2f must beat heavily used Ollama score %.2f", bigModelScore, ollamaScore)
	}
}

func TestRoutePerformanceExcludesQuotaAndPolicySkips(t *testing.T) {
	samples := []metricsSample{
		{Timestamp: time.Now().Add(-time.Minute).Format(time.RFC3339Nano), Provider: "ollama", Upstream: "deepseek", Status: 429, FailureReason: "quota_or_rate_limit"},
		{Timestamp: time.Now().Add(-time.Second).Format(time.RFC3339Nano), Provider: "ollama", Upstream: "deepseek", Status: 200, Success: true, LatencyMS: 800, TokensPerSecond: 60},
		{Timestamp: time.Now().Format(time.RFC3339Nano), Provider: "anthropic", Upstream: "sonnet", FailureReason: "claude_five_hour_reserve"},
		{Timestamp: time.Now().Format(time.RFC3339Nano), Provider: "anthropic", Upstream: "sonnet", FailureReason: "provider_wide_outage"},
		{Timestamp: time.Now().Format(time.RFC3339Nano), Provider: "commandcode", Upstream: "deepseek", FailureReason: "browser_usage_exhausted"},
		{Timestamp: time.Now().Format(time.RFC3339Nano), Provider: "bigmodel", Upstream: "glm", Status: 400, FailureReason: "thinking_signature_mismatch"},
	}
	performance := buildRoutePerformance(samples)
	ollama := performance["ollama|deepseek"]
	if ollama.Samples != 1 || ollama.SuccessRate != 1 || ollama.P50TPS != 60 {
		t.Fatalf("Ollama performance = %#v, want only real successful attempt", ollama)
	}
	if _, exists := performance["anthropic|sonnet"]; exists {
		t.Fatalf("Claude policy skip entered route performance: %#v", performance["anthropic|sonnet"])
	}
	if _, exists := performance["bigmodel|glm"]; exists {
		t.Fatalf("thinking compatibility skip entered route performance: %#v", performance["bigmodel|glm"])
	}
	if _, exists := performance["commandcode|deepseek"]; exists {
		t.Fatalf("browser usage skip entered route performance: %#v", performance["commandcode|deepseek"])
	}
}

func TestMetricsSummarySeparatesPolicySkipsFromDeliveredResponses(t *testing.T) {
	samples := []metricsSample{
		{RequestedModel: "sonnet", Provider: "anthropic", Upstream: "sonnet", RecordKind: "skip", FailureReason: "provider_wide_outage"},
		{RequestedModel: "sonnet", Provider: "anthropic", Upstream: "sonnet", RecordKind: "attempt", Status: 529},
		{RequestedModel: "sonnet", Provider: "anthropic", Upstream: "sonnet", RecordKind: "completion", Status: 200, Success: true},
		{RequestedModel: "sonnet", Provider: "anthropic", Upstream: "sonnet", RecordKind: "count_tokens", Status: 200, Success: true},
		{RequestedModel: "sonnet", Provider: "anthropic", Upstream: "sonnet", RecordKind: "terminal", Status: 502},
		{RequestedModel: "sonnet", Provider: "anthropic", Upstream: "sonnet", RecordKind: "terminal", Status: 200, Success: true},
	}
	response := summarizeMetrics(samples, len(samples))
	if len(response.Groups) != 1 {
		t.Fatalf("groups = %#v, want one", response.Groups)
	}
	group := response.Groups[0]
	if group.Records != 6 || group.Attempts != 2 || group.PolicySkips != 1 || group.CountTokenRequests != 1 || group.TerminalErrors != 1 || group.TerminalSuccesses != 1 || group.DeliveredResponses != 1 || group.SuccessRate != 0.5 {
		t.Fatalf("group = %#v, want separated count/terminal records", group)
	}
}

func TestRequestTraceCorrelatesAttempts(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	request, requestID := ensureProxyRequestID(request)
	again, secondID := ensureProxyRequestID(request)
	if requestID == "" || secondID != requestID || proxyRequestID(again) != requestID {
		t.Fatalf("request IDs first=%q second=%q context=%q", requestID, secondID, proxyRequestID(again))
	}
	metrics := newMetricsStore(metricsConfig{Path: filepath.Join(t.TempDir(), "metrics.jsonl"), MaxSamples: 10})
	defer metrics.close()
	server := &proxyServer{metrics: metrics, cfg: config{DefaultProvider: "anthropic"}}
	trace := requestTrace{ID: requestID, CandidateCount: 4, ChainStarted: time.Now(), ChainBudget: time.Minute}
	server.recordAttempt(trace, modelConfig{Requested: "sonnet"}, modelConfig{Provider: "anthropic", Upstream: "sonnet"}, 2, time.Now(), 529, true, "http_status")
	sample := metrics.read(1)[0]
	if sample.RequestID != requestID || sample.RecordKind != "attempt" || sample.Attempt != 2 || sample.CandidateCount != 4 {
		t.Fatalf("sample trace = %#v", sample)
	}
}

func TestClaudeTransportNeedsTwoProfilesBeforeRequestWideShortCircuit(t *testing.T) {
	if shouldShortCircuitClaudeTransport(1, false) {
		t.Fatal("one profile transport failure short-circuited the Claude pool")
	}
	if !shouldShortCircuitClaudeTransport(2, false) {
		t.Fatal("two profile transport failures did not short-circuit the Claude pool")
	}
	if !shouldShortCircuitClaudeTransport(1, true) {
		t.Fatal("confirmed outage did not short-circuit the Claude pool")
	}
}

func TestClaudeTransportTimeoutContinuesToNextAccount(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer token-one" {
			time.Sleep(100 * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","content":[]}`))
	}))
	defer upstream.Close()
	tempDir := t.TempDir()
	profileOne := claudeUsageProfile{Name: "pool-one", CachePath: filepath.Join(tempDir, "one", ".claude.json"), CredentialsService: "test-pool-one"}
	profileTwo := claudeUsageProfile{Name: "pool-two", CachePath: filepath.Join(tempDir, "two", ".claude.json"), CredentialsService: "test-pool-two"}
	claudeOAuthCredentials.Store(profileOne.CredentialsService, cachedClaudeOAuthCredential{token: "token-one", fetchedAt: time.Now()})
	claudeOAuthCredentials.Store(profileTwo.CredentialsService, cachedClaudeOAuthCredential{token: "token-two", fetchedAt: time.Now()})
	defer claudeOAuthCredentials.Delete(profileOne.CredentialsService)
	defer claudeOAuthCredentials.Delete(profileTwo.CredentialsService)
	server, err := newProxyServer(config{
		DefaultProvider: "anthropic",
		Providers: map[string]providerConfig{
			"anthropic": {BaseURL: upstream.URL, ResponseHeaderTimeoutMS: 40},
		},
		Metrics: metricsConfig{Path: filepath.Join(tempDir, "metrics.jsonl"), MaxSamples: 20},
		ClaudeUsage: claudeUsageConfig{
			Provider:             "anthropic",
			EligibleUpstreams:    []string{"claude-opus-4-8"},
			FiveHourThresholdPct: 95,
			SevenDayThresholdPct: 95,
			CacheTTLSeconds:      60,
			StaleTTLSeconds:      60,
			AutoSelectAccounts:   true,
			AccountPool:          []string{profileOne.Name, profileTwo.Name},
			AccountProfiles: map[string]claudeUsageProfile{
				profileOne.Name: profileOne,
				profileTwo.Name: profileTwo,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.metrics.close()
	for _, entry := range []struct {
		token   string
		profile string
	}{{"token-one", profileOne.Name}, {"token-two", profileTwo.Name}} {
		server.claudeUse.snapshots[claudeTokenKey("Bearer "+entry.token)] = claudeUsageSnapshot{
			FetchedAt: time.Now(), Profile: entry.profile,
			FiveHour: claudeUsageWindow{Utilization: 1}, SevenDay: claudeUsageWindow{Utilization: 1},
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	body := []byte(`{"model":"claude-opus-4-8","messages":[],"max_tokens":1}`)
	selected := modelConfig{Requested: "claude-opus-4-8", Provider: "anthropic", Upstream: "claude-opus-4-8"}
	result, err := server.doWithFallbacks(context.Background(), request, body, map[string]any{"model": selected.Requested}, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer result.resp.Body.Close()
	if result.model.ClaudeProfile != profileTwo.Name || result.attempt != 1 {
		t.Fatalf("result profile=%q attempt=%d, want second account attempt 1", result.model.ClaudeProfile, result.attempt)
	}
}

func TestFallbackChainBudgetAndFreeRouteReserve(t *testing.T) {
	if got := fallbackChainHeaderBudget(nil); got != 60*time.Second {
		t.Fatalf("small chain budget = %s, want 60s", got)
	}
	large := bytes.Repeat([]byte{'x'}, 400000)
	if got := fallbackChainHeaderBudget(large); got != 90*time.Second {
		t.Fatalf("large chain budget = %s, want 90s", got)
	}
	tier0, tier100 := 0, 100
	attempts := []claudeCandidateAttempt{
		{model: modelConfig{Provider: "anthropic", Upstream: "opus", Tier: &tier0}},
		{model: modelConfig{Provider: "bigmodel", Upstream: "glm", Tier: &tier0}},
		{model: modelConfig{Provider: "vercel", Upstream: "glm", Tier: &tier100}},
	}
	server := &proxyServer{}
	trace := requestTrace{CandidateCount: len(attempts), ChainStarted: time.Now(), ChainBudget: time.Minute}
	first, deferred := server.remainingAttemptHeaderBudget(trace, attempts, 0, "anthropic")
	if deferred || first > 50*time.Second || first < 49*time.Second {
		t.Fatalf("first budget = %s deferred=%v, want about 50s with free-route reserve", first, deferred)
	}
	lastFree, deferred := server.remainingAttemptHeaderBudget(trace, attempts, 1, "bigmodel")
	if deferred || lastFree < 59*time.Second {
		t.Fatalf("last free budget = %s deferred=%v, want no reserve for paid-only tail", lastFree, deferred)
	}
}

func TestMetricsStoreReadsMemoryAndKeepsPerRouteWindows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	metrics := newMetricsStore(metricsConfig{Path: path, MaxSamples: 20})
	for index := 0; index < 15; index++ {
		metrics.record(metricsSample{Provider: "provider-a", Upstream: "model", Status: 200, Success: true, Timestamp: time.Now().Format(time.RFC3339Nano)})
	}
	for index := 0; index < 15; index++ {
		metrics.record(metricsSample{Provider: "provider-b", Upstream: "model", Status: 200, Success: true, Timestamp: time.Now().Format(time.RFC3339Nano)})
	}
	if got := len(metrics.read(100)); got != 20 {
		t.Fatalf("global ring samples = %d, want 20", got)
	}
	if got := len(metrics.readPerRoute(10)); got != 20 {
		t.Fatalf("per-route samples = %d, want 10 from each route", got)
	}
	metrics.close()
	reloaded := newMetricsStore(metricsConfig{Path: path, MaxSamples: 20})
	defer reloaded.close()
	if got := len(reloaded.read(100)); got != 20 {
		t.Fatalf("reloaded samples = %d, want 20", got)
	}
}

func TestClientDisconnectMissingStopDoesNotPoisonProviderPerformance(t *testing.T) {
	metrics := newMetricsStore(metricsConfig{Path: filepath.Join(t.TempDir(), "metrics.jsonl"), MaxSamples: 20})
	server := &proxyServer{metrics: metrics}
	stats := &streamStats{}
	stats.ClientDisconnected.Store(true)
	stats.MissingStop.Store(true)
	stats.ProtocolError.Store(true)
	server.recordCompletion(
		requestTrace{},
		modelConfig{Requested: "sonnet", Provider: "bigmodel", Upstream: "glm-5.3-flash"},
		0, time.Now().Add(-10*time.Millisecond), http.StatusOK, time.Millisecond, context.Canceled, stats,
	)
	samples := metrics.read(10)
	if len(samples) != 1 || samples[0].FailureReason != "client_cancel" {
		t.Fatalf("samples = %#v, want neutral client_cancel", samples)
	}
	if performance := buildRoutePerformance(samples); len(performance) != 0 {
		t.Fatalf("client disconnect entered route performance: %#v", performance)
	}
}

func TestObservedTokensPerSecondRejectsBufferedSubsecondBodies(t *testing.T) {
	if got := observedTokensPerSecond(400, 25*time.Millisecond); got != 0 {
		t.Fatalf("buffered TPS = %f, want omitted", got)
	}
	if got := observedTokensPerSecond(120, 2*time.Second); got != 60 {
		t.Fatalf("streamed TPS = %f, want 60", got)
	}
}

func TestProviderCooldownUsesEmbeddedBigModelResetTimestamp(t *testing.T) {
	resetAt := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	body := []byte(`{"type":"error","error":{"message":"[1308][已达到 5 小时的使用上限。您的限额将在 ` + resetAt.Format("2006-01-02 15:04:05") + ` 重置。]"}}`)
	cooldown := providerCooldown(http.Header{}, body)
	if cooldown < 119*time.Minute || cooldown > 121*time.Minute {
		t.Fatalf("cooldown = %s, want reset timestamp near 2h", cooldown)
	}
}

func TestProviderCooldownTreatsRateLimitAsExtended(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit."}}`),
		[]byte(`{"error":{"message":"rate_limit"}}`),
	} {
		if got := providerCooldown(http.Header{}, body); got != 5*time.Minute {
			t.Fatalf("body=%s cooldown=%s, want 5m", body, got)
		}
	}
}

func TestFilterSSEEventCapturesExactUsage(t *testing.T) {
	state := &sseFilterState{drop: map[int]bool{}, remap: map[int]int{}}
	_, eventName, stopReason, inputTokens, outputTokens := filterSSEEvent([]string{
		"event: message_delta\n",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":123,"output_tokens":50}}` + "\n",
	}, state, nil, 0, "worker[1m]")
	if eventName != "message_delta" || stopReason != "end_turn" || inputTokens != 123 || outputTokens != 50 {
		t.Fatalf("event=%q stop=%q input=%d output=%d", eventName, stopReason, inputTokens, outputTokens)
	}
}

func TestFilterSSEEventSanitizesFutureToolIDs(t *testing.T) {
	state := &sseFilterState{drop: map[int]bool{}, remap: map[int]int{}}
	out, eventName, _, _, _ := filterSSEEvent([]string{
		"event: content_block_start\n",
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"Bash:157","name":"Bash","input":{}}}` + "\n",
	}, state, nil, 0, "kimi-k3[1m]")
	if eventName != "content_block_start" || strings.Contains(out, `"id":"Bash:157"`) {
		t.Fatalf("tool ID was not sanitized: %s", out)
	}
	data := ""
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "data: ") {
			data = strings.TrimPrefix(line, "data: ")
		}
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatal(err)
	}
	block := payload["content_block"].(map[string]any)
	if id := block["id"].(string); !validToolUseID(id, false) {
		t.Fatalf("sanitized ID remains invalid: %q", id)
	}
}

func TestTranslateOpenAIJSONUnwrapsClinePassEnvelope(t *testing.T) {
	body := []byte(`{"success":true,"data":{"id":"gen_cline","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"DEEPSEEK_OK"}}],"usage":{"prompt_tokens":11,"completion_tokens":6}}}`)
	translated := translateOpenAIJSONResponse(body, "deepseek-v4-flash[1m]")
	var got struct {
		Type       string `json:"type"`
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(translated, &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "message" || got.Model != "deepseek-v4-flash[1m]" || got.StopReason != "end_turn" {
		t.Fatalf("translated envelope metadata = %#v", got)
	}
	if len(got.Content) != 1 || got.Content[0].Type != "text" || got.Content[0].Text != "DEEPSEEK_OK" {
		t.Fatalf("translated envelope content = %#v", got.Content)
	}
	if got.Usage.InputTokens != 11 || got.Usage.OutputTokens != 6 {
		t.Fatalf("translated envelope usage = %#v", got.Usage)
	}
}

func TestTranslateOpenAIStreamKeepsToolCallFromFirstChunk(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"gen_cline","choices":[{"index":0,"delta":{"role":"assistant","content":null,"tool_calls":[{"index":0,"id":"chatcmpl-tool-1","type":"function","function":{"name":"echo_value","arguments":""}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"gen_cline","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"value\":\"DEEPSEEK_TOOL_OK\"}"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"gen_cline","choices":[{"index":0,"delta":{"content":""},"finish_reason":"tool_calls"}]}`,
		"",
		`data: {"id":"gen_cline","choices":[],"usage":{"prompt_tokens":290,"completion_tokens":50}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	var translated strings.Builder
	if err := translateOpenAIStream(strings.NewReader(stream), &translated, "deepseek-v4-flash[1m]"); err != nil {
		t.Fatal(err)
	}
	output := translated.String()
	for _, want := range []string{
		`"type":"tool_use"`,
		`"id":"chatcmpl-tool-1"`,
		`"name":"echo_value"`,
		`"partial_json":"{\"value\":\"DEEPSEEK_TOOL_OK\"}"`,
		`"stop_reason":"tool_use"`,
		`"input_tokens":290`,
		`"output_tokens":50`,
		`event: message_stop`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("translated stream missing %q:\n%s", want, output)
		}
	}
}

func TestTranslateOpenAIStreamPreservesArgumentsFromOpeningChunk(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"gen_opening","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-opening","function":{"name":"read_file","arguments":"{\"path\":\"README.md\"}"}}]},"finish_reason":"tool_calls"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	var translated strings.Builder
	if err := translateOpenAIStream(strings.NewReader(stream), &translated, "worker[1m]"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(translated.String(), `"partial_json":"{\"path\":\"README.md\"}"`) {
		t.Fatalf("opening tool arguments were dropped:\n%s", translated.String())
	}
}

func TestTranslateOpenAIStreamRejectsMalformedToolArguments(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"gen-malformed","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-malformed","function":{"name":"read_file","arguments":"{broken"}}]},"finish_reason":"tool_calls"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	stats := &streamStats{}
	var translated strings.Builder
	if err := translateOpenAIStreamWithStats(strings.NewReader(stream), &translated, "worker[1m]", stats); err != nil {
		t.Fatal(err)
	}
	if !stats.ProtocolError.Load() || !strings.Contains(translated.String(), "malformed tool call arguments") {
		t.Fatalf("malformed tool arguments were not rejected: protocol=%t output=%s", stats.ProtocolError.Load(), translated.String())
	}
}

func TestAllowedFableHeaderDoesNotClearGenericRateLimitBlock(t *testing.T) {
	server := &proxyServer{
		cfg:            config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}},
		providerStates: map[string]providerRuntimeState{},
	}
	server.markProviderBlocked("anthropic@personal#fable", "quota_or_rate_limit", time.Minute)
	server.observeProviderHeaders("anthropic@personal", http.Header{
		"Anthropic-Ratelimit-Unified-7d_oi-Status": []string{"allowed"},
	})
	if blocked, state := server.providerBlocked("anthropic@personal#fable"); !blocked || state.Reason != "quota_or_rate_limit" {
		t.Fatalf("generic rate-limit block was cleared by Fable allowed header: blocked=%t state=%#v", blocked, state)
	}
}

func TestRecordAttemptDoesNotCallStreamStartSuccess(t *testing.T) {
	metrics := newMetricsStore(metricsConfig{Path: filepath.Join(t.TempDir(), "metrics.jsonl"), MaxSamples: 10})
	defer metrics.close()
	server := &proxyServer{metrics: metrics}
	candidate := modelConfig{Requested: "worker", Provider: "provider", Upstream: "worker"}
	server.recordAttempt(requestTrace{}, candidate, candidate, 0, time.Now(), http.StatusOK, true, "stream_start")
	if metrics.read(1)[0].Success {
		t.Fatal("stream-start failure was recorded as a successful attempt")
	}
}

func TestFallbackSuccessDoesNotHealFailedClaudeAccount(t *testing.T) {
	profiles := map[string]claudeUsageProfile{}
	for _, name := range []string{"audit-primary", "audit-fallback"} {
		credentialService := "test-" + name
		claudeOAuthCredentials.Store(credentialService, cachedClaudeOAuthCredential{token: "sk-ant-oat-" + name, fetchedAt: time.Now()})
		cleanupService := credentialService
		t.Cleanup(func() { claudeOAuthCredentials.Delete(cleanupService) })
		profiles[name] = testClaudeProfile(t, name, credentialService, name)
	}
	calls := map[string]int{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primary := strings.HasSuffix(r.Header.Get("Authorization"), "audit-primary")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/oauth/usage" {
			usage := 10
			if primary {
				usage = 0
			}
			_, _ = fmt.Fprintf(w, `{"five_hour":{"utilization":%d},"seven_day":{"utilization":20}}`, usage)
			return
		}
		if primary {
			calls["primary"]++
			w.Header().Set("Retry-After", "300")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"rate limit"}}`)
			return
		}
		calls["fallback"]++
		_, _ = fmt.Fprint(w, `{"type":"message","model":"claude-fable-5-1","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn"}`)
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		DefaultProvider: "anthropic",
		Providers:       map[string]providerConfig{"anthropic": {BaseURL: upstream.URL}},
		Models:          map[string]modelConfig{"claude-fable-5-1": {Provider: "anthropic", Upstream: "claude-fable-5-1"}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true,
			AccountPool: []string{"audit-primary", "audit-fallback"}, AccountProfiles: profiles,
			EligibleUpstreams: []string{"claude-fable-5-1"}, FiveHourThresholdPct: 100, SevenDayThresholdPct: 100,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", strings.NewReader(`{"model":"claude-fable-5-1","messages":[{"role":"user","content":"test"}]}`))
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || calls["primary"] != 1 || calls["fallback"] != 1 {
		t.Fatalf("status=%d calls=%v, want primary/fallback once", response.Code, calls)
	}
	if blocked, state := server.providerBlocked("anthropic@audit-primary#fable"); !blocked {
		t.Fatalf("failed account block was healed by fallback success: %#v", state)
	}
}

func TestClaudeFallbackReservationFollowsActualAccount(t *testing.T) {
	testClaudeFallbackReservation(t, false)
}

func TestClaudeFallbackReservationReleasesCircuitSkippedAccount(t *testing.T) {
	testClaudeFallbackReservation(t, true)
}

func testClaudeFallbackReservation(t *testing.T, skipPrimary bool) {
	// Repeatable even after prior quota failures, and bounded on assertion failure.
	for _, name := range []string{"reservation-primary", "reservation-fallback"} {
		key := "anthropic@" + name + "|claude-fable-5-1"
		circuitBreakers.Delete(key)
		t.Cleanup(func() { circuitBreakers.Delete(key) })
	}
	profiles := map[string]claudeUsageProfile{}
	for _, name := range []string{"reservation-primary", "reservation-fallback"} {
		service := "test-" + name
		claudeOAuthCredentials.Store(service, cachedClaudeOAuthCredential{token: "sk-ant-oat-" + name, fetchedAt: time.Now()})
		cleanupService := service
		t.Cleanup(func() { claudeOAuthCredentials.Delete(cleanupService) })
		profiles[name] = testClaudeProfile(t, name, service, name)
	}
	fallbackEntered := make(chan struct{})
	allowFallback := make(chan struct{})
	var allowOnce sync.Once
	var enteredOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/oauth/usage" {
			_, _ = fmt.Fprint(w, `{"five_hour":{"utilization":0},"seven_day":{"utilization":0}}`)
			return
		}
		primary := strings.HasSuffix(r.Header.Get("Authorization"), "reservation-primary")
		if primary {
			w.Header().Set("Retry-After", "300")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"rate limit"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		enteredOnce.Do(func() { close(fallbackEntered) })
		<-allowFallback
		_, _ = fmt.Fprint(w, `{"type":"message","model":"claude-fable-5-1","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn"}`)
	}))
	defer upstream.Close()
	defer allowOnce.Do(func() { close(allowFallback) })
	server, err := newProxyServer(config{
		DefaultProvider: "anthropic",
		Providers:       map[string]providerConfig{"anthropic": {BaseURL: upstream.URL}},
		Models:          map[string]modelConfig{"claude-fable-5-1": {Provider: "anthropic", Upstream: "claude-fable-5-1"}},
		ClaudeUsage: claudeUsageConfig{
			Provider: "anthropic", AutoSelectAccounts: true,
			AccountPool: []string{"reservation-primary", "reservation-fallback"}, AccountProfiles: profiles,
			EligibleUpstreams: []string{"claude-fable-5-1"}, FiveHourThresholdPct: 95, SevenDayThresholdPct: 95,
			CacheTTLSeconds: 300, StaleTTLSeconds: 1800, RequestTimeoutMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if skipPrimary {
		candidate := modelConfig{Upstream: "claude-fable-5-1"}
		server.tripCircuit("anthropic@reservation-primary", candidate)
		server.tripCircuit("anthropic@reservation-primary", candidate)
	}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", strings.NewReader(`{"model":"claude-fable-5-1","messages":[{"role":"user","content":"test"}]}`))
	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		server.handler().ServeHTTP(response, request)
		responseCh <- response
	}()
	select {
	case <-fallbackEntered:
	case <-time.After(time.Second):
		t.Fatal("fallback attempt did not start")
	}
	deadline := time.Now().Add(time.Second)
	for {
		server.claudeAccountMu.Lock()
		primaryInFlight := server.claudeAccountInFlight[claudeProfileRegistryKey(profiles["reservation-primary"])]
		fallbackInFlight := server.claudeAccountInFlight[claudeProfileRegistryKey(profiles["reservation-fallback"])]
		server.claudeAccountMu.Unlock()
		if primaryInFlight == 0 && fallbackInFlight == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("in-flight reservations = primary:%d fallback:%d, want primary:0 fallback:1", primaryInFlight, fallbackInFlight)
		}
		time.Sleep(time.Millisecond)
	}
	allowOnce.Do(func() { close(allowFallback) })
	select {
	case response := <-responseCh:
		if response.Code != http.StatusOK {
			t.Fatalf("fallback response status = %d, want 200", response.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback request did not finish")
	}
	server.claudeAccountMu.Lock()
	defer server.claudeAccountMu.Unlock()
	if len(server.claudeAccountInFlight) != 0 {
		t.Fatalf("in-flight reservations leaked: %#v", server.claudeAccountInFlight)
	}
}

func TestClaudeUsageSnapshotContextUsesWarmProfileCache(t *testing.T) {
	profile := claudeUsageProfile{Name: "warm-profile", CachePath: filepath.Join(t.TempDir(), "missing.json"), CredentialsService: "missing-service"}
	server := &proxyServer{
		cfg: config{ClaudeUsage: claudeUsageConfig{CacheTTLSeconds: 300, StaleTTLSeconds: 1800}},
		claudeUse: claudeUsageCache{snapshots: map[string]claudeUsageSnapshot{
			"warm-key": {
				FetchedAt: time.Now().Add(-time.Minute), Profile: profile.Name,
				FiveHour: claudeUsageWindow{Utilization: 12}, SevenDay: claudeUsageWindow{Utilization: 34}, Source: "oauth-api",
			},
		}},
	}
	snapshot, err := server.claudeUsageStatusSnapshotContext(context.Background(), profile)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Profile != profile.Name || snapshot.FiveHour.Utilization != 12 || snapshot.SevenDay.Utilization != 34 {
		t.Fatalf("warm snapshot = %#v, want cached profile usage", snapshot)
	}
}

func TestProviderQuarantineStartsAtFiveMinutesEscalatesAndRecovers(t *testing.T) {
	server := quarantineTestServer(providerQuarantineConfig{
		Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300,
		MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1000, StreamEventStallMS: 1000,
	})
	before := time.Now()
	if !server.observeProviderInstability("provider", "http_5xx") {
		t.Fatal("first threshold signal did not quarantine provider")
	}
	blocked, state := server.providerBlocked("provider")
	if !blocked || state.BlockedUntil.Before(before.Add(299*time.Second)) || state.BlockedUntil.After(before.Add(301*time.Second)) {
		t.Fatalf("initial quarantine until = %s, want about five minutes", state.BlockedUntil.Sub(before))
	}

	for level, want := range []time.Duration{10 * time.Minute, 20 * time.Minute, 30 * time.Minute, 30 * time.Minute} {
		expireProviderQuarantine(server, "provider")
		server.observeProviderInstability("provider", "stream_event_stall")
		_, state = server.providerBlocked("provider")
		remaining := time.Until(state.BlockedUntil)
		if remaining < want-time.Second || remaining > want+time.Second {
			t.Fatalf("level %d quarantine = %s, want %s", level+2, remaining, want)
		}
	}

	expireProviderQuarantine(server, "provider")
	server.observeProviderHealthy("provider")
	server.observeProviderHealthy("provider")
	server.providerStateMu.Lock()
	state = server.providerStates["provider"]
	server.providerStateMu.Unlock()
	if state.QuarantineLevel != 0 || state.Recovering || !state.QuarantinedUntil.IsZero() {
		t.Fatalf("state after recovery = %#v, want reset", state)
	}
}

func TestActiveProviderQuarantineIgnoresConcurrentInstabilitySignals(t *testing.T) {
	server := quarantineTestServer(providerQuarantineConfig{
		Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300,
		MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1000, StreamEventStallMS: 1000,
	})
	if !server.observeProviderInstability("provider", "transport") {
		t.Fatal("initial transport signal did not quarantine provider")
	}
	server.providerStateMu.Lock()
	initial := server.providerStates["provider"]
	server.providerStateMu.Unlock()

	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			server.observeProviderInstability("provider", "ttfb_timeout")
		}()
	}
	close(start)
	workers.Wait()

	server.providerStateMu.Lock()
	final := server.providerStates["provider"]
	server.providerStateMu.Unlock()
	if final.QuarantineLevel != initial.QuarantineLevel {
		t.Fatalf("quarantine level = %d, want active level %d", final.QuarantineLevel, initial.QuarantineLevel)
	}
	if !final.QuarantinedUntil.Equal(initial.QuarantinedUntil) {
		t.Fatalf("quarantine until = %s, want unchanged %s", final.QuarantinedUntil, initial.QuarantinedUntil)
	}
}

func TestAnthropicToolsToOpenAIOmitsNullDescription(t *testing.T) {
	tools := []any{
		map[string]any{
			"name":         "without_description",
			"input_schema": map[string]any{"type": "object"},
		},
		map[string]any{
			"name":         "with_description",
			"description":  "Useful tool",
			"input_schema": map[string]any{"type": "object"},
		},
	}

	converted := anthropicToolsToOpenAI(tools)
	if len(converted) != 2 {
		t.Fatalf("converted tools = %d, want 2", len(converted))
	}
	first := converted[0].(map[string]any)["function"].(map[string]any)
	if _, exists := first["description"]; exists {
		t.Fatalf("missing Anthropic description became OpenAI value %#v", first["description"])
	}
	second := converted[1].(map[string]any)["function"].(map[string]any)
	if second["description"] != "Useful tool" {
		t.Fatalf("description = %#v, want Useful tool", second["description"])
	}
}

func TestLongSteadySSEStreamDoesNotStall(t *testing.T) {
	reader, writer := io.Pipe()
	stats := &streamStats{}
	filtered := newSSEFilterReadCloser(reader, nil, 0, "deepseek", 75*time.Millisecond, stats)
	done := make(chan error, 1)
	go func() {
		frames := []string{
			"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
			"event: ping\ndata: {\"type\":\"ping\"}\n\n",
			"event: ping\ndata: {\"type\":\"ping\"}\n\n",
			"event: ping\ndata: {\"type\":\"ping\"}\n\n",
			"event: ping\ndata: {\"type\":\"ping\"}\n\n",
			"event: ping\ndata: {\"type\":\"ping\"}\n\n",
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
		}
		for _, frame := range frames {
			if _, err := io.WriteString(writer, frame); err != nil {
				done <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		done <- writer.Close()
	}()
	started := time.Now()
	_, err := io.ReadAll(filtered)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if time.Since(started) <= 75*time.Millisecond {
		t.Fatal("test stream did not exceed watchdog duration")
	}
	if stats.StreamStalled.Load() || stats.ProtocolError.Load() {
		t.Fatalf("steady stream marked unhealthy: stalled=%v protocol=%v", stats.StreamStalled.Load(), stats.ProtocolError.Load())
	}
}

func TestSSEWatchdogDetectsNoEventStallDespiteKeepAliveBytes(t *testing.T) {
	reader, writer := io.Pipe()
	stats := &streamStats{}
	filtered := newSSEFilterReadCloser(reader, nil, 0, "deepseek", 30*time.Millisecond, stats)
	go func() {
		defer writer.Close()
		for {
			if _, err := io.WriteString(writer, ": keep-alive\n\n"); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	_, _ = io.ReadAll(filtered)
	if !stats.StreamStalled.Load() {
		t.Fatal("keep-alive-only stream did not trip no-event watchdog")
	}
}

func TestMalformedAnthropicSSEIsRejected(t *testing.T) {
	stats := &streamStats{}
	source := io.NopCloser(strings.NewReader("event: message_start\ndata: not-json\n\n"))
	filtered := newSSEFilterReadCloser(source, nil, 0, "deepseek", time.Second, stats)
	body, err := io.ReadAll(filtered)
	if err == nil {
		t.Fatal("malformed SSE returned no stream error")
	}
	if !stats.ProtocolError.Load() || !strings.Contains(string(body), "malformed upstream Anthropic SSE") {
		t.Fatalf("protocolError=%v body=%q", stats.ProtocolError.Load(), body)
	}
}

func TestResponseHeaderBudgetTimesOutBeforeProviderTransportCeiling(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"bounded": {BaseURL: upstream.URL, ResponseHeaderTimeoutMS: 50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, err = server.doUpstreamWithHeaderBudget(context.Background(), request, []byte(`{"model":"bounded"}`), modelConfig{Provider: "bounded", Upstream: "bounded"})
	var timeoutErr *responseHeaderTimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.budget != 50*time.Millisecond {
		t.Fatalf("error = %#v, want 50ms response-header timeout", err)
	}
}

func TestFallbackChainCanCapCandidateHeaderBudget(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"bounded": {BaseURL: upstream.URL, ResponseHeaderTimeoutMS: 200},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, err = server.doUpstreamWithHeaderBudgetLimit(context.Background(), request, []byte(`{"model":"bounded"}`), modelConfig{Provider: "bounded", Upstream: "bounded"}, 40*time.Millisecond)
	var timeoutErr *responseHeaderTimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.budget != 40*time.Millisecond || !timeoutErr.chainLimited {
		t.Fatalf("error = %#v, want 40ms chain-limited response-header timeout", err)
	}
}

func TestResponseHeaderBudgetDoesNotLimitStreamLifetime(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte("complete"))
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"bounded-stream": {BaseURL: upstream.URL, ResponseHeaderTimeoutMS: 50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	response, err := server.doUpstreamWithHeaderBudget(context.Background(), request, []byte(`{"model":"bounded"}`), modelConfig{Provider: "bounded-stream", Upstream: "bounded"})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "complete" {
		t.Fatalf("stream body = %q err=%v, want complete after header budget", body, err)
	}
}

func TestDeepContextExpandsCandidateHeaderBudgetWithinProviderMaximum(t *testing.T) {
	server := &proxyServer{cfg: config{
		Providers:   map[string]providerConfig{"anthropic": {ResponseHeaderTimeoutMS: 60000}},
		ClaudeUsage: claudeUsageConfig{Provider: "anthropic"},
	}}
	candidate := modelConfig{Provider: "anthropic", Upstream: "claude-fable-5"}
	if got := server.candidateResponseHeaderBudget("anthropic", candidate, []byte("small")); got != 30*time.Second {
		t.Fatalf("small-context budget = %s, want 30s", got)
	}
	large := bytes.Repeat([]byte{'x'}, 700000)
	if got := server.candidateResponseHeaderBudget("anthropic", candidate, large); got != 60*time.Second {
		t.Fatalf("large-context budget = %s, want provider maximum 60s", got)
	}
}

func TestHTTPFailuresAndExcessiveTTFBFeedQuarantine(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		delay     time.Duration
		ttfbLimit int
	}{
		{name: "http 5xx", status: http.StatusServiceUnavailable, ttfbLimit: 1000},
		{name: "excessive ttfb", status: http.StatusOK, delay: 20 * time.Millisecond, ttfbLimit: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(test.delay)
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`{"type":"message"}`))
			}))
			defer upstream.Close()
			server, err := newProxyServer(config{
				Providers: map[string]providerConfig{"provider": {BaseURL: upstream.URL}},
				Quarantine: providerQuarantineConfig{
					Enabled: true, SignalThreshold: 2, WindowSeconds: 300, BaseSeconds: 300,
					MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: test.ttfbLimit, StreamEventStallMS: 1000,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			selected := modelConfig{Requested: "deepseek", Provider: "provider", Upstream: "deepseek"}
			for attempt := 0; attempt < 2; attempt++ {
				req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
				response, err := server.doWithFallbacks(context.Background(), req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.resp.Body.Close()
			}
			if blocked, state := server.providerBlocked("provider"); !blocked || !strings.HasPrefix(state.Reason, "instability_") {
				t.Fatalf("provider state = %#v, want instability quarantine", state)
			}
		})
	}
}

func TestSharedNetworkIncidentSuppressesProviderQuarantineButNotQuotaBlocks(t *testing.T) {
	quota := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"usage limit reached"}}`))
	}))
	defer quota.Close()
	healthyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	})
	healthy := httptest.NewServer(healthyHandler)
	defer healthy.Close()
	healthyC := httptest.NewServer(healthyHandler)
	defer healthyC.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"provider-a": {BaseURL: quota.URL},
			"provider-b": {BaseURL: healthy.URL},
			"provider-c": {BaseURL: healthyC.URL},
		},
		Quarantine: providerQuarantineConfig{
			Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300,
			MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1000, StreamEventStallMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, failure := range []struct {
		provider string
		signal   string
	}{
		{provider: "provider-a", signal: "transport"},
		{provider: "provider-b", signal: "ttfb_timeout"},
		{provider: "provider-c", signal: "excessive_ttfb"},
	} {
		server.observeProviderInstability(failure.provider, failure.signal)
	}
	for _, provider := range []string{"provider-a", "provider-b", "provider-c"} {
		if blocked, state := server.providerBlocked(provider); blocked || state.QuarantineLevel != 0 {
			t.Fatalf("shared-network signal left %s individually quarantined: %#v", provider, state)
		}
	}
	if status := server.networkIncidentStatus(); status["active"] != true {
		t.Fatalf("shared incident not active after independent origins: %#v", status)
	}
	server.observeNetworkSuccess()

	selected := modelConfig{
		Requested: "deepseek", Provider: "provider-a", Upstream: "deepseek",
		Fallbacks: []modelConfig{{Provider: "provider-b", Upstream: "deepseek"}},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
	response, err := server.doWithFallbacks(context.Background(), req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer response.resp.Body.Close()
	if response.providerName != "provider-b" || response.resp.StatusCode != http.StatusOK {
		t.Fatalf("quota fallback = %q status=%d, want provider-b HTTP 200", response.providerName, response.resp.StatusCode)
	}
	if blocked, state := server.providerBlocked("provider-a"); !blocked || state.Reason != "quota_or_rate_limit" {
		t.Fatalf("quota block suppressed with shared-network incident: %#v", state)
	}
	for _, provider := range []string{"provider-b", "provider-c"} {
		if blocked, state := server.providerBlocked(provider); blocked {
			t.Fatalf("provider-specific quota block leaked to %s: %#v", provider, state)
		}
	}
}

func TestSharedNetworkIncidentDeduplicatesProviderOrigins(t *testing.T) {
	servers := make([]*httptest.Server, 3)
	for index := range servers {
		servers[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer servers[index].Close()
	}
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"provider-a":       {BaseURL: servers[0].URL},
			"provider-a-alias": {BaseURL: servers[0].URL},
			"provider-b":       {BaseURL: servers[1].URL},
			"provider-c":       {BaseURL: servers[2].URL},
		},
		Quarantine: providerQuarantineConfig{
			Enabled: true, SignalThreshold: 99, WindowSeconds: 300, BaseSeconds: 300,
			MaxSeconds: 1800, RecoverySuccesses: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"provider-a", "provider-a-alias"} {
		server.observeProviderInstability(provider, "transport")
	}
	if status := server.networkIncidentStatus(); status["active"] == true {
		t.Fatalf("one deduplicated origin triggered shared incident: %#v", status)
	}
	server.observeProviderInstability("provider-b", "transport")
	if status := server.networkIncidentStatus(); status["active"] != true {
		t.Fatalf("two independent origins did not trigger shared incident: %#v", status)
	}
	server.providerStateMu.Lock()
	defer server.providerStateMu.Unlock()
	for provider, state := range server.providerStates {
		if state.InstabilitySignals != 0 || state.QuarantineLevel != 0 {
			t.Fatalf("shared incident retained pending state for %s: %#v", provider, state)
		}
	}
}

func TestStrongLocalNetworkFailureSuppressesProviderPenaltyImmediately(t *testing.T) {
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"provider-a": {BaseURL: "https://provider-a.example"},
		},
		Quarantine: providerQuarantineConfig{
			Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300,
			MaxSeconds: 1800, RecoverySuccesses: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	decision := server.observeNetworkTransportFailure("provider-a", "transport", fmt.Errorf("network is unreachable"))
	if !decision.SuppressProviderPenalty || decision.StopFallback {
		t.Fatalf("decision = %#v, want immediate suppression without confirmed outage", decision)
	}
	if status := server.networkIncidentStatus(); status["phase"] != networkPhaseSuspected {
		t.Fatalf("status = %#v, want suspected", status)
	}
	if blocked, state := server.providerBlocked("provider-a"); blocked || state.QuarantineLevel != 0 {
		t.Fatalf("local outage penalized provider: %#v", state)
	}
}

func TestSharedNetworkRecoveryAllowsOnlyOneProbe(t *testing.T) {
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"provider-a": {BaseURL: "https://provider-a.example"},
			"provider-b": {BaseURL: "https://provider-b.example"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 3, 5, 0, 0, 0, time.UTC)
	server.clockNow = func() time.Time { return now }

	server.observeNetworkTransportFailure("provider-a", "transport", fmt.Errorf("network is unreachable"))
	decision := server.observeNetworkTransportFailure("provider-b", "transport", fmt.Errorf("i/o timeout"))
	if !decision.SuppressProviderPenalty || !decision.StopFallback {
		t.Fatalf("confirmation decision = %#v, want suppressed stop", decision)
	}
	if probe, err := server.beginNetworkRequest(); err == nil || probe {
		t.Fatalf("immediate gate = probe=%t err=%v, want retryable rejection", probe, err)
	}

	now = now.Add(networkRecoveryDelay + time.Millisecond)
	probe, err := server.beginNetworkRequest()
	if err != nil || !probe {
		t.Fatalf("recovery probe = probe=%t err=%v, want sole probe", probe, err)
	}
	if secondProbe, secondErr := server.beginNetworkRequest(); secondErr == nil || secondProbe {
		t.Fatalf("parallel probe = probe=%t err=%v, want rejection", secondProbe, secondErr)
	}
	server.finishNetworkProbe(probe, false)
	if status := server.networkIncidentStatus(); status["phase"] != networkPhaseOffline {
		t.Fatalf("failed probe status = %#v, want offline", status)
	}

	now = now.Add(networkRecoveryDelay + time.Millisecond)
	probe, err = server.beginNetworkRequest()
	if err != nil || !probe {
		t.Fatalf("second recovery probe = probe=%t err=%v", probe, err)
	}
	server.observeNetworkSuccess()
	server.finishNetworkProbe(probe, true)
	if status := server.networkIncidentStatus(); status["phase"] != networkPhaseNormal || status["active"] == true {
		t.Fatalf("recovered status = %#v, want normal", status)
	}
}

func TestSharedNetworkGateReturnsRetryAfterWithoutUpstreamCall(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		DefaultProvider: "provider-a",
		Providers: map[string]providerConfig{
			"provider-a": {BaseURL: upstream.URL},
			"provider-b": {BaseURL: "https://provider-b.example"},
		},
		Models: map[string]modelConfig{
			"test-model": {Provider: "provider-a", Upstream: "test-model"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.observeNetworkTransportFailure("provider-a", "transport", fmt.Errorf("network is unreachable"))
	server.observeNetworkTransportFailure("provider-b", "transport", fmt.Errorf("i/o timeout"))

	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"test-model","messages":[]}`))
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "2" {
		t.Fatalf("response = status=%d retry-after=%q body=%q", response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0", got)
	}
}

func TestAllBlockedFallbackProvidersFailFastWithoutProbing(t *testing.T) {
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls++
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer fallback.Close()

	selected := modelConfig{
		Requested: "deepseek", Provider: "primary", Upstream: "deepseek",
		Fallbacks: []modelConfig{{Provider: "fallback", Upstream: "deepseek"}},
	}
	server, err := newProxyServer(config{
		DefaultProvider: "primary",
		Providers: map[string]providerConfig{
			"primary":  {BaseURL: primary.URL},
			"fallback": {BaseURL: fallback.URL},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.markProviderBlocked("primary", "quota_or_rate_limit", time.Hour)
	server.markProviderBlocked("fallback", "quota_or_rate_limit", time.Hour)

	started := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
	_, err = server.doWithFallbacks(context.Background(), req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
	if err == nil {
		t.Fatal("all-blocked chain unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("all-blocked chain took %s, want fast failure", elapsed)
	}
	if primaryCalls != 0 || fallbackCalls != 0 {
		t.Fatalf("blocked providers were probed: primary=%d fallback=%d", primaryCalls, fallbackCalls)
	}
}

func TestFinalInstabilityQuarantinedProviderFailsOpenAfterHardBlocks(t *testing.T) {
	slowCalls := 0
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer slow.Close()
	quotaCalls := 0
	quota := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		quotaCalls++
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer quota.Close()

	selected := modelConfig{
		Requested: "deepseek", Provider: "slow", Upstream: "deepseek",
		Fallbacks: []modelConfig{{Provider: "quota", Upstream: "deepseek"}},
	}
	server, err := newProxyServer(config{
		DefaultProvider: "slow",
		Providers: map[string]providerConfig{
			"slow":  {BaseURL: slow.URL},
			"quota": {BaseURL: quota.URL},
		},
		Quarantine: providerQuarantineConfig{
			Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300,
			MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1000, StreamEventStallMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.observeProviderInstability("slow", "excessive_ttfb")
	server.markProviderBlocked("quota", "quota_or_rate_limit", time.Hour)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
	response, err := server.doWithFallbacks(context.Background(), req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer response.resp.Body.Close()
	if response.providerName != "slow" || response.resp.StatusCode != http.StatusOK {
		t.Fatalf("provider=%q status=%d, want slow HTTP 200", response.providerName, response.resp.StatusCode)
	}
	if slowCalls != 1 || quotaCalls != 0 {
		t.Fatalf("calls slow=%d quota=%d, want instability fail-open only", slowCalls, quotaCalls)
	}
	if blocked, state := server.providerBlocked("slow"); !blocked || !strings.HasPrefix(state.Reason, "instability_") {
		t.Fatalf("instability telemetry was cleared: %#v", state)
	}
}

func TestFinalInstabilityCircuitFailsOpenAfterHardBlocks(t *testing.T) {
	quotaCalls := 0
	quota := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		quotaCalls++
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	degradedCalls := 0
	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		degradedCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer quota.Close()
	defer degraded.Close()

	selected := modelConfig{
		Requested: "deepseek", Provider: "quota", Upstream: "deepseek",
		Fallbacks: []modelConfig{{Provider: "degraded", Upstream: "deepseek"}},
	}
	server, err := newProxyServer(config{
		DefaultProvider: "quota",
		Providers: map[string]providerConfig{
			"quota":    {BaseURL: quota.URL},
			"degraded": {BaseURL: degraded.URL},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.markProviderBlocked("quota", "quota_or_rate_limit", time.Hour)
	server.tripCircuit("degraded", selected.Fallbacks[0])
	server.tripCircuit("degraded", selected.Fallbacks[0])
	if !server.circuitOpen("degraded", selected.Fallbacks[0]) {
		t.Fatal("degraded circuit did not open")
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
	response, err := server.doWithFallbacks(context.Background(), req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer response.resp.Body.Close()
	if response.providerName != "degraded" || response.resp.StatusCode != http.StatusOK {
		t.Fatalf("provider=%q status=%d, want final degraded HTTP 200", response.providerName, response.resp.StatusCode)
	}
	if quotaCalls != 0 || degradedCalls != 1 {
		t.Fatalf("calls quota=%d degraded=%d, want final circuit fail-open only", quotaCalls, degradedCalls)
	}
}

func TestCircuitHalfOpenAllowsOneProbe(t *testing.T) {
	server := &proxyServer{cfg: config{Providers: map[string]providerConfig{"half-open-provider": {}}}}
	candidate := modelConfig{Upstream: "half-open-model"}
	server.tripCircuit("half-open-provider", candidate)
	server.tripCircuit("half-open-provider", candidate)
	if allowed, _ := server.acquireCircuitAttempt("half-open-provider", candidate, false); allowed {
		t.Fatal("open circuit allowed a pre-cooldown non-final request")
	}
	raw, ok := circuitBreakers.Load(server.circuitKey("half-open-provider", candidate))
	if !ok {
		t.Fatal("circuit state missing")
	}
	state := raw.(*circuitState)
	state.mu.Lock()
	state.openUntil = time.Now().Add(-time.Millisecond)
	state.mu.Unlock()

	allowed, probe := server.acquireCircuitAttempt("half-open-provider", candidate, false)
	if !allowed || probe == nil {
		t.Fatalf("expired circuit = allowed=%t probe=%v, want one probe", allowed, probe)
	}
	if secondAllowed, _ := server.acquireCircuitAttempt("half-open-provider", candidate, true); secondAllowed {
		t.Fatal("parallel half-open probe was allowed")
	}
	releaseCircuitProbe(probe)
	if allowed, probe = server.acquireCircuitAttempt("half-open-provider", candidate, false); !allowed || probe == nil {
		t.Fatalf("released half-open circuit = allowed=%t probe=%v", allowed, probe)
	}
	server.resetCircuit("half-open-provider", candidate)
}

func TestAdaptiveRankingSpreadsEqualTierInFlightRoutes(t *testing.T) {
	server, err := newProxyServer(config{
		DefaultProvider: "load-a",
		Providers: map[string]providerConfig{
			"load-a": {BaseURL: "https://load-a.example"},
			"load-b": {BaseURL: "https://load-b.example"},
		},
		AdaptiveRouting: adaptiveRoutingConfig{
			Enabled: true, WindowSamples: 20, MinSamples: 5, RefreshSeconds: 60,
			StabilityWeight: 1, LatencyWeight: 1, TPSWeight: 1, UsageWeight: 1, HeadroomWeight: 1,
			ProviderTiers: map[string]int{"load-a": 0, "load-b": 0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{Requested: "glm-5.3-flash", Provider: "load-a", Upstream: "glm-5.3-flash"}
	candidates := []modelConfig{
		selected,
		{Requested: selected.Requested, Provider: "load-b", Upstream: "glm-5.3-flash"},
	}
	release := server.reserveRoute("load-a", selected)
	defer release()
	ranked := server.rankCandidates(selected, candidates)
	if ranked[0].Provider != "load-b" {
		t.Fatalf("ranked providers = %#v, want idle equal-tier route first", ranked)
	}
}

func TestRecognizedClientPayload400DoesNotFeedQuarantine(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"invalid request: context window exceeded"}}`))
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"provider": {BaseURL: upstream.URL}},
		Quarantine: providerQuarantineConfig{
			Enabled: true, SignalThreshold: 2, WindowSeconds: 300, BaseSeconds: 300,
			MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1, StreamEventStallMS: 1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{Requested: "deepseek", Provider: "provider", Upstream: "deepseek"}
	for attempt := 0; attempt < 3; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
		response, err := server.doWithFallbacks(context.Background(), req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.resp.Body.Close()
	}
	if blocked, state := server.providerBlocked("provider"); blocked || state.InstabilitySignals != 0 || state.QuarantineLevel != 0 {
		t.Fatalf("recognized client 400 affected provider: %#v", state)
	}
}

func TestContextOverflow400NormalizesToRequestTooLargeAndStopsFallback(t *testing.T) {
	const overflowBody = `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1000844 tokens > 1000000 maximum"},"request_id":"req_context_limit"}`
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(overflowBody))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer fallback.Close()

	selected := modelConfig{
		Requested: "sonnet", Provider: "primary", Upstream: "sonnet",
		Fallbacks: []modelConfig{{Provider: "fallback", Upstream: "deepseek"}},
	}
	server, err := newProxyServer(config{
		DefaultProvider: "primary",
		Providers: map[string]providerConfig{
			"primary":  {BaseURL: primary.URL},
			"fallback": {BaseURL: fallback.URL},
		},
		Models: map[string]modelConfig{"sonnet": selected},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"sonnet","max_tokens":32,"messages":[{"role":"user","content":"work"}]}`))
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	gotBody := recorder.Body.Bytes()
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusRequestEntityTooLarge)
	}
	if recorder.Header().Get("X-Anthropic-Proxy-Error-Class") != "prompt_too_long" {
		t.Fatalf("error class = %q, want prompt_too_long", recorder.Header().Get("X-Anthropic-Proxy-Error-Class"))
	}
	var got struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(gotBody, &got); err != nil {
		t.Fatal(err)
	}
	if got.Error.Type != "request_too_large" || got.Error.Message != "prompt is too long: 1000844 tokens > 1000000 maximum" || got.RequestID != "req_context_limit" {
		t.Fatalf("body = %s, want normalized request_too_large envelope", gotBody)
	}
	if primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("primaryCalls=%d fallbackCalls=%d, want 1 and 0", primaryCalls, fallbackCalls)
	}
}

func TestClientDisconnectRemainsNeutralAfterStallAndRepetition(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer upstream.Close()
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"provider": {BaseURL: upstream.URL}},
		Quarantine: providerQuarantineConfig{
			Enabled: true, SignalThreshold: 2, WindowSeconds: 300, BaseSeconds: 300,
			MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1000, StreamEventStallMS: 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{Requested: "deepseek", Provider: "provider", Upstream: "deepseek"}
	cancelAttempt := func(delay time.Duration) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(delay)
			cancel()
		}()
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"deepseek"}`))
		_, _ = server.doWithFallbacks(ctx, req, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, selected)
	}
	cancelAttempt(2 * time.Millisecond)
	cancelAttempt(2 * time.Millisecond)
	if blocked, state := server.providerBlocked("provider"); blocked || state.InstabilitySignals != 0 {
		t.Fatalf("fast client cancellation affected provider: %#v", state)
	}
	cancelAttempt(30 * time.Millisecond)
	if blocked, _ := server.providerBlocked("provider"); blocked {
		t.Fatal("single stalled client cancellation quarantined provider")
	}
	cancelAttempt(30 * time.Millisecond)
	if blocked, state := server.providerBlocked("provider"); blocked || state.InstabilitySignals != 0 {
		t.Fatalf("repeated cancellations penalized provider: %#v", state)
	}
}

func TestProviderQuarantineStateIsRaceSafe(t *testing.T) {
	server := quarantineTestServer(providerQuarantineConfig{
		Enabled: true, SignalThreshold: 100000, WindowSeconds: 300, BaseSeconds: 300,
		MaxSeconds: 1800, RecoverySuccesses: 2, ExcessiveTTFBMS: 1000, StreamEventStallMS: 1000,
	})
	var workers sync.WaitGroup
	for index := 0; index < 32; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 200; iteration++ {
				server.observeProviderInstability("provider", "http_5xx")
				server.observeProviderHealthy("provider")
				server.providerBlocked("provider")
			}
		}()
	}
	workers.Wait()
}

func quarantineTestServer(policy providerQuarantineConfig) *proxyServer {
	return &proxyServer{
		cfg: config{
			Providers:  map[string]providerConfig{"provider": {}},
			Quarantine: policy,
		},
		providerStates: map[string]providerRuntimeState{},
	}
}

func expireProviderQuarantine(server *proxyServer, provider string) {
	server.providerStateMu.Lock()
	state := server.providerStates[provider]
	state.QuarantinedUntil = time.Now().Add(-time.Second)
	server.providerStates[provider] = state
	server.providerStateMu.Unlock()
	server.providerBlocked(provider)
}

func TestXAIOAuthStateRoundTripUsesPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "xai-oauth.json")
	want := xaiOAuthState{
		Version:       xaiOAuthStateVersion,
		AuthMode:      "xai-oauth",
		AccessToken:   "access-secret",
		RefreshToken:  "refresh-secret",
		TokenEndpoint: xaiOAuthIssuer + "/oauth2/token",
		ExpiresAt:     time.Now().Add(time.Hour).UTC().Truncate(time.Second),
	}
	if err := writeXAIOAuthState(path, want); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("permissions = %04o, want 0600", info.Mode().Perm())
	}
	got, err := readXAIOAuthState(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != want.AccessToken || got.RefreshToken != want.RefreshToken || !got.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("state = %#v, want persisted tokens and expiry", got)
	}
}

func TestXAIOAuthStateRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.json")
	link := filepath.Join(dir, "xai-oauth.json")
	if err := os.WriteFile(target, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readXAIOAuthState(link); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("read error = %v, want symlink rejection", err)
	}
	if err := writeXAIOAuthState(link, xaiOAuthState{}); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("write error = %v, want symlink rejection", err)
	}
}

func TestXAIOAuthRefreshRotatesAndPersistsTokens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "xai-oauth.json")
	if err := writeXAIOAuthState(path, xaiOAuthState{
		Version:       xaiOAuthStateVersion,
		AuthMode:      "xai-oauth",
		AccessToken:   "expired-access",
		RefreshToken:  "old-refresh",
		TokenEndpoint: xaiOAuthIssuer + "/oauth2/token",
		ExpiresAt:     time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	manager := newXAIOAuthManager(path, testHTTPDoer(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != xaiOAuthIssuer+"/oauth2/token" {
			t.Fatalf("refresh URL = %q", request.URL.String())
		}
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("grant_type") != "refresh_token" || request.Form.Get("client_id") != xaiOAuthClientID || request.Form.Get("refresh_token") != "old-refresh" {
			t.Fatalf("refresh form = %#v", request.Form)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"access_token":"new-access","refresh_token":"new-refresh","token_type":"Bearer","expires_in":3600}`)),
		}, nil
	}))
	token, err := manager.accessToken(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "new-access" {
		t.Fatalf("access token = %q, want new-access", token)
	}
	persisted, err := readXAIOAuthState(path)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.AccessToken != "new-access" || persisted.RefreshToken != "new-refresh" || persisted.LastRefresh.IsZero() {
		t.Fatalf("persisted state = %#v, want rotated tokens", persisted)
	}
}

func TestXAIOAuthHeadersStripClientCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "xai-oauth.json")
	if err := writeXAIOAuthState(path, xaiOAuthState{
		Version:      xaiOAuthStateVersion,
		AuthMode:     "xai-oauth",
		AccessToken:  "subscription-access",
		RefreshToken: "subscription-refresh",
		ExpiresAt:    time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	server := &proxyServer{
		cfg: config{Providers: map[string]providerConfig{
			"xai": {AuthMode: "xai-oauth", AuthHeader: "Authorization", AuthPrefix: "Bearer "},
		}},
		xaiOAuth: map[string]*xaiOAuthManager{"xai": newXAIOAuthManager(path, nil)},
	}
	headers := http.Header{
		"Authorization": []string{"Bearer client-token"},
		"x-api-key":     []string{"client-api-key"},
	}
	if err := server.applyProviderHeadersContext(context.Background(), "xai", headers); err != nil {
		t.Fatal(err)
	}
	if headers.Get("Authorization") != "Bearer subscription-access" || headers.Get("x-api-key") != "" {
		t.Fatalf("headers = %#v, want only subscription OAuth", headers)
	}
}

func TestXAIOAuthRetriesOnceAfterUnauthorized(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "xai-oauth.json")
	if err := writeXAIOAuthState(statePath, xaiOAuthState{
		Version:       xaiOAuthStateVersion,
		AuthMode:      "xai-oauth",
		AccessToken:   "old-access",
		RefreshToken:  "old-refresh",
		TokenEndpoint: xaiOAuthIssuer + "/oauth2/token",
		ExpiresAt:     time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	var inferenceCalls atomic.Int32
	inference := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		inferenceCalls.Add(1)
		switch request.Header.Get("Authorization") {
		case "Bearer old-access":
			http.Error(writer, "expired", http.StatusUnauthorized)
		case "Bearer new-access":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
		default:
			http.Error(writer, "unexpected auth", http.StatusForbidden)
		}
	}))
	defer inference.Close()

	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{
			"xai": {
				BaseURL:       inference.URL + "/v1",
				Format:        "openai-chat",
				AuthMode:      "xai-oauth",
				AuthStatePath: statePath,
				AuthHeader:    "Authorization",
				AuthPrefix:    "Bearer ",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.xaiOAuth["xai"].client = testHTTPDoer(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"access_token":"new-access","refresh_token":"new-refresh","token_type":"Bearer","expires_in":3600}`)),
		}, nil
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"grok-build-0.1","messages":[]}`))
	response, err := server.doUpstream(context.Background(), request, []byte(`{"model":"grok-build-0.1","messages":[]}`), modelConfig{
		Requested: "grok-build-0.1",
		Provider:  "xai",
		Upstream:  "grok-build-0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || inferenceCalls.Load() != 2 {
		t.Fatalf("status=%d calls=%d, want 200 and one retry", response.StatusCode, inferenceCalls.Load())
	}
}

func TestXAIUsageReadsSuperGrokWeeklyBilling(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "xai-oauth.json")
	if err := writeXAIOAuthState(statePath, xaiOAuthState{
		Version:       xaiOAuthStateVersion,
		AuthMode:      "xai-oauth",
		AccessToken:   "subscription-access",
		RefreshToken:  "subscription-refresh",
		TokenEndpoint: xaiOAuthIssuer + "/oauth2/token",
		ExpiresAt:     time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	reset := time.Now().UTC().Add(7 * 24 * time.Hour).Truncate(time.Second)
	var calls atomic.Int32
	billing := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != "/v1/billing" || request.URL.Query().Get("format") != "credits" {
			t.Fatalf("billing URL = %s", request.URL.String())
		}
		for key, want := range map[string]string{
			"Authorization":            "Bearer subscription-access",
			"Accept":                   "application/json",
			"X-XAI-Token-Auth":         "xai-grok-cli",
			"X-Grok-Client-Identifier": "grok-shell",
		} {
			if got := request.Header.Get(key); got != want {
				t.Fatalf("header %s = %q, want %q", key, got, want)
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(writer, `{"config":{"creditUsagePercent":18,"currentPeriod":{"start":%q,"end":%q,"type":"USAGE_PERIOD_TYPE_WEEKLY"},"productUsage":[{"product":"GrokBuild","usagePercent":16},{"product":"Api","usagePercent":2}],"isUnifiedBillingUser":false}}`, reset.Add(-7*24*time.Hour).Format(time.RFC3339), reset.Format(time.RFC3339))
	}))
	defer billing.Close()
	server, err := newProxyServer(config{Providers: map[string]providerConfig{
		"xai-oauth": {
			BaseURL:       billing.URL,
			Format:        "openai-chat",
			AuthMode:      "xai-oauth",
			AuthStatePath: statePath,
			AuthHeader:    "Authorization",
			AuthPrefix:    "Bearer ",
			Headers: map[string]string{
				"X-XAI-Token-Auth":         "xai-grok-cli",
				"X-Grok-Client-Identifier": "grok-shell",
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := server.fetchXAIUsageSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || snapshot.BillingKind != "weekly" || snapshot.Windows["weekly"].PercentUsed != 18 || snapshot.Products["GrokBuild"] != 16 {
		t.Fatalf("calls=%d snapshot=%#v", calls.Load(), snapshot)
	}
}

func TestXAIUsageParsesUnifiedMonthlyBilling(t *testing.T) {
	start := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second)
	end := start.Add(30 * 24 * time.Hour)
	snapshot, err := parseXAIMonthlyUsage(xaiBillingConfig{
		MonthlyLimit:       xaiBillingAmount{Val: 15000},
		Used:               xaiBillingAmount{Val: 10500},
		BillingPeriodStart: start.Format(time.RFC3339),
		BillingPeriodEnd:   end.Format(time.RFC3339),
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.BillingKind != "monthly" || snapshot.Windows["monthly"].PercentUsed != 70 || !snapshot.Windows["monthly"].ResetsAt.Equal(end) {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestXAISubscriptionOriginRejectsDeveloperAPI(t *testing.T) {
	if err := validateXAISubscriptionOrigin("https://cli-chat-proxy.grok.com", "inference baseURL"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"https://api.x.ai", "http://cli-chat-proxy.grok.com", "https://cli-chat-proxy.grok.com.evil.test"} {
		if err := validateXAISubscriptionOrigin(raw, "inference baseURL"); err == nil {
			t.Fatalf("origin %q unexpectedly accepted", raw)
		}
	}
}

func TestNamedChainsCompileAndEnforceContracts(t *testing.T) {
	vision := true
	tier := 0
	base := func() config {
		return config{
			DefaultProvider: "provider",
			Providers:       map[string]providerConfig{"provider": {BaseURL: "https://provider.test"}, "backup": {BaseURL: "https://backup.test"}},
			Models: map[string]modelConfig{
				"canonical": {
					Provider: "provider", Upstream: "glm-5.3-flash", ResponseAlias: "glm-5.3-flash[1m]", ContextWindow: 1_000_000,
					Tier: &tier, SupportsImages: &vision,
					Fallbacks: []modelConfig{{Provider: "backup", Upstream: "glm-5.3-flash", ContextWindow: 1_000_000}},
				},
				"friendly": {Chain: "glm-flash"},
			},
			Chains: map[string]routeChain{
				"glm-flash": {Model: "canonical", Family: "glm", ContextWindow: 1_000_000, SupportsImages: &vision},
			},
		}
	}

	t.Run("compile", func(t *testing.T) {
		cfg := base()
		if err := compileNamedChains(&cfg); err != nil {
			t.Fatal(err)
		}
		compiled := cfg.Models["friendly"]
		if compiled.Provider != "provider" || compiled.Upstream != "glm-5.3-flash" || len(compiled.Fallbacks) != 1 {
			t.Fatalf("compiled chain = %#v", compiled)
		}
		compiled.Fallbacks[0].Provider = "mutated"
		if cfg.Models["canonical"].Fallbacks[0].Provider != "backup" {
			t.Fatal("compiled chain shares mutable fallback storage with canonical route")
		}
	})

	for name, mutate := range map[string]func(*config){
		"cycle": func(cfg *config) {
			cfg.Chains["glm-flash"] = routeChain{Model: "friendly", Family: "glm", ContextWindow: 1_000_000}
		},
		"family": func(cfg *config) {
			chain := cfg.Chains["glm-flash"]
			chain.Family = "deepseek"
			cfg.Chains["glm-flash"] = chain
		},
		"context": func(cfg *config) {
			chain := cfg.Chains["glm-flash"]
			chain.ContextWindow = 1_000_001
			cfg.Chains["glm-flash"] = chain
		},
		"duplicate": func(cfg *config) {
			model := cfg.Models["canonical"]
			model.Fallbacks[0] = modelConfig{Provider: "provider", Upstream: "glm-5.3-flash", ContextWindow: 1_000_000}
			cfg.Models["canonical"] = model
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			mutate(&cfg)
			if err := compileNamedChains(&cfg); err == nil {
				t.Fatalf("invalid %s chain accepted", name)
			}
		})
	}
}

func TestImageCompatibilityRejectsKnownTextOnlyChain(t *testing.T) {
	noVision := false
	candidates, changed := imageCompatibleCandidates([]modelConfig{
		{Provider: "one", Upstream: "glm-5.3", SupportsImages: &noVision},
		{Provider: "two", Upstream: "glm-5.3", SupportsImages: &noVision},
	})
	if !changed || len(candidates) != 0 {
		t.Fatalf("candidates=%#v changed=%v, want empty incompatible route", candidates, changed)
	}
}

func TestRotatingLogWriterPreservesBoundedBackups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.log")
	writer, err := newRotatingLogWriter(loggingConfig{Path: path, MaxBytes: 12, Backups: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"first-line\n", "second-line\n", "third-line\n"} {
		if _, err := writer.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]string{path: "third-line\n", path + ".1": "second-line\n", path + ".2": "first-line\n"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != want {
			t.Fatalf("%s = %q, want %q", file, body, want)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("log mode = %o, want 600", info.Mode().Perm())
	}
}

func TestRotatingLogWriterTightensExistingFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.log")
	if err := os.WriteFile(path, []byte("legacy\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	writer, err := newRotatingLogWriter(loggingConfig{Path: path, MaxBytes: 1024, Backups: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("log mode = %o, want 600", info.Mode().Perm())
	}
}

func TestClaudeProfilesUseIsolatedHTTPTransports(t *testing.T) {
	primary := testClaudeProfile(t, "teamA", "transport-accountA", "transport-account-accountA")
	secondary := testClaudeProfile(t, "personal", "transport-personal", "transport-account-personal")
	server, err := newProxyServer(config{
		Providers: map[string]providerConfig{"anthropic": {BaseURL: "https://api.anthropic.com"}},
		ClaudeUsage: claudeUsageConfig{
			Provider:         "anthropic",
			ListenerProfiles: map[string]claudeUsageProfile{"48104": primary},
			AccountProfiles:  map[string]claudeUsageProfile{"personal": secondary},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	primaryClient, ok := server.upstreamClient("anthropic", withSelectedClaudeProfile(request, primary))
	if !ok {
		t.Fatal("primary Claude client missing")
	}
	secondaryClient, ok := server.upstreamClient("anthropic", withSelectedClaudeProfile(request, secondary))
	if !ok {
		t.Fatal("secondary Claude client missing")
	}
	primaryAgain, _ := server.upstreamClient("anthropic", withSelectedClaudeProfile(request, primary))

	if primaryClient != primaryAgain {
		t.Fatal("same Claude profile did not reuse its client")
	}
	if primaryClient == secondaryClient || primaryClient.Transport == secondaryClient.Transport {
		t.Fatal("distinct Claude profiles share an HTTP transport")
	}
	if primaryClient.Transport == server.clients["anthropic"].Transport || secondaryClient.Transport == server.clients["anthropic"].Transport {
		t.Fatal("Claude profile reused the provider-wide HTTP transport")
	}
}

func TestFallbackRetriesSSEFailureBeforeFirstEvent(t *testing.T) {
	circuitBreakers.Delete("failed|worker")
	t.Cleanup(func() { circuitBreakers.Delete("failed|worker") })
	var failedCalls atomic.Int32
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failedCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
	}))
	defer failed.Close()

	const stream = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	var healthyCalls atomic.Int32
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		healthyCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, stream)
	}))
	defer healthy.Close()

	server, err := newProxyServer(config{Providers: map[string]providerConfig{
		"failed":  {BaseURL: failed.URL},
		"healthy": {BaseURL: healthy.URL},
	}})
	if err != nil {
		t.Fatal(err)
	}
	selected := modelConfig{
		Requested: "worker", Provider: "failed", Upstream: "worker",
		Fallbacks: []modelConfig{{Provider: "healthy", Upstream: "worker"}},
	}
	body := []byte(`{"model":"worker","messages":[{"role":"user","content":"hello"}]}`)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:48104/v1/messages", nil)
	response, err := server.doWithFallbacks(context.Background(), request, body, map[string]any{"model": "worker"}, selected)
	if err != nil {
		t.Fatal(err)
	}
	defer response.resp.Body.Close()
	got, err := io.ReadAll(response.resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.providerName != "healthy" || string(got) != stream {
		t.Fatalf("provider=%q body=%q, want healthy preserved stream", response.providerName, got)
	}
	if failedCalls.Load() != 1 || healthyCalls.Load() != 1 {
		t.Fatalf("calls failed=%d healthy=%d, want 1/1", failedCalls.Load(), healthyCalls.Load())
	}
}
