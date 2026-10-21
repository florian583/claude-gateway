package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func pinnedWorkerFixture(t *testing.T) fileConfig {
	for _, account := range []string{"first", "second"} {
		for _, model := range []string{"claude-haiku-5-5", "claude-sonnet-5-5"} {
			key := "endpoint@" + account + "|" + model
			circuitBreakers.Delete(key)
			t.Cleanup(func() { circuitBreakers.Delete(key) })
		}
	}
	f := testClaudeConfig(t)
	m := f.Models["model"]
	m.Upstream = "claude-haiku-5-5"
	f.Models["model"] = m
	m.Upstream = "claude-sonnet-5-5"
	f.Models["sonnet"] = m
	f.Providers["backup"] = providerDefinition{Protocol: "anthropic", BaseURL: "http://127.0.0.1:1", Billing: "local", Auth: authDefinition{Type: "none"}}
	m.Provider, m.Upstream = "backup", "glm-5.3-flash"
	f.Models["glm"] = m
	f.Chains["main"] = chainDefinition{AccountScope: "pinned", Steps: []chainStep{{Model: "model"}, {Model: "sonnet"}, {Model: "glm"}}}
	return f
}

func TestPinnedWorkerExpansion(t *testing.T) {
	for _, scope := range []string{"pinned", ""} {
		t.Run(scope, func(t *testing.T) {
			f := pinnedWorkerFixture(t)
			chain := f.Chains["main"]
			chain.AccountScope = scope
			f.Chains["main"] = chain
			s := testProxy(t, compileFixture(t, f))
			r := withSelectedClaudeProfile(httptest.NewRequest("POST", "/v1/messages", nil), s.cfg.ClaudeUsage.AccountProfiles["second"])
			attempts := s.expandConfiguredClaudeAttempts(r, s.collectCandidates(s.cfg.Models["test"]))
			var got []string
			for _, a := range attempts {
				got = append(got, a.model.Upstream+"@"+a.profile.Name)
			}
			want := "claude-haiku-5-5@second,claude-sonnet-5-5@second,glm-5.3-flash@"
			if scope == "" {
				want = "claude-haiku-5-5@second,claude-haiku-5-5@first,claude-sonnet-5-5@second,claude-sonnet-5-5@first,glm-5.3-flash@"
			}
			if strings.Join(got, ",") != want {
				t.Fatalf("attempts=%v", got)
			}
		})
	}
}

func TestPinnedWorkerFailoverPreservesLease(t *testing.T) {
	for _, status := range []int{429, 500, 503} {
		for _, sonnetOK := range []bool{true, false} {
			t.Run(http.StatusText(status)+map[bool]string{true: "-sonnet", false: "-glm"}[sonnetOK], func(t *testing.T) {
				var mu sync.Mutex
				var calls []string
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body struct {
						Model string `json:"model"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					account := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer sk-ant-oat-test-")
					mu.Lock()
					calls = append(calls, body.Model+"@"+account)
					mu.Unlock()
					if body.Model == "claude-haiku-5-5" || (body.Model == "claude-sonnet-5-5" && !sonnetOK) {
						w.WriteHeader(status)
						io.WriteString(w, `{"error":{"type":"overloaded_error","message":"busy"}}`)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, testMessageResponse)
				}))
				defer upstream.Close()
				cfg := compileFixture(t, pinnedWorkerFixture(t))
				for _, name := range []string{"endpoint", "backup"} {
					p := cfg.Providers[name]
					p.BaseURL = upstream.URL
					cfg.Providers[name] = p
				}
				seedTestCredentials(t, cfg)
				s := testProxy(t, cfg)
				payload := map[string]any{"model": "test", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
				selected := cfg.Models["test"]
				selected.Requested = "test"
				key := routingStickyKey(selected, payload)
				s.refreshClaudeAccountLease(key, cfg.ClaudeUsage.AccountProfiles["second"])
				for n := 0; n < 2; n++ {
					out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
					if out.Code != 200 {
						t.Fatalf("status=%d body=%s", out.Code, out.Body.String())
					}
					s.claudeAccountMu.Lock()
					pin := s.claudeAccounts[key]
					s.claudeAccountMu.Unlock()
					if pin.Profile.Name != "second" {
						t.Fatalf("pin moved: %s", pin.Profile.Name)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				want := "claude-haiku-5-5@second,claude-sonnet-5-5@second"
				if !sonnetOK {
					want += ",glm-5.3-flash@"
				}
				if !strings.HasPrefix(strings.Join(calls, ","), want) || strings.Contains(strings.Join(calls, ","), "@first") {
					t.Fatalf("calls=%v", calls)
				}
			})
		}
	}
}

func TestPinnedWorkerSelection(t *testing.T) {
	for _, reason := range []string{"auth_unavailable", "auth_exhausted", "auth_quarantined", "quota_or_rate_limit", "model_block", "instability_http_5xx", ""} {
		t.Run(reason, func(t *testing.T) {
			cfg := compileFixture(t, pinnedWorkerFixture(t))
			s := testProxy(t, cfg)
			p := cfg.ClaudeUsage.AccountProfiles["second"]
			s.refreshClaudeAccountLease("client", p)
			if reason == "model_block" {
				s.markProviderBlocked("endpoint@second#model:claude-haiku-5-5", "quota_or_rate_limit", time.Minute)
			} else if reason != "" {
				s.markProviderBlocked("endpoint@second", reason, time.Minute)
			}
			chosen, _, release, err := s.selectClaudeProfile(context.Background(), httptest.NewRequest("POST", "/v1/messages", nil), "client", true, cfg.Models["test"])
			if release != nil {
				defer release()
			}
			if err != nil {
				t.Fatal(err)
			}
			want := "second"
			if reason == "auth_quarantined" || reason == "auth_unavailable" || reason == "auth_exhausted" || reason == "quota_or_rate_limit" {
				want = "first"
			}
			if chosen.Name != want || s.claudeAccounts["client"].Profile.Name != want {
				t.Fatalf("chosen=%s lease=%s", chosen.Name, s.claudeAccounts["client"].Profile.Name)
			}
		})
	}
}

func TestPinnedWorkerRejectsInvalidScope(t *testing.T) {
	f := pinnedWorkerFixture(t)
	c := f.Chains["main"]
	c.AccountScope = "random"
	f.Chains["main"] = c
	b, _ := json.Marshal(f)
	if _, err := decodeFileConfig("/tmp/config.json", b); err == nil {
		t.Fatal("accepted invalid account scope")
	}
}

func TestPinnedWorkerQuotaAndNoAvailableAccount(t *testing.T) {
	f := pinnedWorkerFixture(t)
	p := f.Providers["endpoint"]
	p.Usage = usageDefinition{Mode: "api", PollIntervalSeconds: 60}
	f.Providers["endpoint"] = p
	cfg := compileFixture(t, f)
	s := testProxy(t, cfg)
	s.refreshClaudeAccountLease("client", cfg.ClaudeUsage.AccountProfiles["second"])
	s.claudeUse.snapshots = map[string]claudeUsageSnapshot{
		"second": {Profile: "second", Source: "oauth-api", FetchedAt: time.Now(), FiveHour: claudeUsageWindow{Utilization: 100, ResetsAt: time.Now().Add(time.Hour)}},
	}
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	chosen, _, _, err := s.selectClaudeProfile(context.Background(), r, "client", false, cfg.Models["test"])
	if err != nil || chosen.Name != "first" || s.claudeAccounts["client"].Profile.Name != "first" {
		t.Fatalf("chosen=%s err=%v", chosen.Name, err)
	}
	s.markProviderBlocked("endpoint@first", "auth_unavailable", time.Minute)
	if _, _, _, err := s.selectClaudeProfile(context.Background(), r, "client", false, cfg.Models["test"]); err == nil {
		t.Fatal("selected unusable account")
	}
	attempts := s.expandConfiguredClaudeAttempts(r, s.collectCandidates(cfg.Models["test"]))
	if len(attempts) != 1 || attempts[0].model.Upstream != "glm-5.3-flash" {
		t.Fatalf("unexpected attempts: %v", attempts)
	}
}

func TestPinnedWorkerRejectsMixedAccountPools(t *testing.T) {
	f := pinnedWorkerFixture(t)
	m := f.Models["sonnet"]
	m.AccountPool = "separate"
	f.Models["sonnet"] = m
	b, _ := json.Marshal(f)
	if _, err := decodeFileConfig("/tmp/config.json", b); err == nil {
		t.Fatal("accepted different Claude pools")
	}
}

func TestPinnedWorkerIgnoresClassifierReordering(t *testing.T) {
	f := pinnedWorkerFixture(t)
	m := f.Models["sonnet"]
	m.TaskClass = "deep"
	f.Models["sonnet"] = m
	s := testProxy(t, compileFixture(t, f))
	selected := s.cfg.Models["test"]
	ranked := s.rankCandidatesForRequest(selected, s.collectCandidates(selected), "conversation", true, thinkingHistoryNone, taskRouteHint{Decided: true, Tier: "deep"})
	if len(ranked) != 3 || ranked[0].Upstream != "claude-haiku-5-5" || ranked[1].Upstream != "claude-sonnet-5-5" || ranked[2].Upstream != "glm-5.3-flash" {
		t.Fatalf("unexpected order: %+v", ranked)
	}
}
