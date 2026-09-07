package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerReserveRoutesSequentiallyWithoutPaidAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		second                float64
		flashStatus           int
		wantClaude, wantFlash int32
	}{
		{"next account", 30, 200, 1, 0},
		{"flash fallback", 80, 200, 0, 1},
		{"reserve does not authorize paid", 80, 429, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var claude, flash, paid atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/flash":
					flash.Add(1)
					if tc.flashStatus != 200 {
						w.WriteHeader(tc.flashStatus)
						io.WriteString(w, `{"error":{"message":"quota exceeded"}}`)
						return
					}
				case "/paid":
					paid.Add(1)
				default:
					claude.Add(1)
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, testMessageResponse)
			}))
			defer upstream.Close()
			f := testClaudeConfig(t)
			pool := f.AccountPools["shared"]
			pool.WorkerUtilizationLimitPct = 75
			f.AccountPools["shared"] = pool
			m := f.Models["model"]
			m.Upstream = "claude-sonnet-5"
			f.Models["model"] = m
			p := f.Providers["endpoint"]
			p.Usage = usageDefinition{Mode: "api", PollIntervalSeconds: 60}
			f.Providers["endpoint"] = p
			f.Providers["flash"] = providerDefinition{Protocol: "anthropic", BaseURL: "https://example.invalid", MessagesPath: "/flash", Billing: "subscription", Auth: authDefinition{Type: "env", Name: "WORKER_TEST_KEY"}}
			p = f.Providers["flash"]
			p.Billing = "metered"
			p.MessagesPath = "/paid"
			f.Providers["paid"] = p
			m.Provider = "flash"
			m.Upstream = "glm-5.3-flash"
			f.Models["flash"] = m
			m.Provider = "paid"
			f.Models["paid"] = m
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "flash"}, {Model: "paid"}}, AllowPaidFallback: true, PaidFallbackOn: []string{"quota-exhausted"}}
			t.Setenv("WORKER_TEST_KEY", "test-only-token")
			cfg := compileFixture(t, f)
			for id, p := range cfg.Providers {
				p.BaseURL = upstream.URL
				cfg.Providers[id] = p
			}
			seedTestCredentials(t, cfg)
			s := testProxy(t, cfg)
			s.claudeUse.snapshots = map[string]claudeUsageSnapshot{}
			for name, use := range map[string]float64{"first": 80, "second": tc.second, "separate": 80} {
				s.claudeUse.snapshots[name] = claudeUsageSnapshot{Profile: name, Source: "oauth-api", FetchedAt: time.Now(), FiveHour: claudeUsageWindow{Utilization: use, ResetsAt: time.Now().Add(time.Hour)}}
			}
			out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
			if claude.Load() != tc.wantClaude || flash.Load() != tc.wantFlash || paid.Load() != 0 {
				t.Fatalf("calls claude=%d flash=%d paid=%d status=%d", claude.Load(), flash.Load(), paid.Load(), out.Code)
			}
			if tc.flashStatus == 200 && out.Code != 200 {
				t.Fatalf("status %d: %s", out.Code, out.Body.String())
			}
		})
	}
}

func TestWorkerBudget(t *testing.T) {
	now := time.Now().UTC()
	f := testClaudeConfig(t)
	model := f.Models["model"]
	model.Upstream = "claude-sonnet-5"
	f.Models["model"] = model
	p := f.AccountPools["shared"]
	p.WorkerUtilizationLimitPct = 75
	f.AccountPools["shared"] = p
	s := testProxy(t, compileFixture(t, f))
	s.clockNow = func() time.Time { return now }
	m := s.cfg.Models["test"]
	m.Upstream = "claude-sonnet-5"
	base := claudeUsageSnapshot{Profile: "first", Source: "oauth-api", FetchedAt: now,
		FiveHour: claudeUsageWindow{Utilization: 30, ResetsAt: now.Add(time.Hour)},
		SevenDay: claudeUsageWindow{Utilization: 30, ResetsAt: now.Add(24 * time.Hour)}}
	for _, tc := range []struct {
		name   string
		modify func(*claudeUsageSnapshot, *modelConfig)
		want   string
	}{
		{"below limit", func(*claudeUsageSnapshot, *modelConfig) {}, ""},
		{"five hour cap", func(v *claudeUsageSnapshot, _ *modelConfig) { v.FiveHour.Utilization = 75 }, "worker_reserve"},
		{"weekly cap", func(v *claudeUsageSnapshot, _ *modelConfig) { v.SevenDay.Utilization = 75 }, "worker_reserve"},
		{"opus retains reserve", func(v *claudeUsageSnapshot, m *modelConfig) {
			v.FiveHour.Utilization = 90
			m.Upstream = "claude-opus-4-8"
		}, ""},
		{"fable retains reserve", func(v *claudeUsageSnapshot, m *modelConfig) {
			v.SevenDay.Utilization = 90
			m.Upstream = "claude-fable-5-1"
		}, ""},
		{"expired window", func(v *claudeUsageSnapshot, _ *modelConfig) {
			v.FiveHour.Utilization = 100
			v.FiveHour.ResetsAt = now.Add(-time.Second)
		}, ""},
		{"unknown stays unknown", func(v *claudeUsageSnapshot, _ *modelConfig) { v.Source = "unknown" }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, model := base, m
			tc.modify(&v, &model)
			if got := s.claudeWorkerReserveReason(v, model); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name               string
		minutes            int
		growth             float64
		stale, reset, week bool
		want               string
	}{
		{"fast five hour", 20, 20, false, false, false, "worker_pace_reserve"},
		{"slow five hour", 20, 1, false, false, false, ""},
		{"fast weekly", 20, 2, false, false, true, "worker_pace_reserve"},
		{"short sample", 5, 20, false, false, false, ""},
		{"stale", 20, 20, true, false, false, ""},
		{"new window", 20, 20, false, true, false, ""},
		{"old sample", 40, 20, false, false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, old := base, base
			old.FetchedAt = now.Add(-time.Duration(tc.minutes) * time.Minute)
			if tc.week {
				old.SevenDay.Utilization -= tc.growth
			} else {
				old.FiveHour.Utilization -= tc.growth
			}
			if tc.stale {
				v.Source = "oauth-api-stale"
			}
			if tc.reset {
				old.FiveHour.ResetsAt = now.Add(-time.Hour)
			}
			s.claudeUse.workerHistory = map[string][]claudeUsageSnapshot{"first": {old}}
			if got := s.claudeWorkerReserveReason(v, m); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestWorkerHistoryBoundedAndIndependent(t *testing.T) {
	s := &proxyServer{}
	now := time.Now()
	for i := 0; i < 120; i++ {
		s.rememberWorkerUsageLocked(claudeUsageSnapshot{Profile: "first", Source: "oauth-api", FetchedAt: now.Add(time.Duration(i) * time.Minute)})
	}
	if got := len(s.claudeUse.workerHistory["first"]); got != 7 {
		t.Fatalf("history size %d", got)
	}
	s.rememberWorkerUsageLocked(claudeUsageSnapshot{Profile: "second", Source: "oauth-api-stale", FetchedAt: now})
	if len(s.claudeUse.workerHistory["second"]) != 0 {
		t.Fatal("stale observation recorded")
	}
}
