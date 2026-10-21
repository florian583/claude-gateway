package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFableBudgetFallsThroughToOpus(t *testing.T) {
	var fable, opus atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v struct{ Model string }
		_ = json.NewDecoder(r.Body).Decode(&v)
		switch v.Model {
		case "claude-fable-5-1":
			fable.Add(1)
		case "claude-opus-5":
			opus.Add(1)
		default:
			t.Errorf("unexpected model %q", v.Model)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testMessageResponse)
	}))
	defer upstream.Close()
	f := testClaudeConfig(t)
	p := f.AccountPools["shared"]
	p.FableMinWeeklyRemainingPct = 65
	f.AccountPools["shared"] = p
	m := f.Models["model"]
	m.Upstream = "claude-fable-5-1"
	f.Models["model"] = m
	m.Upstream = "claude-opus-5"
	f.Models["opus"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "opus"}}}
	pd := f.Providers["endpoint"]
	pd.Usage = usageDefinition{Mode: "api", PollIntervalSeconds: 60}
	f.Providers["endpoint"] = pd
	cfg := compileFixture(t, f)
	provider := cfg.Providers["endpoint"]
	provider.BaseURL = upstream.URL
	cfg.Providers["endpoint"] = provider
	seedTestCredentials(t, cfg)
	s := testProxy(t, cfg)
	s.claudeUse.snapshots = map[string]claudeUsageSnapshot{}
	for _, name := range []string{"first", "second"} {
		s.claudeUse.snapshots[name] = claudeUsageSnapshot{Profile: name, Source: "oauth-api", FetchedAt: time.Now(), SevenDay: claudeUsageWindow{Utilization: 50, ResetsAt: time.Now().Add(time.Hour)}}
	}
	out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	if out.Code != 200 || fable.Load() != 0 || opus.Load() != 1 {
		t.Fatalf("status=%d Fable=%d Opus=%d body=%s", out.Code, fable.Load(), opus.Load(), out.Body.String())
	}
}

func TestFableWeeklyBudget(t *testing.T) {
	now := time.Now()
	f := testClaudeConfig(t)
	model := f.Models["model"]
	model.Upstream = "claude-fable-5-1"
	f.Models["model"] = model
	opus := model
	opus.Upstream = "claude-opus-5"
	f.Models["opus"] = opus
	p := f.AccountPools["shared"]
	p.FableMinWeeklyRemainingPct = 65
	p.FablePaceGuard = true
	f.AccountPools["shared"] = p
	s := testProxy(t, compileFixture(t, f))
	s.clockNow = func() time.Time { return now }
	m := s.cfg.Models["test"]
	m.Upstream = "claude-fable-5-1"
	base := claudeUsageSnapshot{Profile: "first", Source: "oauth-api", FetchedAt: now, SevenDay: claudeUsageWindow{Utilization: 35, ResetsAt: now.Add(24 * time.Hour)}}
	for _, tc := range []struct {
		name     string
		used     float64
		upstream string
		source   string
		age      time.Duration
		reset    time.Duration
		want     string
	}{
		{"boundary", 35, "claude-fable-5-1", "oauth-api", 0, time.Hour, ""},
		{"below remaining", 36, "claude-fable-5-1", "oauth-api", 0, time.Hour, "fable_weekly_reserve"},
		{"opus still eligible", 90, "claude-opus-5", "oauth-api", 0, time.Hour, ""},
		{"unknown", 90, "claude-fable-5-1", "unknown", 0, time.Hour, ""},
		{"stale", 90, "claude-fable-5-1", "oauth-api", time.Hour, time.Hour, ""},
		{"reset", 90, "claude-fable-5-1", "oauth-api", 0, -time.Second, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := base
			v.SevenDay.Utilization = tc.used
			v.Source = tc.source
			v.FetchedAt = now.Add(-tc.age)
			v.SevenDay.ResetsAt = now.Add(tc.reset)
			model := m
			model.Upstream = tc.upstream
			if got := s.claudeWorkerReserveReason(v, model); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
	old := base
	old.FetchedAt = now.Add(-20 * time.Minute)
	old.SevenDay.Utilization = 30
	s.claudeUse.workerHistory = map[string][]claudeUsageSnapshot{"first": {old}}
	if got := s.claudeWorkerReserveReason(base, m); got != "fable_weekly_pace_reserve" {
		t.Fatalf("forecast %q", got)
	}
	old.SevenDay.ResetsAt = now.Add(-time.Hour)
	s.claudeUse.workerHistory["first"] = []claudeUsageSnapshot{old}
	if got := s.claudeWorkerReserveReason(base, m); got != "" {
		t.Fatalf("cross-reset forecast %q", got)
	}
}

func TestFableFivePercentWeeklyReserveIgnoresFiveHourAndPace(t *testing.T) {
	now := time.Now()
	f := testClaudeConfig(t)
	p := f.AccountPools["shared"]
	p.FableMinWeeklyRemainingPct = 5
	p.FablePaceGuard = false
	f.AccountPools["shared"] = p
	s := testProxy(t, compileFixture(t, f))
	s.clockNow = func() time.Time { return now }
	m := s.cfg.Models["test"]
	m.Upstream = "claude-fable-5-1"
	v := claudeUsageSnapshot{Profile: "first", Source: "oauth-api", FetchedAt: now,
		FiveHour: claudeUsageWindow{Utilization: 99},
		SevenDay: claudeUsageWindow{Utilization: 94, ResetsAt: now.Add(24 * time.Hour)}}
	old := v
	old.FetchedAt = now.Add(-20 * time.Minute)
	old.SevenDay.Utilization = 50
	s.claudeUse.workerHistory = map[string][]claudeUsageSnapshot{"first": {old}}
	for _, tc := range []struct {
		used float64
		want string
	}{
		{94, ""}, {95, ""}, {95.01, "fable_weekly_reserve"},
	} {
		v.SevenDay.Utilization = tc.used
		if got := s.claudeFableReserveReason(v, m); got != tc.want {
			t.Fatalf("weekly used %.2f: got %q want %q", tc.used, got, tc.want)
		}
	}
}

func TestMenuModelDiscovery(t *testing.T) {
	f := testClaudeConfig(t)
	f.Client.MenuModels = map[string]string{"test": "Opus 5 (1M)"}
	s := testProxy(t, compileFixture(t, f))
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/models", nil))
	var v struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"display_name"`
		}
		HasMore bool `json:"has_more"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil || w.Code != 200 || len(v.Data) != 1 || v.Data[0].ID != "test" || v.Data[0].Name != "Opus 5 (1M)" || v.HasMore {
		t.Fatalf("discovery %d %s", w.Code, w.Body.String())
	}
}
