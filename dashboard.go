package main

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Dashboard is a display projection, never a routing/credential refresh trigger.
// Only cached, allowlisted telemetry leaves this endpoint; no config paths,
// credential commands, headers, prompt bodies, or raw provider errors.
type dashboardWindow struct {
	Name     string     `json:"name"`
	UsedPct  float64    `json:"usedPct"`
	ResetsAt *time.Time `json:"resetsAt,omitempty"`
}
type dashboardRow struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Billing     string            `json:"billing,omitempty"`
	Models      []string          `json:"models"`
	Windows     []dashboardWindow `json:"windows"`
	State       string            `json:"state"`
	UpdatedAt   *time.Time        `json:"updatedAt,omitempty"`
	ModelStates map[string]string `json:"modelStates,omitempty"`
}
type dashboardFlow struct {
	At       time.Time `json:"at"`
	Route    string    `json:"route"`
	Model    string    `json:"model"`
	Upstream string    `json:"upstream"`
	HeaderMS int64     `json:"headerMs"`
	TPS      float64   `json:"tps"`
	Fallback bool      `json:"fallback"`
}
type dashboardTraffic struct {
	Route        string  `json:"route"`
	OK           int     `json:"ok"`
	Errors       int     `json:"errors"`
	Fallbacks    int     `json:"fallbacks"`
	TPS          float64 `json:"tps"`
	observations int
}
type dashboardSnapshot struct {
	Version        int                `json:"version"`
	GeneratedAt    time.Time          `json:"generatedAt"`
	ActiveRequests int64              `json:"activeRequests"`
	Draining       bool               `json:"draining"`
	NetworkState   string             `json:"networkState"`
	Accounts       []dashboardRow     `json:"accounts"`
	Providers      []dashboardRow     `json:"providers"`
	Current        *dashboardFlow     `json:"current,omitempty"`
	Traffic        []dashboardTraffic `json:"traffic"`
	TrafficMinutes int                `json:"trafficMinutes"`
	TrafficLimited bool               `json:"trafficLimited"`
}

func dashboardDate(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
func dashboardFresh(at, now time.Time, ttl time.Duration) bool {
	return !at.IsZero() && !at.After(now) && now.Sub(at) <= ttl
}
func dashboardBlock(state providerRuntimeState, now time.Time) string {
	if state.BlockedUntil.After(now) {
		if strings.Contains(state.Reason, "auth") {
			return "AUTH"
		}
		if isQuotaReason(state.Reason) {
			return "FULL"
		}
		return "BLOCKED"
	}
	if state.QuarantinedUntil.After(now) {
		return "DEGRADED"
	}
	return ""
}
func dashboardCapacity(windows []dashboardWindow, at, now time.Time, ttl time.Duration) string {
	if len(windows) == 0 {
		return "UNKNOWN"
	}
	if !dashboardFresh(at, now, ttl) {
		return "STALE"
	}
	highest := 0.0
	for _, w := range windows {
		if w.ResetsAt != nil && !w.ResetsAt.After(now) {
			return "STALE"
		}
		highest = math.Max(highest, w.UsedPct)
	}
	if highest >= 100 {
		return "FULL"
	}
	if highest >= 85 {
		return "LOW"
	}
	return "OK"
}
func dashboardAddWindow(row *dashboardRow, name string, used float64, reset time.Time) {
	if math.IsNaN(used) || math.IsInf(used, 0) || used < 0 || used > 100 {
		return
	}
	row.Windows = append(row.Windows, dashboardWindow{name, used, dashboardDate(reset)})
}
func dashboardWindowName(name string) string {
	s := strings.ToLower(name)
	switch {
	case strings.Contains(s, "week"):
		return "weekly"
	case strings.Contains(s, "month"):
		return "monthly"
	case strings.Contains(s, "hour"), strings.Contains(s, "session"), s == "5h":
		return "fiveHour"
	default:
		return name
	}
}

func (s *proxyServer) dashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.Definition == nil {
		writeAPIError(w, http.StatusNotFound, "not_found", "dashboard requires file configuration")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(s.dashboardSnapshot(s.accountNow()))
}

func (s *proxyServer) dashboardSnapshot(now time.Time) dashboardSnapshot {
	f := s.cfg.Definition
	out := dashboardSnapshot{Version: 1, GeneratedAt: now, ActiveRequests: s.activeRequests.Load(), Draining: s.draining.Load(), NetworkState: "normal", Accounts: []dashboardRow{}, Providers: []dashboardRow{}, Traffic: []dashboardTraffic{}, TrafficMinutes: 30}
	s.providerStateMu.Lock()
	states := make(map[string]providerRuntimeState, len(s.providerStates))
	for id, state := range s.providerStates {
		states[id] = state
	}
	if s.networkIncident.Phase != "" {
		out.NetworkState = string(s.networkIncident.Phase)
	}
	s.providerStateMu.Unlock()
	s.claudeUse.mu.Lock()
	snapshots := map[string]claudeUsageSnapshot{}
	for _, snapshot := range s.claudeUse.snapshots {
		if previous, exists := snapshots[snapshot.Profile]; !exists || snapshot.FetchedAt.After(previous.FetchedAt) {
			snapshots[snapshot.Profile] = snapshot
		}
	}
	s.claudeUse.mu.Unlock()
	for id, p := range f.Profiles {
		row := dashboardRow{ID: id, Name: firstNonEmpty(p.DisplayName, id), Models: []string{}, Windows: []dashboardWindow{}, ModelStates: map[string]string{}}
		snapshot, known := snapshots[id]
		if known && snapshot.Source != "unknown" {
			row.UpdatedAt = dashboardDate(snapshot.FetchedAt)
			dashboardAddWindow(&row, "fiveHour", snapshot.FiveHour.Utilization, snapshot.FiveHour.ResetsAt)
			dashboardAddWindow(&row, "weekly", snapshot.SevenDay.Utilization, snapshot.SevenDay.ResetsAt)
		}
		row.State = dashboardCapacity(row.Windows, snapshot.FetchedAt, now, time.Duration(s.cfg.ClaudeUsage.CacheTTLSeconds)*time.Second)
		for name, m := range f.Models {
			poolID := firstNonEmpty(m.AccountPool, f.Providers[m.Provider].Auth.Pool)
			if f.Providers[m.Provider].Auth.Type != "claude-profile-pool" || !containsString(f.AccountPools[poolID].Profiles, id) {
				continue
			}
			row.Models = append(row.Models, m.Upstream)
			state := firstNonEmpty(dashboardBlock(states[m.Provider+"@"+id], now), dashboardBlock(states[m.Provider], now))
			modelKey := s.claudeModelQuotaBlockKey(m.Provider+"@"+id, modelConfig{Upstream: m.Upstream})
			state = firstNonEmpty(state, dashboardBlock(states[modelKey], now))
			if state == "" {
				state = row.State
				if known && state != "STALE" && state != "FULL" && !s.snapshotAllowedForCandidate(snapshot, modelConfig{Provider: m.Provider, AccountPool: poolID}) {
					state = "RESERVE"
				}
			}
			row.ModelStates[name] = state
		}
		blocked, eligible := "", false
		for _, state := range row.ModelStates {
			switch state {
			case "AUTH", "BLOCKED", "FULL", "RESERVE", "DEGRADED":
				if blocked == "" || state < blocked {
					blocked = state
				}
			default:
				eligible = true
			}
		}
		if blocked != "" {
			if eligible {
				row.State = "RESTRICTED"
			} else {
				row.State = blocked
			}
		}
		row.Models = dashboardSortedUnique(row.Models)
		out.Accounts = append(out.Accounts, row)
	}
	for id, p := range f.Providers {
		if p.Auth.Type == "claude-profile-pool" {
			continue
		}
		row := dashboardRow{ID: id, Name: firstNonEmpty(p.DisplayName, id), Billing: p.Billing, Models: []string{}, Windows: []dashboardWindow{}}
		for _, m := range f.Models {
			if m.Provider == id {
				row.Models = append(row.Models, m.Upstream)
			}
		}
		row.Models = dashboardSortedUnique(row.Models)
		at := s.dashboardProviderWindows(id, &row)
		if len(row.Windows) == 0 {
			for header, raw := range states[id].RateLimitInfo {
				if strings.Contains(strings.ToLower(header), "utilization") {
					if used, ok := parseUtilization(raw); ok {
						dashboardAddWindow(&row, "overall", used*100, time.Time{})
						at = states[id].LastUpdated
					}
				}
			}
		}
		row.UpdatedAt = dashboardDate(at)
		ttl := 5 * time.Minute
		if p.Usage.MaxAgeSeconds > 0 {
			ttl = time.Duration(p.Usage.MaxAgeSeconds) * time.Second
		}
		row.State = firstNonEmpty(dashboardBlock(states[id], now), dashboardCapacity(row.Windows, at, now, ttl))
		out.Providers = append(out.Providers, row)
	}
	sort.Slice(out.Accounts, func(i, j int) bool { return out.Accounts[i].ID < out.Accounts[j].ID })
	sort.Slice(out.Providers, func(i, j int) bool { return out.Providers[i].ID < out.Providers[j].ID })
	s.dashboardTraffic(&out, now)
	return out
}

func dashboardSortedUnique(values []string) []string {
	sort.Strings(values)
	out := []string{}
	for _, value := range values {
		if len(out) == 0 || value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func (s *proxyServer) dashboardProviderWindows(id string, row *dashboardRow) time.Time {
	var at time.Time
	add := func(windows map[string]clineUsageWindow) {
		for key, w := range windows {
			dashboardAddWindow(row, dashboardWindowName(firstNonEmpty(w.Type, key)), w.PercentUsed, w.ResetsAt)
		}
	}
	switch id {
	case s.cfg.OllamaUsage.Provider:
		s.ollamaUse.mu.Lock()
		if s.ollamaUse.hasValue {
			v := s.ollamaUse.snapshot
			at = v.FetchedAt
			dashboardAddWindow(row, "fiveHour", v.Session.Usage*100, time.Time{})
			dashboardAddWindow(row, "weekly", v.Weekly.Usage*100, time.Time{})
		}
		s.ollamaUse.mu.Unlock()
	case s.cfg.ClineUsage.Provider:
		s.clineUse.mu.Lock()
		if s.clineUse.hasValue {
			at = s.clineUse.snapshot.FetchedAt
			add(s.clineUse.snapshot.Windows)
		}
		s.clineUse.mu.Unlock()
	case s.xaiUsageProviderName():
		s.xaiUse.mu.Lock()
		if s.xaiUse.hasValue {
			at = s.xaiUse.snapshot.FetchedAt
			add(s.xaiUse.snapshot.Windows)
		}
		s.xaiUse.mu.Unlock()
	}
	if len(row.Windows) == 0 && s.cfg.Providers[id].Usage.Mode == "browser" {
		s.providerState.mu.Lock()
		v, found := s.providerState.browser[id]
		if found && v.available && v.value.Status == "ok" {
			at = v.value.FetchedAt
			for name, window := range v.value.Windows {
				dashboardAddWindow(row, dashboardWindowName(name), window.PercentUsed, time.Time{})
			}
			if len(row.Windows) == 0 && v.value.Utilization != nil {
				dashboardAddWindow(row, "overall", *v.value.Utilization*100, time.Time{})
			}
		}
		s.providerState.mu.Unlock()
	}
	sort.Slice(row.Windows, func(i, j int) bool { return row.Windows[i].Name < row.Windows[j].Name })
	return at
}

func (s *proxyServer) dashboardRoute(sample metricsSample) string {
	provider := strings.Split(sample.Provider, "@")[0]
	name := firstNonEmpty(s.cfg.Definition.Providers[provider].DisplayName, provider)
	if sample.ClaudeProfile != "" {
		name += " / " + firstNonEmpty(s.cfg.Definition.Profiles[sample.ClaudeProfile].DisplayName, sample.ClaudeProfile)
	}
	return name
}
func (s *proxyServer) dashboardTraffic(out *dashboardSnapshot, now time.Time) {
	if s.metrics == nil {
		return
	}
	limit := min(s.metrics.maxSamples, 20000)
	samples := s.metrics.read(limit)
	out.TrafficLimited = len(samples) == limit
	groups := map[string]*dashboardTraffic{}
	// Completion records win over the corresponding attempt record, avoiding
	// double-counting failed streams and successful response headers.
	completed := map[string]bool{}
	key := func(v metricsSample) string {
		return v.RequestID + "|" + v.Provider + "|" + v.ClaudeProfile + "|" + v.Upstream + "|" + strconv.Itoa(v.Attempt)
	}
	for _, v := range samples {
		if v.RecordKind == "completion" && v.RequestID != "" {
			completed[key(v)] = true
		}
	}
	for _, v := range samples {
		at, err := time.Parse(time.RFC3339Nano, v.Timestamp)
		if err != nil || at.After(now) || now.Sub(at) > 30*time.Minute || v.Provider == "" || metricPolicySkip(v.FailureReason) {
			continue
		}
		if v.RecordKind != "completion" && !(v.RecordKind == "attempt" && v.Status >= 400 && !completed[key(v)]) {
			continue
		}
		name := s.dashboardRoute(v)
		// Keys use IDs, not display names, which need not be unique.
		groupKey := v.Provider + "|" + v.ClaudeProfile
		if groups[groupKey] == nil {
			groups[groupKey] = &dashboardTraffic{Route: name}
		}
		g := groups[groupKey]
		if v.Success {
			g.OK++
		} else {
			g.Errors++
		}
		if v.IsFallback {
			g.Fallbacks++
		}
		if v.Success && v.TokensPerSecond > 0 {
			g.TPS += v.TokensPerSecond
			g.observations++
		}
		if v.RecordKind == "completion" && v.Success && v.Status >= 200 && v.Status < 300 && now.Sub(at) < 10*time.Minute && (out.Current == nil || at.After(out.Current.At)) {
			out.Current = &dashboardFlow{at, name, v.RequestedModel, v.Upstream, v.LatencyMS, v.TokensPerSecond, v.IsFallback}
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		g := groups[key]
		if g.observations > 0 {
			g.TPS /= float64(g.observations)
		}
		out.Traffic = append(out.Traffic, *g)
	}
}
