package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func (s *proxyServer) profilesForCandidate(m modelConfig, r *http.Request) []claudeUsageProfile {
	if s.cfg.Definition == nil {
		return s.eligibleClaudePoolProfiles(requestListenerPort(r))
	}
	pool := m.AccountPool
	if pool == "" {
		pool = s.cfg.Definition.Providers[s.cfg.ClaudeUsage.Provider].Auth.Pool
	}
	def, ok := s.cfg.Definition.AccountPools[pool]
	if !ok {
		return nil
	}
	result := make([]claudeUsageProfile, 0, len(def.Profiles))
	seen := map[string]bool{}
	for _, name := range def.Profiles {
		p, ok := s.cfg.ClaudeUsage.AccountProfiles[name]
		if !ok {
			continue
		}
		key := firstNonEmpty(s.claudeProfileAccountKey(p), p.CredentialsService)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, p)
	}
	return result
}
func (s *proxyServer) snapshotAllowedForCandidate(snapshot claudeUsageSnapshot, m modelConfig) bool {
	if s.cfg.Definition == nil {
		return s.claudeSnapshotEligible(snapshot)
	}
	if snapshot.Source == "unknown" {
		return true
	}
	pool := firstNonEmpty(m.AccountPool, s.cfg.Definition.Providers[s.cfg.ClaudeUsage.Provider].Auth.Pool)
	p := s.cfg.Definition.AccountPools[pool]
	profile := s.cfg.Definition.Profiles[snapshot.Profile]
	five, week := defaultThreshold(p.FiveHourThresholdPct), defaultThreshold(p.SevenDayThresholdPct)
	if profile.FiveHourThresholdPct > 0 && profile.FiveHourThresholdPct < five {
		five = profile.FiveHourThresholdPct
	}
	if profile.SevenDayThresholdPct > 0 && profile.SevenDayThresholdPct < week {
		week = profile.SevenDayThresholdPct
	}
	allows := func(w claudeUsageWindow, threshold float64) bool {
		if w.Utilization < threshold {
			return true
		}
		// A past window is no evidence of exhaustion in the new window.
		// Allow a normal request to re-evaluate; do not fabricate a new balance.
		return !w.ResetsAt.IsZero() && snapshot.FetchedAt.Before(w.ResetsAt) && !s.accountNow().Before(w.ResetsAt)
	}
	return allows(snapshot.FiveHour, five) && allows(snapshot.SevenDay, week)
}
func (s *proxyServer) expandConfiguredClaudeAttempts(r *http.Request, candidates []modelConfig) []claudeCandidateAttempt {
	var result []claudeCandidateAttempt
	seen := map[string]bool{}
	for _, m := range candidates {
		if !s.claudeSubscriptionCandidate(m) {
			result = append(result, claudeCandidateAttempt{model: m})
			continue
		}
		profiles := s.profilesForCandidate(m, r)
		if p, ok := selectedClaudeProfile(r); ok && profileInClaudePool(p, profiles) {
			profiles = s.orderClaudeFallbackProfiles(profiles, p)
		}
		for _, p := range profiles {
			key := p.CredentialsService + "|" + m.Upstream
			if seen[key] {
				continue
			}
			seen[key] = true
			copy := m
			copy.ClaudeProfile = p.Name
			result = append(result, claudeCandidateAttempt{model: copy, profile: p})
		}
	}
	return result
}
func configuredCompatibleCandidates(candidates []modelConfig, payload map[string]any) []modelConfig {
	tools, _ := payload["tools"].([]any)
	images := requestContainsImage(payload)
	result := make([]modelConfig, 0, len(candidates))
	for _, m := range candidates {
		if len(tools) > 0 && !m.SupportsTools {
			continue
		}
		if images && (m.SupportsImages == nil || !*m.SupportsImages) {
			continue
		}
		result = append(result, m)
	}
	return result
}
func (s *proxyServer) rankConfiguredCandidates(candidates []modelConfig, stickyKey string) []modelConfig {
	result := append([]modelConfig(nil), candidates...)
	perf := s.adaptivePerformance()
	s.adaptive.mu.Lock()
	sticky := s.adaptive.sticky[stickyKey]
	s.adaptive.mu.Unlock()
	score := func(m modelConfig) float64 {
		key := routeKey(m.Provider, m)
		v := s.routeScore(m.Provider, key, perf[key])
		blocked, _ := s.providerBlocked(m.Provider)
		if blocked || s.circuitOpen(m.Provider, m) {
			v -= 10000
		}
		if key == sticky.Key && time.Now().Before(sticky.Until) {
			v += 25
		}
		return v
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := result[i], result[j]
		ta, tb := 0, 0
		if a.Tier != nil {
			ta = *a.Tier
		}
		if b.Tier != nil {
			tb = *b.Tier
		}
		if ta != tb {
			return ta < tb
		}
		return score(a) > score(b)
	})
	return result
}

// Paid fallback requires evidence for every non-metered leg visited in this
// request. An earlier 429 cannot excuse a later auth/network/unknown failure.
type paidFallbackEvidence map[string]string

func (e paidFallbackEvidence) allowed(m modelConfig) bool {
	if !m.AllowPaidFallback || len(e) == 0 {
		return false
	}
	for _, reason := range e {
		if reason == "" || !containsString(m.PaidFallbackOn, reason) {
			return false
		}
	}
	return true
}

type outageEvidence struct {
	started     time.Time
	lastRequest string
	count       int
}
type routingState struct {
	mu      sync.Mutex
	outages map[string]outageEvidence
	browser map[string]cachedBrowserUsage
}

type cachedBrowserUsage struct {
	value     browserUsageProvider
	checkedAt time.Time
	available bool
}

func (s *proxyServer) updateOutageEvidence(key, requestID string, status int) bool {
	s.providerState.mu.Lock()
	defer s.providerState.mu.Unlock()
	if s.providerState.outages == nil {
		s.providerState.outages = map[string]outageEvidence{}
	}
	now := s.accountNow()
	state := s.providerState.outages[key]
	if now.Sub(state.started) > time.Minute || now.Before(state.started) {
		state = outageEvidence{started: now}
	}
	if status >= 200 && status < 400 {
		delete(s.providerState.outages, key)
		return false
	}
	if status >= 500 && requestID != "" && requestID != state.lastRequest {
		state.count++
		state.lastRequest = requestID
	}
	s.providerState.outages[key] = state
	return state.count >= 3
}
func isQuotaReason(reason string) bool {
	return strings.Contains(reason, "quota") || (strings.Contains(reason, "usage_") && strings.HasSuffix(reason, "_exhausted"))
}
func (s *proxyServer) configuredBrowserUsage(provider string) (browserUsageProvider, bool, bool) {
	usage := s.cfg.Providers[provider].Usage
	if usage.Mode != "browser" {
		return browserUsageProvider{}, false, false
	}
	s.providerState.mu.Lock()
	defer s.providerState.mu.Unlock()
	now := s.accountNow()
	if s.providerState.browser == nil {
		s.providerState.browser = map[string]cachedBrowserUsage{}
	}
	cached, hasCached := s.providerState.browser[provider]
	freshValue := func(p browserUsageProvider, available bool) (browserUsageProvider, bool, bool) {
		age := now.Sub(p.FetchedAt)
		return p, true, available && p.Status == "ok" && p.Utilization != nil && age >= 0 && age <= time.Duration(usage.MaxAgeSeconds)*time.Second
	}
	if hasCached && now.Sub(cached.checkedAt) >= 0 && now.Sub(cached.checkedAt) < 5*time.Second {
		return freshValue(cached.value, cached.available)
	}
	unknown := func() (browserUsageProvider, bool, bool) {
		if hasCached {
			cached.checkedAt, cached.available = now, false
			s.providerState.browser[provider] = cached
			return cached.value, true, false
		}
		return browserUsageProvider{}, false, false
	}
	// No discovery, process execution, dashboard access, or route mutation.
	file, e := os.Open(usage.SnapshotPath)
	if e != nil {
		return unknown()
	}
	defer file.Close()
	var snapshot browserUsageSnapshot
	// Bounded local telemetry file, never an unbounded page/body.
	data, e := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if e != nil || len(data) > 1<<20 || json.Unmarshal(data, &snapshot) != nil || snapshot.Version != 1 {
		return unknown()
	}
	p, ok := snapshot.Providers[usage.SnapshotKey]
	if !ok {
		return unknown()
	}
	if p.FetchedAt.IsZero() || p.FetchedAt.After(now) || p.LastSuccess.After(now) {
		return unknown()
	}
	if p.Utilization != nil && (*p.Utilization < 0 || *p.Utilization > 1) {
		return unknown()
	}
	for _, w := range p.Windows {
		if w.PercentUsed < 0 || w.PercentUsed > 100 {
			return unknown()
		}
	}
	s.providerState.browser[provider] = cachedBrowserUsage{value: p, checkedAt: now, available: true}
	return freshValue(p, true)
}

// Explicit local snapshots are collected by Go, not by the read-only menu.
// This never opens a browser or contacts a provider.
func (s *proxyServer) monitorConfiguredBrowserUsage(ctx context.Context) {
	if s.cfg.Definition == nil {
		return
	}
	var ids []string
	for id, p := range s.cfg.Providers {
		if p.Usage.Mode == "browser" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	read := func() {
		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}
			s.configuredBrowserUsage(id)
		}
	}
	read()
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			read()
		}
	}
}
func (s *proxyServer) catalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.Definition == nil {
		writeAPIError(w, http.StatusNotFound, "not_found", "catalog unavailable")
		return
	}
	profiles := map[string]any{}
	for id, p := range s.cfg.Definition.Profiles {
		profiles[id] = map[string]any{"displayName": p.DisplayName, "fiveHourThresholdPct": defaultThreshold(p.FiveHourThresholdPct), "sevenDayThresholdPct": defaultThreshold(p.SevenDayThresholdPct), "refreshConfigured": len(p.RefreshCommand) > 0}
	}
	providers := map[string]any{}
	for id, p := range s.cfg.Providers {
		u, known := s.providerUsageUtilization(id)
		entry := map[string]any{"displayName": p.DisplayName, "protocol": s.cfg.Definition.Providers[id].Protocol, "variant": p.Variant, "billing": p.Billing, "usageMode": p.Usage.Mode, "usageKnown": known}
		if known {
			entry["utilization"] = u
		}
		providers[id] = entry
	}
	models := map[string]any{}
	for id, m := range s.cfg.Definition.Models {
		models[id] = map[string]any{"provider": m.Provider, "upstream": m.Upstream, "accountPool": m.AccountPool, "contextWindow": m.ContextWindow, "supportsImages": m.SupportsImages, "supportsTools": m.SupportsTools}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"profiles": profiles, "providers": providers, "models": models, "aliases": s.cfg.Definition.Aliases, "accountPools": s.cfg.Definition.AccountPools, "modelPools": s.cfg.Definition.ModelPools, "chains": s.cfg.Definition.Chains})
}

func (s *proxyServer) configuredClaudeUsageStatus() map[string]any {
	s.claudeUse.mu.Lock()
	snapshots := newestClaudeUsageSnapshots(s.claudeUse.snapshots, s.accountNow(), time.Duration(s.cfg.ClaudeUsage.StaleTTLSeconds)*time.Second)
	s.claudeUse.mu.Unlock()
	profiles := map[string]any{}
	for id, p := range s.cfg.Definition.Profiles {
		snapshot, known := snapshots[id]
		entry := map[string]any{"source": "unknown", "usageKnown": false}
		if known {
			entry = claudeUsageStatusEntry(snapshot, s.cfg.ClaudeUsage)
			entry["usageKnown"] = true
			entry["stale"] = s.accountNow().Sub(snapshot.FetchedAt) > time.Duration(s.cfg.ClaudeUsage.CacheTTLSeconds)*time.Second
		} else {
			snapshot = claudeUsageSnapshot{Profile: id, Source: "unknown"}
		}
		entry["displayName"] = p.DisplayName
		entry["refreshConfigured"] = len(p.RefreshCommand) > 0
		eligibility := map[string]any{}
		for name, m := range s.cfg.Definition.Models {
			if m.Provider != s.cfg.ClaudeUsage.Provider || !containsString(s.cfg.Definition.AccountPools[m.AccountPool].Profiles, id) {
				continue
			}
			candidate := modelConfig{Provider: m.Provider, Upstream: m.Upstream, AccountPool: m.AccountPool}
			blocked, state := s.providerBlockForCandidate(m.Provider+"@"+id, candidate)
			eligibility[name] = map[string]any{"eligible": !blocked && s.snapshotAllowedForCandidate(snapshot, candidate), "blockReason": state.Reason, "blockedUntil": state.BlockedUntil}
		}
		entry["models"] = eligibility
		profiles[id] = entry
	}
	return map[string]any{"enabled": s.cfg.ClaudeUsage.Provider != "", "provider": s.cfg.ClaudeUsage.Provider, "profiles": profiles}
}

func (s *proxyServer) refreshProfileCredentials(ctx context.Context, profile claudeUsageProfile) error {
	if s.cfg.Definition == nil {
		return errors.New("no configured refresh command")
	}
	p, ok := s.cfg.Definition.Profiles[profile.Name]
	if !ok || len(p.RefreshCommand) == 0 {
		return errors.New("refresh not configured; run profiles login for this account")
	}
	before, _ := resolveClaudeProfileOAuthToken(ctx, profile, true)
	cmd, e := makeProfileCommand(ctx, p, p.RefreshCommand, nil)
	if e != nil {
		return e
	}
	if e = cmd.Run(); e != nil {
		return errors.New("configured refresh command failed")
	}
	clearClaudeProfileOAuthToken(profile)
	after, e := resolveClaudeProfileOAuthToken(ctx, profile, true)
	if e != nil {
		return e
	}
	if after == before {
		return errors.New("refresh command did not rotate credential; login may be required")
	}
	raw, ok := claudeOAuthCredentials.Load(profile.CredentialsService)
	if !ok || !raw.(cachedClaudeOAuthCredential).usable(time.Now()) {
		return errors.New("refreshed credential is not usable")
	}
	return nil
}
func (s *proxyServer) monitorProfileCredentials(ctx context.Context) {
	if s.cfg.Definition == nil {
		return
	}
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	check := func() {
		for name, p := range s.cfg.Definition.Profiles {
			if len(p.RefreshCommand) == 0 {
				continue
			}
			profile := s.cfg.ClaudeUsage.AccountProfiles[name]
			if profile.Name == "" {
				continue
			}
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, e := resolveClaudeProfileOAuthToken(readCtx, profile)
			cancel()
			if ctx.Err() != nil {
				return
			}
			raw, ok := claudeOAuthCredentials.Load(profile.CredentialsService)
			if e != nil || !ok || (!raw.(cachedClaudeOAuthCredential).expiresAt.IsZero() && raw.(cachedClaudeOAuthCredential).expiresAt.Before(time.Now().Add(2*time.Minute))) {
				s.triggerClaudeOAuthRefresh(profile)
			}
		}
	}
	check()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			check()
		}
	}
}
