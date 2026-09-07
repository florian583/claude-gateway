package main

import (
	"strings"
	"time"
)

// Called with claudeUse.mu held. Keep only fresh, independent API observations;
// token refresh does not reset an account's history. Nothing is persisted.
func (s *proxyServer) rememberWorkerUsageLocked(snapshot claudeUsageSnapshot) {
	if snapshot.Source != "oauth-api" || snapshot.Profile == "" {
		return
	}
	if s.claudeUse.workerHistory == nil {
		s.claudeUse.workerHistory = map[string][]claudeUsageSnapshot{}
	}
	history := s.claudeUse.workerHistory[snapshot.Profile]
	if len(history) > 0 && snapshot.FetchedAt.Sub(history[len(history)-1].FetchedAt) < 5*time.Minute {
		return
	}
	kept := make([]claudeUsageSnapshot, 0, 7)
	for _, old := range history {
		if snapshot.FetchedAt.Sub(old.FetchedAt) <= 30*time.Minute {
			kept = append(kept, old)
		}
	}
	s.claudeUse.workerHistory[snapshot.Profile] = append(kept, snapshot)
}

// This is a soft, model-scoped reserve, never provider exhaustion or outage.
// Sonnet/Haiku routes are workers; main Opus/Fable routes retain the reserve.
func (s *proxyServer) claudeWorkerReserveReason(snapshot claudeUsageSnapshot, m modelConfig) string {
	if s.cfg.Definition == nil || !s.claudeSubscriptionCandidate(m) {
		return ""
	}
	upstream := strings.ToLower(m.Upstream)
	if !strings.Contains(upstream, "sonnet") && !strings.Contains(upstream, "haiku") {
		return ""
	}
	pool := firstNonEmpty(m.AccountPool, s.cfg.Definition.Providers[s.cfg.ClaudeUsage.Provider].Auth.Pool)
	limit := s.cfg.Definition.AccountPools[pool].WorkerUtilizationLimitPct
	if limit <= 0 || snapshot.Source == "unknown" {
		return ""
	}
	now := s.accountNow()
	s.claudeUse.mu.Lock()
	history := append([]claudeUsageSnapshot(nil), s.claudeUse.workerHistory[snapshot.Profile]...)
	s.claudeUse.mu.Unlock()
	windows := []claudeUsageWindow{snapshot.FiveHour, snapshot.SevenDay}
	for i, window := range windows {
		if !window.ResetsAt.IsZero() && !now.Before(window.ResetsAt) {
			continue // Previous window is not evidence about the new one.
		}
		if window.Utilization >= limit {
			return "worker_reserve"
		}
		if snapshot.Source != "oauth-api" || now.Sub(snapshot.FetchedAt) > 2*time.Minute || now.Before(snapshot.FetchedAt) || window.ResetsAt.IsZero() {
			continue
		}
		for _, old := range history {
			elapsed := snapshot.FetchedAt.Sub(old.FetchedAt)
			if elapsed < 10*time.Minute || elapsed > 30*time.Minute {
				continue
			}
			previous := old.FiveHour
			if i == 1 {
				previous = old.SevenDay
			}
			if !previous.ResetsAt.Equal(window.ResetsAt) || previous.Utilization > window.Utilization {
				continue
			}
			rate := (window.Utilization - previous.Utilization) / elapsed.Seconds()
			if window.Utilization+rate*window.ResetsAt.Sub(snapshot.FetchedAt).Seconds() >= limit {
				return "worker_pace_reserve"
			}
			break // Longest valid observation interval smooths short spikes.
		}
	}
	return ""
}
