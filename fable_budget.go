package main

import "time"

// Soft per-account Fable policy. Never marks an account exhausted or authorizes
// paid fallback. Opus candidates continue through the ordinary chain.
func (s *proxyServer) claudeFableReserveReason(snapshot claudeUsageSnapshot, m modelConfig) string {
	pool := firstNonEmpty(m.AccountPool, s.cfg.Definition.Providers[s.cfg.ClaudeUsage.Provider].Auth.Pool)
	policy := s.cfg.Definition.AccountPools[pool]
	now := s.accountNow()
	w := snapshot.SevenDay
	maxAge := time.Duration(s.cfg.ClaudeUsage.StaleTTLSeconds) * time.Second
	if snapshot.Source != "oauth-api" || snapshot.FetchedAt.IsZero() || now.Before(snapshot.FetchedAt) || now.Sub(snapshot.FetchedAt) > maxAge || w.ResetsAt.IsZero() || !now.Before(w.ResetsAt) {
		return ""
	}
	if policy.FableMinWeeklyRemainingPct > 0 && 100-w.Utilization < policy.FableMinWeeklyRemainingPct {
		return "fable_weekly_reserve"
	}
	if !policy.FablePaceGuard {
		return ""
	}
	s.claudeUse.mu.Lock()
	history := append([]claudeUsageSnapshot(nil), s.claudeUse.workerHistory[snapshot.Profile]...)
	s.claudeUse.mu.Unlock()
	for _, old := range history {
		elapsed := snapshot.FetchedAt.Sub(old.FetchedAt)
		if elapsed < 15*time.Minute || elapsed > 30*time.Minute || !old.SevenDay.ResetsAt.Equal(w.ResetsAt) || old.Source != "oauth-api" {
			continue
		}
		delta := w.Utilization - old.SevenDay.Utilization
		if delta >= 1 && w.Utilization+delta/elapsed.Seconds()*w.ResetsAt.Sub(snapshot.FetchedAt).Seconds() >= 100 {
			return "fable_weekly_pace_reserve"
		}
		break
	}
	return ""
}
