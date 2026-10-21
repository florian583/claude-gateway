package main

import (
	"log"
	"time"
)

// recoverClaudeUsageAuth re-admits manually reauthenticated accounts after a
// successful OAuth usage request. A readable token or cached quota is not proof.
// Usage access cannot establish inference permission, so only refresh-failure
// quarantine is cleared; inference auth errors and quota blocks are preserved.
func (s *proxyServer) recoverClaudeUsageAuth(profile claudeUsageProfile, started time.Time) {
	if profile.Name == "" || s.cfg.ClaudeUsage.Provider == "" || started.IsZero() {
		return
	}
	// Keep failure counters and the account block atomic with respect to refresh
	// failures. Lock order is refresh -> provider, as in refresh-failure handling.
	s.claudeRefreshMu.Lock()
	defer s.claudeRefreshMu.Unlock()
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	key := s.cfg.ClaudeUsage.Provider + "@" + profile.Name
	state := s.providerStates[key]
	if state.LastFailure.After(started) || (state.Reason != "" && state.Reason != "auth_quarantined") {
		return
	}
	if s.claudeUsageAuthVerified == nil {
		s.claudeUsageAuthVerified = map[string]time.Time{}
	}
	if started.After(s.claudeUsageAuthVerified[profile.Name]) {
		s.claudeUsageAuthVerified[profile.Name] = started
	}
	delete(s.claudeRefreshFailures, profile.Name)
	if state.Reason != "auth_quarantined" {
		return
	}
	state.BlockedUntil = time.Time{}
	state.Reason = ""
	state.LastUpdated = s.accountNow()
	s.providerStates[key] = state
	log.Printf("Claude account profile=%q recovered auth quarantine after live OAuth usage verification", profile.Name)
}
