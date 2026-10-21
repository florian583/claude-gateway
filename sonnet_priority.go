package main

import (
	"net/url"
	"strings"
	"time"
)

func (s *proxyServer) claudeAccountPinExpired(candidate modelConfig, until, now time.Time) bool {
	return !now.Before(until) && !s.claudeSubscriptionCandidate(candidate)
}

// Explicit Sonnet subscription requests spend available subscription capacity
// before entering their configured fallback steps. Classification may describe
// the task, but must not promote a fast non-Claude leg ahead of the account pool.
// Quota, authentication and health checks still run normally for every attempt.
func (s *proxyServer) preserveSonnetSubscriptionOrder(selected modelConfig) bool {
	return s.claudeSubscriptionCandidate(selected) &&
		strings.TrimSuffix(selected.Upstream, "[1m]") == "claude-sonnet-5-5"
}

// Advisor is executed by Anthropic, not a client tool that another model can
// call. Never silently remove it or send it to an API-compatible GLM endpoint.
func (s *proxyServer) advisorCompatibleCandidates(candidates []modelConfig, payload map[string]any) []modelConfig {
	tools, _ := payload["tools"].([]any)
	needsAdvisor := false
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		kind, _ := tool["type"].(string)
		if strings.HasPrefix(kind, "advisor_") {
			needsAdvisor = true
			break
		}
	}
	if !needsAdvisor {
		return candidates
	}
	result := make([]modelConfig, 0, len(candidates))
	for _, m := range candidates {
		p := s.cfg.Providers[firstNonEmpty(m.Provider, s.cfg.DefaultProvider)]
		u, _ := url.Parse(p.BaseURL)
		directAnthropic := u != nil && u.Scheme == "https" && u.Hostname() == "api.anthropic.com"
		if strings.HasPrefix(m.Upstream, "claude-") && (p.Variant == "claude-subscription" || directAnthropic) {
			result = append(result, m)
		}
	}
	return result
}
