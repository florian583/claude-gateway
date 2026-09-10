package main

import (
	"log"
	"strings"
	"sync"
	"time"
)

// Model limits can change after an upgrade without a new global reset time.
// Once an hour, fresh allowed global headers permit ONE real request to
// revalidate an old model limit. They never clear it themselves. No background
// inference, duplicate requests, or dashboard-triggered probes.
const fableQuotaProbeInterval = time.Hour

func fableQuotaProbeReady(account, model providerRuntimeState, now time.Time) bool {
	if model.Reason != "fable_quota_rejected" || model.QuotaProbeInFlight ||
		account.BlockedUntil.After(now) || account.QuarantinedUntil.After(now) ||
		!model.BlockedUntil.After(now) || model.LastFailure.IsZero() ||
		now.Sub(model.LastFailure) < fableQuotaProbeInterval ||
		(!model.QuotaProbeAt.IsZero() && now.Sub(model.QuotaProbeAt) < fableQuotaProbeInterval) ||
		account.LastUpdated.After(now) || now.Sub(account.LastUpdated) > 5*time.Minute {
		return false
	}
	for _, window := range []string{"5h", "7d"} {
		status := ""
		for name, value := range account.RateLimitInfo {
			if strings.EqualFold(name, "anthropic-ratelimit-unified-"+window+"-status") {
				status = strings.ToLower(value)
			}
		}
		if status != "allowed" && status != "allowed_warning" {
			return false
		}
	}
	return true
}

func (s *proxyServer) fableQuotaProbeDue(accountKey, modelKey string) bool {
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	return fableQuotaProbeReady(s.providerStates[accountKey], s.providerStates[modelKey], s.accountNow())
}

// Selection only checks eligibility; admission atomically leases the probe.
// Hold the lease until body close, including partial streams and cancellation.
func (s *proxyServer) acquireFableQuotaProbe(accountKey string, candidate modelConfig, countTokens bool) (func(), bool) {
	noop := func() {}
	key := s.claudeModelQuotaBlockKey(accountKey, candidate)
	if key == "" {
		return noop, true
	}
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	now := s.accountNow()
	state := s.providerStates[key]
	if !state.BlockedUntil.After(now) {
		return noop, true
	}
	if countTokens || !fableQuotaProbeReady(s.providerStates[accountKey], state, now) {
		return noop, false
	}
	state.QuotaProbeAt, state.QuotaProbeInFlight = now, true
	s.providerStates[key] = state
	log.Printf("model quota revalidation admitted provider=%s model=fable", accountKey)
	var once sync.Once
	return func() {
		once.Do(func() {
			s.providerStateMu.Lock()
			defer s.providerStateMu.Unlock()
			state := s.providerStates[key]
			state.QuotaProbeInFlight = false
			s.providerStates[key] = state
		})
	}, true
}

// Called only after a fully delivered successful inference for this model.
// Preserve any rejection observed since that inference started.
func (s *proxyServer) clearFableQuotaAfter(key string, started time.Time) {
	s.providerStateMu.Lock()
	state := s.providerStates[key]
	if !started.IsZero() && !state.LastFailure.After(started) && state.Reason == "fable_quota_rejected" {
		state.BlockedUntil, state.Reason = time.Time{}, ""
		s.providerStates[key] = state
		log.Printf("model quota recovered provider=%s evidence=complete_inference", key)
	}
	s.providerStateMu.Unlock()
	s.clearProviderBlockAfter(key, started)
}
