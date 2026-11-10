package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quotaProbeFixture() (*proxyServer, modelConfig, time.Time) {
	now := time.Now()
	s := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}}, clockNow: func() time.Time { return now }, providerStates: map[string]providerRuntimeState{
		"anthropic@test": {LastUpdated: now, RateLimitInfo: map[string]string{
			"Anthropic-Ratelimit-Unified-5h-Status": "allowed",
			"Anthropic-Ratelimit-Unified-7d-Status": "allowed_warning",
		}},
		"anthropic@test#fable": {Reason: "fable_quota_rejected", LastFailure: now.Add(-2 * time.Hour), BlockedUntil: now.Add(72 * time.Hour)},
	}}
	return s, modelConfig{Provider: "anthropic", Upstream: "claude-fable-5-1"}, now
}

func TestFableQuotaProbeSingleFlightAndCooldown(t *testing.T) {
	s, m, _ := quotaProbeFixture()
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, ok := s.acquireFableQuotaProbe("anthropic@test", m, false)
			if ok {
				accepted.Add(1)
			}
			release()
			release() // cancellation/body-close release is idempotent
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("admitted %d probes", accepted.Load())
	}
	if !s.providerStates["anthropic@test#fable"].BlockedUntil.After(time.Now()) {
		t.Fatal("admission cleared quota")
	}
}

func TestFableQuotaRecoveryEvidence(t *testing.T) {
	for _, test := range []string{"fresh rejection", "stale global", "unknown global", "global rejected", "count tokens", "auth block"} {
		t.Run(test, func(t *testing.T) {
			s, m, now := quotaProbeFixture()
			a, b := s.providerStates["anthropic@test"], s.providerStates["anthropic@test#fable"]
			switch test {
			case "fresh rejection":
				b.LastFailure = now
			case "stale global":
				a.LastUpdated = now.Add(-6 * time.Minute)
			case "unknown global":
				a.RateLimitInfo = nil
			case "global rejected":
				a.RateLimitInfo["Anthropic-Ratelimit-Unified-7d-Status"] = "rejected"
			case "auth block":
				b.Reason = "auth_error"
			}
			s.providerStates["anthropic@test"], s.providerStates["anthropic@test#fable"] = a, b
			release, ok := s.acquireFableQuotaProbe("anthropic@test", m, test == "count tokens")
			defer release()
			if ok {
				t.Fatal("unsafe probe allowed")
			}
		})
	}
}

func TestFableQuotaCompleteSuccessAndNewerRejection(t *testing.T) {
	s, m, now := quotaProbeFixture()
	if blocked, _ := s.providerBlockForCandidate("anthropic@test", m); blocked {
		t.Fatal("old block cannot revalidate")
	}
	if s.providerStates["anthropic@test#fable"].QuotaProbeInFlight {
		t.Fatal("selection acquired lease")
	}
	s.clearFableQuotaAfter("anthropic@test#fable", now)
	if s.providerStates["anthropic@test#fable"].Reason != "" {
		t.Fatal("complete model success did not recover")
	}
	s, _, now = quotaProbeFixture()
	b := s.providerStates["anthropic@test#fable"]
	b.LastFailure = now.Add(time.Second)
	s.providerStates["anthropic@test#fable"] = b
	s.clearFableQuotaAfter("anthropic@test#fable", now)
	if s.providerStates["anthropic@test#fable"].Reason == "" {
		t.Fatal("older success erased newer rejection")
	}
}

func TestFableQuotaFailedStreamKeepsBlockAndLease(t *testing.T) {
	s, m, now := quotaProbeFixture()
	release, ok := s.acquireFableQuotaProbe("anthropic@test", m, false)
	if !ok {
		t.Fatal("probe denied")
	}
	// A long-running stream must not admit a second request even after an hour.
	later := now.Add(2 * time.Hour)
	s.clockNow = func() time.Time { return later }
	a := s.providerStates["anthropic@test"]
	a.LastUpdated = later
	s.providerStates["anthropic@test"] = a
	if _, ok := s.acquireFableQuotaProbe("anthropic@test", m, false); ok {
		t.Fatal("overlapping stream admitted")
	}
	// Close without success is how transport/partial-stream failure releases.
	release()
	if s.providerStates["anthropic@test#fable"].Reason != "fable_quota_rejected" {
		t.Fatal("failed stream cleared quota")
	}
	// Ordinary account success cannot clear the separate model block.
	s.clearProviderBlockAfter("anthropic@test", later)
	a = s.providerStates["anthropic@test"]
	a.LastUpdated = later
	s.providerStates["anthropic@test"] = a
	if s.providerStates["anthropic@test#fable"].Reason == "" {
		t.Fatal("other model cleared quota")
	}
	release, ok = s.acquireFableQuotaProbe("anthropic@test", m, false)
	defer release()
	if !ok {
		t.Fatal("failed probe could not retry after cooldown")
	}
}
