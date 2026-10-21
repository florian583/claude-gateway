package main

import (
	"testing"
	"time"
)

func TestSonnetPinDoesNotExpireOnLeaseTimer(t *testing.T) {
	s := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{
		Provider: "anthropic", EligibleUpstreams: []string{"claude-sonnet-5-5", "claude-opus-5-5"},
	}}}
	now := time.Now()
	for _, tc := range []struct {
		upstream string
		expired  bool
	}{
		{"claude-sonnet-5-5", false}, {"claude-opus-5-5", false},
	} {
		m := modelConfig{Provider: "anthropic", Upstream: tc.upstream}
		if got := s.claudeAccountPinExpired(m, now.Add(-time.Hour), now); got != tc.expired {
			t.Fatalf("%s expired=%v want %v", tc.upstream, got, tc.expired)
		}
	}
}

func TestSonnetFreshPinPrefersFiveHourHeadroomOverLoad(t *testing.T) {
	busy := claudeUsageProfile{Name: "busy", CredentialsService: "test-busy"}
	idle := claudeUsageProfile{Name: "idle", CredentialsService: "test-idle"}
	s := &proxyServer{claudeAccountInFlight: map[string]int{claudeProfileRegistryKey(busy): 12}}
	available := []availableClaudeAccount{
		{profile: idle, snapshot: claudeUsageSnapshot{Source: "oauth-api", FiveHour: claudeUsageWindow{Utilization: 80}}},
		{profile: busy, snapshot: claudeUsageSnapshot{Source: "oauth-api", FiveHour: claudeUsageWindow{Utilization: 5}}},
	}
	chosen, release, _ := s.chooseClaudeAccountWithPolicy(available, "session", claudeUsageProfile{}, true, true)
	defer release()
	if chosen.profile.Name != "busy" {
		t.Fatalf("got %s; want highest 5h headroom", chosen.profile.Name)
	}
	// More headroom elsewhere never steals an eligible established pin.
	chosen, release2, changed := s.chooseClaudeAccountWithPolicy(available, "session", idle, true, true)
	defer release2()
	if chosen.profile.Name != "idle" || changed {
		t.Fatal("eligible pin must survive better headroom elsewhere")
	}
}

func TestSonnetUnknownUsageIsNotFreeCapacity(t *testing.T) {
	s := &proxyServer{}
	chosen, _, _ := s.chooseClaudeAccountWithPolicy([]availableClaudeAccount{
		{profile: claudeUsageProfile{Name: "unknown"}, snapshot: claudeUsageSnapshot{Source: "unknown"}},
		{profile: claudeUsageProfile{Name: "known"}, snapshot: claudeUsageSnapshot{Source: "oauth-api", FiveHour: claudeUsageWindow{Utilization: 40}}},
	}, "", claudeUsageProfile{}, false, true)
	if chosen.profile.Name != "known" {
		t.Fatal("unknown usage must not rank as zero consumption")
	}
}
