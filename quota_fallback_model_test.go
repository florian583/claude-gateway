package main

import (
	"net/http"
	"testing"
	"time"
)

func TestFableQuotaUsesActualFallbackModel(t *testing.T) {
	for _, tc := range []struct {
		model modelConfig
		want  string
	}{
		{modelConfig{Requested: "fable", Upstream: "claude-opus-5", ResponseAlias: "fable[1m]"}, "opus-5"},
		{modelConfig{Requested: "claude-opus-5", Upstream: "claude-opus-4-8"}, ""},
		{modelConfig{Requested: "opus", Upstream: "claude-fable-5-1"}, "fable"},
		{modelConfig{Requested: "fable"}, "fable"},
		{modelConfig{Requested: "fable", Upstream: "kimi-k3"}, ""},
	} {
		if got := claudeQuotaFamily(tc.model); got != tc.want {
			t.Fatalf("model=%+v got=%q want=%q", tc.model, got, tc.want)
		}
	}
}

func TestOpusFiveQuotaDoesNotBlockOtherModels(t *testing.T) {
	s, err := newProxyServer(config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}})
	if err != nil {
		t.Fatal(err)
	}
	opus := modelConfig{Provider: "anthropic", Upstream: "claude-opus-5", Requested: "fable"}
	account := "anthropic@test"
	key := s.quotaBlockKeyForResponse(account, opus, http.Header{})
	if key != account+"#opus-5" {
		t.Fatalf("unexpected scope: %s", key)
	}
	s.markProviderBlocked(key, "quota_or_rate_limit", time.Minute)
	if blocked, _ := s.providerBlockForCandidate(account, opus); !blocked {
		t.Fatal("Opus 5 retry not suppressed")
	}
	for _, model := range []string{"claude-opus-4-8", "claude-sonnet-5", "claude-fable-5-1"} {
		if blocked, _ := s.providerBlockForCandidate(account, modelConfig{Provider: "anthropic", Upstream: model}); blocked {
			t.Fatalf("Opus 5 blocked %s", model)
		}
	}
	for _, window := range []string{"5h", "7d"} {
		h := http.Header{}
		h.Set("Anthropic-Ratelimit-Unified-"+window+"-Status", "rejected")
		if got := s.quotaBlockKeyForResponse(account, opus, h); got != account {
			t.Fatalf("global %s rejection not account-scoped: %s", window, got)
		}
	}
	s.markProviderBlocked(account, "quota_or_rate_limit", time.Minute)
	if blocked, _ := s.providerBlockForCandidate(account, modelConfig{Upstream: "claude-opus-4-8"}); !blocked {
		t.Fatal("global rejection bypassed")
	}
}
