package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHeaderLearningUsesHeadersAndCensoredTimeouts(t *testing.T) {
	samples := []metricsSample{
		{Provider: "test", Upstream: "model", RecordKind: "completion", Success: true, LatencyMS: 90000, ResponseHeadersMS: 1200},
		{Provider: "test", Upstream: "model", RecordKind: "attempt", FailureReason: "transport", LatencyMS: 20000, Execution: &attemptExecution{TransportError: "response_header_timeout", HeaderBudgetMS: 20000}},
		{Provider: "test", Upstream: "model", RecordKind: "completion", Success: true, LatencyMS: 100000},
	}
	p := buildRoutePerformance(samples)["test|model"]
	if p.HeaderSamples != 2 || p.P50HeaderMS != 20000 {
		t.Fatalf("header performance: %+v", p)
	}
	limited := samples[1]
	limited.Execution = &attemptExecution{TransportError: "response_header_timeout", HeaderBudgetMS: 2000, ChainLimited: true}
	if observedHeaderMS(limited) != 0 {
		t.Fatal("chain deadline must not train provider budget")
	}
}

func TestHeaderLearningNeverShrinksBaselineAndHonorsCeiling(t *testing.T) {
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{"anthropic": {ResponseHeaderTimeoutMS: 60000}}, ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}, AdaptiveRouting: adaptiveRoutingConfig{Enabled: true, RefreshSeconds: 3600, MinSamples: 4}}}
	model := modelConfig{Provider: "anthropic", Upstream: "claude-opus-4-8"}
	for _, tc := range []struct {
		samples int
		p95     float64
		want    time.Duration
	}{{100, 100, 20 * time.Second}, {19, 50000, 20 * time.Second}, {20, 20000, 42 * time.Second}, {20, 90000, 60 * time.Second}} {
		s.adaptive.performance = map[string]routePerformance{routeKey("anthropic", model): {HeaderSamples: tc.samples, P95HeaderMS: tc.p95}}
		s.adaptive.refreshedAt = time.Now()
		if got := s.candidateResponseHeaderBudget("anthropic", model, []byte("small")); got != tc.want {
			t.Fatalf("%+v: got %s", tc, got)
		}
	}
}

func TestMetricsArchivesPreserveHistoryAndBoundRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	now := time.Now()
	for _, i := range []int{0, 1, 2, metricsArchiveCount} {
		name := path
		if i > 0 {
			name = fmt.Sprintf("%s.%d", path, i)
		}
		if err := os.WriteFile(name, []byte(fmt.Sprint(i)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-metricsRetention - time.Hour)
	if err := os.Chtimes(path+".2", old, old); err != nil {
		t.Fatal(err)
	}
	if err := rotateMetricsArchives(path, now); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{path + ".1": "0", path + ".2": "1"} {
		got, err := os.ReadFile(name)
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q %v", name, got, err)
		}
	}
	for _, name := range []string{path + ".3", fmt.Sprintf("%s.%d", path, metricsArchiveCount)} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("expired/capped segment exists: %s", name)
		}
	}
}

func TestAccountLargeLoadGapMovesLeaseWithoutAdmissionCap(t *testing.T) {
	a := claudeUsageProfile{Name: "a", CredentialsService: "synthetic-a"}
	b := claudeUsageProfile{Name: "b", CredentialsService: "synthetic-b"}
	s := &proxyServer{claudeAccountInFlight: map[string]int{claudeProfileRegistryKey(a): 20, claudeProfileRegistryKey(b): 10}}
	// Pin-until-broken: load imbalance alone never moves a pinned conversation.
	chosen, release, changed := s.chooseClaudeAccount([]availableClaudeAccount{{profile: a}, {profile: b}}, "conversation", a, true)
	if chosen.profile.Name != "a" || changed || s.claudeAccounts["conversation"].Profile.Name != "a" {
		t.Fatal("load imbalance must not move a pinned lease")
	}
	release()
	release()
	if s.claudeAccountInFlight[claudeProfileRegistryKey(a)] != 20 {
		t.Fatal("reservation not released exactly once")
	}
	chosen, release, _ = s.chooseClaudeAccount([]availableClaudeAccount{{profile: a}}, "only", a, true)
	if chosen.profile.Name != "a" {
		t.Fatal("sole eligible account must remain usable regardless of load")
	}
	release()
}

func TestMetricsByteCapIndependentOfMemoryCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	m := newMetricsStore(metricsConfig{Path: path, MaxSamples: 10000})
	defer m.close()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(metricsSegmentBytes); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	m.record(metricsSample{Provider: "synthetic", Upstream: "model", RecordKind: "completion"})
	m.flush()
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("byte cap did not rotate: %v", err)
	}
	if m.persistedBytes != 0 || m.persistedLines != 0 {
		t.Fatal("rotation counters not reset")
	}
}

func TestAttemptPhaseTelemetryReportsActualBudget(t *testing.T) {
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer u.Close()
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"test": {BaseURL: u.URL, ResponseHeaderTimeoutMS: 1000}}})
	if err != nil {
		t.Fatal(err)
	}
	timing := &upstreamTiming{}
	ctx := context.WithValue(context.Background(), upstreamTimingContextKey{}, timing)
	resp, err := s.doUpstreamWithHeaderBudgetLimit(ctx, httptest.NewRequest("POST", "/v1/messages", nil), []byte(`{"model":"test"}`), modelConfig{Provider: "test", Upstream: "test"}, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var sample metricsSample
	selection := int64(7)
	applyExecutionTelemetry(&sample, requestTrace{Execution: &attemptExecution{TransformMS: 3}, Timing: timing, AccountSelectionMS: &selection})
	e := sample.Execution
	if e.HeaderBudgetMS != 500 || e.ChainRemainingMS != 500 || !e.ChainLimited || e.RequestWrittenMS == nil || *e.AccountSelectionMS != 7 || e.TransformMS != 3 {
		t.Fatalf("execution: %+v", e)
	}
}
