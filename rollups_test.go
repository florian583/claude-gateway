package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRollupFailedPersistenceDoesNotAdvanceOffset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "rollups")
	if err := os.WriteFile(dir, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	store := newRollupStore(dir)
	defer store.close()
	store.observe(testSample("completion", "fixture", "model", "model", true))
	store.flush()
	if store.persistenceStatus() == "" {
		t.Fatal("persistence error hidden")
	}
	if len(store.flushed) != 0 {
		t.Fatal("advanced offset without successful persistence")
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	store.flush()
	lines, err := readRollupLines(dir, time.Now().Add(-time.Hour))
	if err != nil || len(lines) != 1 || lines[0].Delta.Successes != 1 {
		t.Fatalf("retry lost usage: %v %#v", err, lines)
	}
}

func TestRollupAmbiguousAppendUsesSameIDAndDoesNotDoubleCount(t *testing.T) {
	dir := t.TempDir()
	store := newRollupStore(dir)
	defer store.close()
	store.appendLine = func(path string, data []byte) error {
		if err := appendRollupLine(path, data); err != nil {
			return err
		}
		return errors.New("simulated close acknowledgment loss")
	}
	store.observe(testSample("completion", "fixture", "model", "model", true))
	store.flush()
	store.observe(testSample("completion", "fixture", "model", "model", true))
	store.appendLine = nil
	store.flush()
	store.flush()
	lines, err := readRollupLines(dir, time.Now().Add(-time.Hour))
	var count int64
	for _, line := range lines {
		count += line.Delta.Successes
	}
	if err != nil || count != 2 {
		t.Fatalf("ambiguous retry total=%d err=%v", count, err)
	}
}

func TestRollupRecoversIncompleteTrailingWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, rollupFileName(time.Now()))
	if err := os.WriteFile(path, []byte(`{"partial":`), 0600); err != nil {
		t.Fatal(err)
	}
	store := newRollupStore(dir)
	defer store.close()
	store.observe(testSample("completion", "fixture", "model", "model", true))
	store.flush()
	lines, err := readRollupLines(dir, time.Now().Add(-time.Hour))
	if err != nil || len(lines) != 1 {
		t.Fatalf("partial tail not recovered: %v %#v", err, lines)
	}
}

func TestRollupFailureStagesRetainExactPendingID(t *testing.T) {
	for _, stage := range []string{"open", "write", "short-write", "sync", "close"} {
		t.Run(stage, func(t *testing.T) {
			store := newRollupStore(t.TempDir())
			defer store.close()
			store.appendLine = func(string, []byte) error { return errors.New(stage + " failure") }
			store.observe(testSample("completion", "fixture", "model", "model", true))
			store.flush()
			var id string
			for _, pending := range store.pending {
				id = pending.line.ID
			}
			if id == "" || len(store.flushed) != 0 || store.persistenceStatus() == "" {
				t.Fatal("failed persistence advanced or hid error")
			}
			store.appendLine = func(path string, data []byte) error {
				var line rollupLine
				if err := json.Unmarshal(data, &line); err != nil || line.ID != id {
					t.Fatal("retry identity changed")
				}
				return appendRollupLine(path, data)
			}
			store.flush()
			if len(store.pending) != 0 || store.persistenceStatus() != "" {
				t.Fatal("recovery not acknowledged")
			}
		})
	}
}

func testSample(kind, provider, upstream, model string, success bool) metricsSample {
	return metricsSample{
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:      "r-1",
		RecordKind:     kind,
		Provider:       provider,
		Upstream:       upstream,
		RequestedModel: model,
		Status:         200,
		Success:        success,
	}
}

func TestRollupStoreDeltaFlushAndRead(t *testing.T) {
	dir := t.TempDir()
	store := newRollupStore(dir)

	ok := testSample("completion", "ollama", "glm-5.3-flash:cloud", "claude-sonnet-5", true)
	ok.LatencyMS, ok.FirstContentMS, ok.TokensPerSecond = 1200, 900, 101
	ok.OutputTokens, ok.InputTokens = 300, 5000
	ok.CostUsdEstimate = 0.02
	store.observe(ok)
	store.observe(testSample("skip", "anthropic", "claude-sonnet-5", "claude-sonnet-5", false))
	failed := testSample("completion", "vercel", "zai/glm-5.3-flash", "claude-sonnet-5", false)
	failed.FailureReason = "http_status"
	store.observe(failed)
	store.flush()

	lines, err := readRollupLines(dir, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 3 {
		t.Fatalf("expected 3 rollup lines, got %d", len(lines))
	}
	var ollama, vercel rollupCounts
	for _, line := range lines {
		if line.Provider == "ollama" {
			ollama = rollupCountsAdd(ollama, line.Delta)
		}
		if line.Provider == "vercel" {
			vercel = rollupCountsAdd(vercel, line.Delta)
		}
	}
	if ollama.Successes != 1 || ollama.OutputTokens != 300 || ollama.CostUSD != 0.02 {
		t.Fatalf("ollama bucket = %#v", ollama)
	}
	if histogramPercentile(rollupLatencyBoundsMS, ollama.FirstContent, 0.5) != 1000 {
		t.Fatalf("firstContent p50 = %v", ollama.FirstContent)
	}
	if vercel.Failures != 1 || vercel.FailureReasons["http_status"] != 1 {
		t.Fatalf("vercel bucket = %#v", vercel)
	}

	// A second flush with no new observations writes nothing.
	store.flush()
	after, err := readRollupLines(dir, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 3 {
		t.Fatalf("quiet flush wrote %d extra lines", len(after)-3)
	}

	// A later observation lands as a delta, not a rewrite.
	store.observe(ok)
	store.flush()
	after, _ = readRollupLines(dir, time.Now().Add(-time.Hour))
	if len(after) != 4 {
		t.Fatalf("expected 4 rollup lines, got %d", len(after))
	}
	var total int64
	for _, line := range after {
		if line.Provider == "ollama" {
			total += line.Delta.Successes
		}
	}
	if total != 2 {
		t.Fatalf("summed ollama successes = %d", total)
	}
}

func TestRollupStoreSweepExpiresOldFiles(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "rollup-2020-01-01.jsonl")
	if err := os.WriteFile(old, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, "rollup-"+time.Now().UTC().Format("2006-01-02")+".jsonl")
	store := newRollupStore(dir)
	store.observe(testSample("terminal", "ollama", "x", "m", false))
	store.flush()
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("expired rollup file survived sweep")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("today's rollup file missing")
	}
}

func TestEventLogWriteTailAndRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log := newEventLog(path)
	log.record(proxyEvent{Kind: "provider_blocked", Provider: "ollama", Reason: "circuit_open"})
	log.record(proxyEvent{Kind: "fallback_exhausted", Provider: "vercel", Detail: "model=sonnet"})
	log.flush()
	events := tailEvents(path, 10)
	if len(events) != 2 || events[0].Kind != "provider_blocked" || events[1].Detail != "model=sonnet" {
		t.Fatalf("events = %#v", events)
	}
	if events[0].Timestamp == "" {
		t.Fatal("event timestamp not stamped")
	}
	if got := tailEvents(path, 1); len(got) != 1 || got[0].Kind != "fallback_exhausted" {
		t.Fatalf("tail limit returned %#v", got)
	}
}

func TestUsageHistoryDedupAndDelta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage-history.jsonl")
	history := newUsageHistoryLog(path)
	history.note("claude:a", "5h", 40, time.Time{})
	history.note("claude:a", "5h", 40.5, time.Time{}) // below delta + interval: dropped
	history.note("claude:a", "7d", 12, time.Time{})
	history.flush()
	entries := readUsageHistory(path, time.Now().Add(-time.Hour))
	if len(entries) != 2 {
		t.Fatalf("expected 2 deduplicated entries, got %d", len(entries))
	}
	history.note("claude:a", "5h", 44, time.Time{}) // >2pp move lands
	history.flush()
	if got := readUsageHistory(path, time.Now().Add(-time.Hour)); len(got) != 3 {
		t.Fatalf("expected moved entry, got %d", len(got))
	}
}

func TestStatsReportAggregatesRollups(t *testing.T) {
	dir := t.TempDir()
	rollDir := filepath.Join(dir, "rollups")
	store := newRollupStore(rollDir)
	a := testSample("completion", "ollama", "glm-5.3-flash:cloud", "claude-sonnet-5", true)
	a.OutputTokens = 100
	a.TokensPerSecond = 103
	store.observe(a)
	b := testSample("completion", "vercel", "zai/glm-5.3-flash", "claude-sonnet-5", true)
	b.IsFallback, b.CostUsdEstimate = true, 0.5
	store.observe(b)
	store.observe(testSample("skip", "anthropic", "claude-sonnet-5", "claude-sonnet-5", false))
	term := testSample("terminal", "ollama", "glm-5.3-flash:cloud", "claude-sonnet-5", false)
	term.FailureReason = "fallback_exhausted"
	store.observe(term)
	store.close()

	report, err := buildStatsReport(dir, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	p := report.Providers["vercel/zai/glm-5.3-flash"]
	if p.Successes != 1 || p.Fallbacks != 1 || p.CostUSD != 0.5 {
		t.Fatalf("vercel stats = %#v", p)
	}
	mix := report.ModelMix["claude-sonnet-5"]
	if mix["ollama"] != 1 || mix["vercel"] != 1 {
		t.Fatalf("model mix = %#v", mix)
	}
	if report.Failures["ollama:fallback_exhausted"] != 1 {
		t.Fatalf("failures = %#v", report.Failures)
	}
	if report.SpendByDay[time.Now().UTC().Format("2006-01-02")]["vercel"] != 0.5 {
		t.Fatalf("spend = %#v", report.SpendByDay)
	}
}

func TestRequestClientLabel(t *testing.T) {
	if got := requestClientLabel("claude-cli/2.1", "48104"); got != "48104|claude-cli/2.1" {
		t.Fatalf("label = %q", got)
	}
	if got := requestClientLabel("", "48107"); got != "port:48107" {
		t.Fatalf("label = %q", got)
	}
	if got := requestClientLabel("", ""); got != "" {
		t.Fatalf("label = %q", got)
	}
	ua := ""
	for i := 0; i < 200; i++ {
		ua += "x"
	}
	if got := requestClientLabel(ua, ""); len(got) != 80 {
		t.Fatalf("truncated label len = %d", len(got))
	}
}

func TestCompletionCostEstimateFromPricing(t *testing.T) {
	dir := t.TempDir()
	server := &proxyServer{
		cfg: config{
			Providers: map[string]providerConfig{
				"vercel": {Pricing: pricingDefinition{InputPerMillionTokens: 1, OutputPerMillionTokens: 10, CacheReadPerMillionTokens: 0.1}},
			},
		},
		metrics: newMetricsStore(metricsConfig{Path: filepath.Join(dir, "metrics.jsonl")}),
	}
	defer server.metrics.close()
	var stats streamStats
	stats.InputTokens.Store(1000)
	stats.OutputTokens.Store(500)
	stats.CacheReadTokens.Store(2000)
	server.recordCompletion(requestTrace{ID: "r", Path: "/v1/messages"}, modelConfig{Requested: "sonnet", Provider: "vercel", Upstream: "m"}, 0, time.Now().Add(-time.Second), 200, 2*time.Second, nil, &stats)
	server.metrics.flush()
	samples := decodeMetricLines(mustReadFile(t, server.metrics.path))
	if len(samples) != 1 {
		t.Fatalf("samples = %d", len(samples))
	}
	// 1000 in * $1/M + 500 out * $10/M + 2000 cache * $0.1/M = 0.001 + 0.005 + 0.0002
	want := 0.0062
	if diff := samples[0].CostUsdEstimate - want; diff < -1e-9 || diff > 1e-9 {
		t.Fatalf("costUsdEstimate = %v want %v", samples[0].CostUsdEstimate, want)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
