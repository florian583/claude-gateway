package main

import (
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func usagePayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestUsagePresenceZeroAndTTLSubsets(t *testing.T) {
	var stats streamStats
	stats.Usage.observeAnthropic(usagePayload(t, `{"usage":{"input_tokens":100,"output_tokens":0,"cache_read_input_tokens":20,"cache_creation_input_tokens":30,"cache_creation":{"ephemeral_5m_input_tokens":10,"ephemeral_1h_input_tokens":20}}}`))
	v := stats.Usage.snapshot()
	if v.Output == nil || *v.Output != 0 || v.Reasoning != nil || v.InputTotal == nil || *v.InputTotal != 150 {
		t.Fatalf("presence/totals = %#v", v)
	}
	var sample metricsSample
	applyTokenUsage(&sample, &stats)
	if sample.InputTokens+sample.CacheReadTokens+sample.CacheWriteTokens != 150 {
		t.Fatal("TTL counted twice")
	}
}

func TestUsageOpenAIInputAndReasoningAreInclusive(t *testing.T) {
	var stats streamStats
	stats.Usage.observeOpenAI(usagePayload(t, `{"usage":{"prompt_tokens":100,"completion_tokens":50,"prompt_tokens_details":{"cached_tokens":30},"completion_tokens_details":{"reasoning_tokens":20}}}`))
	stats.Usage.observeAnthropic(usagePayload(t, `{"usage":{"input_tokens":0,"output_tokens":0}}`))
	var sample metricsSample
	applyTokenUsage(&sample, &stats)
	if sample.InputTokens != 70 || sample.CacheReadTokens != 30 || sample.OutputTokens != 50 || *sample.Usage.Reasoning != 20 {
		t.Fatalf("inclusive normalization = %#v", sample)
	}
}

func TestUsageMissingCacheRemainsUnknown(t *testing.T) {
	var u usageTelemetry
	u.observeOpenAI(usagePayload(t, `{"usage":{"prompt_tokens":100}}`))
	v := u.snapshot()
	if v.InputTotal == nil || v.InputUncached != nil || v.CacheRead != nil || v.Output != nil {
		t.Fatalf("missing became zero: %#v", v)
	}
}

func TestUsageMalformedCountsAndInconsistentSubsets(t *testing.T) {
	for _, value := range []any{float64(-1), 1.5, "2", true} {
		if countPointer(value) != nil {
			t.Fatalf("accepted %#v", value)
		}
	}
	var u usageTelemetry
	u.observeOpenAI(usagePayload(t, `{"usage":{"prompt_tokens":10,"prompt_tokens_details":{"cached_tokens":11}}}`))
	if v := u.snapshot(); !v.Inconsistent || v.InputUncached != nil {
		t.Fatalf("invalid cache subset = %#v", v)
	}
}

func TestUsageJSONResponsePreservesPresence(t *testing.T) {
	var stats streamStats
	reader := newJSONFilterReadCloser(io.NopCloser(strings.NewReader(`{"type":"message","usage":{"input_tokens":0,"output_tokens":3}}`)), nil, "alias", &stats)
	defer reader.Close()
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	if v := stats.Usage.snapshot(); v == nil || v.InputUncached == nil || *v.InputUncached != 0 || v.CacheRead != nil {
		t.Fatalf("JSON usage = %#v", v)
	}
}

func TestUsageReportedModelBeforeAliasAndRollupCoverage(t *testing.T) {
	var stats streamStats
	recordReportedBackend(&stats, usagePayload(t, `{"model":"actual/model-v2"}`))
	recordReportedBackend(&stats, usagePayload(t, `{"model":"client-alias"}`))
	stats.Usage.observeOpenAI(usagePayload(t, `{"usage":{"prompt_tokens":12,"completion_tokens":0,"completion_tokens_details":{"reasoning_tokens":0}}}`))
	var sample metricsSample
	applyTokenUsage(&sample, &stats)
	sample.ReportedModel, _ = stats.ReportedModel.Load().(string)
	if sample.ReportedModel != "actual/model-v2" {
		t.Fatal(sample.ReportedModel)
	}
	sample.RecordKind = "completion"
	sample.Timestamp = "2026-01-02T12:00:00Z"
	store := newRollupStore(t.TempDir())
	defer store.close()
	store.observe(sample)
	for key, count := range store.buckets {
		if key.ActualModel != sample.ReportedModel || count.UsageKnown["outputInclusive"] != 1 || count.UsageKnown["cacheRead"] != 0 || count.UsageTotals["inputTotal"] != 12 {
			t.Fatalf("%+v %+v", key, count)
		}
	}
}

func TestUsageFailedPartialCompletionKeepsObservedNotEstimatedCounts(t *testing.T) {
	server := &proxyServer{cfg: config{Providers: map[string]providerConfig{"fixture": {}}},
		metrics: newMetricsStore(metricsConfig{Path: filepath.Join(t.TempDir(), "metrics.jsonl")})}
	defer server.metrics.close()
	var stats streamStats
	stats.MissingStop.Store(true)
	stats.InputTokens.Store(999)
	stats.InputEstimated.Store(true)
	stats.Usage.observeAnthropic(usagePayload(t, `{"usage":{"input_tokens":100,"output_tokens":7,"cache_creation_input_tokens":30,"cache_creation":{"ephemeral_5m_input_tokens":10,"ephemeral_1h_input_tokens":20}}}`))
	server.recordCompletion(requestTrace{ID: "partial"}, modelConfig{Requested: "alias", Provider: "fixture"}, 0, time.Now().Add(-time.Second), 200, 2*time.Second, io.ErrUnexpectedEOF, &stats)
	server.metrics.flush()
	samples := decodeMetricLines(mustReadFile(t, server.metrics.path))
	if len(samples) != 1 || samples[0].Success || samples[0].InputTokens != 100 || samples[0].OutputTokens != 7 || samples[0].Usage.CacheRead != nil || samples[0].Usage.InputTotal != nil || *samples[0].Usage.CacheCreation1h != 20 {
		t.Fatalf("%+v", samples)
	}
}
