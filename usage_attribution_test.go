package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUsageAttributionLabels(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r = r.WithContext(context.WithValue(r.Context(), sessionRouteContextKey{}, "session-123"))
	r.Header.Set("X-Gateway-Workload", "author-bot")
	r.Header.Set("X-Gateway-Lane", "e")
	r.Header.Set("X-Gateway-Run", "20260101-010000")
	r.Header.Set("X-Gateway-Origin", "/private/path")
	r.Header.Set("X-Gateway-Parent-Session", "parent-1")
	r.Header.Set("X-Gateway-Worker", "child-2")
	r.Header.Set("X-Gateway-Host", "mac-mini")
	a := requestUsageAttribution(r)
	if a.Session != "session-123" || a.Workload != "author-bot" || a.Lane != "e" || a.Run == "" || a.Origin != "" {
		t.Fatalf("%+v", a)
	}
	if a.ParentSession != "parent-1" || a.Worker != "child-2" || a.Host != "mac-mini" {
		t.Fatal(a)
	}
	r.Header.Set("X-Gateway-Future-Label", "private")
	r.Header.Set("Unrelated", "keep")
	stripUsageHeaders(r.Header)
	if r.Header.Get("X-Gateway-Worker") != "" || r.Header.Get("X-Gateway-Future-Label") != "" || r.Header.Get("Unrelated") != "keep" {
		t.Fatal(r.Header)
	}
	r.Header.Del("X-Gateway-Workload")
	r.Header.Set("User-Agent", "cli workload/cron")
	if requestUsageAttribution(r).Workload != "cron-unclassified" {
		t.Fatal("lost cron fallback")
	}
}

func TestUsageAttributionSurvivesTelemetryWithoutExecution(t *testing.T) {
	s := metricsSample{}
	applyExecutionTelemetry(&s, requestTrace{Attribution: usageAttribution{Session: "s", Workload: "author-bot"}})
	b, _ := json.Marshal(s)
	var decoded metricsSample
	if err := json.Unmarshal(b, &decoded); err != nil || decoded.Session != "s" {
		t.Fatalf("%s %v", b, err)
	}
}

func TestUsageRollupsSeparateSessionsAndKeepFailedUsage(t *testing.T) {
	dir := t.TempDir()
	store := newRollupStore(dir)
	s := testSample("completion", "anthropic", "example-model", "example-model", false)
	s.Session = "session-a"
	s.CacheReadTokens = 600000
	s.CacheWriteTokens = 120
	s.OutputTokens = 30
	store.observe(s)
	store.flush()
	store.flush()
	s.Session = "session-b"
	store.observe(s)
	store.flush()
	lines, err := readRollupLines(dir, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("expected two session deltas, got %d", len(lines))
	}
	for _, l := range lines {
		if l.Session == "" || l.Delta.CacheWriteTokens != 120 || l.Delta.CacheReadTokens != 600000 || l.Delta.FailedTokens != 600150 || l.Delta.LargeContexts != 1 {
			t.Fatalf("%+v", l)
		}
		if !rollupCountsZero(rollupCountsSub(l.Delta, l.Delta)) {
			t.Fatal("bad subtraction")
		}
		if rollupCountsAdd(l.Delta, l.Delta).CacheWriteTokens != 240 {
			t.Fatal("bad addition")
		}
	}
}
