package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUsageCanonicalEventRebuildDeduplicatesAndKeepsFailedUsage(t *testing.T) {
	root := t.TempDir()
	source, destination := filepath.Join(root, "events.jsonl"), filepath.Join(root, "rebuilt")
	sample := metricsSample{EventID: "event-1", Timestamp: "2026-01-02T12:00:00Z", RecordKind: "completion", InputTokens: 10, OutputTokens: 5, FailureReason: "client_cancel"}
	sample.Usage = &tokenUsage{Source: "anthropic", InputUncached: countPointer(10), Output: countPointer(5)}
	raw, _ := json.Marshal(sample)
	if err := os.WriteFile(source, append(append(append([]byte{}, raw...), '\n'), append(raw, '\n')...), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runCLI(context.Background(), []string{"--config", filepath.Join(root, "nonexistent.json"), "rebuild-usage", "--source", source, "--out", destination}, nil, &output, &output); err != nil {
		t.Fatal(err)
	}
	lines, err := readRollupLines(destination, time.Time{})
	if err != nil || len(lines) != 1 || lines[0].Delta.Completions != 1 || lines[0].Delta.FailedTokens != 15 || lines[0].Delta.UsageKnown["inputUncached"] != 1 {
		t.Fatalf("%+v %v", lines, err)
	}
	if err := rebuildUsageCLI([]string{"--source", source, "--out", destination}, &output); err == nil {
		t.Fatal("overwrote existing output")
	}
}

func TestUsageCanonicalRebuildRejectsMalformedEvent(t *testing.T) {
	root := t.TempDir()
	source, destination := filepath.Join(root, "events.jsonl"), filepath.Join(root, "rebuilt")
	os.WriteFile(source, []byte("broken\n"), 0600)
	if err := rebuildUsageCLI([]string{"--source", source, "--out", destination}, &bytes.Buffer{}); err == nil {
		t.Fatal("accepted corrupt history")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatal("published partial rebuild")
	}
}
