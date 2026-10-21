package main

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestTelemetryCloseWaitsForWriter(t *testing.T) {
	for _, kind := range []string{"events", "history"} {
		t.Run(kind, func(t *testing.T) {
			var writer *sync.Mutex
			var closeLog func()
			if kind == "events" {
				l := newEventLog(filepath.Join(t.TempDir(), "events.jsonl"))
				l.record(proxyEvent{Kind: "synthetic"})
				writer, closeLog = &l.flushMu, l.close
			} else {
				l := newUsageHistoryLog(filepath.Join(t.TempDir(), "history.jsonl"))
				l.note("synthetic", "weekly", 42, time.Time{})
				writer, closeLog = &l.flushMu, l.close
			}
			writer.Lock()
			done := make(chan struct{}, 2)
			for i := 0; i < 2; i++ {
				go func() { closeLog(); done <- struct{}{} }()
			}
			select {
			case <-done:
				writer.Unlock()
				t.Fatal("close returned while writer active")
			case <-time.After(20 * time.Millisecond):
			}
			writer.Unlock()
			for i := 0; i < 2; i++ {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("close did not finish")
				}
			}
		})
	}
}
