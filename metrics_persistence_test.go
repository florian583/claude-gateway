package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

type controlledMetricsFile struct {
	*os.File
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	failure error
}

func (f *controlledMetricsFile) Write(b []byte) (int, error) {
	if f.entered != nil {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	if f.failure != nil {
		n, _ := f.File.Write(b[:len(b)/2])
		return n, f.failure
	}
	return f.File.Write(b)
}

func newControlledMetrics(t *testing.T, capacity int) (*metricsStore, *controlledMetricsFile) {
	t.Helper()
	m := newMetricsStore(metricsConfig{Path: filepath.Join(t.TempDir(), "metrics.jsonl"), MaxSamples: capacity})
	if err := m.openFileLocked(); err != nil {
		t.Fatal(err)
	}
	f := &controlledMetricsFile{File: m.file.(*os.File)}
	m.file = f
	t.Cleanup(m.close)
	return m, f
}

func TestMetricsSlowWriterBoundsQueueAndClose(t *testing.T) {
	m, f := newControlledMetrics(t, 600)
	f.entered, f.release = make(chan struct{}), make(chan struct{})
	for i := 0; i < 300; i++ {
		m.record(metricsSample{EventID: fmt.Sprint(i)})
	}
	flushed := make(chan struct{})
	go func() { m.flush(); close(flushed) }()
	<-f.entered
	completed := make(chan struct{})
	go func() {
		for i := 300; i < 1000; i++ {
			m.record(metricsSample{EventID: fmt.Sprint(i)})
			m.flush()
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("completion blocked behind disk")
	}
	m.mu.RLock()
	count := len(m.pending)
	m.mu.RUnlock()
	if count != 600 || m.persistenceDropped() != 400 {
		t.Fatalf("pending=%d dropped=%d", count, m.persistenceDropped())
	}
	closed := make(chan struct{})
	go func() { m.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close did not wait for writer")
	case <-time.After(20 * time.Millisecond):
	}
	close(f.release)
	<-flushed
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close failed to drain")
	}
	loaded := newMetricsStore(metricsConfig{Path: m.path, MaxSamples: 600})
	defer loaded.close()
	samples := loaded.read(600)
	if len(samples) != 600 {
		t.Fatalf("persisted=%d", len(samples))
	}
	for i, s := range samples {
		if s.EventID != fmt.Sprint(i) {
			t.Fatalf("record %d=%s", i, s.EventID)
		}
	}
}

func TestMetricsPartialWriteRecoveryPreservesRecords(t *testing.T) {
	for _, failure := range []error{syscall.ENOSPC, syscall.EACCES} {
		t.Run(failure.Error(), func(t *testing.T) {
			m, f := newControlledMetrics(t, 100)
			m.record(metricsSample{EventID: "existing"})
			m.flush()
			f.failure = failure
			m.record(metricsSample{EventID: "retry"})
			m.flush()
			raw, err := os.ReadFile(m.path)
			if err != nil {
				t.Fatal(err)
			}
			if got := decodeMetricLines(raw); len(got) != 1 || got[0].EventID != "existing" {
				t.Fatalf("existing records changed: %s", raw)
			}
			m.close()
			raw, err = os.ReadFile(m.path)
			if err != nil {
				t.Fatal(err)
			}
			got := decodeMetricLines(raw)
			if len(got) != 2 || got[1].EventID != "retry" || m.persistenceDropped() != 0 {
				t.Fatalf("recovery=%v dropped=%d", got, m.persistenceDropped())
			}
		})
	}
}

func TestMetricsCloseCountsUnpersistedRecords(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	m := newMetricsStore(metricsConfig{Path: filepath.Join(blocker, "metrics"), MaxSamples: 10})
	m.record(metricsSample{EventID: "lost"})
	m.close()
	m.close()
	if m.persistenceDropped() != 1 || len(m.pending) != 0 {
		t.Fatalf("dropped=%d pending=%d", m.persistenceDropped(), len(m.pending))
	}
}
