package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuditErrorBodyOutlivesHeaderDeadline(t *testing.T) {
	headers := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(429)
		w.(http.Flusher).Flush()
		close(headers)
		select {
		case <-release:
			io.WriteString(w, `{"error":{"type":"rate_limit_error"}}`)
		case <-r.Context().Done():
		}
	}))
	defer first.Close()
	defer unblock()
	var fallback atomic.Int64
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallback.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testMessageResponse)
	}))
	defer second.Close()
	f := testConfig(first.URL, "anthropic")
	p := f.Providers["endpoint"]
	p.BaseURL = second.URL
	f.Providers["backup"] = p
	m := f.Models["model"]
	m.Provider = "backup"
	f.Models["backup"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "backup"}}}
	cfg := compileFixture(t, f)
	pc := cfg.Providers["endpoint"]
	pc.ResponseHeaderTimeoutMS = 20
	cfg.Providers["endpoint"] = pc
	s := testProxy(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"hello"}]}`)).WithContext(ctx)
	done := make(chan struct{})
	w := httptest.NewRecorder()
	go func() { s.handler().ServeHTTP(w, req); close(done) }()
	select {
	case <-headers:
	case <-time.After(time.Second):
		t.Fatal("no headers")
	}
	select {
	case <-done:
		t.Fatal("stalled body unexpectedly finished")
	case <-time.After(200 * time.Millisecond):
	}
	if fallback.Load() != 0 {
		t.Fatal("fallback ran before body completed")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not recover after body completed")
	}
	if fallback.Load() != 1 || w.Code != 200 {
		t.Fatalf("fallback=%d status=%d", fallback.Load(), w.Code)
	}
	t.Log("429 body blocked fallback for >200ms despite 20ms header deadline")
}

func TestAuditTruncatedJSONRecordedAsSuccess(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, `{"content":`)
	}))
	defer upstream.Close()
	s := testProxy(t, compileFixture(t, testConfig(upstream.URL, "anthropic")))
	w := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	if w.Code != 200 || json.Valid(w.Body.Bytes()) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	samples := s.metrics.readPerRoute(100)
	found := false
	for _, sample := range samples {
		if sample.RecordKind == "completion" {
			found = true
			if !sample.Success {
				t.Fatal("truncated response correctly marked failed")
			}
		}
	}
	if !found {
		t.Fatal("no completion evidence")
	}
	t.Log("truncated upstream JSON forwarded as HTTP 200 and recorded as successful completion")
}

func TestAuditExpiredAccountPinsAccumulate(t *testing.T) {
	s := &proxyServer{}
	p := claudeUsageProfile{Name: "synthetic", CredentialsService: "synthetic"}
	for i := 0; i < maxAccountPins+100; i++ {
		s.rememberClaudeAccountLocked(fmt.Sprint(i), p, -time.Hour)
	}
	if len(s.claudeAccounts) != maxAccountPins+100 {
		t.Fatal("expired pins were pruned")
	}
	t.Logf("retained %d expired in-memory account pins", len(s.claudeAccounts))
}

func TestAuditMetricsPendingGrowsOnDiskFailure(t *testing.T) {
	dir := t.TempDir()
	blocked := filepath.Join(dir, "file")
	if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	m := newMetricsStore(metricsConfig{Path: filepath.Join(blocked, "metrics.jsonl"), MaxSamples: 10})
	defer m.close()
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 100; i++ {
			m.record(metricsSample{Provider: "synthetic", Upstream: "test", Timestamp: time.Now().Format(time.RFC3339Nano), RecordKind: "completion", Status: 200, Success: true})
		}
		m.mu.Lock()
		if m.flushTimer != nil {
			m.flushTimer.Stop()
		}
		m.mu.Unlock()
		m.flush()
	}
	m.mu.Lock()
	pending := len(m.pending)
	m.mu.Unlock()
	if pending != 300 {
		t.Fatalf("pending=%d", pending)
	}
	t.Logf("pending=%d despite MaxSamples=10 after repeated disk failures", pending)
}

func BenchmarkAuditRequestProcessing(b *testing.B) {
	for _, size := range []int{1024, 65536, 1048576} {
		for _, attempts := range []int{0, 3} {
			b.Run(fmt.Sprintf("bytes_%d_attempts_%d", size, attempts), func(b *testing.B) {
				payload := map[string]any{"model": "test", "messages": []any{map[string]any{"role": "user", "content": strings.Repeat("x", size)}}}
				body, _ := json.Marshal(payload)
				s := &proxyServer{cfg: config{Models: map[string]modelConfig{"test": {Provider: "anthropic", Upstream: "claude-haiku-5-5"}}, Providers: map[string]providerConfig{"anthropic": {}, "glm": {Format: "openai-chat"}}, ClaudeUsage: claudeUsageConfig{Provider: "anthropic"}}}
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(body))
					raw, parsed, _, err := s.normalizeRequest(req)
					if err != nil {
						b.Fatal(err)
					}
					if attempts > 0 {
						for _, m := range []modelConfig{{Provider: "anthropic", Upstream: "claude-haiku-5-5"}, {Provider: "anthropic", Upstream: "claude-sonnet-5-5"}, {Provider: "glm", Upstream: "glm-5.3-flash"}} {
							if _, err = s.buildAttemptBody(raw, parsed, m, m.Provider, "/v1/messages"); err != nil {
								b.Fatal(err)
							}
						}
					}
				}
			})
		}
	}
}

func BenchmarkAuditPersistNewAccountPin(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: b.TempDir()}}, claudePersistentPins: map[string]persistedAccountPin{}}
			for i := 0; i < count; i++ {
				s.claudePersistentPins[affinityHash(fmt.Sprint(i))] = persistedAccountPin{ProfileHash: affinityHash("synthetic"), LastUsed: time.Now()}
			}
			p := claudeUsageProfile{Name: "synthetic", CredentialsService: "synthetic"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.claudeAccountMu.Lock()
				s.rememberClaudeAccountLocked(fmt.Sprintf("new-%d", i), p, time.Hour)
				s.claudeAccountMu.Unlock()
			}
		})
	}
}

func BenchmarkAuditPersistExistingAccountPin(b *testing.B) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: b.TempDir()}}}
	p := claudeUsageProfile{Name: "synthetic", CredentialsService: "synthetic"}
	s.rememberClaudeAccountLocked("same", p, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.claudeAccountMu.Lock()
		s.rememberClaudeAccountLocked("same", p, time.Hour)
		s.claudeAccountMu.Unlock()
	}
}
