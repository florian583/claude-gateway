package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Explicit, temporary diagnostic capture. Bodies can contain private user data;
// never send them to the dashboard, ordinary logs, or a remote collector.
type errorTraceConfig struct {
	Enabled       bool     `json:"enabled"`
	CaptureBodies bool     `json:"captureBodies"`
	Providers     []string `json:"providers"`
	ExpiresAt     string   `json:"expiresAt"`
}

const traceRequestLimit = 2 << 20
const traceResponseLimit = 64 << 10
const traceSlots = 8

func validateErrorTrace(c errorTraceConfig, providers map[string]providerDefinition) error {
	if !c.Enabled {
		return nil
	}
	if _, err := time.Parse(time.RFC3339, c.ExpiresAt); err != nil {
		return fmt.Errorf("errorTrace requires an RFC3339 expiresAt")
	}
	if len(c.Providers) == 0 {
		return fmt.Errorf("errorTrace requires explicit providers")
	}
	for _, name := range c.Providers {
		if _, ok := providers[name]; !ok {
			return fmt.Errorf("errorTrace references unknown provider %q", name)
		}
	}
	return nil
}

type errorTraceRecord struct {
	Time              time.Time         `json:"time"`
	RequestID         string            `json:"requestId"`
	Attempt           int               `json:"attempt"`
	Provider          string            `json:"provider"`
	RequestedModel    string            `json:"requestedModel"`
	UpstreamModel     string            `json:"upstreamModel"`
	Status            int               `json:"status"`
	RequestHeaders    map[string]string `json:"requestHeaders"`
	ResponseHeaders   map[string]string `json:"responseHeaders"`
	RequestBytes      int               `json:"requestBytes"`
	RequestSHA256     string            `json:"requestSha256"`
	RequestBody       string            `json:"upstreamRequestBody,omitempty"`
	RequestTruncated  bool              `json:"requestTruncated"`
	ResponseBody      string            `json:"upstreamResponseBody,omitempty"`
	ResponseBytes     int               `json:"responseBytesRead"`
	ResponseTruncated bool              `json:"responseTruncated"`
	ResponseEOF       bool              `json:"responseEOF"`
	ResponseReadError bool              `json:"responseReadError"`
	CaptureBodies     bool              `json:"captureBodies"`
}

// Only protocol and correlation headers: never arbitrary headers, URLs (which
// may carry keys), OAuth credentials, cookies, or profile auth state.
func traceHeaders(h http.Header, names ...string) map[string]string {
	out := map[string]string{}
	for _, name := range names {
		if value := h.Get(name); value != "" {
			out[name] = value[:min(len(value), 1024)]
		}
	}
	return out
}

func (s *proxyServer) traceFailedHTTPAttempt(resp *http.Response, body []byte, candidate modelConfig, provider, id string, attempt int) {
	c := s.cfg.Logging.ErrorTrace
	if !c.Enabled || resp.StatusCode < 400 || s.cfg.Logging.Path == "" {
		return
	}
	expires, err := time.Parse(time.RFC3339, c.ExpiresAt)
	if err != nil || !time.Now().Before(expires) {
		return
	}
	allowed := false
	for _, name := range c.Providers {
		if name == provider {
			allowed = true
			break
		}
	}
	if !allowed {
		return
	}
	record := errorTraceRecord{
		Time: time.Now().UTC(), RequestID: id, Attempt: attempt, Provider: provider,
		RequestedModel: candidate.Requested, UpstreamModel: candidate.Upstream, Status: resp.StatusCode,
		RequestBytes: len(body), RequestSHA256: fmt.Sprintf("%x", sha256.Sum256(body)), CaptureBodies: c.CaptureBodies,
		ResponseHeaders: traceHeaders(resp.Header, "Content-Type", "Request-Id", "X-Request-Id", "X-Correlation-Id", "Retry-After"),
	}
	if resp.Request != nil {
		record.RequestHeaders = traceHeaders(resp.Request.Header, "Content-Type", "Anthropic-Version", "Anthropic-Beta")
	}
	if c.CaptureBodies {
		record.RequestBody = string(body[:min(len(body), traceRequestLimit)])
		record.RequestTruncated = len(body) > traceRequestLimit
	}
	if resp.Body == nil {
		s.writeErrorTrace(record)
		return
	}
	resp.Body = &errorTraceBody{ReadCloser: resp.Body, record: record, save: s.writeErrorTrace}
}

// Observe only bytes already consumed by routing/forwarding. No extra reads,
// requests, buffering delay, or mutation of the response passed to the client.
type errorTraceBody struct {
	io.ReadCloser
	record errorTraceRecord
	buffer []byte
	mu     sync.Mutex
	once   sync.Once
	save   func(errorTraceRecord)
}

func (b *errorTraceBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	b.record.ResponseBytes += n
	if b.record.CaptureBodies {
		take := min(n, traceResponseLimit-len(b.buffer))
		b.buffer = append(b.buffer, p[:take]...)
		b.record.ResponseTruncated = b.record.ResponseBytes > traceResponseLimit
	}
	if err == io.EOF {
		b.record.ResponseEOF = true
	} else if err != nil {
		b.record.ResponseReadError = true
	}
	b.mu.Unlock()
	if err != nil {
		b.flush()
	}
	return n, err
}

func (b *errorTraceBody) Close() error {
	err := b.ReadCloser.Close()
	b.flush()
	return err
}

func (b *errorTraceBody) flush() {
	b.once.Do(func() {
		b.mu.Lock()
		record := b.record
		record.ResponseBody = string(b.buffer)
		b.mu.Unlock()
		b.save(record)
	})
}

func (s *proxyServer) writeErrorTrace(record errorTraceRecord) {
	// Bound concurrent disk work. Failure diagnostics must not form a queue that
	// stalls fallback or competes with successful streaming requests.
	if !s.errorTraceMu.TryLock() {
		log.Printf("error trace skipped request=%s reason=writer_busy", record.RequestID)
		return
	}
	defer s.errorTraceMu.Unlock()
	if err := s.persistErrorTrace(record); err != nil {
		// Deliberately omit OS error text: it may include private paths.
		log.Printf("error trace unavailable request=%s", record.RequestID)
	}
}

func (s *proxyServer) persistErrorTrace(record errorTraceRecord) error {
	dir := filepath.Join(filepath.Dir(s.cfg.Logging.Path), "error-traces")
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return fmt.Errorf("trace directory must be private and not a symlink")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(data) > 16<<20 {
		return fmt.Errorf("trace record exceeds limit")
	}
	f, err := os.CreateTemp(dir, ".trace-*") // 0600, no symlink following
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	slot := s.errorTraceSequence % traceSlots
	s.errorTraceSequence++
	name := fmt.Sprintf("attempt-%02d.json", slot)
	if err := os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	log.Printf("error trace captured request=%s attempt=%d provider=%s status=%d file=%s", record.RequestID, record.Attempt, record.Provider, record.Status, name)
	return nil
}
