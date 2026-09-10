package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func traceFixture(t *testing.T, bodies bool) *proxyServer {
	t.Helper()
	return &proxyServer{cfg: config{Logging: loggingConfig{
		Path:       filepath.Join(t.TempDir(), "proxy.log"),
		ErrorTrace: errorTraceConfig{Enabled: true, CaptureBodies: bodies, Providers: []string{"endpoint"}, ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339)},
	}}}
}

func traceFiles(t *testing.T, s *proxyServer) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(filepath.Dir(s.cfg.Logging.Path), "error-traces", "attempt-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func loadTrace(t *testing.T, file string) errorTraceRecord {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var r errorTraceRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestErrorTraceHTTPIntegration(t *testing.T) {
	const failure = `{"type":"error","error":{"type":"invalid_request_error","code":"1210","message":"invalid parameter"}}`
	var received string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received = string(body)
		w.Header().Set("X-Request-Id", "synthetic-upstream-id")
		w.Header().Set("Set-Cookie", "response-cookie-canary")
		w.WriteHeader(400)
		io.WriteString(w, failure)
	}))
	defer upstream.Close()
	f := testConfig(upstream.URL, "anthropic")
	f.ErrorTrace = traceFixture(t, true).cfg.Logging.ErrorTrace
	cfg := compileFixture(t, f)
	s, err := newProxyServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.metrics.close() })
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","max_tokens":100,"tools":[{"name":"echo","defer_loading":true,"input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"synthetic prompt"}]}`))
	req.Header.Set("Authorization", "Bearer request-auth-canary")
	req.Header.Set("Cookie", "request-cookie-canary")
	req.Header.Set("Anthropic-Beta", "synthetic-beta")
	out := httptest.NewRecorder()
	s.handler().ServeHTTP(out, req)
	if out.Code != 400 || out.Body.String() != failure {
		t.Fatalf("response changed: %d %q", out.Code, out.Body.String())
	}
	files := traceFiles(t, s)
	if len(files) != 1 {
		t.Fatalf("want one trace, got %v", files)
	}
	r := loadTrace(t, files[0])
	if r.RequestBody != received || !strings.Contains(received, `"model":"test-model"`) {
		t.Fatal("trace is not exact transformed upstream body")
	}
	if r.ResponseBody != failure || !r.ResponseEOF || r.Status != 400 || r.RequestID != out.Header().Get("X-Proxy-Request-ID") {
		t.Fatalf("bad trace: %+v", r)
	}
	if r.ResponseHeaders["X-Request-Id"] != "synthetic-upstream-id" || r.RequestHeaders["Anthropic-Beta"] != "synthetic-beta" {
		t.Fatal("missing correlation/protocol headers")
	}
	data, _ := os.ReadFile(files[0])
	if strings.Contains(string(data), "canary") {
		t.Fatal("credential header leaked")
	}
	for path, mode := range map[string]os.FileMode{files[0]: 0600, filepath.Dir(files[0]): 0700} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("unsafe permissions: %s", path)
		}
	}
}

func TestErrorTraceGatesAndMetadata(t *testing.T) {
	for _, name := range []string{"disabled", "expired", "success", "other-provider", "metadata"} {
		t.Run(name, func(t *testing.T) {
			s := traceFixture(t, false)
			status, provider := 400, "endpoint"
			switch name {
			case "disabled":
				s.cfg.Logging.ErrorTrace.Enabled = false
			case "expired":
				s.cfg.Logging.ErrorTrace.ExpiresAt = time.Now().Add(-time.Minute).Format(time.RFC3339)
			case "success":
				status = 200
			case "other-provider":
				provider = "elsewhere"
			}
			resp := &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("private-response"))}
			s.traceFailedHTTPAttempt(resp, []byte("private-request"), modelConfig{}, provider, "test-id", 0)
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(body) != "private-response" {
				t.Fatal("body changed")
			}
			files := traceFiles(t, s)
			if name != "metadata" {
				if len(files) != 0 {
					t.Fatal("capture gate ignored")
				}
				return
			}
			if len(files) != 1 {
				t.Fatal("missing metadata")
			}
			r := loadTrace(t, files[0])
			if r.RequestBody != "" || r.ResponseBody != "" || r.RequestBytes != len("private-request") || r.RequestSHA256 == "" {
				t.Fatal("unsafe/incomplete metadata")
			}
		})
	}
}

func TestErrorTraceBoundsRetentionAndConcurrency(t *testing.T) {
	s := traceFixture(t, true)
	for i := 0; i < traceSlots+3; i++ {
		resp := &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", traceResponseLimit+1)))}
		s.traceFailedHTTPAttempt(resp, []byte(strings.Repeat("x", traceRequestLimit+1)), modelConfig{}, "endpoint", "test", i)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		resp.Body.Close()
	}
	files := traceFiles(t, s)
	if len(files) != traceSlots || s.errorTraceSequence != traceSlots+3 {
		t.Fatal("retention or once-only write broken")
	}
	for _, f := range files {
		r := loadTrace(t, f)
		if !r.RequestTruncated || !r.ResponseTruncated || len(r.RequestBody) != traceRequestLimit || len(r.ResponseBody) != traceResponseLimit {
			t.Fatal("unbounded record")
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.writeErrorTrace(errorTraceRecord{RequestID: "concurrent"}) }()
	}
	wg.Wait()
	if len(traceFiles(t, s)) != traceSlots {
		t.Fatal("concurrency exceeded retention")
	}
}

func TestErrorTraceRejectsUnsafeDirectoryAndInvalidConfig(t *testing.T) {
	s := traceFixture(t, true)
	dir := filepath.Join(filepath.Dir(s.cfg.Logging.Path), "error-traces")
	if err := os.Symlink(t.TempDir(), dir); err != nil {
		t.Fatal(err)
	}
	if err := s.persistErrorTrace(errorTraceRecord{}); err == nil {
		t.Fatal("followed symlink")
	}
	for _, c := range []errorTraceConfig{
		{Enabled: true},
		{Enabled: true, ExpiresAt: time.Now().Format(time.RFC3339)},
		{Enabled: true, ExpiresAt: time.Now().Format(time.RFC3339), Providers: []string{"missing"}},
	} {
		if validateErrorTrace(c, nil) == nil {
			t.Fatal("invalid trace config accepted")
		}
	}
}
