package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestBodyLimits(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprint(chunked), func(t *testing.T) {
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			defer upstream.Close()
			f := testConfig(upstream.URL, "anthropic")
			f.BodyLimits.RequestMaxBytes = 128
			s := testProxy(t, compileFixture(t, f))
			req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"`+strings.Repeat("x", 500)+`"}]}`))
			if chunked {
				req.ContentLength = -1
			}
			w := httptest.NewRecorder()
			s.handler().ServeHTTP(w, req)
			if w.Code != 413 || calls.Load() != 0 {
				t.Fatalf("status=%d calls=%d", w.Code, calls.Load())
			}
		})
	}
}

func TestSlowRequestBodyDeadline(t *testing.T) {
	f := testConfig("http://127.0.0.1:1", "anthropic")
	f.BodyLimits.RequestReadTimeoutMS = 40
	s := testProxy(t, compileFixture(t, f))
	server := httptest.NewServer(s.handler())
	defer server.Close()
	c, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	fmt.Fprint(c, "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000\r\nContent-Type: application/json\r\n\r\n{")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 408 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestBufferedResponseFailuresFallback(t *testing.T) {
	for _, mode := range []string{"timeout", "truncated", "malformed", "oversized", "error-timeout"} {
		t.Run(mode, func(t *testing.T) {
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "timeout", "error-timeout":
					if mode == "error-timeout" {
						w.WriteHeader(503)
					}
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				case "truncated":
					w.Header().Set("Content-Length", "1000")
					io.WriteString(w, `{"content":`)
				case "malformed":
					io.WriteString(w, `{"content":`)
				case "oversized":
					io.WriteString(w, `{"content":"`+strings.Repeat("x", 512)+`"}`)
				}
			}))
			defer primary.Close()
			var calls atomic.Int64
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, testMessageResponse)
			}))
			defer backup.Close()
			f := testConfig(primary.URL, "anthropic")
			p := f.Providers["endpoint"]
			p.BaseURL = backup.URL
			f.Providers["backup"] = p
			m := f.Models["model"]
			m.Provider = "backup"
			f.Models["backup"] = m
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "backup"}}}
			f.BodyLimits = bodyLimitsConfig{JSONMaxBytes: 256, JSONReadTimeoutMS: 40, ErrorReadTimeoutMS: 40}
			s := testProxy(t, compileFixture(t, f))
			w := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
			if w.Code != 200 || calls.Load() != 1 || !json.Valid(w.Body.Bytes()) {
				t.Fatalf("status=%d fallback=%d", w.Code, calls.Load())
			}
			for _, v := range s.metrics.readPerRoute(100) {
				if v.Provider == "endpoint" && v.Success {
					t.Fatal("failed primary marked successful")
				}
			}
		})
	}
}

type partialJSONError struct{}

func (partialJSONError) Read(p []byte) (int, error) {
	return copy(p, `{"content":`), io.ErrUnexpectedEOF
}
func (partialJSONError) Close() error { return nil }
func TestJSONAdaptersPreserveReadErrors(t *testing.T) {
	for _, r := range []io.ReadCloser{newJSONFilterReadCloser(partialJSONError{}, nil, "test"), newOpenAIJSONReadCloser(partialJSONError{}, "test")} {
		b, err := io.ReadAll(r)
		r.Close()
		if !errors.Is(err, io.ErrUnexpectedEOF) || len(b) != 0 {
			t.Fatalf("bytes=%d err=%v", len(b), err)
		}
	}
}

func TestBoundedBodyCancellationAndDribble(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelCaller), func(t *testing.T) {
			r, w := io.Pipe()
			defer w.Close()
			defer r.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := readBoundedResponseBody(ctx, r, 1024, 50*time.Millisecond); done <- err }()
			if cancelCaller {
				cancel()
			} else {
				go func() {
					for {
						if _, err := w.Write([]byte("x")); err != nil {
							return
						}
						time.Sleep(5 * time.Millisecond)
					}
				}()
			}
			select {
			case err := <-done:
				want := errBodyReadTimeout
				if cancelCaller {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("unbounded body read")
			}
		})
	}
}

func TestBodyLimitsConfigValidation(t *testing.T) {
	for _, c := range []bodyLimitsConfig{{RequestMaxBytes: -1}, {JSONMaxBytes: 513 << 20}, {RequestReadTimeoutMS: -1}, {JSONReadTimeoutMS: 600001}, {ErrorReadTimeoutMS: -1}} {
		f := testConfig("http://127.0.0.1:1", "anthropic")
		f.BodyLimits = c
		b, _ := json.Marshal(f)
		if _, err := decodeFileConfig("/synthetic/config.json", b); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
}

func TestBodyLimitExactBoundary(t *testing.T) {
	for _, size := range []int{1023, 1024, 1025} {
		body, err := readBoundedResponseBody(context.Background(), io.NopCloser(strings.NewReader(strings.Repeat("x", size))), 1024, time.Second)
		if size > 1024 {
			if !errors.Is(err, errBodyTooLarge) || len(body) != 0 {
				t.Fatalf("size=%d bytes=%d error=%v", size, len(body), err)
			}
		} else if err != nil || len(body) != size {
			t.Fatalf("size=%d bytes=%d error=%v", size, len(body), err)
		}
	}
}

func TestBodyLimitsConfigRoundTrip(t *testing.T) {
	f := testConfig("http://127.0.0.1:1", "anthropic")
	f.BodyLimits = bodyLimitsConfig{RequestMaxBytes: 96 << 20, JSONMaxBytes: 48 << 20, RequestReadTimeoutMS: 45000, JSONReadTimeoutMS: 30000, ErrorReadTimeoutMS: 2000}
	encoded, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := decodeFileConfig("/synthetic/config.json", encoded)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BodyLimits != f.BodyLimits {
		t.Fatalf("body limits changed: %+v", cfg.BodyLimits)
	}
}
