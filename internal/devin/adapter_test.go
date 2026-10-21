package devin

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
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

const testKey = "synthetic-adapter-key-for-tests"
const testToken = "synthetic-cli-credential"

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func envelope(flags byte, b []byte) []byte {
	return append(binary.BigEndian.AppendUint32([]byte{flags}, uint32(len(b))), b...)
}
func done() []byte { return envelope(2, []byte(`{}`)) }
func response(body []byte) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/connect+proto"}}, Body: io.NopCloser(bytes.NewReader(body))}
}
func textFrames() []byte {
	return join(envelope(0, ps(3, "OK")), envelope(0, join(pv(5, 2), pb(7, join(pv(2, 11), pv(3, 3), ps(9, "swe-2-high"))))), done())
}
func baseRequest(stream bool) string {
	if stream {
		return `{"model":"swe-2-high","messages":[{"role":"user","content":"Hi"}],"stream":true,"stream_options":{"include_usage":true}}`
	}
	return `{"model":"swe-2-high","messages":[{"role":"user","content":"Hi"}]}`
}
func handler(t *testing.T, transport http.RoundTripper, observe func(Event)) *Handler {
	t.Helper()
	h, err := NewHandler(Config{APIKey: testKey, TokenSource: func(context.Context) (string, error) { return testToken, nil }, HTTPClient: &http.Client{Transport: transport}, Observe: observe})
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func call(h http.Handler, body string, auth bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	if auth {
		r.Header.Set("Authorization", "Bearer "+testKey)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestStreamingAndJSON(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(map[bool]string{true: "SSE", false: "JSON"}[stream], func(t *testing.T) {
			var count int
			var event Event
			h := handler(t, roundTrip(func(r *http.Request) (*http.Response, error) {
				count++
				if r.URL.String() != endpoint || r.Header.Get("Authorization") != "Basic "+testToken+"-"+testToken {
					t.Fatal("incorrect destination/auth")
				}
				b, _ := io.ReadAll(r.Body)
				p, err := parse(b[5:])
				if err != nil || p.str(21) != "swe-2-high" {
					t.Fatal("bad model wire")
				}
				meta, err := parse(p[1][0].b)
				if err != nil || meta.str(3) != testToken || len(meta.str(31)) != 732 {
					t.Fatal("bad metadata wire")
				}
				return response(textFrames()), nil
			}), func(e Event) { event = e })
			w := call(h, baseRequest(stream), true)
			if w.Code != 200 || !strings.Contains(w.Body.String(), "OK") || count != 1 || event.Outcome != "complete" {
				t.Fatalf("code=%d count=%d event=%+v body=%s", w.Code, count, event, w.Body)
			}
			if event.Usage == nil || event.Usage.Total != 14 {
				t.Fatal("usage lost")
			}
			if stream {
				b := w.Body.String()
				if !strings.Contains(b, "[DONE]") || strings.Index(b, `"finish_reason":"stop"`) > strings.Index(b, `"usage"`) {
					t.Fatal("bad completion ordering")
				}
			} else {
				var out map[string]any
				if json.Unmarshal(w.Body.Bytes(), &out) != nil || out["object"] != "chat.completion" {
					t.Fatal("bad JSON response")
				}
			}
		})
	}
}

func TestNativeToolsAndHistory(t *testing.T) {
	tool := `{"type":"function","function":{"name":"lookup","description":"Test tool","parameters":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}}}`
	body := `{"model":"swe-2-high","messages":[{"role":"user","content":"Lookup"}],"stream":true,"tools":[` + tool + `]}`
	wire := join(envelope(0, pb(6, join(ps(1, "call_0"), ps(2, "lookup")))), envelope(0, pb(6, ps(3, `{"key":`))), envelope(0, pb(6, ps(3, `"alpha"}`))), envelope(0, pv(5, 2)), done())
	h := handler(t, roundTrip(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		p, _ := parse(b[5:])
		if len(p[10]) != 1 {
			t.Fatal("missing native tool definition")
		}
		return response(wire), nil
	}), nil)
	w := call(h, body, true)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"finish_reason":"tool_calls"`) || strings.Count(w.Body.String(), `"name":"lookup"`) != 1 {
		t.Fatalf("bad native stream %s", w.Body)
	}
	history := `{"model":"swe-2-high","messages":[{"role":"user","content":"Lookup"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_0","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"alpha\"}"}}]},{"role":"tool","tool_call_id":"call_0","content":"result"}],"tools":[` + tool + `]}`
	h = handler(t, roundTrip(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		p, _ := parse(b[5:])
		if len(p[3]) != 3 {
			t.Fatal("history lost")
		}
		a, _ := parse(p[3][1].b)
		tc, _ := parse(a[6][0].b)
		result, _ := parse(p[3][2].b)
		if tc.str(1) != "call_0" || tc.str(2) != "lookup" || tc.str(3) != `{"key":"alpha"}` || result.str(7) != "call_0" || result.str(3) != "result" {
			t.Fatal("tool history corrupted")
		}
		return response(textFrames()), nil
	}), nil)
	if w := call(h, history, true); w.Code != 200 {
		t.Fatalf("history rejected %s", w.Body)
	}
}

func TestFailuresNoRetryNoFalseDone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wire    []byte
		status  int
		partial bool
	}{
		{"auth", envelope(2, []byte(`{"error":{"code":"unauthenticated","message":"synthetic-secret-do-not-echo"}}`)), 401, false},
		{"rate", envelope(2, []byte(`{"error":{"code":"resource_exhausted"}}`)), 429, false},
		{"invalid", envelope(2, []byte(`{"error":{"code":"invalid_argument"}}`)), 400, false},
		{"truncated", envelope(0, ps(3, "partial")), 200, true},
		{"midstream", join(envelope(0, ps(3, "partial")), envelope(2, []byte(`{"error":{"code":"unavailable"}}`))), 200, true},
		{"bad_enum", join(envelope(0, pv(5, 99)), done()), 502, false},
		{"malformed", envelope(0, []byte{0xff}), 502, false},
		{"empty", done(), 502, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			h := handler(t, roundTrip(func(*http.Request) (*http.Response, error) { calls++; return response(tc.wire), nil }), nil)
			w := call(h, baseRequest(true), true)
			if w.Code != tc.status || calls != 1 || strings.Contains(w.Body.String(), "[DONE]") || strings.Contains(w.Body.String(), "synthetic-secret") || !strings.Contains(w.Body.String(), `"error"`) {
				t.Fatalf("bad failure: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestValidationAndAuthNeverDial(t *testing.T) {
	h := handler(t, roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected upstream call"); return nil, nil }), nil)
	if w := call(h, baseRequest(true), false); w.Code != 401 {
		t.Fatal("missing auth accepted")
	}
	for _, body := range []string{
		strings.Replace(baseRequest(true), "swe-2-high", "unknown", 1),
		strings.TrimSuffix(baseRequest(true), "}") + `,"tool_choice":"required"}`,
		strings.TrimSuffix(baseRequest(true), "}") + `,"parallel_tool_calls":false}`,
		strings.TrimSuffix(baseRequest(true), "}") + `,"response_format":{"type":"json_object"}}`,
		strings.Replace(baseRequest(true), `"Hi"`, `[{"type":"image_url","image_url":{"url":"https://example.invalid/image"}}]`, 1),
		baseRequest(true) + `{}`,
		`{"model":"swe-2-high","messages":[{"role":"tool","tool_call_id":"unknown","content":"bad"}]}`,
	} {
		if w := call(h, body, true); w.Code != 400 {
			t.Fatalf("invalid request accepted: %d %s", w.Code, w.Body)
		}
	}
}

func TestCredentialsReloadAndRedaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.toml")
	source := Credentials(path, nil)
	for _, token := range []string{"synthetic-first", "synthetic-rotated"} {
		if err := os.WriteFile(path, []byte("windsurf_api_key = \""+token+"\"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := source(context.Background())
		if err != nil || got != token {
			t.Fatal("credential reload failed")
		}
	}
	if got, err := Credentials("does-not-exist", func() string { return "synthetic-env" })(context.Background()); err != nil || got != "synthetic-env" {
		t.Fatal("env override failed")
	}
	if _, err := Credentials("does-not-exist", nil)(context.Background()); err == nil || strings.Contains(err.Error(), "does-not-exist") {
		t.Fatal("path leaked or missing file accepted")
	}
	if _, err := validToken("synthetic\r\nHeader: injected"); err == nil {
		t.Fatal("header injection allowed")
	}
	if err := os.WriteFile(path, []byte("[other]\nwindsurf_api_key = 'not-root'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := source(context.Background()); err == nil {
		t.Fatal("non-root token accepted")
	}
}

func TestCompressedFramesAndBounds(t *testing.T) {
	var buf bytes.Buffer
	z := gzip.NewWriter(&buf)
	_, _ = z.Write(ps(3, "compressed"))
	_ = z.Close()
	p, terminal, err := readFrame(bytes.NewReader(envelope(1, buf.Bytes())))
	if err != nil || terminal || p.str(3) != "compressed" {
		t.Fatal("gzip decode failed")
	}
	buf.Reset()
	z = gzip.NewWriter(&buf)
	_, _ = z.Write(bytes.Repeat([]byte("a"), maxFrame+1))
	_ = z.Close()
	if _, _, err := readFrame(bytes.NewReader(envelope(1, buf.Bytes()))); err == nil {
		t.Fatal("gzip bomb accepted")
	}
	if _, _, err := readFrame(bytes.NewReader(binary.BigEndian.AppendUint32([]byte{0}, maxFrame+1))); err == nil {
		t.Fatal("oversized frame accepted")
	}
	for _, b := range [][]byte{{0}, {0xff}, {0x0a, 0xff}, {0x0b}, {0x0a, 4, 1}} {
		if _, err := parse(b); err == nil {
			t.Fatalf("malformed protobuf accepted %v", b)
		}
	}
}

func TestRealHTTPDisconnectCancelsSilentUpstream(t *testing.T) {
	started, closed := make(chan struct{}), make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/connect+proto")
		_, _ = w.Write(envelope(0, ps(3, "first")))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(closed)
	}))
	defer up.Close()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	h := handler(t, roundTrip(func(r *http.Request) (*http.Response, error) {
		clone := r.Clone(r.Context())
		clone.URL.Scheme = "http"
		clone.URL.Host = strings.TrimPrefix(up.URL, "http://")
		return transport.RoundTrip(clone)
	}), nil)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(baseRequest(true)))
	r.Header.Set("Authorization", "Bearer "+testKey)
	res, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = bufio.NewReader(res.Body).ReadString('\n')
	<-started
	cancel()
	res.Body.Close()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("silent upstream not cancelled")
	}
}

func TestConcurrentRequestsIsolated(t *testing.T) {
	var calls atomic.Int64
	h := handler(t, roundTrip(func(*http.Request) (*http.Response, error) { calls.Add(1); return response(textFrames()), nil }), nil)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w := call(h, baseRequest(true), true); w.Code != 200 || !strings.Contains(w.Body.String(), "[DONE]") {
				t.Error("concurrent request failed")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 12 {
		t.Fatal("duplicated or dropped requests")
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	count := 0
	h := handler(t, roundTrip(func(*http.Request) (*http.Response, error) {
		count++
		return &http.Response{StatusCode: 307, Header: http.Header{"Location": []string{"https://example.invalid/credential-theft"}}, Body: io.NopCloser(strings.NewReader("do-not-echo"))}, nil
	}), nil)
	w := call(h, baseRequest(true), true)
	if count != 1 || w.Code != 502 || strings.Contains(w.Body.String(), "do-not-echo") {
		t.Fatal("redirect followed or raw error leaked")
	}
}

func TestToolDecoderRejectsUnknownOrAmbiguousCalls(t *testing.T) {
	s := streamState{byID: map[string]int{}, allowed: map[string]bool{"lookup": true}}
	p, _ := parse(pb(6, ps(3, "{}")))
	if _, err := s.consume(p); err == nil {
		t.Fatal("orphan allowed")
	}
	p, _ = parse(pb(6, join(ps(1, "id"), ps(2, "unknown"))))
	if _, err := s.consume(p); err == nil {
		t.Fatal("unknown tool allowed")
	}
	for _, id := range []string{"first", "second"} {
		p, _ = parse(pb(6, join(ps(1, id), ps(2, "lookup"))))
		if _, err := s.consume(p); err != nil {
			t.Fatal(err)
		}
	}
	p, _ = parse(pb(6, ps(3, "{}")))
	if _, err := s.consume(p); err == nil {
		t.Fatal("ambiguous continuation allowed")
	}
}

func FuzzFrameParser(f *testing.F) {
	f.Add(envelope(0, ps(3, "seed")))
	f.Add(done())
	f.Add([]byte{255})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > maxFrame {
			return
		}
		_, _, _ = readFrame(bytes.NewReader(b))
	})
}
