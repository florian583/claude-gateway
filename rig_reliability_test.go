package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEmptyOpenAICompletionRejectedBeforeHeaders(t *testing.T) {
	for _, body := range []string{
		`{"choices":[{"message":{"content":null,"reasoning_content":"thinking"},"finish_reason":"length"}],"usage":{"completion_tokens":120}}`,
		`{"choices":[]}`,
	} {
		s := &proxyServer{}
		resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
		if err := s.bufferUpstreamBody(context.Background(), resp, requestTrace{}, responseContract{openAI: true}); err == nil {
			t.Fatal("accepted empty completion")
		}
	}
	for _, body := range []string{
		`{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"tool_calls":[{"id":"call_1","function":{"name":"answer","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
	} {
		if _, bad := translateOpenAIJSONResponseChecked([]byte(body), "test"); bad {
			t.Fatal("rejected useful response")
		}
	}
}

func TestStrictPreludeDeadlineIgnoresReasoningHeartbeat(t *testing.T) {
	reader, writer := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer writer.Close()
		for {
			if _, err := io.WriteString(writer, "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n"); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	start := time.Now()
	_, err := primeSSEBodyObserved(reader, 30*time.Millisecond, 0, nil, nil, true)
	reader.Close()
	<-done
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("deadline not enforced: %v", err)
	}
}

func TestStrictPreludePreservesBufferedContent(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n" + textFrame
	body, err := primeSSEBodyObserved(io.NopCloser(strings.NewReader(input)), time.Second, 0, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil || string(got) != input {
		t.Fatalf("altered prelude: %q %v", got, err)
	}
}

func TestRigDeadlinesCompile(t *testing.T) {
	f := testConfig("http://127.0.0.1:12345", "anthropic")
	c := f.Chains["main"]
	c.RequestTimeoutMS = 60000
	c.FirstContentTimeoutMS = 30000
	f.Chains["main"] = c
	cfg := compileFixture(t, f)
	m := cfg.Models["test"]
	if m.RequestTimeoutMS != 60000 || m.FirstContentTimeoutMS != 30000 || len(m.Fallbacks) != 0 {
		t.Fatalf("lost exact-route policy: %+v", m)
	}
}

func TestRigHTTPFailureContracts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		wait   bool
		want   int
	}{
		{"empty", 200, `{"choices":[{"message":{"reasoning_content":"thinking"},"finish_reason":"length"}]}`, false, 502},
		{"limited", 429, `{"error":{"message":"rate limited"}}`, false, 429},
		{"deadline", 200, "", true, 504},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload map[string]any
				json.NewDecoder(r.Body).Decode(&payload)
				if payload["model"] != "test-model" {
					t.Error("model substituted")
				}
				if tc.wait {
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer upstream.Close()
			f := testConfig(upstream.URL, "openai-chat-completions")
			chain := f.Chains["main"]
			chain.RequestTimeoutMS = 100
			f.Chains["main"] = chain
			s, err := newProxyServer(compileFixture(t, f))
			if err != nil {
				t.Fatal(err)
			}
			defer s.metrics.close()
			out := httptest.NewRecorder()
			s.handler().ServeHTTP(out, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"hello"}]}`)))
			if out.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", out.Code, tc.want, out.Body.String())
			}
		})
	}
}

func TestStrictNativeResponseContract(t *testing.T) {
	for _, tc := range []struct {
		body   string
		choice any
		valid  bool
	}{
		{`{"content":[]}`, nil, false},
		{`{"content":[{"type":"thinking","thinking":"hidden"}]}`, nil, false},
		{`{"content":[{"type":"text","text":"hello"}]}`, nil, true},
		{`{"content":[{"type":"text","text":"hello"}]}`, map[string]any{"type": "any"}, false},
		{`{"content":[{"type":"tool_use","name":"other"}]}`, map[string]any{"type": "tool", "name": "answer"}, false},
		{`{"content":[{"type":"tool_use","name":"answer"}]}`, map[string]any{"type": "tool", "name": "answer"}, true},
	} {
		err := (responseContract{strict: true, toolChoice: tc.choice}).validate([]byte(tc.body))
		if (err == nil) != tc.valid {
			t.Fatalf("contract result %v for %s", err, tc.body)
		}
	}
}

func TestOpenAIErrorEnvelopeIsProtocolFailure(t *testing.T) {
	if _, invalid := translateOpenAIJSONResponseChecked([]byte(`{"error":{"message":"upstream unavailable"}}`), "test"); !invalid {
		t.Fatal("accepted error as successful completion")
	}
}

func TestRigEndpointIsolatesModelPolicy(t *testing.T) {
	var upstreamModels []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		upstreamModels = append(upstreamModels, body["model"].(string))
		if r.URL.Path != "/v1/messages" {
			t.Errorf("unexpected upstream path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"type":"message","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer upstream.Close()
	f := testConfig(upstream.URL, "anthropic")
	model := f.Models["model"]
	model.Upstream = "exact-test-model"
	f.Models["exact"] = model
	f.Chains["rig"] = chainDefinition{Steps: []chainStep{{Model: "exact"}}, RequestTimeoutMS: 1000, FirstContentTimeoutMS: 500}
	f.Aliases["rig-private"] = "rig"
	f.RigModels = map[string]string{"test": "rig-private"}
	f.RigListen = "127.0.0.1:48105"
	cfg := compileFixture(t, f)
	if cfg.Models["test"].RequestTimeoutMS != 0 {
		t.Fatal("rig deadline leaked into normal route")
	}
	s, err := newProxyServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.metrics.close()
	for _, tc := range []struct {
		path, model string
		status      int
	}{
		{"/v1/messages", "test", 200},
		{"/rig/v1/messages", "test", 200},
		{"/v1/messages", "rig-private", 400},
		{"/rig/v1/messages", "rig-private", 400},
		{"/rig/v1/messages", "unknown", 400},
	} {
		rec := httptest.NewRecorder()
		handler := s.handler()
		path := tc.path
		if strings.HasPrefix(path, "/rig/") {
			handler = s.rigHandler()
			path = strings.TrimPrefix(path, "/rig")
		}
		handler.ServeHTTP(rec, httptest.NewRequest("POST", path, strings.NewReader(`{"model":"`+tc.model+`","messages":[{"role":"user","content":"hello"}]}`)))
		if rec.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.path, tc.model, rec.Code, rec.Body.String())
		}
		if tc.path == "/rig/v1/messages" && rec.Header().Get("X-Proxy-Usage") != "rig" {
			t.Fatal("missing rig provenance")
		}
	}
	if len(upstreamModels) != 2 || upstreamModels[0] != "test-model" || upstreamModels[1] != "exact-test-model" {
		t.Fatalf("wrong upstream requests: %v", upstreamModels)
	}
}

func TestRigEndpointDisabledWithoutConfiguration(t *testing.T) {
	s, err := newProxyServer(compileFixture(t, testConfig("http://127.0.0.1:1", "anthropic")))
	if err != nil {
		t.Fatal(err)
	}
	defer s.metrics.close()
	rec := httptest.NewRecorder()
	s.rigHandler().ServeHTTP(rec, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test"}`)))
	if rec.Code != 404 {
		t.Fatalf("unconfigured rig endpoint: %d", rec.Code)
	}
}

func TestRigListenerRejectsUnsafeConfiguration(t *testing.T) {
	for _, address := range []string{"0.0.0.0:48105", "127.0.0.1:0", "127.0.0.1:99999", "127.0.0.1:12345", ""} {
		f := testConfig("http://127.0.0.1:1", "anthropic")
		f.Listen = "127.0.0.1:12345"
		chain := f.Chains["main"]
		chain.RequestTimeoutMS = 1000
		chain.FirstContentTimeoutMS = 500
		f.Chains["main"] = chain
		f.RigListen = address
		f.RigModels = map[string]string{"test-model": "test"}
		body, _ := json.Marshal(f)
		if _, err := decodeFileConfig("config.json", body); err == nil {
			t.Fatalf("accepted invalid rig listener %q", address)
		}
	}
}

func TestStrictPreludeMetadataAndEmptyDiagnostics(t *testing.T) {
	metadata, err := classifyUsefulPrelude(`{"type":"message_delta","delta":{},"usage":{"output_tokens":3}}`)
	if !metadata || err != nil {
		t.Fatalf("metadata rejected: %v", err)
	}
	_, err = classifyUsefulPrelude(`{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":120}}`)
	empty, ok := err.(*emptyPreludeError)
	if !ok || empty.StopReason != "max_tokens" || empty.OutputTokens != 120 {
		t.Fatalf("missing diagnostics: %#v", err)
	}
	for _, tc := range []struct {
		rig     bool
		reason  string
		retries int
		want    bool
	}{
		{true, "end_turn", 0, true}, {false, "end_turn", 0, false}, {true, "end_turn", 1, false},
		{true, "max_tokens", 0, false}, {true, "refusal", 0, false}, {true, "unknown", 0, false},
	} {
		if got := retryEmptyRigPrelude(tc.rig, &emptyPreludeError{StopReason: tc.reason}, tc.retries); got != tc.want {
			t.Fatalf("retry %+v = %v", tc, got)
		}
	}
}

func TestRigEmptyEndTurnRetriesSameModelBeforeOutput(t *testing.T) {
	for _, reason := range []string{"end_turn", "max_tokens"} {
		t.Run(reason, func(t *testing.T) {
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "text/event-stream")
				if calls == 1 {
					io.WriteString(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\""+reason+"\"},\"usage\":{\"output_tokens\":12}}\n\n")
					return
				}
				io.WriteString(w, "data: {\"type\":\"message_start\",\"message\":{\"id\":\"synthetic\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-test-model\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
				io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n")
			}))
			defer upstream.Close()
			f := testClaudeConfig(t)
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}}, RequestTimeoutMS: 1000, FirstContentTimeoutMS: 500}
			cfg := compileFixture(t, f)
			p := cfg.Providers["endpoint"]
			p.BaseURL = upstream.URL
			cfg.Providers["endpoint"] = p
			seedTestCredentials(t, cfg)
			s := testProxy(t, cfg)
			r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
			// Exercise the selected strict route while preserving the public model fixture.
			r = r.WithContext(context.WithValue(r.Context(), rigEndpointContextKey{}, true))
			cfg.RigModels = map[string]string{"test": "test"}
			s.cfg.RigModels = cfg.RigModels
			out := httptest.NewRecorder()
			s.handler().ServeHTTP(out, r)
			if reason == "end_turn" {
				if calls != 2 || out.Code != 200 || !strings.Contains(out.Body.String(), `"text":"ok"`) {
					t.Fatalf("calls=%d status=%d body=%s", calls, out.Code, out.Body.String())
				}
			} else if calls != 1 || out.Code != 502 {
				t.Fatalf("calls=%d status=%d", calls, out.Code)
			}
		})
	}
}

func TestRigEmptyEndTurnAfterToolResultPassesThrough(t *testing.T) {
	const emptyResponse = "data: {\"type\":\"message_start\",\"message\":{\"id\":\"synthetic\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-5-5\",\"content\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":0}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, emptyResponse)
	}))
	defer upstream.Close()

	f := testClaudeConfig(t)
	opus := f.Models["model"]
	opus.Upstream = "claude-opus-5-5"
	f.Models["opus"] = opus
	const chainName = "rig-exact-claude-opus-5-5"
	f.Chains[chainName] = chainDefinition{
		Steps:                 []chainStep{{Model: "opus"}},
		RequestTimeoutMS:      1000,
		FirstContentTimeoutMS: 500,
	}
	f.Aliases[chainName] = chainName
	f.RigModels = map[string]string{"claude-opus-5-5": chainName}
	f.RigListen = "127.0.0.1:48105"
	cfg := compileFixture(t, f)
	provider := cfg.Providers["endpoint"]
	provider.BaseURL = upstream.URL
	cfg.Providers["endpoint"] = provider
	seedTestCredentials(t, cfg)
	s := testProxy(t, cfg)

	request := `{"model":"claude-opus-5-5","stream":true,"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call_test","name":"report","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_test","content":"reported"}]}]}`
	out := httptest.NewRecorder()
	s.rigHandler().ServeHTTP(out, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(request)))
	if out.Code != http.StatusOK || out.Body.String() != emptyResponse {
		t.Fatalf("status=%d calls=%d body=%q", out.Code, calls, out.Body.String())
	}
	if calls != 1 {
		t.Fatalf("upstream calls=%d, want one", calls)
	}
	if got := out.Header().Get("X-Proxy-Upstream-Model"); got != "claude-opus-5-5" {
		t.Fatalf("served-model header=%q", got)
	}
	samples := s.metrics.read(10)
	if len(samples) != 1 {
		t.Fatalf("metrics samples=%d, want one completion", len(samples))
	}
	sample := samples[0]
	if sample.RecordKind != "completion" || !sample.Success || sample.FailureReason != "" ||
		sample.RequestedModel != "claude-opus-5-5" || sample.Upstream != "claude-opus-5-5" ||
		sample.Execution == nil || !sample.Execution.EmptyCompletion || sample.Execution.PreludeStopReason != "end_turn" {
		t.Fatalf("unexpected empty-completion metric: %+v", sample)
	}
}
