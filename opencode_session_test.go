package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenCodeConversationIdentity(t *testing.T) {
	get := func(user, model, header string, messages []any) string {
		r := httptest.NewRequest("POST", "/v1/messages", nil)
		r.Header.Set("x-opencode-session", header)
		return withOpenCodeSession(r, map[string]any{"model": model, "metadata": map[string]any{"user_id": user}, "messages": messages}).Context().Value(openCodeSessionKey{}).(string)
	}
	first := []any{map[string]any{"role": "user", "content": "prompt-canary"}}
	more := append(append([]any{}, first...), map[string]any{"role": "assistant", "content": "reply"})
	for _, user := range []string{`{"session_id":"conversation-a","account_uuid":"private-account"}`, "user_private_account_private_session_conversation-a", ""} {
		a, b := get(user, "model-a", "", first), get(user, "model-b", "", more)
		if a != b || strings.Contains(a, "private") || strings.Contains(a, "prompt") || !validConversationID(a) {
			t.Fatal("unstable or unsanitized session")
		}
	}
	if get("", "", "explicit-id", first) != "explicit-id" {
		t.Fatal("explicit header not preserved")
	}
	if get(`{"session_id":"a"}`, "", "", first) == get(`{"session_id":"b"}`, "", "", first) {
		t.Fatal("different conversations merged")
	}
	if get(`{"session_id":"a"}`, "", "", first) != get(`{"session_id":"a"}`, "", "", nil) {
		t.Fatal("compaction changed explicit conversation ID")
	}
	if validConversationID(strings.Repeat("a", 257)) || validConversationID("bad\nheader") {
		t.Fatal("unsafe header accepted")
	}
}

func TestOpenCodeHeaderBothAdaptersAndProviderIsolation(t *testing.T) {
	for _, protocol := range []string{"anthropic", "openai-chat-completions"} {
		t.Run(protocol, func(t *testing.T) {
			var sessions, agents []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sessions = append(sessions, r.Header.Get("x-opencode-session"))
				agents = append(agents, r.Header.Get("User-Agent"))
				w.Header().Set("Content-Type", "application/json")
				if protocol == "anthropic" {
					io.WriteString(w, testMessageResponse)
				} else {
					io.WriteString(w, `{"id":"test","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
				}
			}))
			defer upstream.Close()
			f := testConfig(upstream.URL, protocol)
			p := f.Providers["endpoint"]
			p.Variant = "opencode-go"
			f.Providers["endpoint"] = p
			s := testProxy(t, compileFixture(t, f))
			for _, message := range []string{"hello", "after-compaction"} {
				body, _ := json.Marshal(map[string]any{"model": "test", "metadata": map[string]string{"user_id": `{"session_id":"stable-session"}`}, "messages": []any{map[string]string{"role": "user", "content": message}}})
				w := sendTestRequest(s, string(body))
				if w.Code != 200 {
					t.Fatalf("request status %d: %s", w.Code, w.Body.String())
				}
			}
			if len(sessions) != 2 || sessions[0] == "" || sessions[0] != sessions[1] || agents[0] != "claude-gateway/1" {
				t.Fatalf("headers missing or unstable: %v", sessions)
			}
			p = s.cfg.Definition.Providers["endpoint"]
			p.Variant = "generic"
			f.Providers["endpoint"] = p
			s2 := testProxy(t, compileFixture(t, f))
			r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"hello"}]}`))
			r.Header.Set("x-opencode-session", "private-conversation")
			w := httptest.NewRecorder()
			s2.handler().ServeHTTP(w, r)
			if sessions[len(sessions)-1] != "" {
				t.Fatal("OpenCode conversation header leaked to unrelated provider")
			}
		})
	}
}

func TestOpenCodeSessionSurvivesSequentialFallback(t *testing.T) {
	var sessions []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessions = append(sessions, r.Header.Get("x-opencode-session"))
		w.Header().Set("Content-Type", "application/json")
		if len(sessions) == 1 {
			w.WriteHeader(503)
			io.WriteString(w, `{"error":{"type":"api_error","message":"unavailable"}}`)
			return
		}
		io.WriteString(w, testMessageResponse)
	}))
	defer upstream.Close()
	f := testConfig(upstream.URL, "anthropic")
	p := f.Providers["endpoint"]
	p.Variant = "opencode-go"
	f.Providers["endpoint"] = p
	f.Providers["backup"] = p
	m := f.Models["model"]
	m.Provider = "backup"
	f.Models["backup"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "backup"}}}
	s := testProxy(t, compileFixture(t, f))
	w := sendTestRequest(s, `{"model":"test","metadata":{"session_id":"conversation"},"messages":[{"role":"user","content":"hello"}]}`)
	if w.Code != 200 || len(sessions) != 2 || sessions[0] == "" || sessions[0] != sessions[1] {
		t.Fatalf("fallback lost identity: status=%d attempts=%d", w.Code, len(sessions))
	}
}
