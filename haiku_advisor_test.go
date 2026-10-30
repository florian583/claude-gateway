package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHaikuOmitsUnusedAdvisorOnly(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		history     []any
		wantAdvisor bool
	}{
		{"haiku", "claude-haiku-5-5", nil, false},
		{"other-model", "claude-opus-5-5", nil, false},
		{"advisor-history", "claude-haiku-5-5", []any{map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "server_tool_use", "name": "advisor", "id": "srv_1"}}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := map[string]any{"model": tc.model, "messages": tc.history, "tools": []any{map[string]any{"type": "advisor_20260301", "name": "advisor"}, map[string]any{"name": "Read", "input_schema": map[string]any{"type": "object"}}}}
			body, _ := json.Marshal(original)
			gotBody, got := omitUnusedAdvisor(body, original)
			var decoded map[string]any
			if err := json.Unmarshal(gotBody, &decoded); err != nil {
				t.Fatal(err)
			}
			for _, value := range []map[string]any{got, decoded} {
				tools := value["tools"].([]any)
				hasAdvisor := false
				for _, raw := range tools {
					if raw.(map[string]any)["name"] == "advisor" {
						hasAdvisor = true
					}
				}
				if hasAdvisor != tc.wantAdvisor {
					t.Fatalf("advisor=%v want=%v", hasAdvisor, tc.wantAdvisor)
				}
				if len(tools) == 0 || tools[len(tools)-1].(map[string]any)["name"] != "Read" {
					t.Fatal("Read tool lost")
				}
			}
			if len(original["tools"].([]any)) != 2 {
				t.Fatal("original mutated")
			}
		})
	}
}

func TestHaikuAdvisorFallsBackToGLM(t *testing.T) {
	called := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if _, ok := payload["tools"]; ok {
			t.Error("advisor reached GLM")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testMessageResponse)
	}))
	defer upstream.Close()
	f := testClaudeConfig(t)
	m := f.Models["model"]
	m.Upstream = "claude-haiku-5-5"
	f.Models["model"] = m
	f.Providers["glm"] = providerDefinition{Protocol: "anthropic", BaseURL: upstream.URL, Billing: "local", Auth: authDefinition{Type: "none"}}
	m.Provider, m.Upstream = "glm", "zai/glm-5.3-flash"
	f.Models["glm"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "glm"}}}
	cfg := compileFixture(t, f)
	seedTestCredentials(t, cfg)
	s := testProxy(t, cfg)
	for _, profile := range cfg.ClaudeUsage.AccountProfiles {
		s.markProviderBlocked("endpoint@"+profile.Name, "quota_exhausted", time.Hour)
	}
	payload := toolTurnPayload()
	payload["model"] = "test"
	payload["tools"] = []any{map[string]any{"name": "advisor", "type": "advisor_20260301"}}
	body, _ := json.Marshal(payload)
	out := sendTestRequest(s, string(body))
	if out.Code != 200 || !called {
		t.Fatalf("status=%d called=%v body=%s", out.Code, called, out.Body.String())
	}
}

func TestNonClaudeOmitsEmptySystemMessage(t *testing.T) {
	original := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hello"}, map[string]any{"role": "system", "content": []any{}}}}
	body, _ := json.Marshal(original)
	gotBody, got := omitEmptySystemMessage(body, original)
	if len(got["messages"].([]any)) != 1 {
		t.Fatal("empty system message retained")
	}
	var decoded map[string]any
	if err := json.Unmarshal(gotBody, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded["messages"].([]any)) != 1 {
		t.Fatal("empty system message retained in body")
	}
	if len(original["messages"].([]any)) != 2 {
		t.Fatal("original mutated")
	}
}
