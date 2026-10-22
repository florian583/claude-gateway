package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestRigSubscriptionRequestPreservesCallerContract(t *testing.T) {
	cfg := compileFixture(t, testConfig("http://127.0.0.1:1", "anthropic"))
	provider := cfg.Providers["endpoint"]
	provider.Variant = "claude-subscription"
	cfg.Providers["endpoint"] = provider
	cfg.RigModels = map[string]string{"test": "test"}
	s := &proxyServer{cfg: cfg}
	raw := `{"model":"test","max_tokens":120,"system":[{"type":"text","text":"Return a short answer","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"Hello"}],"tools":[{"name":"answer","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"answer"},"metadata":{"user_id":"test-session"}}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(raw))
	req = req.WithContext(context.WithValue(req.Context(), rigEndpointContextKey{}, true))
	wire, payload, _, err := s.normalizeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	system, _ := payload["system"].([]any)
	if len(system) != 2 {
		t.Fatalf("missing subscription envelope: %v", system)
	}
	var original map[string]any
	json.Unmarshal([]byte(raw), &original)
	messages := payload["messages"].([]any)
	content := messages[0].(map[string]any)["content"].([]any)
	reminder := content[0].(map[string]any)
	if reminder["text"] != "<system-reminder>\nReturn a short answer\n</system-reminder>" {
		t.Fatal("caller instructions lost")
	}
	if reminder["cache_control"].(map[string]any)["ttl"] != "1h" {
		t.Fatal("cache missing")
	}
	for _, key := range []string{"max_tokens", "tools", "tool_choice"} {
		if !reflect.DeepEqual(payload[key], original[key]) {
			t.Fatalf("changed %s", key)
		}
	}
	if req.Header.Get("X-App") != "cli" {
		t.Fatal("missing protocol header")
	}
	var decoded map[string]any
	if json.Unmarshal(wire, &decoded) != nil || !reflect.DeepEqual(decoded, payload) {
		t.Fatal("wire payload differs")
	}
}

func TestRigCompatibilityIsolation(t *testing.T) {
	for _, rig := range []bool{false, true} {
		for _, variant := range []string{"claude-subscription", ""} {
			cfg := compileFixture(t, testConfig("http://127.0.0.1:1", "anthropic"))
			provider := cfg.Providers["endpoint"]
			provider.Variant = variant
			cfg.Providers["endpoint"] = provider
			if rig {
				cfg.RigModels = map[string]string{"test": "test"}
			}
			req := httptest.NewRequest("POST", "/v1/messages?existing=1", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"Hello"}],"max_tokens":32}`))
			if rig {
				req = req.WithContext(context.WithValue(req.Context(), rigEndpointContextKey{}, true))
			}
			_, payload, _, err := (&proxyServer{cfg: cfg}).normalizeRequest(req)
			if err != nil {
				t.Fatal(err)
			}
			adapted := rig && variant == "claude-subscription"
			if (payload["system"] != nil) != adapted {
				t.Fatalf("isolation rig=%v variant=%s", rig, variant)
			}
			if req.URL.Query().Get("existing") != "1" {
				t.Fatal("lost query")
			}
			if (req.URL.Query().Get("beta") == "true") != adapted {
				t.Fatal("beta scope")
			}
		}
	}
}

func TestRigModernSystemAndForcedTool(t *testing.T) {
	raw := `{"model":"claude-opus-5-5","system":[{"type":"text","text":"Caller instructions","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"Hello"},{"role":"assistant","content":"Hi"},{"role":"user","content":"Continue"}],"max_tokens":128,"thinking":{"type":"adaptive"},"output_config":{"effort":"high","format":{"type":"json_schema"}},"tool_choice":{"type":"tool","name":"answer"}}`
	var payload map[string]any
	json.Unmarshal([]byte(raw), &payload)
	headers := http.Header{}
	changed, err := adaptRigClaudeRequest(payload, headers)
	if err != nil || !changed {
		t.Fatal(err)
	}
	messages := payload["messages"].([]any)
	if len(messages) != 4 || messages[1].(map[string]any)["role"] != "system" {
		t.Fatal("system placement")
	}
	block := messages[1].(map[string]any)["content"].([]any)[0].(map[string]any)
	if block["text"] != "Caller instructions" || block["cache_control"].(map[string]any)["ttl"] != "5m" {
		t.Fatal("caller text/cache lost")
	}
	if payload["thinking"] != nil || payload["output_config"].(map[string]any)["effort"] != nil {
		t.Fatal("forced tool thinking retained")
	}
	if payload["output_config"].(map[string]any)["format"] == nil || payload["max_tokens"] != float64(128) || payload["fallbacks"] != nil {
		t.Fatal("caller contract changed")
	}
	before, _ := json.Marshal(payload)
	changed, err = adaptRigClaudeRequest(payload, headers)
	after, _ := json.Marshal(payload)
	if err != nil || changed || string(before) != string(after) {
		t.Fatal("adapter not idempotent")
	}
}

func TestRigCacheValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ttls    []string
		invalid bool
	}{
		{"ordered", []string{"1h", "5m"}, false},
		{"reversed", []string{"5m", "1h"}, true},
		{"too_many", []string{"1h", "1h", "1h", "1h", "1h"}, true},
		{"default_after_short", []string{"5m", ""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks := []any{}
			for _, ttl := range tc.ttls {
				cc := map[string]any{"type": "ephemeral"}
				if ttl != "" {
					cc["ttl"] = ttl
				}
				blocks = append(blocks, map[string]any{"type": "text", "text": "cache me", "cache_control": cc})
			}
			payload := map[string]any{"system": blocks}
			err := prepareRigCache(payload)
			if (err != nil) != tc.invalid {
				t.Fatalf("error %v", err)
			}
			if !tc.invalid {
				for _, block := range blocks {
					if block.(map[string]any)["cache_control"] == nil {
						t.Fatal("cache stripped")
					}
				}
			}
		})
	}
}

func TestRigInvalidSystemFailsWithoutDroppingInstructions(t *testing.T) {
	payload := map[string]any{"system": []any{map[string]any{"type": "image"}}, "messages": []any{map[string]any{"role": "user", "content": "Hello"}}}
	if _, err := adaptRigClaudeRequest(payload, http.Header{}); err == nil {
		t.Fatal("non-text system accepted")
	}
}

func TestRigSessionAndCachePrefixStable(t *testing.T) {
	var first []byte
	var session string
	for i := 0; i < 2; i++ {
		var payload map[string]any
		json.Unmarshal([]byte(`{"model":"claude-sonnet-5-5","system":"Persistent instructions","messages":[{"role":"user","content":"Hello 🌍"}],"metadata":{"user_id":"synthetic-conversation"}}`), &payload)
		headers := http.Header{}
		if _, err := adaptRigClaudeRequest(payload, headers); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(payload)
		if i == 0 {
			first = encoded
			session = headers.Get("X-Claude-Code-Session-Id")
		} else if string(encoded) != string(first) || headers.Get("X-Claude-Code-Session-Id") != session {
			t.Fatal("unstable cache prefix")
		}
		found := false
		for _, block := range rigCacheBlocks(payload) {
			if block["cache_control"] != nil {
				found = true
			}
		}
		if !found {
			t.Fatal("cache disabled")
		}
	}
}
