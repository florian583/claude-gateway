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
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(endpoint, protocol string) fileConfig {
	return fileConfig{Version: 1, Listen: "127.0.0.1:0", Providers: map[string]providerDefinition{"endpoint": {Protocol: protocol, BaseURL: endpoint, Billing: "local", Auth: authDefinition{Type: "none"}}}, Models: map[string]modelDefinition{"model": {Provider: "endpoint", Upstream: "test-model", ContextWindow: 1000000, SupportsImages: true, SupportsTools: true}}, Chains: map[string]chainDefinition{"main": {Steps: []chainStep{{Model: "model"}}}}, Aliases: map[string]string{"test": "main"}}
}
func compileFixture(t *testing.T, f fileConfig) config {
	t.Helper()
	b, e := json.Marshal(f)
	if e != nil {
		t.Fatal(e)
	}
	c, e := decodeFileConfig(filepath.Join(t.TempDir(), "config.json"), b)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func TestConfiguredConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		edit func(*fileConfig)
	}{
		{"version", func(f *fileConfig) { f.Version = 0 }},
		{"wildcard", func(f *fileConfig) { f.Listen = "0.0.0.0:9999" }},
		{"named-port", func(f *fileConfig) { f.Listen = "127.0.0.1:http" }},
		{"negative-port", func(f *fileConfig) { f.Listen = "127.0.0.1:-1" }},
		{"overflow-port", func(f *fileConfig) { f.Listen = "127.0.0.1:65536" }},
		{"remote", func(f *fileConfig) { f.Listen = "192.168.1.2:9999" }},
		{"protocol", func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.Protocol = "openai-responses"
			f.Providers["endpoint"] = p
		}},
		{"variant", func(f *fileConfig) { p := f.Providers["endpoint"]; p.Variant = "magic"; f.Providers["endpoint"] = p }},
		{"billing", func(f *fileConfig) { p := f.Providers["endpoint"]; p.Billing = ""; f.Providers["endpoint"] = p }},
		{"remote-http", func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.BaseURL = "http://example.com"
			f.Providers["endpoint"] = p
		}},
		{"url-credentials", func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.BaseURL = "https://secret@example.com"
			f.Providers["endpoint"] = p
		}},
		{"unknown-provider", func(f *fileConfig) { m := f.Models["model"]; m.Provider = "missing"; f.Models["model"] = m }},
		{"missing-chain", func(f *fileConfig) { f.Aliases["test"] = "missing" }},
		{"empty-chain", func(f *fileConfig) { f.Chains["main"] = chainDefinition{} }},
		{"invalid-step", func(f *fileConfig) {
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model", Pool: "also"}}}
		}},
		{"duplicate-model", func(f *fileConfig) {
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "model"}}}
		}},
		{"1m-floor", func(f *fileConfig) {
			m := f.Models["model"]
			m.ContextWindow = 131072
			f.Models["model"] = m
			f.Aliases = map[string]string{"test[1m]": "main"}
		}},
		{"tool-floor", func(f *fileConfig) {
			m := f.Models["model"]
			m.SupportsTools = false
			f.Models["model"] = m
			f.ModelPools = map[string]modelPoolDefinition{"fast": {Models: []string{"model"}, RequireTools: true}}
		}},
		{"unknown-selection", func(f *fileConfig) {
			f.ModelPools = map[string]modelPoolDefinition{"fast": {Models: []string{"model"}, Selection: "hedge"}}
		}},
		{"no-quota-reader", func(f *fileConfig) { p := f.Providers["endpoint"]; p.Usage.Mode = "api"; f.Providers["endpoint"] = p }},
		{"browser-without-path", func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.Usage.Mode = "browser"
			f.Providers["endpoint"] = p
		}},
		{"passive-with-path", func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.Usage.SnapshotPath = "./snapshot.json"
			f.Providers["endpoint"] = p
		}},
		{"content-override", func(f *fileConfig) {
			m := f.Models["model"]
			m.RequestOverrides = map[string]any{"messages": []any{}}
			f.Models["model"] = m
		}},
		{"inline-credential", func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.Headers = map[string]string{"Authorization": "forbidden"}
			f.Providers["endpoint"] = p
		}},
		{"auth-conflict", func(f *fileConfig) { p := f.Providers["endpoint"]; p.Auth.Name = "TOKEN"; f.Providers["endpoint"] = p }},
		{"missing-profile", func(f *fileConfig) {
			f.AccountPools = map[string]accountPoolDefinition{"pool": {Profiles: []string{"unknown"}}}
		}},
		{"wrapper-unknown-directory", func(f *fileConfig) {
			f.Profiles = map[string]profileDefinition{"work": {Command: []string{"claude-account"}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := testConfig("http://127.0.0.1:9999", "anthropic")
			tc.edit(&f)
			b, _ := json.Marshal(f)
			if _, e := decodeFileConfig(filepath.Join(t.TempDir(), "config.json"), b); e == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
func TestConfiguredStrictJSON(t *testing.T) {
	for _, b := range []string{`{"version":1,"version":1}`, `{"version":1,"unknown":true}`, `{"version":1} {}`, `{"version":1,"providers":{"a":{"auth":{"type":"env","type":"none"}}}}`} {
		if _, e := decodeFileConfig("/tmp/config.json", []byte(b)); e == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
}

func TestConfiguredNoBrowserNoUsageDependencies(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/v1/messages" {
			t.Errorf("unexpected usage/other path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("client credential leaked")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_test","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer upstream.Close()
	cfg := compileFixture(t, testConfig(upstream.URL, "anthropic"))
	s, e := newProxyServer(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.metrics.close() })
	// Unconfigured telemetry deliberately exists. Passive mode must not discover it.
	s.browserUsagePath = filepath.Join(t.TempDir(), "provider-usage-browser.json")
	os.WriteFile(s.browserUsagePath, []byte(`{"version":1,"providers":{"endpoint":{"status":"ok","utilization":1}}}`), 0600)
	if _, found, _ := s.browserUsageForProvider("endpoint"); found {
		t.Fatal("passive read a browser snapshot")
	}
	if _, known := s.providerUsageUtilization("endpoint"); known {
		t.Fatal("unknown usage fabricated")
	}
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"test","messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("Authorization", "Bearer client-canary")
	req.Header.Set("Cookie", "private=canary")
	out := httptest.NewRecorder()
	s.handler().ServeHTTP(out, req)
	if out.Code != 200 || !strings.Contains(out.Body.String(), "ok") || requests.Load() != 1 {
		t.Fatalf("status=%d calls=%d body=%s", out.Code, requests.Load(), out.Body.String())
	}
}
func TestConfiguredProtocolPathsAndToolStreams(t *testing.T) {
	for _, protocol := range []string{"anthropic", "openai-chat-completions"} {
		t.Run(protocol, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				expected := "/v1/messages"
				if protocol != "anthropic" {
					expected = "/v1/chat/completions"
				}
				if r.URL.Path != expected {
					t.Errorf("path %s want %s", r.URL.Path, expected)
				}
				var payload map[string]any
				json.NewDecoder(r.Body).Decode(&payload)
				if payload["model"] != "test-model" {
					t.Error("model not rewritten")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if protocol == "anthropic" {
					io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"test-model\",\"content\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call_1\",\"name\":\"lookup\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"query\\\":\\\"hello\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
				} else {
					for _, v := range []string{`{"id":"c","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"query\":\"hello\"}"}}]},"finish_reason":null}]}`, `{"id":"c","choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":5}}`, `[DONE]`} {
						fmt.Fprintf(w, "data: %s\n\n", v)
					}
				}
			}))
			defer upstream.Close()
			cfg := compileFixture(t, testConfig(upstream.URL+"/v1", protocol))
			s, e := newProxyServer(cfg)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { s.metrics.close() })
			body := `{"model":"test","stream":true,"max_tokens":16,"tools":[{"name":"lookup","input_schema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}}],"messages":[{"role":"user","content":"lookup hello"}]}`
			out := httptest.NewRecorder()
			s.handler().ServeHTTP(out, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body)))
			text := out.Body.String()
			if out.Code != 200 || !strings.Contains(text, "message_stop") || !strings.Contains(text, "lookup") || !strings.Contains(text, "hello") {
				t.Fatalf("status=%d stream=%s", out.Code, text)
			}
		})
	}
}
func TestConfiguredBrowserOptInAndMissingCollector(t *testing.T) {
	f := testConfig("http://127.0.0.1:9999", "anthropic")
	p := f.Providers["endpoint"]
	p.Usage = usageDefinition{Mode: "browser", SnapshotPath: filepath.Join(t.TempDir(), "snapshot.json"), SnapshotKey: "custom"}
	f.Providers["endpoint"] = p
	cfg := compileFixture(t, f)
	s := &proxyServer{cfg: cfg}
	if blocked, _ := s.browserUsageExhausted("endpoint"); blocked {
		t.Fatal("absent collector blocks inference")
	}
	used := 1.0
	raw, _ := json.Marshal(browserUsageSnapshot{Version: 1, Providers: map[string]browserUsageProvider{"custom": {Status: "ok", FetchedAt: time.Now(), Utilization: &used}}})
	os.WriteFile(p.Usage.SnapshotPath, raw, 0600)
	if blocked, _ := s.browserUsageExhausted("endpoint"); !blocked {
		t.Fatal("opted-in quota snapshot not respected")
	}
	cfg.Providers["endpoint"] = providerConfig{Usage: usageDefinition{Mode: "passive"}}
	s.cfg = cfg
	if blocked, _ := s.browserUsageExhausted("endpoint"); blocked {
		t.Fatal("disabled snapshot still read")
	}
}

func TestConfiguredProfileCommandIsolation(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "fake-cli")
	log := filepath.Join(dir, "args.json")
	script := "#!/bin/sh\n[ -z \"${ANTHROPIC_BASE_URL+x}\" ] && [ -z \"${ANTHROPIC_API_KEY+x}\" ] && [ -z \"${CLAUDE_CODE_OAUTH_TOKEN+x}\" ] || exit 9\n[ \"$CLAUDE_CONFIG_DIR\" = \"" + dir + "/profile\" ] || exit 10\nprintf '%s\\n' \"$*\" > \"" + log + "\"\nprintf '%s\\n' '{\"loggedIn\":true,\"accessToken\":\"DO-NOT-PRINT\"}'\n"
	if e := os.WriteFile(exe, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("ANTHROPIC_BASE_URL", "http://127.0.0.1:48104")
	t.Setenv("ANTHROPIC_API_KEY", "secret")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "secret")
	cfg := config{Definition: &fileConfig{Profiles: map[string]profileDefinition{"work": {DisplayName: "Work", Command: []string{exe, "fixed"}, ConfigDir: filepath.Join(dir, "profile")}}}}
	var out bytes.Buffer
	if e := runProfilesCommand(context.Background(), cfg, []string{"status", "work"}, nil, &out, io.Discard); e != nil {
		t.Fatal(e)
	}
	args, _ := os.ReadFile(log)
	if string(args) != "fixed auth status --json\n" {
		t.Fatalf("wrong argv: %s", args)
	}
	if strings.Contains(out.String(), "DO-NOT-PRINT") || !strings.Contains(out.String(), `"loggedIn":true`) {
		t.Fatalf("bad status output %s", out.String())
	}
}
