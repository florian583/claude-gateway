package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestGatewayToolHistoryPreservesDefinitionsCacheAndNativeBody(t *testing.T) {
	raw := []byte(`{"model":"test","max_tokens":100,"tools":[{"name":"active","defer_loading":true,"input_schema":{"type":"object","properties":{"current":{"type":"boolean"}}}}],"messages":[{"role":"user","content":[{"type":"text","text":"question","cache_control":{"type":"ephemeral","ttl":"1h"}}]},{"role":"system","content":[{"type":"tool_addition","tool":{"type":"tool_definition","definition":{"name":"discovered","description":"available tool","input_schema":{"type":"object"}}},"cache_control":{"type":"ephemeral"}},{"type":"tool_addition","tool":{"type":"tool_definition","definition":{"name":"active","input_schema":{"type":"string"}}}}]},{"role":"assistant","content":[{"type":"tool_use","id":"tool-a","name":"discovered","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-a","content":"done"}]}]}`)
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{"gateway": {Variant: "vercel-gateway"}, "native": {}}}}
	out, err := s.buildAttemptBody(raw, payload, modelConfig{Provider: "gateway", Upstream: "test"}, "gateway", "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err = json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	tools := got["tools"].([]any)
	if len(tools) != 2 {
		t.Fatal("dynamic tools not retained")
	}
	active := tools[0].(map[string]any)
	if active["defer_loading"] != nil {
		t.Fatal("deferred loading retained")
	}
	if active["input_schema"].(map[string]any)["type"] != "object" {
		t.Fatal("history overwrote current definition")
	}
	messages := got["messages"].([]any)
	if messages[1].(map[string]any)["role"] != "user" {
		t.Fatal("system update was not adapted")
	}
	b := messages[1].(map[string]any)["content"].([]any)[0].(map[string]any)
	if b["type"] != "text" || b["cache_control"] == nil {
		t.Fatal("dynamic history or cache marker lost")
	}
	if !bytes.Contains(out, []byte("available tool")) || !bytes.Contains(out, []byte("tool-a")) || !bytes.Contains(out, []byte(`"ttl":"1h"`)) {
		t.Fatal("history or cache content lost")
	}
	original, _ := json.Marshal(payload)
	var baseline map[string]any
	json.Unmarshal(raw, &baseline)
	want, _ := json.Marshal(baseline)
	if !bytes.Equal(original, want) {
		t.Fatal("attempt mutation leaked into parsed source")
	}
	native, err := s.buildAttemptBody(raw, payload, modelConfig{Provider: "native"}, "native", "/v1/messages")
	if err != nil || !bytes.Equal(native, raw) {
		t.Fatal("native request modified")
	}
}
func TestGatewayToolHistoryMalformedDefinitionFailsClosed(t *testing.T) {
	p := map[string]any{"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_addition"}}}}}
	if adaptGatewayToolHistory(p) == nil {
		t.Fatal("unusable tool silently removed")
	}
}
func TestPaidFallbackAfterTokenPassQuotaAndBudget(t *testing.T) {
	var primaryCalls, paidCalls atomic.Int64
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		w.WriteHeader(429)
		w.Write([]byte(`{"error":{"type":"rate_limit_error","message":"quota exhausted"}}`))
	}))
	defer primary.Close()
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paidCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":10,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}`))
	}))
	defer paid.Close()
	cfg := config{Metrics: metricsConfig{Path: t.TempDir() + "/metrics.jsonl"}, Definition: &fileConfig{}, Providers: map[string]providerConfig{"pass": {BaseURL: primary.URL, Billing: "subscription"}, "paid": {BaseURL: paid.URL, Billing: "metered", Rolling24hBudgetUSD: 25, Pricing: pricingDefinition{InputPerMillionTokens: 2, OutputPerMillionTokens: 10}}}}
	s, err := newProxyServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	image := true
	m := modelConfig{Requested: "kimi", Provider: "pass", Upstream: "test", ContextWindow: 1048576, SupportsImages: &image, AllowPaidFallback: true, PaidFallbackOn: []string{"quota-exhausted"}, Fallbacks: []modelConfig{{Provider: "paid", Upstream: "test-pro", ContextWindow: 1048576, SupportsImages: &image}}}
	payload := map[string]any{"model": "kimi", "max_tokens": 100, "messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	body, _ := json.Marshal(payload)
	response, err := s.doWithFallbacks(context.Background(), httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(body)), body, payload, m)
	if err != nil {
		t.Fatal(err)
	}
	// The response body remains charged until its complete usage is consumed.
	if response.providerName != "paid" || primaryCalls.Load() != 1 || paidCalls.Load() != 1 {
		t.Fatal("fallback order wrong")
	}
	var got map[string]any
	if err = json.NewDecoder(response.resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if err = response.resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	l := readBudget(t, paidBudgetPath(cfg))
	var cost int64
	for _, e := range l.Entries {
		cost += e.MicroUSD
	}
	if cost != 30 {
		t.Fatalf("cost after decoded response %d", cost)
	}
}
