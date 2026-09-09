package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeferredToolsRejectionContinuesFallback(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error","message":"Deferred custom tools are only supported on Anthropic-compatible provider endpoints that implement deferral"}}`))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message"}`))
	}))
	defer fallback.Close()
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"primary": {BaseURL: primary.URL}, "fallback": {BaseURL: fallback.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"model":"glm","messages":[]}`)
	candidate := modelConfig{Requested: "glm", Provider: "primary", Upstream: "glm", Fallbacks: []modelConfig{{Provider: "fallback", Upstream: "glm"}}}
	response, err := s.doWithFallbacks(context.Background(), httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(body)), body, map[string]any{"model": "glm"}, candidate)
	if err != nil {
		t.Fatal(err)
	}
	defer response.resp.Body.Close()
	if response.providerName != "fallback" {
		t.Fatal("deferral rejection vetoed fallback")
	}
	if blocked, _ := s.providerBlocked("primary"); blocked {
		t.Fatal("request capability poisoned provider health")
	}
}

func TestDeferredToolsRejectionIsRouteSpecific(t *testing.T) {
	body := []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Deferred custom tools are only supported on Anthropic models and on Anthropic-compatible provider endpoints that implement deferral. Other endpoints cannot call tools omitted from tools"}}`)
	if !providerSpecificRequestIncompatibility(400, body) {
		t.Fatal("provider deferral limitation must allow next route")
	}
	if providerSpecificRequestIncompatibility(401, body) {
		t.Fatal("auth failure must retain its own classification")
	}
	if providerSpecificRequestIncompatibility(400, []byte(`{"error":{"message":"Invalid tools: missing input_schema"}}`)) {
		t.Fatal("malformed client tools must not be classified as a route limitation")
	}
}
