package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestGatewayDiagnosticAllowlistAndSnapshot(t *testing.T) {
	e := attemptExecution{}
	h := http.Header{}
	h.Set("X-Backend", "Baseten")
	h.Set("X-Vercel-Id", "sin1::abc-123")
	h.Set("Authorization", "Bearer private")
	h.Set("Set-Cookie", "private")
	h.Set("Server", "cloudflare")
	e.observeGatewayHeaders(h)
	e.observeGatewayError([]byte(`{"error":{"type":"overloaded_error","message":"PRIVATE PROMPT","metadata":{"provider_name":"Fireworks","raw":"PRIVATE BODY"}}}`))
	s := metricsSample{}
	applyExecutionTelemetry(&s, requestTrace{Execution: &e})
	e.ResponseMetadata["header.x-backend"] = "changed"
	if s.Execution.ResponseMetadata["header.x-backend"] != "Baseten" {
		t.Fatal("metadata not snapshotted")
	}
	if s.Execution.ResponseMetadata["body.error.metadata.provider_name"] != "Fireworks" {
		t.Fatal("reported backend lost")
	}
	b, _ := json.Marshal(s)
	for _, forbidden := range []string{"PRIVATE", "Authorization", "Set-Cookie", "cloudflare"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("leaked unapproved field %s", forbidden)
		}
	}
}

func TestGatewayDiagnosticsRejectUnsafeOrAbsentMetadata(t *testing.T) {
	for _, v := range []string{"hello\nworld", "https://host?token=private", "sk-secret", "Bearer token", strings.Repeat("a", 161)} {
		if diagnosticIdentifier(v) != "" {
			t.Fatalf("accepted unsafe identifier %q", v)
		}
	}
	e := attemptExecution{}
	e.observeGatewayHeaders(http.Header{"Server": []string{"cloudflare"}})
	e.observeGatewayError([]byte(`{"error":{"message":"on Baseten perhaps"}}`))
	if len(e.ResponseMetadata) != 0 {
		t.Fatal("backend inferred from arbitrary message/server")
	}
	e.observeGatewayError([]byte("invalid json"))
	stats := &streamStats{}
	recordReportedBackend(stats, map[string]any{"error": map[string]any{"metadata": map[string]any{"provider_name": "Fireworks"}}})
	if stats.ReportedBackend.Load() != "Fireworks" {
		t.Fatal("SSE backend metadata lost")
	}
}
