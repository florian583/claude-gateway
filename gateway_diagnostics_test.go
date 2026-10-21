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
	// Rate-limit family headers are captured (retry-after distinguishes "wait
	// seconds" RPM throttling from window exhaustion); unrelated headers are not.
	e4 := attemptExecution{}
	e4.observeGatewayHeaders(http.Header{
		"Retry-After":                                      []string{"37"},
		"Anthropic-Ratelimit-Requests-Remaining":           []string{"0"},
		"Anthropic-Ratelimit-Unified-Reset":                []string{"2026-09-20T23:59:00Z"},
		"Anthropic-Ratelimit-Unified-Representative-Claim": []string{"input_tokens"},
		"X-Ratelimit-Reset":                                []string{"1758390000"},
		"Server":                                           []string{"cloudflare"},
	})
	for _, key := range []string{
		"header.retry-after",
		"header.anthropic-ratelimit-requests-remaining",
		"header.anthropic-ratelimit-unified-reset",
		"header.anthropic-ratelimit-unified-representative-claim",
		"header.x-ratelimit-reset",
	} {
		if e4.ResponseMetadata[key] == "" {
			t.Fatalf("rate-limit header %s not captured: %v", key, e4.ResponseMetadata)
		}
	}
	if _, ok := e4.ResponseMetadata["header.server"]; ok {
		t.Fatal("unrelated header captured")
	}
	e.observeGatewayError([]byte("invalid json"))
	// String-valued error fields (non-object shape) must not panic.
	e.observeGatewayError([]byte(`{"error":"upstream blew up"}`))

	// Error messages fingerprint onto a fixed keyword vocabulary — enough to
	// identify the rejected request shape without persisting provider text.
	e2 := attemptExecution{}
	e2.observeGatewayError([]byte(`{"error":{"type":"invalid_request_error","message":"max_tokens: field is required and must be positive"}}`))
	if e2.ResponseMetadata["body.error.message_keys"] != "max_tokens required must be" {
		t.Fatalf("unexpected fingerprint %q", e2.ResponseMetadata["body.error.message_keys"])
	}
	if e2.ResponseMetadata["body.error.message_len"] != "50" {
		t.Fatalf("unexpected message_len %q", e2.ResponseMetadata["body.error.message_len"])
	}
	// A message matching no keyword yields zero metadata (unchanged contract).
	e3 := attemptExecution{}
	e3.observeGatewayError([]byte(`{"error":{"message":"xyzzy"}}`))
	if len(e3.ResponseMetadata) != 0 {
		t.Fatal("keyword-free message must not produce metadata")
	}
	stats := &streamStats{}
	recordReportedBackend(stats, map[string]any{"error": map[string]any{"metadata": map[string]any{"provider_name": "Fireworks"}}})
	if stats.ReportedBackend.Load() != "Fireworks" {
		t.Fatal("SSE backend metadata lost")
	}
}
