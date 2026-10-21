package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func logFailureDiagnostics(sample metricsSample) {
	if sample.Success || (sample.RecordKind != "attempt" && sample.RecordKind != "completion") || strings.HasPrefix(sample.FailureReason, "client_cancel") {
		return
	}
	// Avoid raw stream errors, prompts, tools, full payloads, and auth headers.
	data, _ := json.Marshal(map[string]any{
		"requestId": sample.RequestID, "provider": sample.Provider, "model": sample.Upstream,
		"attempt": sample.Attempt, "status": sample.Status, "failure": sample.FailureReason,
		"reportedBackend": sample.ReportedBackend, "execution": sample.Execution,
		"streamStalled": sample.StreamStalled, "missingStop": sample.MissingStop, "protocolError": sample.ProtocolError,
		"events": sample.StreamEvents, "bytes": sample.StreamBytes, "lastEventAgeMs": sample.LastEventAgeMS,
		"maximumEventGapMs": sample.MaximumEventGapMS, "firstContentMs": sample.FirstContentMS,
	})
	log.Printf("upstream failure diagnostics %s", data)
}

// Explicit response metadata only. Never infer an inference backend from
// server/CDN headers, and never persist arbitrary headers or raw error bodies.
func diagnosticIdentifier(value string) string {
	if len(value) == 0 || len(value) > 160 {
		return ""
	}
	if strings.Contains(strings.ToLower(value), "bearer") || strings.Contains(value, "sk-") {
		return ""
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(" ._-/():", r)) {
			return ""
		}
	}
	return value
}

func (e *attemptExecution) noteDiagnostic(key, value string) {
	if value = diagnosticIdentifier(value); value == "" {
		return
	}
	if e.ResponseMetadata == nil {
		e.ResponseMetadata = map[string]string{}
	}
	e.ResponseMetadata[key] = value
}

func (e *attemptExecution) observeGatewayHeaders(header http.Header) {
	for _, key := range []string{"request-id", "x-request-id", "x-vercel-id", "cf-ray", "x-provider", "x-backend", "x-upstream-provider", "x-vercel-ai-gateway-provider", "retry-after"} {
		e.noteDiagnostic("header."+key, header.Get(key))
	}
	// Rate-limit header families carry only counts, percentages, and reset
	// timestamps — fixed-vocabulary numeric metadata, still sanitized by
	// diagnosticIdentifier. Captured on every response, not just 429s, so
	// limiter state is visible before exhaustion.
	for name := range header {
		lower := strings.ToLower(name)
		for _, prefix := range []string{"anthropic-ratelimit-", "x-ratelimit-"} {
			if strings.HasPrefix(lower, prefix) {
				e.noteDiagnostic("header."+lower, header.Get(name))
				break
			}
		}
	}
}

// errorMessageVocabulary is the fixed set of request-shape keywords an error
// message is fingerprinted against. The output vocabulary never leaves this
// list, so provider text — which may echo request values — is never persisted.
var errorMessageVocabulary = []string{
	"max_tokens", "max_completion_tokens", "messages", "model", "tools",
	"tool_choice", "thinking", "budget", "system", "metadata", "user_id",
	"stop_sequences", "temperature", "top_p", "top_k", "context", "quota",
	"required", "invalid", "unsupported", "empty", "too long", "too large",
	"not found", "exceed", "must be",
}

func errorMessageFingerprint(message string) string {
	lower := strings.ToLower(message)
	out := ""
	for _, kw := range errorMessageVocabulary {
		if !strings.Contains(lower, kw) {
			continue
		}
		next := kw
		if out != "" {
			next = out + " " + kw
		}
		if len(next) > 150 {
			break
		}
		out = next
	}
	return out
}

func (e *attemptExecution) observeGatewayError(body []byte) {
	var payload map[string]any
	if len(body) > 1<<20 || json.Unmarshal(body, &payload) != nil {
		return
	}
	for _, path := range [][]string{{"provider"}, {"provider_name"}, {"error", "type"}, {"error", "code"}, {"error", "metadata", "provider_name"}, {"error", "metadata", "backend"}} {
		var current any = payload
		for _, key := range path {
			m, _ := current.(map[string]any)
			current = m[key]
		}
		value, _ := current.(string)
		e.noteDiagnostic("body."+strings.Join(path, "."), value)
	}
	if errObj, ok := payload["error"].(map[string]any); ok {
		if msg, _ := errObj["message"].(string); msg != "" {
			if fp := errorMessageFingerprint(msg); fp != "" {
				e.noteDiagnostic("body.error.message_keys", fp)
				e.noteDiagnostic("body.error.message_len", strconv.Itoa(len(msg)))
			}
		}
	}
}
