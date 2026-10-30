package main

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"
)

func hasAdvisorHistory(value any) bool {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			if hasAdvisorHistory(item) {
				return true
			}
		}
	case map[string]any:
		kind, _ := v["type"].(string)
		name, _ := v["name"].(string)
		if strings.HasPrefix(kind, "advisor_") || ((kind == "server_tool_use" || kind == "tool_use" || kind == "tool") && name == "advisor") {
			return true
		}
		for _, item := range v {
			if hasAdvisorHistory(item) {
				return true
			}
		}
	}
	return false
}

// The provider adapter preserves prior advisor calls as text. A request that
// explicitly forces the native advisor still requires a Claude route.
func omitUnusedAdvisor(body []byte, payload map[string]any) ([]byte, map[string]any) {
	if hasAdvisorHistory(payload["tool_choice"]) {
		return body, payload
	}
	tools, ok := payload["tools"].([]any)
	if !ok {
		return body, payload
	}
	kept := make([]any, 0, len(tools))
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		kind, _ := tool["type"].(string)
		if !strings.HasPrefix(kind, "advisor_") {
			kept = append(kept, raw)
		}
	}
	if len(kept) == len(tools) {
		return body, payload
	}
	copyPayload := make(map[string]any, len(payload))
	for key, value := range payload {
		copyPayload[key] = value
	}
	if len(kept) == 0 {
		delete(copyPayload, "tools")
	} else {
		copyPayload["tools"] = kept
	}
	encoded, err := json.Marshal(copyPayload)
	if err != nil {
		return body, payload
	}
	return encoded, copyPayload
}

func omitEmptySystemMessage(body []byte, payload map[string]any) ([]byte, map[string]any) {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return body, payload
	}
	kept := make([]any, 0, len(messages))
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		empty := false
		if ok && message["role"] == "system" {
			switch content := message["content"].(type) {
			case nil:
				empty = true
			case string:
				empty = content == ""
			case []any:
				empty = len(content) == 0
			}
		}
		if !empty {
			kept = append(kept, raw)
		}
	}
	if len(kept) == len(messages) {
		return body, payload
	}
	copyPayload := make(map[string]any, len(payload))
	for key, value := range payload {
		copyPayload[key] = value
	}
	copyPayload["messages"] = kept
	encoded, err := json.Marshal(copyPayload)
	if err != nil {
		return body, payload
	}
	return encoded, copyPayload
}

func (s *proxyServer) claudeAccountPinExpired(candidate modelConfig, until, now time.Time) bool {
	return !now.Before(until) && !s.claudeSubscriptionCandidate(candidate)
}

// Explicit Sonnet subscription requests spend available subscription capacity
// before entering their configured fallback steps. Classification may describe
// the task, but must not promote a fast non-Claude leg ahead of the account pool.
// Quota, authentication and health checks still run normally for every attempt.
func (s *proxyServer) preserveSonnetSubscriptionOrder(selected modelConfig) bool {
	return s.claudeSubscriptionCandidate(selected) &&
		strings.TrimSuffix(selected.Upstream, "[1m]") == "claude-sonnet-5-5"
}

func (s *proxyServer) nativeAdvisorCandidate(m modelConfig) bool {
	p := s.cfg.Providers[firstNonEmpty(m.Provider, s.cfg.DefaultProvider)]
	u, _ := url.Parse(p.BaseURL)
	directAnthropic := u != nil && u.Scheme == "https" && u.Hostname() == "api.anthropic.com"
	return strings.HasPrefix(m.Upstream, "claude-") && (p.Variant == "claude-subscription" || directAnthropic)
}

// An explicit advisor tool choice requires a native Anthropic route.
func (s *proxyServer) advisorCompatibleCandidates(candidates []modelConfig, payload map[string]any) []modelConfig {
	tools, _ := payload["tools"].([]any)
	needsAdvisor := hasAdvisorHistory(payload["tool_choice"])
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		kind, _ := tool["type"].(string)
		if strings.HasPrefix(kind, "advisor_") {
			needsAdvisor = true
			break
		}
	}
	if !needsAdvisor {
		return candidates
	}
	result := make([]modelConfig, 0, len(candidates))
	for _, m := range candidates {
		if s.nativeAdvisorCandidate(m) {
			result = append(result, m)
		}
	}
	return result
}
