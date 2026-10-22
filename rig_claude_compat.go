package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"unicode/utf16"
)

const rigClaudeVersion = "2.1.307"
const rigClaudeSDKIdentity = "You are a Claude agent, built on Anthropic's Claude Agent SDK."

func rigFirstUserText(payload map[string]any) string {
	messages, _ := payload["messages"].([]any)
	for _, raw := range messages {
		message, _ := raw.(map[string]any)
		if message["role"] != "user" {
			continue
		}
		if text, ok := message["content"].(string); ok {
			return text
		}
		blocks, _ := message["content"].([]any)
		for _, rawBlock := range blocks {
			block, _ := rawBlock.(map[string]any)
			if block["type"] == "text" {
				text, _ := block["text"].(string)
				return text
			}
		}
	}
	return ""
}

func rigNativeClaudeEnvelope(payload map[string]any) bool {
	system, _ := payload["system"].([]any)
	if len(system) < 2 {
		return false
	}
	first, _ := system[0].(map[string]any)
	second, _ := system[1].(map[string]any)
	text, _ := first["text"].(string)
	identity, _ := second["text"].(string)
	return strings.HasPrefix(text, "x-anthropic-billing-header: ") && (identity == rigClaudeSDKIdentity || identity == "You are Claude Code, Anthropic's official CLI for Claude." || identity == "You are Claude Code, Anthropic's official CLI for Claude, running within the Claude Agent SDK.")
}

// The wire profile follows the locally measured Claude Agent SDK entrypoint.
// Caller instructions retain their text and cache breakpoints when relocated.
func adaptRigClaudeRequest(payload map[string]any, headers http.Header) (bool, error) {
	if rigNativeClaudeEnvelope(payload) {
		return false, nil
	}
	var original []any
	switch system := payload["system"].(type) {
	case nil:
	case string:
		original = []any{map[string]any{"type": "text", "text": system}}
	case []any:
		original = system
	default:
		return false, fmt.Errorf("rig subscription request requires text or block system instructions")
	}
	text := rigFirstUserText(payload)
	units := utf16.Encode([]rune(text))
	sample := []uint16{'0', '0', '0'}
	for i, index := range []int{4, 7, 20} {
		if index < len(units) {
			sample[i] = units[index]
		}
	}
	fingerprint := sha256.Sum256([]byte("59cf53e54c78" + string(utf16.Decode(sample)) + rigClaudeVersion))
	billing := fmt.Sprintf("x-anthropic-billing-header: cc_version=%s.%s; cc_entrypoint=sdk-cli;", rigClaudeVersion, hex.EncodeToString(fingerprint[:])[:3])
	system := []any{map[string]any{"type": "text", "text": billing}, map[string]any{"type": "text", "text": rigClaudeSDKIdentity}}
	for _, raw := range original {
		block, ok := raw.(map[string]any)
		if !ok || block["type"] != "text" {
			return false, fmt.Errorf("system instructions must contain text blocks")
		}
		if _, ok := block["text"].(string); !ok {
			return false, fmt.Errorf("system text must be a string")
		}
	}
	payload["system"] = system
	if err := relocateRigSystem(payload, original); err != nil {
		return false, err
	}
	if err := prepareRigCache(payload); err != nil {
		return false, err
	}
	choice, _ := payload["tool_choice"].(map[string]any)
	if choice["type"] == "tool" || choice["type"] == "any" {
		delete(payload, "thinking")
		if output, ok := payload["output_config"].(map[string]any); ok {
			delete(output, "effort")
			if len(output) == 0 {
				delete(payload, "output_config")
			}
		}
	}
	delete(payload, "temperature")
	delete(payload, "top_p")
	if thinking, ok := payload["thinking"].(map[string]any); ok && thinking["type"] != "disabled" {
		delete(payload, "top_k")
		thinking["display"] = "omitted"
	}
	metadata, _ := payload["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
	}
	seed := payloadSessionID(payload)
	if seed == "" {
		seed, _ = metadata["user_id"].(string)
	}
	if seed == "" {
		encoded, _ := json.Marshal(map[string]any{"system": original, "firstUser": text})
		seed = string(encoded)
	}
	hostname, _ := os.Hostname()
	device := sha256.Sum256([]byte("claude-gateway-rig-device:" + hostname))
	sessionBytes := sha256.Sum256([]byte("claude-gateway-rig-session:" + seed))
	sessionBytes[6] = (sessionBytes[6] & 0x0f) | 0x50
	sessionBytes[8] = (sessionBytes[8] & 0x3f) | 0x80
	encodedSession := hex.EncodeToString(sessionBytes[:16])
	session := encodedSession[:8] + "-" + encodedSession[8:12] + "-" + encodedSession[12:16] + "-" + encodedSession[16:20] + "-" + encodedSession[20:]
	user, _ := json.Marshal(map[string]string{"device_id": hex.EncodeToString(device[:]), "account_uuid": "", "session_id": session})
	metadata["user_id"] = string(user)
	payload["metadata"] = metadata
	headers.Set("User-Agent", "claude-cli/"+rigClaudeVersion+" (external, sdk-cli)")
	headers.Set("X-App", "cli")
	headers.Set("X-Claude-Code-Session-Id", session)
	headers.Set("anthropic-version", "2023-06-01")
	headers.Set("X-Stainless-Arch", "arm64")
	headers.Set("X-Stainless-Lang", "js")
	headers.Set("X-Stainless-OS", "MacOS")
	headers.Set("X-Stainless-Package-Version", "0.128.0")
	headers.Set("X-Stainless-Retry-Count", "0")
	headers.Set("X-Stainless-Runtime", "node")
	headers.Set("X-Stainless-Runtime-Version", "v26.3.0")
	for _, beta := range []string{"claude-code-20250219", "interleaved-thinking-2025-05-14", "thinking-token-count-2026-05-13", "context-management-2025-06-27", "per-turn-control-2026-07-01", "effort-2025-11-24", "extended-cache-ttl-2025-04-11"} {
		ensureCommaSeparatedHeaderValue(headers, "anthropic-beta", beta)
	}
	model, _ := payload["model"].(string)
	if rigModernSystemModel(model) {
		ensureCommaSeparatedHeaderValue(headers, "anthropic-beta", "mid-conversation-system-2026-04-07")
	}
	return true, nil
}

func rigModernSystemModel(model string) bool {
	switch model {
	case "claude-opus-5", "claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5":
		return true
	}
	return false
}

// OAuth requires caller instructions outside the native identity slot. Modern
// models retain system authority; legacy models use the CLI reminder convention.
func relocateRigSystem(payload map[string]any, blocks []any) error {
	if len(blocks) == 0 {
		return nil
	}
	messages, _ := payload["messages"].([]any)
	first := -1
	for i, raw := range messages {
		message, _ := raw.(map[string]any)
		if message["role"] == "user" {
			first = i
			break
		}
	}
	if first < 0 {
		return fmt.Errorf("system instructions require a user message")
	}
	model, _ := payload["model"].(string)
	if rigModernSystemModel(model) {
		at := first + 1
		for at < len(messages) {
			message, _ := messages[at].(map[string]any)
			content, _ := message["content"].([]any)
			directive := message["role"] == "system" && message["output_config"] != nil && len(content) == 0
			if message["role"] != "user" && !directive {
				break
			}
			at++
		}
		result := append([]any{}, messages[:at]...)
		for _, block := range blocks {
			result = append(result, map[string]any{"role": "system", "content": []any{block}})
		}
		payload["messages"] = append(result, messages[at:]...)
		return nil
	}
	message := messages[first].(map[string]any)
	var content []any
	switch value := message["content"].(type) {
	case string:
		content = []any{map[string]any{"type": "text", "text": value}}
	case []any:
		content = value
	default:
		return fmt.Errorf("user content must be text or blocks")
	}
	at := 0
	for at < len(content) {
		block, _ := content[at].(map[string]any)
		if block["type"] != "tool_result" {
			break
		}
		at++
	}
	result := append([]any{}, content[:at]...)
	for _, raw := range blocks {
		original := raw.(map[string]any)
		block := map[string]any{}
		for key, value := range original {
			block[key] = value
		}
		block["text"] = "<system-reminder>\n" + original["text"].(string) + "\n</system-reminder>"
		result = append(result, block)
	}
	message["content"] = append(result, content[at:]...)
	return nil
}

// Visit protocol content only: tool schemas and tool-result data may contain
// unrelated fields named cache_control and must never be rewritten.
func rigCacheBlocks(payload map[string]any) []map[string]any {
	var blocks []map[string]any
	var visit func([]any)
	visit = func(items []any) {
		for _, raw := range items {
			block, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			blocks = append(blocks, block)
			if content, ok := block["content"].([]any); ok {
				visit(content)
			}
		}
	}
	for _, key := range []string{"tools", "system", "messages"} {
		items, _ := payload[key].([]any)
		visit(items)
	}
	return blocks
}

func prepareRigCache(payload map[string]any) error {
	blocks := rigCacheBlocks(payload)
	count := 0
	for _, block := range blocks {
		if block["cache_control"] != nil {
			count++
		}
	}
	if count > 4 {
		return fmt.Errorf("at most four cache breakpoints are supported")
	}
	// Existing caller breakpoints own placement. Without any, cache the full
	// reusable prefix through the latest user or relocated system instruction.
	if count == 0 {
		for i := len(blocks) - 1; i >= 0; i-- {
			if blocks[i]["type"] == "text" {
				blocks[i]["cache_control"] = map[string]any{"type": "ephemeral", "ttl": "1h"}
				break
			}
		}
	}
	shortSeen := false
	for _, block := range blocks {
		cc, ok := block["cache_control"].(map[string]any)
		if !ok {
			continue
		}
		if cc["type"] != "ephemeral" {
			return fmt.Errorf("unsupported cache control type")
		}
		ttl := cc["ttl"]
		if ttl == nil {
			if shortSeen {
				cc["ttl"] = "5m"
			} else {
				cc["ttl"] = "1h"
			}
			ttl = cc["ttl"]
		}
		switch ttl {
		case "5m":
			shortSeen = true
		case "1h":
			if shortSeen {
				return fmt.Errorf("one-hour cache breakpoints must precede five-minute breakpoints")
			}
		default:
			return fmt.Errorf("unsupported cache TTL")
		}
	}
	return nil
}
