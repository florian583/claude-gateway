package main

import (
	"encoding/json"
	"errors"
)

// Values are a closed vocabulary, never user-supplied event names or content.
func primeFrameKind(data string) string {
	var p map[string]any
	if json.Unmarshal([]byte(data), &p) != nil {
		return "malformed"
	}
	p = unwrapOpenAIEnvelope(p)
	switch p["type"] {
	case "message_start", "message_delta", "message_stop", "content_block_start", "content_block_delta", "content_block_stop", "ping", "error":
		return p["type"].(string)
	}
	if p["error"] != nil {
		return "error"
	}
	if _, ok := p["choices"].([]any); ok {
		return "openai_choices"
	}
	return "unknown"
}

func classifyPrelude(data string, sawStopReason *bool) (bool, error) {
	var p map[string]any
	if json.Unmarshal([]byte(data), &p) != nil {
		return false, nil
	}
	p = unwrapOpenAIEnvelope(p)
	if p["type"] == "message_stop" && !*sawStopReason {
		return false, errors.New("initial SSE message_stop without stop_reason")
	}
	if p["type"] == "message_delta" {
		d, ok := p["delta"].(map[string]any)
		if !ok {
			return false, nil
		}
		for k := range d {
			if k != "stop_reason" && k != "stop_sequence" {
				return false, nil
			}
		}
		if reason, ok := d["stop_reason"].(string); ok && reason != "" {
			*sawStopReason = true
		}
		return true, nil // Require message_stop before accepting an empty response.
	}
	return metadataOnlySSE(data), nil
}

// Only skip known metadata. Unknown frames, reasoning, text, tool starts and
// terminal events commit the stream conservatively, preventing output replay.
func metadataOnlySSE(data string) bool {
	var p map[string]any
	if json.Unmarshal([]byte(data), &p) != nil {
		return false
	}
	p = unwrapOpenAIEnvelope(p)
	switch p["type"] {
	case "ping":
		return true
	case "message_start":
		m, ok := p["message"].(map[string]any)
		if !ok {
			return false
		}
		if m["content"] == nil {
			return true
		}
		content, ok := m["content"].([]any)
		return ok && len(content) == 0
	case "content_block_start":
		b, ok := p["content_block"].(map[string]any)
		if !ok {
			return false
		}
		if b["type"] == "text" {
			return b["text"] == ""
		}
		if b["type"] == "thinking" {
			return b["thinking"] == ""
		}
		return false
	case "content_block_delta":
		d, ok := p["delta"].(map[string]any)
		if !ok {
			return false
		}
		if d["type"] == "text_delta" {
			return d["text"] == ""
		}
		if d["type"] == "thinking_delta" {
			return d["thinking"] == ""
		}
		return false
	}
	choices, ok := p["choices"].([]any)
	if !ok || len(choices) == 0 {
		return false
	}
	for _, raw := range choices {
		c, ok := raw.(map[string]any)
		if !ok || c["finish_reason"] != nil {
			return false
		}
		delta, ok := c["delta"].(map[string]any)
		if !ok {
			return false
		}
		for key, value := range delta {
			if key == "role" && value == "assistant" {
				continue
			}
			if (key == "content" || key == "reasoning_content" || key == "reasoning") && (value == nil || value == "") {
				continue
			}
			return false
		}
	}
	return true
}

type emptyPreludeError struct {
	StopReason   string
	OutputTokens int
}

func (e *emptyPreludeError) Error() string { return "stream ended without text or tool call" }

func safePreludeStopReason(value any) string {
	switch value {
	case "end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal", "stop", "length", "tool_calls":
		return value.(string)
	default:
		return "unknown"
	}
}

func retryEmptyRigPrelude(rig bool, err error, retries int) bool {
	var empty *emptyPreludeError
	return rig && retries == 0 && errors.As(err, &empty) && empty.StopReason == "end_turn"
}

func lastUserTurnHasToolResult(payload map[string]any) bool {
	messages, ok := payload["messages"].([]any)
	if !ok || len(messages) == 0 {
		return false
	}
	last, ok := messages[len(messages)-1].(map[string]any)
	if !ok || last["role"] != "user" {
		return false
	}
	blocks, ok := last["content"].([]any)
	if !ok {
		return false
	}
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if ok && block["type"] == "tool_result" {
			return true
		}
	}
	return false
}

// Strict routes buffer reasoning and metadata before committing any output.
// The caller bounds both the buffered bytes and the absolute wait.
func classifyUsefulPrelude(data string) (bool, error) {
	if data == "[DONE]" {
		return false, &emptyPreludeError{StopReason: "unknown"}
	}
	var p map[string]any
	if json.Unmarshal([]byte(data), &p) != nil {
		return false, errors.New("malformed stream prelude")
	}
	p = unwrapOpenAIEnvelope(p)
	switch p["type"] {
	case "message_stop":
		return false, &emptyPreludeError{StopReason: "unknown"}
	case "message_delta":
		delta, _ := p["delta"].(map[string]any)
		if delta["stop_reason"] == nil {
			return true, nil
		}
		usage, _ := p["usage"].(map[string]any)
		output, _ := usage["output_tokens"].(float64)
		if output < 0 || output > 1000000000 {
			output = 0
		}
		return false, &emptyPreludeError{StopReason: safePreludeStopReason(delta["stop_reason"]), OutputTokens: int(output)}
	case "ping", "message_start", "content_block_stop":
		return true, nil
	case "content_block_start":
		b, _ := p["content_block"].(map[string]any)
		if b["type"] == "tool_use" {
			return false, nil
		}
		if b["type"] == "thinking" {
			return true, nil
		}
		if b["type"] == "text" {
			text, _ := b["text"].(string)
			return text == "", nil
		}
	case "content_block_delta":
		d, _ := p["delta"].(map[string]any)
		switch d["type"] {
		case "thinking_delta", "signature_delta":
			return true, nil
		case "text_delta":
			text, _ := d["text"].(string)
			return text == "", nil
		case "input_json_delta":
			return false, nil
		}
	}
	if choices, ok := p["choices"].([]any); ok {
		for _, raw := range choices {
			c, _ := raw.(map[string]any)
			d, _ := c["delta"].(map[string]any)
			if text, _ := d["content"].(string); text != "" {
				return false, nil
			}
			if calls, _ := d["tool_calls"].([]any); len(calls) > 0 {
				return false, nil
			}
			if c["finish_reason"] != nil {
				return false, &emptyPreludeError{StopReason: "unknown"}
			}
		}
		return true, nil
	}
	return false, errors.New("unsupported stream prelude")
}
