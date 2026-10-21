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
