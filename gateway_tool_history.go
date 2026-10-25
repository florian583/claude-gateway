package main

import (
	"encoding/json"
	"errors"
)

// Claude's dynamic tool registry is meaningful history, but third-party Messages
// schema accepts only ordinary content blocks. Keep its definitions callable
// and preserve the original history as text on this provider's private copy.
func adaptGatewayToolHistory(payload map[string]any) error {
	tools, _ := payload["tools"].([]any)
	positions := map[string]int{}
	current := map[string]bool{}
	for i, raw := range tools {
		if t, ok := raw.(map[string]any); ok {
			if n, ok := t["name"].(string); ok {
				positions[n] = i
				current[n] = true
				delete(t, "defer_loading")
				if t["type"] == "custom" {
					delete(t, "type")
				}
			}
		}
	}
	var blocks func([]any) ([]any, error)
	blocks = func(content []any) ([]any, error) {
		out := make([]any, 0, len(content))
		for _, raw := range content {
			block, ok := raw.(map[string]any)
			if !ok {
				out = append(out, raw)
				continue
			}
			kind, _ := block["type"].(string)
			switch kind {
			case "tool_addition", "tool_definition":
				wrapper := block
				if kind == "tool_addition" {
					wrapper, _ = block["tool"].(map[string]any)
				}
				definition, _ := wrapper["definition"].(map[string]any)
				name, _ := definition["name"].(string)
				if name == "" || definition["input_schema"] == nil {
					return nil, errors.New("dynamic tool history has no usable definition")
				}
				copy := map[string]any{}
				for _, key := range []string{"name", "description", "input_schema", "cache_control"} {
					if value, ok := definition[key]; ok {
						copy[key] = value
					}
				}
				if i, exists := positions[name]; exists {
					if !current[name] {
						tools[i] = copy
					}
				} else {
					positions[name] = len(tools)
					tools = append(tools, copy)
				}
				fallthrough
			case "server_tool_use", "advisor_tool_result":
				encoded, err := json.Marshal(block)
				if err != nil {
					return nil, err
				}
				text := map[string]any{"type": "text", "text": "Historical " + kind + ": " + string(encoded)}
				if cache, ok := block["cache_control"]; ok {
					text["cache_control"] = cache
				}
				out = append(out, text)
			default:
				if nested, ok := block["content"].([]any); ok {
					adapted, err := blocks(nested)
					if err != nil {
						return nil, err
					}
					block["content"] = adapted
				}
				out = append(out, block)
			}
		}
		return out, nil
	}
	messages, _ := payload["messages"].([]any)
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if content, ok := message["content"].([]any); ok {
			adapted, err := blocks(content)
			if err != nil {
				return err
			}
			message["content"] = adapted
		}
		if message["role"] == "system" {
			message["role"] = "user"
		}
	}
	if len(tools) > 0 {
		payload["tools"] = tools
	}
	return nil
}

func gatewayToolHistoryProvider(variant string) bool {
	switch variant {
	case "vercel-gateway", "bigmodel", "ollama-cloud", "kimi-subscription", "opencode-go", "clinepass":
		return true
	default:
		return false
	}
}
