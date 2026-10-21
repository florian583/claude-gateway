package devin

import (
	"encoding/json"
	"errors"
	"regexp"
)

type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}
type Message struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall      `json:"tool_calls,omitempty"`
	text       string
}
type Tool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
		Strict      bool            `json:"strict,omitempty"`
	} `json:"function"`
}
type Request struct {
	Model         string    `json:"model"`
	Messages      []Message `json:"messages"`
	Tools         []Tool    `json:"tools,omitempty"`
	Stream        bool      `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options,omitempty"`
	MaxTokens           int             `json:"max_tokens,omitempty"`
	MaxCompletionTokens int             `json:"max_completion_tokens,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	TopP                *float64        `json:"top_p,omitempty"`
	ToolChoice          json.RawMessage `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool           `json:"parallel_tool_calls,omitempty"`
	Stop                json.RawMessage `json:"stop,omitempty"`
	N                   int             `json:"n,omitempty"`
}

var namePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func (r *Request) validate() error {
	fail := func(s string) error { return errors.New(s) }
	if r.Model != "swe-2-high" {
		return fail("only swe-2-high is supported")
	}
	if len(r.Messages) == 0 || len(r.Messages) > 4096 {
		return fail("messages must contain 1 to 4096 entries")
	}
	if r.MaxTokens != 0 && r.MaxCompletionTokens != 0 {
		return fail("specify only one output token limit")
	}
	if r.MaxCompletionTokens != 0 {
		r.MaxTokens = r.MaxCompletionTokens
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = 4096
	}
	if r.MaxTokens < 1 || r.MaxTokens > 32768 {
		return fail("output limit must be between 1 and 32768")
	}
	if r.N != 0 && r.N != 1 {
		return fail("only n=1 is supported")
	}
	if r.Temperature != nil && (*r.Temperature <= 0 || *r.Temperature > 2) {
		return fail("temperature must be greater than zero and at most two")
	}
	if r.TopP != nil && (*r.TopP <= 0 || *r.TopP > 1) {
		return fail("top_p must be greater than zero and at most one")
	}
	if len(r.Stop) > 0 && string(r.Stop) != "null" && string(r.Stop) != "[]" {
		return fail("stop sequences are not supported")
	}
	if len(r.ToolChoice) > 0 && string(r.ToolChoice) != `"auto"` && string(r.ToolChoice) != "null" {
		return fail("only automatic tool selection is supported")
	}
	if r.ParallelToolCalls != nil {
		return fail("parallel_tool_calls control is not supported")
	}
	if len(r.Tools) > 256 {
		return fail("too many tools")
	}
	names := map[string]bool{}
	for _, t := range r.Tools {
		var schema map[string]any
		if t.Type != "function" || !namePattern.MatchString(t.Function.Name) || names[t.Function.Name] || t.Function.Strict {
			return fail("tools require unique function names; strict mode is unsupported")
		}
		if json.Unmarshal(t.Function.Parameters, &schema) != nil || schema["type"] != "object" {
			return fail("tool parameters must be an object JSON schema")
		}
		names[t.Function.Name] = true
	}
	pending, seen := map[string]bool{}, map[string]bool{}
	for i := range r.Messages {
		m := &r.Messages[i]
		switch m.Role {
		case "system", "developer", "user", "assistant", "tool":
		default:
			return fail("unsupported message role")
		}
		if len(m.Content) > 0 && string(m.Content) != "null" {
			if json.Unmarshal(m.Content, &m.text) != nil {
				var parts []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				}
				if json.Unmarshal(m.Content, &parts) != nil {
					return fail("content must be text")
				}
				for _, p := range parts {
					if p.Type != "text" {
						return fail("images and other non-text content are unsupported")
					}
					m.text += p.Text
				}
			}
		}
		if len(m.ToolCalls) > 0 && m.Role != "assistant" {
			return fail("tool_calls require an assistant message")
		}
		for _, tc := range m.ToolCalls {
			var args map[string]any
			if tc.Type != "function" || tc.ID == "" || len(tc.ID) > 256 || seen[tc.ID] || !namePattern.MatchString(tc.Function.Name) || json.Unmarshal([]byte(tc.Function.Arguments), &args) != nil || args == nil {
				return fail("invalid tool-call history")
			}
			pending[tc.ID], seen[tc.ID] = true, true
		}
		if m.Role == "tool" {
			if !pending[m.ToolCallID] {
				return fail("tool result has no matching pending tool call")
			}
			delete(pending, m.ToolCallID)
		} else if m.ToolCallID != "" {
			return fail("tool_call_id requires a tool message")
		}
	}
	if len(pending) > 0 {
		return fail("tool-call history requires matching results")
	}
	return nil
}
