package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestSSEMeasuredZeroInputSurvivesEstimateAndCache(t *testing.T) {
	for _, initial := range []string{`"input_tokens":0,`, ""} {
		stats := &streamStats{}
		var out bytes.Buffer
		source := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{" + initial + "\"output_tokens\":0,\"cache_read_input_tokens\":10000}}}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":0,\"output_tokens\":5,\"cache_read_input_tokens\":11000}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
		if err := filterSSE(strings.NewReader(source), &out, nil, 999, "m", stats, nil); err != nil {
			t.Fatal(err)
		}
		if stats.InputTokens.Load() != 0 || stats.InputEstimated.Load() || stats.CacheReadTokens.Load() != 11000 || stats.OutputTokens.Load() != 5 {
			t.Fatalf("final measured usage incorrect: input=%d estimated=%v cache=%d output=%d", stats.InputTokens.Load(), stats.InputEstimated.Load(), stats.CacheReadTokens.Load(), stats.OutputTokens.Load())
		}
		if initial != "" && strings.Contains(out.String(), `"input_tokens":999`) {
			t.Fatal("explicit zero overwritten in client stream")
		}
	}
}

func TestSSETrailingDoneRequiresCompleteAnthropicMessage(t *testing.T) {
	start := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":0}}}\n\n"
	delta := "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n"
	stop := "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	for _, tc := range []struct {
		name, prefix string
		complete     bool
	}{
		{"complete", start + delta + stop, true},
		{"complete_named_marker", start + delta + stop + "event: data\n", true},
		{"premature_named_marker", start + delta + "event: data\n", false},
		{"missing_stop", start + delta, false},
		{"missing_reason", start + stop, false},
		{"start_only", start, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			stats := &streamStats{}
			err := filterSSE(strings.NewReader(tc.prefix+"data: [DONE]\n\n"), &out, nil, 0, "m", stats, nil)
			if tc.complete {
				if err != nil || stats.ProtocolError.Load() || strings.Contains(out.String(), "[DONE]") || strings.Contains(out.String(), "event: error") {
					t.Fatalf("valid terminal rejected: %v %s", err, out.String())
				}
			} else if err == nil || !stats.ProtocolError.Load() {
				t.Fatal("premature DONE accepted")
			}
		})
	}
}
