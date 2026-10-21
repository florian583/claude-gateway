package main

import (
	"encoding/json"
	"math"
	"sync"
)

// Pointer counts distinguish absent usage from an explicitly reported zero.
// Output includes reasoning; cache TTL children are subsets of cache creation.
// Neither subset is added to its parent when calculating totals.
type tokenUsage struct {
	Source          string `json:"source"`
	InputUncached   *int64 `json:"inputUncached,omitempty"`
	InputTotal      *int64 `json:"inputTotal,omitempty"`
	CacheRead       *int64 `json:"cacheRead,omitempty"`
	CacheCreation   *int64 `json:"cacheCreation,omitempty"`
	CacheCreation5m *int64 `json:"cacheCreation5m,omitempty"`
	CacheCreation1h *int64 `json:"cacheCreation1h,omitempty"`
	Output          *int64 `json:"outputInclusive,omitempty"`
	Reasoning       *int64 `json:"reasoningSubset,omitempty"`
	Inconsistent    bool   `json:"inconsistent,omitempty"`
}

type usageTelemetry struct {
	mu    sync.Mutex
	value *tokenUsage
}

func countPointer(value any) *int64 {
	var number float64
	switch v := value.(type) {
	case float64:
		number = v
	case int:
		number = float64(v)
	case int64:
		if v < 0 {
			return nil
		}
		copy := v
		return &copy
	case json.Number:
		parsed, err := v.Int64()
		if err != nil || parsed < 0 {
			return nil
		}
		return &parsed
	default:
		return nil
	}
	// Values at 2^63 round outside int64 and must never wrap to negative.
	if math.IsNaN(number) || math.IsInf(number, 0) || number < 0 || number >= math.Exp2(63) || math.Trunc(number) != number {
		return nil
	}
	result := int64(number)
	return &result
}

func assignCount(target **int64, object map[string]any, key string) {
	if value, present := object[key]; present {
		*target = countPointer(value)
	}
}

func payloadUsage(payload map[string]any) map[string]any {
	usage, _ := payload["usage"].(map[string]any)
	if message, ok := payload["message"].(map[string]any); ok {
		if nested, ok := message["usage"].(map[string]any); ok {
			usage = nested
		}
	}
	return usage
}

func (u *usageTelemetry) observeAnthropic(payload map[string]any) {
	usage := payloadUsage(payload)
	if usage == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.value != nil && u.value.Source == "openai-chat" {
		return // translated synthetic usage cannot replace original presence
	}
	if u.value == nil {
		u.value = &tokenUsage{Source: "anthropic"}
	}
	v := u.value
	assignCount(&v.InputUncached, usage, "input_tokens")
	assignCount(&v.Output, usage, "output_tokens")
	assignCount(&v.CacheRead, usage, "cache_read_input_tokens")
	assignCount(&v.CacheCreation, usage, "cache_creation_input_tokens")
	assignCount(&v.Reasoning, usage, "reasoning_tokens")
	if nested, ok := usage["cache_creation"].(map[string]any); ok {
		assignCount(&v.CacheCreation5m, nested, "ephemeral_5m_input_tokens")
		assignCount(&v.CacheCreation1h, nested, "ephemeral_1h_input_tokens")
	}
	v.InputTotal = nil
	if v.InputUncached != nil && v.CacheRead != nil && v.CacheCreation != nil {
		v.InputTotal = safeUsageSum(*v.InputUncached, *v.CacheRead, *v.CacheCreation)
	}
	checkUsageSubsets(v)
}

func (u *usageTelemetry) observeOpenAI(payload map[string]any) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.value == nil || u.value.Source != "openai-chat" {
		u.value = &tokenUsage{Source: "openai-chat"}
	}
	usage, _ := payload["usage"].(map[string]any)
	if usage == nil {
		return
	}
	v := u.value
	assignCount(&v.InputTotal, usage, "prompt_tokens")
	assignCount(&v.InputUncached, usage, "prompt_cache_miss_tokens")
	assignCount(&v.Output, usage, "completion_tokens")
	assignCount(&v.CacheRead, usage, "prompt_cache_hit_tokens")
	assignCount(&v.CacheCreation, usage, "cache_creation_input_tokens")
	if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
		assignCount(&v.CacheRead, details, "cached_tokens")
	}
	if details, ok := usage["completion_tokens_details"].(map[string]any); ok {
		assignCount(&v.Reasoning, details, "reasoning_tokens")
	}
	// OpenAI prompt_tokens already includes cached input; never add it twice.
	if v.InputTotal != nil && v.CacheRead != nil {
		if *v.CacheRead > *v.InputTotal {
			v.Inconsistent, v.InputUncached = true, nil
		} else {
			uncached := *v.InputTotal - *v.CacheRead
			if v.InputUncached != nil && *v.InputUncached != uncached {
				v.Inconsistent = true
			}
			v.InputUncached = &uncached
		}
	}
	checkUsageSubsets(v)
}

func safeUsageSum(values ...int64) *int64 {
	var sum int64
	for _, value := range values {
		if value < 0 || sum > math.MaxInt64-value {
			return nil
		}
		sum += value
	}
	return &sum
}

func checkUsageSubsets(v *tokenUsage) {
	if v.Output != nil && v.Reasoning != nil && *v.Reasoning > *v.Output {
		v.Inconsistent = true
	}
	if v.CacheCreation != nil && v.CacheCreation5m != nil && v.CacheCreation1h != nil {
		total := safeUsageSum(*v.CacheCreation5m, *v.CacheCreation1h)
		if total == nil || *total != *v.CacheCreation {
			v.Inconsistent = true
		}
	}
}

func (u *usageTelemetry) snapshot() *tokenUsage {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.value == nil {
		return nil
	}
	copy := *u.value
	// Counts are immutable pointed values; later observations replace pointers.
	return &copy
}

func reportedCount(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func applyTokenUsage(sample *metricsSample, stats *streamStats) {
	if sample == nil || stats == nil {
		return
	}
	sample.Usage = stats.Usage.snapshot()
	if sample.Usage == nil {
		return // old records/manual counters retain legacy unknown coverage
	}
	v := sample.Usage
	sample.InputTokens = reportedCount(v.InputUncached)
	sample.OutputTokens = reportedCount(v.Output)
	sample.CacheReadTokens = reportedCount(v.CacheRead)
	sample.CacheWriteTokens = reportedCount(v.CacheCreation)
}
