package main

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// upstreamRateLimitError preserves the upstream contract when no response body
// can be forwarded (a cached cooldown or an error before the first SSE event).
// retryAt describes when another attempt is allowed, not confirmed recovery.
type upstreamRateLimitError struct {
	retryAt time.Time
	reason  string
	message string
}

func (e *upstreamRateLimitError) Error() string {
	if e.message != "" {
		return e.message
	}
	return "upstream rate limited; retry after the indicated delay; availability not confirmed"
}

func (e *upstreamRateLimitError) retryAfter() string {
	remaining := time.Until(e.retryAt)
	seconds := int64(remaining / time.Second)
	if remaining > time.Duration(seconds)*time.Second {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}

func explicitProviderCooldown(headers http.Header, body []byte) (time.Duration, bool) {
	if raw := headers.Get("Retry-After"); raw != "" {
		// Bound conversion, not the provider's legitimate retry interval.
		if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
			return time.Duration(seconds) * time.Second, true
		}
		if resetAt, err := http.ParseTime(raw); err == nil && resetAt.After(time.Now()) {
			return time.Until(resetAt), true
		}
	}
	if resetAt, ok := resetTimestampFromBody(string(body)); ok && resetAt.After(time.Now()) {
		return time.Until(resetAt), true
	}
	return 0, false
}

func (s *proxyServer) rememberRateLimit(provider, runtime string, candidate modelConfig, headers http.Header, body []byte) *upstreamRateLimitError {
	return s.rateLimitResponse(provider, runtime, candidate, headers, body, true)
}

func (s *proxyServer) rateLimitResponse(provider, runtime string, candidate modelConfig, headers http.Header, body []byte, remember bool) *upstreamRateLimitError {
	reason := providerFailureReason(http.StatusTooManyRequests, headers, body)
	duration := providerCooldownForReason(reason, headers, body)
	until := time.Now().Add(duration)
	if remember {
		key := s.providerFailureBlockKey(provider, runtime, candidate, headers, reason)
		s.markProviderBlocked(key, reason, duration)
		if blocked, state := s.providerBlocked(key); blocked {
			until = state.BlockedUntil
		}
	}
	return &upstreamRateLimitError{retryAt: until, reason: reason}
}
