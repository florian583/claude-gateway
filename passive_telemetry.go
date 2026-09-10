package main

import (
	"context"
	"errors"
	"net"
	"sort"
	"sync"
)

// Observers only: no admission control, timers, queues, or routing decisions.
type concurrencyTelemetry struct {
	mu     sync.Mutex
	active map[concurrencyScope]int
}

func (s *proxyServer) telemetryChain(selected modelConfig) string {
	if s.cfg.Definition != nil {
		if chain := s.cfg.Definition.Aliases[selected.Requested]; chain != "" {
			return chain
		}
	}
	return firstNonEmpty(selected.Chain, selected.Requested)
}

type concurrencyScope struct {
	Provider string `json:"provider"`
	Account  string `json:"account,omitempty"`
	Upstream string `json:"upstream"`
	Chain    string `json:"chain,omitempty"`
	Path     string `json:"path"`
}

type activeScope struct {
	concurrencyScope
	Active int `json:"active"`
}

type concurrencySnapshot struct {
	Total  int           `json:"total"`
	Scopes []activeScope `json:"scopes"`
}

type attemptOutcome struct {
	Provider   string `json:"provider"`
	Account    string `json:"account,omitempty"`
	Upstream   string `json:"upstream"`
	Attempt    int    `json:"attempt"`
	Status     int    `json:"status"`
	Reason     string `json:"reason"`
	Executions int    `json:"executions"`
}

type attemptExecution struct {
	ResponseMetadata    map[string]string `json:"responseMetadata,omitempty"`
	Chain               string            `json:"chain,omitempty"`
	Number              int               `json:"number"`
	BeforeAttemptMS     int64             `json:"beforeAttemptMs"`
	Previous            *attemptOutcome   `json:"previous,omitempty"`
	ActiveTotal         int               `json:"activeTotal"`
	ActiveAccount       int               `json:"activeAccount"`
	ActiveModel         int               `json:"activeModel"`
	ActiveChain         int               `json:"activeChain"`
	HTTPVersion         string            `json:"httpVersion,omitempty"`
	HTTPStatus          int               `json:"httpStatus,omitempty"`
	UpstreamRequestID   string            `json:"upstreamRequestId,omitempty"`
	TransportError      string            `json:"transportError,omitempty"`
	ResponseReadError   bool              `json:"responseReadError,omitempty"`
	ConnectionReused    *bool             `json:"connectionReused,omitempty"`
	ConnectionIdleMS    int64             `json:"connectionIdleMs,omitempty"`
	FirstResponseByteMS int64             `json:"firstResponseByteMs,omitempty"`
}

func passiveTransportError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var timeout *responseHeaderTimeoutError
	if errors.As(err, &timeout) {
		return "response_header_timeout"
	}
	var auth *providerCredentialError
	if errors.As(err, &auth) {
		return "credential"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "timeout"
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return "network"
	}
	return "other" // Never persist raw errors, which can contain credentials/URLs.
}

func (c *concurrencyTelemetry) begin(provider, account, upstream, chain, path string) (attemptExecution, func()) {
	key := concurrencyScope{provider, account, upstream, chain, path}
	c.mu.Lock()
	if c.active == nil {
		c.active = map[concurrencyScope]int{}
	}
	c.active[key]++
	e := attemptExecution{Chain: chain}
	for scope, count := range c.active {
		e.ActiveTotal += count
		if scope.Provider == provider && scope.Account == account {
			e.ActiveAccount += count
			if scope.Upstream == upstream {
				e.ActiveModel += count
			}
		}
		if scope.Chain == chain {
			e.ActiveChain += count
		}
	}
	c.mu.Unlock()
	var once sync.Once
	return e, func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.active[key] <= 1 {
				delete(c.active, key)
			} else {
				c.active[key]--
			}
		})
	}
}

func (c *concurrencyTelemetry) snapshot() concurrencySnapshot {
	s := concurrencySnapshot{Scopes: []activeScope{}}
	c.mu.Lock()
	for scope, count := range c.active {
		s.Total += count
		s.Scopes = append(s.Scopes, activeScope{scope, count})
	}
	c.mu.Unlock()
	sort.Slice(s.Scopes, func(i, j int) bool {
		a, b := s.Scopes[i], s.Scopes[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		if a.Account != b.Account {
			return a.Account < b.Account
		}
		if a.Upstream != b.Upstream {
			return a.Upstream < b.Upstream
		}
		if a.Chain != b.Chain {
			return a.Chain < b.Chain
		}
		return a.Path < b.Path
	})
	return s
}

func applyExecutionTelemetry(sample *metricsSample, trace requestTrace) {
	if trace.Execution == nil {
		return
	}
	// Copy before recording: metrics may serialize asynchronously, while the
	// request proceeds through its remaining fallback candidates.
	e := *trace.Execution
	if e.ResponseMetadata != nil {
		e.ResponseMetadata = make(map[string]string, len(trace.Execution.ResponseMetadata))
		for key, value := range trace.Execution.ResponseMetadata {
			e.ResponseMetadata[key] = value
		}
	}
	if e.Previous != nil {
		previous := *e.Previous
		e.Previous = &previous
	}
	sample.Execution = &e
	sample.ResponseHeadersMS, sample.FirstEventMS = trace.HeadersMS, trace.FirstEventMS
	if trace.Timing != nil {
		t := trace.Timing
		sample.ConnectionMS, sample.DNSMS, sample.TLSMS = t.connectionMS.Load(), t.dnsMS.Load(), t.tlsMS.Load()
		e.FirstResponseByteMS = t.firstResponseByteMS.Load()
		if t.gotConnection.Load() {
			reused := t.connectionReused.Load()
			e.ConnectionReused = &reused
			e.ConnectionIdleMS = t.connectionIdleMS.Load()
		}
	}
}
