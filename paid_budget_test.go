package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func readBudget(t *testing.T, path string) paidBudgetLedger {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var l paidBudgetLedger
	if json.Unmarshal(raw, &l) != nil {
		t.Fatal("invalid ledger")
	}
	return l
}
func TestPaidBudgetConcurrentRollingWindowAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "budget.json")
	now := time.Now()
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := reservePaidBudget(path, "paid", 25, 1000000, now); err == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 25 {
		t.Fatalf("admitted %d", accepted.Load())
	}
	cfg := config{Metrics: metricsConfig{Path: filepath.Join(filepath.Dir(path), "metrics.jsonl")}, Providers: map[string]providerConfig{"paid": {Rolling24hBudgetUSD: 25}}}
	if err := os.Rename(path, paidBudgetPath(cfg)); err != nil {
		t.Fatal(err)
	}
	// A new gateway conservatively charges interrupted reservations, rather than
	// resetting the cap or freeing them at the original request's start time.
	if err := recoverPaidBudgets(cfg, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	path = paidBudgetPath(cfg)
	if _, err := reservePaidBudget(path, "paid", 25, 1, now.Add(24*time.Hour)); err == nil {
		t.Fatal("restart reset budget")
	}
	if _, err := reservePaidBudget(path, "paid", 25, 1000000, now.Add(26*time.Hour+time.Second)); err != nil {
		t.Fatal(err)
	}
}
func TestPaidBudgetSettlementUsesCompletionTimeAndPreservesUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "budget.json")
	now := time.Now()
	id, err := reservePaidBudget(path, "paid", 25, 25000000, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = settlePaidBudget(path, id, 1000000, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = reservePaidBudget(path, "paid", 25, 25000000, now.Add(24*time.Hour+time.Minute)); err == nil {
		t.Fatal("expired from request start")
	}
	if _, err = reservePaidBudget(path, "paid", 25, 25000000, now.Add(25*time.Hour+time.Second)); err != nil {
		t.Fatal(err)
	}
}
func TestPaidBudgetCorruptMissingAndUnboundedFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "budget.json")
	now := time.Now()
	if _, err := reservePaidBudget(path, "paid", 25, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := reservePaidBudget(path, "paid", 25, 1, now); err == nil {
		t.Fatal("missing ledger reset spend")
	}
	if err := os.WriteFile(path, []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reservePaidBudget(path, "paid", 25, 1, now); err == nil {
		t.Fatal("corrupt ledger accepted")
	}
	p := providerConfig{Pricing: pricingDefinition{InputPerMillionTokens: 2, OutputPerMillionTokens: 10}}
	for _, body := range []string{`{}`, `{"max_tokens":-1}`, `{"max_tokens":9999999}`} {
		if _, err := budgetReserveBound(p, modelConfig{ContextWindow: 1048576}, []byte(body)); err == nil {
			t.Fatal("unbounded request accepted")
		}
	}
	for _, value := range []float64{-1, math.Inf(1), math.NaN()} {
		if validatePaidBudget(providerDefinition{Rolling24hBudgetUSD: value}) == nil {
			t.Fatal("invalid cap accepted")
		}
	}
}
func TestPaidBudgetBodyAccountsCachedInputAndUnknownResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		sse        bool
		status     int
		want       int64
	}{
		{"json", `{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":100,"cache_read_input_tokens":1000,"cache_creation_input_tokens":50,"output_tokens":20}}`, false, 200, 700},
		{"sse", "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":1000,\"cache_creation_input_tokens\":50,\"output_tokens\":0}}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":20}}\n\ndata: {\"type\":\"message_stop\"}\n\n", true, 200, 700},
		{"reported-cost", `{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":20},"provider_metadata":{"gateway":{"cost":"0.0007"}}}`, false, 200, 700},
		{"sse-reported-cost", "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"provider_metadata\":{\"gateway\":{\"cost\":\"0.0007\"}}}\n\ndata: {\"type\":\"message_stop\"}\n", true, 200, 700},
		{"start-cost-is-not-final", "data: {\"type\":\"message_start\",\"provider_metadata\":{\"gateway\":{\"cost\":\"0\"}}}\n\ndata: {\"type\":\"message_stop\"}\n", true, 200, 1000000},
		{"interrupted", "data: {\"type\":\"message_start\"}\n", true, 200, 1000000},
		{"rejected", `{"error":{"type":"rate_limit_error"}}`, false, 429, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "budget.json")
			id, err := reservePaidBudget(path, "paid", 25, 1000000, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			b := &paidBudgetBody{ReadCloser: io.NopCloser(bytes.NewBufferString(tc.body)), path: path, id: id, bound: 1000000, contextTokens: 1000, status: tc.status, sse: tc.sse, cancel: func() {}, provider: providerConfig{Pricing: pricingDefinition{InputPerMillionTokens: 2, OutputPerMillionTokens: 10, CacheReadPerMillionTokens: 0.2}}}
			if _, err = io.Copy(io.Discard, b); err != nil {
				t.Fatal(err)
			}
			if err = b.Close(); err != nil {
				t.Fatal(err)
			}
			if err = b.Close(); err != nil {
				t.Fatal(err)
			}
			l := readBudget(t, path)
			var cost int64
			for _, e := range l.Entries {
				cost += e.MicroUSD
			}
			if cost != tc.want {
				t.Fatalf("cost %d want %d", cost, tc.want)
			}
		})
	}
}
func TestPaidBudgetDenialSendsNoRequestAndDoesNotTripProvider(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{"type":"message"}`)) }))
	defer upstream.Close()
	dir := t.TempDir()
	cfg := config{Metrics: metricsConfig{Path: filepath.Join(dir, "metrics.jsonl")}, Providers: map[string]providerConfig{"paid": {BaseURL: upstream.URL, Billing: "metered", Rolling24hBudgetUSD: 0.01, Pricing: pricingDefinition{InputPerMillionTokens: 2, OutputPerMillionTokens: 10}}}}
	s, err := newProxyServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"model": "test", "max_tokens": 1000, "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	body, _ := json.Marshal(payload)
	m := modelConfig{Requested: "test", Provider: "paid", Upstream: "test", ContextWindow: 1048576}
	for i := 0; i < 4; i++ {
		_, err = s.doWithFallbacks(context.Background(), httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(body)), body, payload, m)
		if err == nil {
			t.Fatal("expected denial")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("budget denial reached upstream")
	}
	if blocked, _ := s.providerBlocked("paid"); blocked {
		t.Fatal("local cap poisoned provider")
	}
}

func TestModelPricesShareOneProviderBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "budget.json")
	now := time.Now()
	p := providerConfig{Pricing: pricingDefinition{InputPerMillionTokens: 2, OutputPerMillionTokens: 10}}
	high, err := budgetReserveBound(p, modelConfig{ContextWindow: 1000000}, []byte(`{"max_tokens":100000}`))
	if err != nil {
		t.Fatal(err)
	}
	p.Pricing = pricingDefinition{InputPerMillionTokens: 1.4, OutputPerMillionTokens: 4.4}
	low, err := budgetReserveBound(p, modelConfig{ContextWindow: 1000000}, []byte(`{"max_tokens":100000}`))
	if err != nil {
		t.Fatal(err)
	}
	if high != 3000000 || low != 1840000 {
		t.Fatalf("wrong per-model bounds %d %d", high, low)
	}
	if _, err = reservePaidBudget(path, "paid", 4, high, now); err != nil {
		t.Fatal(err)
	}
	if _, err = reservePaidBudget(path, "paid", 4, low, now); err == nil {
		t.Fatal("second model bypassed shared cap")
	}
}

func TestPaidBudgetAndModelPricingCompileTogether(t *testing.T) {
	f := testConfig("https://gateway.example.com", "anthropic")
	p := f.Providers["endpoint"]
	p.Billing = "metered"
	p.Auth = authDefinition{Type: "env", Name: "TEST_GATEWAY_KEY"}
	p.Pricing = pricingDefinition{InputPerMillionTokens: 2, OutputPerMillionTokens: 10}
	p.Rolling24hBudgetUSD = 25
	f.Providers["endpoint"] = p
	m := f.Models["model"]
	m.Pricing = pricingDefinition{InputPerMillionTokens: 1.4, OutputPerMillionTokens: 4.4, CacheReadPerMillionTokens: 0.26}
	f.Models["model"] = m
	cfg := compileFixture(t, f)
	if cfg.Providers["endpoint"].Rolling24hBudgetUSD != 25 || cfg.Models["test"].Pricing != m.Pricing {
		t.Fatal("wire budget/pricing was lost")
	}
	if !metricPolicySkip("paid_budget_exhausted") || !metricPolicySkip("paid_budget_unavailable") {
		t.Fatal("local cap entered provider reliability metrics")
	}
}
