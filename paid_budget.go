package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const paidBudgetWindow = 24 * time.Hour
const paidAttemptLifetime = time.Hour
const paidBudgetMaxEntries = 32768

// Pending reservations never age out while the process is serving. Recovery
// charges their full bound for another window plus the maximum attempt lifetime.
type paidBudgetEntry struct {
	Provider string    `json:"provider"`
	At       time.Time `json:"at"`
	MicroUSD int64     `json:"microUSD"`
	Pending  bool      `json:"pending"`
}
type paidBudgetLedger struct {
	Version int                        `json:"version"`
	Entries map[string]paidBudgetEntry `json:"entries"`
}

var paidBudgetFileMu sync.Mutex

func validatePaidBudget(p providerDefinition) error {
	v := p.Rolling24hBudgetUSD
	if v == 0 {
		return nil
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > 1000000 || p.Billing != "metered" || p.Protocol != "anthropic" ||
		p.Pricing.InputPerMillionTokens <= 0 || p.Pricing.OutputPerMillionTokens <= 0 {
		return errors.New("rolling24hBudgetUSD requires a positive finite limit, metered Anthropic protocol, and input/output pricing")
	}
	for _, v := range []float64{p.Pricing.InputPerMillionTokens, p.Pricing.OutputPerMillionTokens, p.Pricing.CacheReadPerMillionTokens} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1000000 {
			return errors.New("invalid budget pricing")
		}
	}
	return nil
}

func paidBudgetPath(cfg config) string {
	return filepath.Join(filepath.Dir(cfg.Metrics.Path), "paid-budget.json")
}

// A separate stable lock survives atomic ledger replacement and coordinates
// overlapping gateway processes as well as goroutines. A corrupt ledger denies spend.
func editPaidBudget(path string, now time.Time, edit func(*paidBudgetLedger) error) error {
	paidBudgetFileMu.Lock()
	defer paidBudgetFileMu.Unlock()
	if !filepath.IsAbs(path) {
		return errors.New("paid budget requires an absolute state path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	fd, err := syscall.Open(path+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	lock := os.NewFile(uintptr(fd), path+".lock")
	defer lock.Close()
	if err = syscall.Flock(fd, syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	lockInfo, err := lock.Stat()
	if err != nil {
		return err
	}
	initialized := lockInfo.Size() > 0
	ledger := paidBudgetLedger{Version: 1, Entries: map[string]paidBudgetEntry{}}
	if info, e := os.Lstat(path); e == nil {
		if !info.Mode().IsRegular() || info.Size() > 8<<20 {
			return errors.New("unsafe paid budget state")
		}
		raw, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(raw, &ledger); e != nil {
			return errors.New("invalid paid budget state")
		}
		if ledger.Version != 1 || ledger.Entries == nil || len(ledger.Entries) > paidBudgetMaxEntries {
			return errors.New("unsupported paid budget state")
		}
	} else if !os.IsNotExist(e) {
		return e
	} else if initialized {
		return errors.New("paid budget ledger disappeared")
	}
	for id, entry := range ledger.Entries {
		if entry.MicroUSD < 0 || entry.MicroUSD > 1000000000000 || entry.At.IsZero() || entry.Provider == "" {
			return errors.New("invalid paid budget entry")
		}
		if !entry.Pending && !entry.At.After(now.Add(-paidBudgetWindow)) {
			delete(ledger.Entries, id)
		}
	}
	if !initialized {
		if _, err = lock.WriteAt([]byte("paid-budget-v1\n"), 0); err != nil {
			return err
		}
		if err = lock.Sync(); err != nil {
			return err
		}
	}
	if err = edit(&ledger); err != nil {
		return err
	}
	raw, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".paid-budget-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(raw); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func recoverPaidBudgets(cfg config, now time.Time) error {
	enabled := false
	for _, p := range cfg.Providers {
		if p.Rolling24hBudgetUSD > 0 {
			enabled = true
		}
	}
	if !enabled {
		return nil
	}
	return editPaidBudget(paidBudgetPath(cfg), now, func(l *paidBudgetLedger) error {
		for id, e := range l.Entries {
			if e.Pending {
				e.Pending = false
				e.At = now.Add(paidAttemptLifetime)
				l.Entries[id] = e
			}
		}
		return nil
	})
}

func budgetReserveBound(p providerConfig, m modelConfig, body []byte) (int64, error) {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return 0, errors.New("paid budget requires a JSON request")
	}
	output := countPointer(payload["max_tokens"])
	if m.ContextWindow <= 0 || m.ContextWindow > 1048576 || output == nil || *output <= 0 || *output > int64(m.ContextWindow) {
		return 0, errors.New("paid budget requires bounded context and max_tokens")
	}
	// Context is a hard upstream bound, including images and tools. Reserve at
	// the higher input/cache-read rate; cache writes use the ordinary input rate.
	inputPrice := math.Max(p.Pricing.InputPerMillionTokens, p.Pricing.CacheReadPerMillionTokens)
	cost := float64(m.ContextWindow)*inputPrice + float64(*output)*p.Pricing.OutputPerMillionTokens
	if math.IsNaN(cost) || math.IsInf(cost, 0) || cost <= 0 || cost > 1e12 {
		return 0, errors.New("invalid paid cost bound")
	}
	return int64(math.Ceil(cost)), nil
}

func reservePaidBudget(path, provider string, limit float64, bound int64, now time.Time) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(random[:])
	err := editPaidBudget(path, now, func(l *paidBudgetLedger) error {
		var total int64
		for _, e := range l.Entries {
			if e.Provider == provider {
				total += e.MicroUSD
			}
		}
		if bound <= 0 || float64(total+bound) > math.Floor(limit*1e6) || len(l.Entries) >= paidBudgetMaxEntries {
			return &upstreamRateLimitError{reason: "paid_budget_exhausted", retryAt: now.Add(time.Minute), message: fmt.Sprintf("paid gateway rolling 24-hour budget of $%.2f cannot admit this request", limit)}
		}
		l.Entries[id] = paidBudgetEntry{Provider: provider, At: now, MicroUSD: bound, Pending: true}
		return nil
	})
	return id, err
}

func settlePaidBudget(path, id string, cost int64, now time.Time) error {
	return editPaidBudget(path, now, func(l *paidBudgetLedger) error {
		e, ok := l.Entries[id]
		if !ok {
			return errors.New("missing paid reservation")
		}
		if cost < 0 || cost > e.MicroUSD {
			return errors.New("reported cost exceeds reserved bound")
		}
		if cost == 0 {
			delete(l.Entries, id)
		} else {
			e.Pending = false
			e.MicroUSD = cost
			e.At = now
			l.Entries[id] = e
		}
		return nil
	})
}

func (s *proxyServer) withPaidBudget(ctx context.Context, r *http.Request, body []byte, m modelConfig, maximum time.Duration) (*http.Response, error) {
	provider := firstNonEmpty(m.Provider, s.cfg.DefaultProvider)
	p := s.cfg.Providers[provider]
	if m.Pricing != (pricingDefinition{}) {
		p.Pricing = m.Pricing
	}
	if p.Rolling24hBudgetUSD == 0 {
		return s.doUpstreamWithHeaderBudgetLimitUnbudgeted(ctx, r, body, m, maximum)
	}
	if strings.HasSuffix(r.URL.Path, "/count_tokens") {
		return nil, &requestCompatibilityError{message: "paid token counting uses local estimation"}
	}
	bound, err := budgetReserveBound(p, m, body)
	if err != nil {
		return nil, paidBudgetUnavailable()
	}
	path := paidBudgetPath(s.cfg)
	id, err := reservePaidBudget(path, provider, p.Rolling24hBudgetUSD, bound, time.Now())
	if err != nil {
		var limit *upstreamRateLimitError
		if errors.As(err, &limit) {
			return nil, err
		}
		return nil, paidBudgetUnavailable()
	}
	attemptCtx, cancel := context.WithTimeout(ctx, paidAttemptLifetime)
	resp, err := s.doUpstreamWithHeaderBudgetLimitUnbudgeted(attemptCtx, r, body, m, maximum)
	if err != nil {
		cancel()
		cost, at := bound, time.Now().Add(paidAttemptLifetime)
		var credentials *providerCredentialError
		var compatibility *requestCompatibilityError
		if errors.As(err, &credentials) || errors.As(err, &compatibility) {
			cost, at = 0, time.Now()
		}
		_ = settlePaidBudget(path, id, cost, at)
		return resp, err
	}
	if resp.Body == nil {
		cancel()
		_ = settlePaidBudget(path, id, bound, time.Now().Add(paidAttemptLifetime))
		return resp, nil
	}
	resp.Body = &paidBudgetBody{ReadCloser: resp.Body, path: path, id: id, bound: bound, provider: p, contextTokens: m.ContextWindow, status: resp.StatusCode, sse: strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream"), cancel: cancel}
	return resp, nil
}

type paidBudgetBody struct {
	io.ReadCloser
	mu                    sync.Mutex
	path, id              string
	bound                 int64
	provider              providerConfig
	contextTokens, status int
	sse, complete, closed bool
	buffer                []byte
	dropping              bool
	usage                 usageTelemetry
	reportedMicroUSD      *int64
	cancel                context.CancelFunc
}

func (b *paidBudgetBody) observe(raw []byte) {
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil {
		return
	}
	b.usage.observeAnthropic(value)
	delta, _ := value["delta"].(map[string]any)
	terminal := value["type"] == "message_stop" || (value["type"] == "message_delta" && delta["stop_reason"] != nil) || (!b.sse && value["stop_reason"] != nil)
	if terminal {
		metadata, _ := value["provider_metadata"].(map[string]any)
		gateway, _ := metadata["gateway"].(map[string]any)
		if text, ok := gateway["cost"].(string); ok {
			cost, err := strconv.ParseFloat(text, 64)
			if err == nil && !math.IsNaN(cost) && !math.IsInf(cost, 0) && cost >= 0 && cost*1e6 <= float64(b.bound) {
				micro := int64(math.Ceil(cost * 1e6))
				b.reportedMicroUSD = &micro
			}
		}
	}
	if value["type"] == "message_stop" || (!b.sse && value["stop_reason"] != nil) {
		b.complete = true
	}
}
func (b *paidBudgetBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range p[:n] {
		if b.sse && c == '\n' {
			if !b.dropping {
				line := bytes.TrimSpace(b.buffer)
				if bytes.HasPrefix(line, []byte("data:")) {
					b.observe(bytes.TrimSpace(line[5:]))
				}
			}
			b.buffer = b.buffer[:0]
			b.dropping = false
			continue
		}
		if !b.dropping {
			if len(b.buffer) >= 1<<20 {
				b.buffer = nil
				b.dropping = true
			} else {
				b.buffer = append(b.buffer, c)
			}
		}
	}
	if err == io.EOF && !b.sse && !b.dropping {
		b.observe(b.buffer)
	}
	return n, err
}
func (b *paidBudgetBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return err
	}
	b.closed = true
	if !b.sse && !b.dropping {
		b.observe(b.buffer)
	}
	cost := b.bound
	at := time.Now().Add(paidAttemptLifetime)
	// Explicit rejected HTTP requests never started generation. Unknown transport,
	// truncated output and incomplete usage retain their full reservation.
	switch b.status {
	case 400, 401, 402, 403, 404, 422, 429:
		cost = 0
		at = time.Now()
	}
	if v := b.usage.snapshot(); b.status >= 200 && b.status < 300 && b.complete && v != nil && !v.Inconsistent && v.InputUncached != nil && v.Output != nil {
		input := float64(b.contextTokens) * math.Max(b.provider.Pricing.InputPerMillionTokens, b.provider.Pricing.CacheReadPerMillionTokens)
		if v.CacheRead != nil && v.CacheCreation != nil {
			readPrice := b.provider.Pricing.CacheReadPerMillionTokens
			if readPrice == 0 {
				readPrice = b.provider.Pricing.InputPerMillionTokens
			}
			input = (float64(*v.InputUncached)+float64(*v.CacheCreation))*b.provider.Pricing.InputPerMillionTokens + float64(*v.CacheRead)*readPrice
		}
		actual := int64(math.Ceil(input + float64(*v.Output)*b.provider.Pricing.OutputPerMillionTokens))
		if actual >= 0 && actual <= b.bound {
			cost = actual
			at = time.Now()
		}
	}
	if b.status >= 200 && b.status < 300 && b.complete && b.reportedMicroUSD != nil {
		cost, at = *b.reportedMicroUSD, time.Now()
	}
	if settleErr := settlePaidBudget(b.path, b.id, cost, at); settleErr != nil && err == nil {
		return settleErr
	}
	return err
}

func paidBudgetUnavailable() error {
	return &upstreamRateLimitError{reason: "paid_budget_unavailable", retryAt: time.Now().Add(time.Minute), message: "paid gateway budget cannot prove available funds; no upstream request sent"}
}

func (s *proxyServer) paidBudgetStatus() map[string]any {
	out := map[string]any{}
	path := paidBudgetPath(s.cfg)
	info, err := os.Lstat(path)
	var raw []byte
	valid := err == nil && info.Mode().IsRegular() && info.Size() <= 8<<20
	if valid {
		raw, err = os.ReadFile(path)
		valid = err == nil
	}
	var ledger paidBudgetLedger
	valid = valid && len(raw) <= 8<<20 && json.Unmarshal(raw, &ledger) == nil && ledger.Version == 1 && ledger.Entries != nil
	if valid {
		for _, e := range ledger.Entries {
			if e.MicroUSD < 0 || e.MicroUSD > 1000000000000 || e.At.IsZero() || e.Provider == "" {
				valid = false
				break
			}
		}
	}
	now := time.Now()
	for name, p := range s.cfg.Providers {
		if p.Rolling24hBudgetUSD <= 0 {
			continue
		}
		row := map[string]any{"limitUSD": p.Rolling24hBudgetUSD, "windowHours": 24, "available": valid}
		if valid {
			var spent, reserved int64
			for _, e := range ledger.Entries {
				if e.Provider == name && (e.Pending || e.At.After(now.Add(-paidBudgetWindow))) {
					if e.Pending {
						reserved += e.MicroUSD
					} else {
						spent += e.MicroUSD
					}
				}
			}
			row["chargedUSD"] = float64(spent) / 1e6
			row["reservedUSD"] = float64(reserved) / 1e6
			row["remainingUSD"] = math.Max(0, p.Rolling24hBudgetUSD-float64(spent+reserved)/1e6)
		}
		out[name] = row
	}
	return out
}
