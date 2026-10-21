package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Rollups persist compact per-hour traffic aggregates so provider, chain, and
// account decisions can be made on history that outlives the raw metrics
// retention window. Lines are counter deltas: summing them always reconstructs
// the totals, even across restarts and partial flushes.

var rollupLatencyBoundsMS = []int64{500, 1000, 2000, 3000, 5000, 8000, 15000, 30000, 60000, 120000}
var rollupTPSBounds = []float64{20, 40, 60, 80, 100, 150, 200, 300}

const (
	rollupFlushDelay    = 60 * time.Second
	rollupRetentionDays = 90
)

type rollupKey struct {
	usageAttribution
	Hour        string `json:"hour"`
	Client      string `json:"client,omitempty"`
	Model       string `json:"model,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Upstream    string `json:"upstream,omitempty"`
	Profile     string `json:"profile,omitempty"`
	Chain       string `json:"chain,omitempty"`
	ActualModel string `json:"actualModel,omitempty"`
}

type rollupCounts struct {
	Records          int64            `json:"records"`
	Completions      int64            `json:"completions,omitempty"`
	Successes        int64            `json:"successes,omitempty"`
	Failures         int64            `json:"failures,omitempty"`
	Fallbacks        int64            `json:"fallbacks,omitempty"`
	Cancels          int64            `json:"cancels,omitempty"`
	CountTokens      int64            `json:"countTokens,omitempty"`
	InputTokens      int64            `json:"inputTokens,omitempty"`
	OutputTokens     int64            `json:"outputTokens,omitempty"`
	CacheReadTokens  int64            `json:"cacheReadTokens,omitempty"`
	CacheWriteTokens int64            `json:"cacheWriteTokens,omitempty"`
	FailedTokens     int64            `json:"failedTokens,omitempty"`
	LargeContexts    int64            `json:"contextsOver500k,omitempty"`
	CostUSD          float64          `json:"costUsd,omitempty"`
	PolicySkips      map[string]int64 `json:"policySkips,omitempty"`
	FailureReasons   map[string]int64 `json:"failureReasons,omitempty"`
	HeaderLatency    []int64          `json:"headerLatencyMs,omitempty"`
	FirstContent     []int64          `json:"firstContentMs,omitempty"`
	TPS              []int64          `json:"tokensPerSecond,omitempty"`
	UsageKnown       map[string]int64 `json:"usageKnown,omitempty"`
	UsageTotals      map[string]int64 `json:"usageTotals,omitempty"`
}

type rollupLine struct {
	rollupKey
	ID        string       `json:"id,omitempty"`
	Timestamp string       `json:"ts"`
	Delta     rollupCounts `json:"d"`
}

func histogramObserve(bounds []int64, value int64) []int64 {
	out := make([]int64, len(bounds)+1)
	i := sort.Search(len(bounds), func(i int) bool { return value <= bounds[i] })
	out[i] = 1
	return out
}

func histogramObserveFloat(bounds []float64, value float64) []int64 {
	out := make([]int64, len(bounds)+1)
	i := sort.Search(len(bounds), func(i int) bool { return value <= bounds[i] })
	out[i] = 1
	return out
}

func int64SliceSub(a, b []int64) []int64 {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	out := make([]int64, n)
	nonzero := false
	for i := 0; i < n; i++ {
		if i < len(a) {
			out[i] += a[i]
		}
		if i < len(b) {
			out[i] -= b[i]
		}
		if out[i] != 0 {
			nonzero = true
		}
	}
	if !nonzero {
		return nil
	}
	return out
}

func int64SliceAdd(dst, src []int64) []int64 {
	if len(src) == 0 {
		return dst
	}
	if len(dst) < len(src) {
		grown := make([]int64, len(src))
		copy(grown, dst)
		dst = grown
	}
	for i, v := range src {
		dst[i] += v
	}
	return dst
}

func stringInt64MapSub(a, b map[string]int64) map[string]int64 {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	out := map[string]int64{}
	for k, v := range a {
		out[k] += v
	}
	for k, v := range b {
		out[k] -= v
	}
	for k, v := range out {
		if v == 0 {
			delete(out, k)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func stringInt64MapAdd(dst, src map[string]int64) map[string]int64 {
	for k, v := range src {
		if v == 0 {
			continue
		}
		if dst == nil {
			dst = map[string]int64{}
		}
		dst[k] += v
	}
	return dst
}

func rollupCountsSub(a, b rollupCounts) rollupCounts {
	return rollupCounts{
		UsageKnown:       stringInt64MapSub(a.UsageKnown, b.UsageKnown),
		UsageTotals:      stringInt64MapSub(a.UsageTotals, b.UsageTotals),
		Records:          a.Records - b.Records,
		Completions:      a.Completions - b.Completions,
		Successes:        a.Successes - b.Successes,
		Failures:         a.Failures - b.Failures,
		Fallbacks:        a.Fallbacks - b.Fallbacks,
		Cancels:          a.Cancels - b.Cancels,
		CountTokens:      a.CountTokens - b.CountTokens,
		InputTokens:      a.InputTokens - b.InputTokens,
		OutputTokens:     a.OutputTokens - b.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens - b.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens - b.CacheWriteTokens,
		FailedTokens:     a.FailedTokens - b.FailedTokens,
		LargeContexts:    a.LargeContexts - b.LargeContexts,
		CostUSD:          a.CostUSD - b.CostUSD,
		PolicySkips:      stringInt64MapSub(a.PolicySkips, b.PolicySkips),
		FailureReasons:   stringInt64MapSub(a.FailureReasons, b.FailureReasons),
		HeaderLatency:    int64SliceSub(a.HeaderLatency, b.HeaderLatency),
		FirstContent:     int64SliceSub(a.FirstContent, b.FirstContent),
		TPS:              int64SliceSub(a.TPS, b.TPS),
	}
}

func rollupCountsAdd(a, b rollupCounts) rollupCounts {
	return rollupCounts{
		UsageKnown:       stringInt64MapAdd(a.UsageKnown, b.UsageKnown),
		UsageTotals:      stringInt64MapAdd(a.UsageTotals, b.UsageTotals),
		Records:          a.Records + b.Records,
		Completions:      a.Completions + b.Completions,
		Successes:        a.Successes + b.Successes,
		Failures:         a.Failures + b.Failures,
		Fallbacks:        a.Fallbacks + b.Fallbacks,
		Cancels:          a.Cancels + b.Cancels,
		CountTokens:      a.CountTokens + b.CountTokens,
		InputTokens:      a.InputTokens + b.InputTokens,
		OutputTokens:     a.OutputTokens + b.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens + b.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens + b.CacheWriteTokens,
		FailedTokens:     a.FailedTokens + b.FailedTokens,
		LargeContexts:    a.LargeContexts + b.LargeContexts,
		CostUSD:          a.CostUSD + b.CostUSD,
		PolicySkips:      stringInt64MapAdd(a.PolicySkips, b.PolicySkips),
		FailureReasons:   stringInt64MapAdd(a.FailureReasons, b.FailureReasons),
		HeaderLatency:    int64SliceAdd(a.HeaderLatency, b.HeaderLatency),
		FirstContent:     int64SliceAdd(a.FirstContent, b.FirstContent),
		TPS:              int64SliceAdd(a.TPS, b.TPS),
	}
}

func rollupCountsZero(c rollupCounts) bool {
	return len(c.UsageKnown) == 0 && len(c.UsageTotals) == 0 && c.CacheWriteTokens == 0 && c.FailedTokens == 0 && c.LargeContexts == 0 && c.Records == 0 && c.Completions == 0 && c.Successes == 0 && c.Failures == 0 &&
		c.Fallbacks == 0 && c.Cancels == 0 && c.CountTokens == 0 && c.InputTokens == 0 &&
		c.OutputTokens == 0 && c.CacheReadTokens == 0 && c.CostUSD == 0 &&
		len(c.PolicySkips) == 0 && len(c.FailureReasons) == 0 &&
		len(c.HeaderLatency) == 0 && len(c.FirstContent) == 0 && len(c.TPS) == 0
}

func rollupCountsClone(c rollupCounts) rollupCounts {
	out := c
	out.UsageKnown = stringInt64MapAdd(nil, c.UsageKnown)
	out.UsageTotals = stringInt64MapAdd(nil, c.UsageTotals)
	if c.PolicySkips != nil {
		out.PolicySkips = map[string]int64{}
		for k, v := range c.PolicySkips {
			out.PolicySkips[k] = v
		}
	}
	if c.FailureReasons != nil {
		out.FailureReasons = map[string]int64{}
		for k, v := range c.FailureReasons {
			out.FailureReasons[k] = v
		}
	}
	out.HeaderLatency = append([]int64(nil), c.HeaderLatency...)
	out.FirstContent = append([]int64(nil), c.FirstContent...)
	out.TPS = append([]int64(nil), c.TPS...)
	return out
}

// histogramPercentile approximates a percentile from fixed-boundary counts by
// returning the upper bound of the first bucket whose cumulative share reaches
// q. Returns 0 with no observations.
func histogramPercentile(bounds []int64, counts []int64, q float64) float64 {
	var total int64
	for _, v := range counts {
		total += v
	}
	if total == 0 {
		return 0
	}
	var cumulative int64
	for i, v := range counts {
		cumulative += v
		if float64(cumulative)/float64(total) >= q {
			if i < len(bounds) {
				return float64(bounds[i])
			}
			return float64(bounds[len(bounds)-1])
		}
	}
	return float64(bounds[len(bounds)-1])
}

func histogramPercentileFloat(bounds []float64, counts []int64, q float64) float64 {
	var total int64
	for _, v := range counts {
		total += v
	}
	if total == 0 {
		return 0
	}
	var cumulative int64
	for i, v := range counts {
		cumulative += v
		if float64(cumulative)/float64(total) >= q {
			if i < len(bounds) {
				return bounds[i]
			}
			return bounds[len(bounds)-1]
		}
	}
	return bounds[len(bounds)-1]
}

type rollupStore struct {
	mu         sync.Mutex
	flushMu    sync.Mutex
	dir        string
	buckets    map[rollupKey]*rollupCounts
	flushed    map[rollupKey]rollupCounts
	flushTimer *time.Timer
	closed     bool
	sweptDay   string
	instance   string
	sequence   uint64
	pending    map[rollupKey]pendingRollup
	lastError  string
	appendLine func(string, []byte) error // nil uses durable local append
}

type pendingRollup struct {
	line     rollupLine
	snapshot rollupCounts
	payload  []byte
}

func newRollupStore(dir string) *rollupStore {
	return &rollupStore{
		dir:      dir,
		buckets:  map[rollupKey]*rollupCounts{},
		flushed:  map[rollupKey]rollupCounts{},
		pending:  map[rollupKey]pendingRollup{},
		instance: fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()),
	}
}

func (r *rollupStore) rollupHour(ts string) string {
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		parsed = time.Now()
	}
	return parsed.UTC().Truncate(time.Hour).Format(time.RFC3339)
}

func cancelMetricReason(reason string) bool {
	return reason == "client_cancel" || reason == "client_cancel_after_stall"
}

func (r *rollupStore) observe(sample metricsSample) {
	if r == nil {
		return
	}
	key := rollupKey{
		usageAttribution: sample.usageAttribution,
		Hour:             r.rollupHour(sample.Timestamp),
		Client:           sample.Client,
		Model:            sample.RequestedModel,
		Provider:         sample.Provider,
		Upstream:         sample.Upstream,
		Profile:          sample.ClaudeProfile,
		ActualModel:      sample.ReportedModel,
	}
	if sample.Execution != nil {
		key.Chain = sample.Execution.Chain
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	bucket := r.buckets[key]
	// Bound additional session cardinality. Raw metrics retain attribution;
	// overflow is explicit in historical aggregates, never silently dropped.
	if bucket == nil && len(r.buckets) >= 10000 {
		key.usageAttribution = usageAttribution{Workload: "attribution-overflow"}
		bucket = r.buckets[key]
	}
	if bucket == nil {
		bucket = &rollupCounts{}
		r.buckets[key] = bucket
	}
	bucket.Records++
	switch sample.RecordKind {
	case "completion":
		bucket.Completions++
		if usage := sample.Usage; usage != nil {
			for field, count := range map[string]*int64{"inputUncached": usage.InputUncached, "inputTotal": usage.InputTotal,
				"cacheRead": usage.CacheRead, "cacheCreation": usage.CacheCreation, "cacheCreation5m": usage.CacheCreation5m,
				"cacheCreation1h": usage.CacheCreation1h, "outputInclusive": usage.Output, "reasoningSubset": usage.Reasoning} {
				if count != nil {
					bucket.UsageKnown = stringInt64MapAdd(bucket.UsageKnown, map[string]int64{field: 1})
					bucket.UsageTotals = stringInt64MapAdd(bucket.UsageTotals, map[string]int64{field: *count})
				}
			}
			if usage.Inconsistent {
				bucket.UsageKnown = stringInt64MapAdd(bucket.UsageKnown, map[string]int64{"inconsistent": 1})
			}
		}
		// Partial failed streams can consume tokens. Preserve observed usage;
		// absent usage remains unknown, not an estimate of provider charges.
		bucket.InputTokens += sample.InputTokens
		bucket.OutputTokens += sample.OutputTokens
		bucket.CacheReadTokens += sample.CacheReadTokens
		bucket.CacheWriteTokens += sample.CacheWriteTokens
		contextTokens := sample.InputTokens + sample.CacheReadTokens + sample.CacheWriteTokens
		if contextTokens > 500000 {
			bucket.LargeContexts++
		}
		if !sample.Success {
			bucket.FailedTokens += contextTokens + sample.OutputTokens
		}
		if sample.Success {
			bucket.Successes++
			bucket.CostUSD += sample.CostUsdEstimate
			if sample.LatencyMS > 0 {
				bucket.HeaderLatency = int64SliceAdd(bucket.HeaderLatency, histogramObserve(rollupLatencyBoundsMS, sample.LatencyMS))
			}
			if sample.FirstContentMS > 0 {
				bucket.FirstContent = int64SliceAdd(bucket.FirstContent, histogramObserve(rollupLatencyBoundsMS, sample.FirstContentMS))
			}
			if sample.TokensPerSecond > 0 {
				bucket.TPS = int64SliceAdd(bucket.TPS, histogramObserveFloat(rollupTPSBounds, sample.TokensPerSecond))
			}
		} else {
			bucket.Failures++
		}
	case "skip":
		reason := firstNonEmpty(sample.FailureReason, "unknown")
		bucket.PolicySkips = stringInt64MapAdd(bucket.PolicySkips, map[string]int64{reason: 1})
	case "terminal":
		if sample.Success {
			bucket.Successes++
		} else {
			bucket.Failures++
		}
	case "count_tokens":
		bucket.CountTokens++
	case "attempt":
		if sample.Success {
			bucket.Successes++
		} else {
			bucket.Failures++
		}
	}
	if sample.IsFallback {
		bucket.Fallbacks++
	}
	if cancelMetricReason(sample.FailureReason) || sample.Status == 499 {
		bucket.Cancels++
	}
	if !sample.Success && sample.FailureReason != "" && sample.RecordKind != "skip" {
		bucket.FailureReasons = stringInt64MapAdd(bucket.FailureReasons, map[string]int64{sample.FailureReason: 1})
	}
	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(rollupFlushDelay, r.flush)
	}
}

func rollupFileName(t time.Time) string {
	return "rollup-" + t.UTC().Format("2006-01-02") + ".jsonl"
}

func (r *rollupStore) flush() {
	if r == nil {
		return
	}
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	r.mu.Lock()
	r.flushTimer = nil
	if r.pending == nil {
		r.pending = map[rollupKey]pendingRollup{}
	}
	for key, counts := range r.buckets {
		if _, exists := r.pending[key]; exists {
			continue // retry exact ambiguous batch before newer observations
		}
		delta := rollupCountsSub(*counts, r.flushed[key])
		if rollupCountsZero(delta) {
			continue
		}
		r.sequence++
		line := rollupLine{rollupKey: key, ID: fmt.Sprintf("%s-%d", r.instance, r.sequence),
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Delta: delta}
		payload, err := json.Marshal(line)
		if err != nil {
			r.lastError = "encode: " + err.Error()
			log.Printf("rollup persistence: %s", r.lastError)
			continue
		}
		r.pending[key] = pendingRollup{line: line, snapshot: rollupCountsClone(*counts), payload: append(payload, '\n')}
	}
	batch := make(map[rollupKey]pendingRollup, len(r.pending))
	for key, pending := range r.pending {
		batch[key] = pending
	}
	closed := r.closed
	r.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	appendLine := r.appendLine
	if appendLine == nil {
		appendLine = appendRollupLine
	}
	failed := false
	for key, pending := range batch {
		hour, err := time.Parse(time.RFC3339, key.Hour)
		if err == nil {
			err = appendLine(filepath.Join(r.dir, rollupFileName(hour)), pending.payload)
		}
		r.mu.Lock()
		if err != nil {
			failed = true
			r.lastError = err.Error()
			log.Printf("rollup persistence: %s", err)
		} else {
			// Advance only after complete write, fsync and successful close.
			r.flushed[key] = pending.snapshot
			delete(r.pending, key)
		}
		r.mu.Unlock()
	}
	r.mu.Lock()
	if !failed && len(r.pending) == 0 {
		r.lastError = ""
	}
	if !r.closed && r.flushTimer == nil {
		// New observations may have arrived while this snapshot persisted.
		for key, counts := range r.buckets {
			if _, waiting := r.pending[key]; waiting || !rollupCountsZero(rollupCountsSub(*counts, r.flushed[key])) {
				r.flushTimer = time.AfterFunc(rollupFlushDelay, r.flush)
				break
			}
		}
	}
	r.mu.Unlock()
	if !closed {
		r.sweepExpired()
	}
}

func appendRollupLine(path string, payload []byte) (result error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() {
		if err := file.Close(); result == nil && err != nil {
			result = fmt.Errorf("close: %w", err)
		}
	}()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 0 {
		last := make([]byte, 1)
		if _, err := file.ReadAt(last, info.Size()-1); err != nil {
			return err
		}
		if last[0] != '\n' {
			start := info.Size() - (4 << 20)
			if start < 0 {
				start = 0
			}
			tail := make([]byte, info.Size()-start)
			if _, err := file.ReadAt(tail, start); err != nil {
				return err
			}
			at := bytes.LastIndexByte(tail, '\n')
			if at < 0 && start > 0 {
				return fmt.Errorf("incomplete tail exceeds recovery bound")
			}
			end := start + int64(at+1)
			if err := file.Truncate(end); err != nil {
				return fmt.Errorf("recover tail: %w", err)
			}
			log.Printf("rollup recovery: discarded %d incomplete trailing bytes; replaying identified batch", info.Size()-end)
		}
	}
	if n, err := file.Write(payload); err != nil {
		return fmt.Errorf("write: %w", err)
	} else if n != len(payload) {
		return io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

func (r *rollupStore) persistenceStatus() string {
	if r == nil {
		return "unavailable"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastError
}

// sweepExpired removes rollup day files older than the retention window. It
// runs at most once per UTC day and only scans the small rollup directory.
func (r *rollupStore) sweepExpired() {
	r.mu.Lock()
	today := time.Now().UTC().Format("2006-01-02")
	if r.sweptDay == today {
		r.mu.Unlock()
		return
	}
	r.sweptDay = today
	r.mu.Unlock()
	cutoff := time.Now().UTC().AddDate(0, 0, -rollupRetentionDays)
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "rollup-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		day, err := time.Parse("2006-01-02", strings.TrimSuffix(strings.TrimPrefix(name, "rollup-"), ".jsonl"))
		if err != nil || !day.Before(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(r.dir, name))
	}
}

func (r *rollupStore) close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	if r.flushTimer != nil {
		r.flushTimer.Stop()
		r.flushTimer = nil
	}
	r.mu.Unlock()
	r.flush()
}

// readRollupLines parses every rollup delta line in dir whose hour falls inside
// [since, now]. File order does not matter; deltas are summed downstream.
func readRollupLines(dir string, since time.Time) ([]rollupLine, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []rollupLine
	seen := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "rollup-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return out, fmt.Errorf("read rollup %s: %w", name, err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(raw))
		scanner.Buffer(make([]byte, 64<<10), 4<<20)
		for scanner.Scan() {
			var line rollupLine
			if json.Unmarshal(scanner.Bytes(), &line) != nil {
				return out, fmt.Errorf("malformed rollup in %s; history incomplete", name)
			}
			if line.ID != "" {
				if seen[line.ID] {
					continue
				}
				seen[line.ID] = true
			}
			hour, err := time.Parse(time.RFC3339, line.Hour)
			if err != nil || hour.Before(since) {
				continue
			}
			out = append(out, line)
		}
		if err := scanner.Err(); err != nil {
			return out, err
		}
	}
	return out, nil
}

// eventLog is a tiny append-only stream of noteworthy transitions: provider
// blocks, quarantine changes, exhausted chains. It answers "did anything break
// while I was not looking" without grepping the request log.
type proxyEvent struct {
	Timestamp string `json:"ts"`
	Kind      string `json:"kind"`
	Provider  string `json:"provider,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

type eventLog struct {
	mu         sync.Mutex
	flushMu    sync.Mutex
	path       string
	pending    []proxyEvent
	flushTimer *time.Timer
	closed     bool
}

const eventLogFlushDelay = time.Second
const eventLogMaxBytes = 4 << 20

func newEventLog(path string) *eventLog {
	return &eventLog{path: path}
}

func (l *eventLog) record(event proxyEvent) {
	if l == nil || l.path == "" {
		return
	}
	event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.pending = append(l.pending, event)
	if l.flushTimer == nil {
		l.flushTimer = time.AfterFunc(eventLogFlushDelay, l.flush)
	}
}

func (l *eventLog) flush() {
	if l == nil || l.path == "" {
		return
	}
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	batch := append([]proxyEvent(nil), l.pending...)
	l.pending = nil
	l.flushTimer = nil
	l.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o700); err != nil {
		return
	}
	if info, err := os.Stat(l.path); err == nil && info.Size() >= eventLogMaxBytes {
		_ = os.Rename(l.path, l.path+".1")
	}
	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	for _, event := range batch {
		line, err := json.Marshal(event)
		if err != nil {
			continue
		}
		_, _ = file.Write(append(line, '\n'))
	}
	_ = file.Close()
}

func (l *eventLog) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	if l.flushTimer != nil {
		l.flushTimer.Stop()
	}
	l.mu.Unlock()
	l.flush()
}

// tailEvents returns the newest events from the log without loading it whole;
// files stay under eventLogMaxBytes so a full read stays cheap.
func tailEvents(path string, limit int) []proxyEvent {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var events []proxyEvent
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 16<<10), 1<<20)
	for scanner.Scan() {
		var event proxyEvent
		if json.Unmarshal(scanner.Bytes(), &event) == nil {
			events = append(events, event)
		}
	}
	if len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events
}

// usageHistoryLog appends deduplicated quota-window readings so utilization
// trajectories survive beyond the live snapshot. Writes are rate-limited per
// scope+window: a line lands at most every 15 minutes unless the value moved.
type usageHistoryLog struct {
	mu         sync.Mutex
	flushMu    sync.Mutex
	path       string
	lastWrite  map[string]usageHistoryEntry
	pending    []usageHistoryEntry
	flushTimer *time.Timer
	closed     bool
}

type usageHistoryEntry struct {
	Timestamp string  `json:"ts"`
	Scope     string  `json:"scope"`
	Window    string  `json:"window"`
	Percent   float64 `json:"pct"`
	ResetsAt  string  `json:"resetsAt,omitempty"`
}

const usageHistoryMinInterval = 15 * time.Minute
const usageHistoryMinDeltaPct = 2.0
const usageHistoryMaxBytes = 4 << 20

func newUsageHistoryLog(path string) *usageHistoryLog {
	return &usageHistoryLog{path: path, lastWrite: map[string]usageHistoryEntry{}}
}

func (l *usageHistoryLog) note(scope, window string, percent float64, resetsAt time.Time) {
	if l == nil || l.path == "" || scope == "" || window == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	key := scope + "|" + window
	now := time.Now().UTC()
	last, seen := l.lastWrite[key]
	if seen && now.Sub(parseHistoryTime(last.Timestamp)) < usageHistoryMinInterval && absFloat(last.Percent-percent) < usageHistoryMinDeltaPct {
		return
	}
	entry := usageHistoryEntry{
		Timestamp: now.Format(time.RFC3339Nano),
		Scope:     scope,
		Window:    window,
		Percent:   percent,
	}
	if !resetsAt.IsZero() {
		entry.ResetsAt = resetsAt.UTC().Format(time.RFC3339)
	}
	l.lastWrite[key] = entry
	l.pending = append(l.pending, entry)
	if l.flushTimer == nil {
		l.flushTimer = time.AfterFunc(eventLogFlushDelay, l.flush)
	}
}

func parseHistoryTime(ts string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, ts)
	return parsed
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func (l *usageHistoryLog) flush() {
	if l == nil || l.path == "" {
		return
	}
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	batch := append([]usageHistoryEntry(nil), l.pending...)
	l.pending = nil
	l.flushTimer = nil
	l.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o700); err != nil {
		return
	}
	if info, err := os.Stat(l.path); err == nil && info.Size() >= usageHistoryMaxBytes {
		_ = os.Rename(l.path, l.path+".1")
	}
	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	for _, entry := range batch {
		line, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		_, _ = file.Write(append(line, '\n'))
	}
	_ = file.Close()
}

func (l *usageHistoryLog) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	if l.flushTimer != nil {
		l.flushTimer.Stop()
	}
	l.mu.Unlock()
	l.flush()
}

func readUsageHistory(path string, since time.Time) []usageHistoryEntry {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []usageHistoryEntry
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 16<<10), 1<<20)
	for scanner.Scan() {
		var entry usageHistoryEntry
		if json.Unmarshal(scanner.Bytes(), &entry) != nil {
			continue
		}
		if parseHistoryTime(entry.Timestamp).Before(since) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// requestClientLabel tags which local client produced a request so orphan or
// abusive senders can be identified from metrics alone. The listener port
// distinguishes claude-hybrid profiles; the user agent distinguishes tools.
func requestClientLabel(ua, listenerPort string) string {
	if len(ua) > 80 {
		ua = ua[:80]
	}
	switch {
	case ua == "" && listenerPort == "":
		return ""
	case ua == "":
		return "port:" + listenerPort
	case listenerPort == "":
		return ua
	default:
		return listenerPort + "|" + ua
	}
}

func (s *proxyServer) recordEvent(kind, provider, reason, detail string) {
	if s.events == nil {
		return
	}
	s.events.record(proxyEvent{Kind: kind, Provider: provider, Reason: reason, Detail: detail})
}

func formatEventDetail(until time.Time) string {
	if until.IsZero() {
		return ""
	}
	return fmt.Sprintf("until=%s", until.UTC().Format(time.RFC3339))
}
