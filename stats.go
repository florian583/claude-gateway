package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// stats renders persisted rollups, usage history, and events into the tables
// used for provider and chain decisions. It is read-only and never touches
// credentials, providers, or a running server.

type statsFlags struct {
	since    time.Duration
	stateDir string
	json     bool
}

func parseStatsArgs(args []string) (statsFlags, error) {
	flags := statsFlags{since: 24 * time.Hour}
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--json":
			flags.json = true
		case args[i] == "--since" && i+1 < len(args):
			d, err := time.ParseDuration(args[i+1])
			if err != nil || d <= 0 {
				return flags, fmt.Errorf("--since requires a positive duration like 24h or 168h")
			}
			flags.since = d
			i++
		case args[i] == "--state-dir" && i+1 < len(args):
			flags.stateDir = args[i+1]
			i++
		default:
			return flags, fmt.Errorf("unknown stats argument %q (usage: stats [--since DURATION] [--state-dir PATH] [--json])", args[i])
		}
	}
	return flags, nil
}

type providerStats struct {
	Completions, Successes, Failures, Fallbacks, Cancels int64
	InputTokens, OutputTokens, CacheReadTokens           int64
	CostUSD                                              float64
	FirstContent, TPS                                    []int64
}

type statsReport struct {
	SinceHours float64                       `json:"sinceHours"`
	Providers  map[string]providerStats      `json:"providers"`
	ModelMix   map[string]map[string]int64   `json:"modelMix"`
	Failures   map[string]int64              `json:"failures"`
	Skips      map[string]int64              `json:"policySkips"`
	Clients    map[string]int64              `json:"clients"`
	SpendByDay map[string]map[string]float64 `json:"spendByDay"`
	Quota      map[string]usageHistoryEntry  `json:"quotaLatest"`
	Events     []proxyEvent                  `json:"events"`
}

func buildStatsReport(stateDir string, since time.Duration) (statsReport, error) {
	report := statsReport{
		SinceHours: since.Hours(),
		Providers:  map[string]providerStats{},
		ModelMix:   map[string]map[string]int64{},
		Failures:   map[string]int64{},
		Skips:      map[string]int64{},
		Clients:    map[string]int64{},
		SpendByDay: map[string]map[string]float64{},
		Quota:      map[string]usageHistoryEntry{},
	}
	cutoff := time.Now().Add(-since)
	lines, err := readRollupLines(filepath.Join(stateDir, "rollups"), cutoff)
	if err != nil {
		return report, err
	}
	for _, line := range lines {
		d := line.Delta
		if line.Client != "" {
			report.Clients[line.Client] += d.Records
		}
		if line.Provider != "" {
			pk := line.Provider
			if line.Upstream != "" {
				pk = line.Provider + "/" + line.Upstream
			}
			p := report.Providers[pk]
			p.Completions += d.Completions
			p.Successes += d.Successes
			p.Failures += d.Failures
			p.Fallbacks += d.Fallbacks
			p.Cancels += d.Cancels
			p.InputTokens += d.InputTokens
			p.OutputTokens += d.OutputTokens
			p.CacheReadTokens += d.CacheReadTokens
			p.CostUSD += d.CostUSD
			p.FirstContent = int64SliceAdd(p.FirstContent, d.FirstContent)
			p.TPS = int64SliceAdd(p.TPS, d.TPS)
			report.Providers[pk] = p
		}
		if line.Model != "" && line.Provider != "" && d.Successes > 0 {
			if report.ModelMix[line.Model] == nil {
				report.ModelMix[line.Model] = map[string]int64{}
			}
			report.ModelMix[line.Model][line.Provider] += d.Successes
		}
		for reason, n := range d.FailureReasons {
			report.Failures[line.Provider+":"+reason] += n
		}
		for reason, n := range d.PolicySkips {
			report.Skips[reason] += n
		}
		if d.CostUSD > 0 && line.Provider != "" {
			day := line.Hour
			if len(day) > 10 {
				day = day[:10]
			}
			if report.SpendByDay[day] == nil {
				report.SpendByDay[day] = map[string]float64{}
			}
			report.SpendByDay[day][line.Provider] += d.CostUSD
		}
	}
	for _, entry := range readUsageHistory(filepath.Join(stateDir, "usage-history.jsonl"), cutoff) {
		key := entry.Scope + "|" + entry.Window
		if parseHistoryTime(entry.Timestamp).After(parseHistoryTime(report.Quota[key].Timestamp)) {
			report.Quota[key] = entry
		}
	}
	report.Events = tailEvents(filepath.Join(stateDir, "events.jsonl"), 10)
	return report, nil
}

func formatTokens(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func renderStatsText(out io.Writer, report statsReport) {
	fmt.Fprintf(out, "claude-proxy stats — last %.0fh\n\n", report.SinceHours)

	fmt.Fprintln(out, "PROVIDER PERFORMANCE (successful completions)")
	fmt.Fprintf(out, "%-44s %6s %6s %8s %10s %9s %8s\n", "provider/upstream", "n", "ok%", "tps p50", "first p50", "out tok", "cost")
	type prow struct {
		key string
		p   providerStats
	}
	rows := []prow{}
	for k, p := range report.Providers {
		if p.Completions == 0 {
			continue
		}
		rows = append(rows, prow{k, p})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].p.Completions > rows[j].p.Completions })
	for _, r := range rows {
		ok := 0.0
		if r.p.Successes+r.p.Failures > 0 {
			ok = 100 * float64(r.p.Successes) / float64(r.p.Successes+r.p.Failures)
		}
		cost := ""
		if r.p.CostUSD > 0 {
			cost = fmt.Sprintf("$%.2f", r.p.CostUSD)
		}
		fmt.Fprintf(out, "%-44s %6d %5.1f%% %8.0f %9.0fms %9s %8s\n",
			r.key, r.p.Completions, ok,
			histogramPercentileFloat(rollupTPSBounds, r.p.TPS, 0.5),
			histogramPercentile(rollupLatencyBoundsMS, r.p.FirstContent, 0.5),
			formatTokens(r.p.OutputTokens), cost)
	}

	fmt.Fprintln(out, "\nREQUESTED MODEL → PROVIDER MIX (successes)")
	models := sortedKeysInt64Map(report.ModelMix)
	for _, model := range models {
		mix := report.ModelMix[model]
		var total int64
		for _, n := range mix {
			total += n
		}
		parts := []string{}
		for _, provider := range sortedKeysInt64(mix) {
			parts = append(parts, fmt.Sprintf("%s %.0f%%", provider, 100*float64(mix[provider])/float64(total)))
		}
		fmt.Fprintf(out, "  %-28s %s\n", model, strings.Join(parts, "  "))
	}

	if len(report.Failures) > 0 {
		fmt.Fprintln(out, "\nTOP FAILURES (provider:reason)")
		for _, key := range topCounts(report.Failures, 10) {
			fmt.Fprintf(out, "  %-52s ×%d\n", key, report.Failures[key])
		}
	}
	if len(report.Skips) > 0 {
		fmt.Fprintln(out, "\nPOLICY SKIPS")
		for _, reason := range topCounts(report.Skips, 10) {
			fmt.Fprintf(out, "  %-52s ×%d\n", reason, report.Skips[reason])
		}
	}
	if len(report.Clients) > 0 {
		fmt.Fprintln(out, "\nCLIENTS (records)")
		for _, client := range topCounts(report.Clients, 10) {
			fmt.Fprintf(out, "  %-52s ×%d\n", client, report.Clients[client])
		}
	}
	if len(report.SpendByDay) > 0 {
		fmt.Fprintln(out, "\nMETERED SPEND BY DAY")
		for _, day := range sortedKeysFloat(report.SpendByDay) {
			parts := []string{}
			for _, provider := range sortedKeysFloatMap(report.SpendByDay[day]) {
				parts = append(parts, fmt.Sprintf("%s $%.2f", provider, report.SpendByDay[day][provider]))
			}
			fmt.Fprintf(out, "  %s  %s\n", day, strings.Join(parts, "  "))
		}
	}
	if len(report.Quota) > 0 {
		fmt.Fprintln(out, "\nQUOTA LATEST")
		for _, key := range sortedKeysEntry(report.Quota) {
			e := report.Quota[key]
			fmt.Fprintf(out, "  %-32s %-8s %5.1f%%  resets %s\n", e.Scope, e.Window, e.Percent, e.ResetsAt)
		}
	}
	if len(report.Events) > 0 {
		fmt.Fprintln(out, "\nRECENT EVENTS")
		for _, e := range report.Events {
			fmt.Fprintf(out, "  %s  %-20s %-30s %s %s\n", e.Timestamp, e.Kind, e.Provider, e.Reason, e.Detail)
		}
	}
	if len(report.Providers) == 0 {
		fmt.Fprintln(out, "no rollup data in window — rollups accumulate from this build onward")
	}
}

func sortedKeysInt64(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysInt64Map(m map[string]map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysFloat(m map[string]map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysFloatMap(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysEntry(m map[string]usageHistoryEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func topCounts(m map[string]int64, limit int) []string {
	keys := sortedKeysInt64(m)
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys
}

func runStats(cfg config, args []string, out io.Writer) error {
	flags, err := parseStatsArgs(args)
	if err != nil {
		return err
	}
	stateDir := flags.stateDir
	if stateDir == "" {
		if cfg.Metrics.Path == "" {
			return errors.New("metrics path is not configured; pass --state-dir")
		}
		stateDir = filepath.Dir(cfg.Metrics.Path)
	}
	report, err := buildStatsReport(stateDir, flags.since)
	if err != nil {
		return err
	}
	if flags.json {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(encoded))
		return err
	}
	renderStatsText(out, report)
	return nil
}
