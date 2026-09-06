package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfiguredBrowserMonitorWarmsCacheWithoutInference(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer upstream.Close()
	p := filepath.Join(t.TempDir(), "usage.json")
	value := .5
	snapshot := browserUsageSnapshot{Version: 1, Providers: map[string]browserUsageProvider{"endpoint": {Status: "ok", FetchedAt: time.Now(), Utilization: &value}}}
	b, _ := json.Marshal(snapshot)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	f := testConfig(upstream.URL, "anthropic")
	provider := f.Providers["endpoint"]
	provider.Usage = usageDefinition{Mode: "browser", SnapshotPath: p}
	f.Providers["endpoint"] = provider
	s := testProxy(t, compileFixture(t, f))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.monitorConfiguredBrowserUsage(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.providerState.mu.Lock()
		cached, ok := s.providerState.browser["endpoint"]
		s.providerState.mu.Unlock()
		if ok && cached.available {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background snapshot not collected")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop")
	}
	if calls.Load() != 0 {
		t.Fatal("snapshot monitor contacted provider")
	}
}

func TestConfiguredMigrationPolicies(t *testing.T) {
	f := testConfig("http://127.0.0.1:9999", "anthropic")
	f.AlsoListen = []string{"127.0.0.1:9998"}
	f.Normalize = normalizeConfig{UnsupportedContentTypes: map[string]string{"tool_reference": "text"}}
	f.AdaptiveRouting = &adaptiveRoutingConfig{Enabled: true, StickySeconds: 300, QualityBonus: map[string]float64{"endpoint|test-model": 9}}
	f.ProviderQuarantine = &providerQuarantineConfig{Enabled: true, NetworkIncidentOrigins: 2, NetworkIncidentSuppressSeconds: 90}
	p := f.Providers["endpoint"]
	disabled := false
	p.CircuitBreaker = &disabled
	p.FoldSystemIntoMessages = true
	p.DropResponseContentTypes = []string{"thinking", "redacted_thinking"}
	f.Providers["endpoint"] = p
	m := f.Models["model"]
	m.ResponseAlias = "test-model[1m]"
	f.Models["model"] = m
	c := compileFixture(t, f)
	if c.Models["test"].ResponseAlias != "test-model[1m]" {
		t.Fatal("response alias lost")
	}
	if len(c.AlsoListen) != 1 || c.AlsoListen[0] != f.AlsoListen[0] || c.AdaptiveRouting.StickySeconds != 300 || c.AdaptiveRouting.QualityBonus["endpoint|test-model"] != 9 || !c.Quarantine.Enabled || c.Quarantine.NetworkIncidentSuppressSeconds != 90 || c.Normalize.UnsupportedContentTypes["tool_reference"] != "text" {
		t.Fatal("migration policy lost")
	}
	p2 := c.Providers["endpoint"]
	if p2.CircuitBreaker == nil || *p2.CircuitBreaker || !p2.FoldSystemIntoMessages || len(p2.DropResponseContentTypes) != 2 {
		t.Fatal("adapter settings lost")
	}
}

func TestConfiguredMigrationRejectsUnsafeSettings(t *testing.T) {
	for name, edit := range map[string]func(*fileConfig){
		"remote-listener":    func(f *fileConfig) { f.AlsoListen = []string{"0.0.0.0:9000"} },
		"duplicate-listener": func(f *fileConfig) { f.Listen = "127.0.0.1:9000"; f.AlsoListen = []string{f.Listen} },
		"zero-extra-port":    func(f *fileConfig) { f.AlsoListen = []string{"127.0.0.1:0"} },
		"drop-tools": func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.DropResponseContentTypes = []string{"tool_use"}
			f.Providers["endpoint"] = p
		},
		"normalize-tools": func(f *fileConfig) { f.Normalize.UnsupportedContentTypes = map[string]string{"tool_use": "text"} },
		"reserve-without-api": func(f *fileConfig) {
			p := f.Providers["endpoint"]
			p.Usage.SessionThresholdPct = 85
			p.Usage.ReserveUpstreams = []string{"test-model"}
			f.Providers["endpoint"] = p
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := testConfig("http://127.0.0.1:9999", "anthropic")
			edit(&f)
			b, _ := json.Marshal(f)
			if _, err := decodeFileConfig(t.TempDir()+"/config.json", b); err == nil {
				t.Fatal("unsafe setting accepted")
			}
		})
	}
}

func TestConfiguredOllamaReservePaidFallback(t *testing.T) {
	var paid, subscription atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/paid" {
			paid.Add(1)
		} else {
			subscription.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testMessageResponse)
	}))
	defer upstream.Close()
	f := testConfig("https://example.invalid", "anthropic")
	p := f.Providers["endpoint"]
	p.Billing = "subscription"
	p.Auth = authDefinition{Type: "env", Name: "RESERVE_TEST_KEY"}
	p.Variant = "ollama-cloud"
	p.Usage = usageDefinition{Mode: "api", PollIntervalSeconds: 300, SessionThresholdPct: 85, WeeklyThresholdPct: 85, ReserveUpstreams: []string{"reserved-model"}}
	f.Providers["endpoint"] = p
	reserved := f.Models["model"]
	reserved.Upstream = "reserved-model"
	f.Models["reserved"] = reserved
	p.Variant = "generic"
	p.Usage = usageDefinition{}
	p.Billing = "metered"
	p.MessagesPath = "/paid"
	f.Providers["paid"] = p
	m := f.Models["model"]
	m.Provider = "paid"
	f.Models["paid"] = m
	f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "paid"}}, AllowPaidFallback: true}
	t.Setenv("RESERVE_TEST_KEY", "test-only-token")
	c := compileFixture(t, f)
	for id, p := range c.Providers {
		p.BaseURL = upstream.URL
		c.Providers[id] = p
	}
	s := testProxy(t, c)
	s.ollamaUse.hasValue = true
	s.ollamaUse.snapshot = ollamaUsageSnapshot{FetchedAt: time.Now(), Session: ollamaUsageWindow{Usage: .9}}
	if blocked, _ := s.ollamaCandidateReservedOut(modelConfig{Provider: "endpoint", Upstream: "reserved-model"}); blocked {
		t.Fatal("reserved model blocked early")
	}
	out := sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
	if out.Code != 200 || paid.Load() != 1 || subscription.Load() != 0 {
		t.Fatalf("status=%d paid=%d subscription=%d %s", out.Code, paid.Load(), subscription.Load(), out.Body.String())
	}
	p = f.Providers["endpoint"]
	p.Usage.ReserveUpstreams = []string{"missing-model"}
	f.Providers["endpoint"] = p
	b, _ := json.Marshal(f)
	if _, err := decodeFileConfig(t.TempDir()+"/invalid.json", b); err == nil || !strings.Contains(err.Error(), "not eligible") {
		t.Fatal("unconfigured reserve model accepted")
	}
}
