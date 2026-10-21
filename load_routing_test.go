package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestLoadSpillDispatchesPaidBackup(t *testing.T) {
	for _, busyPaid := range []bool{false, true} {
		t.Run(fmt.Sprint(busyPaid), func(t *testing.T) {
			var primary, paid, backup atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/primary":
					primary.Add(1)
				case "/paid":
					paid.Add(1)
				case "/backup":
					backup.Add(1)
				}
				io.WriteString(w, testMessageResponse)
			}))
			defer up.Close()
			f := testConfig("https://example.invalid", "anthropic")
			p := f.Providers["endpoint"]
			p.Auth = authDefinition{Type: "env", Name: "PORTABLE_TEST_KEY"}
			p.Billing = "subscription"
			p.MessagesPath = "/primary"
			f.Providers["endpoint"] = p
			for _, id := range []string{"paid", "backup"} {
				p.Billing = "metered"
				p.MessagesPath = "/" + id
				f.Providers[id] = p
				m := f.Models["model"]
				m.Provider = id
				f.Models[id] = m
			}
			f.Chains["main"] = chainDefinition{SoftConcurrency: 1, AllowPaidFallback: true, Steps: []chainStep{{Model: "model"}, {Model: "paid"}, {Model: "backup"}}}
			t.Setenv("PORTABLE_TEST_KEY", "test-only-token")
			cfg := compileFixture(t, f)
			for id, p := range cfg.Providers {
				p.BaseURL = up.URL
				cfg.Providers[id] = p
				circuitBreakers.Delete(id + "|test-model")
			}
			s := testProxy(t, cfg)
			_, release := s.passiveConcurrency.begin("endpoint", "", "test-model", "main", "/v1/messages")
			defer release()
			if busyPaid {
				_, done := s.passiveConcurrency.begin("paid", "", "test-model", "main", "/v1/messages")
				defer done()
			}
			sendTestRequest(s, `{"model":"test","messages":[{"role":"user","content":"hello"}]}`)
			if primary.Load() != 0 || paid.Load()+backup.Load() != 1 || (busyPaid && backup.Load() != 1) || (!busyPaid && paid.Load() != 1) {
				t.Fatalf("wrong dispatch primary=%d paid=%d backup=%d", primary.Load(), paid.Load(), backup.Load())
			}
		})
	}
}

func TestLoadRoutingScopeAndPolicy(t *testing.T) {
	s := &proxyServer{}
	_, release := s.passiveConcurrency.begin("subscription", "", "model", "chain", "/v1/messages")
	if s.routeActive("subscription", "model") != 1 || s.routeActive("other", "model") != 0 || s.routeActive("subscription", "other") != 0 {
		t.Fatal("load must be provider/model scoped")
	}
	release()
	if s.routeActive("subscription", "model") != 0 {
		t.Fatal("load leaked")
	}
	e := paidFallbackEvidence{"subscription": "load_spill"}
	m := modelConfig{AllowPaidFallback: true, SoftConcurrency: 4}
	if !e.loadAllowed(m) {
		t.Fatal("explicit spill denied")
	}
	m.SoftConcurrency = 0
	if e.loadAllowed(m) {
		t.Fatal("spill without opt-in")
	}
	m.SoftConcurrency = 4
	e["unknown"] = ""
	if e.loadAllowed(m) {
		t.Fatal("unknown evidence allowed")
	}
}

func TestPaidBackupBudgetOptIn(t *testing.T) {
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{
		"a": {Billing: "subscription"}, "b": {Billing: "metered"}, "c": {Billing: "metered"},
	}}}
	a := []claudeCandidateAttempt{}
	for _, p := range []string{"a", "b", "c"} {
		a = append(a, claudeCandidateAttempt{model: modelConfig{Provider: p, AllowPaidFallback: true, PaidFallbackOn: []string{"response-header-timeout"}}})
	}
	got, _ := s.remainingAttemptHeaderBudget(requestTrace{ChainBudget: 90 * time.Second}, a, 0, "a")
	if got != 40*time.Second {
		t.Fatalf("primary budget %v", got)
	}
	got, _ = s.remainingAttemptHeaderBudget(requestTrace{ChainBudget: 50 * time.Second}, a, 1, "b")
	if got != 25*time.Second {
		t.Fatalf("backup budget %v", got)
	}
}

func TestAddressUnavailableIsLocalNetwork(t *testing.T) {
	for _, err := range []error{fmt.Errorf("socket: %w", syscall.EADDRNOTAVAIL), fmt.Errorf("read: can't assign requested address")} {
		if !strongLocalNetworkError(err) {
			t.Fatal("local address error not recognized")
		}
	}
}
