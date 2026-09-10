package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGatewayTimeoutsDoNotBlockIndependentRoutes(t *testing.T) {
	s, err := newProxyServer(config{Providers: map[string]providerConfig{
		"merge": {BaseURL: "https://merge.example"}, "vercel": {BaseURL: "https://vercel.example"},
	}, Quarantine: providerQuarantineConfig{Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300, MaxSeconds: 600}})
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"merge", "vercel"} {
		for _, message := range []string{"timeout awaiting response headers", "i/o timeout", "connection reset by peer"} {
			d := s.observeNetworkTransportFailure(provider, "transport", fmt.Errorf("%s", message))
			if d.StopFallback || d.SuppressProviderPenalty {
				t.Fatalf("generic error promoted to network outage: %s %#v", message, d)
			}
		}
		s.observeProviderInstability(routeHealthKey(provider, modelConfig{Upstream: "glm"}), "ttfb_timeout")
	}
	if probe, err := s.beginNetworkRequest(); err != nil || probe {
		t.Fatalf("independent route blocked: probe=%t error=%v", probe, err)
	}
}

func TestGatewayModelQuarantineIsolationThroughFallback(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if payload["model"] == "deepseek" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"type":"message","content":[]}`)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"error":{"message":"model overloaded"}}`)
	}))
	defer up.Close()
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"gateway": {BaseURL: up.URL}},
		Quarantine: providerQuarantineConfig{Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300, MaxSeconds: 600}})
	if err != nil {
		t.Fatal(err)
	}
	glm := modelConfig{Provider: "gateway", Upstream: "glm", Requested: "worker"}
	deepseek := modelConfig{Provider: "gateway", Upstream: "deepseek"}
	r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"worker"}`))
	resp, _ := s.doWithFallbacks(context.Background(), r, []byte(`{"model":"worker"}`), map[string]any{"model": "worker"}, glm)
	if resp.resp != nil {
		resp.resp.Body.Close()
	}
	if blocked, _ := s.providerBlockForCandidate("gateway", glm); !blocked {
		t.Fatal("failed GLM not quarantined")
	}
	if blocked, _ := s.providerBlockForCandidate("gateway", deepseek); blocked {
		t.Fatal("GLM failure quarantined healthy sibling DeepSeek")
	}
	if blocked, _ := s.providerBlockForCandidate("other-gateway", glm); blocked {
		t.Fatal("GLM failure crossed gateway")
	}
	if blocked, _ := s.providerBlocked("gateway"); blocked {
		t.Fatal("model failure created gateway-wide block")
	}
	resp, err = s.doWithFallbacks(context.Background(), r, []byte(`{"model":"deepseek"}`), map[string]any{"model": "deepseek"}, deepseek)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.resp.Body.Close()
	if resp.resp.StatusCode != http.StatusOK {
		t.Fatalf("healthy sibling status=%d", resp.resp.StatusCode)
	}
	s.observeProviderHealthy(routeHealthKey("gateway", deepseek))
	if blocked, _ := s.providerBlockForCandidate("gateway", glm); !blocked {
		t.Fatal("sibling success cleared failing model quarantine")
	}
}

func TestRouteHealthKeepsAccountQuotaAndRecoveryIsolated(t *testing.T) {
	s := quarantineTestServer(providerQuarantineConfig{Enabled: true, SignalThreshold: 1, WindowSeconds: 300, BaseSeconds: 300, MaxSeconds: 600, RecoverySuccesses: 1})
	model := modelConfig{Upstream: "glm"}
	s.observeProviderInstability(routeHealthKey("provider@a", model), "provider_protocol_error")
	if blocked, _ := s.providerBlockForCandidate("provider@b", model); blocked {
		t.Fatal("route quarantine crossed accounts")
	}
	s.markProviderBlocked("provider@a", "auth_unavailable", time.Minute)
	s.observeProviderHealthy(routeHealthKey("provider@a", model))
	if blocked, _ := s.providerBlockForCandidate("provider@a", modelConfig{Upstream: "deepseek"}); !blocked {
		t.Fatal("account authentication failure lost scope")
	}
	s.markProviderBlocked("provider@b", "quota_or_rate_limit", time.Minute)
	if blocked, _ := s.providerBlockForCandidate("provider@b", model); !blocked {
		t.Fatal("account quota ignored")
	}
}

func TestRouteHealthRespectsDisabledBreaker(t *testing.T) {
	disabled := false
	s, err := newProxyServer(config{Providers: map[string]providerConfig{"gateway": {BaseURL: "https://gateway.example", CircuitBreaker: &disabled}}, Quarantine: providerQuarantineConfig{Enabled: true, SignalThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if s.observeProviderInstability(routeHealthKey("gateway", modelConfig{Upstream: "glm"}), "http_5xx") {
		t.Fatal("model health key lost base provider policy")
	}
}
