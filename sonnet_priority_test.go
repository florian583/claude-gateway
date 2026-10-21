package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSonnetSubscriptionPriorityAndAdvisorCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name         string
		quotaBlocked bool
		advisor      bool
		want         string
		firstBlocked bool
	}{
		{"capacity", false, false, "first", false},
		{"next_account_before_glm", false, false, "second", true},
		{"quota_fallback", true, false, "glm", false},
		{"advisor_capacity", false, true, "first", false},
		{"advisor_next_account", false, true, "second", true},
		{"advisor_no_compatible_capacity", true, true, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var calls []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer sk-ant-oat-test-")
				if id == "" {
					id = "glm"
				}
				mu.Lock()
				calls = append(calls, id)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, testMessageResponse)
			}))
			defer upstream.Close()
			f := testClaudeConfig(t)
			m := f.Models["model"]
			m.Upstream, m.TaskClass = "claude-sonnet-5-5", "balanced"
			f.Models["model"] = m
			f.Providers["glm"] = providerDefinition{Protocol: "anthropic", BaseURL: upstream.URL, Billing: "local", Auth: authDefinition{Type: "none"}}
			m.Provider, m.Upstream, m.TaskClass = "glm", "zai/glm-5.3-flash", "fast"
			f.Models["glm"] = m
			f.Chains["main"] = chainDefinition{Steps: []chainStep{{Model: "model"}, {Model: "glm"}}}
			cfg := compileFixture(t, f)
			p := cfg.Providers["endpoint"]
			p.BaseURL = upstream.URL
			cfg.Providers["endpoint"] = p
			seedTestCredentials(t, cfg)
			cfg.Classifier = &classifierConfig{Enabled: true, Provider: "glm", Model: "test-classifier", CacheTTL: time.Hour}
			s := testProxy(t, cfg)
			if tc.quotaBlocked {
				for _, profile := range cfg.ClaudeUsage.AccountProfiles {
					s.markProviderBlocked("endpoint@"+profile.Name, "quota_exhausted", time.Hour)
				}
			}
			if tc.firstBlocked {
				s.markProviderBlocked("endpoint@first", "quota_exhausted", time.Hour)
			}
			payload := toolTurnPayload()
			payload["model"] = "test"
			if tc.advisor {
				payload["tools"] = []any{map[string]any{"name": "advisor", "type": "advisor_20260301"}}
			}
			selected := cfg.Models["test"]
			selected.Requested = "test"
			key := routingStickyKey(selected, payload)
			s.classifier.cache = map[string]cachedClassification{key: {at: time.Now(), hint: taskRouteHint{Decided: true, Tier: "fast"}}}
			body, _ := json.Marshal(payload)
			out := sendTestRequest(s, string(body))
			mu.Lock()
			got := strings.Join(calls, ",")
			mu.Unlock()
			if got != tc.want {
				t.Fatalf("calls=%q want=%q status=%d body=%s", got, tc.want, out.Code, out.Body.String())
			}
			if tc.want != "" && out.Code != 200 {
				t.Fatalf("status=%d body=%s", out.Code, out.Body.String())
			}
			if tc.want == "" && out.Code == 200 {
				t.Fatal("must report unavailable, not fabricate success")
			}
		})
	}
}
