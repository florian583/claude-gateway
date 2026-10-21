package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestObsoleteSubscriptionVariantsRejected(t *testing.T) {
	for _, variant := range []string{"command-code", "merge-gateway", "xai-subscription"} {
		t.Run(variant, func(t *testing.T) {
			f := testConfig("https://subscription.example", "openai-chat-completions")
			p := f.Providers["endpoint"]
			p.Variant = variant
			f.Providers["endpoint"] = p
			body, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodeFileConfig(filepath.Join(t.TempDir(), "config.json"), body)
			if err == nil || !strings.Contains(err.Error(), "variant") {
				t.Fatalf("obsolete variant accepted or wrong failure: %v", err)
			}
		})
	}
}

func TestAnnualClineVariantRetained(t *testing.T) {
	f := testConfig("https://subscription.example", "openai-chat-completions")
	p := f.Providers["endpoint"]
	p.Variant = "clinepass"
	p.Billing = "subscription"
	p.Auth = authDefinition{Type: "env", Name: "SYNTHETIC_CLINE_KEY"}
	p.Usage.Mode = "api"
	f.Providers["endpoint"] = p
	cfg := compileFixture(t, f)
	if cfg.ClineUsage.Provider != "endpoint" {
		t.Fatal("annual Cline quota reader not retained")
	}
}
