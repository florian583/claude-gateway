package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestStableClaudeConversationKey(t *testing.T) {
	model := modelConfig{Requested: "opus", Upstream: "claude-opus-5-5", Provider: "anthropic", AccountPool: "test-pool"}
	for _, identity := range []string{
		`{"device_id":"test-device","account_uuid":"test-account","session_id":"11111111-1111-4111-8111-111111111111"}`,
		`user_test-device_account_test-account_session_11111111-1111-4111-8111-111111111111`,
	} {
		payload := map[string]any{"metadata": map[string]any{"user_id": identity}, "messages": []any{"before compaction"}}
		before := routingStickyKey(model, payload)
		payload["messages"] = []any{"after compaction"}
		alias := model
		alias.Requested = "claude-opus-5-5[1m]"
		if before != routingStickyKey(alias, payload) || !strings.HasPrefix(before, "conversation-v2|") {
			t.Fatal("compaction or alias change lost session affinity")
		}
		payload["metadata"] = map[string]any{"user_id": strings.ReplaceAll(identity, "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222")}
		if before == routingStickyKey(model, payload) {
			t.Fatal("different conversations share a pin")
		}
		payload["metadata"] = map[string]any{"user_id": identity}
		other := model
		other.AccountPool = "other-pool"
		if before == routingStickyKey(other, payload) {
			t.Fatal("different pools share a pin")
		}
	}
	bad := map[string]any{"metadata": map[string]any{"user_id": "not-a-session"}, "messages": []any{"first"}}
	a := routingStickyKey(model, bad)
	bad["messages"] = []any{"second"}
	if a == routingStickyKey(model, bad) {
		t.Fatal("unrecognized metadata must keep fingerprint fallback")
	}
}

func TestAccountPinsPersistAndValidatePool(t *testing.T) {
	cfg := config{Definition: &fileConfig{StateDir: t.TempDir()}}
	s := &proxyServer{cfg: cfg}
	profile := claudeUsageProfile{Name: "test-primary", CredentialsService: "synthetic-service"}
	key := "synthetic-conversation-identity"
	s.refreshClaudeAccountLease(key, profile)
	file := s.accountPinPath()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), key) || strings.Contains(string(data), profile.CredentialsService) || strings.Contains(string(data), profile.Name) {
		t.Fatal("saved pins leak identifiers")
	}
	info, _ := os.Stat(file)
	if info.Mode().Perm() != 0600 {
		t.Fatal("pin file must be private")
	}
	s.refreshClaudeAccountLease(key, profile)
	after, _ := os.Stat(file)
	if !info.ModTime().Equal(after.ModTime()) {
		t.Fatal("unchanged pin caused a disk write")
	}
	restarted := &proxyServer{cfg: cfg}
	restarted.loadClaudeAccountPins()
	if _, ok := restarted.restoreClaudeAccountPinLocked(key, []claudeUsageProfile{{Name: "other", CredentialsService: "other-service"}}); ok {
		t.Fatal("restored pin escaped configured pool")
	}
	pin, ok := restarted.restoreClaudeAccountPinLocked(key, []claudeUsageProfile{profile})
	if !ok || pin.Profile.Name != profile.Name {
		t.Fatal("restart lost pin")
	}
	other := claudeUsageProfile{Name: "test-fallback", CredentialsService: "other-service"}
	chosen, _, changed := restarted.chooseClaudeAccountWithPolicy([]availableClaudeAccount{{profile: other}}, key, profile, false, false)
	if chosen.profile.Name != other.Name || !changed {
		t.Fatal("unavailable pinned account did not fail over")
	}
	restarted.loadClaudeAccountPins()
	pin, ok = restarted.restoreClaudeAccountPinLocked(key, []claudeUsageProfile{profile, other})
	if !ok || pin.Profile.Name != other.Name {
		t.Fatal("fallback account was not persisted")
	}
}

func TestOpusPinSurvivesIdleExpiry(t *testing.T) {
	s := &proxyServer{cfg: config{ClaudeUsage: claudeUsageConfig{Provider: "anthropic", EligibleUpstreams: []string{"claude-opus-5-5"}}}}
	if s.claudeAccountPinExpired(modelConfig{Provider: "anthropic", Upstream: "claude-opus-5-5"}, time.Now().Add(-24*time.Hour), time.Now()) {
		t.Fatal("subscription pin expired while idle")
	}
}

func TestAccountPinsIgnoreCorruptState(t *testing.T) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: t.TempDir()}}}
	if err := os.WriteFile(s.accountPinPath(), []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	s.loadClaudeAccountPins()
	if len(s.claudePersistentPins) != 0 {
		t.Fatal("corrupt state accepted")
	}
}
