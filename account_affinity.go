package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var claudeSessionUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func affinityHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// Claude Code has used both a JSON user_id and user_..._session_UUID.
// Only recognized session identifiers override the conversation-seed fallback.
func stableClaudeConversationKey(model modelConfig, payload map[string]any) string {
	if !strings.HasPrefix(model.Upstream, "claude-") {
		return ""
	}
	metadata, _ := payload["metadata"].(map[string]any)
	userID, _ := metadata["user_id"].(string)
	var identity map[string]any
	session, owner := "", ""
	if json.Unmarshal([]byte(userID), &identity) == nil && identity != nil {
		session, _ = identity["session_id"].(string)
		device, _ := identity["device_id"].(string)
		account, _ := identity["account_uuid"].(string)
		encoded, _ := json.Marshal([]string{device, account})
		owner = string(encoded)
	} else if strings.HasPrefix(userID, "user_") {
		if at := strings.LastIndex(userID, "_session_"); at >= 0 {
			owner, session = userID[:at], userID[at+len("_session_"):]
		}
	}
	if !claudeSessionUUID.MatchString(session) {
		return ""
	}
	encoded, _ := json.Marshal([]string{strings.TrimSuffix(model.Upstream, "[1m]"), model.Provider, model.AccountPool, owner, strings.ToLower(session)})
	return "conversation-v2|" + affinityHash(string(encoded))
}

type persistedAccountPin struct {
	ProfileHash string    `json:"profileHash"`
	LastUsed    time.Time `json:"lastUsed"`
}

type accountPinFile struct {
	Version int                            `json:"version"`
	Pins    map[string]persistedAccountPin `json:"pins"`
}

const maxAccountPins = 10000

func (s *proxyServer) accountPinPath() string {
	if s.cfg.Definition == nil || s.cfg.Definition.StateDir == "" {
		return ""
	}
	return filepath.Join(s.cfg.Definition.StateDir, "claude-account-pins.json")
}

func (s *proxyServer) loadClaudeAccountPins() {
	file, err := os.Open(s.accountPinPath())
	if err != nil {
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4*1024*1024+1))
	var saved accountPinFile
	if err != nil || len(data) > 4*1024*1024 || json.Unmarshal(data, &saved) != nil || saved.Version != 1 || len(saved.Pins) > maxAccountPins {
		log.Print("account affinity: ignoring invalid saved pins")
		return
	}
	for key, pin := range saved.Pins {
		if len(key) != 64 || len(pin.ProfileHash) != 64 || pin.LastUsed.IsZero() {
			delete(saved.Pins, key)
		}
	}
	s.claudePersistentPins = saved.Pins
}

// Called under claudeAccountMu. Only hashes reach disk, never credentials,
// prompts, raw session identifiers, or credential-service names.
func (s *proxyServer) rememberClaudeAccountLocked(key string, profile claudeUsageProfile, ttl time.Duration) {
	if s.claudeAccounts == nil {
		s.claudeAccounts = map[string]claudeAccountSticky{}
	}
	s.claudeAccounts[key] = claudeAccountSticky{Profile: profile, Until: s.accountNow().Add(ttl)}
	if s.accountPinPath() == "" {
		return
	}
	if s.claudePersistentPins == nil {
		s.claudePersistentPins = map[string]persistedAccountPin{}
	}
	digest, profileHash := affinityHash(key), affinityHash(claudeProfileRegistryKey(profile))
	previous, exists := s.claudePersistentPins[digest]
	s.claudePersistentPins[digest] = persistedAccountPin{ProfileHash: profileHash, LastUsed: s.accountNow()}
	// No disk writes for repeated requests on an unchanged account.
	if exists && previous.ProfileHash == profileHash {
		return
	}
	if len(s.claudePersistentPins) > maxAccountPins {
		oldest := ""
		for candidate, pin := range s.claudePersistentPins {
			if candidate != digest && (oldest == "" || pin.LastUsed.Before(s.claudePersistentPins[oldest].LastUsed)) {
				oldest = candidate
			}
		}
		delete(s.claudePersistentPins, oldest)
	}
	if err := s.saveClaudeAccountPinsLocked(); err != nil {
		// Routing remains available when local state storage is unavailable.
		log.Print("account affinity: failed to persist pins")
	}
}

func (s *proxyServer) saveClaudeAccountPinsLocked() error {
	file := s.accountPinPath()
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(file), ".account-pins-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	err = json.NewEncoder(tmp).Encode(accountPinFile{Version: 1, Pins: s.claudePersistentPins})
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp.Name(), file)
}

func (s *proxyServer) restoreClaudeAccountPinLocked(key string, profiles []claudeUsageProfile) (claudeAccountSticky, bool) {
	saved, ok := s.claudePersistentPins[affinityHash(key)]
	if !ok {
		return claudeAccountSticky{}, false
	}
	for _, profile := range profiles {
		if affinityHash(claudeProfileRegistryKey(profile)) == saved.ProfileHash {
			pin := claudeAccountSticky{Profile: profile, Until: s.accountNow().Add(s.claudeAccountLeaseTTL())}
			if s.claudeAccounts == nil {
				s.claudeAccounts = map[string]claudeAccountSticky{}
			}
			s.claudeAccounts[key] = pin
			return pin, true
		}
	}
	return claudeAccountSticky{}, false
}
