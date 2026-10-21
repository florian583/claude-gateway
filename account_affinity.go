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
	"sync"
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

// Called under claudeAccountMu. Only hashes reach disk.
func (s *proxyServer) rememberClaudeAccountLocked(key string, profile claudeUsageProfile, ttl time.Duration) {
	if s.claudeAccounts == nil {
		s.claudeAccounts = map[string]claudeAccountSticky{}
	}
	s.cacheClaudeAccountLocked(key, claudeAccountSticky{Profile: profile, Until: s.accountNow().Add(ttl)})
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
	s.queueClaudeAccountPinsLocked()
}

// The writer retains one bounded snapshot and coalesces changes in the live map.
// A crash can lose updates since the last successful replacement. Shutdown drains
// the latest state; storage failures receive three attempts before being parked.
type accountPinWriter struct {
	wake       chan struct{}
	stop       chan struct{}
	done       chan struct{}
	changed    chan struct{}
	closeOnce  sync.Once
	generation uint64
	attempted  uint64
	err        error
	write      func(accountPinFile) error
}

func (s *proxyServer) queueClaudeAccountPinsLocked() {
	if s.claudePinWriter == nil {
		w := &accountPinWriter{wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}), write: s.saveClaudeAccountPins}
		s.claudePinWriter = w
		go s.runClaudeAccountPinWriter(w)
	}
	w := s.claudePinWriter
	w.generation++
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (s *proxyServer) runClaudeAccountPinWriter(w *accountPinWriter) {
	defer close(w.done)
	for {
		stopping := false
		select {
		case <-w.wake:
		case <-w.stop:
			stopping = true
		}
		s.claudeAccountMu.Lock()
		generation := w.generation
		if generation == w.attempted && (!stopping || w.err == nil) {
			s.claudeAccountMu.Unlock()
			if stopping {
				return
			}
			continue
		}
		pins := make(map[string]persistedAccountPin, len(s.claudePersistentPins))
		for key, pin := range s.claudePersistentPins {
			pins[key] = pin
		}
		s.claudeAccountMu.Unlock()
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
			}
			err = w.write(accountPinFile{Version: 1, Pins: pins})
			if err == nil {
				break
			}
		}
		if err != nil {
			log.Print("account affinity: failed to persist pins")
		}
		s.claudeAccountMu.Lock()
		w.attempted, w.err = generation, err
		close(w.changed)
		w.changed = make(chan struct{})
		s.claudeAccountMu.Unlock()
		if stopping {
			return
		}
	}
}

func (s *proxyServer) flushClaudeAccountPins() error {
	s.claudeAccountMu.Lock()
	w := s.claudePinWriter
	if w == nil {
		s.claudeAccountMu.Unlock()
		return nil
	}
	target := w.generation
	for w.attempted < target {
		changed := w.changed
		s.claudeAccountMu.Unlock()
		<-changed
		s.claudeAccountMu.Lock()
	}
	err := w.err
	s.claudeAccountMu.Unlock()
	return err
}

// Call after request handlers have drained, so no new pin can follow the snapshot.
func (s *proxyServer) closeClaudeAccountPins() error {
	s.claudeAccountMu.Lock()
	w := s.claudePinWriter
	s.claudeAccountMu.Unlock()
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() { close(w.stop) })
	<-w.done
	return w.err
}

func (s *proxyServer) cacheClaudeAccountLocked(key string, pin claudeAccountSticky) {
	if s.claudeAccounts == nil {
		s.claudeAccounts = map[string]claudeAccountSticky{}
	}
	if _, exists := s.claudeAccounts[key]; !exists && len(s.claudeAccounts) >= maxAccountPins {
		oldest := ""
		for candidate, value := range s.claudeAccounts {
			if oldest == "" || value.Until.Before(s.claudeAccounts[oldest].Until) {
				oldest = candidate
			}
		}
		delete(s.claudeAccounts, oldest)
	}
	s.claudeAccounts[key] = pin
}

func (s *proxyServer) saveClaudeAccountPins(snapshot accountPinFile) error {
	file := s.accountPinPath()
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(file), ".account-pins-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	err = json.NewEncoder(tmp).Encode(snapshot)
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
			s.cacheClaudeAccountLocked(key, pin)
			return pin, true
		}
	}
	return claudeAccountSticky{}, false
}
