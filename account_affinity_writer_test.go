package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func affinityTestWriter(s *proxyServer, write func(accountPinFile) error) {
	w := &accountPinWriter{wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}), write: write}
	s.claudePinWriter = w
	go s.runClaudeAccountPinWriter(w)
}

func TestAccountPinSlowStorageAndLatestShutdown(t *testing.T) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: t.TempDir()}}}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	affinityTestWriter(s, func(snapshot accountPinFile) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return s.saveClaudeAccountPins(snapshot)
	})
	primary := claudeUsageProfile{Name: "primary", CredentialsService: "synthetic-primary"}
	backup := claudeUsageProfile{Name: "backup", CredentialsService: "synthetic-backup"}
	s.refreshClaudeAccountLease("conversation", primary)
	<-started
	selected := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			s.refreshClaudeAccountLease("conversation", backup)
		}
		s.chooseClaudeAccountWithPolicy([]availableClaudeAccount{{profile: backup}}, "other", backup, false, false)
		close(selected)
	}()
	select {
	case <-selected:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("storage blocked account selection")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.closeClaudeAccountPins() }()
	select {
	case <-closed:
		t.Fatal("shutdown skipped pending persistence")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	restarted := &proxyServer{cfg: s.cfg}
	restarted.loadClaudeAccountPins()
	pin, ok := restarted.restoreClaudeAccountPinLocked("conversation", []claudeUsageProfile{primary, backup})
	if !ok || pin.Profile.Name != backup.Name {
		t.Fatal("latest pin lost at shutdown")
	}
	if calls.Load() > 2 {
		t.Fatalf("updates did not coalesce: %d writes", calls.Load())
	}
}

func TestAccountPinFailureRetriesAndRecovery(t *testing.T) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: t.TempDir()}}}
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	affinityTestWriter(s, func(snapshot accountPinFile) error {
		calls.Add(1)
		if fail.Load() {
			return errors.New("synthetic storage failure")
		}
		return s.saveClaudeAccountPins(snapshot)
	})
	defer s.closeClaudeAccountPins()
	p := claudeUsageProfile{Name: "primary", CredentialsService: "synthetic"}
	s.refreshClaudeAccountLease("first", p)
	if err := s.flushClaudeAccountPins(); err == nil {
		t.Fatal("failed storage reported success")
	}
	if calls.Load() != 3 {
		t.Fatalf("retry count=%d", calls.Load())
	}
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 3 {
		t.Fatal("failed generation was not parked")
	}
	fail.Store(false)
	s.refreshClaudeAccountLease("second", p)
	if err := s.flushClaudeAccountPins(); err != nil {
		t.Fatal(err)
	}
	restarted := &proxyServer{cfg: s.cfg}
	restarted.loadClaudeAccountPins()
	if len(restarted.claudePersistentPins) != 2 {
		t.Fatal("recovery lost pending state")
	}
}

func TestAccountPinCacheBoundAndRestore(t *testing.T) {
	s := &proxyServer{}
	p := claudeUsageProfile{Name: "primary", CredentialsService: "synthetic"}
	s.claudePersistentPins = map[string]persistedAccountPin{affinityHash("returning"): {ProfileHash: affinityHash(claudeProfileRegistryKey(p)), LastUsed: time.Now()}}
	s.rememberClaudeAccountLocked("returning", p, -time.Hour)
	for i := 0; i < maxAccountPins+100; i++ {
		s.rememberClaudeAccountLocked(fmt.Sprint(i), p, time.Hour)
	}
	if len(s.claudeAccounts) != maxAccountPins {
		t.Fatal("cache is not bounded")
	}
	if _, ok := s.claudeAccounts["returning"]; ok {
		t.Fatal("old transient pin not evicted")
	}
	pin, ok := s.restoreClaudeAccountPinLocked("returning", []claudeUsageProfile{p})
	if !ok || pin.Profile.Name != p.Name || len(s.claudeAccounts) != maxAccountPins {
		t.Fatal("durable pin restoration failed or exceeded cap")
	}
}

func TestAccountPinAtomicFailurePreservesPreviousFile(t *testing.T) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: t.TempDir()}}}
	first := accountPinFile{Version: 1, Pins: map[string]persistedAccountPin{}}
	if err := s.saveClaudeAccountPins(first); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.accountPinPath())
	if err != nil {
		t.Fatal(err)
	}
	bad := accountPinFile{Version: 1, Pins: map[string]persistedAccountPin{"invalid": {LastUsed: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}}}
	if err := s.saveClaudeAccountPins(bad); err == nil {
		t.Fatal("expected encoding failure")
	}
	after, err := os.ReadFile(s.accountPinPath())
	if err != nil || string(before) != string(after) {
		t.Fatal("failed replacement changed durable file")
	}
	leftovers, err := filepath.Glob(filepath.Join(s.cfg.Definition.StateDir, ".account-pins-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatal("temporary snapshot leaked")
	}
}

func TestAccountPinDurableRetentionBound(t *testing.T) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: t.TempDir()}}, claudePersistentPins: map[string]persistedAccountPin{}}
	p := claudeUsageProfile{Name: "primary", CredentialsService: "synthetic"}
	for i := 0; i < maxAccountPins; i++ {
		s.claudePersistentPins[affinityHash(fmt.Sprint(i))] = persistedAccountPin{ProfileHash: affinityHash(claudeProfileRegistryKey(p)), LastUsed: time.Unix(int64(i), 0)}
	}
	s.refreshClaudeAccountLease("new", p)
	if err := s.closeClaudeAccountPins(); err != nil {
		t.Fatal(err)
	}
	restarted := &proxyServer{cfg: s.cfg}
	restarted.loadClaudeAccountPins()
	if len(restarted.claudePersistentPins) != maxAccountPins {
		t.Fatal("durable cap changed")
	}
	if _, ok := restarted.claudePersistentPins[affinityHash("0")]; ok {
		t.Fatal("oldest pin not evicted")
	}
	if _, ok := restarted.restoreClaudeAccountPinLocked("new", []claudeUsageProfile{p}); !ok {
		t.Fatal("new pin lost")
	}
}

func TestAccountPinShutdownReportsFailure(t *testing.T) {
	s := &proxyServer{cfg: config{Definition: &fileConfig{StateDir: t.TempDir()}}}
	var calls atomic.Int32
	affinityTestWriter(s, func(accountPinFile) error { calls.Add(1); return errors.New("synthetic failure") })
	s.refreshClaudeAccountLease("conversation", claudeUsageProfile{Name: "primary"})
	if err := s.closeClaudeAccountPins(); err == nil {
		t.Fatal("shutdown hid persistence failure")
	}
	if calls.Load() > 6 {
		t.Fatal("shutdown retries were not bounded")
	}
	if err := s.closeClaudeAccountPins(); err == nil {
		t.Fatal("repeated close lost error")
	}
}
