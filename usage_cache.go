package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

// All usage HTTP calls, including request-time cache misses, share admission.
// Inference does not use this gate. Cooldown is independent of rotating tokens.
func (s *proxyServer) admitClaudeUsageFetch(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.claudeUse.mu.Lock()
	if s.claudeUse.gate == nil {
		s.claudeUse.gate = make(chan struct{}, 1)
	}
	gate := s.claudeUse.gate
	s.claudeUse.mu.Unlock()
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-gate }
	s.claudeUse.mu.Lock()
	retry, next := s.claudeUse.globalRetryAt, s.claudeUse.nextFetch
	s.claudeUse.mu.Unlock()
	if time.Now().Before(retry) {
		release()
		return nil, errors.New("Claude usage shared cooldown active")
	}
	if delay := time.Until(next); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
	return func() {
		s.claudeUse.mu.Lock()
		if s.cfg.Definition != nil {
			s.claudeUse.nextFetch = time.Now().Add(time.Second)
		}
		s.claudeUse.mu.Unlock()
		release()
	}, nil
}

type persistedClaudeUsage struct {
	Version  int                               `json:"version"`
	RetryAt  time.Time                         `json:"retryAt"`
	Profiles map[string]persistedClaudeProfile `json:"profiles"`
}
type persistedClaudeProfile struct {
	Identity string              `json:"identity"`
	Snapshot claudeUsageSnapshot `json:"snapshot"`
}

func (s *proxyServer) usageCachePath() string {
	if s.cfg.Definition == nil || s.cfg.Definition.StateDir == "" {
		return ""
	}
	return filepath.Join(s.cfg.Definition.StateDir, "claude-usage-cache.json")
}
func (s *proxyServer) usageCacheIdentity(p claudeUsageProfile) string {
	return claudeTokenKey(p.CredentialsService + "\x00" + p.CachePath + "\x00" + s.claudeProfileAccountKey(p))
}
func (s *proxyServer) persistClaudeUsageCache() {
	path := s.usageCachePath()
	if path == "" {
		return
	}
	s.claudeUse.persistMu.Lock()
	defer s.claudeUse.persistMu.Unlock()
	s.claudeUse.mu.Lock()
	values := newestClaudeUsageSnapshots(s.claudeUse.snapshots, time.Now(), time.Duration(s.cfg.ClaudeUsage.StaleTTLSeconds)*time.Second)
	retry := s.claudeUse.globalRetryAt
	s.claudeUse.mu.Unlock()
	out := persistedClaudeUsage{Version: 1, RetryAt: retry, Profiles: map[string]persistedClaudeProfile{}}
	for id, v := range values {
		p, ok := s.cfg.ClaudeUsage.AccountProfiles[id]
		if !ok {
			continue
		}
		out.Profiles[id] = persistedClaudeProfile{Identity: s.usageCacheIdentity(p), Snapshot: v}
	}
	data, err := json.Marshal(out)
	if err != nil {
		return
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".claude-usage-*")
	if err != nil {
		return
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	closeErr := file.Close()
	if err == nil && closeErr == nil {
		_ = os.Rename(file.Name(), path)
	}
}
func (s *proxyServer) loadClaudeUsageCache() {
	path := s.usageCachePath()
	if path == "" {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	var saved persistedClaudeUsage
	if json.NewDecoder(io.LimitReader(f, 1<<20)).Decode(&saved) != nil || saved.Version != 1 {
		return
	}
	now := time.Now()
	if saved.RetryAt.After(now) {
		s.claudeUse.globalRetryAt = saved.RetryAt
	}
	for id, entry := range saved.Profiles {
		p, ok := s.cfg.ClaudeUsage.AccountProfiles[id]
		v := entry.Snapshot
		age := now.Sub(v.FetchedAt)
		if !ok || v.Profile != id || entry.Identity != s.usageCacheIdentity(p) || age < 0 || age > time.Duration(s.cfg.ClaudeUsage.StaleTTLSeconds)*time.Second {
			continue
		}
		if v.FiveHour.Utilization < 0 || v.FiveHour.Utilization > 100 || v.SevenDay.Utilization < 0 || v.SevenDay.Utilization > 100 {
			continue
		}
		s.claudeUse.snapshots["restored:"+id] = v
	}
}
