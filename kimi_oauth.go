package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const kimiOAuthClientID = "17e5f671-d194-4dfb-9706-5516cb48c098"

// The native CLI owns login. This adapter shares its credential slot and
// proper-lockfile directory protocol; it never copies credentials to a second
// account, creates a device identity, or launches an interactive login.
type kimiOAuthManager struct {
	path   string
	issuer string
	client httpDoer
}

func newKimiOAuthManager(path, issuer string, client httpDoer) *kimiOAuthManager {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &kimiOAuthManager{path: path, issuer: issuer, client: client}
}

func kimiOAuthIssuer(base string) (string, error) {
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "/coding" && u.Path != "/coding/" && u.Path != "/coding/v1") {
		return "", errors.New("Kimi OAuth requires native HTTPS coding endpoint")
	}
	switch u.Host {
	case "api.kimi.ai":
		return "https://auth.kimi.ai", nil
	case "api.kimi.com":
		return "https://auth.kimi.com", nil
	}
	return "", errors.New("Kimi OAuth endpoint is not a native subscription origin")
}

func validateKimiCredentialSlot(path, base string) error {
	issuer, e := kimiOAuthIssuer(base)
	if e != nil {
		return e
	}
	api := "https://api.kimi.ai/coding/v1"
	if issuer == "https://auth.kimi.com" {
		api = "https://api.kimi.com/coding/v1"
	}
	b, _ := json.Marshal(struct {
		OAuthHost string `json:"oauthHost"`
		BaseURL   string `json:"baseUrl"`
	}{issuer, api})
	sum := sha256.Sum256(b)
	expected := fmt.Sprintf("kimi-code-env-%x.json", sum[:8])
	name := filepath.Base(path)
	if filepath.Base(filepath.Dir(path)) != "credentials" || (name != expected && !(issuer == "https://auth.kimi.com" && name == "kimi-code.json")) {
		return errors.New("Kimi OAuth statePath must select native region-scoped CLI credentials slot")
	}
	return nil
}

func (m *kimiOAuthManager) home() string { return filepath.Dir(filepath.Dir(m.path)) }
func (m *kimiOAuthManager) lockPath() string {
	return filepath.Join(m.home(), "oauth", strings.TrimSuffix(filepath.Base(m.path), ".json")+".lock")
}

// Open the checked inode, reject links/non-private files, and bound reads.
func readKimiPrivate(path string) ([]byte, os.FileInfo, error) {
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, nil, errors.New("Kimi credentials require a private regular 0600 file")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, nil, errors.New("Kimi credentials unavailable")
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(info, opened) {
		return nil, nil, errors.New("Kimi credentials changed while opening")
	}
	b, e := io.ReadAll(io.LimitReader(f, 65537))
	if e != nil || len(b) > 65536 {
		return nil, nil, errors.New("Kimi credentials unreadable or too large")
	}
	return b, opened, nil
}

type kimiToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

func (m *kimiOAuthManager) read() (kimiToken, []byte, os.FileInfo, error) {
	b, info, e := readKimiPrivate(m.path)
	if e != nil {
		return kimiToken{}, nil, nil, e
	}
	var t kimiToken
	if json.Unmarshal(b, &t) != nil || t.AccessToken == "" || t.RefreshToken == "" || t.Scope != "kimi-code" || !strings.EqualFold(t.TokenType, "Bearer") || t.ExpiresAt <= 0 || t.ExpiresIn <= 0 || strings.ContainsAny(t.AccessToken+t.RefreshToken, "\r\n") {
		return t, nil, nil, errors.New("Kimi subscription login missing or invalid; use native CLI login")
	}
	return t, b, info, nil
}

func kimiNeedsRefresh(t kimiToken) bool {
	threshold := t.ExpiresIn / 2
	if threshold < 300 {
		threshold = 300
	}
	return t.ExpiresAt-time.Now().Unix() < threshold
}

func (m *kimiOAuthManager) accessToken(ctx context.Context) (string, error) { return m.token(ctx, "") }
func (m *kimiOAuthManager) refreshAfterUnauthorized(ctx context.Context, failed string) (string, error) {
	return m.token(ctx, failed)
}

func (m *kimiOAuthManager) token(ctx context.Context, failed string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	before, _, _, err := m.read()
	if err != nil {
		return "", err
	}
	if !kimiNeedsRefresh(before) && (failed == "" || before.AccessToken != failed) {
		return before.AccessToken, nil
	}
	type result struct {
		token string
		err   error
	}
	done := make(chan result, 1)
	go func() { token, err := m.tokenLocked(ctx, failed); done <- result{token, err} }()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case r := <-done:
		return r.token, r.err
	}
}

func (m *kimiOAuthManager) tokenLocked(ctx context.Context, failed string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	before, _, _, e := m.read()
	if e != nil {
		return "", e
	}
	if !kimiNeedsRefresh(before) && (failed == "" || before.AccessToken != failed) {
		return before.AccessToken, nil
	}
	release, e := m.lock(ctx)
	if e != nil {
		return "", e
	}
	defer release()
	lockInfo, e := os.Lstat(m.lockPath())
	if e != nil {
		return "", errors.New("Kimi OAuth lock lost")
	}
	current, raw, info, e := m.read()
	if e != nil {
		return "", e
	}
	// Another process may have rotated while this request waited for the lock.
	if current != before || (!kimiNeedsRefresh(current) && failed == "") {
		return current.AccessToken, nil
	}
	values := url.Values{"client_id": {kimiOAuthClientID}, "grant_type": {"refresh_token"}, "refresh_token": {current.RefreshToken}}
	// Once a refresh grant is sent, finish reading/persisting its rotation even
	// if the caller leaves. Dropping that response can invalidate CLI login.
	// The caller still returns promptly via token's cancellation select.
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(refreshCtx, http.MethodPost, m.issuer+"/api/oauth/token", strings.NewReader(values.Encode()))
	if e != nil {
		return "", errors.New("Kimi refresh request invalid")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	// Keep the existing device; do not impersonate or rewrite the native CLI.
	if e = m.deviceHeaders(req.Header); e != nil {
		return "", e
	}
	req.Header.Set("User-Agent", "claude-gateway/1")
	resp, e := m.client.Do(req)
	if e != nil {
		return "", errors.New("Kimi refresh unavailable")
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if e != nil || len(body) > 65536 {
		return "", errors.New("Kimi refresh response invalid")
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Kimi refresh rejected (HTTP %d); credentials retained", resp.StatusCode)
	}
	var updated kimiToken
	if json.Unmarshal(body, &updated) != nil || updated.AccessToken == "" || updated.RefreshToken == "" || updated.ExpiresIn <= 0 || updated.ExpiresIn > 366*24*3600 || updated.TokenType != "Bearer" || updated.Scope != "kimi-code" || strings.ContainsAny(updated.AccessToken+updated.RefreshToken, "\r\n") {
		return "", errors.New("Kimi refresh returned invalid subscription credentials")
	}
	updated.ExpiresAt = time.Now().Unix() + updated.ExpiresIn
	var merged map[string]json.RawMessage
	if json.Unmarshal(raw, &merged) != nil {
		return "", errors.New("Kimi credentials invalid")
	}
	updatedJSON, _ := json.Marshal(updated)
	var replacements map[string]json.RawMessage
	_ = json.Unmarshal(updatedJSON, &replacements)
	for key, value := range replacements {
		merged[key] = value
	}
	payload, e := json.MarshalIndent(merged, "", "  ")
	if e != nil {
		return "", errors.New("Kimi credential serialization failed")
	}
	// Never overwrite a concurrent login/logout, including one not honoring lock.
	latest, nowInfo, e := readKimiPrivate(m.path)
	if e != nil || !os.SameFile(info, nowInfo) || string(latest) != string(raw) {
		return "", errors.New("Kimi credentials changed during refresh; not overwritten")
	}
	currentLock, e := os.Lstat(m.lockPath())
	if e != nil || !os.SameFile(lockInfo, currentLock) {
		return "", errors.New("Kimi OAuth lock ownership lost; credentials not overwritten")
	}
	f, e := os.CreateTemp(filepath.Dir(m.path), ".kimi-refresh-")
	if e != nil {
		return "", errors.New("Kimi credential persistence unavailable")
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(append(payload, '\n')); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil || closeErr != nil {
		return "", errors.New("Kimi credential persistence failed")
	}
	if e = os.Rename(f.Name(), m.path); e != nil {
		return "", errors.New("Kimi credential replace failed")
	}
	log.Print("Kimi subscription OAuth refreshed using native CLI credential slot")
	return updated.AccessToken, nil
}

func (m *kimiOAuthManager) deviceHeaders(h http.Header) error {
	b, _, e := readKimiPrivate(filepath.Join(m.home(), "device_id"))
	id := strings.TrimSpace(string(b))
	if e != nil || id == "" || len(id) > 256 || strings.ContainsAny(id, "\r\n") {
		return errors.New("Kimi native device identity unavailable")
	}
	h.Set("X-Msh-Device-Id", id)
	return nil
}

// proper-lockfile uses an empty .lock directory, heartbeated mtime, and a 5s
// stale threshold. Share that protocol, not an unrelated flock or file lock.
func (m *kimiOAuthManager) lock(ctx context.Context) (func(), error) {
	path := m.lockPath()
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, errors.New("Kimi OAuth lock unavailable")
	}
	for {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		if e := os.Mkdir(path, 0700); e == nil {
			owned, e := os.Lstat(path)
			if e != nil {
				return nil, errors.New("Kimi OAuth lock lost")
			}
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				tick := time.NewTicker(time.Second)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tick.C:
						info, e := os.Lstat(path)
						if e != nil || !os.SameFile(info, owned) {
							return
						}
						now := time.Now()
						_ = os.Chtimes(path, now, now)
					}
				}
			}()
			return func() {
				close(stop)
				<-done
				info, e := os.Lstat(path)
				if e == nil && os.SameFile(info, owned) {
					_ = os.Remove(path)
				}
			}, nil
		} else if !os.IsExist(e) {
			return nil, errors.New("Kimi OAuth lock unavailable")
		}
		info, e := os.Lstat(path)
		if e == nil && (!info.IsDir() || time.Since(info.ModTime()) > 5*time.Second) {
			// Stat-then-remove/rename cannot atomically distinguish a stale lock
			// from a concurrently replaced live CLI lock. Leave foreign recovery
			// to the native owner instead of racing refresh-token rotation.
			return nil, errors.New("Kimi OAuth foreign or stale lock requires native CLI recovery")
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
