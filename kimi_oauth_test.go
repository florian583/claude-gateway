package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKimiOAuthConfigOriginAndSlot(t *testing.T) {
	seed := []byte(`{"oauthHost":"https://auth.kimi.ai","baseUrl":"https://api.kimi.ai/coding/v1"}`)
	digest := sha256.Sum256(seed)
	path := filepath.Join(t.TempDir(), "credentials", fmt.Sprintf("kimi-code-env-%x.json", digest[:8]))
	f := testConfig("https://api.kimi.ai/coding/", "anthropic")
	p := f.Providers["endpoint"]
	p.Variant, p.Billing = "kimi-subscription", "subscription"
	p.Auth = authDefinition{Type: "kimi-code-oauth", StatePath: path}
	f.Providers["endpoint"] = p
	c := compileFixture(t, f)
	if c.Providers["endpoint"].AuthMode != "kimi-code-oauth" {
		t.Fatal("OAuth config not compiled")
	}
	for _, base := range []string{"https://api.moonshot.ai/anthropic", "https://api.kimi.ai.evil.test/coding/", "https://api.kimi.com/coding/", "http://api.kimi.ai/coding/", "https://api.kimi.ai:443/coding/"} {
		if err := validateKimiCredentialSlot(path, base); err == nil {
			t.Errorf("unsafe or mismatched origin accepted: %s", base)
		}
	}
	comSeed := []byte(`{"oauthHost":"https://auth.kimi.com","baseUrl":"https://api.kimi.com/coding/v1"}`)
	comDigest := sha256.Sum256(comSeed)
	for _, name := range []string{fmt.Sprintf("kimi-code-env-%x.json", comDigest[:8]), "kimi-code.json"} {
		if err := validateKimiCredentialSlot(filepath.Join(t.TempDir(), "credentials", name), "https://api.kimi.com/coding/"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKimiOAuthDoesNotStealStaleForeignLock(t *testing.T) {
	m, _ := kimiFixture(t, -1)
	if err := os.MkdirAll(m.lockPath(), 0700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute)
	_ = os.Chtimes(m.lockPath(), old, old)
	if _, err := m.accessToken(context.Background()); err == nil {
		t.Fatal("foreign stale lock accepted")
	}
	if _, err := os.Stat(m.lockPath()); err != nil {
		t.Fatal("foreign lock stolen")
	}
}

func TestKimiOAuthCanceledCallerStillPersistsInFlightRotation(t *testing.T) {
	m, path := kimiFixture(t, -1)
	started, finish := make(chan struct{}), make(chan struct{})
	m.client = testHTTPDoer(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-finish
		if r.Context().Err() != nil {
			t.Error("caller canceled rotating refresh grant")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"access_token":"synthetic-detached-access","refresh_token":"synthetic-detached-refresh","expires_in":1800,"scope":"kimi-code","token_type":"Bearer"}`))}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.accessToken(ctx); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Error("caller cancellation not returned")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled caller blocked")
	}
	close(finish)
	deadline := time.After(time.Second)
	for {
		b, _ := os.ReadFile(path)
		_, lockErr := os.Stat(m.lockPath())
		if strings.Contains(string(b), "synthetic-detached-refresh") && os.IsNotExist(lockErr) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("rotation not safely persisted")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestKimiOAuthUnauthorizedRefreshNoCredentialLeaks(t *testing.T) {
	m, _ := kimiFixture(t, 1800)
	var calls atomic.Int32
	m.client = testHTTPDoer(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader(`{"error_description":"synthetic-old-refresh"}`))}, nil
	})
	_, err := m.refreshAfterUnauthorized(context.Background(), "synthetic-old-access")
	if err == nil || strings.Contains(err.Error(), "synthetic-old-refresh") {
		t.Fatal("refresh error leaked provider body")
	}
	if calls.Load() != 1 {
		t.Fatal("unbounded refresh retries")
	}
}

func TestKimiOAuthHeadersPreserveCallerIdentity(t *testing.T) {
	m, _ := kimiFixture(t, 1800)
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{"kimi": {AuthMode: "kimi-code-oauth", AuthPrefix: "Bearer "}}}, kimiOAuth: map[string]*kimiOAuthManager{"kimi": m}}
	h := http.Header{"User-Agent": {"synthetic-real-client/1"}, "X-Api-Key": {"synthetic-wrong-provider"}}
	if err := s.applyProviderHeadersContext(context.Background(), "kimi", h); err != nil {
		t.Fatal(err)
	}
	if h.Get("User-Agent") != "synthetic-real-client/1" || h.Get("X-Msh-Device-Id") != "synthetic-device-id" {
		t.Fatal("caller/device identity changed")
	}
	if h.Get("x-api-key") != "" || h.Get("Authorization") != "Bearer synthetic-old-access" {
		t.Fatal("wrong auth isolation")
	}
}

func TestKimiOAuthRetryOnceBeforeResponseVisible(t *testing.T) {
	for _, retryStatus := range []int{200, 401} {
		t.Run(strconv.Itoa(retryStatus), func(t *testing.T) {
			manager, _ := kimiFixture(t, 1800)
			var refreshes, attempts atomic.Int32
			manager.client = testHTTPDoer(func(*http.Request) (*http.Response, error) {
				refreshes.Add(1)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"access_token":"synthetic-retry-access","refresh_token":"synthetic-retry-refresh","expires_in":1800,"scope":"kimi-code","token_type":"Bearer"}`))}, nil
			})
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				if string(b) != "synthetic-request" {
					t.Error("request body not replayed exactly")
				}
				if r.Header.Get("User-Agent") != "synthetic-client/1" {
					t.Error("caller identity rewritten")
				}
				if attempts.Add(1) == 1 {
					w.WriteHeader(401)
					return
				}
				if r.Header.Get("Authorization") != "Bearer synthetic-retry-access" {
					t.Error("stale retry token")
				}
				w.WriteHeader(retryStatus)
			}))
			defer endpoint.Close()
			u, _ := url.Parse(endpoint.URL)
			s := &proxyServer{cfg: config{Providers: map[string]providerConfig{"kimi": {AuthMode: "kimi-code-oauth", AuthPrefix: "Bearer "}}}, providers: map[string]*url.URL{"kimi": u}, clients: map[string]*http.Client{"kimi": endpoint.Client()}, kimiOAuth: map[string]*kimiOAuthManager{"kimi": manager}}
			req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1/v1/messages", nil)
			req.Header.Set("User-Agent", "synthetic-client/1")
			resp, err := s.doUpstream(context.Background(), req, []byte("synthetic-request"), modelConfig{Provider: "kimi"})
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if attempts.Load() != 2 || refreshes.Load() != 1 || resp.StatusCode != retryStatus {
				t.Fatal("retry not bounded to one")
			}
		})
	}
}

func TestKimiOAuthSuccessfulStreamNeverRefreshedOrReplayed(t *testing.T) {
	manager, _ := kimiFixture(t, 1800)
	manager.client = testHTTPDoer(func(*http.Request) (*http.Response, error) {
		t.Fatal("successful stream must not refresh")
		return nil, nil
	})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: synthetic-partial\n\n")
	}))
	defer endpoint.Close()
	u, _ := url.Parse(endpoint.URL)
	s := &proxyServer{cfg: config{Providers: map[string]providerConfig{"kimi": {AuthMode: "kimi-code-oauth", AuthPrefix: "Bearer "}}}, providers: map[string]*url.URL{"kimi": u}, clients: map[string]*http.Client{"kimi": endpoint.Client()}, kimiOAuth: map[string]*kimiOAuthManager{"kimi": manager}}
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1/v1/messages", nil)
	resp, err := s.doUpstream(context.Background(), req, []byte("synthetic"), modelConfig{Provider: "kimi"})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "synthetic-partial") {
		t.Fatal("stream changed")
	}
}

func kimiFixture(t *testing.T, seconds int64) (*kimiOAuthManager, string) {
	t.Helper()
	home := t.TempDir()
	path := filepath.Join(home, "credentials", "test-subscription.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"access_token":"synthetic-old-access","refresh_token":"synthetic-old-refresh","expires_at":` + fmtInt(time.Now().Unix()+seconds) + `,"expires_in":1800,"scope":"kimi-code","token_type":"Bearer","extra":"preserve","large_unknown_number":9007199254740993}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "device_id"), []byte("synthetic-device-id"), 0600); err != nil {
		t.Fatal(err)
	}
	return newKimiOAuthManager(path, "https://auth.kimi.ai", nil), path
}

func fmtInt(v int64) string { return strconv.FormatInt(v, 10) }

func TestKimiOAuthFreshTokenNoNetwork(t *testing.T) {
	m, _ := kimiFixture(t, 1800)
	m.client = testHTTPDoer(func(*http.Request) (*http.Response, error) { t.Fatal("fresh token must not refresh"); return nil, nil })
	token, err := m.accessToken(context.Background())
	if err != nil || token != "synthetic-old-access" {
		t.Fatalf("fresh token: %v", err)
	}
}

func TestKimiOAuthRefreshCoalescesAndPreservesStorage(t *testing.T) {
	m, path := kimiFixture(t, -1)
	var calls atomic.Int32
	m.client = testHTTPDoer(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.URL.String() != "https://auth.kimi.ai/api/oauth/token" {
			t.Error("wrong OAuth origin")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("refresh_token") != "synthetic-old-refresh" || r.Form.Get("grant_type") != "refresh_token" {
			t.Error("wrong refresh grant")
		}
		if r.Header.Get("X-Msh-Device-Id") != "synthetic-device-id" {
			t.Error("device identity changed")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"access_token":"synthetic-new-access","refresh_token":"synthetic-new-refresh","expires_in":1800,"scope":"kimi-code","token_type":"Bearer"}`))}, nil
	})
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := m.accessToken(context.Background())
			if err != nil || token != "synthetic-new-access" {
				t.Errorf("refresh failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("refresh calls=%d", calls.Load())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	_ = json.Unmarshal(data, &state)
	if state["extra"] != "preserve" || state["refresh_token"] != "synthetic-new-refresh" {
		t.Fatal("rotation lost state")
	}
	if !strings.Contains(string(data), "9007199254740993") {
		t.Fatal("unknown numeric field lost precision")
	}
	stat, _ := os.Stat(path)
	if stat.Mode().Perm() != 0600 {
		t.Fatal("credential permissions changed")
	}
}

func TestKimiOAuthNetworkErrorDoesNotRevoke(t *testing.T) {
	m, path := kimiFixture(t, -1)
	before, _ := os.ReadFile(path)
	m.client = testHTTPDoer(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{"error":"unavailable"}`))}, nil
	})
	if _, err := m.accessToken(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("transient failure changed credentials")
	}
}

func TestKimiOAuthForeignLockCancellation(t *testing.T) {
	m, _ := kimiFixture(t, -1)
	if err := os.MkdirAll(m.lockPath(), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := m.accessToken(ctx); err == nil {
		t.Fatal("must honor cancellation")
	}
	if _, err := os.Stat(m.lockPath()); err != nil {
		t.Fatal("removed another process lock")
	}
}

func TestKimiOAuthRejectsUnsafeCredentialFile(t *testing.T) {
	m, path := kimiFixture(t, 1800)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.accessToken(context.Background()); err == nil {
		t.Fatal("public credentials accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := path + "-link"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	m.path = link
	if _, err := m.accessToken(context.Background()); err == nil {
		t.Fatal("symlink credentials accepted")
	}
}
