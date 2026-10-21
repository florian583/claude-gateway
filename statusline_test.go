package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPayloadSessionID(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{"direct session_id", map[string]any{"metadata": map[string]any{"session_id": "abc-123"}}, "abc-123"},
		{"json user_id", map[string]any{"metadata": map[string]any{"user_id": `{"session_id":"s-9"}`}}, "s-9"},
		{"_session_ suffix", map[string]any{"metadata": map[string]any{"user_id": "user_x_account__session_u-42"}}, "u-42"},
		{"no metadata", map[string]any{"messages": []any{}}, ""},
		{"unparseable user_id", map[string]any{"metadata": map[string]any{"user_id": "plain-user"}}, ""},
	}
	for _, tc := range cases {
		if got := payloadSessionID(tc.payload); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestLastRouteEndpoint(t *testing.T) {
	s := &proxyServer{}
	s.noteLastRoute(lastRouteInfo{SessionID: "s1", Requested: "fable", Provider: "anthropic", Upstream: "claude-fable-5-1", Status: 200, Success: true, TaskTier: "fast", TaskShadow: true, At: time.Now().Add(-time.Minute)})
	s.noteLastRoute(lastRouteInfo{SessionID: "s2", Requested: "sonnet", Provider: "ollama", Upstream: "glm-5.3-flash:cloud", Status: 200, Success: true, At: time.Now()})
	s.noteLastRoute(lastRouteInfo{Requested: "orphan", Provider: "x", Status: 200, At: time.Now()}) // no session: ignored

	req := httptest.NewRequest(http.MethodGet, "/last-route?session=s1", nil)
	rec := httptest.NewRecorder()
	s.lastRoute(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var info lastRouteInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Provider != "anthropic" || info.Upstream != "claude-fable-5-1" || info.TaskTier != "fast" || !info.TaskShadow {
		t.Fatalf("unexpected session payload: %+v", info)
	}

	rec = httptest.NewRecorder()
	s.lastRoute(rec, httptest.NewRequest(http.MethodGet, "/last-route?session=unknown", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown session must 404, got %d", rec.Code)
	}

	// No session param: most recent entry across sessions (s2).
	rec = httptest.NewRecorder()
	s.lastRoute(rec, httptest.NewRequest(http.MethodGet, "/last-route", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.SessionID != "s2" || info.Upstream != "glm-5.3-flash:cloud" {
		t.Fatalf("expected latest entry s2, got %+v", info)
	}

	rec = httptest.NewRecorder()
	s.lastRoute(rec, httptest.NewRequest(http.MethodPost, "/last-route", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("non-GET must 404, got %d", rec.Code)
	}
}

func TestLastRouteSessionCap(t *testing.T) {
	s := &proxyServer{}
	for i := 0; i < lastRouteMaxSessions+5; i++ {
		s.noteLastRoute(lastRouteInfo{SessionID: string(rune('a'+i%26)) + string(rune('a'+i/26)), At: time.Now()})
	}
	s.lastRoutesMu.Lock()
	defer s.lastRoutesMu.Unlock()
	if len(s.lastRoutes) > lastRouteMaxSessions {
		t.Fatalf("session map exceeded cap: %d", len(s.lastRoutes))
	}
}

func TestSessionIDFromContext(t *testing.T) {
	if got := sessionIDFromContext(context.Background()); got != "" {
		t.Fatalf("empty context must yield empty session, got %q", got)
	}
	ctx := context.WithValue(context.Background(), sessionRouteContextKey{}, "sess-1")
	if got := sessionIDFromContext(ctx); got != "sess-1" {
		t.Fatalf("got %q", got)
	}
}
