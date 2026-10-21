package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// sessionRouteContextKey carries the extracted client session id through the
// request so terminal records and the served-leg trace can attribute the
// outcome to a Claude Code session for /last-route.
type sessionRouteContextKey struct{}

// lastRouteInfo is the most recent route outcome for one client session,
// exposed read-only for the Claude Code statusline. It carries routing
// metadata only — never prompt content or credentials.
type lastRouteInfo struct {
	SessionID  string    `json:"session"`
	Requested  string    `json:"requested"`
	Provider   string    `json:"provider"`
	Upstream   string    `json:"upstream"`
	Status     int       `json:"status"`
	Success    bool      `json:"success"`
	Reason     string    `json:"reason,omitempty"`
	LatencyMS  int64     `json:"latencyMS"`
	TokensPS   float64   `json:"tokensPerSecond,omitempty"`
	TaskTier   string    `json:"taskTier,omitempty"`
	TaskShadow bool      `json:"taskShadow,omitempty"`
	TaskSample string    `json:"taskSample,omitempty"`
	At         time.Time `json:"at"`
}

const lastRouteMaxSessions = 256

func sessionIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(sessionRouteContextKey{}).(string)
	return v
}

// noteLastRoute records the latest route outcome for a session. Sessions are
// bounded; the oldest entry is evicted past the cap.
func (s *proxyServer) noteLastRoute(info lastRouteInfo) {
	if info.SessionID == "" {
		return
	}
	s.lastRoutesMu.Lock()
	defer s.lastRoutesMu.Unlock()
	if s.lastRoutes == nil {
		s.lastRoutes = map[string]lastRouteInfo{}
	}
	s.lastRoutes[info.SessionID] = info
	if len(s.lastRoutes) > lastRouteMaxSessions {
		oldestKey, oldestAt := "", time.Time{}
		for key, entry := range s.lastRoutes {
			if oldestKey == "" || entry.At.Before(oldestAt) {
				oldestKey, oldestAt = key, entry.At
			}
		}
		delete(s.lastRoutes, oldestKey)
	}
}

// lastRoute serves GET /last-route[?session=id]. With a session parameter it
// returns that session's latest outcome; without one it returns the most
// recent outcome across sessions. Observation-only: it never touches
// credentials, providers, or routing state.
func (s *proxyServer) lastRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	session := r.URL.Query().Get("session")
	s.lastRoutesMu.Lock()
	info, found := lastRouteInfo{}, false
	if session != "" {
		info, found = s.lastRoutes[session]
	} else {
		for _, entry := range s.lastRoutes {
			if !found || entry.At.After(info.At) {
				info, found = entry, true
			}
		}
	}
	s.lastRoutesMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if !found {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"found": false})
		return
	}
	_ = json.NewEncoder(w).Encode(info)
}
