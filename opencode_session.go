package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
)

type openCodeSessionKey struct{}

func validConversationID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, ch := range value {
		if ch < 33 || ch > 126 {
			return false
		}
	}
	return true
}

// Capture identity before adapter rewrites. Explicit IDs survive compaction,
// model switches and retries. Raw user/account metadata never leaves the proxy.
func withOpenCodeSession(r *http.Request, payload map[string]any) *http.Request {
	if id := r.Header.Get("x-opencode-session"); validConversationID(id) {
		return r.WithContext(context.WithValue(r.Context(), openCodeSessionKey{}, id))
	}
	metadata, _ := payload["metadata"].(map[string]any)
	id, _ := metadata["session_id"].(string)
	user, _ := metadata["user_id"].(string)
	if id == "" {
		var identity struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal([]byte(user), &identity) == nil {
			id = identity.SessionID
		}
	}
	if id == "" {
		if _, session, ok := strings.Cut(user, "_session_"); ok {
			id = session
		}
	}
	seed := map[string]any{"session": id}
	if id == "" {
		// Best effort for clients without session metadata. Callers must send an
		// explicit header for distinct identical prompts or compacted histories.
		seed = map[string]any{"user": user}
		if messages, ok := payload["messages"].([]any); ok && len(messages) > 0 {
			seed["firstMessage"] = messages[0]
		}
	}
	encoded, _ := json.Marshal(seed)
	digest := sha256.Sum256(encoded)
	session := "gateway-" + hex.EncodeToString(digest[:16])
	return r.WithContext(context.WithValue(r.Context(), openCodeSessionKey{}, session))
}
