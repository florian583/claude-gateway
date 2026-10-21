package main

import (
	"encoding/json"
	"net/http"
)

// Model discovery is config-only; opening the picker performs no upstream work.
func (s *proxyServer) menuModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	data := []map[string]string{}
	if s.cfg.Definition != nil {
		for _, id := range sortedKeys(s.cfg.Definition.Client.MenuModels) {
			data = append(data, map[string]string{"id": id, "type": "model", "display_name": s.cfg.Definition.Client.MenuModels[id], "created_at": "2026-01-01T00:00:00Z"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "has_more": false})
}
