package main

import (
	"net/http"
	"strings"
)

// Explicit bounded labels, never prompts, user_id blobs, paths or credentials.
// Attribution is observation only; these fields never affect routing.
type usageAttribution struct {
	Session       string `json:"session,omitempty"`
	Workload      string `json:"workload,omitempty"`
	Lane          string `json:"lane,omitempty"`
	Run           string `json:"run,omitempty"`
	Channel       string `json:"channel,omitempty"`
	Origin        string `json:"origin,omitempty"`
	Host          string `json:"host,omitempty"`
	ParentSession string `json:"parentSession,omitempty"`
	Worker        string `json:"worker,omitempty"`
	PR            string `json:"pr,omitempty"`
	Head          string `json:"head,omitempty"`
	Stage         string `json:"stage,omitempty"`
}

func usageLabel(value string) string {
	if len(value) == 0 || len(value) > 96 {
		return ""
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.:", c)) {
			return ""
		}
	}
	return value
}

func requestUsageAttribution(r *http.Request) usageAttribution {
	if r == nil {
		return usageAttribution{}
	}
	a := usageAttribution{
		Session:       usageLabel(sessionIDFromContext(r.Context())),
		Workload:      usageLabel(r.Header.Get("X-Gateway-Workload")),
		Lane:          usageLabel(r.Header.Get("X-Gateway-Lane")),
		Run:           usageLabel(r.Header.Get("X-Gateway-Run")),
		Channel:       usageLabel(r.Header.Get("X-Gateway-Channel")),
		Origin:        usageLabel(r.Header.Get("X-Gateway-Origin")),
		Host:          usageLabel(r.Header.Get("X-Gateway-Host")),
		ParentSession: usageLabel(r.Header.Get("X-Gateway-Parent-Session")),
		Worker:        usageLabel(r.Header.Get("X-Gateway-Worker")),
		PR:            usageLabel(r.Header.Get("X-Gateway-PR")),
		Head:          usageLabel(r.Header.Get("X-Gateway-Head")),
		Stage:         usageLabel(r.Header.Get("X-Gateway-Stage")),
	}
	if a.Workload == "" && strings.Contains(r.UserAgent(), "workload/cron") {
		a.Workload = "cron-unclassified"
	}
	if a.Workload == "" {
		a.Workload = "unclassified"
	}
	return a
}

// Never leak local lineage labels or future gateway-only headers upstream.
func stripUsageHeaders(header http.Header) {
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-gateway-") {
			header.Del(key)
		}
	}
}
