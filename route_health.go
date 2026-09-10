package main

import "net/url"

// Keep route instability separate from account authentication and quota state.
// Upstream is the actual model, not the client alias or original chain model.
func routeHealthKey(runtimeProvider string, candidate modelConfig) string {
	return runtimeProvider + "|model=" + url.QueryEscape(candidate.Upstream)
}
