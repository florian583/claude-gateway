package main

// A soft preference, never an admission limit. The final route is still tried
// at any load. Concurrent arrivals may exceed the threshold by design.
func (s *proxyServer) routeActive(provider, upstream string) int {
	s.passiveConcurrency.mu.Lock()
	defer s.passiveConcurrency.mu.Unlock()
	n := 0
	for scope, count := range s.passiveConcurrency.active {
		if scope.Provider == provider && scope.Upstream == upstream {
			n += count
		}
	}
	return n
}

func (e paidFallbackEvidence) loadAllowed(m modelConfig) bool {
	if !m.AllowPaidFallback || m.SoftConcurrency <= 0 || len(e) == 0 {
		return false
	}
	for _, reason := range e {
		if reason != "load_spill" && !containsString(m.PaidFallbackOn, reason) {
			return false
		}
	}
	return true
}
