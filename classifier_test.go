package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const evalResponseBody = `{
  "answers": {
    "tier": {"type": "choice", "choice": "fast", "probabilities": {"fast": 0.9, "balanced": 0.08, "deep": 0.02}},
    "effort": {"type": "score", "score": 0.4, "probabilities": {"0": 0.1, "1": 0.7, "2": 0.15, "3": 0.05}},
    "risky": {"type": "boolean", "probability": 0.05}
  },
  "providerMetadata": {"typesafe": {"confidence": {"tier": 0.88, "effort": 0.71}}}
}`

func evalServer(t *testing.T, body string, status int, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}
		if r.URL.Path != "/evaluation-model" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		for _, h := range []string{"ai-gateway-auth-method", "ai-model-id", "ai-gateway-protocol-version", "ai-evaluation-model-specification-version"} {
			if r.Header.Get(h) == "" {
				t.Errorf("missing protocol header %s", h)
			}
		}
		if !strings.HasPrefix(r.Header.Get("authorization"), "Bearer test-") {
			t.Errorf("missing bearer auth")
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func classifierTestServer(t *testing.T, srv *httptest.Server, aliasClasses map[string]string) *proxyServer {
	t.Helper()
	t.Setenv("TEST_EVAL_KEY", "test-eval-key")
	return &proxyServer{cfg: config{
		Definition: &fileConfig{},
		Providers: map[string]providerConfig{
			"eval": {BaseURL: srv.URL, AuthTokenEnv: "TEST_EVAL_KEY", Billing: "metered"},
		},
		Classifier: &classifierConfig{
			Enabled: true, Provider: "eval", Model: "typesafe-ai/jev",
			Timeout: 800 * time.Millisecond, CacheTTL: 10 * time.Minute, DedupeTTL: time.Hour, FailureBackoff: time.Minute,
			MinUpgradeConfidence: 0.3, MinDowngradeConfidence: 0.6, RiskyThreshold: 0.7,
			TierMatchBonus: 25, RiskyProviders: map[string]bool{"anthropic": true}, RiskyBonus: 30,
			MaxPromptChars: 3000, AliasClasses: aliasClasses,
		},
	}}
}

func userPayload(text string) map[string]any {
	return map[string]any{"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": text}}}}}
}

func toolTurnPayload() map[string]any {
	return map[string]any{"messages": []any{
		map[string]any{"role": "user", "content": "do the thing"},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "t1", "name": "Bash"}}},
		map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}}},
	}}
}

func TestLastUserText(t *testing.T) {
	if got := lastUserText(userPayload("rename foo"), 0); got != "rename foo" {
		t.Fatalf("expected user text, got %q", got)
	}
	if got := lastUserText(map[string]any{"messages": []any{map[string]any{"role": "user", "content": "plain string"}}}, 0); got != "plain string" {
		t.Fatalf("expected string content, got %q", got)
	}
	if got := lastUserText(toolTurnPayload(), 0); got != "" {
		t.Fatalf("tool turn should yield no text, got %q", got)
	}
	if got := lastUserText(userPayload("abcdefghij"), 4); got != "abcd" {
		t.Fatalf("expected truncation, got %q", got)
	}
	if got := lastUserText(map[string]any{"messages": []any{map[string]any{"role": "assistant", "content": "hi"}}}, 0); got != "" {
		t.Fatalf("assistant-last should yield no text, got %q", got)
	}
}

func TestParseEvaluationResponse(t *testing.T) {
	d, err := parseEvaluationResponse([]byte(evalResponseBody))
	if err != nil {
		t.Fatal(err)
	}
	if d.Tier != "fast" || d.TierConfidence != 0.88 {
		t.Fatalf("unexpected tier parse: %+v", d)
	}
	if d.Effort != 0.4 || d.EffortConfidence != 0.71 {
		t.Fatalf("unexpected effort parse: %+v", d)
	}
	if d.Risky != 0.05 {
		t.Fatalf("unexpected risky parse: %+v", d)
	}
	// probabilities fallback when no calibrated confidence
	body := strings.Replace(evalResponseBody, `"providerMetadata": {"typesafe": {"confidence": {"tier": 0.88, "effort": 0.71}}}`, `"providerMetadata": {}`, 1)
	d, err = parseEvaluationResponse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if d.TierConfidence != 0.9 {
		t.Fatalf("expected max-probability confidence 0.9, got %v", d.TierConfidence)
	}
}

func policyServer(classes map[string]string) *proxyServer {
	return &proxyServer{cfg: config{Classifier: &classifierConfig{
		Enabled: true, MinUpgradeConfidence: 0.3, MinDowngradeConfidence: 0.6, RiskyThreshold: 0.7,
		AliasClasses: classes,
	}}}
}

func TestPolicyHint(t *testing.T) {
	s := policyServer(map[string]string{"fable": "deep", "haiku": "fast", "sonnet": "balanced"})
	cases := []struct {
		name      string
		alias     string
		decision  taskDecision
		wantTier  string
		wantRisky bool
		wantApply bool
	}{
		{"downgrade allowed above bar", "fable", taskDecision{Tier: "fast", TierConfidence: 0.9}, "fast", false, true},
		{"downgrade refused below bar", "fable", taskDecision{Tier: "fast", TierConfidence: 0.4}, "", false, false},
		{"upgrade allowed above bar", "haiku", taskDecision{Tier: "deep", TierConfidence: 0.5}, "deep", false, true},
		{"upgrade refused below bar", "haiku", taskDecision{Tier: "deep", TierConfidence: 0.2}, "", false, false},
		{"same class applies", "sonnet", taskDecision{Tier: "balanced", TierConfidence: 0.5}, "balanced", false, true},
		{"unknown alias needs upgrade bar", "mystery", taskDecision{Tier: "fast", TierConfidence: 0.5}, "fast", false, true},
		{"unknown alias below bar dropped", "mystery", taskDecision{Tier: "fast", TierConfidence: 0.1}, "", false, false},
		{"risky forces deep past bars", "haiku", taskDecision{Tier: "fast", TierConfidence: 0.1, Risky: 0.9}, "deep", true, true},
		{"risky keeps deep hint", "fable", taskDecision{Tier: "deep", TierConfidence: 0.9, Risky: 0.8}, "deep", true, true},
	}
	for _, tc := range cases {
		hint, outcome := s.policyHint(tc.decision, tc.alias)
		if hint.Tier != tc.wantTier || hint.Risky != tc.wantRisky || hint.Decided != tc.wantApply {
			t.Errorf("%s: got %+v (%s), want tier=%q risky=%v decided=%v", tc.name, hint, outcome, tc.wantTier, tc.wantRisky, tc.wantApply)
		}
	}
}

func waitForCalls(t *testing.T, calls *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("eval calls never reached %d", want)
}

// waitForHint polls the classifier cache until a decided hint lands for the
// sticky key — classification is asynchronous off the request path.
func waitForHint(t *testing.T, s *proxyServer, stickyKey string) taskRouteHint {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.classifier.mu.Lock()
		cached, ok := s.classifier.cache[stickyKey]
		s.classifier.mu.Unlock()
		if ok && cached.hint.Decided {
			return cached.hint
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no cached hint arrived for %q", stickyKey)
	return taskRouteHint{}
}

func TestTaskRouteHintClassifiesAndCaches(t *testing.T) {
	var calls atomic.Int64
	srv := evalServer(t, evalResponseBody, 200, &calls)
	defer srv.Close()
	s := classifierTestServer(t, srv, map[string]string{"fable": "deep"})
	selected := modelConfig{Requested: "fable"}

	// first turn: launches async classification, proceeds unhinted
	hint := s.taskRouteHintForRequest(context.Background(), selected, userPayload("rename the local variable"), "conv-1")
	if hint.Decided {
		t.Fatalf("first turn must not block on classification, got %+v", hint)
	}
	if got := waitForHint(t, s, "conv-1"); got.Tier != "fast" {
		t.Fatalf("expected cached fast hint, got %+v", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 classification call, got %d", calls.Load())
	}
	// same text again: cache hit, no new call
	hint = s.taskRouteHintForRequest(context.Background(), selected, userPayload("rename the local variable"), "conv-1")
	if hint.Tier != "fast" || calls.Load() != 1 {
		t.Fatalf("expected cached fast hint without a call: %+v calls=%d", hint, calls.Load())
	}
	// tool turn: no fresh text -> cached hint, no call
	hint = s.taskRouteHintForRequest(context.Background(), selected, toolTurnPayload(), "conv-1")
	if hint.Tier != "fast" || calls.Load() != 1 {
		t.Fatalf("tool turn should reuse cached hint without a call: %+v calls=%d", hint, calls.Load())
	}
	// fresh user text: new classification (the stale cached hint stays decided,
	// so wait on the backend call rather than the cache)
	s.taskRouteHintForRequest(context.Background(), selected, userPayload("now redesign the storage layer"), "conv-1")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && calls.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected second call for new text, got %d", calls.Load())
	}
}

func TestTaskRouteHintFailOpen(t *testing.T) {
	var calls atomic.Int64
	srv := evalServer(t, `{"error":"down"}`, 500, &calls)
	defer srv.Close()
	s := classifierTestServer(t, srv, map[string]string{"fable": "deep"})
	selected := modelConfig{Requested: "fable"}

	hint := s.taskRouteHintForRequest(context.Background(), selected, userPayload("anything"), "conv-1")
	if hint.Decided {
		t.Fatalf("failure must produce empty hint, got %+v", hint)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && calls.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		s.classifier.mu.Lock()
		backing := time.Now().Before(s.classifier.failUntil)
		s.classifier.mu.Unlock()
		if backing || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// second request inside failure backoff performs no call at all
	s.taskRouteHintForRequest(context.Background(), selected, userPayload("different text"), "conv-1")
	if calls.Load() != 1 {
		t.Fatalf("failure backoff should suppress calls, got %d", calls.Load())
	}
}

func TestHintMatchesReordersAcrossSteps(t *testing.T) {
	s := classifierTestServer(t, httptest.NewServer(http.NotFoundHandler()), map[string]string{"fable": "deep"})
	s.cfg.AdaptiveRouting = adaptiveRoutingConfig{Enabled: true, StickySeconds: 300}
	s.adaptive = adaptiveState{sticky: map[string]stickyRoute{}}
	tier := func(stage int) *int { v := stage * 10000; return &v }
	candidates := []modelConfig{
		{Provider: "anthropic", Upstream: "claude-fable-5-1", Tier: tier(0), TaskClass: "deep"},
		{Provider: "moonshot", Upstream: "k3", Tier: tier(1), TaskClass: "fast"},
		{Provider: "ollama", Upstream: "kimi-k3:cloud", Tier: tier(2), TaskClass: "fast"},
		{Provider: "anthropic", Upstream: "claude-opus-5", Tier: tier(3), TaskClass: "deep"},
	}
	ranked := s.rankConfiguredCandidates(candidates, "conv", taskRouteHint{Decided: true, Tier: "fast"})
	if ranked[0].Upstream != "k3" || ranked[1].Upstream != "kimi-k3:cloud" {
		t.Fatalf("fast hint should lead with fast legs in chain order, got %v", upstreams(ranked))
	}
	if ranked[2].Upstream != "claude-fable-5-1" || ranked[3].Upstream != "claude-opus-5" {
		t.Fatalf("non-matching legs must keep chain order, got %v", upstreams(ranked))
	}
	ranked = s.rankConfiguredCandidates(candidates, "conv", taskRouteHint{Decided: true, Tier: "deep"})
	if ranked[0].Upstream != "claude-fable-5-1" || ranked[1].Upstream != "claude-opus-5" {
		t.Fatalf("deep hint should lead with deep legs in chain order, got %v", upstreams(ranked))
	}
	// risky: anthropic providers preferred even beyond their task class
	risky := taskRouteHint{Decided: true, Tier: "deep", Risky: true}
	ranked = s.rankConfiguredCandidates(candidates, "conv", risky)
	for _, m := range ranked[:2] {
		if m.Provider != "anthropic" {
			t.Fatalf("risky hint should lead with anthropic legs, got %v", upstreams(ranked))
		}
	}
	// undecided hint: original order untouched
	ranked = s.rankConfiguredCandidates(candidates, "conv", taskRouteHint{})
	if ranked[0].Upstream != "claude-fable-5-1" || ranked[1].Upstream != "k3" {
		t.Fatalf("empty hint must not reorder, got %v", upstreams(ranked))
	}
}

func upstreams(ms []modelConfig) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Upstream)
	}
	return out
}

func TestCompileClassifierValidation(t *testing.T) {
	providers := map[string]providerDefinition{"eval": {}, "anthropic": {}}
	if _, err := compileClassifier(&classifierDefinition{Enabled: true, Provider: "missing", Model: "m"}, providers); err == nil {
		t.Fatal("missing provider must fail")
	}
	if _, err := compileClassifier(&classifierDefinition{Enabled: true, Provider: "eval"}, providers); err == nil {
		t.Fatal("missing model must fail")
	}
	if _, err := compileClassifier(&classifierDefinition{Enabled: true, Provider: "eval", Model: "m", MinDowngradeConfidence: 1.5}, providers); err == nil {
		t.Fatal("out-of-range threshold must fail")
	}
	if _, err := compileClassifier(&classifierDefinition{Enabled: true, Provider: "eval", Model: "m", AliasClasses: map[string]string{"x": "huge"}}, providers); err == nil {
		t.Fatal("invalid alias class must fail")
	}
	if _, err := compileClassifier(&classifierDefinition{Enabled: true, Provider: "eval", Model: "m", RiskyProviders: []string{"missing"}}, providers); err == nil {
		t.Fatal("missing risky provider must fail")
	}
	c, err := compileClassifier(&classifierDefinition{Enabled: false, Provider: "eval"}, providers)
	if err != nil || c.Enabled {
		t.Fatalf("disabled classifier compiles disabled, got %+v err=%v", c, err)
	}
	c, err = compileClassifier(&classifierDefinition{Enabled: true, Provider: "eval", Model: "typesafe-ai/jev", RiskyProviders: []string{"anthropic"}, AliasClasses: map[string]string{"fable": "deep"}}, providers)
	if err != nil {
		t.Fatal(err)
	}
	if c.Timeout != 10*time.Second || c.CacheTTL != 600*time.Second || c.DedupeTTL != time.Hour || c.RiskyThreshold != 0.7 || c.TierMatchBonus != 25 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestShadowHintLogsButDoesNotReorder(t *testing.T) {
	s := classifierTestServer(t, httptest.NewServer(http.NotFoundHandler()), map[string]string{"fable": "deep"})
	s.cfg.Classifier.Shadow = true
	s.adaptive = adaptiveState{sticky: map[string]stickyRoute{}}
	tier := func(stage int) *int { v := stage * 10000; return &v }
	candidates := []modelConfig{
		{Provider: "anthropic", Upstream: "claude-fable-5-1", Tier: tier(0), TaskClass: "deep"},
		{Provider: "moonshot", Upstream: "k3", Tier: tier(1), TaskClass: "fast"},
	}
	hint := taskRouteHint{Decided: true, Tier: "fast"}

	// Shadow path: production ranking receives an empty hint and keeps order.
	applied := hint
	if hint.Decided && s.classifierShadow() {
		applied = taskRouteHint{}
	}
	ranked := s.rankConfiguredCandidates(candidates, "conv", applied)
	if ranked[0].Upstream != "claude-fable-5-1" {
		t.Fatalf("shadow mode must not reorder production ranking, got %v", upstreams(ranked))
	}
	s.logTaskShadowRoute(modelConfig{Requested: "fable"}, ranked, "conv", false, thinkingHistoryNone, hint)
	if ranked[0].Upstream != "claude-fable-5-1" || len(ranked) != 2 {
		t.Fatalf("shadow log pass must not mutate ranked candidates, got %v", upstreams(ranked))
	}
	// Sanity: the same hint outside shadow does rerank (covered elsewhere).
	reranked := s.rankConfiguredCandidates(candidates, "conv", hint)
	if reranked[0].Upstream != "k3" {
		t.Fatalf("hint would have promoted k3, got %v", upstreams(reranked))
	}
}

func TestCompileClassifierShadowFlag(t *testing.T) {
	providers := map[string]providerDefinition{"eval": {}}
	c, err := compileClassifier(&classifierDefinition{Enabled: true, Shadow: true, ShadowPromptChars: 160, Provider: "eval", Model: "m"}, providers)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Shadow || c.ShadowPromptChars != 160 {
		t.Fatalf("shadow flags must compile through, got %+v", c)
	}
	if _, err := compileClassifier(&classifierDefinition{Enabled: true, Provider: "eval", Model: "m", ShadowPromptChars: -1}, providers); err == nil {
		t.Fatal("negative shadowPromptChars must fail")
	}
}

func TestClassifierSkipsInternalPrompts(t *testing.T) {
	var calls atomic.Int64
	srv := evalServer(t, evalResponseBody, http.StatusOK, &calls)
	defer srv.Close()
	s := classifierTestServer(t, srv, map[string]string{"fable": "deep"})
	for _, ping := range []string{"quota", "Quota", " QUOTA "} {
		hint := s.taskRouteHintForRequest(context.Background(), modelConfig{Requested: "fable"}, userPayload(ping), "k-"+ping)
		if hint.Decided {
			t.Fatalf("internal ping %q must not produce a hint", ping)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("internal pings triggered %d eval calls, want 0", calls.Load())
	}
}

func TestClassifierTextDedupeAcrossConversations(t *testing.T) {
	var calls atomic.Int64
	srv := evalServer(t, evalResponseBody, http.StatusOK, &calls)
	defer srv.Close()
	s := classifierTestServer(t, srv, map[string]string{"fable": "deep"})
	first := s.taskRouteHintForRequest(context.Background(), modelConfig{Requested: "fable"}, userPayload("rename foo"), "conv-a")
	if first.Decided {
		t.Fatal("first turn should not block on classification")
	}
	waitForCalls(t, &calls, 1)
	// Same text in a different conversation reuses the text-keyed hint —
	// no second eval call.
	second := s.taskRouteHintForRequest(context.Background(), modelConfig{Requested: "fable"}, userPayload("rename foo"), "conv-b")
	if !second.Decided || second.Tier != "fast" {
		t.Fatalf("expected deduped fast hint, got %+v", second)
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("identical text across conversations must not reclassify, calls=%d", calls.Load())
	}
}

func TestTruncateRunes(t *testing.T) {
	if got := truncateRunes("hello", 0); got != "" {
		t.Fatalf("zero limit must yield empty, got %q", got)
	}
	if got := truncateRunes("hello", 10); got != "hello" {
		t.Fatalf("short input unchanged, got %q", got)
	}
	if got := truncateRunes("héllo wörld éèê", 6); got != "héllo …" {
		t.Fatalf("rune-safe truncation, got %q", got)
	}
}

func TestEvaluationRequestShape(t *testing.T) {
	body, err := json.Marshal(newEvaluationRequest("test prompt"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	qs := decoded["questions"].(map[string]any)
	tier := qs["tier"].(map[string]any)
	if tier["type"] != "choice" {
		t.Fatal("tier must be a choice question")
	}
	if _, ok := tier["criteria"].(map[string]any); !ok {
		t.Fatal("tier criteria must be an option map")
	}
	effort := qs["effort"].(map[string]any)
	if _, ok := effort["criteria"].([]any); !ok {
		t.Fatal("effort criteria must be a rubric array")
	}
	if qs["risky"].(map[string]any)["type"] != "boolean" {
		t.Fatal("risky must be a boolean question")
	}
	if _, ok := qs["risky"].(map[string]any)["criteria"].(map[string]any); !ok {
		t.Fatal("risky criteria must be an object — the gateway rejects null")
	}
}
