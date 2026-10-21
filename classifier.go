package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// classifierDefinition configures the task-tier classifier stage. When enabled,
// a request carrying fresh user text is classified by an evaluation model
// (e.g. TypeSafe Jev via the Vercel AI Gateway evaluation endpoint) into a
// work tier (fast/balanced/deep), an effort score, and a risk flag. The
// policy then derives a per-request route hint consumed by candidate ranking:
// matching `taskClass` legs receive a bonus and `risky` turns prefer trusted
// providers. Every failure is fail-open: the request proceeds unmodified.
type classifierDefinition struct {
	Enabled               bool              `json:"enabled"`
	Shadow                bool              `json:"shadow"`
	ShadowPromptChars     int               `json:"shadowPromptChars"`
	Provider              string            `json:"provider"`
	Model                 string            `json:"model"`
	TimeoutMS             int               `json:"timeoutMS"`
	CacheTTLSeconds       int               `json:"cacheTTLSeconds"`
	// DedupeTTLSeconds is the lifetime of the textHash->hint dedupe cache —
	// identical prompt text across conversations reuses the classification for
	// this long. Kept longer than CacheTTLSeconds because identical literal
	// text deserves the same tier far beyond a conversation's context window.
	DedupeTTLSeconds      int               `json:"dedupeTTLSeconds"`
	FailureBackoffSeconds int               `json:"failureBackoffSeconds"`
	MinUpgradeConfidence  float64           `json:"minUpgradeConfidence"`
	MinDowngradeConfidence float64          `json:"minDowngradeConfidence"`
	RiskyThreshold        float64           `json:"riskyThreshold"`
	TierMatchBonus        float64           `json:"tierMatchBonus"`
	RiskyProviders        []string          `json:"riskyProviders"`
	RiskyBonus            float64           `json:"riskyBonus"`
	MaxPromptChars        int               `json:"maxPromptChars"`
	// AliasClasses maps a request alias (e.g. "fable") to the natural task
	// class it was configured for ("fast"|"balanced"|"deep"). It anchors the
	// asymmetric confidence bars: downgrading needs more confidence than
	// upgrading. Aliases absent from the map may only be upgraded.
	AliasClasses map[string]string `json:"aliasClasses"`
}

type classifierConfig struct {
	Enabled               bool
	Shadow                bool
	ShadowPromptChars     int
	Provider              string
	Model                 string
	Timeout               time.Duration
	CacheTTL              time.Duration
	DedupeTTL             time.Duration
	FailureBackoff        time.Duration
	MinUpgradeConfidence  float64
	MinDowngradeConfidence float64
	RiskyThreshold        float64
	TierMatchBonus        float64
	RiskyProviders        map[string]bool
	RiskyBonus            float64
	MaxPromptChars        int
	AliasClasses          map[string]string
}

// taskDecision is the raw evaluation-model answer, before policy.
type taskDecision struct {
	Tier         string
	TierConfidence   float64 // 0 when the backend reports none
	Effort       float64
	EffortConfidence float64
	Risky        float64
}

// taskRouteHint is the policy output consumed by candidate ranking.
type taskRouteHint struct {
	Decided bool
	Tier    string  // "fast" | "balanced" | "deep" after confidence gating
	Risky   bool
	Effort  float64
	Sample  string // truncated classified input, only when shadowPromptChars > 0
}

type cachedClassification struct {
	textHash string
	hint     taskRouteHint
	at       time.Time
}

type classifierState struct {
	mu        sync.Mutex
	cache     map[string]cachedClassification
	byText    map[string]cachedClassification // textHash -> hint, dedupes identical prompts across sticky keys
	inFlight  map[string]bool
	failUntil time.Time
}

var tierRank = map[string]int{"fast": 0, "balanced": 1, "deep": 2}

func validTaskClass(v string) bool {
	_, ok := tierRank[v]
	return ok
}

// truncateRunes clips text to n runes for log fields — byte slicing could
// split a multibyte sequence.
func truncateRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func (s *proxyServer) classifierEnabled() bool {
	return s.cfg.Classifier != nil && s.cfg.Classifier.Enabled
}

// classifierShadow reports whether decided hints are observed but not applied.
// Shadow mode classifies normally and logs what the ordering would have been;
// production ranking receives an empty hint.
func (s *proxyServer) classifierShadow() bool {
	return s.classifierEnabled() && s.cfg.Classifier.Shadow
}

// logTaskShadowRoute re-ranks already-ranked candidates with the decided hint
// and reports when the tier would have changed the head of the chain. Ranking
// copies its input and has no side effects, so the shadow pass cannot disturb
// the ordering the request actually uses.
func (s *proxyServer) logTaskShadowRoute(selected modelConfig, ranked []modelConfig, stickyKey string, allowExplore bool, history thinkingHistoryKind, hint taskRouteHint) {
	if len(ranked) == 0 || !hint.Decided {
		return
	}
	shadow := s.rankCandidatesForRequest(selected, ranked, stickyKey, allowExplore, history, hint)
	if len(shadow) == 0 {
		return
	}
	if shadow[0].Provider == ranked[0].Provider && shadow[0].Upstream == ranked[0].Upstream {
		if s.cfg.Logging.DebugRequests {
			log.Printf("taskclass shadow alias=%q tier=%q unchanged head=%s/%s",
				selected.Requested, hint.Tier, shadow[0].Provider, shadow[0].Upstream)
		}
		return
	}
	from := -1
	for i, candidate := range ranked {
		if candidate.Provider == shadow[0].Provider && candidate.Upstream == shadow[0].Upstream {
			from = i
			break
		}
	}
	textField := ""
	if hint.Sample != "" {
		textField = fmt.Sprintf(" text=%q", hint.Sample)
	}
	log.Printf("taskclass shadow alias=%q tier=%q effort=%.2f risky=%v would_serve=%s/%s(rank=%d) actual=%s/%s%s",
		selected.Requested, hint.Tier, hint.Effort, hint.Risky, shadow[0].Provider, shadow[0].Upstream, from, ranked[0].Provider, ranked[0].Upstream, textField)
	s.recordEvent("taskclass_shadow", "", "diff", fmt.Sprintf("alias=%s tier=%s would=%s/%s actual=%s/%s",
		selected.Requested, hint.Tier, shadow[0].Provider, shadow[0].Upstream, ranked[0].Provider, ranked[0].Upstream))
}

// lastUserText returns the text of the final message when it is a fresh user
// turn (plain text content). Tool-loop requests end on tool_result blocks and
// return "" — the turn's cached classification applies to them instead.
func lastUserText(payload map[string]any, maxChars int) string {
	messages, _ := payload["messages"].([]any)
	if len(messages) == 0 {
		return ""
	}
	last, _ := messages[len(messages)-1].(map[string]any)
	if last["role"] != "user" {
		return ""
	}
	var b strings.Builder
	switch content := last["content"].(type) {
	case string:
		b.WriteString(content)
	case []any:
		for _, item := range content {
			block, _ := item.(map[string]any)
			if block == nil || block["type"] != "text" {
				continue
			}
			text, _ := block["text"].(string)
			if b.Len() > 0 && text != "" {
				b.WriteByte('\n')
			}
			b.WriteString(text)
		}
	}
	text := strings.TrimSpace(b.String())
	if maxChars > 0 && len(text) > maxChars {
		text = text[:maxChars]
	}
	return text
}

// claudeInternalPrompt matches Claude Code housekeeping pings that carry no
// routing signal — the background "quota" liveness probe etc. Classifying them
// would burn an evaluation call for nothing.
func claudeInternalPrompt(text string) bool {
	switch strings.ToLower(text) {
	case "quota", "tool loaded.":
		return true
	}
	return false
}

// evaluationRequest/Response mirror the Vercel AI Gateway evaluation-model
// protocol (specification version 4). `criteria` for a choice question is a
// map of option->description; for a score question it is the rubric array.
type evaluationRequest struct {
	State     map[string]any               `json:"state"`
	Questions map[string]evaluationQuestion `json:"questions"`
}

type evaluationQuestion struct {
	Type         string   `json:"type"`
	Instructions string   `json:"instructions"`
	Criteria     any      `json:"criteria"`
}

var tierCriteria = map[string]string{
	"fast":     "mechanical and local: renames, formatting, single-file edits, lookups, short answers",
	"balanced": "ordinary engineering: multi-file features, debugging, refactors, tests",
	"deep":     "hard or high-stakes: architecture, security, migrations, subtle correctness",
}

var effortCriteria = []string{"almost none", "some", "a lot", "as much as possible"}

const riskyInstructions = "Carrying out this task would itself change production, move real money, or alter data that cannot be restored. Writing or testing code that deals with such things, without running it against the real system, does not count."

func newEvaluationRequest(prompt string) evaluationRequest {
	return evaluationRequest{
		State: map[string]any{"prompt": prompt},
		Questions: map[string]evaluationQuestion{
			"tier":   {Type: "choice", Instructions: "Which is the cheapest tier that can complete this coding task well?", Criteria: tierCriteria},
			"effort": {Type: "score", Instructions: "How much step-by-step reasoning does this task need?", Criteria: effortCriteria},
			"risky":  {Type: "boolean", Instructions: riskyInstructions, Criteria: map[string]string{}},
		},
	}
}

type evaluationResponse struct {
	Answers map[string]struct {
		Choice        string             `json:"choice"`
		Score         *float64           `json:"score"`
		Probability   *float64           `json:"probability"`
		Probabilities map[string]float64 `json:"probabilities"`
		Confidence    *float64           `json:"confidence"`
	} `json:"answers"`
	ProviderMetadata struct {
		Typesafe struct {
			Confidence map[string]float64 `json:"confidence"`
		} `json:"typesafe"`
	} `json:"providerMetadata"`
}

func answerConfidence(ans *struct {
	Choice        string             `json:"choice"`
	Score         *float64           `json:"score"`
	Probability   *float64           `json:"probability"`
	Probabilities map[string]float64 `json:"probabilities"`
	Confidence    *float64           `json:"confidence"`
}, metaConf float64) float64 {
	if metaConf > 0 {
		return metaConf
	}
	if ans.Confidence != nil {
		return *ans.Confidence
	}
	best := 0.0
	for _, p := range ans.Probabilities {
		if p > best {
			best = p
		}
	}
	return best
}

func parseEvaluationResponse(body []byte) (taskDecision, error) {
	var resp evaluationResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return taskDecision{}, err
	}
	tier := resp.Answers["tier"]
	if !validTaskClass(tier.Choice) {
		return taskDecision{}, fmt.Errorf("evaluation answer missing tier choice")
	}
	d := taskDecision{Tier: tier.Choice, TierConfidence: answerConfidence(&tier, resp.ProviderMetadata.Typesafe.Confidence["tier"])}
	if effort, ok := resp.Answers["effort"]; ok && effort.Score != nil {
		d.Effort = *effort.Score
		d.EffortConfidence = answerConfidence(&effort, resp.ProviderMetadata.Typesafe.Confidence["effort"])
	}
	if risky, ok := resp.Answers["risky"]; ok && risky.Probability != nil {
		d.Risky = *risky.Probability
	}
	return d, nil
}

// classifyTask calls the configured evaluation provider once. The provider's
// baseURL is the gateway root (…/v4/ai); the evaluation path is appended.
func (s *proxyServer) classifyTask(ctx context.Context, prompt string) (taskDecision, error) {
	cfg := s.cfg.Classifier
	provider, ok := s.cfg.Providers[cfg.Provider]
	if !ok {
		return taskDecision{}, fmt.Errorf("classifier provider %q not configured", cfg.Provider)
	}
	token, configured, err := providerAuthToken(cfg.Provider, provider)
	if err != nil {
		return taskDecision{}, err
	}
	if !configured || token == "" {
		return taskDecision{}, errors.New("classifier provider has no usable auth token")
	}
	encoded, err := json.Marshal(newEvaluationRequest(prompt))
	if err != nil {
		return taskDecision{}, err
	}
	url := strings.TrimRight(provider.BaseURL, "/") + "/evaluation-model"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return taskDecision{}, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+token)
	req.Header.Set("ai-gateway-auth-method", "api-key")
	req.Header.Set("ai-model-id", cfg.Model)
	req.Header.Set("ai-gateway-protocol-version", "0.0.1")
	req.Header.Set("ai-evaluation-model-specification-version", "4")
	resp, err := s.classifierClient().Do(req)
	if err != nil {
		return taskDecision{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return taskDecision{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return taskDecision{}, fmt.Errorf("evaluation backend status %d", resp.StatusCode)
	}
	return parseEvaluationResponse(body)
}

func (s *proxyServer) classifierClient() *http.Client {
	if s.classifierHTTP != nil {
		return s.classifierHTTP
	}
	s.classifierHTTP = &http.Client{Timeout: 5 * time.Second}
	return s.classifierHTTP
}

// policyHint applies the asymmetric confidence bars ported from
// jev-model-router: downgrading the work below the alias's natural class
// needs MinDowngradeConfidence, upgrading needs MinUpgradeConfidence, and a
// risky turn takes the deep tier unconditionally. An alias with no declared
// class has no knowable direction: only confident upgrades apply.
func (s *proxyServer) policyHint(d taskDecision, requested string) (taskRouteHint, string) {
	cfg := s.cfg.Classifier
	hint := taskRouteHint{Effort: d.Effort}
	natural := cfg.AliasClasses[requested]
	switch {
	case !validTaskClass(d.Tier):
		return hint, "no-tier"
	case natural == "":
		if d.TierConfidence < cfg.MinUpgradeConfidence {
			return hint, "unknown-class-low-confidence"
		}
		hint.Tier = d.Tier
	case tierRank[d.Tier] < tierRank[natural]:
		if d.TierConfidence < cfg.MinDowngradeConfidence {
			return hint, "downgrade-below-bar"
		}
		hint.Tier = d.Tier
	case tierRank[d.Tier] > tierRank[natural]:
		if d.TierConfidence < cfg.MinUpgradeConfidence {
			return hint, "upgrade-below-bar"
		}
		hint.Tier = d.Tier
	default:
		hint.Tier = d.Tier
	}
	if d.Risky >= cfg.RiskyThreshold {
		hint.Tier = "deep"
		hint.Risky = true
	}
	hint.Decided = hint.Tier != "" || hint.Risky
	if !hint.Decided {
		return hint, "no-effect"
	}
	return hint, "applied"
}

// taskRouteHintForRequest resolves the cached route hint for a request and,
// on fresh user text, launches an asynchronous classification. Classification
// never blocks the request path: the first turn of a conversation proceeds
// unhinted, and the decision lands in the conversation cache for following
// turns and tool-loop requests (which carry no fresh text).
// Classifier failures are backed off globally so a dead backend cannot add
// latency to every turn.
func (s *proxyServer) taskRouteHintForRequest(ctx context.Context, selected modelConfig, payload map[string]any, stickyKey string) taskRouteHint {
	if !s.classifierEnabled() {
		return taskRouteHint{}
	}
	cfg := s.cfg.Classifier
	text := lastUserText(payload, cfg.MaxPromptChars)
	if claudeInternalPrompt(text) {
		return taskRouteHint{}
	}
	var textHash string
	if text != "" {
		sum := sha256.Sum256([]byte(text))
		textHash = hex.EncodeToString(sum[:])
	}
	now := time.Now()
	s.classifier.mu.Lock()
	if cached, ok := s.classifier.cache[stickyKey]; ok && now.Sub(cached.at) < cfg.CacheTTL {
		if text == "" || cached.textHash == textHash {
			s.classifier.mu.Unlock()
			return cached.hint
		}
	}
	if textHash != "" {
		if tc, ok := s.classifier.byText[textHash]; ok && now.Sub(tc.at) < cfg.DedupeTTL {
			if s.classifier.cache == nil {
				s.classifier.cache = map[string]cachedClassification{}
			}
			s.classifier.cache[stickyKey] = tc
			s.classifier.mu.Unlock()
			return tc.hint
		}
	}
	if text == "" || now.Before(s.classifier.failUntil) || s.classifier.inFlight[textHash] || stickyKey == "" {
		s.classifier.mu.Unlock()
		return taskRouteHint{}
	}
	if s.classifier.inFlight == nil {
		s.classifier.inFlight = map[string]bool{}
	}
	s.classifier.inFlight[textHash] = true
	s.classifier.mu.Unlock()
	go s.classifyAsync(stickyKey, textHash, text, selected.Requested)
	return taskRouteHint{}
}

// classifyAsync performs the evaluation off the request path on a detached
// context — the inbound request ends before the answer arrives — and stores
// the decided hint in the conversation cache for subsequent turns.
func (s *proxyServer) classifyAsync(stickyKey, textHash, text, requested string) {
	cfg := s.cfg.Classifier
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	started := time.Now()
	decision, err := s.classifyTask(ctx, text)
	cancel()
	elapsed := time.Since(started).Milliseconds()
	s.classifier.mu.Lock()
	delete(s.classifier.inFlight, textHash)
	if err != nil {
		s.classifier.failUntil = time.Now().Add(cfg.FailureBackoff)
		s.classifier.mu.Unlock()
		log.Printf("taskclass failed alias=%q backoff=%s err=%v", requested, cfg.FailureBackoff, err)
		s.recordEvent("taskclass", "", "failed", fmt.Sprintf("alias=%s th=%s", requested, textHash[:8]))
		return
	}
	s.classifier.mu.Unlock()
	hint, outcome := s.policyHint(decision, requested)
	textField := ""
	if cfg.ShadowPromptChars > 0 {
		hint.Sample = truncateRunes(text, cfg.ShadowPromptChars)
		textField = fmt.Sprintf(" text=%q", hint.Sample)
	}
	log.Printf("taskclass alias=%q tier=%s conf=%.2f effort=%.2f risky=%.2f -> %s hint=%q ms=%d%s",
		requested, decision.Tier, decision.TierConfidence, decision.Effort, decision.Risky, outcome, hint.Tier, elapsed, textField)
	s.recordEvent("taskclass", "", outcome, fmt.Sprintf("alias=%s tier=%s conf=%.2f effort=%.2f risky=%.2f hint=%s ms=%d th=%s",
		requested, decision.Tier, decision.TierConfidence, decision.Effort, decision.Risky, hint.Tier, elapsed, textHash[:8]))
	s.classifier.mu.Lock()
	if s.classifier.cache == nil {
		s.classifier.cache = map[string]cachedClassification{}
	}
	s.classifier.cache[stickyKey] = cachedClassification{textHash: textHash, hint: hint, at: started}
	if s.classifier.byText == nil {
		s.classifier.byText = map[string]cachedClassification{}
	}
	s.classifier.byText[textHash] = cachedClassification{textHash: textHash, hint: hint, at: started}
	for _, sweep := range []struct {
		m   map[string]cachedClassification
		ttl time.Duration
	}{{s.classifier.cache, cfg.CacheTTL}, {s.classifier.byText, cfg.DedupeTTL}} {
		if len(sweep.m) > 2048 {
			for key, entry := range sweep.m {
				if time.Since(entry.at) >= sweep.ttl {
					delete(sweep.m, key)
				}
			}
		}
	}
	s.classifier.mu.Unlock()
}

// hintMatches reports whether a candidate belongs to the hint's preferred
// partition: its task class matches the decided tier, or it is a trusted
// provider on a risky turn. Preferred candidates sort ahead of the rest while
// keeping chain order inside each partition — the hint reroutes across steps
// but never removes a leg, so fallback semantics are unchanged.
func (s *proxyServer) hintMatches(m modelConfig, hint taskRouteHint) bool {
	if !hint.Decided {
		return false
	}
	cfg := s.cfg.Classifier
	return hint.Tier != "" && m.TaskClass == hint.Tier || hint.Risky && cfg.RiskyProviders[m.Provider]
}

// hintBonus is a secondary score adjustment applied within one chain step,
// after the hintMatches partition and before remaining score ties.
func (s *proxyServer) hintBonus(m modelConfig, hint taskRouteHint) float64 {
	if !hint.Decided {
		return 0
	}
	cfg := s.cfg.Classifier
	bonus := 0.0
	if hint.Tier != "" && m.TaskClass == hint.Tier {
		bonus += cfg.TierMatchBonus
	}
	if hint.Risky && cfg.RiskyProviders[m.Provider] {
		bonus += cfg.RiskyBonus
	}
	return bonus
}
