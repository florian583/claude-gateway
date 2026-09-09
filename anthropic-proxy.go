package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	Definition      *fileConfig               `json:"-"`
	SourcePath      string                    `json:"-"`
	SourceHash      string                    `json:"-"`
	Listen          string                    `json:"listen"`
	AlsoListen      []string                  `json:"alsoListen"`
	DefaultProvider string                    `json:"defaultProvider"`
	Providers       map[string]providerConfig `json:"providers"`
	Chains          map[string]routeChain     `json:"chains"`
	Models          map[string]modelConfig    `json:"models"`
	ModelFamilies   []familyRule              `json:"modelFamilies"`
	Normalize       normalizeConfig           `json:"normalize"`
	Metrics         metricsConfig             `json:"metrics"`
	Logging         loggingConfig             `json:"logging"`
	OllamaUsage     ollamaUsageConfig         `json:"ollamaUsage"`
	ClineUsage      clineUsageConfig          `json:"clineUsage"`
	ClaudeUsage     claudeUsageConfig         `json:"claudeUsage"`
	AdaptiveRouting adaptiveRoutingConfig     `json:"adaptiveRouting"`
	Quarantine      providerQuarantineConfig  `json:"providerQuarantine"`
}

// familyRule matches a requested model name by case-insensitive substring
// (e.g. "sonnet" matches claude-sonnet-4-5, claude-sonnet-5, ...) so routing
// survives Anthropic version bumps without config edits. First match wins;
// exact entries in Models take precedence over family rules.
type familyRule struct {
	Match         string `json:"match"`
	Provider      string `json:"provider"`
	Upstream      string `json:"upstream"`
	Fallback      string `json:"fallback"`
	FallbackChain string `json:"fallbackChain"`
	ResponseAlias string `json:"responseAlias"`
	ContextWindow int    `json:"contextWindow"`
	model         modelConfig
	matchLower    string
}

// routeChain gives canonical fallback trees stable semantic names. Models and
// family rules reference the name; the target model remains the single route
// definition. Family/context/vision contracts are checked before serving.
type routeChain struct {
	Model          string `json:"model"`
	Family         string `json:"family"`
	ContextWindow  int    `json:"contextWindow"`
	SupportsImages *bool  `json:"supportsImages,omitempty"`
}

type providerConfig struct {
	Variant                  string            `json:"-"`
	Billing                  string            `json:"-"`
	DisplayName              string            `json:"-"`
	Usage                    usageDefinition   `json:"-"`
	MessagesPath             string            `json:"-"`
	BaseURL                  string            `json:"baseURL"`
	Headers                  map[string]string `json:"headers"`
	RequestOverrides         map[string]any    `json:"requestOverrides"`
	DropResponseContentTypes []string          `json:"dropResponseContentTypes"`
	FoldSystemIntoMessages   bool              `json:"foldSystemIntoMessages"`
	// Format selects the upstream wire protocol. Empty means the provider
	// speaks the Anthropic Messages API natively; "openai-chat" runs the
	// built-in chat-completions adapter (request/response translation).
	Format string `json:"format"`
	// ResponseHeaderTimeoutMS bounds time-to-first-byte per attempt so a hung
	// upstream fails over instead of blocking forever. 0 means the 120s
	// default; -1 disables the bound entirely.
	ResponseHeaderTimeoutMS int      `json:"responseHeaderTimeoutMS"`
	StreamIdleTimeoutMS     int      `json:"streamIdleTimeoutMS"`
	AuthTokenEnv            string   `json:"authTokenEnv"`
	AuthTokenCommand        []string `json:"authTokenCommand"`
	AuthHeader              string   `json:"authHeader"`
	AuthPrefix              string   `json:"authPrefix"`
	// AuthMode enables provider-managed rotating credentials. "xai-oauth"
	// uses the user's SuperGrok/X Premium+ device-code grant instead of an
	// xAI pay-as-you-go API key. AuthStatePath is resolved relative to this
	// config file and must remain private (0600).
	AuthMode      string `json:"authMode"`
	AuthStatePath string `json:"authStatePath"`
	// CircuitBreaker defaults to true. Set false for sole-provider routes
	// (e.g. direct Anthropic passthrough) where tripping the circuit only
	// creates dead windows — there is no fallback leg to absorb the traffic.
	CircuitBreaker *bool `json:"circuitBreaker"`
}

type modelConfig struct {
	ModelID              string         `json:"-"`
	AccountStickySeconds int            `json:"-"`
	AccountPool          string         `json:"-"`
	ConfiguredPool       string         `json:"-"`
	SupportsTools        bool           `json:"-"`
	ModelOverrides       map[string]any `json:"-"`
	AllowPaidFallback    bool           `json:"-"`
	PaidFallbackOn       []string       `json:"-"`
	Provider             string         `json:"provider"`
	Upstream             string         `json:"upstream"`
	ResponseAlias        string         `json:"responseAlias"`
	ContextWindow        int            `json:"contextWindow"`
	Tier                 *int           `json:"tier,omitempty"`
	SupportsImages       *bool          `json:"supportsImages,omitempty"`
	Fallbacks            []modelConfig  `json:"fallbacks"`
	Alias                string         `json:"alias"`
	Chain                string         `json:"chain"`
	Requested            string         `json:"-"`
	ClaudeProfile        string         `json:"-"`
}

type upstreamResponse struct {
	resp          *http.Response
	providerName  string
	model         modelConfig
	attempt       int
	started       time.Time
	trace         requestTrace
	unstable      bool
	failureReason string
}

type requestTrace struct {
	Timing         *upstreamTiming
	Ingress        time.Time
	HeadersMS      int64
	FirstEventMS   int64
	ID             string
	Path           string
	CandidateCount int
	ChainStarted   time.Time
	ChainBudget    time.Duration
}

type releaseReadCloser struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (r *releaseReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.release)
	return err
}

type readerReadCloser struct {
	io.Reader
	io.Closer
}

const (
	initialSSEEventTimeout = 30 * time.Second
	maxInitialSSEBytes     = 256 << 10
	maxSSEFrameBytes       = 1 << 20
	maxToolArgumentBytes   = 8 << 20
	maxToolCalls           = 128
)

// ReadSlice bounds allocation even when the peer never sends a newline.
func readBoundedSSELine(reader *bufio.Reader, limit int) (string, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > limit {
			return "", fmt.Errorf("SSE frame exceeds %d bytes", limit)
		}
		line = append(line, part...)
		if err != bufio.ErrBufferFull {
			return string(line), err
		}
	}
}

type initialSSEError struct{ kind string }

func (e *initialSSEError) Error() string { return "initial SSE provider error: " + e.kind }

func validateInitialSSE(event, data string) error {
	if data == "[DONE]" {
		return errors.New("initial SSE contains no completion")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil || payload == nil {
		return errors.New("initial SSE contains malformed JSON")
	}
	payload = unwrapOpenAIEnvelope(payload)
	if errBody, ok := payload["error"].(map[string]any); ok {
		kind, _ := errBody["type"].(string)
		return &initialSSEError{kind: firstNonEmpty(kind, "api_error")}
	}
	if event == "error" || payload["type"] == "error" || payload["error"] != nil {
		return &initialSSEError{kind: "api_error"}
	}
	return nil
}

// primeSSEBody proves that a streaming response contains one complete data
// event before the proxy commits response bytes downstream. Failures here can
// safely use the next route; later failures cannot be replayed without risking
// duplicate output or tool calls.
func primeSSEBody(body io.ReadCloser, timeout time.Duration) (io.ReadCloser, error) {
	reader := bufio.NewReader(body)
	var prefix bytes.Buffer
	hasData := false
	var event string
	var dataLines []string

	var timedOut atomic.Bool
	var timer *time.Timer
	timerDone := make(chan struct{})
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			timedOut.Store(true)
			_ = body.Close()
			close(timerDone)
		})
	}
	stopTimer := func() {
		if timer != nil && !timer.Stop() {
			<-timerDone
		}
	}
	defer stopTimer()

	for prefix.Len() <= maxInitialSSEBytes {
		line, err := readBoundedSSELine(reader, maxInitialSSEBytes-prefix.Len())
		if len(line) > 0 {
			_, _ = prefix.WriteString(line)
			trimmed := strings.TrimRight(line, "\r\n")
			switch {
			case strings.HasPrefix(trimmed, "event:"):
				event = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			case strings.HasPrefix(trimmed, "data:"):
				hasData = true
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
			case trimmed == "" && hasData:
				if timedOut.Load() {
					return nil, fmt.Errorf("initial SSE event timed out after %s", timeout)
				}
				if err := validateInitialSSE(event, strings.Join(dataLines, "\n")); err != nil {
					return nil, err
				}
				return &readerReadCloser{
					Reader: io.MultiReader(bytes.NewReader(prefix.Bytes()), reader),
					Closer: body,
				}, nil
			}
		}
		if err != nil {
			if timedOut.Load() {
				return nil, fmt.Errorf("initial SSE event timed out after %s", timeout)
			}
			return nil, fmt.Errorf("initial SSE event unavailable: %w", err)
		}
	}
	return nil, fmt.Errorf("initial SSE event exceeds %d bytes", maxInitialSSEBytes)
}

type normalizeConfig struct {
	UnsupportedContentTypes map[string]string `json:"unsupportedContentTypes"`
}

type metricsConfig struct {
	Path       string `json:"path"`
	MaxSamples int    `json:"maxSamples"`
}

type loggingConfig struct {
	Path          string `json:"path"`
	MaxBytes      int64  `json:"maxBytes"`
	Backups       int    `json:"backups"`
	DebugRequests bool   `json:"debugRequests"`
}

// adaptiveRoutingConfig reorders a model's configured candidates from recent
// real traffic. Provider tiers are defaults; model candidates can override
// them to create route-local subscription stages.
type adaptiveRoutingConfig struct {
	Enabled         bool               `json:"enabled"`
	WindowSamples   int                `json:"windowSamples"`
	MinSamples      int                `json:"minSamples"`
	RefreshSeconds  int                `json:"refreshSeconds"`
	StickySeconds   int                `json:"stickySeconds"`
	ExploreEvery    int                `json:"exploreEvery"`
	StabilityWeight float64            `json:"stabilityWeight"`
	LatencyWeight   float64            `json:"latencyWeight"`
	TPSWeight       float64            `json:"tpsWeight"`
	UsageWeight     float64            `json:"usageWeight"`
	HeadroomWeight  float64            `json:"headroomWeight"`
	SwitchMargin    float64            `json:"switchMargin"`
	ProviderTiers   map[string]int     `json:"providerTiers"`
	QualityBonus    map[string]float64 `json:"qualityBonus"`
	UsageCost       map[string]float64 `json:"usageCost"`
}

// providerQuarantineConfig temporarily removes an unstable provider from all
// routes. It complements the provider+upstream circuit breaker: circuits catch
// a broken model leg, while quarantine catches provider-wide brownouts. Long
// generation time is deliberately absent; only first-byte latency, event
// stalls, protocol/status failures, and stall-related disconnects are signals.
type providerQuarantineConfig struct {
	Enabled                        bool `json:"enabled"`
	SignalThreshold                int  `json:"signalThreshold"`
	WindowSeconds                  int  `json:"windowSeconds"`
	BaseSeconds                    int  `json:"baseSeconds"`
	MaxSeconds                     int  `json:"maxSeconds"`
	RecoverySuccesses              int  `json:"recoverySuccesses"`
	ExcessiveTTFBMS                int  `json:"excessiveTTFBMS"`
	StreamEventStallMS             int  `json:"streamEventStallMS"`
	NetworkIncidentOrigins         int  `json:"networkIncidentOrigins"`
	NetworkIncidentWindowSeconds   int  `json:"networkIncidentWindowSeconds"`
	NetworkIncidentSuppressSeconds int  `json:"networkIncidentSuppressSeconds"`
}

// ollamaUsageConfig protects the shared Ollama allowance. Once a configured
// threshold is reached, only ReserveUpstreams remain eligible. At 100% the
// whole provider blocks until the next telemetry refresh.
type ollamaUsageConfig struct {
	Provider            string   `json:"provider"`
	WeeklyThresholdPct  float64  `json:"weeklyThresholdPct"`
	SessionThresholdPct float64  `json:"sessionThresholdPct"`
	CacheTTLSeconds     int      `json:"cacheTTLSeconds"`
	EligibleUpstreams   []string `json:"eligibleUpstreams"`
	ReserveUpstreams    []string `json:"reserveUpstreams"`
	SwitchModel         string   `json:"switchModel"`
}

type ollamaUsageResponse struct {
	Limits struct {
		Session ollamaUsageWindow `json:"session"`
		Weekly  ollamaUsageWindow `json:"weekly"`
	} `json:"limits"`
}

type ollamaUsageWindow struct {
	Usage  float64 `json:"usage"`
	Models []struct {
		Name         string `json:"name"`
		RequestCount int    `json:"request_count"`
	} `json:"models"`
}

type ollamaUsageSnapshot struct {
	FetchedAt time.Time
	Weekly    ollamaUsageWindow
	Session   ollamaUsageWindow
}

type ollamaUsageCache struct {
	mu         sync.Mutex
	snapshot   ollamaUsageSnapshot
	hasValue   bool
	refreshing bool
}

type clineUsageConfig struct {
	Provider          string  `json:"provider"`
	CacheTTLSeconds   int     `json:"cacheTTLSeconds"`
	BlockThresholdPct float64 `json:"blockThresholdPct"`
}

type clineUsageWindow struct {
	Type        string    `json:"type"`
	PercentUsed float64   `json:"percentUsed"`
	ResetsAt    time.Time `json:"resetsAt"`
}

type clineUsageResponse struct {
	Data struct {
		Limits []clineUsageWindow `json:"limits"`
	} `json:"data"`
	Success bool `json:"success"`
}

type clineUsageSnapshot struct {
	FetchedAt time.Time                   `json:"fetchedAt"`
	Windows   map[string]clineUsageWindow `json:"windows"`
}

type clineUsageCache struct {
	mu         sync.Mutex
	snapshot   clineUsageSnapshot
	hasValue   bool
	refreshing bool
}

type xaiUsageSnapshot struct {
	FetchedAt   time.Time                   `json:"fetchedAt"`
	Windows     map[string]clineUsageWindow `json:"windows"`
	Products    map[string]float64          `json:"products,omitempty"`
	BillingKind string                      `json:"billingKind"`
	Source      string                      `json:"source"`
}

type xaiUsageCache struct {
	mu         sync.Mutex
	snapshot   xaiUsageSnapshot
	hasValue   bool
	refreshing bool
}

type xaiBillingAmount struct {
	Val float64 `json:"val"`
}

type xaiBillingConfig struct {
	CreditUsagePercent *float64 `json:"creditUsagePercent"`
	CurrentPeriod      struct {
		Start string `json:"start"`
		End   string `json:"end"`
		Type  string `json:"type"`
	} `json:"currentPeriod"`
	ProductUsage []struct {
		Product      string  `json:"product"`
		UsagePercent float64 `json:"usagePercent"`
	} `json:"productUsage"`
	IsUnifiedBillingUser bool             `json:"isUnifiedBillingUser"`
	MonthlyLimit         xaiBillingAmount `json:"monthlyLimit"`
	Used                 xaiBillingAmount `json:"used"`
	BillingPeriodStart   string           `json:"billingPeriodStart"`
	BillingPeriodEnd     string           `json:"billingPeriodEnd"`
}

type xaiBillingEnvelope struct {
	Config xaiBillingConfig `json:"config"`
}

// browserUsageSnapshot is sanitized telemetry produced by the user-level Ego
// collector. Cookies and dashboard response bodies remain inside Ego; the
// proxy receives only percentages, reset labels, and collection status.
type browserUsageWindow struct {
	PercentUsed float64 `json:"percentUsed"`
	Reset       string  `json:"reset,omitempty"`
}

type browserUsageProvider struct {
	Source      string                        `json:"source,omitempty"`
	Status      string                        `json:"status"`
	FetchedAt   time.Time                     `json:"fetchedAt"`
	LastSuccess time.Time                     `json:"lastSuccessAt,omitempty"`
	Plan        string                        `json:"plan,omitempty"`
	Utilization *float64                      `json:"utilization,omitempty"`
	Windows     map[string]browserUsageWindow `json:"windows,omitempty"`
	Error       string                        `json:"error,omitempty"`
}

type browserUsageSnapshot struct {
	Version   int                             `json:"version"`
	UpdatedAt time.Time                       `json:"updatedAt"`
	Collector map[string]any                  `json:"collector,omitempty"`
	Providers map[string]browserUsageProvider `json:"providers"`
}

type browserUsageCache struct {
	mu        sync.Mutex
	loadedAt  time.Time
	snapshot  browserUsageSnapshot
	hasValue  bool
	lastError string
}

// claudeUsageConfig tracks Claude subscription headroom and can select among
// several independently logged-in Claude Code profiles. ListenerProfiles keep
// compatibility with dedicated ports; AccountProfiles extend that set with
// additional standby accounts.
type claudeUsageConfig struct {
	Provider             string                        `json:"provider"`
	EligibleUpstreams    []string                      `json:"eligibleUpstreams"`
	PreferBelowUsagePct  float64                       `json:"preferBelowUsagePct"`
	FiveHourThresholdPct float64                       `json:"fiveHourThresholdPct"`
	SevenDayThresholdPct float64                       `json:"sevenDayThresholdPct"`
	CacheTTLSeconds      int                           `json:"cacheTTLSeconds"`
	StaleTTLSeconds      int                           `json:"staleTTLSeconds"`
	RequestTimeoutMS     int                           `json:"requestTimeoutMS"`
	OAuthRefreshEnabled  bool                          `json:"oauthRefreshEnabled"`
	OAuthRefreshInterval int                           `json:"oauthRefreshIntervalSeconds"`
	OAuthRefreshTimeout  int                           `json:"oauthRefreshTimeoutSeconds"`
	ListenerProfiles     map[string]claudeUsageProfile `json:"listenerProfiles"`
	AutoSelectAccounts   bool                          `json:"autoSelectAccounts"`
	AccountStickySeconds int                           `json:"accountStickySeconds"`
	AccountPool          []string                      `json:"accountPool"`
	AccountProfiles      map[string]claudeUsageProfile `json:"accountProfiles"`
	// Paid fallbacks remain fail-closed for account/auth failures. Provider-wide
	// failures must repeat inside a short window before pay-as-you-go activates.
	PaidFallbackProviders         []string `json:"paidFallbackProviders"`
	PaidFallbackMinOutageFailures int      `json:"paidFallbackMinOutageFailures"`
	PaidFallbackWindowSeconds     int      `json:"paidFallbackWindowSeconds"`
	PaidFallbackActiveSeconds     int      `json:"paidFallbackActiveSeconds"`
}

type claudeUsageProfile struct {
	Name                 string   `json:"name"`
	CachePath            string   `json:"cachePath"`
	CredentialsService   string   `json:"credentialsService"`
	SevenDayThresholdPct *float64 `json:"sevenDayThresholdPct,omitempty"`
}

type claudeUsageWindow struct {
	Utilization float64   `json:"utilization"`
	ResetsAt    time.Time `json:"resets_at"`
}

type claudeUsageResponse struct {
	FiveHour claudeUsageWindow `json:"five_hour"`
	SevenDay claudeUsageWindow `json:"seven_day"`
}

type claudeUsageSnapshot struct {
	FetchedAt time.Time         `json:"fetchedAt"`
	Profile   string            `json:"profile,omitempty"`
	FiveHour  claudeUsageWindow `json:"fiveHour"`
	SevenDay  claudeUsageWindow `json:"sevenDay"`
	Source    string            `json:"source"`
	TokenKey  string            `json:"-"`
}

type claudeUsageCache struct {
	gate              chan struct{}
	nextFetch         time.Time
	globalRetryAt     time.Time
	credentialRetryAt map[string]time.Time
	pollCursor        int
	persistMu         sync.Mutex
	mu                sync.Mutex
	workerHistory     map[string][]claudeUsageSnapshot
	snapshots         map[string]claudeUsageSnapshot
	refreshing        map[string]chan struct{}
	retryAt           map[string]time.Time
}

type claudeUsageRateLimit struct{ delay time.Duration }

func (e *claudeUsageRateLimit) Error() string {
	return "Claude usage endpoint returned status 429"
}

type claudeIdentityCacheEntry struct {
	modifiedAt time.Time
	size       int64
	accountKey string
}

type claudeAccountSticky struct {
	Profile claudeUsageProfile
	Until   time.Time
}

type availableClaudeAccount struct {
	profile  claudeUsageProfile
	snapshot claudeUsageSnapshot
}

type claudeProfileRoutingLoad struct {
	profile  claudeUsageProfile
	inFlight int
	blocked  bool
	snapshot claudeUsageSnapshot
	hasUsage bool
}

type claudeAccountPool struct {
	Primary  claudeUsageProfile
	Fallback claudeUsageProfile
}

type claudeCandidateAttempt struct {
	model       modelConfig
	profile     claudeUsageProfile
	fromProfile string
}

type metricsSample struct {
	ReportedBackend    string  `json:"reportedBackend,omitempty"`
	PreparationMS      int64   `json:"preparationMs,omitempty"`
	TotalMS            int64   `json:"totalMs,omitempty"`
	ResponseHeadersMS  int64   `json:"responseHeadersMs,omitempty"`
	FirstEventMS       int64   `json:"firstEventMs,omitempty"`
	FirstContentMS     int64   `json:"firstContentMs,omitempty"`
	MaximumEventGapMS  int64   `json:"maximumEventGapMs,omitempty"`
	CacheReadTokens    int64   `json:"cacheReadInputTokens,omitempty"`
	CacheWriteTokens   int64   `json:"cacheCreationInputTokens,omitempty"`
	InputTokenEstimate int64   `json:"inputTokenEstimate,omitempty"`
	ConnectionMS       int64   `json:"connectionMs,omitempty"`
	DNSMS              int64   `json:"dnsMs,omitempty"`
	TLSMS              int64   `json:"tlsMs,omitempty"`
	Timestamp          string  `json:"timestamp"`
	RequestID          string  `json:"requestId,omitempty"`
	Path               string  `json:"path,omitempty"`
	RecordKind         string  `json:"recordKind,omitempty"`
	Attempt            int     `json:"attempt"`
	CandidateCount     int     `json:"candidateCount,omitempty"`
	ChainElapsedMS     int64   `json:"chainElapsedMs,omitempty"`
	RequestedModel     string  `json:"model"`
	Provider           string  `json:"provider"`
	ClaudeProfile      string  `json:"claudeProfile,omitempty"`
	Upstream           string  `json:"upstream,omitempty"`
	ResponseAlias      string  `json:"responseAlias,omitempty"`
	Status             int     `json:"status"`
	LatencyMS          int64   `json:"headerLatencyMs"`
	Success            bool    `json:"success"`
	IsFallback         bool    `json:"isFallback"`
	FallbackTrigger    bool    `json:"fallbackTrigger"`
	FailureReason      string  `json:"failureReason,omitempty"`
	StreamDurationMS   int64   `json:"streamDurationMs,omitempty"`
	StreamBytes        int64   `json:"streamBytes,omitempty"`
	StreamEvents       int64   `json:"streamEvents,omitempty"`
	StreamError        string  `json:"streamError,omitempty"`
	Stream             bool    `json:"stream,omitempty"`
	InputTokens        int64   `json:"inputTokens,omitempty"`
	OutputTokens       int64   `json:"outputTokens,omitempty"`
	TokensPerSecond    float64 `json:"tokensPerSecond,omitempty"`
}

// Sub-second bodies are usually buffered gateway responses, cache hits, or
// tiny control turns. Dividing token counts by those durations produces
// impossible TPS outliers and corrupts adaptive routing. Require a meaningful
// streamed interval before treating throughput as an observation.
const minimumTPSObservationDuration = time.Second

func observedTokensPerSecond(outputTokens int64, streamDuration time.Duration) float64 {
	if outputTokens <= 0 || streamDuration < minimumTPSObservationDuration {
		return 0
	}
	return float64(outputTokens) / streamDuration.Seconds()
}

type metricsStore struct {
	mu             sync.RWMutex
	ioMu           sync.Mutex
	path           string
	maxSamples     int
	samples        metricsRing
	routeSamples   map[string]*metricsRing
	pending        []metricsSample
	flushTimer     *time.Timer
	persistedLines int
	closed         bool
	file           *os.File
}

type metricsRing struct {
	values []metricsSample
	next   int
	full   bool
}

func newMetricsRing(capacity int) metricsRing {
	if capacity < 1 {
		capacity = 1
	}
	return metricsRing{values: make([]metricsSample, 0, capacity)}
}

func (r *metricsRing) add(sample metricsSample) {
	if len(r.values) < cap(r.values) {
		r.values = append(r.values, sample)
		return
	}
	r.values[r.next] = sample
	r.next = (r.next + 1) % cap(r.values)
	r.full = true
}

func (r *metricsRing) latest(limit int) []metricsSample {
	count := len(r.values)
	if limit <= 0 || limit > count {
		limit = count
	}
	if limit == 0 {
		return nil
	}
	start := 0
	if r.full {
		start = r.next
	}
	ordered := make([]metricsSample, count)
	for index := 0; index < count; index++ {
		ordered[index] = r.values[(start+index)%count]
	}
	return append([]metricsSample(nil), ordered[count-limit:]...)
}

type metricsGroupKey struct {
	Model    string
	Provider string
	Upstream string
}

type metricsAggregate struct {
	Model              string  `json:"model"`
	Provider           string  `json:"provider"`
	Upstream           string  `json:"upstream"`
	Records            int     `json:"records"`
	Attempts           int     `json:"attempts"`
	PolicySkips        int     `json:"policySkips"`
	CountTokenRequests int     `json:"countTokenRequests"`
	TerminalErrors     int     `json:"terminalErrors"`
	DeliveredResponses int     `json:"deliveredResponses"`
	Successes          int     `json:"successes"`
	Failures           int     `json:"failures"`
	TerminalSuccesses  int     `json:"terminalSuccesses"`
	FallbackAttempts   int     `json:"fallbackAttempts"`
	FallbackTriggers   int     `json:"fallbackTriggers"`
	SuccessRate        float64 `json:"successRate"`
	AvgHeaderLatencyMS float64 `json:"avgHeaderLatencyMs"`
	P50HeaderLatencyMS float64 `json:"p50HeaderLatencyMs"`
	P95HeaderLatencyMS float64 `json:"p95HeaderLatencyMs"`
	P50TokensPerSecond float64 `json:"p50TokensPerSecond"`
	latencies          []float64
	tokensPerSecond    []float64
}

type routePerformance struct {
	Samples      int       `json:"samples"`
	SuccessRate  float64   `json:"successRate"`
	P50HeaderMS  float64   `json:"p50HeaderMs"`
	P95HeaderMS  float64   `json:"p95HeaderMs"`
	P50TPS       float64   `json:"p50TokensPerSecond"`
	InputTokens  int64     `json:"inputTokens"`
	OutputTokens int64     `json:"outputTokens"`
	LastSeen     time.Time `json:"lastSeen"`
}

type stickyRoute struct {
	Key         string
	Family      string
	Until       time.Time
	FamilyUntil time.Time
}

type thinkingHistoryKind uint8

const (
	thinkingHistoryNone thinkingHistoryKind = iota
	thinkingHistoryAnthropic
	thinkingHistoryNonAnthropic
	thinkingHistoryMixed
)

type adaptiveState struct {
	mu          sync.Mutex
	refreshedAt time.Time
	performance map[string]routePerformance
	sticky      map[string]stickyRoute
	requests    atomic.Uint64
}

type providerRuntimeState struct {
	LastFailure        time.Time         `json:"lastFailure,omitempty"`
	BlockedUntil       time.Time         `json:"blockedUntil,omitempty"`
	Reason             string            `json:"reason,omitempty"`
	LastUpdated        time.Time         `json:"lastUpdated,omitempty"`
	RateLimitInfo      map[string]string `json:"rateLimitInfo,omitempty"`
	QuarantinedUntil   time.Time         `json:"quarantinedUntil,omitempty"`
	QuarantineReason   string            `json:"quarantineReason,omitempty"`
	InstabilitySignals int               `json:"instabilitySignals,omitempty"`
	SignalWindowStart  time.Time         `json:"signalWindowStart,omitempty"`
	QuarantineLevel    int               `json:"quarantineLevel,omitempty"`
	RecoverySuccesses  int               `json:"recoverySuccesses,omitempty"`
	Recovering         bool              `json:"recovering,omitempty"`
	LastSignal         string            `json:"lastSignal,omitempty"`
}

type sharedNetworkIncidentState struct {
	Signals       map[string]time.Time
	Phase         string
	SuspectedAt   time.Time
	ActiveUntil   time.Time
	NextProbeAt   time.Time
	ProbeInFlight bool
	Epoch         uint64
	LastDetected  time.Time
	Origins       []string
}

const (
	networkPhaseNormal     = "normal"
	networkPhaseSuspected  = "suspected"
	networkPhaseOffline    = "offline"
	networkPhaseRecovering = "recovering"
	networkRecoveryDelay   = 2 * time.Second
)

type networkFailureDecision struct {
	SuppressProviderPenalty bool
	StopFallback            bool
}

type retryableNetworkError struct {
	message string
}

func (e *retryableNetworkError) Error() string {
	if e == nil || e.message == "" {
		return "local network unavailable; retry shortly"
	}
	return e.message
}

type requestCompatibilityError struct {
	message string
}

func (e *requestCompatibilityError) Error() string { return e.message }

type proxyRequestIDContextKey struct{}
type proxyIngressContextKey struct{}
type upstreamTimingContextKey struct{}

type upstreamTiming struct {
	connectionMS atomic.Int64
	dnsMS        atomic.Int64
	tlsMS        atomic.Int64
}

var proxyRequestSequence atomic.Uint64

func ensureProxyRequestID(r *http.Request) (*http.Request, string) {
	if r == nil {
		return r, ""
	}
	if requestID, ok := r.Context().Value(proxyRequestIDContextKey{}).(string); ok && requestID != "" {
		return r, requestID
	}
	requestID := fmt.Sprintf("p-%x-%x", time.Now().UnixMilli(), proxyRequestSequence.Add(1))
	ctx := context.WithValue(r.Context(), proxyRequestIDContextKey{}, requestID)
	ctx = context.WithValue(ctx, proxyIngressContextKey{}, time.Now())
	return r.WithContext(ctx), requestID
}

func proxyRequestID(r *http.Request) string {
	if r == nil {
		return ""
	}
	requestID, _ := r.Context().Value(proxyRequestIDContextKey{}).(string)
	return requestID
}

type responseHeaderTimeoutError struct {
	provider     string
	upstream     string
	budget       time.Duration
	chainLimited bool
}

func (e *responseHeaderTimeoutError) Error() string {
	return fmt.Sprintf("timeout awaiting response headers provider=%s upstream=%q budget=%s", e.provider, e.upstream, e.budget)
}

func (e *responseHeaderTimeoutError) Timeout() bool   { return true }
func (e *responseHeaderTimeoutError) Temporary() bool { return true }

type claudePaidFallbackState struct {
	Signals     []time.Time
	ActiveUntil time.Time
}

type metricsResponse struct {
	Limit       int                `json:"limit"`
	Samples     int                `json:"samples"`
	GeneratedAt string             `json:"generatedAt"`
	Groups      []metricsAggregate `json:"groups"`
}

type rotatingLogWriter struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	backups  int
	file     *os.File
	size     int64
}

func newRotatingLogWriter(cfg loggingConfig) (*rotatingLogWriter, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o700); err != nil {
		return nil, err
	}
	writer := &rotatingLogWriter{path: cfg.Path, maxBytes: cfg.MaxBytes, backups: cfg.Backups}
	if info, err := os.Stat(cfg.Path); err == nil && info.Size() >= cfg.MaxBytes {
		if err := writer.rotateLocked(); err != nil {
			return nil, err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := writer.openLocked(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *rotatingLogWriter) openLocked() error {
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	// OpenFile's mode only applies when creating a file. Tighten legacy files too;
	// older launchd logging left the operational log world-readable.
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file = file
	w.size = info.Size()
	return nil
}

func (w *rotatingLogWriter) rotateLocked() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	for index := w.backups - 1; index >= 1; index-- {
		from := fmt.Sprintf("%s.%d", w.path, index)
		to := fmt.Sprintf("%s.%d", w.path, index+1)
		_ = os.Remove(to)
		if err := os.Rename(from, to); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if w.backups > 0 {
		to := w.path + ".1"
		_ = os.Remove(to)
		if err := os.Rename(w.path, to); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err := os.Remove(w.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	w.size = 0
	return nil
}

func (w *rotatingLogWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		if err := w.openLocked(); err != nil {
			return 0, err
		}
	}
	if w.maxBytes > 0 && w.size > 0 && w.size+int64(len(body)) > w.maxBytes {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
		if err := w.openLocked(); err != nil {
			return 0, err
		}
	}
	written, err := w.file.Write(body)
	w.size += int64(written)
	return written, err
}

func (w *rotatingLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

type proxyServer struct {
	providerState            routingState
	activeRequests           atomic.Int64
	draining                 atomic.Bool
	cfg                      config
	providers                map[string]*url.URL
	clients                  map[string]*http.Client
	claudeProfileClients     map[string]*http.Client
	transports               []*http.Transport
	metrics                  *metricsStore
	ollamaUse                ollamaUsageCache
	clineUse                 clineUsageCache
	xaiUse                   xaiUsageCache
	browserUse               browserUsageCache
	browserUsagePath         string
	claudeUse                claudeUsageCache
	claudeIdentityMu         sync.Mutex
	claudeIdentities         map[string]claudeIdentityCacheEntry
	claudeAccountMu          sync.Mutex
	claudeAccounts           map[string]claudeAccountSticky
	claudeAccountInFlight    map[string]int
	claudeSelectionLogMu     sync.Mutex
	claudeSelectionLogged    map[string]string
	routeLoadMu              sync.Mutex
	routeInFlight            map[string]int
	adaptive                 adaptiveState
	providerStateMu          sync.Mutex
	providerStates           map[string]providerRuntimeState
	networkIncident          sharedNetworkIncidentState
	clockNow                 func() time.Time
	claudeRefreshMu          sync.Mutex
	claudeAuthRefreshGate    chan struct{}
	claudeRefreshInFlight    map[string]bool
	claudeRefreshLastAttempt map[string]time.Time
	claudeRefreshRunner      func(context.Context, claudeUsageProfile) error
	claudeScheduledRunner    func(context.Context) error
	claudeRefreshStatus      string
	claudeRefreshSummary     string
	claudePaidFallbackMu     sync.Mutex
	claudePaidFallback       claudePaidFallbackState
	xaiOAuth                 map[string]*xaiOAuthManager
}

func main() {
	if err := runCLI(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() > 0 {
			os.Exit(exit.ExitCode())
		}
		os.Exit(1)
	}
}

func serve(cfg config) error {
	proxyLog, err := newRotatingLogWriter(cfg.Logging)
	if err != nil {
		return fmt.Errorf("proxy log setup: %w", err)
	}
	defer proxyLog.Close()
	defer log.SetOutput(log.Writer())
	log.SetOutput(proxyLog)
	server, err := newProxyServer(cfg)
	if err != nil {
		return err
	}
	defer server.metrics.close()

	handler := server.handler()

	servers := []*http.Server{}
	for _, addr := range append([]string{cfg.Listen}, cfg.AlsoListen...) {
		servers = append(servers, &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 30 * time.Second,
			// NOTE: never set WriteTimeout here — it would kill long-lived SSE streams.
		})
	}
	listeners := make([]net.Listener, 0, len(servers))
	for _, srv := range servers {
		listener, listenErr := net.Listen("tcp", srv.Addr)
		if listenErr != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("proxy bind on %s: %w", srv.Addr, listenErr)
		}
		listeners = append(listeners, listener)
	}
	log.Printf("anthropic proxy listening on %s providers=%d models=%d", cfg.Listen, len(cfg.Providers), len(cfg.Models))
	for _, extra := range cfg.AlsoListen {
		log.Printf("anthropic proxy also listening on %s", extra)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go server.monitorOllamaUsage(ctx)
	go server.monitorClineUsage(ctx)
	go server.monitorXAIUsage(ctx)
	go server.monitorProfileCredentials(ctx)
	go server.monitorConfiguredBrowserUsage(ctx)
	go server.monitorClaudeUsage(ctx)
	errCh := make(chan error, len(servers)+1)
	go func() {
		<-ctx.Done()
		server.draining.Store(true)
		log.Printf("proxy draining activeRequests=%d", server.activeRequests.Load())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		var drained sync.WaitGroup
		for _, srv := range servers {
			drained.Add(1)
			go func(srv *http.Server) {
				defer drained.Done()
				if err := srv.Shutdown(shutdownCtx); err != nil {
					log.Printf("proxy drain deadline reached activeRequests=%d", server.activeRequests.Load())
					_ = srv.Close()
				}
			}(srv)
		}
		drained.Wait()
		// Serve filters ErrServerClosed; signal completion after draining.
		errCh <- nil
	}()
	for index, srv := range servers {
		go func(s *http.Server, listener net.Listener) {
			if err := s.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}(srv, listeners[index])
	}
	return <-errCh
}

func (s *proxyServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/status", s.status)
	// Claude Code probes custom API endpoints with HEAD /api/hello. Handle it
	// locally: forwarding a model-less probe can poison provider circuits.
	mux.HandleFunc("/api/hello", s.health)
	mux.HandleFunc("/routes", s.routes)
	mux.HandleFunc("/metrics", s.metricsHandler)
	mux.HandleFunc("/quota", s.quota)
	mux.HandleFunc("/routing", s.routingStatus)
	mux.HandleFunc("/catalog", s.catalog)
	mux.HandleFunc("/dashboard", s.dashboard)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Definition != nil && (r.Method != http.MethodPost || (r.URL.Path != "/v1/messages" && r.URL.Path != "/v1/messages/count_tokens")) {
			writeAPIError(w, http.StatusNotFound, "invalid_request_error", "unsupported local proxy endpoint")
			return
		}
		if s.draining.Load() {
			_, requestID := ensureProxyRequestID(r)
			s.recordRequestTerminal(requestID, r.URL.Path, modelConfig{}, http.StatusServiceUnavailable, "proxy_draining", time.Now())
			w.Header().Set("Retry-After", "2")
			writeAPIError(w, http.StatusServiceUnavailable, "api_error", "proxy draining; retry shortly")
			return
		}
		s.activeRequests.Add(1)
		defer s.activeRequests.Add(-1)
		s.proxy(w, r)
	})
	return mux
}

func normalizeRuntimeConfig(path string, body []byte, cfg config) (config, error) {
	cfg.SourcePath = path
	cfg.SourceHash = fmt.Sprintf("%x", sha256.Sum256(body))
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:" + getenv("ANTHROPIC_PROXY_PORT", "48104")
	}
	if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		cfg.Listen = "127.0.0.1:" + cfg.Listen
	}
	if cfg.DefaultProvider == "" {
		cfg.DefaultProvider = "fireworks"
	}
	if len(cfg.Providers) == 0 {
		return config{}, errors.New("proxy config has no providers")
	}
	for name, provider := range cfg.Providers {
		provider.AuthMode = strings.ToLower(strings.TrimSpace(provider.AuthMode))
		if provider.AuthMode == "" {
			continue
		}
		if provider.AuthMode != "xai-oauth" {
			return config{}, fmt.Errorf("provider %q has unsupported authMode %q", name, provider.AuthMode)
		}
		if provider.AuthTokenEnv != "" || len(provider.AuthTokenCommand) > 0 {
			return config{}, fmt.Errorf("provider %q xai-oauth cannot also configure a static auth token", name)
		}
		if provider.Format != "openai-chat" {
			return config{}, fmt.Errorf("provider %q xai-oauth requires format openai-chat", name)
		}
		if err := validateXAISubscriptionOrigin(provider.BaseURL, "inference baseURL"); err != nil {
			return config{}, fmt.Errorf("provider %q: %w", name, err)
		}
		if strings.TrimSpace(provider.AuthStatePath) == "" {
			provider.AuthStatePath = filepath.Join(filepath.Dir(path), "xai-oauth.json")
		} else if !filepath.IsAbs(provider.AuthStatePath) {
			provider.AuthStatePath = filepath.Join(filepath.Dir(path), provider.AuthStatePath)
		}
		provider.AuthStatePath = filepath.Clean(provider.AuthStatePath)
		cfg.Providers[name] = provider
	}
	if cfg.Normalize.UnsupportedContentTypes == nil {
		cfg.Normalize.UnsupportedContentTypes = map[string]string{}
	}
	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = filepath.Join(filepath.Dir(path), "proxy-metrics.jsonl")
	} else if !filepath.IsAbs(cfg.Metrics.Path) {
		cfg.Metrics.Path = filepath.Join(filepath.Dir(path), cfg.Metrics.Path)
	}
	if cfg.Metrics.MaxSamples <= 0 {
		cfg.Metrics.MaxSamples = 2000
	}
	if cfg.Logging.Path == "" {
		cfg.Logging.Path = filepath.Join(filepath.Dir(path), "anthropic-proxy.log")
	} else if !filepath.IsAbs(cfg.Logging.Path) {
		cfg.Logging.Path = filepath.Join(filepath.Dir(path), cfg.Logging.Path)
	}
	if cfg.Logging.MaxBytes <= 0 {
		cfg.Logging.MaxBytes = 32 << 20
	}
	if cfg.Logging.Backups <= 0 {
		cfg.Logging.Backups = 3
	}
	if err := compileNamedChains(&cfg); err != nil {
		return config{}, err
	}
	if err := resolveModelAliases(cfg.Models); err != nil {
		return config{}, err
	}
	if err := validateConfiguredModelRoutes(cfg); err != nil {
		return config{}, err
	}
	if cfg.OllamaUsage.WeeklyThresholdPct > 0 || cfg.OllamaUsage.SessionThresholdPct > 0 {
		if cfg.OllamaUsage.WeeklyThresholdPct < 0 || cfg.OllamaUsage.WeeklyThresholdPct > 100 {
			return config{}, fmt.Errorf("ollamaUsage.weeklyThresholdPct must be between 0 and 100")
		}
		if cfg.OllamaUsage.SessionThresholdPct < 0 || cfg.OllamaUsage.SessionThresholdPct > 100 {
			return config{}, fmt.Errorf("ollamaUsage.sessionThresholdPct must be between 0 and 100")
		}
		if cfg.OllamaUsage.Provider == "" {
			cfg.OllamaUsage.Provider = "ollama"
		}
		if _, ok := cfg.Providers[cfg.OllamaUsage.Provider]; !ok {
			return config{}, fmt.Errorf("ollamaUsage references unknown provider %q", cfg.OllamaUsage.Provider)
		}
		if cfg.OllamaUsage.SwitchModel != "" {
			if _, ok := cfg.Models[cfg.OllamaUsage.SwitchModel]; !ok {
				return config{}, fmt.Errorf("ollamaUsage.switchModel references unknown model %q", cfg.OllamaUsage.SwitchModel)
			}
		}
		if len(cfg.OllamaUsage.EligibleUpstreams) == 0 {
			return config{}, errors.New("ollamaUsage.eligibleUpstreams is required when usage thresholds are set")
		}
		if len(cfg.OllamaUsage.ReserveUpstreams) == 0 {
			return config{}, errors.New("ollamaUsage.reserveUpstreams is required when usage thresholds are set")
		}
		for _, upstream := range cfg.OllamaUsage.ReserveUpstreams {
			if !containsString(cfg.OllamaUsage.EligibleUpstreams, upstream) {
				return config{}, fmt.Errorf("ollamaUsage reserve upstream %q is not eligible", upstream)
			}
		}
		if cfg.OllamaUsage.CacheTTLSeconds <= 0 {
			cfg.OllamaUsage.CacheTTLSeconds = 300
		}
	}
	if cfg.ClineUsage.Provider != "" {
		if _, ok := cfg.Providers[cfg.ClineUsage.Provider]; !ok {
			return config{}, fmt.Errorf("clineUsage references unknown provider %q", cfg.ClineUsage.Provider)
		}
		if cfg.ClineUsage.CacheTTLSeconds <= 0 {
			cfg.ClineUsage.CacheTTLSeconds = 300
		}
		if cfg.ClineUsage.BlockThresholdPct == 0 {
			cfg.ClineUsage.BlockThresholdPct = 100
		}
		if cfg.ClineUsage.BlockThresholdPct < 0 || cfg.ClineUsage.BlockThresholdPct > 100 {
			return config{}, fmt.Errorf("clineUsage.blockThresholdPct must be between 0 and 100")
		}
	}
	if cfg.ClaudeUsage.Provider != "" {
		if _, ok := cfg.Providers[cfg.ClaudeUsage.Provider]; !ok {
			return config{}, fmt.Errorf("claudeUsage references unknown provider %q", cfg.ClaudeUsage.Provider)
		}
		if len(cfg.ClaudeUsage.EligibleUpstreams) == 0 {
			return config{}, errors.New("claudeUsage.eligibleUpstreams is required")
		}
		if cfg.ClaudeUsage.FiveHourThresholdPct <= 0 || cfg.ClaudeUsage.FiveHourThresholdPct > 100 {
			return config{}, errors.New("claudeUsage.fiveHourThresholdPct must be between 0 and 100")
		}
		if cfg.ClaudeUsage.SevenDayThresholdPct <= 0 || cfg.ClaudeUsage.SevenDayThresholdPct > 100 {
			return config{}, errors.New("claudeUsage.sevenDayThresholdPct must be between 0 and 100")
		}
		if cfg.ClaudeUsage.PreferBelowUsagePct < 0 || cfg.ClaudeUsage.PreferBelowUsagePct > 100 {
			return config{}, errors.New("claudeUsage.preferBelowUsagePct must be between 0 and 100")
		}
		if cfg.ClaudeUsage.CacheTTLSeconds <= 0 {
			cfg.ClaudeUsage.CacheTTLSeconds = 300
		}
		if cfg.ClaudeUsage.StaleTTLSeconds <= 0 {
			cfg.ClaudeUsage.StaleTTLSeconds = 1800
		}
		if cfg.ClaudeUsage.StaleTTLSeconds < cfg.ClaudeUsage.CacheTTLSeconds {
			return config{}, errors.New("claudeUsage.staleTTLSeconds must be at least cacheTTLSeconds")
		}
		if cfg.ClaudeUsage.RequestTimeoutMS <= 0 {
			cfg.ClaudeUsage.RequestTimeoutMS = 5000
		}
		if cfg.ClaudeUsage.OAuthRefreshInterval <= 0 {
			cfg.ClaudeUsage.OAuthRefreshInterval = 900
		}
		if cfg.ClaudeUsage.OAuthRefreshInterval < 60 {
			return config{}, errors.New("claudeUsage.oauthRefreshIntervalSeconds must be at least 60")
		}
		if cfg.ClaudeUsage.OAuthRefreshTimeout <= 0 {
			cfg.ClaudeUsage.OAuthRefreshTimeout = 600
		}
		if cfg.ClaudeUsage.OAuthRefreshTimeout < 120 {
			return config{}, errors.New("claudeUsage.oauthRefreshTimeoutSeconds must be at least 120")
		}
		if cfg.ClaudeUsage.AccountStickySeconds <= 0 {
			cfg.ClaudeUsage.AccountStickySeconds = 1800
		}
		if len(cfg.ClaudeUsage.PaidFallbackProviders) == 0 {
			cfg.ClaudeUsage.PaidFallbackProviders = []string{"vercel-kimi"}
		}
		if cfg.ClaudeUsage.PaidFallbackMinOutageFailures <= 0 {
			cfg.ClaudeUsage.PaidFallbackMinOutageFailures = 3
		}
		if cfg.ClaudeUsage.PaidFallbackWindowSeconds <= 0 {
			cfg.ClaudeUsage.PaidFallbackWindowSeconds = 120
		}
		if cfg.ClaudeUsage.PaidFallbackActiveSeconds <= 0 {
			cfg.ClaudeUsage.PaidFallbackActiveSeconds = 300
		}
		for listener, profile := range cfg.ClaudeUsage.ListenerProfiles {
			if _, err := strconv.Atoi(listener); err != nil {
				return config{}, fmt.Errorf("claudeUsage listener profile %q must be a port", listener)
			}
			if strings.TrimSpace(profile.Name) == "" {
				return config{}, fmt.Errorf("claudeUsage listener profile %q has empty name", listener)
			}
			if profile.SevenDayThresholdPct != nil && (*profile.SevenDayThresholdPct <= 0 || *profile.SevenDayThresholdPct > 100) {
				return config{}, fmt.Errorf("claudeUsage listener profile %q sevenDayThresholdPct must be between 0 and 100", listener)
			}
		}
		for name, profile := range cfg.ClaudeUsage.AccountProfiles {
			if strings.TrimSpace(profile.Name) == "" {
				profile.Name = name
				cfg.ClaudeUsage.AccountProfiles[name] = profile
			}
			if strings.TrimSpace(profile.CredentialsService) == "" {
				return config{}, fmt.Errorf("claudeUsage account profile %q has no credentials service", name)
			}
			if profile.SevenDayThresholdPct != nil && (*profile.SevenDayThresholdPct <= 0 || *profile.SevenDayThresholdPct > 100) {
				return config{}, fmt.Errorf("claudeUsage account profile %q sevenDayThresholdPct must be between 0 and 100", name)
			}
		}
	}
	if cfg.AdaptiveRouting.Enabled {
		if cfg.AdaptiveRouting.WindowSamples <= 0 {
			cfg.AdaptiveRouting.WindowSamples = 600
		}
		if cfg.AdaptiveRouting.MinSamples <= 0 {
			cfg.AdaptiveRouting.MinSamples = 4
		}
		if cfg.AdaptiveRouting.RefreshSeconds <= 0 {
			cfg.AdaptiveRouting.RefreshSeconds = 15
		}
		if cfg.AdaptiveRouting.StickySeconds <= 0 {
			cfg.AdaptiveRouting.StickySeconds = 90
		}
		if cfg.AdaptiveRouting.ExploreEvery <= 0 {
			cfg.AdaptiveRouting.ExploreEvery = 20
		}
		if cfg.AdaptiveRouting.StabilityWeight <= 0 {
			cfg.AdaptiveRouting.StabilityWeight = 8
		}
		if cfg.AdaptiveRouting.LatencyWeight <= 0 {
			cfg.AdaptiveRouting.LatencyWeight = 1
		}
		if cfg.AdaptiveRouting.TPSWeight <= 0 {
			cfg.AdaptiveRouting.TPSWeight = 0.35
		}
		if cfg.AdaptiveRouting.UsageWeight <= 0 {
			cfg.AdaptiveRouting.UsageWeight = 4
		}
		if cfg.AdaptiveRouting.HeadroomWeight <= 0 {
			cfg.AdaptiveRouting.HeadroomWeight = 20
		}
		if cfg.AdaptiveRouting.SwitchMargin <= 0 {
			cfg.AdaptiveRouting.SwitchMargin = 8
		}
		if cfg.AdaptiveRouting.ProviderTiers == nil {
			cfg.AdaptiveRouting.ProviderTiers = map[string]int{}
		}
		if cfg.AdaptiveRouting.QualityBonus == nil {
			cfg.AdaptiveRouting.QualityBonus = map[string]float64{}
		}
		if cfg.AdaptiveRouting.UsageCost == nil {
			cfg.AdaptiveRouting.UsageCost = map[string]float64{}
		}
	}
	if cfg.Quarantine.Enabled {
		if cfg.Quarantine.SignalThreshold <= 0 {
			cfg.Quarantine.SignalThreshold = 2
		}
		if cfg.Quarantine.WindowSeconds <= 0 {
			cfg.Quarantine.WindowSeconds = 300
		}
		if cfg.Quarantine.BaseSeconds <= 0 {
			cfg.Quarantine.BaseSeconds = 300
		}
		if cfg.Quarantine.MaxSeconds <= 0 {
			cfg.Quarantine.MaxSeconds = 1800
		}
		if cfg.Quarantine.MaxSeconds < cfg.Quarantine.BaseSeconds {
			return config{}, errors.New("providerQuarantine.maxSeconds must be at least baseSeconds")
		}
		if cfg.Quarantine.RecoverySuccesses <= 0 {
			cfg.Quarantine.RecoverySuccesses = 2
		}
		if cfg.Quarantine.ExcessiveTTFBMS <= 0 {
			cfg.Quarantine.ExcessiveTTFBMS = 45000
		}
		if cfg.Quarantine.StreamEventStallMS <= 0 {
			cfg.Quarantine.StreamEventStallMS = 120000
		}
		if cfg.Quarantine.NetworkIncidentOrigins <= 0 {
			cfg.Quarantine.NetworkIncidentOrigins = 2
		}
		if cfg.Quarantine.NetworkIncidentWindowSeconds <= 0 {
			cfg.Quarantine.NetworkIncidentWindowSeconds = 15
		}
		if cfg.Quarantine.NetworkIncidentSuppressSeconds <= 0 {
			cfg.Quarantine.NetworkIncidentSuppressSeconds = 90
		}
	}
	for i, rule := range cfg.ModelFamilies {
		if rule.Match == "" {
			return config{}, fmt.Errorf("modelFamilies[%d] has empty match", i)
		}
		if rule.Provider == "" {
			return config{}, fmt.Errorf("modelFamilies[%d] (%q) has empty provider", i, rule.Match)
		}
		resolved := modelConfig{Provider: rule.Provider, Upstream: rule.Upstream, ResponseAlias: rule.ResponseAlias, ContextWindow: rule.ContextWindow}
		if rule.Fallback != "" && rule.FallbackChain != "" {
			return config{}, fmt.Errorf("modelFamilies[%d] (%q) cannot set fallback and fallbackChain", i, rule.Match)
		}
		fallbackTarget := rule.Fallback
		if rule.FallbackChain != "" {
			chain, ok := cfg.Chains[rule.FallbackChain]
			if !ok {
				return config{}, fmt.Errorf("modelFamilies[%d] (%q) references unknown fallbackChain %q", i, rule.Match, rule.FallbackChain)
			}
			fallbackTarget = chain.Model
		}
		if fallbackTarget != "" {
			target, ok := cfg.Models[fallbackTarget]
			if !ok {
				return config{}, fmt.Errorf("modelFamilies[%d] (%q) fallback references unknown model %q", i, rule.Match, fallbackTarget)
			}
			resolved.Fallbacks = []modelConfig{target}
		}
		cfg.ModelFamilies[i].model = resolved
		cfg.ModelFamilies[i].matchLower = strings.ToLower(rule.Match)
	}
	return cfg, nil
}

// matchFamily returns the compiled modelConfig for the first family rule whose
// Match string appears (case-insensitive) in the requested model name.
func (cfg config) matchFamily(model string) (modelConfig, bool) {
	lower := strings.ToLower(model)
	for _, rule := range cfg.ModelFamilies {
		if strings.Contains(lower, rule.matchLower) {
			matched := rule.model
			matched.Requested = model
			return matched, true
		}
	}
	return modelConfig{}, false
}

func cloneModelRoute(model modelConfig) modelConfig {
	cloned := model
	cloned.Fallbacks = make([]modelConfig, len(model.Fallbacks))
	for i, fallback := range model.Fallbacks {
		cloned.Fallbacks[i] = cloneModelRoute(fallback)
	}
	return cloned
}

func modelReferenceOnly(model modelConfig) bool {
	return model.Provider == "" && model.Upstream == "" && model.ResponseAlias == "" &&
		model.ContextWindow == 0 && model.Tier == nil && model.SupportsImages == nil &&
		len(model.Fallbacks) == 0 && model.Requested == "" && model.ClaudeProfile == ""
}

func resolveNamedRoute(models map[string]modelConfig, chains map[string]routeChain, modelName string) (modelConfig, error) {
	seen := map[string]bool{}
	var resolveModel func(string) (modelConfig, error)
	var resolveChain func(string) (modelConfig, error)
	resolveModel = func(name string) (modelConfig, error) {
		key := "model:" + name
		if seen[key] {
			return modelConfig{}, fmt.Errorf("model/chain reference cycle at %q", name)
		}
		model, ok := models[name]
		if !ok {
			return modelConfig{}, fmt.Errorf("unknown model %q", name)
		}
		seen[key] = true
		defer delete(seen, key)
		switch {
		case model.Alias != "" && model.Chain != "":
			return modelConfig{}, fmt.Errorf("model %q cannot set alias and chain", name)
		case model.Alias != "":
			if !modelReferenceOnly(model) {
				return modelConfig{}, fmt.Errorf("model %q alias cannot also configure route fields", name)
			}
			return resolveModel(model.Alias)
		case model.Chain != "":
			if !modelReferenceOnly(model) {
				return modelConfig{}, fmt.Errorf("model %q chain cannot also configure route fields", name)
			}
			return resolveChain(model.Chain)
		default:
			return cloneModelRoute(model), nil
		}
	}
	resolveChain = func(name string) (modelConfig, error) {
		key := "chain:" + name
		if seen[key] {
			return modelConfig{}, fmt.Errorf("model/chain reference cycle at chain %q", name)
		}
		chain, ok := chains[name]
		if !ok {
			return modelConfig{}, fmt.Errorf("unknown chain %q", name)
		}
		if strings.TrimSpace(chain.Model) == "" {
			return modelConfig{}, fmt.Errorf("chain %q has empty model", name)
		}
		seen[key] = true
		defer delete(seen, key)
		return resolveModel(chain.Model)
	}
	return resolveModel(modelName)
}

func routeHasExplicitImageSupport(model modelConfig) bool {
	if model.SupportsImages != nil && *model.SupportsImages {
		return true
	}
	for _, fallback := range model.Fallbacks {
		if routeHasExplicitImageSupport(fallback) {
			return true
		}
	}
	return false
}

func validateRouteTree(label string, route modelConfig, cfg config, contract routeChain) error {
	seen := map[string]bool{}
	var walk func(modelConfig) error
	walk = func(candidate modelConfig) error {
		if candidate.Alias != "" || candidate.Chain != "" {
			return fmt.Errorf("%s contains unresolved route reference", label)
		}
		providerName := firstNonEmpty(candidate.Provider, cfg.DefaultProvider)
		if _, ok := cfg.Providers[providerName]; !ok {
			return fmt.Errorf("%s references unknown provider %q", label, providerName)
		}
		key := providerName + "|" + candidate.Upstream
		if cfg.Definition != nil {
			key += "|" + candidate.ModelID + "|" + candidate.AccountPool
		}
		if seen[key] {
			return fmt.Errorf("%s contains duplicate route %q", label, key)
		}
		seen[key] = true
		if contract.ContextWindow > 0 && candidate.ContextWindow < contract.ContextWindow {
			return fmt.Errorf("%s route %q contextWindow=%d below contract=%d", label, key, candidate.ContextWindow, contract.ContextWindow)
		}
		for _, fallback := range candidate.Fallbacks {
			if err := walk(fallback); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(route); err != nil {
		return err
	}
	if contract.Family != "" && candidateModelFamily(route) != strings.ToLower(contract.Family) {
		return fmt.Errorf("%s primary family=%q differs from contract=%q", label, candidateModelFamily(route), contract.Family)
	}
	if contract.SupportsImages != nil && *contract.SupportsImages && !routeHasExplicitImageSupport(route) {
		return fmt.Errorf("%s declares image support but has no explicitly vision-capable route", label)
	}
	return nil
}

func compileNamedChains(cfg *config) error {
	if cfg.Chains == nil {
		cfg.Chains = map[string]routeChain{}
	}
	for name, chain := range cfg.Chains {
		if strings.TrimSpace(name) == "" {
			return errors.New("chain name cannot be empty")
		}
		resolved, err := resolveNamedRoute(cfg.Models, cfg.Chains, chain.Model)
		if err != nil {
			return fmt.Errorf("chain %q: %w", name, err)
		}
		if err := validateRouteTree("chain "+strconv.Quote(name), resolved, *cfg, chain); err != nil {
			return err
		}
	}
	for name, model := range cfg.Models {
		if model.Chain == "" {
			continue
		}
		if !modelReferenceOnly(model) {
			return fmt.Errorf("model %q chain cannot also configure route fields", name)
		}
		chain, ok := cfg.Chains[model.Chain]
		if !ok {
			return fmt.Errorf("model %q references unknown chain %q", name, model.Chain)
		}
		resolved, err := resolveNamedRoute(cfg.Models, cfg.Chains, chain.Model)
		if err != nil {
			return fmt.Errorf("model %q chain %q: %w", name, model.Chain, err)
		}
		cfg.Models[name] = resolved
	}
	return nil
}

func validateConfiguredModelRoutes(cfg config) error {
	for name, route := range cfg.Models {
		if err := validateRouteTree("model "+strconv.Quote(name), route, cfg, routeChain{}); err != nil {
			return err
		}
	}
	return nil
}

// resolveModelAliases replaces every model entry whose only meaningful field is
// "alias" with a copy of the target entry, following chains and rejecting cycles.
func resolveModelAliases(models map[string]modelConfig) error {
	for key, model := range models {
		if model.Alias == "" {
			continue
		}
		seen := map[string]bool{key: true}
		target := model.Alias
		for {
			resolved, ok := models[target]
			if !ok {
				return fmt.Errorf("model %q aliases unknown model %q", key, target)
			}
			if resolved.Alias == "" {
				models[key] = resolved
				break
			}
			if seen[target] {
				return fmt.Errorf("model alias cycle involving %q and %q", key, target)
			}
			seen[target] = true
			target = resolved.Alias
		}
	}
	return nil
}

const defaultResponseHeaderTimeout = 120 * time.Second

func newProxyServer(cfg config) (*proxyServer, error) {
	providers := make(map[string]*url.URL, len(cfg.Providers))
	clients := make(map[string]*http.Client, len(cfg.Providers))
	xaiOAuth := map[string]*xaiOAuthManager{}
	xaiOAuthByPath := map[string]*xaiOAuthManager{}
	// Providers pointing at the same origin with the same header timeout share
	// one transport, so fallback legs that differ only in requestOverrides
	// (the vercel-* family) reuse warm connections instead of keeping six
	// separate idle pools to one host.
	transports := map[string]*http.Transport{}
	for name, provider := range cfg.Providers {
		if provider.BaseURL == "" {
			return nil, fmt.Errorf("provider %q has empty baseURL", name)
		}
		parsed, err := url.Parse(provider.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("provider %q baseURL: %w", name, err)
		}
		providers[name] = parsed

		headerTimeout := defaultResponseHeaderTimeout
		switch {
		case provider.ResponseHeaderTimeoutMS > 0:
			headerTimeout = time.Duration(provider.ResponseHeaderTimeoutMS) * time.Millisecond
		case provider.ResponseHeaderTimeoutMS < 0:
			headerTimeout = 0
		}
		transportKey := parsed.Scheme + "|" + parsed.Host + "|" + headerTimeout.String()
		transport, ok := transports[transportKey]
		if !ok {
			transport = http.DefaultTransport.(*http.Transport).Clone()
			transport.DialContext = (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext
			// Keep a deep pool of warm upstream connections: Claude Code fires
			// concurrent requests (main loop, subagents, count_tokens) and the
			// default of 2 idle conns per host forces constant TCP+TLS re-dials,
			// adding ~100-300ms and exposing each request to transient network
			// blips. Stale-idle-conn retries are handled transparently by the
			// transport, so a generous IdleConnTimeout is safe.
			transport.MaxIdleConns = 256
			transport.MaxIdleConnsPerHost = 32
			transport.IdleConnTimeout = 5 * time.Minute
			// Session resumption skips most of the TLS handshake on fresh
			// dials. ForceAttemptHTTP2 survives the custom config (Clone
			// keeps it true), so h2 negotiation is unaffected.
			transport.TLSClientConfig = &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(32)}
			transport.ResponseHeaderTimeout = headerTimeout
			transports[transportKey] = transport
		}
		clients[name] = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		if provider.AuthMode == "xai-oauth" {
			manager := xaiOAuthByPath[provider.AuthStatePath]
			if manager == nil {
				manager = newXAIOAuthManager(provider.AuthStatePath, &http.Client{Timeout: 30 * time.Second})
				xaiOAuthByPath[provider.AuthStatePath] = manager
			}
			xaiOAuth[name] = manager
		}
	}

	refreshDir := filepath.Dir(cfg.Metrics.Path)
	sharedTransports := make([]*http.Transport, 0, len(transports))
	for _, transport := range transports {
		sharedTransports = append(sharedTransports, transport)
	}
	// Anthropic account profiles must not share an HTTP/2 connection pool.
	// A single GOAWAY/TCP reset on a shared connection would otherwise abort
	// unrelated streams for every logged-in Claude subscription at once.
	claudeProfileClients := map[string]*http.Client{}
	if baseClient := clients[cfg.ClaudeUsage.Provider]; baseClient != nil {
		if baseTransport, ok := baseClient.Transport.(*http.Transport); ok {
			for _, profile := range configuredClaudeCredentialProfiles(cfg.ClaudeUsage) {
				transport := baseTransport.Clone()
				claudeProfileClients[profile.Name] = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
				sharedTransports = append(sharedTransports, transport)
			}
		}
	}
	server := &proxyServer{
		cfg:                  cfg,
		providers:            providers,
		clients:              clients,
		claudeProfileClients: claudeProfileClients,
		transports:           sharedTransports,
		metrics:              newMetricsStore(cfg.Metrics),
		adaptive: adaptiveState{
			performance: map[string]routePerformance{},
			sticky:      map[string]stickyRoute{},
		},
		claudeUse: claudeUsageCache{
			gate:       make(chan struct{}, 1),
			snapshots:  map[string]claudeUsageSnapshot{},
			refreshing: map[string]chan struct{}{},
		},
		claudeIdentities:      map[string]claudeIdentityCacheEntry{},
		claudeAccounts:        map[string]claudeAccountSticky{},
		claudeAccountInFlight: map[string]int{},
		routeInFlight:         map[string]int{},
		providerStates:        map[string]providerRuntimeState{},
		networkIncident:       sharedNetworkIncidentState{Signals: map[string]time.Time{}},
		claudeRefreshInFlight: map[string]bool{},
		claudeRefreshStatus:   filepath.Join(refreshDir, "claude-oauth-refresh-status.json"),
		xaiOAuth:              xaiOAuth,
	}
	server.claudeRefreshRunner = func(ctx context.Context, profile claudeUsageProfile) error {
		return server.refreshProfileCredentials(ctx, profile)
	}
	server.loadClaudeUsageCache()
	return server, nil
}

func newMetricsStore(cfg metricsConfig) *metricsStore {
	if cfg.MaxSamples <= 0 {
		cfg.MaxSamples = 10000
	}
	store := &metricsStore{
		path:         cfg.Path,
		maxSamples:   cfg.MaxSamples,
		samples:      newMetricsRing(cfg.MaxSamples),
		routeSamples: map[string]*metricsRing{},
	}
	store.load()
	return store
}

const metricsFlushDelay = 100 * time.Millisecond

func metricsRouteKey(sample metricsSample) string {
	return sample.Provider + "|" + sample.Upstream
}

func (m *metricsStore) routeCapacity() int {
	capacity := m.maxSamples
	if capacity > 1000 {
		capacity = 1000
	}
	return capacity
}

func (m *metricsStore) addMemoryLocked(sample metricsSample) {
	m.samples.add(sample)
	key := metricsRouteKey(sample)
	ring := m.routeSamples[key]
	if ring == nil {
		created := newMetricsRing(m.routeCapacity())
		ring = &created
		m.routeSamples[key] = ring
	}
	ring.add(sample)
}

func decodeMetricLines(raw []byte) []metricsSample {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	lines := bytes.Split(raw, []byte{'\n'})
	samples := make([]metricsSample, 0, len(lines))
	for _, line := range lines {
		var sample metricsSample
		if json.Unmarshal(line, &sample) == nil {
			samples = append(samples, sample)
		}
	}
	return samples
}

func (m *metricsStore) load() {
	if m == nil || m.path == "" {
		return
	}
	for _, path := range []string{m.path + ".1", m.path} {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		samples := decodeMetricLines(raw)
		for _, sample := range samples {
			m.addMemoryLocked(sample)
		}
		if path == m.path {
			m.persistedLines = len(samples)
		}
	}
}

func (m *metricsStore) record(sample metricsSample) {
	if m == nil || m.path == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.addMemoryLocked(sample)
	m.pending = append(m.pending, sample)
	if m.flushTimer == nil {
		m.flushTimer = time.AfterFunc(metricsFlushDelay, m.flush)
	}
}

func (m *metricsStore) openFileLocked() error {
	if m.file != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(m.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	m.file = file
	return nil
}

func (m *metricsStore) requeue(batch []metricsSample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.pending = append(batch, m.pending...)
	if m.flushTimer == nil {
		m.flushTimer = time.AfterFunc(time.Second, m.flush)
	}
}

func (m *metricsStore) rotateLocked() {
	if m.file != nil {
		_ = m.file.Close()
		m.file = nil
	}
	archive := m.path + ".1"
	_ = os.Remove(archive)
	if err := os.Rename(m.path, archive); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("metrics rotation failed path=%q error=%v", m.path, err)
		return
	}
	m.persistedLines = 0
}

func (m *metricsStore) flush() {
	if m == nil || m.path == "" {
		return
	}
	m.mu.Lock()
	batch := append([]metricsSample(nil), m.pending...)
	m.pending = nil
	m.flushTimer = nil
	m.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	var buffer bytes.Buffer
	written := 0
	for _, sample := range batch {
		encoded, err := json.Marshal(sample)
		if err != nil {
			continue
		}
		buffer.Write(encoded)
		buffer.WriteByte('\n')
		written++
	}
	if written == 0 {
		return
	}

	m.ioMu.Lock()
	defer m.ioMu.Unlock()
	if err := m.openFileLocked(); err != nil {
		log.Printf("metrics open failed path=%q error=%v", m.path, err)
		m.requeue(batch)
		return
	}
	if _, err := m.file.Write(buffer.Bytes()); err != nil {
		log.Printf("metrics write failed path=%q error=%v", m.path, err)
		_ = m.file.Close()
		m.file = nil
		m.requeue(batch)
		return
	}
	m.persistedLines += written
	if m.persistedLines >= m.maxSamples {
		m.rotateLocked()
	}
}

func (m *metricsStore) close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	if m.flushTimer != nil {
		m.flushTimer.Stop()
		m.flushTimer = nil
	}
	m.mu.Unlock()
	m.flush()
	m.ioMu.Lock()
	if m.file != nil {
		_ = m.file.Close()
		m.file = nil
	}
	m.ioMu.Unlock()
}

func (m *metricsStore) read(limit int) []metricsSample {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.samples.latest(limit)
}

func (m *metricsStore) readPerRoute(limit int) []metricsSample {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	samples := make([]metricsSample, 0, len(m.routeSamples)*limit)
	for _, ring := range m.routeSamples {
		samples = append(samples, ring.latest(limit)...)
	}
	return samples
}

func (s *proxyServer) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	limit := s.metrics.maxSamples
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 {
			http.Error(w, "invalid metrics limit", http.StatusBadRequest)
			return
		}
		if parsed < limit {
			limit = parsed
		}
	}
	samples := s.metrics.read(limit)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summarizeMetrics(samples, limit))
}

func summarizeMetrics(samples []metricsSample, limit int) metricsResponse {
	groups := make(map[metricsGroupKey]*metricsAggregate)
	for _, sample := range samples {
		if sample.RequestedModel == "" || sample.Provider == "" {
			continue
		}
		key := metricsGroupKey{
			Model:    sample.RequestedModel,
			Provider: sample.Provider,
			Upstream: sample.Upstream,
		}
		aggregate := groups[key]
		if aggregate == nil {
			aggregate = &metricsAggregate{
				Model:    key.Model,
				Provider: key.Provider,
				Upstream: key.Upstream,
			}
			groups[key] = aggregate
		}
		aggregate.Records++
		if sample.RecordKind == "count_tokens" {
			aggregate.CountTokenRequests++
			continue
		}
		if sample.RecordKind == "terminal" {
			if sample.Success {
				aggregate.TerminalSuccesses++
			} else {
				aggregate.TerminalErrors++
			}
			continue
		}
		if sample.RecordKind == "skip" || metricPolicySkip(sample.FailureReason) {
			aggregate.PolicySkips++
			continue
		}
		aggregate.Attempts++
		if sample.Success {
			aggregate.Successes++
		} else {
			aggregate.Failures++
		}
		if sample.IsFallback {
			aggregate.FallbackAttempts++
		}
		if sample.FallbackTrigger {
			aggregate.FallbackTriggers++
		}
		if sample.RecordKind == "completion" && sample.Success {
			aggregate.DeliveredResponses++
		}
		aggregate.latencies = append(aggregate.latencies, float64(sample.LatencyMS))
		if sample.TokensPerSecond > 0 {
			aggregate.tokensPerSecond = append(aggregate.tokensPerSecond, sample.TokensPerSecond)
		}
	}

	result := make([]metricsAggregate, 0, len(groups))
	for _, aggregate := range groups {
		if aggregate.Attempts > 0 {
			aggregate.SuccessRate = float64(aggregate.Successes) / float64(aggregate.Attempts)
		}
		if len(aggregate.latencies) > 0 {
			sort.Float64s(aggregate.latencies)
			var total float64
			for _, latency := range aggregate.latencies {
				total += latency
			}
			aggregate.AvgHeaderLatencyMS = total / float64(len(aggregate.latencies))
			aggregate.P50HeaderLatencyMS = aggregate.latencies[len(aggregate.latencies)/2]
			aggregate.P95HeaderLatencyMS = aggregate.latencies[int(float64(len(aggregate.latencies)-1)*0.95)]
		}
		if len(aggregate.tokensPerSecond) > 0 {
			sort.Float64s(aggregate.tokensPerSecond)
			aggregate.P50TokensPerSecond = aggregate.tokensPerSecond[len(aggregate.tokensPerSecond)/2]
		}
		aggregate.latencies = nil
		aggregate.tokensPerSecond = nil
		result = append(result, *aggregate)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].SuccessRate != result[j].SuccessRate {
			return result[i].SuccessRate > result[j].SuccessRate
		}
		if result[i].AvgHeaderLatencyMS != result[j].AvgHeaderLatencyMS {
			return result[i].AvgHeaderLatencyMS < result[j].AvgHeaderLatencyMS
		}
		return result[i].Provider+result[i].Model < result[j].Provider+result[j].Model
	})
	return metricsResponse{
		Limit:       limit,
		Samples:     len(samples),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Groups:      result,
	}
}

func metricPolicySkip(failureReason string) bool {
	switch failureReason {
	case "provider_wide_outage", "browser_usage_exhausted", "paid_fallback_suppressed", "provider_limited",
		"circuit_open", "ollama_reserved_for_deepseek", "chain_budget_reserved", "chain_budget_exhausted":
		return true
	}
	return strings.HasPrefix(failureReason, "claude_")
}

func traceElapsedMS(trace requestTrace) int64 {
	if trace.ChainStarted.IsZero() {
		return 0
	}
	return time.Since(trace.ChainStarted).Milliseconds()
}

func (s *proxyServer) recordAttempt(trace requestTrace, selected, candidate modelConfig, attempt int, started time.Time, status int, fallbackTrigger bool, failureReason string) {
	if selected.Requested == "" || s.metrics == nil {
		return
	}
	providerName := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
	recordKind := "attempt"
	if metricPolicySkip(failureReason) {
		recordKind = "skip"
	}
	s.metrics.record(metricsSample{
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:       trace.ID,
		Path:            trace.Path,
		RecordKind:      recordKind,
		Attempt:         attempt,
		CandidateCount:  trace.CandidateCount,
		ChainElapsedMS:  traceElapsedMS(trace),
		RequestedModel:  selected.Requested,
		Provider:        providerName,
		ClaudeProfile:   candidate.ClaudeProfile,
		Upstream:        candidate.Upstream,
		ResponseAlias:   firstNonEmpty(candidate.ResponseAlias, selected.Requested),
		Status:          status,
		LatencyMS:       time.Since(started).Milliseconds(),
		Success:         status >= http.StatusOK && status < http.StatusMultipleChoices && failureReason == "",
		IsFallback:      attempt > 0,
		FallbackTrigger: fallbackTrigger,
		FailureReason:   failureReason,
	})
}

// recordCompletion records the winning attempt after its response body has been
// fully streamed (or has failed mid-stream), so metrics reflect stream-level
// failures instead of only time-to-headers.
func (s *proxyServer) recordCompletion(trace requestTrace, candidate modelConfig, attempt int, started time.Time, status int, streamDuration time.Duration, streamErr error, stats *streamStats, failureOverride ...string) {
	s.recordCompletionKind(trace, candidate, attempt, started, status, streamDuration, streamErr, stats, "completion", failureOverride...)
}

func (s *proxyServer) recordCompletionKind(trace requestTrace, candidate modelConfig, attempt int, started time.Time, status int, streamDuration time.Duration, streamErr error, stats *streamStats, recordKind string, failureOverride ...string) {
	if candidate.Requested == "" || s.metrics == nil {
		return
	}
	failureReason := ""
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		failureReason = "http_status"
	}
	if len(failureOverride) > 0 && failureOverride[0] != "" {
		failureReason = failureOverride[0]
	}
	streamErrMsg := ""
	if streamErr != nil {
		failureReason = "stream_error"
		streamErrMsg = streamErr.Error()
	}
	if stats != nil {
		switch {
		case stats.ClientDisconnectAfterStall.Load():
			failureReason = "client_cancel_after_stall"
		case stats.ClientDisconnected.Load():
			failureReason = "client_cancel"
		case stats.ProtocolError.Load():
			failureReason = "protocol_error"
		case stats.StreamStalled.Load():
			failureReason = "stream_stall"
		}
	}
	clientDisconnected := stats != nil && stats.ClientDisconnected.Load()
	missingStop := stats != nil && stats.MissingStop.Load() && !clientDisconnected
	if missingStop {
		// The stream looked HTTP-200-clean but never carried a stop_reason;
		// the SSE filter converted it into a client-visible error. Count it
		// as a failure so gateway-side truncation is visible in metrics.
		failureReason = "protocol_error"
	}
	// Preserve historical headerLatencyMs (headers plus first-event priming).
	// responseHeadersMs and firstEventMs expose those phases independently.
	headerLatency := time.Since(started) - streamDuration
	if headerLatency < 0 {
		headerLatency = 0
	}
	sample := metricsSample{
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:      trace.ID,
		Path:           trace.Path,
		RecordKind:     recordKind,
		Attempt:        attempt,
		CandidateCount: trace.CandidateCount,
		ChainElapsedMS: traceElapsedMS(trace),
		RequestedModel: candidate.Requested,
		Provider:       firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider),
		ClaudeProfile:  candidate.ClaudeProfile,
		Upstream:       candidate.Upstream,
		ResponseAlias:  firstNonEmpty(candidate.ResponseAlias, candidate.Requested),
		Status:         status,
		LatencyMS:      headerLatency.Milliseconds(),
		Success: status >= http.StatusOK && status < http.StatusMultipleChoices && streamErr == nil && !clientDisconnected && !missingStop &&
			(stats == nil || !stats.ProtocolError.Load()),
		IsFallback:       attempt > 0,
		FailureReason:    failureReason,
		StreamDurationMS: streamDuration.Milliseconds(),
		StreamError:      streamErrMsg,
	}
	if stats != nil {
		sample.ReportedBackend, _ = stats.ReportedBackend.Load().(string)
		sample.Stream = stats.Events.Load() > 0
		sample.StreamBytes = stats.Bytes.Load()
		sample.StreamEvents = stats.Events.Load()
		sample.InputTokens = stats.InputTokens.Load()
		sample.OutputTokens = stats.OutputTokens.Load()
		if stats.InputEstimated.Load() {
			sample.InputTokenEstimate, sample.InputTokens = sample.InputTokens, 0
		}
		sample.CacheReadTokens = stats.CacheReadTokens.Load()
		sample.CacheWriteTokens = stats.CacheWriteTokens.Load()
		sample.MaximumEventGapMS = stats.MaximumGapNS.Load() / int64(time.Millisecond)
		if at := stats.FirstContentNS.Load(); at > 0 {
			sample.FirstContentMS = time.Unix(0, at).Sub(started).Milliseconds()
		}
		sample.TokensPerSecond = observedTokensPerSecond(sample.OutputTokens, streamDuration)
	}
	if !trace.Ingress.IsZero() {
		sample.PreparationMS = trace.ChainStarted.Sub(trace.Ingress).Milliseconds()
		sample.TotalMS = time.Since(trace.Ingress).Milliseconds()
	}
	sample.ResponseHeadersMS, sample.FirstEventMS = trace.HeadersMS, trace.FirstEventMS
	if trace.Timing != nil {
		sample.ConnectionMS, sample.DNSMS, sample.TLSMS = trace.Timing.connectionMS.Load(), trace.Timing.dnsMS.Load(), trace.Timing.tlsMS.Load()
	}
	s.metrics.record(sample)
}

func (s *proxyServer) recordRequestTerminal(requestID, path string, selected modelConfig, status int, reason string, started time.Time) {
	s.recordRequestTerminalOutcome(requestID, path, selected, status, reason, started, false)
}

func (s *proxyServer) recordRequestTerminalOutcome(requestID, path string, selected modelConfig, status int, reason string, started time.Time, success bool) {
	if s.metrics == nil {
		return
	}
	providerName := firstNonEmpty(selected.Provider, s.cfg.DefaultProvider)
	s.metrics.record(metricsSample{
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:      requestID,
		Path:           path,
		RecordKind:     "terminal",
		RequestedModel: selected.Requested,
		Provider:       providerName,
		Upstream:       selected.Upstream,
		ResponseAlias:  firstNonEmpty(selected.ResponseAlias, selected.Requested),
		Status:         status,
		LatencyMS:      time.Since(started).Milliseconds(),
		Success:        success,
		FailureReason:  reason,
	})
}

func routeKey(providerName string, candidate modelConfig) string {
	return providerName + "|" + candidate.Upstream
}

type routePerformanceAccumulator struct {
	attempts     int
	successes    int
	inputTokens  int64
	outputTokens int64
	latencies    []float64
	tps          []float64
	lastSeen     time.Time
}

func buildRoutePerformance(samples []metricsSample) map[string]routePerformance {
	accumulators := map[string]*routePerformanceAccumulator{}
	for _, sample := range samples {
		if sample.Provider == "" || excludeFromRoutePerformance(sample) {
			continue
		}
		key := sample.Provider + "|" + sample.Upstream
		acc := accumulators[key]
		if acc == nil {
			acc = &routePerformanceAccumulator{}
			accumulators[key] = acc
		}
		acc.attempts++
		if sample.Success {
			acc.successes++
		}
		if sample.LatencyMS > 0 {
			acc.latencies = append(acc.latencies, float64(sample.LatencyMS))
		}
		if sample.TokensPerSecond > 0 {
			acc.tps = append(acc.tps, sample.TokensPerSecond)
		}
		acc.inputTokens += sample.InputTokens
		acc.outputTokens += sample.OutputTokens
		if parsed, err := time.Parse(time.RFC3339Nano, sample.Timestamp); err == nil && parsed.After(acc.lastSeen) {
			acc.lastSeen = parsed
		}
	}
	performance := make(map[string]routePerformance, len(accumulators))
	for key, acc := range accumulators {
		perf := routePerformance{
			Samples:      acc.attempts,
			InputTokens:  acc.inputTokens,
			OutputTokens: acc.outputTokens,
			LastSeen:     acc.lastSeen,
		}
		if acc.attempts > 0 {
			perf.SuccessRate = float64(acc.successes) / float64(acc.attempts)
		}
		if len(acc.latencies) > 0 {
			sort.Float64s(acc.latencies)
			perf.P50HeaderMS = acc.latencies[len(acc.latencies)/2]
			perf.P95HeaderMS = acc.latencies[int(float64(len(acc.latencies)-1)*0.95)]
		}
		if len(acc.tps) > 0 {
			sort.Float64s(acc.tps)
			perf.P50TPS = acc.tps[len(acc.tps)/2]
		}
		performance[key] = perf
	}
	return performance
}

// Policy/quota skips describe account state, not model transport quality. Keep
// them in audit metrics, but never poison stability after a quota reset.
func excludeFromRoutePerformance(sample metricsSample) bool {
	if sample.RecordKind != "" && sample.RecordKind != "completion" && sample.RecordKind != "attempt" {
		return true
	}
	if sample.RecordKind == "attempt" && sample.Success {
		return true // Successful attempts are represented by their completion.
	}
	if sample.RecordKind == "skip" || metricPolicySkip(sample.FailureReason) {
		return true
	}
	if sample.Status == http.StatusTooManyRequests || sample.Status == http.StatusPaymentRequired {
		return true
	}
	switch sample.FailureReason {
	case "circuit_open", "provider_limited", "client_cancel", "client_cancel_after_stall", "quota_or_rate_limit", "ollama_reserved_for_deepseek",
		"thinking_signature_mismatch", "thinking_history_incompatible", "client_invalid", "route_incompatible", "local_network", "chain_deadline":
		return true
	}
	if sample.Status == http.StatusUnauthorized || sample.Status == http.StatusForbidden {
		return true
	}
	return strings.HasPrefix(sample.FailureReason, "claude_")
}

func (s *proxyServer) adaptivePerformance() map[string]routePerformance {
	if !s.cfg.AdaptiveRouting.Enabled {
		return nil
	}
	refreshEvery := time.Duration(s.cfg.AdaptiveRouting.RefreshSeconds) * time.Second
	s.adaptive.mu.Lock()
	defer s.adaptive.mu.Unlock()
	if len(s.adaptive.performance) > 0 && time.Since(s.adaptive.refreshedAt) < refreshEvery {
		return s.adaptive.performance
	}
	samples := s.metrics.readPerRoute(s.cfg.AdaptiveRouting.WindowSamples)
	now := time.Now()
	cutoff := now.Add(-30 * time.Minute)
	fresh := make([]metricsSample, 0, len(samples))
	for _, sample := range samples {
		at, err := time.Parse(time.RFC3339Nano, sample.Timestamp)
		if err == nil && !at.Before(cutoff) && !at.After(now) && sample.RecordKind != "" {
			fresh = append(fresh, sample)
		}
	}
	performance := buildRoutePerformance(fresh)
	s.adaptive.performance = performance
	s.adaptive.refreshedAt = time.Now()
	return performance
}

type rankedCandidate struct {
	model    modelConfig
	key      string
	phase    int
	tier     int
	score    float64
	perf     routePerformance
	blocked  bool
	original int
}

func (s *proxyServer) providerTier(providerName string) int {
	if tier, ok := s.cfg.AdaptiveRouting.ProviderTiers[providerName]; ok {
		return tier
	}
	return 50
}

func (s *proxyServer) candidateTier(candidate modelConfig, providerName string) int {
	if candidate.Tier != nil {
		return *candidate.Tier
	}
	return s.providerTier(providerName)
}

func (s *proxyServer) routeScore(providerName, key string, perf routePerformance) float64 {
	stability := 0.8
	ttft := 0.5
	tps := 0.35
	if perf.Samples >= s.cfg.AdaptiveRouting.MinSamples {
		// Bayesian prior prevents tiny sample sets from looking perfectly stable.
		stability = (perf.SuccessRate*float64(perf.Samples) + 8) / float64(perf.Samples+10)
		if perf.P50HeaderMS > 0 {
			ttft = math.Max(1-perf.P50HeaderMS/15000, 0)
		}
		if perf.P50TPS > 0 {
			tps = math.Min(perf.P50TPS/50, 1)
		}
	}
	score := 35*stability*s.cfg.AdaptiveRouting.StabilityWeight/8 +
		15*ttft*s.cfg.AdaptiveRouting.LatencyWeight +
		25*tps*s.cfg.AdaptiveRouting.TPSWeight/0.35
	score += s.cfg.AdaptiveRouting.QualityBonus[key]
	score -= s.cfg.AdaptiveRouting.UsageCost[key] * s.cfg.AdaptiveRouting.UsageWeight
	utilization, known := s.providerUsageUtilization(providerName)
	if !known {
		utilization = 0.5
	}
	score -= utilization * s.cfg.AdaptiveRouting.HeadroomWeight
	score -= float64(s.routeInFlightCount(key)) * 8
	return score
}

func (s *proxyServer) routeInFlightCount(key string) int {
	s.routeLoadMu.Lock()
	defer s.routeLoadMu.Unlock()
	return s.routeInFlight[key]
}

func (s *proxyServer) reserveRoute(providerName string, candidate modelConfig) func() {
	key := routeKey(providerName, candidate)
	s.routeLoadMu.Lock()
	if s.routeInFlight == nil {
		s.routeInFlight = map[string]int{}
	}
	s.routeInFlight[key]++
	s.routeLoadMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.routeLoadMu.Lock()
			if s.routeInFlight[key] > 1 {
				s.routeInFlight[key]--
			} else {
				delete(s.routeInFlight, key)
			}
			s.routeLoadMu.Unlock()
		})
	}
}

const (
	browserUsageReloadInterval = 10 * time.Second
	browserUsageFreshDuration  = 3 * time.Minute
)

func canonicalBrowserUsageProvider(providerName string) string {
	if strings.HasPrefix(providerName, "opencode-go") {
		return "opencode-go"
	}
	if providerName == "commandcode" {
		return providerName
	}
	return ""
}

// browserUsageForProvider returns both visibility and routing freshness. A
// stale available value cannot influence scoring. Exhaustion uses asymmetric
// retention in browserUsageExhausted so collector blips cannot reopen spend.
func (s *proxyServer) browserUsageForProvider(providerName string) (browserUsageProvider, bool, bool) {
	if s.cfg.Definition != nil {
		return s.configuredBrowserUsage(providerName)
	}
	providerKey := canonicalBrowserUsageProvider(providerName)
	if providerKey == "" || s.browserUsagePath == "" {
		return browserUsageProvider{}, false, false
	}

	now := s.accountNow()
	s.browserUse.mu.Lock()
	defer s.browserUse.mu.Unlock()
	if s.browserUse.loadedAt.IsZero() || now.Sub(s.browserUse.loadedAt) >= browserUsageReloadInterval {
		s.browserUse.loadedAt = now
		body, err := os.ReadFile(s.browserUsagePath)
		if err != nil {
			s.browserUse.lastError = err.Error()
		} else {
			var snapshot browserUsageSnapshot
			if err := json.Unmarshal(body, &snapshot); err != nil {
				s.browserUse.lastError = err.Error()
			} else if snapshot.Version != 1 || snapshot.Providers == nil {
				s.browserUse.lastError = "unsupported browser usage snapshot"
			} else {
				s.browserUse.snapshot = snapshot
				s.browserUse.hasValue = true
				s.browserUse.lastError = ""
			}
		}
	}
	if !s.browserUse.hasValue {
		return browserUsageProvider{}, false, false
	}
	provider, found := s.browserUse.snapshot.Providers[providerKey]
	if !found {
		return browserUsageProvider{}, false, false
	}
	age := now.Sub(provider.FetchedAt)
	fresh := provider.Status == "ok" && provider.Utilization != nil && !provider.FetchedAt.IsZero() && age >= -time.Minute && age <= browserUsageFreshDuration
	return provider, true, fresh
}

func (s *proxyServer) browserUsageExhausted(providerName string) (bool, string) {
	provider, found, fresh := s.browserUsageForProvider(providerName)
	if !found {
		return false, ""
	}
	exhausted := provider.Utilization != nil && *provider.Utilization >= 1
	for _, window := range provider.Windows {
		exhausted = exhausted || window.PercentUsed >= 100
	}
	if !exhausted {
		return false, ""
	}
	if fresh {
		return true, "browser_usage_limit_exhausted"
	}
	base := provider.LastSuccess
	if base.IsZero() {
		base = provider.FetchedAt
	}
	latestReset := time.Time{}
	for _, window := range provider.Windows {
		if window.PercentUsed < 100 {
			continue
		}
		if duration, ok := parseBrowserResetDuration(window.Reset); ok {
			resetAt := base.Add(duration)
			if resetAt.After(latestReset) {
				latestReset = resetAt
			}
		}
	}
	if latestReset.IsZero() && !base.IsZero() {
		latestReset = base.Add(24 * time.Hour)
	}
	if latestReset.IsZero() || s.accountNow().After(latestReset.Add(2*time.Minute)) {
		return false, ""
	}
	return true, "browser_usage_limit_exhausted_stale"
}

func parseBrowserResetDuration(label string) (time.Duration, bool) {
	cleaned := strings.NewReplacer("·", " ", ",", " ", ";", " ", "(", " ", ")", " ").Replace(strings.ToLower(label))
	fields := strings.Fields(cleaned)
	start := -1
	for index, field := range fields {
		if field == "in" {
			start = index + 1
			break
		}
	}
	if start < 0 || start >= len(fields) {
		return 0, false
	}
	var total time.Duration
	parsedAny := false
	for index := start; index < len(fields); index++ {
		field := fields[index]
		valueText := field
		unitText := ""
		for split := 0; split < len(field); split++ {
			if field[split] < '0' || field[split] > '9' {
				valueText = field[:split]
				unitText = field[split:]
				break
			}
		}
		value, err := strconv.Atoi(valueText)
		if err != nil {
			continue
		}
		if unitText == "" && index+1 < len(fields) {
			unitText = fields[index+1]
			index++
		}
		unitText = strings.ToLower(strings.TrimSpace(unitText))
		if len(unitText) > 1 {
			unitText = strings.TrimSuffix(unitText, "s")
		}
		var unit time.Duration
		switch unitText {
		case "d", "day":
			unit = 24 * time.Hour
		case "h", "hour":
			unit = time.Hour
		case "m", "min", "minute":
			unit = time.Minute
		case "s", "sec", "second":
			unit = time.Second
		default:
			continue
		}
		total += time.Duration(value) * unit
		parsedAny = true
	}
	return total, parsedAny && total > 0
}

func (s *proxyServer) providerUsageUtilization(providerName string) (float64, bool) {
	if providerName == s.cfg.OllamaUsage.Provider {
		s.ollamaUse.mu.Lock()
		if s.ollamaUse.hasValue {
			utilization := math.Max(s.ollamaUse.snapshot.Session.Usage, s.ollamaUse.snapshot.Weekly.Usage)
			s.ollamaUse.mu.Unlock()
			return utilization, true
		}
		s.ollamaUse.mu.Unlock()
	}
	if providerName == s.cfg.ClineUsage.Provider {
		s.clineUse.mu.Lock()
		if s.clineUse.hasValue {
			utilization := 0.0
			for _, window := range s.clineUse.snapshot.Windows {
				utilization = math.Max(utilization, window.PercentUsed/100)
			}
			s.clineUse.mu.Unlock()
			return math.Min(utilization, 1), true
		}
		s.clineUse.mu.Unlock()
	}
	if providerName == s.xaiUsageProviderName() {
		s.xaiUse.mu.Lock()
		if s.xaiUse.hasValue && time.Since(s.xaiUse.snapshot.FetchedAt) <= 15*time.Minute {
			utilization := 0.0
			for _, window := range s.xaiUse.snapshot.Windows {
				utilization = math.Max(utilization, window.PercentUsed/100)
			}
			s.xaiUse.mu.Unlock()
			return math.Min(utilization, 1), true
		}
		s.xaiUse.mu.Unlock()
	}
	if provider, _, fresh := s.browserUsageForProvider(providerName); fresh && provider.Utilization != nil {
		return math.Min(math.Max(*provider.Utilization, 0), 1), true
	}
	s.providerStateMu.Lock()
	state := s.providerStates[providerName]
	s.providerStateMu.Unlock()
	utilization := 0.0
	known := false
	for header, raw := range state.RateLimitInfo {
		if !strings.Contains(strings.ToLower(header), "utilization") {
			continue
		}
		if parsed, ok := parseUtilization(raw); ok {
			utilization = math.Max(utilization, parsed)
			known = true
		}
	}
	return utilization, known
}

func parseUtilization(raw string) (float64, bool) {
	value := strings.TrimSpace(strings.Split(raw, ",")[0])
	percentage := strings.HasSuffix(value, "%")
	value = strings.TrimSpace(strings.TrimSuffix(value, "%"))
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed < 0 {
		return 0, false
	}
	if percentage || parsed > 1 {
		parsed /= 100
	}
	if parsed > 1 {
		return 0, false
	}
	return parsed, true
}

func candidateModelFamily(candidate modelConfig) string {
	value := strings.ToLower(strings.Join([]string{candidate.Provider, candidate.Upstream, candidate.ResponseAlias}, "|"))
	switch {
	case strings.Contains(value, "glm"):
		return "glm"
	case strings.Contains(value, "deepseek"):
		return "deepseek"
	case strings.Contains(value, "claude") || candidate.Provider == "anthropic":
		return "anthropic"
	case strings.Contains(value, "kimi"):
		return "kimi"
	default:
		return value
	}
}

// candidateRoutePhase preserves cost boundaries before adaptive performance.
// A direct alias always starts inside its requested family. Cross-family
// subscription fallbacks may then precede same-family pay-as-you-go legs.
func (s *proxyServer) candidateRoutePhase(selected, candidate modelConfig, providerName string) int {
	requestedFamily := s.explicitConversationFamily(selected)
	if requestedFamily == "" {
		return 0
	}
	candidateFamily := candidateModelFamily(candidate)
	tier := s.candidateTier(candidate, providerName)
	paid := tier >= 95

	if requestedFamily != "anthropic" {
		switch {
		case candidateFamily == requestedFamily && !paid:
			return 0
		case providerName == "xai-oauth":
			return 10
		case candidateFamily == requestedFamily:
			return 20
		default:
			return 30
		}
	}

	if candidateFamily == "anthropic" {
		return 0
	}
	requested := strings.ToLower(selected.Requested)
	switch {
	case strings.Contains(requested, "fable"):
		switch {
		case candidateFamily == "kimi" && !paid:
			return 10
		case providerName == "xai-oauth":
			return 20
		case candidateFamily == "kimi":
			return 30
		default:
			return 40
		}
	case strings.Contains(requested, "opus"):
		switch {
		case candidateFamily == "glm" && !paid:
			return 10
		case providerName == "xai-oauth":
			return 20
		case candidateFamily == "glm":
			return 30
		default:
			return 40
		}
	default: // Sonnet and worker slots.
		switch {
		case candidateFamily == "glm" && !paid:
			return 10
		case candidateFamily == "deepseek" && !paid:
			return 20
		case providerName == "xai-oauth":
			return 30
		case candidateFamily == "glm":
			return 40
		default:
			return 50
		}
	}
}

// explicitConversationFamily identifies model slots with a fixed family
// contract. Adaptive worker selection must not preemptively replace Opus,
// Fable, or Sonnet with open-source models while a Claude subscription account
// remains below its configured reserve threshold.
func (s *proxyServer) explicitConversationFamily(selected modelConfig) string {
	if selected.Requested == "" || (selected.Provider == "" && selected.Upstream == "") {
		return ""
	}
	return candidateModelFamily(selected)
}

func (s *proxyServer) conversationFamilyTTL() time.Duration {
	seconds := s.cfg.AdaptiveRouting.StickySeconds
	if seconds <= 0 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func (s *proxyServer) explicitAnthropicPrimary(selected modelConfig) bool {
	return s.explicitConversationFamily(selected) == "anthropic"
}

// routingStickyKey fingerprints only stable conversation seed data. No prompt
// text leaves process memory or appears in logs/metrics.
func routingStickyKey(selected modelConfig, payload map[string]any) string {
	if selected.Requested == "" || payload == nil {
		return selected.Requested
	}
	seed := map[string]any{"model": selected.Requested}
	if metadata, ok := payload["metadata"].(map[string]any); ok {
		if userID, ok := metadata["user_id"].(string); ok && userID != "" {
			seed["user_id"] = userID
		}
	}
	if messages, ok := payload["messages"].([]any); ok && len(messages) > 0 {
		seed["first_message"] = messages[0]
	}
	encoded, err := json.Marshal(seed)
	if err != nil {
		return selected.Requested
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%s|%x", selected.Requested, digest[:12])
}

func detectThinkingHistory(payload map[string]any) thinkingHistoryKind {
	if payload == nil {
		return thinkingHistoryNone
	}
	hasAnthropic := false
	hasNonAnthropic := false
	messages, _ := payload["messages"].([]any)
	for _, item := range messages {
		message, _ := item.(map[string]any)
		if role, _ := message["role"].(string); role != "assistant" {
			continue
		}
		blocks, _ := message["content"].([]any)
		for _, rawBlock := range blocks {
			block, _ := rawBlock.(map[string]any)
			blockType, _ := block["type"].(string)
			switch blockType {
			case "redacted_thinking":
				hasAnthropic = true
			case "thinking":
				signature, _ := block["signature"].(string)
				if len(signature) >= 128 {
					hasAnthropic = true
				} else {
					hasNonAnthropic = true
				}
			}
		}
	}
	switch {
	case hasAnthropic && hasNonAnthropic:
		return thinkingHistoryMixed
	case hasAnthropic:
		return thinkingHistoryAnthropic
	case hasNonAnthropic:
		return thinkingHistoryNonAnthropic
	default:
		return thinkingHistoryNone
	}
}

func providerAcceptsAnthropicThinking(providerName string) bool {
	return providerName == "anthropic"
}

func thinkingHistoryNeedsSanitization(history thinkingHistoryKind, providerName string) bool {
	switch history {
	case thinkingHistoryAnthropic:
		return !providerAcceptsAnthropicThinking(providerName)
	case thinkingHistoryNonAnthropic:
		return providerAcceptsAnthropicThinking(providerName)
	case thinkingHistoryMixed:
		return true
	default:
		return false
	}
}

// stripHistoricalThinking removes provider-bound opaque signatures only.
// Text and tool-use/result blocks survive unchanged, preserving tool state.
func stripHistoricalThinking(payload map[string]any) {
	messages, _ := payload["messages"].([]any)
	filteredMessages := make([]any, 0, len(messages))
	for _, item := range messages {
		message, ok := item.(map[string]any)
		if !ok {
			filteredMessages = append(filteredMessages, item)
			continue
		}
		role, _ := message["role"].(string)
		blocks, hasBlocks := message["content"].([]any)
		if role != "assistant" || !hasBlocks {
			filteredMessages = append(filteredMessages, message)
			continue
		}
		filteredBlocks := make([]any, 0, len(blocks))
		for _, rawBlock := range blocks {
			block, _ := rawBlock.(map[string]any)
			blockType, _ := block["type"].(string)
			if blockType == "thinking" || blockType == "redacted_thinking" {
				continue
			}
			filteredBlocks = append(filteredBlocks, rawBlock)
		}
		if len(filteredBlocks) == 0 {
			continue
		}
		message["content"] = filteredBlocks
		filteredMessages = append(filteredMessages, message)
	}
	payload["messages"] = filteredMessages
}

func (s *proxyServer) rankCandidates(selected modelConfig, candidates []modelConfig) []modelConfig {
	return s.rankCandidatesForRequest(selected, candidates, selected.Requested, true, thinkingHistoryNone)
}

// rankCandidatesForRequest keeps one conversation on one model family. A GLM
// thread therefore fails over to another GLM route (including Vercel) before
// trying DeepSeek or Claude. Exploration is limited to history-free requests;
// switching a signed tool conversation mid-turn invalidates thinking blocks.
func (s *proxyServer) rankCandidatesForRequest(selected modelConfig, candidates []modelConfig, stickyKey string, allowExplore bool, history thinkingHistoryKind) []modelConfig {
	if s.cfg.Definition != nil {
		return s.rankConfiguredCandidates(candidates, stickyKey)
	}
	if !s.cfg.AdaptiveRouting.Enabled || len(candidates) < 2 {
		return candidates
	}
	if stickyKey == "" {
		stickyKey = selected.Requested
	}
	s.adaptive.mu.Lock()
	sticky := s.adaptive.sticky[stickyKey]
	s.adaptive.mu.Unlock()
	preferredFamily := sticky.Family
	if !sticky.FamilyUntil.IsZero() && time.Now().After(sticky.FamilyUntil) {
		preferredFamily = ""
		sticky.Family = ""
	}
	if preferredFamily == "" && history == thinkingHistoryAnthropic {
		preferredFamily = "anthropic"
	}
	performance := s.adaptivePerformance()
	ranked := make([]rankedCandidate, 0, len(candidates))
	for index, candidate := range candidates {
		providerName := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
		key := routeKey(providerName, candidate)
		blocked, _ := s.providerBlocked(providerName)
		browserExhausted, _ := s.browserUsageExhausted(providerName)
		reservedOut, _ := s.ollamaCandidateReservedOut(candidate)
		blocked = blocked || browserExhausted || reservedOut || s.circuitOpen(providerName, candidate)
		score := s.routeScore(providerName, key, performance[key])
		if blocked {
			score -= 10000
		}
		ranked = append(ranked, rankedCandidate{
			model: candidate, key: key, phase: s.candidateRoutePhase(selected, candidate, providerName), tier: s.candidateTier(candidate, providerName), score: score,
			perf: performance[key], blocked: blocked, original: index,
		})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].phase != ranked[j].phase {
			return ranked[i].phase < ranked[j].phase
		}
		if preferredFamily != "" {
			iMatches := candidateModelFamily(ranked[i].model) == preferredFamily
			jMatches := candidateModelFamily(ranked[j].model) == preferredFamily
			if iMatches != jMatches {
				return iMatches
			}
		}
		if ranked[i].tier != ranked[j].tier {
			return ranked[i].tier < ranked[j].tier
		}
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].original < ranked[j].original
	})
	if preferredFamily == "" && history == thinkingHistoryNonAnthropic && len(ranked) > 0 {
		// After a proxy restart no in-memory conversation affinity exists. Use
		// the best current non-Anthropic route as the family anchor, then keep
		// its same-model Vercel leg ahead of cross-family fallbacks immediately.
		for index := range ranked {
			if !ranked[index].blocked && candidateModelFamily(ranked[index].model) != "anthropic" {
				preferredFamily = candidateModelFamily(ranked[index].model)
				break
			}
		}
		if preferredFamily != "" {
			sort.SliceStable(ranked, func(i, j int) bool {
				if ranked[i].phase != ranked[j].phase {
					return ranked[i].phase < ranked[j].phase
				}
				iMatches := candidateModelFamily(ranked[i].model) == preferredFamily
				jMatches := candidateModelFamily(ranked[j].model) == preferredFamily
				return iMatches && !jMatches
			})
		}
	}

	// Controlled exploration keeps subscription legs measured without paid
	// synthetic probes. Only the cheapest healthy tier participates.
	requestNumber := s.adaptive.requests.Add(1)
	if allowExplore && sticky.Family == "" && s.cfg.AdaptiveRouting.ExploreEvery > 0 && requestNumber%uint64(s.cfg.AdaptiveRouting.ExploreEvery) == 0 {
		bestExplore := -1
		for index := range ranked {
			if ranked[index].phase != ranked[0].phase || ranked[index].tier != ranked[0].tier || ranked[index].blocked {
				continue
			}
			if bestExplore < 0 || ranked[index].perf.Samples < ranked[bestExplore].perf.Samples ||
				(ranked[index].perf.Samples == ranked[bestExplore].perf.Samples && ranked[index].perf.LastSeen.Before(ranked[bestExplore].perf.LastSeen)) {
				bestExplore = index
			}
		}
		if bestExplore > 0 {
			ranked[0], ranked[bestExplore] = ranked[bestExplore], ranked[0]
		}
		if stickyKey != "" {
			s.adaptive.mu.Lock()
			s.adaptive.sticky[stickyKey] = stickyRoute{
				Key: ranked[0].key, Family: candidateModelFamily(ranked[0].model),
				Until: time.Now().Add(time.Duration(s.cfg.AdaptiveRouting.StickySeconds) * time.Second), FamilyUntil: time.Now().Add(s.conversationFamilyTTL()),
			}
			s.adaptive.mu.Unlock()
		}
	} else if stickyKey != "" {
		// Keep a winning model/provider stable across adjacent tool turns. A
		// challenger must beat it by SwitchMargin; hard failure bypasses dwell.
		s.adaptive.mu.Lock()
		sticky = s.adaptive.sticky[stickyKey]
		stickyIndex := -1
		for index := range ranked {
			if ranked[index].key == sticky.Key {
				stickyIndex = index
				break
			}
		}
		if stickyIndex > 0 && time.Now().Before(sticky.Until) && !ranked[stickyIndex].blocked && ranked[stickyIndex].phase == ranked[0].phase &&
			ranked[stickyIndex].tier == ranked[0].tier && ranked[0].score-ranked[stickyIndex].score < s.cfg.AdaptiveRouting.SwitchMargin {
			ranked[0], ranked[stickyIndex] = ranked[stickyIndex], ranked[0]
		}
		family := sticky.Family
		if family == "" {
			for index := range ranked {
				if !ranked[index].blocked {
					family = candidateModelFamily(ranked[index].model)
					break
				}
			}
		}
		s.adaptive.sticky[stickyKey] = stickyRoute{
			Key: ranked[0].key, Family: family,
			Until: time.Now().Add(time.Duration(s.cfg.AdaptiveRouting.StickySeconds) * time.Second), FamilyUntil: time.Now().Add(s.conversationFamilyTTL()),
		}
		s.adaptive.mu.Unlock()
	}
	if requestNumber%256 == 0 {
		now := time.Now()
		s.adaptive.mu.Lock()
		for key, route := range s.adaptive.sticky {
			if !route.FamilyUntil.IsZero() && now.After(route.FamilyUntil) && now.After(route.Until) {
				delete(s.adaptive.sticky, key)
			}
		}
		s.adaptive.mu.Unlock()
	}

	result := make([]modelConfig, len(ranked))
	for index := range ranked {
		result[index] = ranked[index].model
	}
	return result
}

func (s *proxyServer) providerBlocked(providerName string) (bool, providerRuntimeState) {
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state, exists := s.providerStates[providerName]
	if !exists {
		return false, providerRuntimeState{}
	}
	now := s.accountNow()
	if !state.BlockedUntil.IsZero() && !now.Before(state.BlockedUntil) {
		state.BlockedUntil = time.Time{}
		state.Reason = ""
	}
	if !state.QuarantinedUntil.IsZero() && !now.Before(state.QuarantinedUntil) {
		state.QuarantinedUntil = time.Time{}
		state.Recovering = true
	}
	if s.providerStates == nil {
		s.providerStates = map[string]providerRuntimeState{}
	}
	s.providerStates[providerName] = state
	blocked := !state.BlockedUntil.IsZero()
	if !state.QuarantinedUntil.IsZero() {
		if !blocked || state.QuarantinedUntil.After(state.BlockedUntil) {
			state.BlockedUntil = state.QuarantinedUntil
			state.Reason = "instability_" + state.QuarantineReason
		}
		blocked = true
	}
	return blocked, state
}

func claudeQuotaFamily(candidate modelConfig) string {
	value := strings.ToLower(strings.Join([]string{candidate.Requested, candidate.Upstream, candidate.ResponseAlias}, "|"))
	if strings.Contains(value, "fable") {
		return "fable"
	}
	return ""
}

func (s *proxyServer) claudeModelQuotaBlockKey(runtimeProviderName string, candidate modelConfig) string {
	providerName := s.cfg.ClaudeUsage.Provider
	if providerName == "" || (runtimeProviderName != providerName && !strings.HasPrefix(runtimeProviderName, providerName+"@")) {
		return ""
	}
	if family := claudeQuotaFamily(candidate); family != "" {
		return runtimeProviderName + "#" + family
	}
	return ""
}

func (s *proxyServer) providerBlockForCandidate(runtimeProviderName string, candidate modelConfig) (bool, providerRuntimeState) {
	if blocked, state := s.providerBlocked(runtimeProviderName); blocked {
		return true, state
	}
	if modelKey := s.claudeModelQuotaBlockKey(runtimeProviderName, candidate); modelKey != "" {
		return s.providerBlocked(modelKey)
	}
	return false, providerRuntimeState{}
}

func (s *proxyServer) quotaBlockKeyForResponse(runtimeProviderName string, candidate modelConfig, header http.Header) string {
	modelKey := s.claudeModelQuotaBlockKey(runtimeProviderName, candidate)
	if modelKey == "" {
		return runtimeProviderName
	}
	fiveHour := strings.ToLower(header.Get("Anthropic-Ratelimit-Unified-5h-Status"))
	sevenDay := strings.ToLower(header.Get("Anthropic-Ratelimit-Unified-7d-Status"))
	if fiveHour == "rejected" || sevenDay == "rejected" {
		return runtimeProviderName
	}
	// Anthropic does not consistently return the model-specific quota header on
	// Fable 429 responses. Keep ambiguous Fable failures model-scoped so an
	// exhausted Fable bucket cannot disable Sonnet or Opus on the same account.
	return modelKey
}

func (s *proxyServer) excessiveTTFB(elapsed time.Duration) bool {
	return s.cfg.Quarantine.Enabled && elapsed >= time.Duration(s.cfg.Quarantine.ExcessiveTTFBMS)*time.Millisecond
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func (s *proxyServer) candidateResponseHeaderBudget(providerName string, candidate modelConfig, body []byte) time.Duration {
	provider := s.cfg.Providers[providerName]
	providerMaximum := defaultResponseHeaderTimeout
	if provider.ResponseHeaderTimeoutMS > 0 {
		providerMaximum = time.Duration(provider.ResponseHeaderTimeoutMS) * time.Millisecond
	} else if provider.ResponseHeaderTimeoutMS < 0 {
		providerMaximum = 2 * defaultResponseHeaderTimeout
	}
	base := providerMaximum
	floor := 8 * time.Second
	upstream := strings.ToLower(candidate.Upstream)
	variant := firstNonEmpty(provider.Variant, providerName)
	switch {
	case providerName == s.cfg.ClaudeUsage.Provider && strings.Contains(upstream, "fable"):
		base = minDuration(base, 30*time.Second)
		floor = 12 * time.Second
	case providerName == s.cfg.ClaudeUsage.Provider:
		base = minDuration(base, 20*time.Second)
	case variant == "bigmodel" && strings.Contains(upstream, "glm-5.3") && !strings.Contains(upstream, "flash"):
		base = minDuration(base, 25*time.Second)
		floor = 10 * time.Second
	case variant == "xai-oauth" || variant == "xai-subscription":
		base = minDuration(base, 15*time.Second)
	case variant == "vercel-kimi" || variant == "vercel-gateway" && strings.Contains(upstream, "kimi"):
		base = minDuration(base, 20*time.Second)
	case variant == "opencode-go" && strings.Contains(upstream, "glm"):
		base = minDuration(base, 60*time.Second)
		floor = 20 * time.Second
	}
	if floor > base {
		floor = base
	}
	if performance := s.adaptivePerformance(); performance != nil {
		perf := performance[routeKey(providerName, candidate)]
		if perf.Samples >= s.cfg.AdaptiveRouting.MinSamples && perf.P95HeaderMS > 0 {
			learned := time.Duration(perf.P95HeaderMS*2)*time.Millisecond + 2*time.Second
			base = minDuration(base, maxDuration(floor, learned))
		}
	}
	estimatedInput := estimateInputTokens(body)
	switch {
	case estimatedInput > 300000:
		base *= 3
	case estimatedInput > 150000:
		base *= 2
	case estimatedInput > 75000:
		base = base * 3 / 2
	}
	return minDuration(base, providerMaximum)
}

const (
	defaultFallbackChainHeaderBudget = 60 * time.Second
	fallbackRouteReserve             = 10 * time.Second
)

func fallbackChainHeaderBudget(body []byte) time.Duration {
	budget := defaultFallbackChainHeaderBudget
	switch estimatedInput := estimateInputTokens(body); {
	case estimatedInput > 300000:
		budget *= 3
	case estimatedInput > 150000:
		budget *= 2
	case estimatedInput > 75000:
		budget = budget * 3 / 2
	}
	return budget
}

func (s *proxyServer) hasLaterUnmeteredProvider(attempts []claudeCandidateAttempt, index int, providerName string) bool {
	for next := index + 1; next < len(attempts); next++ {
		candidate := attempts[next].model
		nextProvider := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
		if nextProvider == providerName {
			continue
		}
		if s.candidateTier(candidate, nextProvider) < 95 {
			return true
		}
	}
	return false
}

func (s *proxyServer) remainingAttemptHeaderBudget(trace requestTrace, attempts []claudeCandidateAttempt, index int, providerName string) (time.Duration, bool) {
	remaining := trace.ChainBudget
	if !trace.ChainStarted.IsZero() {
		remaining = trace.ChainStarted.Add(trace.ChainBudget).Sub(time.Now())
	}
	reserve := time.Duration(0)
	if s.hasLaterUnmeteredProvider(attempts, index, providerName) {
		reserve = fallbackRouteReserve
	}
	if remaining <= reserve {
		return 0, reserve > 0
	}
	return remaining - reserve, false
}

func (s *proxyServer) streamEventStallTimeout() time.Duration {
	if !s.cfg.Quarantine.Enabled {
		return 0
	}
	return time.Duration(s.cfg.Quarantine.StreamEventStallMS) * time.Millisecond
}

func (s *proxyServer) quarantineDuration(level int) time.Duration {
	duration := time.Duration(s.cfg.Quarantine.BaseSeconds) * time.Second
	maximum := time.Duration(s.cfg.Quarantine.MaxSeconds) * time.Second
	for current := 1; current < level && duration < maximum; current++ {
		duration *= 2
		if duration > maximum {
			duration = maximum
		}
	}
	return duration
}

func (s *proxyServer) networkIncidentOriginThreshold() int {
	if s.cfg.Quarantine.NetworkIncidentOrigins > 0 {
		return s.cfg.Quarantine.NetworkIncidentOrigins
	}
	return 2
}

func (s *proxyServer) networkIncidentWindow() time.Duration {
	seconds := s.cfg.Quarantine.NetworkIncidentWindowSeconds
	if seconds <= 0 {
		seconds = 15
	}
	return time.Duration(seconds) * time.Second
}

func (s *proxyServer) networkIncidentSuppression() time.Duration {
	seconds := s.cfg.Quarantine.NetworkIncidentSuppressSeconds
	if seconds <= 0 {
		seconds = 90
	}
	return time.Duration(seconds) * time.Second
}

func sharedNetworkSignal(signal string) bool {
	switch signal {
	case "transport", "ttfb_timeout", "excessive_ttfb", "client_disconnect_after_stall", "stream_event_stall", "stream_error":
		return true
	default:
		return false
	}
}

func strongLocalNetworkError(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && (dnsErr.IsTimeout || dnsErr.IsTemporary || dnsErr.Err == "no such host") {
		return true
	}
	for _, target := range []error{syscall.ENETDOWN, syscall.ENETUNREACH, syscall.EHOSTUNREACH} {
		if errors.Is(err, target) {
			return true
		}
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "no route to host") || strings.Contains(text, "network is unreachable") ||
		strings.Contains(text, "network is down") || strings.Contains(text, "temporary failure in name resolution") ||
		strings.Contains(text, "server misbehaving") && strings.Contains(text, "lookup")
}

func sharedTransportError(err error) bool {
	if err == nil {
		return false
	}
	if strongLocalNetworkError(err) {
		return true
	}
	text := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection reset by peer", "broken pipe", "connection refused", "i/o timeout",
		"timeout awaiting response headers", "tls handshake timeout", "unexpected eof",
	} {
		if strings.Contains(text, needle) {
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (s *proxyServer) normalizeNetworkIncidentLocked(now time.Time) {
	window := s.networkIncidentWindow()
	for origin, observedAt := range s.networkIncident.Signals {
		if now.Sub(observedAt) > window {
			delete(s.networkIncident.Signals, origin)
		}
	}
	if s.networkIncident.Phase == "" {
		s.networkIncident.Phase = networkPhaseNormal
	}
	if s.networkIncident.Phase == networkPhaseSuspected && (s.networkIncident.SuspectedAt.IsZero() || now.Sub(s.networkIncident.SuspectedAt) > window) {
		s.networkIncident.Phase = networkPhaseNormal
		s.networkIncident.SuspectedAt = time.Time{}
		s.networkIncident.Signals = map[string]time.Time{}
	}
	if (s.networkIncident.Phase == networkPhaseOffline || s.networkIncident.Phase == networkPhaseRecovering) &&
		!s.networkIncident.ActiveUntil.IsZero() && !now.Before(s.networkIncident.ActiveUntil) && !s.networkIncident.ProbeInFlight {
		s.networkIncident.Phase = networkPhaseNormal
		s.networkIncident.ActiveUntil = time.Time{}
		s.networkIncident.NextProbeAt = time.Time{}
		s.networkIncident.Signals = map[string]time.Time{}
	}
}

func (s *proxyServer) clearSharedNetworkProviderSignalsLocked(now time.Time) {
	for key, state := range s.providerStates {
		if !sharedNetworkSignal(state.QuarantineReason) && !sharedNetworkSignal(state.LastSignal) {
			continue
		}
		state.QuarantinedUntil = time.Time{}
		state.QuarantineReason = ""
		state.InstabilitySignals = 0
		state.SignalWindowStart = time.Time{}
		state.QuarantineLevel = 0
		state.RecoverySuccesses = 0
		state.Recovering = false
		state.LastSignal = ""
		state.LastUpdated = now
		if strings.HasPrefix(state.Reason, "instability_") {
			state.BlockedUntil = time.Time{}
			state.Reason = ""
		}
		s.providerStates[key] = state
	}
}

func (s *proxyServer) providerIncidentOrigin(providerName string) string {
	base, _, _ := strings.Cut(providerName, "@")
	if upstream := s.providers[base]; upstream != nil && upstream.Host != "" {
		return strings.ToLower(upstream.Scheme + "://" + upstream.Host)
	}
	if provider, ok := s.cfg.Providers[base]; ok && provider.BaseURL != "" {
		if upstream, err := url.Parse(provider.BaseURL); err == nil && upstream.Host != "" {
			return strings.ToLower(upstream.Scheme + "://" + upstream.Host)
		}
	}
	return base
}

// observeSharedNetworkIncidentLocked distinguishes a local/shared transport
// incident from independent provider failures. Caller holds providerStateMu.
func (s *proxyServer) observeSharedNetworkIncidentLocked(providerName, signal string, now time.Time) bool {
	if !sharedNetworkSignal(signal) {
		return false
	}
	if s.networkIncident.Signals == nil {
		s.networkIncident.Signals = map[string]time.Time{}
	}
	s.normalizeNetworkIncidentLocked(now)
	origin := s.providerIncidentOrigin(providerName)
	s.networkIncident.Signals[origin] = now
	if s.networkIncident.Phase == networkPhaseOffline || s.networkIncident.Phase == networkPhaseRecovering {
		return true
	}
	if len(s.networkIncident.Signals) < s.networkIncidentOriginThreshold() {
		return false
	}
	origins := make([]string, 0, len(s.networkIncident.Signals))
	for observedOrigin := range s.networkIncident.Signals {
		origins = append(origins, observedOrigin)
	}
	sort.Strings(origins)
	s.networkIncident.ActiveUntil = now.Add(s.networkIncidentSuppression())
	s.networkIncident.NextProbeAt = now.Add(networkRecoveryDelay)
	s.networkIncident.Phase = networkPhaseOffline
	s.networkIncident.ProbeInFlight = false
	s.networkIncident.Epoch++
	s.networkIncident.LastDetected = now
	s.networkIncident.Origins = origins
	s.clearSharedNetworkProviderSignalsLocked(now)
	log.Printf("shared network incident detected origins=%d suppress=%s", len(origins), s.networkIncidentSuppression())
	return true
}

func (s *proxyServer) observeNetworkTransportFailure(providerName, signal string, err error) networkFailureDecision {
	if !sharedTransportError(err) {
		return networkFailureDecision{}
	}
	now := s.accountNow()
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	if s.networkIncident.Signals == nil {
		s.networkIncident.Signals = map[string]time.Time{}
	}
	s.normalizeNetworkIncidentLocked(now)
	origin := s.providerIncidentOrigin(providerName)
	s.networkIncident.Signals[origin] = now

	if s.networkIncident.Phase == networkPhaseOffline || s.networkIncident.Phase == networkPhaseRecovering {
		return networkFailureDecision{SuppressProviderPenalty: true, StopFallback: true}
	}
	if s.networkIncident.Phase == networkPhaseNormal && strongLocalNetworkError(err) {
		s.networkIncident.Phase = networkPhaseSuspected
		s.networkIncident.SuspectedAt = now
		s.networkIncident.Epoch++
	}
	if len(s.networkIncident.Signals) >= s.networkIncidentOriginThreshold() {
		origins := make([]string, 0, len(s.networkIncident.Signals))
		for observedOrigin := range s.networkIncident.Signals {
			origins = append(origins, observedOrigin)
		}
		sort.Strings(origins)
		s.networkIncident.Phase = networkPhaseOffline
		s.networkIncident.ActiveUntil = now.Add(s.networkIncidentSuppression())
		s.networkIncident.NextProbeAt = now.Add(networkRecoveryDelay)
		s.networkIncident.ProbeInFlight = false
		s.networkIncident.LastDetected = now
		s.networkIncident.Origins = origins
		s.clearSharedNetworkProviderSignalsLocked(now)
		log.Printf("shared network incident confirmed origins=%d retry=%s", len(origins), networkRecoveryDelay)
		return networkFailureDecision{SuppressProviderPenalty: true, StopFallback: true}
	}
	return networkFailureDecision{SuppressProviderPenalty: s.networkIncident.Phase == networkPhaseSuspected}
}

func (s *proxyServer) beginNetworkRequest() (bool, error) {
	now := s.accountNow()
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	if s.networkIncident.Signals == nil {
		s.networkIncident.Signals = map[string]time.Time{}
	}
	s.normalizeNetworkIncidentLocked(now)
	switch s.networkIncident.Phase {
	case networkPhaseOffline:
		if s.networkIncident.ProbeInFlight || now.Before(s.networkIncident.NextProbeAt) {
			return false, &retryableNetworkError{}
		}
		s.networkIncident.Phase = networkPhaseRecovering
		s.networkIncident.ProbeInFlight = true
		return true, nil
	case networkPhaseRecovering:
		return false, &retryableNetworkError{}
	default:
		return false, nil
	}
}

func (s *proxyServer) finishNetworkProbe(probe, success bool) {
	if !probe || success {
		return
	}
	now := s.accountNow()
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	if s.networkIncident.Phase == networkPhaseRecovering {
		s.networkIncident.Phase = networkPhaseOffline
		s.networkIncident.ProbeInFlight = false
		s.networkIncident.NextProbeAt = now.Add(networkRecoveryDelay)
	}
}

func (s *proxyServer) observeNetworkSuccess() {
	s.providerStateMu.Lock()
	phase := s.networkIncident.Phase
	if phase == "" || phase == networkPhaseNormal {
		s.providerStateMu.Unlock()
		return
	}
	s.networkIncident.Phase = networkPhaseNormal
	s.networkIncident.SuspectedAt = time.Time{}
	s.networkIncident.ActiveUntil = time.Time{}
	s.networkIncident.NextProbeAt = time.Time{}
	s.networkIncident.ProbeInFlight = false
	s.networkIncident.Signals = map[string]time.Time{}
	s.providerStateMu.Unlock()
	for _, transport := range s.transports {
		transport.CloseIdleConnections()
	}
	log.Printf("shared network incident recovered previous_phase=%s", phase)
}

func (s *proxyServer) networkIncidentStatus() map[string]any {
	now := s.accountNow()
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	s.normalizeNetworkIncidentLocked(now)
	phase := s.networkIncident.Phase
	if phase == "" {
		phase = networkPhaseNormal
	}
	active := phase != networkPhaseNormal
	status := map[string]any{"active": active, "phase": phase, "epoch": s.networkIncident.Epoch, "origins": append([]string(nil), s.networkIncident.Origins...)}
	if !s.networkIncident.ActiveUntil.IsZero() {
		status["until"] = s.networkIncident.ActiveUntil.UTC().Format(time.RFC3339)
	}
	if !s.networkIncident.LastDetected.IsZero() {
		status["lastDetected"] = s.networkIncident.LastDetected.UTC().Format(time.RFC3339)
	}
	return status
}

// observeProviderInstability adds one request-level signal. Multiple symptoms
// from the same request are collapsed by callers. Recovery probes fail fast:
// one new signal while recovering reopens quarantine with longer backoff.
func (s *proxyServer) observeProviderInstability(providerName, signal string) bool {
	if !s.cfg.Quarantine.Enabled || !s.circuitEnabled(providerName) {
		return false
	}
	now := s.accountNow()
	window := time.Duration(s.cfg.Quarantine.WindowSeconds) * time.Second
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	if s.providerStates == nil {
		s.providerStates = map[string]providerRuntimeState{}
	}
	if s.observeSharedNetworkIncidentLocked(providerName, signal, now) {
		return false
	}
	state := s.providerStates[providerName]
	state.LastFailure = time.Now()
	if now.Before(state.QuarantinedUntil) && !state.Recovering {
		state.LastSignal = signal
		state.LastUpdated = now
		s.providerStates[providerName] = state
		return true
	}
	if state.SignalWindowStart.IsZero() || now.Sub(state.SignalWindowStart) > window {
		state.SignalWindowStart = now
		state.InstabilitySignals = 0
	}
	state.InstabilitySignals++
	state.RecoverySuccesses = 0
	state.LastSignal = signal
	state.LastUpdated = now
	threshold := s.cfg.Quarantine.SignalThreshold
	if state.Recovering {
		threshold = 1
	}
	quarantined := state.InstabilitySignals >= threshold
	if quarantined {
		state.QuarantineLevel++
		duration := s.quarantineDuration(state.QuarantineLevel)
		until := now.Add(duration)
		if until.After(state.QuarantinedUntil) {
			state.QuarantinedUntil = until
		}
		state.QuarantineReason = signal
		state.InstabilitySignals = 0
		state.SignalWindowStart = now
		state.Recovering = false
		log.Printf("provider quarantined provider=%s signal=%s level=%d cooldown=%s", providerName, signal, state.QuarantineLevel, duration)
	}
	s.providerStates[providerName] = state
	return quarantined
}

// observeProviderHealthy records only a fully delivered, low-TTFB response.
// Stream duration never participates: a multi-hour stream with steady events
// is healthy. Escalation resets after consecutive clean recovery responses.
func (s *proxyServer) observeProviderHealthy(providerName string) {
	s.observeProviderHealthyAfter(providerName, time.Time{})
}

func (s *proxyServer) observeProviderHealthyAfter(providerName string, started time.Time) {
	if !s.cfg.Quarantine.Enabled || !s.circuitEnabled(providerName) {
		return
	}
	now := s.accountNow()
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	if !started.IsZero() && state.LastFailure.After(started) {
		return
	}
	if now.Before(state.QuarantinedUntil) {
		return
	}
	if state.QuarantineLevel == 0 && state.InstabilitySignals == 0 && !state.Recovering {
		return
	}
	state.RecoverySuccesses++
	state.LastUpdated = now
	if state.RecoverySuccesses >= s.cfg.Quarantine.RecoverySuccesses {
		state.QuarantinedUntil = time.Time{}
		state.QuarantineReason = ""
		state.InstabilitySignals = 0
		state.SignalWindowStart = time.Time{}
		state.QuarantineLevel = 0
		state.RecoverySuccesses = 0
		state.Recovering = false
		state.LastSignal = ""
	}
	if s.providerStates == nil {
		s.providerStates = map[string]providerRuntimeState{}
	}
	s.providerStates[providerName] = state
}

func (s *proxyServer) markProviderBlocked(providerName, reason string, duration time.Duration) bool {
	if duration <= 0 {
		duration = time.Minute
	}
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	changed := state.BlockedUntil.IsZero() || state.Reason != reason || time.Now().After(state.BlockedUntil)
	until := time.Now().Add(duration)
	if until.After(state.BlockedUntil) {
		state.BlockedUntil = until
		state.Reason = reason
	}
	state.LastUpdated = time.Now()
	state.LastFailure = state.LastUpdated
	s.providerStates[providerName] = state
	return changed
}

func (s *proxyServer) clearProviderBlock(providerName string) {
	s.clearProviderBlockAfter(providerName, time.Time{})
}

func (s *proxyServer) clearProviderBlockAfter(providerName string, started time.Time) {
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	if !started.IsZero() && (state.LastFailure.After(started) || strings.HasPrefix(state.Reason, "auth_") ||
		strings.Contains(state.Reason, "quota") || state.Reason == "quota_or_rate_limit") {
		return
	}
	if strings.HasPrefix(state.Reason, "ollama_usage_") || strings.HasPrefix(state.Reason, "cline_usage_") || strings.HasPrefix(state.Reason, "xai_usage_") {
		return
	}
	state.BlockedUntil = time.Time{}
	state.Reason = ""
	state.LastUpdated = time.Now()
	s.providerStates[providerName] = state
}

func (s *proxyServer) clearClaudeProfileAuthBlock(profile claudeUsageProfile, started ...time.Time) bool {
	providerName := s.cfg.ClaudeUsage.Provider + "@" + profile.Name
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	if len(started) > 0 && state.LastFailure.After(started[0]) {
		return false
	}
	if !strings.HasPrefix(state.Reason, "auth_") {
		return false
	}
	state.BlockedUntil = time.Time{}
	state.Reason = ""
	state.LastUpdated = s.accountNow()
	s.providerStates[providerName] = state
	return true
}

func (s *proxyServer) clearOllamaUsageBlock(providerName string) {
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	if !strings.HasPrefix(state.Reason, "ollama_usage_") {
		return
	}
	state.BlockedUntil = time.Time{}
	state.Reason = ""
	state.LastUpdated = time.Now()
	s.providerStates[providerName] = state
}

func (s *proxyServer) clearClineUsageBlock(providerName string) {
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	if !strings.HasPrefix(state.Reason, "cline_usage_") {
		return
	}
	state.BlockedUntil = time.Time{}
	state.Reason = ""
	state.LastUpdated = time.Now()
	s.providerStates[providerName] = state
}

func (s *proxyServer) clearXAIUsageBlock(providerName string) {
	s.providerStateMu.Lock()
	defer s.providerStateMu.Unlock()
	state := s.providerStates[providerName]
	if !strings.HasPrefix(state.Reason, "xai_usage_") {
		return
	}
	state.BlockedUntil = time.Time{}
	state.Reason = ""
	state.LastUpdated = time.Now()
	s.providerStates[providerName] = state
}

func (s *proxyServer) observeProviderHeaders(providerName string, headers http.Header) {
	s.observeProviderHeadersAt(providerName, headers, time.Now())
}

func (s *proxyServer) observeProviderHeadersAt(providerName string, headers http.Header, started time.Time) {
	info := map[string]string{}
	for key, values := range headers {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "ratelimit") || strings.Contains(lower, "rate-limit") {
			info[key] = strings.Join(values, ",")
		}
	}
	if len(info) == 0 {
		return
	}
	s.providerStateMu.Lock()
	state := s.providerStates[providerName]
	state.RateLimitInfo = info
	state.LastUpdated = time.Now()
	s.providerStates[providerName] = state
	if providerName == s.cfg.ClaudeUsage.Provider || strings.HasPrefix(providerName, s.cfg.ClaudeUsage.Provider+"@") {
		status := strings.ToLower(headers.Get("Anthropic-Ratelimit-Unified-7d_oi-Status"))
		modelKey := providerName + "#fable"
		modelState := s.providerStates[modelKey]
		switch status {
		case "rejected":
			blockedUntil := time.Now().Add(5 * time.Minute)
			if raw := headers.Get("Anthropic-Ratelimit-Unified-7d_oi-Reset"); raw != "" {
				if epoch, err := strconv.ParseInt(raw, 10, 64); err == nil && time.Unix(epoch, 0).After(time.Now()) {
					blockedUntil = time.Unix(epoch, 0)
				}
			}
			modelState.BlockedUntil = blockedUntil
			// This header is the Fable-specific weekly allowance. Keep its
			// reason distinct from a generic HTTP 429 so a later successful
			// request on another route cannot erase a still-valid quota block.
			modelState.Reason = "fable_quota_rejected"
			modelState.LastUpdated = time.Now()
			modelState.LastFailure = modelState.LastUpdated
			s.providerStates[modelKey] = modelState
		case "allowed", "allowed_warning":
			if modelState.Reason == "fable_quota_rejected" && !modelState.LastFailure.After(started) {
				modelState.BlockedUntil = time.Time{}
				modelState.Reason = ""
				modelState.LastUpdated = time.Now()
				s.providerStates[modelKey] = modelState
			}
		}
	}
	s.providerStateMu.Unlock()
}

func (s *proxyServer) routingStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	performance := s.adaptivePerformance()
	s.providerStateMu.Lock()
	states := make(map[string]providerRuntimeState, len(s.providerStates))
	for providerName, state := range s.providerStates {
		states[providerName] = state
	}
	s.providerStateMu.Unlock()
	providerUsage := map[string]any{}
	for providerName := range s.cfg.Providers {
		utilization, known := s.providerUsageUtilization(providerName)
		entry := map[string]any{
			"known":       known,
			"utilization": utilization,
		}
		if browser, found, fresh := s.browserUsageForProvider(providerName); found {
			entry["browser"] = browser
			entry["browserFresh"] = fresh
		}
		if providerName == s.cfg.ClineUsage.Provider {
			s.clineUse.mu.Lock()
			if s.clineUse.hasValue {
				entry["windows"] = s.clineUse.snapshot.Windows
				entry["fetchedAt"] = s.clineUse.snapshot.FetchedAt.UTC().Format(time.RFC3339)
			}
			s.clineUse.mu.Unlock()
		}
		if providerName == s.xaiUsageProviderName() {
			s.xaiUse.mu.Lock()
			if s.xaiUse.hasValue {
				entry["windows"] = s.xaiUse.snapshot.Windows
				entry["products"] = s.xaiUse.snapshot.Products
				entry["billingKind"] = s.xaiUse.snapshot.BillingKind
				entry["source"] = s.xaiUse.snapshot.Source
				entry["fetchedAt"] = s.xaiUse.snapshot.FetchedAt.UTC().Format(time.RFC3339)
			}
			s.xaiUse.mu.Unlock()
		}
		providerUsage[providerName] = entry
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":         s.cfg.AdaptiveRouting.Enabled,
		"generatedAt":     time.Now().UTC().Format(time.RFC3339Nano),
		"performance":     performance,
		"providerStates":  states,
		"networkIncident": s.networkIncidentStatus(),
		"providerTiers":   s.cfg.AdaptiveRouting.ProviderTiers,
		"providerUsage":   providerUsage,
		"claudeUsage":     s.claudeUsageStatus(),
		"headroomWeight":  s.cfg.AdaptiveRouting.HeadroomWeight,
	})
}

func (s *proxyServer) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

var buildSourceHash = "development"

func (s *proxyServer) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"pid": os.Getpid(), "sourceHash": buildSourceHash,
		"configHash": s.cfg.SourceHash, "activeRequests": s.activeRequests.Load(), "draining": s.draining.Load()})
}

// quota exposes only quota metadata on the loopback proxy; credentials remain
// inside the configured provider auth source.
func (s *proxyServer) quota(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if s.cfg.OllamaUsage.WeeklyThresholdPct <= 0 && s.cfg.OllamaUsage.SessionThresholdPct <= 0 {
		writeAPIError(w, http.StatusNotFound, "not_found_error", "Ollama usage routing is disabled")
		return
	}
	snapshot, err := s.ollamaUsageSnapshot(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, "api_error", err.Error())
		return
	}
	s.updateOllamaUsageState(snapshot)
	reserveActive, reserveReason := ollamaUsageThresholdActive(s.cfg.OllamaUsage, snapshot)
	providerBlocked, providerState := s.providerBlocked(s.cfg.OllamaUsage.Provider)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"provider":            s.cfg.OllamaUsage.Provider,
		"weeklyUsagePct":      snapshot.Weekly.Usage * 100,
		"weeklyThresholdPct":  s.cfg.OllamaUsage.WeeklyThresholdPct,
		"sessionUsagePct":     snapshot.Session.Usage * 100,
		"sessionThresholdPct": s.cfg.OllamaUsage.SessionThresholdPct,
		"reserveActive":       reserveActive,
		"reserveReason":       reserveReason,
		"providerBlocked":     providerBlocked,
		"blockReason":         providerState.Reason,
		"eligibleUpstreams":   s.cfg.OllamaUsage.EligibleUpstreams,
		"reserveUpstreams":    s.cfg.OllamaUsage.ReserveUpstreams,
		"fetchedAt":           snapshot.FetchedAt.UTC().Format(time.RFC3339),
		"weeklyModels":        snapshot.Weekly.Models,
		"sessionModels":       snapshot.Session.Models,
		"claudeUsage":         s.claudeUsageStatus(),
	})
}

func (s *proxyServer) applyOllamaUsageRoute(ctx context.Context, selected modelConfig) modelConfig {
	policy := s.cfg.OllamaUsage
	if (policy.WeeklyThresholdPct <= 0 && policy.SessionThresholdPct <= 0) || selected.Provider != policy.Provider || !containsString(policy.EligibleUpstreams, selected.Upstream) {
		return selected
	}
	snapshot, err := s.ollamaUsageSnapshot(ctx)
	if err != nil {
		// Quota telemetry must never prevent a normal inference request.
		log.Printf("ollama usage unavailable model=%q provider=%s error=%v; retaining configured route", selected.Requested, policy.Provider, err)
		return selected
	}
	s.updateOllamaUsageState(snapshot)
	return selected
}

func (s *proxyServer) updateOllamaUsageState(snapshot ollamaUsageSnapshot) {
	policy := s.cfg.OllamaUsage
	exhausted, reason := ollamaUsageExhausted(snapshot)
	if !exhausted {
		s.clearOllamaUsageBlock(policy.Provider)
		return
	}
	if s.markProviderBlocked(policy.Provider, "ollama_usage_"+reason+"_exhausted", time.Duration(policy.CacheTTLSeconds+1)*time.Second) {
		log.Printf("ollama usage exhausted weekly=%.1f%% session=%.1f%% reason=%s; blocking provider", snapshot.Weekly.Usage*100, snapshot.Session.Usage*100, reason)
	}
}

func ollamaUsageExhausted(snapshot ollamaUsageSnapshot) (bool, string) {
	if snapshot.Session.Usage >= 1 {
		return true, "session"
	}
	if snapshot.Weekly.Usage >= 1 {
		return true, "weekly"
	}
	return false, ""
}

func ollamaUsageThresholdActive(policy ollamaUsageConfig, snapshot ollamaUsageSnapshot) (bool, string) {
	if policy.SessionThresholdPct > 0 && snapshot.Session.Usage*100 >= policy.SessionThresholdPct {
		return true, "session"
	}
	if policy.WeeklyThresholdPct > 0 && snapshot.Weekly.Usage*100 >= policy.WeeklyThresholdPct {
		return true, "weekly"
	}
	return false, ""
}

// ollamaCandidateReservedOut applies the 85%-100% reserve without blocking
// DeepSeek on the shared Ollama subscription. It uses cached telemetry so
// candidate ranking never adds a network request.
func (s *proxyServer) ollamaCandidateReservedOut(candidate modelConfig) (bool, string) {
	policy := s.cfg.OllamaUsage
	providerName := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
	if providerName != policy.Provider || !containsString(policy.EligibleUpstreams, candidate.Upstream) || containsString(policy.ReserveUpstreams, candidate.Upstream) {
		return false, ""
	}
	s.ollamaUse.mu.Lock()
	snapshot, hasValue := s.ollamaUse.snapshot, s.ollamaUse.hasValue
	s.ollamaUse.mu.Unlock()
	if !hasValue {
		return false, ""
	}
	return ollamaUsageThresholdActive(policy, snapshot)
}

func (s *proxyServer) ollamaUsageSnapshot(ctx context.Context) (ollamaUsageSnapshot, error) {
	policy := s.cfg.OllamaUsage
	s.ollamaUse.mu.Lock()
	if s.ollamaUse.hasValue && time.Since(s.ollamaUse.snapshot.FetchedAt) < time.Duration(policy.CacheTTLSeconds)*time.Second {
		snapshot := s.ollamaUse.snapshot
		s.ollamaUse.mu.Unlock()
		return snapshot, nil
	}
	if s.ollamaUse.hasValue {
		stale := s.ollamaUse.snapshot
		if !s.ollamaUse.refreshing {
			s.ollamaUse.refreshing = true
			go s.refreshOllamaUsageInBackground()
		}
		s.ollamaUse.mu.Unlock()
		return stale, nil
	}
	if s.ollamaUse.refreshing {
		s.ollamaUse.mu.Unlock()
		return ollamaUsageSnapshot{}, errors.New("Ollama usage refresh already in progress")
	}
	s.ollamaUse.refreshing = true
	s.ollamaUse.mu.Unlock()
	snapshot, err := s.fetchOllamaUsageSnapshot(ctx)
	s.ollamaUse.mu.Lock()
	s.ollamaUse.refreshing = false
	if err == nil {
		s.ollamaUse.snapshot = snapshot
		s.ollamaUse.hasValue = true
	}
	s.ollamaUse.mu.Unlock()
	return snapshot, err
}

func (s *proxyServer) fetchOllamaUsageSnapshot(ctx context.Context) (ollamaUsageSnapshot, error) {
	policy := s.cfg.OllamaUsage
	providerURL, ok := s.providers[policy.Provider]
	if !ok {
		return ollamaUsageSnapshot{}, fmt.Errorf("Ollama usage provider %q is unavailable", policy.Provider)
	}
	usageURL := *providerURL
	usageURL.Path = joinURLPath(providerURL.Path, "/api/usage")
	if s.cfg.Definition != nil {
		usageURL.Path = "/api/usage"
	}
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, usageURL.String(), nil)
	if err != nil {
		return ollamaUsageSnapshot{}, err
	}
	if err := s.applyProviderHeaders(policy.Provider, req.Header); err != nil {
		return ollamaUsageSnapshot{}, err
	}
	resp, err := s.clients[policy.Provider].Do(req)
	if err != nil {
		return ollamaUsageSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
		return ollamaUsageSnapshot{}, fmt.Errorf("Ollama usage endpoint returned status %d", resp.StatusCode)
	}
	var payload ollamaUsageResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return ollamaUsageSnapshot{}, fmt.Errorf("decode Ollama usage response: %w", err)
	}
	if payload.Limits.Weekly.Usage < 0 || payload.Limits.Weekly.Usage > 1 {
		return ollamaUsageSnapshot{}, fmt.Errorf("Ollama weekly usage is outside 0..1")
	}
	return ollamaUsageSnapshot{FetchedAt: time.Now(), Weekly: payload.Limits.Weekly, Session: payload.Limits.Session}, nil
}

func (s *proxyServer) refreshOllamaUsageInBackground() {
	snapshot, err := s.fetchOllamaUsageSnapshot(context.Background())
	s.ollamaUse.mu.Lock()
	s.ollamaUse.refreshing = false
	if err == nil {
		s.ollamaUse.snapshot = snapshot
		s.ollamaUse.hasValue = true
	}
	s.ollamaUse.mu.Unlock()
	if err != nil {
		log.Printf("ollama usage background refresh failed: %v", err)
		return
	}
	s.updateOllamaUsageState(snapshot)
}

func (s *proxyServer) monitorOllamaUsage(ctx context.Context) {
	policy := s.cfg.OllamaUsage
	if policy.Provider == "" || (s.cfg.Definition == nil && policy.WeeklyThresholdPct <= 0 && policy.SessionThresholdPct <= 0) {
		return
	}
	s.ollamaUse.mu.Lock()
	if !s.ollamaUse.refreshing {
		s.ollamaUse.refreshing = true
		go s.refreshOllamaUsageInBackground()
	}
	s.ollamaUse.mu.Unlock()
	ticker := time.NewTicker(time.Duration(policy.CacheTTLSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ollamaUse.mu.Lock()
			if !s.ollamaUse.refreshing {
				s.ollamaUse.refreshing = true
				go s.refreshOllamaUsageInBackground()
			}
			s.ollamaUse.mu.Unlock()
		}
	}
}

func (s *proxyServer) fetchClineUsageSnapshot(ctx context.Context) (clineUsageSnapshot, error) {
	policy := s.cfg.ClineUsage
	providerURL, ok := s.providers[policy.Provider]
	if !ok {
		return clineUsageSnapshot{}, fmt.Errorf("Cline usage provider %q is unavailable", policy.Provider)
	}
	usageURL := *providerURL
	usageURL.Path = joinURLPath(providerURL.Path, "/v1/users/me/plan/usage-limits")
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, usageURL.String(), nil)
	if err != nil {
		return clineUsageSnapshot{}, err
	}
	if err := s.applyProviderHeaders(policy.Provider, req.Header); err != nil {
		return clineUsageSnapshot{}, err
	}
	resp, err := s.clients[policy.Provider].Do(req)
	if err != nil {
		return clineUsageSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
		return clineUsageSnapshot{}, fmt.Errorf("Cline usage endpoint returned status %d", resp.StatusCode)
	}
	var payload clineUsageResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return clineUsageSnapshot{}, fmt.Errorf("decode Cline usage response: %w", err)
	}
	if !payload.Success || len(payload.Data.Limits) == 0 {
		return clineUsageSnapshot{}, errors.New("Cline usage response has no limits")
	}
	windows := make(map[string]clineUsageWindow, len(payload.Data.Limits))
	for _, window := range payload.Data.Limits {
		if window.Type == "" || window.PercentUsed < 0 {
			return clineUsageSnapshot{}, errors.New("Cline usage response contains an invalid limit")
		}
		windows[window.Type] = window
	}
	return clineUsageSnapshot{FetchedAt: time.Now(), Windows: windows}, nil
}

func (s *proxyServer) updateClineUsageState(snapshot clineUsageSnapshot) {
	policy := s.cfg.ClineUsage
	for _, window := range snapshot.Windows {
		if window.PercentUsed >= policy.BlockThresholdPct {
			duration := time.Duration(policy.CacheTTLSeconds+1) * time.Second
			if s.markProviderBlocked(policy.Provider, "cline_usage_"+window.Type+"_exhausted", duration) {
				log.Printf("ClinePass usage exhausted window=%s usage=%.1f%%; blocking provider", window.Type, window.PercentUsed)
			}
			return
		}
	}
	s.clearClineUsageBlock(policy.Provider)
}

func (s *proxyServer) refreshClineUsageInBackground() {
	snapshot, err := s.fetchClineUsageSnapshot(context.Background())
	s.clineUse.mu.Lock()
	s.clineUse.refreshing = false
	if err == nil {
		s.clineUse.snapshot = snapshot
		s.clineUse.hasValue = true
	}
	s.clineUse.mu.Unlock()
	if err != nil {
		log.Printf("ClinePass usage background refresh failed: %v", err)
		return
	}
	s.updateClineUsageState(snapshot)
}

func (s *proxyServer) monitorClineUsage(ctx context.Context) {
	policy := s.cfg.ClineUsage
	if policy.Provider == "" {
		return
	}
	s.clineUse.mu.Lock()
	s.clineUse.refreshing = true
	s.clineUse.mu.Unlock()
	go s.refreshClineUsageInBackground()
	ticker := time.NewTicker(time.Duration(policy.CacheTTLSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.clineUse.mu.Lock()
			if !s.clineUse.refreshing {
				s.clineUse.refreshing = true
				go s.refreshClineUsageInBackground()
			}
			s.clineUse.mu.Unlock()
		}
	}
}

const xaiUsageRefreshInterval = 5 * time.Minute

func (s *proxyServer) xaiUsageProviderName() string {
	for name, provider := range s.cfg.Providers {
		if s.cfg.Definition != nil && provider.Usage.Mode != "api" {
			continue
		}
		if provider.AuthMode == "xai-oauth" {
			return name
		}
	}
	return ""
}

func parseXAIPeriod(startRaw, endRaw string) (time.Time, time.Time, error) {
	start, err := time.Parse(time.RFC3339, startRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid xAI billing period start: %w", err)
	}
	end, err := time.Parse(time.RFC3339, endRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid xAI billing period end: %w", err)
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, errors.New("xAI billing period end is not after start")
	}
	return start, end, nil
}

func parseXAIWeeklyUsage(raw xaiBillingConfig, now time.Time) (xaiUsageSnapshot, bool, error) {
	if !strings.Contains(strings.ToUpper(raw.CurrentPeriod.Type), "WEEK") {
		return xaiUsageSnapshot{}, false, errors.New("xAI credits response is not weekly")
	}
	_, end, err := parseXAIPeriod(raw.CurrentPeriod.Start, raw.CurrentPeriod.End)
	if err != nil {
		return xaiUsageSnapshot{}, false, err
	}
	inferred := raw.CreditUsagePercent == nil
	percent := 0.0
	if inferred {
		if !end.After(now) {
			return xaiUsageSnapshot{}, false, errors.New("xAI weekly usage omitted for an expired period")
		}
	} else {
		percent = *raw.CreditUsagePercent
		if percent < 0 || percent > 100 {
			return xaiUsageSnapshot{}, false, errors.New("xAI weekly usage is outside 0..100")
		}
	}
	products := map[string]float64{}
	for _, product := range raw.ProductUsage {
		name := strings.TrimSpace(product.Product)
		if name == "" || product.UsagePercent < 0 || product.UsagePercent > 100 {
			continue
		}
		products[name] = product.UsagePercent
	}
	return xaiUsageSnapshot{
		FetchedAt: now,
		Windows: map[string]clineUsageWindow{
			"weekly": {Type: "weekly", PercentUsed: percent, ResetsAt: end},
		},
		Products:    products,
		BillingKind: "weekly",
		Source:      "cli-chat-proxy.grok.com/v1/billing",
	}, inferred, nil
}

func parseXAIMonthlyUsage(raw xaiBillingConfig, now time.Time) (xaiUsageSnapshot, error) {
	_, end, err := parseXAIPeriod(raw.BillingPeriodStart, raw.BillingPeriodEnd)
	if err != nil {
		return xaiUsageSnapshot{}, err
	}
	if raw.MonthlyLimit.Val <= 0 || raw.Used.Val < 0 {
		return xaiUsageSnapshot{}, errors.New("xAI monthly usage has no positive included quota")
	}
	percent := math.Min(raw.Used.Val/raw.MonthlyLimit.Val*100, 100)
	return xaiUsageSnapshot{
		FetchedAt: now,
		Windows: map[string]clineUsageWindow{
			"monthly": {Type: "monthly", PercentUsed: percent, ResetsAt: end},
		},
		BillingKind: "monthly",
		Source:      "cli-chat-proxy.grok.com/v1/billing",
	}, nil
}

func (s *proxyServer) fetchXAIBillingConfig(ctx context.Context, providerName, format string) (xaiBillingConfig, error) {
	providerURL := s.providers[providerName]
	if providerURL == nil {
		return xaiBillingConfig{}, fmt.Errorf("xAI usage provider %q is unavailable", providerName)
	}
	billingURL := *providerURL
	billingURL.Path = joinURLPath(providerURL.Path, "/v1/billing")
	query := billingURL.Query()
	if format != "" {
		query.Set("format", format)
	}
	billingURL.RawQuery = query.Encode()
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	buildRequest := func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, billingURL.String(), nil)
		if err != nil {
			return nil, err
		}
		if err := s.applyProviderHeadersContext(requestCtx, providerName, req.Header); err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		return req, nil
	}
	req, err := buildRequest()
	if err != nil {
		return xaiBillingConfig{}, err
	}
	client := s.clients[providerName]
	if client == nil {
		return xaiBillingConfig{}, fmt.Errorf("xAI usage provider %q has no HTTP client", providerName)
	}
	resp, err := client.Do(req)
	if err != nil {
		return xaiBillingConfig{}, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		failedToken := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
		_ = resp.Body.Close()
		manager := s.xaiOAuth[providerName]
		if manager == nil {
			return xaiBillingConfig{}, errors.New("xAI OAuth manager is unavailable")
		}
		if _, err := manager.refreshAfterUnauthorized(requestCtx, failedToken); err != nil {
			return xaiBillingConfig{}, fmt.Errorf("refresh xAI OAuth after billing 401: %w", err)
		}
		req, err = buildRequest()
		if err != nil {
			return xaiBillingConfig{}, err
		}
		resp, err = client.Do(req)
		if err != nil {
			return xaiBillingConfig{}, err
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
		return xaiBillingConfig{}, fmt.Errorf("xAI billing endpoint returned status %d", resp.StatusCode)
	}
	var payload xaiBillingEnvelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return xaiBillingConfig{}, fmt.Errorf("decode xAI billing response: %w", err)
	}
	return payload.Config, nil
}

func (s *proxyServer) fetchXAIUsageSnapshot(ctx context.Context) (xaiUsageSnapshot, error) {
	providerName := s.xaiUsageProviderName()
	if providerName == "" {
		return xaiUsageSnapshot{}, errors.New("xAI OAuth provider is not configured")
	}
	now := time.Now()
	credits, creditsErr := s.fetchXAIBillingConfig(ctx, providerName, "credits")
	weekly, inferred, weeklyErr := parseXAIWeeklyUsage(credits, now)
	if creditsErr != nil {
		weeklyErr = creditsErr
	}
	if creditsErr != nil || weeklyErr != nil || credits.IsUnifiedBillingUser {
		monthlyRaw, monthlyFetchErr := s.fetchXAIBillingConfig(ctx, providerName, "")
		if monthlyFetchErr == nil {
			if monthly, monthlyErr := parseXAIMonthlyUsage(monthlyRaw, now); monthlyErr == nil {
				return monthly, nil
			}
			if weeklyErr == nil && (!inferred || monthlyRaw.MonthlyLimit.Val == 0) {
				return weekly, nil
			}
		}
		if weeklyErr != nil {
			return xaiUsageSnapshot{}, fmt.Errorf("xAI billing has no valid weekly or monthly quota: %w", weeklyErr)
		}
		if inferred && credits.IsUnifiedBillingUser {
			return xaiUsageSnapshot{}, errors.New("xAI unified billing could not confirm an inferred weekly quota")
		}
	}
	if weeklyErr != nil {
		return xaiUsageSnapshot{}, weeklyErr
	}
	return weekly, nil
}

func (s *proxyServer) updateXAIUsageState(snapshot xaiUsageSnapshot) {
	providerName := s.xaiUsageProviderName()
	for name, window := range snapshot.Windows {
		if window.PercentUsed >= 100 {
			if s.markProviderBlocked(providerName, "xai_usage_"+name+"_exhausted", xaiUsageRefreshInterval+time.Second) {
				log.Printf("SuperGrok usage exhausted window=%s usage=%.1f%%; blocking provider", name, window.PercentUsed)
			}
			return
		}
	}
	s.clearXAIUsageBlock(providerName)
}

func (s *proxyServer) refreshXAIUsageInBackground() {
	snapshot, err := s.fetchXAIUsageSnapshot(context.Background())
	s.xaiUse.mu.Lock()
	s.xaiUse.refreshing = false
	if err == nil {
		s.xaiUse.snapshot = snapshot
		s.xaiUse.hasValue = true
	}
	s.xaiUse.mu.Unlock()
	if err != nil {
		log.Printf("SuperGrok usage background refresh failed: %v", err)
		return
	}
	s.updateXAIUsageState(snapshot)
}

func (s *proxyServer) monitorXAIUsage(ctx context.Context) {
	if s.xaiUsageProviderName() == "" {
		return
	}
	s.xaiUse.mu.Lock()
	s.xaiUse.refreshing = true
	s.xaiUse.mu.Unlock()
	go s.refreshXAIUsageInBackground()
	interval := xaiUsageRefreshInterval
	if s.cfg.Definition != nil {
		interval = time.Duration(s.cfg.Providers[s.xaiUsageProviderName()].Usage.PollIntervalSeconds) * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.xaiUse.mu.Lock()
			if !s.xaiUse.refreshing {
				s.xaiUse.refreshing = true
				go s.refreshXAIUsageInBackground()
			}
			s.xaiUse.mu.Unlock()
		}
	}
}

func (s *proxyServer) claudeSubscriptionCandidate(candidate modelConfig) bool {
	policy := s.cfg.ClaudeUsage
	upstream := firstNonEmpty(candidate.Upstream, candidate.Requested)
	return policy.Provider != "" &&
		firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider) == policy.Provider &&
		containsString(policy.EligibleUpstreams, upstream)
}

func requestOAuthBearer(r *http.Request) (string, error) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	authKind := "missing"
	if len(auth) >= len("Bearer ") && strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		token := strings.TrimSpace(auth[len("Bearer "):])
		authKind = claudeTokenKind(token)
		if strings.HasPrefix(token, "sk-ant-oat") {
			return "Bearer " + token, nil
		}
	}
	// Some Claude Code profiles place the same OAuth token in x-api-key when a
	// custom ANTHROPIC_BASE_URL is active. Accept OAuth-token prefixes only;
	// ordinary sk-ant-api keys would turn this subscription fallback into API
	// billing and must remain ineligible.
	xAPIKey := strings.TrimSpace(r.Header.Get("x-api-key"))
	if strings.HasPrefix(xAPIKey, "sk-ant-oat") {
		token := xAPIKey
		return "Bearer " + token, nil
	}
	return "", fmt.Errorf("incoming request has no Claude OAuth token (authorization=%s x-api-key=%s)", authKind, claudeTokenKind(xAPIKey))
}

func claudeTokenKind(token string) string {
	switch {
	case token == "":
		return "missing"
	case strings.HasPrefix(token, "sk-ant-oat"):
		return "oauth"
	case strings.HasPrefix(token, "sk-ant-api"):
		return "api"
	default:
		return "other"
	}
}

type cachedClaudeOAuthCredential struct {
	token     string
	fetchedAt time.Time
	expiresAt time.Time
}

var claudeOAuthCredentials sync.Map // Keychain service -> cachedClaudeOAuthCredential
var claudeCredentialReads sync.Map  // Keychain service -> in-progress read completion

func (c cachedClaudeOAuthCredential) usable(now time.Time) bool {
	return now.Sub(c.fetchedAt) < claudeOAuthCredentialTTL && (c.expiresAt.IsZero() || c.expiresAt.After(now.Add(5*time.Second)))
}

const claudeOAuthCredentialTTL = 5 * time.Minute

func resolveClaudeProfileOAuthToken(ctx context.Context, profile claudeUsageProfile, forceRead ...bool) (string, error) {
	if profile.CredentialsService == "" {
		return "", errors.New("Claude profile has no credentials service")
	}
	if raw, ok := claudeOAuthCredentials.Load(profile.CredentialsService); ok {
		cached := raw.(cachedClaudeOAuthCredential)
		if cached.usable(time.Now()) && !(len(forceRead) > 0 && forceRead[0]) {
			return cached.token, nil
		}
	}
	readDone := make(chan struct{})
	if existing, loaded := claudeCredentialReads.LoadOrStore(profile.CredentialsService, readDone); loaded {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-existing.(chan struct{}):
			return resolveClaudeProfileOAuthToken(ctx, profile)
		}
	}
	defer func() { claudeCredentialReads.Delete(profile.CredentialsService); close(readDone) }()
	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, "security", "find-generic-password", "-s", profile.CredentialsService, "-w")
	output, err := cmd.Output()
	if commandCtx.Err() != nil {
		return "", commandCtx.Err()
	}
	if err != nil {
		return "", fmt.Errorf("read Claude OAuth credential: %w", err)
	}
	var credentials struct {
		ClaudeAIOAuth struct {
			AccessToken string `json:"accessToken"`
			ExpiresAt   int64  `json:"expiresAt"`
		} `json:"claudeAiOauth"`
	}
	if err := json.Unmarshal(output, &credentials); err != nil {
		return "", fmt.Errorf("decode Claude OAuth credential: %w", err)
	}
	token := strings.TrimSpace(credentials.ClaudeAIOAuth.AccessToken)
	if !strings.HasPrefix(token, "sk-ant-oat") {
		return "", errors.New("Claude Keychain credential does not contain an OAuth token")
	}
	expiresAt := time.Time{}
	if epoch := credentials.ClaudeAIOAuth.ExpiresAt; epoch > 0 {
		if epoch > 1e12 {
			expiresAt = time.UnixMilli(epoch)
		} else {
			expiresAt = time.Unix(epoch, 0)
		}
	}
	cached := cachedClaudeOAuthCredential{token: token, fetchedAt: time.Now(), expiresAt: expiresAt}
	claudeOAuthCredentials.Store(profile.CredentialsService, cached)
	if !cached.usable(time.Now()) {
		return "", errors.New("Claude subscription auth: credential expired; refresh required")
	}
	return token, nil
}

func clearClaudeProfileOAuthToken(profile claudeUsageProfile) {
	if profile.CredentialsService != "" {
		claudeOAuthCredentials.Delete(profile.CredentialsService)
	}
}

var claudeOAuthRefreshEnvKeys = map[string]bool{
	"ANTHROPIC_BASE_URL":   true,
	"ANTHROPIC_AUTH_TOKEN": true,
	"ANTHROPIC_API_KEY":    true,
	"CLAUDE_CONFIG_DIR":    true,
}

func sanitizedClaudeOAuthRefreshEnv(environ []string) []string {
	clean := make([]string, 0, len(environ))
	for _, entry := range environ {
		key, _, found := strings.Cut(entry, "=")
		if found && claudeOAuthRefreshEnvKeys[key] {
			continue
		}
		clean = append(clean, entry)
	}
	return clean
}

func runClaudeOAuthRefreshHelper(ctx context.Context, helperPath string, profile claudeUsageProfile) error {
	configDir := filepath.Dir(profile.CachePath)
	if !filepath.IsAbs(helperPath) || !filepath.IsAbs(configDir) || profile.CredentialsService == "" {
		return errors.New("Claude OAuth refresh profile is incomplete")
	}
	cmd := exec.CommandContext(ctx, helperPath,
		"--config-dir", configDir,
		"--credentials-service", profile.CredentialsService,
		"--auth-error",
	)
	cmd.Dir = "/private/tmp"
	cmd.Env = sanitizedClaudeOAuthRefreshEnv(os.Environ())
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Claude OAuth refresh helper failed: %w", err)
	}
	return nil
}

func runClaudeOAuthScheduledRefreshHelper(ctx context.Context, helperPath, configPath string) error {
	if !filepath.IsAbs(helperPath) || !filepath.IsAbs(configPath) {
		return errors.New("Claude OAuth scheduled refresh paths must be absolute")
	}
	cmd := exec.CommandContext(ctx, helperPath, "--config", configPath)
	cmd.Dir = "/private/tmp"
	cmd.Env = sanitizedClaudeOAuthRefreshEnv(os.Environ())
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Claude OAuth scheduled refresh helper failed: %w", err)
	}
	return nil
}

func (s *proxyServer) clearConfiguredClaudeOAuthTokens() {
	for _, profile := range configuredClaudeCredentialProfiles(s.cfg.ClaudeUsage) {
		clearClaudeProfileOAuthToken(profile)
	}
}

func (s *proxyServer) monitorClaudeOAuthRefresh(ctx context.Context) {
	if !s.cfg.ClaudeUsage.OAuthRefreshEnabled || s.claudeScheduledRunner == nil {
		return
	}
	interval := time.Duration(s.cfg.ClaudeUsage.OAuthRefreshInterval) * time.Second
	timeout := time.Duration(s.cfg.ClaudeUsage.OAuthRefreshTimeout) * time.Second
	run := func() {
		started := time.Now()
		refreshCtx, cancel := context.WithTimeout(ctx, timeout)
		runErr := s.claudeScheduledRunner(refreshCtx)
		// Reconcile credentials in the background, without invalidating every
		// healthy account and sending request goroutines back to Keychain.
		for _, profile := range configuredClaudeCredentialProfiles(s.cfg.ClaudeUsage) {
			_, _ = resolveClaudeProfileOAuthToken(refreshCtx, profile, true)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		healthy, failed, statusErr := s.validateClaudeScheduledRefreshOutcome(started)
		switch {
		case statusErr != nil:
			s.logClaudeRefreshState("unverified error=" + strconv.Quote(statusErr.Error()))
		case healthy == 0:
			s.logClaudeRefreshState("unavailable profiles=" + strings.Join(failed, ","))
		case len(failed) > 0:
			s.logClaudeRefreshState(fmt.Sprintf("partial healthy=%d unavailable=%s", healthy, strings.Join(failed, ",")))
		case runErr != nil:
			// Fresh per-profile status is stronger evidence than aggregate helper
			// exit: quota-limited model probes may return non-zero after refresh.
			s.logClaudeRefreshState(fmt.Sprintf("healthy profiles=%d helperExit=nonzero", healthy))
		default:
			s.logClaudeRefreshState(fmt.Sprintf("healthy profiles=%d", healthy))
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (s *proxyServer) triggerClaudeOAuthRefresh(profile claudeUsageProfile) {
	if (profile.CachePath == "" && s.cfg.Definition == nil) || profile.CredentialsService == "" || s.claudeRefreshRunner == nil {
		return
	}
	key := filepath.Dir(profile.CachePath) + "\x00" + profile.CredentialsService
	s.claudeRefreshMu.Lock()
	if s.claudeRefreshInFlight == nil {
		s.claudeRefreshInFlight = map[string]bool{}
	}
	if s.claudeRefreshInFlight[key] || time.Since(s.claudeRefreshLastAttempt[key]) < time.Minute {
		s.claudeRefreshMu.Unlock()
		return
	}
	s.claudeRefreshInFlight[key] = true
	if s.claudeRefreshLastAttempt == nil {
		s.claudeRefreshLastAttempt = map[string]time.Time{}
	}
	s.claudeRefreshLastAttempt[key] = time.Now()
	s.claudeRefreshMu.Unlock()

	go func() {
		defer func() {
			s.claudeRefreshMu.Lock()
			delete(s.claudeRefreshInFlight, key)
			s.claudeRefreshMu.Unlock()
		}()
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := s.claudeRefreshRunner(ctx, profile); err != nil {
			clearClaudeProfileOAuthToken(profile)
			log.Printf("Claude OAuth refresh helper profile=%q failed", profile.Name)
			return
		}
		clearClaudeProfileOAuthToken(profile)
		if s.cfg.Definition == nil {
			if err := s.validateClaudeProfileRefreshOutcome(profile, started); err != nil {
				log.Printf("Claude OAuth refresh helper profile=%q unverified error=%q", profile.Name, err.Error())
				return
			}
		}
		if s.clearClaudeProfileAuthBlock(profile, started) {
			log.Printf("Claude OAuth refresh helper profile=%q cleared account auth block", profile.Name)
		}
		log.Printf("Claude OAuth refresh helper profile=%q completed", profile.Name)
	}()
}

type claudeCredentialRefreshProfileHealth struct {
	State            string  `json:"state"`
	ExpiresAt        *string `json:"expiresAt"`
	RemainingSeconds *int64  `json:"remainingSeconds"`
	LastAttemptAt    *string `json:"lastAttemptAt"`
	LastSuccessAt    *string `json:"lastSuccessAt"`
	Error            *string `json:"error"`
}

type claudeCredentialRefreshHealth struct {
	CheckedAt   *string                                         `json:"checkedAt"`
	GeneratedAt *string                                         `json:"generatedAt"`
	Profiles    map[string]claudeCredentialRefreshProfileHealth `json:"profiles"`
}

func sanitizedRefreshTimestamp(value *string) *string {
	if value == nil {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, *value)
	if err != nil {
		return nil
	}
	normalized := parsed.UTC().Format(time.RFC3339)
	return &normalized
}

func (s *proxyServer) claudeCredentialRefreshHealth() claudeCredentialRefreshHealth {
	health := claudeCredentialRefreshHealth{Profiles: map[string]claudeCredentialRefreshProfileHealth{}}
	body, err := os.ReadFile(s.claudeRefreshStatus)
	if err != nil {
		return health
	}
	var stored claudeCredentialRefreshHealth
	if json.Unmarshal(body, &stored) != nil {
		return health
	}
	health.CheckedAt = sanitizedRefreshTimestamp(stored.CheckedAt)
	health.GeneratedAt = sanitizedRefreshTimestamp(stored.GeneratedAt)
	allowedStates := map[string]bool{
		"valid": true, "refreshed": true, "recovered": true, "refresh_failed": true,
		"cooldown": true, "locked": true, "credential_unavailable": true,
		"invalid_expiry": true,
	}
	allowedErrors := map[string]bool{
		"keychain_read_failed": true, "invalid_expires_at": true, "claude_cli_failed": true,
		"expiry_not_extended": true, "claude_cli_failed_credential_valid": true,
	}
	for _, configured := range configuredClaudeCredentialProfiles(s.cfg.ClaudeUsage) {
		entry, ok := stored.Profiles[configured.Name]
		if !ok {
			continue
		}
		if !allowedStates[entry.State] {
			continue
		}
		entry.ExpiresAt = sanitizedRefreshTimestamp(entry.ExpiresAt)
		entry.LastAttemptAt = sanitizedRefreshTimestamp(entry.LastAttemptAt)
		entry.LastSuccessAt = sanitizedRefreshTimestamp(entry.LastSuccessAt)
		if entry.Error != nil && !allowedErrors[*entry.Error] {
			entry.Error = nil
		}
		health.Profiles[configured.Name] = entry
	}
	return health
}

func claudeRefreshEntryUsable(entry claudeCredentialRefreshProfileHealth) bool {
	switch entry.State {
	case "valid", "refreshed", "recovered", "cooldown":
	default:
		return false
	}
	return entry.RemainingSeconds != nil && *entry.RemainingSeconds > 0
}

func (s *proxyServer) freshClaudeRefreshHealth(started time.Time) (claudeCredentialRefreshHealth, error) {
	health := s.claudeCredentialRefreshHealth()
	if health.GeneratedAt == nil {
		return health, errors.New("Claude OAuth refresh status missing generatedAt")
	}
	generatedAt, err := time.Parse(time.RFC3339, *health.GeneratedAt)
	if err != nil {
		return health, errors.New("Claude OAuth refresh status has invalid generatedAt")
	}
	// The shell helper timestamps to whole seconds. Permit that truncation, but
	// never accept a previous run's status as proof for this refresh attempt.
	if generatedAt.Before(started.UTC().Truncate(time.Second)) {
		return health, fmt.Errorf("Claude OAuth refresh status is stale: %s", generatedAt.Format(time.RFC3339))
	}
	return health, nil
}

func (s *proxyServer) validateClaudeProfileRefreshOutcome(profile claudeUsageProfile, started time.Time) error {
	health, err := s.freshClaudeRefreshHealth(started)
	if err != nil {
		return err
	}
	entry, ok := health.Profiles[profile.Name]
	if !ok {
		return fmt.Errorf("Claude OAuth refresh status missing profile %q", profile.Name)
	}
	if !claudeRefreshEntryUsable(entry) {
		return fmt.Errorf("Claude OAuth refresh profile %q state=%s", profile.Name, firstNonEmpty(entry.State, "missing"))
	}
	return nil
}

func (s *proxyServer) validateClaudeScheduledRefreshOutcome(started time.Time) (int, []string, error) {
	health, err := s.freshClaudeRefreshHealth(started)
	if err != nil {
		return 0, nil, err
	}
	healthy := 0
	failed := []string{}
	for _, profile := range configuredClaudeCredentialProfiles(s.cfg.ClaudeUsage) {
		entry, ok := health.Profiles[profile.Name]
		if ok && claudeRefreshEntryUsable(entry) {
			healthy++
			continue
		}
		state := "missing"
		if ok && entry.State != "" {
			state = entry.State
		}
		failed = append(failed, profile.Name+":"+state)
	}
	return healthy, failed, nil
}

func (s *proxyServer) logClaudeRefreshState(summary string) {
	s.claudeRefreshMu.Lock()
	if s.claudeRefreshSummary == summary {
		s.claudeRefreshMu.Unlock()
		return
	}
	s.claudeRefreshSummary = summary
	s.claudeRefreshMu.Unlock()
	log.Printf("Claude OAuth refresh state %s", summary)
}

func configuredClaudeCredentialProfiles(policy claudeUsageConfig) []claudeUsageProfile {
	profiles := make([]claudeUsageProfile, 0, len(policy.ListenerProfiles)+len(policy.AccountProfiles))
	seenDirs := map[string]bool{}
	seenServices := map[string]bool{}
	addMap := func(profileMap map[string]claudeUsageProfile) {
		keys := make([]string, 0, len(profileMap))
		for key := range profileMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			profile := profileMap[key]
			if profile.Name == "" {
				profile.Name = key
			}
			configDir := filepath.Dir(profile.CachePath)
			if profile.Name == "" || !filepath.IsAbs(configDir) || profile.CredentialsService == "" || seenDirs[configDir] || seenServices[profile.CredentialsService] {
				continue
			}
			seenDirs[configDir] = true
			seenServices[profile.CredentialsService] = true
			profiles = append(profiles, profile)
		}
	}
	addMap(policy.ListenerProfiles)
	addMap(policy.AccountProfiles)
	return profiles
}

type selectedClaudeProfileContextKey struct{}

// Claude account reservations are request-scoped. The initially selected
// account is reserved by bindClaudeAccount; fallback accounts acquire their
// own reservation only while their attempt is active. Keeping the release
// closure in context lets doWithFallbacks hand the initial lease off at the
// exact failure boundary without changing the public handler signature.
type claudeAccountReservationContextKey struct{}

type claudeAccountReservation struct {
	profile claudeUsageProfile
	release func()
}

func selectedClaudeProfile(r *http.Request) (claudeUsageProfile, bool) {
	if r == nil {
		return claudeUsageProfile{}, false
	}
	profile, ok := r.Context().Value(selectedClaudeProfileContextKey{}).(claudeUsageProfile)
	return profile, ok && profile.Name != ""
}

func (s *proxyServer) listenerClaudeProfile(r *http.Request) claudeUsageProfile {
	if s.cfg.ClaudeUsage.AutoSelectAccounts {
		if pool := s.claudeAccountPoolForListener(requestListenerPort(r)); pool.Primary.Name != "" {
			return pool.Primary
		}
	}
	return s.cfg.ClaudeUsage.ListenerProfiles[requestListenerPort(r)]
}

func (s *proxyServer) claudeOAuthBearer(ctx context.Context, r *http.Request) (string, error) {
	if profile, ok := selectedClaudeProfile(r); ok {
		token, err := resolveClaudeProfileOAuthToken(ctx, profile)
		if err != nil {
			return "", fmt.Errorf("Claude subscription auth: %w", err)
		}
		return "Bearer " + token, nil
	}
	if auth, err := requestOAuthBearer(r); err == nil {
		return auth, nil
	}
	profile := s.claudeProfileForRequest(r)
	token, err := resolveClaudeProfileOAuthToken(ctx, profile)
	if err != nil {
		return "", fmt.Errorf("Claude subscription auth: %w", err)
	}
	return "Bearer " + token, nil
}

func requestListenerPort(r *http.Request) string {
	if local, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		if _, port, err := net.SplitHostPort(local.String()); err == nil {
			return port
		}
	}
	if _, port, err := net.SplitHostPort(r.Host); err == nil {
		return port
	}
	return ""
}

func defaultClaudeProfile(name, configDirName string) claudeUsageProfile {
	home, err := os.UserHomeDir()
	if err != nil {
		return claudeUsageProfile{Name: name}
	}
	configDir := filepath.Join(home, configDirName)
	digest := sha256.Sum256([]byte(configDir))
	return claudeUsageProfile{
		Name:               name,
		CachePath:          filepath.Join(configDir, ".claude.json"),
		CredentialsService: "Claude Code-credentials-" + fmt.Sprintf("%x", digest[:4]),
	}
}

func defaultClaudeAccountPools() map[string]claudeAccountPool {
	return map[string]claudeAccountPool{}
}

func (s *proxyServer) configuredClaudeProfileByName(name string) (claudeUsageProfile, bool) {
	for _, profile := range s.cfg.ClaudeUsage.ListenerProfiles {
		if profile.Name == name {
			return profile, true
		}
	}
	for key, profile := range s.cfg.ClaudeUsage.AccountProfiles {
		if profile.Name == "" {
			profile.Name = key
		}
		if profile.Name == name {
			return profile, true
		}
	}
	return claudeUsageProfile{}, false
}

// claudeAccountPoolForListener preserves dedicated-listener compatibility for
// callers that disable automatic account selection. Automatic selection uses
// the unified AccountPool instead.
func (s *proxyServer) claudeAccountPoolForListener(listener string) claudeAccountPool {
	defaults, ok := defaultClaudeAccountPools()[listener]
	if !ok {
		return claudeAccountPool{Primary: s.cfg.ClaudeUsage.ListenerProfiles[listener]}
	}
	if configured, found := s.configuredClaudeProfileByName(defaults.Primary.Name); found {
		defaults.Primary = configured
	}
	if configured, found := s.configuredClaudeProfileByName(defaults.Fallback.Name); found {
		defaults.Fallback = configured
	}
	return defaults
}

func claudePoolProfiles(pool claudeAccountPool) []claudeUsageProfile {
	profiles := make([]claudeUsageProfile, 0, 2)
	seen := map[string]bool{}
	for _, profile := range []claudeUsageProfile{pool.Primary, pool.Fallback} {
		key := firstNonEmpty(profile.CredentialsService, profile.Name)
		if profile.Name == "" || key == "" || seen[key] {
			continue
		}
		seen[key] = true
		profiles = append(profiles, profile)
	}
	return profiles
}

func claudeProfileRegistryKey(profile claudeUsageProfile) string {
	if profile.CredentialsService != "" {
		return profile.CredentialsService
	}
	if profile.Name != "" {
		return "name:" + profile.Name
	}
	return ""
}

func (s *proxyServer) canonicalClaudeProfileOrder() []claudeUsageProfile {
	names := s.cfg.ClaudeUsage.AccountPool
	if len(names) == 0 {
		names = sortedKeys(s.cfg.ClaudeUsage.AccountProfiles)
	}
	profiles := make([]claudeUsageProfile, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		profile, found := s.configuredClaudeProfileByName(name)
		if !found || profile.Name == "" {
			continue
		}
		key := claudeProfileRegistryKey(profile)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		profiles = append(profiles, profile)
	}
	return profiles
}

// eligibleClaudePoolProfiles returns one identity-deduplicated account pool
// for every listener. The listener argument remains for call-site and config
// compatibility; it no longer partitions subscription capacity.
func (s *proxyServer) eligibleClaudePoolProfiles(_ string) []claudeUsageProfile {
	result := make([]claudeUsageProfile, 0, len(s.cfg.ClaudeUsage.AccountPool))
	seenAccounts := map[string]bool{}
	seenProfiles := map[string]bool{}
	for _, profile := range s.canonicalClaudeProfileOrder() {
		profileKey := claudeProfileRegistryKey(profile)
		if profileKey == "" || seenProfiles[profileKey] {
			continue
		}
		accountKey := s.claudeProfileAccountKey(profile)
		if accountKey == "" || seenAccounts[accountKey] {
			if accountKey != "" {
				continue
			}
		}
		seenProfiles[profileKey] = true
		if accountKey != "" {
			seenAccounts[accountKey] = true
		}
		result = append(result, profile)
	}
	return result
}

func (s *proxyServer) claudeProfileForRequest(r *http.Request) claudeUsageProfile {
	if profile, ok := selectedClaudeProfile(r); ok {
		return profile
	}
	return s.listenerClaudeProfile(r)
}

func (s *proxyServer) configuredClaudeProfiles() []claudeUsageProfile {
	profiles := []claudeUsageProfile{}
	seenServices := map[string]bool{}
	seenAccounts := map[string]bool{}
	add := func(profile claudeUsageProfile) {
		if profile.Name == "" || (profile.CredentialsService == "" && profile.CachePath == "") {
			return
		}
		serviceKey := firstNonEmpty(profile.CredentialsService, "name:"+profile.Name)
		if seenServices[serviceKey] {
			return
		}
		accountKey := s.claudeProfileAccountKey(profile)
		if accountKey != "" && seenAccounts[accountKey] {
			return
		}
		seenServices[serviceKey] = true
		if accountKey != "" {
			seenAccounts[accountKey] = true
		}
		profiles = append(profiles, profile)
	}
	addMap := func(profileMap map[string]claudeUsageProfile) {
		keys := make([]string, 0, len(profileMap))
		for key := range profileMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			add(profileMap[key])
		}
	}
	if s.cfg.ClaudeUsage.AutoSelectAccounts {
		for _, profile := range s.canonicalClaudeProfileOrder() {
			add(profile)
		}
	}
	addMap(s.cfg.ClaudeUsage.ListenerProfiles)
	addMap(s.cfg.ClaudeUsage.AccountProfiles)
	return profiles
}

func readClaudeProfileAccountKey(profile claudeUsageProfile) string {
	if profile.CachePath == "" {
		return ""
	}
	data, err := os.ReadFile(expandHomePath(profile.CachePath))
	if err != nil {
		return ""
	}
	var state struct {
		OAuthAccount struct {
			AccountUUID      string `json:"accountUuid"`
			OrganizationUUID string `json:"organizationUuid"`
		} `json:"oauthAccount"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return ""
	}
	if state.OAuthAccount.AccountUUID != "" && state.OAuthAccount.OrganizationUUID != "" {
		return "account:" + state.OAuthAccount.AccountUUID + "/organization:" + state.OAuthAccount.OrganizationUUID
	}
	if state.OAuthAccount.AccountUUID != "" {
		return "account:" + state.OAuthAccount.AccountUUID
	}
	if state.OAuthAccount.OrganizationUUID != "" {
		return "organization:" + state.OAuthAccount.OrganizationUUID
	}
	return ""
}

func (s *proxyServer) claudeProfileAccountKey(profile claudeUsageProfile) string {
	path := expandHomePath(profile.CachePath)
	if path == "" {
		return ""
	}
	info, err := os.Stat(path)
	if err != nil {
		return ""
	}
	s.claudeIdentityMu.Lock()
	defer s.claudeIdentityMu.Unlock()
	if s.claudeIdentities == nil {
		s.claudeIdentities = map[string]claudeIdentityCacheEntry{}
	}
	if cached, ok := s.claudeIdentities[path]; ok && cached.size == info.Size() && cached.modifiedAt.Equal(info.ModTime()) {
		return cached.accountKey
	}
	key := readClaudeProfileAccountKey(profile)
	s.claudeIdentities[path] = claudeIdentityCacheEntry{modifiedAt: info.ModTime(), size: info.Size(), accountKey: key}
	return key
}

func (s *proxyServer) claudeSnapshotEligible(snapshot claudeUsageSnapshot) bool {
	policy := s.cfg.ClaudeUsage
	return snapshot.FiveHour.Utilization < policy.FiveHourThresholdPct &&
		snapshot.SevenDay.Utilization < policy.sevenDayThresholdForProfile(snapshot.Profile)
}

func (policy claudeUsageConfig) sevenDayThresholdForProfile(name string) float64 {
	for key, profile := range policy.AccountProfiles {
		if (key == name || profile.Name == name) && profile.SevenDayThresholdPct != nil {
			return *profile.SevenDayThresholdPct
		}
	}
	for _, profile := range policy.ListenerProfiles {
		if profile.Name == name && profile.SevenDayThresholdPct != nil {
			return *profile.SevenDayThresholdPct
		}
	}
	return policy.SevenDayThresholdPct
}

func (s *proxyServer) accountNow() time.Time {
	if s.clockNow != nil {
		return s.clockNow()
	}
	return time.Now()
}

func (s *proxyServer) claudeAccountLeaseTTL() time.Duration {
	seconds := s.cfg.ClaudeUsage.AccountStickySeconds
	if seconds <= 0 {
		seconds = 1800
	}
	return time.Duration(seconds) * time.Second
}

func (s *proxyServer) refreshClaudeAccountLease(stickyKey string, profile claudeUsageProfile, seconds ...int) {
	if stickyKey == "" || profile.Name == "" {
		return
	}
	s.claudeAccountMu.Lock()
	if s.claudeAccounts == nil {
		s.claudeAccounts = map[string]claudeAccountSticky{}
	}
	ttl := s.claudeAccountLeaseTTL()
	if len(seconds) > 0 && seconds[0] > 0 {
		ttl = time.Duration(seconds[0]) * time.Second
	}
	s.claudeAccounts[stickyKey] = claudeAccountSticky{Profile: profile, Until: s.accountNow().Add(ttl)}
	s.claudeAccountMu.Unlock()
}

func profileInClaudePool(profile claudeUsageProfile, profiles []claudeUsageProfile) bool {
	key := claudeProfileRegistryKey(profile)
	for _, candidate := range profiles {
		if claudeProfileRegistryKey(candidate) == key {
			return true
		}
	}
	return false
}

func (s *proxyServer) releaseClaudeAccount(profile claudeUsageProfile) {
	key := claudeProfileRegistryKey(profile)
	if key == "" {
		return
	}
	s.claudeAccountMu.Lock()
	defer s.claudeAccountMu.Unlock()
	if current := s.claudeAccountInFlight[key]; current > 1 {
		s.claudeAccountInFlight[key] = current - 1
	} else {
		delete(s.claudeAccountInFlight, key)
	}
}

func (s *proxyServer) reserveClaudeAccount(profile claudeUsageProfile) func() {
	key := claudeProfileRegistryKey(profile)
	if key == "" {
		return nil
	}
	s.claudeAccountMu.Lock()
	if s.claudeAccountInFlight == nil {
		s.claudeAccountInFlight = map[string]int{}
	}
	s.claudeAccountInFlight[key]++
	s.claudeAccountMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { s.releaseClaudeAccount(profile) })
	}
}

func (s *proxyServer) orderClaudeFallbackProfiles(profiles []claudeUsageProfile, selected claudeUsageProfile) []claudeUsageProfile {
	selectedKey := claudeProfileRegistryKey(selected)
	if selectedKey == "" {
		return profiles
	}

	s.claudeAccountMu.Lock()
	inFlight := make(map[string]int, len(s.claudeAccountInFlight))
	for key, count := range s.claudeAccountInFlight {
		inFlight[key] = count
	}
	s.claudeAccountMu.Unlock()
	s.claudeUse.mu.Lock()
	snapshots := newestClaudeUsageSnapshots(s.claudeUse.snapshots, s.accountNow(), time.Duration(s.cfg.ClaudeUsage.StaleTTLSeconds)*time.Second)
	s.claudeUse.mu.Unlock()

	ordered := make([]claudeUsageProfile, 0, len(profiles))
	remaining := make([]claudeProfileRoutingLoad, 0, len(profiles))
	for _, profile := range profiles {
		key := claudeProfileRegistryKey(profile)
		if key == selectedKey {
			ordered = append(ordered, profile)
			continue
		}
		snapshot, hasUsage := snapshots[profile.Name]
		blocked, _ := s.providerBlocked(s.cfg.ClaudeUsage.Provider + "@" + profile.Name)
		remaining = append(remaining, claudeProfileRoutingLoad{
			profile: profile, inFlight: inFlight[key], blocked: blocked,
			snapshot: snapshot, hasUsage: hasUsage,
		})
	}
	if len(ordered) == 0 {
		return profiles
	}
	sort.SliceStable(remaining, func(i, j int) bool {
		if remaining[i].blocked != remaining[j].blocked {
			return !remaining[i].blocked
		}
		if remaining[i].inFlight != remaining[j].inFlight {
			return remaining[i].inFlight < remaining[j].inFlight
		}
		if remaining[i].hasUsage != remaining[j].hasUsage {
			return remaining[i].hasUsage
		}
		if remaining[i].snapshot.FiveHour.Utilization != remaining[j].snapshot.FiveHour.Utilization {
			return remaining[i].snapshot.FiveHour.Utilization < remaining[j].snapshot.FiveHour.Utilization
		}
		return remaining[i].snapshot.SevenDay.Utilization < remaining[j].snapshot.SevenDay.Utilization
	})
	for _, load := range remaining {
		ordered = append(ordered, load.profile)
	}
	return ordered
}

const claudeAccountHeadroomSwitchMargin = 20.0

func claudeUsagePressure(snapshot claudeUsageSnapshot) float64 {
	return math.Max(snapshot.FiveHour.Utilization, snapshot.SevenDay.Utilization)
}

func (s *proxyServer) chooseClaudeAccount(available []availableClaudeAccount, stickyKey string, stickyProfile claudeUsageProfile, reserve bool, seconds ...int) (availableClaudeAccount, func(), bool) {
	s.claudeAccountMu.Lock()
	defer s.claudeAccountMu.Unlock()
	if s.claudeAccountInFlight == nil {
		s.claudeAccountInFlight = map[string]int{}
	}

	stickyRegistryKey := claudeProfileRegistryKey(stickyProfile)
	stickyAvailable := false
	var stickySnapshot claudeUsageSnapshot
	for _, candidate := range available {
		if claudeProfileRegistryKey(candidate.profile) == stickyRegistryKey {
			stickyAvailable = true
			stickySnapshot = candidate.snapshot
			break
		}
	}
	sort.SliceStable(available, func(i, j int) bool {
		iKey := claudeProfileRegistryKey(available[i].profile)
		jKey := claudeProfileRegistryKey(available[j].profile)
		if reserve {
			iInFlight := s.claudeAccountInFlight[iKey]
			jInFlight := s.claudeAccountInFlight[jKey]
			if iInFlight != jInFlight {
				return iInFlight < jInFlight
			}
		}
		if stickyRegistryKey != "" {
			// Unknown is not 100% remaining: never compare fabricated zero
			// utilization against measured quota to break an affinity lease.
			if s.cfg.Definition != nil && (available[i].snapshot.Source == "unknown" || available[j].snapshot.Source == "unknown") {
				if iKey == stickyRegistryKey || jKey == stickyRegistryKey {
					return iKey == stickyRegistryKey
				}
			}
			iSticky := iKey == stickyRegistryKey
			jSticky := jKey == stickyRegistryKey
			if iSticky != jSticky {
				if iSticky {
					return claudeUsagePressure(available[i].snapshot) <= claudeUsagePressure(available[j].snapshot)+claudeAccountHeadroomSwitchMargin
				}
				return claudeUsagePressure(available[j].snapshot) > claudeUsagePressure(available[i].snapshot)+claudeAccountHeadroomSwitchMargin
			}
		}
		if s.cfg.Definition != nil && (available[i].snapshot.Source == "unknown") != (available[j].snapshot.Source == "unknown") {
			return available[i].snapshot.Source != "unknown"
		}
		if available[i].snapshot.FiveHour.Utilization != available[j].snapshot.FiveHour.Utilization {
			return available[i].snapshot.FiveHour.Utilization < available[j].snapshot.FiveHour.Utilization
		}
		return available[i].snapshot.SevenDay.Utilization < available[j].snapshot.SevenDay.Utilization
	})

	chosen := available[0]
	chosenKey := claudeProfileRegistryKey(chosen.profile)
	stickyMateriallyWorse := stickyAvailable && claudeUsagePressure(stickySnapshot) > claudeUsagePressure(chosen.snapshot)+claudeAccountHeadroomSwitchMargin
	stickyChanged := stickyRegistryKey != "" && chosenKey != stickyRegistryKey && stickyMateriallyWorse
	if stickyRegistryKey == "" || chosenKey == stickyRegistryKey || !stickyAvailable || stickyMateriallyWorse {
		if s.claudeAccounts == nil {
			s.claudeAccounts = map[string]claudeAccountSticky{}
		}
		if stickyKey != "" {
			ttl := s.claudeAccountLeaseTTL()
			if len(seconds) > 0 && seconds[0] > 0 {
				ttl = time.Duration(seconds[0]) * time.Second
			}
			s.claudeAccounts[stickyKey] = claudeAccountSticky{Profile: chosen.profile, Until: s.accountNow().Add(ttl)}
		}
	}
	if !reserve {
		return chosen, nil, stickyChanged
	}
	s.claudeAccountInFlight[chosenKey]++
	var once sync.Once
	return chosen, func() {
		once.Do(func() { s.releaseClaudeAccount(chosen.profile) })
	}, stickyChanged
}

func (s *proxyServer) selectClaudeProfile(ctx context.Context, r *http.Request, stickyKey string, reserve bool, candidate modelConfig) (claudeUsageProfile, claudeUsageSnapshot, func(), error) {
	listener := s.listenerClaudeProfile(r)
	if !s.cfg.ClaudeUsage.AutoSelectAccounts {
		snapshot, err := s.claudeUsageStatusSnapshotContext(ctx, listener)
		return listener, snapshot, nil, err
	}

	profiles := s.profilesForCandidate(candidate, r)
	if len(profiles) == 0 {
		return claudeUsageProfile{}, claudeUsageSnapshot{}, nil, errors.New("unified Claude account pool has no unique eligible profiles")
	}
	now := s.accountNow()
	var stickyProfile claudeUsageProfile
	if stickyKey != "" {
		s.claudeAccountMu.Lock()
		sticky, ok := s.claudeAccounts[stickyKey]
		if ok && !now.Before(sticky.Until) {
			delete(s.claudeAccounts, stickyKey)
			ok = false
		}
		s.claudeAccountMu.Unlock()
		if ok && profileInClaudePool(sticky.Profile, profiles) {
			stickyProfile = sticky.Profile
		}
	}
	available := make([]availableClaudeAccount, 0, len(profiles))
	var failures []string
	for _, profile := range profiles {
		runtimeKey := s.cfg.ClaudeUsage.Provider + "@" + profile.Name
		blocked, state := s.providerBlockForCandidate(runtimeKey, candidate)
		if blocked {
			failures = append(failures, profile.Name+":"+firstNonEmpty(state.Reason, "blocked"))
			continue
		}
		snapshot, err := s.claudeUsageStatusSnapshotContext(ctx, profile)
		if err != nil {
			if s.cfg.Definition == nil {
				failures = append(failures, profile.Name+":usage_unavailable")
				continue
			}
			snapshot = claudeUsageSnapshot{Profile: profile.Name, Source: "unknown"}
		}
		if !s.snapshotAllowedForCandidate(snapshot, candidate) {
			failures = append(failures, profile.Name+":usage_exhausted")
			continue
		}
		if reason := s.claudeWorkerReserveReason(snapshot, candidate); reason != "" {
			failures = append(failures, profile.Name+":"+reason)
			continue
		}
		available = append(available, availableClaudeAccount{profile: profile, snapshot: snapshot})
	}
	if len(available) > 0 {
		chosen, release, stickyChanged := s.chooseClaudeAccount(available, stickyKey, stickyProfile, reserve, candidate.AccountStickySeconds)
		s.logClaudeAccountSelection(requestListenerPort(r), chosen, reserve, stickyChanged)
		return chosen.profile, chosen.snapshot, release, nil
	}
	return claudeUsageProfile{}, claudeUsageSnapshot{}, nil, fmt.Errorf("unified Claude account pool exhausted or blocked: %s", strings.Join(failures, ", "))
}

func (s *proxyServer) logClaudeAccountSelection(listener string, chosen availableClaudeAccount, reserve, stickyChanged bool) {
	if listener == "" {
		listener = "default"
	}
	s.claudeSelectionLogMu.Lock()
	if s.claudeSelectionLogged == nil {
		s.claudeSelectionLogged = map[string]string{}
	}
	activeKey := listener + "|active"
	previous, alreadyLogged := s.claudeSelectionLogged[activeKey]
	shouldLog := !alreadyLogged || stickyChanged
	if shouldLog {
		s.claudeSelectionLogged[activeKey] = chosen.profile.Name
	}
	delete(s.claudeSelectionLogged, listener+"|error")
	s.claudeSelectionLogMu.Unlock()
	if !shouldLog {
		return
	}
	log.Printf("Claude account selection changed pool=unified listener=%q from=%q to=%q fiveHour=%.1f%% sevenDay=%.1f%% reserved=%t stickyChanged=%t", listener, previous, chosen.profile.Name, chosen.snapshot.FiveHour.Utilization, chosen.snapshot.SevenDay.Utilization, reserve, stickyChanged)
}

func (s *proxyServer) logClaudeAccountSelectionError(listener string, err error) {
	if listener == "" {
		listener = "default"
	}
	message := err.Error()
	s.claudeSelectionLogMu.Lock()
	if s.claudeSelectionLogged == nil {
		s.claudeSelectionLogged = map[string]string{}
	}
	key := listener + "|error"
	if s.claudeSelectionLogged[key] == message {
		s.claudeSelectionLogMu.Unlock()
		return
	}
	s.claudeSelectionLogged[key] = message
	s.claudeSelectionLogMu.Unlock()
	log.Printf("Claude account auto-selection unavailable listener=%q error=%v", listener, err)
}

func (s *proxyServer) autoSelectClaudeProfile(ctx context.Context, r *http.Request, stickyKey string) (claudeUsageProfile, claudeUsageSnapshot, error) {
	profile, snapshot, _, err := s.selectClaudeProfile(ctx, r, stickyKey, false, modelConfig{})
	return profile, snapshot, err
}

func (s *proxyServer) requestMayUseClaude(selected modelConfig) bool {
	for _, candidate := range s.collectCandidates(selected) {
		if s.claudeSubscriptionCandidate(candidate) {
			return true
		}
	}
	return false
}

func (s *proxyServer) bindClaudeAccount(ctx context.Context, r *http.Request, selected modelConfig, payload map[string]any, reserve bool) (*http.Request, func()) {
	if !s.cfg.ClaudeUsage.AutoSelectAccounts || firstNonEmpty(selected.Provider, s.cfg.DefaultProvider) != s.cfg.ClaudeUsage.Provider {
		return r, nil
	}
	stickyKey := routingStickyKey(selected, payload)
	reserve = reserve && firstNonEmpty(selected.Provider, s.cfg.DefaultProvider) == s.cfg.ClaudeUsage.Provider
	profile, _, release, err := s.selectClaudeProfile(ctx, r, stickyKey, reserve, selected)
	if err != nil {
		s.logClaudeAccountSelectionError(s.listenerClaudeProfile(r).Name, err)
		return r, nil
	}
	boundCtx := context.WithValue(r.Context(), selectedClaudeProfileContextKey{}, profile)
	if release != nil {
		boundCtx = context.WithValue(boundCtx, claudeAccountReservationContextKey{}, claudeAccountReservation{profile: profile, release: release})
	}
	return r.WithContext(boundCtx), release
}

// providerRuntimeKey isolates subscription-backed Anthropic health state by
// Claude account. Both hybrid profiles share one proxy process, but a 429 on
// one OAuth allowance must not quarantine the other profile. Other providers
// keep provider-wide state because their credentials and quotas are shared.
func (s *proxyServer) providerRuntimeKey(r *http.Request, providerName string) string {
	if providerName == "" || providerName != s.cfg.ClaudeUsage.Provider {
		return providerName
	}
	if profile := s.claudeProfileForRequest(r); profile.Name != "" {
		return providerName + "@" + profile.Name
	}
	if port := requestListenerPort(r); port != "" {
		return providerName + "@listener-" + port
	}
	if auth, err := requestOAuthBearer(r); err == nil {
		key := claudeTokenKey(auth)
		return providerName + "@oauth-" + key[:12]
	}
	return providerName
}

func (s *proxyServer) providerRuntimeKeyForCandidate(r *http.Request, providerName string, candidate modelConfig) string {
	if providerName == s.cfg.ClaudeUsage.Provider && candidate.ClaudeProfile != "" {
		return providerName + "@" + candidate.ClaudeProfile
	}
	return s.providerRuntimeKey(r, providerName)
}

func claudeTokenKey(auth string) string {
	sum := sha256.Sum256([]byte(auth))
	return fmt.Sprintf("%x", sum[:])
}

func (s *proxyServer) fetchClaudeUsageSnapshot(ctx context.Context, auth, profile string) (claudeUsageSnapshot, error) {
	release, err := s.admitClaudeUsageFetch(ctx)
	if err != nil {
		return claudeUsageSnapshot{}, err
	}
	defer release()
	policy := s.cfg.ClaudeUsage
	providerURL, ok := s.providers[policy.Provider]
	if !ok {
		return claudeUsageSnapshot{}, fmt.Errorf("Claude usage provider %q is unavailable", policy.Provider)
	}
	usageURL := *providerURL
	usageURL.Path = joinURLPath(providerURL.Path, "/api/oauth/usage")
	usageURL.RawQuery = ""
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(policy.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, usageURL.String(), nil)
	if err != nil {
		return claudeUsageSnapshot{}, err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.clients[policy.Provider].Do(req)
	if err != nil {
		return claudeUsageSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := providerCooldown(resp.Header, nil)
			s.claudeUse.mu.Lock()
			s.claudeUse.globalRetryAt = time.Now().Add(delay)
			s.claudeUse.mu.Unlock()
			return claudeUsageSnapshot{}, &claudeUsageRateLimit{delay: delay}
		}
		return claudeUsageSnapshot{}, fmt.Errorf("Claude usage endpoint returned status %d", resp.StatusCode)
	}
	var payload claudeUsageResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return claudeUsageSnapshot{}, fmt.Errorf("decode Claude usage response: %w", err)
	}
	if payload.FiveHour.Utilization < 0 || payload.FiveHour.Utilization > 100 ||
		payload.SevenDay.Utilization < 0 || payload.SevenDay.Utilization > 100 {
		return claudeUsageSnapshot{}, errors.New("Claude usage response contains utilization outside 0..100")
	}
	return claudeUsageSnapshot{
		FetchedAt: time.Now(), Profile: profile, FiveHour: payload.FiveHour,
		SevenDay: payload.SevenDay, Source: "oauth-api",
	}, nil
}

func expandHomePath(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return path
}

func readClaudeCLIUsage(profile claudeUsageProfile, staleAfter time.Duration) (claudeUsageSnapshot, error) {
	if profile.CachePath == "" {
		return claudeUsageSnapshot{}, errors.New("Claude profile has no CLI cache path")
	}
	body, err := os.ReadFile(expandHomePath(profile.CachePath))
	if err != nil {
		return claudeUsageSnapshot{}, err
	}
	var cached struct {
		Usage struct {
			FetchedAtMS int64               `json:"fetchedAtMs"`
			Utilization claudeUsageResponse `json:"utilization"`
		} `json:"cachedUsageUtilization"`
	}
	if err := json.Unmarshal(body, &cached); err != nil {
		return claudeUsageSnapshot{}, err
	}
	fetchedAt := time.UnixMilli(cached.Usage.FetchedAtMS)
	if cached.Usage.FetchedAtMS <= 0 || time.Since(fetchedAt) > staleAfter {
		return claudeUsageSnapshot{}, errors.New("Claude CLI usage cache is stale")
	}
	if cached.Usage.Utilization.FiveHour.Utilization < 0 || cached.Usage.Utilization.FiveHour.Utilization > 100 ||
		cached.Usage.Utilization.SevenDay.Utilization < 0 || cached.Usage.Utilization.SevenDay.Utilization > 100 {
		return claudeUsageSnapshot{}, errors.New("Claude CLI usage cache contains utilization outside 0..100")
	}
	return claudeUsageSnapshot{
		FetchedAt: fetchedAt, Profile: profile.Name,
		FiveHour: cached.Usage.Utilization.FiveHour,
		SevenDay: cached.Usage.Utilization.SevenDay,
		Source:   "claude-cli-cache",
	}, nil
}

func (s *proxyServer) claudeUsageSnapshot(ctx context.Context, r *http.Request) (claudeUsageSnapshot, error) {
	profile := s.claudeProfileForRequest(r)
	if profile.Name != "" && profile.CredentialsService != "" {
		return s.claudeUsageStatusSnapshotContext(ctx, profile)
	}
	auth, err := s.claudeOAuthBearer(ctx, r)
	if err != nil {
		return claudeUsageSnapshot{}, err
	}
	return s.claudeUsageSnapshotWithAuth(ctx, auth, profile)
}

func (s *proxyServer) claudeUsageSnapshotWithAuth(ctx context.Context, auth string, profile claudeUsageProfile) (claudeUsageSnapshot, error) {
	return s.claudeUsageSnapshotRefresh(ctx, auth, profile, false)
}

func (s *proxyServer) claudeUsageSnapshotRefresh(ctx context.Context, auth string, profile claudeUsageProfile, force bool) (claudeUsageSnapshot, error) {
	policy := s.cfg.ClaudeUsage
	key := claudeTokenKey(auth)
	cacheTTL := time.Duration(policy.CacheTTLSeconds) * time.Second
	staleTTL := time.Duration(policy.StaleTTLSeconds) * time.Second

	s.claudeUse.mu.Lock()
	if s.claudeUse.snapshots == nil {
		s.claudeUse.snapshots = map[string]claudeUsageSnapshot{}
	}
	if s.claudeUse.refreshing == nil {
		s.claudeUse.refreshing = map[string]chan struct{}{}
	}
	staleSnapshot, hasStaleSnapshot := s.claudeUse.snapshots[key]
	if !hasStaleSnapshot && profile.Name != "" {
		for _, cached := range s.claudeUse.snapshots {
			if cached.Profile == profile.Name && (!hasStaleSnapshot || cached.FetchedAt.After(staleSnapshot.FetchedAt)) {
				staleSnapshot, hasStaleSnapshot = cached, true
			}
		}
	}
	if time.Now().Before(s.claudeUse.retryAt[key]) {
		s.claudeUse.mu.Unlock()
		if hasStaleSnapshot && time.Since(staleSnapshot.FetchedAt) < staleTTL {
			if staleSnapshot.Source == "oauth-api" {
				staleSnapshot.Source = "oauth-api-stale"
			}
			return staleSnapshot, nil
		}
		if cached, err := readClaudeCLIUsage(profile, staleTTL); err == nil {
			return cached, nil
		}
		return claudeUsageSnapshot{}, errors.New("Claude usage refresh cooling down after failed fetch")
	}
	if hasStaleSnapshot && !force {
		if math.Max(staleSnapshot.FiveHour.Utilization, staleSnapshot.SevenDay.Utilization) >= 90 && cacheTTL > 30*time.Second {
			cacheTTL = 30 * time.Second
		}
		if time.Since(staleSnapshot.FetchedAt) < cacheTTL {
			s.claudeUse.mu.Unlock()
			return staleSnapshot, nil
		}
	}
	if waiting, ok := s.claudeUse.refreshing[key]; ok {
		s.claudeUse.mu.Unlock()
		select {
		case <-ctx.Done():
			return claudeUsageSnapshot{}, ctx.Err()
		case <-waiting:
		}
		s.claudeUse.mu.Lock()
		cached, ok := s.claudeUse.snapshots[key]
		s.claudeUse.mu.Unlock()
		if ok {
			age := time.Since(cached.FetchedAt)
			if age < cacheTTL || age < staleTTL {
				if age >= cacheTTL && cached.Source == "oauth-api" {
					cached.Source = "oauth-api-stale"
				}
				return cached, nil
			}
		}
		return s.claudeUsageSnapshotWithAuth(ctx, auth, profile)
	}
	waiting := make(chan struct{})
	s.claudeUse.refreshing[key] = waiting
	s.claudeUse.mu.Unlock()

	snapshot, fetchErr := s.fetchClaudeUsageSnapshot(ctx, auth, profile.Name)
	if fetchErr != nil {
		delay := 5 * time.Second
		var limited *claudeUsageRateLimit
		if errors.As(fetchErr, &limited) {
			delay = limited.delay
		}
		s.claudeUse.mu.Lock()
		if s.claudeUse.retryAt == nil {
			s.claudeUse.retryAt = map[string]time.Time{}
		}
		s.claudeUse.retryAt[key] = time.Now().Add(delay)
		s.claudeUse.mu.Unlock()
	}
	if fetchErr != nil {
		if force {
			log.Printf("Claude usage background fetch failed profile=%q error=%v", profile.Name, fetchErr)
		}
		if cliSnapshot, cliErr := readClaudeCLIUsage(profile, staleTTL); cliErr == nil && (!hasStaleSnapshot || cliSnapshot.FetchedAt.After(staleSnapshot.FetchedAt)) {
			snapshot = cliSnapshot
			fetchErr = nil
		} else if hasStaleSnapshot && time.Since(staleSnapshot.FetchedAt) < staleTTL {
			snapshot = staleSnapshot
			if snapshot.Source == "oauth-api" {
				snapshot.Source = "oauth-api-stale"
			}
			fetchErr = nil
		}
	}

	s.claudeUse.mu.Lock()
	if fetchErr == nil {
		snapshot.TokenKey = key
		s.rememberWorkerUsageLocked(snapshot)
		for existingKey, cached := range s.claudeUse.snapshots {
			if existingKey != key && snapshot.Profile != "" && cached.Profile == snapshot.Profile && !cached.FetchedAt.After(snapshot.FetchedAt) {
				delete(s.claudeUse.snapshots, existingKey)
			}
		}
		s.claudeUse.snapshots[key] = snapshot
	}
	close(waiting)
	delete(s.claudeUse.refreshing, key)
	s.claudeUse.mu.Unlock()
	s.persistClaudeUsageCache()
	return snapshot, fetchErr
}

func (s *proxyServer) refreshClaudeUsageProfiles(ctx context.Context, force bool) bool {
	s.claudeUse.mu.Lock()
	coolingDown := time.Now().Before(s.claudeUse.globalRetryAt)
	s.claudeUse.mu.Unlock()
	if coolingDown {
		return false
	}
	profiles := s.configuredClaudeProfiles()
	// Rotate the first account so a throttled account cannot monopolize recovery.
	if len(profiles) > 1 {
		s.claudeUse.mu.Lock()
		start := s.claudeUse.pollCursor % len(profiles)
		s.claudeUse.pollCursor++
		s.claudeUse.mu.Unlock()
		profiles = append(profiles[start:], profiles[:start]...)
	}
	failed := false
	for _, profile := range profiles {
		if err := ctx.Err(); err != nil {
			return false
		}
		// Check profile snapshots before resolving credentials, including restored
		// snapshots whose key is intentionally independent of the OAuth token.
		if !force {
			fresh := false
			s.claudeUse.mu.Lock()
			for _, cached := range s.claudeUse.snapshots {
				age := time.Since(cached.FetchedAt)
				if cached.Profile == profile.Name && cached.Source == "oauth-api" && age >= 0 && age < time.Duration(s.cfg.ClaudeUsage.CacheTTLSeconds)*time.Second {
					fresh = true
					break
				}
			}
			s.claudeUse.mu.Unlock()
			if fresh {
				continue
			}
		}
		s.claudeUse.mu.Lock()
		credentialBlocked := time.Now().Before(s.claudeUse.credentialRetryAt[profile.CredentialsService])
		s.claudeUse.mu.Unlock()
		if credentialBlocked {
			failed = true
			continue
		}
		// Anthropic's OAuth usage endpoint rate-limits bursts across accounts.
		// Shared admission also serializes request-time cache misses.
		refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		token, err := resolveClaudeProfileOAuthToken(refreshCtx, profile)
		if err != nil {
			failed = true
			s.claudeUse.mu.Lock()
			if s.claudeUse.credentialRetryAt == nil {
				s.claudeUse.credentialRetryAt = map[string]time.Time{}
			}
			s.claudeUse.credentialRetryAt[profile.CredentialsService] = time.Now().Add(time.Minute)
			s.claudeUse.mu.Unlock()
			log.Printf("Claude usage background credentials unavailable profile=%q", profile.Name)
			if refreshCtx.Err() == nil {
				s.triggerClaudeOAuthRefresh(profile)
			}
			cancel()
			continue
		}
		auth := "Bearer " + token
		if !force {
			s.claudeUse.mu.Lock()
			cached, ok := s.claudeUse.snapshots[claudeTokenKey(auth)]
			s.claudeUse.mu.Unlock()
			if ok && cached.Source == "oauth-api" && time.Since(cached.FetchedAt) < time.Duration(s.cfg.ClaudeUsage.CacheTTLSeconds)*time.Second {
				cancel()
				continue
			}
		}
		snapshot, err := s.claudeUsageSnapshotRefresh(refreshCtx, auth, profile, true)
		if err != nil || snapshot.Source != "oauth-api" || time.Since(snapshot.FetchedAt) >= time.Duration(s.cfg.ClaudeUsage.CacheTTLSeconds)*time.Second {
			failed = true
		}
		cancel()
	}
	return !failed
}

func (s *proxyServer) monitorClaudeUsage(ctx context.Context) {
	if s.cfg.Definition != nil && s.cfg.Providers[s.cfg.ClaudeUsage.Provider].Usage.Mode != "api" {
		return
	}
	if s.cfg.ClaudeUsage.Provider == "" || len(s.configuredClaudeProfiles()) == 0 {
		return
	}
	interval := time.Minute
	if s.cfg.Definition != nil {
		interval = time.Duration(s.cfg.Providers[s.cfg.ClaudeUsage.Provider].Usage.PollIntervalSeconds) * time.Second
	}
	// Cheap due checks; fresh profiles skip both credential and HTTP work.
	// Startup honors persisted snapshots instead of forcing a quota burst.
	tick := min(30*time.Second, interval)
	for {
		s.refreshClaudeUsageProfiles(ctx, false)
		timer := time.NewTimer(tick)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *proxyServer) claudeSubscriptionAllowed(ctx context.Context, r *http.Request, candidate modelConfig) (bool, string) {
	if !s.claudeSubscriptionCandidate(candidate) {
		return true, ""
	}
	snapshot, err := s.claudeUsageSnapshot(ctx, r)
	if err != nil {
		if s.cfg.Definition != nil {
			return true, "usage_unknown"
		}
		log.Printf("Claude subscription usage unavailable profile=%q error=%v", s.claudeProfileForRequest(r).Name, err)
		return false, "usage_unavailable"
	}
	if s.cfg.Definition != nil {
		if !s.snapshotAllowedForCandidate(snapshot, candidate) {
			return false, "configured_quota_limit"
		}
		if reason := s.claudeWorkerReserveReason(snapshot, candidate); reason != "" {
			return false, reason
		}
		return true, ""
	}
	policy := s.cfg.ClaudeUsage
	if snapshot.FiveHour.Utilization >= policy.FiveHourThresholdPct {
		return false, "five_hour_reserve"
	}
	if snapshot.SevenDay.Utilization >= policy.sevenDayThresholdForProfile(snapshot.Profile) {
		return false, "seven_day_reserve"
	}
	return true, ""
}

func (s *proxyServer) claudePreferredAtConversationOpen(ctx context.Context, r *http.Request, candidates []modelConfig) (bool, claudeUsageSnapshot) {
	policy := s.cfg.ClaudeUsage
	if policy.Provider == "" || policy.PreferBelowUsagePct <= 0 {
		return false, claudeUsageSnapshot{}
	}
	hasClaude := false
	for _, candidate := range candidates {
		if s.claudeSubscriptionCandidate(candidate) {
			hasClaude = true
			break
		}
	}
	if !hasClaude {
		return false, claudeUsageSnapshot{}
	}
	snapshot, err := s.claudeUsageSnapshot(ctx, r)
	if err != nil {
		log.Printf("Claude opener usage unavailable profile=%q error=%v; using open-source pool", s.claudeProfileForRequest(r).Name, err)
		return false, claudeUsageSnapshot{}
	}
	consumed := math.Max(snapshot.FiveHour.Utilization, snapshot.SevenDay.Utilization)
	eligible := consumed < policy.PreferBelowUsagePct &&
		snapshot.FiveHour.Utilization < policy.FiveHourThresholdPct &&
		snapshot.SevenDay.Utilization < policy.sevenDayThresholdForProfile(snapshot.Profile)
	return eligible, snapshot
}

func (s *proxyServer) openingConversationFamily(ctx context.Context, r *http.Request, candidates []modelConfig) string {
	if preferClaude, snapshot := s.claudePreferredAtConversationOpen(ctx, r, candidates); preferClaude {
		log.Printf("conversation opener family=anthropic profile=%q fiveHour=%.1f%% sevenDay=%.1f%%", snapshot.Profile, snapshot.FiveHour.Utilization, snapshot.SevenDay.Utilization)
		return "anthropic"
	}
	performance := s.adaptivePerformance()
	bestFamily := ""
	bestTier := int(^uint(0) >> 1)
	bestScore := math.Inf(-1)
	for _, candidate := range candidates {
		family := candidateModelFamily(candidate)
		if family == "anthropic" {
			continue
		}
		providerName := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
		blocked, _ := s.providerBlocked(providerName)
		reservedOut, _ := s.ollamaCandidateReservedOut(candidate)
		if blocked || reservedOut || s.circuitOpen(providerName, candidate) {
			continue
		}
		tier := s.candidateTier(candidate, providerName)
		score := s.routeScore(providerName, routeKey(providerName, candidate), performance[routeKey(providerName, candidate)])
		if tier < bestTier || (tier == bestTier && score > bestScore) {
			bestFamily, bestTier, bestScore = family, tier, score
		}
	}
	if bestFamily != "" {
		log.Printf("conversation opener family=%s source=open-source tier=%d score=%.1f", bestFamily, bestTier, bestScore)
	}
	return bestFamily
}

func (s *proxyServer) seedConversationFamily(stickyKey, family string) {
	if stickyKey == "" || family == "" {
		return
	}
	now := time.Now()
	s.adaptive.mu.Lock()
	sticky := s.adaptive.sticky[stickyKey]
	if sticky.Family == "" || (!sticky.FamilyUntil.IsZero() && now.After(sticky.FamilyUntil)) {
		sticky.Family = family
		sticky.FamilyUntil = now.Add(s.conversationFamilyTTL())
		s.adaptive.sticky[stickyKey] = sticky
	}
	s.adaptive.mu.Unlock()
}

// forceConversationFamily replaces stale optimizer affinity when the requested
// model has an explicit family contract (currently Opus/Fable -> Anthropic).
func (s *proxyServer) forceConversationFamily(stickyKey, family string) {
	if stickyKey == "" || family == "" {
		return
	}
	now := time.Now()
	s.adaptive.mu.Lock()
	sticky := s.adaptive.sticky[stickyKey]
	if sticky.Family != family {
		sticky.Key = ""
		sticky.Until = time.Time{}
	}
	sticky.Family = family
	sticky.FamilyUntil = now.Add(s.conversationFamilyTTL())
	s.adaptive.sticky[stickyKey] = sticky
	s.adaptive.mu.Unlock()
}

func (s *proxyServer) conversationFamily(stickyKey string) string {
	if stickyKey == "" {
		return ""
	}
	s.adaptive.mu.Lock()
	defer s.adaptive.mu.Unlock()
	sticky := s.adaptive.sticky[stickyKey]
	if !sticky.FamilyUntil.IsZero() && time.Now().After(sticky.FamilyUntil) {
		return ""
	}
	return sticky.Family
}

func (s *proxyServer) claudeUsageStatus() map[string]any {
	if s.cfg.Definition != nil {
		return s.configuredClaudeUsageStatus()
	}
	policy := s.cfg.ClaudeUsage
	profiles := map[string]any{}
	for _, profile := range s.configuredClaudeProfiles() {
		profiles[profile.Name] = map[string]any{"available": false, "eligible": false, "routingEligible": false, "source": "pending", "error": "waiting for background quota refresh"}
	}
	s.claudeUse.mu.Lock()
	cachedSnapshots := newestClaudeUsageSnapshots(s.claudeUse.snapshots, s.accountNow(), time.Duration(policy.StaleTTLSeconds)*time.Second)
	s.claudeUse.mu.Unlock()
	for name, snapshot := range cachedSnapshots {
		entry := claudeUsageStatusEntry(snapshot, policy)
		entry["available"] = true
		entry["stale"] = time.Since(snapshot.FetchedAt) >= time.Duration(policy.CacheTTLSeconds)*time.Second
		blocked, state := s.providerBlocked(policy.Provider + "@" + name)
		entry["quotaEligible"] = entry["eligible"]
		entry["routingEligible"] = entry["eligible"] == true && !blocked
		entry["blockReason"], entry["blockedUntil"] = state.Reason, state.BlockedUntil
		fableBlocked, fableState := s.providerBlocked(policy.Provider + "@" + name + "#fable")
		entry["fableEligible"] = entry["routingEligible"] == true && !fableBlocked
		entry["fableBlockReason"], entry["fableBlockedUntil"] = fableState.Reason, fableState.BlockedUntil
		profiles[name] = entry
	}
	return map[string]any{
		"enabled": policy.Provider != "", "provider": policy.Provider,
		"autoSelectAccounts": policy.AutoSelectAccounts, "accountStickySeconds": policy.AccountStickySeconds,
		"preferBelowUsagePct":  policy.PreferBelowUsagePct,
		"fiveHourThresholdPct": policy.FiveHourThresholdPct, "sevenDayThresholdPct": policy.SevenDayThresholdPct,
		"eligibleUpstreams": policy.EligibleUpstreams, "profiles": profiles,
		"credentialRefresh": s.claudeCredentialRefreshHealth(),
	}
}

func newestClaudeUsageSnapshots(snapshots map[string]claudeUsageSnapshot, now time.Time, staleTTL time.Duration) map[string]claudeUsageSnapshot {
	if staleTTL <= 0 {
		staleTTL = 30 * time.Minute
	}
	newest := map[string]claudeUsageSnapshot{}
	for key, snapshot := range snapshots {
		if snapshot.FetchedAt.IsZero() || snapshot.FetchedAt.After(now) || now.Sub(snapshot.FetchedAt) > staleTTL {
			continue
		}
		name := snapshot.Profile
		if name == "" {
			prefix := key
			if len(prefix) > 8 {
				prefix = prefix[:8]
			}
			name = "oauth-" + prefix
		}
		if current, ok := newest[name]; !ok || snapshot.FetchedAt.After(current.FetchedAt) {
			newest[name] = snapshot
		}
	}
	return newest
}

func claudeUsageStatusEntry(snapshot claudeUsageSnapshot, policy claudeUsageConfig) map[string]any {
	sevenDayThreshold := policy.sevenDayThresholdForProfile(snapshot.Profile)
	return map[string]any{
		"source": snapshot.Source, "fetchedAt": snapshot.FetchedAt.UTC().Format(time.RFC3339),
		"fiveHourUsagePct": snapshot.FiveHour.Utilization, "fiveHourResetsAt": snapshot.FiveHour.ResetsAt,
		"sevenDayUsagePct": snapshot.SevenDay.Utilization, "sevenDayResetsAt": snapshot.SevenDay.ResetsAt,
		"sevenDayThresholdPct": sevenDayThreshold,
		"eligible":             snapshot.FiveHour.Utilization < policy.FiveHourThresholdPct && snapshot.SevenDay.Utilization < sevenDayThreshold,
	}
}

func (s *proxyServer) claudeUsageStatusSnapshot(profile claudeUsageProfile) (claudeUsageSnapshot, error) {
	return s.claudeUsageStatusSnapshotContextInternal(context.Background(), profile, false)
}

// claudeUsageStatusSnapshotContext serves the scheduler's latest per-profile
// snapshot before touching Keychain or the usage endpoint. The monitor refreshes
// every minute, so normal request routing stays local and cannot inherit a
// usage-API hiccup's latency.
func (s *proxyServer) claudeUsageStatusSnapshotContext(ctx context.Context, profile claudeUsageProfile) (claudeUsageSnapshot, error) {
	if s.cfg.Definition != nil && s.cfg.Providers[s.cfg.ClaudeUsage.Provider].Usage.Mode != "api" {
		return claudeUsageSnapshot{}, errors.New("usage unknown: no API reader enabled")
	}
	return s.claudeUsageStatusSnapshotContextInternal(ctx, profile, true)
}

func (s *proxyServer) claudeUsageStatusSnapshotContextInternal(ctx context.Context, profile claudeUsageProfile, useProfileCache bool) (claudeUsageSnapshot, error) {
	if s.cfg.Definition != nil && s.cfg.Providers[s.cfg.ClaudeUsage.Provider].Usage.Mode != "api" {
		return claudeUsageSnapshot{}, errors.New("usage unknown: no API reader enabled")
	}
	policy := s.cfg.ClaudeUsage
	cacheTTL := time.Duration(policy.CacheTTLSeconds) * time.Second
	staleTTL := time.Duration(policy.StaleTTLSeconds) * time.Second
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}
	if staleTTL < cacheTTL {
		staleTTL = 30 * time.Minute
	}
	if useProfileCache {
		s.claudeUse.mu.Lock()
		var cached claudeUsageSnapshot
		var cachedAge time.Duration
		for _, snapshot := range s.claudeUse.snapshots {
			if snapshot.Profile != profile.Name || snapshot.FetchedAt.IsZero() {
				continue
			}
			age := time.Since(snapshot.FetchedAt)
			if age < 0 || age >= staleTTL || (!cached.FetchedAt.IsZero() && !snapshot.FetchedAt.After(cached.FetchedAt)) {
				continue
			}
			cached = snapshot
			cachedAge = age
		}
		s.claudeUse.mu.Unlock()
		if !cached.FetchedAt.IsZero() {
			if cachedAge >= cacheTTL && cached.Source == "oauth-api" {
				cached.Source = "oauth-api-stale"
			}
			return cached, nil
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requestTimeout := time.Duration(policy.RequestTimeoutMS) * time.Millisecond
	if requestTimeout <= 0 {
		requestTimeout = 5 * time.Second
	}
	refreshCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	token, err := resolveClaudeProfileOAuthToken(refreshCtx, profile)
	if err != nil {
		if snapshot, cliErr := readClaudeCLIUsage(profile, staleTTL); cliErr == nil {
			return snapshot, nil
		}
		return claudeUsageSnapshot{}, err
	}
	return s.claudeUsageSnapshotWithAuth(refreshCtx, "Bearer "+token, profile)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// requestNeedsStructuredOutputCompatibility identifies the exact turns where
// Claude Code expects its deferred StructuredOutput tool. Legacy GLM routes
// repeatedly called ToolSearch instead of invoking the returned tool reference; DeepSeek,
// native Anthropic Sonnet, and Kimi K3 complete the same harness contract. Inspect only
// the active turn (plus
// explicit top-level tool/schema declarations) so an old mention in history
// does not pin the rest of a conversation to DeepSeek.
func requestNeedsStructuredOutputCompatibility(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	if output, ok := payload["output_config"].(map[string]any); ok {
		if output["format"] != nil || output["schema"] != nil {
			return true
		}
	}
	if _, ok := payload["output_schema"]; ok {
		return true
	}
	if tools, ok := payload["tools"].([]any); ok {
		for _, item := range tools {
			tool, ok := item.(map[string]any)
			if !ok {
				continue
			}
			name, _ := tool["name"].(string)
			if strings.EqualFold(name, "StructuredOutput") {
				return true
			}
		}
	}
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	for index := len(messages) - 1; index >= 0; index-- {
		message, ok := messages[index].(map[string]any)
		if !ok || message["role"] != "user" {
			continue
		}
		return containsStructuredOutputMarker(message["content"])
	}
	return false
}

func containsStructuredOutputMarker(value any) bool {
	switch typed := value.(type) {
	case string:
		lower := strings.ToLower(typed)
		return strings.Contains(lower, "structuredoutput") || strings.Contains(lower, "structured-output-enforce")
	case []any:
		for _, item := range typed {
			if containsStructuredOutputMarker(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range typed {
			if containsStructuredOutputMarker(item) {
				return true
			}
		}
	}
	return false
}

func structuredOutputCompatibleCandidates(candidates []modelConfig) ([]modelConfig, bool) {
	compatible := make([]modelConfig, 0, len(candidates))
	for _, candidate := range candidates {
		upstream := strings.ToLower(candidate.Upstream)
		if strings.Contains(upstream, "deepseek") ||
			strings.Contains(upstream, "glm-5.3") ||
			strings.Contains(upstream, "kimi-k3") ||
			strings.Contains(upstream, "grok") ||
			candidate.Provider == "anthropic" {
			compatible = append(compatible, candidate)
		}
	}
	if len(compatible) == 0 || len(compatible) == len(candidates) {
		return candidates, false
	}
	return compatible, true
}

func requestContainsImage(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	return containsImageContent(payload["messages"])
}

func containsImageContent(value any) bool {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if containsImageContent(item) {
				return true
			}
		}
	case map[string]any:
		contentType, _ := typed["type"].(string)
		switch strings.ToLower(strings.TrimSpace(contentType)) {
		case "image", "image_url", "input_image":
			return true
		}
		for _, item := range typed {
			if containsImageContent(item) {
				return true
			}
		}
	}
	return false
}

func imageCompatibleCandidates(candidates []modelConfig) ([]modelConfig, bool) {
	compatible := make([]modelConfig, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.SupportsImages != nil && !*candidate.SupportsImages {
			continue
		}
		compatible = append(compatible, candidate)
	}
	if len(compatible) == len(candidates) {
		return candidates, false
	}
	return compatible, true
}

func (s *proxyServer) routes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	type route struct {
		Provider      string  `json:"provider"`
		Upstream      string  `json:"upstream"`
		ResponseAlias string  `json:"responseAlias"`
		ContextWindow int     `json:"contextWindow,omitempty"`
		Tier          *int    `json:"tier,omitempty"`
		Fallbacks     []route `json:"fallbacks,omitempty"`
	}
	var describe func(modelConfig) route
	describe = func(model modelConfig) route {
		fallbacks := make([]route, 0, len(model.Fallbacks))
		for _, fallback := range model.Fallbacks {
			fallbacks = append(fallbacks, describe(fallback))
		}
		return route{
			Provider:      firstNonEmpty(model.Provider, s.cfg.DefaultProvider),
			Upstream:      model.Upstream,
			ResponseAlias: model.ResponseAlias,
			ContextWindow: model.ContextWindow,
			Tier:          model.Tier,
			Fallbacks:     fallbacks,
		}
	}
	routes := map[string]route{}
	for alias, model := range s.cfg.Models {
		described := describe(model)
		described.ResponseAlias = firstNonEmpty(described.ResponseAlias, alias)
		routes[alias] = described
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"defaultProvider": s.cfg.DefaultProvider,
		"models":          routes,
	})
}

func (s *proxyServer) proxy(w http.ResponseWriter, r *http.Request) {
	r, requestID := ensureProxyRequestID(r)
	requestStarted := time.Now()
	w.Header().Set("X-Proxy-Request-ID", requestID)
	body, payload, selectedModel, err := s.normalizeRequest(r)
	if r.Context().Err() != nil {
		s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, 499, "client_cancel", requestStarted)
		return
	}
	if err != nil {
		if errors.Is(err, errUnknownModel) {
			s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, http.StatusBadRequest, "client_invalid", requestStarted)
			writeAPIError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
			return
		}
		s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, http.StatusInternalServerError, "normalize_error", requestStarted)
		writeAPIError(w, http.StatusInternalServerError, "api_error", err.Error())
		return
	}
	selectedModel = s.applyOllamaUsageRoute(r.Context(), selectedModel)
	countTokens := strings.HasSuffix(r.URL.Path, "/count_tokens")
	var releaseClaudeAccount func()
	r, releaseClaudeAccount = s.bindClaudeAccount(r.Context(), r, selectedModel, payload, !countTokens)
	if releaseClaudeAccount != nil {
		defer releaseClaudeAccount()
	}

	if countTokens {
		s.handleCountTokens(w, r, body, payload, selectedModel, requestStarted)
		return
	}

	ctx := r.Context()
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
	}

	upstream, err := s.doWithFallbacks(ctx, r, body, payload, selectedModel)
	if err != nil {
		if r.Context().Err() != nil {
			s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, 499, "client_cancel", requestStarted)
			return
		}
		var compatibilityErr *requestCompatibilityError
		if errors.As(err, &compatibilityErr) {
			s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, http.StatusBadRequest, "route_incompatible", requestStarted)
			writeAPIError(w, http.StatusBadRequest, "invalid_request_error", compatibilityErr.Error())
			return
		}
		var networkErr *retryableNetworkError
		if errors.As(err, &networkErr) {
			s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, http.StatusServiceUnavailable, "local_network", requestStarted)
			w.Header().Set("Retry-After", "2")
			writeAPIError(w, http.StatusServiceUnavailable, "api_error", networkErr.Error())
			return
		}
		s.recordRequestTerminal(requestID, r.URL.Path, selectedModel, http.StatusBadGateway, "fallback_exhausted", requestStarted)
		writeAPIError(w, http.StatusBadGateway, "api_error", err.Error())
		return
	}
	resp := upstream.resp
	providerName := upstream.providerName
	selectedModel = upstream.model
	// Compute account-scoped health from the actual winning candidate. Using
	// the initially selected model here cleared the failed account's Fable
	// block after a different Claude account succeeded.
	runtimeProviderName := s.providerRuntimeKeyForCandidate(r, providerName, selectedModel)

	copyHeaders(w.Header(), resp.Header)
	// Body is re-encoded by the filters below, so upstream framing no longer applies.
	w.Header().Del("Content-Length")
	w.Header().Del("Content-Encoding")
	w.WriteHeader(resp.StatusCode)

	stats := &streamStats{}
	var responseBody io.ReadCloser
	providerCfg := s.cfg.Providers[providerName]
	openAIChat := providerCfg.Format == "openai-chat" && resp.StatusCode == http.StatusOK
	dropTypes := responseDropSet(providerCfg)
	contentType := resp.Header.Get("Content-Type")
	responseAlias := firstNonEmpty(selectedModel.ResponseAlias, selectedModel.Requested)
	switch {
	case strings.Contains(contentType, "text/event-stream"):
		var source io.ReadCloser = resp.Body
		if idleTimeout := streamIdleTimeout(providerCfg); idleTimeout > 0 {
			source = &idleTimeoutReadCloser{source: resp.Body, timeout: idleTimeout}
		}
		if openAIChat {
			source = newOpenAIStreamReadCloserWithStats(source, responseAlias, stats)
		}
		inputTokenHint := 0
		if providerCfg.Variant == "vercel-gateway" || s.cfg.Definition == nil && strings.HasPrefix(providerName, "vercel") {
			inputTokenHint = estimateInputTokens(body)
		}
		responseBody = newSSEFilterReadCloser(source, dropTypes, inputTokenHint, responseAlias, s.streamEventStallTimeout(), stats)
	case strings.Contains(contentType, "application/json"):
		var source io.ReadCloser = resp.Body
		if openAIChat {
			source = newOpenAIJSONReadCloserWithStats(source, responseAlias, stats)
		}
		responseBody = newJSONFilterReadCloser(source, dropTypes, responseAlias)
	default:
		responseBody = resp.Body
	}
	defer responseBody.Close()

	streamStarted := time.Now()
	streamErr := streamCopy(w, responseBody, stats)
	clientDisconnected := r.Context().Err() != nil
	if clientDisconnected {
		stats.ClientDisconnected.Store(true)
	}
	providerSignal := ""
	switch {
	case clientDisconnected:
		// Closing the downstream also closes the upstream reader. Its synthetic
		// missing-stop marker is cancellation fallout, not provider corruption.
	case stats.ProtocolError.Load() || stats.MissingStop.Load():
		providerSignal = "provider_protocol_error"
	case stats.StreamStalled.Load():
		providerSignal = "stream_event_stall"
	case streamErr != nil && !clientDisconnected:
		providerSignal = "stream_error"
	}
	networkDecision := networkFailureDecision{}
	if providerSignal == "stream_error" {
		networkDecision = s.observeNetworkTransportFailure(runtimeProviderName, providerSignal, streamErr)
	}
	providerScopedSignal := providerSignal != "malformed_anthropic_sse" && providerSignal != "stream_event_stall"
	if providerSignal != "" && providerScopedSignal && !upstream.unstable && !networkDecision.SuppressProviderPenalty {
		s.observeProviderInstability(runtimeProviderName, providerSignal)
	}
	if !clientDisconnected && (streamErr != nil || stats.MissingStop.Load() || stats.ProtocolError.Load()) && !networkDecision.SuppressProviderPenalty {
		// The upstream leg failed mid-stream (dropped connection, idle
		// timeout, or a truncated/degenerate ending): mark it broken so the
		// next request goes straight to a fallback instead of paying this
		// leg's latency again. Client disconnects are not provider faults.
		s.tripCircuit(runtimeProviderName, selectedModel)
	} else if !clientDisconnected && streamErr == nil && !stats.MissingStop.Load() && !stats.ProtocolError.Load() && !upstream.unstable &&
		resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		// A fully delivered response is the only proof a leg is healthy —
		// closing on 2xx headers alone would let a stream-flapping leg dodge
		// the breaker forever. This also clears the escalation backoff.
		s.resetCircuitAfter(runtimeProviderName, selectedModel, upstream.started)
		s.clearProviderBlockAfter(runtimeProviderName, upstream.started)
		if modelKey := s.claudeModelQuotaBlockKey(runtimeProviderName, selectedModel); modelKey != "" {
			s.clearProviderBlockAfter(modelKey, upstream.started)
		}
		s.observeProviderHealthyAfter(runtimeProviderName, upstream.started)
	}
	s.recordCompletion(upstream.trace, selectedModel, upstream.attempt, upstream.started, resp.StatusCode, time.Since(streamStarted), streamErr, stats, upstream.failureReason)
	s.logCompletion(r, selectedModel, providerName, upstream.attempt, resp.StatusCode, upstream.started, streamStarted, streamErr, stats)
}

// countTokensUnsupportedTTL is how long a provider+upstream pair is remembered
// as not implementing count_tokens (404/405/501), avoiding a paid round-trip
// on every context-tracking call.
const countTokensUnsupportedTTL = time.Hour

var countTokensUnsupported sync.Map // key -> time.Time of the negative verdict

// handleCountTokens forwards token-count requests upstream (with the same model
// remapping and fallback chain as messages) and falls back to a local byte-size
// estimate when no upstream answers successfully.
func (s *proxyServer) handleCountTokens(w http.ResponseWriter, r *http.Request, body []byte, payload map[string]any, selected modelConfig, requestStarted time.Time) {
	if selected.Requested != "" {
		cacheKey := firstNonEmpty(selected.Provider, s.cfg.DefaultProvider) + "|" + selected.Upstream
		unsupported := false
		if raw, ok := countTokensUnsupported.Load(cacheKey); ok {
			if when, ok := raw.(time.Time); ok && time.Since(when) < countTokensUnsupportedTTL {
				unsupported = true
			} else {
				countTokensUnsupported.Delete(cacheKey)
			}
		}
		if !unsupported {
			if s.tryUpstreamCountTokens(w, r, body, payload, selected, cacheKey) {
				return
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{
		"input_tokens": estimateInputTokens(body),
	})
	s.recordRequestTerminalOutcome(proxyRequestID(r), r.URL.Path, selected, http.StatusOK, "", requestStarted, true)
}

// tryUpstreamCountTokens reports whether the response was handled upstream.
func (s *proxyServer) tryUpstreamCountTokens(w http.ResponseWriter, r *http.Request, body []byte, payload map[string]any, selected modelConfig, cacheKey string) bool {
	providerName := firstNonEmpty(selected.Provider, s.cfg.DefaultProvider)
	if s.cfg.Providers[providerName].Format == "openai-chat" {
		return false
	}
	if blocked, _ := s.providerBlockForCandidate(s.providerRuntimeKey(r, providerName), selected); blocked {
		return false
	}
	countCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	countRequest := r.WithContext(countCtx)
	if s.cfg.Providers[providerName].Variant == "opencode-go" {
		countRequest = withOpenCodeSession(countRequest, payload)
	}
	// Counting is a capability probe, never an inference fallback cascade.
	started := time.Now()
	resp, err := s.doUpstreamWithHeaderBudgetLimit(countCtx, countRequest, body, selected, 5*time.Second)
	if err != nil {
		log.Printf("count_tokens upstream failed model=%q error=%v; using local estimate", selected.Requested, err)
		return false
	}
	upstream := upstreamResponse{resp: resp, providerName: providerName, model: selected, started: started,
		trace: requestTrace{ID: proxyRequestID(r), Path: r.URL.Path, ChainStarted: started, CandidateCount: 1}}
	defer upstream.resp.Body.Close()
	status := upstream.resp.StatusCode
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented {
		countTokensUnsupported.Store(cacheKey, time.Now())
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(upstream.resp.Body, 1<<20))
		log.Printf("count_tokens upstream rejected model=%q provider=%s status=%d; using local estimate", selected.Requested, upstream.providerName, status)
		return false
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(upstream.resp.Body, 65537))
	var result struct {
		InputTokens *int64 `json:"input_tokens"`
	}
	if readErr != nil || len(responseBody) > 65536 || json.Unmarshal(responseBody, &result) != nil || result.InputTokens == nil || *result.InputTokens < 0 {
		return false
	}
	copyHeaders(w.Header(), upstream.resp.Header)
	w.Header().Del("Content-Length")
	w.Header().Del("Content-Encoding")
	w.WriteHeader(status)
	stats := &streamStats{}
	stats.InputTokens.Store(*result.InputTokens)
	_, streamErr := w.Write(responseBody)
	s.recordCompletionKind(upstream.trace, upstream.model, upstream.attempt, upstream.started, status, 0, streamErr, stats, "count_tokens", upstream.failureReason)
	return true
}

func (s *proxyServer) logCompletion(r *http.Request, model modelConfig, providerName string, attempt, status int, started, streamStarted time.Time, streamErr error, stats *streamStats) {
	errKind := ""
	errMsg := ""
	switch {
	case streamErr == nil:
	case r.Context().Err() != nil:
		errKind = "client_disconnected"
		errMsg = streamErr.Error()
	default:
		errKind = "upstream_stream"
		errMsg = streamErr.Error()
	}
	if errKind == "" && stats != nil && stats.MissingStop.Load() {
		errKind = "missing_stop_reason"
		errMsg = "message_stop received without a preceding stop_reason; converted to error event"
	}
	if !s.cfg.Logging.DebugRequests && attempt == 0 && status < http.StatusBadRequest && errKind == "" {
		return
	}
	log.Printf(
		"request complete request=%s method=%s path=%s model=%q provider=%s profile=%q upstream=%q alias=%q attempt=%d status=%d headerMs=%d streamMs=%d bytes=%d events=%d err=%q kind=%s",
		proxyRequestID(r),
		r.Method,
		r.URL.Path,
		model.Requested,
		providerName,
		model.ClaudeProfile,
		model.Upstream,
		firstNonEmpty(model.ResponseAlias, model.Requested),
		attempt,
		status,
		streamStarted.Sub(started).Milliseconds(),
		time.Since(streamStarted).Milliseconds(),
		stats.Bytes.Load(),
		stats.Events.Load(),
		errMsg,
		errKind,
	)
}

// writeAPIError returns an Anthropic-shaped error body so Claude Code surfaces
// and classifies proxy-side failures correctly instead of a bare HTTP error.
func writeAPIError(w http.ResponseWriter, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": errType, "message": message},
	})
}

// collectCandidates flattens the selected model and its fallback chain
// (including nested fallbacks on referenced model entries) into an ordered
// attempt list, deduplicated by provider+upstream and depth-capped so a
// fallback cycle cannot loop forever.
func (s *proxyServer) collectCandidates(selected modelConfig) []modelConfig {
	candidates := []modelConfig{selected}
	keyFor := func(m modelConfig) string {
		key := firstNonEmpty(m.Provider, s.cfg.DefaultProvider) + "|" + m.Upstream
		if s.cfg.Definition != nil {
			key += "|" + m.AccountPool + "|" + m.ModelID
		}
		return key
	}
	seen := map[string]bool{
		keyFor(selected): true,
	}
	var walk func(m modelConfig, depth int)
	walk = func(m modelConfig, depth int) {
		if depth >= 4 {
			return
		}
		for _, fallback := range m.Fallbacks {
			key := keyFor(fallback)
			if seen[key] {
				continue
			}
			seen[key] = true
			candidates = append(candidates, fallback)
			walk(fallback, depth+1)
		}
	}
	walk(selected, 0)
	return candidates
}

// deferInstabilityQuarantinedCandidates keeps slow/brownout legs behind every
// normal or hard-blocked candidate. doWithFallbacks may then fail open through
// the final instability-only leg after every real alternative is exhausted.
// Quota and rate-limit blocks remain hard. An instability circuit may fail
// open only when it is the final remaining leg.
func (s *proxyServer) deferInstabilityQuarantinedCandidates(r *http.Request, candidates []modelConfig) []modelConfig {
	if len(candidates) < 2 {
		return candidates
	}
	regular := make([]modelConfig, 0, len(candidates))
	instabilityQuarantined := make([]modelConfig, 0, len(candidates))
	for _, candidate := range candidates {
		providerName := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
		runtimeProviderName := s.providerRuntimeKey(r, providerName)
		blocked, state := s.providerBlocked(runtimeProviderName)
		if blocked && strings.HasPrefix(state.Reason, "instability_") {
			instabilityQuarantined = append(instabilityQuarantined, candidate)
			continue
		}
		regular = append(regular, candidate)
	}
	return append(regular, instabilityQuarantined...)
}

// circuitBreakers remembers failing provider+upstream pairs so requests skip
// a known-broken leg instead of paying its timeout/error on every call. A leg
// opens only after circuitFailureThreshold consecutive failures inside
// circuitFailureWindow (one transient blip no longer buys a ban), cools down
// with escalating backoff (30s doubling to 5min) while failures keep coming,
// and closes on any fully-successful response. When every leg is blocked or
// circuit-open, doWithFallbacks returns immediately. It must not bypass those
// states and recreate the same timeout chain on every client retry.
var circuitBreakers sync.Map // "provider|upstream" -> *circuitState

type circuitState struct {
	mu        sync.Mutex
	failures  int // consecutive failures within circuitFailureWindow
	opens     int // times opened since last success, drives backoff
	lastFail  time.Time
	openUntil time.Time
	halfOpen  bool
	lastProbe time.Time
}

type circuitProbe struct {
	state      *circuitState
	acquiredAt time.Time
}

const (
	circuitFailureThreshold = 2
	circuitFailureWindow    = 10 * time.Minute
	circuitBaseCooldown     = 30 * time.Second
	circuitMaxCooldown      = 5 * time.Minute
)

func (s *proxyServer) circuitKey(providerName string, m modelConfig) string {
	return firstNonEmpty(providerName, s.cfg.DefaultProvider) + "|" + m.Upstream
}

func (s *proxyServer) circuitEnabled(providerName string) bool {
	cfg, ok := s.cfg.Providers[providerName]
	if !ok {
		// Account-scoped runtime keys retain the base provider's policy.
		if base, _, scoped := strings.Cut(providerName, "@"); scoped {
			cfg, ok = s.cfg.Providers[base]
		}
	}
	if !ok || cfg.CircuitBreaker == nil {
		return true
	}
	return *cfg.CircuitBreaker
}

func (s *proxyServer) circuitOpen(providerName string, m modelConfig) bool {
	if !s.circuitEnabled(providerName) {
		return false
	}
	raw, ok := circuitBreakers.Load(s.circuitKey(providerName, m))
	if !ok {
		return false
	}
	state := raw.(*circuitState)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.halfOpen || time.Now().Before(state.openUntil)
}

// acquireCircuitAttempt grants one half-open probe after cooldown. The final
// remaining route may probe early, but never more than once per recovery
// interval and never concurrently.
func (s *proxyServer) acquireCircuitAttempt(providerName string, m modelConfig, lastRemaining bool) (bool, *circuitProbe) {
	if !s.circuitEnabled(providerName) {
		return true, nil
	}
	raw, ok := circuitBreakers.Load(s.circuitKey(providerName, m))
	if !ok {
		return true, nil
	}
	state := raw.(*circuitState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.halfOpen {
		return false, nil
	}
	now := time.Now()
	if now.Before(state.openUntil) {
		if !lastRemaining || !state.lastProbe.IsZero() && now.Sub(state.lastProbe) < networkRecoveryDelay {
			return false, nil
		}
	}
	if state.openUntil.IsZero() {
		return true, nil
	}
	state.halfOpen = true
	state.lastProbe = now
	return true, &circuitProbe{state: state, acquiredAt: now}
}

func releaseCircuitProbe(probe *circuitProbe) {
	if probe == nil {
		return
	}
	state := probe.state
	state.mu.Lock()
	if state.lastProbe.Equal(probe.acquiredAt) {
		state.halfOpen = false
	}
	state.mu.Unlock()
}

func (s *proxyServer) tripCircuit(providerName string, m modelConfig) {
	if !s.circuitEnabled(providerName) {
		return
	}
	raw, _ := circuitBreakers.LoadOrStore(s.circuitKey(providerName, m), &circuitState{})
	state := raw.(*circuitState)
	state.mu.Lock()
	defer state.mu.Unlock()
	now := time.Now()
	if now.Sub(state.lastFail) > circuitFailureWindow {
		state.failures = 0
		state.opens = 0
	}
	state.failures++
	state.lastFail = now
	state.halfOpen = false
	if state.failures < circuitFailureThreshold {
		return
	}
	cooldown := circuitMaxCooldown
	if state.opens < 4 {
		cooldown = circuitBaseCooldown << state.opens
		if cooldown > circuitMaxCooldown {
			cooldown = circuitMaxCooldown
		}
	}
	state.opens++
	state.openUntil = now.Add(cooldown)
	log.Printf("circuit opened provider=%s upstream=%q failures=%d cooldown=%s", providerName, m.Upstream, state.failures, cooldown)
}

func (s *proxyServer) resetCircuit(providerName string, m modelConfig) {
	s.resetCircuitAfter(providerName, m, time.Time{})
}

func (s *proxyServer) resetCircuitAfter(providerName string, m modelConfig, started time.Time) {
	raw, ok := circuitBreakers.Load(s.circuitKey(providerName, m))
	if !ok {
		return
	}
	state := raw.(*circuitState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if !started.IsZero() && (state.lastFail.After(started) || state.halfOpen && state.lastProbe.After(started)) {
		return
	}
	// Keep the same object: deleting it can orphan a concurrent failure update.
	state.failures, state.opens = 0, 0
	state.openUntil, state.lastFail = time.Time{}, time.Time{}
	state.halfOpen = false
}

func withSelectedClaudeProfile(r *http.Request, profile claudeUsageProfile) *http.Request {
	if profile.Name == "" {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), selectedClaudeProfileContextKey{}, profile))
}

func (s *proxyServer) isClaudePaidFallbackProvider(providerName string) bool {
	if s.cfg.Definition != nil {
		return s.cfg.Providers[providerName].Billing == "metered"
	}
	// Metered gateway legs inherit the guard independently of their model.
	if strings.HasPrefix(providerName, "vercel-") || strings.HasPrefix(providerName, "merge-gateway") {
		return true
	}
	providers := s.cfg.ClaudeUsage.PaidFallbackProviders
	if len(providers) == 0 {
		providers = []string{"vercel-kimi"}
	}
	for _, configured := range providers {
		if providerName == configured {
			return true
		}
	}
	return false
}

func (s *proxyServer) observeClaudeServiceFailure() bool {
	now := s.accountNow()
	windowSeconds := s.cfg.ClaudeUsage.PaidFallbackWindowSeconds
	if windowSeconds <= 0 {
		windowSeconds = 120
	}
	minimum := s.cfg.ClaudeUsage.PaidFallbackMinOutageFailures
	if minimum <= 0 {
		minimum = 3
	}
	activeSeconds := s.cfg.ClaudeUsage.PaidFallbackActiveSeconds
	if activeSeconds <= 0 {
		activeSeconds = 300
	}

	s.claudePaidFallbackMu.Lock()
	defer s.claudePaidFallbackMu.Unlock()
	if now.Before(s.claudePaidFallback.ActiveUntil) {
		return true
	}
	cutoff := now.Add(-time.Duration(windowSeconds) * time.Second)
	kept := s.claudePaidFallback.Signals[:0]
	for _, signal := range s.claudePaidFallback.Signals {
		if !signal.Before(cutoff) {
			kept = append(kept, signal)
		}
	}
	s.claudePaidFallback.Signals = append(kept, now)
	if len(s.claudePaidFallback.Signals) < minimum {
		return false
	}
	s.claudePaidFallback.Signals = nil
	s.claudePaidFallback.ActiveUntil = now.Add(time.Duration(activeSeconds) * time.Second)
	log.Printf("Claude paid fallback armed after confirmed provider outage failures=%d active=%s", minimum, time.Duration(activeSeconds)*time.Second)
	return true
}

func (s *proxyServer) clearClaudeServiceFailures() {
	s.claudePaidFallbackMu.Lock()
	s.claudePaidFallback.Signals = nil
	s.claudePaidFallback.ActiveUntil = time.Time{}
	s.claudePaidFallbackMu.Unlock()
}

func shouldShortCircuitClaudeTransport(failures int, outageConfirmed bool) bool {
	return outageConfirmed || failures >= 2
}

func (s *proxyServer) claudePoolHasAuthBlock(r *http.Request) bool {
	for _, profile := range s.eligibleClaudePoolProfiles(requestListenerPort(r)) {
		blocked, state := s.providerBlocked(s.cfg.ClaudeUsage.Provider + "@" + profile.Name)
		if blocked && strings.HasPrefix(state.Reason, "auth_") {
			return true
		}
	}
	return false
}

func (s *proxyServer) expandClaudeAccountAttempts(r *http.Request, candidates []modelConfig, selected modelConfig) []claudeCandidateAttempt {
	if s.cfg.Definition != nil {
		return s.expandConfiguredClaudeAttempts(r, candidates)
	}
	if !s.cfg.ClaudeUsage.AutoSelectAccounts || !s.explicitAnthropicPrimary(selected) {
		attempts := make([]claudeCandidateAttempt, 0, len(candidates))
		for _, candidate := range candidates {
			attempts = append(attempts, claudeCandidateAttempt{model: candidate})
		}
		return attempts
	}

	profiles := s.eligibleClaudePoolProfiles(requestListenerPort(r))
	if selectedProfile, ok := selectedClaudeProfile(r); ok && profileInClaudePool(selectedProfile, profiles) {
		profiles = s.orderClaudeFallbackProfiles(profiles, selectedProfile)
	}
	fromProfile := ""
	if len(profiles) > 0 {
		fromProfile = profiles[0].Name
	}
	attempts := make([]claudeCandidateAttempt, 0, len(candidates)+len(profiles))
	for _, candidate := range candidates {
		if s.claudeSubscriptionCandidate(candidate) && len(profiles) > 0 {
			for _, profile := range profiles {
				copy := candidate
				copy.ClaudeProfile = profile.Name
				attempts = append(attempts, claudeCandidateAttempt{model: copy, profile: profile, fromProfile: fromProfile})
			}
			continue
		}
		attempts = append(attempts, claudeCandidateAttempt{model: candidate})
	}
	return attempts
}

func (s *proxyServer) doWithFallbacks(ctx context.Context, r *http.Request, body []byte, payload map[string]any, selected modelConfig) (upstreamResponse, error) {
	r, requestID := ensureProxyRequestID(r)
	networkProbe, err := s.beginNetworkRequest()
	if err != nil {
		return upstreamResponse{}, err
	}
	networkRecovered := false
	defer func() { s.finishNetworkProbe(networkProbe, networkRecovered) }()
	// count_tokens is a best-effort side channel: it must never trip circuits
	// shared with /v1/messages traffic (a 404 body saying "not found" would
	// otherwise ban the leg for real generation requests).
	countTokens := strings.HasSuffix(r.URL.Path, "/count_tokens")
	candidates := s.collectCandidates(selected)
	if s.cfg.Definition != nil && !countTokens {
		candidates = configuredCompatibleCandidates(candidates, payload)
		if len(candidates) == 0 {
			return upstreamResponse{}, &requestCompatibilityError{message: "no configured route supports this request's tools/images"}
		}
	}
	if !countTokens && requestContainsImage(payload) {
		if compatible, changed := imageCompatibleCandidates(candidates); changed {
			candidates = compatible
			if len(candidates) == 0 {
				return upstreamResponse{}, &requestCompatibilityError{message: fmt.Sprintf("model %q has no image-capable route", selected.Requested)}
			}
			if s.cfg.Logging.DebugRequests {
				log.Printf("image compatibility route model=%q candidates=%d; skipping non-vision models", selected.Requested, len(candidates))
			}
		}
	}
	if s.cfg.Definition == nil && !countTokens && requestNeedsStructuredOutputCompatibility(payload) {
		if compatible, changed := structuredOutputCompatibleCandidates(candidates); changed {
			candidates = compatible
			if s.cfg.Logging.DebugRequests {
				log.Printf("structured-output compatibility route model=%q candidates=%d; using compatible models only", selected.Requested, len(candidates))
			}
		}
	}
	history := detectThinkingHistory(payload)
	stickyKey := routingStickyKey(selected, payload)
	if family := s.explicitConversationFamily(selected); !countTokens && family != "" {
		s.forceConversationFamily(stickyKey, family)
	} else if history == thinkingHistoryNone && s.conversationFamily(stickyKey) == "" {
		s.seedConversationFamily(stickyKey, s.openingConversationFamily(ctx, r, candidates))
	}
	candidates = s.rankCandidatesForRequest(selected, candidates, stickyKey, history == thinkingHistoryNone, history)
	if s.cfg.Definition == nil {
		candidates = s.deferInstabilityQuarantinedCandidates(r, candidates)
	}
	attempts := s.expandClaudeAccountAttempts(r, candidates, selected)
	initialReservation, _ := r.Context().Value(claudeAccountReservationContextKey{}).(claudeAccountReservation)
	initialReservationKey := claudeProfileRegistryKey(initialReservation.profile)
	trace := requestTrace{
		ID:             requestID,
		Path:           r.URL.Path,
		CandidateCount: len(attempts),
		ChainStarted:   time.Now(),
		ChainBudget:    fallbackChainHeaderBudget(body),
	}
	trace.Ingress, _ = r.Context().Value(proxyIngressContextKey{}).(time.Time)
	var lastErr error
	var lastStatus int
	var lastBody []byte
	anthropicServiceOutage := false
	anthropicFailureClass := ""
	s.claudePaidFallbackMu.Lock()
	anthropicOutageConfirmed := s.accountNow().Before(s.claudePaidFallback.ActiveUntil)
	s.claudePaidFallbackMu.Unlock()
	anthropicServiceSignalRecorded := false
	anthropicTransportFailures := 0
	paidEvidence := paidFallbackEvidence{}
	paidStarted := s.cfg.Definition != nil && s.cfg.Providers[selected.Provider].Billing == "metered"
	for i, accountAttempt := range attempts {
		if ctx.Err() != nil {
			return upstreamResponse{}, ctx.Err()
		}
		// A selected account may have been skipped before acquiring an attempt.
		// Drop its selection lease as soon as a different account is considered.
		if initialReservation.release != nil && claudeProfileRegistryKey(accountAttempt.profile) != initialReservationKey {
			initialReservation.release()
			initialReservation.release = nil
		}
		candidate := accountAttempt.model
		attemptRequest := withSelectedClaudeProfile(r, accountAttempt.profile)
		if candidate.Requested == "" {
			candidate.Requested = selected.Requested
		}
		providerName := firstNonEmpty(candidate.Provider, s.cfg.DefaultProvider)
		if s.cfg.Definition != nil && !paidStarted && s.cfg.Providers[providerName].Billing == "metered" {
			if !paidEvidence.allowed(selected) {
				lastErr = errors.New("metered fallback suppressed by configured cost policy")
				s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, false, "paid_fallback_suppressed")
				log.Printf("metered fallback suppressed request=%s provider=%s model=%q reason=configured_cost_policy", trace.ID, providerName, selected.Requested)
				continue
			}
		}
		if s.cfg.Definition == nil && s.explicitAnthropicPrimary(selected) && s.isClaudePaidFallbackProvider(providerName) {
			reason := ""
			switch anthropicFailureClass {
			case "auth_or_client":
				reason = "anthropic_auth_or_client_failure"
			case "service":
				if !anthropicOutageConfirmed {
					reason = "anthropic_outage_unconfirmed"
				}
			default:
				if s.claudePoolHasAuthBlock(r) {
					reason = "anthropic_auth_block_active"
				}
			}
			if reason != "" {
				log.Printf("Claude paid fallback suppressed request=%s provider=%s model=%q reason=%s", trace.ID, providerName, selected.Requested, reason)
				s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, false, "paid_fallback_suppressed")
				lastErr = fmt.Errorf("Claude subscription failed (%s); pay-as-you-go fallback suppressed", reason)
				continue
			}
		}
		if providerName == s.cfg.ClaudeUsage.Provider && candidate.ClaudeProfile == "" {
			candidate.ClaudeProfile = s.claudeProfileForRequest(attemptRequest).Name
		}
		if anthropicServiceOutage && providerName == s.cfg.ClaudeUsage.Provider {
			s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, i < len(attempts)-1, "provider_wide_outage")
			continue
		}
		runtimeProviderName := s.providerRuntimeKey(attemptRequest, providerName)
		evidenceKey := runtimeProviderName + "|" + candidate.Upstream
		if s.cfg.Definition != nil && s.cfg.Providers[providerName].Billing != "metered" {
			paidEvidence[evidenceKey] = ""
		}
		if reservedOut, reason := s.ollamaCandidateReservedOut(candidate); reservedOut {
			paidEvidence[evidenceKey] = "quota-exhausted"
			if s.cfg.Logging.DebugRequests {
				log.Printf("ollama reserve active, skipping provider=%s upstream=%q model=%q reason=%q", providerName, candidate.Upstream, candidate.Requested, reason)
			}
			s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, i < len(attempts)-1, "ollama_reserved_for_deepseek")
			lastErr = fmt.Errorf("provider %s upstream %q reserved for DeepSeek after %s threshold", providerName, candidate.Upstream, reason)
			continue
		}
		if exhausted, reason := s.browserUsageExhausted(providerName); exhausted {
			paidEvidence[evidenceKey] = "quota-exhausted"
			if s.cfg.Logging.DebugRequests {
				log.Printf("browser usage limit active, skipping provider=%s upstream=%q model=%q reason=%q", providerName, candidate.Upstream, candidate.Requested, reason)
			}
			s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, i < len(attempts)-1, "browser_usage_exhausted")
			lastErr = fmt.Errorf("provider %s subscription usage exhausted", providerName)
			continue
		}
		if allowed, reason := s.claudeSubscriptionAllowed(ctx, attemptRequest, candidate); !allowed {
			if reason == "configured_quota_limit" {
				paidEvidence[evidenceKey] = "quota-exhausted"
			}
			if s.explicitAnthropicPrimary(selected) && reason == "usage_unavailable" {
				if s.cfg.Logging.DebugRequests {
					log.Printf("Claude usage unavailable for explicit %q request; preserving Anthropic primary", selected.Requested)
				}
			} else {
				if s.cfg.Logging.DebugRequests {
					log.Printf("Claude subscription reserve active, skipping provider=%s upstream=%q model=%q reason=%q", providerName, candidate.Upstream, candidate.Requested, reason)
				}
				s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, i < len(attempts)-1, "claude_"+reason)
				lastErr = fmt.Errorf("Claude subscription upstream %q unavailable: %s", candidate.Upstream, reason)
				continue
			}
		}
		if blocked, state := s.providerBlockForCandidate(runtimeProviderName, candidate); blocked {
			if isQuotaReason(state.Reason) {
				paidEvidence[evidenceKey] = "quota-exhausted"
			}
			if s.cfg.Definition != nil && strings.HasPrefix(state.Reason, "instability_") && s.updateOutageEvidence(evidenceKey, "", 0) {
				paidEvidence[evidenceKey] = "confirmed-provider-outage"
			}
			instabilityOnly := strings.HasPrefix(state.Reason, "instability_")
			if providerName == s.cfg.ClaudeUsage.Provider {
				if strings.HasPrefix(state.Reason, "auth_") {
					anthropicFailureClass = "auth_or_client"
				} else if instabilityOnly && anthropicFailureClass != "auth_or_client" {
					anthropicFailureClass = "service"
				}
			}
			if instabilityOnly && i == len(attempts)-1 {
				log.Printf("provider quarantine bypassed request=%s provider=%s upstream=%q model=%q reason=%q cause=last_remaining_provider", trace.ID, providerName, candidate.Upstream, candidate.Requested, state.Reason)
			} else {
				if s.cfg.Logging.DebugRequests {
					log.Printf("provider limited, skipping provider=%s upstream=%q model=%q reason=%q until=%s", providerName, candidate.Upstream, candidate.Requested, state.Reason, state.BlockedUntil.UTC().Format(time.RFC3339))
				}
				s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, i < len(attempts)-1, "provider_limited")
				lastErr = fmt.Errorf("provider %s limited until %s", providerName, state.BlockedUntil.UTC().Format(time.RFC3339))
				continue
			}
		}
		circuitAllowed, circuitProbe := s.acquireCircuitAttempt(runtimeProviderName, candidate, i == len(attempts)-1)
		if !circuitAllowed {
			if s.cfg.Definition != nil && s.updateOutageEvidence(evidenceKey, "", 0) {
				paidEvidence[evidenceKey] = "confirmed-provider-outage"
			}
			if providerName == s.cfg.ClaudeUsage.Provider && anthropicFailureClass != "auth_or_client" {
				anthropicFailureClass = "service"
			}
			if s.cfg.Logging.DebugRequests {
				log.Printf("circuit open, skipping provider=%s upstream=%q model=%q (probe busy or cooldown active)", providerName, candidate.Upstream, candidate.Requested)
			}
			s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, true, "circuit_open")
			lastErr = fmt.Errorf("provider %s upstream %q circuit open (recent failures)", providerName, candidate.Upstream)
			continue
		}
		attemptBody, err := s.buildAttemptBody(body, payload, candidate, providerName, r.URL.Path)
		if err != nil {
			releaseCircuitProbe(circuitProbe)
			return upstreamResponse{}, err
		}
		attemptMaximum, reserveForLater := s.remainingAttemptHeaderBudget(trace, attempts, i, providerName)
		if attemptMaximum <= 0 {
			releaseCircuitProbe(circuitProbe)
			reason := "chain_budget_exhausted"
			if reserveForLater {
				reason = "chain_budget_reserved"
			}
			s.recordAttempt(trace, selected, candidate, i, time.Now(), 0, i < len(attempts)-1, reason)
			lastErr = fmt.Errorf("fallback first-byte budget exhausted after %s", time.Since(trace.ChainStarted).Round(time.Millisecond))
			if reserveForLater {
				continue
			}
			return upstreamResponse{}, lastErr
		}
		var releaseAccount func()
		if !countTokens && providerName == s.cfg.ClaudeUsage.Provider && accountAttempt.profile.Name != "" {
			attemptProfileKey := claudeProfileRegistryKey(accountAttempt.profile)
			switch {
			case attemptProfileKey == initialReservationKey && initialReservation.release != nil:
				// The initial account was reserved before fallback expansion. Reuse
				// that lease so it can be released as soon as this attempt ends.
				releaseAccount = initialReservation.release
				initialReservation.release = nil
			default:
				releaseAccount = s.reserveClaudeAccount(accountAttempt.profile)
			}
		}
		releaseAttemptAccount := func() {
			if releaseAccount != nil {
				releaseAccount()
				releaseAccount = nil
			}
		}
		attemptStarted := time.Now()
		trace.Timing = &upstreamTiming{}
		trace.HeadersMS, trace.FirstEventMS = 0, 0
		releaseRoute := s.reserveRoute(runtimeProviderName, candidate)
		if s.cfg.Definition != nil && s.cfg.Providers[providerName].Billing == "metered" {
			paidStarted = true
		}
		if s.cfg.Providers[providerName].Variant == "opencode-go" {
			attemptRequest = withOpenCodeSession(attemptRequest, payload)
		}
		resp, err := s.doUpstreamWithHeaderBudgetLimit(context.WithValue(ctx, upstreamTimingContextKey{}, trace.Timing), attemptRequest, attemptBody, candidate, attemptMaximum)
		if resp != nil {
			trace.HeadersMS = time.Since(attemptStarted).Milliseconds()
		}
		if err != nil {
			releaseAttemptAccount()
			releaseRoute()
			releaseCircuitProbe(circuitProbe)
			if ctx.Err() != nil {
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, 0, false, "client_cancel")
				return upstreamResponse{}, ctx.Err()
			}
			var headerTimeout *responseHeaderTimeoutError
			chainLimited := errors.As(err, &headerTimeout) && headerTimeout.chainLimited
			var credentialErr *providerCredentialError
			authFailure := errors.As(err, &credentialErr) || (providerName == s.cfg.ClaudeUsage.Provider && strings.Contains(err.Error(), "Claude subscription auth"))
			failureReason := "transport"
			if authFailure {
				failureReason = "auth_unavailable"
			}
			if chainLimited {
				failureReason = "chain_deadline"
			}
			networkDecision := networkFailureDecision{}
			if !countTokens && !chainLimited && !authFailure {
				signal := "transport"
				if s.excessiveTTFB(time.Since(attemptStarted)) || strings.Contains(strings.ToLower(err.Error()), "timeout awaiting response headers") {
					signal = "ttfb_timeout"
				}
				networkDecision = s.observeNetworkTransportFailure(runtimeProviderName, signal, err)
				if networkDecision.SuppressProviderPenalty {
					failureReason = "local_network"
				} else {
					s.observeProviderInstability(runtimeProviderName, signal)
					s.tripCircuit(runtimeProviderName, candidate)
				}
			}
			if providerName == s.cfg.ClaudeUsage.Provider {
				if authFailure {
					anthropicFailureClass = "auth_or_client"
				} else if !chainLimited && !networkDecision.SuppressProviderPenalty {
					if anthropicFailureClass != "auth_or_client" {
						anthropicFailureClass = "service"
					}
					anthropicTransportFailures++
					if !anthropicServiceSignalRecorded {
						anthropicOutageConfirmed = s.observeClaudeServiceFailure()
						anthropicServiceSignalRecorded = true
					}
					anthropicServiceOutage = shouldShortCircuitClaudeTransport(anthropicTransportFailures, anthropicOutageConfirmed)
				}
			}
			if networkDecision.StopFallback {
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, 0, false, failureReason)
				return upstreamResponse{}, &retryableNetworkError{message: "local network unavailable; retry shortly"}
			}
			s.recordAttempt(trace, selected, candidate, i, attemptStarted, 0, i < len(attempts)-1, failureReason)
			lastErr = err
			log.Printf("provider fallback candidate failed request=%s model=%q provider=%s upstream=%q error=%v", trace.ID, candidate.Requested, providerName, candidate.Upstream, err)
			continue
		}
		if resp.Body != nil {
			resp.Body = &releaseReadCloser{ReadCloser: resp.Body, release: func() {
				releaseRoute()
				releaseCircuitProbe(circuitProbe)
				releaseAttemptAccount()
			}}
		} else {
			releaseRoute()
			releaseCircuitProbe(circuitProbe)
			releaseAttemptAccount()
		}
		if !countTokens && resp.Body != nil && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices &&
			strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
			attemptBudget := s.candidateResponseHeaderBudget(providerName, candidate, attemptBody)
			if attemptMaximum < attemptBudget {
				attemptBudget = attemptMaximum
			}
			primeBudget := attemptBudget - time.Since(attemptStarted)
			if primeBudget > initialSSEEventTimeout {
				primeBudget = initialSSEEventTimeout
			}
			var primedBody io.ReadCloser
			var primeErr error
			if primeBudget <= 0 {
				primeErr = fmt.Errorf("first-event budget exhausted")
			} else {
				primedBody, primeErr = primeSSEBody(resp.Body, primeBudget)
			}
			if primeErr != nil {
				_ = resp.Body.Close()
				if ctx.Err() != nil {
					s.recordAttempt(trace, selected, candidate, i, attemptStarted, 0, false, "client_cancel")
					return upstreamResponse{}, ctx.Err()
				}
				failureReason := "stream_start"
				var providerError *initialSSEError
				kind := ""
				if errors.As(primeErr, &providerError) {
					kind = providerError.kind
				}
				if kind == "invalid_request_error" {
					s.recordAttempt(trace, selected, candidate, i, attemptStarted, 400, false, "client_invalid")
					return upstreamResponse{}, &requestCompatibilityError{message: primeErr.Error()}
				}
				quotaError := kind == "rate_limit_error"
				authError := kind == "authentication_error" || kind == "permission_error"
				if quotaError {
					paidEvidence[evidenceKey] = "quota-exhausted"
					failureReason = "quota_or_rate_limit"
					s.markProviderBlocked(s.quotaBlockKeyForResponse(runtimeProviderName, candidate, resp.Header), failureReason, providerCooldown(resp.Header, nil))
				}
				if authError {
					failureReason = "auth_exhausted"
				}
				networkDecision := networkFailureDecision{}
				if !quotaError && !authError {
					networkDecision = s.observeNetworkTransportFailure(runtimeProviderName, failureReason, primeErr)
				}
				if networkDecision.SuppressProviderPenalty {
					failureReason = "local_network"
				} else if !quotaError && !authError {
					s.observeProviderInstability(runtimeProviderName, "stream_start")
					s.tripCircuit(runtimeProviderName, candidate)
				}
				if providerName == s.cfg.ClaudeUsage.Provider {
					if authError {
						anthropicFailureClass = "auth_or_client"
						s.markProviderBlocked(runtimeProviderName, "auth_exhausted", time.Minute)
						clearClaudeProfileOAuthToken(accountAttempt.profile)
						s.triggerClaudeOAuthRefresh(accountAttempt.profile)
					} else if !quotaError && !networkDecision.SuppressProviderPenalty && anthropicFailureClass != "auth_or_client" {
						anthropicFailureClass = "service"
						if !networkDecision.SuppressProviderPenalty && !anthropicServiceSignalRecorded {
							anthropicOutageConfirmed = s.observeClaudeServiceFailure()
							anthropicServiceSignalRecorded = true
						}
					}
				}
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, resp.StatusCode, i < len(attempts)-1, failureReason)
				lastErr = fmt.Errorf("provider %s upstream %q failed before first stream event: %w", providerName, candidate.Upstream, primeErr)
				log.Printf("provider fallback stream start failed request=%s model=%q provider=%s upstream=%q error=%v", trace.ID, candidate.Requested, providerName, candidate.Upstream, primeErr)
				if networkDecision.StopFallback {
					return upstreamResponse{}, &retryableNetworkError{message: "local network unavailable; retry shortly"}
				}
				continue
			}
			resp.Body = primedBody
			trace.FirstEventMS = time.Since(attemptStarted).Milliseconds()
		}
		s.observeNetworkSuccess()
		if s.cfg.Definition != nil {
			if s.updateOutageEvidence(evidenceKey, trace.ID, resp.StatusCode) && resp.StatusCode >= 500 {
				paidEvidence[evidenceKey] = "confirmed-provider-outage"
			}
			if resp.StatusCode == 429 || resp.StatusCode == 402 {
				paidEvidence[evidenceKey] = "quota-exhausted"
			}
		}
		networkRecovered = true
		unstable := false
		responseFailureReason := ""
		if !countTokens {
			ttfb := time.Since(attemptStarted)
			signal := ""
			instabilityKey := runtimeProviderName
			switch {
			case resp.StatusCode == http.StatusTooManyRequests && !s.explicitAnthropicPrimary(selected):
				signal = "http_rate_limit"
			case resp.StatusCode >= http.StatusInternalServerError:
				signal = "http_5xx"
				if providerName == s.cfg.ClaudeUsage.Provider {
					instabilityKey = providerName
					anthropicServiceOutage = true
					if anthropicFailureClass != "auth_or_client" {
						anthropicFailureClass = "service"
					}
					if !anthropicServiceSignalRecorded {
						anthropicOutageConfirmed = s.observeClaudeServiceFailure()
						anthropicServiceSignalRecorded = true
					}
				}
			case resp.StatusCode < http.StatusBadRequest && s.excessiveTTFB(ttfb):
				signal = "excessive_ttfb"
			}
			if signal != "" {
				unstable = true
				s.observeProviderInstability(instabilityKey, signal)
			}
			if resp.StatusCode == http.StatusTooManyRequests && !s.explicitAnthropicPrimary(selected) {
				s.markProviderBlocked(runtimeProviderName, "quota_or_rate_limit", providerCooldown(resp.Header, nil))
			}
		}
		if !countTokens {
			s.observeProviderHeadersAt(runtimeProviderName, resp.Header, attemptStarted)
		}
		if providerName == s.cfg.ClaudeUsage.Provider &&
			resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError &&
			resp.StatusCode != http.StatusPaymentRequired && resp.StatusCode != http.StatusTooManyRequests {
			anthropicFailureClass = "auth_or_client"
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			// Cached credentials may be stale (rotated key): drop them so
			// the next attempt re-resolves auth from the source.
			authTokenCache.Delete(providerName)
			if providerName == s.cfg.ClaudeUsage.Provider {
				anthropicFailureClass = "auth_or_client"
				profile := s.claudeProfileForRequest(attemptRequest)
				clearClaudeProfileOAuthToken(profile)
				s.triggerClaudeOAuthRefresh(profile)
				s.markProviderBlocked(runtimeProviderName, "auth_exhausted", time.Minute)
			}
		}
		fourXXClassified := false
		if i < len(attempts)-1 && fallbackCandidateStatus(resp.StatusCode) {
			errorBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			if readErr == nil && thinkingSignatureMismatchBody(errorBody) {
				// Opaque thinking signatures are bound to the provider that made
				// them. This is route compatibility, not provider instability.
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, resp.StatusCode, true, "thinking_signature_mismatch")
				lastStatus = resp.StatusCode
				lastBody = errorBody
				log.Printf("provider fallback thinking signature mismatch request=%s model=%q provider=%s upstream=%q status=%d", trace.ID, candidate.Requested, providerName, candidate.Upstream, resp.StatusCode)
				continue
			}
			routeIncompatible := readErr == nil && providerSpecificRequestIncompatibility(resp.StatusCode, errorBody)
			if routeIncompatible {
				if !countTokens {
					s.tripCircuit(runtimeProviderName, candidate)
				}
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, resp.StatusCode, true, "route_incompatible")
				lastStatus = resp.StatusCode
				lastBody = errorBody
				log.Printf("provider fallback route incompatibility request=%s model=%q provider=%s upstream=%q status=%d body=%q", trace.ID, candidate.Requested, providerName, candidate.Upstream, resp.StatusCode, trimForLog(string(errorBody), 240))
				continue
			}
			recognizedClientError := readErr == nil && recognizedClientPayloadError(resp.StatusCode, errorBody)
			if readErr == nil && !recognizedClientError && (resp.StatusCode >= http.StatusInternalServerError || shouldFallbackBody(errorBody) || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusPaymentRequired) {
				fourXXClassified = resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError
				failureReason := classifyFallbackBody(errorBody)
				if resp.StatusCode == http.StatusPaymentRequired || resp.StatusCode == http.StatusTooManyRequests {
					failureReason = "quota_or_rate_limit"
				}
				if failureReason == "quota_or_rate_limit" || resp.StatusCode == http.StatusTooManyRequests {
					blockKey := s.quotaBlockKeyForResponse(runtimeProviderName, candidate, resp.Header)
					s.markProviderBlocked(blockKey, failureReason, providerCooldown(resp.Header, errorBody))
				}
				if !countTokens && failureReason != "quota_or_rate_limit" && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
					s.tripCircuit(runtimeProviderName, candidate)
				}
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, resp.StatusCode, true, failureReason)
				lastStatus = resp.StatusCode
				lastBody = errorBody
				log.Printf("provider fallback status request=%s model=%q provider=%s upstream=%q status=%d body=%q", trace.ID, candidate.Requested, providerName, candidate.Upstream, resp.StatusCode, trimForLog(string(errorBody), 240))
				continue
			}
			if recognizedClientError {
				fourXXClassified = true
				responseFailureReason = "client_invalid"
				// The upstream affirmatively rejected the request payload
				// itself (too long, malformed): every leg would fail the same
				// way, so stop the chain. Context overflow is normalized to
				// Claude Code's non-retryable 413 prompt_too_long contract.
				originalStatus := resp.StatusCode
				if normalizedBody, normalized := normalizeContextOverflowResponse(resp, errorBody); normalized {
					errorBody = normalizedBody
					log.Printf("context overflow normalized model=%q provider=%s upstream=%q status=%d->%d", candidate.Requested, providerName, candidate.Upstream, originalStatus, resp.StatusCode)
				}
				log.Printf("provider fallback vetoed by body request=%s model=%q provider=%s upstream=%q status=%d body=%q", trace.ID, candidate.Requested, providerName, candidate.Upstream, resp.StatusCode, trimForLog(string(errorBody), 240))
				resp.Body = io.NopCloser(bytes.NewReader(errorBody))
			} else {
				fourXXClassified = resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError
				// Opaque 4xx with no recognizable cause (e.g. OpenCode Go's
				// bare {"model":"..."} when the model is unavailable or
				// overloaded): do NOT trust it as a client error — try the
				// next leg, and trip the circuit so concurrent requests skip
				// this brownout leg instead of paying its latency each time.
				if !countTokens && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
					s.tripCircuit(runtimeProviderName, candidate)
				}
				s.recordAttempt(trace, selected, candidate, i, attemptStarted, resp.StatusCode, true, "opaque_4xx")
				lastStatus = resp.StatusCode
				lastBody = errorBody
				log.Printf("provider fallback opaque 4xx request=%s model=%q provider=%s upstream=%q status=%d body=%q", trace.ID, candidate.Requested, providerName, candidate.Upstream, resp.StatusCode, trimForLog(string(errorBody), 240))
				continue
			}
		}
		if !fourXXClassified && resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError && resp.StatusCode != http.StatusTooManyRequests {
			errorBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if readErr == nil {
				_ = resp.Body.Close()
				originalStatus := resp.StatusCode
				if normalizedBody, normalized := normalizeContextOverflowResponse(resp, errorBody); normalized {
					errorBody = normalizedBody
					log.Printf("context overflow normalized model=%q provider=%s upstream=%q status=%d->%d", candidate.Requested, providerName, candidate.Upstream, originalStatus, resp.StatusCode)
				}
				resp.Body = io.NopCloser(bytes.NewReader(errorBody))
				if providerSpecificRequestIncompatibility(resp.StatusCode, errorBody) {
					responseFailureReason = "route_incompatible"
					if !countTokens {
						s.tripCircuit(runtimeProviderName, candidate)
					}
				} else if recognizedClientPayloadError(resp.StatusCode, errorBody) {
					responseFailureReason = "client_invalid"
				} else if !countTokens && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusPaymentRequired {
					responseFailureReason = "opaque_4xx"
					s.tripCircuit(runtimeProviderName, candidate)
				}
			}
		}
		if responseFailureReason == "client_invalid" && !countTokens {
			s.resetCircuitAfter(runtimeProviderName, candidate, attemptStarted)
		}
		if !countTokens && providerName == s.cfg.ClaudeUsage.Provider && resp.StatusCode < http.StatusBadRequest {
			s.clearClaudeServiceFailures()
			profile := s.claudeProfileForRequest(attemptRequest)
			s.refreshClaudeAccountLease(stickyKey, profile, candidate.AccountStickySeconds)
			if accountAttempt.fromProfile != "" && accountAttempt.fromProfile != profile.Name {
				log.Printf("Claude account failover request=%s listener=%q from=%q to=%q reason=account_local_exhaustion", trace.ID, requestListenerPort(r), accountAttempt.fromProfile, profile.Name)
			}
		}
		return upstreamResponse{resp: resp, providerName: providerName, model: candidate, attempt: i, started: attemptStarted, trace: trace, unstable: unstable, failureReason: responseFailureReason}, nil
	}
	if lastErr != nil {
		return upstreamResponse{}, lastErr
	}
	return upstreamResponse{}, fmt.Errorf("all fallback providers failed; last status=%d body=%s", lastStatus, trimForLog(string(lastBody), 240))
}

func (s *proxyServer) doUpstream(ctx context.Context, r *http.Request, body []byte, selectedModel modelConfig) (*http.Response, error) {
	providerName := firstNonEmpty(selectedModel.Provider, s.cfg.DefaultProvider)
	provider := s.cfg.Providers[providerName]
	providerURL, ok := s.providers[providerName]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q", providerName)
	}
	if s.cfg.Logging.DebugRequests {
		log.Printf(
			"request request=%s method=%s path=%s model=%q provider=%s profile=%q upstream=%q alias=%q",
			proxyRequestID(r),
			r.Method,
			r.URL.Path,
			selectedModel.Requested,
			providerName,
			selectedModel.ClaudeProfile,
			selectedModel.Upstream,
			firstNonEmpty(selectedModel.ResponseAlias, selectedModel.Requested),
		)
	}
	upstreamURL := *providerURL
	upstreamURL.Path = joinURLPath(providerURL.Path, r.URL.Path)
	if s.cfg.Definition != nil {
		upstreamURL.Path = joinURLPath(providerURL.Path, provider.MessagesPath)
		if strings.HasSuffix(r.URL.Path, "/count_tokens") {
			upstreamURL.Path += "/count_tokens"
		}
	}
	if provider.Format == "openai-chat" && s.cfg.Definition == nil {
		// OpenAI chat-completions providers expose /v1/chat/completions
		// instead of /v1/messages (count_tokens rides along and 404s, which
		// the count_tokens handler already treats as "unsupported").
		upstreamURL.Path = joinURLPath(providerURL.Path, strings.Replace(r.URL.Path, "/v1/messages", "/v1/chat/completions", 1))
	}
	upstreamURL.RawQuery = r.URL.RawQuery

	upstreamReq, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	copyHeaders(upstreamReq.Header, r.Header)
	if s.cfg.Definition != nil {
		for _, key := range []string{"Authorization", "x-api-key", "api-key", "Cookie", "Proxy-Authorization"} {
			upstreamReq.Header.Del(key)
		}
	}
	// Remove the client's Accept-Encoding so the transport negotiates gzip itself
	// and transparently decompresses. Forwarding the client's value would leave
	// possibly-compressed bytes flowing into the text SSE/JSON filters.
	upstreamReq.Header.Del("Accept-Encoding")
	upstreamReq.Host = providerURL.Host
	upstreamReq.ContentLength = int64(len(body))
	upstreamReq.Header.Set("Content-Length", strconv.Itoa(len(body)))
	if providerName == s.cfg.ClaudeUsage.Provider {
		auth, err := s.claudeOAuthBearer(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("Claude subscription auth: %w", err)
		}
		// Never let a saved Anthropic API key win over subscription OAuth.
		upstreamReq.Header.Del("x-api-key")
		upstreamReq.Header.Del("api-key")
		upstreamReq.Header.Set("Authorization", auth)
		ensureCommaSeparatedHeaderValue(upstreamReq.Header, "anthropic-beta", "oauth-2025-04-20")
	}
	if err := s.applyProviderHeadersContext(ctx, providerName, upstreamReq.Header); err != nil {
		return nil, err
	}
	upstreamReq.Header.Del("x-opencode-session")
	if provider.Variant == "opencode-go" {
		if session, ok := r.Context().Value(openCodeSessionKey{}).(string); ok {
			upstreamReq.Header.Set("x-opencode-session", session)
		}
		upstreamReq.Header.Set("User-Agent", "claude-gateway/1")
	}
	client, ok := s.upstreamClient(providerName, r)
	if !ok {
		return nil, fmt.Errorf("provider %q has no HTTP client", providerName)
	}
	resp, err := client.Do(upstreamReq)
	if err != nil || resp.StatusCode != http.StatusUnauthorized || provider.AuthMode != "xai-oauth" {
		return resp, err
	}

	manager := s.xaiOAuth[providerName]
	if manager == nil || upstreamReq.GetBody == nil {
		return resp, nil
	}
	failedToken := strings.TrimPrefix(upstreamReq.Header.Get("Authorization"), "Bearer ")
	refreshedToken, refreshErr := manager.refreshAfterUnauthorized(ctx, failedToken)
	if refreshErr != nil {
		log.Printf("provider=%s xAI OAuth refresh after 401 failed: %v", providerName, refreshErr)
		return resp, nil
	}
	retryBody, bodyErr := upstreamReq.GetBody()
	if bodyErr != nil {
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
	_ = resp.Body.Close()

	retryReq := upstreamReq.Clone(ctx)
	retryReq.Body = retryBody
	retryReq.Header = upstreamReq.Header.Clone()
	retryReq.Header.Set("Authorization", "Bearer "+refreshedToken)
	log.Printf("provider=%s retrying once after xAI OAuth token refresh", providerName)
	return client.Do(retryReq)
}

func (s *proxyServer) upstreamClient(providerName string, r *http.Request) (*http.Client, bool) {
	if providerName == s.cfg.ClaudeUsage.Provider {
		profile, ok := selectedClaudeProfile(r)
		if !ok {
			profile = s.listenerClaudeProfile(r)
		}
		if client := s.claudeProfileClients[profile.Name]; client != nil {
			return client, true
		}
	}
	client, ok := s.clients[providerName]
	return client, ok
}

func (s *proxyServer) doUpstreamWithHeaderBudget(ctx context.Context, r *http.Request, body []byte, selectedModel modelConfig) (*http.Response, error) {
	return s.doUpstreamWithHeaderBudgetLimit(ctx, r, body, selectedModel, 0)
}

func (s *proxyServer) doUpstreamWithHeaderBudgetLimit(ctx context.Context, r *http.Request, body []byte, selectedModel modelConfig, maximum time.Duration) (*http.Response, error) {
	providerName := firstNonEmpty(selectedModel.Provider, s.cfg.DefaultProvider)
	budget := s.candidateResponseHeaderBudget(providerName, selectedModel, body)
	chainLimited := maximum > 0 && maximum < budget
	if chainLimited {
		budget = maximum
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	if timing, ok := ctx.Value(upstreamTimingContextKey{}).(*upstreamTiming); ok {
		var connectionStart, dnsStart, tlsStart atomic.Int64
		attemptCtx = httptrace.WithClientTrace(attemptCtx, &httptrace.ClientTrace{
			GetConn: func(string) { connectionStart.Store(time.Now().UnixNano()) },
			GotConn: func(httptrace.GotConnInfo) {
				if at := connectionStart.Load(); at > 0 {
					timing.connectionMS.Store(time.Since(time.Unix(0, at)).Milliseconds())
				}
			},
			DNSStart: func(httptrace.DNSStartInfo) { dnsStart.Store(time.Now().UnixNano()) },
			DNSDone: func(httptrace.DNSDoneInfo) {
				if at := dnsStart.Load(); at > 0 {
					timing.dnsMS.Store(time.Since(time.Unix(0, at)).Milliseconds())
				}
			},
			TLSHandshakeStart: func() { tlsStart.Store(time.Now().UnixNano()) },
			TLSHandshakeDone: func(tls.ConnectionState, error) {
				if at := tlsStart.Load(); at > 0 {
					timing.tlsMS.Store(time.Since(time.Unix(0, at)).Milliseconds())
				}
			},
		})
	}
	headerStarted := time.Now()
	var timedOut atomic.Bool
	timerFired := make(chan struct{})
	timer := time.AfterFunc(budget, func() {
		timedOut.Store(true)
		cancel()
		close(timerFired)
	})
	resp, err := s.doUpstream(attemptCtx, r, body, selectedModel)
	if !timer.Stop() {
		<-timerFired
	}
	var transportTimeout net.Error
	transportBudgetExpired := err != nil && errors.As(err, &transportTimeout) && transportTimeout.Timeout() && time.Since(headerStarted) >= budget
	if ctx.Err() != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel()
		return nil, ctx.Err()
	}
	if timedOut.Load() || transportBudgetExpired {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel()
		return nil, &responseHeaderTimeoutError{provider: providerName, upstream: selectedModel.Upstream, budget: budget, chainLimited: chainLimited}
	}
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.Body == nil {
		cancel()
		return resp, nil
	}
	resp.Body = &releaseReadCloser{ReadCloser: resp.Body, release: cancel}
	return resp, nil
}

var errUnknownModel = errors.New("unknown model")

// normalizeRequest reads the request body and parses it exactly once. The
// parsed payload is returned alongside the raw bytes so downstream stages
// (model rewrite, provider options) never re-parse; the body is re-marshaled
// here only when content-type normalization actually mutated it. Model
// rewriting happens per-candidate in buildAttemptBody.
func (s *proxyServer) normalizeRequest(r *http.Request) ([]byte, map[string]any, modelConfig, error) {
	invalid := func() ([]byte, map[string]any, modelConfig, error) {
		return nil, nil, modelConfig{}, fmt.Errorf("%w: request requires valid JSON and an explicit configured model alias", errUnknownModel)
	}
	if r.Body == nil {
		if s.cfg.Definition != nil {
			return invalid()
		}
		return nil, nil, modelConfig{}, nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil, modelConfig{}, err
	}
	_ = r.Body.Close()
	if len(body) == 0 {
		if s.cfg.Definition != nil {
			return invalid()
		}
		return body, nil, modelConfig{}, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		if s.cfg.Definition != nil {
			return invalid()
		}
		return body, nil, modelConfig{}, nil
	}

	selected := modelConfig{}
	if s.cfg.Definition != nil {
		model, ok := payload["model"].(string)
		if !ok || strings.TrimSpace(model) == "" {
			return invalid()
		}
	}
	if model, ok := payload["model"].(string); ok {
		if mapped, exists := s.cfg.Models[model]; exists {
			selected = mapped
		} else if family, matched := s.cfg.matchFamily(model); matched {
			selected = family
		} else {
			return body, payload, modelConfig{Requested: model}, fmt.Errorf("%w %q", errUnknownModel, model)
		}
		selected.Requested = model
	}
	mutated := normalizeHistoricalToolIDs(payload)
	if len(s.cfg.Normalize.UnsupportedContentTypes) > 0 {
		if messages, ok := payload["messages"].([]any); ok {
			payload["messages"] = s.normalizeMessages(messages)
			mutated = true
		}
	}
	if mutated {
		normalized, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, selected, err
		}
		body = normalized
	}
	return body, payload, selected, nil
}

// buildAttemptBody produces the upstream body for one fallback candidate from
// the request parsed exactly once in normalizeRequest. The common paths avoid
// re-parsing multi-megabyte conversation bodies: an untouched request (no
// model rewrite, no provider options) forwards the client's original bytes
// verbatim, and a plain model rewrite mutates the shared parsed payload and
// marshals once. Only providers that reshape the payload (openai-chat
// translation, system folding, request overrides) parse a fresh copy, because
// those mutations must not leak into a later candidate's body.
func (s *proxyServer) buildAttemptBody(body []byte, payload map[string]any, candidate modelConfig, providerName, requestPath string) ([]byte, error) {
	if payload == nil {
		return body, nil // empty or non-JSON request: forward as-is
	}
	provider := s.cfg.Providers[providerName]
	history := detectThinkingHistory(payload)
	compatibilityProvider := providerName
	if providerName == s.cfg.ClaudeUsage.Provider {
		compatibilityProvider = "anthropic"
	}
	sanitizeThinking := thinkingHistoryNeedsSanitization(history, compatibilityProvider)
	countTokens := strings.HasSuffix(requestPath, "/count_tokens")
	openAIChat := provider.Format == "openai-chat" && !countTokens
	fold := provider.FoldSystemIntoMessages && !countTokens
	sanitizeSonnet := providerName == s.cfg.ClaudeUsage.Provider && strings.Contains(strings.ToLower(candidate.Upstream), "claude-sonnet-5")
	overrides := provider.RequestOverrides
	if len(candidate.ModelOverrides) > 0 {
		overrides = make(map[string]any, len(provider.RequestOverrides)+len(candidate.ModelOverrides))
		for k, v := range provider.RequestOverrides {
			overrides[k] = v
		}
		for k, v := range candidate.ModelOverrides {
			overrides[k] = v
		}
	}
	if countTokens {
		overrides = nil
	}
	if openAIChat || fold || len(overrides) > 0 || sanitizeThinking || sanitizeSonnet {
		var fresh map[string]any
		if err := json.Unmarshal(body, &fresh); err != nil {
			return body, nil
		}
		if candidate.Upstream != "" {
			fresh["model"] = candidate.Upstream
		}
		if sanitizeThinking {
			stripHistoricalThinking(fresh)
		}
		if sanitizeSonnet {
			delete(fresh, "temperature")
		}
		if openAIChat {
			return anthropicToOpenAIRequest(fresh, overrides)
		}
		if fold {
			foldSystemIntoMessages(fresh)
		}
		for key, value := range overrides {
			fresh[key] = value
		}
		return json.Marshal(fresh)
	}
	if candidate.Upstream == "" {
		return body, nil // passthrough: client bytes untouched
	}
	// Shallow-copy the parsed top-level object so one candidate's model rewrite
	// cannot leak into the next fallback attempt.
	fresh := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		fresh[key] = value
	}
	fresh["model"] = candidate.Upstream
	return json.Marshal(fresh)
}

func (s *proxyServer) normalizeMessages(messages []any) []any {
	for i, item := range messages {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		content, ok := message["content"].([]any)
		if !ok {
			continue
		}
		message["content"] = s.normalizeContentBlocks(content)
		messages[i] = message
	}
	return messages
}

func (s *proxyServer) normalizeContentBlocks(blocks []any) []any {
	for i, item := range blocks {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		blockType, _ := block["type"].(string)
		if replacement, replace := s.cfg.Normalize.UnsupportedContentTypes[blockType]; replace {
			if replacement == "text" {
				raw, _ := json.Marshal(block)
				blocks[i] = map[string]any{
					"type": "text",
					"text": "[" + blockType + " omitted: " + string(raw) + "]",
				}
				continue
			}
		}
		if nested, ok := block["content"].([]any); ok {
			block["content"] = s.normalizeContentBlocks(nested)
			blocks[i] = block
		}
	}
	return blocks
}

// normalizeHistoricalToolIDs repairs model-generated IDs before replaying a
// conversation to another provider. Some gateways emit IDs such as
// "Bash:157"; Anthropic rejects the colon. The deterministic rewrite is also
// applied to tool_result references, preserving tool/result pairing.
func normalizeHistoricalToolIDs(payload map[string]any) bool {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	changed := false
	for _, item := range messages {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if content, ok := message["content"].([]any); ok && normalizeToolIDsInBlocks(content) {
			changed = true
		}
	}
	return changed
}

func normalizeToolIDsInBlocks(blocks []any) bool {
	changed := false
	for _, item := range blocks {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if normalizeToolIDBlock(block) {
			changed = true
		}
		if nested, ok := block["content"].([]any); ok && normalizeToolIDsInBlocks(nested) {
			changed = true
		}
	}
	return changed
}

func normalizeToolIDBlock(block map[string]any) bool {
	changed := false
	blockType, _ := block["type"].(string)
	if id, ok := block["id"].(string); ok && (blockType == "tool_use" || blockType == "server_tool_use") {
		normalized := sanitizeToolUseID(id, blockType == "server_tool_use")
		if normalized != id {
			block["id"] = normalized
			changed = true
		}
	}
	if id, ok := block["tool_use_id"].(string); ok {
		normalized := sanitizeToolUseID(id, strings.HasPrefix(id, "srvtoolu_"))
		if normalized != id {
			block["tool_use_id"] = normalized
			changed = true
		}
	}
	return changed
}

func sanitizeToolUseID(id string, serverTool bool) string {
	if validToolUseID(id, serverTool) {
		return id
	}
	original := id
	if serverTool {
		id = strings.TrimPrefix(id, "srvtoolu_")
	}
	var cleaned strings.Builder
	for _, char := range id {
		if isASCIIAlphaNumeric(char) || char == '_' || (!serverTool && char == '-') {
			cleaned.WriteRune(char)
		} else {
			cleaned.WriteByte('_')
		}
	}
	base := strings.Trim(cleaned.String(), "_")
	if base == "" {
		base = "tool"
	}
	digest := sha256.Sum256([]byte(original))
	suffix := fmt.Sprintf("_%x", digest[:6])
	prefix := ""
	if serverTool {
		prefix = "srvtoolu_"
	}
	const maxToolUseIDLength = 64
	maxBase := maxToolUseIDLength - len(prefix) - len(suffix)
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return prefix + base + suffix
}

func validToolUseID(id string, serverTool bool) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	value := id
	if serverTool {
		if !strings.HasPrefix(value, "srvtoolu_") {
			return false
		}
		value = strings.TrimPrefix(value, "srvtoolu_")
		if value == "" {
			return false
		}
	}
	for _, char := range value {
		if isASCIIAlphaNumeric(char) || char == '_' || (!serverTool && char == '-') {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlphaNumeric(char rune) bool {
	return char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9'
}

const (
	xaiOAuthIssuer        = "https://auth.x.ai"
	xaiOAuthDiscoveryURL  = xaiOAuthIssuer + "/.well-known/openid-configuration"
	xaiOAuthDeviceCodeURL = xaiOAuthIssuer + "/oauth2/device/code"
	xaiOAuthClientID      = "b1a00492-073a-47ea-816f-4c329264a828"
	xaiOAuthScope         = "openid profile email offline_access grok-cli:access api:access"
	xaiOAuthRefreshSkew   = 2 * time.Minute
	xaiOAuthStateVersion  = 1
)

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type xaiOAuthManager struct {
	mu        sync.Mutex
	statePath string
	client    httpDoer
	state     xaiOAuthState
}

type xaiOAuthState struct {
	Version       int       `json:"version"`
	AuthMode      string    `json:"authMode"`
	AccessToken   string    `json:"accessToken,omitempty"`
	RefreshToken  string    `json:"refreshToken,omitempty"`
	IDToken       string    `json:"idToken,omitempty"`
	TokenType     string    `json:"tokenType,omitempty"`
	ExpiresAt     time.Time `json:"expiresAt,omitempty"`
	TokenEndpoint string    `json:"tokenEndpoint,omitempty"`
	LastRefresh   time.Time `json:"lastRefresh,omitempty"`
	LastError     string    `json:"lastError,omitempty"`
}

type xaiOIDCDiscovery struct {
	TokenEndpoint string `json:"token_endpoint"`
}

type xaiDeviceCodeResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

type xaiTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Error        string `json:"error"`
	Description  string `json:"error_description"`
}

func newXAIOAuthManager(statePath string, client httpDoer) *xaiOAuthManager {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &xaiOAuthManager{statePath: statePath, client: client}
}

func validateXAIOrigin(rawURL, label string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("invalid xAI %s: %w", label, err)
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("xAI %s must use HTTPS", label)
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" || (host != "x.ai" && !strings.HasSuffix(host, ".x.ai")) {
		return fmt.Errorf("xAI %s host %q is outside x.ai", label, host)
	}
	return nil
}

func validateXAISubscriptionOrigin(rawURL, label string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("invalid xAI %s: %w", label, err)
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("xAI %s must use HTTPS", label)
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "cli-chat-proxy.grok.com" {
		return fmt.Errorf("xAI %s host %q is not the SuperGrok subscription origin", label, host)
	}
	return nil
}

func jwtExpiry(token string) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Expires int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Expires <= 0 {
		return time.Time{}
	}
	return time.Unix(claims.Expires, 0)
}

func xaiStateExpiry(state xaiOAuthState) time.Time {
	if expiry := jwtExpiry(state.AccessToken); !expiry.IsZero() {
		return expiry
	}
	return state.ExpiresAt
}

func readXAIOAuthState(path string) (xaiOAuthState, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return xaiOAuthState{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return xaiOAuthState{}, fmt.Errorf("xAI OAuth state %q must be a regular file, not a symlink", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return xaiOAuthState{}, fmt.Errorf("xAI OAuth state %q permissions are %04o, want 0600", path, info.Mode().Perm())
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return xaiOAuthState{}, err
	}
	var state xaiOAuthState
	if err := json.Unmarshal(body, &state); err != nil {
		return xaiOAuthState{}, fmt.Errorf("decode xAI OAuth state: %w", err)
	}
	if state.Version != xaiOAuthStateVersion {
		return xaiOAuthState{}, fmt.Errorf("xAI OAuth state version %d is unsupported", state.Version)
	}
	if state.TokenEndpoint != "" {
		if err := validateXAIOrigin(state.TokenEndpoint, "token endpoint"); err != nil {
			return xaiOAuthState{}, err
		}
	}
	return state, nil
}

func writeXAIOAuthState(path string, state xaiOAuthState) error {
	if path == "" {
		return errors.New("xAI OAuth state path is empty")
	}
	state.Version = xaiOAuthStateVersion
	state.AuthMode = "oauth_device_code"
	if state.TokenType == "" {
		state.TokenType = "Bearer"
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("xAI OAuth state %q must be a regular file, not a symlink", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	temp, err := os.CreateTemp(dir, ".xai-oauth-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	keep := false
	defer func() {
		_ = temp.Close()
		if !keep {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return err
	}
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	keep = true
	return nil
}

func (m *xaiOAuthManager) loadLocked() error {
	state, err := readXAIOAuthState(m.statePath)
	if err != nil {
		return err
	}
	m.state = state
	return nil
}

func (m *xaiOAuthManager) accessToken(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.AccessToken == "" {
		if err := m.loadLocked(); err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("xAI OAuth is not connected; run anthropic-proxy xai-login")
			}
			return "", err
		}
	}
	expiresAt := xaiStateExpiry(m.state)
	if !expiresAt.IsZero() && !time.Now().Add(xaiOAuthRefreshSkew).Before(expiresAt) {
		if err := m.refreshLocked(ctx); err != nil {
			return "", err
		}
	}
	if strings.TrimSpace(m.state.AccessToken) == "" {
		return "", fmt.Errorf("xAI OAuth has no usable access token; run anthropic-proxy xai-login")
	}
	return m.state.AccessToken, nil
}

func (m *xaiOAuthManager) refreshAfterUnauthorized(ctx context.Context, failedToken string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if diskState, err := readXAIOAuthState(m.statePath); err == nil && diskState.AccessToken != "" && diskState.AccessToken != failedToken {
		m.state = diskState
		return m.state.AccessToken, nil
	}
	if m.state.AccessToken == "" {
		if err := m.loadLocked(); err != nil {
			return "", err
		}
	}
	if m.state.AccessToken != "" && m.state.AccessToken != failedToken {
		return m.state.AccessToken, nil
	}
	if err := m.refreshLocked(ctx); err != nil {
		return "", err
	}
	return m.state.AccessToken, nil
}

func (m *xaiOAuthManager) discover(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, xaiOAuthDiscoveryURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("xAI OIDC discovery: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("xAI OIDC discovery returned HTTP %d", resp.StatusCode)
	}
	var discovery xaiOIDCDiscovery
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&discovery); err != nil {
		return "", fmt.Errorf("decode xAI OIDC discovery: %w", err)
	}
	if err := validateXAIOrigin(discovery.TokenEndpoint, "token endpoint"); err != nil {
		return "", err
	}
	return discovery.TokenEndpoint, nil
}

func (m *xaiOAuthManager) postForm(ctx context.Context, endpoint string, values url.Values) (*http.Response, error) {
	if err := validateXAIOrigin(endpoint, "OAuth endpoint"); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	return m.client.Do(req)
}

func decodeXAITokenResponse(resp *http.Response) (xaiTokenResponse, []byte, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return xaiTokenResponse{}, nil, err
	}
	var token xaiTokenResponse
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &token); err != nil {
			return xaiTokenResponse{}, body, fmt.Errorf("decode xAI OAuth response: %w", err)
		}
	}
	return token, body, nil
}

func (m *xaiOAuthManager) refreshLocked(ctx context.Context) error {
	if strings.TrimSpace(m.state.RefreshToken) == "" {
		return errors.New("xAI OAuth refresh token is missing; run anthropic-proxy xai-login")
	}
	endpoint := m.state.TokenEndpoint
	if endpoint == "" {
		var err error
		endpoint, err = m.discover(ctx)
		if err != nil {
			return err
		}
	}
	resp, err := m.postForm(ctx, endpoint, url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {xaiOAuthClientID},
		"refresh_token": {m.state.RefreshToken},
	})
	if err != nil {
		return fmt.Errorf("xAI OAuth refresh: %w", err)
	}
	token, body, err := decodeXAITokenResponse(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		detail := firstNonEmpty(token.Description, token.Error, trimForLog(string(body), 240))
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			m.state.AccessToken = ""
			m.state.RefreshToken = ""
			m.state.LastError = fmt.Sprintf("refresh HTTP %d: %s", resp.StatusCode, detail)
			_ = writeXAIOAuthState(m.statePath, m.state)
		}
		return fmt.Errorf("xAI OAuth refresh returned HTTP %d: %s", resp.StatusCode, detail)
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		return errors.New("xAI OAuth refresh response omitted access_token")
	}
	m.state.AccessToken = token.AccessToken
	if token.RefreshToken != "" {
		m.state.RefreshToken = token.RefreshToken
	}
	if token.IDToken != "" {
		m.state.IDToken = token.IDToken
	}
	m.state.TokenType = firstNonEmpty(token.TokenType, "Bearer")
	m.state.TokenEndpoint = endpoint
	m.state.LastRefresh = time.Now().UTC()
	m.state.LastError = ""
	if token.ExpiresIn > 0 {
		m.state.ExpiresAt = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).UTC()
	} else {
		m.state.ExpiresAt = jwtExpiry(token.AccessToken)
	}
	return writeXAIOAuthState(m.statePath, m.state)
}

func runXAIOAuthLogin(ctx context.Context, cfg config, output io.Writer, openBrowser bool) error {
	var providerName string
	var provider providerConfig
	for name, candidate := range cfg.Providers {
		if candidate.AuthMode == "xai-oauth" {
			providerName, provider = name, candidate
			break
		}
	}
	if providerName == "" {
		return errors.New("proxy config has no xai-oauth provider")
	}
	manager := newXAIOAuthManager(provider.AuthStatePath, &http.Client{Timeout: 30 * time.Second})
	tokenEndpoint, err := manager.discover(ctx)
	if err != nil {
		return err
	}
	resp, err := manager.postForm(ctx, xaiOAuthDeviceCodeURL, url.Values{
		"client_id": {xaiOAuthClientID},
		"scope":     {xaiOAuthScope},
	})
	if err != nil {
		return fmt.Errorf("xAI device authorization: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("xAI device authorization returned HTTP %d: %s", resp.StatusCode, trimForLog(string(body), 240))
	}
	var device xaiDeviceCodeResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&device); err != nil {
		return fmt.Errorf("decode xAI device authorization: %w", err)
	}
	verificationURL := firstNonEmpty(device.VerificationURIComplete, device.VerificationURI)
	if device.DeviceCode == "" || device.UserCode == "" || verificationURL == "" || device.ExpiresIn <= 0 {
		return errors.New("xAI device authorization response is incomplete")
	}
	fmt.Fprintf(output, "Open: %s\nCode: %s\n", verificationURL, device.UserCode)
	if openBrowser {
		if openPath, err := exec.LookPath("open"); err == nil {
			if err := exec.Command(openPath, verificationURL).Start(); err == nil {
				fmt.Fprintln(output, "Browser opened. Waiting for approval...")
			}
		}
	}
	interval := time.Duration(device.Interval) * time.Second
	if interval < time.Second {
		interval = time.Second
	}
	deadline := time.Now().Add(time.Duration(device.ExpiresIn) * time.Second)
	for time.Now().Before(deadline) {
		resp, err := manager.postForm(ctx, tokenEndpoint, url.Values{
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
			"client_id":   {xaiOAuthClientID},
			"device_code": {device.DeviceCode},
		})
		if err != nil {
			return fmt.Errorf("xAI device token polling: %w", err)
		}
		token, body, err := decodeXAITokenResponse(resp)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusOK {
			if token.AccessToken == "" || token.RefreshToken == "" {
				return errors.New("xAI device token response omitted required tokens")
			}
			state := xaiOAuthState{
				Version:       xaiOAuthStateVersion,
				AuthMode:      "oauth_device_code",
				AccessToken:   token.AccessToken,
				RefreshToken:  token.RefreshToken,
				IDToken:       token.IDToken,
				TokenType:     firstNonEmpty(token.TokenType, "Bearer"),
				TokenEndpoint: tokenEndpoint,
				LastRefresh:   time.Now().UTC(),
			}
			if token.ExpiresIn > 0 {
				state.ExpiresAt = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).UTC()
			} else {
				state.ExpiresAt = jwtExpiry(token.AccessToken)
			}
			if err := writeXAIOAuthState(provider.AuthStatePath, state); err != nil {
				return err
			}
			fmt.Fprintf(output, "Connected %s. Credentials: %s\n", providerName, provider.AuthStatePath)
			return nil
		}
		switch token.Error {
		case "authorization_pending":
		case "slow_down":
			interval += time.Second
		default:
			return fmt.Errorf("xAI device token polling returned HTTP %d: %s", resp.StatusCode, firstNonEmpty(token.Description, token.Error, trimForLog(string(body), 240)))
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("xAI device authorization timed out")
}

func printXAIOAuthStatus(cfg config, output io.Writer) error {
	for name, provider := range cfg.Providers {
		if provider.AuthMode != "xai-oauth" {
			continue
		}
		state, err := readXAIOAuthState(provider.AuthStatePath)
		if err != nil {
			return err
		}
		expiresAt := xaiStateExpiry(state)
		fmt.Fprintf(output, "provider=%s connected=%t expiresAt=%s lastRefresh=%s\n", name, state.AccessToken != "" && state.RefreshToken != "", expiresAt.UTC().Format(time.RFC3339), state.LastRefresh.UTC().Format(time.RFC3339))
		return nil
	}
	return errors.New("proxy config has no xai-oauth provider")
}

func (s *proxyServer) applyProviderHeaders(providerName string, headers http.Header) error {
	return s.applyProviderHeadersContext(context.Background(), providerName, headers)
}

type providerCredentialError struct{ cause error }

func (e *providerCredentialError) Error() string { return e.cause.Error() }
func (e *providerCredentialError) Unwrap() error { return e.cause }

func (s *proxyServer) applyProviderHeadersContext(ctx context.Context, providerName string, headers http.Header) error {
	provider := s.cfg.Providers[providerName]
	for key, value := range provider.Headers {
		headers.Set(key, value)
	}
	var token string
	var configured bool
	var err error
	if provider.AuthMode == "xai-oauth" {
		configured = true
		manager := s.xaiOAuth[providerName]
		if manager == nil {
			err = errors.New("xAI OAuth manager is unavailable")
		} else {
			token, err = manager.accessToken(ctx)
		}
	} else {
		token, configured, err = providerAuthToken(providerName, provider)
	}
	if err != nil {
		return &providerCredentialError{cause: fmt.Errorf("provider %q auth unavailable", providerName)}
	}
	if token == "" {
		if configured {
			return &providerCredentialError{cause: fmt.Errorf("provider %q auth token is empty", providerName)}
		}
		return nil
	}
	// Requests arrive from Claude Code with its own account credential. Once a
	// provider has an explicit auth source, strip all client auth forms before
	// setting that provider's token; otherwise Anthropic keys leak to gateways.
	headers.Del("Authorization")
	headers.Del("x-api-key")
	headers.Del("api-key")
	authHeader := firstNonEmpty(provider.AuthHeader, "Authorization")
	headers.Set(authHeader, provider.AuthPrefix+token)
	return nil
}

func ensureCommaSeparatedHeaderValue(headers http.Header, key, value string) {
	for _, existing := range strings.Split(headers.Get(key), ",") {
		if strings.TrimSpace(existing) == value {
			return
		}
	}
	if current := strings.TrimSpace(headers.Get(key)); current != "" {
		headers.Set(key, current+","+value)
		return
	}
	headers.Set(key, value)
}

// authTokenCache avoids resolving credentials on every attempt: the configured
// commands fork a login shell or hit the Keychain (~20-200ms) and their output
// is static API keys. The TTL is a mere backstop — a rotated key evicts its
// entry eagerly via the upstream's 401/403 (see doWithFallbacks), and a proxy
// restart clears the cache entirely.
var authTokenCache sync.Map // providerName -> cachedAuthToken

type cachedAuthToken struct {
	token   string
	fetched time.Time
}

const authTokenCacheTTL = 24 * time.Hour

func providerAuthToken(providerName string, provider providerConfig) (string, bool, error) {
	if provider.Variant != "" && provider.AuthTokenEnv != "" {
		return strings.TrimSpace(os.Getenv(provider.AuthTokenEnv)), true, nil
	}
	if provider.AuthTokenEnv == "" && len(provider.AuthTokenCommand) == 0 {
		return "", false, nil
	}
	if raw, ok := authTokenCache.Load(providerName); ok {
		if cached, ok := raw.(cachedAuthToken); ok && time.Since(cached.fetched) < authTokenCacheTTL {
			return cached.token, true, nil
		}
	}
	token, configured, err := resolveProviderAuthToken(provider)
	if err == nil && token != "" {
		authTokenCache.Store(providerName, cachedAuthToken{token: token, fetched: time.Now()})
	}
	return token, configured, err
}

func resolveProviderAuthToken(provider providerConfig) (string, bool, error) {
	if provider.AuthTokenEnv != "" {
		if token := strings.TrimSpace(os.Getenv(provider.AuthTokenEnv)); token != "" {
			return token, true, nil
		}
	}
	if len(provider.AuthTokenCommand) > 0 {
		if provider.AuthTokenCommand[0] == "" {
			return "", true, errors.New("authTokenCommand has empty executable")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, provider.AuthTokenCommand[0], provider.AuthTokenCommand[1:]...)
		var out limitedBuffer
		out.limit = 64 << 10
		cmd.Stdout = &out
		cmd.Stderr = io.Discard
		err := cmd.Run()
		if ctx.Err() != nil {
			return "", true, ctx.Err()
		}
		if err != nil {
			return "", true, fmt.Errorf("authTokenCommand failed: %w", err)
		}
		return strings.TrimSpace(string(out.Bytes())), true, nil
	}
	return "", false, nil
}

func shouldFallbackStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return true
	default:
		return status >= 500
	}
}

// fallbackCandidateStatus reports whether a failure status is worth checking
// against the fallback body needles. 400/404 are normally client errors, but
// gateways use them for "model unavailable" (OpenCode Go) and "model not
// found" (Ollama), so they are let through and the body check decides.
func fallbackCandidateStatus(status int) bool {
	return shouldFallbackStatus(status) || status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusUnprocessableEntity
}

func shouldFallbackBody(body []byte) bool {
	text := strings.ToLower(string(body))
	if strings.TrimSpace(text) == "" {
		return true
	}
	needles := []string{
		"quota",
		"usage",
		"rate limit",
		"rate_limit",
		"limit exceeded",
		"insufficient",
		"billing",
		"credit",
		"exhausted",
		"unavailable",
		"timeout",
		"deadline",
		"overloaded",
		"not found",
		"not_found",
		"not supported",
		"supported api model",
		"unknown model",
		"does not exist",
		"invalid model",
		"no such model",
	}
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

// clientErrorBody reports whether an error body affirmatively indicates the
// request payload itself is at fault — retrying it against another provider
// would fail identically, so the fallback chain should stop (veto). Anything
// unrecognized is treated as a provider-side condition instead (see the
// opaque-4xx branch in doWithFallbacks): gateways like OpenCode Go answer
// model-unavailable/overload with a bare {"model":"..."} 400.
func clientErrorBody(body []byte) bool {
	text := strings.ToLower(string(body))
	if strings.TrimSpace(text) == "" {
		return false
	}
	if contextOverflowBody(body) {
		return true
	}
	needles := []string{
		"max_tokens",
		"invalid_request",
		"invalid request",
		"malformed",
		"validation",
		"is invalid",
	}
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func contextOverflowBody(body []byte) bool {
	text := strings.ToLower(string(body))
	for _, needle := range []string{
		"prompt is too long",
		"input is too long for requested model",
		"context length",
		"context window",
		"context limit",
		"too many tokens",
		"maximum context",
		"input length and `max_tokens` exceed context limit",
	} {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

// normalizeContextOverflowResponse maps vendor-specific 400 context-limit
// errors to the Anthropic 413 request_too_large contract. Claude Code treats
// this status as prompt_too_long and stops generic workflow retries, while the
// exact upstream message remains available for compaction diagnostics.
func normalizeContextOverflowResponse(resp *http.Response, body []byte) ([]byte, bool) {
	if resp == nil || (resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusRequestEntityTooLarge) || !contextOverflowBody(body) {
		return body, false
	}
	var original struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(body, &original)
	message := strings.TrimSpace(original.Error.Message)
	if message == "" {
		message = strings.TrimSpace(string(body))
	}
	normalized := map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    "request_too_large",
			"message": message,
		},
	}
	if original.RequestID != "" {
		normalized["request_id"] = original.RequestID
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return body, false
	}
	resp.StatusCode = http.StatusRequestEntityTooLarge
	resp.Status = fmt.Sprintf("%d %s", http.StatusRequestEntityTooLarge, http.StatusText(http.StatusRequestEntityTooLarge))
	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Content-Length", strconv.Itoa(len(encoded)))
	resp.Header.Set("X-Anthropic-Proxy-Error-Class", "prompt_too_long")
	resp.ContentLength = int64(len(encoded))
	return encoded, true
}

func recognizedClientPayloadError(status int, body []byte) bool {
	return (status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge) && clientErrorBody(body)
}

// providerSpecificRequestIncompatibility identifies payload options or
// capabilities rejected by one route but potentially accepted by the next.
// These failures may open only that provider+model circuit; they must never
// veto the whole chain or poison provider-wide health.
func providerSpecificRequestIncompatibility(status int, body []byte) bool {
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return false
	}
	text := strings.ToLower(string(body))
	if strings.Contains(text, "deferred custom tools are only supported") && strings.Contains(text, "implement deferral") {
		return true
	}
	capability := strings.Contains(text, "image input") || strings.Contains(text, "vision") ||
		strings.Contains(text, "tool choice") || strings.Contains(text, "structured output")
	parameter := strings.Contains(text, "temperature") || strings.Contains(text, "parameter") ||
		strings.Contains(text, "field") || strings.Contains(text, "property")
	rejection := strings.Contains(text, "not support") || strings.Contains(text, "unsupported") ||
		strings.Contains(text, "deprecated") || strings.Contains(text, "not allowed") ||
		strings.Contains(text, "unrecognized") || strings.Contains(text, "unknown")
	return rejection && (capability || parameter)
}

func thinkingSignatureMismatchBody(body []byte) bool {
	text := strings.ToLower(string(body))
	return strings.Contains(text, "thinking") && strings.Contains(text, "signature") &&
		(strings.Contains(text, "invalid") || strings.Contains(text, "mismatch") || strings.Contains(text, "does not match"))
}

func classifyFallbackBody(body []byte) string {
	text := strings.ToLower(string(body))
	for _, needle := range []string{
		"quota",
		"rate limit",
		"rate_limit",
		"limit exceeded",
		"billing",
		"credit",
		"exhausted",
	} {
		if strings.Contains(text, needle) {
			return "quota_or_rate_limit"
		}
	}
	return "availability"
}

func providerCooldown(headers http.Header, body []byte) time.Duration {
	if raw := strings.TrimSpace(headers.Get("Retry-After")); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
		if resetAt, err := http.ParseTime(raw); err == nil && resetAt.After(time.Now()) {
			return time.Until(resetAt)
		}
	}
	if resetAt, ok := resetTimestampFromBody(string(body)); ok && resetAt.After(time.Now()) {
		return time.Until(resetAt)
	}
	text := strings.ToLower(string(body))
	if strings.Contains(text, "usage limit") || strings.Contains(text, "rate limit") || strings.Contains(text, "rate_limit") || strings.Contains(text, "quota") || strings.Contains(text, "exhausted") || strings.Contains(text, "insufficient") {
		return 5 * time.Minute
	}
	return time.Minute
}

func resetTimestampFromBody(body string) (time.Time, bool) {
	const width = len("2006-01-02 15:04:05")
	for start := 0; start+width <= len(body); start++ {
		candidate := body[start : start+width]
		if candidate[4] != '-' || candidate[7] != '-' ||
			(candidate[10] != ' ' && candidate[10] != 'T') ||
			candidate[13] != ':' || candidate[16] != ':' {
			continue
		}
		candidate = candidate[:10] + " " + candidate[11:]
		if parsed, err := time.ParseInLocation("2006-01-02 15:04:05", candidate, time.Local); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func trimForLog(value string, max int) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}

// skippedHeaders are end-to-end unsafe: framing and hop-by-hop headers must not
// be forwarded in either direction.
var skippedHeaders = map[string]bool{
	"Host":                true,
	"Content-Length":      true,
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		if skippedHeaders[http.CanonicalHeaderKey(key)] {
			continue
		}
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// streamCopy pumps the (possibly filtered) upstream body to the client, flushing
// after every read so SSE events arrive promptly. It returns the first read or
// write error encountered, nil on clean EOF.
func streamCopy(w http.ResponseWriter, body io.Reader, stats *streamStats) error {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if stats != nil {
				stats.Bytes.Add(int64(n))
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// streamIdleTimeout returns the per-provider idle watchdog for SSE streams:
// if no bytes arrive within the window the upstream body is closed so the
// request fails loudly instead of hanging until the client's own timeout.
// Defaults to 5 minutes; set streamIdleTimeoutMS to -1 to disable.
func streamIdleTimeout(provider providerConfig) time.Duration {
	if provider.StreamIdleTimeoutMS < 0 {
		return 0
	}
	if provider.StreamIdleTimeoutMS == 0 {
		return 5 * time.Minute
	}
	return time.Duration(provider.StreamIdleTimeoutMS) * time.Millisecond
}

type streamStats struct {
	ReportedBackend            atomic.Value // Explicit upstream-reported identifier, never inferred.
	InputEstimated             atomic.Bool
	CacheReadTokens            atomic.Int64
	CacheWriteTokens           atomic.Int64
	MaximumGapNS               atomic.Int64
	FirstContentNS             atomic.Int64
	Bytes                      atomic.Int64
	Events                     atomic.Int64
	InputTokens                atomic.Int64
	OutputTokens               atomic.Int64
	LastEventNS                atomic.Int64
	StreamStalled              atomic.Bool
	ProtocolError              atomic.Bool
	ClientDisconnected         atomic.Bool
	ClientDisconnectAfterStall atomic.Bool
	// MissingStop is set when an SSE stream reached its terminal event (or
	// EOF) without ever carrying a message_delta with a stop_reason. The
	// filter converts such streams into a client-visible error, but this
	// flag makes the failure visible in request logs too.
	MissingStop atomic.Bool
}

func recordReportedBackend(stats *streamStats, payload map[string]any) {
	if stats == nil {
		return
	}
	name, _ := payload["provider"].(string)
	if len(name) == 0 || len(name) > 80 {
		return
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(" ._-/:()", r)) {
			return
		}
	}
	stats.ReportedBackend.Store(name)
}

func (s *streamStats) noteEvent() {
	if s != nil {
		now := time.Now().UnixNano()
		if before := s.LastEventNS.Swap(now); before > 0 {
			gap := now - before
			for old := s.MaximumGapNS.Load(); gap > old; old = s.MaximumGapNS.Load() {
				if s.MaximumGapNS.CompareAndSwap(old, gap) {
					break
				}
			}
		}
	}
}

func (s *streamStats) eventStalledSince(streamStarted time.Time, timeout time.Duration) bool {
	if s == nil || timeout <= 0 {
		return false
	}
	lastEvent := streamStarted
	if raw := s.LastEventNS.Load(); raw > 0 {
		lastEvent = time.Unix(0, raw)
	}
	return time.Since(lastEvent) >= timeout
}

// idleTimeoutReadCloser closes the underlying body if a single Read blocks for
// longer than timeout, which unblocks the pending Read with an error.
type idleTimeoutReadCloser struct {
	source  io.ReadCloser
	timeout time.Duration
	mu      sync.Mutex
	timer   *time.Timer
}

func (r *idleTimeoutReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.timer == nil {
		r.timer = time.AfterFunc(r.timeout, func() { _ = r.source.Close() })
	} else {
		r.timer.Reset(r.timeout)
	}
	r.mu.Unlock()
	return r.source.Read(p)
}

func (r *idleTimeoutReadCloser) Close() error {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	return r.source.Close()
}

func foldSystemIntoMessages(payload map[string]any) {
	systemParts := make([]string, 0, 2)
	if system, exists := payload["system"]; exists {
		delete(payload, "system")
		if text := strings.TrimSpace(systemContentText(system)); text != "" {
			systemParts = append(systemParts, text)
		}
	}
	messages, _ := payload["messages"].([]any)
	if len(messages) > 0 {
		kept := make([]any, 0, len(messages))
		for _, item := range messages {
			message, ok := item.(map[string]any)
			if !ok {
				kept = append(kept, item)
				continue
			}
			if role, _ := message["role"].(string); role == "system" {
				if text := strings.TrimSpace(systemContentText(message["content"])); text != "" {
					systemParts = append(systemParts, text)
				}
				continue
			}
			kept = append(kept, item)
		}
		messages = kept
		payload["messages"] = messages
	}
	systemText := strings.TrimSpace(strings.Join(systemParts, "\n\n"))
	if systemText == "" {
		return
	}
	systemBlock := map[string]any{
		"type": "text",
		"text": "System instructions:\n" + systemText,
	}
	if len(messages) == 0 {
		payload["messages"] = []any{map[string]any{
			"role":    "user",
			"content": []any{systemBlock},
		}}
		return
	}
	for _, item := range messages {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if role, _ := message["role"].(string); role != "user" {
			continue
		}
		message["content"] = prependContentBlock(systemBlock, message["content"])
		return
	}
	payload["messages"] = append([]any{map[string]any{
		"role":    "user",
		"content": []any{systemBlock},
	}}, messages...)
}

func systemContentText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := systemContentText(item); strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n\n")
	case map[string]any:
		if text, ok := typed["text"].(string); ok {
			return text
		}
		raw, _ := json.Marshal(typed)
		return string(raw)
	default:
		return fmt.Sprint(typed)
	}
}

func prependContentBlock(block map[string]any, content any) any {
	switch typed := content.(type) {
	case string:
		return []any{block, map[string]any{"type": "text", "text": typed}}
	case []any:
		next := make([]any, 0, len(typed)+1)
		next = append(next, block)
		next = append(next, typed...)
		return next
	case nil:
		return []any{block}
	default:
		raw, _ := json.Marshal(typed)
		return []any{block, map[string]any{"type": "text", "text": string(raw)}}
	}
}

func joinURLPath(basePath, requestPath string) string {
	if basePath == "" || basePath == "/" {
		return requestPath
	}
	return strings.TrimRight(basePath, "/") + "/" + strings.TrimLeft(requestPath, "/")
}

type bufferedReadCloser struct {
	source io.ReadCloser
	reader *strings.Reader
}

func newJSONFilterReadCloser(source io.ReadCloser, dropTypes map[string]bool, responseAlias string) io.ReadCloser {
	body, err := io.ReadAll(source)
	if err != nil {
		return &bufferedReadCloser{source: source, reader: strings.NewReader(string(body))}
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) == nil {
		setResponseModelAlias(payload, responseAlias)
		if content, ok := payload["content"].([]any); ok {
			payload["content"] = filterContentBlocks(content, dropTypes)
		}
		if normalized, err := json.Marshal(payload); err == nil {
			body = normalized
		}
	}
	return &bufferedReadCloser{
		source: source,
		reader: strings.NewReader(string(body)),
	}
}

func (r *bufferedReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *bufferedReadCloser) Close() error {
	return r.source.Close()
}

func responseDropSet(provider providerConfig) map[string]bool {
	if len(provider.DropResponseContentTypes) == 0 {
		return nil
	}
	drop := make(map[string]bool, len(provider.DropResponseContentTypes))
	for _, blockType := range provider.DropResponseContentTypes {
		if blockType != "" {
			drop[blockType] = true
		}
	}
	return drop
}

type sseFilterReadCloser struct {
	source io.ReadCloser
	reader *io.PipeReader
}

type sseFilterState struct {
	drop  map[int]bool
	remap map[int]int
	next  int
}

type streamEventWatchdog struct {
	source  io.Closer
	timeout time.Duration
	stats   *streamStats
	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
}

func newStreamEventWatchdog(source io.Closer, timeout time.Duration, stats *streamStats) *streamEventWatchdog {
	if timeout <= 0 {
		return nil
	}
	watchdog := &streamEventWatchdog{source: source, timeout: timeout, stats: stats}
	watchdog.timer = time.AfterFunc(timeout, watchdog.expire)
	return watchdog
}

func (w *streamEventWatchdog) expire() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	if w.stats != nil {
		w.stats.StreamStalled.Store(true)
	}
	w.stopped = true
	w.mu.Unlock()
	_ = w.source.Close()
}

func (w *streamEventWatchdog) activity() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if !w.stopped {
		w.timer.Reset(w.timeout)
	}
	w.mu.Unlock()
}

func (w *streamEventWatchdog) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.stopped = true
	if w.timer != nil {
		w.timer.Stop()
	}
	w.mu.Unlock()
}

func newSSEFilterReadCloser(source io.ReadCloser, dropTypes map[string]bool, inputTokenHint int, responseAlias string, eventStallTimeout time.Duration, stats *streamStats) io.ReadCloser {
	reader, writer := io.Pipe()
	filter := &sseFilterReadCloser{source: source, reader: reader}
	go func() {
		watchdog := newStreamEventWatchdog(source, eventStallTimeout, stats)
		defer watchdog.stop()
		err := filterSSE(source, writer, dropTypes, inputTokenHint, responseAlias, stats, watchdog)
		_ = source.Close()
		_ = writer.CloseWithError(err)
	}()
	return filter
}

func (f *sseFilterReadCloser) Read(p []byte) (int, error) {
	return f.reader.Read(p)
}

func (f *sseFilterReadCloser) Close() error {
	_ = f.source.Close()
	return f.reader.Close()
}

func filterSSE(source io.Reader, writer io.Writer, dropTypes map[string]bool, inputTokenHint int, responseAlias string, stats *streamStats, watchdog *streamEventWatchdog) error {
	reader := bufio.NewReader(source)
	state := &sseFilterState{drop: map[int]bool{}, remap: map[int]int{}}
	var lines []string
	frameBytes := 0
	sawMessageStart := false
	sawTerminal := false   // message_stop or upstream error event
	sawStopReason := false // a message_delta with non-null stop_reason
	sawMessageStop := false
	flush := func() error {
		frameBytes = 0
		// OpenRouter appends an OpenAI sentinel after a complete Anthropic
		// message. Ignore only that trailing marker; premature DONE remains
		// a protocol error and cannot turn a truncated stream into success.
		terminalFrame := strings.ReplaceAll(strings.TrimSpace(strings.Join(lines, "")), "\r\n", "\n")
		if sawMessageStop && sawStopReason && (terminalFrame == "data: [DONE]" || terminalFrame == "event: data\ndata: [DONE]") {
			lines = nil
			return nil
		}
		validatedType, validationErr := validateAnthropicSSEEvent(lines, sawMessageStart, sawTerminal)
		if validationErr != nil {
			if stats != nil {
				stats.ProtocolError.Store(true)
				stats.Events.Add(1)
			}
			_, _ = io.WriteString(writer, formatSSEEvent("error", `{"type":"error","error":{"type":"api_error","message":`+strconv.Quote("malformed upstream Anthropic SSE: "+validationErr.Error())+`}}`))
			return fmt.Errorf("malformed Anthropic SSE: %w", validationErr)
		}
		out, eventType, stopReason, _, outputTokens := filterSSEEvent(lines, state, dropTypes, inputTokenHint, responseAlias, stats)
		lines = nil
		if eventType == "" {
			eventType = validatedType
		}
		if stats != nil {
			if outputTokens > 0 {
				stats.OutputTokens.Store(outputTokens)
			}
		}
		switch eventType {
		case "message_start":
			sawMessageStart = true
		case "message_delta":
			if stopReason != "" {
				sawStopReason = true
			}
		case "message_stop":
			sawTerminal = true
			sawMessageStop = true
			if sawMessageStart && !sawStopReason {
				// A well-formed Anthropic stream always ends with
				// message_delta(stop_reason) followed by message_stop. Some
				// upstreams (observed via Vercel AI Gateway) occasionally emit
				// a bare message_stop after a handful of deltas; clients then
				// accept a truncated one-word reply as a finished message.
				// Replace message_stop with an error so the client retries.
				if stats != nil {
					stats.MissingStop.Store(true)
				}
				out = formatSSEEvent("error", `{"type":"error","error":{"type":"api_error","message":`+strconv.Quote("upstream stream ended without stop_reason (truncated response)")+`}}`)
			}
		case "error":
			sawTerminal = true
		}
		if out == "" {
			return nil
		}
		if eventType != "" {
			if stats != nil {
				stats.Events.Add(1)
				stats.noteEvent()
			}
			watchdog.activity()
		}
		_, err := io.WriteString(writer, out)
		return err
	}
	var streamErr error
	for {
		line, err := readBoundedSSELine(reader, maxSSEFrameBytes-frameBytes)
		frameBytes += len(line)
		if len(line) > 0 {
			if strings.TrimRight(line, "\r\n") == "" {
				if len(lines) == 0 {
					// Standalone blank line (keep-alive): forward verbatim so the
					// client sees liveness during long generation pauses.
					if _, writeErr := io.WriteString(writer, line); writeErr != nil {
						return writeErr
					}
				} else if flushErr := flush(); flushErr != nil {
					return flushErr
				}
			} else {
				lines = append(lines, line)
			}
		}
		if err != nil {
			if len(lines) > 0 {
				if flushErr := flush(); flushErr != nil {
					return flushErr
				}
			}
			if err != io.EOF {
				streamErr = err
			}
			break
		}
	}
	if sawMessageStart && !sawTerminal {
		// The stream ended (cleanly or not) before message_stop. Fail loudly with
		// a well-formed error event instead of leaving the client hanging on a
		// bare EOF with a pending tool_use block.
		msg := "upstream stream ended before message_stop"
		if streamErr != nil {
			msg = "upstream stream interrupted: " + streamErr.Error()
		}
		if stats != nil {
			stats.MissingStop.Store(true)
		}
		out := formatSSEEvent("error", `{"type":"error","error":{"type":"api_error","message":`+strconv.Quote(msg)+`}}`)
		if stats != nil {
			stats.Events.Add(1)
		}
		if _, err := io.WriteString(writer, out); err != nil {
			return err
		}
	}
	return streamErr
}

var anthropicSSEEventTypes = map[string]bool{
	"message_start":       true,
	"content_block_start": true,
	"content_block_delta": true,
	"content_block_stop":  true,
	"message_delta":       true,
	"message_stop":        true,
	"ping":                true,
	"error":               true,
}

// validateAnthropicSSEEvent rejects wire data that Claude Code cannot safely
// interpret. Comment-only frames remain valid keep-alives, but they do not
// reset the event watchdog.
func validateAnthropicSSEEvent(lines []string, sawMessageStart, sawTerminal bool) (string, error) {
	eventName := ""
	dataLines := []string{}
	for _, raw := range lines {
		line := strings.TrimRight(raw, "\r\n")
		switch {
		case strings.HasPrefix(line, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		case line == "" || strings.HasPrefix(line, ":"):
		default:
			return "", fmt.Errorf("unsupported SSE field %q", trimForLog(line, 80))
		}
	}
	if len(dataLines) == 0 {
		if eventName != "" {
			return "", fmt.Errorf("event %q has no data", eventName)
		}
		return "", nil
	}
	data := strings.Join(dataLines, "\n")
	if data == "[DONE]" {
		return "", errors.New("OpenAI [DONE] marker on Anthropic stream")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return "", fmt.Errorf("invalid JSON data: %w", err)
	}
	payloadType, _ := payload["type"].(string)
	if payloadType == "" {
		return "", errors.New("event payload has no type")
	}
	if eventName == "" {
		eventName = payloadType
	} else if eventName != payloadType {
		return "", fmt.Errorf("event type %q disagrees with payload type %q", eventName, payloadType)
	}
	if !anthropicSSEEventTypes[eventName] {
		return "", fmt.Errorf("unknown event type %q", eventName)
	}
	if sawTerminal && eventName != "ping" {
		return "", fmt.Errorf("event %q follows terminal event", eventName)
	}
	switch eventName {
	case "message_start":
		if sawMessageStart {
			return "", errors.New("duplicate message_start")
		}
	case "ping", "error":
	default:
		if !sawMessageStart {
			return "", fmt.Errorf("event %q precedes message_start", eventName)
		}
	}
	return eventName, nil
}

// filterSSEEvent returns the (possibly transformed) event text, the event
// type (taken from the event: line or the payload's "type" field), and, for
// message_delta events, the stop_reason if one is present.
func filterSSEEvent(lines []string, state *sseFilterState, dropTypes map[string]bool, inputTokenHint int, responseAlias string, telemetry ...*streamStats) (string, string, string, int64, int64) {
	if len(lines) == 0 {
		return "", "", "", 0, 0
	}
	eventName := ""
	dataLines := []string{}
	for _, raw := range lines {
		line := strings.TrimRight(raw, "\r\n")
		switch {
		case strings.HasPrefix(line, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if len(dataLines) == 0 {
		return strings.Join(lines, "") + "\n", eventName, "", 0, 0
	}
	data := strings.Join(dataLines, "\n")
	if data == "[DONE]" {
		return formatSSEEvent(eventName, data), eventName, "", 0, 0
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return strings.Join(lines, "") + "\n", eventName, "", 0, 0
	}
	if eventName == "" {
		eventName, _ = payload["type"].(string)
	}
	stopReason := ""
	if eventName == "message_delta" {
		if delta, ok := payload["delta"].(map[string]any); ok {
			stopReason, _ = delta["stop_reason"].(string)
		}
	}
	if len(telemetry) > 0 && telemetry[0] != nil {
		stats := telemetry[0]
		recordReportedBackend(stats, payload)
		usage, _ := payload["usage"].(map[string]any)
		if message, ok := payload["message"].(map[string]any); ok {
			if nested, ok := message["usage"].(map[string]any); ok {
				usage = nested
			}
		}
		if value, ok := usage["cache_read_input_tokens"]; ok {
			stats.CacheReadTokens.Store(metricInt64(value))
		}
		if value, ok := usage["cache_creation_input_tokens"]; ok {
			stats.CacheWriteTokens.Store(metricInt64(value))
		}
		if value, ok := usage["input_tokens"]; ok {
			stats.InputTokens.Store(metricInt64(value))
			stats.InputEstimated.Store(false)
		} else if eventName == "message_start" && inputTokenHint > 0 {
			stats.InputTokens.Store(int64(inputTokenHint))
			stats.InputEstimated.Store(true)
		}
		delta, _ := payload["delta"].(map[string]any)
		block, _ := payload["content_block"].(map[string]any)
		useful := eventName == "content_block_delta" && (delta["type"] == "text_delta" || delta["type"] == "input_json_delta") || eventName == "content_block_start" && block["type"] == "tool_use"
		if useful {
			stats.FirstContentNS.CompareAndSwap(0, time.Now().UnixNano())
		}
	}
	addMessageStartUsageHint(payload, inputTokenHint)
	inputTokens, outputTokens := extractAnthropicUsage(payload)
	setResponseModelAlias(payload, responseAlias)
	if shouldDropSSEPayload(payload, state, dropTypes) {
		return "", eventName, stopReason, inputTokens, outputTokens
	}
	normalized, err := json.Marshal(payload)
	if err != nil {
		return strings.Join(lines, "") + "\n", eventName, stopReason, inputTokens, outputTokens
	}
	return formatSSEEvent(eventName, string(normalized)), eventName, stopReason, inputTokens, outputTokens
}

func extractAnthropicUsage(payload map[string]any) (int64, int64) {
	usage, _ := payload["usage"].(map[string]any)
	if message, ok := payload["message"].(map[string]any); ok {
		if nested, ok := message["usage"].(map[string]any); ok {
			usage = nested
		}
	}
	if usage == nil {
		return 0, 0
	}
	return metricInt64(usage["input_tokens"]), metricInt64(usage["output_tokens"])
}

func metricInt64(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int:
		return int64(typed)
	case int64:
		return typed
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	default:
		return 0
	}
}

func estimateInputTokens(body []byte) int {
	if len(body) == 0 {
		return 0
	}
	estimate := len(body) / 4
	if estimate < 1 {
		return 1
	}
	return estimate
}

func addMessageStartUsageHint(payload map[string]any, inputTokenHint int) {
	if inputTokenHint <= 0 {
		return
	}
	eventType, _ := payload["type"].(string)
	if eventType != "message_start" {
		return
	}
	message, _ := payload["message"].(map[string]any)
	if message == nil {
		return
	}
	usage, _ := message["usage"].(map[string]any)
	if usage == nil {
		usage = map[string]any{}
		message["usage"] = usage
	}
	// Zero is a measured value (for example, a fully cached prompt), not
	// missing telemetry. Never replace it with a body-size estimate.
	if _, ok := usage["input_tokens"]; ok {
		return
	}
	usage["input_tokens"] = inputTokenHint
	if _, ok := usage["output_tokens"]; !ok {
		usage["output_tokens"] = 0
	}
}

func setResponseModelAlias(payload map[string]any, responseAlias string) {
	if responseAlias == "" {
		return
	}
	if _, exists := payload["model"]; exists {
		payload["model"] = responseAlias
	}
	if message, ok := payload["message"].(map[string]any); ok {
		if _, exists := message["model"]; exists {
			message["model"] = responseAlias
		}
	}
}

func shouldDropSSEPayload(payload map[string]any, state *sseFilterState, dropTypes map[string]bool) bool {
	eventType, _ := payload["type"].(string)
	switch eventType {
	case "message_start":
		filterMessageContent(payload, dropTypes)
	case "content_block_start":
		idx, ok := payloadIndex(payload)
		if !ok {
			return false
		}
		contentBlock, _ := payload["content_block"].(map[string]any)
		normalizeToolIDBlock(contentBlock)
		blockType, _ := contentBlock["type"].(string)
		if dropTypes[blockType] {
			state.drop[idx] = true
			return true
		}
		mapped := state.next
		state.next++
		state.remap[idx] = mapped
		payload["index"] = mapped
	case "content_block_delta", "content_block_stop":
		idx, ok := payloadIndex(payload)
		if !ok {
			return false
		}
		if state.drop[idx] {
			return true
		}
		mapped, exists := state.remap[idx]
		if !exists {
			// Defensive: gateways translating OpenAI-style streams sometimes emit
			// deltas without a preceding content_block_start. Allocate a mapping
			// instead of leaking the raw index, which could collide with an
			// already-remapped block and interleave tool JSON with text.
			mapped = state.next
			state.next++
			state.remap[idx] = mapped
		}
		payload["index"] = mapped
	}
	return false
}

func filterMessageContent(payload map[string]any, dropTypes map[string]bool) {
	message, _ := payload["message"].(map[string]any)
	content, _ := message["content"].([]any)
	if len(content) == 0 {
		return
	}
	message["content"] = filterContentBlocks(content, dropTypes)
}

func filterContentBlocks(content []any, dropTypes map[string]bool) []any {
	filtered := make([]any, 0, len(content))
	for _, item := range content {
		block, ok := item.(map[string]any)
		if !ok {
			filtered = append(filtered, item)
			continue
		}
		blockType, _ := block["type"].(string)
		normalizeToolIDBlock(block)
		if !dropTypes[blockType] {
			filtered = append(filtered, block)
		}
	}
	return filtered
}

func payloadIndex(payload map[string]any) (int, bool) {
	raw, ok := payload["index"]
	if !ok {
		return 0, false
	}
	switch value := raw.(type) {
	case float64:
		return int(value), true
	case int:
		return value, true
	default:
		return 0, false
	}
}

// openAINumber extracts an int from a JSON number (float64 after
// Unmarshal) or a raw int.
func openAINumber(raw any) (int, bool) {
	switch value := raw.(type) {
	case float64:
		return int(value), true
	case int:
		return value, true
	default:
		return 0, false
	}
}

func formatSSEEvent(eventName, data string) string {
	var builder strings.Builder
	if eventName != "" {
		builder.WriteString("event: ")
		builder.WriteString(eventName)
		builder.WriteByte('\n')
	}
	builder.WriteString("data: ")
	builder.WriteString(data)
	builder.WriteString("\n\n")
	return builder.String()
}

// ---------- OpenAI chat-completions adapter ----------
//
// Providers with format:"openai-chat" (e.g. OpenCode Go) expose
// /v1/chat/completions instead of the Anthropic Messages API. The functions
// below translate requests Anthropic->OpenAI and responses OpenAI->Anthropic
// (both streaming SSE and plain JSON). Mapping choices follow
// github.com/shantoislamdev/claude-adapter, with two deliberate fixes:
// finish_reason is mapped faithfully (stop/length/tool_calls/content_filter)
// instead of collapsing to end_turn, and block stop events are deduplicated.

// anthropicToOpenAIRequest translates an already-parsed Anthropic request
// payload; the caller (buildAttemptBody) owns parsing and the non-JSON
// passthrough case.
func anthropicToOpenAIRequest(payload map[string]any, overrides map[string]any) ([]byte, error) {
	// Do not silently flatten multimodal tool results or unknown image sources.
	messages, _ := payload["messages"].([]any)
	for _, item := range messages {
		message, _ := item.(map[string]any)
		blocks, _ := message["content"].([]any)
		for _, item := range blocks {
			block, _ := item.(map[string]any)
			if block["type"] == "tool_result" {
				parts, _ := block["content"].([]any)
				for _, part := range parts {
					value, _ := part.(map[string]any)
					if value["type"] != "text" {
						return nil, &requestCompatibilityError{message: "OpenAI route cannot preserve non-text tool results"}
					}
				}
			}
			if block["type"] == "image" {
				source, _ := block["source"].(map[string]any)
				data, _ := source["data"].(string)
				imageURL, _ := source["url"].(string)
				if data == "" && imageURL == "" {
					return nil, &requestCompatibilityError{message: "OpenAI route requires a base64 or URL image source"}
				}
			}
			if block["type"] == "document" {
				return nil, &requestCompatibilityError{message: "OpenAI route cannot preserve document input"}
			}
		}
	}
	out := map[string]any{}
	for _, key := range []string{"model", "max_tokens", "temperature", "top_p"} {
		if value, ok := payload[key]; ok {
			out[key] = value
		}
	}
	if stop, ok := payload["stop_sequences"]; ok {
		out["stop"] = stop
	}
	if stream, _ := payload["stream"].(bool); stream {
		out["stream"] = true
		out["stream_options"] = map[string]any{"include_usage": true}
	}
	if tools, ok := payload["tools"].([]any); ok && len(tools) > 0 {
		out["tools"] = anthropicToolsToOpenAI(tools)
	}
	if choice, ok := payload["tool_choice"]; ok {
		if mapped := anthropicToolChoiceToOpenAI(choice); mapped != nil {
			out["tool_choice"] = mapped
		}
	}
	out["messages"] = anthropicMessagesToOpenAI(payload)
	for key, value := range overrides {
		out[key] = value
	}
	return json.Marshal(out)
}

func anthropicToolsToOpenAI(tools []any) []any {
	out := make([]any, 0, len(tools))
	for _, item := range tools {
		tool, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fn := map[string]any{
			"name":       tool["name"],
			"parameters": tool["input_schema"],
		}
		// Several OpenAI-compatible gateways reject an explicit JSON null
		// description even though Anthropic allows tools without one. Omit the
		// optional field unless Claude supplied a real string.
		if description, ok := tool["description"].(string); ok {
			fn["description"] = description
		}
		if fn["parameters"] == nil {
			fn["parameters"] = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, map[string]any{"type": "function", "function": fn})
	}
	return out
}

func anthropicToolChoiceToOpenAI(choice any) any {
	switch typed := choice.(type) {
	case string:
		return typed // already "auto"/"required"/...
	case map[string]any:
		switch typed["type"] {
		case "auto":
			return "auto"
		case "any":
			return "required"
		case "none":
			return "none"
		case "tool":
			if name, ok := typed["name"].(string); ok && name != "" {
				return map[string]any{"type": "function", "function": map[string]any{"name": name}}
			}
		}
	}
	return "auto"
}

// anthropicMessagesToOpenAI converts the system prompt and message list.
// tool_result blocks become standalone OpenAI tool-role messages (emitted
// before any plain user text, matching claude-adapter), assistant tool_use
// blocks become tool_calls, and duplicate tool_use IDs in history are renamed
// (with the rename applied to later tool_results) since OpenAI rejects them.
func anthropicMessagesToOpenAI(payload map[string]any) []any {
	out := []any{}
	if systemText := anthropicSystemText(payload["system"]); systemText != "" {
		out = append(out, map[string]any{"role": "system", "content": systemText})
	}
	messages, _ := payload["messages"].([]any)
	seenToolIDs := map[string]bool{}
	idRemap := map[string]string{}
	dupCounter := 0
	for _, item := range messages {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		role, _ := message["role"].(string)
		content := message["content"]
		if text, ok := content.(string); ok {
			if text != "" {
				out = append(out, map[string]any{"role": role, "content": text})
			}
			continue
		}
		blocks, ok := content.([]any)
		if !ok {
			continue
		}
		switch role {
		case "user":
			var toolMessages []any
			var texts []string
			var images []any
			for _, blockItem := range blocks {
				block, ok := blockItem.(map[string]any)
				if !ok {
					continue
				}
				switch block["type"] {
				case "text":
					if text, ok := block["text"].(string); ok && text != "" {
						texts = append(texts, text)
					}
				case "image":
					if source, ok := block["source"].(map[string]any); ok {
						mediaType, _ := source["media_type"].(string)
						data, _ := source["data"].(string)
						if data != "" {
							images = append(images, map[string]any{
								"type":      "image_url",
								"image_url": map[string]any{"url": "data:" + mediaType + ";base64," + data},
							})
						} else if imageURL, _ := source["url"].(string); imageURL != "" {
							images = append(images, map[string]any{"type": "image_url", "image_url": map[string]any{"url": imageURL}})
						}
					}
				case "tool_result":
					toolCallID, _ := block["tool_use_id"].(string)
					if remapped, ok := idRemap[toolCallID]; ok {
						toolCallID = remapped
					}
					resultText := anthropicToolResultText(block["content"])
					if isError, _ := block["is_error"].(bool); isError {
						resultText = "Error: " + resultText
					}
					toolMessages = append(toolMessages, map[string]any{
						"role":         "tool",
						"tool_call_id": toolCallID,
						"content":      resultText,
					})
				}
			}
			out = append(out, toolMessages...)
			if len(texts) == 1 && len(images) == 0 {
				out = append(out, map[string]any{"role": "user", "content": texts[0]})
			} else if len(texts) > 0 || len(images) > 0 {
				parts := []any{}
				for _, text := range texts {
					parts = append(parts, map[string]any{"type": "text", "text": text})
				}
				parts = append(parts, images...)
				out = append(out, map[string]any{"role": "user", "content": parts})
			}
		case "assistant":
			var texts []string
			var toolCalls []any
			for _, blockItem := range blocks {
				block, ok := blockItem.(map[string]any)
				if !ok {
					continue
				}
				switch block["type"] {
				case "text":
					if text, ok := block["text"].(string); ok {
						texts = append(texts, text)
					}
				case "tool_use":
					id, _ := block["id"].(string)
					if seenToolIDs[id] {
						renamed := fmt.Sprintf("call_renamed_%d", dupCounter)
						dupCounter++
						idRemap[id] = renamed
						id = renamed
					} else {
						seenToolIDs[id] = true
					}
					name, _ := block["name"].(string)
					arguments := "{}"
					if input, ok := block["input"]; ok && input != nil {
						if raw, err := json.Marshal(input); err == nil {
							arguments = string(raw)
						}
					}
					toolCalls = append(toolCalls, map[string]any{
						"id":       id,
						"type":     "function",
						"function": map[string]any{"name": name, "arguments": arguments},
					})
				}
			}
			text := strings.Join(texts, "")
			if text == "" && len(toolCalls) == 0 {
				continue
			}
			converted := map[string]any{"role": "assistant"}
			if text != "" {
				converted["content"] = text
			}
			if len(toolCalls) > 0 {
				converted["tool_calls"] = toolCalls
			}
			out = append(out, converted)
		}
	}
	return out
}

func anthropicSystemText(system any) string {
	switch typed := system.(type) {
	case string:
		return typed
	case []any:
		var texts []string
		for _, item := range typed {
			if block, ok := item.(map[string]any); ok {
				if text, ok := block["text"].(string); ok && text != "" {
					texts = append(texts, text)
				}
			}
		}
		return strings.Join(texts, "\n")
	}
	return ""
}

func anthropicToolResultText(content any) string {
	if text, ok := content.(string); ok {
		return text
	}
	blocks, ok := content.([]any)
	if !ok {
		return ""
	}
	var texts []string
	for _, item := range blocks {
		if block, ok := item.(map[string]any); ok {
			if text, ok := block["text"].(string); ok {
				texts = append(texts, text)
			}
		}
	}
	return strings.Join(texts, "\n")
}

func mapOpenAIFinishReason(reason string) string {
	switch reason {
	case "stop":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call":
		return "tool_use"
	case "content_filter":
		return "refusal"
	default:
		return "end_turn"
	}
}

// openAIStreamState tracks the Anthropic content blocks being synthesized
// from an OpenAI chunk stream.
type openaiStreamState struct {
	argumentBytes     int
	alias             string
	started           bool
	terminated        bool
	textOpen          bool
	textIndex         int
	nextBlock         int
	tools             map[int]*openAIToolStreamBlock
	toolOrder         []int
	pendingStopReason string
	protocolError     bool

	inputTokens     int64
	outputTokens    int64
	cacheReadTokens int64
}

type openAIToolStreamBlock struct {
	argumentBytes     int
	id                string
	name              string
	blockIndex        int
	started           bool
	closed            bool
	pendingArgs       []string
	argumentFragments []string
}

func newOpenAIStreamReadCloser(source io.ReadCloser, responseAlias string) io.ReadCloser {
	return newOpenAIStreamReadCloserWithStats(source, responseAlias, nil)
}

func newOpenAIStreamReadCloserWithStats(source io.ReadCloser, responseAlias string, stats *streamStats) io.ReadCloser {
	reader, writer := io.Pipe()
	go func() {
		err := translateOpenAIStreamWithStats(source, writer, responseAlias, stats)
		_ = source.Close()
		_ = writer.CloseWithError(err)
	}()
	return &sseFilterReadCloser{source: source, reader: reader}
}

func writeAnthropicSSE(w io.Writer, event string, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, formatSSEEvent(event, string(raw)))
	return err
}

func translateOpenAIStream(source io.Reader, writer io.Writer, responseAlias string) error {
	return translateOpenAIStreamWithStats(source, writer, responseAlias, nil)
}

func translateOpenAIStreamWithStats(source io.Reader, writer io.Writer, responseAlias string, stats *streamStats) error {
	reader := bufio.NewReader(source)
	state := &openaiStreamState{alias: responseAlias, textIndex: -1, tools: map[int]*openAIToolStreamBlock{}}
	var dataLines []string
	frameBytes := 0
	fail := func(message string) error {
		state.protocolError, state.terminated = true, true
		if stats != nil {
			stats.ProtocolError.Store(true)
		}
		if err := writeAnthropicSSE(writer, "error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": message}}); err != nil {
			return err
		}
		return errors.New(message)
	}
	sawDone := false
	flush := func() error {
		data := strings.Join(dataLines, "\n")
		dataLines = nil
		frameBytes = 0
		if data == "" {
			return nil
		}
		if data == "[DONE]" {
			sawDone = true
			return nil
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return fail("OpenAI adapter: malformed SSE JSON")
		}
		chunk = unwrapOpenAIEnvelope(chunk)
		recordReportedBackend(stats, chunk)
		if errPayload, ok := chunk["error"].(map[string]any); ok {
			// Upstream sent an OpenAI-shaped error mid-stream (or as the whole
			// response body with a 200 status): surface it as an Anthropic
			// error event so the client retries instead of hanging.
			message, _ := errPayload["message"].(string)
			if message == "" {
				message = "upstream stream error"
			}
			return fail(message)
		}
		if err := processOpenAIChunk(writer, state, chunk); err != nil {
			return err
		}
		return nil
	}
	var streamErr error
loop:
	for {
		line, err := readBoundedSSELine(reader, maxSSEFrameBytes-frameBytes)
		frameBytes += len(line)
		if len(line) > 0 {
			trimmed := strings.TrimRight(line, "\r\n")
			switch {
			case trimmed == "":
				if flushErr := flush(); flushErr != nil {
					return flushErr
				}
				if sawDone || state.terminated {
					break loop
				}
			case strings.HasPrefix(trimmed, "data:"):
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
			default:
				// event:/id:/retry: lines carry no information for the
				// translation; drop them.
			}
		}
		if err != nil {
			if len(dataLines) > 0 {
				if flushErr := flush(); flushErr != nil {
					return flushErr
				}
			}
			if err != io.EOF {
				streamErr = err
			}
			break
		}
	}
	if streamErr != nil {
		return fail("upstream stream interrupted: " + streamErr.Error())
	}
	if !state.started {
		// Clean EOF without a single chunk: fail loudly rather than letting
		// the client accept an empty 200.
		return fail("upstream stream ended without any completion chunk")
	}
	if !state.terminated {
		if !sawDone && state.pendingStopReason == "" {
			return fail("OpenAI adapter: unexpected EOF without completion signal")
		}
		// OpenCode Go omits finish_reason on tool-call streams and ends with
		// [DONE]: synthesize a well-formed ending (close open blocks, then
		// message_delta + message_stop) so the client sees a valid message.
		stopReason := state.pendingStopReason
		if stopReason == "" && len(state.toolOrder) > 0 {
			stopReason = "tool_use"
		}
		if stopReason == "" {
			stopReason = "end_turn"
		}
		finishErr := finishOpenAIMessageChecked(writer, state, stopReason)
		if stats != nil && state.protocolError {
			stats.ProtocolError.Store(true)
		}
		if finishErr != nil {
			return finishErr
		}
	}
	return nil
}

func processOpenAIChunk(w io.Writer, state *openaiStreamState, chunk map[string]any) error {
	if usage, ok := chunk["usage"].(map[string]any); ok {
		if v, ok := openAINumber(usage["prompt_tokens"]); ok {
			state.inputTokens = int64(v)
		}
		if v, ok := openAINumber(usage["completion_tokens"]); ok {
			state.outputTokens = int64(v)
		}
		if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
			if v, ok := openAINumber(details["cached_tokens"]); ok {
				state.cacheReadTokens = int64(v)
			}
		} else if v, ok := openAINumber(usage["prompt_cache_hit_tokens"]); ok {
			state.cacheReadTokens = int64(v)
		}
	}
	if !state.started {
		state.started = true
		id, _ := chunk["id"].(string)
		if id == "" {
			id = "adapter"
		}
		if !strings.HasPrefix(id, "msg_") {
			id = "msg_" + id
		}
		if err := writeAnthropicSSE(w, "message_start", map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"id":            id,
				"type":          "message",
				"role":          "assistant",
				"model":         state.alias,
				"content":       []any{},
				"stop_reason":   nil,
				"stop_sequence": nil,
				"usage":         map[string]any{"input_tokens": state.inputTokens, "output_tokens": 0},
			},
		}); err != nil {
			return err
		}
	}
	choices, _ := chunk["choices"].([]any)
	for _, choiceItem := range choices {
		choice, ok := choiceItem.(map[string]any)
		if !ok {
			continue
		}
		delta, _ := choice["delta"].(map[string]any)
		if content, ok := delta["content"].(string); ok && content != "" {
			if !state.textOpen {
				state.textOpen = true
				state.textIndex = state.nextBlock
				state.nextBlock++
				if err := writeAnthropicSSE(w, "content_block_start", map[string]any{
					"type":          "content_block_start",
					"index":         state.textIndex,
					"content_block": map[string]any{"type": "text", "text": ""},
				}); err != nil {
					return err
				}
			}
			if err := writeAnthropicSSE(w, "content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": state.textIndex,
				"delta": map[string]any{"type": "text_delta", "text": content},
			}); err != nil {
				return err
			}
		}
		if toolCalls, ok := delta["tool_calls"].([]any); ok {
			for _, toolCallItem := range toolCalls {
				if err := processOpenAIToolCallChunk(w, state, toolCallItem); err != nil {
					return err
				}
			}
		}
		if finishReason, _ := choice["finish_reason"].(string); finishReason != "" && !state.terminated {
			// Usage commonly arrives in a final chunk after finish_reason.
			// Defer Anthropic message_delta/message_stop until [DONE] or EOF so
			// token accounting is not frozen at zero.
			state.pendingStopReason = mapOpenAIFinishReason(finishReason)
		}
	}
	return nil
}

func processOpenAIToolCallChunk(w io.Writer, state *openaiStreamState, toolCallItem any) error {
	toolCall, ok := toolCallItem.(map[string]any)
	if !ok {
		return nil
	}
	index := 0
	if v, ok := openAINumber(toolCall["index"]); ok {
		index = v
	}
	block := state.tools[index]
	if block == nil {
		if len(state.tools) >= maxToolCalls {
			return errors.New("OpenAI adapter: too many tool calls")
		}
		block = &openAIToolStreamBlock{}
		state.tools[index] = block
	}
	if id, _ := toolCall["id"].(string); id != "" {
		block.id = id
	}
	arguments := ""
	if fn, ok := toolCall["function"].(map[string]any); ok {
		if name, _ := fn["name"].(string); name != "" {
			block.name = name
		}
		arguments, _ = fn["arguments"].(string)
	}
	block.argumentBytes += len(arguments)
	state.argumentBytes += len(arguments)
	if state.argumentBytes > maxToolArgumentBytes {
		return errors.New("OpenAI adapter: tool arguments exceed limit")
	}
	if !block.started {
		if arguments != "" {
			block.pendingArgs = append(block.pendingArgs, arguments)
			block.argumentFragments = append(block.argumentFragments, arguments)
		}
		if block.id == "" || block.name == "" {
			// Wait until both id and name are known (they normally arrive in
			// the first chunk for the index); buffer argument fragments.
			return nil
		}
		if state.textOpen {
			if err := writeAnthropicSSE(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": state.textIndex}); err != nil {
				return err
			}
			state.textOpen = false
		}
		block.blockIndex = state.nextBlock
		state.nextBlock++
		block.started = true
		state.toolOrder = append(state.toolOrder, index)
		if err := writeAnthropicSSE(w, "content_block_start", map[string]any{
			"type":  "content_block_start",
			"index": block.blockIndex,
			"content_block": map[string]any{
				"type":  "tool_use",
				"id":    block.id,
				"name":  block.name,
				"input": map[string]any{},
			},
		}); err != nil {
			return err
		}
		for _, pending := range block.pendingArgs {
			if err := writeAnthropicSSE(w, "content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": block.blockIndex,
				"delta": map[string]any{"type": "input_json_delta", "partial_json": pending},
			}); err != nil {
				return err
			}
		}
		block.pendingArgs = nil
		return nil
	}
	if arguments != "" {
		block.argumentFragments = append(block.argumentFragments, arguments)
		return writeAnthropicSSE(w, "content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": block.blockIndex,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": arguments},
		})
	}
	return nil
}

// finishOpenAIMessage closes any open blocks and emits message_delta +
// message_stop exactly once.
func finishOpenAIMessage(w io.Writer, state *openaiStreamState, stopReason string) {
	_ = finishOpenAIMessageChecked(w, state, stopReason)
}

func finishOpenAIMessageChecked(w io.Writer, state *openaiStreamState, stopReason string) error {
	if state.terminated {
		return nil
	}
	for _, block := range state.tools {
		if block == nil {
			continue
		}
		arguments := strings.TrimSpace(strings.Join(block.argumentFragments, ""))
		var object map[string]any
		if arguments != "" && (json.Unmarshal([]byte(arguments), &object) != nil || object == nil) {
			state.protocolError = true
			state.terminated = true
			return writeAnthropicSSE(w, "error", map[string]any{
				"type":  "error",
				"error": map[string]any{"type": "api_error", "message": "OpenAI adapter: malformed tool call arguments"},
			})
		}
		if !block.started && (block.id != "" || block.name != "" || len(block.argumentFragments) > 0) {
			state.protocolError = true
			state.terminated = true
			return writeAnthropicSSE(w, "error", map[string]any{
				"type":  "error",
				"error": map[string]any{"type": "api_error", "message": "OpenAI adapter: incomplete tool call"},
			})
		}
	}
	state.terminated = true
	if state.textOpen {
		if err := writeAnthropicSSE(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": state.textIndex}); err != nil {
			return err
		}
		state.textOpen = false
	}
	for _, index := range state.toolOrder {
		block := state.tools[index]
		if block.started && !block.closed {
			block.closed = true
			if err := writeAnthropicSSE(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": block.blockIndex}); err != nil {
				return err
			}
		}
	}
	usage := map[string]any{
		"input_tokens":  state.inputTokens,
		"output_tokens": state.outputTokens,
	}
	if state.cacheReadTokens > 0 {
		usage["cache_read_input_tokens"] = state.cacheReadTokens
	}
	if err := writeAnthropicSSE(w, "message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil},
		"usage": usage,
	}); err != nil {
		return err
	}
	return writeAnthropicSSE(w, "message_stop", map[string]any{"type": "message_stop"})
}

// newOpenAIJSONReadCloser translates a non-streaming OpenAI chat.completion
// body into an Anthropic message JSON body.
func newOpenAIJSONReadCloser(source io.ReadCloser, responseAlias string) io.ReadCloser {
	return newOpenAIJSONReadCloserWithStats(source, responseAlias, nil)
}

func newOpenAIJSONReadCloserWithStats(source io.ReadCloser, responseAlias string, stats *streamStats) io.ReadCloser {
	body, err := io.ReadAll(source)
	if err != nil {
		return &bufferedReadCloser{source: source, reader: strings.NewReader(string(body))}
	}
	translated, protocolError := translateOpenAIJSONResponseChecked(body, responseAlias)
	if protocolError && stats != nil {
		stats.ProtocolError.Store(true)
	}
	return &bufferedReadCloser{source: source, reader: strings.NewReader(string(translated))}
}

func translateOpenAIJSONResponse(body []byte, responseAlias string) []byte {
	translated, _ := translateOpenAIJSONResponseChecked(body, responseAlias)
	return translated
}

func translateOpenAIJSONResponseChecked(body []byte, responseAlias string) ([]byte, bool) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body, false
	}
	payload = unwrapOpenAIEnvelope(payload)
	if errPayload, ok := payload["error"].(map[string]any); ok {
		message, _ := errPayload["message"].(string)
		out, err := json.Marshal(map[string]any{
			"type":  "error",
			"error": map[string]any{"type": "api_error", "message": firstNonEmpty(message, "upstream error")},
		})
		if err == nil {
			return out, false
		}
		return body, false
	}
	choices, _ := payload["choices"].([]any)
	if len(choices) == 0 {
		return body, false
	}
	choice, _ := choices[0].(map[string]any)
	message, _ := choice["message"].(map[string]any)
	content := []any{}
	if text, _ := message["content"].(string); text != "" {
		content = append(content, map[string]any{"type": "text", "text": text})
	}
	if toolCalls, ok := message["tool_calls"].([]any); ok {
		for _, toolCallItem := range toolCalls {
			toolCall, ok := toolCallItem.(map[string]any)
			if !ok {
				continue
			}
			fn, _ := toolCall["function"].(map[string]any)
			name, _ := fn["name"].(string)
			arguments, _ := fn["arguments"].(string)
			var input any
			trimmedArguments := strings.TrimSpace(arguments)
			if trimmedArguments == "" || trimmedArguments == "null" {
				input = map[string]any{}
			} else if err := json.Unmarshal([]byte(trimmedArguments), &input); err != nil {
				return openAIAdapterErrorBody("OpenAI adapter: malformed tool call arguments"), true
			}
			id, _ := toolCall["id"].(string)
			content = append(content, map[string]any{
				"type":  "tool_use",
				"id":    firstNonEmpty(id, "call_adapter"),
				"name":  name,
				"input": input,
			})
		}
	}
	usage := map[string]any{"input_tokens": 0, "output_tokens": 0}
	if usagePayload, ok := payload["usage"].(map[string]any); ok {
		if v, ok := openAINumber(usagePayload["prompt_tokens"]); ok {
			usage["input_tokens"] = v
		}
		if v, ok := openAINumber(usagePayload["completion_tokens"]); ok {
			usage["output_tokens"] = v
		}
		if details, ok := usagePayload["prompt_tokens_details"].(map[string]any); ok {
			if v, ok := openAINumber(details["cached_tokens"]); ok && v > 0 {
				usage["cache_read_input_tokens"] = v
			}
		} else if v, ok := openAINumber(usagePayload["prompt_cache_hit_tokens"]); ok && v > 0 {
			usage["cache_read_input_tokens"] = v
		}
	}
	finishReason, _ := choice["finish_reason"].(string)
	id, _ := payload["id"].(string)
	if !strings.HasPrefix(id, "msg_") {
		id = "msg_" + firstNonEmpty(id, "adapter")
	}
	out, err := json.Marshal(map[string]any{
		"id":            id,
		"type":          "message",
		"role":          "assistant",
		"model":         responseAlias,
		"content":       content,
		"stop_reason":   mapOpenAIFinishReason(finishReason),
		"stop_sequence": nil,
		"usage":         usage,
	})
	if err != nil {
		return body, false
	}
	return out, false
}

func openAIAdapterErrorBody(message string) []byte {
	out, err := json.Marshal(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": message},
	})
	if err != nil {
		return []byte(`{"type":"error","error":{"type":"api_error","message":"OpenAI adapter error"}}`)
	}
	return out
}

// ClinePass wraps OpenAI-compatible responses as {"success":true,"data":{...}}.
// Unwrap only recognized chat-completion/error envelopes so unrelated provider
// metadata named "data" is preserved.
func unwrapOpenAIEnvelope(payload map[string]any) map[string]any {
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return payload
	}
	if _, ok := data["choices"]; ok {
		return data
	}
	if _, ok := data["error"]; ok {
		return data
	}
	return payload
}
