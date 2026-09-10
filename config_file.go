package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fileConfig is the JSON schema; config contains compiled routing tables.
type fileConfig struct {
	Version            int                              `json:"version"`
	Listen             string                           `json:"listen"`
	AlsoListen         []string                         `json:"alsoListen,omitempty"`
	Normalize          normalizeConfig                  `json:"normalize,omitempty"`
	AdaptiveRouting    *adaptiveRoutingConfig           `json:"adaptiveRouting,omitempty"`
	ProviderQuarantine *providerQuarantineConfig        `json:"providerQuarantine,omitempty"`
	StateDir           string                           `json:"stateDir"`
	ErrorTrace         errorTraceConfig                 `json:"errorTrace,omitempty"`
	Client             clientDefinition                 `json:"client"`
	Profiles           map[string]profileDefinition     `json:"profiles"`
	AccountPools       map[string]accountPoolDefinition `json:"accountPools"`
	Providers          map[string]providerDefinition    `json:"providers"`
	Models             map[string]modelDefinition       `json:"models"`
	ModelPools         map[string]modelPoolDefinition   `json:"modelPools"`
	Chains             map[string]chainDefinition       `json:"chains"`
	Aliases            map[string]string                `json:"aliases"`
}
type clientDefinition struct {
	Command     []string `json:"command"`
	ConfigDir   string   `json:"configDir"`
	Model       string   `json:"model"`
	OpusModel   string   `json:"opusModel"`
	SonnetModel string   `json:"sonnetModel"`
	HaikuModel  string   `json:"haikuModel"`
}
type profileDefinition struct {
	DefaultDirectory     bool     `json:"-"`
	DisplayName          string   `json:"displayName"`
	Command              []string `json:"command"`
	ConfigDir            string   `json:"configDir"`
	CredentialsService   string   `json:"credentialsService"`
	FiveHourThresholdPct float64  `json:"fiveHourThresholdPct"`
	SevenDayThresholdPct float64  `json:"sevenDayThresholdPct"`
	// Optional executable argv, not a shell expression. Never enabled implicitly:
	// some CLI refresh mechanisms spend inference quota.
	RefreshCommand []string `json:"refreshCommand"`
}
type accountPoolDefinition struct {
	WorkerUtilizationLimitPct float64  `json:"workerUtilizationLimitPct,omitempty"`
	Profiles                  []string `json:"profiles"`
	FiveHourThresholdPct      float64  `json:"fiveHourThresholdPct"`
	SevenDayThresholdPct      float64  `json:"sevenDayThresholdPct"`
	StickySeconds             int      `json:"stickySeconds"`
}
type usageDefinition struct {
	Mode                string   `json:"mode"`
	PollIntervalSeconds int      `json:"pollIntervalSeconds"`
	SnapshotPath        string   `json:"snapshotPath"`
	SnapshotKey         string   `json:"snapshotKey"`
	MaxAgeSeconds       int      `json:"maxAgeSeconds"`
	SessionThresholdPct float64  `json:"sessionThresholdPct,omitempty"`
	WeeklyThresholdPct  float64  `json:"weeklyThresholdPct,omitempty"`
	ReserveUpstreams    []string `json:"reserveUpstreams,omitempty"`
}
type authDefinition struct {
	Type      string   `json:"type"`
	Name      string   `json:"name"`
	Command   []string `json:"command"`
	Pool      string   `json:"pool"`
	StatePath string   `json:"statePath"`
	Header    string   `json:"header"`
	Prefix    *string  `json:"prefix"`
}
type providerDefinition struct {
	DisplayName              string            `json:"displayName"`
	Protocol                 string            `json:"protocol"`
	Variant                  string            `json:"variant"`
	BaseURL                  string            `json:"baseURL"`
	MessagesPath             string            `json:"messagesPath"`
	Billing                  string            `json:"billing"`
	Auth                     authDefinition    `json:"auth"`
	Usage                    usageDefinition   `json:"usage"`
	Headers                  map[string]string `json:"headers"`
	RequestOverrides         map[string]any    `json:"requestOverrides"`
	ResponseHeaderTimeoutMS  int               `json:"responseHeaderTimeoutMS"`
	StreamIdleTimeoutMS      int               `json:"streamIdleTimeoutMS"`
	DropResponseContentTypes []string          `json:"dropResponseContentTypes,omitempty"`
	FoldSystemIntoMessages   bool              `json:"foldSystemIntoMessages,omitempty"`
	CircuitBreaker           *bool             `json:"circuitBreaker,omitempty"`
}
type modelDefinition struct {
	Provider         string         `json:"provider"`
	Upstream         string         `json:"upstream"`
	ResponseAlias    string         `json:"responseAlias,omitempty"`
	AccountPool      string         `json:"accountPool"`
	ContextWindow    int            `json:"contextWindow"`
	SupportsImages   bool           `json:"supportsImages"`
	SupportsTools    bool           `json:"supportsTools"`
	RequestOverrides map[string]any `json:"requestOverrides"`
}
type modelPoolDefinition struct {
	Models               []string `json:"models"`
	Selection            string   `json:"selection"`
	MinimumContextWindow int      `json:"minimumContextWindow"`
	RequireTools         bool     `json:"requireTools"`
	RequireImages        bool     `json:"requireImages"`
}
type chainStep struct {
	Model string `json:"model"`
	Pool  string `json:"pool"`
}
type chainDefinition struct {
	Steps                []chainStep `json:"steps"`
	AllowPaidFallback    bool        `json:"allowPaidFallback"`
	PaidFallbackOn       []string    `json:"paidFallbackOn"`
	MinimumContextWindow int         `json:"minimumContextWindow"`
	RequireTools         bool        `json:"requireTools"`
	RequireImages        bool        `json:"requireImages"`
}

func configFilePath() (string, error) {
	p := os.Getenv("CLAUDE_PROXY_CONFIG")
	if p == "" {
		home, e := os.UserHomeDir()
		if e != nil {
			return "", e
		}
		p = filepath.Join(home, ".config", "claude-proxy", "config.json")
	}
	return filepath.Abs(p)
}
func loadConfigFile(path string) (config, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return config{}, e
	}
	return decodeFileConfig(path, b)
}

func rejectDuplicateKeys(d *json.Decoder) error {
	t, e := d.Token()
	if e != nil {
		return e
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return errors.New("unexpected JSON delimiter")
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			k, e := d.Token()
			if e != nil {
				return e
			}
			s, ok := k.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if seen[s] {
				return fmt.Errorf("duplicate JSON key %q", s)
			}
			seen[s] = true
		}
		if e := rejectDuplicateKeys(d); e != nil {
			return e
		}
	}
	_, e = d.Token()
	return e
}
func strictJSON(b []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	if e := rejectDuplicateKeys(d); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("expected exactly one JSON value")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(out)
}
func configPath(base, p string) (string, error) {
	if p == "" {
		return "", nil
	}
	if strings.Contains(p, "$") || strings.ContainsAny(p, "\r\n\x00") {
		return "", errors.New("paths cannot contain shell expressions or control characters")
	}
	if p == "~" || strings.HasPrefix(p, "~/") {
		h, e := os.UserHomeDir()
		if e != nil {
			return "", e
		}
		if p == "~" {
			p = h
		} else {
			p = filepath.Join(h, strings.TrimPrefix(p, "~/"))
		}
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(base, p)
	}
	return filepath.Clean(p), nil
}
func validID(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_./[]", r)) {
			return false
		}
	}
	return true
}
func sortedKeys[T any](m map[string]T) []string {
	k := make([]string, 0, len(m))
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}
func threshold(v float64) bool { return v >= 0 && v <= 100 }
func defaultThreshold(v float64) float64 {
	if v == 0 {
		return 100
	}
	return v
}
func checkArgv(argv []string) error {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return errors.New("command must be a nonempty executable argument array")
	}
	for _, arg := range argv {
		if strings.ContainsRune(arg, 0) {
			return errors.New("command contains NUL")
		}
	}
	return nil
}
func normalizeArgv(base string, argv []string) ([]string, error) {
	if e := checkArgv(argv); e != nil {
		return nil, e
	}
	out := append([]string(nil), argv...)
	if strings.Contains(out[0], "/") {
		p, e := configPath(base, out[0])
		if e != nil {
			return nil, e
		}
		out[0] = p
	}
	return out, nil
}

func decodeFileConfig(path string, body []byte) (config, error) {
	var f fileConfig
	if e := strictJSON(body, &f); e != nil {
		return config{}, fmt.Errorf("config: %w", e)
	}
	if f.Version != 1 {
		return config{}, errors.New("unsupported config version: expected 1")
	}
	if len(f.Providers) == 0 || len(f.Models) == 0 || len(f.Aliases) == 0 {
		return config{}, errors.New("providers, models and aliases must be nonempty")
	}
	base := filepath.Dir(path)
	if f.Listen == "" {
		f.Listen = "127.0.0.1:48104"
	}
	host, port, e := net.SplitHostPort(f.Listen)
	portNumber, portErr := strconv.Atoi(port)
	if e != nil || portErr != nil || portNumber < 0 || portNumber > 65535 || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return config{}, errors.New("listen must be a literal loopback IP and port")
	}
	seenListeners := map[string]bool{f.Listen: true}
	for _, addr := range f.AlsoListen {
		h, p, err := net.SplitHostPort(addr)
		n, parseErr := strconv.Atoi(p)
		if err != nil || parseErr != nil || n < 1 || n > 65535 || net.ParseIP(h) == nil || !net.ParseIP(h).IsLoopback() || seenListeners[addr] {
			return config{}, errors.New("alsoListen requires unique literal loopback addresses and nonzero ports")
		}
		seenListeners[addr] = true
	}
	if f.StateDir == "" {
		f.StateDir = "./state"
	}
	f.StateDir, e = configPath(base, f.StateDir)
	if e != nil {
		return config{}, e
	}
	if len(f.Client.Command) == 0 {
		f.Client.Command = []string{"claude"}
	}
	f.Client.Command, e = normalizeArgv(base, f.Client.Command)
	if e != nil {
		return config{}, fmt.Errorf("client: %w", e)
	}
	f.Client.ConfigDir, e = configPath(base, firstNonEmpty(f.Client.ConfigDir, "./client"))
	if e != nil {
		return config{}, fmt.Errorf("client: %w", e)
	}
	for _, alias := range []string{f.Client.Model, f.Client.OpusModel, f.Client.SonnetModel, f.Client.HaikuModel} {
		if alias != "" && f.Aliases[alias] == "" {
			return config{}, fmt.Errorf("client references unknown model alias %q", alias)
		}
	}
	cfg := config{Definition: &f, Listen: f.Listen, Providers: map[string]providerConfig{}, Models: map[string]modelConfig{}, Chains: map[string]routeChain{}}
	cfg.AlsoListen = append([]string(nil), f.AlsoListen...)
	cfg.Normalize = f.Normalize
	for from, to := range f.Normalize.UnsupportedContentTypes {
		if from != "tool_reference" || to != "text" {
			return config{}, errors.New("normalize supports only tool_reference to text")
		}
	}
	if f.ProviderQuarantine != nil {
		cfg.Quarantine = *f.ProviderQuarantine
	}
	cfg.Metrics = metricsConfig{Path: filepath.Join(f.StateDir, "metrics.jsonl"), MaxSamples: 10000}
	cfg.Logging = loggingConfig{Path: filepath.Join(f.StateDir, "proxy.log")}
	if e := validateErrorTrace(f.ErrorTrace, f.Providers); e != nil {
		return config{}, e
	}
	cfg.Logging.ErrorTrace = f.ErrorTrace
	for _, id := range sortedKeys(f.Profiles) {
		p := f.Profiles[id]
		if !validID(id) {
			return config{}, fmt.Errorf("invalid profile ID %q", id)
		}
		if len(p.Command) == 0 {
			p.Command = []string{"claude"}
		}
		p.Command, e = normalizeArgv(base, p.Command)
		if e != nil {
			return config{}, fmt.Errorf("profile %s: %w", id, e)
		}
		if !threshold(p.FiveHourThresholdPct) || !threshold(p.SevenDayThresholdPct) {
			return config{}, fmt.Errorf("profile %s thresholds must be within (0,100], or omitted", id)
		}
		if p.ConfigDir == "" {
			if filepath.Base(p.Command[0]) != "claude" && p.CredentialsService == "" {
				return config{}, fmt.Errorf("profile %s wrapper requires configDir or credentialsService", id)
			}
			if filepath.Base(p.Command[0]) == "claude" {
				p.DefaultDirectory = true
				p.ConfigDir = "~/.claude"
				if p.CredentialsService == "" {
					p.CredentialsService = "Claude Code-credentials"
				}
			}
		}
		p.ConfigDir, e = configPath(base, p.ConfigDir)
		if e != nil {
			return config{}, e
		}
		if p.ConfigDir != "" && sameDirectory(p.ConfigDir, f.Client.ConfigDir) {
			return config{}, fmt.Errorf("client.configDir must differ from profile %q configDir", id)
		}
		if p.CredentialsService == "" {
			digest := sha256.Sum256([]byte(p.ConfigDir))
			p.CredentialsService = fmt.Sprintf("Claude Code-credentials-%x", digest[:4])
		}
		if len(p.RefreshCommand) > 0 {
			p.RefreshCommand, e = normalizeArgv(base, p.RefreshCommand)
			if e != nil {
				return config{}, e
			}
		}
		p.DisplayName = firstNonEmpty(p.DisplayName, id)
		f.Profiles[id] = p
	}
	for _, id := range sortedKeys(f.AccountPools) {
		p := f.AccountPools[id]
		if p.WorkerUtilizationLimitPct < 0 || p.WorkerUtilizationLimitPct > 100 {
			return config{}, fmt.Errorf("invalid worker utilization limit for pool %s", id)
		}
		if !validID(id) || len(p.Profiles) == 0 {
			return config{}, fmt.Errorf("account pool %q is empty or invalid", id)
		}
		if !threshold(p.FiveHourThresholdPct) || !threshold(p.SevenDayThresholdPct) || p.StickySeconds < 0 {
			return config{}, fmt.Errorf("invalid account pool policy %s", id)
		}
		p.FiveHourThresholdPct = defaultThreshold(p.FiveHourThresholdPct)
		p.SevenDayThresholdPct = defaultThreshold(p.SevenDayThresholdPct)
		if p.StickySeconds == 0 {
			p.StickySeconds = 1800
		}
		seen := map[string]bool{}
		for _, member := range p.Profiles {
			v, ok := f.Profiles[member]
			if !ok {
				return config{}, fmt.Errorf("pool %s references missing profile %s", id, member)
			}
			if seen[v.CredentialsService] {
				return config{}, fmt.Errorf("pool %s repeats a credential identity", id)
			}
			seen[v.CredentialsService] = true
		}
		f.AccountPools[id] = p
	}
	for _, id := range sortedKeys(f.Providers) {
		p := f.Providers[id]
		if !validID(id) {
			return config{}, fmt.Errorf("invalid provider ID %q", id)
		}
		p.DisplayName = firstNonEmpty(p.DisplayName, id)
		p.Variant = firstNonEmpty(p.Variant, "generic")
		allowed := map[string]bool{"generic": true, "claude-subscription": true, "ollama-cloud": true, "opencode-go": true, "clinepass": true, "command-code": true, "bigmodel": true, "vercel-gateway": true, "merge-gateway": true, "xai-subscription": true}
		if !allowed[p.Variant] {
			return config{}, fmt.Errorf("unknown provider variant %q", p.Variant)
		}
		if p.Protocol != "anthropic" && p.Protocol != "openai-chat-completions" {
			return config{}, fmt.Errorf("provider %s requires anthropic or openai-chat-completions protocol", id)
		}
		if p.Billing != "subscription" && p.Billing != "metered" && p.Billing != "local" {
			return config{}, fmt.Errorf("provider %s requires explicit billing: subscription, metered, or local", id)
		}
		u, e := url.Parse(p.BaseURL)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return config{}, fmt.Errorf("provider %s has invalid baseURL", id)
		}
		local := net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback()
		if u.Scheme != "https" && !(u.Scheme == "http" && local && p.Billing == "local") {
			return config{}, fmt.Errorf("provider %s requires HTTPS (HTTP only for literal loopback local providers)", id)
		}
		if p.ResponseHeaderTimeoutMS < 0 || p.StreamIdleTimeoutMS < 0 {
			return config{}, fmt.Errorf("provider %s timeouts must be nonnegative", id)
		}
		for k, v := range p.Headers {
			if strings.ContainsAny(k+v, "\r\n") || strings.EqualFold(k, "authorization") || strings.EqualFold(k, "x-api-key") || strings.EqualFold(k, "api-key") || strings.EqualFold(k, "proxy-authorization") || strings.EqualFold(k, "cookie") || strings.EqualFold(k, "host") {
				return config{}, fmt.Errorf("provider %s: use auth references, not credential/host headers", id)
			}
		}
		if e := validateOverrides(p.RequestOverrides); e != nil {
			return config{}, e
		}
		q := providerConfig{BaseURL: p.BaseURL, Headers: p.Headers, RequestOverrides: p.RequestOverrides, Variant: p.Variant, Billing: p.Billing, DisplayName: p.DisplayName, Usage: p.Usage, ResponseHeaderTimeoutMS: p.ResponseHeaderTimeoutMS, StreamIdleTimeoutMS: p.StreamIdleTimeoutMS}
		for _, contentType := range p.DropResponseContentTypes {
			if contentType != "thinking" && contentType != "redacted_thinking" {
				return config{}, errors.New("dropResponseContentTypes supports only thinking and redacted_thinking")
			}
		}
		q.DropResponseContentTypes = p.DropResponseContentTypes
		q.FoldSystemIntoMessages = p.FoldSystemIntoMessages
		q.CircuitBreaker = p.CircuitBreaker
		if p.Protocol == "openai-chat-completions" {
			q.Format = "openai-chat"
		}
		suffix := "/v1/messages"
		if q.Format != "" {
			suffix = "/v1/chat/completions"
		}
		if strings.HasSuffix(strings.TrimRight(u.Path, "/"), "/v1") {
			suffix = strings.TrimPrefix(suffix, "/v1")
		}
		if p.MessagesPath != "" {
			suffix = p.MessagesPath
		}
		if !strings.HasPrefix(suffix, "/") || strings.ContainsAny(suffix, "?#\r\n") || strings.Contains(suffix, "..") {
			return config{}, fmt.Errorf("provider %s has invalid messagesPath suffix", id)
		}
		q.MessagesPath = suffix
		if p.Auth.Type != "env" && p.Auth.Name != "" || p.Auth.Type != "command" && len(p.Auth.Command) > 0 || p.Auth.Type != "claude-profile-pool" && p.Auth.Pool != "" || p.Auth.Type != "device-oauth" && p.Auth.StatePath != "" {
			return config{}, fmt.Errorf("provider %s has conflicting auth fields", id)
		}
		switch p.Auth.Type {
		case "env":
			if p.Auth.Name == "" || strings.ContainsAny(p.Auth.Name, "= \t\r\n") {
				return config{}, fmt.Errorf("provider %s requires an environment variable name", id)
			}
			q.AuthTokenEnv = p.Auth.Name
		case "command":
			q.AuthTokenCommand, e = normalizeArgv(base, p.Auth.Command)
			if e != nil {
				return config{}, e
			}
		case "none":
			if p.Billing != "local" {
				return config{}, errors.New("auth none is only allowed for local providers")
			}
		case "claude-profile-pool":
			if p.Variant != "claude-subscription" || p.Protocol != "anthropic" || p.Billing != "subscription" || u.String() != "https://api.anthropic.com" {
				return config{}, errors.New("Claude subscriptions require native Anthropic at https://api.anthropic.com")
			}
			if cfg.ClaudeUsage.Provider != "" {
				return config{}, errors.New("use one Claude subscription provider with multiple named accountPools; select accountPool per model")
			}
			pool, ok := f.AccountPools[p.Auth.Pool]
			if !ok {
				return config{}, fmt.Errorf("provider %s references missing account pool", id)
			}
			cfg.ClaudeUsage = claudeUsageConfig{Provider: id, AccountPool: pool.Profiles, AutoSelectAccounts: true, FiveHourThresholdPct: 100, SevenDayThresholdPct: 100, AccountStickySeconds: pool.StickySeconds, AccountProfiles: map[string]claudeUsageProfile{}, CacheTTLSeconds: 60, StaleTTLSeconds: 1800, RequestTimeoutMS: 5000}
			for name, profile := range f.Profiles {
				weekly := defaultThreshold(profile.SevenDayThresholdPct)
				cachePath := ""
				if profile.ConfigDir != "" {
					cachePath = filepath.Join(profile.ConfigDir, ".claude.json")
				}
				if profile.DefaultDirectory {
					cachePath = filepath.Join(filepath.Dir(profile.ConfigDir), ".claude.json")
				}
				cfg.ClaudeUsage.AccountProfiles[name] = claudeUsageProfile{Name: name, CachePath: cachePath, CredentialsService: profile.CredentialsService, SevenDayThresholdPct: &weekly}
			}
		case "device-oauth":
			if p.Variant != "xai-subscription" || p.Protocol != "openai-chat-completions" || p.Billing != "subscription" {
				return config{}, errors.New("device-oauth requires xai-subscription with OpenAI protocol and subscription billing")
			}
			q.AuthMode = "xai-oauth"
			state := p.Auth.StatePath
			if state == "" {
				state = filepath.Join(f.StateDir, id+"-oauth.json")
			}
			q.AuthStatePath, e = configPath(base, state)
			if e != nil {
				return config{}, e
			}
		default:
			return config{}, fmt.Errorf("provider %s has unsupported auth type %q", id, p.Auth.Type)
		}
		if p.Variant == "claude-subscription" && p.Auth.Type != "claude-profile-pool" || p.Variant == "xai-subscription" && p.Auth.Type != "device-oauth" {
			return config{}, errors.New("subscription variant requires its corresponding subscription auth type")
		}
		if p.Auth.Type == "claude-profile-pool" || p.Auth.Type == "device-oauth" {
			if p.Auth.Header != "" || p.Auth.Prefix != nil {
				return config{}, errors.New("subscription auth header overrides are not permitted")
			}
		}
		q.AuthHeader = firstNonEmpty(p.Auth.Header, "Authorization")
		q.AuthPrefix = "Bearer "
		if p.Auth.Prefix != nil {
			q.AuthPrefix = *p.Auth.Prefix
		}
		if strings.ContainsAny(q.AuthHeader+q.AuthPrefix, "\r\n") {
			return config{}, errors.New("invalid auth header")
		}
		q.Usage.Mode = firstNonEmpty(p.Usage.Mode, "passive")
		if !threshold(p.Usage.SessionThresholdPct) || !threshold(p.Usage.WeeklyThresholdPct) {
			return config{}, errors.New("usage reserve thresholds must be in 0..100")
		}
		hasReserve := p.Usage.SessionThresholdPct != 0 || p.Usage.WeeklyThresholdPct != 0 || len(p.Usage.ReserveUpstreams) > 0
		if hasReserve && (p.Variant != "ollama-cloud" || q.Usage.Mode != "api" || len(p.Usage.ReserveUpstreams) == 0 || p.Usage.SessionThresholdPct == 0 && p.Usage.WeeklyThresholdPct == 0) {
			return config{}, errors.New("usage reserves require Ollama API usage, a threshold, and reserveUpstreams")
		}
		switch q.Usage.Mode {
		case "passive":
			if p.Usage.SnapshotPath != "" || p.Usage.SnapshotKey != "" || p.Usage.PollIntervalSeconds != 0 || p.Usage.MaxAgeSeconds != 0 {
				return config{}, errors.New("passive usage cannot configure polling or browser snapshots")
			}
		case "browser":
			if p.Usage.SnapshotPath == "" || p.Usage.PollIntervalSeconds != 0 {
				return config{}, errors.New("browser usage requires snapshotPath and cannot configure API polling")
			}
			q.Usage.SnapshotPath, e = configPath(base, p.Usage.SnapshotPath)
			if e != nil {
				return config{}, e
			}
			q.Usage.SnapshotKey = firstNonEmpty(p.Usage.SnapshotKey, id)
			if q.Usage.MaxAgeSeconds == 0 {
				q.Usage.MaxAgeSeconds = 180
			}
			if q.Usage.MaxAgeSeconds < 1 {
				return config{}, errors.New("browser maxAgeSeconds must be positive")
			}
		case "api":
			if p.Usage.SnapshotPath != "" || p.Usage.SnapshotKey != "" || p.Usage.MaxAgeSeconds != 0 {
				return config{}, errors.New("API usage cannot configure browser fields")
			}
			if q.Usage.PollIntervalSeconds == 0 {
				q.Usage.PollIntervalSeconds = 60
			}
			if q.Usage.PollIntervalSeconds < 30 {
				return config{}, errors.New("API polling interval must be at least 30 seconds")
			}
			switch p.Variant {
			case "claude-subscription", "ollama-cloud", "clinepass", "xai-subscription":
			default:
				return config{}, fmt.Errorf("variant %s has no API quota reader; use passive or opt-in browser mode", p.Variant)
			}
		default:
			return config{}, fmt.Errorf("provider %s has unknown usage mode", id)
		}
		if q.Usage.Mode == "api" && p.Variant == "ollama-cloud" {
			if cfg.OllamaUsage.Provider != "" {
				return config{}, errors.New("only one Ollama API reader per instance")
			}
			cfg.OllamaUsage.Provider = id
			cfg.OllamaUsage.CacheTTLSeconds = q.Usage.PollIntervalSeconds
			cfg.OllamaUsage.SessionThresholdPct = q.Usage.SessionThresholdPct
			cfg.OllamaUsage.WeeklyThresholdPct = q.Usage.WeeklyThresholdPct
			cfg.OllamaUsage.ReserveUpstreams = q.Usage.ReserveUpstreams
		}
		if q.Usage.Mode == "api" && p.Variant == "clinepass" {
			if cfg.ClineUsage.Provider != "" {
				return config{}, errors.New("only one Cline API reader per instance")
			}
			cfg.ClineUsage.Provider = id
			cfg.ClineUsage.CacheTTLSeconds = q.Usage.PollIntervalSeconds
		}
		if q.Usage.Mode == "api" && p.Variant == "claude-subscription" {
			cfg.ClaudeUsage.CacheTTLSeconds = q.Usage.PollIntervalSeconds
		}
		p.Usage = q.Usage
		f.Providers[id] = p
		cfg.Providers[id] = q
	}
	normalizedModels := map[string]modelConfig{}
	for _, id := range sortedKeys(f.Models) {
		m := f.Models[id]
		p, ok := f.Providers[m.Provider]
		if !validID(id) || !ok || strings.TrimSpace(m.Upstream) == "" || m.ContextWindow < 0 {
			return config{}, fmt.Errorf("invalid model %s or missing provider/upstream", id)
		}
		if e := validateOverrides(m.RequestOverrides); e != nil {
			return config{}, e
		}
		if m.ResponseAlias != "" && !validID(m.ResponseAlias) {
			return config{}, fmt.Errorf("model %s has invalid responseAlias", id)
		}
		if m.AccountPool != "" && p.Variant != "claude-subscription" {
			return config{}, errors.New("accountPool only applies to Claude subscription models")
		}
		if p.Variant == "claude-subscription" {
			m.AccountPool = firstNonEmpty(m.AccountPool, p.Auth.Pool)
			if _, ok := f.AccountPools[m.AccountPool]; !ok {
				return config{}, fmt.Errorf("model %s references missing accountPool", id)
			}
			cfg.ClaudeUsage.EligibleUpstreams = append(cfg.ClaudeUsage.EligibleUpstreams, m.Upstream)
		}
		if m.Provider == cfg.OllamaUsage.Provider && !containsString(cfg.OllamaUsage.EligibleUpstreams, m.Upstream) {
			cfg.OllamaUsage.EligibleUpstreams = append(cfg.OllamaUsage.EligibleUpstreams, m.Upstream)
		}
		images := m.SupportsImages
		normalizedModels[id] = modelConfig{ModelID: id, Provider: m.Provider, Upstream: m.Upstream, ResponseAlias: m.ResponseAlias, AccountPool: m.AccountPool, AccountStickySeconds: f.AccountPools[m.AccountPool].StickySeconds, ContextWindow: m.ContextWindow, SupportsImages: &images, SupportsTools: m.SupportsTools, ModelOverrides: m.RequestOverrides}
		f.Models[id] = m
	}
	for _, id := range sortedKeys(f.ModelPools) {
		p := f.ModelPools[id]
		if !validID(id) || len(p.Models) == 0 {
			return config{}, fmt.Errorf("invalid/empty model pool %s", id)
		}
		p.Selection = firstNonEmpty(p.Selection, "fixed-order")
		if p.Selection != "fixed-order" && p.Selection != "sticky-health" {
			return config{}, fmt.Errorf("unknown pool selection %s", p.Selection)
		}
		seen := map[string]bool{}
		for _, name := range p.Models {
			m, ok := f.Models[name]
			if !ok || seen[name] {
				return config{}, fmt.Errorf("pool %s has missing/repeated model %s", id, name)
			}
			seen[name] = true
			if e := checkCapabilities(m, p.MinimumContextWindow, p.RequireTools, p.RequireImages); e != nil {
				return config{}, fmt.Errorf("pool %s: %w", id, e)
			}
		}
		f.ModelPools[id] = p
	}
	compiled := map[string]modelConfig{}
	for _, id := range sortedKeys(f.Chains) {
		c := f.Chains[id]
		if !validID(id) || len(c.Steps) == 0 {
			return config{}, fmt.Errorf("invalid/empty chain %s", id)
		}
		if !c.AllowPaidFallback && len(c.PaidFallbackOn) > 0 {
			return config{}, errors.New("paidFallbackOn requires allowPaidFallback")
		}
		if c.AllowPaidFallback && len(c.PaidFallbackOn) == 0 {
			c.PaidFallbackOn = []string{"quota-exhausted"}
		}
		seenReasons := map[string]bool{}
		for _, r := range c.PaidFallbackOn {
			if (r != "quota-exhausted" && r != "confirmed-provider-outage") || seenReasons[r] {
				return config{}, errors.New("paidFallbackOn allows unique quota-exhausted and confirmed-provider-outage only")
			}
			seenReasons[r] = true
		}
		var members []modelConfig
		seen := map[string]bool{}
		for stage, step := range c.Steps {
			if (step.Model == "") == (step.Pool == "") {
				return config{}, fmt.Errorf("chain %s step requires exactly one model or pool", id)
			}
			names := []string{step.Model}
			selection := "fixed-order"
			if step.Pool != "" {
				p, ok := f.ModelPools[step.Pool]
				if !ok {
					return config{}, fmt.Errorf("chain %s has missing model pool", id)
				}
				names = p.Models
				selection = p.Selection
			}
			for offset, name := range names {
				m, ok := normalizedModels[name]
				if !ok || seen[name] {
					return config{}, fmt.Errorf("chain %s has missing/repeated model %s", id, name)
				}
				seen[name] = true
				if e := checkCapabilities(f.Models[name], c.MinimumContextWindow, c.RequireTools, c.RequireImages); e != nil {
					return config{}, fmt.Errorf("chain %s: %w", id, e)
				}
				tier := stage * 10000
				if selection == "fixed-order" {
					tier += offset
				}
				m.Tier = &tier
				m.ConfiguredPool = step.Pool
				m.AllowPaidFallback = c.AllowPaidFallback
				m.PaidFallbackOn = c.PaidFallbackOn
				if len(members) > 0 && cfg.Providers[m.Provider].Billing == "metered" && !c.AllowPaidFallback {
					return config{}, fmt.Errorf("chain %s includes metered fallback without opt-in", id)
				}
				members = append(members, m)
			}
		}
		root := members[0]
		root.Fallbacks = members[1:]
		compiled[id] = root
		f.Chains[id] = c
	}
	for _, alias := range sortedKeys(f.Aliases) {
		target := f.Aliases[alias]
		if !validID(alias) {
			return config{}, fmt.Errorf("invalid alias %s", alias)
		}
		m, ok := compiled[target]
		if !ok {
			return config{}, fmt.Errorf("alias %s references missing chain %s", alias, target)
		}
		if strings.Contains(alias, "[1m]") {
			for _, v := range append([]modelConfig{m}, m.Fallbacks...) {
				if v.ContextWindow < 1000000 {
					return config{}, fmt.Errorf("alias %s requires 1M context on every leg", alias)
				}
			}
		}
		cfg.Models[alias] = m
	}
	cfg.DefaultProvider = cfg.Models[sortedKeys(f.Aliases)[0]].Provider
	cfg.AdaptiveRouting.Enabled = true
	if f.AdaptiveRouting != nil {
		cfg.AdaptiveRouting = *f.AdaptiveRouting
	}
	return normalizeRuntimeConfig(path, body, cfg)
}

func validateOverrides(m map[string]any) error {
	for k := range m {
		switch k {
		case "model", "messages", "stream", "tools", "system", "max_tokens":
			return fmt.Errorf("requestOverrides cannot replace routing/content field %q", k)
		}
	}
	return nil
}
func checkCapabilities(m modelDefinition, context int, tools, images bool) error {
	if context < 0 || m.ContextWindow < context || tools && !m.SupportsTools || images && !m.SupportsImages {
		return fmt.Errorf("model %s lacks required context/tools/images capabilities", m.Upstream)
	}
	return nil
}
