# Configuration v1

`config_file.go` defines and validates the JSON format. Validation performs no login, Keychain lookup, usage polling, or inference.

## Root sections

| Field | Purpose / default |
| --- | --- |
| `version` | Required integer `1` |
| `listen` | Literal loopback and numeric port; default `127.0.0.1:48104` |
| `alsoListen` | Optional additional literal-loopback listeners with unique nonzero ports; one process and shared pools |
| `stateDir` | Private runtime files; default `./state` |
| `client` | Proxy-facing Claude command, settings directory, and model aliases |
| `profiles` | Local Claude identities and executable commands |
| `accountPools` | Explicit lists of profile IDs and reserves |
| `providers` | Nonempty endpoint/auth/billing definitions |
| `models` | Nonempty exact upstream model definitions |
| `modelPools` | Named model-ID lists and selection policy |
| `chains` | Ordered model/pool stages and paid-fallback policy |
| `aliases` | Nonempty client model string to chain-ID map |
| `normalize` | Optional `unsupportedContentTypes: {"tool_reference":"text"}` compatibility mapping |
| `adaptiveRouting` | Optional scoring settings from `adaptiveRoutingConfig`; applies within sticky-health pools, never across chain stages |
| `providerQuarantine` | Optional provider-health/network incident settings from `providerQuarantineConfig` |

All aliases must be explicit, including dated names and `[1m]` variants. No family-name wildcard imports an unregistered model. `[1m]` requires every candidate to declare at least 1,000,000 context tokens.

Unknown keys, duplicate keys, trailing JSON, invalid references, unsupported protocols/variants, empty/repeated pools, invalid thresholds, and implicit metered fallback fail validation. Paths support absolute, `~/`, and config-relative names, not shell-variable interpolation. Commands are executable argv, not shell strings.

## Claude Code client

`client.command` is executable argv, default `["claude"]`. `client.configDir` defaults to `./client`, relative to the config file. It must differ from account profile directories, including symlink aliases.

`client.model` selects the initial configured alias. It can be omitted when passing `--model ALIAS` to `claude-proxy run`. Optional `opusModel`, `sonnetModel`, and `haikuModel` must also reference configured aliases; omitted values use the session model. These set Claude Code's corresponding `ANTHROPIC_DEFAULT_*_MODEL` environment variables.

`run` sets `ANTHROPIC_BASE_URL` to the configured listener and `ANTHROPIC_AUTH_TOKEN` to a non-secret local placeholder. It clears inherited upstream credentials and preserves the working directory. This is independent of the account selected by the routing pool.

## Profiles and account pools

Profile fields: `displayName`, `command` (argv, default `["claude"]`), optional `configDir`, optional `credentialsService`, optional `refreshCommand` (argv), `fiveHourThresholdPct`, `sevenDayThresholdPct`.

Refresh commands must rotate a usable credential or verify authentication and emit `{"version":1,"authVerified":true}` on stdout. A successful exit alone does not prove unchanged-token recovery. The gateway serializes these helper executions.

Claude quota HTTP calls share a cancellable gate and one-second spacing across background and request-time refreshes. HTTP 429 cooldown applies across accounts and token rotation, independently of inference. Last-good quota readings and cooldown persist in `stateDir/claude-usage-cache.json` (mode 0600), without tokens. Restored readings retain their original timestamps, expire after the stale TTL, and are rejected if configured profile identity changes.

The Claude background monitor checks due profiles every 30 seconds (or the configured poll interval if shorter). Fresh profile snapshots skip credential lookup and HTTP calls, including at startup after restoration. `providers.anthropic.usage.pollIntervalSeconds: 300` provides a gentler five-minute polling cadence; quota-based decisions can lag by that interval. The menu reads the local cached dashboard only. Dashboard `STALE` allows an additional 60 seconds for scheduler and serialized-fetch latency; it never renews the reading timestamp.

Create a new account with `profiles add NAME`. Options: `--display-name NAME`, `--command EXECUTABLE`, `--config-dir PATH`, and `--pool ID`. The directory defaults to `./profiles/NAME`. If a Claude provider exists, the profile joins its default account pool; otherwise it is saved without routing membership. Account login is a separate `profiles login NAME` command. `profiles open NAME -- ARGS...` provides direct account access without the proxy.

Profile creation refuses existing names/directories and validates the entire updated configuration before saving. It uses a temporary file, an exclusive config lock, and an atomic rename. A lock owned by another process is never removed. Use a regular config file path when adding profiles; other commands can read symlinked configs. Restart the server after changing configuration.

Existing executable wrappers can be configured manually with their real directory or `credentialsService`; shell aliases/functions are not executables. Omitting the directory for the standard `claude` command uses its normal profile. Custom directories derive their Keychain service from the normalized path; `credentialsService` overrides this convention. `profiles doctor` verifies executable, directory, and Keychain-item presence without printing credentials.

Pool fields: `profiles` (nonempty ordered IDs), `fiveHourThresholdPct`, `sevenDayThresholdPct`, `stickySeconds` (default 1800). Omitted/0 thresholds mean 100; otherwise values must be greater than 0 and at most 100. The lower profile/pool threshold applies. Repeated credential-service identities in one pool are rejected; known duplicate account identities are deduplicated at runtime.

One Claude subscription provider is supported per instance, with multiple named pools. `auth.pool` supplies its default pool; a Claude model's `accountPool` can choose another. No account outside that model's pool is tried. No speculative duplicate generation is issued.

Optional pool `workerUtilizationLimitPct` (0 disables; e.g. 75) reserves remaining
capacity for Opus/Fable. Sonnet/Haiku requests, including main sessions using those
models, skip an account once either 5-hour or weekly utilization reaches this
limit. Other eligible accounts are tried before the configured fallback chain.
This is a routing reserve, not a provider ban or evidence authorizing paid fallback.

With API usage collection enabled, the proxy retains one actual subscription
percentage observation per account every five minutes, for 30 minutes, in memory.
After at least ten minutes of observations in the same quota window, it projects
recent percentage growth to reset. Workers skip accounts projected to reach the
limit early; as the measured pace slows, they become eligible again. No token
budget or dollar estimate is involved. Stale/future samples and changed reset
windows cannot establish pace. After restart the fixed cap applies while fresh
history accumulates. Unknown usage remains unknown. This cannot guarantee an
exact reserve with concurrent in-flight requests or delayed provider telemetry.

When known, quota affects eligibility and selection alongside affinity and in-flight load. Unknown quota remains eligible without pretending to be unused. Actual auth, account, and model-specific blocks still apply. Proactive reserves require fresh telemetry; without it they cannot be guaranteed.

## Provider fields

Required: `protocol`, `baseURL`, `billing`, `auth`.

Optional: `displayName`, `variant`, `messagesPath`, `headers`, `requestOverrides`, `responseHeaderTimeoutMS`, `streamIdleTimeoutMS`, `usage`.

Adapter options: `dropResponseContentTypes` may contain `thinking` and
`redacted_thinking` only; it cannot discard tool calls or text. Set
`foldSystemIntoMessages` for endpoints requiring system text in messages.
`circuitBreaker: false` disables the per-route circuit, not quota/auth checks.

Protocols: `anthropic` (native Messages), `openai-chat-completions` (Messages-to-Chat translation and response translation). Not OpenAI Responses/realtime or arbitrary endpoints.

Variants: `generic` default, `claude-subscription`, `ollama-cloud`, `opencode-go`, `clinepass`, `command-code`, `bigmodel`, `vercel-gateway`, `merge-gateway`, `xai-subscription`. These identify built-in auth, telemetry, and timing behavior; they do not supply credentials, model lists, entitlements, or fallback chains. OAuth variants require their corresponding protocol and subscription billing.

Billing: `subscription`, `metered`, `local`. Classification is the operator's assertion, not price discovery. Disable provider-side extra usage if unwanted; the proxy cannot prove remote billing behavior.

### URL paths and transport

HTTPS required, except literal loopback HTTP with `local` billing. URL credentials, queries, and fragments are rejected. OAuth is restricted to its native provider origin. HTTP redirects are not followed.

Default suffix: `/v1/messages` or `/v1/chat/completions`. If the base already ends in `/v1`, append only `/messages` or `/chat/completions`. `messagesPath` replaces this suffix, not the base prefix; it must start with `/` and cannot contain query, fragment, or traversal syntax.

| Base URL | Protocol | Final path |
| --- | --- | --- |
| `https://example.test` | Anthropic | `/v1/messages` |
| `https://example.test/v1` | Chat | `/v1/chat/completions` |
| `https://example.test/prefix/v1` | Anthropic | `/prefix/v1/messages` |
| `https://api-gateway.merge.dev/v1/anthropic` | Anthropic | `/v1/anthropic/v1/messages` |

Timeouts are milliseconds; 0 selects inherited defaults, negatives are rejected. Header timeout is an upper bound: adaptive timing, first-event timing, and the chain deadline may end an attempt earlier. Streaming idle timeout is separate. Partial client-visible streams are reported as interrupted, never replayed on a different provider.

### Authentication

- `{"type":"env","name":"PROVIDER_API_KEY"}`: read an environment variable. Missing/empty means auth failure, not anonymous fallthrough.
- `{"type":"command","command":["/absolute/path/helper","argument"]}`: trusted executable prints only the token; output bounded to 64 KiB, execution to five seconds. Successful credentials are cached and invalidated on HTTP 401/403.
- `{"type":"none"}`: requires explicit `local` billing.
- `{"type":"claude-profile-pool","pool":"pool-id"}`: native configured Claude credentials; requires `claude-subscription`, Anthropic protocol, and exactly `https://api.anthropic.com`.
- `{"type":"device-oauth","statePath":"./state/xai-oauth.json"}`: existing provider-specific xAI device flow; requires `xai-subscription` and Chat protocol. `xai-login --no-browser` prints authorization instructions without launching a browser. Not generic website OAuth.

API-key/helper auth defaults to `Authorization: Bearer <token>`. Override `auth.header` and `auth.prefix` for other endpoints, e.g. `x-api-key` and `""`. OAuth header overrides are rejected. Inline credential/cookie/host headers are rejected; use auth references.

## Models, model pools, chains

Model fields: `provider`, `upstream`, optional `accountPool`, `contextWindow`, `supportsImages`, `supportsTools`, `requestOverrides`. Capabilities default to unknown/false. Options merge shallowly, model over provider. Overrides cannot replace `model`, `messages`, `stream`, `tools`, `system`, or `max_tokens`. Other options are provider-native; the operator must verify supported names/values, including reasoning effort.

Optional `responseAlias` controls the model label returned to clients without
changing the upstream model ID or its declared capabilities.

Model pool fields: `models` (nonempty IDs), `selection` (`fixed-order` default, or `sticky-health`), `minimumContextWindow`, `requireTools`, `requireImages`. Requirements apply to every member. Selection cannot reorder chain stages. Use separate provider IDs to isolate gateway backend preferences; performance aggregates by provider/upstream model.

Chain fields: `steps`, `allowPaidFallback` (false), `paidFallbackOn`, `minimumContextWindow`, `requireTools`, `requireImages`. Each step has exactly one `model` or `pool`. Chains cannot reference chains, so cycles are not expressible. Repeated model IDs are rejected.

`paidFallbackOn` accepts `quota-exhausted` and `confirmed-provider-outage`; omission with paid opt-in defaults to quota only. Every visited non-metered leg needs an allowed evidenced reason before crossing the paid boundary. After entering an authorized paid stage, configured paid alternatives remain available. A chain starting with a metered model is paid from its first attempt.

## Optional browser snapshots

Omitted usage or `{"mode":"passive"}` performs no collection. API mode is explicit: `{"mode":"api","pollIntervalSeconds":60}`. Readers exist for Claude, Ollama, ClinePass, and xAI. Unsupported readers fail validation with instructions to use passive mode. Reader failure does not establish inference failure. Ollama's reader uses the origin's `/api/usage` path; other readers retain their provider-native endpoint conventions.

Browser mode reads only a configured file:

```json
{"mode":"browser","snapshotPath":"./state/provider-usage.json","snapshotKey":"my-go","maxAgeSeconds":180}
```

Snapshot format:

```json
{
  "version": 1,
  "providers": {
    "my-go": {
      "status": "ok",
      "fetchedAt": "2026-01-01T12:00:00Z",
      "utilization": 0.25,
      "windows": {"weekly":{"percentUsed":25,"reset":"in 2d 3h"}}
    }
  }
}
```

Use current RFC3339 timestamps, utilization fraction 0..1, and window percentages 0..100. Future timestamps are rejected. Files are bounded to 1 MiB; valid snapshots are cached for five seconds. Missing/invalid/stale non-exhausted telemetry does not block inference. Losing a collector preserves its last known exhaustion until a reported reset or 24 hours without reset evidence, plus two minutes' grace. Disabling browser mode stops consulting the source, without erasing independent inference-derived blocks.

Go reads configured snapshot files at startup and every 15 seconds, including while inference is idle. This does not launch a browser or contact a provider. Collectors, browser/dashboard login, installation, and scheduling are outside this project. Never store cookies, tokens, raw webpages, or prompts in snapshots. A collector reports evidence; Go owns routing decisions.

For Ollama API usage, optional `sessionThresholdPct`, `weeklyThresholdPct` and
`reserveUpstreams` reserve remaining allowance for specific configured upstreams.
At either configured threshold, other models are skipped; reserved models remain
eligible until real exhaustion. Values must be in 0..100; zero disables that
threshold. Requires API mode and at least one threshold and reserve upstream.
These deliberate quota skips qualify as quota evidence for an explicitly enabled
paid fallback, but authentication or network failures do not.
