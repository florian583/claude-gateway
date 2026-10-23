# Exact-model testing routes

Worker aliases permit model substitution. Do not use them to compare model behavior.
Prepare separate routes with:

```sh
python3 scripts/configure_rig_routes.py CONFIG_PATH CANDIDATE_PATH
bin/claude-proxy --config CANDIDATE_PATH validate-config
```

The command creates a new private file. It does not overwrite the active configuration,
contact providers, or restart a service. It preserves existing worker chains and aliases.
Review the candidate before activation. After a coordinated proxy restart, set the
rig's `LOCAL_LLM_BASE_URL` to `http://127.0.0.1:48105`. The dedicated listener accepts
`/v1/messages` and `/v1/messages/count_tokens`, plus read-only `/health` and `/routes`.
Its `/routes` response lists only rig models. The normal listener remains unchanged.
The rig listener is loopback-only and disabled unless `rigListen` and `rigModels` are
configured together. Administrative endpoints are not exposed on that listener.

Send normal model IDs: `claude-haiku-4-5`, `claude-haiku-4-5-20251001`,
`claude-haiku-5-5`, `claude-sonnet-5-5`, and `claude-opus-5-5`.
The legacy `claude-opus-5` name is an explicit compatibility mapping to Opus 5.5;
record the upstream-model response header when labeling evidence. The explicit `rigModels`
map selects private rig aliases. Unknown IDs fail instead of entering a worker chain.
The normal endpoint rejects these private aliases, preventing accidental use of rig
policy by Claude Hybrid. The listener is enabled only with explicit configuration.
Responses include `X-Proxy-Usage: rig`; response.model retains the requested model ID.

Each route has one model, uses its existing account pool, and permits no cross-model
fallback. The dated Haiku route requests that actual upstream ID with a 200,000-token
context declaration. Configuration validation does not prove account entitlement or
model availability. An unsupported model must fail explicitly.

The endpoint separates routing and timing policy. It still shares the proxy process,
provider accounts, and subscription quotas with normal traffic. It is not a separate
capacity reservation or an authentication boundary.

These routes set `firstContentTimeoutMs: 30000` and `requestTimeoutMs: 60000`.
The first-content budget includes upstream header time for each attempt. Metadata and
reasoning frames cannot reset it. Buffered data retains its original order and has the
existing prelude byte limit. Text or a tool start commits the stream; later failures
cannot trigger replay. The request deadline bounds generation across attempts, including
the response body. Set application retry budgets below the product turn deadline too:
several 60-second application retries are not a 60-second turn.

Before headers, exhausted routes return an error: rate limits retain 429; malformed or
empty responses return 502; the request deadline returns 504. After headers, a deadline
interrupts the stream; it cannot replace the already-sent HTTP status. Consumers must
check stream completion, not just HTTP 200. Strict non-streaming responses also enforce
an explicitly requested tool name. Streaming tool arguments remain validated by the
existing adapter; a tool start alone cannot prove the final arguments are valid.

Response headers identify `X-Proxy-Request-ID`, `X-Proxy-Upstream-Model`, and
`X-Proxy-Upstream-Provider`. The response body's model field remains the requested alias
for client compatibility. Record the headers when correlating test evidence.

Do not restart a shared proxy during a replay. Coordinate activation of the binary,
configuration, and application environment. First verify `/routes` has one exact upstream
and no model fallbacks for each rig alias; then run a small labeled canary before a full
replay. An unavailable account is an infrastructure failure, not a model-quality result.

## Subscription request compatibility

Only rig requests routed to a `claude-subscription` provider receive the Claude
client wire profile. Native Claude envelopes pass through. API-key providers and
normal Claude Hybrid requests retain their existing behavior.

The adapter follows the OAuth compatibility rules in VibeProxy's CLIProxyAPI
backend: the beta query, client headers, billing and identity envelope, stable
session metadata, and model-aware caller instruction placement. Modern rig models
carry caller instructions in system turns after the leading user turns. Dated
Haiku uses system reminders in the first user message. Text and cache markers
survive the move. Non-text system blocks fail locally rather than disappearing.

Forced tools remove incompatible thinking and effort controls. Adapted requests
remove temperature and top_p; active thinking also removes top_k and omits the
thinking display. Tool schemas, output formats, max_tokens, stream mode, exact
model routing, and rig deadlines remain caller-owned. No upstream model fallback
is injected. This adapter is a compatibility profile, not a verbatim implementation
of every VibeProxy feature.

Caching stays enabled. Existing breakpoints own placement. Requests without a
breakpoint receive one at the last text block. Missing TTLs use one hour, or five
minutes after an existing five-minute breakpoint. Explicit TTLs remain intact.
More than four breakpoints or a one-hour breakpoint after a five-minute breakpoint
fails locally. The matching extended-cache beta accompanies adapted requests.
Stable request metadata avoids introducing a fresh cache identity on every retry.

Reference implementation audited at CLIProxyAPI revision
`07b5a7a1ca267c5d9b743e38dff4ed8ff1da0c91`, under
`internal/runtime/executor/claude_executor_{cloaking,request,execute}.go`.
VibeProxy's blanket cache stripping is deliberately excluded. Verify cache creation
and cache-read usage on repeated requests after each wire-profile update. A short
successful answer alone does not prove cache reuse or structured-tool compatibility.

A 2026-10-22 live canary returned an upstream 400 for forced `tool_choice: tool`
on Opus 5.5 and Sonnet 5.5, with the response stating that `tool` and `any` are
not supported for those models. Haiku 5.5 accepted the forced choice. All three
models streamed successfully, and Opus and Sonnet returned tool calls with
`tool_choice: auto`. The rig preserves the requested choice and upstream error;
it does not weaken the constraint or switch models.

## Empty stream recovery

Rig streams distinguish metadata-only `message_delta` frames from terminal
frames. A terminal frame without text or a tool call records an allow-listed
stop reason and bounded output-token count, without recording prompt or reasoning
text. A normal empty `end_turn` may try one further eligible account for the same
model before any output is committed, inside the existing whole-request deadline.
Token exhaustion, refusals, unknown endings, and ordinary Hybrid streams do not
use this retry. No new account is created and no model fallback is introduced.
Rig response-header timeouts return504 before headers instead of generic502.
