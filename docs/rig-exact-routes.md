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
`claude-haiku-5-5`, `claude-sonnet-5-5`, and `claude-opus-5`. The explicit `rigModels`
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
