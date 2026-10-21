# Devin SWE-2 adapter (experimental, opt-in)

`cmd/devin-adapter` exposes OpenAI Chat Completions over loopback HTTP and translates
to Devin's private Connect/protobuf inference protocol. It does **not** launch Devin
CLI, run an agent loop or execute tools. The calling client owns tool execution.

This is a private, reverse-engineered protocol, not an official supported inference
API. The wire metadata version is pinned to the CLI version used for the probe.
Server-side protocol or entitlement changes can break it independently of this repo.

## Build and run

```sh
go build -trimpath -o bin/devin-adapter ./cmd/devin-adapter
```

Authenticate separately using Devin CLI. The adapter reloads
`~/.local/share/devin/credentials.toml` before each request. A nonempty
`WINDSURF_API_KEY` overrides that file; unset it if CLI credential rotation should
take effect. Neither credential is written or refreshed by the adapter. File parsing
supports root `windsurf_api_key` as a single-line quoted TOML string; advanced TOML
string forms and inline comments on that key are not supported.

Set a separate random local client key in `DEVIN_ADAPTER_API_KEY` (at least 16
characters) using your existing secret manager. Both adapter and gateway must receive
the same local key. **Do not use the Devin credential as the local client key.**

```sh
./bin/devin-adapter --listen 127.0.0.1:48105
```

Options: `--credentials PATH`, `--request-timeout 10m`, `--listen IP:PORT`.
Only literal loopback bind addresses are accepted. All `/v1/*` endpoints require
`Authorization: Bearer <local-key>`. `/healthz` is unauthenticated liveness only;
it never reads credentials or contacts Devin. No browser CORS access is enabled.

Shutdown on SIGINT/SIGTERM cancels active requests. Model thinking is bounded by the
total request deadline, not a separate 120-second idle watchdog. Slow downstream
writes have a 30-second bound. Request disconnect cancels the upstream HTTP request,
including while it is silent. Remote billing cessation cannot be guaranteed.

## Connect the gateway

See [standalone example](../examples/adapters/devin.json). Validate before use:

```sh
./bin/claude-proxy --config examples/adapters/devin.json validate-config
./bin/claude-proxy --config examples/adapters/devin.json serve
```

For an existing config, add the example provider/model/chain and a **new opt-in
alias**, preserving other routes. Do not advertise `[1m]`: this adapter declares a
262,144-token model window, not a million-token model. The byte limit is not a
tokenizer or guarantee that a request fits the upstream context.

The provider uses `billing: local` solely because the gateway requires that label
for loopback HTTP. **Devin usage is not free merely because this label is local.**
Actual subscription/credit/overage rules remain Devin's. Do not add it to an automatic
fallback chain on the assumption it is unmetered. No usage/quota polling is performed.

## Supported surface

- `GET /v1/models`: `swe-2-high` only.
- `POST /v1/chat/completions`: streaming SSE and non-streaming JSON.
- Text messages; text-only content arrays; system/developer instructions.
- Native function tools with JSON object schemas and matching tool-result history.
- Single/sequential tool-fragment assembly; explicit-ID fragments can be associated.
  Ambiguous ID-less interleaving fails closed rather than executing the wrong tool.
- Upstream input/output counts when both are observed; SSE usage chunk after finish
  when `stream_options.include_usage` is true. No estimated/cache token fabrication.
- `max_tokens` or `max_completion_tokens` (1–32768, default 4096), temperature greater
  than zero through two, and `top_p` greater than zero through one.
- Reasoning text is exposed as `reasoning_content`. The gateway's current Anthropic
  translator does not render that extension as an Anthropic thinking block.

Unsupported options return HTTP 400: images/documents, structured response formats,
forced tool choice, parallel-tool policy controls, strict schema enforcement,
nonempty stop sequences, multiple choices, and unknown request fields. Tool schemas
and descriptions are preserved, not stripped to avoid upstream policy checks.

Only observed finish semantics are trusted. A completion count equal to the requested
output limit maps conservatively to `length`; unknown finish enums are errors. This
is not proof that upstream always enforces the requested limit.

## Failures and safety

No automatic retries, credential refresh, queues or provider fallback. Gateway owns
sequential fallback. Before output, Connect status maps to HTTP status; after output,
SSE emits an error **without** a success finish or `[DONE]`. Truncated and malformed
streams are errors, not completed answers. Redirects are never followed.

Bounds: 4 MiB client body, 2 MiB encoded/decompressed frame, 32 MiB response and
decoded content budget, 65,536 frames, 256 tools/calls, 2 MiB accumulated arguments
per tool. Telemetry records request ID, model, timing, chunk count, outcome and known
usage; never prompts, credentials, tool arguments or upstream error text.

`permission_denied` with the upstream MCP-configuration diagnostic is classified as
`tool_configuration_rejected`. This is distinct from missing local authentication.
No signature hiding, tool-description removal or policy-bypass workaround is applied.

## Verification

```sh
go test -race ./internal/devin ./cmd/devin-adapter
go test ./... -timeout 120s
```

Tests use synthetic credentials, mock transports and loopback servers only. Coverage
includes streaming, JSON, native tool history, ID-less fragments, gzip/frame bounds,
credential rotation, error redaction, no retry, redirect rejection, concurrent request
isolation and cancellation during a silent upstream stream.

Live probe: direct OpenAI HTTP -> SWE-2-high text/tool/result/usage succeeded. Real
`claude-hybrid` through a separate gateway reached the provider but was rejected with
`permission_denied` before tool execution. Therefore **not yet validated as a working
Claude Code coding provider**. Keep opt-in; do not deploy it as a fallback replacement.
