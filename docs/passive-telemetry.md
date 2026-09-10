# Passive request telemetry

Telemetry observes existing attempts only. It adds no concurrency caps, waiting
queues, busy responses, inference probes, or fallback policy changes.

`GET /status` includes `upstreamConcurrency.total` and `scopes`, grouped by
provider, Claude account/profile, actual upstream model, configured chain, and
request path. Counts span attempt preparation/credential resolution, headers,
and response-body lifetime; they are not exclusively active token streams.
Completed/canceled scopes disappear. Status reads never refresh usage or auth.
For non-Claude providers, account is empty and provider defines the scope.

Existing `stateDir/metrics.jsonl` attempt/completion records gain `execution`:

- `number`: actual execution ordinal, excluding skipped candidates. Existing
  `attempt` remains the zero-based candidate index, which includes skips.
- `previous`: preceding executed provider/account/model, candidate index,
  HTTP status and classified failure reason. A policy skip never becomes the
  preceding execution. Follow `requestId` across attempt/skip/completion/terminal
  records to reconstruct the whole decision sequence.
- `beforeAttemptMs`: cumulative time since chain start, before this attempt;
  includes earlier failed attempts and routing work. Not pure network latency.
- `activeTotal`, `activeAccount`, `activeModel`, `activeChain`: snapshots at
  attempt start, including the current attempt. Model count is account-scoped;
  chain count spans its providers/accounts. These are not concurrency limits.
- `httpVersion`, `httpStatus`, `upstreamRequestId`: response protocol, original
  upstream status before normalization, and bounded correlation ID when supplied.
- `connectionReused`, `connectionIdleMs`, `firstResponseByteMs`: transport
  observations, omitted when unavailable. Millisecond values may round to zero.
- `transportError`, `responseReadError`: safe transport-error category and failed
  error-body-read flag. Existing `failureReason`/stream fields cover fallback,
  cancellation, protocol errors, and stream stalls.

Existing connection/DNS/TLS/header/first-event timings now accompany failed
attempts as well as completions. Connection acquisition includes DNS/TLS when a
new connection is needed; these timings overlap and must not be added together.
Policy skips have no execution or inherited network timings. Snapshots are copied
before asynchronous persistence; no prompt, tool arguments, credentials, or raw
transport errors are added by this instrumentation. Existing opt-in error-body
tracing is separate and may contain sensitive payloads.

Gateway diagnostics add `execution.responseMetadata`: bounded, allowlisted
response identifiers and explicitly named provider/backend fields. Keys retain
their source (`header.x-backend`, `body.error.metadata.provider_name`, etc.).
`header.x-vercel-id` and `header.cf-ray` are correlation identifiers, not inference
backend identities. Missing metadata means unknown, never inferred from `server`
or an error message. Error metadata is extracted only from bodies already read by
the fallback classifier; no extra upstream requests or body reads are added.

Completion records include `streamStalled`, `missingStop`, `protocolError`, and
`lastEventAgeMs`. These distinguish a long silence followed by a synthetic missing
stop from an immediately malformed response. Existing classifications remain
unchanged. Failed attempts/completions also emit a bounded structured `upstream
failure diagnostics` line in proxy.log with route, execution, backend metadata,
event counts and timing; no new raw prompt/tool/error-body capture is enabled.
