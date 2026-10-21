# Performance and telemetry policy

These controls do not introduce admission limits or replay requests. Account quota eligibility remains authoritative. Stream-idle limits are unchanged. Explicit soft-concurrency configuration can skip busy routes for new requests.

## Optional paid load spill

`softConcurrency` requires `allowPaidFallback` and defaults to zero (disabled). At the threshold, a new request skips a busy provider/model for the next configured route. The final route is never skipped for load. This is a snapshot-based preference, not a cap; simultaneous arrivals can overshoot. Skips emit `load_spill` telemetry. It is concurrency-based, not an adaptive latency controller.

Chains opting into `response-header-timeout` reserve header time for paid backups too: a 90-second three-provider chain targets 40/25/25 seconds. Authorization is still checked separately. `route-unavailable` explicitly permits paid fallback for instability blocks and open circuits; unknown quota/auth states remain excluded. Local address-assignment errors are classified as network failures, not provider outages. No post-output failover is permitted.

## Header budgets

Header performance uses `responseHeadersMs`, not total generation duration. A provider header timeout contributes its applied budget as a censored lower bound. Chain-limited timeouts, local-network failures, quota skips, and cancellations do not train this budget.

Learning requires at least 20 header observations (or the configured adaptive minimum, if higher). The learned budget is twice p95 plus two seconds. It can expand the existing model baseline but never shrink it. Existing input-size multipliers and provider ceilings still apply; the remaining fallback-chain budget can further constrain an attempt. This is conservative route-level learning, not a context-matched latency predictor.

## Phase telemetry

Attempt execution records add:

- `accountSelectionMs`: initial account-selection duration, including eligibility/cache checks; omitted when selection did not run.
- `transformMs`: request transformation for this candidate.
- `headerBudgetMs`: applied header deadline.
- `chainRemainingMs`: maximum supplied by caller; omitted when unrestricted. For token-count probes this can be a probe limit rather than a fallback-chain limit.
- `chainLimited`: whether that caller limit reduced the candidate budget.
- `requestWrittenMs`: time from transport attempt start to successful request write; nullable so a sub-millisecond write differs from no completed write.

Existing connection, DNS, TLS, first-byte, first-event, first-content, stream-gap and total-duration fields remain. These phases overlap; do not add them together. HTTP authentication retries may produce multiple trace callbacks; request-write timing reflects the latest successful write. No request bodies or credentials are added to telemetry.

## Sequential fallback reserves and Ollama stream priming

Fallback header reserves use explicit provider billing (`subscription` or `local`), not adaptive ranking tiers. Each distinct later unmetered provider targets a 25-second reserve; repeated models/accounts on the same provider do not multiply it. If insufficient time remains, divide the remaining budget evenly across the current and later providers rather than starving the current attempt. Paid routes do not reserve time or bypass existing cost authorization. A 90-second chain with Ollama, BigModel and OpenCode Go initially permits at most 40 seconds for Ollama, preserving 50 seconds for the remaining subscription routes. This preserves an opportunity, not a guarantee that a provider responds within its remaining window.

Ollama priming buffers known metadata-only SSE frames before committing the stream. An error before semantic content follows the existing sequential fallback/error classification. Text, reasoning, tool starts, terminal events or unknown event shapes commit conservatively; later errors cannot cause replay. Prefix buffering remains bounded at 256 KiB.

Empty/null message-start content and empty text/thinking deltas remain metadata. A metadata-only Anthropic response must contain a stop reason followed by message_stop before it is accepted; a bare stop or EOF is retryable before downstream commitment. `execution.primedFrames` counts inspected frames and `execution.primeLastFrame` identifies the last frame using a closed category vocabulary. Neither captures response content. These cases are covered by synthetic regressions; previous production failures lacked raw frames and cannot be claimed as exact reproductions.

The initial-event timeout is unchanged. After a valid metadata event, priming uses the existing provider stream-idle allowance (not a new short thinking timeout). Metadata waiting is tracked as `execution.preludeMs` and excluded from subsequent providers' header reserve. The first-event metric still marks the initial valid event, not completion of priming. Disabling stream-idle protection also disables extended metadata priming. No body/error-message capture is introduced.

## Soft account balancing

Existing eligible-account selection prefers lower in-flight load. Temporary parallel spills retain the original conversation lease for cache reuse. When the chosen account has at least four fewer active requests than the sticky account, move the lease to the selected account. Selection and reservation remain under the account mutex. A sole eligible account remains usable at any concurrency; this is not a cap or queue. Material quota-headroom switching remains unchanged.

## Bounded retention

Keep 32 closed metric segments instead of one. Rotate at the existing sample count or 8 MiB, checked after each asynchronous flush. Remove closed segments older than 72 hours during rotation. Disk capacity is approximately 256 MiB plus the active segment and flush overshoot; high traffic may shorten coverage. This is not a guaranteed 72-hour minimum, nor a strict wall-clock deletion deadline while idle.

In-memory rings and restart loading remain limited to the current and newest archived segment. Older archives are for offline audit, not dashboard refresh or routing training. Read every `metrics.jsonl.N` needed for historical analysis and filter by each record's timestamp; do not interpret segment modification time as request time.

Tests use temporary files and loopback servers. Building does not restart the service. Restart the intended local service separately to activate changes.
