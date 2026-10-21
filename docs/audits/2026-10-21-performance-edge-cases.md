# Go proxy performance and edge-case audit

Audited source: `6a14806610e2947e34e24c6204328b4592b8e3a1`, the deployed gateway revision on both Macs. Audit branch: `audit/proxy-performance-20261021`. Production code and configuration were not changed. Tests use temporary files and loopback mock providers. Production evidence comes only from existing metric files; this audit sent no provider requests.

The main priorities are bounded body reads, correct failure reporting, and removing disk synchronization from the account-selection lock. A rewrite is not justified by the measured request-transformation cost alone.

## Implementation follow-up

All six findings now have fixes and regression coverage. The findings below describe the original audited revision, and its line numbers are historical. The four audit tests now assert the corrected behavior. Requests and buffered responses have byte and time limits. JSON read and parse failures reach fallback before client output. Account pins use one ordered asynchronous writer outside the selection lock, and both transient pins and metric persistence queues are bounded. See `docs/configuration.md` for defaults and durability limits.

Post-fix local benchmark: two 20-operation runs at 10000 durable pins measured 0.317–0.327 ms per new-pin update. This measures map updates and enqueue latency with the writer running concurrently, not completion of durable storage. It must not be interpreted as an fsync throughput result. The original synchronous path measured 42–265 ms per update across runs.

## Observed traffic

The sample starts at 12:35 UTC, after both deployments. The collector read at most 8 MiB from each of the active and most recent archived metric files. Counts are attempt/completion records, not distinct requests. Timings are milliseconds. Account-selection duration can repeat across the attempts of one request. These short, observational samples are not a controlled host comparison or a capacity test.

| Host/provider | Records | Account selection p50 / p95 / max | Transform p50 / p95 / max | Response headers p50 / p95 / max |
| --- | ---: | --- | --- | --- |
| Studio / Anthropic | 84 | 2 / 364 / 990 | 3 / 12 / 41 | 1798 / 3215 / 4380 |
| Studio / Ollama | 8 | 232 / 847 / 847 | 0 / 2 / 2 | 1977 / 2371 / 2371 |
| Mini / Anthropic | 71 | 0 / 0 / 30 | 1 / 2 / 3 | 1654 / 3169 / 3971 |
| Mini / Ollama | 192 | 4 / 4 / 4, one observation | 1 / 2 / 48 | 2047 / 3962 / 12213 |

Three Studio Anthropic records have no response-header measurement. Provider response time dominates most observations. Studio account selection has a substantial tail. Synchronous pin persistence is a confirmed mechanism that can add latency; this sample does not prove what proportion of that tail comes from persistence rather than credential/cache checks or lock contention.

## Confirmed findings

### P1 — A stalled error body can stop fallback indefinitely

`anthropic-proxy.go:8179` stops the header timer after headers arrive. The 429, 5xx and client-error paths then read error bodies at lines 7791, 7853 and 7933. Their byte limits do not impose a time limit. The normal request path creates a cancellable context at line 6592, without a total request deadline. The fallback-chain budget is checked between attempts; it cannot interrupt these reads.

`TestAuditErrorBodyOutlivesHeaderDeadline` sends 429 headers immediately and withholds the body. With a 20 ms header budget, no fallback occurs after 200 ms. Releasing the body allows exactly one backup call and a successful response. The finite wait proves the unprotected read; indefinite blocking follows from the absence of a body deadline, not from an infinitely long test.

Successful non-streaming JSON responses have the same absence of a body timeout at lines 9263 and 10603. SSE idle protection does not cover those reads.

Fix: apply separate bounded error-body and JSON-body read deadlines that obey cancellation and the remaining pre-response budget. Keep SSE generation independent of a blanket response timeout. Test a peer that sends headers but never sends a body, as well as a peer that dribbles bytes.

### P1 — Truncated JSON is forwarded and recorded as success

`newJSONFilterReadCloser` at `anthropic-proxy.go:9262` and the OpenAI equivalent at line 10602 discard `io.ReadAll` errors and return the partial bytes through a reader that ends with ordinary EOF. The proxy has already written the upstream status at line 6638. Completion accounting therefore has no read error to classify.

`TestAuditTruncatedJSONRecordedAsSuccess` uses a real loopback HTTP server that advertises a 1000-byte JSON response but closes after an incomplete prefix. The proxy returns HTTP 200 with invalid JSON, and the completion metric has `Success: true`.

Fix: carry read and parse errors to completion accounting. Because JSON is already buffered, validate it before committing downstream headers. Decide whether a pre-output failure can use the existing fallback policy; never retry after client-visible output. Cover both Anthropic and OpenAI JSON adapters, including a full-length but malformed JSON response.

### P1 — Request and JSON-response memory use has no byte ceiling

`normalizeRequest` reads all input at `anthropic-proxy.go:8227`. Both JSON adapters also read without a limit. The main handler has no `MaxBytesReader` wrapper. The server at line 1140 sets a header timeout but no request-body read deadline. SSE has separate bounded line/frame readers; this finding does not apply to those buffers.

A large or never-ending request can retain a connection and continue allocating before route compatibility is checked. A large non-streaming upstream response can do the same. An out-of-memory failure was not induced during this audit.

Fix: enforce explicit request and buffered-response limits, with limits chosen to accommodate supported large-context and image payloads. Return 413 for oversized input, reject oversized upstream bodies, and add a request-body deadline. Test chunked input, false Content-Length, and concurrent large bodies. A byte limit alone does not stop a stalled peer.

### P2 — New account pins serialize requests behind disk writes

`chooseClaudeAccountWithPolicy` takes `claudeAccountMu` at `anthropic-proxy.go:5406`. It calls `rememberClaudeAccountLocked`, which serializes the complete pin map and calls `tmp.Sync()` before releasing the lock (`account_affinity.go:92–145`). Other account selections and reservations need that mutex too.

The benchmark measured 42–265 ms average time per new pin across its runs. Reusing an existing pin took 0.40–0.62 microseconds. At 10000 saved pins, the new-pin path allocated approximately 1.7–2.8 MB per operation. Those results include encoding, file creation, synchronization, rename, and bookkeeping; they do not isolate fsync alone. Disk timings varied and should not be treated as guaranteed latency or converted into a production throughput limit.

Fix: update and snapshot the bounded pin state under the mutex, then serialize and persist through one coalescing writer outside the mutex. Preserve atomic replacement and version ordering so an older snapshot cannot overwrite a newer one. Explicitly define the crash-durability window and drain the writer on shutdown. Test slow/failing storage while existing conversations continue selecting accounts.

### P2 — Metrics retry buffering is unbounded during storage failure

The memory rings are bounded, but `metricsStore.pending` is not. `record` appends to it and `requeue` prepends failed flush batches (`anthropic-proxy.go:1951–2002`). Each failed flush retries the accumulated batch. Flush work can also queue behind the separate I/O mutex.

`TestAuditMetricsPendingGrowsOnDiskFailure` points the metrics path beneath an ordinary file, causing deterministic write failure. With `MaxSamples: 10`, the retry queue retains all 300 submitted records after three failed batches. The cap applies to the rings, not to pending writes.

Fix: use one bounded persistence queue with bounded batch sizes, backoff and an explicit dropped-record counter. Keep request completion independent of disk availability. Cover permission failure, full disk, recovery and shutdown. Preserve EventID semantics when retrying partial writes.

### P2 — Expired in-memory account pins have no global eviction

`rememberClaudeAccountLocked` inserts into `claudeAccounts` at `account_affinity.go:96`. The 10000-entry cap applies only to `claudePersistentPins`. Expired in-memory entries are removed when that same conversation key is looked up (`anthropic-proxy.go:5497`); abandoned keys receive no further lookup.

`TestAuditExpiredAccountPinsAccumulate` retains 10100 expired transient pins. This is a retention mechanism, not evidence of a measured production memory leak rate.

Fix: cap or periodically prune the transient lookup map without changing the durable pin-until-broken policy. A later lookup can restore a valid persistent pin. Cover many one-shot conversations and returning conversations after eviction.

## Local benchmarks

Apple M4 Max, Darwin arm64, Go benchmark parallelism suffix 16. The machine was shared with normal workloads. Three runs were used. Request benchmarks include construction of the test request and normalization; the three-attempt variant adds Haiku, Sonnet and OpenAI-format GLM transformations. They exclude network, auth, selection, response streaming and production concurrency. Allocated bytes are cumulative allocations per operation, not peak resident memory.

| Input text | Normalize only | Normalize plus three transformations | Allocations for three transformations |
| --- | --- | --- | --- |
| 1 KiB | 3.6–6.2 microseconds | 16.2–21.3 microseconds | about 18 KB/op |
| 64 KiB | 94–116 microseconds | 0.375–0.485 ms | about 585 KB/op |
| 1 MiB | 1.55–2.98 ms | 4.28–5.02 ms | about 9.6 MB/op |

This does not benchmark every 1-million-token request shape. Base64 images, tool histories and many small content blocks can have different costs. Optimizing repeated decoding and allocation could help large concurrent contexts, but the measured transformation latency is much smaller than observed provider response time and new-pin persistence.

## Existing protections checked

- Pinned worker fallback preserves one Claude account and stage order; default chains retain their distinct behavior.
- Provider transports reuse connections, with separate Claude-account pools.
- SSE line/frame reads are bounded before unlimited allocation. Priming has a separate bounded buffer.
- Existing tests cover fallback before semantic stream output, refusal to replay after semantic output, malformed/truncated SSE, idle closure, cancellation neutrality and reservation release.
- Existing tests cover paid-fallback evidence, header-budget reserves, credential-refresh serialization/cancellation, dormant profiles and verified renewal outcomes.

The full suite and race detector are the verification for those existing cases. This audit does not establish that every possible wire shape or concurrency interleaving is safe. In particular, downstream write stalls, authentication-command cold-start concurrency and long-running mixed streaming load remain candidates for a dedicated load test.

## Verification and reproduction

At the original audit commit, `proxy_audit_test.go` contained characterization tests whose PASS reproduced the defects. The implementation follow-up inverted those assertions into regression tests.

Commands, run from the isolated audit worktree:

```sh
go test ./... -run '^TestAudit' -count=1 -v -timeout 30s
go test -run '^$' -bench '^BenchmarkAuditRequestProcessing$' -benchmem -benchtime=500ms -count=3
go test -run '^$' -bench '^BenchmarkAuditPersistNewAccountPin$' -benchmem -benchtime=20x -count=3
go test -run '^$' -bench '^BenchmarkAuditPersistExistingAccountPin$' -benchmem -benchtime=500ms -count=3
make check build
```

Four reproductions passed. Benchmark commands passed. `make check build` passed: 28 Python tests, all Go tests, race tests, vet, example-config checks, staged/history privacy checks and the build. The Go test and race runs were uncached. No runtime fixes, deployment or provider-based load test formed part of this analysis.

## Recommended implementation order

1. Bound error and JSON body reads; preserve body errors; validate buffered JSON before committing success. Add request size and body-time limits in the same boundary review.
2. Move pin persistence outside the account mutex with ordered snapshots and shutdown tests.
3. Bound telemetry persistence and transient pin retention.
4. Run a controlled loopback load test with mixed short requests, large contexts, long streams, cancellations, slow clients and storage failures. Compare p50/p95/p99, allocations, goroutines and open descriptors before considering deeper transformation optimizations.

## Completion receipt

- Worktree directory: `claude-gateway-audit-20261021` (isolated audit checkout).
- Branch: `audit/proxy-performance-20261021`.
- Starting HEAD and production source reviewed: `6a14806610e2947e34e24c6204328b4592b8e3a1`.
- Changes: this report and `proxy_audit_test.go`; no production source changes.
- Source review covered main HTTP ingress/egress, fallback and header budgets, JSON/SSE adapters, account affinity, metrics persistence, transport pooling, refresh coordination and their existing tests.
- Four new loopback/temporary-file reproductions and two benchmark families ran successfully. Full checks exited zero.
- No PR or deployment was opened for the audit artifacts. Original deployment CI remains associated with the production revision.
