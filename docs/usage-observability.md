# Usage attribution

Gateway metrics and 90-day hourly rollups carry `session`, `workload`, `lane`,
`run`, `channel`, and `origin` when available. Session comes from existing
request metadata. Optional `X-Gateway-*` labels supply the other dimensions;
they are bounded identifiers and are stripped before upstream forwarding.
No prompt/body logging is added. Routing and quota policies are unchanged.

`python3 scripts/usage-report.py --hours 24 --group workload,session,model`
shows observed token totals, cache writes, average context, failures and
fallback records. `--json` exports machine-readable rows. Group by `lane,run`
for automation, `origin,session` for terminal sessions, or `profile,model` for
account usage. Rollups are hourly, so window boundaries round down to the hour.

Flags highlight repeated contexts over 500k, average context over 500k, and
failure/fallback churn. They are diagnostic heuristics, not automatic limits.
Partial failed-stream usage is included where observed. Unknown usage is not
zero billed usage. No subscription quota percentage or dollar conversion is
claimed. Fallback records are not unique client retries.

Old records cannot recover missing attribution/cache-write counts. Existing
sessions gain metadata-based identity when the new gateway starts; launcher
labels require a new process. Automation wrappers export inherited headers so
Claude requests from their children are tagged even if launched indirectly.
Traffic bypassing this gateway (including direct Codex/Devin calls) is outside
this report. Native provider telemetry is needed for total cross-provider usage.

At 10,000 live aggregate keys, new attribution dimensions collapse into an
explicit `attribution-overflow` bucket; raw metrics still retain identifiers.
The report sorts by output tokens, not estimated cost or subscription burn.

## Candidate accounting update (not activated)

`usage` preserves presence separately from zero for input, total input, cache
reads/creation, 5m/1h creation subsets, inclusive output and reasoning subset.
Anthropic input is uncached; total context requires all input components.
OpenAI chat prompt totals include cached reads; output includes reasoning.
Neither reasoning nor cache TTL subsets are added to their parent totals.
Rollups retain `usageKnown` coverage and `usageTotals`; legacy integer fields
alone cannot establish completeness. Failed partial usage stays included.

Requested, configured upstream and explicitly reported model remain distinct.
Optional lineage adds host, parentSession, worker, PR, head and stage. All
`X-Gateway-*` headers, including unknown future labels, are stripped upstream.
The staged `scripts/gateway-usage-env.sh` relabels inherited child headers and
preserves unrelated custom headers; do not install it against an old binary
that does not strip the new labels. Neither helper nor binary is activated by
this source change.

Hourly batches have stable retry IDs, fsync before advancing saved offsets,
visible persistence errors and incomplete-tail recovery. Readers/reports dedupe
identified retries. Metrics events get `eventId`, checked writes and fsync.
`claude-proxy rebuild-usage --source FILE [--source FILE] --out NEW_DIRECTORY`
reconstructs aggregates offline, without config, credentials or listeners.
It refuses existing destinations and malformed canonical events. Only supplied
persisted sources are reconstructed; in-memory crash gaps and expired/missing
source files remain unknown. Historical unidentified events cannot be safely
deduplicated. This is not a claim of zero-loss transactional event persistence.

Native Codex/Devin receipts live in the PR coordinator's separate indexed store.
Codex completed-turn usage is imported incrementally; Devin ATIF-v1.7 per-step
metrics are counted once and compared with (not added to) final totals. Kimi
token coverage is unavailable until a verified receipt schema exists. Requested
models are never substituted for missing reported models. Native and gateway
observations are alternative coverage, never added together without exact
cross-source request identities. No shared account balances are summed and no
subscription quota conversion is inferred from token prices.
