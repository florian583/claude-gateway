# Fable weekly budget and model menu

Account pools support two optional policies:

- `fableMinWeeklyRemainingPct`: skip Fable for an account when its weekly remaining capacity is strictly below this percentage. Zero disables the threshold.
- `fablePaceGuard`: skip Fable when observed consumption predicts exhaustion before weekly reset. Requires observations spanning 15–30 minutes in the same quota window, with at least one percentage point consumed. History is in memory and warms up after restart.

Both policies use existing OAuth usage snapshots, ignore unknown, expired or overly stale observations, and leave other models eligible. Configure an Opus model after Fable in the chain to downgrade sequentially. A soft budget skip does not mark an account exhausted or authorize a metered fallback. Existing telemetry reports `fable_weekly_reserve` or `fable_weekly_pace_reserve` through the reserve fields.

`65` means 65% **remaining**, equivalent to more than 35% consumed. This is a heuristic cost policy, not a guarantee that Opus will last until reset. Accounts are evaluated individually, so a healthier account may still serve Fable.

Set `client.menuModels` to an object mapping existing aliases to display names. The config-only `GET /v1/models` endpoint publishes these entries. Enable `CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1` in the gateway client to add them to its picker while retaining built-in options. Existing sessions can select an alias directly with `/model <alias>`; restart the client to load changed launcher environment.
