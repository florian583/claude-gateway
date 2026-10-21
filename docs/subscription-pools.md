# Subscription pool operations

Routing stays config-driven and sequential. No speculative parallel inference,
credential refresh from dashboards, metered automatic rescue, or new scheduler.

## Retained plans and routing

- Claude subscriptions remain primary. Sonnet aliases use Haiku 5.5; Opus and
  Fable retain their native Claude slots.
- Opus/Fable exhaustion or unavailable auth falls back to the Kimi K3 pool.
- Normal Haiku aliases fall back to K2.7 Code, then GLM 5.3 Flash. The explicitly
  approved 1M aliases skip the 256k K2.7 pool and go directly to GLM Flash.
- Kimi Code native subscription auth, Ollama Cloud, Vercel Token Pass, OpenCode
  Go, BigModel annual, and Cline annual are retained. Credentials stay outside Git.
- Cline annual is first in the fixed-order GLM Flash pool. Never use Cline K3,
  GLM non-Flash, or DeepSeek. K2.7 Code is permitted by policy but not installed:
  the catalog-listed Cline model returned HTTP 404 in live verification.
- No OpenCode Free alias or provider is installed.

Native Kimi credentials remain in their original region-scoped CLI slot.
The owner explicitly selected `kimi-for-coding` for both native subscription
legs; this is an upstream override, not proof that the subscription serves two
different model versions. Other providers retain their exact model IDs.

## Bridge lanes

Run `python3 scripts/install_bridge_pools.py` on each host after installing the
pinned Pi ACP and Pi coding agent dependencies. It preserves other Bridge agents
and installs named fixed-model lanes:

| Bridge agent | Explicit Pi model |
| --- | --- |
| `pool-kimi-k27` | `gateway/kimi-k2.7-code` |
| `pool-glm-flash` | `gateway/glm-5.3-flash` |
| `pool-kimi-k3` | `gateway/kimi-k3` |

Do not pass a model override to the generic Pi agent and assume it took effect:
Bridge's generic ACP transport ignores that argument. These lanes pin provider
and model through the actual Pi command flags instead. An offline regression
test checks the generated commands. Pi JSONL `model_change` records establish
the actual selected model; task completion alone is insufficient.

Muse Spark Contributor currently requires OpenCode workspace permission to train
on request data. The API rejects it without that permission. Muse is disabled by
default, excluded from all automatic fallback, and not advertised as working.
Only after explicit owner privacy consent should the workspace setting change
and `install_bridge_pools.py --include-muse-training` be used. Then verify the
`pool-muse-go` lane independently. Never silently relax workspace privacy.

## Operator workflow

1. Run `make check build` in an isolated source snapshot. Tests use only temporary
   synthetic credentials and loopback mocks; live smokes are separate actions.
2. Use `reconcile_subscriptions.py` to validate a candidate against the native
   binary. Apply only with the exact current config SHA256. The script keeps a
   private backup, uses an exclusive lock, and refuses changed inputs/symlinks.
3. Transfer the candidate binary privately. `deploy_subscription_build.py`
   checks binary checksum, expected old source, candidate source and config,
   signs on the destination Mac, atomically replaces the resolved target
   (preserving Mini's symlink), restarts, and verifies the running `/status`
   source hash. Failed health restores the previous binary.
4. Deploy Studio first, then Mini. `subscription_smoke.py --gateway --stream`
   checks a completed translated stream; `subscription_receipt.py` reports
   bounded safe runtime/backend metadata. A healthy endpoint is not inference proof.
5. `bridge_pool_smoke.py` dispatches through remote Agent Bridge MCP, checks a
   static response sentinel, and ends only the session it owns. It requires
   explicit `--allow-live-dispatch`. No raw worker CLI execution.

If a restored auth helper reports a missing private fallback file, repair only
that exact credential slot with private 0600 storage. Do not copy native OAuth
tokens, expose helper output, wipe credentials, or infer all accounts logged out.

Keep unrelated dirty work and its index untouched. Review/commit the isolated
source patch; private runtime configs, logs, credentials and binaries do not
belong in the Git commit. PR automation is a separate, paused task.
