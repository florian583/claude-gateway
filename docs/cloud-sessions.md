# Claude Code cloud sessions

The model gateway and the cloud-session service have different account boundaries. Local inference can move between eligible subscription accounts or API providers. A cloud session keeps its owning subscription account for its whole lifetime.

`scripts/claude_cloud.py` reuses configured gateway profiles. It launches the native Claude CLI with upstream routing credentials and model overrides removed. It does not change the running proxy or copy credentials into a second store.

## Commands

Install the helper as `claude-cloud`, then use:

```sh
claude-cloud launch --profile work -- --cloud "Fix the failing tests"
claude-cloud list
claude-cloud bind <session-id-or-url> --profile work
claude-cloud send <session-id> --message-file /absolute/path/to/message.txt
claude-cloud read <session-id>
```

`launch --parent-session <local-uuid>` can select the parent's last successful subscription profile from bounded gateway metrics. An explicit `--profile` or `CLAUDE_CLOUD_PROFILE` takes precedence. A standalone launch can match the Hybrid login's account identity to one configured profile. Ambiguous or missing evidence requires an explicit profile.

The helper records each verified cloud ID and its owning profile in `<stateDir>/cloud-sessions/`. Records use mode 0600 and contain an account fingerprint, not a token. `session_` and `cse_` aliases resolve to the same record. Changing the current preference cannot change an existing session's owner. A missing profile or changed login fails explicitly.

`send` uses the documented native CLI follow-up command. Success means queued. It does not mean a model responded. A timeout or missing acknowledgment remains unknown and blocks further sends. Inspect the remote history, then use `resolve-send --delivery queued|not-queued --evidence "..."` only when evidence establishes the result.

`read` returns the latest 100 remote events' assistant text, the session status, worker status, and an older-events cursor. It makes no model call. Its bounded read-only adapter uses the Claude Code cloud control API shape from native 2.1.311. This is distinct from the Managed Agents API and its billing. API incompatibility, authorization and device-policy errors fail explicitly; they do not trigger another account.

## Local installation

Use stable installation paths so removing a development worktree does not remove the command or skill:

```sh
mkdir -p "$HOME/.local/lib/claude-hybrid" "$HOME/.local/bin"
install -m 755 scripts/claude_cloud.py "$HOME/.local/lib/claude-hybrid/claude_cloud.py"
ln -s "$HOME/.local/lib/claude-hybrid/claude_cloud.py" "$HOME/.local/bin/claude-cloud"
mkdir -p "$HOME/.claude/skills/claude-cloud-sessions"
install -m 644 skills/claude-cloud-sessions/SKILL.md "$HOME/.claude/skills/claude-cloud-sessions/SKILL.md"
```

Preserve existing commands before installation. Existing Hybrid launchers should dispatch `--cloud`, its deprecated `--remote` alias, and `--environment` to `claude-cloud launch -- "$@"` before setting inference-proxy variables or adding channel flags. Preserve the SSH desktop-Keychain handoff. Forward `CLAUDE_CLOUD_PROFILE` and `CLAUDE_CODE_SESSION_ID` as non-secret controls when that handoff uses an environment allowlist.

An orchestration launcher must also leave cloud invocations unchanged. Cloud session creation owns its ID and native channel setup. Inserting a local UUID, display identity or development channel can stop startup at an unrelated confirmation dialog.

Creation remains interactive. Native repository trust, organization policy, device approval and GitHub access checks remain in force. Use a named terminal or tmux pane and inspect its prompts. Do not clear nesting guards or automatically accept prompts.

## Validation

The helper's tests use temporary profiles, synthetic credentials and fake native commands. They cover fixed ownership, account changes, exact parent evidence, alias normalization, secret-free state, uncertain sends, malformed history and owned-child cleanup. A separate live verification used one disposable synthetic repository and one subscription profile: launch, automatic owner capture, initial result, one native follow-up and its result all succeeded. Changing the current profile preference before the last read retained the recorded owner. No existing user session received a message.

Official CLI behavior: [Move tasks between terminal and cloud](https://code.claude.com/docs/en/claude-code-on-the-web#move-tasks-between-terminal-and-cloud).
