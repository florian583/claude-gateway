# Claude Gateway

A local gateway for Claude Code. Route requests through Claude accounts and API providers using configurable account pools, model aliases, and fallback chains.

- Anthropic Messages and OpenAI Chat Completions upstreams.
- Sequential account/provider failover, quota reserves, and session affinity.
- Streaming text and tool calls, with cancellation and interrupted-stream handling.
- Optional usage APIs and browser snapshots. Neither is required for routing.
- Optional [macOS menu app](docs/menubar.md) with live activity, remaining usage, and local reset times.

## Requirements

- Go 1.27 or later when building from source; not required for packaged binaries.
- Claude Code installed and available as `claude`.
- macOS for Claude subscription credentials, which use Keychain. API-key providers do not require Keychain.

## Quick start

### 1. Build and install

Download a matching archive and `.sha256` file from [Releases](https://github.com/florian583/claude-gateway/releases).
`darwin` means macOS; `arm64` is Apple Silicon and `amd64` is Intel/x64.
Linux packages support API providers; Claude subscription credentials require macOS Keychain.
Verify the archive before extracting, then install its `claude-proxy` executable
into a directory on your `PATH`. For example, on Apple Silicon:

```sh
shasum -a 256 -c claude-proxy-darwin-arm64.tar.gz.sha256
tar -xzf claude-proxy-darwin-arm64.tar.gz
mkdir -p "$HOME/.local/bin"
install -m 755 claude-proxy-darwin-arm64/claude-proxy "$HOME/.local/bin/claude-proxy"
```

Packages are not Apple-notarized. Checksums detect corruption, not publisher identity;
download both files from this repository's release. The optional menu app is built separately.
Until a versioned release is published, download packages from a successful
[Package workflow](https://github.com/florian583/claude-gateway/actions/workflows/package.yml)
run (GitHub sign-in required; artifacts expire after 14 days), or build from source:

```sh
git clone https://github.com/florian583/claude-gateway.git
cd claude-gateway
make check build
mkdir -p "$HOME/.local/bin"
install -m 755 bin/claude-proxy "$HOME/.local/bin/claude-proxy"
```

Ensure `$HOME/.local/bin` is on your `PATH`. The executable remains `claude-proxy`. Python 3 is needed only for repository privacy checks, not to run the proxy.

### 2. Create a configuration

```sh
claude-proxy init
```

This creates `~/.config/claude-proxy/config.json` with a `work` account and a separate proxy client. Edit `models.worker.upstream` to use a model available on your account, then validate:

```sh
claude-proxy validate-config
```

The default listener is `127.0.0.1:48104`. Change `listen` if that port is occupied. Use `--config PATH` before the command, or set `CLAUDE_PROXY_CONFIG`, to select another configuration.

### 3. Add and log in accounts

```sh
claude-proxy profiles login work
claude-proxy profiles add backup --display-name "Backup"
claude-proxy profiles login backup
claude-proxy profiles status
```

`profiles add` creates a new settings directory, adds the profile to the default Claude account pool, and prints login and alias commands. It does not open a browser or log in. Switch browser profile before each login if needed.

For a specific pool or executable:

```sh
claude-proxy profiles add secondary --pool claude --command /path/to/claude
```

Existing profile names and directories are never overwritten. For an already configured account, add its existing directory or executable wrapper under `profiles` in the JSON instead.

### 4. Start the proxy

```sh
claude-proxy serve
```

Leave it running in this terminal. Restart it after configuration changes.

### 5. Create separate account and proxy aliases

Add these to `~/.zshrc` or `~/.bashrc`, then open a new terminal:

```sh
# Direct account access: login, /usage, or a normal account session.
alias claude-work='claude-proxy profiles open work --'
alias claude-backup='claude-proxy profiles open backup --'

# Daily work through the proxy.
alias claude-routed='claude-proxy run --'
```

| Command | Connection | Settings directory |
| --- | --- | --- |
| `claude-work` | Work account directly | `profiles.work.configDir` |
| `claude-backup` | Backup account directly | `profiles.backup.configDir` |
| `claude-routed` | Local proxy; account chosen by routing policy | `client.configDir` |

Use the account aliases for account management. Use the proxy alias from any project:

```sh
cd /path/to/project
claude-routed
claude-routed --model worker
claude-routed --resume
```

The launcher preserves the project directory and forwards Claude arguments. It sets the proxy URL and a non-secret local token, while removing inherited upstream credentials. The proxy supplies the selected provider's credentials. Do not log in to a subscription through `claude-routed`; use `profiles login` or an account alias.

## Bring your existing Claude preferences and memory

Already use Claude Code? Import your old profile into the gateway's separate
`client.configDir` instead of starting from scratch:

```sh
# Preview only; creates nothing.
claude-proxy migrate-client --from "$HOME/.claude"
# Stop Claude sessions using either directory, review the preview, then apply.
claude-proxy migrate-client --from "$HOME/.claude" --apply
```

For another gateway config, prepend `--config /path/to/config.json`. Destination
comes from that config's `client.configDir`, not a hardcoded `claude-hybrid` folder.
The proxy service does not need a restart; open a new routed Claude session afterward.

Imports instructions, skills, agents, commands, rules, hooks, project memory and
sessions, plans, todos, history, MCP definitions, and selected user/project preferences.
Reads both profile-local `.claude.json` and the legacy sibling location (prefers local).
Existing files are retained; settings/MCP JSON merges missing keys only, recursively.
Existing arrays and values win. Original merged JSON files get a private backup
under the destination; source files are never changed. Reruns are safe to preview.

Account credentials, old model/auth/routing settings, plugin registrations/caches,
and unrelated runtime data are **not** imported. Reinstall plugins through Claude's
plugin manager. Nested symlinks are reported and skipped; linked skills need a
separate reviewed copy/install. Project paths are retained, not relocated. Review
copied hooks, MCP commands, permissions, and absolute paths before launching; MCP
configuration and conversation history can contain secrets. Keep migration data local.
Repository-level `.claude/` files stay in their projects and need no copy.

Large history/session files are streamed, not loaded together into memory.
Migration is not a whole-directory transaction: a filesystem error can leave some
files imported. Stop destination sessions, inspect the reported backup, and rerun
the preview. Existing non-JSON file conflicts are left for manual reconciliation.

## Agent-assisted setup

Copy this prompt into your coding agent:

```text
Set up Claude Gateway on this computer from https://github.com/florian583/claude-gateway.
Read README.md and docs/configuration.md from the selected release/checkout first.

Inspect OS, architecture, Claude Code installation, existing profiles, shell aliases,
and occupied ports. Preserve existing settings and running sessions. Ask only for
missing choices: accounts/providers to use, model aliases, and whether paid fallback
is allowed. Do not assume model IDs, capabilities, subscriptions, or credentials.

Prefer a matching release binary and verify its SHA-256 checksum; otherwise build
from source with the documented Go version and run the checks. Keep configuration,
credentials, and runtime data outside the repository. Never print tokens or commit
personal settings. On Linux use supported API providers, not macOS Keychain profiles.

Create or reuse explicitly selected account profiles. Use profiles add and profiles
login for new accounts; let me complete interactive login. Keep direct account access
separate from the proxy client directory. Create non-conflicting aliases for direct
account management and daily proxy use (claude-proxy run --).

Configure exact model aliases and sequential fallback chains. Keep usage collection
passive by default, browser collection off, and metered fallback off unless I approve.
Explain optional API usage polling and workerUtilizationLimitPct if I want quota reserves.

Validate config, start only one loopback listener, and verify /health and /status.
With my approval, send one small test through the proxy and confirm its actual route.
Do not claim login or inference works from config validation alone. Ask before
installing an autostart service or the optional menu app. Finish with paths, aliases,
start/stop instructions, verification results, and any remaining login steps.
```

## Client configuration

The `client` section controls the Claude Code instance that consumes the proxy. It is separate from account profiles:

```json
"client": {
  "command": ["claude"],
  "configDir": "./client",
  "model": "worker"
}
```

`model` is a configured alias, not an upstream model ID. `--model` overrides it for one session. Optional `opusModel`, `sonnetModel`, and `haikuModel` map Claude's model choices to different configured aliases; each defaults to the session model.

The client gets its own settings, sessions, and plugins. Its directory cannot also be an account directory. Configure any client-specific plugins there; account credentials stay in their own profile's Keychain entry.

Shell aliases are not executable commands. In JSON, use `["claude"]`, an absolute executable path, or an executable wrapper with its actual `configDir`.

## Providers and routing

Examples:

- [Claude account pool](examples/claude-pool.json)
- [API-key providers](examples/api-only.json)
- [Subscription and metered fallbacks](examples/fallbacks.json)

Providers declare their protocol, endpoint, authentication source, and billing type. Models declare exact upstream IDs and capabilities. Chains define order; aliases select a chain.

Non-primary metered providers require explicit paid-fallback permission. Missing usage, authentication errors, and local network failures do not authorize paid fallback. Model capabilities are declared, not inferred: use valid image/tool support and context limits for every fallback.

## Usage and credentials

Usage collection defaults to `passive`. Routing still handles real quota errors without a usage API. Optional API polling and local browser snapshots are configured per provider; no browser or collector is launched automatically.

Fable-specific limits do not disable other Claude models. An old Fable limit can be revalidated by one real inference per account per hour when fresh global quota headers allow requests. This handles allowance changes after an upgrade without background inference or duplicate requests. A complete successful Fable response clears the old limit; failures retain it. The menu labels partial restrictions `FABLE LIMIT`, with the model reset time in its tooltip.

Claude Code manages login credentials. The proxy rereads rotated credentials and can use an explicitly configured `refreshCommand` for unattended refresh. A successful helper must actually rotate credentials; no hidden inference probe is used.

Set per-profile or pool thresholds to preserve quota. Proactive reserves need fresh usage data; unknown usage is never presented as a full balance.

## Operations

See [passive request telemetry](docs/passive-telemetry.md) for concurrency,
connection timing, and correlated fallback/error records. Observation only: no
request caps or queues.

Temporary HTTP error capture is available with top-level `errorTrace`: set
`enabled: true`, an explicit `providers` array, and an RFC3339 `expiresAt`.
`captureBodies` defaults to false (metadata only); setting it to true stores the
actual transformed upstream request and error response, including potentially
sensitive conversation text, code, and tool arguments. Do not publish captures.
Authentication/cookie headers and URL query strings are never captured; only
protocol/correlation headers are allowlisted. Bodies are not secret-redacted.

Captures live in `stateDir/error-traces` (directory 0700, files 0600), with eight
rotating slots. Requests are capped at 2 MiB, errors at 64 KiB, and records at
16 MiB. Truncation, incomplete reads, request hash, proxy request ID, upstream
request ID, and attempt index are recorded. Capture expires automatically, but
existing files remain until rotated or manually removed. Concurrent disk writes
are skipped rather than queued. Successes, transport failures without an HTTP
response, and HTTP-200 SSE errors are not captured. No extra upstream reads or
requests are made. `proxy.log` contains only capture references, not trace bodies.

Status endpoints: `/health`, `/status`, `/routes`, `/routing`, `/metrics`, and `/catalog`. `/dashboard` is a versioned, cached-only display endpoint for the menu app. `/quota` exposes Ollama-specific quota data and may refresh its cache.

For OpenCode Go, configure `variant: "opencode-go"`. The gateway adds `x-opencode-session` and identifies itself as `claude-gateway/1` for either protocol. An explicit incoming session header is preserved. Otherwise Claude session metadata is hashed into a stable, non-identifying ID before adapter rewrites. Clients without session metadata get a best-effort first-message fingerprint; send an explicit header to distinguish identical prompts or preserve identity across compaction. Generate that ID once per conversation, not per request. Direct HTTPie/curl calls bypassing this gateway still need their own header. [OpenCode Go requirements](https://github.com/anomalyco/opencode/blob/337fd144d2ba144743368f78d9579a99cce175bd/packages/web/src/content/docs/go.mdx).

Keep configuration, credentials, snapshots, and runtime logs outside Git. Bind only to loopback; the listener is not an authentication boundary between local processes and must not be exposed through a tunnel. Ctrl-C or SIGTERM allows active requests to drain. Client-visible partial streams are not replayed.

Use credentials in accordance with [provider terms](https://code.claude.com/docs/en/legal-and-compliance). Claude subscription credentials have restrictions on third-party integrations; use supported API credentials where required. A redistribution license has not yet been selected.

See [configuration](docs/configuration.md), [architecture](docs/architecture.md), [testing](docs/testing.md), [contributing safely](CONTRIBUTING.md), and the [local migration checklist](docs/local-migration.md). Claude Code's [gateway guide](https://code.claude.com/docs/en/llm-gateway) documents the underlying client environment variables.
