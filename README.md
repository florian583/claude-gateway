# Claude Gateway

A local gateway for Claude Code. Route requests through Claude accounts and API providers using configurable account pools, model aliases, and fallback chains.

- Anthropic Messages and OpenAI Chat Completions upstreams.
- Sequential account/provider failover, quota reserves, and session affinity.
- Streaming text and tool calls, with cancellation and interrupted-stream handling.
- Optional usage APIs and browser snapshots. Neither is required for routing.

## Requirements

- Go 1.27 or later.
- Claude Code installed and available as `claude`.
- macOS for Claude subscription credentials, which use Keychain. API-key providers do not require Keychain.

## Quick start

### 1. Build and install

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

Claude Code manages login credentials. The proxy rereads rotated credentials and can use an explicitly configured `refreshCommand` for unattended refresh. A successful helper must actually rotate credentials; no hidden inference probe is used.

Set per-profile or pool thresholds to preserve quota. Proactive reserves need fresh usage data; unknown usage is never presented as a full balance.

## Operations

Read-only endpoints: `/health`, `/status`, `/routes`, `/routing`, `/metrics`, and `/catalog`. `/quota` exposes Ollama-specific quota data.

Keep configuration, credentials, snapshots, and runtime logs outside Git. Bind only to loopback; the listener is not an authentication boundary between local processes and must not be exposed through a tunnel. Ctrl-C or SIGTERM allows active requests to drain. Client-visible partial streams are not replayed.

Use credentials in accordance with [provider terms](https://code.claude.com/docs/en/legal-and-compliance). Claude subscription credentials have restrictions on third-party integrations; use supported API credentials where required. A redistribution license has not yet been selected.

See [configuration](docs/configuration.md), [architecture](docs/architecture.md), [testing](docs/testing.md), and [contributing safely](CONTRIBUTING.md). Claude Code's [gateway guide](https://code.claude.com/docs/en/llm-gateway) documents the underlying client environment variables.
