# Architecture

## Configuration

`config_file.go` validates JSON and compiles it into routing tables. References, model capabilities, account membership, and paid-fallback permissions are checked before the server starts.

| Section | Responsibility |
| --- | --- |
| `client` | Claude Code command, consumer settings directory, model defaults |
| `profiles` | Account settings directories, credential references, login and refresh commands |
| `accountPools` | Eligible accounts and quota reserves |
| `providers` | Endpoint, protocol, authentication, billing, optional usage collection |
| `models` | Upstream model IDs and capabilities |
| `modelPools` | Candidate selection within a chain stage |
| `chains` | Stage order and paid-fallback policy |
| `aliases` | Client-facing model names |

## CLI and account isolation

`cli.go` dispatches commands. `profile_setup.go` creates account profiles using a validated, atomic config update. `profile_cli.go` handles direct account login and commands. `client.go` launches a separate Claude Code configuration against the proxy.

Account commands clear inherited gateway credentials and model overrides. The consumer launcher uses a non-secret local token; incoming client credentials are stripped before upstream authentication is applied. macOS Keychain supplies Claude subscription credentials.

## Request flow

1. Resolve the requested alias and filter candidates by required capabilities.
2. Select an eligible account from the model's account pool.
3. Try candidates sequentially, respecting quota, circuits, and metered-fallback policy.
4. Forward native Messages or translate Chat Completions requests and responses.
5. Record outcomes, usage, and performance for subsequent requests.

The proxy does not send speculative duplicate generations. Network failures are distinguished from provider quota or authentication failures. Once output is visible to the client, interrupted streams are reported instead of being replayed.

## Telemetry

Usage collection is optional. Passive mode uses inference outcomes without polling. API readers and browser snapshots provide additional quota evidence when configured. The proxy owns routing and credential-refresh decisions; UI consumers read sanitized endpoints.

## Limits

- No configuration hot reload; restart after edits.
- One Claude subscription provider per process, with multiple account pools.
- Claude subscription credentials use macOS Keychain; other platforms are not release-tested.
- No generic website OAuth, OpenAI Responses adapter, monetary budget cap, or price discovery.
- Browser collectors, service installers, and a menubar UI are not included.

See [configuration](configuration.md) for fields and [testing](testing.md) for validation.
