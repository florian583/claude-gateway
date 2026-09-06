# macOS menu app

The optional Swift/AppKit app lives in `menubar/` so the gateway API and client
can be tested and released together. It uses native system fonts and aligned
columns, without emoji. It does not run or manage the gateway process.

## Build and open

Requires macOS 13+, Xcode Command Line Tools, and Swift 5.9+.

```sh
make build menubar-check menubar
mkdir -p "$HOME/.config/claude-proxy"
# Run only when this menu configuration does not already exist.
cp -n menubar/example.json "$HOME/.config/claude-proxy/menubar.json"
open "bin/Claude Gateway Menu.app"
```

Run `claude-proxy serve` separately. The bundle has a local ad-hoc signature;
it is not a notarized download. Nothing is installed or auto-started by the build.
Use macOS Login Items if you want the built app to start at login. Keep the bundle
in a stable location first, and avoid registering more than one copy.

For another settings file or a headless integration check:

```sh
open "bin/Claude Gateway Menu.app" --args --config /path/to/menubar.json
"bin/Claude Gateway Menu.app/Contents/MacOS/GatewayMenu" --config /path/to/menubar.json --check
```

Quit an existing copy before opening with different arguments. The bundle refuses
a duplicate running instance with the same bundle identifier. This does not
detect an older, differently named companion app; stop that separately at cutover.

## Configuration

Menu configuration is separate from gateway credentials and routing configuration.
Default path: `~/.config/claude-proxy/menubar.json`. A missing default file uses the
defaults below. A missing explicit file, unknown keys, or invalid values is an error.
Use **Reload menu configuration** after editing; it does not reload the gateway.

| Field | Default | Meaning |
| --- | --- | --- |
| `version` | `1` | Menu config schema version |
| `gatewayURL` | `http://127.0.0.1:48104` | Loopback origin, including custom port; no credentials, path, query, or fragment |
| `refreshSeconds` | `15` | Display polling interval, 5–300 seconds |
| `showMeteredProviders` | `false` | Include metered APIs in the provider usage table |
| `accountOrder` | `[]` | Profile IDs first in this order; remaining IDs sorted |
| `providerOrder` | `[]` | Provider IDs first in this order; remaining IDs sorted |
| `maxTrafficRows` | `8` | Visible routes, 1–30 |

Account and provider names come from gateway `displayName`, falling back to the
configured ID. Model labels come from configured upstream model IDs, not a
hardcoded product list. Long labels truncate with full details in row tooltips.
Reset times follow the Mac's current timezone and locale (12/24-hour preference).

## Read-only contract

One bounded `GET /dashboard` per refresh; no overlapping poll. Redirects are
rejected, the response is capped at 1 MiB, and requests have an eight-second total
timeout. The app has no provider credentials, Keychain access, log readers, browser
collectors, or restart/login actions. **Refresh display** only reads existing state.

Dashboard schema `version: 1` includes current activity, account/provider usage,
reset timestamps, per-model status, and recent traffic. Unsupported API versions
and missing endpoints are reported explicitly. A failed refresh clears displayed
quota data; the next scheduled poll retries. It never keeps a stale green screen
after losing the gateway.

The endpoint projects cached gateway state without contacting providers or
refreshing credentials. Passive/no-API providers remain usable but display
`UNKNOWN` until supported telemetry is observed. Optional browser snapshots are
visible only after the gateway has consumed them; opening the menu never runs a
collector. Stale and already-reset windows are labelled `STALE`, not fresh/full.
`RESTRICTED` means some configured models have different availability; hover for
per-model states. A generic account percentage is not proof every model has quota.

Traffic includes completed responses and failed HTTP upstream attempts, excludes
policy skips/count-token/terminal bookkeeping, and deduplicates completion versus
attempt records. `FB` counts fallback legs, not unique user prompts. TPS averages
successful observed completion rates. Window: 30 minutes, bounded to retained
metrics (maximum 20,000 records); `retained sample` warns when the limit is hit.
Latest response is historical completion evidence, not a claim about every active
conversation. Idle means no successful completion in ten minutes.

Metered providers are hidden from the usage table by default, but their actual
traffic remains visible. No billing or payment data is fetched.

## Verification

```sh
make check build menubar-check menubar
python3 -B scripts/menubar_smoke.py
```

The smoke test starts an isolated gateway on a random loopback port with temporary
configuration and no real credentials, then runs the compiled Swift client against
its real dashboard response. It also checks offline detection and asserts no
provider request occurred. It does not launch a second menu in your desktop.
