# Run the repository build locally

Use this repository as the source for both gateway and menu. Keep machine-specific
profiles, credentials, provider settings, state, and logs outside the checkout.
Do not copy a live configuration into an example, issue, commit, or CI artifact.

An older private installation may have different routing policy and schema.
Copying its JSON or replacing its binary blindly is not a supported migration.

## Before cutover

1. Inventory the old listener(s), service registration, profile commands/directories,
   credentials services, provider protocols, model aliases, fallback order, context
   requirements, image/tool support, quota reserves, and refresh behavior.
2. Map those settings into the current [configuration schema](configuration.md).
   Reuse existing profile directories and credential services explicitly. Do not
   create replacement logins or run `profiles add` over existing accounts.
3. Audit feature parity. Check model-specific quota handling, account selection,
   paid-fallback gates, network recovery, adapters, and interrupted-stream behavior.
   Identify unsupported legacy settings rather than silently dropping them.
4. Build and run all Go/Swift/privacy tests. Validate the new private configuration.
   Start a candidate on another loopback port with a separate `stateDir` and client
   directory. Disable scheduled credential refresh and active usage polling during
   parallel comparison: two processes must not refresh the same account credentials.
5. Check dashboard display names, balances, reset times, unavailable telemetry,
   aliases and provider order against the intended configuration. Use bounded,
   explicitly approved inference tests for representative models/adapters. Do not
   replay ongoing conversations to multiple providers to compare performance.

## Cutover

Drain and stop the old gateway without dropping active streams. Stop the old menu
and its auto-start registration. Point the existing service registration to the
repository-built binary and private config, retaining the existing listener where
appropriate. Start exactly one gateway and one new menu. Verify process ownership,
listener, health, real generation, cancellation/fallback behavior, and dashboard.
Keep the immediately previous binary/config available until verification passes;
no elaborate backup system is necessary.

The menu config must target the new listener. It intentionally does not support
the older companion's private file paths or diagnostic log format.

## Ongoing improvements

Reproduce a runtime issue using sanitized inputs and temporary mock providers.
Add a regression test in this repository, implement the narrow fix, run CI, and
deploy the verified build. Compare `/status` source/config hashes with the expected
build and private configuration. Diagnose locally from logs; share only reduced,
redacted summaries. Never attach credentials, raw conversation payloads, account
identities, or private configuration to public issues.

This checklist is preparation, not evidence an existing installation has migrated.
