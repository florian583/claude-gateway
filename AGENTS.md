# Local proxy development

- Keep tests and development isolated from running services and real credentials.
- JSON config is versioned and strictly validated. Runtime structs are compiled implementation details.
- Keep direct account management separate from the proxy-facing Claude client.
- Tests use temporary directories, fake credential commands, and loopback mock servers. Never invoke real login, Keychain, inference providers, browsers, or production listeners from tests.
- Browser telemetry is opt-in. Unknown usage does not mean exhausted or healthy/full quota.
- Preserve sequential fallback, account isolation, cancellation, bounded streams, and no replay after client-visible output. No speculative duplicate requests.
- Never commit credentials, personal account/config fixtures, runtime state, or logs. Commit only reviewed source paths.
- Use a GitHub handle and GitHub no-reply email for both author and committer. Install `.githooks` with `make hooks`; never bypass a privacy-check failure. See CONTRIBUTING.md.
- Use neutral account labels and visibly synthetic credentials in tests. Do not copy real account data into fixtures.
- Before handoff: `make check build` (includes staged files and reachable-history privacy checks).
- Menu integration: `make menubar-check menubar` and `python3 -B scripts/menubar_smoke.py`. Keep `/dashboard` observation-only; do not refresh credentials, probe providers, or alter routing from display polling. No private configuration or live service changes in tests.
