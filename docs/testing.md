# Testing

Run the complete check locally:

```sh
make check build
```

This runs privacy-check tests, staged-file/history privacy checks, Go unit tests, the race detector, `go vet`, example validation, and a build. Python 3 is required for privacy checks. GitHub Actions fetches full history, runs the same checks on macOS, and rejects unformatted Go files.

## Test boundaries

Tests use temporary files, fake credentials, and loopback mock servers. `isolation_test.go` blocks real Claude and Keychain executables by default. Credential tests provide explicit fake executables. No login, paid inference, browser collection, or service installation is needed.

## Coverage

- Strict configuration validation, aliases, capabilities, and protocol paths.
- Account membership, quota reserves, affinity, and sequential failover.
- Metered-fallback evidence and local-network failure handling.
- Native and translated text/tool streams, cancellation, and interrupted responses.
- Optional telemetry, stale snapshots, and quota-reset recovery.
- Credential rotation, bounded helper output, and sanitized status responses.
- Separate account/client launchers, argument forwarding, and environment isolation.
- Profile creation, atomic updates, permissions, and overwrite protection.

Mock tests establish local behavior, not provider availability or account entitlement. Validate real credentials and upstream model capabilities separately. Use a separate config and free listener port for live integration tests.
