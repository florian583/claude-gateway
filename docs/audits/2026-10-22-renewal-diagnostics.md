# Credential renewal diagnostics

The reported weekly simultaneous logout remains unproven. A helper defect is reproduced independently: textual millisecond expiry values were parsed as seconds. A near-expiry credential could therefore skip scheduled refresh. Numeric millisecond values were already normalized correctly. Textual values now use the same normalization.

Each helper profile status now retains its own checkedAt, the last CLI exit code, attempt duration, and up to 16 attempt records. The records contain timestamps, normalized expiry, enumerated state/error and numeric exit/duration fields. They never contain credentials, CLI output or prompt content. File permissions remain 0600. The aggregate status preserves individual check times so a newly written aggregate does not hide old profile observations. These additional diagnostics are in the helper status file; the proxy's existing public status projection does not expose them.

The helper calculates remaining lifetime after the CLI finishes. It still accepts token rotation even when a model request fails. A nonzero CLI exit does not prove token revocation: quota limits and transient failures can produce it too.

Regression coverage uses synthetic credentials and fake commands: missing expiry, expired token rotation despite a model error, textual millisecond expiry, valid dormant profile, concurrent refresh exclusion, transient failure, CLI deadline, and bounded attempt history. Existing Go tests cover dormant monitoring, unknown expiry, queue cancellation and verified versus unverified refresh outcomes.

This does not prove the weekly cause or simulate macOS sleep. The monitor checks expired profiles after resuming. The helper's directory-lock stale threshold remains time-based; a process suspended longer than that threshold can overlap with another helper after stale-lock recovery. External Claude clients also refresh outside the helper lock. These need direct evidence before attributing provider invalidation to them. Revoked refresh tokens still require login.

For the next incident, inspect individual checkedAt values and bounded attempts before invoking login. Distinguish credential-unavailable, timeout, unchanged expiry and completed rotation. Never collect token values or raw CLI output for diagnostics.
