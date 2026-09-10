# Route health and shared-network failures

Instability quarantine is keyed by runtime provider/account plus the actual
upstream model, just like model circuit breakers. Client aliases do not create
separate health buckets. A gateway's failing GLM route cannot quarantine its
DeepSeek route or another gateway. Recovery for one model cannot clear another
model's quarantine. Candidate ranking, quarantine deferral and execution consult
the same route key.

Authentication and account quota remain separate account/provider state. Existing
Anthropic family-specific quota handling is unchanged. Quota errors are not
reclassified as transient route failures.

Generic response-header timeouts, resets, stalls and stream-error labels do not
establish a machine-wide network outage, even across two gateway origins. Global
offline state requires corroborated strong local-network errors, using the
existing origin deduplication/window and recovery probe mechanism. Model errors
retain sequential fallback and never cause replay after client-visible output.

Explicitly reported gateway backends remain in `reportedBackend` telemetry.
Current routing does not select backend-specific candidates, so quarantine uses
gateway + model, not an inferred Fireworks/Baseten identity. Backend-specific
exclusion requires a separately implemented gateway routing capability; a backend
reported after response startup alone is insufficient to select the next backend.
