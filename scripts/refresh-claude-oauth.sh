#!/bin/bash
# shellcheck disable=SC2016,SC2329

set -euo pipefail

CONFIG_PATH="$HOME/.config/claude-proxy/config.json"
STATUS_PATH="$HOME/.config/claude-proxy/claude-oauth-refresh-status.json"
STATE_DIR="$HOME/.config/claude-proxy/oauth-refresh"
SECURITY_BIN="/usr/bin/security"
CLAUDE_BIN="$HOME/.local/bin/claude"
JQ_BIN="$(command -v jq || true)"
PERL_BIN="/usr/bin/perl"
REFRESH_WINDOW_SECONDS=1800
COOLDOWN_SECONDS=600
LOCK_STALE_SECONDS=600
CLI_TIMEOUT_SECONDS=120
SELECT_CONFIG_DIR=""
SELECT_SERVICE=""
AUTH_ERROR=0
AUTH_VERIFIED=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--config)
			CONFIG_PATH="$2"
			shift 2
			;;
		--config-dir)
			SELECT_CONFIG_DIR="$2"
			shift 2
			;;
		--credentials-service)
			SELECT_SERVICE="$2"
			shift 2
			;;
		--auth-error)
			AUTH_ERROR=1
			shift
			;;
		*)
			exit 64
			;;
	esac
done

if [[ -n "$SELECT_CONFIG_DIR" || -n "$SELECT_SERVICE" ]]; then
	[[ -n "$SELECT_CONFIG_DIR" && -n "$SELECT_SERVICE" ]] || exit 64
fi
if [[ "$AUTH_ERROR" -eq 1 ]]; then
	[[ -n "$SELECT_CONFIG_DIR" && -n "$SELECT_SERVICE" ]] || exit 64
fi

# Test-only dependency injection. launchd never sets this gate.
if [[ "${CLAUDE_OAUTH_TEST_MODE:-}" == "1" ]]; then
	SECURITY_BIN="${CLAUDE_OAUTH_SECURITY_BIN:-$SECURITY_BIN}"
	CLAUDE_BIN="${CLAUDE_OAUTH_CLAUDE_BIN:-$CLAUDE_BIN}"
	JQ_BIN="${CLAUDE_OAUTH_JQ_BIN:-$JQ_BIN}"
	STATE_DIR="${CLAUDE_OAUTH_STATE_DIR:-$STATE_DIR}"
	STATUS_PATH="${CLAUDE_OAUTH_STATUS_PATH:-$STATUS_PATH}"
	CLI_TIMEOUT_SECONDS="${CLAUDE_OAUTH_CLI_TIMEOUT_SECONDS:-$CLI_TIMEOUT_SECONDS}"
fi

[[ -r "$CONFIG_PATH" && -x "$SECURITY_BIN" && -x "$CLAUDE_BIN" && -x "$JQ_BIN" && -x "$PERL_BIN" ]] || exit 69
[[ "$CLI_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || exit 64

umask 077
/bin/mkdir -p "$STATE_DIR/locks" "$STATE_DIR/cooldowns" "$STATE_DIR/profiles"
RUN_DIR="$(/usr/bin/mktemp -d /private/tmp/claude-oauth-refresh.XXXXXX)"
CURRENT_LOCK=""
cleanup() {
	if [[ -n "$CURRENT_LOCK" ]]; then
		/bin/rmdir "$CURRENT_LOCK" 2>/dev/null || true
	fi
	/bin/rmdir "$RUN_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

now_epoch() {
	/bin/date +%s
}

iso_at_epoch() {
	"$JQ_BIN" -nr --argjson epoch "$1" '$epoch | todateiso8601'
}

profile_key() {
	/usr/bin/printf '%s\000%s' "$1" "$2" | /usr/bin/shasum -a 256 | /usr/bin/awk '{print $1}'
}

read_credential_expiry() {
	local service="$1"
	"$SECURITY_BIN" find-generic-password -s "$service" -w 2>/dev/null |
		"$JQ_BIN" -ce '
			def expiry_epoch:
				if type == "number" then (if . > 100000000000 then . / 1000 else . end)
				elif type == "string" then (try (tonumber | expiry_epoch) catch fromdateiso8601)
				else error("invalid expiresAt") end;
			select(.claudeAiOauth.accessToken | type == "string" and length > 0)
			| (try (.claudeAiOauth.expiresAt | expiry_epoch | floor) catch 0) as $epoch
			| {expiresAt: ($epoch | todateiso8601), expiresEpoch: $epoch}
		'
}

previous_field() {
	local path="$1"
	local field="$2"
	if [[ -r "$path" ]]; then
		"$JQ_BIN" -r --arg field "$field" '.[$field] // empty' "$path" 2>/dev/null || true
	fi
}

write_profile_status() {
	local path="$1"
	local name="$2"
	local state="$3"
	local expires_at="$4"
	local remaining="$5"
	local last_attempt="$6"
	local last_success="$7"
	local error_code="$8"
	local checked_at="$9"
	local tmp previous
	previous=/dev/null
	[[ ! -r "$path" ]] || previous="$path"
	tmp="$(/usr/bin/mktemp "${path}.tmp.XXXXXX")"
	"$JQ_BIN" -n --slurpfile previous "$previous" \
		--arg cliExitCode "${cli_exit_code:-}" \
		--arg attemptDurationSeconds "${attempt_duration:-}" \
		--arg name "$name" \
		--arg state "$state" \
		--arg expiresAt "$expires_at" \
		--arg remainingSeconds "$remaining" \
		--arg lastAttemptAt "$last_attempt" \
		--arg lastSuccessAt "$last_success" \
		--arg error "$error_code" \
		--arg checkedAt "$checked_at" '
		{
			name: $name,
			checkedAt: $checkedAt,
			state: $state,
			expiresAt: (if $expiresAt == "" then null else $expiresAt end),
			remainingSeconds: (if $remainingSeconds == "" then null else ($remainingSeconds | tonumber) end),
			lastAttemptAt: (if $lastAttemptAt == "" then null else $lastAttemptAt end),
			lastSuccessAt: (if $lastSuccessAt == "" then null else $lastSuccessAt end),
			error: (if $error == "" then null else $error end),
            cliExitCode: (if $cliExitCode == "" then ($previous[0].cliExitCode // null) else ($cliExitCode | tonumber) end),
            attemptDurationSeconds: (if $attemptDurationSeconds == "" then ($previous[0].attemptDurationSeconds // null) else ($attemptDurationSeconds | tonumber) end)
        }
        | . as $current
        | .attempts = ((($previous[0].attempts // []) +
            (if $cliExitCode != "" then [$current | {checkedAt,lastAttemptAt,state,error,cliExitCode,attemptDurationSeconds,expiresAt}] else [] end)) | .[-16:])' >"$tmp"
	/bin/chmod 600 "$tmp"
	/bin/mv -f "$tmp" "$path"
}

write_cooldown() {
	local path="$1"
	local epoch="$2"
	local tmp
	tmp="$(/usr/bin/mktemp "${path}.tmp.XXXXXX")"
	/usr/bin/printf '%s\n' "$epoch" >"$tmp"
	/bin/chmod 600 "$tmp"
	/bin/mv -f "$tmp" "$path"
}

run_official_cli() {
	local config_dir="$1"
	(
		cd "$RUN_DIR"
		/usr/bin/env \
			-u ANTHROPIC_BASE_URL \
			-u ANTHROPIC_AUTH_TOKEN \
			-u ANTHROPIC_API_KEY \
			CLAUDE_CONFIG_DIR="$config_dir" \
			"$PERL_BIN" -e 'alarm shift @ARGV; exec @ARGV or exit 127' "$CLI_TIMEOUT_SECONDS" \
			"$CLAUDE_BIN" \
			--print \
			--model haiku \
			--max-turns 1 \
			--tools "" \
			--mcp-config '{"mcpServers":{}}' \
			--strict-mcp-config \
			--no-session-persistence \
			--disable-slash-commands \
			--safe-mode \
			--restricted \
			--no-chrome \
			--permission-mode dontAsk \
			'Respond only with OK.'
	) >/dev/null 2>/dev/null
}

all_profiles="$($JQ_BIN -c '
	if .version != 1 then error("expected portable config v1") else . end
	| (.profiles // {}) | to_entries
	| map({name: .key, configDir: (.value.configDir // ""), credentialsService: (.value.credentialsService // "")})
	| map(select(.name != "" and .configDir != "" and .credentialsService != ""))
	| reduce .[] as $profile
		({profiles: [], dirs: {}, services: {}};
			if ((.dirs[$profile.configDir] // false) or (.services[$profile.credentialsService] // false))
			then .
			else (.profiles += [$profile]
				| .dirs[$profile.configDir] = true
				| .services[$profile.credentialsService] = true)
			end)
	| .profiles[]
' "$CONFIG_PATH")"

active_status_files=()
matched=0
had_error=0
while IFS= read -r profile_json; do
	[[ -n "$profile_json" ]] || continue
	name="$($JQ_BIN -r '.name' <<<"$profile_json")"
	config_dir="$($JQ_BIN -r '.configDir' <<<"$profile_json")"
	service="$($JQ_BIN -r '.credentialsService' <<<"$profile_json")"
	key="$(profile_key "$config_dir" "$service")"
	profile_status="$STATE_DIR/profiles/$key.json"
	active_status_files+=("$profile_status")

	if [[ -n "$SELECT_CONFIG_DIR" && ("$config_dir" != "$SELECT_CONFIG_DIR" || "$service" != "$SELECT_SERVICE") ]]; then
		continue
	fi
	matched=$((matched + 1))
	cli_exit_code=""
	attempt_duration=""
	checked_epoch="$(now_epoch)"
	checked_at="$(iso_at_epoch "$checked_epoch")"
	last_attempt="$(previous_field "$profile_status" lastAttemptAt)"
	last_success="$(previous_field "$profile_status" lastSuccessAt)"
	lock_dir="$STATE_DIR/locks/$key.lock"

	lock_acquired=0
	if /bin/mkdir "$lock_dir" 2>/dev/null; then
		lock_acquired=1
	else
		lock_mtime="$(/usr/bin/stat -f %m "$lock_dir" 2>/dev/null || /usr/bin/printf '0')"
		if [[ "$lock_mtime" =~ ^[0-9]+$ ]] && ((checked_epoch - lock_mtime > LOCK_STALE_SECONDS)); then
			/bin/rmdir "$lock_dir" 2>/dev/null || true
			if /bin/mkdir "$lock_dir" 2>/dev/null; then
				lock_acquired=1
			fi
		fi
	fi
	if [[ "$lock_acquired" -ne 1 ]]; then
		write_profile_status "$profile_status" "$name" locked "" "" "$last_attempt" "$last_success" "" "$checked_at"
		continue
	fi
	CURRENT_LOCK="$lock_dir"

	if ! credential="$(read_credential_expiry "$service")"; then
		write_profile_status "$profile_status" "$name" credential_unavailable "" "" "$last_attempt" "$last_success" keychain_read_failed "$checked_at"
		had_error=1
		/bin/rmdir "$CURRENT_LOCK" 2>/dev/null || true
		CURRENT_LOCK=""
		continue
	fi
	expires_at="$($JQ_BIN -r '.expiresAt' <<<"$credential")"
	expires_epoch="$($JQ_BIN -r '.expiresEpoch' <<<"$credential")"
	if ! [[ "$expires_epoch" =~ ^[0-9]+$ ]]; then
		write_profile_status "$profile_status" "$name" invalid_expiry "" "" "$last_attempt" "$last_success" invalid_expires_at "$checked_at"
		had_error=1
		/bin/rmdir "$CURRENT_LOCK" 2>/dev/null || true
		CURRENT_LOCK=""
		continue
	fi
	remaining=$((expires_epoch - checked_epoch))
	if [[ "$AUTH_ERROR" -ne 1 ]] && ((remaining > REFRESH_WINDOW_SECONDS)); then
		write_profile_status "$profile_status" "$name" valid "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
		/bin/rmdir "$CURRENT_LOCK" 2>/dev/null || true
		CURRENT_LOCK=""
		continue
	fi

	cooldown_path="$STATE_DIR/cooldowns/$key"
	last_cooldown="$(/bin/cat "$cooldown_path" 2>/dev/null || /usr/bin/printf '0')"
	if [[ "$AUTH_ERROR" -ne 1 && "$remaining" -gt 0 && "$last_cooldown" =~ ^[0-9]+$ ]] && ((checked_epoch - last_cooldown < COOLDOWN_SECONDS)); then
		write_profile_status "$profile_status" "$name" valid "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
		/bin/rmdir "$CURRENT_LOCK" 2>/dev/null || true
		CURRENT_LOCK=""
		continue
	fi

	write_cooldown "$cooldown_path" "$checked_epoch"
	last_attempt="$checked_at"
	previous_expires_epoch="$expires_epoch"
	cli_succeeded=0
	attempt_started="$(now_epoch)"
	if run_official_cli "$config_dir"; then
		cli_exit_code=0
		cli_succeeded=1
		AUTH_VERIFIED=1
	else
		cli_exit_code=$?
	fi
	completed_epoch="$(now_epoch)"
	attempt_duration=$((completed_epoch - attempt_started))
	checked_epoch="$completed_epoch"
	checked_at="$(iso_at_epoch "$checked_epoch")"
	if credential="$(read_credential_expiry "$service")"; then
		expires_at="$($JQ_BIN -r '.expiresAt' <<<"$credential")"
		expires_epoch="$($JQ_BIN -r '.expiresEpoch' <<<"$credential")"
		remaining=$((expires_epoch - checked_epoch))
		if ((expires_epoch >= previous_expires_epoch + 60)); then
			# Claude can refresh OAuth before returning a non-zero model result
			# (for example, a valid account whose session quota is exhausted).
			last_success="$checked_at"
			write_profile_status "$profile_status" "$name" refreshed "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
		elif [[ "$cli_succeeded" -eq 1 && "$AUTH_ERROR" -eq 1 ]]; then
			# Direct HTTP success proves auth recovery even when Claude keeps
			# the same still-valid credential expiry.
			last_success="$checked_at"
			write_profile_status "$profile_status" "$name" recovered "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
		elif [[ "$cli_succeeded" -eq 1 && "$remaining" -gt "$REFRESH_WINDOW_SECONDS" ]]; then
			last_success="$checked_at"
			write_profile_status "$profile_status" "$name" refreshed "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
		elif [[ "$AUTH_ERROR" -ne 1 && "$remaining" -gt 0 ]]; then
			# Claude does not rotate every still-valid token proactively. A
			# successful probe, or a quota-limited probe with valid credentials,
			# is not an authentication failure.
			if [[ "$cli_succeeded" -eq 1 ]]; then
				last_success="$checked_at"
				write_profile_status "$profile_status" "$name" valid "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
			else
				write_profile_status "$profile_status" "$name" valid "$expires_at" "$remaining" "$last_attempt" "$last_success" claude_cli_failed_credential_valid "$checked_at"
			fi
		else
			error_code=claude_cli_failed
			[[ "$cli_succeeded" -eq 1 ]] && error_code=expiry_not_extended
			write_profile_status "$profile_status" "$name" refresh_failed "$expires_at" "$remaining" "$last_attempt" "$last_success" "$error_code" "$checked_at"
			had_error=1
		fi
	else
		write_profile_status "$profile_status" "$name" refresh_failed "" "" "$last_attempt" "$last_success" keychain_read_failed "$checked_at"
		had_error=1
	fi
	/bin/rmdir "$CURRENT_LOCK" 2>/dev/null || true
	CURRENT_LOCK=""
done <<<"$all_profiles"

generated_epoch="$(now_epoch)"
generated_at="$(iso_at_epoch "$generated_epoch")"
status_tmp="$(/usr/bin/mktemp "${STATUS_PATH}.tmp.XXXXXX")"
existing_status_files=()
for status_file in "${active_status_files[@]}"; do
	[[ -r "$status_file" ]] && existing_status_files+=("$status_file")
done
if ((${#existing_status_files[@]} > 0)); then
	"$JQ_BIN" -s --arg checkedAt "$generated_at" --arg generatedAt "$generated_at" '
		{
			checkedAt: $checkedAt,
			generatedAt: $generatedAt,
			profiles: (map({key: .name, value: (del(.name))}) | from_entries)
		}
	' "${existing_status_files[@]}" >"$status_tmp"
else
	"$JQ_BIN" -n --arg checkedAt "$generated_at" --arg generatedAt "$generated_at" \
		'{checkedAt: $checkedAt, generatedAt: $generatedAt, profiles: {}}' >"$status_tmp"
fi
/bin/chmod 600 "$status_tmp"
/bin/mv -f "$status_tmp" "$STATUS_PATH"

if [[ -n "$SELECT_CONFIG_DIR" && "$matched" -eq 0 ]]; then
	exit 66
fi
if [[ -n "$SELECT_CONFIG_DIR" && "$matched" -eq 1 && "$had_error" -eq 0 && "$AUTH_VERIFIED" -eq 1 ]]; then
	"$JQ_BIN" -n --arg profile "$name" '{version:1,authVerified:true}'
fi
exit "$had_error"
