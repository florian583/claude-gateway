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

if [[ "${CLAUDE_OAUTH_REFRESH_REASON:-}" == scheduled ]]; then
	AUTH_ERROR=0
fi

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
	/bin/rm -f "$RUN_DIR"/metadata.json "$RUN_DIR"/cli-result.json
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

read_credential_metadata() {
	local service="$1"
	local codes
	if "$PERL_BIN" -e 'alarm 5; exec @ARGV or exit 127' "$SECURITY_BIN" find-generic-password -s "$service" -w 2>/dev/null |
		"$JQ_BIN" -cse '
			def expiry_epoch:
				if type == "number" then (if . > 100000000000 then . / 1000 else . end)
				elif type == "string" then (try (tonumber | expiry_epoch) catch fromdateiso8601)
				else 0 end;
			if length != 1 or (.[0] | type) != "object" then error("invalid credential") else .[0] end
			| (.claudeAiOauth // {}) as $oauth
			| if ($oauth | type) != "object" then error("invalid oauth") else . end
			| (try ($oauth.expiresAt | expiry_epoch | floor) catch 0) as $epoch
			| {keychainExitCode: 0, jsonValid: true,
			   accessTokenPresent: ($oauth.accessToken | type == "string" and length > 0),
			   refreshTokenPresent: ($oauth.refreshToken | type == "string" and length > 0),
			   expiresEpoch: $epoch, expiresAt: (try ($epoch | todateiso8601) catch null), error: null}
		' >"$RUN_DIR/metadata.json" 2>/dev/null; then
		codes=("${PIPESTATUS[@]}")
	else
		codes=("${PIPESTATUS[@]}")
	fi
	if [[ "${codes[0]}" -ne 0 ]]; then
		local error=keychain_read_failed
		[[ "${codes[0]}" -ne 44 ]] || error=keychain_item_missing
		"$JQ_BIN" -cn --argjson code "${codes[0]}" --arg error "$error" \
			'{keychainExitCode:$code,jsonValid:null,accessTokenPresent:null,refreshTokenPresent:null,expiresEpoch:0,expiresAt:null,error:$error}'
	elif [[ "${codes[1]}" -ne 0 ]]; then
		"$JQ_BIN" -cn '{keychainExitCode:0,jsonValid:false,accessTokenPresent:null,refreshTokenPresent:null,expiresEpoch:0,expiresAt:null,error:"credential_invalid_json"}'
	else
		/bin/cat "$RUN_DIR/metadata.json"
	fi
}

previous_field() {
	local path="$1"
	local field="$2"
	if [[ -r "$path" ]]; then
		"$JQ_BIN" -r --arg field "$field" '.[$field] // empty' "$path" 2>/dev/null || true
	fi
}

write_profile_status() {
	local path="$1" name="$2" state="$3" expires_at="$4" remaining="$5"
	local last_attempt="$6" last_success="$7" error_code="$8" checked_at="$9"
	local tmp previous=/dev/null
	[[ ! -r "$path" ]] || previous="$path"
	tmp="$(/usr/bin/mktemp "${path}.tmp.XXXXXX")"
	"$JQ_BIN" -n --slurpfile previous "$previous" \
		--arg name "$name" --arg state "$state" --arg expiresAt "$expires_at" \
		--arg remainingSeconds "$remaining" --arg lastAttemptAt "$last_attempt" \
		--arg lastSuccessAt "$last_success" --arg error "$error_code" --arg checkedAt "$checked_at" \
		--arg cliExitCode "$cli_exit_code" --arg duration "$attempt_duration" --arg cliVersion "$cli_version" \
		--argjson credential "$credential" --argjson before "$before_credential" --argjson result "$cli_result" '
		($previous[0] // {}) as $old
		| {name:$name,checkedAt:$checkedAt,state:$state,
		   expiresAt:(if $expiresAt == "" then null else $expiresAt end),
		   remainingSeconds:(if $remainingSeconds == "" then null else ($remainingSeconds | tonumber) end),
		   lastAttemptAt:(if $lastAttemptAt == "" then null else $lastAttemptAt end),
		   lastSuccessAt:(if $lastSuccessAt == "" then null else $lastSuccessAt end),
		   error:(if $error == "" then null else $error end),credential:$credential,
		   cliExitCode:(if $cliExitCode == "" then ($old.cliExitCode // null) else ($cliExitCode | tonumber) end),
		   attemptDurationSeconds:(if $duration == "" then ($old.attemptDurationSeconds // null) else ($duration | tonumber) end),
		   cliVersion:(if $cliVersion == "" then ($old.cliVersion // null) else $cliVersion end),
		   cliResultSubtype:(if $cliExitCode == "" then ($old.cliResultSubtype // "unknown") else $result.subtype end),
		   providerErrorType:(if $cliExitCode == "" then ($old.providerErrorType // "unknown") else $result.errorType end)}
		| . as $current
		| .attempts = ((($old.attempts // []) +
		   (if $cliExitCode == "" then [] else [$current | del(.credential) | . + {before:$before,after:$credential}] end)) | .[-64:])
	' >"$tmp"
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
	local codes version_output
	version_output="$("$PERL_BIN" -e 'alarm 5; exec @ARGV or exit 127' "$CLAUDE_BIN" --version 2>/dev/null | /usr/bin/head -c 128)" || true
	cli_version=unknown
	if [[ "$version_output" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\ \(Claude\ Code\)$ ]]; then
		cli_version="${BASH_REMATCH[1]}"
	fi
	(
		cd "$RUN_DIR"
		/usr/bin/env \
			-u ANTHROPIC_BASE_URL -u ANTHROPIC_AUTH_TOKEN -u ANTHROPIC_API_KEY \
			-u CLAUDE_CODE_OAUTH_TOKEN -u ANTHROPIC_CUSTOM_HEADERS \
			-u CLAUDE_CODE_USE_BEDROCK -u CLAUDE_CODE_USE_VERTEX -u CLAUDE_CODE_USE_FOUNDRY \
			CLAUDE_CONFIG_DIR="$config_dir" \
			"$PERL_BIN" -e 'alarm shift @ARGV; exec @ARGV or exit 127' "$CLI_TIMEOUT_SECONDS" \
			"$CLAUDE_BIN" --print --model haiku --max-turns 1 --tools "" \
			--mcp-config '{"mcpServers":{}}' --strict-mcp-config --no-session-persistence \
			--disable-slash-commands --safe-mode --restricted --no-chrome \
			--permission-mode dontAsk --output-format json 'Respond only with OK.'
	) 2>/dev/null | /usr/bin/head -c 65536 | "$JQ_BIN" -cs '
		def allowed($values): . as $v | if ($values | index($v)) != null then $v else "unknown" end;
		(map(select(type == "object" and .type == "result")) | last // {}) as $r
		| {subtype: ($r.subtype | allowed(["success","error_during_execution","error_max_turns","error_max_budget_usd","error_max_structured_output_retries"])),
		   errorType: (($r.error | if type == "object" then .type else null end) | allowed(["authentication_error","rate_limit_error","overloaded_error","api_error","invalid_request_error"])),
		   verified: ($r.subtype == "success" and $r.is_error == false)}
	' >"$RUN_DIR/cli-result.json" 2>/dev/null
	codes=("${PIPESTATUS[@]}")
	cli_exit_code="${codes[0]}"
	cli_result='{"subtype":"unknown","errorType":"unknown","verified":false}'
	if [[ "${codes[2]}" -eq 0 ]]; then cli_result="$(/bin/cat "$RUN_DIR/cli-result.json")"; fi
	return "$cli_exit_code"
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
	checked_epoch="$(now_epoch)"
	checked_at="$(iso_at_epoch "$checked_epoch")"
	last_attempt="$(previous_field "$profile_status" lastAttemptAt)"
	last_success="$(previous_field "$profile_status" lastSuccessAt)"
	cli_exit_code=""; attempt_duration=""; cli_version=""
	cli_result='{"subtype":"unknown","errorType":"unknown","verified":false}'
	credential='{}'; before_credential='{}'
	lock_dir="$STATE_DIR/locks/$key.lock"
	if ! /bin/mkdir "$lock_dir" 2>/dev/null; then
		lock_mtime="$(/usr/bin/stat -f %m "$lock_dir" 2>/dev/null || /usr/bin/printf '0')"
		if [[ "$lock_mtime" =~ ^[0-9]+$ ]] && ((checked_epoch - lock_mtime > LOCK_STALE_SECONDS)); then
			/bin/rmdir "$lock_dir" 2>/dev/null || true
			/bin/mkdir "$lock_dir" 2>/dev/null || continue
		else
			continue
		fi
	fi
	CURRENT_LOCK="$lock_dir"
	# Only the lock owner writes the profile receipt.
	credential="$(read_credential_metadata "$service")"
	before_credential="$credential"
	error_code="$($JQ_BIN -r '.error // empty' <<<"$credential")"
	expires_at="$($JQ_BIN -r '.expiresAt // empty' <<<"$credential")"
	expires_epoch="$($JQ_BIN -r '.expiresEpoch' <<<"$credential")"
	remaining=$((expires_epoch - checked_epoch))
	access_present="$($JQ_BIN -r '.accessTokenPresent' <<<"$credential")"
	refresh_present="$($JQ_BIN -r '.refreshTokenPresent' <<<"$credential")"
	profile_auth_error="$AUTH_ERROR"
	if [[ "$(previous_field "$profile_status" error)" == authentication_rejected && "$(previous_field "$profile_status" expiresAt)" == "$expires_at" ]]; then
		profile_auth_error=1
	fi
	if [[ -z "$error_code" && "$refresh_present" != true && ( "$access_present" != true || "$remaining" -le 0 ) ]]; then
		error_code=login_required
	fi
	if [[ -n "$error_code" ]]; then
		write_profile_status "$profile_status" "$name" credential_unavailable "$expires_at" "$remaining" "$last_attempt" "$last_success" "$error_code" "$checked_at"
		had_error=1
	elif [[ "$profile_auth_error" -ne 1 && "$access_present" == true && "$remaining" -gt "$REFRESH_WINDOW_SECONDS" ]]; then
		write_profile_status "$profile_status" "$name" valid "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
	else
		cooldown_path="$STATE_DIR/cooldowns/$key"
		last_cooldown="$(/bin/cat "$cooldown_path" 2>/dev/null || /usr/bin/printf '0')"
		previous_expiry="$(previous_field "$profile_status" expiresAt)"
		if [[ "$last_cooldown" =~ ^[0-9]+$ && "$previous_expiry" == "$expires_at" ]] && ((checked_epoch - last_cooldown < COOLDOWN_SECONDS)); then
			if [[ "$profile_auth_error" -ne 1 && "$access_present" == true && "$remaining" -gt 0 ]]; then
				write_profile_status "$profile_status" "$name" cooldown "$expires_at" "$remaining" "$last_attempt" "$last_success" "" "$checked_at"
			else
				write_profile_status "$profile_status" "$name" refresh_failed "$expires_at" "$remaining" "$last_attempt" "$last_success" "$(previous_field "$profile_status" error)" "$checked_at"
				had_error=1
			fi
		else
			write_cooldown "$cooldown_path" "$checked_epoch"
			last_attempt="$checked_at"
			attempt_started="$checked_epoch"
			previous_expires_epoch="$expires_epoch"
			cli_succeeded=0
			if run_official_cli "$config_dir"; then
				if [[ "$($JQ_BIN -r '.verified' <<<"$cli_result")" == true ]]; then cli_succeeded=1; fi
			fi
			checked_epoch="$(now_epoch)"
			checked_at="$(iso_at_epoch "$checked_epoch")"
			attempt_duration=$((checked_epoch - attempt_started))
			credential="$(read_credential_metadata "$service")"
			expires_at="$($JQ_BIN -r '.expiresAt // empty' <<<"$credential")"
			expires_epoch="$($JQ_BIN -r '.expiresEpoch' <<<"$credential")"
			remaining=$((expires_epoch - checked_epoch))
			error_code="$($JQ_BIN -r '.error // empty' <<<"$credential")"
			state=refresh_failed
			if [[ "$($JQ_BIN -r '.errorType' <<<"$cli_result")" == authentication_error ]]; then error_code=authentication_rejected; fi
			if [[ -z "$error_code" && "$($JQ_BIN -r '.accessTokenPresent' <<<"$credential")" == true && "$remaining" -gt 0 ]]; then
				if ((expires_epoch >= previous_expires_epoch + 60)); then
					state=refreshed; last_success="$checked_at"
				elif [[ "$cli_succeeded" -eq 1 ]]; then
					state=recovered; last_success="$checked_at"; AUTH_VERIFIED=1
				elif [[ "$profile_auth_error" -ne 1 ]]; then
					state=valid; error_code=claude_cli_failed_credential_valid
				fi
			fi
			if [[ "$state" == refresh_failed ]]; then
				if [[ -z "$error_code" ]]; then
					error_code=claude_cli_failed
					if [[ "$(previous_field "$profile_status" error)" == authentication_rejected ]]; then error_code=authentication_rejected; fi
					[[ "$cli_exit_code" -ne 142 ]] || error_code=cli_timeout
					if [[ "$($JQ_BIN -r '.accessTokenPresent' <<<"$credential")" != true && "$($JQ_BIN -r '.refreshTokenPresent' <<<"$credential")" != true ]]; then error_code=login_required; fi
				fi
				had_error=1
			fi
			if [[ "$state" != refresh_failed && "$cli_succeeded" -eq 1 ]]; then AUTH_VERIFIED=1; fi
			write_profile_status "$profile_status" "$name" "$state" "$expires_at" "$remaining" "$last_attempt" "$last_success" "$error_code" "$checked_at"
		fi
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
			profiles: (map({key: .name, value: (del(.name, .checkedAt))}) | from_entries)
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
if [[ -n "$SELECT_CONFIG_DIR" && "$matched" -eq 1 && "$(previous_field "$profile_status" error)" == authentication_rejected ]]; then
	"$JQ_BIN" -n '{version:1,authRejected:true}'
fi
if [[ -n "$SELECT_CONFIG_DIR" && "$matched" -eq 1 && "$had_error" -eq 0 && "$AUTH_VERIFIED" -eq 1 ]]; then
	"$JQ_BIN" -n --arg profile "$name" '{version:1,authVerified:true}'
fi
exit "$had_error"
