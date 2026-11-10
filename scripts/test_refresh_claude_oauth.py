import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest


@unittest.skipUnless(sys.platform == "darwin" and shutil.which("jq"), "macOS helper requires jq")
class RefreshHelperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.credential = self.root / "credential.json"
        self.set_credential("synthetic-access", "synthetic-refresh", 1)
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"version": 1, "profiles": {"example": {
            "configDir": str(self.root), "credentialsService": "synthetic-service"}}}))
        self.rotated = self.root / "rotated.json"
        self.rotated.write_text(json.dumps({"claudeAiOauth": {
            "accessToken": "synthetic-rotated", "refreshToken": "synthetic-next",
            "expiresAt": int((time.time() + 28800) * 1000)}}))
        security = self.root / "security"
        security.write_text('#!/bin/bash\nexit_code=${FIXTURE_SECURITY_EXIT:-0}\n'
                            '[[ "$exit_code" -eq 0 ]] || exit "$exit_code"\n'
                            '/bin/cat "$FIXTURE_CREDENTIAL"\n')
        cli = self.root / "claude"
        cli.write_text('''#!/bin/bash
if [[ "${1:-}" == --version ]]; then printf '2.1.999 (Claude Code)\n'; exit 0; fi
printf 'call\n' >> "$FIXTURE_CALLS"
if [[ -n "${FIXTURE_SLEEP:-}" ]]; then /bin/sleep "$FIXTURE_SLEEP"; fi
if [[ "${FIXTURE_ROTATE:-1}" == 1 ]]; then /bin/cp "$FIXTURE_ROTATED" "$FIXTURE_CREDENTIAL"; fi
printf '%s' "${FIXTURE_OUTPUT}"
printf 'synthetic-secret-marker\n' >&2
exit "${FIXTURE_EXIT:-0}"
''')
        security.chmod(0o700)
        cli.chmod(0o700)
        self.env = dict(os.environ, CLAUDE_OAUTH_TEST_MODE="1", CLAUDE_OAUTH_SECURITY_BIN=str(security),
                        CLAUDE_OAUTH_CLAUDE_BIN=str(cli), CLAUDE_OAUTH_STATE_DIR=str(self.root / "state"),
                        CLAUDE_OAUTH_STATUS_PATH=str(self.root / "status.json"), FIXTURE_CREDENTIAL=str(self.credential),
                        FIXTURE_ROTATED=str(self.rotated), FIXTURE_CALLS=str(self.root / "calls"),
                        FIXTURE_OUTPUT=json.dumps({"type": "result", "subtype": "success", "is_error": False}))
        self.env.pop("CLAUDE_OAUTH_REFRESH_REASON", None)

    def set_credential(self, access, refresh, expiry):
        self.credential.write_text(json.dumps({"claudeAiOauth": {
            "accessToken": access, "refreshToken": refresh, "expiresAt": expiry}}))

    def run_helper(self, scheduled=False):
        env = dict(self.env)
        if scheduled:
            env["CLAUDE_OAUTH_REFRESH_REASON"] = "scheduled"
        result = subprocess.run(["/bin/bash", str(Path(__file__).with_name("refresh-claude-oauth.sh")),
                                 "--config", str(self.config), "--config-dir", str(self.root),
                                 "--credentials-service", "synthetic-service", "--auth-error"],
                                env=env, capture_output=True, text=True, timeout=12)
        status = json.loads((self.root / "status.json").read_text())["profiles"]["example"]
        for path in (self.root / "state").rglob("*.json"):
            contents = path.read_text()
            for secret in ("synthetic-access", "synthetic-refresh", "synthetic-secret-marker"):
                self.assertNotIn(secret, contents)
        self.assertNotIn("synthetic-secret-marker", result.stdout + result.stderr)
        return result, status

    def calls(self):
        path = self.root / "calls"
        return len(path.read_text().splitlines()) if path.exists() else 0

    def test_missing_expiry_is_renewed(self):
        self.set_credential("synthetic-access", "synthetic-refresh", None)
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status["state"], "refreshed")

    def test_expired_token_rotates_despite_model_limit(self):
        self.env["FIXTURE_EXIT"] = "1"
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status["state"], "refreshed")
        self.assertEqual(status["cliExitCode"], 1)

    def test_refresh_only_is_recovered(self):
        self.set_credential(None, "synthetic-refresh", 0)
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status["state"], "refreshed")
        self.assertEqual(self.calls(), 1)
        self.assertFalse(status["attempts"][-1]["before"]["accessTokenPresent"])
        self.assertTrue(status["attempts"][-1]["before"]["refreshTokenPresent"])

    def test_empty_credential_requires_login_without_probe(self):
        self.set_credential(None, None, 0)
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(status["error"], "login_required")
        self.assertEqual(self.calls(), 0)

    def test_locked_keychain_is_distinct(self):
        self.env["FIXTURE_SECURITY_EXIT"] = "36"
        _, status = self.run_helper()
        self.assertEqual(status["error"], "keychain_read_failed")
        self.assertEqual(status["credential"]["keychainExitCode"], 36)
        self.assertEqual(self.calls(), 0)

    def test_missing_keychain_item_is_distinct(self):
        self.env["FIXTURE_SECURITY_EXIT"] = "44"
        _, status = self.run_helper()
        self.assertEqual(status["error"], "keychain_item_missing")
        self.assertEqual(self.calls(), 0)

    def test_malformed_credential_is_distinct(self):
        self.credential.write_text("not-json")
        _, status = self.run_helper()
        self.assertEqual(status["error"], "credential_invalid_json")
        self.assertEqual(self.calls(), 0)

    def test_scheduled_reason_overrides_legacy_auth_flag(self):
        self.set_credential("synthetic-access", "synthetic-refresh", int((time.time() + 7200) * 1000))
        result, status = self.run_helper(scheduled=True)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status["state"], "valid")
        self.assertEqual(self.calls(), 0)

    def test_expired_failure_obeys_cooldown_even_for_auth_error(self):
        self.env.update(FIXTURE_ROTATE="0", FIXTURE_EXIT="1", FIXTURE_OUTPUT="not-json")
        first, status = self.run_helper()
        second, status = self.run_helper()
        self.assertEqual(first.returncode, 1)
        self.assertEqual(second.returncode, 1)
        self.assertEqual(self.calls(), 1)
        self.assertEqual(status["state"], "refresh_failed")
        self.assertEqual(status["cliResultSubtype"], "unknown")
        self.assertEqual(status["cliVersion"], "2.1.999")

    def test_scheduled_model_failure_preserves_valid_auth(self):
        self.set_credential("synthetic-access", "synthetic-refresh", int((time.time() + 60) * 1000))
        self.env.update(FIXTURE_ROTATE="0", FIXTURE_EXIT="1")
        result, status = self.run_helper(scheduled=True)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status["state"], "valid")
        self.assertEqual(status["error"], "claude_cli_failed_credential_valid")

    def test_timeout_is_recorded_and_not_replayed(self):
        self.env.update(FIXTURE_SLEEP="2", FIXTURE_ROTATE="0", CLAUDE_OAUTH_CLI_TIMEOUT_SECONDS="1")
        _, status = self.run_helper()
        self.assertEqual(status["error"], "cli_timeout")
        self.run_helper()
        self.assertEqual(self.calls(), 1)

    def test_auth_error_does_not_claim_cooldown_is_verified(self):
        self.set_credential("synthetic-access", "synthetic-refresh", int((time.time() + 7200) * 1000))
        self.env["FIXTURE_ROTATE"] = "0"
        result, _ = self.run_helper()
        self.assertTrue(json.loads(result.stdout)["authVerified"])
        result, _ = self.run_helper()
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.calls(), 1)

    def test_untrusted_cli_fields_are_not_persisted(self):
        self.env.update(FIXTURE_ROTATE="0", FIXTURE_EXIT="1", FIXTURE_OUTPUT=json.dumps({
            "type": "result", "subtype": "synthetic-secret-marker", "is_error": True,
            "error": {"type": "synthetic-secret-marker"}, "errors": ["synthetic-secret-marker"]}))
        _, status = self.run_helper()
        self.assertEqual(status["cliResultSubtype"], "unknown")

    def test_numeric_string_millisecond_expiry_is_normalized(self):
        self.set_credential("synthetic-access", "synthetic-refresh", str(int((time.time() + 7200) * 1000)))
        result, status = self.run_helper(scheduled=True)
        self.assertEqual(result.returncode, 0)
        self.assertLess(status["remainingSeconds"], 7300)
        self.assertEqual(self.calls(), 0)

    def test_credentials_lost_during_cli_are_recorded_without_retry(self):
        self.rotated.write_text('{"claudeAiOauth": {}}')
        self.env["FIXTURE_EXIT"] = "1"
        _, status = self.run_helper()
        self.assertEqual(status["error"], "login_required")
        self.assertTrue(status["attempts"][-1]["before"]["refreshTokenPresent"])
        self.assertFalse(status["attempts"][-1]["after"]["refreshTokenPresent"])
        self.run_helper()
        self.assertEqual(self.calls(), 1)

    def test_concurrent_helper_does_not_probe_or_overwrite_owner(self):
        self.env["FIXTURE_SLEEP"] = "2"
        command = ["/bin/bash", str(Path(__file__).with_name("refresh-claude-oauth.sh")),
                   "--config", str(self.config), "--config-dir", str(self.root),
                   "--credentials-service", "synthetic-service", "--auth-error"]
        first = subprocess.Popen(command, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(lambda: first.kill() if first.poll() is None else None)
        deadline = time.monotonic() + 5
        while self.calls() == 0 and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.calls(), 1)
        second = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=5)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(second.stdout, "")
        self.assertEqual(list((self.root / "state" / "profiles").glob("*.json")), [])
        _, stderr = first.communicate(timeout=6)
        self.assertEqual(first.returncode, 0, stderr)
        self.assertEqual(self.calls(), 1)
        receipt = json.loads(next((self.root / "state" / "profiles").glob("*.json")).read_text())
        self.assertEqual(receipt["state"], "refreshed")

    def test_changed_credentials_bypass_old_failure_cooldown(self):
        self.env.update(FIXTURE_ROTATE="0", FIXTURE_EXIT="1")
        self.run_helper()
        self.set_credential(None, "synthetic-next", 2)
        self.env.update(FIXTURE_ROTATE="1", FIXTURE_EXIT="0")
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status["state"], "refreshed")
        self.assertEqual(self.calls(), 2)

    def test_explicit_auth_rejection_survives_scheduled_checks(self):
        self.set_credential("synthetic-access", "synthetic-refresh", int((time.time() + 7200) * 1000))
        self.env.update(FIXTURE_ROTATE="0", FIXTURE_EXIT="1", FIXTURE_OUTPUT=json.dumps({
            "type": "result", "subtype": "error_during_execution", "is_error": True,
            "error": {"type": "authentication_error"}}))
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(status["error"], "authentication_rejected")
        self.assertTrue(json.loads(result.stdout)["authRejected"])
        result, status = self.run_helper(scheduled=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(status["error"], "authentication_rejected")
        self.assertEqual(self.calls(), 1)

    def test_string_error_does_not_break_receipt(self):
        self.env.update(FIXTURE_ROTATE="0", FIXTURE_EXIT="1", FIXTURE_OUTPUT=json.dumps({
            "type": "result", "subtype": "error_during_execution", "is_error": True,
            "error": "synthetic-secret-marker"}))
        result, status = self.run_helper()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(status["providerErrorType"], "unknown")
