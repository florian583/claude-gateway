import hashlib
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
    def run_helper(self, expiry, cli_exit, auth_error=True, expect_call=True, rotate=True, delay=0, timeout=5, concurrent=False, previous_attempts=0):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            credential = root / "credential.json"
            credential.write_text(json.dumps({"claudeAiOauth": {"accessToken": "synthetic-access", "expiresAt": expiry}}))
            config = root / "config.json"
            config.write_text(json.dumps({"version": 1, "profiles": {"example": {"configDir": str(root), "credentialsService": "synthetic-service"}}}))
            security = root / "security"
            security.write_text('#!/bin/bash\n/bin/cat "$FIXTURE_CREDENTIAL"\n')
            cli = root / "claude"
            cli.write_text('#!/bin/bash\necho call >> "$FIXTURE_CALLS"\n/bin/sleep "$FIXTURE_DELAY"\nif [[ "$FIXTURE_ROTATE" == 1 ]]; then /bin/cp "$FIXTURE_ROTATED" "$FIXTURE_CREDENTIAL"; fi\nexit "$FIXTURE_EXIT"\n')
            security.chmod(0o700)
            cli.chmod(0o700)
            rotated = root / "rotated.json"
            rotated.write_text(json.dumps({"claudeAiOauth": {"accessToken": "synthetic-rotated", "expiresAt": int((time.time()+28800)*1000)}}))
            env = dict(os.environ, CLAUDE_OAUTH_TEST_MODE="1", CLAUDE_OAUTH_SECURITY_BIN=str(security),
                       CLAUDE_OAUTH_CLAUDE_BIN=str(cli), CLAUDE_OAUTH_STATE_DIR=str(root / "state"),
                       CLAUDE_OAUTH_STATUS_PATH=str(root / "status.json"), FIXTURE_CREDENTIAL=str(credential),
                       FIXTURE_ROTATED=str(rotated), FIXTURE_EXIT=str(cli_exit), FIXTURE_CALLS=str(root / "calls"),
                       FIXTURE_ROTATE="1" if rotate else "0", FIXTURE_DELAY=str(delay), CLAUDE_OAUTH_CLI_TIMEOUT_SECONDS=str(timeout))
            if previous_attempts:
                key=hashlib.sha256((str(root)+'\0synthetic-service').encode()).hexdigest()
                previous=root/'state/profiles'/(key+'.json');previous.parent.mkdir(parents=True)
                previous.write_text(json.dumps({'attempts':[{'state':'refresh_failed'}]*previous_attempts}))
            command = ["/bin/bash", str(Path(__file__).with_name("refresh-claude-oauth.sh")),
                                     "--config", str(config), "--config-dir", str(root),
                                     "--credentials-service", "synthetic-service"] + (["--auth-error"] if auth_error else [])
            if concurrent:
                with subprocess.Popen(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) as first:
                    deadline=time.monotonic()+5
                    while not (root/"calls").exists():
                        self.assertLess(time.monotonic(),deadline)
                        time.sleep(0.01)
                    second=subprocess.run(command,env=env,capture_output=True,text=True,timeout=10)
                    self.assertEqual(second.returncode,0)
                    stdout,stderr=first.communicate(timeout=10)
                    result=subprocess.CompletedProcess(command,first.returncode,stdout,stderr)
            else:
                result=subprocess.run(command,env=env,capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode, 0 if rotate or not expect_call else 1, result.stderr)
            entry=json.loads((root / "status.json").read_text())["profiles"]["example"]
            self.assertEqual(entry["state"], ("refreshed" if rotate else "refresh_failed") if expect_call else "valid")
            self.assertIn("checkedAt",entry)
            if expect_call:
                self.assertEqual(entry["cliExitCode"],cli_exit if delay<timeout else 142)
                self.assertEqual(len(entry["attempts"]),min(16,previous_attempts+1))
                self.assertEqual((root/"calls").read_text().splitlines(),["call"])
                self.assertGreaterEqual(entry["attemptDurationSeconds"],0)

    def test_missing_expiry_is_renewed(self):
        self.run_helper(None, 0)

    def test_expired_token_rotates_despite_model_limit(self):
        self.run_helper(1, 1)

    def test_string_milliseconds_near_expiry_is_not_treated_as_far_future(self):
        self.run_helper(str(int((time.time()+60)*1000)),0,auth_error=False)

    def test_dormant_valid_profile_needs_no_probe(self):
        self.run_helper(int((time.time()+7200)*1000),0,auth_error=False,expect_call=False)

    def test_transient_failure_preserves_failure_evidence(self):
        self.run_helper(1,1,rotate=False)

    def test_timed_out_cli_records_deadline(self):
        self.run_helper(1,0,rotate=False,delay=2,timeout=1)

    def test_concurrent_refresh_runs_one_cli(self):
        self.run_helper(1,0,delay=2,concurrent=True)

    def test_attempt_history_is_bounded(self):
        self.run_helper(1,0,previous_attempts=16)
