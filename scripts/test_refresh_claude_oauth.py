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
    def run_helper(self, expiry, cli_exit):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            credential = root / "credential.json"
            credential.write_text(json.dumps({"claudeAiOauth": {"accessToken": "synthetic-access", "expiresAt": expiry}}))
            config = root / "config.json"
            config.write_text(json.dumps({"version": 1, "profiles": {"example": {"configDir": str(root), "credentialsService": "synthetic-service"}}}))
            security = root / "security"
            security.write_text('#!/bin/bash\n/bin/cat "$FIXTURE_CREDENTIAL"\n')
            cli = root / "claude"
            cli.write_text('#!/bin/bash\n/bin/cp "$FIXTURE_ROTATED" "$FIXTURE_CREDENTIAL"\nexit "$FIXTURE_EXIT"\n')
            security.chmod(0o700)
            cli.chmod(0o700)
            rotated = root / "rotated.json"
            rotated.write_text(json.dumps({"claudeAiOauth": {"accessToken": "synthetic-rotated", "expiresAt": int((time.time()+28800)*1000)}}))
            env = dict(os.environ, CLAUDE_OAUTH_TEST_MODE="1", CLAUDE_OAUTH_SECURITY_BIN=str(security),
                       CLAUDE_OAUTH_CLAUDE_BIN=str(cli), CLAUDE_OAUTH_STATE_DIR=str(root / "state"),
                       CLAUDE_OAUTH_STATUS_PATH=str(root / "status.json"), FIXTURE_CREDENTIAL=str(credential),
                       FIXTURE_ROTATED=str(rotated), FIXTURE_EXIT=str(cli_exit))
            result = subprocess.run(["/bin/bash", str(Path(__file__).with_name("refresh-claude-oauth.sh")),
                                     "--config", str(config), "--config-dir", str(root),
                                     "--credentials-service", "synthetic-service", "--auth-error"],
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads((root / "status.json").read_text())["profiles"]["example"]["state"], "refreshed")

    def test_missing_expiry_is_renewed(self):
        self.run_helper(None, 0)

    def test_expired_token_rotates_despite_model_limit(self):
        self.run_helper(1, 1)
