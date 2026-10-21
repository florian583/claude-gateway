import os
from pathlib import Path
import subprocess
import unittest


class UsageEnvTests(unittest.TestCase):
    def test_inherited_tags_relabel_and_preserve_unrelated(self):
        helper = Path(__file__).with_name("gateway-usage-env.sh")
        env = dict(os.environ, ANTHROPIC_CUSTOM_HEADERS="Keep: yes\nX-Gateway-Worker: parent\nX-Gateway-Lane: old",
                   GATEWAY_USAGE_TAGGED="1", GATEWAY_SESSION="parent-session", GATEWAY_WORKER="child",
                   WELL_PR_BOT_LANE="d", WELL_PR_BOT_RUN_ID="run-2", GATEWAY_HOST="mac-mini")
        for shell in ("/bin/bash", "/bin/zsh"):
            result = subprocess.run([shell, "-c", '. "$1"; . "$1"; printf "%s" "$ANTHROPIC_CUSTOM_HEADERS"', "test", str(helper)],
                                    env=env, capture_output=True, text=True, check=True)
            lines = result.stdout.splitlines()
            self.assertIn("Keep: yes", lines)
            self.assertIn("X-Gateway-Worker: child", lines)
            self.assertIn("X-Gateway-Parent-Session: parent-session", lines)
            self.assertIn("X-Gateway-Lane: d", lines)
            self.assertNotIn("X-Gateway-Lane: old", lines)
            self.assertEqual(len(lines), len(set(lines)))


if __name__ == "__main__":
    unittest.main()
