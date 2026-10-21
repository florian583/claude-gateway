"""Offline Bridge installation test: no workers, credentials, or services."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch

import install_bridge_pools


class BridgeInstallTests(unittest.TestCase):
    def test_fixed_model_commands_not_ignored_dispatch_overrides(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            config = home/'.config/claude-proxy/config.json'
            config.parent.mkdir(parents=True)
            config.write_text(json.dumps({'listen': '127.0.0.1:48104', 'providers': {
                'opencode-go': {'auth': {'type': 'command', 'command': ['fake-helper']}}}}))
            with patch.object(install_bridge_pools.Path, 'home', return_value=home), \
                    patch.object(install_bridge_pools.shutil, 'which', side_effect=lambda n: '/test/bin/'+n), \
                    contextlib.redirect_stdout(io.StringIO()):
                install_bridge_pools.main(include_muse=True)
                before = (home/'.agent-bridge/agents.toml').read_text()
                install_bridge_pools.main(include_muse=True)
                self.assertEqual((home/'.agent-bridge/agents.toml').read_text(), before)
            agents = tomllib.loads(before)['agents']
            for name, provider, model in [
                    ('pool-kimi-k27', 'gateway', 'kimi-k2.7-code'),
                    ('pool-glm-flash', 'gateway', 'glm-5.3-flash'),
                    ('pool-kimi-k3', 'gateway', 'kimi-k3'),
                    ('pool-muse-go', 'opencode-go', 'muse-spark-1.3-contributor')]:
                command = Path(agents[name]['command'][0])
                wrapper = command.read_text()
                self.assertIn('export PI_SKIP_VERSION_CHECK=1', wrapper)
                rpc = home/'.local/bin'/('pi-rpc-'+name)
                self.assertIn(str(rpc), wrapper)
                self.assertIn('--provider '+provider+' --model '+model, rpc.read_text())
                self.assertEqual(command.stat().st_mode & 0o777, 0o700)
            models = json.loads((home/'.pi/agent-gateway/models.json').read_text())
            self.assertEqual(models['providers']['opencode-go']['models'][0]['contextWindow'], 1048576)
            with patch.object(install_bridge_pools.Path, 'home', return_value=home), \
                    patch.object(install_bridge_pools.shutil, 'which', side_effect=lambda n: '/test/bin/'+n), \
                    contextlib.redirect_stdout(io.StringIO()):
                install_bridge_pools.main()
            active = tomllib.loads((home/'.agent-bridge/agents.toml').read_text())['agents']
            self.assertNotIn('pool-muse-go', active)
            models = json.loads((home/'.pi/agent-gateway/models.json').read_text())
            self.assertNotIn('opencode-go', models['providers'])


if __name__ == '__main__':
    unittest.main()
