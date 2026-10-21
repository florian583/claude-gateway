"""Expose gateway chains through the existing Pi ACP Agent Bridge lane."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import tempfile


def atomic(path, text, mode):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise SystemExit('Refusing symlink target')
    if path.exists():
        backup = path.with_name(path.name+'.before-subscription-pools')
        if not backup.exists():
            fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as f:
                f.write(path.read_bytes())
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(temp, mode)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def main(include_muse=False):
    home = Path.home()
    acp = shutil.which('pi-acp')
    pi = shutil.which('pi')
    if not acp or not pi:
        raise SystemExit('Install pinned pi-acp and Pi coding agent first')
    config = json.loads((home/'.config/claude-proxy/config.json').read_text())
    auth = config['providers']['opencode-go']['auth']
    if auth['type'] != 'command':
        raise SystemExit('Expected existing command credential helper')
    models = [{'id': n, 'name': n+' subscription pool', 'input': ['text'],
               'contextWindow': context, 'maxTokens': 16384, 'reasoning': True}
              for n, context in [('kimi-k2.7-code', 262144), ('glm-5.3-flash', 1000000),
                                 ('kimi-k3', 1000000)]]
    providers = {'gateway': {'baseUrl': 'http://'+config['listen'],
                             'api': 'anthropic-messages', 'apiKey': 'local-gateway',
                             'models': models},
                 'opencode-go': {'baseUrl': 'https://opencode.ai/inference/go/openai/v1',
                                 'api': 'openai-responses',
                                 'apiKey': '!'+shlex.join(auth['command']),
                                 'models': [{'id': 'muse-spark-1.3-contributor',
                                             'name': 'Muse Spark 1.3 Contributor (Go)',
                                             'input': ['text'], 'contextWindow': 1048576, 'maxTokens': 16384}]}}
    directory = home/'.pi/agent-gateway'
    if not include_muse:
        del providers['opencode-go']
    atomic(directory/'models.json', json.dumps({'providers': providers}, indent=2)+'\n', 0o600)
    atomic(directory/'settings.json', json.dumps({'defaultProvider': 'gateway',
           'defaultModel': 'kimi-k2.7-code', 'quietStartup': True}, indent=2)+'\n', 0o600)
    wrapper = '#!/bin/bash\nset -euo pipefail\n'
    wrapper += 'export PATH='+shlex.quote(str(Path(acp).parent))+':"$HOME/.local/bin:/opt/homebrew/bin:$PATH"\n'
    wrapper += 'export PI_CODING_AGENT_DIR='+shlex.quote(str(directory))+'\n'
    wrapper += 'export PI_SKIP_VERSION_CHECK=1\n'
    wrapper += 'unset PI_ACP_PI_COMMAND\nexec '+shlex.quote(acp)+' "$@"\n'
    atomic(home/'.local/bin/pi-bridge-acp', wrapper, 0o700)
    bridge = home/'.agent-bridge/agents.toml'
    text = bridge.read_text() if bridge.exists() else ''
    import tomllib
    parsed = tomllib.loads(text)
    if 'pi' not in parsed.get('agents', {}):
        text += '\n[agents.pi]\nprotocol = "acp"\ncommand = ['+json.dumps(str(home/'.local/bin/pi-bridge-acp'))+']\nrevivable = true\nidle_unload_sec = 900\n'
    # Generic ACP ignores Bridge's model argument. Fixed CLI model flags are
    # authoritative, including over project-local Pi settings.
    lanes = [
            ('pool-kimi-k27', 'gateway', 'kimi-k2.7-code'),
            ('pool-glm-flash', 'gateway', 'glm-5.3-flash'),
            ('pool-kimi-k3', 'gateway', 'kimi-k3')]
    if include_muse:
        lanes.append(('pool-muse-go', 'opencode-go', 'muse-spark-1.3-contributor'))
    else:
        # Only remove the exact profile owned by this installer, never arbitrary
        # user-managed TOML. Muse requires separate request-data training consent.
        owned = '\n[agents.pool-muse-go]\nprotocol = "acp"\ncommand = '+json.dumps([
            str(home/'.local/bin/pi-acp-pool-muse-go')])+'\nrevivable = true\nidle_unload_sec = 900\n'
        if 'pool-muse-go' in parsed.get('agents', {}):
            if owned not in text:
                raise SystemExit('Muse profile modified externally; refusing overwrite')
            text = text.replace(owned, '')
    for name, provider, model in lanes:
        rpc = home/'.local/bin'/('pi-rpc-'+name)
        atomic(rpc, '#!/bin/bash\nset -euo pipefail\nexec '+shlex.quote(pi)+
               ' --provider '+shlex.quote(provider)+' --model '+shlex.quote(model)+' "$@"\n', 0o700)
        command = home/'.local/bin'/('pi-acp-'+name)
        pinned = wrapper.replace('unset PI_ACP_PI_COMMAND',
                                  'export PI_ACP_PI_COMMAND='+shlex.quote(str(rpc)))
        atomic(command, pinned, 0o700)
        expected = [str(command)]
        existing = parsed.get('agents', {}).get(name)
        if existing and existing.get('command') != expected:
            raise SystemExit('Existing named pool agent has conflicting command')
        if not existing:
            text += '\n[agents.'+name+']\nprotocol = "acp"\ncommand = '+json.dumps(expected)+'\nrevivable = true\nidle_unload_sec = 900\n'
    tomllib.loads(text)
    atomic(bridge, text, 0o600)
    print('Fixed-model ACP lanes configured: '+', '.join(name for name, _, _ in lanes))
    if not include_muse:
        print('Muse disabled: requires explicit owner consent for request-data training')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--include-muse-training', action='store_true',
                        help='Use only after owner approves provider request-data training')
    main(parser.parse_args().include_muse_training)
