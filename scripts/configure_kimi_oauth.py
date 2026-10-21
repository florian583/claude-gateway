"""Explicit operator setup; reads CLI references, never token values."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib


def prepare(source, native_home):
    native = tomllib.loads((native_home / 'config.toml').read_text())
    provider = native['providers']['managed:kimi-code']
    oauth = provider['oauth']
    if provider.get('api_key') or provider.get('api_key_env') or oauth.get('storage') != 'file':
        raise ValueError('Expected existing native managed OAuth file login')
    host = oauth['oauth_host'].rstrip('/')
    if host not in ('https://auth.kimi.ai', 'https://auth.kimi.com'):
        raise ValueError('Unsupported native OAuth region')
    suffix = 'ai' if host.endswith('.ai') else 'com'
    if provider['base_url'].rstrip('/') != f'https://api.kimi.{suffix}/coding/v1':
        raise ValueError('Native auth/inference region mismatch')
    key = oauth['key']
    if not key.startswith('oauth/') or '/' in key[6:] or key[6:].startswith('.'):
        raise ValueError('Invalid native credential slot')
    c = copy.deepcopy(source)
    p = c['providers']['moonshot']
    p.update(variant='kimi-subscription', billing='subscription', protocol='anthropic',
             baseURL=f'https://api.kimi.{suffix}/coding/',
             auth={'type': 'kimi-code-oauth',
                   'statePath': str(native_home / 'credentials' / (key[6:] + '.json'))},
             usage={'mode': 'passive'})
    p.pop('messagesPath', None)
    # Preserve the operator's explicit kimi-for-coding choice for both legs.
    for m in c['models'].values():
        if m['provider'] == 'moonshot':
            m['upstream'] = 'kimi-for-coding'
    return c


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--native-home', type=Path, required=True)
    p.add_argument('--binary', required=True)
    p.add_argument('--canary-output', type=Path)
    p.add_argument('--apply-sha256')
    a = p.parse_args()
    if a.config.is_symlink():
        raise SystemExit('Refusing symlink config')
    original = a.config.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    c = prepare(json.loads(original), a.native_home)
    if a.canary_output:
        c = {'version': 1, 'listen': '127.0.0.1:48129',
             'stateDir': str(a.canary_output.parent / 'kimi-canary-state'),
             'providers': {'moonshot': c['providers']['moonshot']},
             'models': {'native': {'provider': 'moonshot', 'upstream': 'kimi-for-coding',
                                  'contextWindow': 1048576, 'supportsTools': True}},
             'chains': {'native': {'steps': [{'model': 'native'}]}},
             'aliases': {'kimi-native-oauth': 'native'}}
    target = a.canary_output or a.config
    fd, candidate = tempfile.mkstemp(dir=target.parent, prefix='.kimi-config-')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(c, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        subprocess.run([a.binary, '--config', candidate, 'validate-config'],
                       check=True, stdout=subprocess.DEVNULL)
        if a.canary_output:
            if target.exists() or target.is_symlink():
                raise SystemExit('Canary output already exists')
            os.link(candidate, target)
        elif a.apply_sha256:
            if a.apply_sha256 != digest:
                raise SystemExit('Config digest mismatch')
            lock = a.config.with_suffix(a.config.suffix + '.lock')
            lockfd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            owned = os.fstat(lockfd)
            try:
                if a.config.is_symlink() or a.config.read_bytes() != original:
                    raise SystemExit('Config changed; refusing overwrite')
                backup = a.config.with_name(a.config.name + '.before-kimi-oauth-' + digest[:12])
                if backup.exists():
                    if backup.read_bytes() != original:
                        raise SystemExit('Backup collision')
                else:
                    bfd = os.open(backup, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    with os.fdopen(bfd, 'wb') as f:
                        f.write(original)
                        f.flush()
                        os.fsync(f.fileno())
                os.replace(candidate, a.config)
            finally:
                os.close(lockfd)
                if os.path.samestat(owned, lock.lstat()):
                    lock.unlink()
        print(json.dumps({'validated': True, 'sourceSHA256': digest,
                          'applied': bool(a.apply_sha256), 'canary': bool(a.canary_output)}))
    finally:
        if os.path.exists(candidate):
            os.unlink(candidate)


if __name__ == '__main__':
    main()
