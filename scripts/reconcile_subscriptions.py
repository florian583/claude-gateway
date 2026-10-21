"""One-off operator migration; no scheduler, routing controller, or token reads."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


def reconcile(source, archived):
    c = copy.deepcopy(source)
    if 'clinepass' not in c['providers']:
        c['providers']['clinepass'] = copy.deepcopy(archived['providers']['clinepass'])
    p = c['providers']['clinepass']
    if p.get('billing') != 'subscription' or p.get('variant') != 'clinepass':
        raise ValueError('Expected existing prepaid Cline subscription configuration')
    # Annual quota is reserved for these two efficient models, never K3.
    # K2.7 Code is permitted by policy, but Cline currently returns HTTP 404.
    # Do not install a known-broken leg; other K2.7 subscriptions remain intact.
    rows = [('glm-5.3-flash', 'glm-5.3-flash', 1000000)]
    allowed = {upstream + '-cline' for _, upstream, _ in rows}
    removed = {name for name, model in c['models'].items()
               if model.get('provider') == 'clinepass' and name not in allowed}
    for name in removed:
        del c['models'][name]
    for pool in c['modelPools'].values():
        pool['models'] = [name for name in pool['models'] if name not in removed]
    for pool, upstream, context in rows:
        name = upstream + '-cline'
        c['models'][name] = {'provider': 'clinepass', 'upstream': 'cline-pass/' + upstream,
                             'contextWindow': context, 'supportsTools': True,
                             'supportsImages': False}
        members = c['modelPools'][pool]['models']
        c['modelPools'][pool]['models'] = [name] + [m for m in members if m != name]
        c['modelPools'][pool]['selection'] = 'fixed-order'
    # Current documented endpoint; no credential change and no metered mirror.
    c['providers']['opencode-go']['baseURL'] = 'https://opencode.ai/inference/go/openai'
    return c


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--archive', type=Path, required=True)
    p.add_argument('--binary', required=True)
    p.add_argument('--candidate', type=Path)
    p.add_argument('--apply-sha256')
    a = p.parse_args()
    if a.config.is_symlink():
        raise SystemExit('Refusing symlink config')
    original = a.config.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    result = reconcile(json.loads(original), json.loads(a.archive.read_bytes()))
    fd, candidate = tempfile.mkstemp(dir=a.config.parent, prefix='.subscription-reconcile-')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(result, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        subprocess.run([a.binary, '--config', candidate, 'validate-config'],
                       check=True, stdout=subprocess.DEVNULL)
        if a.candidate:
            if a.candidate.is_symlink() or a.candidate.exists():
                raise SystemExit('Candidate output already exists')
            os.link(candidate, a.candidate)
        if a.apply_sha256:
            if a.apply_sha256 != digest:
                raise SystemExit('Config digest mismatch')
            lock = a.config.with_suffix(a.config.suffix + '.lock')
            lockfd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            owned = os.fstat(lockfd)
            try:
                if a.config.is_symlink() or a.config.read_bytes() != original:
                    raise SystemExit('Config changed; refusing overwrite')
                backup = a.config.with_name(a.config.name + '.before-cline-reconcile-' + digest[:12])
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
                if lock.exists() and os.path.samestat(owned, lock.lstat()):
                    lock.unlink()
        print(json.dumps({'validated': True, 'sourceSHA256': digest,
                          'applied': bool(a.apply_sha256), 'clineRestored': True}))
    finally:
        if os.path.exists(candidate):
            os.unlink(candidate)


if __name__ == '__main__':
    main()
