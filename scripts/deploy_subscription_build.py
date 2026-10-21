"""Explicit on-host operator rollout. Never called by tests or observation endpoints."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlsplit


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--candidate', type=Path, required=True)
    p.add_argument('--binary-sha256', required=True)
    p.add_argument('--source-hash', required=True)
    p.add_argument('--expected-old-source', required=True)
    a = p.parse_args()
    home = Path.home()
    target = (home/'Projects/claude-gateway/bin/claude-proxy').resolve(strict=True)
    config = home/'.config/claude-proxy/config.json'
    if digest(a.candidate) != a.binary_sha256:
        raise SystemExit('Candidate checksum mismatch')
    old_version = subprocess.check_output([str(target), 'version'], text=True).strip()
    new_version = subprocess.check_output([str(a.candidate), 'version'], text=True).strip()
    if old_version != 'claude-proxy config-v1 source='+a.expected_old_source:
        raise SystemExit('Deployed source changed; refusing overwrite')
    if new_version != 'claude-proxy config-v1 source='+a.source_hash:
        raise SystemExit('Candidate source mismatch')
    subprocess.run([str(a.candidate), '--config', str(config), 'validate-config'], check=True)
    backup = target.with_name('claude-proxy.before-subscription-cleanup-'+digest(target)[:12])
    if backup.exists() and digest(backup) != digest(target):
        raise SystemExit('Backup collision')
    if not backup.exists():
        shutil.copy2(target, backup)
    fd, temporary = tempfile.mkstemp(dir=target.parent, prefix='.claude-proxy-rollout-')
    os.close(fd)
    try:
        shutil.copyfile(a.candidate, temporary)
        os.chmod(temporary, 0o755)
        subprocess.run(['codesign', '--force', '--sign', '-', temporary], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(['codesign', '--verify', '--strict', temporary], check=True)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    label = 'gui/'+str(os.getuid())+'/com.florianvidal.anthropic-proxy'
    subprocess.run(['launchctl', 'kickstart', '-k', label], check=True)
    c = json.loads(config.read_text())
    url = urlsplit('http://'+c['listen'])
    healthy = False
    for _ in range(20):
        conn = http.client.HTTPConnection(url.hostname, url.port, timeout=2)
        try:
            conn.request('GET', '/status')
            response = conn.getresponse()
            status = json.loads(response.read(65536))
            healthy = response.status == 200 and status.get('sourceHash') == a.source_hash
        except (OSError, ValueError, http.client.HTTPException):
            pass
        finally:
            conn.close()
        if healthy:
            break
        time.sleep(0.5)
    if not healthy:
        # Preserve known-good binary; no credential or config rollback required.
        fd, rollback = tempfile.mkstemp(dir=target.parent, prefix='.claude-proxy-rollback-')
        os.close(fd)
        shutil.copy2(backup, rollback)
        subprocess.run(['codesign', '--force', '--sign', '-', rollback], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.replace(rollback, target)
        subprocess.run(['launchctl', 'kickstart', '-k', label], check=True)
        raise SystemExit('Health failed; previous binary restored')
    print(json.dumps({'healthy': healthy, 'pid': status['pid'], 'sourceHash': a.source_hash,
                      'binarySHA256': digest(target), 'backup': backup.name,
                      'providers': sorted(c['providers']),
                      'clineModels': [k for k, m in c['models'].items() if m['provider'] == 'clinepass'],
                      'flashPool': c['modelPools']['glm-5.3-flash']['models']}))


if __name__ == '__main__':
    main()
