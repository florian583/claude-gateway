"""Read-only on-host rollout receipt; prints no credentials or request content."""
import http.client
import json
from pathlib import Path
from urllib.parse import urlsplit


def main():
    home = Path.home()
    config = json.loads((home/'.config/claude-proxy/config.json').read_text())
    url = urlsplit('http://'+config['listen'])
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
    try:
        conn.request('GET', '/status')
        response = conn.getresponse()
        status = json.loads(response.read(65536))
        if response.status != 200:
            raise SystemExit('Status endpoint unhealthy')
    finally:
        conn.close()
    cline = {name: m['upstream'] for name, m in config['models'].items()
             if m['provider'] == 'clinepass'}
    if set(cline.values()) != {'cline-pass/glm-5.3-flash'}:
        raise SystemExit('Unexpected Cline annual routing')
    if any(p['billing'] != 'subscription' for p in config['providers'].values()):
        raise SystemExit('Unexpected non-subscription provider in retained pool')
    state = Path(config.get('stateDir', './state')).expanduser()
    if not state.is_absolute():
        state = home/'.config/claude-proxy'/state
    metrics = state/'metrics.jsonl'
    completions = []
    if metrics.exists():
        with metrics.open('rb') as f:
            f.seek(max(0, metrics.stat().st_size - 131072))
            lines = f.read(131072).decode('utf-8', errors='replace').splitlines()
        for line in lines:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get('provider') in config['providers']:
                completions.append({key: row.get(key) for key in
                                    ('timestamp', 'provider', 'upstream', 'status', 'failureReason')})
    runtime = {key: status[key] for key in ('pid', 'sourceHash', 'configHash', 'activeRequests', 'draining')}
    print(json.dumps({'runtime': runtime, 'clineModels': cline,
                      'providers': sorted(config['providers']),
                      'claudeProfileCount': len(config.get('profiles', {})),
                      'flashSelection': config['modelPools']['glm-5.3-flash'].get('selection'),
                      'recentMetrics': completions[-8:]}))


if __name__ == '__main__':
    main()
