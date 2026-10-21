"""Explicit live operator smoke, never imported by tests or dashboard."""
import argparse
import http.client
import json
from pathlib import Path
import subprocess
import uuid
from urllib.parse import urlsplit


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--model', required=True)
    p.add_argument('--gateway', action='store_true')
    p.add_argument('--stream', action='store_true')
    a = p.parse_args()
    c = json.loads(a.config.read_text())
    m = {'provider': 'local-gateway', 'upstream': a.model} if a.gateway else c['models'][a.model]
    provider = ({'baseURL': 'http://'+c['listen'], 'protocol': 'anthropic',
                 'auth': {'type': 'local'}} if a.gateway else c['providers'][m['provider']])
    auth = provider['auth']
    if auth['type'] not in ('command', 'local'):
        raise SystemExit('Smoke supports command auth only')
    secret = 'local-gateway'
    if auth['type'] == 'command':
        r = subprocess.run(auth['command'], capture_output=True, text=True, timeout=5)
        if r.returncode or not r.stdout.strip():
            raise SystemExit('Provider credential helper unavailable (details suppressed)')
        secret = r.stdout.strip()
    u = urlsplit(provider['baseURL'])
    headers = {'Content-Type': 'application/json', 'anthropic-version': '2023-06-01',
               auth.get('header', 'Authorization'): auth.get('prefix', 'Bearer ')+secret}
    if provider.get('variant') == 'opencode-go':
        headers.update({'x-opencode-session': 'ses_'+uuid.uuid4().hex,
                        'User-Agent': 'claude-gateway/1'})
    chat = provider['protocol'] == 'openai-chat-completions'
    suffix = provider.get('messagesPath') or ('/chat/completions' if chat else '/messages')
    path = u.path.rstrip('/') + ('' if u.path.rstrip('/').endswith('/v1') else '/v1') + suffix
    request = {'model': m['upstream'], 'messages': [{'role': 'user', 'content': 'Reply OK.'}],
               'max_tokens': 512, 'stream': a.stream}
    connection = http.client.HTTPConnection if a.gateway else http.client.HTTPSConnection
    conn = connection(u.hostname, u.port, timeout=90)
    try:
        conn.request('POST', path, json.dumps(request), headers)
        response = conn.getresponse()
        if a.stream:
            events = set()
            content = False
            size = 0
            for line in response:
                size += len(line)
                if size > 1048576:
                    raise SystemExit('Smoke stream exceeds bounded response budget')
                if not line.startswith(b'data: '):
                    continue
                raw = line[6:].strip()
                if raw == b'[DONE]':
                    events.add('message_stop')
                    continue
                try:
                    item = json.loads(raw)
                except ValueError:
                    continue
                events.add(item.get('type', 'chat_chunk'))
                delta = item.get('delta', {})
                content = content or bool(delta.get('text') or delta.get('thinking'))
                for choice in item.get('choices', []):
                    content = content or bool(choice.get('delta', {}).get('content'))
                    if choice.get('finish_reason'):
                        events.add('message_stop')
            ok = response.status == 200 and content and 'message_stop' in events and 'error' not in events
            print(json.dumps({'provider': m['provider'], 'model': m['upstream'],
                              'status': response.status, 'validStream': ok,
                              'events': sorted(events)}))
            if not ok:
                raise SystemExit(1)
            return
        body = response.read(1048576)
        try:
            data = json.loads(body)
        except ValueError:
            data = {}
        if isinstance(data.get('data'), dict):
            data = data['data']
        ok = response.status == 200 and bool(data.get('content') or data.get('choices'))
        error = data.get('error', data)
        print(json.dumps({'provider': m['provider'], 'model': m['upstream'],
                          'status': response.status, 'validResponse': ok,
                          'errorType': error.get('type') if isinstance(error, dict) else None,
                          'errorMessage': str(error.get('message', '')).replace(secret, '<redacted>')[:400]
                          if isinstance(error, dict) else None}))
        if not ok:
            raise SystemExit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
