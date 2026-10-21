#!/usr/bin/env python3
"""Black-box generic provider fallback on an explicit binary; loopback only.

Account-pool identity/rotation has separate Go tests. This fixture intentionally
uses no profiles, credentials, real subscription endpoints or public requests.
"""
import argparse
from contextlib import contextmanager
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest

from subscription_fallbacks import configure
from test_subscription_fallbacks import fixture


BINARY = None


class RuntimeTests(unittest.TestCase):
    @contextmanager
    def gateway(self, failures=None, stream=False):
        calls = []
        failures = failures or {}

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                provider = self.path.split('/')[1]
                calls.append((provider, body['model']))
                if stream and provider == 'anthropic':
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream')
                    self.end_headers()
                    for event, value in [
                        ('message_start', {'type': 'message_start', 'message': {
                            'id': 'msg_synthetic', 'type': 'message', 'role': 'assistant',
                            'model': body['model'], 'content': [], 'usage': {
                                'input_tokens': 1, 'output_tokens': 0}}}),
                        ('content_block_start', {'type': 'content_block_start', 'index': 0,
                         'content_block': {'type': 'text', 'text': ''}}),
                        ('content_block_delta', {'type': 'content_block_delta', 'index': 0,
                         'delta': {'type': 'text_delta', 'text': 'started'}}),
                        ('error', {'type': 'error', 'error': {
                            'type': 'overloaded_error', 'message': 'synthetic interruption'}}),
                    ]:
                        self.wfile.write(('event: ' + event + '\ndata: ' + json.dumps(value)
                                          + '\n\n').encode())
                        self.wfile.flush()
                    self.close_connection = True
                    return
                status = failures.get(provider, 200)
                if status != 200:
                    kind = 'rate_limit_error' if status == 429 else 'authentication_error'
                    if status == 400:
                        kind = 'invalid_request_error'
                    value = {'error': {'type': kind, 'message':
                             'quota exceeded' if status == 429 else 'synthetic rejection'}}
                else:
                    value = {'id': 'msg_synthetic', 'type': 'message', 'role': 'assistant',
                             'model': body['model'], 'content': [{'type': 'text', 'text': 'ok'}],
                             'stop_reason': 'end_turn', 'usage': {'input_tokens': 1, 'output_tokens': 1}}
                data = json.dumps(value).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        with tempfile.TemporaryDirectory(prefix='subscription-fallback-test-') as folder:
            root = Path(folder)
            upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
            upstream.daemon_threads = True
            thread = threading.Thread(target=upstream.serve_forever, daemon=True)
            thread.start()
            with socket.socket() as available:
                available.bind(('127.0.0.1', 0))
                port = available.getsockname()[1]
            config = configure(fixture())
            config.update(listen='127.0.0.1:' + str(port), stateDir=str(root / 'state'),
                          profiles={}, accountPools={})
            config['client'] = {'command': ['/usr/bin/false'], 'configDir': str(root / 'client'),
                                'model': 'opus', 'sonnetModel': 'sonnet', 'haikuModel': 'haiku'}
            for name in list(config['providers']):
                config['providers'][name] = {
                    'protocol': 'anthropic', 'billing': 'local', 'auth': {'type': 'none'},
                    'baseURL': 'http://127.0.0.1:' + str(upstream.server_port) + '/' + name}
            for model in config['models'].values():
                model.update(contextWindow=1000000, supportsTools=True, supportsImages=True)
            path = root / 'gateway.json'
            path.write_text(json.dumps(config))
            validation = subprocess.run([BINARY, '--config', str(path), 'validate-config'],
                                        capture_output=True, text=True, timeout=10)
            self.assertEqual(validation.returncode, 0, validation.stderr)
            log = (root / 'gateway.log').open('wb')
            env = {k: v for k, v in os.environ.items()
                   if not k.startswith(('ANTHROPIC_', 'CLAUDE_', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY'))}
            env['HOME'] = str(root)
            process = subprocess.Popen([BINARY, '--config', str(path), 'serve'], env=env,
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        log.flush()
                        self.fail('isolated gateway failed: ' + (root / 'gateway.log').read_text()[-1200:])
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.05)
                else:
                    self.fail('isolated gateway startup timeout')

                def send(alias='opus', tools=False):
                    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
                    body = {'model': alias, 'max_tokens': 8, 'stream': stream,
                            'messages': [{'role': 'user', 'content': 'synthetic check'}]}
                    if tools:
                        body['tools'] = [{'name': 'synthetic_tool', 'description': 'test',
                                          'input_schema': {'type': 'object', 'properties': {}}}]
                    try:
                        connection.request('POST', '/v1/messages', json.dumps(body),
                                           {'Content-Type': 'application/json'})
                        response = connection.getresponse()
                        return response.status, response.read().decode()
                    finally:
                        connection.close()
                yield send, calls
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                log.close()
                upstream.shutdown()
                upstream.server_close()
                thread.join(timeout=2)

    def test_healthy_primary_does_not_call_fallback(self):
        with self.gateway() as (send, calls):
            self.assertEqual(send()[0], 200)
            self.assertEqual(calls, [('anthropic', 'claude-opus-test')])

    def test_401_redirects_to_kimi_subscription(self):
        with self.gateway({'anthropic': 401}) as (send, calls):
            self.assertEqual(send()[0], 200)
            self.assertEqual(calls, [('anthropic', 'claude-opus-test'), ('moonshot', 'k3')])

    def test_403_redirects_to_kimi_subscription(self):
        with self.gateway({'anthropic': 403}) as (send, calls):
            self.assertEqual(send()[0], 200)
            self.assertEqual(calls[-1], ('moonshot', 'k3'))

    def test_quota_redirects_to_kimi_subscription(self):
        with self.gateway({'anthropic': 429}) as (send, calls):
            self.assertEqual(send()[0], 200)
            self.assertEqual(calls[-1], ('moonshot', 'k3'))

    def test_unavailable_kimi_subscription_redirects_to_ollama_kimi(self):
        with self.gateway({'anthropic': 401, 'moonshot': 429}) as (send, calls):
            self.assertEqual(send(tools=True)[0], 200)
            self.assertEqual(calls[-2:], [('moonshot', 'k3'), ('ollama', 'kimi-k3:cloud')])

    def test_haiku_auth_redirects_to_glm_flash_not_kimi(self):
        with self.gateway({'anthropic': 401}) as (send, calls):
            self.assertEqual(send('haiku')[0], 200)
            self.assertEqual(calls[-1], ('ollama', 'glm-5.3-flash:cloud'))
            self.assertFalse(any(provider == 'moonshot' for provider, _ in calls))

    def test_haiku_glm_cloud_failure_redirects_to_bigmodel_glm_flash(self):
        with self.gateway({'anthropic': 429, 'ollama': 503}) as (send, calls):
            self.assertEqual(send('haiku', tools=True)[0], 200)
            self.assertEqual(calls[-1], ('bigmodel', 'glm-5.3-flash'))

    def test_sonnet_shared_primary_kept_but_fallback_is_kimi(self):
        with self.gateway({'anthropic': 401}) as (send, calls):
            self.assertEqual(send('sonnet[1m]')[0], 200)
            self.assertEqual(calls, [('anthropic', 'claude-haiku-test'), ('moonshot', 'k3')])

    def test_invalid_request_does_not_trigger_provider_fallback(self):
        with self.gateway({'anthropic': 400}) as (send, calls):
            self.assertEqual(send()[0], 400)
            self.assertEqual(len(calls), 1)

    def test_partial_stream_is_not_replayed_on_fallback(self):
        with self.gateway(stream=True) as (send, calls):
            status, body = send()
            self.assertEqual(status, 200)
            self.assertIn('started', body)
            self.assertEqual(calls, [('anthropic', 'claude-opus-test')])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    args = parser.parse_args()
    BINARY = str(Path(args.binary).resolve(strict=True))
    unittest.main(argv=[__file__])
