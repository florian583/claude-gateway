#!/usr/bin/env python3
"""Native Claude cloud client with durable subscription-profile ownership."""

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pty
import re
import select
import signal
import subprocess
import sys
import tempfile
import time
import tty
import termios
import urllib.error
import urllib.parse
import urllib.request


class CloudError(Exception):
    pass


def session_id(value):
    if value.startswith(('https://', 'http://', 'claude.ai/')):
        url = urllib.parse.urlsplit(value if '://' in value else 'https://' + value)
        if url.scheme != 'https' or url.netloc != 'claude.ai' or not url.path.startswith('/code/'):
            raise CloudError('Expected a claude.ai/code session URL')
        value = url.path[len('/code/'):]
    if not re.fullmatch(r'(?:session_|cse_)[A-Za-z0-9_-]{1,180}', value):
        raise CloudError('Expected a cloud session ID, not a local UUID or path')
    return 'cse_' + value[len('session_'):] if value.startswith('session_') else value


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def native_environment(profile):
    denied = {'CLAUDE_CONFIG_DIR', 'CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_API_KEY',
              'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_BASE_URL', 'ANTHROPIC_CUSTOM_HEADERS',
              'ANTHROPIC_MODEL', 'CLAUDE_CODE_SUBAGENT_MODEL', 'CLAUDE_CODE_SUBAGENT_MODEL_FORCE',
              'CLAUDE_CODE_USE_GATEWAY', 'CLAUDE_CODE_USE_BEDROCK', 'CLAUDE_CODE_USE_VERTEX',
              'CLAUDE_CODE_USE_FOUNDRY', 'CLAUDE_PROXY_CONFIG', 'ANTHROPIC_PROXY_CONFIG',
              'API_TIMEOUT_MS'}
    env = {k: v for k, v in os.environ.items() if k not in denied
           and not k.startswith(('ANTHROPIC_DEFAULT_', 'ANTHROPIC_CUSTOM_MODEL_'))}
    env['CLAUDE_CONFIG_DIR'] = profile['configDir']
    return env


def identity(profile):
    directory = Path(profile['configDir']).expanduser().resolve()
    metadata = directory / '.claude.json'
    if not metadata.is_file():
        raise CloudError('Profile account metadata is missing; use profiles login for that profile')
    account = json.loads(metadata.read_text()).get('oauthAccount', {})
    parts = [account.get('accountUuid'), account.get('organizationUuid')]
    if not all(isinstance(p, str) and p for p in parts):
        raise CloudError('Profile account identity is unknown; use profiles login for that profile')
    return hashlib.sha256(json.dumps(parts).encode()).hexdigest(), parts[1]


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.cloud-')
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(data, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def finish_child(pid, grace=2):
    def notify(signum):
        try:
            os.killpg(pid, signum)
        except ProcessLookupError:
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass
    try:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            return os.waitstatus_to_exitcode(status)
        notify(signal.SIGHUP)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                return os.waitstatus_to_exitcode(status)
            time.sleep(0.05)
        notify(signal.SIGKILL)
        return os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1])
    except ChildProcessError:
        return 1


class Sessions:
    def __init__(self, config):
        self.config = config
        self.root = Path(config['stateDir']) / 'cloud-sessions'

    def path(self, value):
        return self.root / (session_id(value) + '.json')

    def profile(self, name):
        p = self.config['profiles'].get(name)
        if not p:
            raise CloudError('Saved or selected profile is not configured; no account fallback is allowed')
        return p

    @contextlib.contextmanager
    def lock(self, value):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(str(self.path(value)) + '.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CloudError('Another command owns this cloud session; inspect its outcome first')
            yield
        finally:
            os.close(fd)

    def bind(self, value, profile, parent=None):
        sid = session_id(value)
        fingerprint, _ = identity(self.profile(profile))
        with self.lock(sid):
            if self.path(sid).exists():
                old = self.owner(sid)
                if old['profile'] != profile or old['accountFingerprint'] != fingerprint:
                    raise CloudError('Cloud session already belongs to another profile')
                return old
            row = {'version': 1, 'sessionId': sid, 'url': 'https://claude.ai/code/' + sid,
                   'profile': profile, 'accountFingerprint': fingerprint,
                   'parentSession': parent, 'createdAt': now()}
            atomic_json(self.path(sid), row)
            return row

    def owner(self, value):
        sid = session_id(value)
        try:
            row = json.loads(self.path(sid).read_text())
        except FileNotFoundError:
            raise CloudError('Owner unknown: bind this session with an explicit profile first')
        if row.get('version') != 1 or row.get('sessionId') != sid:
            raise CloudError('Invalid cloud ownership record')
        fingerprint, _ = identity(self.profile(row['profile']))
        if fingerprint != row['accountFingerprint']:
            raise CloudError('Profile account identity changed; restore its original login, do not rebind')
        return row

    def choose_profile(self, explicit, parent=None):
        name = explicit or os.environ.get('CLAUDE_CLOUD_PROFILE')
        if name:
            self.profile(name)
            return name
        if parent:
            metrics = Path(self.config['stateDir']) / 'metrics.jsonl'
            try:
                with metrics.open('rb') as stream:
                    stream.seek(0, 2)
                    length = stream.tell()
                    stream.seek(max(0, length - 4 * 1024 * 1024))
                    if length > 4 * 1024 * 1024:
                        stream.readline()
                    rows = stream.read().splitlines()
                for line in reversed(rows):
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if row.get('session') == parent and row.get('success') is True and row.get('claudeProfile'):
                        self.profile(row['claudeProfile'])
                        return row['claudeProfile']
            except FileNotFoundError:
                pass
            raise CloudError('No exact parent subscription evidence; select an explicit profile')
        directory = os.environ.get('CLAUDE_HYBRID_CONFIG_DIR', str(Path.home() / '.claude-main'))
        try:
            fingerprint, _ = identity({'configDir': directory})
            matches = []
            for name, p in self.config['profiles'].items():
                try:
                    if identity(p)[0] == fingerprint:
                        matches.append(name)
                except (CloudError, ValueError, OSError):
                    continue
        except (CloudError, ValueError, OSError):
            matches = []
        if len(matches) == 1:
            return matches[0]
        raise CloudError('Cloud subscription is unknown; select an explicit profile with --profile or CLAUDE_CLOUD_PROFILE')

    def argv(self, profile, args):
        command = self.profile(profile).get('command', [])
        if not command or Path(command[0]).name in {'claude-hybrid', 'claude-orchestrated', 'claude-cloud'}:
            raise CloudError('The cloud profile needs a native Claude command, not a proxy wrapper')
        return command + args

    def request(self, profile, path):
        p = self.profile(profile)
        _, org = identity(p)
        service = p.get('credentialsService')
        if not service:
            raise CloudError('Profile has no credential service; configure it through the proxy')
        try:
            result = subprocess.run(['/usr/bin/security', 'find-generic-password', '-s', service, '-w'],
                                    capture_output=True, timeout=5, check=True)
            oauth = json.loads(result.stdout).get('claudeAiOauth', {})
            token = oauth.get('accessToken')
        except (subprocess.SubprocessError, ValueError):
            raise CloudError('Profile credentials unavailable; use its native login in the desktop Keychain context')
        if not token:
            raise CloudError('Profile has no Claude subscription token')
        request = urllib.request.Request('https://api.anthropic.com' + path, headers={
            'Authorization': 'Bearer ' + token, 'x-organization-uuid': org,
            'anthropic-version': '2023-06-01', 'User-Agent': 'claude-gateway-cloud/1',
            'anthropic-client-platform': 'cli'})
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            with opener.open(request, timeout=20) as response:
                data = response.read(8 * 1024 * 1024 + 1)
            if len(data) > 8 * 1024 * 1024:
                raise CloudError('Cloud response exceeds the 8 MiB bound; use a smaller event page')
            return json.loads(data)
        except urllib.error.HTTPError as error:
            raise CloudError('Cloud read HTTP %d for the saved profile. Use native Claude for login/device or organization approval; no account fallback was attempted.' % error.code)
        except (urllib.error.URLError, TimeoutError, ValueError):
            raise CloudError('Cloud read unavailable or incompatible; no retry or account fallback was attempted')

    def read(self, value, cursor=None):
        row = self.owner(value)
        sid, profile = row['sessionId'], row['profile']
        reply = self.request(profile, '/v1/code/sessions/' + sid)
        session = reply.get('response_shape', reply.get('session', {}))
        if not isinstance(session, dict) or session.get('id') != sid:
            raise CloudError('Cloud returned an incompatible session response')
        query = {'limit': 100, 'sort_order': 'desc'}
        if cursor:
            query['cursor'] = cursor
        events = self.request(profile, '/v1/code/sessions/' + sid + '/events?' + urllib.parse.urlencode(query))
        if not isinstance(events, dict) or not isinstance(events.get('data'), list):
            raise CloudError('Cloud returned an incompatible event response')
        messages = []
        for event in reversed(events['data']):
            payload = event.get('payload') if isinstance(event, dict) else None
            if not isinstance(payload, dict) or not isinstance(payload.get('type'), str):
                raise CloudError('Cloud returned an incompatible event payload')
            if payload.get('type') == 'assistant':
                message = payload.get('message')
                content = message.get('content') if isinstance(message, dict) else None
                if not isinstance(content, list) or any(not isinstance(c, dict) for c in content):
                    raise CloudError('Cloud returned incompatible assistant content')
                messages.extend(c['text'] for c in content if c.get('type') == 'text' and isinstance(c.get('text'), str))
        return {'sessionId': sid, 'profile': profile, 'url': row['url'],
                'status': session.get('session_status', session.get('status', 'unknown')),
                'workerStatus': session.get('worker_status', 'unknown'),
                'title': session.get('title'), 'assistantMessages': messages,
                'nextCursor': events.get('next_cursor'), 'truncated': bool(events.get('next_cursor')),
                'source': 'Claude Code cloud control API; compatibility pinned to native 2.1.311'}

    def send(self, value, message):
        sid = session_id(value)
        digest = hashlib.sha256(message.encode()).hexdigest()
        with self.lock(sid):
            row = self.owner(sid)
            previous = row.get('lastSend', {})
            if previous.get('delivery') in {'pending', 'unknown'}:
                raise CloudError('Previous send outcome unknown. Read the session and resolve-send before another message')
            if previous.get('messageHash') == digest and previous.get('delivery') in {'pending', 'unknown', 'queued'}:
                raise CloudError('This message may already be queued. Read the session; do not resend automatically')
            row['lastSend'] = {'messageHash': digest, 'delivery': 'pending', 'at': now()}
            atomic_json(self.path(sid), row)
            try:
                result = subprocess.run(self.argv(row['profile'], ['-p', message, '--cloud', sid, '--output-format', 'json']),
                                        env=native_environment(self.profile(row['profile'])), capture_output=True, text=True, timeout=45)
                response = json.loads(result.stdout)
                delivery = 'queued' if result.returncode == 0 and isinstance(response, dict) and response.get('ok') is True and session_id(response.get('session_id', '')) == sid else 'unknown'
            except (subprocess.SubprocessError, OSError, ValueError, TypeError, CloudError):
                delivery = 'unknown'
            row['lastSend']['delivery'] = delivery
            atomic_json(self.path(sid), row)
            if delivery != 'queued':
                raise CloudError('Cloud send outcome unknown. Do not resend; read the saved-profile session first')
            return {'sessionId': sid, 'profile': row['profile'], 'delivery': 'queued', 'modelResult': 'not read; use read'}

    def resolve_send(self, value, delivery, evidence):
        if not evidence.strip() or len(evidence) > 1000:
            raise CloudError('Resolution needs a short reason based on the remote session, not a timeout guess')
        with self.lock(value):
            row = self.owner(value)
            if row.get('lastSend', {}).get('delivery') not in {'pending', 'unknown'}:
                raise CloudError('No unresolved send receipt exists')
            row['lastSend'].update(delivery=delivery, reviewedAt=now(), resolution=evidence)
            atomic_json(self.path(value), row)
        return row['lastSend']

    def launch(self, profile, args, parent=None):
        cloud_value = None
        for i, arg in enumerate(args):
            if arg in {'--cloud', '--remote'}:
                cloud_value = args[i + 1] if i + 1 < len(args) and not args[i + 1].startswith('-') else None
            elif arg.startswith(('--cloud=', '--remote=')):
                cloud_value = arg.split('=', 1)[1]
        existing = None
        if cloud_value:
            try:
                existing = session_id(cloud_value)
            except CloudError:
                pass
        if existing:
            row = self.owner(existing)
            if profile and profile != row['profile']:
                raise CloudError('Cloud session already belongs to another profile')
            profile = row['profile']
        else:
            profile = self.choose_profile(profile, parent)
        identity(self.profile(profile))
        argv = self.argv(profile, args)
        env = native_environment(self.profile(profile))
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            if existing:
                return subprocess.call(argv, env=env)
            raise CloudError('Creating a cloud session requires a TTY. Run launch in a terminal or a named tmux pane; native prompts remain interactive')
        print('Cloud subscription profile: ' + profile, file=sys.stderr, flush=True)
        started = time.time()
        previous = termios.tcgetattr(sys.stdin.fileno())
        pid, master = pty.fork()
        if pid == 0:
            os.execvpe(argv[0], argv, env)
        seen, buffer = set(), ''
        old_signals = {}
        def interrupted(signum, frame):
            raise CloudError('Local cloud client interrupted; inspect the saved session before restarting work')
        try:
            for sig in [signal.SIGTERM, signal.SIGHUP]:
                old_signals[sig] = signal.signal(sig, interrupted)
            tty.setraw(sys.stdin.fileno())
            while True:
                ready, _, _ = select.select([master, sys.stdin.fileno()], [], [], 1)
                if master in ready:
                    try:
                        output = os.read(master, 65536)
                    except OSError:
                        break
                    if not output:
                        break
                    os.write(sys.stdout.fileno(), output)
                    buffer = (buffer + output.decode(errors='replace'))[-32768:]
                    plain = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', buffer)
                    for captured in re.findall(r'https://claude\.ai/code/((?:session_|cse_)[A-Za-z0-9_-]+)', plain):
                        sid = session_id(captured)
                        if sid in seen:
                            continue
                        seen.add(sid)
                        try:
                            response = self.request(profile, '/v1/code/sessions/' + sid)
                            session = response.get('response_shape', response.get('session', {}))
                            created = datetime.datetime.fromisoformat(session.get('created_at', '').replace('Z', '+00:00')).timestamp()
                            if session.get('id') == sid and created >= started - 5:
                                self.bind(sid, profile, parent)
                        except (CloudError, ValueError, TypeError):
                            os.write(sys.stderr.fileno(), b'\r\nCloud ownership could not be verified; use bind with this profile before a follow-up.\r\n')
                if sys.stdin.fileno() in ready:
                    data = os.read(sys.stdin.fileno(), 65536)
                    if data:
                        os.write(master, data)
                    else:
                        break
        finally:
            try:
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, previous)
            finally:
                os.close(master)
                code = finish_child(pid)
                for sig, handler in old_signals.items():
                    signal.signal(sig, handler)
        return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=os.environ.get('CLAUDE_PROXY_CONFIG', str(Path.home() / '.config/claude-proxy/config.json')))
    sub = parser.add_subparsers(dest='action', required=True)
    launch = sub.add_parser('launch')
    launch.add_argument('--profile')
    launch.add_argument('--parent-session', default=os.environ.get('CLAUDE_CODE_SESSION_ID'))
    launch.add_argument('args', nargs=argparse.REMAINDER)
    bind = sub.add_parser('bind')
    bind.add_argument('session')
    bind.add_argument('--profile', required=True)
    read = sub.add_parser('read')
    read.add_argument('session')
    read.add_argument('--cursor')
    send = sub.add_parser('send')
    send.add_argument('session')
    send.add_argument('--message-file', required=True)
    resolve = sub.add_parser('resolve-send')
    resolve.add_argument('session')
    resolve.add_argument('--delivery', choices=['queued', 'not-queued'], required=True)
    resolve.add_argument('--evidence', required=True)
    sub.add_parser('list')
    args = parser.parse_args()
    config = json.loads(Path(args.config).expanduser().read_text())
    sessions = Sessions(config)
    if args.action == 'launch':
        return sessions.launch(args.profile, args.args[1:] if args.args[:1] == ['--'] else args.args, args.parent_session)
    if args.action == 'bind':
        sid = session_id(args.session)
        response = sessions.request(args.profile, '/v1/code/sessions/' + sid)
        session = response.get('response_shape', response.get('session', {}))
        if session.get('id') != sid:
            raise CloudError('Session ownership could not be verified with the explicit profile')
        result = sessions.bind(sid, args.profile)
    elif args.action == 'read':
        result = sessions.read(args.session, args.cursor)
    elif args.action == 'send':
        message = Path(args.message_file).read_text()
        if not message.strip() or len(message.encode()) > 128 * 1024:
            raise CloudError('Message must contain 1 to 131072 bytes')
        result = sessions.send(args.session, message)
    elif args.action == 'resolve-send':
        result = sessions.resolve_send(args.session, args.delivery, args.evidence)
    else:
        paths = sorted(sessions.root.glob('*.json'))
        if len(paths) > 10000:
            raise CloudError('Local registry exceeds the 10000-session read bound')
        result = [sessions.owner(path.stem) for path in paths]
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (CloudError, OSError, ValueError, KeyError) as error:
        print('claude-cloud: ' + (str(error) if isinstance(error, CloudError) else 'Configuration or local state is unavailable or invalid'), file=sys.stderr)
        sys.exit(1)
