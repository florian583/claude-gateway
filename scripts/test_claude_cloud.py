import importlib.util
import json
import os
from pathlib import Path
import subprocess
import signal
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('claude_cloud', Path(__file__).with_name('claude_cloud.py'))
cloud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud)


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        profiles = {}
        for name in ['work', 'backup']:
            directory = self.root / name
            directory.mkdir()
            (directory / '.claude.json').write_text(json.dumps({'oauthAccount': {
                'accountUuid': name + '-account', 'organizationUuid': 'test-org'}}))
            profiles[name] = {'configDir': str(directory), 'credentialsService': 'test-' + name,
                              'command': ['/synthetic/claude']}
        self.config = {'profiles': profiles, 'stateDir': str(self.root / 'state')}
        self.manager = cloud.Sessions(self.config)

    def test_saved_owner_survives_other_profile_selection(self):
        self.manager.bind('session_test', 'work')
        with patch.dict(os.environ, {'CLAUDE_CLOUD_PROFILE': 'backup'}):
            self.assertEqual('work', self.manager.owner('session_test')['profile'])
        with self.assertRaisesRegex(cloud.CloudError, 'already belongs'):
            self.manager.bind('session_test', 'backup')
        record = self.manager.owner('session_test')
        self.assertEqual(0o600, self.manager.path('session_test').stat().st_mode & 0o777)
        self.assertNotIn('token', json.dumps(record).lower())

    def test_unknown_owner_is_not_guessed(self):
        with self.assertRaisesRegex(cloud.CloudError, 'bind'):
            self.manager.owner('session_missing')

    def test_profile_account_change_refuses_reuse(self):
        self.manager.bind('session_test', 'work')
        p = self.root / 'work' / '.claude.json'
        p.write_text(json.dumps({'oauthAccount': {'accountUuid': 'different-account', 'organizationUuid': 'test-org'}}))
        with self.assertRaisesRegex(cloud.CloudError, 'identity changed'):
            self.manager.owner('session_test')

    def test_parent_resolution_is_exact_successful_subscription_evidence(self):
        state = self.root / 'state'
        state.mkdir()
        rows = [
            {'session': 'other', 'claudeProfile': 'backup', 'success': True},
            {'session': 'parent', 'claudeProfile': 'work', 'success': True},
            {'session': 'parent', 'claudeProfile': 'backup', 'success': False},
        ]
        (state / 'metrics.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows))
        self.assertEqual('work', self.manager.choose_profile(None, 'parent'))
        with self.assertRaisesRegex(cloud.CloudError, 'explicit profile'):
            self.manager.choose_profile(None, 'unknown')

    def test_environment_drops_routing_and_keeps_native_policy(self):
        with patch.dict(os.environ, {'ANTHROPIC_AUTH_TOKEN': 'synthetic-gateway',
                                    'ANTHROPIC_BASE_URL': 'http://127.0.0.1:1',
                                    'ANTHROPIC_DEFAULT_OPUS_MODEL': 'proxy-alias',
                                    'CLAUDECODE': '1', 'CLAUDE_CODE_USE_GATEWAY': '1'}):
            env = cloud.native_environment(self.config['profiles']['work'])
        self.assertNotIn('ANTHROPIC_AUTH_TOKEN', env)
        self.assertNotIn('ANTHROPIC_BASE_URL', env)
        self.assertNotIn('ANTHROPIC_DEFAULT_OPUS_MODEL', env)
        self.assertNotIn('CLAUDE_CODE_USE_GATEWAY', env)
        self.assertEqual('1', env['CLAUDECODE'])
        self.assertEqual(str(self.root / 'work'), env['CLAUDE_CONFIG_DIR'])

    def test_send_acknowledges_queue_and_never_retries_timeout(self):
        self.manager.bind('session_test', 'work')
        with patch.object(cloud.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '{"ok":true,"session_id":"session_test"}', '')) as run:
            result = self.manager.send('session_test', 'Continue')
            self.assertEqual('queued', result['delivery'])
            self.assertEqual(1, run.call_count)
            self.assertEqual(['/synthetic/claude', '-p', 'Continue', '--cloud', 'cse_test', '--output-format', 'json'], run.call_args.args[0])
        with patch.object(cloud.subprocess, 'run', side_effect=subprocess.TimeoutExpired('synthetic', 30)) as run:
            with self.assertRaisesRegex(cloud.CloudError, 'unknown.*Do not resend'):
                self.manager.send('session_test', 'Continue again')
            self.assertEqual(1, run.call_count)
        self.assertEqual('unknown', self.manager.owner('session_test')['lastSend']['delivery'])

    def test_results_read_saved_profile_and_surface_pagination(self):
        self.manager.bind('session_test', 'work')
        def request(profile, path):
            self.assertEqual('work', profile)
            if '/events?' in path:
                return {'data': [{'payload': {'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'Done'}]}}}], 'next_cursor': 'older'}
            return {'session': {'id': 'cse_test', 'session_status': 'idle'}}
        with patch.object(self.manager, 'request', side_effect=request):
            result = self.manager.read('session_test')
        self.assertEqual('Done', result['assistantMessages'][0])
        self.assertEqual('older', result['nextCursor'])
        self.assertTrue(result['truncated'])

    def test_unknown_send_blocks_intervening_send_until_reviewed(self):
        self.manager.bind('session_test', 'work')
        with patch.object(cloud.subprocess, 'run', side_effect=subprocess.TimeoutExpired('synthetic', 30)):
            with self.assertRaises(cloud.CloudError):
                self.manager.send('session_test', 'A')
        with patch.object(cloud.subprocess, 'run') as run:
            with self.assertRaisesRegex(cloud.CloudError, 'Previous send'):
                self.manager.send('session_test', 'B')
            run.assert_not_called()

    def test_incompatible_events_are_an_error_not_empty_history(self):
        self.manager.bind('session_test', 'work')
        for events in [{}, {'error': 'incompatible'}, {'data': None}, {'data': [{}]},
                       {'data': [{'payload': {'type': 'assistant', 'message': {'content': 'wrong'}}}]}]:
            with self.subTest(events=events), patch.object(self.manager, 'request', side_effect=[
                    {'session': {'id': 'session_test'}}, events]):
                with self.assertRaisesRegex(cloud.CloudError, 'incompatible'):
                    self.manager.read('session_test')

    def test_normalize_rejects_untrusted_hosts_and_path_traversal(self):
        self.assertEqual('cse_test', cloud.session_id('https://claude.ai/code/session_test?from=cli'))
        self.assertEqual('cse_test', cloud.session_id('cse_test'))
        for value in ['https://evil.invalid/code/session_test', '../session_test', 'session_test/other']:
            with self.assertRaises(cloud.CloudError):
                cloud.session_id(value)

    def test_cleanup_reaps_only_the_owned_client(self):
        process = subprocess.Popen(['/bin/sleep', '30'], start_new_session=True)
        try:
            result = cloud.finish_child(process.pid, grace=0.2)
            process.returncode = result
            self.assertEqual(-signal.SIGHUP, result)
            with self.assertRaises(ChildProcessError):
                os.waitpid(process.pid, os.WNOHANG)
        finally:
            if process.returncode is None:
                process.kill()
                process.wait()

    def test_reviewed_resolution_is_required_before_a_later_message(self):
        self.manager.bind('session_test', 'work')
        with patch.object(cloud.subprocess, 'run', side_effect=subprocess.TimeoutExpired('synthetic', 30)):
            with self.assertRaises(cloud.CloudError):
                self.manager.send('session_test', 'A')
        self.manager.resolve_send('session_test', 'not-queued', 'Inspected remote history and confirmed no matching message')
        with patch.object(cloud.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '{"ok":true,"session_id":"cse_test"}', '')):
            self.assertEqual('queued', self.manager.send('session_test', 'B')['delivery'])

    def test_cleanup_handles_a_child_without_its_own_process_group(self):
        process = subprocess.Popen(['/bin/sleep', '30'])
        try:
            result = cloud.finish_child(process.pid, grace=0.2)
            self.assertEqual(-signal.SIGHUP, result)
            process.returncode = result
            with self.assertRaises(ChildProcessError):
                os.waitpid(process.pid, os.WNOHANG)
        finally:
            if process.returncode is None:
                process.kill()
                process.wait()


if __name__ == '__main__':
    unittest.main()
