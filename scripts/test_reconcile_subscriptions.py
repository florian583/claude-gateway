"""Offline subscription migration invariants; synthetic config only."""
import copy
import unittest

from reconcile_subscriptions import reconcile


class ReconcileTests(unittest.TestCase):
    def fixture(self):
        source = {
            'providers': {'opencode-go': {'auth': {'type': 'command', 'command': ['fake']}}},
            'models': {'k3-old': {'provider': 'clinepass'},
                       'flash-other': {'provider': 'ollama'},
                       'k27-other': {'provider': 'moonshot'}},
            'modelPools': {'kimi-k3': {'models': ['k3-old', 'k3-other']},
                           'kimi-k2.7-code': {'models': ['k27-other']},
                           'glm-5.3-flash': {'models': ['flash-other']}},
            'aliases': {'sonnet': 'haiku'},
            'chains': {'haiku': {'steps': [{'pool': 'claude'}], 'allowPaidFallback': False}},
        }
        archive = {'providers': {'clinepass': {'billing': 'subscription', 'variant': 'clinepass',
                                              'auth': {'type': 'command', 'command': ['fake-cline']}}}}
        return source, archive

    def test_only_verified_flash_and_no_free_or_k3(self):
        source, archive = self.fixture()
        original = copy.deepcopy(source)
        result = reconcile(source, archive)
        self.assertEqual(source, original)
        self.assertEqual(result['modelPools']['glm-5.3-flash']['models'],
                         ['glm-5.3-flash-cline', 'flash-other'])
        self.assertEqual(result['modelPools']['glm-5.3-flash']['selection'], 'fixed-order')
        self.assertEqual(result['modelPools']['kimi-k3']['models'], ['k3-other'])
        self.assertEqual(result['modelPools']['kimi-k2.7-code']['models'], ['k27-other'])
        self.assertEqual([name for name, m in result['models'].items()
                          if m['provider'] == 'clinepass'], ['glm-5.3-flash-cline'])
        self.assertEqual(result['aliases'], source['aliases'])
        self.assertEqual(result['chains'], source['chains'])
        self.assertNotIn('opencode-free', result['providers'])
        self.assertEqual(result['providers']['clinepass']['auth'], archive['providers']['clinepass']['auth'])

    def test_idempotent(self):
        source, archive = self.fixture()
        result = reconcile(source, archive)
        self.assertEqual(reconcile(result, archive), result)

    def test_rejects_metered_cline(self):
        source, archive = self.fixture()
        archive['providers']['clinepass']['billing'] = 'metered'
        with self.assertRaises(ValueError):
            reconcile(source, archive)


if __name__ == '__main__':
    unittest.main()
