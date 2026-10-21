import copy
from pathlib import Path
import tempfile
import unittest

from configure_kimi_oauth import prepare


class NativeKimiSetupTests(unittest.TestCase):
    def test_uses_existing_oauth_reference_without_reading_tokens(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / 'config.toml').write_text('''
[providers."managed:kimi-code"]
base_url = "https://api.kimi.ai/coding/v1"
api_key = ""
[providers."managed:kimi-code".oauth]
storage = "file"
key = "oauth/kimi-code-env-synthetic"
oauth_host = "https://auth.kimi.ai"
''')
            source = {'providers': {'moonshot': {'auth': {'type': 'env', 'name': 'OLD_KEY'}}},
                      'models': {'kimi': {'provider': 'moonshot', 'upstream': 'k3'},
                                 'other': {'provider': 'other', 'upstream': 'unchanged'}}}
            original = copy.deepcopy(source)
            result = prepare(source, home)
            self.assertEqual(source, original)
            provider = result['providers']['moonshot']
            self.assertEqual(provider['auth']['type'], 'kimi-code-oauth')
            self.assertEqual(provider['auth']['statePath'], str(home / 'credentials/kimi-code-env-synthetic.json'))
            self.assertEqual(result['models']['kimi']['upstream'], 'kimi-for-coding')
            self.assertEqual(result['models']['other'], original['models']['other'])
            self.assertFalse((home / 'credentials').exists())

    def test_rejects_cross_region_and_credential_traversal(self):
        for host, key in [('https://auth.kimi.com', 'oauth/synthetic'),
                          ('https://auth.kimi.ai', 'oauth/../synthetic')]:
            with tempfile.TemporaryDirectory() as directory:
                home = Path(directory)
                (home / 'config.toml').write_text('''
[providers."managed:kimi-code"]
base_url = "https://api.kimi.ai/coding/v1"
[providers."managed:kimi-code".oauth]
storage = "file"
key = "''' + key + '''"
oauth_host = "''' + host + '''"
''')
                with self.assertRaises(ValueError):
                    prepare({'providers': {}, 'models': {}}, home)


if __name__ == '__main__':
    unittest.main()
