import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "subscription_fallbacks", Path(__file__).with_name("subscription_fallbacks.py"))
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def fixture():
    return {
        "version": 1,
        "providers": {
            "anthropic": {"variant": "claude-subscription", "billing": "subscription",
                          "auth": {"type": "claude-profile-pool", "pool": "shared"}},
            "moonshot": {"billing": "subscription"},
            "ollama": {"billing": "subscription"},
            "bigmodel": {"billing": "subscription"},
            "metered": {"billing": "metered"},
        },
        "models": {
            "opus": {"provider": "anthropic", "upstream": "claude-opus-test"},
            "haiku": {"provider": "anthropic", "upstream": "claude-haiku-test"},
            "kimi-sub": {"provider": "moonshot", "upstream": "k3"},
            "kimi-cloud": {"provider": "ollama", "upstream": "kimi-k3:cloud"},
            "glm-sub": {"provider": "bigmodel", "upstream": "glm-5.3-flash"},
            "glm-cloud": {"provider": "ollama", "upstream": "glm-5.3-flash:cloud"},
            "other": {"provider": "metered", "upstream": "unrelated"},
        },
        "modelPools": {},
        "chains": {
            "main": {"steps": [{"model": "opus"}, {"model": "other"}],
                     "allowPaidFallback": True, "paidFallbackOn": ["quota-exhausted"],
                     "softConcurrency": 4, "minimumContextWindow": 0, "requireTools": True},
            "small": {"steps": [{"model": "haiku"}], "allowPaidFallback": False},
            "unrelated": {"steps": [{"model": "other"}]},
        },
        "aliases": {"opus": "main", "claude-opus-test": "main", "haiku": "small",
                    "haiku[1m]": "small", "claude-haiku-test": "small",
                    "sonnet": "small", "sonnet[1m]": "small", "unrelated": "unrelated"},
        "profiles": {"first": {"configDir": "profiles/first"}},
        "accountPools": {"shared": {"profiles": ["first"]}},
        "client": {"model": "opus", "sonnetModel": "sonnet", "haikuModel": "haiku"},
    }


class SubscriptionFallbackTests(unittest.TestCase):
    def test_exact_subscription_pools_and_no_paid_overflow(self):
        old = fixture()
        new = module.configure(old)
        self.assertEqual(new["modelPools"][module.KIMI_POOL]["models"], ["kimi-sub", "kimi-cloud"])
        self.assertEqual(new["modelPools"][module.GLM_POOL]["models"], ["glm-cloud", "glm-sub"])
        self.assertEqual(new["chains"]["main"]["steps"],
                         [{"model": "opus"}, {"pool": module.KIMI_POOL}])
        self.assertFalse(new["chains"]["main"]["allowPaidFallback"])
        self.assertNotIn("paidFallbackOn", new["chains"]["main"])
        self.assertNotIn("softConcurrency", new["chains"]["main"])
        self.assertTrue(new["chains"]["main"]["requireTools"])

    def test_shared_haiku_sonnet_chain_splits_without_changing_primary(self):
        old = fixture()
        new = module.configure(old)
        for alias in old["aliases"]:
            before = old["chains"][old["aliases"][alias]]["steps"][0]
            after = new["chains"][new["aliases"][alias]]["steps"][0]
            self.assertEqual(before, after, alias)
        self.assertNotEqual(new["aliases"]["sonnet"], new["aliases"]["haiku"])
        self.assertEqual(new["chains"][new["aliases"]["haiku"]]["steps"][1],
                         {"pool": module.GLM_POOL})
        self.assertEqual(new["chains"][new["aliases"]["sonnet"]]["steps"][1],
                         {"pool": module.KIMI_POOL})

    def test_idempotent_and_input_unmodified(self):
        old = fixture()
        snapshot = copy.deepcopy(old)
        new = module.configure(old)
        self.assertEqual(old, snapshot)
        self.assertEqual(module.configure(new), new)

    def test_preserves_unrelated_routes_credentials_and_account_policy(self):
        old = fixture()
        new = module.configure(old)
        for section in ["profiles", "accountPools", "providers", "client", "models"]:
            self.assertEqual(new[section], old[section], section)
        self.assertEqual(new["chains"]["unrelated"], old["chains"]["unrelated"])
        self.assertEqual(new["aliases"]["unrelated"], old["aliases"]["unrelated"])

    def test_missing_ambiguous_or_metered_pool_member_fails_closed(self):
        for change in ["missing", "ambiguous", "metered"]:
            value = fixture()
            if change == "missing":
                del value["models"]["kimi-sub"]
            elif change == "ambiguous":
                value["models"]["duplicate"] = value["models"]["kimi-sub"]
            else:
                value["providers"]["moonshot"]["billing"] = "metered"
            with self.subTest(change=change), self.assertRaises(ValueError):
                module.configure(value)

    def test_existing_different_reserved_pool_is_not_overwritten(self):
        value = fixture()
        value["modelPools"][module.KIMI_POOL] = {"models": ["other"]}
        with self.assertRaisesRegex(ValueError, "pool already exists"):
            module.configure(value)

    def test_apply_is_validated_private_backed_up_and_compare_and_swap(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.json"
            original = json.dumps(fixture()).encode()
            path.write_bytes(original)
            path.chmod(0o600)
            digest = module.digest(original)
            with patch.object(module.subprocess, "run") as validate:
                validate.return_value.returncode = 0
                report = module.update(path, "fake-validator", apply=True, expected=digest)
            self.assertEqual(Path(report["backup"]).read_bytes(), original)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertFalse(Path(str(path) + ".lock").exists())
            with self.assertRaisesRegex(ValueError, "config changed"):
                module.update(path, "fake-validator", apply=True, expected=digest)

    def test_failed_validation_or_existing_lock_never_mutates_config(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.json"
            original = json.dumps(fixture()).encode()
            path.write_bytes(original)
            with patch.object(module.subprocess, "run") as validate:
                validate.return_value.returncode = 1
                with self.assertRaisesRegex(ValueError, "validation"):
                    module.update(path, "fake-validator", apply=True, expected=module.digest(original))
            self.assertEqual(path.read_bytes(), original)
            lock = Path(str(path) + ".lock")
            lock.write_text("another writer")
            with self.assertRaises(FileExistsError):
                module.update(path, "fake-validator", apply=True, expected=module.digest(original))
            self.assertEqual(lock.read_text(), "another writer")
