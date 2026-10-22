import unittest
from configure_rig_routes import prepare


class RigRoutesTest(unittest.TestCase):
    def fixture(self):
        return {"models": {name: {"provider": "mock", "upstream": name,
                "accountPool": "test-pool", "supportsTools": True}
                for name in ["claude-haiku-5-5", "claude-sonnet-5-5", "claude-opus-5"]},
                "chains": {"worker": {"steps": [{"model": "claude-haiku-5-5"}]}},
                "aliases": {"claude-sonnet-5-5": "worker"}}

    def test_exact_routes_preserve_workers(self):
        original = self.fixture()
        result = prepare(original)
        self.assertEqual(result["chains"]["worker"], original["chains"]["worker"])
        self.assertEqual(result["aliases"]["claude-sonnet-5-5"], "worker")
        self.assertEqual(prepare(result), result)
        for alias, chain in result["chains"].items():
            if alias == "worker":
                continue
            self.assertEqual(len(chain["steps"]), 1)
            model = result["models"][chain["steps"][0]["model"]]
            self.assertEqual(model["upstream"], alias.removeprefix("rig-exact-"))
            self.assertNotIn("accountScope", chain)
            self.assertEqual(chain["requestTimeoutMs"], 60000)
        self.assertNotIn("rig-model-claude-haiku-4-5-20251001", original["models"])

    def test_conflict_fails_closed(self):
        config = self.fixture()
        config["aliases"]["rig-exact-claude-opus-5"] = "worker"
        with self.assertRaises(ValueError):
            prepare(config)
