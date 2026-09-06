import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("privacy_check", Path(__file__).with_name("privacy_check.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class PrivacyCheckTests(unittest.TestCase):
    def test_identities(self):
        accepted = [
            "dev <123+dev@users.noreply.github.com> 1 +0000",
            "dev <dev@users.noreply.github.com> 1 +0000",
            "github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com> 1 +0000",
            "GitHub <noreply@github.com> 1 +0000",
        ]
        for value in accepted:
            with self.subTest(value=value):
                self.assertEqual(guard.identity_issues(value), [])
        for value in ["Full Name <123+dev@users.noreply.github.com> 1 +0000", "invalid", "dev <dev@" + "mail.com> 1 +0000"]:
            self.assertTrue(guard.identity_issues(value))

    def test_emails_and_paths(self):
        self.assertFalse(guard.content_issues(b"alice@example.com; /v1/users/me/plan/usage-limits", "fixture.go"))
        email = "private-person@" + "mail.com"
        for value in [email, "/Users/" + "someone/project", "/home/" + "someone", "claude" + "2", "DuplicateClaude" + "2", "personal-max" + "20"]:
            self.assertTrue(guard.content_issues(value.encode(), "file.go"))
        self.assertNotIn(email, repr(guard.content_issues(email.encode(), "file.go")))

    def test_credentials(self):
        values = ["sk-ant-api03-" + "X9" * 30, "mg_" + "Ab9" * 20, "ghp_" + "A" * 36, "-----BEGIN" + " PRIVATE KEY-----"]
        for value in values:
            self.assertTrue(guard.content_issues(value.encode(), "fixture_test.go"))
        fake = "sk-ant-oat-" + "test-primary-reservation"
        self.assertFalse(guard.content_issues(fake.encode(), "fixture_test.go"))
        self.assertTrue(guard.content_issues(fake.encode(), "production.go"))
        self.assertTrue(guard.content_issues(b"\x00binary", "image.png"))

    def test_runtime_paths(self):
        for path in ["config.json", "nested/.credentials.json", "nested/.env.prod", "state/usage.json", "profiles/a/settings.json", "client/settings.json", "out/proxy.log", "key.pem"]:
            self.assertTrue(guard.private_path(path), path)
        for path in ["examples/api-only.json", "profile_setup.go", "docs/configuration.md"]:
            self.assertFalse(guard.private_path(path), path)

    def test_staged_bytes_not_worktree(self):
        with self.repo() as path:
            target = path / "notes.md"
            target.write_text("private-person@" + "mail.com")
            self.run_git(path, "add", "notes.md")
            target.write_text("now clean")
            result = self.run_guard(path, "--staged")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("non-placeholder email", result.stderr)
            self.assertNotIn("private-person", result.stderr)

    def test_old_committer_and_deleted_blob(self):
        with self.repo() as path:
            target = path / "notes.md"
            target.write_text("private-person@" + "mail.com")
            self.run_git(path, "add", ".")
            self.run_git(path, "commit", "-m", "first", bad_committer=True)
            target.unlink()
            self.run_git(path, "add", "-u")
            self.run_git(path, "commit", "-m", "remove old file")
            result = self.run_guard(path, "--history")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("committer", result.stderr)
            self.assertIn("non-placeholder email", result.stderr)
            self.assertNotIn("private-person", result.stderr)

    def test_environment_identity_override_and_message(self):
        with self.repo() as path:
            env = self.env()
            env["GIT_AUTHOR_EMAIL"] = "private-person@" + "mail.com"
            result = self.run_guard(path, "--identity", env=env)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("private-person", result.stderr)
            message = path / "message"
            message.write_text("Change\n\nCo-authored-by: Person <person@" + "mail.com>\n")
            self.assertNotEqual(self.run_guard(path, "--message", str(message)).returncode, 0)

    def test_clean_history(self):
        with self.repo() as path:
            (path / "notes.md").write_text("Generic fixture")
            self.run_git(path, "add", ".")
            self.run_git(path, "commit", "-m", "Initial source")
            result = self.run_guard(path, "--staged", "--history", "--identity")
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_installed_hooks(self):
        with self.repo() as path:
            source = Path(guard.__file__).resolve().parent.parent
            shutil.copytree(source / ".githooks", path / ".githooks")
            (path / "scripts").mkdir()
            shutil.copyfile(source / "scripts/privacy_check.py", path / "scripts/privacy_check.py")
            self.run_git(path, "config", "core.hooksPath", ".githooks")
            (path / "notes.md").write_text("Generic fixture")
            self.run_git(path, "add", ".")
            env = self.env()
            env["GIT_AUTHOR_EMAIL"] = "private-person@" + "mail.com"
            result = subprocess.run(["git", "commit", "-m", "Initial source"], cwd=path, env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"GIT_AUTHOR_IDENT", result.stderr)
            message = "Co-authored-by: Person <person@" + "mail.com>"
            result = subprocess.run(["git", "commit", "-m", message], cwd=path, env=self.env(), capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"commit-message", result.stderr)
            self.run_git(path, "commit", "-m", "Initial source")
            result = subprocess.run([str(path / ".githooks/pre-push")], cwd=path, env=self.env(), capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            # Simulate imported history created without this repository's hooks.
            self.run_git(path, "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "-m", "Imported source", bad_committer=True)
            result = subprocess.run([str(path / ".githooks/pre-push")], cwd=path, env=self.env(), capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"committer", result.stderr)

    @staticmethod
    def env():
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
        return env

    def repo(self):
        case = self

        class Repo:
            def __enter__(self):
                self.temp = tempfile.TemporaryDirectory()
                path = Path(self.temp.name)
                case.run_git(path, "init", "--template=", "-b", "main")
                case.run_git(path, "config", "user.name", "dev")
                case.run_git(path, "config", "user.email", "123+dev@users.noreply.github.com")
                return path

            def __exit__(self, *args):
                self.temp.cleanup()

        return Repo()

    def run_git(self, path, *args, bad_committer=False):
        env = self.env()
        if bad_committer:
            env["GIT_COMMITTER_EMAIL"] = "private-person@" + "mail.com"
        return subprocess.run(["git", *args], cwd=path, env=env, text=True, capture_output=True, check=True)

    def run_guard(self, path, *args, env=None):
        return subprocess.run(["python3", "-B", str(Path(guard.__file__).resolve()), *args], cwd=path, env=env or self.env(), text=True, capture_output=True, check=False)


if __name__ == "__main__":
    unittest.main()
