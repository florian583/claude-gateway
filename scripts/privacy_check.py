#!/usr/bin/env python3
"""Read-only publication guard. Diagnostics never include matched values."""

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath


EMAIL = re.compile(r"[A-Za-z0-9._%+\-\[\]]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
NOREPLY = re.compile(r"(?:[0-9]+\+)?([A-Za-z0-9-]+(?:\[bot\])?)@users\.noreply\.github\.com", re.I)
IDENTITY = re.compile(r"(.+) <([^<>]+)> [0-9]+ [+-][0-9]{4}")
HOME_PATH = re.compile(r"(?:/Users/|/home/|[A-Z]:\\Users\\)[A-Za-z0-9_.-]+")
ACCOUNT_ALIAS = re.compile(r"claude[0-9]+|personal-max[0-9]+", re.I)
SECRET = re.compile(
    r"\b(?:sk-ant-[A-Za-z0-9_-]{12,}|sk-[A-Za-z0-9_-]{24,}|"
    r"mg_[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|"
    r"github_pat_[A-Za-z0-9_]{20,}|AKIA[A-Z0-9]{16}|"
    r"AIza[A-Za-z0-9_-]{30,}|eyJ[A-Za-z0-9_-]{15,}\."
    r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\b"
)
TEST_TOKEN = re.compile(r"sk-ant-(?:oat|api03)-test-[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z][A-Za-z0-9]*)*")
PRIVATE_KEY = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")


def git(*args):
    result = subprocess.run(["git", *args], capture_output=True, check=False)
    if result.returncode:
        # Git errors can include identities, config values, or sensitive paths.
        raise RuntimeError("Git inspection failed; no values printed")
    return result.stdout


def public_email(value):
    return NOREPLY.fullmatch(value) is not None or value.lower() == "noreply@github.com"


def placeholder_email(value):
    domain = value.rsplit("@", 1)[-1].lower()
    return domain in {"example.com", "example.org", "example.net"} or domain.endswith((".test", ".invalid", ".example"))


def identity_issues(value):
    match = IDENTITY.fullmatch(value.strip())
    if not match:
        return ["unparseable Git identity"]
    name, email = match.groups()
    account = NOREPLY.fullmatch(email)
    if name == "GitHub" and email == "noreply@github.com":
        return []  # GitHub-generated merge committer.
    if account is None:
        return ["Git identity must use a GitHub no-reply email"]
    if name.lower() != account.group(1).lower():
        return ["Git display name must match the no-reply GitHub handle"]
    return []


def content_issues(data, path):
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return [(1, "non-UTF-8 payload requires separate review")]
    if "\0" in text:
        return [(1, "binary payload requires separate review")]
    issues = []
    for number, line in enumerate(text.splitlines(), 1):
        for email in EMAIL.findall(line):
            if not public_email(email) and not placeholder_email(email):
                issues.append((number, "non-placeholder email address"))
        for expression, category in [(HOME_PATH, "personal home path"), (ACCOUNT_ALIAS, "numbered/personal account alias"), (PRIVATE_KEY, "private key material")]:
            if expression.search(line):
                issues.append((number, category))
        for match in SECRET.finditer(line):
            value = match.group()
            if path.endswith("_test.go") and len(value) <= 80 and TEST_TOKEN.fullmatch(value):
                continue
            issues.append((number, "credential-shaped value"))
    return sorted(set(issues))


def private_path(path):
    parts = PurePosixPath(path).parts
    name = parts[-1].lower()
    return (
        any(p.lower() in {"state", "profiles", "client", "secrets", ".claude"} for p in parts[:-1])
        or name in {"config.json", ".credentials.json", "credentials.json", "auth.json", ".env"}
        or name.startswith(".env.")
        or name.endswith((".log", ".jsonl", ".sqlite", ".db", ".pem", ".key", ".p12"))
    )


def safe_location(path):
    # A filename can itself contain an email or credential.
    if content_issues(path.encode(), ""):
        return "path-" + hashlib.sha256(path.encode()).hexdigest()[:12]
    return path


def check_blob(path, oid, mode, label):
    location = f"{label}:{safe_location(path)}"
    errors = []
    if private_path(path):
        errors.append(f"{location}: runtime/credential file must stay outside Git")
    if mode not in {"100644", "100755"}:
        errors.append(f"{location}: symlink/submodule requires separate review")
        return errors
    errors.extend(f"{location}: filename contains {kind}" for _, kind in content_issues(path.encode(), ""))
    errors.extend(f"{location}:{line}: {kind}" for line, kind in content_issues(git("cat-file", "blob", oid), path))
    return errors


def check_staged():
    errors = []
    for entry in git("ls-files", "--stage", "-z").decode().split("\0"):
        if not entry:
            continue
        fields, path = entry.split("\t", 1)
        mode, oid, stage = fields.split()
        if stage != "0":
            errors.append("index contains unresolved entries")
            continue
        errors.extend(check_blob(path, oid, mode, "index"))
    return errors


def check_history():
    errors, seen = [], set()
    for commit in git("rev-list", "--all").decode().splitlines():
        raw = git("cat-file", "commit", commit)
        text = raw.decode()
        headers, message = text.split("\n\n", 1)
        for role in ("author", "committer"):
            values = [line[len(role) + 1:] for line in headers.splitlines() if line.startswith(role + " ")]
            if len(values) != 1:
                errors.append(f"{commit[:12]}: invalid {role} metadata")
            else:
                errors.extend(f"{commit[:12]}: {role}: {issue}" for issue in identity_issues(values[0]))
        errors.extend(f"{commit[:12]}:message:{line}: {kind}" for line, kind in content_issues(message.encode(), ""))
        for entry in git("ls-tree", "-rz", "--full-tree", commit).decode().split("\0"):
            if not entry:
                continue
            fields, path = entry.split("\t", 1)
            mode, _, oid = fields.split()
            key = (path, oid, mode)
            if key not in seen:
                seen.add(key)
                errors.extend(check_blob(path, oid, mode, commit[:12]))
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged", action="store_true")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--identity", action="store_true")
    parser.add_argument("--message", type=Path)
    args = parser.parse_args()
    if not any((args.staged, args.history, args.identity, args.message)):
        parser.error("choose --staged, --history, --identity, or --message")
    errors = []
    try:
        if args.identity:
            for role in ("GIT_AUTHOR_IDENT", "GIT_COMMITTER_IDENT"):
                errors.extend(f"{role}: {issue}" for issue in identity_issues(git("var", role).decode()))
        if args.message:
            errors.extend(f"commit-message:{line}: {kind}" for line, kind in content_issues(args.message.read_bytes(), ""))
        if args.staged:
            errors.extend(check_staged())
        if args.history:
            errors.extend(check_history())
    except (OSError, RuntimeError, ValueError, UnicodeError):
        errors.append("Privacy check could not inspect input; refusing to continue")
    if errors:
        print("PRIVACY_CHECK_FAILED", file=sys.stderr)
        for error in sorted(set(errors)):
            print(error, file=sys.stderr)
        return 1
    print("PRIVACY_CHECK_PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
