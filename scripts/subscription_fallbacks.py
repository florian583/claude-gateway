#!/usr/bin/env python3
"""Preview/apply subscription-only Claude fallback routes; never handles tokens."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


KIMI_POOL = "pool-claude-kimi-fallback"
GLM_POOL = "pool-haiku-glm53-flash-fallback"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def configure(document):
    """Preserve primary models and credentials; replace only Claude fallbacks."""
    result = copy.deepcopy(document)
    if result.get("version") != 1:
        raise ValueError("expected version-1 gateway config")
    models, providers = result["models"], result["providers"]

    def find(provider, upstream):
        matches = [name for name, model in models.items()
                   if model.get("provider") == provider and model.get("upstream") == upstream]
        if len(matches) != 1 or providers[provider].get("billing") != "subscription":
            raise ValueError("expected one subscribed model for " + provider + "/" + upstream)
        return matches[0]

    pools = {
        KIMI_POOL: {"selection": "sticky-health", "models": [
            find("moonshot", "k3"), find("ollama", "kimi-k3:cloud")]},
        GLM_POOL: {"selection": "sticky-health", "models": [
            find("ollama", "glm-5.3-flash:cloud"), find("bigmodel", "glm-5.3-flash")]},
    }
    for name, value in pools.items():
        previous = result.setdefault("modelPools", {}).get(name)
        if previous is not None and previous != value:
            raise ValueError("reserved fallback pool already exists with different policy")
        result["modelPools"][name] = value

    def claude(model):
        provider = providers[models[model]["provider"]]
        return (provider.get("variant") == "claude-subscription"
                and provider.get("auth", {}).get("type") == "claude-profile-pool")

    def tier(alias):
        name = alias.removeprefix("claude-")
        if name.startswith("haiku"):
            return "haiku"
        if name.startswith(("sonnet", "opus", "fable")):
            return "main"
        return None

    def routed(chain, policy):
        value = copy.deepcopy(chain)
        value["steps"] = [copy.deepcopy(chain["steps"][0]),
                          {"pool": GLM_POOL if policy == "haiku" else KIMI_POOL}]
        value["allowPaidFallback"] = False
        value.pop("paidFallbackOn", None)
        value.pop("softConcurrency", None)
        return value

    aliases, chains = result["aliases"], result["chains"]
    policies = {}
    for alias, target in aliases.items():
        if target in models and claude(target):
            raise ValueError("direct Claude model alias needs an explicit chain: " + alias)
    for name, chain in list(chains.items()):
        first = chain["steps"][0]
        model = first.get("model")
        if model is None:
            members = result["modelPools"][first["pool"]]["models"]
            if any(claude(member) for member in members):
                raise ValueError("Claude primary model pools require explicit routing")
            continue
        if not claude(model):
            continue
        policy = "haiku" if "haiku" in models[model]["upstream"].lower() else "main"
        requested = {tier(alias) for alias, target in aliases.items() if target == name}
        requested.discard(None)
        if len(requested) == 1:
            policy = requested.pop()
        policies[name] = policy
        chains[name] = routed(chain, policy)

    # A shared primary can serve distinct requested tiers without changing the
    # actual primary model (for example an existing low-cost Sonnet mapping).
    for alias, target in list(aliases.items()):
        requested = tier(alias)
        if target not in policies or requested is None or policies[target] == requested:
            continue
        clone = target + "-" + requested + "-subscription-fallback"
        value = routed(chains[target], requested)
        if clone in chains and chains[clone] != value:
            raise ValueError("fallback chain name collision")
        chains[clone] = value
        aliases[alias] = clone
    if not policies:
        raise ValueError("no Claude subscription primary chains found")
    return result


def update(path, binary, *, apply=False, expected=None):
    path = Path(path).absolute()
    if path.is_symlink() or not path.is_file():
        raise ValueError("config must be a regular file")
    if apply and (not expected or not re.fullmatch("[0-9a-f]{64}", expected)):
        raise ValueError("apply requires preview's exact expected config SHA")
    lock = Path(str(path) + ".lock")
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    staged = None
    try:
        original = path.read_bytes()
        if expected and digest(original) != expected:
            raise ValueError("config changed since preview")
        before = json.loads(original)
        after = configure(before)
        report = {
            "beforeSHA": digest(original),
            "changedChains": sorted(k for k, v in after["chains"].items()
                                    if before["chains"].get(k) != v),
            "changedAliases": sorted(k for k, v in after["aliases"].items()
                                     if before["aliases"].get(k) != v),
            "poolMembers": {k: after["modelPools"][k]["models"] for k in (KIMI_POOL, GLM_POOL)},
            "applied": False,
        }
        if before == after:
            return dict(report, afterSHA=digest(original), unchanged=True)
        content = (json.dumps(after, indent=2, ensure_ascii=False) + "\n").encode()
        handle, name = tempfile.mkstemp(prefix=".subscription-fallback-", suffix=".json", dir=path.parent)
        staged = Path(name)
        with os.fdopen(handle, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        validation = subprocess.run([str(binary), "--config", str(staged), "validate-config"],
                                    capture_output=True, timeout=30)
        if validation.returncode:
            raise ValueError("native gateway config validation failed")
        if path.is_symlink() or path.read_bytes() != original:
            raise ValueError("config changed during validation")
        if apply:
            backup = path.with_name(path.name + ".before-subscription-fallback-" + digest(original)[:12])
            if backup.is_symlink() or (backup.exists() and backup.read_bytes() != original):
                raise ValueError("rollback backup already exists with different content")
            if not backup.exists():
                with backup.open("xb") as stream:
                    os.fchmod(stream.fileno(), 0o600)
                    stream.write(original)
                    stream.flush()
                    os.fsync(stream.fileno())
            os.replace(staged, path)
            report.update(applied=True, backup=str(backup))
        return dict(report, afterSHA=digest(content), unchanged=False)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
        owned = os.fstat(fd).st_ino
        os.close(fd)
        if lock.exists() and lock.lstat().st_ino == owned:
            lock.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-sha")
    args = parser.parse_args()
    print(json.dumps(update(args.config, args.binary, apply=args.apply, expected=args.expected_sha)))


if __name__ == "__main__":
    main()
