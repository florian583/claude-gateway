"""Prepare exact-model rig routes without changing worker aliases or live state."""
import argparse
import copy
import json
from pathlib import Path


def prepare(config):
    result = copy.deepcopy(config)
    if result.get("rigListen", "127.0.0.1:48105") != "127.0.0.1:48105":
        raise ValueError("Conflicting rig listener")
    result["rigListen"] = "127.0.0.1:48105"
    models = result["models"]
    targets = ["claude-haiku-5-5", "claude-sonnet-5-5", "claude-opus-5-5"]
    for model in targets:
        if model not in models or not models[model].get("accountPool"):
            raise ValueError(f"Missing pool-backed model: {model}")
    # Availability is decided by the provider; never substitute another model.
    dated = "claude-haiku-4-5-20251001"
    model_key = "rig-model-" + dated
    definition = copy.deepcopy(models["claude-haiku-5-5"])
    definition.update(upstream=dated, contextWindow=200000)
    definition.pop("responseAlias", None)
    definition.pop("requestOverrides", None)
    if model_key in models and models[model_key] != definition:
        raise ValueError("Conflicting dated Haiku rig definition")
    models[model_key] = definition
    for alias_model, model in [(m, m) for m in targets] + [(dated, model_key), ("claude-opus-5", "claude-opus-5-5")]:
        alias = "rig-exact-" + alias_model
        chain = {"steps": [{"model": model}], "allowPaidFallback": False,
                 "requireTools": True, "requestTimeoutMs": 60000,
                 "firstContentTimeoutMs": 30000}
        if alias in result["chains"] and result["chains"][alias] != chain:
            previous = copy.deepcopy(chain)
            previous["steps"] = [{"model": "claude-opus-5"}]
            if alias_model != "claude-opus-5" or result["chains"][alias] != previous:
                raise ValueError(f"Conflicting rig chain: {alias}")
        if alias in result["aliases"] and result["aliases"][alias] != alias:
            raise ValueError(f"Conflicting rig alias: {alias}")
        result["chains"][alias] = chain
        result["aliases"][alias] = alias
        rig_models = result.setdefault("rigModels", {})
        names = [alias_model] + (["claude-haiku-4-5"] if alias_model == dated else [])
        for name in names:
            if name in rig_models and rig_models[name] != alias:
                raise ValueError(f"Conflicting rig model: {name}")
            rig_models[name] = alias
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = prepare(json.loads(args.source.read_text()))
    # Exclusive creation avoids overwriting either live config or a prior review.
    with args.output.open("x") as stream:
        args.output.chmod(0o600)
        json.dump(result, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
