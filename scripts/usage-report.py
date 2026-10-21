#!/usr/bin/env python3
"""Read-only burn attribution from hourly rollup deltas; no network or prompts."""
import argparse
import collections
import datetime as dt
import json
from pathlib import Path


def summarize(root, hours, group, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    since = now - dt.timedelta(hours=hours)
    rows = collections.defaultdict(collections.Counter)
    malformed = 0
    duplicates = 0
    seen = set()
    for path in sorted(Path(root).glob("rollup-*.jsonl")):
        if path.stem[7:] < since.date().isoformat():
            continue
        with path.open() as stream:
            for line in stream:
                try:
                    r = json.loads(line)
                    event_id = r.get("id")
                    if event_id:
                        if event_id in seen:
                            duplicates += 1
                            continue
                        seen.add(event_id)
                    hour = dt.datetime.fromisoformat(r["hour"].replace("Z", "+00:00"))
                    if hour < since.replace(minute=0, second=0, microsecond=0) or hour > now:
                        continue
                    key = tuple(r.get(k) or "unknown" for k in group)
                    c = rows[key]
                    for k, v in r["d"].items():
                        if k in ("usageKnown", "usageTotals") and isinstance(v, dict):
                            for field, count in v.items():
                                if isinstance(count, (int, float)) and not isinstance(count, bool):
                                    c[k + "." + field] += count
                            continue
                        if isinstance(v, (int, float)):
                            c[k] += v
                except (ValueError, KeyError, TypeError):
                    malformed += 1
    result = []
    for key, c in rows.items():
        n = c["completions"]
        context = c["inputTokens"] + c["cacheReadTokens"] + c["cacheWriteTokens"]
        flags = []
        if n and context/n > 500000: flags.append("large-context")
        if c["contextsOver500k"] >= 20: flags.append("repeated-large-context")
        if c["failures"] >= 10: flags.append("failure-churn")
        if c["fallbacks"] >= 10: flags.append("fallback-churn")
        coverage={field:c["usageKnown."+field]/n if n else None for field in
                  ("inputUncached","inputTotal","cacheRead","cacheCreation","cacheCreation5m","cacheCreation1h","outputInclusive","reasoningSubset")}
        result.append(dict(zip(group,key), **dict(c), usageCoverage=coverage, averageContext=round(context/n) if n else None, flags=flags))
    result.sort(key=lambda r:r.get("outputTokens",0),reverse=True)
    return {"windowHours":hours,"hourAligned":True,"malformedLines":malformed,"duplicateLines":duplicates,"rows":result,
            "limitations":"Observed gateway traffic only. Historical missing attribution/cache writes cannot be recovered. Failed tokens may be incomplete. No subscription quota or cost estimate. Fallbacks are not unique client retries. Sorted by output tokens, not quota cost."}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--hours",type=int,default=24)
    p.add_argument("--group",default="workload,session,lane,run,model")
    p.add_argument("--root",default=str(Path.home()/".config/claude-proxy/state/rollups"))
    p.add_argument("--json",action="store_true")
    p.add_argument("--limit",type=int,default=20)
    a=p.parse_args()
    group=a.group.split(",")
    if a.hours<1 or a.hours>2160 or any(k not in ["workload","session","lane","run","model","actualModel","profile","client","channel","origin","host","parentSession","worker","pr","head","stage"] for k in group):p.error("invalid window/group")
    report=summarize(a.root,a.hours,group)
    if a.json:print(json.dumps(report,indent=2));return
    print("USAGE — observed gateway tokens; hourly window; output-token ranking")
    print("GROUP | CALLS | OUTPUT | CACHE READ | CACHE WRITE | AVG CONTEXT | FAIL | FALLBACK | FLAGS")
    for r in report["rows"][:a.limit]:
        print(" | ".join([" / ".join(r[k] for k in group),str(r.get("completions",0)),str(r.get("outputTokens",0)),str(r.get("cacheReadTokens",0)),str(r.get("cacheWriteTokens",0)),str(r["averageContext"]),str(r.get("failures",0)),str(r.get("fallbacks",0)),",".join(r["flags"])]))
    print(report["limitations"])


if __name__=="__main__":main()
