import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import datetime as dt

spec=importlib.util.spec_from_file_location("usage_report",Path(__file__).with_name("usage-report.py"))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class UsageReportTests(unittest.TestCase):
    def test_identified_retry_and_presence_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            row={"id":"batch-1","hour":"2026-01-02T12:00:00Z","d":{"completions":2,"usageKnown":{"outputInclusive":1},"usageTotals":{"outputInclusive":0}}}
            (Path(tmp)/"rollup-2026-01-02.jsonl").write_text((json.dumps(row)+"\n")*2)
            report=m.summarize(tmp,1,["workload"],dt.datetime(2026,1,2,13,tzinfo=dt.timezone.utc))
            self.assertEqual(report["duplicateLines"],1)
            self.assertEqual(report["rows"][0]["usageCoverage"]["outputInclusive"],.5)
            self.assertEqual(report["rows"][0]["usageCoverage"]["reasoningSubset"],0)
    def test_deltas_and_unknowns(self):
        with tempfile.TemporaryDirectory() as tmp:
            row={"hour":"2026-01-02T12:00:00Z","session":"s","d":{"completions":1,"outputTokens":10,"cacheReadTokens":600001,"cacheWriteTokens":20}}
            p=Path(tmp)/"rollup-2026-01-02.jsonl"
            p.write_text(json.dumps(row)+"\n"+json.dumps(row)+"\nbroken\n")
            result=m.summarize(tmp,1,["workload","session"],dt.datetime(2026,1,2,13,tzinfo=dt.timezone.utc))
            r=result["rows"][0]
            self.assertEqual(r["workload"],"unknown")
            self.assertEqual(r["outputTokens"],20)
            self.assertEqual(r["cacheWriteTokens"],40)
            self.assertIn("large-context",r["flags"])
            self.assertEqual(result["malformedLines"],1)

if __name__=="__main__":unittest.main()
