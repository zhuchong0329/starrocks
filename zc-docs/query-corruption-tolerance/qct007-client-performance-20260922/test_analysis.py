#!/usr/bin/env python3
import unittest
import analyze_client as analysis
import split_suite


class AnalysisTests(unittest.TestCase):
    def test_comparison_is_geometric_and_keeps_negative_runs(self):
        value = analysis.comparison([2, .5])
        self.assertAlmostEqual(value["geometric_mean_change_percent"], 0)
        self.assertEqual(value["per_block_change_percent"], [100, -50])
        self.assertEqual(value["paired_blocks"], 2)

    def test_constant_ratio_interval(self):
        value = analysis.comparison([2] * 4)
        self.assertAlmostEqual(value["geometric_mean_change_percent"], 100)
        self.assertEqual(value["exploratory_95_percent"], [100, 100])

    def test_after_first_uses_each_query_difference(self):
        result = analysis.metrics([{"first_row_ms": 100, "complete_ms": 101},
                                   {"first_row_ms": 1, "complete_ms": 51}])
        self.assertEqual(result["after_first_ms"]["p99"], 50)
        self.assertEqual(result["after_first_ms"]["p50"], 25.5)

    def test_split_controls_and_abc_are_distinct_phases(self):
        rows = split_suite.schedule()
        self.assertEqual(len(rows), 22)
        controls = [r for r in rows if r["phase"] == "client"]
        abc = [r for r in rows if r["phase"] == "abc"]
        self.assertEqual(len(controls), 4)
        self.assertEqual(len(abc), 18)
        self.assertTrue(all(r["variant"] == "A-baseline" for r in controls))
        self.assertEqual([r["client_mode"] for r in controls], ["threads", "processes", "processes", "threads"])
        for variant in ("A-baseline", "B-off", "C-on"):
            for position in (1, 2, 3):
                self.assertEqual(sum(r["variant"] == variant and r["position"] == position for r in abc), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
