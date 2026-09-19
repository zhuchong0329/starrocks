# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import json
from pathlib import Path
import tempfile
import unittest

import benchmark
import summarize


class EvidenceSummaryTest(unittest.TestCase):
    def test_metric_distinguishes_missing_from_zero_and_sums_labels(self):
        self.assertIsNone(summarize.metric({}, "hit_count"))
        self.assertIsNone(summarize.metric("other_metric 0\n", "hit_count"))
        self.assertEqual(0, summarize.metric("hit_count 0\n", "hit_count"))
        self.assertEqual(5, summarize.metric('hit_count{shard="a"} 2\nhit_count{shard="b"} 3\n', "hit_count"))

    def test_resource_delta_uses_same_process_and_actual_interval(self):
        first = {"utc": "2026-09-19T00:00:00+00:00", "nodes": {
            "be0": {"pid": 1, "start_ticks": "2", "cpu_seconds": 3, "memory_kib": {"VmRSS": 100}}}}
        last = copy.deepcopy(first)
        last["utc"] = "2026-09-19T00:00:02+00:00"
        last["nodes"]["be0"]["cpu_seconds"] = 4
        self.assertEqual(0.5, summarize.resources(first, last)["server_average_cores"])
        last["nodes"]["be0"]["start_ticks"] = "3"
        with self.assertRaises(ValueError):
            summarize.resources(first, last)

    def test_raw_sample_count_and_summary_are_verified(self):
        sample = {"type": "sample", "case": "small_limit", "rows": 10, "warning_count": 0,
                  "first_row_ms": 1, "complete_ms": 2, "started_ns": 1, "completed_ns": 2_000_001}
        records = [{"type": "metadata", "settings": {"concurrency": 1, "repetitions": 1},
                    "plans": {"small_limit": "plan"}}, sample,
                   {"type": "summary", "measured": benchmark.summarize([sample])}]
        with tempfile.TemporaryDirectory(prefix="qct-summary-") as directory:
            path = Path(directory) / "run.jsonl"
            path.write_text("\n".join(json.dumps(record) for record in records))
            summarize.load_run(path)
            records[0]["settings"]["repetitions"] = 2
            path.write_text("\n".join(json.dumps(record) for record in records))
            with self.assertRaises(ValueError):
                summarize.load_run(path)
            records[0]["settings"]["repetitions"] = 1
            records[-1]["measured"]["small_limit"]["complete_ms"]["p99"] = 99
            path.write_text("\n".join(json.dumps(record) for record in records))
            with self.assertRaises(ValueError):
                summarize.load_run(path)


if __name__ == "__main__":
    unittest.main()
