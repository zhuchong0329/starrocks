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

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import benchmark


class BenchmarkSummaryTest(unittest.TestCase):
    def test_healthy_samples_require_expected_rows_and_no_warning(self):
        for name, rows in benchmark.EXPECTED_ROWS.items():
            benchmark.check_healthy_sample({"case": name, "rows": rows, "warning_count": 0})
            for wrong_rows, warnings in ((rows + 1, 0), (rows, 1)):
                with self.assertRaises(RuntimeError):
                    benchmark.check_healthy_sample({"case": name, "rows": wrong_rows,
                                                    "warning_count": warnings})

    @staticmethod
    def http_response(records, status=200):
        response = io.BytesIO(b"\n".join(json.dumps(record).encode() for record in records) + b"\n")
        response.status = status
        return response

    def test_http_timing_includes_data_and_complete_stream(self):
        for fields in ({}, {"partial_result": False, "warnings": []}):
            response = self.http_response([{"connectionId": 7}, {"meta": []}, {"data": [1]},
                                           {"data": [2]}, {"statistics": {}, **fields}])
            with patch.object(benchmark.time, "perf_counter_ns", side_effect=(100, 300)):
                result = benchmark.consume_http(response)
            self.assertEqual(2, result["rows"])
            self.assertEqual(100, result["first_at"])
            self.assertEqual(300, result["completed_ns"])
            self.assertEqual(7, result["connection_id"])
            self.assertEqual(bool(fields), result["tolerance_fields"])
        with patch.object(benchmark.time, "perf_counter_ns", return_value=500):
            empty = benchmark.consume_http(self.http_response([{"statistics": {}}]))
        self.assertEqual(0, empty["rows"])
        self.assertEqual(empty["completed_ns"], empty["first_at"])

    def test_http_errors_and_partial_results_never_become_healthy_samples(self):
        for records, status in (([{"meta": []}], 200), ([{"statistics": {}}], 500),
                                ([{"statistics": {}}, {"data": [1]}], 200),
                                ([{"statistics": {}, "partial_result": True}], 200),
                                ([{"statistics": {}, "warnings": [{"code": "partial"}]}], 200)):
            with self.assertRaises(RuntimeError):
                benchmark.consume_http(self.http_response(records, status))

        class Truncated:
            status = 200

            def __iter__(self):
                yield b'{"statistics": {}}\n'
                raise benchmark.http.client.IncompleteRead(b"")

        with self.assertRaises(benchmark.http.client.IncompleteRead):
            benchmark.consume_http(Truncated())

    def test_variant_matches_every_running_node_before_measurement(self):
        with tempfile.TemporaryDirectory(prefix="qct-benchmark-label-test-") as directory:
            root = Path(directory)
            selection = {"variant": "C-on", "storage_page_cache": True}
            (root / "selection.json").write_text(json.dumps(selection))
            with patch.object(benchmark.runtime, "checked_root", return_value=root), \
                    patch.object(benchmark.runtime, "live_record", return_value={"selection": selection}):
                self.assertEqual(selection, benchmark.validate_variant("C-on"))
                with self.assertRaises(ValueError):
                    benchmark.validate_variant("A-baseline")
            with patch.object(benchmark.runtime, "checked_root", return_value=root), \
                    patch.object(benchmark.runtime, "live_record", return_value=None):
                with self.assertRaises(ValueError):
                    benchmark.validate_variant("C-on")
            with patch.object(benchmark.runtime, "checked_root", return_value=root), \
                    patch.object(benchmark.runtime, "live_record", return_value={"selection": {"variant": "B-off"}}):
                with self.assertRaises(ValueError):
                    benchmark.validate_variant("C-on")

    def test_nearest_rank_percentiles_and_grouping(self):
        self.assertEqual(99, benchmark.percentile(list(range(1, 101)), 99))
        self.assertEqual(4, benchmark.percentile([4], 99))
        samples = [{"case": "limit", "first_row_ms": value, "complete_ms": value + 1,
                    "warning_count": 0} for value in (1, 2, 3, 4)]
        samples.append({"case": "scan", "first_row_ms": 10, "complete_ms": 20, "warning_count": 1})
        result = benchmark.summarize(samples)
        self.assertEqual(2.5, result["limit"]["first_row_ms"]["median"])
        self.assertEqual(5, result["limit"]["complete_ms"]["p99"])
        self.assertEqual(4, result["limit"]["samples"])
        self.assertEqual(1, result["scan"]["warnings"])

    def test_no_empty_percentile(self):
        with self.assertRaises(ValueError):
            benchmark.percentile([], 99)

    def test_throughput_uses_overlapping_measured_window_not_sum_of_latencies(self):
        samples = [{"started_ns": 1_000_000_000, "completed_ns": 3_000_000_000},
                   {"started_ns": 2_000_000_000, "completed_ns": 5_000_000_000}]
        self.assertEqual({"measured_wall_seconds": 4.0, "measured_queries_per_second": 0.5},
                         benchmark.measured_throughput(samples))
        with self.assertRaises(ValueError):
            benchmark.measured_throughput([{"started_ns": 1, "completed_ns": 1}])

    def test_process_counters_are_actual_linux_fields(self):
        with tempfile.TemporaryDirectory(prefix="qct-process-metric-test-") as directory:
            root = Path(directory)
            (root / "123").mkdir()
            fields = ["S"] + ["0"] * 21
            fields[11], fields[12], fields[19] = "123", "77", "456"
            (root / "123/stat").write_text("123 (java (qct)) " + " ".join(fields))
            (root / "123/status").write_text("Name:\tjava\nVmRSS:\t4096 kB\nVmHWM:\t8192 kB\n")
            self.assertEqual({"pid": 123, "start_ticks": "456", "cpu_seconds": 2.0,
                              "memory_kib": {"VmRSS": 4096, "VmHWM": 8192}},
                             benchmark.process_resources(123, root, ticks=100))


if __name__ == "__main__":
    unittest.main()
