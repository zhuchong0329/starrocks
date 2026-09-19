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

import unittest

import probe


class ProbeTest(unittest.TestCase):
    def test_select_only(self):
        self.assertEqual("select k from qct_faults.healthy", probe.checked_select("select k from qct_faults.healthy"))
        for sql in ("drop table x", "select 1; select 2", "select * from x into outfile '/tmp/x'"):
            with self.assertRaises(ValueError):
                probe.checked_select(sql)

    def test_warning_count_and_message_must_agree(self):
        warning = [("Warning", 9000, probe.WARNING_NAME + "; query_id=test")]
        probe.check_warning(1, warning, True)
        probe.check_warning(0, [], False)
        for count, details, partial in ((0, warning, True), (1, [], True), (1, warning, False)):
            with self.assertRaises(AssertionError):
                probe.check_warning(count, details, partial)

    def test_http_requires_real_completion_and_final_trailer(self):
        trailer = {"statistics": {}, "partial_result": True,
                   "warnings": [{"code": probe.WARNING_NAME}]}
        probe.check_http([trailer], 200, True, "partial")
        for records, complete in (([], True), ([trailer], False), ([trailer, {"data": []}], True)):
            with self.assertRaises(AssertionError):
                probe.check_http(records, 200, complete, "partial")

    def test_http_strict_cannot_masquerade_as_success(self):
        probe.check_http([{"meta": []}], 200, False, "strict")
        probe.check_http([{"msg": "Corruption: Bad page"}], 500, True, "strict")
        with self.assertRaises(AssertionError):
            probe.check_http([{"msg": "syntax error"}], 500, True, "strict")
        with self.assertRaises(AssertionError):
            probe.check_http([], 200, True, "strict")
        with self.assertRaises(AssertionError):
            probe.check_http([{"statistics": {}}], 500, False, "strict")

    def test_http_off_and_raw_do_not_gain_fields(self):
        probe.check_http([{"statistics": {}}], 200, True, "healthy", enabled=False)
        probe.check_http([{"meta": []}, {"data": [1]}], 200, True, "healthy", raw=True)
        with self.assertRaises(AssertionError):
            probe.check_http([{"statistics": {}, "partial_result": False}],
                             200, True, "healthy", enabled=False)

    def test_summary_bounds_samples_but_hashes_all_rows(self):
        small = probe.row_summary(([n] for n in range(20)))
        large = probe.row_summary(([n] for n in range(21)))
        self.assertEqual(20, small["rows"])
        self.assertEqual(16, len(small["sample"]))
        self.assertNotEqual(small["ordered_rows_sha256"], large["ordered_rows_sha256"])


if __name__ == "__main__":
    unittest.main()
