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

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import supplement


class SupplementTest(unittest.TestCase):
    def test_tablet_metadata_accepts_numeric_text_but_not_another_tablet(self):
        supplement.confirm_tablet([("10155",)], 10155)
        supplement.confirm_tablet([(10155,)], 10155)
        for rows in ([], [("10156",)], [("10155",), ("10155",)]):
            with self.assertRaises(ValueError):
                supplement.confirm_tablet(rows, 10155)

    def test_fault_restores_after_assertion_failure(self):
        calls = []
        root = Path("/test/qct")
        segment = root / "be0/storage/data/1/42/1/test.dat"
        with patch.object(supplement.runtime, "checked_root", return_value=root), \
                patch.object(supplement.runtime, "stop", side_effect=lambda node: calls.append("stop:" + node)), \
                patch.object(supplement, "start_ready", side_effect=lambda node: calls.append("start:" + node)), \
                patch.object(supplement.fault_file, "inject", return_value=Path("manifest")), \
                patch.object(supplement.fault_file, "restore", side_effect=lambda path: calls.append("restore")):
            with self.assertRaisesRegex(AssertionError, "query failed"):
                with supplement.fault(segment, 42, "page"):
                    calls.append("query")
                    raise AssertionError("query failed")
        self.assertEqual(["stop:be0", "start:be0", "query", "stop:be0", "restore", "start:be0"], calls)

    def test_fault_does_not_signal_non_be_path(self):
        root = Path("/test/qct")
        with patch.object(supplement.runtime, "checked_root", return_value=root), \
                patch.object(supplement.runtime, "stop") as stop:
            with self.assertRaises(ValueError):
                with supplement.fault(root / "fe/test.dat", 42, "page"):
                    self.fail("Invalid fault scope was accepted")
            stop.assert_not_called()

    def test_snapshot_requires_matching_bytes_and_mid_chunk_page(self):
        snapshot = {"segment": "test.dat", "sha256": supplement.fault_file.digest(b"original"),
                    "num_rows": 32768, "columns": [{"column_id": 1, "pages": [
                        {"first_ordinal": 0, "offset": 100}, {"first_ordinal": 421, "offset": 200}]}]}
        path, segment = Mock(), Mock()
        path.read_text.side_effect = lambda: json.dumps(snapshot)
        segment.read_bytes.return_value = b"original"
        with patch.object(supplement.fault_file, "checked_segment", return_value=segment):
            self.assertEqual(200, supplement.checked_snapshot(path, 42)[1]["offset"])
            segment.read_bytes.return_value = b"changed"
            with self.assertRaisesRegex(ValueError, "current"):
                supplement.checked_snapshot(path, 42)
            segment.read_bytes.return_value = b"original"
            snapshot["columns"][0]["pages"][1]["first_ordinal"] = 8192
            with self.assertRaisesRegex(ValueError, "mid-chunk"):
                supplement.checked_snapshot(path, 42)

    def test_evidence_never_overwrites_prior_run(self):
        with TemporaryDirectory() as directory:
            supplement.save(Path(directory), "result", {"passed": True})
            with self.assertRaises(FileExistsError):
                supplement.save(Path(directory), "result", {"passed": False})
            self.assertTrue(json.loads((Path(directory) / "result.json").read_text())["passed"])


if __name__ == "__main__":
    unittest.main()
