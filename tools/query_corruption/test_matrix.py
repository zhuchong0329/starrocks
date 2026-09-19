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
import tempfile
import unittest

import matrix


class MatrixGuardTest(unittest.TestCase):
    def test_live_or_paused_build_blocks_measurement_but_zombies_do_not(self):
        with tempfile.TemporaryDirectory(prefix="qct-matrix-process-") as directory:
            root = Path(directory)
            entry = root / "123"
            entry.mkdir()
            for command, arguments in (("ninja", b"ninja"), ("cc1plus", b"cc1plus"),
                                       ("java", b"org.codehaus.plexus.classworlds.launcher.Launcher")):
                (entry / "comm").write_text(command)
                (entry / "cmdline").write_bytes(arguments)
                for state in ("R", "S", "T"):
                    (entry / "stat").write_text("123 (" + command + ") " + state + " 1")
                    with self.assertRaises(RuntimeError):
                        matrix.ensure_idle_builds(root)
                (entry / "stat").write_text("123 (" + command + ") Z 1")
                matrix.ensure_idle_builds(root)
            (entry / "comm").write_text("java")
            (entry / "cmdline").write_bytes(b"com.starrocks.StarRocksFE")
            (entry / "stat").write_text("123 (java) S 1")
            matrix.ensure_idle_builds(root)

    def test_all_injection_manifests_must_be_restored_before_performance(self):
        with tempfile.TemporaryDirectory(prefix="qct-matrix-restore-") as directory:
            root = Path(directory)
            segment = root / "fixture.dat"
            segment.write_bytes(b"original-test-data")
            manifest = root / "fault.json"
            manifest.write_text(json.dumps({"segment": str(segment), "original_sha256": matrix.sha256(segment)}))
            matrix.ensure_restored(root)
            segment.write_bytes(b"changed-test-data")
            with self.assertRaises(RuntimeError):
                matrix.ensure_restored(root)


if __name__ == "__main__":
    unittest.main()
