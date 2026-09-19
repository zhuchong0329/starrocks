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
from unittest.mock import Mock, patch

import runtime


class RuntimeGuardTest(unittest.TestCase):
    def test_launch_retries_transient_procfs_identity(self):
        child = Mock(pid=123)
        child.poll.return_value = None
        expected = {"start_ticks": "456", "home": "/test/be0"}
        with patch.object(runtime, "process_identity", side_effect=[None, {"home": None}, expected]), \
                patch.object(runtime.time, "sleep"):
            self.assertEqual(expected, runtime.await_launch_identity(child, Path("/test/be0")))
        child.terminate.assert_not_called()

    def test_unidentifiable_owned_child_is_not_left_running(self):
        child = Mock(pid=123)
        child.poll.return_value = None
        with patch.object(runtime, "process_identity", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "child terminated"):
                runtime.await_launch_identity(child, Path("/test/be0"), timeout=0)
        child.terminate.assert_called_once()
        child.wait.assert_called_once_with(timeout=30)

    def test_process_disappearing_during_procfs_read_is_dead(self):
        with patch.object(Path, "read_text", side_effect=ProcessLookupError()):
            self.assertIsNone(runtime.process_identity(123))

    def test_descriptor_limit_is_process_local_and_never_exceeds_hard_limit(self):
        with patch.object(runtime.resource, "getrlimit", return_value=(1024, 524288)), \
                patch.object(runtime.resource, "setrlimit") as set_limit:
            runtime.ensure_file_limit()
            set_limit.assert_called_once_with(runtime.resource.RLIMIT_NOFILE, (65535, 524288))
        with patch.object(runtime.resource, "getrlimit", return_value=(65535, 524288)), \
                patch.object(runtime.resource, "setrlimit") as set_limit:
            runtime.ensure_file_limit()
            set_limit.assert_not_called()
        with patch.object(runtime.resource, "getrlimit", return_value=(1024, 4096)), \
                patch.object(runtime.resource, "setrlimit") as set_limit:
            with self.assertRaises(RuntimeError):
                runtime.ensure_file_limit()
            set_limit.assert_not_called()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="qct-runtime-tool-test-")
        self.root = Path(self.temp.name).resolve()
        self.cluster = self.root / "qct-cluster"
        self.patch = patch.object(runtime, "CLUSTER_ROOT", self.cluster)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.temp.cleanup()

    def test_marker_and_node_symlink_checks(self):
        with self.assertRaises(ValueError):
            runtime.checked_root()
        runtime.checked_root(create=True)
        outside = self.root / "outside"
        outside.mkdir()
        (self.cluster / "fe").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            runtime.live_record("fe")
        with self.assertRaises(ValueError):
            runtime.node_home("../outside")

    def test_existing_unmarked_directory_is_never_claimed(self):
        self.cluster.mkdir()
        with self.assertRaises(ValueError):
            runtime.checked_root(create=True)

    def test_pid_reuse_or_foreign_home_never_signalled(self):
        runtime.checked_root(create=True)
        home = self.cluster / "be0"
        home.mkdir()
        (home / "launch.json").write_text(json.dumps({"pid": 123, "start_ticks": "456"}))
        with patch.object(runtime, "process_identity", return_value={"start_ticks": "789", "home": str(home)}), \
                patch.object(runtime.os, "kill") as kill:
            with self.assertRaises(ValueError):
                runtime.stop("be0")
            kill.assert_not_called()
        with patch.object(runtime, "process_identity", return_value=None):
            self.assertIsNone(runtime.live_record("be0"))

    def test_linux_process_identity_and_zombie(self):
        proc = self.root / "proc/123"
        proc.mkdir(parents=True)
        # /proc stat field 22 is the start time. The command may contain spaces/parens.
        (proc / "stat").write_text("123 (qct (test)) S " + "0 " * 18 + "456 0\n")
        (proc / "environ").write_bytes(b"OTHER=ok\0STARROCKS_HOME=/test/be0\0")
        self.assertEqual({"start_ticks": "456", "home": "/test/be0"},
                         runtime.process_identity(123, self.root / "proc"))
        (proc / "stat").write_text("123 (qct) Z " + "0 " * 18 + "456 0\n")
        self.assertIsNone(runtime.process_identity(123, self.root / "proc"))

    def test_prepare_preserves_history_and_records_page_cache_separately(self):
        source = self.root / "src"
        files = {"be/build_Release/CMakeCache.txt": "MAKE_TEST:BOOL=OFF\nCMAKE_BUILD_TYPE:STRING=Release\n",
                 "be/output/lib/starrocks_be": "test fixture, never executed",
                 "fe/fe-core/target/starrocks-fe.jar": "test fixture",
                 "fe/fe-core/target/lib/dependency.jar": "test fixture"}
        for name, contents in files.items():
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        with patch.object(runtime, "WORKSPACE", self.root):
            runtime.prepare("C-on", page_cache=True)
            self.assertIn("enable_query_corruption_tolerance = true",
                          (self.cluster / "fe/conf/fe.conf").read_text())
            for backend in range(3):
                self.assertIn("disable_storage_page_cache = false",
                              (self.cluster / ("be%d/conf/be.conf" % backend)).read_text())
            runtime.prepare("B-off")
        self.assertFalse(json.loads((self.cluster / "selection.json").read_text())["storage_page_cache"])
        self.assertEqual(1, len(list((self.cluster / "be0/history").glob("*.conf"))))
        self.assertIn("disable_storage_page_cache = true", (self.cluster / "be0/conf/be.conf").read_text())


if __name__ == "__main__":
    unittest.main()
