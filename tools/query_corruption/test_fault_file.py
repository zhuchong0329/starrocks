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
import struct
import tempfile
import unittest
from unittest.mock import patch

import fault_file


class FaultFileTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="qct-fault-tool-test-")
        self.root = Path(self.temp.name).resolve()
        self.cluster = self.root / "cluster"
        self.cluster.mkdir()
        (self.cluster / ".qct-test-cluster").touch()
        self.backups = self.root / "backups"
        self.patch_cluster = patch.object(fault_file, "CLUSTER_ROOT", self.cluster)
        self.patch_backups = patch.object(fault_file, "BACKUP_ROOT", self.backups)
        self.patch_cluster.start()
        self.patch_backups.start()
        self.segment = self.cluster / "be0/storage/data/1/123/456/rowset_0.dat"
        self.segment.parent.mkdir(parents=True)
        self.original = b"test-page-bytes" + b"\x08\x01" + struct.pack("<II", 2, 0) + b"D0R1"
        self.segment.write_bytes(self.original)

    def tearDown(self):
        self.patch_cluster.stop()
        self.patch_backups.stop()
        self.temp.cleanup()

    def test_all_faults_restore_and_keep_evidence(self):
        for kind in ("page", "magic", "footer"):
            with self.subTest(kind=kind):
                manifest = fault_file.inject(self.segment, 123, kind)
                self.assertNotEqual(self.original, self.segment.read_bytes())
                fault_file.restore(manifest)
                fault_file.restore(manifest)
                self.assertEqual(self.original, self.segment.read_bytes())
                self.assertTrue(Path(json.loads(manifest.read_text())["backup"]).is_file())

    def test_wrong_tablet_and_outside_path_are_rejected(self):
        with self.assertRaises(ValueError):
            fault_file.inject(self.segment, 124, "page")
        outside = self.root / "outside.dat"
        outside.write_bytes(self.original)
        with self.assertRaises(ValueError):
            fault_file.inject(outside, 123, "page")
        self.assertEqual(self.original, self.segment.read_bytes())

    def test_symlink_and_invalid_page_offset_are_rejected(self):
        link = self.segment.parent / "link.dat"
        link.symlink_to(self.segment)
        with self.assertRaises(ValueError):
            fault_file.inject(link, 123, "page")
        with self.assertRaises(ValueError):
            fault_file.inject(self.segment, 123, "page", len(self.original))

    def test_restore_never_overwrites_unrelated_new_contents(self):
        manifest = fault_file.inject(self.segment, 123, "magic")
        replacement = b"x" * len(self.original)
        self.segment.write_bytes(replacement)
        with self.assertRaises(ValueError):
            fault_file.restore(manifest)
        self.assertEqual(replacement, self.segment.read_bytes())

    def test_damaged_backup_is_rejected(self):
        manifest = fault_file.inject(self.segment, 123, "page")
        backup = Path(json.loads(manifest.read_text())["backup"])
        backup.write_bytes(b"invalid")
        with self.assertRaises(ValueError):
            fault_file.restore(manifest)

    def test_concurrent_replacement_is_rejected_before_write(self):
        original_inode = self.segment.stat().st_ino
        replacement = self.segment.with_name("replacement.dat")
        replacement.write_bytes(self.original)
        replacement.replace(self.segment)
        with self.assertRaises(ValueError):
            fault_file.mutate_checked(self.segment, b"x" * len(self.original),
                                      fault_file.digest(self.original), original_inode)
        self.assertEqual(self.original, self.segment.read_bytes())

    def test_symlink_cluster_root_is_rejected(self):
        alias = self.root / "cluster-alias"
        alias.symlink_to(self.cluster, target_is_directory=True)
        with patch.object(fault_file, "CLUSTER_ROOT", alias):
            with self.assertRaises(ValueError):
                fault_file.inject(self.segment, 123, "page")


if __name__ == "__main__":
    unittest.main()
