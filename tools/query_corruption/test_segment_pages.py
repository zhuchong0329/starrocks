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

import struct
import unittest

from segment_pages import data_pages, varint


class SegmentPagesTest(unittest.TestCase):
    def test_uncompressed_ordinal_entries(self):
        entries = b"\x08" + (0).to_bytes(8, "big") + b"\x00\x10" \
            + b"\x08" + (8192).to_bytes(8, "big") + b"\x10\x10"
        index = entries + b"\x08\x02" + struct.pack("<II", 2, 0)
        data = b"x" * 32 + index
        root = {"root_page": {"offset": 32, "size": len(index)}, "is_root_data_page": False}
        self.assertEqual([{"first_ordinal": 0, "offset": 0, "size": 16},
                          {"first_ordinal": 8192, "offset": 16, "size": 16}], data_pages(data, root))

    def test_single_page_and_invalid_pointer(self):
        root = {"root_page": {"offset": 0, "size": 16}, "is_root_data_page": True}
        self.assertEqual(1, len(data_pages(b"x" * 16, root)))
        with self.assertRaises(ValueError):
            data_pages(b"x" * 8, root)

    def test_truncated_and_overflowing_varint(self):
        self.assertEqual((300, 2), varint(b"\xac\x02", 0))
        for value in (b"\x80", b"\xff" * 10, b"\x80" * 9 + b"\x02"):
            with self.assertRaises(ValueError):
                varint(value, 0)


if __name__ == "__main__":
    unittest.main()
