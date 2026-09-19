#!/usr/bin/env python3
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

"""Read scalar-column page positions from an explicit QCT segment/footer snapshot.

The footer JSON comes from the same binary's read-only meta_tool
--operation=show_segment_footer. This is a narrow test helper for the 4.0.11
uncompressed ordinal index format, not a general parser or integrity verifier.
It performs no writes. Actual injected faults still require fault_file.py guards.
"""

import argparse
import json
from pathlib import Path
import struct

from fault_file import checked_segment, digest


def varint(data, position):
    result = 0
    for shift in range(0, 70, 7):
        if position >= len(data):
            raise ValueError("Truncated ordinal index varint")
        value = data[position]
        position += 1
        result |= (value & 127) << shift
        if value < 128:
            if result >= 1 << 64:
                raise ValueError("Ordinal index varint overflows uint64")
            return result, position
    raise ValueError("Invalid ordinal index varint")


def data_pages(data, root):
    pointer = root["root_page"]
    offset, size = int(pointer["offset"]), int(pointer["size"])
    if offset < 0 or size < 8 or offset + size > len(data):
        raise ValueError("Ordinal root pointer outside segment")
    if root["is_root_data_page"]:
        return [{"first_ordinal": 0, "offset": offset, "size": size}]
    # PageIO: uncompressed index body | protobuf footer | uint32 footer length | CRC32C.
    page = data[offset:offset + size]
    footer_size = struct.unpack_from("<I", page, len(page) - 8)[0]
    if footer_size == 0 or footer_size + 8 >= len(page):
        raise ValueError("Invalid ordinal index footer bounds")
    body = page[:len(page) - footer_size - 8]
    entries, position = [], 0
    while position < len(body):
        key_size, position = varint(body, position)
        if key_size != 8 or position + key_size > len(body):
            raise ValueError("Only uint64 scalar ordinal keys are supported")
        ordinal = int.from_bytes(body[position:position + key_size], "big")
        position += key_size
        page_offset, position = varint(body, position)
        page_size, position = varint(body, position)
        if page_size < 8 or page_offset + page_size > len(data):
            raise ValueError("Data page pointer outside segment")
        if entries and ordinal <= entries[-1]["first_ordinal"]:
            raise ValueError("Ordinals must increase")
        entries.append({"first_ordinal": ordinal, "offset": page_offset, "size": page_size})
    if not entries or entries[0]["first_ordinal"] != 0:
        raise ValueError("Expected ordinal index starting at zero")
    return entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("segment")
    parser.add_argument("--confirm-test-tablet", type=int, required=True)
    parser.add_argument("--footer-json", type=Path, required=True)
    args = parser.parse_args()
    segment = checked_segment(args.segment, args.confirm_test_tablet)
    data = segment.read_bytes()
    footer = json.loads(args.footer_json.read_text())
    result = {"segment": str(segment), "sha256": digest(data), "num_rows": footer["num_rows"], "columns": []}
    for column in footer["columns"]:
        for index in column.get("indexes", []):
            if "ordinal_index" in index:
                result["columns"].append({"column_id": column["column_id"],
                                           "pages": data_pages(data, index["ordinal_index"]["root_page"])})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
