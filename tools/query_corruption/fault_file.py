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

"""Reversible byte faults for THIS TASK'S isolated, disposable test tablets only.

Not a production repair tool. Stop test BE before footer/magic faults to invalidate
cached segment metadata. Keep compaction off and page cache off during these tests.
Every mutation preserves a full backup and a hash-checked restoration manifest.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import uuid


CLUSTER_ROOT = Path("/query-corruption-workspace/runtime/qct-cluster")
BACKUP_ROOT = Path("/query-corruption-workspace/test-data/qct-backups")
MAX_SEGMENT_BYTES = 64 * 1024 * 1024


def digest(data):
    return hashlib.sha256(data).hexdigest()


def checked_segment(path, tablet_id):
    path = Path(path).absolute()
    resolved = path.resolve(strict=True)
    if (CLUSTER_ROOT.absolute() != CLUSTER_ROOT.resolve() or resolved != path
            or not resolved.is_relative_to(CLUSTER_ROOT.resolve())):
        raise ValueError("Only non-symlink files inside the dedicated qct-cluster are allowed")
    if not (CLUSTER_ROOT / ".qct-test-cluster").is_file():
        raise ValueError("Missing dedicated test-cluster marker")
    # Local OLAP layout: storage/data/<shard>/<tablet>/<schema>/<rowset>_<segment>.dat
    if (not resolved.is_file() or resolved.suffix != ".dat" or tablet_id <= 0
            or resolved.parent.parent.name != str(tablet_id)
            or resolved.parents[3].name != "data"):
        raise ValueError("Segment does not match the explicitly confirmed test tablet")
    if not 12 < resolved.stat().st_size <= MAX_SEGMENT_BYTES:
        raise ValueError("Unexpected test segment size")
    return resolved


def durable_write(path, data, exclusive=False):
    flags = os.O_WRONLY | os.O_NOFOLLOW | (os.O_CREAT | os.O_EXCL if exclusive else 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            output.write(data)
            output.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def mutate_checked(path, data, expected_hash, expected_inode):
    # Validate the opened inode, not just the pathname, before modifying bytes.
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        with os.fdopen(descriptor, "r+b", closefd=False) as output:
            current = os.fstat(descriptor)
            if (current.st_ino != expected_inode or current.st_size != len(data)
                    or digest(output.read()) != expected_hash):
                raise ValueError("Segment changed concurrently; refusing to overwrite it")
            output.seek(0)
            output.write(data)
            output.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def inject(path, tablet_id, kind, page_offset=0):
    path = checked_segment(path, tablet_id)
    original = path.read_bytes()
    if original[-4:] != b"D0R1":
        raise ValueError("Original segment is not a healthy-format D0R1 file")
    footer_length = struct.unpack_from("<I", original, len(original) - 12)[0]
    footer_start = len(original) - 12 - footer_length
    if footer_start <= 0 or footer_length == 0:
        raise ValueError("Invalid original footer bounds")
    if kind == "magic":
        offset, value = len(original) - 1, original[-1] ^ 1
    elif kind == "footer":
        offset, value = footer_start, 0  # Illegal protobuf tag; parser fails before CRC check.
        if original[offset] == 0:
            raise ValueError("Original footer is already invalid")
    elif kind == "page":
        if not 0 <= page_offset < footer_start:
            raise ValueError("Page byte must precede the segment footer")
        offset, value = page_offset, original[page_offset] ^ 1
    else:
        raise ValueError("Unknown fault kind")
    changed = bytearray(original)
    changed[offset] = value
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    if BACKUP_ROOT.absolute() != BACKUP_ROOT.resolve():
        raise ValueError("Backup directory must not be a symlink")
    identity = uuid.uuid4().hex
    backup = BACKUP_ROOT / (identity + ".original")
    manifest = BACKUP_ROOT / (identity + ".json")
    record = {"segment": str(path), "tablet_id": tablet_id, "kind": kind,
              "backup": str(backup), "offset": offset, "before": original[offset], "after": value,
              "original_sha256": digest(original), "fault_sha256": digest(changed),
              "size": len(original), "inode": path.stat().st_ino}
    durable_write(backup, original, exclusive=True)
    durable_write(manifest, json.dumps(record, indent=2).encode() + b"\n", exclusive=True)
    # Refuse concurrent compaction/replacement. No filename deletion or rename is performed.
    mutate_checked(path, changed, record["original_sha256"], record["inode"])
    if digest(path.read_bytes()) != record["fault_sha256"]:
        raise RuntimeError("Fault verification failed; retain manifest for inspection: " + str(manifest))
    return manifest


def restore(manifest):
    manifest = Path(manifest).resolve(strict=True)
    if manifest.parent != BACKUP_ROOT.resolve() or manifest.suffix != ".json":
        raise ValueError("Restoration requires a manifest from the dedicated backup directory")
    record = json.loads(manifest.read_text())
    path = checked_segment(record["segment"], record["tablet_id"])
    backup = Path(record["backup"]).resolve(strict=True)
    if backup.parent != BACKUP_ROOT.resolve() or backup.stem != manifest.stem:
        raise ValueError("Backup/manifest mismatch")
    original = backup.read_bytes()
    if len(original) != record["size"] or digest(original) != record["original_sha256"]:
        raise ValueError("Backup is not intact; refusing restoration")
    current = digest(path.read_bytes())
    if current == record["original_sha256"]:
        return path
    if path.stat().st_ino != record["inode"] or current != record["fault_sha256"]:
        raise ValueError("Segment changed after fault injection; refusing to overwrite it")
    mutate_checked(path, original, record["fault_sha256"], record["inode"])
    if digest(path.read_bytes()) != record["original_sha256"]:
        raise RuntimeError("Restoration verification failed")
    return path  # Keep backup and manifest as test evidence.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fault = commands.add_parser("inject")
    fault.add_argument("segment")
    fault.add_argument("--confirm-test-tablet", required=True, type=int)
    fault.add_argument("--kind", required=True, choices=("page", "magic", "footer"))
    fault.add_argument("--page-offset", type=int, default=0)
    undo = commands.add_parser("restore")
    undo.add_argument("manifest")
    arguments = parser.parse_args()
    if arguments.command == "inject":
        print(inject(arguments.segment, arguments.confirm_test_tablet, arguments.kind, arguments.page_offset))
    else:
        print(restore(arguments.manifest))


if __name__ == "__main__":
    main()
