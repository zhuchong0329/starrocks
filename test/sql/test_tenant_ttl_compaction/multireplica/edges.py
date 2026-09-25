#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Duplicate multiplicity, table-property fallback, NULL and empty rewrite checks."""
import collections
import time

import cluster as c
from faults import collect_logs
from verify import Case, COLS, TABLES, kept, make_row, write_rows


def run():
    case = Case("EDGE", rows_per_table=28)
    case.setup()
    case.sql(f"DELETE FROM {case.db}.policy WHERE tenant='default' AND table_name='round043.events_b'")
    for table in TABLES:
        case.sql(
            f"INSERT INTO {case.db}.policy VALUES ('ExactCase','round043.{table}',2),('padded','round043.{table}',2)"
        )
    case.save("policies-edge.json", case.sql(f"SELECT * FROM {case.db}.policy ORDER BY table_name,tenant"))
    inputs = []
    for partition, index, multiplicity in (
        (0, 0, 3),
        (0, 999, 1),
        (2, 0, 3),
        (2, 150, 2),
        (6, 300, 3),
        (6, 999, 2),
        (6, 500, 1),
        (9, 300, 3),
    ):
        inputs.extend([make_row(case.anchor, partition, index, 0)] * multiplicity)
    for index, key, multiplicity in (
        (1, "ExactCase", 3),
        (500, "exactcase", 3),
        (2, "padded", 2),
        (501, " padded ", 2),
    ):
        row = list(make_row(case.anchor, 3, index, 0))
        row[1] = key
        inputs.extend([tuple(row)] * multiplicity)
    assert len(inputs) == 28
    expected = [row for row in inputs if kept(row, case.anchor, int(time.time()), 20)]
    assert len(expected) == 14
    write_rows(case.directory / "edge-input.jsonl.gz", inputs)
    originals = {}
    for table in TABLES:
        with c.connect(c.leader()) as conn, conn.cursor() as cursor:
            # Two inserts preserve repeated business rows across independent transactions/Rowsets.
            for rows in (inputs[:8], inputs[8:]):
                cursor.executemany(f"INSERT INTO {case.db}.{table} VALUES (%s,%s,%s,%s,%s,%s)", rows)
        parts, groups = case.layout(table)
        case.save(f"baseline-{table}-layout.json", {"partitions": parts, "tablets": groups})
        combined = []
        for tablet_id, replicas in groups.items():
            previous = None
            for replica in replicas:
                rows = case.scan_replica(table, replica, "before")
                assert previous is None or collections.Counter(rows) == collections.Counter(previous)
                previous = rows
            originals[(table, tablet_id)] = previous
            combined.extend(previous)
            write_rows(case.directory / f"baseline-{table}-t{tablet_id}.gz", previous)
        assert collections.Counter(combined) == collections.Counter(inputs)
    case.bind()
    case.wait_snapshot()

    def ready():
        states = case.status()
        return (
            states
            if all(
                case.sql(f"SELECT count(*) n FROM {case.db}.{t}")[0]["n"] == 14
                and int(states[t]["CompletedPhysicalPartitions"] or 0) >= 4
                and int(states[t]["RunningReplicaTasks"] or 0) == 0
                for t in TABLES
            )
            else None
        )

    states = c.wait_for(ready, timeout=300, description="edge all-replica completion")
    case.save("edge-cleanup-status.json", states)
    # The second logical table has explicit tenant policies but no dictionary default row.
    assert states["events_a"]["TableDefaultMatch"] == "MATCHED", states
    assert states["events_b"]["TableDefaultMatch"] == "NOT_FOUND", states
    summary = {}
    for table in TABLES:
        parts, groups = case.layout(table)
        assert len(parts) == 4, len(parts)
        scanned_empty = 0
        checked = 0
        for (source_table, tablet_id), original in originals.items():
            if source_table != table:
                continue
            wanted = [row for row in original if kept(row, case.anchor, int(time.time()), 20)]
            deleted = list((collections.Counter(original) - collections.Counter(wanted)).elements())
            write_rows(case.directory / f"expected-kept-{table}-t{tablet_id}.gz", wanted)
            write_rows(case.directory / f"expected-deleted-{table}-t{tablet_id}.gz", deleted)
            if tablet_id not in groups:
                assert not wanted
                continue
            for replica in groups[tablet_id]:
                rows = case.scan_replica(table, replica, "after")
                extra = collections.Counter(rows) - collections.Counter(wanted)
                missing = collections.Counter(wanted) - collections.Counter(rows)
                case.save(
                    f'diff-{table}-t{tablet_id}-r{replica["ReplicaId"]}.json',
                    {"extra": list(extra.items()), "missing": list(missing.items())},
                )
                assert not extra and not missing, (table, replica, extra, missing)
                checked += 1
                if original and not rows:
                    scanned_empty += 1
        assert scanned_empty >= 3, "nonempty input rewritten to a proven empty scan is required"
        ordinary = case.sql(f"SELECT {COLS} FROM {case.db}.{table}")
        ordinary_rows = [tuple(row[col] for col in COLS.split(",")) for row in ordinary]
        write_rows(case.directory / f"ordinary-{table}.rows.gz", ordinary_rows)
        assert collections.Counter(ordinary_rows) == collections.Counter(expected)
        aggregate = case.sql(f"SELECT count(*) n,min(metric) lo,max(metric) hi FROM {case.db}.{table}")[0]
        assert (int(aggregate["n"]), int(aggregate["lo"]), int(aggregate["hi"])) == (
            len(expected),
            min(row[4] for row in expected),
            max(row[4] for row in expected),
        )
        case.save(f"aggregate-{table}.json", aggregate)
        grouped = case.sql(
            f"SELECT category,tenant,count(*) n FROM {case.db}.{table} GROUP BY category,tenant"
        )
        assert {(r["category"], r["tenant"]): int(r["n"]) for r in grouped} == dict(
            collections.Counter((r[3], r[1]) for r in expected)
        )
        case.save(f"grouped-{table}.json", grouped)
        summary[table] = {
            "input": 28,
            "kept": 14,
            "deleted": 14,
            "checked_replicas": checked,
            "rewritten_empty_replicas": scanned_empty,
        }
    control = case.sql(f"SELECT event_id,metric FROM {case.db}.control ORDER BY event_id")
    assert [(r["event_id"], r["metric"]) for r in control] == [(1, -987), (2, 987)], control
    case.save("data-verdict.json", {"passed": True, "tables": summary})
    case.log("data_verified", tables=summary)
    collect_logs(case)
    case.archive()
    case.save("scenario-verdict.json", {"passed": True, "scenario": "EDGE"})


if __name__ == "__main__":
    run()
