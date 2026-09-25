#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Deterministic inputs and complete multiset checks on every real Tablet Replica."""
import argparse
import collections
import concurrent.futures
import datetime as dt
import gzip
import json
import random
import re
import time
import urllib.request
from pathlib import Path

import cluster as c

AGES = (0, 1, 4, 5, 6, 9, 12, 15, 18, 25, 40, 60)
COLS = "event_id,tenant,recordTimestamp,category,metric,payload"
TABLES = ("events_a", "events_b")


def tenant(index):
    if index < 150:
        return "短租户" if index == 0 else f"short_a_{index:03d}"
    if index < 300:
        return f"short_b_{index:03d}"
    if index < 500:
        return f"long_{index:03d}"
    return {500: " unknown ", 501: "多租户", 502: "TenantCase", 503: "tenantcase", 998: "", 999: None}.get(
        index, f"other_{index:03d}"
    )


def retention(index, long_days):
    return 2 if index < 150 else 3 if index < 300 else long_days if index < 500 else 10


def coordinates(event_id):
    partition, tail = divmod(event_id, 10_000_000)
    tenant_index, copy = divmod(tail, 1000)
    return partition, tenant_index, copy


def make_row(anchor, partition, tenant_index, copy):
    event_id = partition * 10_000_000 + tenant_index * 1000 + copy
    upper = anchor - AGES[partition] * 86400
    metric = -event_id if partition >= 2 and tenant_index < 300 else event_id
    payload = None if copy % 7 == 0 else f"p{partition}_t{tenant_index}_j{copy}_雪"
    return (event_id, tenant(tenant_index), upper - 43200, f"age_{AGES[partition]}", metric, payload)


def kept(row, anchor, evaluation_time, long_days):
    partition, tenant_index, _ = coordinates(row[0])
    upper = anchor - AGES[partition] * 86400
    if upper <= evaluation_time - max(10, long_days) * 86400:
        return False
    return row[1] is None or upper > evaluation_time - retention(tenant_index, long_days) * 86400


def write_rows(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as out:
        for row in rows:
            out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def read_rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as source:
        return [tuple(json.loads(line)) for line in source]


def be_config(index, key, value):
    request = urllib.request.Request(
        f"http://127.0.0.1:{c.http_port('be',index)}/api/update_config?{key}={value}",
        method="POST",
        data=b"",
        headers={"Authorization": c.AUTH},
    )
    with c.LOCAL_HTTP.open(request, timeout=15) as response:
        result = json.load(response)
    assert result["status"] == "OK", result
    return result


class Case:
    def __init__(self, scenario, copies=10, directory=None, rows_per_table=None):
        self.scenario = scenario
        self.copies = copies
        self.anchor = int(
            dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        )
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.db = f"ttl_r043_{scenario.lower()}_{copies}_{stamp}"
        self.dictionary = self.db + "_dict"
        self.directory = Path(directory) if directory else c.ARTIFACTS / self.db
        self.directory.mkdir(parents=True, exist_ok=False)
        self.evaluation_time = int(time.time())
        self.backend_ips = {}
        self.initial_tablets = {}
        self.log(
            "created",
            copies=copies,
            rows_per_table=copies * 12_000 if rows_per_table is None else rows_per_table,
            anchor=self.anchor,
        )

    @classmethod
    def reopen(cls, directory):
        case = cls.__new__(cls)
        case.directory = Path(directory)
        for key, value in json.loads((case.directory / "case.json").read_text()).items():
            setattr(case, key, value)
        case.initial_tablets = {}
        for table in TABLES:
            path = case.directory / f"baseline-{table}-layout.json"
            if path.exists():
                for tablet_id, replicas in json.loads(path.read_text())["tablets"].items():
                    case.initial_tablets[(table, tablet_id)] = replicas
        return case

    def log(self, event, **values):
        record = {"event": event, "time": time.time(), "scenario": self.scenario, **values}
        with (self.directory / "events.jsonl").open("a") as out:
            out.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        print(json.dumps(record, ensure_ascii=False, default=str), flush=True)

    def save(self, filename, obj):
        (self.directory / filename).write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str))

    def sql(self, statement, params=None):
        database = None if statement.startswith("CREATE DATABASE") else self.db
        if not getattr(self, "leader_index", None):
            self.leader_index = c.wait_for(c.leader, description="leader")
        return c.sql(self.leader_index, statement, params, database=database)

    def setup(self):
        for index in (1, 2, 3):
            be_config(index, "max_compaction_concurrency", 0)
        self.sql(f"CREATE DATABASE {self.db}")
        self.sql(
            f"CREATE TABLE {self.db}.policy (tenant VARCHAR(128) NOT NULL, table_name VARCHAR(256) NOT NULL, "
            "retention_days INT NOT NULL) PRIMARY KEY(tenant,table_name) DISTRIBUTED BY HASH(tenant) BUCKETS 4 "
            "PROPERTIES('replication_num'='3')"
        )
        values = []
        for table in TABLES:
            values.append(("default", f"round043.{table}", 10))
            values.extend((tenant(i), f"round043.{table}", retention(i, 20)) for i in range(500))
        with c.connect(c.leader()) as conn, conn.cursor() as cursor:
            cursor.executemany(f"INSERT INTO {self.db}.policy VALUES (%s,%s,%s)", values)
        self.save("policies-original.json", values)
        self.sql(
            f"CREATE DICTIONARY {self.dictionary} USING policy "
            "(tenant KEY,table_name KEY,retention_days VALUE) "
            "PROPERTIES('dictionary_refresh_interval'='0','dictionary_warm_up'='false')"
        )
        ddl = (
            "(event_id BIGINT NOT NULL,tenant VARCHAR(128) NULL,recordTimestamp BIGINT NOT NULL,"
            "category VARCHAR(32) NOT NULL,metric BIGINT NOT NULL,payload VARCHAR(128) NULL) "
            "DUPLICATE KEY(event_id) PARTITION BY date_trunc('day',from_unixtime(recordTimestamp)) "
            "DISTRIBUTED BY HASH(event_id) BUCKETS 4 PROPERTIES('replication_num'='3')"
        )
        for table in TABLES:
            statement = f"CREATE TABLE {self.db}.{table} {ddl}"
            self.sql(statement)
            self.save(table + "-ddl.json", self.sql(f"SHOW CREATE TABLE {self.db}.{table}"))
        self.sql(f"CREATE TABLE {self.db}.control {ddl}")
        self.sql(
            f"INSERT INTO {self.db}.control VALUES (1,'control',{self.anchor-80*86400},'control',-987,NULL),"
            f"(2,NULL,{self.anchor-80*86400},'control',987,'keep')"
        )
        self.backend_ips = {str(row["BackendId"]): row["IP"] for row in self.sql("SHOW BACKENDS")}
        self.save(
            "case.json",
            {
                "scenario": self.scenario,
                "copies": self.copies,
                "anchor": self.anchor,
                "db": self.db,
                "dictionary": self.dictionary,
                "backend_ips": self.backend_ips,
            },
        )

    def load(self, partitions=range(12), copy_range=None, label="main"):
        copy_range = range(self.copies) if copy_range is None else copy_range
        batches = [[] for _ in range(20)]
        # Stable shuffle changes load order while retaining an independently computable row for every event id.
        rng = random.Random(43043)
        for partition in partitions:
            for tenant_index in range(1000):
                for copy in copy_range:
                    row = make_row(self.anchor, partition, tenant_index, copy)
                    batches[(tenant_index * self.copies + copy + partition) % 20].append(row)
        for i, rows in enumerate(batches):
            if not rows:
                continue
            rng.shuffle(rows)
            data = (
                "\n".join("\t".join("\\N" if v is None else str(v) for v in row) for row in rows) + "\n"
            ).encode()
            (self.directory / f"input-{label}-{i:02d}.tsv.gz").write_bytes(gzip.compress(data))
            for table in TABLES:
                request = urllib.request.Request(
                    f"http://127.0.0.1:{c.http_port('be',1)}/api/{self.db}/{table}/_stream_load",
                    data=data,
                    method="PUT",
                    headers={
                        "Authorization": c.AUTH,
                        "label": f"{self.db}_{table}_{label}_{i}",
                        "columns": COLS,
                        "max_filter_ratio": "0",
                        "strict_mode": "true",
                        "timeout": "300",
                    },
                )
                with c.LOCAL_HTTP.open(request, timeout=360) as response:
                    result = json.load(response)
                self.save(f"load-{table}-{label}-{i:02d}.json", result)
                assert result["Status"] == "Success" and int(result["NumberLoadedRows"]) == len(rows), result
        self.log("loaded", partitions=list(partitions), copies=list(copy_range), label=label)

    def bind(self):
        for table in TABLES:
            self.sql(
                f"ALTER TABLE {self.db}.{table} SET "
                f"('compaction_retention_condition'=\"dictionary_ttl('{self.dictionary}','round043.{table}',10)\","
                "'compaction_retention_time_zone'='UTC')"
            )
        self.log("bound")

    def status(self):
        return {table: self.sql(f"SHOW TENANT TTL STATUS FROM {self.db}.{table}")[0] for table in TABLES}

    def wait_snapshot(self):
        def ready():
            statuses = self.status()
            return (
                statuses
                if all(
                    r["BindingState"] == "ACTIVE" and r["SnapshotTxnId"] == r["DictionaryLastSuccessTxnId"]
                    for r in statuses.values()
                )
                else None
            )

        result = c.wait_for(ready, timeout=600, description="policy snapshot")
        self.save("snapshot-ready.json", result)
        self.log("snapshot_ready", status=result)
        return int(result[TABLES[0]]["SnapshotTxnId"])

    def layout(self, table):
        partitions = self.sql(f"SHOW PARTITIONS FROM {self.db}.{table}")
        tablets = []
        for partition in partitions:
            part_tablets = self.sql(
                f"SHOW TABLET FROM {self.db}.{table} PARTITION({partition['PartitionName']})"
            )
            for row in part_tablets:
                row["PartitionName"] = partition["PartitionName"]
                row["PartitionId"] = partition["PartitionId"]
                row["PartitionList"] = partition["List"]
                row["VisibleVersion"] = partition["VisibleVersion"]
            values = json.loads(partition["List"])
            assert len(values) == 1 and len(values[0]) == 1, partition
            lower = dt.datetime.strptime(values[0][0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
            upper = int(lower.timestamp()) + 86400
            assert upper in {self.anchor - age * 86400 for age in AGES}, (
                "unexpected partition upper",
                partition,
            )
            for row in part_tablets:
                row["OracleUpperEpochSeconds"] = upper
            tablets.extend(part_tablets)
        groups = collections.defaultdict(list)
        for row in tablets:
            groups[row["TabletId"]].append(row)
        for replicas in groups.values():
            assert len(replicas) == 3 and len({r["BackendId"] for r in replicas}) == 3, replicas
        return partitions, groups

    def scan_replica(self, table, replica, stage):
        tablet_id, replica_id = replica["TabletId"], replica["ReplicaId"]
        prefix = f"{stage}-{table}-t{tablet_id}-r{replica_id}"
        query = f"SELECT {COLS} FROM {self.db}.{table} TABLET({tablet_id}) REPLICA({replica_id}) ORDER BY event_id"
        with c.connect(c.leader()) as conn, conn.cursor() as cursor:
            cursor.execute("SET enable_profile=true")
            cursor.execute("SET pipeline_profile_level=2")
            cursor.execute("EXPLAIN " + query)
            explain = "\n".join(str(row[0]) for row in cursor.fetchall())
            assert f"tabletList={tablet_id}" in explain, explain
            cursor.execute(query)
            rows = list(cursor.fetchall())
            cursor.execute("SELECT last_query_id()")
            query_id = cursor.fetchone()[0]
            profile = ""
            for _ in range(30):
                cursor.execute("SELECT get_query_profile(%s)", (query_id,))
                profile = cursor.fetchone()[0] or ""
                if "OLAP_SCAN (plan_node_id=" in profile:
                    break
                time.sleep(0.1)
            fragments = re.split(r"\n    Fragment \d+:", profile)
            scan_fragments = [fragment for fragment in fragments if "OLAP_SCAN (plan_node_id=" in fragment]
            address = self.backend_ips[replica["BackendId"]] + ":9060"
            assert len(scan_fragments) == 1, profile
            scanned = set(re.findall(r" - Address: ([^\s]+)", scan_fragments[0]))
            assert scanned == {address}, (scanned, address, profile)
            assert re.search(r" - TabletCount:\s+1(?:\s|$)", scan_fragments[0]), profile
        (self.directory / (prefix + ".sql")).write_text(query + ";\n" + explain)
        with gzip.open(self.directory / (prefix + ".profile.gz"), "wt") as out:
            out.write(profile)
        write_rows(self.directory / (prefix + ".rows.gz"), rows)
        return rows

    def validate_input_row(self, row):
        partition, tenant_index, copy = coordinates(row[0])
        assert 0 <= partition < 12 and 0 <= tenant_index < 1000 and 0 <= copy < self.copies, row
        assert tuple(row) == make_row(self.anchor, partition, tenant_index, copy), row
        return (partition * 1000 + tenant_index) * self.copies + copy

    def verify_placeholder(self, table, business_tablet_ids):
        all_replicas = self.sql(f"SHOW TABLET FROM {self.db}.{table}")
        hidden = [r for r in all_replicas if r["TabletId"] not in business_tablet_ids]
        self.save(f"placeholder-{table}-layout.json", hidden)
        assert len(hidden) == 12 and len({r["TabletId"] for r in hidden}) == 4, hidden
        for replica in hidden:
            be_index = int(self.backend_ips[replica["BackendId"]].rsplit(".", 1)[1]) - 20
            url = f"http://127.0.0.1:{c.http_port('be',be_index)}/api/compaction/show?tablet_id={replica['TabletId']}"
            with c.LOCAL_HTTP.open(url) as response:
                rowsets = json.load(response)
            assert all(int(rowset["num_segments"]) == 0 for rowset in rowsets["rowset_details"]), rowsets
            self.save(f"placeholder-{table}-r{replica['ReplicaId']}-rowsets.json", rowsets)

    def baseline(self):
        for table in TABLES:
            partitions, groups = self.layout(table)
            assert len(partitions) == 12 and len(groups) == 48, (partitions, len(groups))
            self.verify_placeholder(table, set(groups))
            self.save(f"baseline-{table}-layout.json", {"partitions": partitions, "tablets": groups})
            seen = bytearray(self.copies * 12_000)
            multiset_rows = 0
            multiple_rowsets = 0
            for tablet_id, replicas in groups.items():
                expected = None
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                    scans = list(pool.map(lambda r: self.scan_replica(table, r, "before"), replicas))
                for replica, rows in zip(replicas, scans):
                    actual = collections.Counter(rows)
                    if expected is None:
                        expected = actual
                        write_rows(self.directory / f"baseline-{table}-t{tablet_id}.gz", rows)
                        for row in rows:
                            ordinal = self.validate_input_row(row)
                            assert seen[ordinal] == 0, ("duplicate input id", row)
                            seen[ordinal] = 1
                        multiset_rows += len(rows)
                    else:
                        assert actual == expected, ("baseline replica mismatch", replica)
                    be_index = int(self.backend_ips[replica["BackendId"]].rsplit(".", 1)[1]) - 20
                    with c.LOCAL_HTTP.open(
                        f"http://127.0.0.1:{c.http_port('be',be_index)}/api/compaction/show?tablet_id={tablet_id}"
                    ) as response:
                        rowsets = json.load(response)
                    self.save(f"rowsets-{table}-t{tablet_id}-r{replica['ReplicaId']}.json", rowsets)
                    if sum(int(r["num_segments"]) > 0 for r in rowsets.get("rowset_details", [])) >= 2:
                        multiple_rowsets += 1
                self.initial_tablets[(table, tablet_id)] = replicas
            assert all(seen) and multiset_rows == self.copies * 12_000, multiset_rows
            assert multiple_rowsets > 0, "multi-Rowset coverage missing"
            self.log(
                "baseline_verified",
                table=table,
                rows=multiset_rows,
                tablets=len(groups),
                replicas=len(groups) * 3,
                multi_rowset_replicas=multiple_rowsets,
            )

    def wait_cleanup(self, long_days=20, timeout=600):
        expected = self.copies * (5403 if long_days == 20 else 4000)

        def ready():
            statuses = self.status()
            counts = {
                table: int(self.sql(f"SELECT count(*) n FROM {self.db}.{table}")[0]["n"]) for table in TABLES
            }
            return (
                statuses
                if all(
                    counts[t] == expected
                    and statuses[t]["BindingState"] == "ACTIVE"
                    and int(statuses[t]["RunningReplicaTasks"] or 0) == 0
                    and int(statuses[t]["PendingRewritePartitions"] or 0) == 0
                    and int(statuses[t]["CompletedPhysicalPartitions"] or 0) >= (9 if long_days == 20 else 6)
                    for t in TABLES
                )
                else None
            )

        result = c.wait_for(
            ready, timeout=timeout, interval=2, description="all-replica TTL progress and expected row count"
        )
        self.save("cleanup-status.json", result)
        self.log("cleanup_converged", expected_rows=expected, status=result)

    def verify(self, long_days=20):
        self.evaluation_time = int(time.time())
        assert (
            self.anchor <= self.evaluation_time < self.anchor + 86400
        ), "test crossed the fixed oracle window"
        expected_count = self.copies * (5403 if long_days == 20 else 4000)
        summary = {}
        for table in TABLES:
            partitions, groups = self.layout(table)
            self.save(f"after-{table}-layout.json", {"partitions": partitions, "tablets": groups})
            expected_partitions = 9 if long_days == 20 else 6
            assert len(partitions) == expected_partitions and len(groups) == expected_partitions * 4
            expected_all = []
            checked = 0
            for (baseline_table, tablet_id), original_replicas in self.initial_tablets.items():
                if baseline_table != table:
                    continue
                original = read_rows(self.directory / f"baseline-{table}-t{tablet_id}.gz")
                partition_upper = original_replicas[0].get("OracleUpperEpochSeconds")
                if partition_upper is not None:
                    assert all(
                        self.anchor - AGES[coordinates(row[0])[0]] * 86400 == partition_upper
                        for row in original
                    )
                expected = [
                    row for row in original if kept(row, self.anchor, self.evaluation_time, long_days)
                ]
                deleted = [
                    row for row in original if not kept(row, self.anchor, self.evaluation_time, long_days)
                ]
                write_rows(self.directory / f"expected-kept-{table}-t{tablet_id}.gz", expected)
                write_rows(self.directory / f"expected-deleted-{table}-t{tablet_id}.gz", deleted)
                expected_all.extend(expected)
                if tablet_id not in groups:
                    assert not expected, ("dropped keeper partition", tablet_id)
                    continue
                before_ids = {(r["ReplicaId"], r["BackendId"]) for r in original_replicas}
                after_ids = {(r["ReplicaId"], r["BackendId"]) for r in groups[tablet_id]}
                assert before_ids == after_ids, (
                    "replica replacement requires separate investigation",
                    before_ids,
                    after_ids,
                )
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                    scans = list(pool.map(lambda r: self.scan_replica(table, r, "after"), groups[tablet_id]))
                for replica, actual in zip(groups[tablet_id], scans):
                    extra = collections.Counter(actual) - collections.Counter(expected)
                    missing = collections.Counter(expected) - collections.Counter(actual)
                    self.save(
                        f"diff-{table}-t{tablet_id}-r{replica['ReplicaId']}.json",
                        {"extra": list(extra.items()), "missing": list(missing.items())},
                    )
                    assert not extra and not missing, ("replica data mismatch", replica, extra, missing)
                    assert int(replica["Version"]) >= int(original_replicas[0]["VisibleVersion"]), replica
                    checked += 1
            assert len(expected_all) == expected_count, len(expected_all)
            ordinary = self.sql(f"SELECT {COLS} FROM {self.db}.{table} ORDER BY event_id")
            ordinary_rows = [tuple(row[col] for col in COLS.split(",")) for row in ordinary]
            write_rows(self.directory / f"ordinary-{table}.rows.gz", ordinary_rows)
            assert collections.Counter(ordinary_rows) == collections.Counter(expected_all)
            aggregate = self.sql(f"SELECT count(*) n,min(metric) lo,max(metric) hi FROM {self.db}.{table}")[0]
            assert tuple(int(aggregate[key]) for key in ("n", "lo", "hi")) == (
                expected_count,
                min(row[4] for row in expected_all),
                max(row[4] for row in expected_all),
            ), aggregate
            self.save(f"aggregate-{table}.json", aggregate)
            grouped = self.sql(
                f"SELECT category,tenant,count(*) n FROM {self.db}.{table} GROUP BY category,tenant"
            )
            actual_groups = {(row["category"], row["tenant"]): int(row["n"]) for row in grouped}
            expected_groups = dict(collections.Counter((row[3], row[1]) for row in expected_all))
            assert actual_groups == expected_groups, ("tenant/partition grouping mismatch", table)
            self.save(f"grouped-{table}.json", grouped)
            summary[table] = {
                "rows": expected_count,
                "checked_replicas": checked,
                "kept_partitions": expected_partitions,
                "deleted_rows": self.copies * 12_000 - expected_count,
            }
        control = self.sql(f"SELECT event_id,metric FROM {self.db}.control ORDER BY event_id")
        assert [(r["event_id"], r["metric"]) for r in control] == [(1, -987), (2, 987)], control
        self.save(
            "data-verdict.json", {"passed": True, "evaluation_time": self.evaluation_time, "tables": summary}
        )
        self.log("data_verified", tables=summary)

    def assert_no_reissue(self):
        dictionary_id = int(self.status()[TABLES[0]]["DictionaryId"])

        def tasks():
            result = {}
            for i in (1, 2, 3):
                content = c.docker("exec", c.name("be", i), "cat", "/node/be/log/be.INFO").stdout
                records = [
                    line
                    for line in content.splitlines()
                    if "Tenant-TTL task finished." in line and f" dictionary_id={dictionary_id} " in line
                ]
                result[i] = records
            return result

        before = tasks()
        assert all(
            len({re.search(r"tablet_id=(\d+)", line).group(1) for line in records}) == 56
            for records in before.values()
        ), {k: len(v) for k, v in before.items()}
        time.sleep(22)
        after = tasks()
        assert before == after, "fully completed replicas were dispatched again without new input or policy"
        self.save(
            "no-reissue.json",
            {
                "dictionary_id": dictionary_id,
                "wait_seconds": 22,
                "task_counts": {k: len(v) for k, v in after.items()},
            },
        )
        self.log(
            "no_reissue_verified",
            dictionary_id=dictionary_id,
            task_counts={k: len(v) for k, v in after.items()},
        )

    def archive(self):
        # Only after a complete passing verdict: retain business data, retire this case's Dictionary so later
        # faults do not re-enqueue recovery for already-completed scenarios. Never used to rescue an active case.
        assert (self.directory / "data-verdict.json").exists()
        self.save("final-status.json", self.status())
        self.save("final-dictionary.json", self.sql(f"SHOW DICTIONARY {self.dictionary}"))
        assert self.dictionary.startswith("ttl_r043_")
        self.sql(f"DROP DICTIONARY {self.dictionary}")
        self.log("archived_dictionary", dictionary=self.dictionary, business_data_retained=True)


def normal(copies):
    case = Case("E01", copies)
    case.setup()
    environment = {
        "frontends": case.sql("SHOW FRONTENDS"),
        "backends": case.sql("SHOW BACKENDS"),
        "fault_rules": {},
    }
    for role in ("frontends", "backends"):
        assert len(environment[role]) == 3 and all(
            str(row["Alive"]).lower() == "true" for row in environment[role]
        ), environment
    for role, index in [("fe", i) for i in (1, 2, 3)] + [("be", 3)]:
        rules = c.docker("exec", c.name(role, index), "iptables", "-S", "OUTPUT").stdout
        assert "tenant-ttl-round043" not in rules, rules
        environment["fault_rules"][f"{role}{index}"] = rules
    case.save("normal-environment.json", environment)
    case.load()
    case.baseline()
    case.bind()
    case.wait_snapshot()
    case.wait_cleanup()
    case.verify()
    case.assert_no_reissue()
    case.archive()
    return case


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--copies", type=int, choices=(10, 100), default=10)
    args = parser.parse_args()
    result = normal(args.copies)
    result.save("scenario-verdict.json", {"passed": True, "scenario": "E01", "copies": args.copies})
