#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Round 050: exact generic-key retention on every real replica, with saved evidence."""
import argparse
import collections
import concurrent.futures
import datetime as dt
import gzip
import json
from pathlib import Path
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "multireplica"))
import cluster as c
import verify as previous

COLS = "event_id,dst_ip,device,recordTimestamp,metric,payload"
previous.COLS = COLS
AGES = previous.AGES
SPECIAL = ["", " ", "Case", "case", "001", "1", "192.0.2.1", "设备 A", "missing", None]


def key(i):
    return SPECIAL[i] if i < len(SPECIAL) else f"key_{i:04d}"


def policy(i):
    # Distinct empty/case/numeric-looking keys, plus a genuine missing-key fallback.
    return [2, 20, 3, 15, 4, 12, 5, 18, 10, 10][i] if i < 10 else [2, 3, 20, 10][i % 4]


class Case:
    scan_replica = previous.Case.scan_replica

    def __init__(self, copies):
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.db = f"ttl_generic_{copies}_{stamp}"
        self.dictionary = self.db + "_dict"
        self.anchor = int(dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        self.copies = copies
        self.directory = Path(__file__).resolve().parent / "artifacts" / self.db
        self.directory.mkdir(parents=True)
        self.backend_ips = {str(r["BackendId"]): r["IP"] for r in self.sql("SHOW BACKENDS")}
        self.initial = {}
        self.results = {}
        self.save("case.json", {"db": self.db, "copies": copies, "anchor": self.anchor})

    def save(self, name, value):
        (self.directory / name).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str))

    def log(self, event, **values):
        value = {"event": event, "db": self.db, "time": time.time(), **values}
        with (self.directory / "events.jsonl").open("a") as out:
            out.write(json.dumps(value, default=str) + "\n")
        print(json.dumps(value, default=str), flush=True)

    def sql(self, sql, args=None):
        return c.sql(c.leader(), sql, args, database=None if sql.startswith(("CREATE DATABASE", "SHOW BACKENDS")) else self.db)

    def rows(self):
        for p in range(12):
            for i in range(1000):
                for copy in range(self.copies):
                    # Intentional duplicates retain the full multiset, not just unique event IDs.
                    event = p * 10_000_000 + i * 1000 + copy // 2
                    yield (event, key(i), key((i + 7) % 1000), self.anchor - AGES[p] * 86400 - 43200,
                           -event if i < 10 else event, None if copy % 4 < 2 else "payload_雪")

    def setup(self):
        self.sql(f"CREATE DATABASE {self.db}")
        self.sql(f"CREATE TABLE {self.db}.policy (policy_value VARCHAR(256) NOT NULL, "
                 "table_name VARCHAR(128) NOT NULL, retention_days INT NOT NULL) "
                 "PRIMARY KEY(policy_value,table_name) DISTRIBUTED BY HASH(policy_value) BUCKETS 4 "
                 "PROPERTIES('replication_num'='3')")
        values = []
        for table in ("by_ip", "by_device"):
            values.append(("default", "generic." + table, 10))
            values.extend((key(i), "generic." + table, policy(i)) for i in range(1000) if i not in (8, 9))
        with c.connect(c.leader()) as conn, conn.cursor() as cursor:
            cursor.executemany(f"INSERT INTO {self.db}.policy VALUES (%s,%s,%s)", values)
        self.save("policies.json", values)
        self.sql(f"CREATE DICTIONARY {self.dictionary} USING policy "
                 "(policy_value KEY,table_name KEY,retention_days VALUE) "
                 "PROPERTIES('dictionary_refresh_interval'='0','dictionary_warm_up'='false')")
        ddl = ("(event_id BIGINT NOT NULL,dst_ip VARCHAR(40) NULL,device VARCHAR(64) NULL,"
               "recordTimestamp BIGINT NOT NULL,metric BIGINT NOT NULL,payload VARCHAR(128) NULL) "
               "DUPLICATE KEY(event_id) PARTITION BY date_trunc('day',from_unixtime(recordTimestamp)) "
               "DISTRIBUTED BY HASH(event_id) BUCKETS 4 PROPERTIES('replication_num'='3')")
        for table in ("by_ip", "by_device", "control"):
            self.sql(f"CREATE TABLE {self.db}.{table} {ddl}")
        self.sql(f"INSERT INTO {self.db}.control VALUES (1,'',NULL,{self.anchor-100*86400},1,'control')")
        batch = []
        index = 0
        for row in self.rows():
            batch.append(row)
            if len(batch) == 10000:
                self.load_batch(batch, index)
                batch = []
                index += 1
        if batch:
            self.load_batch(batch, index)
        self.log("loaded", rows_per_table=self.copies * 12000)

    def load_batch(self, rows, index):
        data = ("\n".join("\t".join("\\N" if v is None else str(v) for v in r) for r in rows) + "\n").encode()
        (self.directory / f"input-{index}.tsv.gz").write_bytes(gzip.compress(data))
        for table in ("by_ip", "by_device"):
            request = urllib.request.Request(
                f"http://127.0.0.1:{c.http_port('be',1)}/api/{self.db}/{table}/_stream_load", data=data, method="PUT",
                headers={"Authorization": c.AUTH, "label": f"{self.db}_{table}_{index}", "columns": COLS,
                         "max_filter_ratio": "0", "strict_mode": "true", "timeout": "300"})
            with c.LOCAL_HTTP.open(request, timeout=360) as response:
                result = json.load(response)
            self.save(f"load-{table}-{index}.json", result)
            assert result["Status"] == "Success" and int(result["NumberLoadedRows"]) == len(rows), result

    def layout(self, table):
        parts = self.sql(f"SHOW PARTITIONS FROM {self.db}.{table}")
        groups = collections.defaultdict(list)
        for p in parts:
            tablets = self.sql(f"SHOW TABLET FROM {self.db}.{table} PARTITION({p['PartitionName']})")
            for r in tablets:
                r["PartitionName"] = p["PartitionName"]
                groups[r["TabletId"]].append(r)
        for replicas in groups.values():
            assert len(replicas) == 3 and len({r["BackendId"] for r in replicas}) == 3, replicas
        return parts, groups

    def baseline(self):
        expected = collections.Counter(self.rows())
        for table in ("by_ip", "by_device"):
            parts, groups = self.layout(table)
            assert len(parts) == 12 and len(groups) == 48, (len(parts), len(groups))
            self.save(f"baseline-{table}-layout.json", {"partitions": parts, "tablets": groups})
            all_rows = collections.Counter()
            for tablet, replicas in groups.items():
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                    scans = list(pool.map(lambda r: self.scan_replica(table, r, "baseline"), replicas))
                for rows in scans[1:]:
                    assert collections.Counter(rows) == collections.Counter(scans[0]), (table, tablet)
                self.initial[(table, str(tablet))] = scans[0]
                all_rows.update(scans[0])
            assert all_rows == expected, (table, all_rows - expected, expected - all_rows)
        self.log("baseline_exact", rows_per_table=sum(expected.values()))

    def bind(self, table, column):
        self.sql(f"ALTER TABLE {self.db}.{table} SET ("
                 f"'compaction_retention_condition'=\"dictionary_ttl('{self.dictionary}','generic.{table}',30)\","
                 f"'compaction_retention_key_column'='{column}','compaction_retention_time_zone'='UTC')")

    def status(self, table):
        return self.sql(f"SHOW COMPACTION TTL STATUS FROM {self.db}.{table}")[0]

    def expected(self, table, rows):
        index = 1 if table == "by_ip" else 2
        policies = {key(i): policy(i) for i in range(1000) if i not in (8, 9)}
        for r in rows:
            age = AGES[r[0] // 10_000_000]
            if age >= 20:
                continue  # Entire partition is expired, including NULL.
            if r[index] is None or age < policies.get(r[index], 10):
                yield r

    def verify_final(self):
        for table in ("by_ip", "by_device"):
            parts, groups = self.layout(table)
            expected_all = collections.Counter(self.expected(table, self.rows()))
            actual_all = collections.Counter()
            replicas_scanned = 0
            for tablet, replicas in groups.items():
                expected = collections.Counter(self.expected(table, self.initial[(table, str(tablet))]))
                previous.write_rows(self.directory / f"expected-{table}-{tablet}.rows.gz", expected.elements())
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                    scans = list(pool.map(lambda r: self.scan_replica(table, r, "final"), replicas))
                for replica, rows in zip(replicas, scans):
                    actual = collections.Counter(rows)
                    if actual != expected:
                        self.save(f"diff-{table}-{tablet}-{replica['ReplicaId']}.json",
                                  {"unexpected": list((actual-expected).items()), "missing": list((expected-actual).items())})
                    assert actual == expected, (table, tablet, replica["ReplicaId"])
                    replicas_scanned += 1
                actual_all.update(scans[0])
            assert actual_all == expected_all, table
            # Full table scan additionally rejects misplaced rows or an omitted partition.
            actual = collections.Counter(tuple(r.values()) for r in self.sql(f"SELECT {COLS} FROM {self.db}.{table}"))
            assert actual == expected_all, table
            self.results[table] = {"input": self.copies*12000, "kept": sum(expected_all.values()),
                                   "deleted": self.copies*12000-sum(expected_all.values()),
                                   "partitions": len(parts), "replicas_scanned": replicas_scanned}
        control = self.sql(f"SELECT {COLS} FROM {self.db}.control")
        assert len(control) == 1 and control[0]["dst_ip"] == "", control
        self.save("result.json", {"passed": True, "tables": self.results})
        self.log("all_replicas_exact", tables=self.results)

    def ddl_checks(self):
        for statement in [f"ALTER TABLE {self.db}.by_ip SET ('compaction_retention_key_column'='device')",
                          f"ALTER TABLE {self.db}.by_ip RENAME COLUMN dst_ip TO renamed"]:
            try:
                self.sql(statement)
            except c.pymysql.MySQLError as error:
                assert "unbind TTL first" in str(error), str(error)
            else:
                raise AssertionError(statement)
        for table, column in [("by_ip", "dst_ip"), ("by_device", "device")]:
            generic = self.sql(f"SHOW COMPACTION TTL STATUS FROM {self.db}.{table} FOR VALUE ''")[0]
            legacy = self.sql(f"SHOW TENANT TTL STATUS FROM {self.db}.{table} FOR TENANT ''")[0]
            assert generic["KeyColumn"] == column and generic["DictionaryKeyColumn"] == "policy_value", generic
            assert generic["Value"] == "" and legacy["Tenant"] == "", (generic, legacy)
            assert generic["EffectiveRetentionDays"] == legacy["EffectiveRetentionDays"] == 2, (generic, legacy)
            assert generic["ResolutionType"] == legacy["ResolutionType"] == "TENANT_OVERRIDE", (generic, legacy)
            self.save(f"show-{table}.json", {"generic": generic, "legacy": legacy})

    def run(self):
        self.setup()
        self.baseline()
        return self.finish()

    def finish(self):
        started = time.monotonic()
        for table, column in [("by_ip", "dst_ip"), ("by_device", "device")]:
            ddl_started = time.monotonic()
            self.bind(table, column)
            self.log("bound", table=table, column=column, ddl_seconds=time.monotonic()-ddl_started)
        def ready():
            statuses = {t: self.status(t) for t in ("by_ip", "by_device")}
            # All-replica completed progress gates the subsequent exact row comparison.
            return statuses if all(s["BindingState"] == "ACTIVE" and int(s["CompletedPhysicalPartitions"] or 0) >= 9
                                   for s in statuses.values()) else None
        self.save("ready.json", c.wait_for(ready, timeout=900, interval=2, description="generic TTL completion"))
        self.log("cleanup_complete", seconds_since_bind_start=time.monotonic()-started)
        self.ddl_checks()
        self.verify_final()
        # Leave business evidence in place, but retire completed bindings to isolate later runs.
        for table in ("by_ip", "by_device"):
            self.sql(f"ALTER TABLE {self.db}.{table} SET ('compaction_retention_condition'='')")
        return {"directory": str(self.directory), "tables": self.results}


class ComparisonCase(Case):
    """Same inputs, key type/position, policy and replicas; compare the two configuration entrances."""

    def expected(self, table, rows):
        return super().expected("by_ip", rows)

    def metrics(self, role, index):
        with c.LOCAL_HTTP.open(f"http://127.0.0.1:{c.http_port(role,index)}/metrics", timeout=15) as response:
            lines = response.read().decode().splitlines()
        selected = {}
        for line in lines:
            if line.startswith(("starrocks_be_tenant_ttl_compaction_", "tenant_ttl_compaction_", "jvm_heap_size_bytes")):
                name, value = line.rsplit(" ", 1)
                selected[name] = float(value)
        return selected

    def run(self):
        self.setup()
        self.baseline()
        observations = {}
        self.sql(f"ALTER TABLE {self.db}.by_ip RENAME COLUMN dst_ip TO tenant")
        for table, legacy in [("by_ip", True), ("by_device", False)]:
            before = {i: self.metrics("be", i) for i in (1, 2, 3)}
            heap_before = self.metrics("fe", c.leader())
            started = time.monotonic()
            if legacy:
                self.sql(f"ALTER TABLE {self.db}.{table} SET ("
                         f"'compaction_retention_condition'=\"dictionary_ttl('{self.dictionary}','generic.{table}',30)\","
                         "'compaction_retention_time_zone'='UTC')")
            else:
                self.bind(table, "dst_ip")
            ddl_seconds = time.monotonic()-started
            samples = []
            def ready():
                status = self.status(table)
                samples.append({"seconds": time.monotonic()-started, "status": status,
                                "fe_heap": self.metrics("fe", c.leader())})
                return status if (status["BindingState"] == "ACTIVE" and
                                  int(status["CompletedPhysicalPartitions"] or 0) >= 9) else None
            status = c.wait_for(ready, timeout=900, interval=1, description="comparison TTL completion")
            elapsed = time.monotonic()-started
            assert status["KeyColumn"] == ("tenant" if legacy else "dst_ip"), status
            after = {i: self.metrics("be", i) for i in (1, 2, 3)}
            delta = {i: {key: value-before[i].get(key, 0) for key, value in after[i].items()} for i in after}
            observations["legacy" if legacy else "explicit"] = {
                "ddl_seconds": ddl_seconds, "completion_seconds": elapsed,
                "be_before": before, "be_after": after, "be_delta": delta,
                "fe_heap_before": heap_before, "samples": samples}
            self.sql(f"ALTER TABLE {self.db}.{table} SET ('compaction_retention_condition'='')")
            if legacy:
                self.sql(f"ALTER TABLE {self.db}.by_ip RENAME COLUMN tenant TO dst_ip")
            self.log("comparison_phase_complete", legacy=legacy, seconds=elapsed)
        # Rename is performed only after unbind; row verification still reads actual stored values.
        self.verify_final()
        self.save("comparison.json", {"passed": True, "observations": observations,
                  "limits": "Sequential Debug observations include scheduler wait, GC and cache effects; no SLA or causal speedup claim."})
        return {"directory": str(self.directory), "tables": self.results, "comparison": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--copies", type=int, default=10)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--compare-legacy", action="store_true")
    args = parser.parse_args()
    for i in (1, 2, 3):
        previous.be_config(i, "max_compaction_concurrency", 0)
    c.config("tenant_ttl_scheduler_interval_seconds", 10)
    c.config("tenant_ttl_policy_snapshot_recovery_cooldown_seconds", 1)
    try:
        if args.prepare_only:
            case = Case(args.copies)
            case.setup()
            case.baseline()
            case.log("prepared", directory=str(case.directory))
        elif args.resume:
            case = Case.__new__(Case)
            case.directory = args.resume.resolve()
            metadata = json.loads((case.directory / "case.json").read_text())
            case.db, case.copies, case.anchor = metadata["db"], metadata["copies"], metadata["anchor"]
            case.dictionary = case.db + "_dict"
            case.backend_ips = {str(r["BackendId"]): r["IP"] for r in case.sql("SHOW BACKENDS")}
            case.initial, case.results = {}, {}
            for table in ("by_ip", "by_device"):
                layout = json.loads((case.directory / f"baseline-{table}-layout.json").read_text())
                for tablet, replicas in layout["tablets"].items():
                    replica = replicas[0]
                    case.initial[(table, tablet)] = previous.read_rows(case.directory /
                        f"baseline-{table}-t{tablet}-r{replica['ReplicaId']}.rows.gz")
            case.finish()
            print(json.dumps({"directory": str(case.directory), "tables": case.results}, indent=2))
        elif args.compare_legacy:
            print(json.dumps(ComparisonCase(args.copies).run(), indent=2))
        else:
            print(json.dumps(Case(args.copies).run(), indent=2))
    finally:
        c.config("tenant_ttl_scheduler_interval_seconds", 600)
        c.config("tenant_ttl_policy_snapshot_recovery_cooldown_seconds", 300)
        for i in (1, 2, 3):
            previous.be_config(i, "max_compaction_concurrency", -1)
