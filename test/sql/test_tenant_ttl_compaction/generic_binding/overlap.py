#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Real partial-replica deletion, immediate rebind and delivery of an old-column request after new completion."""
import argparse
import collections
import datetime as dt
import importlib.util
import json
from pathlib import Path
import sys
import time
import urllib.request

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("generic_verify", HERE / "verify.py")
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
c = g.c
previous = g.previous


def firewall(enable):
    for i in (1, 2, 3):
        rule = ["OUTPUT", "-d", c.ip("be", 3), "-p", "tcp", "--dport", "9060", "-m", "comment",
                "--comment", "tenant-ttl-generic-overlap", "-j", "REJECT", "--reject-with", "tcp-reset"]
        present = c.docker("exec", c.name("fe", i), "iptables", "-C", *rule, check=False)
        if present.returncode not in (0, 1):
            raise RuntimeError(present.stderr)
        if (present.returncode == 0) != enable:
            c.docker("exec", c.name("fe", i), "iptables", "-I" if enable else "-D", *rule)


def run(row_count=12000):
    case = g.Case.__new__(g.Case)
    case.db = "ttl_generic_overlap_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    case.directory = HERE / "artifacts" / case.db
    case.directory.mkdir(parents=True)
    case.backend_ips = {str(r["BackendId"]): r["IP"] for r in c.sql(c.leader(), "SHOW BACKENDS")}
    case.sql(f"CREATE DATABASE {case.db}")
    case.sql(f"CREATE TABLE {case.db}.policy (tenant VARCHAR(128) NOT NULL,table_name VARCHAR(128) NOT NULL,"
             "retention_days INT NOT NULL) PRIMARY KEY(tenant,table_name) DISTRIBUTED BY HASH(tenant) BUCKETS 1 "
             "PROPERTIES('replication_num'='3')")
    case.sql(f"INSERT INTO {case.db}.policy VALUES('expire','generic.events',1),('default','generic.events',30)")
    dictionary = case.db + "_dict"
    case.sql(f"CREATE DICTIONARY {dictionary} USING policy (tenant KEY,table_name KEY,retention_days VALUE) "
             "PROPERTIES('dictionary_refresh_interval'='0','dictionary_warm_up'='false')")
    case.sql(f"CREATE TABLE {case.db}.events (event_id BIGINT NOT NULL,dst_ip VARCHAR(40),device VARCHAR(64),"
             "recordTimestamp BIGINT NOT NULL,metric BIGINT NOT NULL,payload VARCHAR(128)) DUPLICATE KEY(event_id) "
             "PARTITION BY date_trunc('day',from_unixtime(recordTimestamp)) DISTRIBUTED BY HASH(event_id) BUCKETS 1 "
             "PROPERTIES('replication_num'='3')")
    timestamp = int(time.time()) - 5*86400
    case.sql(f"INSERT INTO {case.db}.events SELECT generate_series,if(generate_series%2=0,'expire','keep'),"
             f"'keep',{timestamp},generate_series,'overlap' FROM TABLE(generate_series(1,{row_count}))")
    _, groups = case.layout("events")
    assert len(groups) == 1, groups
    tablet, replicas = next(iter(groups.items()))
    baseline = {r["BackendId"]: case.scan_replica("events", r, "baseline") for r in replicas}
    original = next(iter(baseline.values()))
    assert len(original) == row_count and all(collections.Counter(rows) == collections.Counter(original) for rows in baseline.values())
    expected = collections.Counter(r for r in original if r[1] != "expire")
    def rows(replica):
        return [tuple(r.values()) for r in case.sql(f"SELECT {g.COLS} FROM {case.db}.events "
                                                    f"TABLET({tablet}) REPLICA({replica['ReplicaId']})")]
    def bind(column):
        begin = time.monotonic()
        case.sql(f"ALTER TABLE {case.db}.events SET ('compaction_retention_condition'=\"dictionary_ttl('{dictionary}',"
                 f"'generic.events',30)\",'compaction_retention_key_column'='{column}',"
                 "'compaction_retention_time_zone'='UTC')")
        return time.monotonic()-begin
    blocked = False
    try:
        blocked = True
        firewall(True)
        bind("dst_ip")
        def partial():
            actual = {r["BackendId"]: collections.Counter(rows(r)) for r in replicas}
            return actual if all(actual[r["BackendId"]] == (collections.Counter(original) if
                case.backend_ips[r["BackendId"]] == c.ip("be",3) else expected) for r in replicas) else None
        c.wait_for(partial, timeout=900, interval=1, description="two old-policy deletions and blocked third replica")
        old_status = case.status("events")
        assert int(old_status["CompletedPhysicalPartitions"] or 0) == 0, old_status
        with c.LOCAL_HTTP.open(f"http://127.0.0.1:{c.http_port('be',3)}/api/meta/header/{tablet}") as response:
            header = json.load(response)
        case.save("old-header.json", header)
        case.save("old-status.json", old_status)
        request_args = [str(9_000_000_000_000 + int(time.time())), str(tablet), str(header["partition_id"]),
                        str(old_status["KeyColumnUniqueId"]), "expire", str(old_status["DictionaryId"]),
                        str(old_status["SnapshotTxnId"]), str(int(time.time())), str(header["schema"]["id"]),
                        str(header["schema"]["schema_version"]), str(max(r["end_version"] for r in header["rs_metas"]))]
        case.save("delayed-request.json", request_args)
        begin = time.monotonic()
        case.sql(f"ALTER TABLE {case.db}.events SET ('compaction_retention_condition'='')")
        unbind_seconds = time.monotonic()-begin
        bind_seconds = bind("device")
        firewall(False)
        blocked = False
        c.wait_for(lambda: int(case.status("events")["CompletedPhysicalPartitions"] or 0) == 1,
                   timeout=900, interval=1, description="new-column plan completed on all replicas")
        new_status = case.status("events")
        assert new_status["KeyColumn"] == "device", new_status
        # The new policy retains everything present. Old deleted rows are not restored.
        new_counts = {}
        for replica in replicas:
            actual = collections.Counter(case.scan_replica("events", replica, "new-completed"))
            assert actual == (collections.Counter(original) if case.backend_ips[replica["BackendId"]] == c.ip("be",3) else expected)
            new_counts[case.backend_ips[replica["BackendId"]]] = sum(actual.values())
        result = c.docker("exec", c.name("be",3), "/usr/lib/jvm/java-17-openjdk-arm64/bin/java", "-cp",
                          c.ROOT + "/generic-binding-20260927/tools:" + c.ROOT + "/fe/fe-core/target/*:" +
                          c.ROOT + "/fe/fe-core/target/lib/*", "DelayedRequest", *request_args)
        (case.directory / "delayed-submit.txt").write_text(result.stdout+result.stderr)
        target = next(r for r in replicas if case.backend_ips[r["BackendId"]] == c.ip("be",3))
        c.wait_for(lambda: collections.Counter(rows(target)) == expected, timeout=120, interval=1,
                   description="late old-column request deleted after new plan completed")
        for replica in replicas:
            assert collections.Counter(case.scan_replica("events", replica, "late-old-completed")) == expected
        final_status = case.status("events")
        assert final_status["KeyColumn"] == "device", final_status
        case.save("result.json", {"passed": True, "input_rows": len(original), "kept_per_replica": sum(expected.values()),
                                   "replica_divergence_after_new_plan": [new_counts[c.ip("be",i)] for i in (1,2,3)],
                                   "unbind_seconds": unbind_seconds, "bind_seconds": bind_seconds,
                                   "old_status": old_status, "new_status": new_status, "final_status": final_status})
        case.sql(f"ALTER TABLE {case.db}.events SET ('compaction_retention_condition'='')")
        print(json.dumps({"passed": True, "directory": str(case.directory)}), flush=True)
    finally:
        if blocked:
            firewall(False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=12000)
    args = parser.parse_args()
    if args.rows < 2 or args.rows % 2:
        parser.error("--rows must be an even integer of at least 2")
    c.config("tenant_ttl_scheduler_interval_seconds",10)
    c.config("tenant_ttl_policy_snapshot_recovery_cooldown_seconds",1)
    c.config("tenant_ttl_agent_task_max_attempts",1)
    c.config("tenant_ttl_agent_task_soft_timeout_seconds",3)
    try:
        run(args.rows)
    finally:
        c.config("tenant_ttl_agent_task_max_attempts",30)
        c.config("tenant_ttl_agent_task_soft_timeout_seconds",3600)
        c.config("tenant_ttl_policy_snapshot_recovery_cooldown_seconds",300)
        c.config("tenant_ttl_scheduler_interval_seconds",600)
