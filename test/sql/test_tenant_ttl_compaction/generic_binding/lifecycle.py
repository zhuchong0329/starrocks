#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Real DDL, follower replay and leader replacement without waiting for old BE work."""
import datetime as dt
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "multireplica"))
import cluster as c


def run():
    db = "ttl_generic_ddl_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    directory = Path(__file__).resolve().parent / "artifacts" / db
    directory.mkdir(parents=True)
    evidence = {"db": db, "errors": [], "statuses": []}
    def sql(statement):
        return c.sql(c.leader(), statement, database=None if statement.startswith("CREATE DATABASE") else db)
    def rejected(statement, message):
        try:
            sql(statement)
        except c.pymysql.MySQLError as error:
            assert message in str(error), str(error)
            evidence["errors"].append({"sql": statement, "error": str(error)})
        else:
            raise AssertionError(statement)
    def show():
        row = sql(f"SHOW COMPACTION TTL STATUS FROM {db}.events")[0]
        evidence["statuses"].append(row)
        return row
    sql(f"CREATE DATABASE {db}")
    sql(f"CREATE TABLE {db}.policy (value_key VARCHAR(256) NOT NULL,table_name VARCHAR(128) NOT NULL,"
        "retention_days INT NOT NULL) PRIMARY KEY(value_key,table_name) DISTRIBUTED BY HASH(value_key) BUCKETS 1 "
        "PROPERTIES('replication_num'='3')")
    sql(f"CREATE DICTIONARY {db}_dict USING policy (value_key KEY,table_name KEY,retention_days VALUE) "
        "PROPERTIES('dictionary_refresh_interval'='0','dictionary_warm_up'='false')")
    ddl = ("(id BIGINT NOT NULL,dst_ip VARCHAR(40),device VARCHAR(64),recordTimestamp BIGINT NOT NULL,"
           "payload INT) DUPLICATE KEY(id) PARTITION BY date_trunc('day',from_unixtime(recordTimestamp)) "
           "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='3')")
    sql(f"CREATE TABLE {db}.events {ddl}")
    sql(f"INSERT INTO {db}.events VALUES(1,'',NULL,{int(time.time())},1)")
    def bind(column="dst_ip", default=30):
        suffix = "" if column is None else f",'compaction_retention_key_column'='{column}'"
        sql(f"ALTER TABLE {db}.events SET ('compaction_retention_condition'=\"dictionary_ttl('{db}_dict','generic.events',{default})\","
            f"'compaction_retention_time_zone'='UTC'{suffix})")
    bind()
    assert show()["KeyColumn"] == "dst_ip"
    rejected(f"ALTER TABLE {db}.events SET ('compaction_retention_key_column'='device')", "unbind TTL first")
    rejected(f"ALTER TABLE {db}.events RENAME COLUMN dst_ip TO address", "unbind TTL first")
    sql(f"ALTER TABLE {db}.events RENAME COLUMN payload TO note")
    bind(None, 60)
    assert show()["KeyColumn"] == "dst_ip" and show()["DefaultDays"] == 60
    for _ in range(2):
        sql(f"ALTER TABLE {db}.events SET ('compaction_retention_condition'='')")
        assert show()["BindingState"] == "DISABLED"
    rejected(f"ALTER TABLE {db}.events SET ('compaction_retention_key_column'='device')", "requires")
    rejected(f"ALTER TABLE {db}.events SET ('compaction_retention_condition'=\"dictionary_ttl('{db}_dict','generic.events',30)\","
             "'compaction_retention_key_column'='missing','compaction_retention_time_zone'='UTC')", "requires column")
    assert show()["BindingState"] == "DISABLED"
    sql(f"ALTER TABLE {db}.events RENAME COLUMN dst_ip TO address")
    bind("address")
    assert show()["KeyColumn"] == "address"
    # Replicated journal carries the new identity/name; a different FE must serve it after election.
    previous = c.leader()
    evidence["leader_before"] = previous
    c.stop("fe", previous)
    try:
        c.wait_for(lambda: c.leader() and c.leader() != previous, timeout=120, description="new leader")
        evidence["leader_after"] = c.leader()
        assert show()["KeyColumn"] == "address"
        sql(f"ALTER TABLE {db}.events SET ('compaction_retention_condition'='')")
        assert show()["BindingState"] == "DISABLED"
    finally:
        c.start("fe", previous)
        c.wait_for(lambda: c.sql(previous, "SELECT 1"), timeout=120, description="original FE recovered")
    ddl_after = sql(f"SHOW CREATE TABLE {db}.events")
    evidence["ddl_after"] = ddl_after
    assert "compaction_retention" not in str(ddl_after), ddl_after
    assert len(sql(f"SELECT id,address,device,recordTimestamp,note FROM {db}.events")) == 1
    assert sql(f"SHOW TABLES FROM {db}"), db
    evidence["passed"] = True
    (directory / "result.json").write_text(json.dumps(evidence, indent=2, default=str))
    print(json.dumps({"passed": True, "directory": str(directory)}), flush=True)


if __name__ == "__main__":
    run()
