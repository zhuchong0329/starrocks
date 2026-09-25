#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Restore only round043 node availability, scoped firewall rules and temporary configuration."""
import json
import shlex

import cluster as c
from verify import be_config


def restore():
    for role in ("fe", "be"):
        for index in (1, 2, 3):
            state = json.loads(c.docker("inspect", c.name(role, index)).stdout)[0]["State"]
            if not state["Running"]:
                c.start(role, index)
    for index in (1, 2, 3):
        c.wait_for(lambda: c.sql(index, "SELECT 1"), timeout=180, description="FE SQL restored")
    for role, index, ports in [("fe", i, ("8060", "9060")) for i in (1, 2, 3)] + [("be", 3, ("9020",))]:
        rules = c.docker("exec", c.name(role, index), "iptables", "-S", "OUTPUT").stdout.splitlines()
        for rule in rules:
            if "--comment tenant-ttl-round043" not in rule:
                continue
            args = shlex.split(rule)
            assert args[:2] == ["-A", "OUTPUT"] and "--dport" in args, rule
            assert args[args.index("--dport") + 1] in ports, rule
            args[0] = "-D"
            c.docker("exec", c.name(role, index), "iptables", *args)
    c.config("tenant_ttl_scheduler_interval_seconds", 600)
    c.config("tenant_ttl_policy_snapshot_auto_recover_enabled", True)
    c.config("tenant_ttl_policy_snapshot_recovery_cooldown_seconds", 300)
    backend_updates = {}
    for index in (1, 2, 3):
        backend_updates[index] = c.wait_for(
            lambda: be_config(index, "max_compaction_concurrency", -1), timeout=180
        )
    leader = c.leader()
    result = {
        "leader": leader,
        "frontends": c.sql(leader, "SHOW FRONTENDS"),
        "backends": c.sql(leader, "SHOW BACKENDS"),
        "frontend_config": {},
        "backend_config_updates": backend_updates,
        "firewall": {},
    }
    assert len(result["frontends"]) == 3 and all(
        str(r["Alive"]).lower() == "true" for r in result["frontends"]
    )
    assert len(result["backends"]) == 3 and all(str(r["Alive"]).lower() == "true" for r in result["backends"])
    for index in (1, 2, 3):
        result["frontend_config"][index] = c.sql(index, "ADMIN SHOW FRONTEND CONFIG LIKE 'tenant_ttl%'")
        values = {row["Key"]: row["Value"] for row in result["frontend_config"][index]}
        assert values["tenant_ttl_scheduler_interval_seconds"] == "600", values
        assert values["tenant_ttl_policy_snapshot_auto_recover_enabled"] == "true", values
        assert values["tenant_ttl_policy_snapshot_recovery_cooldown_seconds"] == "300", values
        result["firewall"][f"fe{index}"] = c.docker(
            "exec", c.name("fe", index), "iptables", "-S", "OUTPUT"
        ).stdout
        assert "tenant-ttl-round043" not in result["firewall"][f"fe{index}"]
    result["firewall"]["be3"] = c.docker("exec", c.name("be", 3), "iptables", "-S", "OUTPUT").stdout
    assert "tenant-ttl-round043" not in result["firewall"]["be3"]
    (c.ARTIFACTS / "restored-cluster.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    restore()
