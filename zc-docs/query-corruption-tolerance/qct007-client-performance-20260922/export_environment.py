#!/usr/bin/env python3
"""Export only required environment evidence; omit arbitrary container env vars."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent


def select(record):
    config = record["HostConfig"]
    return {"name": record["Name"], "id": record["Id"], "image": record["Image"],
            "resource_config": {k: config.get(k) for k in
                ("NanoCpus", "CpuPeriod", "CpuQuota", "CpusetCpus", "Memory", "PidsLimit", "PidMode", "NetworkMode")},
            "mounts": [{k: mount.get(k) for k in ("Type", "Name", "Source", "Destination", "RW")}
                       for mount in record["Mounts"]],
            "state": {k: record["State"].get(k) for k in ("Status", "Running", "OOMKilled", "StartedAt", "FinishedAt")}}


def main():
    before = json.loads((ROOT / "split-environment-before.json").read_text())
    active = json.loads((ROOT / "split-environment-active.json").read_text())
    after = json.loads((ROOT / "split-environment-after.json").read_text())
    # Preserve the original failed clear-to-empty record. Docker ignored that
    # empty update. An explicit 0-7 update restores the original effective range
    # without recreating the durable build container; metadata is not identical.
    assert after["completed_groups"] == 22
    recovered = json.loads(subprocess.check_output(
        ["docker", "inspect", "starrocks-query-corruption-4.0-build"], text=True))[0]
    client_final = json.loads(subprocess.check_output(
        ["docker", "inspect", "starrocks-query-corruption-perf-client"], text=True))[0]
    effective = subprocess.check_output(["docker", "exec", "starrocks-query-corruption-4.0-build", "cat",
        "/sys/fs/cgroup/cpuset.cpus.effective"], text=True).strip()
    assert effective == "0-7" and recovered["HostConfig"]["CpusetCpus"] == "0-7"
    assert before["HostConfig"]["CpusetCpus"] == ""
    assert not client_final["State"]["Running"] and not client_final["State"]["OOMKilled"]
    for key in ("NanoCpus", "CpuPeriod", "CpuQuota", "Memory", "PidsLimit", "PidMode", "NetworkMode"):
        assert before["HostConfig"][key] == recovered["HostConfig"][key]
    assert before["Mounts"] == recovered["Mounts"]
    assert before["Id"] == recovered["Id"]
    assert before["State"]["StartedAt"] == recovered["State"]["StartedAt"]
    assert active["server"]["HostConfig"]["CpusetCpus"] == "0-5"
    assert active["client"]["HostConfig"]["CpusetCpus"] == "6-7"
    assert active["client"]["HostConfig"]["NanoCpus"] == 2000000000
    mounts = active["client"]["Mounts"]
    assert all("tenant-ttl" not in m.get("Name", "") for m in mounts)
    qct = next(m for m in mounts if m["Destination"] == "/query-corruption-workspace")
    assert qct["Name"] == "sr-query-corruption-4.0-build-cache-arm64" and not qct["RW"]
    with (ROOT / "environment-audit.json").open("x") as out:
        json.dump({"server_before": select(before), "server_during": select(active["server"]),
                   "client_during": select(active["client"]), "client_final": select(client_final),
                   "server_after_empty_update": select(after["server"]), "server_final": select(recovered),
                   "script_hashes": active["scripts"], "completed_groups": after["completed_groups"],
                   "original_cpu_quota_memory_mounts_and_container_identity_preserved": True,
                   "effective_cpu_range_restored": True, "effective_cpu_range": effective,
                   "cpuset_metadata_exactly_restored": False,
                   "recovery_note": "Empty cpuset update ignored; explicit 0-7 restores all current VM CPUs. If VM CPU count changes, update this explicit mask.",
                   "client_qct_asset_mount_read_only": True,
                   "no_tenant_ttl_mount": True}, out, indent=2)
    print("Environment audit passed; only whitelisted fields exported")


if __name__ == "__main__":
    main()
