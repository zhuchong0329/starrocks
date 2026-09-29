#!/usr/bin/env python3
"""Client-only adapter; read-only QCT assets, separate durable result volume."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import client_perf as core

core.ROOT = Path("/qct-client-workspace/results")
original_snapshot = core.snapshot


def snapshot(pids):
    result = original_snapshot(pids)
    # Both containers share PID/network namespaces, not CPU cgroups. Read the
    # server's original cgroup mount through its process root; never write it.
    fe = core.runtime.live_record("fe")
    root = Path("/proc") / str(fe["pid"]) / "root/sys/fs/cgroup"
    result["server_cgroup"] = {name: (root / name).read_text().strip()
                              for name in ("cpu.max", "cpu.stat", "memory.events", "cpuset.cpus.effective")}
    result["client_cpuset"] = Path("/sys/fs/cgroup/cpuset.cpus.effective").read_text().strip()
    if result["server_cgroup"]["cpu.max"] != "600000 100000":
        raise RuntimeError("Unexpected server CPU budget")
    if result["server_cgroup"]["cpuset.cpus.effective"] != "0-5" or result["client_cpuset"] != "6-7":
        raise RuntimeError("Client/server CPU affinity is not isolated")
    return result


core.snapshot = snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=core.iso.VARIANTS.values(), required=True)
    parser.add_argument("--client-mode", choices=(core.THREADS, core.PROCESSES), required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check = snapshot([os.getpid()])
    print(json.dumps({"event": "split_preflight", "server_cpu_max": check["server_cgroup"]["cpu.max"],
                      "server_cpuset": check["server_cgroup"]["cpuset.cpus.effective"],
                      "client_cpu_max": check["cgroup"]["cpu.max"],
                      "client_cpuset": check["client_cpuset"],
                      "adapter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}), flush=True)
    core.client(args)


if __name__ == "__main__":
    main()
