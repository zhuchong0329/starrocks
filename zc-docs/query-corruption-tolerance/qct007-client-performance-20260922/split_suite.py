#!/usr/bin/env python3
"""Host orchestration plus server-side checks for CPU-isolated QCT controls.

No production builds/edits, data writes, fault injection, or TTL operations.
The client has read-only QCT assets and a separate persistent evidence volume.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

SERVER = "starrocks-query-corruption-4.0-build"
CLIENT = "starrocks-query-corruption-perf-client"
VOLUME = "sr-query-corruption-client-perf-arm64"
IMAGE = "sha256:98326df55743ab4f081fcba9fa9cb31b7ff5ee06d69ddd54c11d7fae9cbbaeb2"
PYTHON = "/query-corruption-workspace/tools/python/bin/python"
LOGROOT = "/query-corruption-workspace/logs/QCT-007"
SCRIPT = LOGROOT + "/split-suite-20260922.py"
SERIES = "split-client-20260922-r1"


def command(argv, **kwargs):
    return subprocess.run(argv, check=True, text=True, **kwargs)


def capture(argv):
    return subprocess.check_output(argv, text=True)


def schedule():
    runs = []
    for block, order in enumerate((("threads", "processes"), ("processes", "threads")), 1):
        for pos, mode in enumerate(order, 1):
            runs.append({"name": f"control-{block}-pos-{pos}-{mode}", "variant": "A-baseline",
                         "client_mode": mode, "block": block, "position": pos, "phase": "client"})
    variants = {"A": "A-baseline", "B": "B-off", "C": "C-on"}
    for block, order in enumerate(("ABC", "ACB", "BAC", "BCA", "CAB", "CBA"), 1):
        for pos, letter in enumerate(order, 1):
            runs.append({"name": f"abc-{block}-{order}-pos-{pos}-{letter}", "variant": variants[letter],
                         "client_mode": "processes", "block": block, "position": pos, "phase": "abc"})
    return runs


def server_stage(args):
    source = Path(LOGROOT) / "client-perf-20260922.py"
    spec = importlib.util.spec_from_file_location("client_perf", source)
    core = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = core
    spec.loader.exec_module(core)
    root = Path(LOGROOT) / SERIES
    if args.stage == "init":
        core.matrix.ensure_idle_builds()
        core.matrix.ensure_restored()
        if any(core.runtime.live_record(n) for n in core.runtime.NODES):
            raise RuntimeError("Another QCT run is active")
        core.checked_destination(root).mkdir()
        core.iso.write_json(root / "storage-before.json", core.iso.storage_files())
        core.iso.write_json(root / "design.json", {"utc": core.iso.utc(), "schedule": schedule(),
            "experiment": "split", "case": "detail_page", "concurrency": 4, "dop": 2,
            "samples_per_run": 10000, "warmup_seconds": 60, "query_cache": False, "storage_page_cache": True,
            "server_cpus": "0-5", "server_quota": 6, "client_cpus": "6-7", "client_quota": 2,
            "client_volume": VOLUME, "baseline": core.matrix.BASELINE, "feature": core.iso.FEATURE,
            "harness_sha256": core.matrix.sha256(source),
            "orchestrator_sha256": core.matrix.sha256(Path(__file__)),
            "limitations": "Separate client/server CPU sets and quotas, but shared VM/host/cache/network; other containers untouched"})
        return
    if args.stage == "start":
        if args.name not in {r["name"] for r in schedule()}:
            raise ValueError("Unknown run")
        core.matrix.ensure_idle_builds()
        core.matrix.ensure_restored()
        if any(core.runtime.live_record(n) for n in core.runtime.NODES):
            raise RuntimeError("Active QCT nodes before start")
        original = json.loads((root / "storage-before.json").read_text())
        if core.iso.storage_files() != original:
            raise RuntimeError("Physical data changed")
        run = next(r for r in schedule() if r["name"] == args.name)
        core.runtime.prepare(run["variant"], page_cache=True)
        for node in core.runtime.NODES:
            core.runtime.start(node)
        ready = core.matrix.wait_ready(run["variant"], timeout=90)
        for _ in range(3):
            time.sleep(5)
            ready = core.matrix.wait_ready(run["variant"])
            if any(row.get("ErrMsg") for row in ready["backends"]):
                raise RuntimeError("Backend heartbeat reports error")
        evidence = core.matrix.manifest(run["variant"], ready)
        expected = core.matrix.BASELINE if run["variant"] == "A-baseline" else core.iso.FEATURE
        if evidence["source_revision"] != expected:
            raise RuntimeError("Unexpected source revision")
        placements = core.iso.placement(ready)
        for older in root.glob("*-build.json"):
            old = json.loads(older.read_text())
            if core.iso.placement(old["readiness"]) != placements:
                raise RuntimeError("Placement changed")
            for node in core.runtime.NODES:
                if (old["nodes"][node]["pid"], old["nodes"][node]["start_ticks"]) == (
                        evidence["nodes"][node]["pid"], evidence["nodes"][node]["start_ticks"]):
                    raise RuntimeError("Reused server process")
        core.iso.write_json(root / (args.name + "-build.json"), evidence)
        return
    if args.stage in ("stop", "final"):
        core.matrix.stop_all()
        core.matrix.ensure_restored()
        original = json.loads((root / "storage-before.json").read_text())
        unchanged = core.iso.storage_files() == original
        record = {"utc": core.iso.utc(), "nodes": {n: core.runtime.live_record(n) for n in core.runtime.NODES},
                  "physical_files_unchanged": unchanged,
                  "memory_events": Path("/sys/fs/cgroup/memory.events").read_text()}
        if args.stage == "final":
            core.iso.write_json(root / "final-state.json", record)
        if not unchanged:
            raise RuntimeError("Data changed; evidence preserved, no restore attempted")


def host(args):
    folder = Path(__file__).resolve().parent
    state = json.loads(capture(["docker", "inspect", SERVER]))[0]
    settings = state["HostConfig"]
    if settings["NanoCpus"] != 6000000000 or settings["CpusetCpus"] != "":
        raise RuntimeError("Unexpected initial server CPU configuration; refuse overwrite")
    if capture(["docker", "ps", "-a", "--filter", "name=^/" + CLIENT + "$", "--format", "{{.ID}}"]).strip():
        raise RuntimeError("Client container already exists; inspect before reuse")
    with (folder / "split-environment-before.json").open("x") as target:
        json.dump(state, target, indent=2)
    stage = ["docker", "exec", SERVER, PYTHON, SCRIPT, "server"]
    # Refuse concurrent benchmark BEFORE any affinity/environment change.
    command(stage + ["--stage", "init"])
    created, updated, completed = False, False, []
    reference_plan = None
    try:
        command(["docker", "update", "--cpuset-cpus", "0-5", SERVER])
        updated = True
        command(["docker", "volume", "create", "--label", "purpose=qct-client-performance",
                 "--label", "architecture=arm64", VOLUME])
        command(["docker", "run", "-d", "--name", CLIENT, "--label", "purpose=qct-client-performance",
                 "--cpus", "2", "--cpuset-cpus", "6-7", "--memory", "2g", "--pids-limit", "128",
                 "--network", "container:" + SERVER, "--pid", "container:" + SERVER,
                 "--mount", "type=volume,src=sr-query-corruption-4.0-build-cache-arm64,dst=/query-corruption-workspace,readonly",
                 "--mount", "type=volume,src=" + VOLUME + ",dst=/qct-client-workspace",
                 "--env", "PYTHONDONTWRITEBYTECODE=1", IMAGE, "sleep", "infinity"])
        created = True
        command(["docker", "exec", CLIENT, "mkdir", "-p", "/qct-client-workspace/scripts",
                 "/qct-client-workspace/results/" + SERIES])
        for name in ("client_perf.py", "split_client.py"):
            command(["docker", "cp", str(folder / name), CLIENT + ":/qct-client-workspace/scripts/" + name])
        details = {"server": json.loads(capture(["docker", "inspect", SERVER]))[0],
                   "client": json.loads(capture(["docker", "inspect", CLIENT]))[0],
                   "scripts": {name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
                               for name in ("client_perf.py", "split_client.py", "split_suite.py")}}
        with (folder / "split-environment-active.json").open("x") as target:
            json.dump(details, target, indent=2)
        for index, run in enumerate(schedule(), 1):
            print(json.dumps({"event": "run_start", "number": index, "total": len(schedule()), **run}), flush=True)
            command(stage + ["--stage", "start", "--name", run["name"]])
            destination = "/qct-client-workspace/results/" + SERIES + "/" + run["name"] + ".jsonl"
            command(["docker", "exec", CLIENT, PYTHON, "-u", "/qct-client-workspace/scripts/split_client.py",
                     "--variant", run["variant"], "--client-mode", run["client_mode"],
                     "--manifest", LOGROOT + "/" + SERIES + "/" + run["name"] + "-build.json",
                     "--output", destination], timeout=900)
            # Check the plan between runs, outside measured intervals.
            first = capture(["docker", "exec", CLIENT, "head", "-n", "1", destination])
            last = capture(["docker", "exec", CLIENT, "tail", "-n", "1", destination])
            meta, summary = json.loads(first), json.loads(last)
            if summary["type"] != "summary" or summary["samples"] != 10000:
                raise RuntimeError("Incomplete result")
            plan = meta["plans"]["detail_page"]
            reference_plan = reference_plan or plan
            if plan != reference_plan:
                raise RuntimeError("Plan changed")
            completed.append({**run, "qps": summary["measured_queries_per_second"],
                              "measured": summary["measured"], "warmup_samples": summary["warmup_samples"]})
            print(json.dumps({"event": "run_complete", "number": index, **completed[-1]}), flush=True)
            command(stage + ["--stage", "stop"])
        with (folder / "split-completed.json").open("x") as target:
            json.dump(completed, target, indent=2)
    finally:
        # On client failure stop its own container first, including owned workers;
        # server data/build assets remain mounted read-only from the client.
        if created:
            command(["docker", "stop", "--time", "15", CLIENT])
        try:
            command(stage + ["--stage", "final"])
        finally:
            if updated:
                command(["docker", "update", "--cpuset-cpus", settings["CpusetCpus"], SERVER])
            final = json.loads(capture(["docker", "inspect", SERVER]))[0]
            with (folder / "split-environment-after.json").open("x") as target:
                json.dump({"server": final, "completed_groups": len(completed),
                           "restored_cpuset": final["HostConfig"]["CpusetCpus"] == settings["CpusetCpus"]}, target, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("host", "server", "test"))
    parser.add_argument("--stage", choices=("init", "start", "stop", "final"))
    parser.add_argument("--name")
    args = parser.parse_args()
    if args.mode == "host":
        host(args)
    elif args.mode == "server":
        server_stage(args)
    else:
        rows = schedule()
        assert len(rows) == len({r["name"] for r in rows}) == 22
        assert [r["client_mode"] for r in rows[:4]] == ["threads", "processes", "processes", "threads"]
        for pos in (1, 2, 3):
            for variant in ("A-baseline", "B-off", "C-on"):
                assert sum(r["phase"] == "abc" and r["position"] == pos and r["variant"] == variant for r in rows) == 2
        print("Split schedule tests passed: 22 unique runs; TP/PT controls; balanced ABC positions")


if __name__ == "__main__":
    main()
