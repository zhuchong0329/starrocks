#!/usr/bin/env python3
"""Read-only audit of the QCT client-architecture control and subsequent ABC runs."""
import argparse
from collections import defaultdict
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


def kv(value):
    return {key: int(number) for key, number in (line.split() for line in value.splitlines())}


def quantile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def metrics(rows):
    result = {}
    for field in ("first_row_ms", "complete_ms", "after_first_ms"):
        values = [r["complete_ms"] - r["first_row_ms"] if field == "after_first_ms" else r[field] for r in rows]
        result[field] = {"p50": statistics.median(values), "p95": quantile(values, .95),
                         "p99": quantile(values, .99), "mean": statistics.mean(values)}
    return result


def comparison(ratios):
    logs = [math.log(r) for r in ratios]
    rng = random.Random(20260922)
    boot = [100 * math.expm1(statistics.mean(rng.choices(logs, k=len(logs)))) for _ in range(10000)]
    return {"paired_blocks": len(ratios), "geometric_mean_change_percent": 100 * math.expm1(statistics.mean(logs)),
            "per_block_change_percent": [100 * (r - 1) for r in ratios],
            "exploratory_95_percent": [quantile(boot, .025), quantile(boot, .975)],
            "limitation": "Few shared-VM blocks; not independent SQL samples, an equivalence test, or causal GIL profile"}


def audit(root, partial, results_root=None):
    design = json.loads((root / "design.json").read_text())
    results, identities, client_ids, plans, placements, hashes = [], set(), set(), set(), set(), defaultdict(set)
    for item in design["schedule"]:
        path = (results_root or root) / (item["name"] + ".jsonl")
        if not path.exists() and partial:
            continue
        with path.open() as source:
            records = [json.loads(line) for line in source]
        if (not records or records[-1].get("type") != "summary") and partial:
            continue
        meta, summary = records[0], records[-1]
        assert summary["type"] == "summary"
        manifest_path = root / (item["name"] + "-build.json")
        assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == meta["build_manifest_sha256"]
        manifest = json.loads(manifest_path.read_text())
        assert meta["client_mode"] == item["client_mode"] and meta["variant"] == item["variant"]
        assert meta["harness_sha256"] == design["harness_sha256"]
        assert meta["client_pid"] not in client_ids
        client_ids.add(meta["client_pid"])
        for node, launch in manifest["nodes"].items():
            identity = node, launch["pid"], launch["start_ticks"]
            assert identity not in identities
            identities.add(identity)
        for path_name, value in manifest["files"].items():
            if path_name.endswith(("starrocks_be", "starrocks-fe.jar")):
                hashes[item["variant"] + ":" + Path(path_name).name].add(value["sha256"])
        plans.add(meta["plans"][design["case"]])
        fields = ("TabletId", "BackendId", "Version", "SchemaHash")
        mapping = {table: sorted(tuple(str(r.get(f)) for f in fields) for r in rows)
                   for table, rows in manifest["readiness"]["fixture_tablets"].items()}
        placements.add(json.dumps(mapping, sort_keys=True))
        rows = [r for r in records if r["type"] == "sample"]
        warm = [r for r in records if r["type"] == "warmup"]
        assert len(rows) == summary["samples"] == design["samples_per_run"]
        assert len(warm) == summary["warmup_samples"]
        assert all(r["rows"] == 1000 and r["warning_count"] == 0 and r["case"] == design["case"] for r in rows + warm)
        for worker in range(4):
            owned = sorted((r for r in rows if r["worker"] == worker), key=lambda r: r["round"])
            assert [r["round"] for r in owned] == list(range(2500))
            assert all(a["completed_ns"] <= b["started_ns"] for a, b in zip(owned, owned[1:]))
        elapsed = (max(r["completed_ns"] for r in rows) - min(r["started_ns"] for r in rows)) / 1e9
        qps = len(rows) / elapsed
        assert abs(qps - summary["measured_queries_per_second"]) < 1e-8
        stats = metrics(rows)
        assert stats["complete_ms"]["p99"] == summary["measured"][design["case"]]["complete_ms"]["p99"]
        before, after = summary["server_resources_before"], summary["server_resources_after"]
        duration = (datetime.fromisoformat(after["utc"]) - datetime.fromisoformat(before["utc"])).total_seconds()
        expected_workers = 1 if item["client_mode"] == "threads" else 4
        assert len(before["worker_processes"]) == len(after["worker_processes"]) == expected_workers
        assert set(before["worker_processes"]) == set(after["worker_processes"])
        server_cpu = 0
        for node, initial in before["nodes"].items():
            final = after["nodes"][node]
            assert initial["pid"] == final["pid"] and initial["start_ticks"] == final["start_ticks"]
            server_cpu += final["cpu_seconds"] - initial["cpu_seconds"]
        client_cpu = 0
        for pid, initial in before["worker_processes"].items():
            final = after["worker_processes"][pid]
            assert final["start_ticks"] == initial["start_ticks"]
            client_cpu += final["cpu_seconds"] - initial["cpu_seconds"]
        active_cpu = sum(w["thread_cpu_seconds"] for w in summary["workers"])
        cb, ca = kv(before.get("server_cgroup", before["cgroup"])["cpu.stat"]), kv(after.get("server_cgroup", after["cgroup"])["cpu.stat"])
        client_cb, client_ca = kv(before["cgroup"]["cpu.stat"]), kv(after["cgroup"]["cpu.stat"])
        if "server_cgroup" in before:
            for point in (before, after):
                assert point["server_cgroup"]["cpu.max"] == "600000 100000"
                assert point["server_cgroup"]["cpuset.cpus.effective"] == "0-5"
                assert point["cgroup"]["cpu.max"] == "200000 100000"
                assert point["client_cpuset"] == "6-7"
        assert kv(before["cgroup"]["memory.events"])["oom_kill"] == kv(after["cgroup"]["memory.events"])["oom_kill"]
        ordered = sorted(rows, key=lambda r: r["started_ns"])
        results.append({**item, "qps": qps, "samples": len(rows), "warmup_samples": len(warm),
            "metrics": stats, "elapsed_seconds": elapsed, "resource_interval_seconds": duration,
            # UTC can slew in the development VM. QPS and approximate core use
            # share the query window's monotonic clock, not UTC subtraction.
            "client_cpu_cores": client_cpu / elapsed, "server_cpu_cores": server_cpu / elapsed,
            "active_worker_cpu_cores": active_cpu / elapsed,
            "client_cpu_ms_per_query": client_cpu * 1000 / len(rows),
            "worker_active_cpu_ms_per_query": active_cpu * 1000 / len(rows),
            "server_cpu_ms_per_query": server_cpu * 1000 / len(rows),
            "throttled_seconds": (ca["throttled_usec"] - cb["throttled_usec"]) / 1e6,
            "throttled_periods": ca["nr_throttled"] - cb["nr_throttled"],
            "total_periods": ca["nr_periods"] - cb["nr_periods"],
            "client_throttled_seconds": (client_ca["throttled_usec"] - client_cb["throttled_usec"]) / 1e6,
            "cpu_isolated": "server_cgroup" in before,
            "worker_pids": [w["pid"] for w in summary["workers"]],
            "python": meta["python"], "pymysql": meta["pymysql"],
            "first_half": metrics(ordered[:len(rows)//2]), "second_half": metrics(ordered[len(rows)//2:])})
    assert len(plans) <= 1 and len(placements) <= 1
    assert all(len(values) == 1 for values in hashes.values())
    for name in ("starrocks_be", "starrocks-fe.jar"):
        if "C-on:" + name in hashes:
            assert hashes["C-on:" + name] == hashes["B-off:" + name]
    blocks = defaultdict(dict)
    for row in results:
        phase = row.get("phase", "client" if design["experiment"] == "client" else "abc")
        key = row["client_mode"] if phase == "client" else row["variant"]
        blocks[(phase, row["block"])][key] = row
    pairs = []
    if design["experiment"] in ("client", "split"):
        pairs += [("client", "processes", "threads")]
    if design["experiment"] in ("abc", "split"):
        pairs += [("abc", "B-off", "A-baseline"), ("abc", "C-on", "A-baseline"), ("abc", "C-on", "B-off")]
    comparisons = {}
    for phase, numerator, denominator in pairs:
        complete = [b for (p, _), b in blocks.items() if p == phase and numerator in b and denominator in b]
        if not complete:
            continue
        functions = {"qps": lambda r: r["qps"], "p50": lambda r: r["metrics"]["complete_ms"]["p50"],
                     "p99": lambda r: r["metrics"]["complete_ms"]["p99"],
                     "client_cpu_ms_per_query": lambda r: r["client_cpu_ms_per_query"],
                     "server_cpu_ms_per_query": lambda r: r["server_cpu_ms_per_query"]}
        comparisons[numerator + "/" + denominator] = {
            key: comparison([fn(b[numerator]) / fn(b[denominator]) for b in complete]) for key, fn in functions.items()}
    final = None
    if not partial:
        assert len(results) == len(design["schedule"])
        final = json.loads((root / "final-state.json").read_text())
        assert final["physical_files_unchanged"] and all(v is None for v in final["nodes"].values())
        assert kv(final["memory_events"])["oom_kill"] == 2
    return {"groups": len(results), "samples": sum(r["samples"] for r in results),
            "unique_server_identities": len(identities), "unique_client_parents": len(client_ids),
            "plans_identical": len(plans) == 1, "placements_identical": len(placements) == 1,
            "artifact_hashes": {k: sorted(v) for k, v in hashes.items()},
            "comparisons": comparisons, "runs": results, "final_state": final}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--partial", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--results-root", type=Path)
    args = parser.parse_args()
    value = audit(args.root, args.partial, args.results_root)
    if args.output:
        with args.output.open("x") as target:
            json.dump(value, target, indent=2)
    else:
        print(json.dumps(value, indent=2))
