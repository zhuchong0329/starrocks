#!/usr/bin/env python3
"""Read-only audit and independent-run comparisons for isolated QCT samples."""
import argparse
from collections import defaultdict
import datetime
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


def percentile(values, fraction):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)]


def kv(text):
    return {key: int(value) for key, value in (line.split() for line in text.splitlines())}


def prometheus(text):
    result = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            key, value = line.rsplit(" ", 1)
            try:
                result[key] = float(value)
            except ValueError:
                pass
    return result


def metrics(rows):
    result = {}
    for label in ("first_row_ms", "complete_ms", "after_first_ms"):
        values = ([r["complete_ms"] - r["first_row_ms"] for r in rows] if label == "after_first_ms"
                  else [r[label] for r in rows])
        result[label] = {"p50": statistics.median(values), "p95": percentile(values, .95),
                         "p99": percentile(values, .99), "mean": statistics.mean(values)}
    return result


def paired_interval(ratios):
    logs = [math.log(value) for value in ratios]
    rng = random.Random(20260921)
    boot = [math.exp(statistics.mean(rng.choices(logs, k=len(logs)))) - 1 for _ in range(10000)]
    return {"blocks": len(ratios), "geometric_mean_change_percent": (math.exp(statistics.mean(logs)) - 1) * 100,
            "bootstrap_95_percent": [percentile(boot, .025) * 100, percentile(boot, .975) * 100],
            "per_block_change_percent": [(value - 1) * 100 for value in ratios],
            "note": "Exploratory resampling of six complete blocks, not individual SQL samples"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--partial", action="store_true")
    args = parser.parse_args()
    design = json.loads((args.root / "design.json").read_text())
    results, all_plans, identities, source_hashes, blocks = [], set(), set(), defaultdict(set), defaultdict(dict)
    for name, letter, block, position in design["schedule"]:
        path = args.root / (name + ".jsonl")
        if not path.exists():
            if args.partial:
                continue
            raise ValueError("Missing " + name)
        with path.open() as source:
            records = [json.loads(line) for line in source]
        if records[-1].get("type") != "summary":
            if args.partial:
                continue
            raise ValueError("Incomplete " + name)
        meta, summary = records[0], records[-1]
        manifest_path = args.root / (name + "-build.json")
        assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == meta["build_manifest_sha256"]
        manifest = json.loads(manifest_path.read_text())
        for node, launch in manifest["nodes"].items():
            identity = (node, launch["pid"], launch["start_ticks"])
            assert identity not in identities, "Server not restarted"
            identities.add(identity)
        for key, value in manifest["files"].items():
            if key.endswith(("starrocks_be", "starrocks-fe.jar")):
                source_hashes[letter + ":" + Path(key).name].add(value["sha256"])
        all_plans.add(meta["plans"][design["case"]])
        rows = [r for r in records if r["type"] == "sample"]
        assert len(rows) == design["samples_per_run"]
        assert all(r["warning_count"] == 0 and r["rows"] == 1000 and r["case"] == design["case"] for r in rows)
        elapsed = (max(r["completed_ns"] for r in rows) - min(r["started_ns"] for r in rows)) / 1e9
        qps = len(rows) / elapsed
        assert abs(qps - summary["measured_queries_per_second"]) < 1e-6
        calculated = metrics(rows)
        for field in ("first_row_ms", "complete_ms"):
            assert abs(calculated[field]["p99"] - summary["measured"][design["case"]][field]["p99"]) < 1e-8
        before, after = summary["server_resources_before"], summary["server_resources_after"]
        duration = (datetime.datetime.fromisoformat(after["utc"]) - datetime.datetime.fromisoformat(before["utc"])).total_seconds()
        cpu = {node: after["nodes"][node]["cpu_seconds"] - value["cpu_seconds"] for node, value in before["nodes"].items()}
        cpu["client"] = after["client_process"]["cpu_seconds"] - before["client_process"]["cpu_seconds"]
        cb, ca = kv(before["cgroup"]["cpu.stat"]), kv(after["cgroup"]["cpu.stat"])
        mb, ma = prometheus(before["19300"]), prometheus(after["19300"])
        gc = {key: value - mb[key] for key, value in ma.items() if key in mb and "gc" in key.lower()}
        sorted_rows = sorted(rows, key=lambda row: row["started_ns"])
        result = {"name": name, "variant": letter, "block": block, "position": position,
                  "samples": len(rows), "warmup_samples": summary["warmup_samples"], "qps": qps,
                  "metrics": calculated, "cpu_cores": {key: value / duration for key, value in cpu.items()},
                  "server_cpu_ms_per_query": sum(value for key, value in cpu.items() if key != "client") * 1000 / len(rows),
                  "client_cpu_ms_per_query": cpu["client"] * 1000 / len(rows),
                  "throttled_seconds": (ca["throttled_usec"] - cb["throttled_usec"]) / 1e6,
                  "gc_delta": gc,
                  "warmup_buckets": summary["warmup_buckets"],
                  "measurement_first_half": metrics(sorted_rows[:len(rows)//2]),
                  "measurement_second_half": metrics(sorted_rows[len(rows)//2:])}
        results.append(result)
        if block is not None:
            blocks[block][letter] = result
    assert len(all_plans) <= 1
    for hashes in source_hashes.values():
        assert len(hashes) == 1
    if "B:starrocks_be" in source_hashes and "C:starrocks_be" in source_hashes:
        for name in ("starrocks_be", "starrocks-fe.jar"):
            assert source_hashes["B:" + name] == source_hashes["C:" + name]
    comparisons = {}
    complete_blocks = [values for _, values in sorted(blocks.items()) if set(values) == set("ABC")]
    if complete_blocks:
        for numerator, denominator in (("B", "A"), ("C", "A"), ("C", "B")):
            columns = {"qps": lambda r: r["qps"], "server_cpu_ms_per_query": lambda r:r["server_cpu_ms_per_query"],
                       "p99": lambda r: r["metrics"]["complete_ms"]["p99"],
                       "p50": lambda r: r["metrics"]["complete_ms"]["p50"],
                       "first_p99": lambda r: r["metrics"]["first_row_ms"]["p99"]}
            comparisons[numerator + "/" + denominator] = {
                label: paired_interval([fn(b[numerator]) / fn(b[denominator]) for b in complete_blocks])
                for label, fn in columns.items()}
    final = None
    if not args.partial:
        assert len(results) == len(design["schedule"]) == 22
        final = json.loads((args.root / "final-state.json").read_text())
        assert final["physical_files_unchanged"] and all(value is None for value in final["nodes"].values())
        assert kv(final["memory_events"])["oom_kill"] == 2
    print(json.dumps({"groups": len(results), "samples": sum(r["samples"] for r in results),
                      "plans_identical": len(all_plans) == 1, "unique_server_identities": len(identities),
                      "artifact_hashes": {key: sorted(value) for key, value in source_hashes.items()},
                      "comparisons": comparisons, "runs": results, "final_state": final}, indent=2))


if __name__ == "__main__":
    main()
