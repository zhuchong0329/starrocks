#!/usr/bin/env python3
"""QCT single-scenario experiment. No production changes, data reset or cache purge.

Reuses the committed query execution/timing functions; adds process-per-cell
orchestration, fixed-duration warmup and balanced independent repetitions.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import unittest

TOOLS = Path(os.environ.get("QCT_TOOLS", "/query-corruption-workspace/src/tools/query_corruption"))
sys.path.insert(0, str(TOOLS))
import benchmark
import matrix
import runtime

VARIANTS = {"A": "A-baseline", "B": "B-off", "C": "C-on"}
ORDERS = ("ABC", "ACB", "BAC", "BCA", "CAB", "CBA")
FEATURE = "46ca14950dc76004c5df355ce2c975a77c50041a"


def utc():
    return datetime.now(timezone.utc).isoformat()


def emit(event, **values):
    print(json.dumps({"utc": utc(), "event": event, **values}), flush=True)


def write_json(path, value):
    with path.open("x") as out:
        json.dump(value, out, indent=2)


def schedule():
    runs = [("aa-before-1", "A", None, None), ("aa-before-2", "A", None, None)]
    for block, order in enumerate(ORDERS, 1):
        runs += [(f"block-{block}-{order}-pos-{position}-{letter}", letter, block, position)
                 for position, letter in enumerate(order, 1)]
    return runs + [("aa-after-1", "A", None, None), ("aa-after-2", "A", None, None)]


def placement(readiness):
    fields = ("TabletId", "BackendId", "Version", "SchemaHash")
    return {name: sorted([tuple(str(row.get(field)) for field in fields) for row in rows])
            for name, rows in readiness["fixture_tablets"].items()}


def storage_files():
    root = runtime.checked_root()
    files = {}
    for node in runtime.NODES[1:]:
        for path in sorted((root / node / "storage").rglob("*.dat")):
            if path.is_symlink() or path.resolve() != path:
                raise RuntimeError("Unexpected storage symlink")
            files[str(path.relative_to(root))] = {"bytes": path.stat().st_size, "sha256": matrix.sha256(path)}
    if not files:
        raise RuntimeError("No fixture segment files")
    return files


def extra_summary(samples):
    summary = benchmark.summarize(samples)
    for name in summary:
        values = [r["complete_ms"] - r["first_row_ms"] for r in samples if r["case"] == name]
        summary[name]["after_first_ms"] = {
            "p50": benchmark.percentile(values, 50), "p95": benchmark.percentile(values, 95),
            "p99": benchmark.percentile(values, 99)}
    return summary


def client(args):
    output = args.output.absolute()
    root = (runtime.WORKSPACE / "logs/QCT-007").resolve()
    if not output.is_relative_to(root) or output.resolve() != output or output.exists():
        raise ValueError("Require a new, non-symlink evidence path under QCT-007")
    selection = benchmark.validate_variant(args.variant)
    metadata = {"type": "metadata", "started_utc": utc(), "variant": args.variant,
                "protocol": "mysql", "runtime_selection": selection,
                "settings": {"case": args.case, "concurrency": 4, "cache": False, "dop": 2,
                             "warmup_seconds": 60, "repetitions_per_worker": 2500},
                "client_pid": os.getpid(), "build_manifest": str(args.manifest),
                "build_manifest_sha256": matrix.sha256(args.manifest),
                "harness_sha256": matrix.sha256(Path(__file__)), "plans": {}}
    with benchmark.connect() as connection:
        benchmark.configure(connection, False, 2)
        with connection.cursor() as cursor:
            cursor.execute("show variables")
            metadata["control_connection_session_variables"] = dict(cursor.fetchall())
            cursor.execute("select current_version()")
            metadata["server_version"] = list(cursor.fetchall())
            cursor.execute("explain " + benchmark.CASES[args.case])
            metadata["plans"][args.case] = "\n".join(str(row[0]) for row in cursor.fetchall())
    shared = {}

    def warm_start():
        shared["warmup_started_ns"] = time.perf_counter_ns()
        shared["deadline"] = time.monotonic() + 60

    def measure_start():
        shared["measurement_resources_before"] = benchmark.server_snapshot()
        emit("measurement_start", output=output.name)

    start_barrier = threading.Barrier(4, action=warm_start)
    measure_barrier = threading.Barrier(4, action=measure_start)

    def worker(worker_id):
        warm, measured = [], []
        try:
            with benchmark.connect() as connection:
                benchmark.configure(connection, False, 2)
                start_barrier.wait(timeout=120)
                while time.monotonic() < shared["deadline"]:
                    sample = benchmark.execute(connection, args.case)
                    benchmark.check_healthy_sample(sample)
                    sample["worker"] = worker_id
                    warm.append(sample)
                measure_barrier.wait(timeout=120)
                for round_id in range(2500):
                    sample = benchmark.execute(connection, args.case)
                    benchmark.check_healthy_sample(sample)
                    sample.update(worker=worker_id, round=round_id)
                    measured.append(sample)
        except BaseException:
            start_barrier.abort()
            measure_barrier.abort()
            raise
        return warm, measured

    with output.open("x") as destination:
        metadata["server_resources_before_warmup"] = benchmark.server_snapshot()
        destination.write(json.dumps(metadata) + "\n")
        destination.flush()
        emit("warmup_start", output=output.name, seconds=60)
        with ThreadPoolExecutor(max_workers=4) as pool:
            groups = list(pool.map(worker, range(4)))
        resources_after = benchmark.server_snapshot()
        warm = [sample for group in groups for sample in group[0]]
        samples = [sample for group in groups for sample in group[1]]
        result = {"type": "summary", "finished_utc": utc(), "samples": len(samples),
                  "measured": extra_summary(samples), **benchmark.measured_throughput(samples),
                  "server_resources_before": shared["measurement_resources_before"],
                  "server_resources_after": resources_after,
                  "warmup_samples": len(warm), "warmup_buckets": {}}
        for bucket in range(6):
            selected = [r for r in warm if bucket * 10 <=
                        (r["started_ns"] - shared["warmup_started_ns"]) / 1e9 < (bucket + 1) * 10]
            result["warmup_buckets"][str(bucket)] = extra_summary(selected) if selected else {}
        for kind, rows in (("warmup", warm), ("sample", samples)):
            for sample in rows:
                destination.write(json.dumps({"type": kind, **sample}) + "\n")
        destination.write(json.dumps(result) + "\n")
    emit("client_complete", output=output.name, samples=len(samples),
         qps=result["measured_queries_per_second"], measured=result["measured"])


def suite(args):
    root = runtime.WORKSPACE / "logs/QCT-007" / args.series
    if root.resolve() != root:
        raise ValueError("Unexpected evidence symlink")
    root.mkdir(exist_ok=False)
    matrix.ensure_idle_builds()
    matrix.ensure_restored()
    if any(runtime.live_record(node) for node in runtime.NODES):
        raise RuntimeError("Unexpected active QCT nodes; not taking over another run")
    original = storage_files()
    write_json(root / "storage-before.json", original)
    runs = schedule()
    write_json(root / "design.json", {"started_utc": utc(), "case": args.case,
               "warmup_seconds": 60, "samples_per_run": 10000, "concurrency": 4,
               "protocol": "mysql", "query_cache": False, "storage_page_cache": True,
               "orders": ORDERS, "schedule": runs, "baseline": matrix.BASELINE, "feature": FEATURE,
               "harness_sha256": matrix.sha256(Path(__file__)),
               "reset": "Fresh FE/3BE/client processes per run; same verified physical data; no OS cache purge",
               "limitations": "Shared VM; client shares server CPU quota; one scenario; six independent ABC blocks"})
    reference_placement, reference_plan, previous_identities = None, None, set()
    completed = []
    try:
        for number, (name, letter, block, position) in enumerate(runs, 1):
            variant = VARIANTS[letter]
            emit("run_start", number=number, total=len(runs), name=name, variant=variant)
            matrix.ensure_idle_builds()
            matrix.stop_all()
            if storage_files() != original:
                raise RuntimeError("Physical fixture changed before run")
            runtime.prepare(variant, page_cache=True)
            for node in runtime.NODES:
                runtime.start(node)
            readiness = matrix.wait_ready(variant, timeout=90)
            # Do not treat a single persisted Alive value as proof of a stable new BE.
            # Startup/settling is outside all measured and warmup query intervals.
            for _ in range(3):
                time.sleep(5)
                readiness = matrix.wait_ready(variant)
                if any(row.get("ErrMsg") for row in readiness["backends"]):
                    raise RuntimeError("Backend heartbeat still reports an error")
            actual_placement = placement(readiness)
            if reference_placement is None:
                reference_placement = actual_placement
            if actual_placement != reference_placement:
                raise RuntimeError("Tablet placement/version changed")
            evidence = matrix.manifest(variant, readiness)
            expected = matrix.BASELINE if letter == "A" else FEATURE
            if evidence["source_revision"] != expected:
                raise RuntimeError("Unexpected production revision")
            identities = {(node, launch["pid"], launch["start_ticks"])
                          for node, launch in evidence["nodes"].items()}
            if identities & previous_identities:
                raise RuntimeError("Server process reused between runs")
            previous_identities |= identities
            manifest_path = root / (name + "-build.json")
            write_json(manifest_path, evidence)
            output = root / (name + ".jsonl")
            command = [sys.executable, str(Path(__file__)), "client", "--variant", variant,
                       "--manifest", str(manifest_path), "--output", str(output), "--case", args.case]
            with (root / (name + ".log")).open("x") as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
            with output.open() as source:
                meta = json.loads(next(source))
                last = None
                for line in source:
                    last = json.loads(line)
            if last is None or last.get("type") != "summary" or last["samples"] != 10000:
                raise RuntimeError("Incomplete measurement")
            plan = meta["plans"][args.case]
            if reference_plan is None:
                reference_plan = plan
            if plan != reference_plan:
                raise RuntimeError("Execution plan changed")
            completed.append({"name": name, "variant": variant, "block": block, "position": position,
                              "qps": last["measured_queries_per_second"], "measured": last["measured"],
                              "warmup_samples": last["warmup_samples"]})
            emit("run_complete", number=number, total=len(runs), **completed[-1])
            matrix.stop_all()
            if storage_files() != original:
                raise RuntimeError("Physical fixture changed after run")
        write_json(root / "completed.json", completed)
        emit("suite_complete", groups=len(completed), samples=len(completed) * 10000)
    finally:
        matrix.stop_all()
        matrix.ensure_restored()
        final = storage_files()
        write_json(root / "final-state.json", {"utc": utc(), "completed_groups": len(completed),
                   "nodes": {node: runtime.live_record(node) for node in runtime.NODES},
                   "physical_files_unchanged": final == original,
                   "memory_events": Path("/sys/fs/cgroup/memory.events").read_text()})
        if final != original:
            raise RuntimeError("Physical data changed; retained evidence, did not overwrite files")


class HarnessTests(unittest.TestCase):
    def test_schedule_is_balanced(self):
        rows = schedule()
        self.assertEqual(len(rows), 22)
        main = [r for r in rows if r[2] is not None]
        self.assertEqual(Counter(r[1] for r in main), {"A": 6, "B": 6, "C": 6})
        for pos in (1, 2, 3):
            self.assertEqual(Counter(r[1] for r in main if r[3] == pos), {"A": 2, "B": 2, "C": 2})
        self.assertEqual(len({r[0] for r in rows}), 22)

    def test_aa_runs_at_both_ends(self):
        self.assertEqual([r[1] for r in schedule()[:2] + schedule()[-2:]], list("AAAA"))

    def test_after_first_uses_paired_samples(self):
        rows = [{"case": "detail_page", "warning_count": 0, "first_row_ms": a, "complete_ms": b}
                for a, b in ((100, 101), (1, 51))]
        self.assertEqual(extra_summary(rows)["detail_page"]["after_first_ms"]["p99"], 50)

    def test_placement_ignores_heartbeat_but_not_version(self):
        data = {"fixture_tablets": {"events": [{"TabletId": 1, "BackendId": 2,
                                                "Version": 3, "LastCheckTime": 4}]}}
        before = placement(data)
        data["fixture_tablets"]["events"][0]["LastCheckTime"] = 5
        self.assertEqual(placement(data), before)
        data["fixture_tablets"]["events"][0]["Version"] = 4
        self.assertNotEqual(placement(data), before)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("suite", "client", "test"))
    parser.add_argument("--series", default="isolated-detail-20260921-r1")
    parser.add_argument("--case", choices=benchmark.CASES, default="detail_page")
    parser.add_argument("--variant", choices=VARIANTS.values())
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", args.series):
        parser.error("Invalid series name")
    if args.mode == "test":
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(HarnessTests))
        raise SystemExit(not result.wasSuccessful())
    elif args.mode == "client":
        client(args)
    else:
        suite(args)


if __name__ == "__main__":
    main()
