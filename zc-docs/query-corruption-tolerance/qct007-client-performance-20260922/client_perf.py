#!/usr/bin/env python3
"""QCT client architecture control: identical SQL, four connections, full consumption.

Only experimental orchestration; reuses committed benchmark.execute unchanged.
Never mutates production code, fixture data, VM settings, or another container.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import unittest

TOOLS = Path(os.environ.get("QCT_TOOLS", "/query-corruption-workspace/src/tools/query_corruption"))
sys.path.insert(0, str(TOOLS))
import benchmark
import matrix
import runtime

HISTORICAL = Path(os.environ.get("QCT_ISOLATED_SCRIPT",
    "/query-corruption-workspace/logs/QCT-007/isolated-perf-20260921.py"))
spec = importlib.util.spec_from_file_location("isolated_helpers", HISTORICAL)
iso = importlib.util.module_from_spec(spec)
spec.loader.exec_module(iso)
ROOT = runtime.WORKSPACE / "logs/QCT-007"
THREADS = "threads"
PROCESSES = "processes"


def schedule(experiment):
    if experiment == "client":
        orders = ((THREADS, PROCESSES), (PROCESSES, THREADS),
                  (PROCESSES, THREADS), (THREADS, PROCESSES))
        return [{"name": f"pair-{pair}-pos-{pos}-{mode}", "variant": "A-baseline",
                 "client_mode": mode, "block": pair, "position": pos}
                for pair, order in enumerate(orders, 1) for pos, mode in enumerate(order, 1)]
    return [{"name": f"block-{block}-{order}-pos-{pos}-{letter}",
             "variant": iso.VARIANTS[letter], "client_mode": PROCESSES,
             "block": block, "position": pos}
            for block, order in enumerate(iso.ORDERS, 1) for pos, letter in enumerate(order, 1)]


def checked_destination(path):
    path = path.absolute()
    if not path.is_relative_to(ROOT) or path.resolve() != path or path.exists():
        raise ValueError("Require a new non-symlink evidence path under QCT-007")
    return path


def worker(index, output, pids, deadline, barriers, repetitions, fake=False):
    warm, measured = [], []
    connection = None
    try:
        if not fake:
            connection = benchmark.connect()
            benchmark.configure(connection, False, 2)
        pids[index] = os.getpid()
        barriers[5].wait(timeout=120)
        barriers[0].wait(timeout=120)
        while time.monotonic() < deadline.value:
            sample = benchmark.execute(connection, "detail_page")
            benchmark.check_healthy_sample(sample)
            sample["worker"] = index
            warm.append(sample)
        barriers[1].wait(timeout=120)
        barriers[2].wait(timeout=120)
        cpu_before = time.thread_time_ns()
        for iteration in range(repetitions):
            if fake:
                now = time.perf_counter_ns()
                sample = {"case": "detail_page", "rows": 1000, "warning_count": 0,
                          "started_ns": now, "completed_ns": now + 1000,
                          "first_row_ms": 0.0005, "complete_ms": 0.001}
            else:
                sample = benchmark.execute(connection, "detail_page")
            benchmark.check_healthy_sample(sample)
            sample.update(worker=index, round=iteration)
            measured.append(sample)
        cpu_seconds = (time.thread_time_ns() - cpu_before) / 1e9
        barriers[3].wait(timeout=600)
        barriers[4].wait(timeout=120)
        # Write only AFTER all measured queries and resource snapshots complete.
        with output.open("x") as target:
            target.write(json.dumps({"type": "worker", "worker": index, "pid": os.getpid(),
                                    "thread_cpu_seconds": cpu_seconds}) + "\n")
            for kind, samples in (("warmup", warm), ("sample", measured)):
                for sample in samples:
                    target.write(json.dumps({"type": kind, **sample}) + "\n")
    except BaseException:
        traceback.print_exc()
        for barrier in barriers:
            barrier.abort()
        raise
    finally:
        if connection is not None:
            connection.close()


def snapshot(pids):
    result = benchmark.server_snapshot()
    result["worker_processes"] = {str(pid): benchmark.process_resources(pid) for pid in sorted(set(pids))}
    return result


def run_workers(mode, output, warmup=60, repetitions=2500, fake=False):
    ctx = mp.get_context("spawn")
    barriers = [ctx.Barrier(5) for _ in range(6)]
    pids = ctx.Array("i", 4, lock=False)
    deadline = ctx.Value("d", 0)
    files = [output.with_name(output.stem + f"-worker-{i}.jsonl") for i in range(4)]
    pool, jobs = None, []
    try:
        if mode == THREADS:
            pool = ThreadPoolExecutor(max_workers=4)
            jobs = [pool.submit(worker, i, files[i], pids, deadline, barriers, repetitions, fake)
                    for i in range(4)]
        else:
            jobs = [ctx.Process(target=worker,
                    args=(i, files[i], pids, deadline, barriers, repetitions, fake)) for i in range(4)]
            for job in jobs:
                job.start()
        # Workers open/configure separate connections before this barrier.
        # The deadline is assigned just before release; setup is not measured.
        barriers[5].wait(timeout=120)
        deadline.value = time.monotonic() + warmup
        barriers[0].wait(timeout=120)
        barriers[1].wait(timeout=180)
        before = {} if fake else snapshot(pids)
        iso.emit("measurement_start", client_mode=mode, worker_pids=list(pids))
        barriers[2].wait(timeout=120)
        barriers[3].wait(timeout=600)
        after = {} if fake else snapshot(pids)
        barriers[4].wait(timeout=120)
        if mode == THREADS:
            for job in jobs:
                job.result(timeout=120)
        else:
            for job in jobs:
                job.join(timeout=120)
                if job.exitcode != 0:
                    raise RuntimeError(f"Worker {job.pid} exited {job.exitcode}")
    finally:
        for barrier in barriers:
            barrier.abort()
        if pool is not None:
            pool.shutdown(wait=True)
        elif mode == PROCESSES:
            for job in jobs:
                if job.pid is not None and job.is_alive():
                    job.terminate()
                if job.pid is not None:
                    job.join(timeout=10)
    warm, samples, workers = [], [], []
    for path in files:
        with path.open() as source:
            workers.append(json.loads(next(source)))
            for line in source:
                row = json.loads(line)
                (warm if row["type"] == "warmup" else samples).append(row)
    if len(samples) != 4 * repetitions or set(pids) == {0}:
        raise RuntimeError("Missing worker measurements")
    if len(set(pids)) != (1 if mode == THREADS else 4):
        raise RuntimeError("Unexpected client process topology")
    return {"before": before, "after": after, "warm": warm, "samples": samples, "workers": workers}


def client(args):
    output = checked_destination(args.output)
    selection = benchmark.validate_variant(args.variant)
    metadata = {"type": "metadata", "utc": iso.utc(), "variant": args.variant,
                "client_mode": args.client_mode, "client_pid": os.getpid(), "selection": selection,
                "settings": {"case": "detail_page", "concurrency": 4, "dop": 2,
                             "cache": False, "warmup_seconds": 60, "samples_per_worker": 2500},
                "build_manifest_sha256": matrix.sha256(args.manifest),
                "harness_sha256": matrix.sha256(Path(__file__)), "plans": {}}
    import pymysql
    metadata["python"] = sys.version
    metadata["pymysql"] = pymysql.VERSION_STRING
    with benchmark.connect() as connection:
        benchmark.configure(connection, False, 2)
        with connection.cursor() as cursor:
            cursor.execute("show variables")
            metadata["session_variables"] = dict(cursor.fetchall())
            cursor.execute("explain " + benchmark.CASES["detail_page"])
            metadata["plans"]["detail_page"] = "\n".join(str(row[0]) for row in cursor.fetchall())
    with output.open("x") as destination:
        destination.write(json.dumps(metadata) + "\n")
        destination.flush()
        iso.emit("warmup_start", client_mode=args.client_mode)
        values = run_workers(args.client_mode, output)
        rows = values["samples"]
        result = {"type": "summary", "utc": iso.utc(), "samples": len(rows),
                  "measured": iso.extra_summary(rows), **benchmark.measured_throughput(rows),
                  "warmup_samples": len(values["warm"]), "workers": values["workers"],
                  "server_resources_before": values["before"], "server_resources_after": values["after"]}
        for row in values["warm"] + rows:
            destination.write(json.dumps(row) + "\n")
        destination.write(json.dumps(result) + "\n")
    iso.emit("client_complete", qps=result["measured_queries_per_second"], measured=result["measured"])


def run_client(command, log):
    with log.open("x") as target:
        child = subprocess.Popen(command, stdout=target, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            status = child.wait(timeout=900)
            if status:
                raise subprocess.CalledProcessError(status, command)
        except BaseException:
            # The process group contains only this invocation's Python client workers.
            # FE/BE are launched in separate sessions by runtime.start.
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                child.wait(timeout=30)
            raise


def suite(args):
    matrix.ensure_idle_builds()
    matrix.ensure_restored()
    if any(runtime.live_record(node) for node in runtime.NODES):
        raise RuntimeError("Existing active QCT nodes: refusing takeover")
    root = checked_destination(ROOT / args.series)
    root.mkdir()
    original = iso.storage_files()
    iso.write_json(root / "storage-before.json", original)
    runs = schedule(args.experiment)
    iso.write_json(root / "design.json", {"utc": iso.utc(), "experiment": args.experiment,
        "schedule": runs, "case": "detail_page", "warmup_seconds": 60, "samples_per_run": 10000,
        "concurrency": 4, "query_cache": False, "storage_page_cache": True, "dop": 2,
        "baseline": matrix.BASELINE, "feature": iso.FEATURE,
        "harness_sha256": matrix.sha256(Path(__file__)),
        "historical_helpers_sha256": matrix.sha256(HISTORICAL),
        "reset": "Fresh FE/3BE/client per run; preserved data; no OS cache purge",
        "limitations": "Same shared 6 CPU container/VM; not physical client/server isolation"})
    completed, identities = [], set()
    reference_placement, reference_plan = None, None
    try:
        for number, run in enumerate(runs, 1):
            iso.emit("run_start", number=number, total=len(runs), **run)
            matrix.ensure_idle_builds()
            matrix.stop_all()
            if iso.storage_files() != original:
                raise RuntimeError("Physical data changed before run")
            runtime.prepare(run["variant"], page_cache=True)
            for node in runtime.NODES:
                runtime.start(node)
            ready = matrix.wait_ready(run["variant"], timeout=90)
            for _ in range(3):
                time.sleep(5)
                ready = matrix.wait_ready(run["variant"])
                if any(row.get("ErrMsg") for row in ready["backends"]):
                    raise RuntimeError("Backend heartbeat reports an error")
            placement = iso.placement(ready)
            reference_placement = reference_placement or placement
            if placement != reference_placement:
                raise RuntimeError("Tablet placement/version changed")
            evidence = matrix.manifest(run["variant"], ready)
            expected = matrix.BASELINE if run["variant"] == "A-baseline" else iso.FEATURE
            if evidence["source_revision"] != expected:
                raise RuntimeError("Unexpected production revision")
            current = {(node, row["pid"], row["start_ticks"]) for node, row in evidence["nodes"].items()}
            if current & identities:
                raise RuntimeError("Reused server process")
            identities |= current
            manifest = root / (run["name"] + "-build.json")
            iso.write_json(manifest, evidence)
            output = root / (run["name"] + ".jsonl")
            command = [sys.executable, str(Path(__file__)), "client", "--variant", run["variant"],
                       "--client-mode", run["client_mode"], "--manifest", str(manifest), "--output", str(output)]
            run_client(command, root / (run["name"] + ".log"))
            with output.open() as source:
                meta = json.loads(next(source))
                for line in source:
                    last = json.loads(line)
            if last.get("type") != "summary" or last["samples"] != 10000:
                raise RuntimeError("Incomplete measurement")
            plan = meta["plans"]["detail_page"]
            reference_plan = reference_plan or plan
            if plan != reference_plan:
                raise RuntimeError("Execution plan changed")
            completed.append({**run, "qps": last["measured_queries_per_second"],
                              "measured": last["measured"], "warmup_samples": last["warmup_samples"]})
            iso.emit("run_complete", number=number, total=len(runs), **completed[-1])
            matrix.stop_all()
            if iso.storage_files() != original:
                raise RuntimeError("Physical data changed after run")
        iso.write_json(root / "completed.json", completed)
        iso.emit("suite_complete", groups=len(completed), samples=len(completed) * 10000)
    finally:
        matrix.stop_all()
        matrix.ensure_restored()
        unchanged = iso.storage_files() == original
        iso.write_json(root / "final-state.json", {"utc": iso.utc(), "completed_groups": len(completed),
            "nodes": {node: runtime.live_record(node) for node in runtime.NODES},
            "physical_files_unchanged": unchanged,
            "memory_events": Path("/sys/fs/cgroup/memory.events").read_text()})
        if not unchanged:
            raise RuntimeError("Physical data changed; preserved evidence without overwriting")


class Tests(unittest.TestCase):
    def test_client_schedule_balances_order(self):
        runs = schedule("client")
        self.assertEqual(len(runs), 8)
        self.assertEqual(len({r["name"] for r in runs}), 8)
        for position in (1, 2):
            self.assertEqual(sorted(r["client_mode"] for r in runs if r["position"] == position),
                             [PROCESSES, PROCESSES, THREADS, THREADS])
        self.assertTrue(all(r["variant"] == "A-baseline" for r in runs))

    def test_abc_schedule(self):
        runs = schedule("abc")
        self.assertEqual(len(runs), 18)
        for variant in iso.VARIANTS.values():
            self.assertEqual(sum(r["variant"] == variant for r in runs), 6)
        self.assertTrue(all(r["client_mode"] == PROCESSES for r in runs))

    def test_output_scope(self):
        with self.assertRaises(ValueError):
            checked_destination(Path("/tmp/outside.json"))

    def test_sql_and_full_results_unchanged(self):
        self.assertEqual(benchmark.CASES["detail_page"],
                         "select k, message from qct_perf.events order by k desc limit 1000")
        for rows, warnings in ((999, 0), (1000, 1)):
            with self.assertRaises(RuntimeError):
                benchmark.check_healthy_sample({"case": "detail_page", "rows": rows, "warning_count": warnings})

    def test_thread_and_spawn_barriers(self):
        import tempfile
        # Synthetic data only: validates actual threads/spawn/barriers/CPU/evidence collection.
        # Temporary artifacts intentionally retained; no database or server involved.
        folder = Path(tempfile.mkdtemp(prefix="qct-client-harness-test-"))
        for mode in (THREADS, PROCESSES):
            values = run_workers(mode, folder / (mode + ".jsonl"), warmup=0, repetitions=3, fake=True)
            self.assertEqual(len(values["samples"]), 12)
            self.assertEqual(len(values["workers"]), 4)
            self.assertEqual(len({r["pid"] for r in values["workers"]}), 1 if mode == THREADS else 4)
            self.assertTrue(all(r["thread_cpu_seconds"] >= 0 for r in values["workers"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("suite", "client", "test"))
    parser.add_argument("--experiment", choices=("client", "abc"), default="client")
    parser.add_argument("--series", default="client-architecture-20260922-r1")
    parser.add_argument("--client-mode", choices=(THREADS, PROCESSES), default=THREADS)
    parser.add_argument("--variant", choices=iso.VARIANTS.values(), default="A-baseline")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", args.series):
        parser.error("Invalid series")
    if args.action == "test":
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
        raise SystemExit(not result.wasSuccessful())
    elif args.action == "client":
        client(args)
    else:
        suite(args)


if __name__ == "__main__":
    main()
