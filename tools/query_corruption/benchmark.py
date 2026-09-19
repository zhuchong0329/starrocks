#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read-only benchmark for the isolated QCT cluster; requires PyMySQL.

Run only after functional implementation and acceptance. Labels do not switch the
server configuration: record/verify the actual binaries and FE configuration first.
First-row latency includes client protocol decoding; this is not BE-only CPU time.
Raw samples, plans, session settings and warnings are preserved for honest A/B/C
comparison. No timing threshold or fabricated 'zero overhead' verdict is applied.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import random
import statistics
import threading
import time
from urllib.error import URLError
from urllib.request import urlopen

from fault_file import CLUSTER_ROOT
import runtime


CASES = {
    "small_limit": "select k, message from qct_perf.events limit 10",
    "detail_page": "select k, message from qct_perf.events order by k desc limit 1000",
    "empty_scan": "select k, message from qct_perf.events where message = 'qct-no-such-message' limit 10",
    "full_scan": "select sum(length(message)), sum(k) from qct_perf.events",
    "point_lookup": "select /*+ SET_VAR(enable_short_circuit=true) */ k, message from qct_perf.points where k = 42",
    "aggregation": "select k % 8, count(*), sum(length(message)) from qct_perf.events group by k % 8",
    "join": "select count(*), sum(length(a.message)) from qct_perf.events a "
            "join qct_perf.dimension b on a.k % 8 = b.k",
    "join_no_remote_filter": "select /*+ SET_VAR(enable_global_runtime_filter=false) */ "
                             "count(*), sum(length(a.message)) from qct_perf.events a "
                             "join qct_perf.dimension b on a.k % 8 = b.k",
}
EXPECTED_ROWS = {"small_limit": 10, "detail_page": 1000, "empty_scan": 0, "full_scan": 1,
                 "point_lookup": 1, "aggregation": 8, "join": 1, "join_no_remote_filter": 1}


def check_healthy_sample(sample):
    if sample["warning_count"] or sample["rows"] != EXPECTED_ROWS[sample["case"]]:
        raise RuntimeError("Invalid healthy fixture sample: " + str(sample))


def validate_variant(variant):
    selection = json.loads((runtime.checked_root() / "selection.json").read_text())
    if selection["variant"] != variant:
        raise ValueError("Benchmark label does not match the prepared QCT variant")
    for node in runtime.NODES:
        launch = runtime.live_record(node)
        if launch is None or launch["selection"] != selection:
            raise ValueError("QCT node is absent or was launched with another selection: " + node)
    return selection


def process_resources(pid, proc_root=Path("/proc"), ticks=None):
    fields = (proc_root / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
    ticks = ticks or os.sysconf("SC_CLK_TCK")
    status = (proc_root / str(pid) / "status").read_text().splitlines()
    return {"pid": pid, "start_ticks": fields[19],
            "cpu_seconds": (int(fields[11]) + int(fields[12])) / ticks,
            "memory_kib": {line.split(":", 1)[0]: int(line.split()[1]) for line in status
                           if line.startswith(("VmRSS:", "VmHWM:", "VmSize:"))}}


def server_snapshot():
    # Outside the measured query interval; preserve raw counters, do not invent cache-hit/IO statistics.
    result = {"utc": datetime.now(timezone.utc).isoformat(), "nodes": {},
              "client_process": process_resources(os.getpid()), "cgroup": {}}
    for name in ("cpu.max", "cpu.stat", "memory.max", "memory.current", "memory.events"):
        path = Path("/sys/fs/cgroup") / name
        result["cgroup"][name] = path.read_text().strip() if path.exists() else None
    for node in runtime.NODES:
        launch = runtime.live_record(node)
        if launch is None:
            raise RuntimeError("QCT node is not running: " + node)
        result["nodes"][node] = process_resources(launch["pid"])
    for port in (19300, 19400, 19500, 19600):
        try:
            with urlopen("http://127.0.0.1:%d/metrics" % port, timeout=2) as response:
                result[str(port)] = response.read().decode()
        except (OSError, URLError) as error:
            result[str(port)] = {"unavailable": str(error)}
    return result


def percentile(samples, percent):
    if not samples:
        raise ValueError("No samples")
    values = sorted(samples)
    return values[max(0, math.ceil(len(values) * percent / 100) - 1)]


def summarize(samples):
    output = {}
    for name in sorted({sample["case"] for sample in samples}):
        group = [sample for sample in samples if sample["case"] == name]
        entry = {"samples": len(group), "warnings": sum(sample["warning_count"] for sample in group)}
        for field in ("first_row_ms", "complete_ms"):
            values = [sample[field] for sample in group]
            entry[field] = {"mean": statistics.mean(values), "median": statistics.median(values),
                            "p95": percentile(values, 95), "p99": percentile(values, 99),
                            "min": min(values), "max": max(values)}
        output[name] = entry
    return output


def measured_throughput(samples):
    seconds = (max(sample["completed_ns"] for sample in samples)
               - min(sample["started_ns"] for sample in samples)) / 1e9
    if seconds <= 0:
        raise ValueError("Measured interval must be positive")
    return {"measured_wall_seconds": seconds, "measured_queries_per_second": len(samples) / seconds}


def connect():
    import pymysql
    return pymysql.connect(host="127.0.0.1", port=19303, user="root", password="",
                           autocommit=True, connect_timeout=10, read_timeout=60,
                           cursorclass=pymysql.cursors.SSCursor)


def configure(connection, cache, dop):
    with connection.cursor() as cursor:
        cursor.execute("set enable_query_cache = " + ("true" if cache else "false"))
        cursor.execute("set pipeline_dop = %s", (dop,))


def execute(connection, name):
    with connection.cursor() as cursor:
        started = time.perf_counter_ns()
        cursor.execute(CASES[name])
        first = cursor.fetchone()
        first_at = time.perf_counter_ns()
        rows = 0 if first is None else 1
        # Avoid hashing/stringifying every row in the measured path.
        while cursor.fetchone() is not None:
            rows += 1
        completed = time.perf_counter_ns()
        warning_count = cursor.warning_count
    return {"case": name, "first_row_ms": (first_at - started) / 1e6,
            "complete_ms": (completed - started) / 1e6, "rows": rows,
            "warning_count": warning_count, "started_ns": started, "completed_ns": completed}


def consume_http(response):
    if response.status != 200:
        raise RuntimeError("HTTP benchmark request failed: " + str(response.status))
    rows, first_at, trailer, connection_id = 0, None, None, None
    for line in response:
        if not line.strip():
            continue
        record = json.loads(line)
        if trailer is not None:
            raise RuntimeError("Record after final HTTP statistics")
        if "data" in record:
            if first_at is None:
                first_at = time.perf_counter_ns()
            rows += 1
        elif "statistics" in record:
            trailer = record
        elif "connectionId" in record:
            connection_id = record["connectionId"]
    # HTTPResponse iteration also verifies the terminating transfer chunk. A 200
    # with missing statistics or an IncompleteRead is never a successful sample.
    completed = time.perf_counter_ns()
    if trailer is None:
        raise RuntimeError("HTTP benchmark response is missing final statistics")
    warnings = trailer.get("warnings", [])
    if trailer.get("partial_result") or warnings:
        raise RuntimeError("Healthy performance fixture unexpectedly returned a partial result")
    return {"rows": rows, "warning_count": len(warnings), "first_at": first_at or completed,
            "completed_ns": completed, "connection_id": connection_id,
            "tolerance_fields": "partial_result" in trailer}


def execute_http(connection, name, cache, dop):
    request = json.dumps({"query": CASES[name], "onlyOutputResultRaw": False,
                          "sessionVariables": {"enable_query_cache": str(cache).lower(), "pipeline_dop": str(dop)}})
    started = time.perf_counter_ns()
    connection.request("POST", "/api/v1/catalogs/default_catalog/sql", request,
                       {"Authorization": "Basic cm9vdDo=", "Content-Type": "application/json",
                        "Connection": "keep-alive"})
    with connection.getresponse() as response:
        sample = consume_http(response)
    sample.update(case=name, started_ns=started,
                  first_row_ms=(sample.pop("first_at") - started) / 1e6,
                  complete_ms=(sample["completed_ns"] - started) / 1e6)
    return sample


def worker(worker_id, args, names, barrier):
    samples = []
    try:
        selected = connect() if args.protocol == "mysql" else \
            closing(http.client.HTTPConnection("127.0.0.1", 19300, timeout=60))
        with selected as connection:
            if args.protocol == "mysql":
                configure(connection, args.cache, args.dop)
            rng = random.Random(args.seed + worker_id)
            for round_id in range(args.warmup + args.repetitions):
                if round_id == args.warmup:
                    # Client-side benchmark start only, never an in-query or server-side wait.
                    barrier.wait(timeout=120)
                order = list(names)
                rng.shuffle(order)
                for name in order:
                    sample = execute(connection, name) if args.protocol == "mysql" else \
                        execute_http(connection, name, args.cache, args.dop)
                    check_healthy_sample(sample)
                    if round_id >= args.warmup:
                        sample.update(worker=worker_id, round=round_id - args.warmup)
                        samples.append(sample)
    except BaseException:
        barrier.abort()  # A failed warmup must not leave peers blocked forever.
        raise
    return samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", required=True, choices=("A-baseline", "B-off", "C-on"))
    parser.add_argument("--protocol", choices=("mysql", "http"), default="mysql")
    parser.add_argument("--build-manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cases", nargs="+", choices=tuple(CASES), default=list(CASES))
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--repetitions", type=int, default=200)
    parser.add_argument("--concurrency", type=int, choices=range(1, 17), default=1)
    parser.add_argument("--dop", type=int, choices=range(1, 9), default=2)
    parser.add_argument("--cache", action="store_true")
    parser.add_argument("--seed", type=int, default=20260919)
    args = parser.parse_args()
    if args.warmup < 0 or args.repetitions < 1:
        parser.error("warmup must be nonnegative and repetitions positive")
    if CLUSTER_ROOT.absolute() != CLUSTER_ROOT.resolve() or not (CLUSTER_ROOT / ".qct-test-cluster").is_file():
        parser.error("This benchmark only targets the dedicated marked test cluster")
    log_root = Path("/query-corruption-workspace/logs/QCT-006").resolve()
    output = args.output.absolute()
    if not output.is_relative_to(log_root) or output != output.resolve():
        parser.error("Output must be a non-symlink path under the QCT-006 log directory")
    if output.exists():
        parser.error("Refusing to overwrite an earlier measurement")
    selection = validate_variant(args.variant)
    manifest = args.build_manifest.read_bytes()
    metadata = {"type": "metadata", "variant": args.variant, "protocol": args.protocol,
                "runtime_selection": selection,
                "started_utc": datetime.now(timezone.utc).isoformat(),
                "build_manifest": str(args.build_manifest),
                "build_manifest_sha256": hashlib.sha256(manifest).hexdigest(),
                "settings": {"cache": args.cache, "dop": args.dop, "concurrency": args.concurrency,
                             "warmup": args.warmup, "repetitions": args.repetitions, "seed": args.seed},
                "plans": {}}
    with connect() as connection:
        configure(connection, args.cache, args.dop)
        with connection.cursor() as cursor:
            cursor.execute("show variables")
            metadata["control_connection_session_variables"] = dict(cursor.fetchall())
            cursor.execute("select current_version()")
            metadata["server_version"] = list(cursor.fetchall())
            for name in args.cases:
                cursor.execute("explain " + CASES[name])
                metadata["plans"][name] = "\n".join(str(row[0]) for row in cursor.fetchall())
                if name == "point_lookup" and "Short Circuit Scan: true" not in metadata["plans"][name]:
                    raise RuntimeError("Point lookup did not use the intended original short-circuit path")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        metadata["server_resources_before"] = server_snapshot()
        destination.write(json.dumps(metadata) + "\n")
        destination.flush()
        started = time.perf_counter()
        barrier = threading.Barrier(args.concurrency)
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            groups = list(executor.map(lambda worker_id: worker(worker_id, args, args.cases, barrier),
                                       range(args.concurrency)))
        elapsed = time.perf_counter() - started
        samples = [sample for group in groups for sample in group]
        for sample in samples:
            destination.write(json.dumps({"type": "sample", **sample}) + "\n")
        result = {"type": "summary", "measured": summarize(samples),
                  **measured_throughput(samples),
                  "elapsed_including_warmup_seconds": elapsed,
                  "queries_including_warmup_per_second":
                      args.concurrency * len(args.cases) * (args.warmup + args.repetitions) / elapsed,
                  "server_resources_after": server_snapshot()}
        destination.write(json.dumps(result) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "server_resources_after"}, indent=2))


if __name__ == "__main__":
    main()
