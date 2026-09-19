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

"""Additional real-file regressions, only for the marked QCT fixture cluster.

Requires an explicit, hash-matching page snapshot from segment_pages.py. All faults
are restored in finally, with original backups retained. Run after, never alongside,
the performance matrix. No SQL data writes or production configuration changes.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import threading
import time
from urllib.request import urlopen

import benchmark
import fault_file
import matrix
import probe
import runtime


SETTINGS = {"enable_query_cache": "false", "enable_tablet_internal_parallel": "false",
            "enable_short_circuit": "false", "enable_global_runtime_filter": "false",
            "pipeline_dop": "2", "enable_profile": "true", "pipeline_profile_level": "2",
            "cbo_cte_reuse_rate": "0"}


def confirm_tablet(rows, expected):
    # SHOW PROC/SHOW TABLET metadata is returned as text by this baseline.
    if len(rows) != 1 or int(rows[0][0]) != expected:
        raise ValueError("Unexpected actual one_tablet metadata: " + repr(rows))


def checked_snapshot(path, tablet):
    snapshot = json.loads(path.read_text())
    segment = fault_file.checked_segment(snapshot["segment"], tablet)
    if fault_file.digest(segment.read_bytes()) != snapshot["sha256"]:
        raise ValueError("Page snapshot does not describe the current, restored test segment")
    message = next(column for column in snapshot["columns"] if column["column_id"] == 1)
    page = message["pages"][1]
    if snapshot["num_rows"] != 32768 or not 0 < page["first_ordinal"] < 4096:
        raise ValueError("Expected the fixed one_tablet fixture and a mid-chunk message page")
    return segment, page


def configured(profile=True):
    connection = benchmark.connect()
    with connection.cursor() as cursor:
        for name, value in SETTINGS.items():
            cursor.execute("set " + name + " = %s", (value,))
        if not profile:
            cursor.execute("set enable_profile=false")
    return connection


def save(directory, name, value):
    with (directory / (name + ".json")).open("x") as output:
        json.dump(value, output, indent=2, default=str)


def run_query(directory, name, sql, rows, expectation="partial", plan_marker=None):
    with configured() as connection:
        with connection.cursor() as cursor:
            cursor.execute("explain verbose " + sql)
            plan = "\n".join(row[0] for row in cursor.fetchall())
        evidence = {"sql": sql, "settings": SETTINGS, "plan": plan}
        try:
            if plan_marker:
                assert plan_marker in plan, (plan_marker, plan)
            result = probe.mysql_query(connection, sql, expectation)
            evidence["result"] = result
            assert result["rows"] == rows, result
            if name.startswith("left-"):
                assert len(result["sample"]) == 8 and all(row[1] is None for row in result["sample"])
            if result["warnings"]:
                query_id = re.search(r"query_id=([0-9a-f-]+)", result["warnings"][0][2]).group(1)
                # Query has already finished. Profile retrieval is not an execution barrier.
                deadline = time.monotonic() + 10
                while True:
                    with connection.cursor() as cursor:
                        cursor.execute("select get_query_profile(%s)", (query_id,))
                        profile = cursor.fetchone()[0]
                        cursor.fetchall()
                    if profile and "OLAP_SCAN" in profile:
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError("Completed query profile unavailable: " + query_id)
                    time.sleep(0.2)
                with (directory / (name + "-profile.txt")).open("x") as output:
                    output.write(profile)
                evidence["query_id"] = query_id
            evidence["passed"] = True
        except BaseException as error:
            evidence.update(passed=False, error=repr(error))
            raise
        finally:
            save(directory, name, evidence)


def start_ready(node):
    runtime.start(node)
    port = 19400 + int(node[-1]) * 100
    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen("http://127.0.0.1:%d/api/health" % port, timeout=2) as response:
                assert response.status == 200
                response.read()
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)
    matrix.wait_ready("C-on")


@contextmanager
def fault(segment, tablet, kind, offset=0):
    node = segment.relative_to(runtime.checked_root()).parts[0]
    if node not in ("be0", "be1", "be2"):
        raise ValueError("Unexpected QCT fixture BE")
    runtime.stop(node)
    manifest = fault_file.inject(segment, tablet, kind, offset)
    try:
        start_ready(node)
        yield manifest
    finally:
        runtime.stop(node)
        fault_file.restore(manifest)
        start_ready(node)


def interference_phase(directory, name, fault_traffic, repetitions, warmup=50):
    stop, ready = threading.Event(), threading.Event()
    results = {"phase": name, "fault_traffic": fault_traffic, "warmup": warmup, "healthy_samples_ms": [],
               "server_before": benchmark.server_snapshot()}

    def traffic():
        count = 0
        try:
            with configured(False) as connection:
                ready.set()
                while not stop.is_set():
                    result = probe.mysql_query(connection, "select k from qct_faults.one_tablet", "partial")
                    assert result["rows"] == 0
                    count += 1
                    stop.wait(0.01)  # Bounded fault load, not a new server wait.
            return count
        finally:
            ready.set()

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            background = pool.submit(traffic) if fault_traffic else None
            if background:
                assert ready.wait(15)
            try:
                with configured(False) as connection:
                    for index in range(warmup + repetitions):
                        started = time.perf_counter()
                        with connection.cursor() as cursor:
                            cursor.execute(probe.HEALTHY_SQL)
                            assert len(cursor.fetchall()) == 8
                            assert cursor.warning_count == 0
                        if index >= warmup:
                            results["healthy_samples_ms"].append((time.perf_counter() - started) * 1000)
            finally:
                stop.set()
            results["fault_queries"] = background.result() if background else 0
        values = results["healthy_samples_ms"]
        results["latency_ms"] = {str(p): benchmark.percentile(values, p) for p in (50, 95, 99)}
        results["server_after"] = benchmark.server_snapshot()
        results["passed"] = True
    except BaseException as error:
        results.update(passed=False, error=repr(error))
        raise
    finally:
        save(directory, name, results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series", required=True)
    parser.add_argument("--pages-json", type=Path, required=True)
    parser.add_argument("--confirm-test-tablet", type=int, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", args.series):
        parser.error("Use a unique plain series name")
    matrix.ensure_idle_builds()
    matrix.ensure_restored()
    segment, page = checked_snapshot(args.pages_json, args.confirm_test_tablet)
    destination = matrix.LOG_ROOT / args.series
    if destination.resolve() != destination:
        raise ValueError("Evidence path must not contain symlinks")
    destination.mkdir(exist_ok=False)
    try:
        matrix.stop_all()
        runtime.prepare("C-on", False)
        for node in runtime.NODES:
            runtime.start(node)
        readiness = matrix.wait_ready("C-on")
        save(destination, "build", matrix.manifest("C-on", readiness))
        with configured() as connection, connection.cursor() as cursor:
            cursor.execute("show tablet from qct_faults.one_tablet")
            tablets = list(cursor.fetchall())
            confirm_tablet(tablets, args.confirm_test_tablet)
        interference_phase(destination, "control-before", False, 1000)
        with fault(segment, args.confirm_test_tablet, "page", 0) as manifest:
            save(destination, "first-page-fault", {"manifest": str(manifest)})
            for distribution in ("broadcast", "shuffle"):
                for join, rows in (("left", 8), ("inner", 0)):
                    sql = ("select /*+ SET_VAR(disable_join_reorder=true) */ a.k,b.message "
                           "from qct_faults.multiple_tablets a " + join + " join [" + distribution + "] "
                           "qct_faults.one_tablet b on a.k=b.k where a.k<=8")
                    run_query(destination, join + "-" + distribution, sql, rows,
                              plan_marker="LEFT OUTER JOIN" if join == "left" else "INNER JOIN")
            run_query(destination, "reused-cte", "with x as (select k,message from qct_faults.one_tablet) "
                      "select * from x union all select * from x", 0, plan_marker="MultiCastDataSinks")
            jdbc = runtime.WORKSPACE / "tools/jdbc-probe"
            jars = {"mysql": "com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar",
                    "mariadb": "org/mariadb/jdbc/mariadb-java-client/3.3.2/mariadb-java-client-3.3.2.jar"}
            with (destination / "jdbc-real-parameter.log").open("x") as output:
                for driver, jar in jars.items():
                    subprocess.run([str(runtime.JAVA_HOME / "bin/java"), "-cp", str(jdbc) + ":"
                                    + str(runtime.WORKSPACE / "cache/maven" / jar), "JdbcProbe", driver,
                                    "prepared", "partial", "select k,message from qct_faults.one_tablet where k > ?"],
                                   stdout=output, stderr=subprocess.STDOUT, check=True)
            interference_phase(destination, "fault-concurrent", True, 1000)
        interference_phase(destination, "control-after", False, 1000)
        with fault(segment, args.confirm_test_tablet, "page", page["offset"]) as manifest:
            save(destination, "mid-chunk-fault", {"manifest": str(manifest), "page": page})
            run_query(destination, "mid-chunk", "select k,message from qct_faults.one_tablet", 0)
            run_query(destination, "unread-column", "select k from qct_faults.one_tablet", 32768, "healthy")
        with fault(segment, args.confirm_test_tablet, "magic"):
            run_query(destination, "magic-cache-on", "select /*+ SET_VAR(enable_query_cache=true) */ "
                      "k%8,count(*) from qct_faults.one_tablet group by k%8", 0)
        run_query(destination, "restored", "select k,message from qct_faults.one_tablet order by k", 32768, "healthy")
        matrix.ensure_restored()
        save(destination, "completed", {"passed": True, "utc": datetime.now(timezone.utc).isoformat()})
    finally:
        matrix.stop_all()


if __name__ == "__main__":
    main()
