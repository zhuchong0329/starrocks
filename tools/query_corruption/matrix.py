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

"""Repeatable A/B/C runs on the marked QCT cluster, preserving all prior evidence.

Stops only this cluster's identity-checked nodes. Refuses active compilation and
unrestored test faults. No data creation/deletion, cache purge, or global OS tuning.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import benchmark
from fault_file import BACKUP_ROOT
import runtime


LOG_ROOT = runtime.WORKSPACE / "logs/QCT-006"
BASELINE = "9559176fab6e2cb885779f1e7b680133d58d6972"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ensure_idle_builds(proc_root=Path("/proc")):
    for entry in proc_root.iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            command = (entry / "comm").read_text().strip()
            arguments = (entry / "cmdline").read_bytes()
        except FileNotFoundError:
            continue
        compiler = command in ("ninja", "cc1plus", "ld.lld", "javac", "cc1")
        maven = command == "java" and b"org.codehaus.plexus.classworlds.launcher.Launcher" in arguments
        if fields[0] != "Z" and (compiler or maven):
            raise RuntimeError("Finish all QCT compilation before measuring: " + entry.name + " " + command)


def ensure_restored(backup_root=BACKUP_ROOT):
    for manifest in backup_root.glob("*.json"):
        record = json.loads(manifest.read_text())
        if sha256(Path(record["segment"])) != record["original_sha256"]:
            raise RuntimeError("Unrestored physical test fault: " + str(manifest))


def wait_ready(variant, timeout=60):
    source = runtime.WORKSPACE / ("baseline-src" if variant == "A-baseline" else "src")
    expected = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    if variant == "A-baseline" and expected != BASELINE:
        raise RuntimeError("Unexpected community baseline")
    deadline = time.monotonic() + timeout
    while True:
        try:
            with benchmark.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("select 1")
                    assert cursor.fetchall() == [(1,)]
                    cursor.execute("show backends")
                    fields = [column[0] for column in cursor.description]
                    backends = [dict(zip(fields, row)) for row in cursor.fetchall()]
                    assert len(backends) == 3 and all(row["Alive"] == "true" for row in backends)
                    assert all(expected in row["Version"] for row in backends)
                    cursor.execute("admin show frontend config like 'enable_query_corruption_tolerance'")
                    config = cursor.fetchall()
                    if variant == "A-baseline":
                        assert not config
                    else:
                        assert config[0][2] == ("true" if variant == "C-on" else "false")
                    cursor.execute("admin show frontend config like 'tablet_sched_disable_balance'")
                    balance = cursor.fetchall()
                    assert balance[0][2] == "true"
                    tablets = {}
                    for table in ("events", "points", "dimension"):
                        cursor.execute("show tablet from qct_perf." + table)
                        fields = [column[0] for column in cursor.description]
                        tablets[table] = [dict(zip(fields, row)) for row in cursor.fetchall()]
                    return {"backends": backends, "frontend_config": config,
                            "balance_config": balance, "fixture_tablets": tablets}
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def manifest(variant, readiness):
    selection = benchmark.validate_variant(variant)
    source = Path(selection["source"])
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    if variant == "A-baseline" and revision != BASELINE:
        raise RuntimeError("Unexpected community baseline")
    scopes = ["be/src", "be/CMakeLists.txt", ":(glob)fe/*/src/main/**", "fe/pom.xml",
              ":(glob)fe/*/pom.xml", "gensrc", "build.sh", "env.sh", "CMakeLists.txt", "cmake"]
    difference = subprocess.check_output(["git", "diff", "HEAD", "--", *scopes], cwd=source)
    status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all", "--", *scopes],
                                     cwd=source)
    if difference or status:
        raise RuntimeError("Production sources have uncommitted changes; rebuild and record provenance first")
    files = [source / name for name in ("be/output/lib/starrocks_be", "fe/fe-core/target/starrocks-fe.jar",
                                        "be/build_Release/CMakeCache.txt", "be/build_Release/compile_commands.json")]
    files += sorted((source / "fe/fe-core/target/lib").glob("*.jar"))
    files += [runtime.node_home(node) / "conf" / ("fe.conf" if node == "fe" else "be.conf")
              for node in runtime.NODES]
    environment = dict(os.environ, LD_LIBRARY_PATH=str(runtime.JAVA_HOME / "lib/server")
                       + ":/var/local/thirdparty/installed/jemalloc/lib-shared")
    version = subprocess.check_output([str(files[0]), "--version"], env=environment, text=True)
    if revision not in version:
        raise RuntimeError("Native binary does not identify the recorded source revision")
    return {"utc": datetime.now(timezone.utc).isoformat(), "source_revision": revision,
            "checked_production_scopes": scopes,
            "selection": selection, "be_version": version, "readiness": readiness,
            "nodes": {node: runtime.live_record(node) for node in runtime.NODES},
            "files": {str(path): {"bytes": path.stat().st_size, "sha256": sha256(path)} for path in files},
            "tool_sha256": {path.name: sha256(path) for path in (Path(__file__), Path(benchmark.__file__))}}


def stop_all():
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(runtime.stop, runtime.NODES))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series", required=True, help="Unique lowercase evidence directory name")
    parser.add_argument("--variants", nargs="+", required=True, choices=("A-baseline", "B-off", "C-on"))
    parser.add_argument("--page-cache", action="store_true")
    parser.add_argument("--repetitions", type=int, default=200, help="Measured rounds per worker")
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4], choices=range(1, 17))
    parser.add_argument("--protocols", nargs="+", default=["mysql", "http"], choices=("mysql", "http"))
    parser.add_argument("--query-cache", nargs="+", default=["off", "on"], choices=("off", "on"))
    parser.add_argument("--cases", nargs="+", choices=tuple(benchmark.CASES))
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", args.series):
        parser.error("Use a plain, unique series name")
    if args.repetitions < 1 or args.warmup < 0 or len(set(args.variants)) != len(args.variants):
        parser.error("Invalid rounds or duplicate variant")
    ensure_idle_builds()
    ensure_restored()
    destination = LOG_ROOT / args.series
    if destination.resolve() != destination:
        parser.error("Evidence directory must not contain symlinks")
    destination.mkdir(exist_ok=False)
    try:
        for variant in args.variants:
            ensure_idle_builds()
            stop_all()
            runtime.prepare(variant, args.page_cache)
            for node in runtime.NODES:
                runtime.start(node)
            readiness = wait_ready(variant)
            evidence = destination / (variant + "-build.json")
            evidence.write_text(json.dumps(manifest(variant, readiness), indent=2))
            for protocol in args.protocols:
                for cache in args.query_cache:
                    for concurrency in args.concurrency:
                        name = f"{variant}-{protocol}-cache-{cache}-c{concurrency}"
                        command = [sys.executable, str(Path(benchmark.__file__)), "--variant", variant,
                                   "--protocol", protocol, "--build-manifest", str(evidence),
                                   "--output", str(destination / (name + ".jsonl")),
                                   "--repetitions", str(args.repetitions), "--warmup", str(args.warmup),
                                   "--concurrency", str(concurrency)]
                        if cache == "on":
                            command.append("--cache")
                        cases = args.cases or (list(benchmark.CASES) if protocol == "mysql" else
                                               ["small_limit", "empty_scan", "detail_page"])
                        command.extend(["--cases", *cases])
                        print(json.dumps({"utc": datetime.now(timezone.utc).isoformat(), "command": command}), flush=True)
                        with (destination / (name + ".log")).open("x") as log:
                            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
                        print(json.dumps({"completed": name}), flush=True)
    finally:
        stop_all()  # Preserve data/config/logs; never leave a performance run fighting a later build.


if __name__ == "__main__":
    main()
