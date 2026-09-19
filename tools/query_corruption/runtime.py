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

"""Start/stop only the marked, isolated QCT test nodes. Never deletes storage.

Preparation uses this task's production builds, not UT or Tenant-TTL artifacts.
Variant selection requires all nodes stopped and preserves previous configuration
and launch records. It does not register BEs or create/overwrite SQL fixtures.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import time

from fault_file import CLUSTER_ROOT


WORKSPACE = Path("/query-corruption-workspace")
NODES = ("fe", "be0", "be1", "be2")
JAVA_HOME = Path("/usr/lib/jvm/java-17-openjdk-arm64")


def ensure_file_limit():
    # StorageEngine rejects fewer than 60000 descriptors. Change this launcher/child
    # only, not Docker defaults, the VM, or other running processes.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY or soft >= 60000:
        return
    if hard != resource.RLIM_INFINITY and hard < 60000:
        raise RuntimeError("QCT node requires a hard file-descriptor limit of at least 60000")
    resource.setrlimit(resource.RLIMIT_NOFILE,
                       (65535 if hard == resource.RLIM_INFINITY else min(65535, hard), hard))


def checked_root(create=False):
    if CLUSTER_ROOT.absolute() != CLUSTER_ROOT.resolve():
        raise ValueError("Test root must not contain symlinks")
    marker = CLUSTER_ROOT / ".qct-test-cluster"
    if not CLUSTER_ROOT.exists() and create:
        CLUSTER_ROOT.mkdir(parents=True)
        marker.write_text("QCT dedicated test data; preserve storage and evidence\n")
    if not marker.is_file() or marker.is_symlink():
        raise ValueError("Missing dedicated cluster marker")
    return CLUSTER_ROOT


def process_identity(pid, proc_root=Path("/proc")):
    try:
        fields = (proc_root / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return None
        environment = (proc_root / str(pid) / "environ").read_bytes().split(b"\0")
        return {"start_ticks": fields[19], "home": next(
            (entry.split(b"=", 1)[1].decode() for entry in environment
             if entry.startswith(b"STARROCKS_HOME=")), None)}
    except (FileNotFoundError, ProcessLookupError):
        return None


def await_launch_identity(child, home, timeout=5):
    # procfs can briefly expose an empty environment across exec. Popen's own
    # unreaped child identity is authoritative here; do not declare it dead after
    # one procfs read and leave an unrecorded, still-running BE behind.
    deadline = time.monotonic() + timeout
    while child.poll() is None:
        identity = process_identity(child.pid)
        if identity is not None and identity["home"] == str(home):
            return identity
        if time.monotonic() >= deadline:
            child.terminate()
            child.wait(timeout=30)
            raise RuntimeError("Could not establish QCT child identity; child terminated, inspect launcher.log")
        time.sleep(0.01)
    raise RuntimeError("Node exited during launch; inspect launcher.log")


def node_home(node):
    if node not in NODES:
        raise ValueError("Unexpected node")
    home = checked_root() / node
    if home.resolve() != home:
        raise ValueError("Node home must not be a symlink")
    for name in ("bin", "conf", "log", "meta", "storage", "udf", "history", "launch.json"):
        path = home / name
        if path.resolve() != path:
            raise ValueError("Node paths must not be symlinks")
    return home


def live_record(node):
    home = node_home(node)
    record = home / "launch.json"
    if not record.exists():
        return None
    launch = json.loads(record.read_text())
    identity = process_identity(launch["pid"])
    if identity is None:
        return None
    if identity != {"start_ticks": launch["start_ticks"], "home": str(home)}:
        raise ValueError("PID was reused or does not belong to this QCT node; refusing to signal it")
    return launch


def prepare(variant, page_cache=False):
    root = checked_root(create=True)
    if any(live_record(node) is not None for node in NODES):
        raise ValueError("Stop all QCT nodes before selecting artifacts/configuration")
    source = WORKSPACE / ("baseline-src" if variant == "A-baseline" else "src")
    cache = (source / "be/build_Release/CMakeCache.txt").read_text()
    if "MAKE_TEST:BOOL=OFF" not in cache or "CMAKE_BUILD_TYPE:STRING=Release" not in cache:
        raise ValueError("Require a verified production Release build, not a UT binary")
    be = source / "be/output/lib/starrocks_be"
    fe = source / "fe/fe-core/target/starrocks-fe.jar"
    fe_lib = source / "fe/fe-core/target/lib"
    if not be.is_file() or not fe.is_file() or not any(fe_lib.glob("*.jar")):
        raise ValueError("Build both production BE and packaged FE before preparation")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    for node in NODES:
        home = node_home(node)
        for directory in ("bin", "conf", "log", "meta", "storage", "udf", "history"):
            (home / directory).mkdir(parents=True, exist_ok=True)
        config = home / "conf" / ("fe.conf" if node == "fe" else "be.conf")
        if config.is_symlink():
            raise ValueError("Configuration must not be a symlink")
        if config.exists():
            shutil.copy2(config, home / "history" / (timestamp + ".conf"))
        template = Path(__file__).parent / "conf" / (node + ".conf")
        contents = template.read_text()
        if node == "fe":
            # Upstream does not know this new option; use its original default behavior.
            setting = "enable_query_corruption_tolerance = false\n"
            contents = contents.replace(setting, "" if variant == "A-baseline" else
                                        "enable_query_corruption_tolerance = "
                                        + ("true" if variant == "C-on" else "false") + "\n")
        elif page_cache:
            contents = contents.replace("disable_storage_page_cache = true\n",
                                        "disable_storage_page_cache = false\n")
        config.write_text(contents)
    selection = root / "selection.json"
    if selection.is_symlink():
        raise ValueError("Selection record must not be a symlink")
    if selection.exists():
        shutil.copy2(selection, root / "fe/history" / (timestamp + "-selection.json"))
    selection.write_text(json.dumps({"variant": variant, "source": str(source),
                                     "storage_page_cache": page_cache,
                                     "selected_utc": timestamp}, indent=2))


def start(node):
    root = checked_root()
    if live_record(node) is not None:
        raise ValueError("QCT node is already running")
    selection = json.loads((root / "selection.json").read_text())
    source = Path(selection["source"])
    if source not in (WORKSPACE / "src", WORKSPACE / "baseline-src"):
        raise ValueError("Unexpected source tree")
    home = node_home(node)
    environment = dict(os.environ, STARROCKS_HOME=str(home), JAVA_HOME=str(JAVA_HOME),
                       LOG_DIR=str(home / "log"), PID_DIR=str(home / "bin"),
                       UDF_RUNTIME_DIR=str(home / "udf"))
    if node == "fe":
        # This tool launches Java directly. Keep options identical for A/B/C.
        classpath = str(source / "fe/fe-core/target/starrocks-fe.jar") + ":" \
            + str(source / "fe/fe-core/target/lib/*") + ":" + str(home / "conf")
        command = [str(JAVA_HOME / "bin/java"), "-Xms512m", "-Xmx2g", "-XX:+UseG1GC",
                   "-Dlog4j2.formatMsgNoLookups=true", "-cp", classpath, "com.starrocks.StarRocksFE"]
    else:
        environment["LD_LIBRARY_PATH"] = str(JAVA_HOME / "lib/server") \
            + ":/var/local/thirdparty/installed/jemalloc/lib-shared"
        environment["JEMALLOC_CONF"] = "background_thread:true,prof:false"
        command = [str(source / "be/output/lib/starrocks_be")]
    launch_path = home / "launch.json"
    if launch_path.exists():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        shutil.copy2(launch_path, home / "history" / (timestamp + "-launch.json"))
    with (home / "log/launcher.log").open("ab") as output:
        ensure_file_limit()
        child = subprocess.Popen(command, cwd=home, env=environment, stdin=subprocess.DEVNULL,
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    identity = await_launch_identity(child, home)
    launch = {"pid": child.pid, **identity, "command": command, "selection": selection}
    launch_path.write_text(json.dumps(launch, indent=2))
    print(json.dumps({"node": node, **launch}))


def stop(node):
    launch = live_record(node)
    if launch is None:
        print(json.dumps({"node": node, "state": "already stopped"}))
        return
    try:
        os.kill(launch["pid"], signal.SIGTERM)
    except ProcessLookupError:
        print(json.dumps({"node": node, "state": "already stopped"}))
        return
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if live_record(node) is None:
            print(json.dumps({"node": node, "state": "stopped"}))
            return
        time.sleep(0.2)
    raise TimeoutError("Node did not exit after SIGTERM; no SIGKILL or data cleanup was attempted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("variant", choices=("A-baseline", "B-off", "C-on"))
    prepare_parser.add_argument("--page-cache", action="store_true",
                                help="Enable the BE storage page cache for warm-cache performance runs")
    for action in ("start", "stop", "status"):
        commands.add_parser(action).add_argument("node", choices=NODES)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.variant, args.page_cache)
    elif args.action == "start":
        start(args.node)
    elif args.action == "stop":
        stop(args.node)
    else:
        print(json.dumps(live_record(args.node)))


if __name__ == "__main__":
    main()
