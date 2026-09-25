#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Isolated, persistent 3 FE / 3 BE acceptance cluster; never removes a volume."""
import argparse
import base64
import json
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pymysql
from pymysql.constants import FIELD_TYPE

PREFIX = "sr-ttl-r043"
BUILD = "starrocks-tenant-ttl-e2e"
VOLUME = "sr-tenant-ttl-4.0-build-cache-arm64"
ROOT = "/tenant-ttl-workspace"
ASSETS = ROOT + "/round043-assets"
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
IMAGE = "starrocks/dev-env-ubuntu:4.0-latest"
AUTH = "Basic " + base64.b64encode(b"root:").decode()
LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def docker(*args, check=True, **kwargs):
    return subprocess.run(["docker", *args], check=check, text=True, capture_output=True, **kwargs)


def name(role, index):
    return f"{PREFIX}-{role}{index}"


def ip(role, index):
    return f"172.28.43.{(10 if role == 'fe' else 20) + index}"


def query_port(index):
    return 29030 + index


def http_port(role, index):
    return (28030 if role == "fe" else 28040) + index


def connect(index, database=None, set_timezone=True, read_timeout=180):
    converters = dict(pymysql.converters.conversions)
    # SHOW exposes BOOLEAN metadata but serializes true/false text. Preserve typed booleans.
    converters[FIELD_TYPE.TINY] = lambda v: (
        (v.lower() == "true") if v.lower() in ("true", "false") else int(v)
    )
    conn = pymysql.connect(
        host="127.0.0.1",
        port=query_port(index),
        user="root",
        database=database,
        autocommit=True,
        connect_timeout=2,
        read_timeout=read_timeout,
        write_timeout=180,
        charset="utf8mb4",
        conv=converters,
    )
    if set_timezone:
        with conn.cursor() as cursor:
            cursor.execute("SET time_zone = 'UTC'")
    return conn


def sql(index, statement, params=None, database=None):
    with connect(index, database) as conn, conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(statement, params)
        return cursor.fetchall()


def wait_for(fn, timeout=180, interval=1, description="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (pymysql.MySQLError, OSError, ValueError) as error:
            last = str(error)
        time.sleep(interval)
    raise TimeoutError(f"{description}: {last}")


def leader():
    for index in range(1, 4):
        try:
            with connect(index, set_timezone=False, read_timeout=2) as conn, conn.cursor(
                pymysql.cursors.DictCursor
            ) as cursor:
                cursor.execute("SHOW FRONTENDS")
                rows = cursor.fetchall()
            for row in rows:
                if (
                    row.get("Role") == "LEADER"
                    or str(row.get("IsMaster", row.get("IsLeader"))).lower() == "true"
                ):
                    return int(row["IP"].rsplit(".", 1)[1]) - 10
        except (pymysql.MySQLError, OSError):
            pass
    return None


def config(key, value, indices=(1, 2, 3)):
    results = []
    for index in indices:
        query = urllib.parse.urlencode({key: str(value).lower()})
        request = urllib.request.Request(
            f"http://127.0.0.1:{http_port('fe', index)}/api/_set_config?{query}",
            headers={"Authorization": AUTH},
        )
        with LOCAL_HTTP.open(request, timeout=15) as response:
            result = json.load(response)
            if result.get("err") or result.get("errConfigs"):
                raise RuntimeError(result)
            results.append(result)
    return results


def prepare_assets():
    script = r"""
import hashlib, json, shutil
from pathlib import Path
root = Path('/tenant-ttl-workspace')
assets = root/'round043-assets'
assets.mkdir(exist_ok=True)
old = root/'tenant-ttl-e2e-runtime-20260911-1457'
for role in ('fe','be'):
    (assets/role).mkdir(exist_ok=True)
    shutil.copytree(old/role/'bin', assets/role/'bin', dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns('*.pid'))
    shutil.copytree(old/role/'conf', assets/role/'conf', dirs_exist_ok=True)
lib = assets/'fe/lib'
lib.mkdir(exist_ok=True)
for file in (root/'fe/fe-core/target/lib').iterdir():
    destination = lib/file.name
    if not destination.exists():
        destination.symlink_to(file)
shutil.copy2(root/'fe/fe-core/target/starrocks-fe.jar', lib/'starrocks-fe.jar')
be_lib = assets/'be/lib'
if not be_lib.exists():
    be_lib.symlink_to(old/'be/lib', target_is_directory=True)
def digest(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for part in iter(lambda:f.read(1024*1024),b''):
            h.update(part)
    return h.hexdigest()
manifest={'fe_sha256':digest(lib/'starrocks-fe.jar'),'be_sha256':digest(be_lib/'starrocks_be'),
          'be_origin':str(old/'be/lib/starrocks_be'),
          'be_note':'Reused existing Debug/MAKE_TEST=OFF binary; round034 count default set explicitly to 1000000.'}
(assets/'manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest))
"""
    result = docker("exec", "-i", BUILD, "python3", "-", input=script)
    ARTIFACTS.mkdir(exist_ok=True)
    (ARTIFACTS / "binaries.json").write_text(result.stdout)


def create_node(role, index):
    node = name(role, index)
    if docker("container", "inspect", node, check=False).returncode == 0:
        return
    volume = node + "-data"
    docker("volume", "create", "--label", "purpose=tenant-ttl-round043", volume)
    args = [
        "run",
        "-d",
        "--name",
        node,
        "--hostname",
        node,
        "--network",
        PREFIX,
        "--ip",
        ip(role, index),
        "--init",
        "--cap-add=NET_ADMIN",
        "--ulimit",
        "nofile=65535:65535",
        "--label",
        "purpose=tenant-ttl-round043",
        "--mount",
        f"type=volume,src={VOLUME},dst={ROOT},readonly",
        "--mount",
        f"type=volume,src={volume},dst=/node",
        "-p",
        f"127.0.0.1:{http_port(role,index)}:{8030 if role == 'fe' else 8040}",
    ]
    if role == "fe":
        args += ["-p", f"127.0.0.1:{query_port(index)}:9030"]
    docker(*args, IMAGE, "sleep", "infinity")
    configure_node(role, index)


def configure_node(role, index):
    if role == "fe":
        conf = """JAVA_OPTS = -Xms512m -Xmx1536m -XX:+UseG1GC -XX:ActiveProcessorCount=2
meta_dir = /node/fe/meta
sys_log_dir = /node/fe/log
audit_log_dir = /node/fe/log
http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010
priority_networks = 172.28.43.0/24
tenant_ttl_scheduler_interval_seconds = 10
tablet_create_timeout_second = 30
enable_collect_query_detail_info = true
"""
    else:
        conf = """be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
starlet_port = 9070
storage_root_path = /node/be/storage
sys_log_dir = /node/be/log
priority_networks = 172.28.43.0/24
mem_limit = 3G
brpc_num_threads = 4
be_service_threads = 8
pipeline_scan_thread_pool_thread_num = 4
pipeline_exec_thread_pool_thread_num = 4
pipeline_prepare_thread_pool_thread_num = 2
pipeline_sink_io_thread_pool_thread_num = 2
tenant_ttl_policy_export_max_rows = 1000000
"""
    script = f"""
from pathlib import Path
import shutil
base=Path('/node/{role}')
for directory in ('bin','conf','log','meta','storage','lib'):
    (base/directory).mkdir(parents=True,exist_ok=True)
shutil.copytree('{ASSETS}/{role}/bin',base/'bin',dirs_exist_ok=True)
shutil.copytree('{ASSETS}/{role}/conf',base/'conf',dirs_exist_ok=True)
for source in Path('{ASSETS}/{role}/lib').iterdir():
    target=base/'lib'/source.name
    if not target.exists():
        target.symlink_to(source,target_is_directory=source.is_dir())
(base/'conf/{role}.conf').write_text({conf!r})
"""
    docker("exec", "-i", name(role, index), "python3", "-", input=script)


def start(role, index):
    node = name(role, index)
    docker("start", node)
    process_check = (
        ("pgrep", "-f", "com.starrocks.StarRocksFE") if role == "fe" else ("pgrep", "-x", "starrocks_be")
    )
    if docker("exec", node, *process_check, check=False).returncode == 0:
        return node + " process is already running; no second process started"
    if role == "fe":
        helper = "" if index == 1 else " --helper " + ip("fe", 1) + ":9010"
        command = (
            f"export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64; /node/fe/bin/start_fe.sh --daemon{helper}"
        )
        result = docker("exec", node, "bash", "-lc", command)
    else:
        command = """export STARROCKS_HOME=/node/be UDF_RUNTIME_DIR=/node/be/udf-runtime LOG_DIR=/node/be/log PID_DIR=/node/be/bin
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export LD_LIBRARY_PATH=/node/be/lib:/usr/lib/jvm/java-17-openjdk-arm64/lib/server:/node/be/lib/hadoop/native
export JEMALLOC_CONF=percpu_arena:percpu,background_thread:true
mkdir -p /node/be/udf-runtime
nohup /node/be/lib/starrocks_be >> /node/be/log/be.out 2>&1 < /dev/null &
"""
        result = docker("exec", node, "bash", "-lc", command)
    return result.stdout + result.stderr


def stop(role, index):
    # Only this isolated scenario's process/container is affected; its data volume remains intact.
    docker("stop", "-t", "5", name(role, index))


def bootstrap():
    for role in ("fe", "be"):
        for index in (1, 2, 3):
            if docker("container", "inspect", name(role, index), check=False).returncode == 0:
                raise RuntimeError(
                    "Existing round043 nodes found; use status/start/stop instead of overwriting active assets"
                )
    ARTIFACTS.mkdir(exist_ok=True)
    if docker("network", "inspect", PREFIX, check=False).returncode:
        docker(
            "network",
            "create",
            "--subnet",
            "172.28.43.0/24",
            "--label",
            "purpose=tenant-ttl-round043",
            PREFIX,
        )
    prepare_assets()
    for role in ("fe", "be"):
        for index in range(1, 4):
            create_node(role, index)
    print(start("fe", 1), flush=True)
    wait_for(lambda: sql(1, "SELECT 1"), description="first FE SQL")
    current = sql(1, "SHOW FRONTENDS")
    for index in (2, 3):
        if ip("fe", index) not in [row["IP"] for row in current]:
            sql(1, f"ALTER SYSTEM ADD FOLLOWER '{ip('fe',index)}:9010'")
        print(start("fe", index), flush=True)
    wait_for(
        lambda: len([r for r in sql(1, "SHOW FRONTENDS") if str(r["Alive"]).lower() == "true"]) == 3,
        description="three voting FEs",
    )
    current = sql(1, "SHOW BACKENDS")
    for index in range(1, 4):
        print(start("be", index), flush=True)
        if ip("be", index) not in [row["IP"] for row in current]:
            sql(1, f"ALTER SYSTEM ADD BACKEND '{ip('be',index)}:9050'")
    wait_for(
        lambda: len([r for r in sql(1, "SHOW BACKENDS") if str(r["Alive"]).lower() == "true"]) == 3,
        timeout=240,
        description="three healthy BEs",
    )
    topology = {"frontends": sql(1, "SHOW FRONTENDS"), "backends": sql(1, "SHOW BACKENDS")}
    (ARTIFACTS / "topology.json").write_text(json.dumps(topology, indent=2, default=str))
    print(json.dumps(topology, indent=2, default=str), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["bootstrap", "status", "start", "stop"])
    parser.add_argument("role", nargs="?", choices=["fe", "be"])
    parser.add_argument("index", nargs="?", type=int)
    args = parser.parse_args()
    if args.action == "bootstrap":
        bootstrap()
    elif args.action == "status":
        current = leader()
        print(
            json.dumps(
                {
                    "leader": current,
                    "frontends": sql(current, "SHOW FRONTENDS"),
                    "backends": sql(current, "SHOW BACKENDS"),
                },
                indent=2,
                default=str,
            )
        )
    else:
        print(globals()[args.action](args.role, args.index))
