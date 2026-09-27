#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Replace binary symlinks only in the existing isolated acceptance cluster; keep all data and old assets."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "multireplica"))
import cluster as c

ASSETS = c.ROOT + "/generic-binding-20260927/assets"
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"


def deploy(role, revision="initial"):
    source = c.ROOT + ("/fe/fe-core/target/starrocks-fe.jar" if role == "fe" else "/be/output/lib/starrocks_be")
    filename = "starrocks-fe.jar" if role == "fe" else "starrocks_be"
    suffix = "" if revision == "initial" else "-" + revision
    destination = ASSETS + suffix + "/" + filename
    script = """
import hashlib,json,shutil,sys
from pathlib import Path
source,destination=map(Path,sys.argv[1:])
destination.parent.mkdir(parents=True,exist_ok=True)
if destination.exists():
    raise RuntimeError('Versioned asset already exists; do not overwrite active binary: '+str(destination))
shutil.copy2(source,destination)
h=hashlib.sha256()
with destination.open('rb') as f:
    for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
print(json.dumps({'source':str(source),'path':str(destination),'sha256':h.hexdigest(),'bytes':destination.stat().st_size}))
"""
    result = c.docker("exec", "-i", c.BUILD, "python3", "-", source, destination, input=script)
    ARTIFACTS.mkdir(exist_ok=True)
    manifest = json.loads(result.stdout)
    manifest["previous_links"] = {}
    for i in (1, 2, 3):
        # These containers use a sleep entrypoint; restarting one does not start its database process.
        c.docker("start", c.name(role, i))
        manifest["previous_links"][i] = c.docker("exec", c.name(role, i), "readlink", f"/node/{role}/lib/{filename}").stdout.strip()
    (ARTIFACTS / f"deploy-{role}{suffix}.json").write_text(json.dumps(manifest, indent=2))
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda i: c.stop(role, i), (1, 2, 3)))
    for i in (1, 2, 3):
        c.docker("start", c.name(role, i))
        c.docker("exec", c.name(role, i), "ln", "-sfn", destination, f"/node/{role}/lib/{filename}")
    for i in (1, 2, 3):
        print(c.start(role, i), flush=True)
    c.wait_for(c.leader, timeout=180, description="cluster leader after deployment")
    for i in (1, 2, 3):
        c.wait_for(lambda: c.sql(i, "SELECT 1"), timeout=180, description="FE SQL")
    if role == "be":
        c.wait_for(lambda: all(str(r["Alive"]).lower() == "true" for r in c.sql(c.leader(), "SHOW BACKENDS")),
                   timeout=180, description="all BE heartbeats")
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("fe", "be"))
    parser.add_argument("--revision", default="initial")
    args = parser.parse_args()
    deploy(args.role, args.revision)
