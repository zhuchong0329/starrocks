#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Run the remaining matrix sequentially; a failed case stops all subsequent fault injection."""
import datetime
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
logs = ROOT / "artifacts" / "suite-logs"
logs.mkdir(exist_ok=True)
commands = [["faults.py", f"E{i:02d}"] for i in range(3, 11)]
commands += [
    ["verify.py", "--copies", "100"],
    ["faults.py", "E03", "--copies", "100"],
    ["faults.py", "E06", "--copies", "100"],
    ["edges.py"],
    ["verify.py", "--copies", "10"],
]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--start", type=int, default=0, help="zero-based matrix index to resume after an inspected failure"
)
options = parser.parse_args()
for index, args in enumerate(commands):
    if index < options.start:
        continue
    log = logs / (f"{index:02d}-" + "-".join(args).replace(".py", "") + ".log")
    if log.exists():
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
        log = log.with_name(f"{log.stem}-{stamp}.log")
    print(datetime.datetime.now(datetime.timezone.utc).isoformat(), "START", args, str(log), flush=True)
    with log.open("w") as output:
        result = subprocess.run(
            [sys.executable, "-u", str(ROOT / args[0]), *args[1:]], stdout=output, stderr=subprocess.STDOUT
        )
    print(
        datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "END",
        args,
        "exit",
        result.returncode,
        flush=True,
    )
    if result.returncode:
        sys.exit(result.returncode)
