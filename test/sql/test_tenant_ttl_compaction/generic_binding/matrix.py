#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Run the isolated acceptance scenarios sequentially, preserving every log and result."""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--standard-dir", type=Path)
    parser.add_argument("--large-dir", type=Path)
    args = parser.parse_args()
    directory = HERE / "artifacts" / ("matrix_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S"))
    directory.mkdir(parents=True)
    stages = [("lifecycle", "lifecycle.py", [])]
    for name, copies, resume in [("standard", 10, args.standard_dir), ("large", 100, args.large_dir)]:
        options = ["--resume", str(resume.resolve())] if resume else ["--copies", str(copies)]
        stages.append((name, "verify.py", options))
    stages.extend([("comparison", "verify.py", ["--copies", "10", "--compare-legacy"]),
                   ("overlap_small", "overlap.py", []),
                   ("overlap_large", "overlap.py", ["--rows", "1200000"])])
    results = []
    for name, script, options in stages:
        command = [sys.executable, str(HERE / script), *options]
        log = directory / (name + ".log")
        started = time.monotonic()
        print(json.dumps({"stage": name, "status": "started", "log": str(log)}), flush=True)
        with log.open("w") as output:
            process = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
        entry = {"stage": name, "command": command, "exit_code": process.returncode,
                 "seconds": time.monotonic()-started, "log": log.name}
        # Scenario output ends with a JSON object containing its evidence directory.
        content = log.read_text()
        for offset, character in enumerate(content):
            if character != "{" or (offset and content[offset-1] != "\n"):
                continue
            try:
                value, _ = json.JSONDecoder().raw_decode(content[offset:])
                if "directory" in value:
                    entry["evidence_directory"] = value["directory"]
            except (ValueError, TypeError):
                pass
        if process.returncode == 0:
            evidence = Path(entry["evidence_directory"])
            verdict = json.loads((evidence / "result.json").read_text())
            assert verdict["passed"], verdict
            entry["passed"] = True
        else:
            entry["passed"] = False
        results.append(entry)
        (directory / "result.json").write_text(json.dumps({"passed": len(results) == len(stages) and
                                                         all(r["passed"] for r in results),
                                                         "stages": results}, indent=2))
        print(json.dumps(entry), flush=True)
        if process.returncode:
            raise SystemExit(process.returncode)
    print(json.dumps({"passed": True, "directory": str(directory)}), flush=True)


if __name__ == "__main__":
    main()
