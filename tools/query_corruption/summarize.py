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

"""Read completed QCT measurements; emit measured comparisons, never a pass threshold.

Keeps each repeated run separate. Pooled latency percentiles do not manufacture
extra independent runs; per-run throughput and resource deltas remain separate.
"""

import argparse
from collections import defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

import benchmark


def metric(text, name):
    if not isinstance(text, str):
        return None
    matches = re.findall(r"^" + re.escape(name) + r"(?:\{[^\n]*\})?\s+([-+.0-9eE]+)$", text, re.MULTILINE)
    return sum(float(value) for value in matches) if matches else None


def resources(before, after):
    seconds = (datetime.fromisoformat(after["utc"]) - datetime.fromisoformat(before["utc"])).total_seconds()
    if seconds <= 0:
        raise ValueError("Resource snapshot timestamps did not increase")
    result = {"snapshot_wall_seconds": seconds, "nodes": {}, "query_cache": {}}
    for node, last in after["nodes"].items():
        first = before["nodes"][node]
        if (first["pid"], first["start_ticks"]) != (last["pid"], last["start_ticks"]):
            raise ValueError("Node restarted during measurement")
        cpu = last["cpu_seconds"] - first["cpu_seconds"]
        if cpu < 0:
            raise ValueError("CPU counter decreased")
        result["nodes"][node] = {"cpu_seconds": cpu, "average_cores": cpu / seconds,
                                 "rss_before_kib": first["memory_kib"]["VmRSS"],
                                 "rss_after_kib": last["memory_kib"]["VmRSS"]}
    result["server_average_cores"] = sum(node["average_cores"] for node in result["nodes"].values())
    if "client_process" in before and "client_process" in after:
        result["client_cpu_seconds"] = after["client_process"]["cpu_seconds"] - before["client_process"]["cpu_seconds"]
    for port in ("19400", "19500", "19600"):
        counters = {}
        for name in ("query_cache_lookup_count", "query_cache_hit_count"):
            first = metric(before.get(port), "starrocks_be_" + name)
            last = metric(after.get(port), "starrocks_be_" + name)
            counters[name + "_delta"] = None if first is None or last is None else last - first
        result["query_cache"][port] = counters
    return result


def load_run(path):
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if len(records) < 3 or records[0]["type"] != "metadata" or records[-1]["type"] != "summary":
        raise ValueError("Incomplete run: " + str(path))
    metadata, final = records[0], records[-1]
    samples = records[1:-1]
    if any(sample["type"] != "sample" for sample in samples):
        raise ValueError("Unexpected measurement record")
    expected = metadata["settings"]["concurrency"] * metadata["settings"]["repetitions"] * len(metadata["plans"])
    if len(samples) != expected:
        raise ValueError("Measured sample count does not match declared experiment")
    for sample in samples:
        benchmark.check_healthy_sample(sample)
    if benchmark.summarize(samples) != final["measured"]:
        raise ValueError("Stored percentiles do not match raw samples")
    return metadata, samples, final


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    args = parser.parse_args()
    groups, plans = defaultdict(list), defaultdict(set)
    runs = []
    for directory in args.directories:
        files = sorted(directory.glob("*.jsonl"))
        if not files:
            raise ValueError("No measurement files: " + str(directory))
        for path in files:
            metadata, samples, final = load_run(path)
            settings = metadata["settings"]
            key = (metadata["variant"], metadata["protocol"], settings["cache"], settings["concurrency"],
                   metadata["runtime_selection"]["storage_page_cache"])
            groups[key].extend(samples)
            for case, plan in metadata["plans"].items():
                # Compare exactly for the same SQL/cache/DOP, independently of variant and run order.
                plans[(metadata["protocol"], settings["cache"], settings["dop"], case)].add(
                    hashlib.sha256(plan.encode()).hexdigest())
            runs.append({"file": str(path), "variant": metadata["variant"], "protocol": metadata["protocol"],
                         "settings": settings, "storage_page_cache": key[4], "measured": final["measured"],
                         "throughput": benchmark.measured_throughput(samples),
                         "resources_including_warmup": resources(metadata["server_resources_before"],
                                                                   final["server_resources_after"])})
    output = {"runs": runs,
              "plan_comparison": [{"protocol": key[0], "query_cache": key[1], "dop": key[2], "case": key[3],
                                    "distinct_plan_sha256": sorted(value)} for key, value in sorted(plans.items())],
              "pooled_latency": [{"variant": key[0], "protocol": key[1], "query_cache": key[2],
                                  "concurrency": key[3], "storage_page_cache": key[4],
                                  "measured": benchmark.summarize(value)} for key, value in sorted(groups.items())]}
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
