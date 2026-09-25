#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Audit recorded verdicts and evidence completeness; never manufactures missing test results."""
import argparse
import json
import re

import cluster as c


def completed(directory):
    if not (directory / "data-verdict.json").exists():
        return None
    meta = json.loads((directory / "case.json").read_text())
    data = json.loads((directory / "data-verdict.json").read_text())
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    assert data["passed"] is True
    archived = [e for e in events if e["event"] == "archived_dictionary"]
    if not archived:
        return None
    # E01/E02/E03 predate the wrapper verdict. Their successful archive follows all functional assertions.
    wrapper = directory / "scenario-verdict.json"
    if wrapper.exists():
        assert json.loads(wrapper.read_text())["passed"] is True
    elif meta["scenario"] not in ("E01", "E02", "E03"):
        return None
    final = json.loads((directory / "final-status.json").read_text())
    if meta["scenario"] == "EDGE":
        assert final["events_a"]["TableDefaultMatch"] == "MATCHED", final
        assert final["events_b"]["TableDefaultMatch"] == "NOT_FOUND", final
        assert all(table["rewritten_empty_replicas"] >= 3 for table in data["tables"].values())
    reply_loss = directory / "reply-loss-proof.json"
    if reply_loss.exists():
        proof = json.loads(reply_loss.read_text())
        assert proof["before_backend"]["LastStartTime"] != proof["after_backend"]["LastStartTime"]
        assert proof["before_backend"]["BackendId"] == proof["after_backend"]["BackendId"]
        for key, code in (("committed", "SUCCESS"), ("retried_noop", "NOOP_VERIFIED")):
            assert any(
                f" code={code} " in line and re.search(r"task_id=(\d+)", line).group(1) == proof["task_id"]
                for line in proof[key]
            ), proof
    profile_count = 0
    before_count = 0
    total_input = 0
    total_kept = 0
    for table, verdict in data["tables"].items():
        assert final[table]["BindingState"] == "ACTIVE"
        assert final[table]["SnapshotTxnId"] == final[table]["DictionaryLastSuccessTxnId"]
        count = verdict["checked_replicas"]
        assert len(list(directory.glob(f"after-{table}-*.profile.gz"))) == count
        assert len(list(directory.glob(f"after-{table}-*.rows.gz"))) == count
        differences = list(directory.glob(f"diff-{table}-*.json"))
        assert len(differences) == count
        for path in differences:
            difference = json.loads(path.read_text())
            assert not difference["extra"] and not difference["missing"], path
        layout = json.loads((directory / f"baseline-{table}-layout.json").read_text())
        baseline_replicas = sum(len(replicas) for replicas in layout["tablets"].values())
        assert len(list(directory.glob(f"before-{table}-*.profile.gz"))) == baseline_replicas
        assert len(list(directory.glob(f"baseline-{table}-t*.gz"))) == len(layout["tablets"])
        assert len(list(directory.glob(f"expected-kept-{table}-t*.gz"))) == len(layout["tablets"])
        assert len(list(directory.glob(f"expected-deleted-{table}-t*.gz"))) == len(layout["tablets"])
        if meta["scenario"] != "EDGE":
            assert (directory / f"grouped-{table}.json").exists()
            assert (directory / f"aggregate-{table}.json").exists()
            total_input += meta["copies"] * 12000
            total_kept += verdict["rows"]
        else:
            total_input += verdict["input"]
            total_kept += verdict["kept"]
        before_count += baseline_replicas
        profile_count += count
    return {
        "scenario": meta["scenario"],
        "fault_window": (
            "committed_reply_lost"
            if reply_loss.exists()
            else "submission_blocked" if meta["scenario"] == "E06" else None
        ),
        "copies": meta["copies"],
        "directory": str(directory),
        "input_rows": total_input,
        "kept_rows": total_kept,
        "deleted_rows": total_input - total_kept,
        "baseline_replicas": before_count,
        "final_replicas": profile_count,
        "tables": data["tables"],
        "completed_at": archived[-1]["time"],
        "no_reissue_checked": (directory / "no-reissue.json").exists(),
    }


def audit(allow_incomplete=False):
    cases = []
    for directory in sorted(c.ARTIFACTS.glob("ttl_r043_*")):
        result = completed(directory)
        if result:
            cases.append(result)
    required = (
        [(f"E{i:02d}", 10) for i in range(1, 11)] + [(s, 100) for s in ("E01", "E03", "E06")] + [("EDGE", 10)]
    )
    pending = [
        {"scenario": s, "copies": n}
        for s, n in required
        if not any(row["scenario"] == s and row["copies"] == n for row in cases)
    ]
    for copies in (10, 100):
        if not any(
            row["scenario"] == "E06"
            and row["copies"] == copies
            and row["fault_window"] == "committed_reply_lost"
            for row in cases
        ):
            pending.append({"scenario": "E06 committed reply lost / BE restart", "copies": copies})
    normals = [row for row in cases if row["scenario"] == "E01" and row["copies"] == 10]
    final_normal = len(normals) >= 2 and normals[-1]["completed_at"] == max(
        row["completed_at"] for row in cases
    )
    if not final_normal:
        pending.append({"scenario": "final normal regression", "copies": 10})
    for copies in (10, 100):
        if not any(
            row["scenario"] == "E01" and row["copies"] == copies and row["no_reissue_checked"]
            for row in cases
        ):
            pending.append({"scenario": "E01 no reissue after complete", "copies": copies})
    summary = {
        "passed": not pending,
        "completed": cases,
        "pending": pending,
        "input_rows": sum(row["input_rows"] for row in cases),
        "kept_rows": sum(row["kept_rows"] for row in cases),
        "deleted_rows": sum(row["deleted_rows"] for row in cases),
        "baseline_replica_reads": sum(row["baseline_replicas"] for row in cases),
        "final_replica_reads": sum(row["final_replicas"] for row in cases),
    }
    (c.ARTIFACTS / "acceptance-summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps({key: value for key, value in summary.items() if key != "completed"}, indent=2))
    if not allow_incomplete:
        assert summary["passed"], "required acceptance scenarios remain incomplete"
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    audit(args.allow_incomplete)
