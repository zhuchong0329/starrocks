#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Fault injection confined to the isolated round043 cluster, with no REFRESH rescue."""
import argparse
import collections
import concurrent.futures
import gzip
import json
import re
import time

import cluster as c
from verify import Case, TABLES, be_config, kept, read_rows


def healthy():
    leader = c.leader()
    return leader and all(str(r["Alive"]).lower() == "true" for r in c.sql(leader, "SHOW BACKENDS"))


def export_counts():
    result = {}
    for i in (1, 2, 3):
        output = c.docker("exec", c.name("be", i), "curl", "-s", "http://127.0.0.1:8060/vars").stdout
        result[i] = int(re.search(r"export_dictionary_cache_count\s+:\s+(\d+)", output).group(1))
    return result


def firewall(be, port, enabled, indices=(1, 2, 3)):
    for i in indices:
        action = "-I" if enabled else "-D"
        c.docker(
            "exec",
            c.name("fe", i),
            "iptables",
            action,
            "OUTPUT",
            "-d",
            c.ip("be", be),
            "-p",
            "tcp",
            "--dport",
            str(port),
            "-m",
            "comment",
            "--comment",
            "tenant-ttl-round043",
            "-j",
            "REJECT",
            "--reject-with",
            "tcp-reset",
        )


def restart_bes(case, indices):
    before = {row["IP"]: row["LastStartTime"] for row in case.sql("SHOW BACKENDS")}
    case.save("be-start-before.json", before)
    case.log("be_stop", nodes=list(indices), data_volumes_retained=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda i: c.stop("be", i), indices))
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda i: c.start("be", i), indices))

    def fresh_heartbeats():
        rows = case.sql("SHOW BACKENDS")
        by_ip = {row["IP"]: row for row in rows}
        return (
            rows
            if all(str(row["Alive"]).lower() == "true" for row in rows)
            and all(by_ip[c.ip("be", i)]["LastStartTime"] != before[c.ip("be", i)] for i in indices)
            else None
        )

    after = c.wait_for(
        fresh_heartbeats, timeout=180, description="restarted BE process observed by fresh heartbeat"
    )
    case.save("be-start-after.json", after)
    for i in indices:
        c.wait_for(lambda: be_config(i, "max_compaction_concurrency", 0), timeout=90)
    case.log("be_restarted", nodes=list(indices), export_counts=export_counts())


def record(case, label):
    value = case.status()
    case.log(label, status=value)
    return value


def follower_barrier(case, txn):
    old = c.leader()
    result = {}
    for i in (1, 2, 3):
        if i == old:
            continue

        def ready():
            rows = c.sql(i, f"SHOW TENANT TTL STATUS FROM {case.db}.{TABLES[0]}")
            return rows[0] if rows and rows[0]["DictionaryLastSuccessTxnId"] == txn else None

        result[i] = c.wait_for(ready, description="follower replays successful Dictionary transaction")
        assert result[i]["SnapshotTxnId"] is None, result[i]
    case.save("followers-before-leadership.json", result)
    return old


def switch_leader(case, txn):
    old = follower_barrier(case, txn)
    case.log("leader_stop", old=old, known_success_txn=txn)
    c.stop("fe", old)

    def elected():
        current = c.leader()
        return current if current and current != old else None

    new = c.wait_for(elected, timeout=120, description="new majority-elected Leader")
    case.leader_index = new
    case.log("leader_changed", old=old, new=new, status=case.status())
    return old, new


def restart_old_fe(case, old):
    c.start("fe", old)
    c.wait_for(lambda: c.sql(old, "SELECT 1"), timeout=180, description="old FE rejoins")
    c.wait_for(
        lambda: len([r for r in case.sql("SHOW FRONTENDS") if str(r["Alive"]).lower() == "true"]) == 3,
        timeout=180,
        description="all FEs healthy",
    )
    case.log("old_fe_rejoined", node=old)


def seed(case):
    case.setup()
    case.load(partitions=(0,), label="fresh-seed")
    case.bind()
    txn = case.wait_snapshot()

    def done():
        states = case.status()
        return (
            states
            if all(
                int(s["CompletedPhysicalPartitions"] or 0) >= 1
                and int(s["RunningReplicaTasks"] or 0) == 0
                and int(s["PendingRewritePartitions"] or 0) == 0
                for s in states.values()
            )
            else None
        )

    case.save("seed-idle.json", c.wait_for(done, timeout=180, description="fresh seed NOOP round complete"))
    old = c.leader()
    # The daemon adopts its interval at the end of a round. Let it finish one harmless seed-only round.
    # Followers retain 10s, so a newly elected Leader can evaluate immediately after snapshot publication.
    freeze = 600 if case.copies == 100 else 180
    c.config("tenant_ttl_scheduler_interval_seconds", freeze, indices=(old,))
    time.sleep(12)
    case.log("preparation_window", leader=old, interval_seconds=freeze)
    return txn


def prepare(case):
    txn = seed(case)
    case.load(partitions=range(1, 12), label="historical")
    case.baseline()
    record(case, "before_fault")
    return txn


def observe(case, predicate, timeout=600, interval=1):
    deadline = time.monotonic() + timeout
    previous = None
    while time.monotonic() < deadline:
        statuses = case.status()
        compact = {
            t: {
                k: s[k]
                for k in (
                    "BindingState",
                    "SnapshotTxnId",
                    "DictionaryLastSuccessTxnId",
                    "RunningReplicaTasks",
                    "CompletedPhysicalPartitions",
                    "ErrorMessage",
                )
            }
            for t, s in statuses.items()
        }
        encoded = json.dumps(compact, sort_keys=True)
        if encoded != previous:
            case.log("observation", status=statuses)
            previous = encoded
        if predicate(statuses):
            return statuses
        time.sleep(interval)
    raise TimeoutError("scenario observation did not reach expected state")


def finish(case, txn, changed, long_days=20):
    new = case.wait_snapshot()
    assert new > txn if changed else new == txn, (txn, new)
    case.log("txn_verified", before=txn, after=new, changed=changed)
    c.config("tenant_ttl_scheduler_interval_seconds", 10)
    case.wait_cleanup(long_days, timeout=1200)
    case.verify(long_days)
    collect_logs(case)
    case.archive()
    return case


def collect_logs(case):
    for role in ("fe", "be"):
        for i in (1, 2, 3):
            result = c.docker(
                "exec",
                c.name(role, i),
                "bash",
                "-lc",
                f"cat /node/{role}/log/{'fe.log' if role=='fe' else 'be.INFO'}",
                check=False,
            )
            with gzip.open(case.directory / f"{role}{i}.log.gz", "wt") as out:
                out.write(result.stdout + result.stderr)


def cache_recovery(scenario, copies):
    case = Case(scenario, copies)
    txn = prepare(case)
    if scenario == "E08":
        c.config("tenant_ttl_policy_snapshot_auto_recover_enabled", False)
        case.log("automatic_recovery_disabled")
    if scenario == "E09":
        case.sql(f"UPDATE {case.db}.policy SET retention_days=3 WHERE tenant LIKE 'long_%'")
        policies = case.sql(f"SELECT * FROM {case.db}.policy ORDER BY table_name,tenant")
        assert sum(r["retention_days"] == 3 and r["tenant"].startswith("long_") for r in policies) == 400
        case.save("policies-tightened.json", policies)
        case.log("policy_tightened_without_refresh")
    if scenario == "E05":
        case.sql(f"ALTER TABLE {case.db}.policy RENAME policy_unavailable")
        case.log("source_name_unavailable")
    restart_bes(case, (1, 2) if scenario == "E02" else (1, 2, 3))
    if scenario == "E04":
        firewall(1, 8060, True)
        case.log("export_rpc_rejected", node=1)
    before = export_counts()
    fault_start = time.monotonic()
    old, new = switch_leader(case, txn)
    try:
        if scenario == "E02":
            recovered = case.wait_snapshot()
            assert recovered == txn, (txn, recovered)
            after = export_counts()
            assert all(after[i] > before[i] for i in (1, 2, 3)), (before, after)
            case.log("third_node_export_verified", before=before, after=after, unchanged_txn=txn)
        elif scenario == "E04":
            saw_three = False
            max_sweeps = 0

            def ready(states):
                nonlocal saw_three, max_sweeps
                state = states[TABLES[0]]
                message = state["ErrorMessage"] or ""
                match = re.search(r"uncertainSweeps=(\d+)", message)
                sweeps = int(match.group(1)) if match else 0
                max_sweeps = max(max_sweeps, sweeps)
                if sweeps >= 3 and time.monotonic() - fault_start < 60:
                    assert state["SnapshotTxnId"] is None and "WAITING_REFRESH" not in message
                    saw_three = True
                if "WAITING_REFRESH" in message:
                    assert max_sweeps >= 3 and time.monotonic() - fault_start >= 60, (max_sweeps, message)
                    return True
                assert state["DictionaryLastSuccessTxnId"] == txn, state
                assert all(
                    s["SnapshotTxnId"] is None and int(s["RunningReplicaTasks"] or 0) == 0
                    for s in states.values()
                )
                return False

            observe(case, ready, timeout=360, interval=0.25)
            assert (
                saw_three
            ), "must observe at least three complete failed sweeps while still below 60 seconds"
            case.log(
                "network_threshold_verified",
                elapsed=time.monotonic() - fault_start,
                observed_three_sweeps_before_60_seconds=saw_three,
            )
            firewall(1, 8060, False, indices=tuple(i for i in (1, 2, 3) if i != old))
            # Stopping a container clears its network namespace and its temporary rule.
            case.log("export_network_restored")
        elif scenario == "E05":
            # Invalid source metadata takes precedence over snapshot diagnostics in SHOW TTL STATUS.
            # The appended ordinary Dictionary error proves its refresh actually failed before repair.
            observe(
                case,
                lambda s: "Getting analyzing error. Detail message: Unknown table"
                in (s[TABLES[0]]["ErrorMessage"] or ""),
                timeout=180,
            )
            case.save("failed-dictionary.json", case.sql(f"SHOW DICTIONARY {case.dictionary}"))
            assert all(
                case.sql(f"SELECT count(*) n FROM {case.db}.{t}")[0]["n"] == copies * 12000 for t in TABLES
            )
            case.sql(f"ALTER TABLE {case.db}.policy_unavailable RENAME policy")
            failed = record(case, "source_restored_without_refresh")
            message = failed[TABLES[0]]["ErrorMessage"]
            assert "lastRefresh=FAILED" in message, message
            remaining = int(re.search(r"cooldownRemainingMillis=(\d+)", message).group(1))
            assert 0 < remaining <= 300_000, message
            failed_observed = time.monotonic()
            case.log(
                "default_cooldown_remaining",
                cooldown_remaining_ms=remaining,
                estimated_failure_finished_epoch=time.time() - (300 - remaining / 1000),
            )

            def ready(states):
                state = states[TABLES[0]]
                if state["SnapshotTxnId"] is not None:
                    assert time.monotonic() - failed_observed >= (remaining / 1000) - 2
                    return True
                assert all(int(s["RunningReplicaTasks"] or 0) == 0 for s in states.values()), states
                if time.monotonic() - failed_observed < (remaining / 1000) - 2:
                    assert "WAITING_REFRESH" not in (state["ErrorMessage"] or ""), state
                return False

            observe(case, ready, timeout=420, interval=2)
            case.log("default_300s_cooldown_verified", elapsed=time.monotonic() - failed_observed)
        elif scenario == "E08":
            states = observe(
                case, lambda s: "recovery=DISABLED" in (s[TABLES[0]]["ErrorMessage"] or ""), timeout=120
            )
            for _ in range(12):
                time.sleep(2)
                states = record(case, "disabled_hold")
                assert all(
                    s["SnapshotTxnId"] is None
                    and s["DictionaryLastSuccessTxnId"] == txn
                    and int(s["RunningReplicaTasks"] or 0) == 0
                    for s in states.values()
                )
                assert all(
                    case.sql(f"SELECT count(*) n FROM {case.db}.{t}")[0]["n"] == copies * 12000
                    for t in TABLES
                )
            c.config(
                "tenant_ttl_policy_snapshot_auto_recover_enabled",
                True,
                indices=tuple(i for i in (1, 2, 3) if i != old),
            )
            case.log("automatic_recovery_enabled_without_refresh")
        restart_old_fe(case, old)
        return finish(case, txn, scenario != "E02", long_days=3 if scenario == "E09" else 20)
    except BaseException:
        collect_logs(case)
        raise


def first_rewrite(case):
    # The first rewrite uses the oldest physical id among the non-NOOP, non-DROP partitions.
    candidates = []
    for (table, tablet_id), replicas in case.initial_tablets.items():
        if table != TABLES[0]:
            continue
        rows = read_rows(case.directory / f"baseline-{table}-t{tablet_id}.gz")
        expected = [r for r in rows if kept(r, case.anchor, int(time.time()), 20)]
        if 0 < len(expected) < len(rows):
            candidates.append(
                (int(replicas[0]["PartitionId"]), int(tablet_id), table, replicas, rows, expected)
            )
    return min(candidates, key=lambda v: v[:2])


def partial_replica(case):
    _, tablet_id, table, replicas, original, expected = first_rewrite(case)

    def ready():
        counts = {}
        for replica in replicas:
            result = case.sql(
                f'SELECT event_id FROM {case.db}.{table} TABLET({tablet_id}) REPLICA({replica["ReplicaId"]})'
            )
            counts[case.backend_ips[replica["BackendId"]]] = len(result)
        return (
            counts
            if counts[c.ip("be", 1)] == len(expected)
            and counts[c.ip("be", 2)] == len(expected)
            and counts[c.ip("be", 3)] == len(original)
            else None
        )

    counts = c.wait_for(
        ready, timeout=900, interval=1, description="first two replicas committed, third untouched"
    )
    states = record(case, "partial_replica_status")
    assert int(states[table]["CompletedPhysicalPartitions"]) < 9, states
    assert int(states[table]["RunningReplicaTasks"]) > 0, states
    for replica in replicas:
        actual = case.scan_replica(table, replica, "partial")
        target = original if case.backend_ips[replica["BackendId"]] == c.ip("be", 3) else expected
        assert collections.Counter(actual) == collections.Counter(target)
    case.log("partial_replica_verified", tablet_id=tablet_id, counts=counts)


def interrupted_ttl(scenario, copies):
    case = Case(scenario, copies)
    txn = prepare(case)
    firewall(3, 9060, True)
    case.log("third_replica_task_blocked")
    c.config("tenant_ttl_scheduler_interval_seconds", 10)
    partial_replica(case)
    if scenario == "E06":
        c.stop("be", 3)
        case.log("partially_executing_be_stopped", node=3)
        time.sleep(10)
        states = record(case, "be_down_incomplete")
        assert int(states[TABLES[0]]["CompletedPhysicalPartitions"]) < 9
        firewall(3, 9060, False)
        c.start("be", 3)
        c.wait_for(healthy, timeout=180)
        c.wait_for(lambda: be_config(3, "max_compaction_concurrency", 0), timeout=90)
        case.log("same_be_data_volume_restarted", node=3)
    else:
        old, new = switch_leader(case, txn)
        firewall(3, 9060, False, indices=tuple(i for i in (1, 2, 3) if i != old))
        restart_old_fe(case, old)
    return finish(case, txn, False)


def reply_loss(copies):
    case = Case("E06", copies)
    txn = prepare(case)
    _, tablet_id, table, replicas, original, expected = first_rewrite(case)
    dictionary_id = int(case.status()[table]["DictionaryId"])
    before = next(row for row in case.sql("SHOW BACKENDS") if row["IP"] == c.ip("be", 3))
    # Block BE3's completion/report channel while FE->BE task submission and heartbeats remain available.
    for index in (1, 2, 3):
        c.docker(
            "exec",
            c.name("be", 3),
            "iptables",
            "-I",
            "OUTPUT",
            "-d",
            c.ip("fe", index),
            "-p",
            "tcp",
            "--dport",
            "9020",
            "-m",
            "comment",
            "--comment",
            "tenant-ttl-round043",
            "-j",
            "REJECT",
            "--reject-with",
            "tcp-reset",
        )
    case.log("third_replica_reply_blocked", tablet_id=tablet_id, dictionary_id=dictionary_id)
    c.config("tenant_ttl_scheduler_interval_seconds", 10)
    return complete_reply_loss(case, txn, before)


def complete_reply_loss(case, txn, before):
    _, tablet_id, table, replicas, original, expected = first_rewrite(case)
    dictionary_id = int(case.status()[table]["DictionaryId"])

    def task_lines(index, code):
        pattern = (
            f"Tenant-TTL task finished.*tablet_id={tablet_id} .*dictionary_id={dictionary_id} .*code={code} "
        )
        result = c.docker(
            "exec", c.name("be", index), "grep", "-aE", pattern, "/node/be/log/be.INFO", check=False
        )
        return result.stdout.splitlines()

    committed = c.wait_for(
        lambda: task_lines(3, "SUCCESS"),
        timeout=1000,
        interval=0.2,
        description="third BE committed, completion reply unavailable",
    )
    proofs = {index: task_lines(index, "SUCCESS") for index in (1, 2, 3)}
    assert proofs[3], proofs
    # Replica order is Catalog-dependent. BE3 may execute first and block later replicas' dispatch.
    # Require its durable commit; validate any peers that already committed without assuming their order.
    for lines in proofs.values():
        if not lines:
            continue
        line = lines[-1]
        assert int(re.search(r" kept_rows=(\d+)", line).group(1)) == len(expected), line
        assert int(re.search(r" deleted_rows=(\d+)", line).group(1)) == len(original) - len(expected), line
    task_id = re.search(r"task_id=(\d+)", committed[-1]).group(1)
    states = record(case, "committed_reply_missing_incomplete")
    assert int(states[table]["CompletedPhysicalPartitions"]) < 9, states
    assert int(states[table]["RunningReplicaTasks"]) > 0, states
    case.save("before-crash-commits.json", proofs)
    case.save(
        "before-crash-report-firewall.json",
        c.docker("exec", c.name("be", 3), "iptables", "-S", "OUTPUT").stdout,
    )
    case.log(
        "third_replica_committed_without_ack",
        tablet_id=tablet_id,
        task_id=task_id,
        kept_rows=len(expected),
        deleted_rows=len(original) - len(expected),
    )
    c.stop("be", 3)
    case.log("committed_be_stopped_before_reply", node=3)
    time.sleep(10)
    states = record(case, "committed_be_down_incomplete")
    assert int(states[table]["CompletedPhysicalPartitions"]) < 9
    c.start("be", 3)

    def restarted():
        node = next(row for row in case.sql("SHOW BACKENDS") if row["IP"] == c.ip("be", 3))
        return (
            node
            if str(node["Alive"]).lower() == "true" and node["LastStartTime"] != before["LastStartTime"]
            else None
        )

    after = c.wait_for(restarted, timeout=180, description="same BE data volume with fresh heartbeat")
    c.wait_for(lambda: be_config(3, "max_compaction_concurrency", 0), timeout=90)
    # Docker restart creates a fresh network namespace; no temporary rule may survive.
    rules = c.docker("exec", c.name("be", 3), "iptables", "-S", "OUTPUT").stdout
    assert "tenant-ttl-round043" not in rules, rules
    noop = c.wait_for(
        lambda: task_lines(3, "NOOP_VERIFIED"),
        timeout=240,
        interval=1,
        description="persisted rewrite recognized after BE restart",
    )
    assert any(re.search(r"task_id=(\d+)", line).group(1) == task_id for line in noop), noop
    case.save(
        "reply-loss-proof.json",
        {
            "tablet_id": tablet_id,
            "task_id": task_id,
            "before_backend": before,
            "after_backend": after,
            "committed": committed,
            "retried_noop": noop,
        },
    )
    case.log("restarted_be_same_task_noop_verified", tablet_id=tablet_id, task_id=task_id)
    return finish(case, txn, False)


def existing_snapshot(copies):
    case = Case("E10", copies)
    txn = seed(case)
    seed_layout = {}
    for table in TABLES:
        parts, groups = case.layout(table)
        assert len(parts) == 1 and len(groups) == 4
        case.save(f"seed-{table}-layout.json", {"partitions": parts, "tablets": groups})
        seen = set()
        for tablet_id, replicas in groups.items():
            seed_layout[(table, tablet_id)] = {(r["ReplicaId"], r["BackendId"]) for r in replicas}
            reference = None
            for replica in replicas:
                rows = case.scan_replica(table, replica, "before-cache-loss-seed")
                current = collections.Counter(rows)
                assert reference is None or current == reference
                reference = current
            for row in reference:
                assert row[0] < 10_000_000
                seen.add(case.validate_input_row(row))
        assert len(seen) == copies * 1000
    case.log("fresh_seed_all_replicas_verified_before_cache_loss", rows_per_table=copies * 1000)
    restart_bes(case, (1, 2, 3))
    before = export_counts()
    case.load(partitions=range(1, 12), label="late-historical-after-cache-loss")
    case.baseline()
    for key, replica_ids in seed_layout.items():
        assert replica_ids == {(r["ReplicaId"], r["BackendId"]) for r in case.initial_tablets[key]}
    record(case, "late_historical_before_cleanup")
    finish(case, txn, False)
    after = export_counts()
    assert after == before, (before, after)
    case.log("existing_snapshot_protected", export_counts_before=before, export_counts_after=after)
    return case


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=[f"E{i:02d}" for i in range(2, 11)])
    parser.add_argument("--copies", type=int, choices=(10, 100), default=10)
    parser.add_argument(
        "--fault-window",
        choices=("reply", "submission"),
        default="reply",
        help="E06: lose the completion reply after durable commit, or block submission",
    )
    args = parser.parse_args()
    if args.scenario == "E06" and args.fault_window == "reply":
        result = reply_loss(args.copies)
    elif args.scenario in ("E06", "E07"):
        result = interrupted_ttl(args.scenario, args.copies)
    elif args.scenario == "E10":
        result = existing_snapshot(args.copies)
    else:
        result = cache_recovery(args.scenario, args.copies)
    result.save("scenario-verdict.json", {"passed": True, "scenario": args.scenario, "copies": args.copies})
