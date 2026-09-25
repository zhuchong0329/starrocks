# Tenant-TTL round 043: real multi-replica acceptance

These scripts use an isolated Docker network and six persistent node volumes. They do not remove volumes or build directories. The existing build-cache volume is read-only in every runtime container. Only `starrocks-tenant-ttl-e2e` writes that build cache.

The recorded 2026-09-25 run passed all 16 cases: 10,080,056 input rows, 4,510,488 kept and 5,569,568 deleted. The tracked [acceptance-result.json](acceptance-result.json) contains per-case results, binary hashes, evidence hashes and the actual unit-test counts. Full rows, Profiles and logs remain in the ignored `artifacts/` directory; `node-logs/` preserves the final snapshots of all six node log directories.

Requirements: Docker, Python 3.9+, PyMySQL (the recorded run uses 1.2.3), the cached FE package and BE binary. All FE containers and BE3 need `iptables` for the scoped RPC fault rules. `cluster.py` declares the exact network, ports, volumes, binary paths and test configuration. Do not run multiple scenarios at once on this cluster.

```sh
python3 -m venv .venv
.venv/bin/pip install 'PyMySQL==1.2.3'
.venv/bin/python cluster.py bootstrap
.venv/bin/python verify.py --copies 10
.venv/bin/python faults.py E02
.venv/bin/python run_remaining.py
.venv/bin/python audit.py
.venv/bin/python restore.py
```

`bootstrap` is for initial creation. Use `cluster.py status` to inspect an existing cluster; do not bootstrap an already running test, replace its active FE assets, or start another copy of a node process.

`run_remaining.py` runs E03–E10 with 120,000 rows per table, E01/E03/E06 with 1,200,000 rows per table, a 28-row duplicate/empty-rewrite/fallback/byte-exact-key boundary case, and a final normal regression. It stops at the first failure. Each full-scale case has two business tables sharing one Dictionary, 12 daily partitions, four buckets and three real replicas. An unbound table is a deletion-isolation control.

The oracle computes retention independently from the recorded original rows, each partition's upper boundary, the policy and the evaluation time. Expected kept/deleted records and actual records preserve NULLs, all business columns and duplicate multiplicity. Every final retained Tablet is read on all three Replica IDs. EXPLAIN and the actual scan fragment's Profile must identify the intended Tablet and BE, including empty results. Full-table rows, COUNT/MIN/MAX, and tenant/partition counts are also checked. A count match alone cannot pass. The intermediate fault probe also reads actual event rows: plain `COUNT(*)` can be folded to a whole-table FE constant and lose Tablet/Replica hints, which was reproduced on an unbound control table. It is never used to prove per-Replica contents.

The automatic List layout additionally contains an internal empty placeholder without explicit values. `SHOW PARTITIONS` lists the 12 business partitions; the scripts enumerate their tablets explicitly. The placeholder remains fail-closed and can make the table's diagnostic state `FAIL_CLOSED`; it is not counted as one of the 12 business partitions. Successful progress requires all nine (six after policy tightening) surviving business partitions to complete.

Fault preparation first binds fresh seed rows and waits for its NOOP round. It then temporarily increases only the current Leader's scheduler interval (180 seconds at the main scale, 600 at the larger scale) before loading historical rows and recording the full baseline. Followers retain the ten-second test interval. The full baseline asserts no earlier cleanup occurred. E10 loads its 110,000 historical rows only after BE caches are lost, in addition to the 10,000 fresh seed rows. All rows, including the late inserts, belong to its oracle.

E06 defaults to the committed-but-unacknowledged window: block BE3's reports to the FE RPC port, observe its durable deletion while FE progress remains incomplete, restart BE3 with its original data volume, and require the same task ID to return `NOOP_VERIFIED`. Any peers that already committed must have the expected kept/deleted counts; replica dispatch order is not fixed, so BE3 may commit before those peers start. Both data scales must pass this window and the final complete comparison on all three replicas. `--fault-window submission` additionally checks recovery when the third BE has not received the task; that case alone does not satisfy the reply-loss check. E07 changes Leader after partial replica completion and requires verified NOOP results for already completed replicas. Every BE restart waits for `LastStartTime` to change and `Alive` to be true, avoiding stale catalog heartbeats.

The tests never run manual `REFRESH DICTIONARY`, rebind, issue business-row `DELETE`, or lower replication after fault injection. Renaming the isolated policy source back, removing isolated firewall rules, and restarting the original node data volumes repair the environment. After all data checks pass, the case's Dictionary is retired so a later Leader switch does not enqueue recovery for completed cases; business tables and all evidence remain. This retirement never rescues an active case.

`artifacts/` contains inputs, DDL, policies, original/final topology, per-Replica queries, EXPLAIN, Profiles, row dumps, independent expected rows, full bidirectional differences, status transitions, node logs, binary hashes and verdicts. It is deliberately ignored by Git because the large-scale complete evidence is substantial. Failed attempts are retained. A missing passing verdict or an exception means the case is not accepted, regardless of intermediate success.

After the matrix, restore all six nodes, remove only this suite's commented firewall rules, restore FE scheduler interval to 600 seconds and BE ordinary compaction concurrency to -1, and confirm auto-recovery is enabled with the default 300-second cooldown (`python restore.py`). Retain all runtime/build volumes and logs.
