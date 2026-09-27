# Generic VARCHAR retention binding acceptance (round 050)

This suite reuses the isolated 3 FE / 3 BE cluster described in
[the round 043 runner](../multireplica/README.md). It preserves all node volumes,
old binary assets, build caches and failed-run evidence. Run one scenario at a
time. PyMySQL and the existing cluster are required; `bootstrap` is not used.

`deploy.py fe` and `deploy.py be` install versioned binary copies, change only the
isolated nodes' binary symlinks and restart those nodes with their existing data.
The FE package and non-test BE binary must be built first. The recorded build
uses the existing Debug / MAKE_TEST=OFF cache because `build_Release` currently
contains a test configuration; it never reconfigures or cleans that cache.
Use `--revision NAME` when installing another build; an existing versioned asset
is never overwritten. FE deployment checks FE SQL health, and subsequent BE
deployment checks the complete cluster's heartbeats.

```sh
python deploy.py fe
python lifecycle.py
python deploy.py be
python verify.py --copies 10
python verify.py --copies 100
python verify.py --copies 10 --compare-legacy
```

Each data run has two business tables with 12 partitions, four tablets per
partition and three real replicas. Both tables contain `dst_ip` and `device`,
with no `tenant` column. One binds `dst_ip`, the other binds `device`, and both
share a Dictionary whose first key is `policy_value`. The VARCHAR widths differ.
The inputs include 1,000 value classes, empty strings, spaces, case differences,
`001` versus `1`, IP text, Unicode, missing policy values, NULL and duplicate rows.
The two scales contain 120,000 and 1,200,000 rows **per table**.

The independent oracle applies partition-upper-bound TTL and distinguishes a
partial rewrite (preserve NULL) from whole-partition expiry. It verifies every
replica's full row multiset before and after cleanup. EXPLAIN and actual scan
Profiles must prove the requested Tablet and BE were read. Whole-table row reads
also reject omitted or misplaced rows; COUNT/MIN/MAX constants never serve as the
data oracle. An unbound control remains intact. New and legacy SHOW outputs,
empty-value resolution, and DDL rejection are checked on the real data tables.

The comparison run loads identical data and policy distributions, first uses the
omitted-property `tenant` default and then explicitly binds `dst_ip`. It records
DDL/scheduler completion times, BE TTL counter deltas and sampled FE heap usage,
and verifies the full row multisets on every replica. These sequential Debug
observations include GC, scheduler wait and cache effects; they are not a Release
benchmark or proof of a causal speed difference. TTL metrics expose rewritten
and linked artifact bytes, not a dedicated physical-read-byte counter.

To prepare baseline data while binaries are building, use `--prepare-only`.
After deployment, resume exactly the saved directory with `--resume PATH`.
A directory lacking a passing `result.json` is not a passed test.

After deploying both binaries, `python matrix.py` runs the lifecycle, both data
scales, comparison and both overlap scales sequentially, with a separate log per
scenario. `--standard-dir PATH --large-dir PATH` resumes prepared baselines. It
stops on a failed scenario and never treats a partial matrix as passed.

`lifecycle.py` checks immediate unbind, idempotence, failed re-enable remaining
unbound, ordinary rename after unbind, other-column rename, omitted-key ALTER,
and a different FE Leader replaying the binding and unbind logs.

The overlap test uses the ordinary Agent Thrift service, with no manual TTL HTTP
endpoint. Compile the test-only helper in the persistent build workspace:

```sh
mkdir -p /tenant-ttl-workspace/generic-binding-20260927/tools
# Copy DelayedRequest.java into that directory, then:
javac -cp '/tenant-ttl-workspace/fe/fe-core/target/*:/tenant-ttl-workspace/fe/fe-core/target/lib/*' \
  /tenant-ttl-workspace/generic-binding-20260927/tools/DelayedRequest.java
# Run on the host with the same Python environment as the other scenarios:
python overlap.py
python overlap.py --rows 1200000
```

`overlap.py` blocks only FE task submission to BE3 in this cluster. After BE1/BE2
have deleted with the old column, it unbinds and binds the other column, removes
the fault, and waits for the new plan to complete. It verifies the accepted
6,000/6,000/12,000-row replica divergence. It then sends a saved old-column request
to BE3 after the new plan, verifying that this late request can still delete data.
The request is explicitly constructed from the old table/schema/snapshot identity;
it is a controlled late-delivery injection, not a claim that a naturally delayed
packet was captured. The per-Tablet busy mutex is additionally covered by the
existing deterministic BE admission tests.

All temporary firewall rules are scoped with `tenant-ttl-generic-overlap` and
removed in `finally`; configuration changes are restored on exit. Full inputs,
rows, profiles, schemas, status transitions, requests and verdicts are kept in
ignored `artifacts/`. Completed bindings are retired without deleting the business
or policy tables. The post-unbind cached aggregate optimization question is
explicitly deferred; this suite neither adds a persistent guard nor claims to
resolve that boundary.
