#!/usr/bin/env bash
set -euo pipefail
cd /query-corruption-workspace/src/tools/query_corruption
py=/query-corruption-workspace/tools/python/bin/python
root=/query-corruption-workspace/logs/QCT-006
"$py" -c 'import matrix, runtime; matrix.ensure_idle_builds(); matrix.ensure_restored(); assert all(runtime.live_record(n) is None for n in runtime.NODES)'
date -u
"$py" matrix.py --series qct007-20260921-page-cache-off-r3 --variants A-baseline B-off C-on --repetitions 200 --warmup 50 --protocols mysql --query-cache off --cases small_limit empty_scan full_scan
"$py" matrix.py --series qct007-20260921-page-cache-off-r4 --variants C-on B-off A-baseline --repetitions 200 --warmup 50 --protocols mysql --query-cache off --cases small_limit empty_scan full_scan
"$py" summarize.py "$root/qct007-20260921-page-cache-off-r1" "$root/qct007-20260921-page-cache-off-r2" "$root/qct007-20260921-page-cache-off-r3" "$root/qct007-20260921-page-cache-off-r4" > /query-corruption-workspace/logs/QCT-007/perf-page-cache-off-all-summary.json
date -u
