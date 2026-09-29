#!/usr/bin/env bash
set -euo pipefail
cd /query-corruption-workspace/src/tools/query_corruption
py=/query-corruption-workspace/tools/python/bin/python
out=/query-corruption-workspace/logs/QCT-007
data=/query-corruption-workspace/logs/QCT-006
prefix=qct007-20260921
date -u
"$py" matrix.py --series "$prefix-process-restart" --variants A-baseline B-off C-on --repetitions 1 --warmup 0 --concurrency 1 --protocols mysql --query-cache off --cases small_limit empty_scan detail_page point_lookup
"$py" summarize.py "$data/$prefix-process-restart" > "$out/perf-process-restart-summary.json"
"$py" matrix.py --series "$prefix-warm-page-r1" --variants A-baseline B-off C-on --page-cache
"$py" matrix.py --series "$prefix-warm-page-r2" --variants C-on B-off A-baseline --page-cache
"$py" summarize.py "$data/$prefix-warm-page-r1" "$data/$prefix-warm-page-r2" > "$out/perf-warm-page-summary.json"
"$py" matrix.py --series "$prefix-page-cache-off-r1" --variants A-baseline B-off C-on --repetitions 200 --warmup 50 --protocols mysql --query-cache off --cases small_limit empty_scan full_scan
"$py" matrix.py --series "$prefix-focused-r1" --variants A-baseline B-off C-on --page-cache --repetitions 500 --warmup 200 --cases small_limit empty_scan detail_page
"$py" matrix.py --series "$prefix-focused-r2" --variants C-on B-off A-baseline --page-cache --repetitions 500 --warmup 200 --cases small_limit empty_scan detail_page
"$py" summarize.py "$data/$prefix-focused-r1" "$data/$prefix-focused-r2" > "$out/perf-focused-summary.json"
"$py" matrix.py --series "$prefix-page-cache-off-r2" --variants C-on B-off A-baseline --repetitions 500 --warmup 200 --protocols mysql --query-cache off --cases small_limit empty_scan full_scan
"$py" summarize.py "$data/$prefix-page-cache-off-r1" "$data/$prefix-page-cache-off-r2" > "$out/perf-page-cache-off-summary.json"
date -u
"$py" -c 'import matrix, runtime; matrix.ensure_idle_builds(); matrix.ensure_restored(); assert all(runtime.live_record(n) is None for n in runtime.NODES); print("COMPLETE: seven matrices; faults restored; all four QCT nodes stopped")'
