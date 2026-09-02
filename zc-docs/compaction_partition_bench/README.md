# StarRocks partition-granularity compaction benchmark

This tool compresses a seven-day partition experiment into 35 minutes:

- `zc_test.http_log_5m_x7`: seven consecutive five-minute partitions.
- `zc_test.http_log_35m_x1`: one partition covering the same 35-minute range.
- One real five-minute interval represents one production day, so
  `base_compaction_interval_seconds_since_last_operation` is temporarily scaled from 86400 to 300.
- The two stages use the same deterministic JSON rows and Stream Load transaction cadence. They always run
  sequentially, and the second stage starts only after the first table and all global compaction workers are idle.

The schema follows the supplied production `http_log` DDL: 124 loadable columns plus generated columns, ARRAY and
LARGEINT fields, 14 NGRAM Bloom Filter indexes, expression partitioning by `from_unixtime(recordTimestamp)`, the
original duplicate/sort keys, and one hash bucket per partition. Automatic DAY dynamic partitioning is deliberately
disabled so that no partitions outside the controlled 35-minute window are created during the comparison.
`recordTimestamp`, `recordTime`, partition boundaries, and Stream Load all use `Asia/Shanghai`, matching the supplied
sample (`1787868576` = `2026-08-28 06:09:36` in UTC+8).

## Requirements

- `mysql` command-line client
- Python 3.8+
- A StarRocks FE query port, FE HTTP port, and BE metrics endpoint reachable from the runner
- A user allowed to create tables, Stream Load data, read `information_schema.be_configs`, and update mutable BE configs

## Full reproduction

From the StarRocks repository root:

```bash
python3 zc-docs/compaction_partition_bench/compaction_partition_bench.py \
  --mysql-host 127.0.0.1 \
  --mysql-port 9130 \
  --fe-http-url http://127.0.0.1:8130 \
  --be-http-url http://127.0.0.1:8140 \
  --be-metrics-url http://127.0.0.1:8140/metrics \
  --recreate-tables
```

Defaults are one Stream Load per second, 0.5 MiB/s raw JSON, 35 minutes per table, a 300-second Base Compaction
interval, a one-second Base Compaction check interval, ZSTD table compression, and a 20 GiB free-disk safety floor.

Set a password without putting it in the process list:

```bash
export STARROCKS_PASSWORD='your-password'
python3 zc-docs/compaction_partition_bench/compaction_partition_bench.py --recreate-tables
```

For a short format and connectivity check, use a new/empty test database and a small time scale:

```bash
python3 zc-docs/compaction_partition_bench/compaction_partition_bench.py \
  --duration-seconds 10 \
  --baseline-seconds 2 \
  --base-interval 5 \
  --base-check 1 \
  --drain-extra-seconds 2 \
  --drain-stable-seconds 10 \
  --drain-timeout-seconds 90 \
  --target-mib-per-second 0.05 \
  --min-disk-free-gib 1 \
  --recreate-tables
```

`--recreate-tables` drops only the two benchmark-owned tables. Without that option, the program refuses to reuse
an existing benchmark table.

## Output

Each run gets a timestamped directory below `results/` with:

- `manifest.json`: arguments and before/after configuration intent
- `restore_config.sql`: emergency manual restoration statements
- `baseline/metrics.csv`: idle global BE baseline
- one directory per table containing `loads.csv`, `metrics.csv`, `tablets.csv`, and `result.json`
- `summary.json` and `summary.md`: comparison results

The script restores both modified BE settings in a `finally` block on success, load failure, safety stop, Ctrl-C, or
SIGTERM. SIGKILL and machine loss cannot run cleanup; use the generated `restore_config.sql` in that case.

Global BE Compaction counters also include internal tables. The idle baseline and raw per-second CSV are retained so
that unrelated background activity is visible rather than silently attributed to either benchmark table.
