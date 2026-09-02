# StarRocks compaction partition benchmark

| Metric | 7 x 5-minute partitions | 1 x 35-minute partition |
|---|---:|---:|
| Loaded rows | 40 | 40 |
| Raw input MiB | 0.6 | 0.6 |
| Table data MiB (final) | 1.3 | 1.2 |
| Load latency avg ms | 87.2 | 73.0 |
| Load latency p95 ms | 192.3 | 93.4 |
| Base compaction MiB | 1.3 | 1.6 |
| Cumulative compaction MiB | 0.0 | 0.0 |
| Compaction bytes / raw byte | 1.980 | 2.512 |
| Base active samples % | 9.52 | 3.23 |
| Cumulative active samples % | 0.00 | 0.00 |
| Peak total rowsets | 12 | 5 |
| Peak max rowsets/tablet | 6 | 5 |
| Peak cumulative score | 0 | 0 |
| CPU busy % | 3.13 | 3.18 |
| CPU iowait % | 0.06 | 0.04 |
| Disk busy % | 0.52 | 0.35 |
| Minimum disk free GiB | 45.31 | 45.31 |
| Drain seconds | 10.2 | 20.2 |

Global compaction counters can contain unrelated internal-table compactions. The raw CSV files are retained
for checking time windows and subtracting the measured idle baseline when needed.
