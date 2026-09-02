# StarRocks compaction partition benchmark

| Metric | 7 x 5-minute partitions | 1 x 35-minute partition |
|---|---:|---:|
| Loaded rows | 65100 | 65100 |
| Raw input MiB | 1054.5 | 1054.5 |
| Table data MiB (final) | 1744.5 | 1744.2 |
| Load latency avg ms | 399.8 | 451.6 |
| Load latency p95 ms | 505.4 | 597.3 |
| Base compaction MiB | 3259.8 | 2313.8 |
| Cumulative compaction MiB | 2186.9 | 4721.5 |
| Compaction bytes / raw byte | 5.165 | 6.672 |
| Base active samples % | 34.07 | 36.04 |
| Cumulative active samples % | 37.58 | 60.30 |
| Peak total rowsets | 55 | 18 |
| Peak max rowsets/tablet | 16 | 18 |
| Peak cumulative score | 43 | 46 |
| CPU busy % | 15.78 | 21.61 |
| CPU iowait % | 0.08 | 0.10 |
| Disk busy % | 0.72 | 0.93 |
| Minimum disk free GiB | 35.07 | 29.15 |
| Drain seconds | 393.0 | 312.1 |

Global compaction counters can contain unrelated internal-table compactions. The raw CSV files are retained
for checking time windows and subtracting the measured idle baseline when needed.
