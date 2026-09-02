# StarRocks 分区粒度 Compaction 压测报告

## 结论

在本次固定顺序、单轮对照中，将 7 个 5 分钟分区合并成 1 个 35 分钟分区，会明显增加**持续写入期间**的 Compaction 压力：

- Compaction 处理字节从约 4,323 MiB 增加到 7,035 MiB，增加 62.7%。
- CPU busy 从 15.98% 增加到 24.64%，增加 54.2%。
- 宿主磁盘写入从约 9,100 MiB 增加到 13,412 MiB，增加 47.4%。
- Cumulative Compaction 活跃采样占比从 44.60% 增加到 69.26%，增加 24.66 个百分点。
- Compaction 内存 P95 从约 1.01 GiB 增加到 6.70 GiB；峰值从 1.72 GiB 增加到 8.67 GiB。
- Stream Load 平均延迟从 399.8 ms 增加到 451.6 ms，增加 13.0%；P95 从 505.4 ms 增加到 597.3 ms，增加 18.2%。

因此，这组数据支持如下判断：在相同总数据量、写入频率、表结构和 bucket 数下，分区变粗使同一个 tablet 在更长时间内反复参与合并，Cumulative Compaction 更活跃、单次任务更重，并对写入延迟形成可观察的影响。

这里的 `Compaction 处理字节 / 原始输入字节` 是 StarRocks Compaction counter 的放大系数，不等同于块设备层面的严格写放大。块设备 I/O 同时包含导入、WAL、索引和其他存储活动。

## 测试设计

| 项目 | 设置 |
|---|---|
| StarRocks | 4.0.11-9559176，单 FE + 单 BE |
| 细分区表 | `zc_test.http_log_5m_x7`，7 个连续 5 分钟分区 |
| 粗分区表 | `zc_test.http_log_35m_x1`，1 个 35 分钟分区 |
| 分桶 | 每个分区 1 bucket |
| 压缩 | `ZSTD(3)` |
| 动态分区 | 关闭，只保留手工创建的受控分区 |
| 表结构 | 124 个可写列、16 个生成列、14 个 NGRAM Bloom Filter 索引 |
| 大字段 | 12 KiB `responseBody`，按 90/10 分流到建索引/不建索引列 |
| 写入 | 每秒 1 次 Stream Load，每批 31 行，约 0.5 MiB/s，持续 2100 秒/表 |
| 单表负载 | 2100 次导入、65,100 行、1,105,726,473 字节原始 JSON |
| 随机性 | 固定种子 `20260828`；两表使用完全一致的确定性数据序列 |
| 时间缩放 | 生产 1 天映射为 5 分钟，`86400` 秒相应缩放为 `300` 秒 |
| 临时 BE 参数 | `base_compaction_interval_seconds_since_last_operation=300`；`base_compaction_check_interval_seconds=1` |
| 原始 BE 参数 | `86400` 和 `60`；测试结束后已恢复 |
| 执行隔离 | 先压细分区表，完全停止写入并排空 Compaction，再压粗分区表 |
| 空闲基线 | 正式压测前采集 60 秒，Compaction counter 增量为 0 |
| 磁盘保护 | Colima 数据盘扩至约 100 GiB；压测安全下限 20 GiB |

两轮各 2100 次 Stream Load 全部成功，没有 filtered row。两表最终都可查询到 65,100 行，时间戳范围均为 `1787868000` 至 `1787870099`。大字段非空分布均为 58,521/6,579，符合 90/10 设置。

## 持续写入窗口对比

该表只统计每张表的 2100 秒写入窗口，排除停写后的 drain，适合比较写入与 Compaction 同时发生时的压力。

| 指标 | 7 × 5 分钟 | 1 × 35 分钟 | 粗分区变化 |
|---|---:|---:|---:|
| Base Compaction 处理量 | 2,139.2 MiB | 2,313.7 MiB | +8.2% |
| Cumulative Compaction 处理量 | 2,184.1 MiB | 4,721.5 MiB | +116.2% |
| Compaction 总处理量 | 4,323.3 MiB | 7,035.2 MiB | +62.7% |
| Compaction 字节 / 原始输入字节 | 4.100 | 6.672 | +62.7% |
| 宿主磁盘写入 | 9,099.8 MiB | 13,411.8 MiB | +47.4% |
| CPU busy | 15.98% | 24.64% | +54.2% |
| CPU iowait | 0.090% | 0.114% | +0.024 pp |
| 磁盘 busy | 0.769% | 0.982% | +0.213 pp |
| Base 活跃采样 | 32.84% | 41.40% | +8.56 pp |
| Cumulative 活跃采样 | 44.60% | 69.26% | +24.66 pp |
| Compaction 内存 P95 | 1.01 GiB | 6.70 GiB | 6.6 倍 |
| Compaction 内存峰值 | 1.72 GiB | 8.67 GiB | 5.0 倍 |
| 最长 Base 任务 | 50.8 s | 309.6 s | 6.1 倍 |
| 最长 Cumulative 任务 | 6.9 s | 72.8 s | 10.5 倍 |
| 单 tablet 峰值 rowset | 16 | 18 | +12.5% |

两轮都没有观察到 wait queue：Base/Cumulative 的 `wait_*` P95 均为 0。也就是说，本次默认负载尚未把 Compaction worker 压到排队，但粗分区已经显著抬高资源消耗和导入延迟。

## 全阶段与 drain 观察

| 指标 | 7 × 5 分钟 | 1 × 35 分钟 |
|---|---:|---:|
| Stream Load 平均延迟 | 399.8 ms | 451.6 ms |
| Stream Load P95 | 505.4 ms | 597.3 ms |
| Stream Load 最大值 | 804.1 ms | 1,039.8 ms |
| 全阶段 Compaction 字节 / 输入字节 | 5.165 | 6.672 |
| 全阶段 CPU busy | 15.78% | 21.61% |
| 全阶段 Cumulative 活跃采样 | 37.58% | 60.30% |
| 峰值总 rowset | 55 | 18 |
| 最终总 rowset | 11 | 8 |
| 最终数据大小 | 1,744.5 MiB | 1,744.2 MiB |
| 停写后 drain | 393.0 s | 312.1 s |

`峰值总 rowset` 不能直接作为两种方案的单 tablet 压力对比：细分区有 7 个 tablet，粗分区只有 1 个。更可比的是单 tablet 峰值，分别为 16 和 18。

细分区在停写后又执行了约 1,121 MiB Base Compaction，持续到约 393 秒才满足稳定条件；这是早期分区逐个进入空闲状态后，interval 型 Base Compaction 被错峰触发的结果。粗分区在写入阶段已经持续完成较重的合并，停写后的 312 秒观察窗没有新 Compaction。粗分区 drain 更短不代表持续写入期间压力更低。

## 环境与当前状态

- 压测目标容器：`starrocks-quickstart`，镜像 `starrocks/allin1-ubuntu:4.0.11`。
  - MySQL：`127.0.0.1:9130`
  - FE HTTP：`127.0.0.1:8130`
  - BE HTTP/metrics：`127.0.0.1:8140`
- 另一套容器：`starrocks-partition-memory-20260820`，端口 `9030/8030/8040`。正式压测期间已停止，压测结束后已恢复为 healthy。
- 两套容器最终均为 healthy；目标 BE `SHOW PROC '/compactions'` 为空。
- 目标 BE 参数已恢复为 `base_compaction_interval_seconds_since_last_operation=86400` 和 `base_compaction_check_interval_seconds=60`。
- 当前 Colima 数据盘约 99 GiB，最终约 31 GiB 可用。两张测试表保留在 `zc_test` 中，便于继续检查。

## 结果边界

本次是单节点、单轮、固定顺序测试。全局 BE Compaction counter 理论上也可能包含 StarRocks 内部表任务；为降低影响，测试前确认无存量任务、采集了 counter 增量为 0 的空闲基线，并停止了同一 Colima VM 中另一套 StarRocks 容器。正式用于容量决策前，建议在目标硬件上做 3 至 5 轮重复，并增加反向顺序轮次，以消除冷热缓存和固定执行顺序的影响。
