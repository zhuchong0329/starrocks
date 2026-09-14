# Tenant-TTL `WAITING_REWRITE` 复测与异常定位记录

## 1. 结论

本次在 `starrocks-tenant-ttl-e2e` 容器内按《tenant ttl测试.md》的三分区场景重新构造 17 行数据，并分别用 60 秒和 1 秒调度周期复测。

定位到两个会放大观察耗时的因素，以及一个状态刷新缺陷：

1. 容器复测开始时，`tenant_ttl_scheduler_interval_seconds` 实际值是默认的 60 秒，而不是测试文档预期的 1 秒。`ADMIN SET FRONTEND CONFIG` 是运行时配置，FE 重启后未写入 `fe.conf` 的值会恢复；此外，从 60 秒动态改为 1 秒不会唤醒已经按旧周期睡眠的调度线程，本次实测残留等待为 31.410 秒。
2. 当前调度设计把“Catalog 分区删除”“rewrite 下发”“rewrite 结果回收”分在不同调度轮次执行，以保证全局同时最多有一个破坏性操作。因此数据只有 7 行也不能缩短下发前的等待，等待时间主要由 FE 调度周期决定。
3. rewrite 成功结果被回收并持久化 progress 后，`pendingRewritePlans` 和分区运行状态仍保留了本轮回收前的计算结果，导致 `SchedulerState` 虚假地再显示一个调度周期的 `WAITING_REWRITE`。这是本次修复的源码缺陷。

修复后，在 1 秒调度周期下观测到：

- `WAITING_REWRITE` 从 `22:06:35.507` 到 `22:06:36.503`，持续约 **996 ms**。
- BE 实际 rewrite 只耗时 **10.026 ms**，扫描 7 行、保留 5 行、删除 2 行。
- rewrite 结果回收后直接从 `REWRITE_RUNNING` 进入 `IDLE`，不再出现第二段虚假的 `WAITING_REWRITE`。
- 最终剩余 11 行，`PendingRewritePartitions=0`、`RunningReplicaTasks=0`、`CompletedPhysicalPartitions=2`。

## 2. 测试环境

| 项目 | 值 |
| --- | --- |
| 容器 | `starrocks-tenant-ttl-e2e` |
| 镜像 | `starrocks/dev-env-ubuntu:4.0-latest` |
| 架构 | `aarch64` |
| FE MySQL | `127.0.0.1:9030` |
| FE HTTP | `127.0.0.1:8030` |
| BE HTTP | `127.0.0.1:8040` |
| 运行目录 | `/tenant-ttl-workspace/tenant-ttl-e2e-runtime-20260911-1457` |
| 持久构建卷 | `sr-tenant-ttl-4.0-build-cache-arm64`，挂载到 `/tenant-ttl-workspace` |
| Dictionary | `tenant_ttl_policy_dict`，状态 `FINISHED` |
| 时区 | 记录中的用户侧时间为 `Asia/Shanghai`；FE/BE 文件日志为 UTC |

构建和部署期间只有该 e2e 容器以读写方式挂载上述持久卷，没有启动第二个写入者，也没有清理任何已有 BE/FE 构建缓存。

## 3. 测试数据

原文档中的固定时间戳会随日期推移而改变分区含义。本次重新选择了下面的边界，保证在 2026-09-14 复测时仍分别覆盖 Catalog drop、row rewrite 和 FE noop：

| 分区 | 范围/数据时间（Asia/Shanghai） | 行数 | 预期 |
| --- | --- | ---: | --- |
| `p_drop` | 上界 `1785933600`（2026-08-05 20:40:00）；数据时间 `1785930000` | 4 | 整分区物理删除 |
| `p_rewrite` | 上界 `1789130400`（2026-09-11 20:40:00）；数据时间 `1789126800` | 7 | 删除 2 行 `tenant_short`，保留 5 行 |
| `p_noop` | 上界 `1789648800`（2026-09-17 20:40:00）；数据时间 `1789389600` | 6 | 全部保留 |

绑定条件与原文档一致：

```sql
ALTER TABLE <test_table> SET (
    "compaction_retention_condition" =
        "dictionary_ttl('tenant_ttl_policy_dict', 'business.http_log', 20)"
);
```

每次复测使用独立表名，未复用旧 progress：

- 修复前 60 秒周期：`http_log_wait60_20260914`
- 修复前切换到 1 秒周期：`http_log_wait1_20260914`
- 修复后最终采样：`http_log_wait1_fixed2_20260914`

## 4. 修复前复测记录

### 4.1 实际配置为 60 秒

复测开始时执行：

```sql
ADMIN SHOW FRONTEND CONFIG LIKE 'tenant_ttl_scheduler_interval_seconds';
```

返回值为 `60`。这解释了为什么小数据量仍会长时间停留在等待状态：等待发生在 FE 调度轮次之间，尚未进入 BE 数据处理。

关键时间线：

| 时间（Asia/Shanghai） | 状态/事件 | pending | running | completed |
| --- | --- | ---: | ---: | ---: |
| 20:41:49.753 | FE 删除 `p_drop`；rewrite 随后等待下一轮 | 1 | 0 | 1 |
| 20:42:23（轮询开始） | `WAITING_REWRITE` | 1 | 0 | 1 |
| 20:42:49 | `REWRITE_RUNNING` | 1 | 1 | 1 |
| 20:42:49.296 | BE rewrite 完成 | - | - | - |
| 20:43:49 | `WAITING_REWRITE`（回收完成进度后留下的陈旧状态） | 1 | 0 | 2 |
| 20:44:48 | `IDLE` | 0 | 0 | 2 |

从 FE 的 `p_drop` 完成日志到 BE rewrite 完成日志相差约 **59.543 秒**。BE 日志中的实际处理统计为：

```text
scanned_rows=7 kept_rows=5 deleted_rows=2 duration_us=19185 code=SUCCESS
```

因此约 60 秒的首段等待不是由数据处理造成。完成后又出现约 59 秒陈旧 `WAITING_REWRITE`，使从首次等待到稳定 `IDLE` 总共接近 3 个 60 秒调度轮次。

### 4.2 动态切换到 1 秒的对照

执行：

```sql
ADMIN SET FRONTEND CONFIG ("tenant_ttl_scheduler_interval_seconds" = "1");
```

在刚从 60 秒改成 1 秒后立即创建并绑定对照表，得到：

| 时间（Asia/Shanghai） | 状态 | 说明 |
| --- | --- | --- |
| 20:50:13 | `NOT_EVALUATED` | 绑定完成 |
| 20:50:44 | `WAITING_REWRITE` | 距上一状态 31.410 秒，属于旧 60 秒 sleep 的剩余时间 |
| 20:50:45 | `REWRITE_RUNNING` | 首段 `WAITING_REWRITE` 实测约 1.068 秒 |
| 20:50:45.519 | BE rewrite 完成 | `duration_us=13684` |
| 20:50:46 | `WAITING_REWRITE` | 修复前的陈旧状态，约 1.078 秒 |
| 20:50:47 | `IDLE` | progress 已满足当前 plan |

该对照同时证明：

- 一旦 1 秒配置真正被调度线程采用，首段等待随之缩短到约 1 秒。
- 动态配置更新不会中断 `Daemon` 已经开始的旧周期 sleep；最坏可残留接近 60 秒。
- 即使 BE 在 13.684 ms 内完成，修复前仍会额外暴露一轮虚假的 `WAITING_REWRITE`。

## 5. 根因定位

### 5.1 调度间隔决定等待时间，和本次数据量基本无关

`Config.tenant_ttl_scheduler_interval_seconds` 默认是 60 秒。`TenantTtlScheduler.runAfterCatalogReady()` 每次醒来时读取该配置并调用 `setInterval()`，而公共 `Daemon.run()` 在一轮结束后执行 `Thread.sleep(getInterval())`。

因此在当前实现中：

- `ADMIN SET` 修改静态配置值后，正在进行的旧 sleep 不会被唤醒。
- FE 重启后，如果没有在 `fe.conf` 中持久配置 1 秒，需要重新执行 `ADMIN SET`。
- 测试中应以 `ADMIN SHOW FRONTEND CONFIG` 的实际返回值为准，不能只依赖此前执行过设置命令。

### 5.2 Catalog drop 与 rewrite 按轮次隔离是有意设计

`TenantTtlScheduler.scheduleOnce()` 中存在明确的互斥分支：本轮有 Catalog drop 时只执行第一个 drop；只有没有 drop 时才调用 `rewriteCoordinator.dispatchNext()`。rewrite 完成结果也由后续轮次的 `reconcile()` 回收。

这保证全局同时最多有一个破坏性操作，不能为了测试数据少而把 drop 和 rewrite 合并到同一轮。将测试周期配置为 1 秒是缩短 e2e 等待的正确方式。

### 5.3 完成回收后未刷新本轮 plan/status 是源码缺陷

修复前的顺序是：

1. 根据旧 progress 计算 `pendingRewritePlans` 并写入 `WAITING_REWRITE`。
2. `rewriteCoordinator.reconcile()` 发现 BE 已完成，持久化新 progress 并移除执行。
3. 本轮继续对外暴露步骤 1 的 pending/status。
4. 直到下一调度轮次重新计算，才变成 `IDLE`。

这就是第二段 `WAITING_REWRITE`。它不代表还存在 rewrite 工作，只是状态快照比刚持久化的 progress 落后一轮。

## 6. 源码改动

### 6.1 调度器

文件：`fe/fe-core/src/main/java/com/starrocks/tenantttl/scheduler/TenantTtlScheduler.java`

在 coordinator `reconcile()` 之后，使用刚持久化的 progress 对本轮 pending plan 再执行一次纯判断：

- 如果仍有真实触发原因（例如回收期间又产生更高 data version，或仍在 retry），保留 pending plan。
- 如果所有触发条件都已满足，把分区状态更新为 `IDLE`，并立即从对外暴露的 `pendingRewritePlans` 中移除。

这样既消除陈旧状态，又不会吞掉并发 late write、policy change、expiry event 或 retry。

### 6.2 回归测试

文件：`fe/fe-core/src/test/java/com/starrocks/tenantttl/scheduler/TenantTtlEndToEndTest.java`

原测试在两个 replica task 成功后，还额外调用一次 `scheduleOnce()` 才断言 pending 为空。现在移除这次额外调度，并在完成结果被 `reconcile()` 的同一轮直接断言：

- `getPendingRewritePlans().isEmpty()`；
- 对应 physical partition 状态为 `IDLE`。

### 6.3 日志策略

本次没有新增 INFO/WARN 日志。现有日志已包含定位所需的信息：

- FE `LocalMetastore.dropPartition()` 的分区删除完成时间；
- BE `engine_tenant_ttl_compaction_task.cpp` 的 task id、扫描/保留/删除行数和 `duration_us`。

新增每轮 scheduler 日志会在正常运行时持续产生噪声，因此没有为了本次定位保留临时日志。

## 7. 修复后 e2e 复测

### 7.1 部署

1. 将两处改动同步到持久构建工作区 `/tenant-ttl-workspace`。
2. 在 `starrocks-tenant-ttl-e2e` 内执行 `./build.sh --fe`，增量构建成功。
3. 备份旧运行包为 `fe/lib/starrocks-fe.jar.pre-032`。
4. 用 `/tenant-ttl-workspace/output/fe/lib/starrocks-fe.jar` 替换运行包，只重启 FE；BE 未重启、未重建。
5. FE 启动后重新设置并确认 `tenant_ttl_scheduler_interval_seconds=1`，Dictionary 状态为 `FINISHED`。

### 7.2 50 ms 轮询时间线

| 时间（Asia/Shanghai） | 状态/事件 | pending | running | completed |
| --- | --- | ---: | ---: | ---: |
| 22:06:35.092 | 开始绑定 | - | - | - |
| 22:06:35.149 | 绑定完成 | - | - | - |
| 22:06:35.180 | `NOT_EVALUATED` | 0 | 0 | 0 |
| 22:06:35.507 | `WAITING_REWRITE` | 1 | 0 | 1 |
| 22:06:35.527 | FE 完成 `p_drop` 删除 | - | - | - |
| 22:06:36.503 | `REWRITE_RUNNING` | 1 | 1 | 1 |
| 22:06:36.540 | BE rewrite 完成 | - | - | - |
| 22:06:37.447 | `IDLE` | 0 | 0 | 2 |

计算结果：

- `WAITING_REWRITE` 持续：`36.503 - 35.507 = 0.996` 秒。
- 从 BE 完成到 FE 下一轮回收并显示 `IDLE`：约 0.907 秒，这是 1 秒轮询回收周期。
- 完成后没有任何第二段 `WAITING_REWRITE`。

BE 完成日志：

```text
Tenant-TTL task finished ... scanned_rows=7 kept_rows=5 deleted_rows=2
... rewritten_bytes=1190 duration_us=10026 code=SUCCESS detail_status=OK
```

最终状态：

```text
SchedulerState             IDLE
PendingRewritePartitions   0
RunningReplicaTasks        0
CompletedPhysicalPartitions 2
IgnoredZeroRows            1
ErrorMessage               NULL
```

最终数据：

```text
final_rows = 11

<NULL>           2
Tenant_Short     2
tenant_fallback  2
tenant_long      2
tenant_short     1
tenant_zero      2
```

结果与预期一致：`p_drop` 的 4 行随分区删除，`p_rewrite` 删除 2 行 `tenant_short`，`p_noop` 保留 6 行。

## 8. 验证结果

实际执行的自动化验证：

```text
./run-fe-ut.sh --test \
  "com.starrocks.tenantttl.scheduler.TenantTtlEndToEndTest,\
com.starrocks.tenantttl.policy.TenantTtlSchedulerTest,\
com.starrocks.tenantttl.policy.TenantTtlStatusServiceTest" -j1

Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

其中：

- `TenantTtlEndToEndTest`: 1 个用例通过。
- `TenantTtlSchedulerTest`: 2 个用例通过。
- `TenantTtlStatusServiceTest`: 4 个用例通过。
- `git diff --check`: 通过。
- `./build.sh --fe`: 通过，`Successfully build StarRocks Frontend`。
- 修复后容器 e2e：17 行变为 11 行，状态稳定为 `IDLE`。

本次没有运行完整 FE 测试集和 BE 单测：改动仅位于 FE scheduler 状态刷新路径，已执行直接相关的 scheduler、status 和端到端 FE 用例，并复用了现有 BE 二进制完成容器 e2e 验证。

## 9. 后续测试注意事项

1. 每次 FE 重启后先执行并检查：

   ```sql
   ADMIN SET FRONTEND CONFIG ("tenant_ttl_scheduler_interval_seconds" = "1");
   ADMIN SHOW FRONTEND CONFIG LIKE 'tenant_ttl_scheduler_interval_seconds';
   ```

2. 刚从 60 秒动态改为 1 秒时，旧 sleep 最多仍可能残留近 60 秒。需要完全可重复的短周期测试时，应在 FE 启动前把 1 秒写入该测试实例的 `fe.conf`；或者设置后等待旧周期结束，再创建和绑定测试表。
3. 固定 epoch 时间戳会随测试日期老化。复测前应重新计算三段边界，尤其要保证 `p_noop` 的数据时间仍处于最短正数 TTL 内。
4. 判断 BE 是否慢，应以 `Tenant-TTL task finished` 的 `duration_us` 为准；`REWRITE_RUNNING` 是 FE 轮询状态，其持续时间包含下一轮结果回收等待。
