# 第 033 轮：日粒度 date_trunc 自动 List 分区

日期：2026-09-16。需求依据：FE 澄清文档 FE-TIME-006，用户已确认实现及本地集群升级。

## 1. 交付范围

支持以下自动 List 分区的 CREATE/ALTER Tenant-TTL 绑定：

```sql
PARTITION BY date_trunc('day', from_unixtime(recordTimestamp))
PARTITION BY (tenant_bucket, date_trunc('day', from_unixtime(recordTimestamp)))
PARTITION BY (tenant_bucket, date_trunc('day', CAST(from_unixtime(recordTimestamp) AS DATETIME)))
```

生产代码仅涉及两个类：

- `TenantTtlBindingAnalyzer`：增加 `LIST_DATE_TRUNC_DAY_FROM_UNIXTIME`，严格匹配 day 字面量、recordTimestamp 身份及可选的单层 DATETIME CAST。原始生成列表达式未完成类型分析，因此显式 CAST 读取 `TypeDef`，不能只检查 `getType()`。
- `TenantTtlPartitionBoundResolver`：严格校验 DATETIME 日零点，通过绑定时区的 `date.plusDays(1).atStartOfDay(zone)` 推导上界；多值取最大上界，任一非法值使整个物理分区 fail-closed。

复用现有绑定格式版本 1、时区声明、持久化、评估上下文、任务协议和调度流程。没有修改通用解析器、写入路径、BE 或 DDL 生成工具。显式 `RANGE(date_trunc(...))`、其他粒度和其他时间源仍不在本轮范围内。

## 2. 自动化验证

在现有 `starrocks-tenant-ttl-e2e` 容器中复用持久卷 `sr-tenant-ttl-4.0-build-cache-arm64`；没有启动第二个卷写入者，没有改动任何 BE 构建目录。

| 验证 | 实际结果 |
| --- | --- |
| 全部 `*TenantTtl*Test`，14 个测试类 | 83 个用例，0 failures / errors / skipped |
| `mvn -pl fe-core -DskipTests checkstyle:check` | 0 violations，BUILD SUCCESS |
| `./build.sh --fe -j1` | BUILD SUCCESS，68 秒；仅 FE |
| `git diff --check` | 通过 |

新增 6 个测试方法，包含多个子场景：

1. 真实 CREATE/ALTER、单列/多列表达式、DAY 大小写、显式 CAST、规范化时区、缺失时区拒绝及无部分绑定、动态分区时区冲突复核。
2. SHOW CREATE、TableProperty JSON 和 ModifyTablePropertyOperationLog round-trip；既有四类绑定随整套测试一起回归。
3. 拒绝 hour/month/year、非字面量、错误时间列、格式化 from_unixtime、时间算术、DATE CAST、双层 CAST、额外 date_trunc、重复时间分量和非普通列的 bucket 分量。
4. DST 23/25 小时日、闰日、跨月/年、多 tuple 最大上界、非午夜（小时/分钟/秒/微秒）、错误字面量类型、NULL、缺失/空值、非法日期及非法时区。
5. 到期前一秒/恰好到期/后一秒的计划，以及 DELETE_LIST → KEEP_LIST → DROP；与 `%Y%m%d` 得到相同边界和计划指纹；完成后下一天不重复调度；自动 List 空占位分区 fail-closed 不阻止其他分区评估。

实际全量测试命令：

```bash
cd /tenant-ttl-workspace
./run-fe-ut.sh --test '*TenantTtl*Test' -j1
cd fe
mvn -pl fe-core -DskipTests checkstyle:check
cd ..
./build.sh --fe -j1
```

注意：现有 `run-fe-ut.sh` 间接进入 POM 的 `auto-clean`，本次发现它清理了 FE 的 target 并重新编译；运行产物、集群数据和 BE 缓存未受影响。本轮已重新生成 FE target 并保留。后续复测应显式跳过该阶段，使用：

```bash
CUSTOM_MVN='mvn -Dmaven.clean.skip=true' ./run-fe-ut.sh --test '*TenantTtl*Test' -j1
```

首轮测试暴露的原始 CAST 类型问题已修复；第二轮发现 ALTER 将 DdlException 包装为 AlterJobException，已修正测试断言。上表为最终代码的整套通过结果，不将中间失败算作通过。

未执行全部 FE 单测、BE 重新编译/单测、多 FE 切主、混合版本、显式 Range 扩展或 Dictionary Cache 的 BE 重启补齐测试。本轮没有这些实现变更。

## 3. 本地集群升级

运行目录：`/tenant-ttl-workspace/tenant-ttl-e2e-runtime-20260911-1457`。

- 保留原 FE jar：`fe/lib/starrocks-fe.jar.pre-033-20260916`。
- FE 停机后备份元数据：`/tenant-ttl-workspace/round033-fe-meta-pre-upgrade.tar.gz`（源目录约 41 MB）。
- 仅用本轮 `output/fe/lib/starrocks-fe.jar` 替换运行 FE jar；未替换 BE、未重建集群、未清空任何数据库。
- 新 FE jar SHA-256：`d5f55fedca7bd8f568e9089ac87f47aeec2b9a546e6347d87e12a4a9d22ade43`。
- 原 FE jar SHA-256：`7014cf332c1a63363bae008c85a0bb934eb7b2be8ea61e8ab344c10161a12513`。
- BE 一直保留 PID 1045、启动时间 `2026-09-11 15:55:55`（服务端 UTC）。原二进制 SHA-256：`d245c04c4a0b21c599c59a1ec1de2c661f3d09995e170a3d0a626b2512588380`。
- 使用既有 stop/start 脚本。停止脚本的 `kill -0` 等待达到 45 秒时提示 forced termination；重启前已确认没有运行中的 StarRocksFE 服务进程。启动后 FE/BE 均 Alive，原数据库保留。
- 升级后及恢复验证重启后，恢复升级前的运行时测试配置 `tenant_ttl_scheduler_interval_seconds=1`；未改持久化 fe.conf，未来重启仍会恢复默认 60 秒。

本轮只重启 FE；不能据此推断已解决 BE/CN 重启后的 Dictionary Cache 补齐问题。

## 4. MySQL client 逐步复测

入口仍为 `127.0.0.1:9030`，用户 root，当前本地测试环境无密码。以下脚本没有 DROP/TRUNCATE，首次运行使用独立库 `tenant_ttl_r033_20260916` 和 Dictionary `tenant_ttl_r033_dict`。再次完整复测应先在脚本副本中替换为新的库名和全局 Dictionary 名，避免覆盖已有结果。

### 步骤一：策略、表、数据与绑定

执行仓库中的 `zc-docs/tenant_ttl_round033_setup.sql`：

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round033_setup.sql
```

必须保留 `--skip-comments`：本次本地 MySQL 8.4 客户端默认将独立注释发往服务端，触发 comment-only SQL 解析错误；过滤注释后成功，不涉及数据库代码修改。

脚本每个动作及实测结果：

| 动作 | 实测结果 |
| --- | --- |
| 创建三列 PRIMARY KEY 策略表并写入三行 | default=10、short=2、long=20，table_key=`round033.events` |
| 创建双 KEY Dictionary | 成功，Dictionary ID=4；普通刷新及 FE 快照随后成功 |
| 创建尚未绑定的 `events_single` | 成功，日粒度自动 List |
| 写入 0/5/15/40 天前四组数据，每组 short/long/other/NULL | 共 16 行，绑定前每组查询均为 4 行 |
| 记录原分区和 Tablet，再创建已绑定的 `events_multi_cast` | 多列自动 List + 显式 DATETIME CAST 成功；写入同样 16 行，bucket=7 |
| 对 `events_single` 执行 ALTER SET 绑定 | 成功，未产生 Schema Change 作业，保留分区/Tablet 身份 |
| SHOW CREATE / SHOW TENANT TTL STATUS | 两表均显示新类型、Asia/Shanghai 和配置的 Dictionary/table_key；初始允许 WAITING_DICTIONARY |

属性默认值为 30 天；策略的 default=10 天优先，NULL 使用有效默认与全部 override 的最大值，即 20 天。写入会话显式设为 Asia/Shanghai，与绑定声明一致。

### 步骤二：等待刷新和调度收敛并检查结果

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round033_verify.sql
```

脚本可反复执行，只读。不要以绑定刚返回时的 WAITING 状态判定失败：Dictionary 刷新/导出和 Tenant-TTL 调度是异步链路，动态缩短调度周期也不会唤醒旧的 sleep。

两表的最终结果完全一致：

| 场景 | 本次日期 | 初始行数 | 最终行数 | 剩余 tenant |
| --- | --- | ---: | ---: | --- |
| NOOP | 2026-09-16 | 4 | 4 | short、long、other、NULL |
| DELETE_LIST | 2026-09-11 | 4 | 3 | long、other、NULL |
| KEEP_LIST | 2026-09-01 | 4 | 2 | long、NULL |
| DROP | 2026-08-07 | 4 | 0 | 整个业务分区从 Catalog 删除 |
| 合计 | | 16 | 9 | |

`events_single` 保留的分区 ID 为 871054、871047、871042，Tablet ID 为 871055、871051、871043，与绑定前一致；已过期分区 871048 被正常删除。`SHOW ALTER TABLE COLUMN` 返回空集。

BE 的实际执行日志：

| 表 / 场景 | Tablet ID | scanned / kept / deleted | 结果 |
| --- | ---: | --- | --- |
| single / DELETE_LIST | 871043 | 4 / 3 / 1 | SUCCESS |
| single / KEEP_LIST | 871055 | 4 / 2 / 2 | SUCCESS |
| multi_cast / KEEP_LIST | 871067 | 4 / 2 / 2 | SUCCESS |
| multi_cast / DELETE_LIST | 871078 | 4 / 3 / 1 | SUCCESS |

本次 rewrite 固定使用快照事务 9；查询收敛状态时 Dictionary/FE 快照均已到事务 10。相同策略刷新后无需重复 rewrite，这是既有语义指纹机制的预期行为。

### 步骤三：理解空占位分区的状态

自动 List 表内部存在无显式 List 值的空占位分区。它被现有边界规则判为 `DEFAULT_OR_MISSING_LIST_VALUES`，不会下发清理任务。它也不出现在普通 SHOW PARTITIONS 的业务分区列表中。

因此本次两表出现：

```text
BindingState = ACTIVE
PartitionExpressionType = LIST_DATE_TRUNC_DAY_FROM_UNIXTIME
ProvablePhysicalPartitions = 3
UnprovablePhysicalPartitions = 1
SchedulerState = FAIL_CLOSED
PendingRewritePartitions = 0
RunningReplicaTasks = 0
CompletedPhysicalPartitions = 3
```

这里的 FAIL_CLOSED 是空占位分区的聚合诊断，不表示整个表未执行清理；三份可证明业务分区均已完成。没有为改善状态展示而放宽已确认的缺失值 fail-closed 规则。后续如希望忽略内部占位分区的展示，需要单独对齐。

### 步骤四：FE 重启恢复

完成清理后，再次停止/启动同一个新版本 FE（BE 不动），重新执行步骤二。

实测：两表仍各有 9 行；新绑定类型、CAST 表达式、Dictionary ID=4、table_key、时区和列身份均恢复；未手动 REFRESH DICTIONARY，引用重建自动刷新后 Dictionary/FE 快照推进到事务 12，BindingState 恢复 ACTIVE，持久化完成分区数仍为 3。

重建过程中实际短暂观察到 `CAPTURE_SNAPSHOT_UNAVAILABLE`；下一调度轮次后已恢复步骤三所述的空占位分区 FAIL_CLOSED 诊断，Pending/Running 均为 0，NextExpiryTime 恢复为 `2026-09-19 00:00:00`。快照已恢复后的临时诊断没有永久残留。

## 5. 留存证据

容器持久卷中的日志：

- `/tenant-ttl-workspace/round033-fe-ut-initial.log`、`round033-fe-ut-targeted.log`：中间失败及定位过程。
- `/tenant-ttl-workspace/round033-fe-ut-full.log`：最终 83 个测试通过。
- `/tenant-ttl-workspace/round033-checkstyle.log`、`round033-fe-build.log`。
- `/tenant-ttl-workspace/round033-e2e-setup.log`、`round033-e2e-verify.log`、`round033-e2e-after-restart.log`、`round033-restart-health.log`：SQL 动作及完整输出。

本地集群和本轮测试表保留供后续手动查看。相对日期脚本用于重新造数；已有数据仍会随时间推移按 TTL 继续清理，不能长期把 9 行视为固定预期。
