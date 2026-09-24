# 第 039 轮：Tenant-TTL 聚合常量修复与验收

需求确认与实施日期：2026-09-24。依据：FE-QUERY-001、FE 详细编码计划第 20 节。用户已授权开始实现；本轮不修改 BE 契约或调度策略。

## 1. 实现范围

`RewriteSimpleAggToMetaScanRule.tryReplaceByMetaData()` 取到表对象后，检查持久化 TTL 配置或 Dictionary 绑定；命中时返回空，不读取 FE 行数/MIN/MAX 缓存。`check()` 和 `transform()` 原有路径不变，合格聚合仍由 BE MetaScan 执行。

- 生产代码仅增加 9 行（含 2 个 import），没有新增配置、状态管理器或持久化字段。
- 不随 TTL 完成、超时、快照等待等运行态恢复常量替换；普通表行为不变。
- 不改变分区版本/时间、`hasDelete()`、ColumnMinMaxMgr、ReportHandler 或 BE 协议。
- 后续动态恢复仍是可选讨论项，不在本轮实现。

## 2. 自动化测试与构建

新增 `TenantTtlAggregateMetaTest`，使用独立表和现有计划测试基类，覆盖 8 个用例：COUNT、MIN/MAX/日期和混合聚合、部分缓存命中、普通表对照、只有结构化绑定、配置复制/JSON 恢复、原有准入和开关、真实 CREATE/ALTER 绑定。强制旧 rowCount=7919、旧数值极值=-991731/991733，并令统计新鲜度检查通过；不是利用缓存 miss 来证明修复。

复用 `starrocks-tenant-ttl-e2e` 和持久卷 `sr-tenant-ttl-4.0-build-cache-arm64`，原 build 容器保持停止。JDK 17、Maven 3.6.3、arm64，Maven `-T 1`；只同步本轮变化的两个 Java 文件，不 clean，不重建或删除 BE 缓存。

| 阶段 | 实际结果 | 持久卷日志 |
| --- | --- | --- |
| 新测试首次编译 | 两次测试辅助方法签名/返回类型编译错误，尚未执行用例；改为复用基类 `getOlapTable()` | `round039-fe-red.log`、`round039-fe-red-v2.log` |
| 未修复实现的红灯验证 | 8 项，7 项 TTL 防护断言失败、1 项普通表正向对照通过；0 errors/skipped。失败计划出现旧 COUNT 或极值常量 | `round039-fe-red-v3.log`、`round039-fe-red-v3.xml` |
| 修复后完整选择器回归 | 273 项，0 failures/errors/skipped，BUILD SUCCESS；含新增 8 项和既有 AggregateMetaTest 3 项 | `round039-fe-green.log` |
| Checkstyle / FE package | 0 violations；package BUILD SUCCESS，44 秒 | `round039-checkstyle.log`、`round039-fe-package.log` |
| 真实 SQL 闭环 | 通过：旧 RowCount 合计仍为 8 时，MetaScan COUNT=5，MIN/MAX=21/33，明细一致；空表和普通表对照通过 | `06-observe.log`、`08-storage-timeline.log` |

编译与测试命令（容器内）：

```bash
export STARROCKS_HOME=/tenant-ttl-workspace
source /tenant-ttl-workspace/env.sh
cd /tenant-ttl-workspace/fe
mvn -pl fe-core -am -Dmaven.clean.skip=true -Dcheckstyle.skip \
  '-Dtest=AggregateMetaTest,AggregateTest,MinMaxMonotonicRewriteTest,*TenantTtl*Test,OlapTableTest' \
  -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -T 1 test
mvn -pl fe-core -DskipTests checkstyle:check
mvn -pl fe-core -am -Dmaven.clean.skip=true -Dcheckstyle.skip -DskipTests -T 1 package
```

273 为本轮实际选择器执行数量，不是全 FE 测试数量。未重跑 BE UT、未做真实多 FE/多 BE 故障注入；本轮没有对应 BE 产品改动。

## 3. 可复测 SQL 和预期

独立库：`tenant_ttl_r039_20260924`；Dictionary：`tenant_ttl_r039_dict`（本次 ID=8）。复测必须统一替换三个脚本中的库名和 Dictionary 名，不覆盖现有测试数据。

本轮不修改全局优化、统计或调度参数；现场统计间隔 300 秒、TTL 周期 600 秒。绑定后由正常后台调度删除该独立库的过期数据。

### 3.1 创建数据，先不绑定 TTL

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round039_setup.sql
```

策略：default=10 天、short=2 天、long=20 天；NULL 按最长 20 天保留。数据分为今天、5 天前、15 天前和 40 天前。setup 不绑定 TTL，可先观察统计和 MIN/MAX 缓存预热。

| 表 | 绑定前 COUNT / MIN / MAX | 收敛后 COUNT / MIN / MAX | 覆盖范围 |
| --- | --- | --- | --- |
| events | 16 / -3000 / 5000 | 9 / 11 / 33 | NOOP、DELETE_LIST、KEEP_LIST、DROP |
| rewrite_only | 8 / -2000 / 2000 | 5 / 21 / 33 | 只做 Rowset Rewrite，无整分区删除干扰 |
| rewrite_empty | 2 / -1000 / -1000 | 0 / NULL / NULL | 非整分区删除的全部行过期 |
| control | 8 / -2000 / 2000 | 不变 | 从不绑定 TTL 的普通表对照 |

已执行：setup 成功，初始行数和极值与表格一致，旧 FE 已观察到 MIN/MAX 部分常量替换。首次执行时间为 UTC 05:42 左右；原始输出在 `01-setup.log`。

### 3.2 新 FE 上预热、保存计划，再绑定

升级后先执行并保存下列查询/计划；MIN/MAX 是异步预热，必要时再次查询；COUNT 常量仍需满足原有新鲜度条件，不能通过调整全局配置伪造。

```sql
USE tenant_ttl_r039_20260924;
SELECT count(*), min(metric), max(metric) FROM rewrite_only;
SELECT count(*), min(metric), max(metric) FROM control;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM rewrite_only;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM control;
```

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round039_bind.sql
```

预期：绑定后 `rewrite_only` 的合格 COUNT/MIN/MAX 计划含 MetaScan，不使用刚预热的旧常量；control 保持普通表优化。Dictionary 手动 REFRESH 是使测试策略快照及时可用，不改变普通刷新契约。

已执行：UTC 05:55 左右，新 FE 上两表均显示 `EXECUTE IN FE`，常量为 8/-2000/2000（`03-prewarm.log`、`04-prewarm-confirm.log`）。05:56:44 绑定后，rewrite_only 立即变为 `sum(rows_metric), min(min_metric), max(max_metric)` + MetaScan；control 仍为原 FE 常量（`05-bind.log`）。当时 TTL 状态仍为 WAITING_DICTIONARY，证明防护不依赖调度或快照就绪；05:56:51 快照事务 34 发布后状态 ACTIVE。自动 List 空占位分区仍按既有 DEFAULT_OR_MISSING_LIST_VALUES 跳过，不是新增错误。

### 3.3 重复观察真实改写和统计滞后窗口

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round039_verify.sql
```

收敛后 events 分组预期：noop=4、delete_list=3、keep_list=2，drop 分区删除；rewrite_only 保留 long/other/NULL 的 5 天数据和 long/NULL 的 15 天数据，`COUNT(optional_metric)=3`、`COUNT(DISTINCT metric)=5`。

必须同时记录 TTL 完成时间、FE 统计更新时间和查询时间，证明至少一次结果在统计仍旧的窗口内正确。只在统计刷新后复测一致不能替代该项验收。

实际时间线（以下统一 UTC，verify SQL 中 Asia/Shanghai 显示需加 8 小时）：

| 时间 | 实际动作/结果 |
| --- | --- |
| 06:04:03 | 改写前采样仍为 events=16、rewrite_only=8、rewrite_empty=2，control=8 |
| 06:04:09.141 | FE 完成本轮统计更新；此时 rewrite_only 尚未开始改写 |
| 06:04:13～16 | rewrite_only 的 4 个 Replica 任务处理两个物理分区，返回 SUCCESS/NOOP_VERIFIED，processedThrough=2 |
| 06:04:17～18 | rewrite_empty 的 2 个 Replica 任务完成，其中有数据的 Tablet 删除 2 行；processedThrough=3 |
| 06:04:23～24 | 四张表全部达到预期；立即执行完整 verify，明细、分组、普通聚合一致 |

06:04:24 的关键证据：

- rewrite_only 普通混合聚合为 **5 / 21 / 33**，逐行读取正好是 21、22、23、31、33；COUNT(optional_metric)=3，COUNT(DISTINCT metric)=5。
- 同次 `SHOW PARTITIONS` 中，分区 10425362、10425368 的 RowCount **仍各为 4，合计 8**。VisibleVersion 均仍为 2，VisibleVersionTime 均为原插入时刻 05:42:28 UTC；没有用分区版本/时间推进掩盖旧统计。
- 混合聚合计划仍包含 `sum(rows_metric), min(min_metric), max(max_metric)` + MetaScan，没有使用旧 8/-2000/2000；改写期间 06:04:13 观察到的 7 行属于逐 Tablet 收敛中间态，不解释为表级原子可见性。
- rewrite_empty 返回 **0 / NULL / NULL**，明细为空，COUNT 计划仍有 MetaScan；不需要 Catalog 删除分区才能得到正确零值。
- events 返回 **9 / 11 / 33**，分组正好为 4/3/2；control 仍为 **8 / -2000 / 2000**，EXPLAIN 仍显示 EXECUTE IN FE。
- 共 10 个 Replica 任务在约 9 秒内执行：5 个 SUCCESS（实际删行）和 5 个 NOOP_VERIFIED（空 Tablet）；不改变调度器默认周期，也没有修改任何统计或优化全局配置。

结论：本轮限定修复已完成真实统计滞后窗口验收，不是等待统计自愈后才得到一致结果。

## 4. 环境保留和日志

- 主机 SQL 日志目录：`/private/tmp/tenant-ttl-r039.faKNzT/`。
- 完整 SQL 日志、只读采样及升级脚本已复制到持久卷 `/tenant-ttl-workspace/round039-sql-faKNzT/`，与编译缓存一并保留。
- 编译/测试日志位于 `/tenant-ttl-workspace/round039-*`，不删除缓存。
- 运行环境：`/tenant-ttl-workspace/tenant-ttl-e2e-runtime-20260911-1457`；UTC 05:54:07 启动新 FE，PID 62575、LEADER/Alive；BE PID 903 未变。
- 备份目录 `/tenant-ttl-workspace/round039-fe-backup-20260924/`：旧 runtime/output jar、停机 `meta.tar.gz`（约 26 MiB）和旧 PID。升级脚本首次因 PID 文件无末尾换行而在停服前退出，校验备份与运行 jar 一致后修正读取并重试成功；未强杀 FE，旧进程结束/僵尸态后才备份元数据。
- 旧 FE jar SHA-256：`9e04f98b297a252adba615d353bd46a4ad001b83849318302c84397a5ff20969`。
- 新 FE jar SHA-256（target 与运行目录一致）：`7ac43809467971226c440ba33aa4255268f7a616d2e2d014503c5efd9ae934eb`。本次直接 Maven package 的版本展示为 UNKNOWN-UNKNOWN，验收以 jar hash、源码 hash、实际计划和结果确认，不用版本字符串证明部署。
- BE binary SHA-256 仍为 `d245c04c4a0b21c599c59a1ec1de2c661f3d09995e170a3d0a626b2512588380`。
- 主机/容器两个 Java 文件 SHA-256 均一致：产品文件 `889eabc0ebb44e60768c969d88bab4f0e577578d9f6831ce7660037c4cb2f0e6`；测试文件 `c867803c8328e752fa75474800f1793f869d5e455b97d9cb8473ffbe06ef0ffa`。
- 测试报告另归档为 `/tenant-ttl-workspace/round039-fe-surefire-reports.tar.gz`（含缓存目录保留的其他历史报告，本轮执行集合/数量以 round039-fe-green.log 为准）。

## 5. 结论与未测边界

第 038 轮续验曾出现普通 COUNT=7、明细=6，EXPLAIN 为 FE 常量 7，统计更新后才自愈；该历史失败事实保留，不追溯改成通过。第 039 轮在相同类型的同版本改写/旧统计窗口中完成针对性修复验收，已收敛这一 FE 聚合常量阻塞。

本轮 FE 自动化、构建和隔离库 SQL 通过，保留集群已升级到本次 FE 产物。没有重跑全 FE 测试或 BE UT，没有真实多 FE/多 BE 故障注入；没有实现动态恢复缓存、BE Query Cache/异步 MV 兼容性或跨 Replica 原子读取。以上不作为已完成能力宣传；如需后续改造，按澄清文档独立对齐。
