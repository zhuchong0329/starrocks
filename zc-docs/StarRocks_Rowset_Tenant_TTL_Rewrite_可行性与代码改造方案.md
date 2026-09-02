# Rowset Tenant-TTL Rewrite

**深度可行性与代码改造方案**

面向本地存储 DUP_KEYS 表的 Segment 级 KEEP / DROP / REWRITE

> **评审结论**
>
> 方案在严格约束下可实现；它不是现有 Compaction 的配置开关，而是一条新的 FE 策略调度 + BE 单 Rowset 重写链路。建议先以 replication_num=1、本地 POSIX 存储、无 Rollup、canonical 数字 tenant 为生产 MVP 边界。

源码基线：StarRocks main<br>Commit：9559176fab6e2cb885779f1e7b680133d58d6972<br>评估日期：2026-09-01<br>文档状态：设计评审稿 v1.0

# 文档控制与阅读说明

| 项目 | 内容 |
| --- | --- |
| 目标 | 判断 Tenant-TTL 是否能利用排序键与 Segment ZoneMap，做到文件级复用并给出可落地的源码改造方案。 |
| 表模型 | DUP_KEYS；ORDER BY(tenant_id, recordTimestamp)；tenant_id 为 CAST(tenant AS BIGINT) 生成列。 |
| 重点 | FE 分区三态裁决；BE 单个 NONOVERLAPPING Rowset 的 Segment 级 KEEP / DROP / REWRITE。 |
| 非目标 | 本稿不把共享数据湖对象引用复用、多副本强一致删除、任意 Rollup/MV 作为首期交付。 |
| 证据口径 | 文件名与行号基于 commit 9559176fab6e；“NEW”表示建议新增文件，尚无现存行号。 |

# 执行摘要

> **一句话结论**
>
> 总体可行，但必须先解决身份一致性、策略快照、部分 Segment 统计、并发提交和结果回传五类阻塞；否则最危险的失败不是任务失败，而是“错误地删除仍应保留的数据”。

- 当前源码中不存在 compaction_retention_condition 或 dictionary_ttl；现有 partition_retention_condition 只做分区表达式筛选，不能表达按 tenant 的行级 TTL。

- 当前 FE PartitionTTLScheduler 明确拒绝多列 Range 分区，因此该表的 (tenant_bucket, date) 不能直接复用现有 TTL 调度。

- BE 已具备四块可复用底座：NONOVERLAPPING Rowset、整 Rowset hard link、同版本 Rowset 替换、引用计数延迟 GC；缺少的是“单 Segment 重编号复用”和逐 Segment 统计。

- ORDER BY 实际是 (tenant_id, recordTimestamp)。只有 tenant 为 canonical BIGINT 字符串时，tenant_id 才是可靠身份；非法、溢出、前导零和空白会破坏删除语义。

- Segment ZoneMap 可做保守排除；精确首尾 tenant_id 可确认“整个 Segment 只有一个 tenant”。跨 tenant Segment 必须 REWRITE，不能仅凭两个独立 ZoneMap 推断每个 tenant 的时间范围。

- 分区直接删除必须走 FE 的 LocalMetastore.dropPartition 元数据路径；不能让 BE 先物理删文件。物理空间回收仍由既有异步清理链路完成。

- replication_num=1 可规避副本间可见性窗口；replicated_storage=true 只优化导入复制，不能提供 TTL rewrite 的多副本原子屏障。

## 首期建议边界

| 维度 | MVP 约束 | 超出边界的处理 |
| --- | --- | --- |
| 存储模式 | 本地存储、同 Tablet 目录、POSIX hard link | Shared-data/lake 返回 UNSUPPORTED，另立对象引用方案 |
| 副本 | replication_num=1 | 多副本只能定义为最终一致，或新增可见版本/屏障协议 |
| 索引 | 仅 Base Index；允许嵌入式 Bloom/Bitmap/ShortKey | Rollup/MV 拒绝；GIN/Vector 需完成外部文件重映射后放开 |
| 数据身份 | tenant 非空、canonical 十进制、BIGINT 范围内且一一映射 tenant_id | 发现 NULL/别名/溢出：整 Tablet 任务失败，不提交部分结果 |
| Rowset | DUP_KEYS、同 schema、NONOVERLAPPING、单个待处理 Rowset | 返回 INELIGIBLE / NEED_BASE_COMPACTION，不隐式冒险处理 |

# 目录

1. DDL 与身份语义审计

2. 当前源码能力与缺口

3. 总体架构与数据流

4. FE 属性、字典快照与分区裁决

5. 任务协议、调度与结果闭环

6. BE 资格检查、Segment 分类与混合 RowsetWriter

7. 原子提交、查询并发与 GC

8. 索引、加密、多副本与共享存储

9. 类关系、调用栈与文件级改造清单

10. 测试、工作量、灰度与验收标准

# 1. DDL 与身份语义审计

## 1.1 提交的 DDL 不能按原文直接作为实现契约

- 属性 compaction_retention_condition 在当前仓库无定义，dictionary_ttl 也没有解析器或执行函数；需要新增元数据和调度语义。

- 示例属性行含中文右引号/中文逗号；实际 SQL 必须替换为 ASCII 的双引号与逗号。Markdown 中的 \*、xx\_hash3\_64 也要还原为 *、xx_hash3_64。

- 方案文字写“按 (tenant, recordTimestamp) 排序”，但表定义实际按 (tenant_id, recordTimestamp) 排序。所有 Segment 分类必须以 tenant_id 为物理有序列，以 tenant 为业务身份校验列。

- tenant_bucket 是哈希分组，不是 tenant 身份。哈希碰撞不会影响正确性，但 FE 只能使用按 bucket 聚合后的保守 TTL 上下界，不能据 bucket 推断实际 tenant 集合。

## 1.2 tenant → tenant_id 的一一映射是删除正确性的前置条件

BE 的 VARCHAR→BIGINT 向量化 CAST 在解析失败时会产生 NULL；源码见 be/src/exprs/cast_expr.cpp:399-459，BIGINT 注册见同文件:567。由此产生以下风险：

| 输入样例 | tenant_id 结果/关系 | TTL 风险 |
| --- | --- | --- |
| '2' | 2 | 合法 canonical 值 |
| '02' | 2，与 2 冲突 | 两个业务 tenant 共用排序身份，DROP 可能误删 |
| ' 2 ' / '+2' | 取决于解析规则或归一到 2 | 兼容行为不应成为身份协议 |
| 'tenant_111' | NULL | 多个非法 tenant 聚到 NULL 区域，无法按 tenant_id 精确删 |
| 超 BIGINT 范围 | NULL/解析失败 | 与其他非法值聚合，策略无法一一对应 |
| tenant=NULL | tenant_id=NULL | dictionary key、bucket 与 TTL 默认语义均需明确 |

> **生产级硬门禁**
>
> 启用属性时必须验证 tenant 是非空 canonical 十进制字符串，并在写入路径拒绝非法值。仅在 TTL 任务中“遇到 NULL 再兜底”是不够的，因为历史数据可能已经按同一个 NULL 排序身份混在一起。

## 1.3 推荐的 V1 属性契约

```text
PROPERTIES (
"compaction_retention_condition" =
"dictionary_ttl('dns_tenant_retention_dict', 180)",
"tenant_ttl_timezone" = "Asia/Shanghai",
"tenant_ttl_strict_numeric_tenant" = "true"
)
```

V1 不把 dictionary_ttl 当任意 SQL 表达式执行，而是解析成强类型 CompactionRetentionPolicy。字典契约建议固定为：一个 VARCHAR tenant key、一个整数 ttl_days value；缺失 key 使用 default_days=180。ttl_days 必须在配置上限内，刷新失败时保留上一成功版本。

# 2. 当前源码能力与缺口

## 2.1 FE：现有 Partition TTL 只能“整分区删除”

PartitionTTLScheduler 维护 TTL 表集合并周期扫描（fe/fe-core/src/main/java/com/starrocks/clone/PartitionTTLScheduler.java:61-133）；对每张表计算过期分区后，在 WRITE 锁内调用 LocalMetastore.dropPartition（同文件:162-186）。

关键限制在 PartitionTTLScheduler.java:189-215：RangePartitionInfo 的分区列数不是 1 时直接判无效。当前表以 tenant_bucket 与日期表达式组成复合分区，因此无法直接注册进这条调度链。现有按时长删除只比较分区 upper endpoint 与 TTL 下界（同文件:251-290），也没有 tenant 字典。

## 2.2 FE：可复用属性持久化框架，但不能复用表达式语义

| 能力 | 当前源码锚点 | 结论 |
| --- | --- | --- |
| 属性常量/解析 | PropertyAnalyzer.java:205-208、463-500 | 可复制生命周期；不能复用任意 SQL retention expression |
| 建表写入属性 | OlapTableFactory.java:727-737 | 新增 compaction retention 分支 |
| ALTER TABLE | AlterJobExecutor.java:571-578 | 新增属性路由 |
| 持久化/注册 | LocalMetastore.java:3876-3903、3929-3968 | 新增 TableProperty 字段、EditLog 与 scheduler 注册 |
| FE 重启重建集合 | DynamicPartitionScheduler.java:465-485 | 补充 isTenantTtlTable 注册 |
| 现有字段模式 | TableProperty.java:199-206、572-574、954-959 | 新增 build/get/setCompactionRetentionPolicy |

## 2.3 FE→BE 手工 Compaction 协议不能承载 TTL 结果

本地模式下 CompactionHandler 按 backend 汇总所有可见 Index 的 Tablet，并发送 CompactionTask（CompactionHandler.java:77-120）。当前 TCompactionReq 只有 tablet_ids 和 is_base_compaction（gensrc/thrift/AgentService.thrift:282-285）。BE run_compaction_task 调用 EngineManualCompactionTask，却丢弃 execute_task 返回值并固定回 OK（be/src/agent/agent_task.cpp:571-593）；FE finishCompactionTask 也只移除队列项（LeaderImpl.java:446-448）。

> **阻塞项**
>
> 如果直接复用当前 COMPACTION 任务，FE 会把失败、NOOP、策略版本缺失都当成功，无法安全重试，也无法形成报告指标。建议新增 TENANT_TTL_COMPACTION 任务类型和逐 Tablet 结果结构。

## 2.4 BE：底层原语已具备，但粒度不够

| 现有原语 | 源码锚点 | 可复用程度 |
| --- | --- | --- |
| 标准 Compaction 全量读写 | be/src/storage/compaction.cpp:105-159、162-352 | 构建、校验、提交可借鉴；合并路径不能直接复用 |
| 输出 NONOVERLAPPING Rowset | be/src/storage/compaction_utils.cpp:53-74 | 直接复用 context 构造模式 |
| 整 Rowset shortcut hard link | be/src/storage/compaction_task.cpp:206-265 | 证明零拷贝可行；需降到单 Segment |
| 整 Rowset 文件链接 | be/src/storage/rowset/rowset.cpp:447-514 | 已有 .dat、GIN、Vector、delete/update 链接；不支持 src/dst Segment 重编号 |
| RowsetWriter 汇总元数据 | be/src/storage/rowset/rowset_writer.cpp:145-168、758-790 | 需新增 add_linked_segment 与逐 Segment 统计 |
| 同版本替换 | be/src/storage/tablet.cpp:373-422 | 支持单版本旧 Rowset→新 Rowset |
| 旧 Rowset 延迟回收 | be/src/storage/storage_engine.cpp:1234-1308 | use_count>1 时不删，满足存量查询安全 |

## 2.5 Segment 的顺序与边界读取

一个 Rowset 的 Segment 文件名由 rowset_id 与连续 segment_id 组成（rowset.cpp:169-182），加载时按 0..num_segments-1 顺序打开（rowset.cpp:194-224）。NONOVERLAPPING Rowset 用 union iterator 按此顺序拼接（rowset.cpp:813-861），Rowset::verify 会检查整体排序（rowset.cpp:1047-1093）。因此新 Rowset 必须按源 segment_id 顺序生成连续目标编号。

Segment 可为指定列创建 ColumnIterator（segment.h:115-143），并使用 seek_to_ordinal(0/num_rows-1) 读取精确首尾值（column_iterator.h:96-108）。这比把 VARCHAR ZoneMap 当精确边界更可靠。

# 3. 可行性判定与总体架构

## 3.1 判定：条件可行

| 问题 | 结论 | 原因 |
| --- | --- | --- |
| 能否用 ZoneMap 跳过不相关 Segment？ | 可以，保守使用 | tenant_id/recordTimestamp 为 BIGINT；目标落在 min/max 外可直接 KEEP |
| 能否确认 Segment 全属于一个 tenant？ | 可以 | 物理排序下 exact first_tenant_id == last_tenant_id 即可证明 |
| 能否对 KEEP 零拷贝？ | 本地 POSIX 可以 | 同 Tablet 目录 hard link；旧、新路径共享 inode |
| 能否 DROP 整个 Segment？ | 强证明下可以 | 单 tenant 且 max_ts < cutoff；或全局 maxTTL 证明所有行过期 |
| 能否跨 tenant 边界只删部分行？ | 可以，但必须重写 | 读取 REWRITE Segment，按固定策略快照过滤并重建索引 |
| 能否原子替换？ | 单副本本地可复用现有机制 | header lock 下同版本替换并保存 TabletMeta |
| 能否开箱即用？ | 不可以 | 缺属性、调度、协议、partial link、统计和并发校验 |

## 3.2 五个必须先解决的阻塞

1. 身份阻塞：tenant_id 必须与 tenant 一一对应；否则 tenant_id 相等不能证明业务 tenant 相等。

2. 策略阻塞：字典缓存只有当前成功版本；任务必须携带 dictionary_id + txn_id + evaluation_time，并在版本不匹配时失败重试。

3. 分区阻塞：现有 TTL scheduler 不支持复合分区，需要新的三态分类器，不可在原实现中简单放开 size!=1 检查。

4. 存储阻塞：现有 add_rowset 只能整 Rowset 链接，RowsetMeta 又缺逐 Segment row/data/index/raw-size 统计。

5. 并发阻塞：当前 event compaction 提交只检查同 version 是否存在（compaction_task.h:261-299），未比较 rowset_id；同版本 TTL 重写必须加入精确身份校验。

## 3.3 端到端分层

| 层 | 建议组件 | 职责 |
| --- | --- | --- |
| 元数据 | CompactionRetentionPolicy / TenantTtlPolicySnapshot | 解析属性、绑定字典 schema、记录版本与 TTL 上下界 |
| FE 决策 | TenantTtlScheduler / TenantTtlPartitionClassifier | 按分区输出 NOOP、DROP_PARTITION、REWRITE |
| 任务闭环 | TenantTtlCompactionTask / TenantTtlTaskTracker | 按 BE/Tablet 派发、去重、超时、重试、汇总结果 |
| BE 准入 | EngineTenantTtlCompactionTask | 校验 tablet、schema、rowset、存储类型、策略版本和并发锁 |
| Segment 决策 | SegmentTtlClassifier | ZoneMap 粗排 + exact first/last 分类 KEEP/DROP/REWRITE |
| 输出构建 | TenantTtlRowsetRewriter + HorizontalRowsetWriter | 链接 KEEP、跳过 DROP、过滤重写边界 Segment |
| 提交回收 | Tablet / StorageEngine | ID 校验、同版本原子替换、stale/unused rowset 延迟 GC |

```text
Dictionary refresh ──> PolicySnapshot(version, min/max TTL, bucket bounds)
│
DynamicPartitionScheduler ──> TenantTtlScheduler
├─ NOOP
├─ DROP_PARTITION ──> LocalMetastore.dropPartition
└─ REWRITE ──> Agent task ──> BE Tablet
├─ KEEP: hard link
├─ DROP: omit
└─ REWRITE: filter + SegmentWriter
│
TabletMeta same-version swap
│
stale Rowset ──> ref=0 ──> GC
```

# 4. FE 属性、字典快照与分区三态裁决

## 4.1 属性解析与表级资格校验

新增 PropertyAnalyzer.analyzeCompactionRetentionCondition()，只允许白名单函数 dictionary_ttl(name, default_days)。解析结果不是 Expr，而是不可变 CompactionRetentionPolicy。建表和 ALTER 时执行以下校验：

- 表必须为 OLAP + DUP_KEYS；首期拒绝 PRIMARY/UNIQUE/AGG。

- 排序键前两列必须是 BIGINT tenant_id、BIGINT recordTimestamp，且 tenant_id 的生成表达式 AST 精确等价于 CAST(tenant AS BIGINT)。

- tenant_bucket 的生成表达式必须引用同一个 tenant，并与 64 桶约定一致；分区第二维必须能解析为 recordTimestamp 的日期范围。

- 字典必须存在，key/value 类型满足 V1 契约；default_days、所有 ttl_days 值均在合法范围。

- strict_numeric_tenant 必须开启；首期表中不能存在不兼容 Rollup/物化索引。

- Shared-data 模式、跨文件系统目录、未知文件系统能力直接拒绝启用 Segment reuse。

## 4.2 策略快照必须与字典刷新事务绑定

FE Dictionary 记录 lastSuccessVersion（Dictionary.java:70-81）；DictionaryMgr 在刷新完成后更新版本（DictionaryMgr.java:353-359、764-802）。BE DictionaryCacheManager::commit 用 txn_id 原子替换当前缓存并记录版本（dictionary_cache_manager.cpp:216-245），get_dictionary_by_version 要求版本完全相等（同文件:294-313）。

因此每次字典刷新应同步产出 TenantTtlPolicySnapshot：

| 字段 | 作用 | 生成位置 |
| --- | --- | --- |
| dictionary_id / txn_id | 任务固定策略版本，禁止静默读 latest | Dictionary refresh commit |
| default_ttl_days | 字典缺失 key 的定义 | 表属性 |
| global_min/max_ttl_days | FE 分区和 BE Segment 的保守判定 | 刷新数据流聚合 |
| bucket[0..63] min/max | 利用 tenant_bucket 改善分区裁剪 | 用相同 xx_hash3_64 语义在刷新流聚合 |
| schema fingerprint | 确保 key/value 列与类型未漂移 | 字典元数据 |
| commit_time / valid | 调度门禁和审计 | FE finish refresh |

> **安全降级**
>
> 如果某字典版本没有完整 min/max 快照，FE 只能发 REWRITE，不能直接 DROP_PARTITION。BE 若拿不到请求中的精确 txn_id，返回 POLICY_VERSION_MISSING；不得自动切换 latest。已取得 shared_ptr 的运行中任务可继续使用旧缓存，排队后才启动的旧版本任务会失败并由 FE 重新计划。

## 4.3 分区 NOOP / DROP / REWRITE 的数学条件

定义行过期条件为 recordTimestamp < C(t)，其中 C(t)=evaluation_time−ttl_days(t)×86400。evaluation_time 在一次调度开始时固定并随任务下发；分区时间范围为 [L,U)，TTL 上下界来自该 tenant_bucket 的安全聚合。

| 决策 | 充分条件 | 说明 |
| --- | --- | --- |
| DROP_PARTITION | U ≤ evaluation_time − maxTTL(bucket)×86400 | 连保留期最长的 tenant 也全部过期；直接走 FE 分区删除 |
| NOOP | L ≥ evaluation_time − minTTL(bucket)×86400 | 连保留期最短的 tenant 也没有过期行；不发任务 |
| REWRITE | 其他情况 | 该分区可能同时包含过期与未过期数据；发送 Tablet 级任务 |

```text
Decision classify(PartitionRange p, PolicySnapshot s, int64 now) {
TimeInterval ts = extract_timestamp_interval(p, s.timezone);
TtlBounds b = s.bounds_for_bucket(extract_tenant_bucket(p));
if (ts.upper_exclusive <= now - days(b.max_days)) return DROP_PARTITION;
if (ts.lower_inclusive >= now - days(b.min_days)) return NOOP;
return REWRITE;
}
```

## 4.4 为什么不能只看 tenant_bucket

bucket 内可能有多个 tenant，且可能存在字典未列出的 tenant。安全的 maxTTL(bucket) 必须至少是 max(default_days, bucket 中所有显式策略)，minTTL(bucket) 至少按“可能存在默认 tenant”取 min(default_days, 显式最小值)。只有业务另有“字典覆盖全部 tenant”的强约束时，才能去掉 default 的保守项。

## 4.5 直接分区删除的正确路径

DROP_PARTITION 决策应在 table WRITE 锁内再次确认 tableId、partitionId、policy txn_id 与分区范围未变化，然后调用现有 LocalMetastore.dropPartition。不要新增“BE 物理删除分区文件”快捷任务：FE Catalog 是分区可见性的权威，先删文件会破坏存量查询和副本一致性。现有调度的正确锁与调用范式见 PartitionTTLScheduler.java:173-185。

# 5. 任务协议、调度与结果闭环

## 5.1 建议新增独立任务类型

在 Types.thrift 的 TTaskType 末尾（NUM_TASK_TYPE 前）追加 TENANT_TTL_COMPACTION；在 AgentService.thrift 新增 TTenantTtlCompactionReq，在 MasterService.thrift:TFinishTaskRequest 追加可选 tenant_ttl_results。混合集群下 FE 只向声明 capability 的 BE 派发。

| 请求字段组 | 核心字段 |
| --- | --- |
| 任务身份 | task_id、db/table/index/partition/tablet_id、attempt、dry_run |
| 策略固定 | dictionary_id、dictionary_txn_id、evaluation_time_epoch_sec、default/min/max_ttl_days、timezone |
| Schema 固定 | schema_id/hash、tenant/tenant_id/timestamp/bucket column unique_id、sort-key fingerprint |
| Rowset 期望 | require_dup_keys、require_nonoverlap、single_rowset_only、local_posix_only |
| 幂等水位 | policy_version + evaluation_time；可选 last completed watermark |

| 结果字段 | 说明 |
| --- | --- |
| outcome | SUCCESS / NOOP / INELIGIBLE / NEED_BASE_COMPACTION / POLICY_VERSION_MISSING / STALE_ROWSET / FAILED |
| rowset | source_rowset_id/version、output_rowset_id、schema id |
| segment counters | scanned、zone-map-pruned、kept-linked、dropped、rewritten |
| row/byte counters | input、expired、output rows；linked/read/written/index bytes |
| correctness | policy txn、evaluation time、verify status、commit status、invariant violation |
| timing | queue/scan/rewrite/fsync/commit/total ms |

## 5.2 FE 调度状态机

```text
REGISTERED
│ schedule snapshot
├─ NOOP ───────────────────────────────> WAIT_NEXT_CYCLE
├─ DROP_PARTITION ─> catalog drop ─────> DONE
└─ REWRITE ─> DISPATCHED ─> RUNNING
├─ SUCCESS/NOOP ─> DONE
├─ STALE/POLICY_MISSING ─> RESCHEDULE_LATEST
├─ NEED_BASE_COMPACTION ─> WAIT_PREREQUISITE
└─ FAILED ─> BACKOFF_RETRY / ALERT
```

- 去重键建议为 (tableId, partitionId, tabletId, policyTxnId, evaluationBucket)，同一 Tablet 同一水位只允许一个 in-flight。

- 失败重试必须保持原 evaluation_time；如果因字典版本缺失而重新规划，生成新的 task_id、policyTxnId 与 evaluation_time。

- FE 只有收齐所有目标 Tablet 的逐项结果才完成一轮。当前 replication_num=1；未来多副本要按 Replica 维度追踪。

- 队列和结果应持久化最小水位，FE 重启后可通过幂等结果安全重发；不要只把任务存在内存 Set 中。

## 5.3 为什么不能扩展当前 run_compaction_task 一点点就结束

现有路径的两个行为不满足数据删除任务：BE 丢弃执行 Status（agent_task.cpp:577-581），FE 完成回调只移除任务（LeaderImpl.java:446-448）。即使在 TCompactionReq 增加 mode，也必须同时改执行状态、逐 Tablet result、FE tracker、超时/重试和 mixed-version capability。独立任务类型能避免把普通 Compaction 的“尽力而为”语义扩散到 TTL。

# 6. BE 准入、Segment 分类与 Tenant-TTL Rewrite

## 6.1 BE 资格检查顺序

1. 按 tablet_id 获取 Tablet，确认 RUNNING、本地非 PK Tablet、keys_type=DUP_KEYS。

2. 校验 schema id/hash 与 column unique_id；排序键必须精确为 tenant_id、recordTimestamp，类型均为 BIGINT。

3. 校验 tenant_id 无 NULL 且 strict identity watermark 已建立；否则返回 DATA_INVARIANT_VIOLATION。

4. 从 DictionaryCacheManager 取得精确 dictionary_id/txn_id 缓存；失败返回 POLICY_VERSION_MISSING。

5. 以固定顺序 try-lock base_lock、cumulative_lock；阻止普通 Base/Cumulative Compaction 与 TTL 同时改同一 Tablet。

6. 选择一个可见、schema 一致、NONOVERLAPPING、无 delete predicate/partial update/DCG 的 Rowset；不满足则不提交。

7. 记录 expected source rowset_id + version。后续所有工作使用 immutable Rowset 引用。

> **关于“Base Compaction 后只有一个 Rowset”**
>
> 不能作为通用事实。Base Compaction 完成后仍可能有更新的 cumulative Rowset，任务执行期间也可能产生新版本。首期只处理一个明确选中的 consolidated Rowset；新写入 Rowset 留给下一轮。若业务强制要求 Tablet 只有一个 Rowset，则将其作为资格条件并返回 NEED_BASE_COMPACTION。

## 6.2 ZoneMap 的正确用法

SegmentWriter 为 key、DUP 值列或 sort-key 创建 ZoneMap（segment_writer.cpp:163-175）。tenant_id 与 recordTimestamp 是 BIGINT，不存在字符串前缀截断，可作为精确数值上下界。原 tenant 为 VARCHAR 且不是 DUP key 时，字符串 ZoneMap 可能在写入时截断；截断实现会把 max 前缀追加 0xFF 保持上界（zone_map_index.cpp:231-249），只能保守排除，不能当精确 tenant。

现有 Segment 读取已经支持 Segment ZoneMap pruning（segment.cpp:307-347），元数据读取也能拿到 Segment min/max（meta_reader.cpp:481-512）。新的分类器可直接复用 ColumnReader/ZoneMapPB，exact first/last 再用 ColumnIterator ordinal seek。

## 6.3 单目标 tenant 的 KEEP / DROP / REWRITE

| 条件 | 分类 | 理由 |
| --- | --- | --- |
| target < tenant_min 或 target > tenant_max | KEEP | 有序范围外，确定不含目标 tenant |
| segment_min_ts ≥ cutoff(target) | KEEP | Segment 中没有任何达到过期线的行 |
| first_tenant=last_tenant=target 且 max_ts < cutoff | DROP | 整 Segment 仅目标 tenant 且全部过期 |
| target 落入 tenant 范围，但上述条件均不成立 | REWRITE | 可能存在目标过期行，也可能跨 tenant 边界 |

## 6.4 一次处理全字典策略时的保守分类

| 条件 | 分类 | 说明 |
| --- | --- | --- |
| max_ts < now − global_maxTTL | DROP | 连最长保留期都过期，所有行可删 |
| min_ts ≥ now − global_minTTL | KEEP | 连最短保留期都未过期 |
| first_tenant == last_tenant | 按该 tenant 的 cutoff 精确判定 | 单 tenant Segment 可最大化 KEEP/DROP |
| 多 tenant 且无法全局证明 | REWRITE | 独立 tenant/time ZoneMap 不表达相关性 |

```text
SegmentDecision classify(Segment& s, Policy& p, int64 now) {
Zone zTenant = exact_bigint_zonemap(s, tenant_id_uid);
Zone zTime = exact_bigint_zonemap(s, timestamp_uid);
if (zTenant.has_null) return DATA_INVARIANT_VIOLATION;
if (zTime.max < now - days(p.global_max_ttl)) return DROP;
if (zTime.min >= now - days(p.global_min_ttl)) return KEEP;

int64 first = read_ordinal(s, tenant_id_uid, 0);
int64 last = read_ordinal(s, tenant_id_uid, s.num_rows() - 1);
if (first == last) return classify_single_tenant(first, zTime, p, now);
return REWRITE;
}
```

> **禁止的推断**
>
> “tenant ZoneMap 覆盖目标值”不等于 Segment 一定含该 tenant；“tenant min/max + timestamp min/max”也不能推出某个中间 tenant 的 timestamp min/max。无法形成充分条件时必须 REWRITE，不能 DROP。

# 7. 混合 RowsetWriter：KEEP 链接、DROP 跳过、REWRITE 重建

## 7.1 为什么首期强制 Horizontal

Vertical Compaction 按列组多遍读取并用 RowSourceMask 对齐（compaction.cpp:239-352）；一个已完整 hard link 的 Segment 无法自然插入到尚未完成的列组输出中。Tenant-TTL 的 mixed writer 首期应固定为 HorizontalRowsetWriter：每个输出 Segment 一次包含完整列、ShortKey 与所有嵌入索引。

## 7.2 处理算法

```text
for src_segment_id in [0, input.num_segments):
decision = classifier.classify(src_segment)
switch decision:
KEEP:
writer.flush_pending_rewrite_segment()
writer.add_linked_segment(input, src_segment_id, next_dst_segment_id)
DROP:
stats.deleted_rows += src_segment.num_rows
REWRITE:
read full rows in source order
keep_mask = !(recordTimestamp < cutoff(tenant))
writer.add_chunk(filtered_rows)
writer.flush_at_source_boundary_if_next_is_linked()

if no segment changed: return NOOP
output = writer.build(); output.load(); output.verify(); commit_if_source_unchanged()
```

处理顺序必须严格按源 segment_id。REWRITE 可产生 0、1 或多个目标 Segment；KEEP 前先 flush 已重写数据，随后把源 Segment 链接到连续的目标编号。这样仍满足 NONOVERLAPPING，且 Rowset::verify 能对整体排序做最终校验。

## 7.3 需要新增的 Rowset/Writer 接口

| 接口 | 建议签名/职责 |
| --- | --- |
| Rowset::link_segment_files_to | 接收 src_segment_id、dst_rowset_id、dst_segment_id；链接 .dat 与所有 standalone index，失败时可回滚 |
| HorizontalRowsetWriter::add_linked_segment | 登记 rows/data/index/raw-size、encryption meta、输出 ordinal，并更新 _num_segment |
| HorizontalRowsetWriter::flush_segment_boundary | 在链接 Segment 前封口当前 SegmentWriter，防止编号/顺序交错 |
| RowsetWriter::add_rewrite_meta | 写 policy txn、evaluation_time、source rowset id、deleted rows 等幂等水位 |
| Rowset::verify_artifacts | 验证每个目标 .dat、GIN/Vector 路径、encryption meta 数量与 Segment 数一致 |

## 7.4 逐 Segment 统计是必要元数据

RowsetMetaPB 当前只保存 rowset 级 num_rows、data/index/total size 和 num_segments（olap_file.proto:112-164）；SegmentFooterPB 只有 num_rows，旧的 footprint 字段已废弃（segment.proto:204-217）。SegmentPB 在写入时有 data_size、index_size、num_rows、row_size，但它是传输/写入对象，不持久在本地 RowsetMeta（data.proto:111-140）。所以部分链接后无法从旧聚合值精确拆分。

建议在 RowsetMetaPB 追加兼容字段 repeated SegmentStatsPB segment_stats = 67，至少包含：num_rows、segment_file_size、embedded_index_size、external_index_size、raw_row_size、可选 boundary fingerprint。所有新写 Rowset 都填充；老 Rowset 缺失时，TTL 任务要么全量重写，要么返回 NEED_SEGMENT_STATS，不能按比例猜测。

| 兼容场景 | 行为 |
| --- | --- |
| 新 BE 写、新 BE 读 | 持久化逐 Segment 统计，支持混合链接 |
| 老 Rowset 被新 BE 读 | 字段缺失；安全降级全量重写或先做一次新版本 Base Compaction |
| 新 Rowset 被老 BE 读 | Protobuf 未知字段被忽略；数据文件仍可读，但老 BE 不执行 Tenant-TTL |
| 降级/回滚 | 保留字段不影响旧读取；不得在 mixed-version 集群启用新任务能力 |

## 7.5 索引与加密文件的重映射

- ShortKey、ZoneMap、Bloom、Bitmap 等嵌入 .dat 的结构随 KEEP Segment 原样保留；REWRITE 由 SegmentWriter 重建。

- 当前 whole-rowset linker 已逐 GIN 目录和 Vector 文件 hard link（rowset.cpp:457-490），新接口必须把 src ordinal 映射到 dst ordinal，不能沿用 segment_n=i。

- segment_encryption_metas 是按 Segment ordinal 保存（olap_file.proto:182）；KEEP 必须复制源 ordinal 的 meta 到目标 ordinal。

- 异常清理要覆盖 .dat、GIN 目录、Vector 文件和所有已建 hard link。现有 writer 析构清理见 rowset_writer.cpp:508-562，需要补齐部分链接的 artifact registry。

# 8. 原子提交、查询并发、幂等与 GC

## 8.1 同版本替换可复用，但提交前必须比较 Rowset ID

Tablet::modify_rowsets_without_lock 先删除旧 version 映射，再加入同 version 新 Rowset，源码明确支持 single-version compaction（tablet.cpp:373-412）。新任务在 header lock 内执行：

1. 重新读取 current = tablet.get_rowset_by_version(expected_version)。

2. 要求 current != null 且 current.rowset_id == expected_source_rowset_id；仅比较 version 不够。

3. 再次验证 schema fingerprint 与 task policy watermark 未被更高水位覆盖。

4. modify_rowsets_without_lock({output},{current})，保存 TabletMeta，关闭输入引用。

5. 把被替换 Rowset 放入 stale/unused 链路；返回实际 output_rowset_id。

> **现有竞态**
>
> event compaction 的 _commit_compaction 在 compaction_task.h:261-299 只检查 get_rowset_by_version(version) 非空。如果 TTL-A 已把同版本 R1 换成 R2，TTL-B 仍可能把“存在的 R2”当作 R1，随后按 version 删除当前 Rowset。Tenant-TTL 必须比较 rowset_id，并建议顺手强化通用 compaction 提交。

## 8.2 存量查询为何仍安全

查询在打开 Rowset/Segment 后持有 shared_ptr。替换只改变 Tablet 当前 version→Rowset 映射，已经取得旧 Rowset 的查询继续读取旧路径。旧 Rowset 进入 stale map 后最终转入 unused 集合（tablet.cpp:750-810）；StorageEngine::delete_unused_rowset 只有 use_count==1 才真正 remove 文件（storage_engine.cpp:1234-1288）。

KEEP 使用 hard link，不是 mv：新路径与旧路径指向同一 inode。GC 删除旧路径时 inode 仍由新路径引用；只有两套路径和所有打开句柄都释放后才回收物理块。

## 8.3 fsync 与崩溃一致性

- 所有 REWRITE 文件必须 finalize/close；所有 KEEP hard link 完成后对 Tablet 目录执行 sync_dir。RowsetWriter::build 当前在有行时调用 sync_dir（rowset_writer.cpp:145-148）。

- 先 build/load/verify，后进入 header lock 提交；提交失败或 STALE_ROWSET 时新 Rowset 作为 orphan 清理，旧 Rowset 不动。

- Tablet::save_meta 当前失败会 CHECK（tablet.cpp:168-173），Tenant-TTL 应沿用现有本地 Compaction 的故障语义，同时增加 failpoint 覆盖“链接后崩溃、meta 前崩溃、meta 后回报前崩溃”。

## 8.4 幂等水位

在输出 RowsetMeta 增加 TenantTtlRewriteMetaPB：policy_txn_id、evaluation_time、source_rowset_id、deleted_rows、task_id。重复任务若发现当前 Rowset 已包含同一或更高的明确水位，返回 NOOP_ALREADY_APPLIED。注意 TTL 延长不能恢复已删数据；策略变更语义必须定义为“只影响未来删除”，保留期延长需在旧截止线到达前发布或从备份恢复。

# 9. 关键障碍与边界分析

| 障碍 | 严重度 | 后果 | 处理 |
| --- | --- | --- | --- |
| tenant_id 非一一映射 | 阻断 | 误删其他 tenant 或无法应用字典 | 严格写入校验 + 历史扫描 + 运行时 invariant gate |
| 复合分区现有 TTL 不支持 | 阻断 | FE 不会调度或错误裁剪 | 新增三态分类器，不放宽旧 scheduler |
| 字典版本刷新竞态 | 高 | 同一任务前后使用不同 TTL | 固定 txn_id/evaluation_time；缺版本失败重排 |
| Rowset 不是单一/非交叠 | 高 | 首尾/顺序证明失效 | 资格门禁；NEED_BASE_COMPACTION |
| 跨 tenant ZoneMap 相关性缺失 | 高 | 错误 DROP | 无法充分证明一律 REWRITE |
| 部分链接统计缺失 | 高 | Compaction 评分、空间统计和校验漂移 | SegmentStatsPB；legacy 安全降级 |
| 普通 Compaction 并发 | 高 | 浪费、同版本替换竞态 | base+cumulative unique locks + rowset_id CAS |
| 多副本无可见屏障 | 高 | 查询在副本间短暂看到不同 TTL 结果 | MVP replication=1；后续定义最终一致或新事务协议 |
| Rollup/MV 不含 TTL 列 | 阻断 | 查询经 Rollup 仍看到过期数据 | 首期拒绝；长期要求每个 Index 有可计算 TTL 身份 |
| 共享存储不支持 hard link | 范围 | 对象存储无法按 inode 复用 | 本方案只走 local；lake 另做 segment reference metadata |

## 9.1 Page 级过滤的真实收益边界

Segment ZoneMap 能让 KEEP/DROP 文件完全避免重写，这是主要收益。对 REWRITE Segment，即使 Page ZoneMap 能判断某页不含过期目标，输出 Rowset 仍需要该页的保留行；当前没有“复制压缩 Page 并重写 footer”的接口，因此仍需读取并重新编码这些保留行。Page ZoneMap 可减少 TTL 表达式判断或 key-column 扫描，但不能把 REWRITE 的所有列 I/O 变成零。若未来要做 Page 级零拷贝，需要新的 page-offset 重排与索引/footer 合并协议，工作量远大于本方案。

## 9.2 Fulltext/GIN、Vector 与 replicated_storage

本地 whole-rowset linker 已处理 GIN 与 Vector 文件，因此技术上可以扩展到单 Segment；REWRITE Segment 由 SegmentWriter 重建索引。需要注意 FE 创建表时发现 GIN 且 replicated_storage=true 会把 replicated storage 关闭（OlapTableFactory.java:516-522）。这不影响本地 TTL 正确性，但要以实际 SHOW CREATE TABLE 结果为准。

## 9.3 本地存储与 Shared-data 的分界

POSIX FileSystem::link_file 调用 hard link（fs_posix.cpp:655-659），S3、Starlet、HDFS、Azure 均返回 NotSupported（fs_s3.cpp:431-433；fs_starlet.cpp:580-582；fs_hdfs.cpp:580-582；fs_azblob.cpp:461-463）。CompactionHandler 也已在 shared-data 与 local 模式分支（CompactionHandler.java:61-77）。因此 V1 必须在 FE/BE 双重拒绝 shared-data；后续 lake 版本应在 TabletMetadata 中复用旧 segment object name，而不是模拟 hard link。

# 10. 类关系与调用栈

## 10.1 FE 类关系

```text
PropertyAnalyzer
└─ parses ─> CompactionRetentionPolicy [NEW]
├─ persisted by ─> TableProperty / LocalMetastore
└─ binds ─> Dictionary + TenantTtlPolicySnapshot [NEW]

DynamicPartitionScheduler
└─ owns ─> TenantTtlScheduler [NEW]
├─ uses ─> TenantTtlPartitionClassifier [NEW]
├─ DROP ─> LocalMetastore.dropPartition
└─ REWRITE ─> TenantTtlCompactionTask [NEW]
└─ tracked by ─> TenantTtlTaskTracker [NEW]

LeaderImpl.finishTask
└─ TENANT_TTL_COMPACTION ─> TenantTtlTaskTracker.finish(...)
```

## 10.2 BE 类关系

```text
AgentServer / agent_task.cpp
└─ EngineTenantTtlCompactionTask [NEW]
├─ loads exact version ─> DictionaryCacheManager
├─ locks/selects ─> Tablet / Rowset
└─ runs ─> TenantTtlRowsetRewriter [NEW]
├─ classifies ─> SegmentTtlClassifier [NEW]
│ ├─ ColumnReader ZoneMap
│ └─ ColumnIterator ordinal seek
├─ writes ─> HorizontalRowsetWriter
│ ├─ add_linked_segment [NEW]
│ └─ add_chunk / flush / build
└─ commits ─> Tablet::modify_rowsets_without_lock
└─ stale/unused ─> StorageEngine GC
```

## 10.3 FE 调用栈（建议）

```text
DynamicPartitionScheduler.runAfterCatalogReady()
TenantTtlScheduler.schedule()
CompactionRetentionPolicyResolver.resolve(table)
DictionaryMgr.getTenantTtlPolicySnapshot(dictId)
TenantTtlPartitionClassifier.classify(partition, snapshot, now)
├─ DROP: LocalMetastore.dropPartition(db, table, clause)
├─ NOOP: metrics + next partition
└─ REWRITE:
TenantTtlScheduler.collectVisibleBaseTablets(partition)
TenantTtlCompactionTask.toThrift()
AgentTaskQueue.addTask() / AgentTaskExecutor.submit()

LeaderImpl.finishTask()
finishTenantTtlCompactionTask()
TenantTtlTaskTracker.onResult()
retry / complete / alert
```

## 10.4 BE 调用栈（建议）

```text
AgentServer::submit_tasks()
run_tenant_ttl_compaction_task()
StorageEngine::execute_task(EngineTenantTtlCompactionTask)
EngineTenantTtlCompactionTask::execute()
_validate_request_and_dictionary_version()
_lock_and_pick_single_rowset()
TenantTtlRowsetRewriter::rewrite()
SegmentTtlClassifier::classify_all()
HorizontalRowsetWriter::add_linked_segment() / add_chunk()
RowsetWriter::build()
Rowset::load() / verify() / verify_artifacts()
_commit_if_source_rowset_id_matches()
Tablet::modify_rowsets_without_lock()
Tablet::save_meta()
fill per-tablet TTenantTtlCompactionResult
finish_task(TFinishTaskRequest)
```

# 11. 文件级代码改造清单

## 11.1 FE 修改

| 文件 | 当前锚点 | 修改 |
| --- | --- | --- |
| common/util/PropertyAnalyzer.java | 205-208、463-500 | 新增属性常量和严格 dictionary_ttl 解析；不走任意 SQL Expr。 |
| catalog/TableProperty.java | 199-206、572-574、954-959 | 新增策略字段、build/get/set、Gson 恢复。 |
| server/OlapTableFactory.java | 727-737 | CREATE TABLE 解析、DDL 资格校验、注册。 |
| alter/AlterJobExecutor.java | 571-578 | ALTER 属性路由。 |
| server/LocalMetastore.java | 3876-3903、3929-3968 | 持久化/EditLog、注册/删除 scheduler、版本二次校验。 |
| clone/DynamicPartitionScheduler.java | 135-140、465-485 | 持有并在 FE 重启后重建 TenantTtlScheduler 表集合。 |
| catalog/Dictionary.java / DictionaryMgr.java | Dictionary.java:70-81；Mgr:353-359、764-802 | 持久化/同步 TTL policy snapshot；刷新成功原子发布。 |
| task/AgentTaskExecutor + leader/LeaderImpl.java | LeaderImpl:330-335、446-448 | 新增任务分发、结果解析、Tracker 回调。 |
| sql/planner load path | OlapTableSink 生成处（需实现时定位） | 把 strict tenant identity 标志和列 unique id 下发到 BE 写入校验。 |

## 11.2 FE 新增文件

| 建议路径（NEW） | 类/职责 |
| --- | --- |
| fe/fe-core/.../catalog/CompactionRetentionPolicy.java | 强类型属性、列绑定、字典 schema 与表资格校验 |
| fe/fe-core/.../catalog/TenantTtlPolicySnapshot.java | 字典 txn 与 global/bucket TTL bounds |
| fe/fe-core/.../clone/TenantTtlScheduler.java | 周期扫描、三态执行、批量/限流/重试 |
| fe/fe-core/.../clone/TenantTtlPartitionClassifier.java | 复合分区范围解析与数学裁决 |
| fe/fe-core/.../clone/TenantTtlTaskTracker.java | in-flight、结果、重试、持久化水位 |
| fe/fe-core/.../task/TenantTtlCompactionTask.java | 请求序列化与任务签名 |

## 11.3 BE 修改

| 文件 | 当前锚点 | 修改 |
| --- | --- | --- |
| agent/agent_server.cpp、agent_task.cpp/h | server:525-531；task:571-593 | 注册新任务、传播真实 Status、填逐 Tablet result。 |
| storage/dictionary_cache_manager.cpp/h | cpp:216-245、294-313 | TTL schema 校验、获取精确版本；可选导出 cache stats。 |
| storage/rowset/rowset.h/cpp | cpp:169-224、447-514 | link_segment_files_to，src/dst ordinal 重映射与 artifact rollback。 |
| storage/rowset/rowset_writer.h/cpp | cpp:145-168、508-562、758-790 | add_linked_segment、统计、加密 meta、异常清理。 |
| storage/compaction_task.h | 261-299 | 通用提交增强：version + rowset_id 校验。 |
| storage/tablet.cpp | 373-422、750-810 | 复用同版本替换；可增加 CAS 风格 helper。 |
| storage/storage_engine.cpp | 1234-1308 | 复用 unused GC；增加 TTL 指标/日志，无需改变引用语义。 |
| exec/tablet_sink.cpp | 700-767 | 严格 tenant identity 数据校验，发现非法值整批失败或按策略拒绝。 |
| gensrc/proto/olap_file.proto | 112-185 | SegmentStatsPB、TenantTtlRewriteMetaPB，使用新 field number。 |
| gensrc/thrift/*.thrift | Types:186-220；Agent:282-285；Master:77-97 | 新 task type、request、result、finish optional field。 |

## 11.4 BE 新增文件

| 建议路径（NEW） | 类/职责 |
| --- | --- |
| be/src/storage/task/engine_tenant_ttl_compaction_task.{h,cpp} | 任务准入、锁、字典版本、选 Rowset、结果封装 |
| be/src/storage/tenant_ttl/tenant_ttl_policy.{h,cpp} | 字典 probe、TTL cutoff、缺失默认值 |
| be/src/storage/tenant_ttl/segment_ttl_classifier.{h,cpp} | ZoneMap + exact boundary 三态分类 |
| be/src/storage/tenant_ttl/tenant_ttl_rowset_rewriter.{h,cpp} | 按源顺序 mixed rewrite、验证与提交准备 |
| be/test/storage/tenant_ttl/*_test.cpp | 分类、链接、并发、失败恢复、索引与 GC 单测 |

## 11.5 核心提交伪代码

```text
Status commit(Tablet* tablet, RowsetPtr source, RowsetPtr output) {
unique_lock l(tablet->get_header_lock());
RowsetPtr current = tablet->get_rowset_by_version(source->version());
if (current == nullptr || current->rowset_id() != source->rowset_id()) {
return Status::Aborted("STALE_ROWSET");
}
tablet->modify_rowsets_without_lock({output}, {current}, &to_replace);
tablet->save_meta(config::skip_schema_in_rowset_meta);
Rowset::close_rowsets({current});
for (auto& rs : to_replace) StorageEngine::instance()->add_unused_rowset(rs);
return Status::OK();
}
```

# 12. 测试与验证方案

## 12.1 FE 单元测试矩阵

| 测试组 | 必须覆盖 |
| --- | --- |
| 属性 | 合法/非法语法、字典不存在、类型错误、default 越界、ALTER 清空、EditLog replay |
| DDL 资格 | 非 DUP、错误 sort key、生成表达式不一致、timestamp 非 BIGINT、Rollup、shared-data |
| 分区裁决 | U=cutoff 边界、L=cutoff、global/bucket min/max、默认 tenant、hash collision、时区/DST |
| 调度 | NOOP 不发、DROP 走 catalog、REWRITE 按 BE 分组、限流、超时、FE restart、重复 finish |
| 字典 | 刷新成功/失败、版本更新中、snapshot 缺失、旧版本任务、schema 变更 |

## 12.2 BE 单元测试矩阵

| 测试组 | 必须覆盖 |
| --- | --- |
| Segment 分类 | 目标范围外；单 tenant 全过期/全保留/部分；跨 tenant 边界；空 Segment；NULL tenant_id |
| 排序 | 多个 KEEP/REWRITE 交错；REWRITE 拆多 Segment；输出 NONOVERLAPPING；Rowset::verify |
| 文件复用 | inode 相同、目标 ordinal 连续、旧路径删除后新路径可读、跨文件系统拒绝 |
| 索引 | ShortKey/ZoneMap/Bloom/Bitmap；GIN 目录；Vector 文件；REWRITE 重建后查询正确 |
| 加密 | segment_encryption_metas 重排；透明加密开关；链接/重写混合 |
| 统计 | rows/data/index/raw-size 精确；legacy 无 SegmentStats 安全降级 |
| 并发 | 普通 Base/Cumulative、两个 TTL、schema change、clone、源 Rowset 被替换后 CAS 失败 |
| 崩溃恢复 | 每个文件操作 failpoint、build 前后、meta 提交前后、finish RPC 丢失、幂等重试 |
| GC/查询 | 提交前打开旧 Rowset，提交后继续读；use_count 归零前不删；hardlink inode 生命周期 |

## 12.3 集成复测步骤

1. 创建符合约束的表和字典，确认 SHOW CREATE TABLE、字典 id/txn、policy snapshot 的 global/bucket bounds。

2. 构造 Segment 布局：纯 tenant、跨 tenant 边界、目标范围外、临界 timestamp；触发 Base Compaction，确认 Rowset NONOVERLAPPING。

3. 以 dry_run 运行任务，记录每个 Segment 的 min/max、first/last、分类与预计删除行数；用 SQL 全量计算对账。

4. 执行真实 rewrite；核对 source/output Rowset version 相同、rowset_id 不同、目标文件 ordinal 连续。

5. 对 KEEP 文件执行 inode/文件大小校验；对 REWRITE 文件确认 inode 不同、ShortKey/ZoneMap/索引均可读。

6. 查询验证：按 tenant、tenant_id、timestamp、全文/向量索引（如启用）核对结果；运行 Rowset::verify。

7. 在提交前保持一个长查询读取旧 Rowset；提交后新查询看新 Rowset，旧查询完成后观察 GC 才删除旧路径。

8. 注入字典刷新、普通 Compaction 和 BE 重启；确认只出现 NOOP/STALE/重试，不出现错误提交。

## 12.4 可观测性

| 指标/日志 | 建议字段 |
| --- | --- |
| FE scheduler | table/partition decisions、snapshot version、drop/rewrite/noop counts、queue age、retry reason |
| BE task | tablet/rowset ids、segment classifications、rows expired、bytes linked/read/written、elapsed by phase |
| 安全告警 | invalid tenant、policy missing、stale rowset、verify failure、orphan cleanup failure、multi-replica lag |
| Profile | ZoneMapFilteredSegments、LinkedSegments、DroppedSegments、RewrittenSegments、HardlinkBytesSaved |

# 13. 工作量评估与交付分期

## 13.1 人日估算

| 工作包 | 人日 | 主要责任 |
| --- | --- | --- |
| A. 属性/元数据/DDL 校验 | 4–6 | FE |
| B. 字典 TTL snapshot 与版本绑定 | 8–12 | FE + BE dictionary |
| C. 复合分区分类、scheduler、限流/重试 | 10–15 | FE |
| D. Thrift 任务、capability、逐 Tablet 结果 | 5–8 | FE + BE |
| E. strict tenant 写入门禁与历史校验工具 | 6–10 | FE planner + BE sink |
| F. Segment 分类器与精确边界读取 | 8–12 | BE |
| G. partial hard link、artifact remap、SegmentStats | 15–24 | BE storage |
| H. rewrite、校验、CAS 提交、幂等/GC | 10–16 | BE storage |
| I. 单测、集成、failpoint、可观测性 | 18–28 | 全链路 |
| 合计：本地单副本生产 MVP | 84–131 | 约 17–26 人周 |

> **排期解释**
>
> 两名熟悉 FE/BE 存储的工程师并行，生产 MVP 建议预留 9–13 周（含联调、故障注入与灰度）。若只做无索引、无统计兼容、无自动调度的 POC，可压缩到约 20–30 人日，但不能直接上线。

## 13.2 分期

| 阶段 | 范围 | 退出标准 |
| --- | --- | --- |
| P0 设计/数据门禁 | 属性仅解析与 dry-run；历史 tenant canonical 扫描 | 零非法 tenant；分区决策与 SQL 全量对账一致 |
| P1 BE POC | 手工单 Tablet、无外部索引、replica=1、single Rowset | KEEP inode 复用；DROP/REWRITE 行数与查询正确 |
| P2 生产 MVP | FE scheduler、任务结果、SegmentStats、失败恢复、Base Index | 全测试矩阵通过；可灰度、可回滚、无 silent success |
| P3 能力扩展 | GIN/Vector、更多 Index、replica>1 最终一致 | 副本 lag 可观测并有 SLA；索引全覆盖 |
| P4 Shared-data | lake object reference reuse + metadata transaction | 不依赖 hard link；对象 GC 与 snapshot 引用正确 |

## 13.3 不应塞进首期的内容

- Page 级压缩块零拷贝；它需要重组 Segment footer 与所有 Page index。

- 任意 SQL retention condition 或 UDF；会让策略版本、确定性和审计失控。

- 对缺 tenant_id/time 的 Rollup 做猜测性删除。

- 在 replication_num>1 下宣称强一致可见，而没有新事务版本或副本屏障。

- Shared-data 用“复制对象”冒充 hard link；对象引用与 GC 必须走 lake metadata。

# 14. 灰度、回滚与验收标准

## 14.1 灰度顺序

1. 先部署能读新 Protobuf/Thrift 但不开启功能的 BE/FE，确认 capability 全覆盖。

2. 属性以 dry_run=true 注册，只输出 FE 分区决策和 BE Segment 分类，不创建新 Rowset。

3. 选择一张 replication_num=1、无 Rollup 的测试表，按 1% Tablet 白名单执行。

4. 对账 rows、tenant 分布、文件 inode、RowsetMeta 统计、查询结果和 GC；连续运行多个 TTL 周期。

5. 逐步扩大 Tablet 并打开自动调度；保留每表 kill switch 与全局并发/带宽上限。

## 14.2 回滚策略

- 提交前失败：删除新 Rowset artifacts，旧 Rowset 未改变，可直接重试。

- 提交后程序回报丢失：通过输出 Rowset 的 rewrite watermark 识别 ALREADY_APPLIED，不能再删一次。

- 发现逻辑缺陷：立即关闭表属性/全局开关，停止未来 rewrite；已删除数据只能从备份/上游重灌，不能靠旧 Rowset 长期恢复。

- 代码降级：旧 BE 忽略新 Protobuf 字段并能读数据，但在 mixed-version 期间 FE 不得继续派发 Tenant-TTL。

## 14.3 上线验收门槛

| 类别 | 硬指标 |
| --- | --- |
| 正确性 | SQL 全量对账 0 差异；无 invalid tenant；所有输出 Rowset::verify 成功；无 silent success |
| 并发 | TTL/普通 Compaction/schema change 竞态均只产生 STALE/重试，不产生同版本误替换 |
| 恢复 | 所有 failpoint 可重启恢复；orphan 可 GC；重复任务幂等 |
| 收益 | HardlinkBytesSaved、写放大和任务时长相对全量 Compaction 达到预设目标 |
| 运维 | 有 per-table kill switch、限流、结果查询、告警、手工 dry-run 与重试入口 |

> **Go / No-Go 建议**
>
> 在 strict tenant 数据门禁、SegmentStats、rowset_id CAS、逐 Tablet 结果闭环四项完成前，结论为 No-Go；四项完成并在 replication_num=1、本地存储、单 Base Index 边界通过故障注入后，可进入受控灰度。

# 附录 A：关键源码锚点

| 主题 | 源码路径与行号 |
| --- | --- |
| FE 分区 TTL 注册/扫描 | fe/fe-core/src/main/java/com/starrocks/clone/PartitionTTLScheduler.java:61-133 |
| FE 分区删除 | .../PartitionTTLScheduler.java:162-186 |
| FE 单列 Range 限制 | .../PartitionTTLScheduler.java:189-215 |
| Retention 属性解析 | fe/fe-core/src/main/java/com/starrocks/common/util/PropertyAnalyzer.java:463-500 |
| 字典成功版本 | fe/fe-core/src/main/java/com/starrocks/catalog/Dictionary.java:70-81；DictionaryMgr.java:353-359 |
| 字典刷新完成 | fe/fe-core/src/main/java/com/starrocks/catalog/DictionaryMgr.java:764-802 |
| BE 字典原子 commit | be/src/storage/dictionary_cache_manager.cpp:216-245 |
| BE 精确字典版本 | be/src/storage/dictionary_cache_manager.cpp:294-313 |
| 手工 Compaction 派发 | fe/fe-core/src/main/java/com/starrocks/alter/CompactionHandler.java:77-120 |
| 当前 Compaction 请求 | gensrc/thrift/AgentService.thrift:282-285 |
| BE 丢弃手工任务状态 | be/src/agent/agent_task.cpp:571-593 |
| FE 只移除 Compaction task | fe/fe-core/src/main/java/com/starrocks/leader/LeaderImpl.java:446-448 |
| 标准 Compaction build/commit | be/src/storage/compaction.cpp:105-159、355-369 |
| Shortcut hard link | be/src/storage/compaction_task.cpp:206-265 |
| Rowset whole-file link | be/src/storage/rowset/rowset.cpp:447-514 |
| RowsetWriter build/sync/meta | be/src/storage/rowset/rowset_writer.cpp:145-168 |
| RowsetWriter whole-rowset add | be/src/storage/rowset/rowset_writer.cpp:758-790 |
| Segment 加载顺序 | be/src/storage/rowset/rowset.cpp:194-224 |
| NONOVERLAPPING union | be/src/storage/rowset/rowset.cpp:813-861 |
| Rowset 排序验证 | be/src/storage/rowset/rowset.cpp:1047-1093 |
| ZoneMap 创建/字符串截断 | be/src/storage/rowset/segment_writer.cpp:163-182 |
| 字符串 ZoneMap 上界 | be/src/storage/rowset/zone_map_index.cpp:231-249 |
| Segment ZoneMap pruning | be/src/storage/rowset/segment.cpp:307-347 |
| 精确 ordinal seek | be/src/storage/rowset/column_iterator.h:96-108 |
| 同版本 Rowset 替换 | be/src/storage/tablet.cpp:373-422 |
| 现有提交仅查 version | be/src/storage/compaction_task.h:261-299 |
| stale→unused | be/src/storage/tablet.cpp:750-810 |
| 引用计数 GC | be/src/storage/storage_engine.cpp:1234-1308 |
| POSIX hard link | be/src/fs/fs_posix.cpp:655-659 |
| 对象/远端 FS 不支持 link | be/src/fs/fs_s3.cpp:431-433；fs_starlet.cpp:580-582；hdfs/fs_hdfs.cpp:580-582 |

# 附录 B：建议修正后的表属性片段

```text
PROPERTIES (
"bloom_filter_columns" = "queries",
"compaction_retention_condition" =
"dictionary_ttl('dns_tenant_retention_dict', 180)",
"tenant_ttl_timezone" = "Asia/Shanghai",
"tenant_ttl_strict_numeric_tenant" = "true",
"compression" = "ZSTD(3)",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
```

说明：这只是目标语法，当前 commit 尚不识别前三个 Tenant-TTL 属性。真正上线前还应确认实际环境对复合表达式分区、生成列作为 sort key、字典 schema 的支持状态，并以 SHOW CREATE TABLE 的回显为准。

# 附录 C：最终建议

把该功能定义为“策略驱动的单 Rowset 重写”，而不是普通 Compaction 的一个过滤条件。FE 负责做可证明安全的分区级三态裁决，BE 负责在固定策略版本下做可证明安全的 Segment 级三态裁决；两层遇到不确定性都向更保守的 REWRITE/NOOP 退化，绝不向 DROP 猜测。这样才能在获得显著读写放大收益的同时，保持 TTL 删除的可审计性与故障可恢复性。
