# StarRocks Tenant-TTL Compaction FE 详细编码计划

> 本文档是 FE 需求澄清完成后的实施计划与实施记录。它把已确认的 FE/BE 契约映射到源码、独立提交轮次、测试和退出条件；第 17 节记录最终实现与验证结果。

源码基线：StarRocks `4.0.11-zc_docs`，commit `0a590df8d`

建立日期：2026-09-11

当前状态：第 017～030 轮已完成

## 1. 输入文档与结论优先级

本计划综合以下材料：

1. `StarRocks_Tenant_TTL_Compaction_FE_需求澄清记录.md`。
2. 当前 BE Tenant-TTL 实现、`StarRocks_Tenant_TTL_Compaction_BE_详细编码计划.md`、`StarRocks_Tenant_TTL_Compaction_代码Review对齐记录.md` 和 `be_tenant_ttl_review.md`。
3. `DICTIONARY功能与社区差异.md`。
4. `compaction ttl简要方案.md`、`StarRocks_Tenant_TTL_Compaction_研发澄清与实施记录.md` 和 `StarRocks_Rowset_Tenant_TTL_Rewrite_可行性与代码改造方案.md`。

发生冲突时，优先级固定为：

1. FE 需求澄清记录中标记“已确认”的条款。
2. 当前代码中已完成的 BE 契约。
3. 本计划的实施建议。
4. 早期方案和背景材料。

早期文档中的 `tenant_id`、固定 Short Key、固定 64 tenant bucket、业务表物化 `retentionDays`、通过普通 `COMPACTION` 或手工 HTTP 下发任务等设计均已失效，不得重新带入实现。

## 2. 首期目标与非目标

### 2.1 首期交付目标

首期交付以下完整闭环：

1. 支持在 CREATE TABLE 或轻量 ALTER TABLE 属性中配置三参数：

   ```sql
   dictionary_ttl(dict_name, table_key, default_days)
   ```

2. 持久化 Dictionary ID、逻辑 `table_key`、默认 TTL、tenant 列身份、时间列身份、分区表达式指纹和声明式时区。
3. 只为被 Tenant-TTL 引用的 Dictionary 按需构建 FE 可枚举、不可变策略快照；普通 Dictionary 查询路径保持不变。
4. 从固定策略快照和固定评估时间生成分区级 `FE_NOOP`、`DROP_LOGICAL_PARTITION`、`ROWSET_REWRITE` 或 `FAIL_CLOSED` 计划。
5. 对 Range/List 分区做严格上界证明；无法证明的物理分区单独 fail-closed。
6. 通过专用 Agent Task 将完整、规范化的 `DELETE_LIST` 或 `KEEP_LIST` 下发至每个 Base Index Tablet 的全部 Catalog Replica。
7. 全局串行执行破坏性单元，正确处理重试、未知结果、拓扑变化、Leader 切换和迟到结果。
8. 持久化 Physical Partition 级完成进度，支持到期事件、首次追平、策略变化和迟到写入补偿。
9. 通过 `SHOW TENANT TTL STATUS FROM db.table [FOR TENANT '...']` 提供表级绑定状态和按 tenant 策略解析。

### 2.2 首期明确不做

1. 不修改业务写入流程，不校验实际写入时区，不禁止迟到写入。
2. 不新增业务表 `retentionDays`、`expire_at`、`tenant_id` 或 tenant bucket 列。
3. 不支持 Primary Key 业务表、Shared-data、Rollup、同步物化索引和临时分区。
4. 不支持在分区内部按 `recordTimestamp` 精确删行；FE 只能证明整个物理分区内某 tenant 全部过期。
5. 不实现任务取消协议；已经下发的任务自然结束，结果由 fingerprint fencing 隔离。
6. 不实现多任务并发；当前 Leader 同时最多一个 Catalog 删除或一个 Replica Agent Task。
7. 不实现跨 RPC Session 或首期分页 Dictionary 导出。
8. 不实现 `KEEP_LIST` 超限分片；超限按分区 fail-closed。
9. 不恢复已按旧策略删除的数据，不承诺策略变更的跨表原子切换。

## 3. 当前实现基础与主要缺口

### 3.1 可以直接复用的基础

| 能力 | 当前实现位置 | 复用方式 |
| --- | --- | --- |
| Dictionary 查询刷新 | `DictionaryMgr.RefreshDictionaryCacheWorker` | 保留 BEGIN、查询、`DictionaryCacheSink`、COMMIT 和普通状态提交；成功提交后异步触发策略导出 |
| Dictionary Cache | `be/src/storage/dictionary_cache_manager.*` | 从当前成功版本取得 `shared_ptr`，新增只读类型化全量导出 |
| 表属性持久化 | `TableProperty`、`ModifyTablePropertyOperationLog`、`LocalMetastore` | 在同一表元数据提交中持久化属性字符串和结构化绑定 |
| 自动删分区 | `PartitionTTLScheduler`、`LocalMetastore.dropPartition()` | 复用正式非临时分区 FORCE 删除和 EditLog 路径，不复用 `partition_retention_condition` 的内部查询语义 |
| Agent Task 框架 | `AgentTask`、`AgentBatchTask`、`AgentTaskQueue`、`LeaderImpl.finishTask()` | 新增独立任务类型、请求、结果和完成处理器 |
| Tablet/Replica 元数据 | `OlapTable`、`Partition`、`PhysicalPartition`、`LocalTablet`、`Replica` | 生成 Base Index 全 Replica 拓扑和 fingerprint |
| BE Tenant-TTL 执行器 | `EngineTenantTtlCompactionTask` 及其 request/result | Agent worker 做一对一转换后直接调用，不改变内部执行语义 |

### 3.2 必须新增的能力

1. `compaction_retention_condition` 的专用受限语法解析和结构化绑定。
2. Tenant/时间列身份、分区表达式、时区和 Rollup 等静态资格校验。
3. Dictionary 当前成功 Cache 的单次逻辑全量导出 RPC。
4. FE 不可变策略快照、Builder、按需引用、追平和错误隔离。
5. Range/List 分区时间上界解释器。
6. 表级不可变评估上下文、策略解析器、名单极性选择器和资源上限校验。
7. 分区执行进度的 Image/EditLog 持久化。
8. 专用 Tenant-TTL Agent Task 传输适配层。
9. 到期/策略/版本驱动的 Leader 调度器及完整结果收敛。
10. 表级状态和 tenant 策略解析查看接口。

## 4. 总体架构

```text
CREATE/ALTER TABLE
    │
    ├─ TenantTtlPropertyParser / TenantTtlBindingAnalyzer
    │      └─ TableProperty
    │          ├─ TenantTtlDictionaryBinding
    │          └─ TenantTtlTableBinding
    │
Dictionary refresh success N
    │
    ├─ ordinary Dictionary state/cache commit remains successful
    └─ TenantTtlPolicySnapshotManager (only when referenced)
           ├─ export exact N from one BE/CN
           ├─ TenantTtlPolicySnapshotBuilder
           └─ publish immutable TenantTtlPolicySnapshot
                    └─ TablePolicy(table_key)

TenantTtlScheduler (Leader only)
    │
    ├─ TenantTtlEvaluationContext (snapshot N + evaluation_time fixed)
    ├─ TenantTtlPartitionBoundResolver
    ├─ TenantTtlPolicyPlanner / TenantTtlScheduleDecision
    └─ TenantTtlEvaluationContext.PartitionPlan
           ├─ FE_NOOP
           ├─ DROP_LOGICAL_PARTITION ──> LocalMetastore.dropPartition()
           ├─ ROWSET_REWRITE ──────────> TenantTtlCompactionTask per Replica
           └─ FAIL_CLOSED
                    │
                    └─ TenantTtlPartitionProgressManager
                         (only advances after confirmed completion)
```

### 4.1 模块隔离原则

1. `partition_retention_condition` 继续使用 `partitions_meta + dictionary_get()` 和普通 Dictionary Cache，不调用 Tenant-TTL Snapshot Manager。
2. 未被 Tenant-TTL 引用的 Dictionary 不导出 FE 策略快照，刷新链路不增加额外工作。
3. Tenant-TTL 导出或 Builder 失败只保留旧 FE 快照或进入等待状态，不回滚普通 Dictionary 的成功事务。
4. FE 计划器不读取 BE Rowset/Segment；BE 不读取 Dictionary 或 TTL 天数。
5. Catalog 分区删除和 Rowset Rewrite 是互斥执行路径，禁止相互降级。

## 5. 核心对象与建议包结构

以下类名是本计划的建议。澄清文档已经确认的 `TenantTtlPolicySnapshotManager`、`TenantTtlPolicySnapshotBuilder`、`TenantTtlPolicySnapshot` 和 `TablePolicy` 保持不变；其他类名可在首轮编码评审时按现有工程风格微调。

### 5.1 Catalog 与 DDL

建议目录：`fe/fe-core/src/main/java/com/starrocks/catalog/` 和 `.../sql/analyzer/`

```text
TenantTtlDictionaryBinding {
    formatVersion
    dictionaryId
    dictionaryName
    tableKey
    defaultDays
}

TenantTtlTableBinding {
    formatVersion
    tenantColumnId
    tenantColumnUniqueId
    timeColumnId
    timeColumnUniqueId
    partitionExpressionType
    listTimeComponentIndex
    normalizedTimeZone
    partitionExpressionFingerprint
}
```

`TableProperty` 同时保存用户可见属性字符串和上述结构化对象。运行时不得每轮重新解析属性字符串或按列名重新猜测身份。

### 5.2 策略快照

建议目录：`fe/fe-core/src/main/java/com/starrocks/tenantttl/policy/`

```text
TenantTtlPolicySnapshotManager
├─ references: dictionaryId -> Set<TableIdRef>
├─ currentSnapshots: dictionaryId -> TenantTtlPolicySnapshot
├─ retryStates / blockedTxnIds / generations
└─ export executors and publish fencing

TenantTtlPolicySnapshot
├─ dictionaryId
├─ snapshotTxnId
├─ snapshotTime
├─ ignoredZeroRetentionRows
├─ immutable Map<tableKeyBytes, TablePolicy>
└─ immutable raw-byte policy representation

TablePolicy
├─ optional tableDefaultDays
├─ immutable Map<tenantBytes, retentionDays>
└─ stable semantic fingerprint input
```

原始 tenant/table key 使用不可变字节键对象，比较、排序和 hash 均按 unsigned/raw bytes 明确定义；不得依赖 Java 默认 Charset、Locale 或 Unicode 归一化。

### 5.3 评估与计划

建议目录：`fe/fe-core/src/main/java/com/starrocks/tenantttl/planner/`

```text
TenantTtlEvaluationContext
TenantTtlPartitionTimeEvaluator
TenantTtlPolicyResolver
TenantTtlFilterPlanner
TenantTtlPartitionPlan
TenantTtlReplicaTaskSpec
TenantTtlFingerprintCodec
```

`TenantTtlPartitionPlan` 为不可变对象，并携带发布/执行前复核所需的全部 fingerprint 和 Catalog 身份。计划类型只允许 `FE_NOOP`、`DROP_LOGICAL_PARTITION`、`ROWSET_REWRITE`、`FAIL_CLOSED`。

### 5.4 调度与进度

建议目录：`fe/fe-core/src/main/java/com/starrocks/tenantttl/scheduler/`

```text
TenantTtlPartitionProgressManager
TenantTtlCompactionScheduler
TenantTtlCompactionCoordinator
TenantTtlRetryPolicy
TenantTtlRuntimeStatus
```

进度 Manager 负责持久化“已经完成的事实”；Scheduler 的队列、候选上下文、在途 Agent Task 和部分 Replica 成功仅为 Leader 本地运行时状态，不持久化。

## 6. 关键数据契约

### 6.1 Dictionary 导出 Protobuf

按 FE-DICT-011 新增：

- `PDictionaryCacheExportOutcome`
- `PExportDictionaryCacheRequest`
- `PTenantTtlPolicyEntryPB`
- `PTenantTtlPolicyBatchPB`
- `PCompressedTenantTtlPolicyBatchPB`
- `PExportDictionaryCacheResult`
- `PInternalService.export_dictionary_cache(...)`

实现约束：

1. 单次 RPC 返回完整快照，不跨请求拼接。
2. `tenant`、`table_name` 使用 `bytes`；`retention_days` 使用 `int32`。
3. BE 先在 Manager 锁内原子取得 `(dictionaryId, txnId, schema, cache shared_ptr)`，随后释放 Manager 锁并在局部 `shared_ptr` 上完成导出。
4. Cache 版本不等于 `expected_txn_id` 时返回 `status=OK + VERSION_MISMATCH + actual_txn_id`，不返回其他版本数据。
5. 使用类型化批次、默认 SNAPPY、低收益时无压缩；每批和整体做 CRC32C、行数和字节数校验。
6. 任一上限或导出错误都不携带部分成功批次。
7. 不复用 FE 无法解码的现有 `ChunkPB.data` 私有格式。

### 6.2 Tenant-TTL Agent Thrift

在现有 Thrift 末尾追加兼容字段和枚举值：

- `TTaskType.TENANT_TTL_COMPACTION`，位于 `NUM_TASK_TYPE` 之前。
- `TTenantTtlFilterMode`、`TTenantTtlFilter`。
- `TTenantTtlPolicyWatermark`、`TTenantTtlSchemaExpectation`。
- `TTenantTtlCompactionReq`。
- `TTenantTtlTaskCode`、`TTenantTtlRowsetAction`、`TTenantTtlRowsetResult`。
- `TTenantTtlCompactionResult`。
- `TAgentTaskRequest.tenant_ttl_compaction_req` 可选字段。
- `TFinishTaskRequest.tenant_ttl_compaction_result` 可选字段。

传输结构与 BE 内部 `TenantTtlCompactionRequest/Result` 一对一映射。tenant 名单建议使用 Thrift `binary` 而不是 Java `String`，避免 UTF-8 重编码破坏原始字节；C++ 侧仍映射到 `std::string` 字节容器。

FE 成功判定必须同时满足：

```text
code in {SUCCESS, NOOP_VERIFIED}
task_id/tablet_id/partition_id match immutable request
processed_through_version >= fe_observed_max_version
all FE-side binding/policy/boundary/topology/watermark fingerprints still match
```

### 6.3 持久化契约

1. 表绑定随 `TableProperty` Image 持久化，并与属性字符串使用同一条表属性 EditLog 提交。
2. `DropDictionaryInfo` 新增可选 `dictionary_id`；新日志按 ID 回放，旧日志继续按名称兼容。
3. 新增 `TenantTtlPartitionProgress` 和专用 Progress Manager MetaBlock。
4. 新增进度 upsert/remove（允许批量）的 EditLog 操作和 replay 入口。
5. Progress Key 固定为 `(dbId, tableId, physicalPartitionId)`。
6. 只有计划完成才写持久化进度；创建、排队、发送、单 Replica 成功和未知结果均不得写完成水位。
7. 旧 Image 不含新字段时按“未绑定/无进度”读取；禁止因缺少新字段而将普通表错误注册进调度器。

## 7. 端到端状态与并发模型

### 7.1 Dictionary 快照状态

表级主要状态：

```text
DISABLED
INVALID
WAITING_DICTIONARY
WAITING_POLICY_SNAPSHOT
ACTIVE
PAUSED
```

普通 Dictionary 最新成功事务与 FE SnapshotTxnId 是两个水位。导出失败允许 SnapshotTxnId 短暂落后；可用旧快照继续服务。没有任何成功 FE 快照时必须等待，不得直接使用属性默认值。

系统错误使用 `10 秒起步、10 分钟封顶、±20% jitter` 退避；每次重试重新读取 FE 权威的 DictionaryLastSuccessTxnId。确定性错误记录 blocked txn，不对同一事务自动重试。

### 7.2 表级评估固定输入

一轮只针对一张表：

1. 在表读锁下捕获绑定、列身份、分区表达式、时区和 binding fingerprint。
2. 按持久化 Dictionary ID 取得一个不可变策略快照强引用。
3. 读取一次 Unix epoch 秒作为 `evaluation_time_epoch_seconds`。
4. 枚举分区、生成计划和 Replica Task Spec 时只使用该上下文。
5. 发布计划前再次检查表绑定 fingerprint；不一致则丢弃整轮候选。
6. Manager 中快照切换不修改已经物化或已下发的任务。

### 7.3 全局串行破坏性执行

Leader-local Coordinator 同时只允许一个破坏性单元：

- 一个 `DROP_LOGICAL_PARTITION`；或
- 一个 `(backendId, tabletId)` Replica Agent Task。

Replica Task 以稳定 `(tabletId, backendId)` 顺序执行。明确未进入执行或已经终止的可重试结果可以释放全局槽并进入退避；网络超时、响应丢失和 `TTL_ALREADY_RUNNING` 保持原不可变 task ID 收敛，不能另发不同请求并放开冲突任务。

### 7.4 Leader 切换

1. 新 Leader 从 TableProperty 重建 Dictionary 引用索引。
2. 从持久化 Progress 和当前 Catalog 重建调度候选，不恢复旧 Leader 的在途 Agent Task 或部分 Replica 成功集合。
3. 新 Leader 为重新规划的任务分配新 task ID。
4. 旧任务可能继续在 BE 自然完成；BE Tablet admission 防止同 Tablet 冲突，新任务最终通过成功或 `NOOP_VERIFIED` 收敛。
5. 新 Leader 收到不在本地 AgentTaskQueue 的旧结果时只确认、记录，不推进当前进度。

## 8. 分轮实施总览

所有轮次均满足以下规则：

1. 一轮一个独立 commit，不混入无关改动。
2. commit subject 使用 `<type>(<scope>): [NNN] <summary>`。
3. commit body 包含非空 `Problem`、`Implementation`、`Compatibility`、`Tests`。
4. 测试只记录实际执行项；未执行的相关测试写明原因。
5. 每轮开始前检查工作树，保留用户和上游已有修改。

建议轮次如下：

| 轮次 | 主题 | 主要产物 | 前置依赖 |
| --- | --- | --- | --- |
| 017 | 表属性语法与持久化绑定 | 两类 binding、解析器、TableProperty round-trip | 禁用语法在 018 前确认 |
| 018 | DDL 准入与绑定生命周期 | CREATE/ALTER 校验、引用原子切换、Rollup guard、drop/recreate | 017；禁用语义确认 |
| 019 | Dictionary 全量导出 RPC | 类型化 Protobuf、BE 精确版本导出、FE RPC client | 无业务调度依赖 |
| 020 | FE 策略快照模型与 Builder | 完整性校验、零值过滤、重复键拒绝、不可变快照 | 019 的 PB 类型 |
| 021 | Snapshot Manager 与刷新集成 | 按需导出、旧快照保留、退避追平、Leader 重建 | 018～020；BE/CN 重启语义确认 |
| 022 | 时间分区绑定和边界证明 | Range/List 解释器、时区、MAXVALUE/DEFAULT fail-closed | 017～018 |
| 023 | TablePolicy 解析和名单规划 | 三级回退、NULL 最大 TTL、DELETE/KEEP/NOOP/DROP | 020、022 |
| 024 | 不可变评估上下文与指纹 | context、五类 fingerprint、Replica Task Spec | 021～023 |
| 025 | 分区进度持久化与恢复 | Progress Manager、Image/EditLog/replay/GC | 024 |
| 026 | 正式 Tenant-TTL Agent Task 适配 | Thrift、FE AgentTask、BE worker、finish result | BE core 已完成；混部能力门槛确认 |
| 027 | Leader 调度与 NOOP/Catalog 删除 | 触发器、四态分流、DROP 复核、全局槽框架 | 024～026 |
| 028 | 全 Replica Rewrite 编排与收敛 | 串行下发、重试、未知结果、拓扑/Leader fencing | 025～027 |
| 029 | SHOW 状态与诊断 | 表状态、tenant resolve、双水位、错误信息 | SHOW 契约确认；021～028 |
| 030 | 端到端故障测试与首期收口 | 多分区/Tablet/Replica、重启、切主、长链路回归 | 全部前序轮次 |

019、020 的纯导出/Builder 工作可以与 022 的纯时间边界单元在人员层面并行准备，但同一工作树仍按独立 commit 顺序合入。026 可以在 023～025 期间独立开发，最终在 027 前合入。

## 9. 各轮详细计划

### 第 017 轮：表属性语法与持久化绑定

建议提交：`feat(catalog): [017] persist tenant ttl table bindings`

#### 实现

1. 在 `PropertyAnalyzer` 注册 `compaction_retention_condition` 和 `compaction_retention_time_zone`。
2. 新增只接受 `dictionary_ttl(<string>, <string>, <positive int>)` 的专用解析器；不注册通用 SQL scalar function。
3. 校验函数名、参数数量和字面量类型；`table_key` 为非空 `db.table`；`default_days` 为 `[1, INT_MAX]`。
4. 新增 `TenantTtlDictionaryBinding` 和 `TenantTtlTableBinding` 可选字段及 format version。
5. 在 TableProperty build/gsonPostProcess/复制/SHOW CREATE property round-trip 中保留字段。
6. 不在本轮启动调度、不访问 BE、不构建快照。

#### 主要文件

- `fe/fe-core/src/main/java/com/starrocks/common/util/PropertyAnalyzer.java`
- `fe/fe-core/src/main/java/com/starrocks/catalog/TableProperty.java`
- `fe/fe-core/src/main/java/com/starrocks/catalog/OlapTable.java`
- 新增 binding 和 parser 类
- 对应 FE test 类

#### 测试

1. 三参数合法表达式、引号/转义、合法逻辑 table key。
2. 参数缺失/多余、非字面量、空名、非法 key、0/负数/INT 溢出拒绝。
3. 属性字符串和结构化对象 JSON/Image round-trip。
4. 旧 TableProperty JSON 无新字段兼容加载。
5. SHOW CREATE 保留用户可见表达式和规范化时区。

#### 退出条件

绑定对象可稳定持久化和回放，但不会触发任何数据删除。

### 第 018 轮：DDL 准入、引用原子切换与对象生命周期

建议提交：`feat(catalog): [018] validate tenant ttl bindings`

#### 实现

1. CREATE/ALTER 时完成 FE-DDL-002 的全部静态检查：
   - shared-nothing 本地 OLAP、`DUP_KEYS`、非临时表；
   - 固定名称 `tenant` 为 VARCHAR，持久化 Column ID/Unique ID；
   - 固定 `recordTimestamp BIGINT` Unix 秒；
   - 受支持的 Range/List 分区表达式；
   - from_unixtime 表显式时区及与 dynamic partition 时区一致；
   - 无 Rollup/同步物化索引且表状态稳定；
   - Dictionary 存在、三列类型/KEY-VALUE 顺序正确；
   - 源表是 `(tenant, table_name)` Primary Key 的 OLAP Primary Key 表。
2. DDL 不要求 Dictionary 已成功刷新，不扫描业务表或策略表数据。
3. 表属性字符串和两类 binding 在一次元数据操作/EditLog 中提交。
4. 建立 Snapshot Manager 的最小引用索引外壳：`dictionaryId -> Set<(dbId, tableId)>`，增删幂等。
5. 启用、改绑、禁用和 drop table 后原子更新引用；drop Dictionary 时保留仍然存在的表引用，但按旧 Dictionary ID 废弃快照并令表进入 PAUSED；同名 recreate 不自动换绑。EditLog 是提交点，Leader 内存索引为派生状态。
6. 同文表达式重新 ALTER 时仍解析当前 Dictionary ID，允许显式重新绑定新 ID。
7. `DropDictionaryInfo` 增加可选 ID 和兼容 replay。
8. 拒绝已绑定表新增 Rollup/同步物化索引。

#### 主要文件

- `CreateTableAnalyzer.java`
- `AlterTableClauseAnalyzer.java`、`AlterJobExecutor.java`、`SchemaChangeHandler.java`
- `LocalMetastore.java`、`DictionaryMgr.java`
- `DropDictionaryInfo.java`、`EditLog.java`/replay 相关类
- `OlapTable.java` 及 Rollup/MV 准入路径
- Snapshot Manager 最小骨架

#### 测试

1. CREATE/ALTER 合法矩阵及所有确定性拒绝矩阵。
2. Dictionary 暂无成功事务仍可绑定，并进入等待而非回退默认值。
3. ALTER 只改元数据，无 Schema Change Job、数据复制或同步 Compaction。
4. 改绑引用 `old--/new++` 原子切换；相同表达式绑定到新 Dictionary ID 不被错误当作 no-op。
5. drop/recreate 后旧 ID 保持 PAUSED，不按同名自动换绑。
6. replay 后 binding 和引用索引重建一致；重复 replay 幂等。
7. 启用表 ADD ROLLUP 拒绝；已有 Rollup 表启用拒绝。

#### 退出条件

静态绑定身份可靠，DDL 不会造成高耗时数据操作；运行时仍不下发 Tenant-TTL 任务。

### 第 019 轮：Dictionary 单次逻辑全量导出 RPC

建议提交：`feat(dictionary): [019] export tenant ttl policy snapshots`

#### 实现

1. 按 FE-DICT-011 增加 Protobuf 和 `PInternalService` RPC。
2. 在 `DictionaryCacheManager` 增加原子获取版本、Schema 和 Cache `shared_ptr` 的只读 handle。
3. 为已提交且不再原地修改的 Cache 增加受控枚举接口；按原 Schema 解码两列 key 和一列 value。
4. 严格校验 Tenant-TTL Schema，生成类型化 entry batch。
5. 实现 1 MiB 未压缩批次、SNAPPY/无压缩选择、每批和整体 CRC32C。
6. 执行 100,000 行、64 MiB 未压缩、64 MiB响应等请求/本地双重限制；超限不返回部分数据。
7. FE BRPC client 增加带动态 60 秒 deadline 的调用方法，不做跨 RPC 分页。

#### 主要文件

- `gensrc/proto/internal_service.proto`
- `be/src/storage/dictionary_cache_manager.*`
- `be/src/service/internal_service.*`
- `fe/.../rpc/PBackendService.java`
- `fe/.../rpc/PBackendServiceWithMetrics.java`
- `fe/.../rpc/BackendServiceClient.java`
- FE/BE 配置与对应测试

#### 测试

1. 精确事务成功、空 Cache 成功、VERSION_MISMATCH、CACHE_NOT_FOUND、SCHEMA_MISMATCH。
2. Cache 在导出期间被新版本替换，局部 shared_ptr 仍返回完整旧版本。
3. 多批次序号、SNAPPY/无压缩阈值、CRC32C、统计一致。
4. 行数/未压缩/响应上限分别触发 `LIMIT_EXCEEDED`，不含部分 batch。
5. tenant/table key 中 UTF-8 多字节、NUL 和边界字节 round-trip。
6. FE RPC timeout 动态修改只影响新请求。
7. 普通 Dictionary BEGIN/REFRESH/COMMIT/lookup 回归不变。

#### 退出条件

给定 `(dictionaryId, expectedTxnId)` 可从一台持有该成功版本的 BE/CN 一次取得完整、可验证快照；尚不发布为 FE 策略。

### 第 020 轮：不可变策略快照与 Builder

建议提交：`feat(fe): [020] build tenant ttl policy snapshots`

#### 实现

1. 实现 Snapshot、TablePolicy、Builder 和 raw-byte key。
2. 按 batch 顺序解压，验证协议、ID/事务、complete、序号、行数、长度和 CRC。
3. 全量检查 `(tenant, table_name)` 重复键，检查发生在零值过滤之前。
4. `retention_days=0` 计数后忽略；负数、NULL/缺失、非法键拒绝整个候选。
5. 建立 table default 和显式 tenant override；保留值 `default` 只能作为表默认键。
6. 估算解压、中间 Map 和冻结对象内存，默认 128 MiB 超限拒绝。
7. Map 冻结后禁止修改；计算表策略语义 fingerprint 的规范化输入。

#### 测试

1. 三层策略数据构建、全零快照、仅表默认、仅显式 override。
2. 相同值和不同值的重复键均返回 `TENANT_TTL_POLICY_DUPLICATE_KEY`。
3. 零值重复仍先判重复；单独零值被忽略且不生成失败错误。
4. 每种响应完整性损坏均拒绝整份候选，无部分发布。
5. 100,000 行和内存边界；超一行/一字节失败。
6. 输入顺序不同得到相同 TablePolicy 语义 fingerprint。
7. 并发读只能看到完整不可变对象。

#### 退出条件

Builder 能把一个完整导出响应转换为不可变候选，且所有错误都有确定性/系统性分类。

### 第 021 轮：Snapshot Manager、双路隔离与刷新追平

建议提交：`feat(dictionary): [021] manage tenant ttl policy snapshots`

#### 实现

1. 完成 Snapshot Manager 的引用、当前快照、候选、generation、blocked txn 和 retry state。
2. 第一张表引用此前未采集的 Dictionary 时，异步触发一次完整 Dictionary refresh；DDL 不等待。
3. Dictionary 普通 COMMIT 和 `DictionaryLastSuccessTxnId` 成功提交后，按需异步导出同一事务。
4. 导出/Builder 成功以 CAS 风格发布；不得倒退 SnapshotTxnId，不得发布到已删除/改绑/零引用对象。
5. 导出或 Builder 失败不修改 Dictionary 成功状态；已有旧快照继续服务。
6. 系统错误按 10 秒至 10 分钟、±20% jitter 重试；每次重新读取权威最新成功事务并最多换两个节点。
7. 确定性错误阻塞同一事务；新的 Dictionary 成功事务解除旧 blocked txn 并重新尝试。
8. 最后引用解除时停止重试、摘除快照和诊断；在途响应通过 generation fencing 丢弃。
9. 新 Leader 扫描 TableProperty 重建引用，并按现有 Dictionary 成功事务恢复快照。

#### 主要文件

- `DictionaryMgr.java`、`RefreshDictionaryCacheTaskDaemon.java`
- Snapshot Manager/Builder 模块
- `GlobalStateMgr.java` 的生命周期和 Leader 回调
- `Config.java`、metrics/logging

#### 测试

1. 未引用 Dictionary 刷新不触发导出；0→1 引用触发完整刷新。
2. 普通提交成功、策略导出失败：Dictionary 成功，Tenant-TTL 保留旧快照。
3. 导出 N 期间成功事务前进至 K：N 可安全发布后继续追 K；禁止版本倒退。
4. VERSION_MISMATCH/CACHE_NOT_FOUND 换节点和退避；确定性错误不重试同一事务。
5. 引用解除、Dictionary drop、改绑、Leader 失效时迟到响应不发布。
6. Leader 切换引用扫描、等待状态和快照恢复。
7. 并发 refresh、首次绑定和解除引用无重复任务或引用泄漏。

#### 退出条件

绑定表能够获得版本明确的 FE 策略快照，并在各种失败中保持普通 Dictionary 单向隔离和 fail-closed。

### 第 022 轮：时间分区绑定与上界证明

建议提交：`feat(fe): [022] prove tenant ttl partition bounds`

#### 实现

1. 解析并持久化 FE-TIME-001～004 支持的表达式类别和 fingerprint。
2. Range 支持直接 Unix 秒和 parser 生成的 `from_unixtime(recordTimestamp)` 内部 CAST 形态。
3. List 支持直接 Unix 秒、单表达式 `%Y%m%d`、以及恰有一个时间分量的多表达式 tuple。
4. 日期按声明式 ZoneId 的 `atStartOfDay`/`plusDays(1)` 得到 23/24/25 小时边界。
5. 为一个物理分区保留完整区间集合，并取所有区间最大上界作为保守证明值。
6. MAXVALUE、nullable MINVALUE、DEFAULT、NULL、非法日期、tuple 不匹配和精确算术溢出均返回分区级不可证明原因。
7. 不根据分区名、预期日粒度或 tenant bucket 推断时间。

#### 测试

1. Range 秒/分钟/小时/不等宽区间，MINVALUE/MAXVALUE 和 nullable 组合。
2. List 直接秒、多个值、多 tuple、时间分量不同位置。
3. `%Y%m%d` 闰日、非法日期、DST 23/25 小时和 ZoneId 别名规范化。
4. dynamic partition time zone 一致/不一致。
5. 整数 `t+1`、days×86400、cutoff 的 Math.*Exact 溢出。
6. 相同语义稳定 fingerprint；表达式/列身份变化 fingerprint 必变。

#### 退出条件

任一受支持物理分区得到可信有限上界或明确 fail-closed 原因，不存在猜测性边界。

### 第 023 轮：策略解析、补集证明与名单模式

建议提交：`feat(fe): [023] plan tenant ttl filters`

#### 实现

1. 对固定 TablePolicy 实现 override→table default→property default 的三级解析和命中类型。
2. 缺少 table key 时产生 `NOT_FOUND_USE_DEFAULT`，仅在快照本身可用时生效。
3. NULL tenant 的 TTL 为 `max(effectiveDefaultDays, all valid overrides)`。
4. 使用 `partition_upper <= evaluation_time - days*86400` 判定每个 cohort 整体过期。
5. 默认未过期时生成过期 override 的 `DELETE_LIST`；默认过期时生成尚未过期 override 的 `KEEP_LIST`。
6. 删除集合为空→`FE_NOOP`；全部有效策略过期→`DROP_LOGICAL_PARTITION`；非空完整名单→`ROWSET_REWRITE`。
7. 名单排序、去重、按 raw bytes 编码并验证行数/字节资源上限；超限不截断、不自动换极性，返回 `FAIL_CLOSED`。
8. 使用纯函数产生稳定语义指纹和计划，便于 oracle 测试。

#### 测试

1. override/table default/property default 的完整命中矩阵。
2. table key 缺失、零值被过滤后的回退和快照不可用的区别。
3. 默认 TTL 未过期/已过期、部分/全部/无 tenant 到期的四态计划矩阵。
4. nullable tenant 最大 TTL 对整分区删除的影响。
5. cutoff 恰等上界、落在分区内、早/晚一秒。
6. 名单上限恰好通过/超一项/超一字节；不得截断和换极性。
7. 随机小策略集与逐项 oracle 对比补集完整性。

#### 退出条件

对固定快照和分区上界能纯函数式地产生唯一、安全且资源完整的分区计划。

### 第 024 轮：不可变评估上下文、计划身份和指纹

建议提交：`feat(fe): [024] freeze tenant ttl evaluation inputs`

#### 实现

1. 实现 `TenantTtlEvaluationContext` 捕获/复核流程，一张表一轮，一个时间和一个 Snapshot 强引用。
2. 规范化并计算：
   - `tableBindingFingerprint`
   - `tablePolicyFingerprint`
   - `partitionBoundaryFingerprint`
   - `ReplicaTopologyFingerprint`
   - 完整 immutable request fingerprint
3. fingerprint 编码使用版本、字段 tag、显式长度、大端整数和排序后的 raw bytes，统一 SHA-256。
4. 生成 Physical Partition 计划和全部 Replica Task Spec；只展开当轮 Base Index。
5. 每个 Replica Task Spec 固定 backend/tablet、正 long signature、tenant Column Unique ID、schema、visible version、filter 和 policy watermark。
6. 任何请求字段变化都生成新 task ID；同一物理任务重试保留完整字节等价请求和 task ID。

#### 测试

1. Manager 快照在枚举中途切换，当前 context 仍只使用旧快照。
2. 并发 ALTER 使发布前复核失败，整轮候选被丢弃。
3. SnapshotTxnId 和名单必须来自同一对象，禁止拼接。
4. Map/Replica 输入顺序变化不改变 fingerprint；任一身份/版本/名单变化必改变。
5. 多 Physical Partition、Tablet、Replica 生成独立任务身份且共享正确 watermark。
6. Base Index 以外索引不能进入 task spec。

#### 退出条件

计划从创建至重试均可证明输入不可变，迟到结果具备完整 fencing 身份。

### 第 025 轮：Physical Partition 进度持久化与恢复

建议提交：`feat(fe): [025] persist tenant ttl partition progress`

#### 实现

1. 实现 FE-SCHED-003 的 Progress model 和 Manager。
2. 新增 MetaBlock ID、Image save/load、EditLog upsert/remove 和 replay。
3. 写入前在 Manager 锁内按 key 和计划 fingerprint 做 compare-and-advance；禁止水位倒退。
4. 只在整个物理分区计划成功后一次推进 cursor、processed version 和成功快照/时间。
5. 逻辑分区删除成功后批量删除其全部 Physical Partition 进度。
6. 表/库删除、truncate/partition replacement 和孤儿扫描时回收无效进度。
7. Leader 启动从进度和当前表状态派生 dirty/next-expiry，不持久化运行时优先队列。

#### 测试

1. Image/EditLog/replay round-trip、旧 Image 无 block 兼容和重复 replay 幂等。
2. 同 key 水位单调、旧 fingerprint 不得覆盖新 fingerprint。
3. 部分 Replica 成功、任务入队、发送成功均不写完成进度。
4. Catalog drop 后清理、表删除/分区替换后的 orphan GC。
5. Leader 重启后从进度发现未完成到期事件和数据版本推进。
6. 批量进度日志大小和 checkpoint 一致性。

#### 退出条件

调度完成事实可跨 FE 重启/切主恢复，且没有把运行时任务误持久化为完成状态。

### 第 026 轮：正式 Agent Task 与 BE 执行适配

建议提交：`feat(compaction): [026] add tenant ttl agent task transport`

#### 实现

1. 增加第 6.2 节 Thrift 契约，字段仅追加，不复用普通 COMPACTION。
2. FE 新增 `TenantTtlCompactionTask extends AgentTask`、`toThrift()` 和 AgentBatchTask 分支。
3. BE AgentServer 注册新 task type、请求 wrapper 和 worker。
4. worker 严格转换为当前 `TenantTtlCompactionRequest`，直接执行 `EngineTenantTtlCompactionTask`。
5. 将所有 result code、版本、digest、rowset 结果和统计一对一转回 Thrift；不能用通用 TStatus 丢失业务终态。
6. `LeaderImpl.finishTask()` 增加独立处理分支；未知/缺失/畸形结果不记成功。
7. 首期不增加取消消息，也不修改手工 HTTP 的构建隔离。

`TFinishTaskRequest.task_status` 只描述 Agent worker/传输层是否形成了合法业务结果；只要内部执行器返回完整 `TTenantTtlCompactionResult`，FE 以其中的 `code` 做业务分类，不能把所有非 SUCCESS 结果折叠为一个通用 `RUNTIME_ERROR`。

#### 测试

1. FE request 所有字段和 raw tenant bytes 的 Thrift round-trip。
2. C++ Thrift↔内部 request/result 每字段映射测试。
3. 所有 TenantTtlTaskCode 的 TStatus/业务 result 保真。
4. AgentServer 接收、执行、finishTask、remove signature 的闭环。
5. 缺少必填字段、task/result 身份不匹配、KEEP empty 拒绝。
6. 现有 `EngineTenantTtlCompactionTask` 测试全量回归，证明核心语义未变。
7. 普通 COMPACTION/COMPACTION_CONTROL Agent Task 回归。

#### 退出条件

可以通过正式 Agent 通道执行一个指定 Replica，并把 BE 完整结果可靠返回 FE；尚不自动调度业务表。

### 第 027 轮：Leader 调度、到期事件与 Catalog 删除

建议提交：`feat(fe): [027] schedule tenant ttl partition plans`

#### 实现

1. 新增 Leader-only Scheduler，并在 GlobalStateMgr 生命周期中启动/停用；不挤入普通 `PartitionTTLScheduler` 的条件求值路径。
2. 从引用表、快照和 Progress 派生优先队列；支持 `EXPIRY_EVENT_DUE`、`INITIAL_CATCH_UP`、`POLICY_CHANGED`、`DATA_VERSION_ADVANCED`、`RETRY_PENDING`。
3. `expireAt=partitionUpper+days*86400` 全程 Math.*Exact；同轮多个到期 cohort 合并为一个当前完整计划。
4. 实现 `FE_NOOP`/`FAIL_CLOSED` 状态记录，不占用破坏性全局槽。`FE_NOOP` 可记录已评估的策略 fingerprint、覆盖到的到期 cursor 和下一个 `expireAt`，但必须保持 `processedThroughVersion` 不变，不能伪造 BE 已处理的数据版本；`FAIL_CLOSED` 不推进完成水位。
5. 实现逻辑分区聚合：全部 Physical Partition 均为 DROP 才允许 `DROP_LOGICAL_PARTITION`。
6. Catalog drop 前持表写锁复核 IDs/name、Physical Partition 集、三类 fingerprint 和 NORMAL 状态。
7. 复用 `DropPartitionClause(false, name, false, true)` 与 `LocalMetastore.dropPartition()`。
8. 同逻辑分区有旧 rewrite 在途时只跳过本轮；Catalog 删除失败不降级为 rewrite。

#### 测试

1. 到期当天触发、次日无新事件不重复、错过阈值后追平、多个 cohort 合并。
2. Snapshot txn 变化但策略 fingerprint 不变不重复；策略变化立即 dirty。
3. 可见版本推进触发，普通 compaction 不推进版本时不触发。
4. FE_NOOP、FAIL_CLOSED 不产生 BE/Catalog 操作。
5. 多 Physical Partition 逻辑分区只允许全体 DROP；临时分区跳过。
6. drop 锁内任一 fingerprint/状态变化均放弃。
7. 旧 rewrite 在途跳过一次，下轮可重新判断。
8. Catalog 成功进度清理和 EditLog replay；失败保留分区及重试状态。

#### 退出条件

Leader 可以安全识别需要处理的分区并完成 NOOP/Catalog 分支，Rewrite 只生成待执行计划。

### 第 028 轮：全部 Replica Rewrite 编排、重试和故障收敛

建议提交：`feat(fe): [028] orchestrate tenant ttl replica rewrites`

#### 实现

1. Coordinator 持有唯一全局破坏性执行槽，按 `(tabletId, backendId)` 稳定顺序逐个发送。
2. 每个 Base Index Tablet 的所有 Catalog Replica 均为必需；不可用 Replica 进入 `WAITING_REPLICA` 并释放全局槽。
3. 固定拓扑 fingerprint；Replica 增删、backend 迁移或 Base Index/schema 变化立即使计划失效并重新规划。
4. 完成处理严格校验 AgentTaskQueue、task ID、目标、watermark、binding/policy/boundary/topology fingerprint 和 processed version。
5. 所有 Replica 成功后，分区 processed version 取 BE `processed_through_version` 的最小值，再次复核全部 fingerprint 后原子推进 Progress。
6. 错误分类按 FE-SCHED-007：
   - 原 task 重试：网络/未知、TABLET_BUSY、TTL_ALREADY_RUNNING、REPLICA_NOT_CAUGHT_UP、STALE_ROWSET、计划仍有效的异常 CANCELLED；
   - 重读重规划：SCHEMA_CHANGED、TABLET_NOT_FOUND、拓扑变化；
   - BLOCKED：INVALID_ARGUMENT、NOT_SUPPORTED、DATA_INVARIANT_VIOLATION、INTERNAL_ERROR。
7. soft timeout 默认 3600 秒；响应未知不假定任务停止，不生成不同请求并发抢占。
8. Leader 切换不恢复旧在途集合；未知旧 finish report 只确认和诊断。
9. 禁用/改绑只停止生成后续旧计划任务，已发送任务自然完成且结果被 fencing；首期无 cancel。

#### 测试

1. 多 Tablet×多 Replica 全部成功才推进；任一缺失/失败不推进。
2. Replica 不可用不长期占槽，其他分区可继续；恢复后追平。
3. 每种 BE code 的分类、退避、task ID 保持或重新生成符合契约。
4. 网络超时后相同请求/task ID 收敛为 SUCCESS 或 NOOP_VERIFIED。
5. soft timeout、迟到结果、禁用/改绑/策略变化后的 fencing。
6. 拓扑增删、schema change、partition replace 中旧结果不能推进新进度。
7. Leader 切换、旧 Agent Task 自然完成、新 Leader 新 task 收敛。
8. 全局同时最多一个破坏性单元；候选计算和等待不占槽。

#### 退出条件

自动 Rowset Rewrite 在失败、重试、切主和拓扑变化下最终安全收敛，完成水位只代表全部必需 Replica 已处理。

### 第 029 轮：SHOW 状态、策略解析与诊断

建议提交：`feat(sql): [029] expose tenant ttl runtime status`

#### 实现

1. 按最终确认语法增加表级状态和 tenant 解析 AST、parser、analyzer、executor。
2. 表级状态至少覆盖静态属性、持久化 Dictionary ID、DictionaryLastSuccessTxnId、SnapshotTxnId/SnapshotTime、TableKeyMatch、状态、最近错误、列/时间绑定、可证明分区和调度摘要。
3. tenant 解析返回 `TENANT_OVERRIDE`、`TABLE_DEFAULT`、`PROPERTY_DEFAULT` 或无法解析状态；表键缺失显示 `NOT_FOUND_USE_DEFAULT`。
4. Snapshot 字段只来自当前成功 FE 快照；重启重建期间不伪造 0 为成功事务。
5. 最近导出/Builder/计划/Agent 失败统一进入受控 ErrorMessage，禁止输出完整真实 tenant 名单。
6. 落实最终权限检查和敏感信息裁剪。

#### 测试

1. parser/analyzer/权限/不存在对象/非 Tenant-TTL 表矩阵。
2. DISABLED、WAITING_DICTIONARY、WAITING_POLICY_SNAPSHOT、ACTIVE、PAUSED、INVALID。
3. 双水位领先/相等、旧快照继续服务、重启重建过程。
4. 三层命中和 `NOT_FOUND_USE_DEFAULT`。
5. zero rows 忽略计数、分区不可证明原因和最近错误展示，以最终契约为准。
6. 结果列类型、顺序、NULL 语义和 SHOW CREATE 一致性。

#### 退出条件

用户能判断“绑定是否生效、当前用了哪个策略版本、为何未调度以及某 tenant 命中哪一级”。

### 第 030 轮：端到端故障测试与首期收口

建议提交：`test(tenant-ttl): [030] harden tenant ttl FE orchestration`

#### 实现与测试

1. 建立 FE mock/fake Dictionary export、Agent completion 和可控 clock，测试不依赖 sleep。
2. SQL 建表→绑定→Dictionary 刷新→快照发布→时间评估→Replica rewrite→进度推进闭环。
3. Range/List、多逻辑/物理分区、多 Tablet/Replica、nullable tenant、DELETE/KEEP/NOOP/DROP 全矩阵。
4. Dictionary 导出 CRC/超限/版本竞争、旧快照、drop/recreate/rebind。
5. FE restart、Leader switch、BE restart/cache missing、Replica unavailable/replacement、response loss。
6. 策略改变、迟到写入、可见版本推进和到期事件追平。
7. 禁用/改绑时在途任务自然完成、迟到结果 fencing。
8. 回归普通 Dictionary、普通 partition TTL、普通 compaction、表属性 replay、Rollup DDL。
9. 在保留构建缓存的既有容器/volume 中完成增量 FE/BE 编译和相关 UT；按需要执行 SQL 集成测试。
10. 汇总 metrics/logging，检查无 tenant/task/tablet 高基数 label 和无原始名单泄漏。

#### 退出条件

1. 所有确认契约均有对应自动化测试或明确的手工验收步骤。
2. FE/BE 增量构建成功，相关 UT 和端到端用例通过。
3. 没有改变 BE Tenant-TTL 核心过滤、coverage、锁和原子提交语义。
4. 尚未确认或后续轮次能力未被暗中启用。

## 10. 测试分层与执行要求

### 10.1 纯单元测试

- 属性 parser、时区规范化、fingerprint codec。
- Snapshot Builder、policy resolver、filter planner、expire/cutoff 算术。
- Progress compare-and-advance、retry classification。

纯函数测试应覆盖边界和随机 oracle，避免依赖真实时间和线程 sleep。

### 10.2 FE 元数据与 replay 测试

- CREATE/ALTER/drop/recreate/rebind。
- TableProperty、EditLog、Image、checkpoint/replay。
- Rollup guard、表/分区删除和 orphan progress 回收。
- Leader 重建引用、快照状态和调度候选。

### 10.3 协议与 BE 适配测试

- Protobuf Dictionary export 完整性和资源边界。
- Thrift request/result 字段保真。
- AgentServer worker 直接调用当前 Engine task。
- 普通 Dictionary/Compaction 回归。

### 10.4 调度确定性测试

注入以下可控依赖：

- `Clock`
- Dictionary Snapshot provider
- Catalog snapshot/topology provider
- Agent task submitter/completion source
- retry scheduler/jitter source

断言全局槽、顺序、task ID、重试 request byte equality、fingerprint fencing 和 Progress 推进，而不是通过等待真实 daemon 周期观察结果。

### 10.5 集成与故障测试

至少覆盖：

1. 正常 DELETE_LIST/KEEP_LIST。
2. Catalog 整分区删除。
3. 部分 Replica 成功、失败、不可用和替换。
4. FE/BE 重启与 Leader 切换。
5. Dictionary 新旧版本竞争、导出失败和恢复。
6. 策略变更与迟到写入。
7. 升级/回滚和旧元数据读取。

### 10.6 构建资产要求

按仓库 `AGENTS.md` 复用 `starrocks-tenant-ttl-4.0-build` 和 Docker volume `sr-tenant-ttl-4.0-build-cache-arm64`；只同步变更文件并增量构建。不得清理现有 CMake、对象文件、输出和测试日志。每轮先确认构建目录的实际 CMakeCache/flags，再选择 Release/Debug/ASAN/UBSAN 目录。

## 11. 社区 Dictionary 修复的隔离与触发条件

`DICTIONARY功能与社区差异.md` 记录的社区修复不作为本次 Tenant-TTL 编码前置，也不混入 [017]～[030] 轮次：

1. 本次 Tenant-TTL 策略表固定为两个 NOT NULL Primary Key（`tenant`、`table_name`）和一个 value（`retention_days`），FE 策略快照通过专用导出 RPC 从已提交 Dictionary Cache 枚举，不走 `partition_retention_condition` 的 `DictionaryGetExpr.toSqlImpl()` 序列化路径。
2. 因此首期不为 Tenant-TTL 主动回移 `keySize + null_if_not_exist` 修复，也不扩大本轮回归范围去覆盖普通多 KEY `dictionary_get()` 表达式转 SQL 再解析。实际实现和回归未遇到该问题。
3. 只有后续 Tenant-TTL 实际路径复现丢 KEY、重复布尔参数或重解析不一致时，才启动独立社区修复；届时的最小回归矩阵覆盖两个及以上 KEY，以及 `null_if_not_exist` 省略、显式 `false`、显式 `true` 三种形式。
4. 若该问题被复现，优先核对社区 4.0 backport `#78794`（来源 `#78706`）与当前分支上下文；能直接 cherry-pick 时使用原提交，存在冲突时只做等价最小移植。
5. 社区修复必须单独编译、测试和提交，不与任何 Tenant-TTL commit 合并。每个独立社区问题单独使用：

   ```text
   <type>(<scope>): [COMMUNITY-FIX] <summary>
   ```

6. nullable key、子表达式错误传播等其他 P0/P1 项同样不能与 [017]～[030] 合并；必须先由实际路径证明需要回移。
7. WAL/Apply 大重构和 BE Dictionary 模块搬迁不作为本功能前置。

## 12. 后续轮次优化，不进入首期 [017]～[030]

### 12.1 强制后续项：DELETE_LIST 无损分片

首期稳定后必须专项对齐并实现：

- 单片行数/字节上限。
- 分片 ID 和全局计划 ID。
- 稳定顺序。
- 部分成功恢复。
- 完成水位。
- Leader 切换恢复。

多片执行的集合语义必须等于 DELETE_LIST 并集。`KEEP_LIST` 超限继续 fail-closed。

### 12.2 可选后续项

1. 根据实测增加全局/每 BE/每表并发令牌和公平调度。
2. Agent Task 协作式取消。
3. Dictionary 无 Session 分页导出；每页固定首页事务，版本变化即失败并从头重试。
4. 百万级策略时评估有序物化或短期 Session，避免 offset 分页重复遍历。
5. Tenant-TTL 调度历史、分区进度和最近任务的独立历史查询接口。

## 13. 编码前门槛的最终结论

以下结论已在编码开始前与用户确认，实施不得自行扩大范围：

1. 本轮不处理多 KEY Dictionary 的 `keySize + null_if_not_exist` 社区差异；只有 Tenant-TTL 实际路径遇到该问题时，才另立 `[COMMUNITY-FIX]` 提交。本次实现未遇到该问题。
2. 首期不提供清空或禁用 `compaction_retention_condition` 的 DDL 语法；改绑时已发送任务自然结束，迟到结果由 binding/request fingerprint fencing 隔离。
3. 首期不新增 BE/CN 重启后的 Dictionary Cache 主动补齐行为；沿用普通 Dictionary 的既有恢复刷新，FE 无成功策略快照时保持 fail-closed 等待。
4. 首期不考虑 FE/BE 混合版本部署，不增加 capability/version gate；启用 Tenant-TTL 前要求部署侧确保 FE/BE 同版本。
5. SHOW 最终采用 `SHOW TENANT TTL STATUS FROM db.table [FOR TENANT '...']`，要求目标表 `SELECT` 权限，输出类型、双水位、zero-row、分区可证明性和错误字段按第 029 轮实现。
6. 首期不提供持久化调度历史或最近任务明细查询，只提供表级有界实时摘要；独立历史接口保留为后续优化。

## 14. 首期验收标准

### 14.1 正确性

1. 任何删除均由固定 SnapshotTxnId、固定 evaluation time 和可信分区上界共同证明。
2. 名单补集完整，NULL tenant 和默认 TTL 语义符合 FE-PLAN-001～003。
3. 无快照、边界不可证明、名单超限、身份变化和未知结果一律 fail-closed。
4. 全部 Catalog Replica 成功前不推进分区完成水位。
5. Catalog 删除前完成锁内身份复核；Rewrite 不与同逻辑分区 Catalog 删除并发。

### 14.2 可恢复性

1. Dictionary 导出失败不破坏普通 Dictionary，旧成功 FE 快照可继续使用。
2. FE 重启/Leader 切换可从 binding 和 progress 重建，不依赖旧内存任务。
3. 网络响应丢失使用原 task ID 和原请求收敛。
4. 策略变化、迟到写入和拓扑变化最终触发重新评估。

### 14.3 兼容性

1. 未配置 Tenant-TTL 的表行为不变。
2. 未被引用的 Dictionary 刷新路径不增加导出开销。
3. 现有 `partition_retention_condition`、普通 partition TTL 和普通 Compaction 行为不变。
4. 旧 Image/EditLog 可读取；新可选字段不改变旧对象默认语义。
5. BE 核心 Tenant-TTL 执行器只增加正式传输适配，不改变其已完成边界。

### 14.4 可运维性

1. 可以判断绑定是否有效、Dictionary 和 FE Snapshot 双水位、table key 是否命中、某 tenant 最终采用哪一级 TTL。
2. 等待、暂停、fail-closed、blocked 和 retry 能区分，最近错误可诊断但不泄漏 tenant 名单。
3. 配置上限、超时和重试参数有默认值、边界校验和动态修改测试。

## 15. 对齐后执行方式

本计划获得确认后，按以下节奏开始编码：

1. 每次只启动一轮，开始前再次列出该轮契约、文件和测试。
2. 完成代码后先做 diff/review 和目标测试，再形成独立 commit。
3. 向用户报告实际测试、未测项、风险和 commit，获得继续指令后进入下一轮。
4. 遇到澄清文档未覆盖且会改变外部行为的选择时暂停，不在代码中先行定案。

## 16. 已确认契约到编码轮次的追踪矩阵

| 澄清条款 | 主要实现轮次 | 验证重点 |
| --- | --- | --- |
| FE-TABLE-001～003 | 018、023、030 | 不物化 TTL、不依赖 bucket、default/NULL 语义 |
| FE-TABLE-004 | 018、024、027、028 | 绑定和后续 Rollup guard、只展开 Base Index |
| FE-TIME-001 | 017、018、022、026 | tenant/recordTimestamp 身份固定及 Unique ID 下发 |
| FE-TIME-002～005 | 022、023、027、030 | Range/List、时区、有限上界和按分区 fail-closed |
| FE-DICT-001～002 | 017、018 | 三列 Dictionary 和 Primary Key 源表准入 |
| FE-DICT-003～007 | 021、029、030 | 旧快照、双路隔离、追平和退避 |
| FE-DICT-008～009 | 018、020、021 | 对象模型、按需启用和首次完整刷新 |
| FE-DICT-010～011 | 019～021 | 精确版本单 RPC 导出、压缩、完整性和资源边界 |
| FE-DICT-012～013 | 017、018、021、025 | Leader/Follower、绑定 ID、引用重建和 drop/recreate |
| FE-DICT-014 | 020、021、022、029 | 零值、重复、溢出、确定性/系统错误 |
| FE-PROP-001～003 | 017、020、023、029 | 三参数、逻辑 table key、三级回退和 fail-closed |
| FE-DDL-001～003 | 017～018、021 | 轻量 DDL、静态拒绝和运行时等待 |
| FE-OBS-001～004 | 021、025、027～029 | 静态/运行时状态、tenant 解析和双水位 |
| FE-EVAL-001～004 | 024、026、028 | 固定时间/快照、watermark、Replica task identity |
| FE-PLAN-001～003 | 023、024、030 | NULL 最大 TTL、名单极性、空/超限语义 |
| FE-SCHED-001～003 | 025、027、028 | 到期事件、补偿触发、持久化进度和 Leader 队列 |
| FE-SCHED-004 | 027～028 | 四态分流、Catalog/Rewrite 互斥和旧任务在途跳过 |
| FE-SCHED-005 | 026、028 | 专用 Agent Task、完整结果、无取消和 fencing |
| FE-SCHED-006～007 | 025、027、028、030 | 全 Replica、全局串行、错误收敛和切主 |

该矩阵用于每轮 review 时检查需求覆盖；若实现 diff 触及不属于该轮的条款，优先拆分提交，而不是扩大当轮范围。

## 17. 实施结果与验证记录

### 17.1 独立实现轮次

| 轮次 | Commit | 实际交付 |
| --- | --- | --- |
| 017 | `c3aad1f5c` | 属性 parser、结构化 Dictionary/Table 绑定及元数据持久化 |
| 018 | `5ba7ca330` | CREATE/ALTER 准入、Dictionary 三列契约、分区/列/时区和 Rollup 校验 |
| 019 | `142d6a536` | 单次逻辑 Dictionary 全量导出 Protobuf、BE 精确版本导出和 FE RPC |
| 020 | `c5afbbb73` | 不可变策略快照、完整性/资源/行级校验和 raw-byte key |
| 021 | `63afc225c` | 按引用启用、普通 Dictionary 单向故障隔离、重试和 Leader 恢复 |
| 022 | `9f07e685b` | Range/List 时间边界证明、声明式时区和按物理分区 fail-closed |
| 023 | `91645414c` | 三级 TTL 解析、DELETE/KEEP/NOOP/DROP 规划和名单上限 |
| 024 | `10f31d798` | 固定快照/评估时间、表/策略/边界/拓扑 fingerprint 和 Replica 任务身份 |
| 025 | `ce719d765` | Physical Partition 完成进度、Image/EditLog 和 compare-and-advance |
| 026 | `1270ee814` | 专用 Thrift Agent Task、FE/BE 适配与完整结果校验 |
| 027 | `83b1b970f` | 到期事件和补偿触发、四态分流、Catalog 删除与全局串行调度 |
| 028 | `96516ebf6` | 全 Replica 编排、重试/未知结果、拓扑校验、迟到结果和切主收敛 |
| 029 | `f72fe14b5` | 表级/单 tenant SHOW、双水位、状态、分区和调度诊断 |
| 030 | 本提交 | 可控评估时钟、跨 Builder/Planner/Scheduler/Coordinator 闭环测试和最终回归 |

### 17.2 最终代码结构

计划中的职责最终落在以下实现中：

1. Catalog/DDL：`TenantTtlPropertyParser`、`TenantTtlBindingAnalyzer`、`TenantTtlDictionaryBinding`、`TenantTtlTableBinding` 和 `TableProperty`。
2. Dictionary 策略：`TenantTtlPolicySnapshotManager`、`TenantTtlPolicySnapshotBuilder`、`TenantTtlPolicySnapshot`、`TablePolicy`，普通 Dictionary 成功提交后按引用异步导出。
3. 边界与规划：`TenantTtlPartitionBoundResolver`、`TenantTtlPolicyPlanner`、`TenantTtlEvaluationContext` 和 `TenantTtlScheduleDecision`。
4. 进度与调度：`TenantTtlPartitionProgressManager`、`TenantTtlScheduler` 和 `TenantTtlRewriteCoordinator`。
5. 传输与执行：`TenantTtlCompactionTask`、`TENANT_TTL_COMPACTION` Thrift 协议和 BE Agent worker；BE 继续调用既有 `EngineTenantTtlCompactionTask`。
6. 可观测性：`ShowTenantTtlStatusStmt` 和 `TenantTtlStatusService`。

### 17.3 自动化覆盖闭环

1. 属性、DDL 和 replay 测试覆盖合法/非法三参数、Primary Key 策略源表、Range/List、时区、列 Unique ID、Rollup guard、drop/recreate/rebind 和旧元数据读取。
2. 导出与 Builder 测试覆盖精确版本、单 RPC 多 batch、压缩、CRC、完整性、100000 行边界、内存/响应上限、重复键、零值忽略、确定性错误和可重试错误。
3. 纯规划测试覆盖 tenant 精确字节匹配、三级默认、NULL tenant 最长 TTL、DELETE/KEEP/NOOP/DROP、名单超限、算术溢出、Range/List/DST/MAXVALUE 和随机 oracle。
4. 持久化与调度测试覆盖固定 `evaluation_time`/`SnapshotTxnId`、多 Physical Partition/Tablet/Replica、Catalog 删除互斥、FE NOOP、首次/策略/数据补偿、进度 replay 和 orphan 清理。
5. Agent/Coordinator 测试覆盖完整 Thrift 字段、全业务错误码、全局单任务、全部 Replica 成功后推进、确定性/未知重试、响应丢失、Replica 不可用/替换、在途改绑 fencing 和 Leader 本地状态重建。
6. SHOW 测试覆盖权限、列类型/顺序/NULL、六种绑定状态、双水位、旧快照、三级 tenant resolve、zero-row 和不可证明分区诊断。
7. 第 030 轮闭环测试从编码后的 Dictionary 导出响应开始，经 Builder 发布不可变快照，依次验证 Catalog Drop、FE NOOP、两个 Base Index Tablet/Replica 的 DELETE_LIST 物理任务以及最终完成水位，全程使用可控时钟且不依赖 sleep 或真实 BE。

### 17.4 最终构建与测试

1. FE 在 `starrocks-tenant-ttl-4.0-build` 中执行 `mvn -T 1C -pl fe-core -am ... -Dcheckstyle.skip test`，覆盖 15 个 Tenant-TTL/权限相关测试选择项，实际运行 78 个用例，0 failure、0 error、0 skipped。
2. FE 独立执行 `mvn -pl fe-core -DskipTests checkstyle:check`，结果为 0 个 Checkstyle 违规。
3. BE 继续复用 `be/ut_build_Debug`；实际 CMakeCache 为 `CMAKE_BUILD_TYPE=Debug`、`MAKE_TEST=ON`、`USE_AVX2=ON`、`WITH_STARCACHE=ON`、`WITH_TENANN=OFF`。以 `-j4` 增量构建 `dictionary_cache_manager_test`、`tenant_ttl_compaction_types_test`、`tenant_ttl_row_filter_test`、`tenant_ttl_tablet_primitives_test`、`tenant_ttl_compaction_fixture_test` 和 `engine_tenant_ttl_compaction_task_test`，六个目标全部成功。
4. BE Debug 运行上述六个二进制的全部用例，并运行 `starrocks_test` 中 `TenantTtlAgentTaskAdapterTest.*` 和 `AgentTaskTest.tenant_ttl_workerReturnsBusinessResultAndRemovesSignature`；合计 58 个用例全部通过。其中 Dictionary Cache 5、types 5、row filter 10、tablet primitives 10、fixture 2、engine task 22、Agent 4。
5. 一次额外的 `be/build_Release` 构建在发现其实际为 `Release + MAKE_TEST=ON`、不是本轮一直使用的 Debug UT 目录后主动中止；已产生的独立缓存保留，未改变 `be/ut_build_Debug`，该次中止构建不计入验收结果。
6. `git diff --check` 和最终工作区/提交隔离检查在第 030 轮提交前执行；`zc-docs/path/` 为用户原有未跟踪内容，不纳入任何 Tenant-TTL 提交。

### 17.5 明确未纳入首期的验证

1. 不执行混合 FE/BE 版本矩阵；首期部署边界已明确为同版本。
2. 不验证新增/重启 BE/CN 的 Dictionary Cache 主动补齐，因为首期未实现该行为。
3. 不验证禁用属性 DDL，因为首期没有该语法。
4. 不回移或验证多 KEY `dictionary_get(..., null_if_not_exist)` 社区修复；本功能三列 Primary Key 策略路径未遇到该问题。
5. 不验证后续轮次的 DELETE_LIST 分片、KEEP_LIST 超限执行、并发调度、取消协议、Dictionary 分页导出或调度历史接口。
