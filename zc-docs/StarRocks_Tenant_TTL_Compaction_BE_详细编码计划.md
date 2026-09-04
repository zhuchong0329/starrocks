# StarRocks Tenant-TTL Compaction BE 详细编码计划

> 状态：设计口径已确认，待进入第四步编码和测试  
> 源码基线：StarRocks main，commit `10adb6a028de5218ef3be355d7fc7e3b82f9ab9d`  
> 编制日期：2026-09-04  
> 实施范围：首期 shared-nothing BE Tenant-TTL Compaction  
> 实施约束：本次只完成设计文档更新，第四步编码和测试从后续指令开始

## 1. 编制依据与结论优先级

本计划依据以下两份文档编制：

1. `zc-docs/compaction ttl简要方案.md`：提供业务背景、分区三态、目标表结构和长期 Segment 优化方向。
2. `zc-docs/StarRocks_Tenant_TTL_Compaction_研发澄清与实施记录.md`：提供首期已经确认的执行语义、并发边界、原子提交、幂等性、测试和手工入口要求。

两份文档存在表达差异时，以澄清文档中的“已确认结论”为准。特别是首期 BE 不按简要方案早期描述重新读取 `recordTimestamp` 或策略字典，而是执行 FE 已经物化完成的 tenant-only 删除语义。

## 2. 首期交付目标

首期交付一个可被未来 FE Agent Task 和当前专用 HTTP 手工入口共同调用的、真实修改本地 Tablet Rowset 的 BE Tenant-TTL Compaction 执行链。

必须完成的能力如下：

1. 仅支持 shared-nothing、本地、非 PK、`DUP_KEYS` Tablet。
2. 请求消费互斥的 `DELETE_LIST` 或 `KEEP_LIST`，只精确扫描业务 `tenant` 列。
3. NULL tenant 在两种模式下始终保守保留。
4. 不读取 `recordTimestamp`，不访问 DictionaryCache，不校验或依赖 `tenant_id` 生成列及排序键。
5. 固定 BE 获得执行互斥后的完整连续 coverage，支持 coverage 中任意数量的当前可见 Rowset。
6. 对每个源 Segment 精确生成 `keep_row_ranges`，收敛为 KEEP、DROP 或 REWRITE。
7. KEEP Segment 在同一 Tablet 目录内 hard link 复用；DROP Segment 不进入输出；REWRITE Segment 纵向重写所有列组。
8. 一个源 Segment 最多产生一个目标 Segment，目标 Segment ordinal 连续，源行和源 Segment 顺序不变。
9. 为每个发生变化的源 Rowset生成一个同 version 的输出 Rowset；全量 DROP 的源 Rowset生成同 version 空 Rowset。
10. 所有输出 staged/load/verify 成功后，在一个 header lock 临界区内校验完整 coverage 的 `(version,rowset_id)`，一次批量替换并只保存一次 TabletMeta。
11. 全 coverage 待删除行数为零时返回 `NOOP_VERIFIED`，不创建 Writer、新 Rowset、不修改 TabletMeta、不进入 stale/unused。
12. 同 Tablet 的 Tenant-TTL 串行，并通过独占 base/cumulative locks 与普通 Compaction 执行互斥；抢锁使用 try-lock，首期不实现防饥饿。
13. 提供受专用编译宏保护的同步 HTTP 手工入口；默认和发布构建中不包含 Handler 或路由。
14. 完成澄清文档中确认的第一批最小测试集，并为第二批增强测试预留 SyncPoint/failpoint 和观测点。

## 3. 明确不在首期范围内

以下内容不进入本轮 BE 实现：

- FE 分区三态计算、字典加载、任务调度、超时重试和完成水位持久化。
- shared-data/Lake Tenant-TTL Compaction。
- Primary Key、Unique Key 或 Aggregate Key Tablet。
- 对 `recordTimestamp` 做分区内部分时间删除。
- `tenant_id`、Short Key、排序键或定制 ZoneMap 的候选范围选择优化。
- 动态扩展 coverage 追赶任务执行期间持续到来的新写入。
- 多个 TTL 任务对同一 Tablet 并行、局部成功提交或动态重规划。
- try-lock 公平队列、`TTL_PENDING` 跨请求 lease、优先级提升或防饥饿。
- 持久化 `AppliedProof`、tenant 已处理集合、谓词历史或逐 Segment `SegmentStatsPB`。
- 修改普通 Horizontal/Vertical Compaction 的提交语义。
- 修改现有 `Tablet::modify_rowsets_without_lock()` 的签名或内部行为。

## 4. 首期正确性不变式

编码和评审必须围绕以下不变式展开：

### 4.1 数据不变式

对固定 coverage 中任意源行 `r`：

```text
DELETE_LIST:
    r.tenant IS NULL              -> KEEP
    r.tenant IN request.tenants   -> DROP
    otherwise                     -> KEEP

KEEP_LIST:
    r.tenant IS NULL              -> KEEP
    r.tenant IN request.tenants   -> KEEP
    otherwise                     -> DROP
```

输出只删除行，不改变保留行的列值、行序或 Segment 间先后关系。

### 4.2 coverage 不变式

```text
snapshot_end_version = BE 在取得 Tablet TTL admission 和两把独占
                       Compaction lock 后观察到的最大连续可读版本上界

coverage = 当前 version map 中完整覆盖 [0, snapshot_end_version]
           的全部 (version, rowset_id)
```

快照时已经在连续可读路径中的 Rowset 不得因数量、`is_compacting` 或资源原因被静默跳过。快照固定后 publish 且 version 高于 `snapshot_end_version` 的 Rowset 不属于本次 coverage。

### 4.3 Rowset/Segment 不变式

- coverage 中每个 Rowset 都必须得到 `VERIFIED_NO_CHANGE`、DROP 或 REWRITE 结论。
- Rowset 内每个 Segment 都必须得到 KEEP、DROP 或 REWRITE 结论。
- KEEP 不生成新数据，只链接该完整 Segment 的全部 artifact。
- DROP 不生成目标 Segment。
- REWRITE 对所有列组重放同一份 `SparseRange<rowid_t>`。
- 每个变化 Rowset 的输出 version 必须与源 version 完全一致。
- 源 Rowset 全量删除时输出 `num_rows=0`、`num_segments=0` 的同 version 空 Rowset。

### 4.4 提交不变式

- 第一次 Tablet 元数据修改前，完整 coverage 的所有身份校验必须已经通过。
- KEEP/`VERIFIED_NO_CHANGE` Rowset 也参加提交前 Rowset ID 校验。
- 所有变化 Rowset 一次传给现有 `modify_rowsets_without_lock()`。
- TabletMeta 只保存一次。
- 任一 Rowset 在扫描、构建、load、verify 或提交校验中失败，TabletMeta 必须零修改。

### 4.5 幂等不变式

- FE 保证同一 task ID 的所有重试携带完全相同的请求内容；BE 首期不为请求内容另外计算 digest。
- 不同 task ID 重复相同谓词允许重新扫描，但不得进行无效 Rowset 重写。
- 全 coverage 零命中返回 `NOOP_VERIFIED`；该结果只证明 `[0,snapshot_end_version]`，不覆盖之后的新版本。
- BE 不为谓词幂等持久化 tenant 级证明。

## 5. 总体架构与调用链

建议的首期调用链如下：

```text
TenantTtlCompactionAction（仅特殊宏构建）
    │ 解析 JSON、规范化名单、校验请求字段
    ▼
EngineTenantTtlCompactionTask
    │ 获取 Tablet、资格校验、内存跟踪
    │ Tablet TTL admission: IDLE -> PENDING
    │ try-lock base -> try-lock cumulative
    │ Tablet TTL state: PENDING -> RUNNING
    ▼
Tablet::capture_tenant_ttl_coverage()
    │ header read lock 内固定完整 coverage
    │ 持有当前 TabletSchemaCSPtr 并记录全部 (version,rowset_id)
    │ 标记 coverage Rowset is_compacting=true
    ▼
TenantTtlRowFilter
    │ 每个 Segment 只扫描业务 tenant 列
    │ 生成 keep_row_ranges 和待删除行数
    ▼
SegmentFilterPlan: KEEP / DROP / REWRITE
    ▼
FilteredRowsetWriter（每个变化源 Rowset 一个实例）
    ├─ KEEP    -> hard link 完整 Segment artifact
    ├─ DROP    -> 不产生目标 Segment
    └─ REWRITE -> VerticalSegmentRewriter
                   对所有列组重放同一 keep_row_ranges
    ▼
输出 Rowset build + load + verify
    ▼
Tablet::commit_tenant_ttl_rowsets()
    │ 一个 header write lock
    │ 校验完整 coverage 的 version + rowset_id + captured schema
    │ 调用原样保留的 modify_rowsets_without_lock(all_outputs, changed_sources)
    │ save_meta() 一次
    ▼
释放源引用、进入 stale/unused；返回完整 TenantTtlCompactionResult
```

`EngineTenantTtlCompactionTask` 不继承现有 `CompactionTask`。现有类围绕“多个输入合并为一个输出 version”和单把共享 Compaction lock 设计，无法自然表达“多个源 Rowset分别生成多个同 version 输出、同时持有两把独占锁、完整 coverage 原子提交”。首期让它直接继承 `EngineTask`，复用底层 Reader、Writer、Tablet 和 GC 能力，但使用独立生命周期和结果协议。

## 6. 核心数据结构和接口草案

以下名称是编码计划中的推荐名称。实现时可以按项目风格微调，但职责和字段语义不能改变。

### 6.1 请求模型

建议新增 `be/src/storage/tenant_ttl_compaction_types.h`：

```cpp
enum class TenantFilterMode {
    DELETE_LIST,
    KEEP_LIST,
};

struct TenantFilter {
    TenantFilterMode mode;
    std::vector<std::string> tenants; // BE 内排序、去重后的规范形式
};

struct TenantTtlPolicyWatermark {
    int64_t dictionary_id;
    int64_t dictionary_txn_id;
    int64_t evaluation_time_epoch_seconds;
};

struct TenantTtlSchemaExpectation {
    int64_t schema_id;
    int32_t schema_version;
};

struct TenantTtlCompactionRequest {
    uint32_t protocol_version;
    int64_t task_id; // 正数，与 Agent Task signature 类型一致
    int64_t tablet_id;
    int64_t partition_id;
    int32_t tenant_column_unique_id;
    TenantFilter filter;
    TenantTtlPolicyWatermark policy_watermark;
    TenantTtlSchemaExpectation expected_schema;
    std::optional<int64_t> fe_observed_max_version;
};
```

HTTP 和内部请求都不接收 `schema_hash`。如需路径或环境诊断，BE 可自行读取 `tablet->schema_hash()` 并以 `observed_schema_hash` 记入日志或响应；该值始终不参与任务准入或提交 CAS。`dictionary_txn_id` 与当前 `DictionaryCacheManager` 术语保持一致；它就是字典刷新成功后作为缓存版本的 FE 全局单调事务 ID。

`fe_observed_max_version` 只用于检查目标副本是否至少追上 FE 规划水位和记录诊断信息，不得把 BE 实际 coverage 截断到该水位：

- BE 当前连续版本小于 FE 观测水位：返回可重试副本未追上状态。
- BE 当前连续版本等于或高于 FE 观测水位：仍以 BE 取得互斥后的当前连续上界固定 coverage。

首轮 HTTP 和未来 FE adapter 都必须先转换成此内部请求，执行器不感知请求来自 HTTP 还是 Agent Task。正式 FE 任务使用 Agent Task `signature` 作为 `task_id`；同一逻辑任务因超时或 busy 重试时保持 task ID 和全部请求内容不变。首轮 HTTP 由客户端生成并传入正数 `int64` task ID，BE 不代为生成。

### 6.2 请求规范化与重试契约

在 `tenant_ttl_compaction_types.cpp` 中集中实现：

```text
normalize:
    校验 protocol_version、task_id 和 mode
    tenants 按原始字节序排序
    精确去重
    不做 VARCHAR -> BIGINT 转换
    不做大小写、前后空格或 Unicode 归一化
```

BE 不新增 `request_digest/predicate_digest`，不对 JSON 或规范化请求计算哈希。FE/手工客户端遵守现有 Agent Task 契约：同一 task ID 的重试必须携带同一份请求。新一轮策略计算或任何请求内容变化都必须分配新 task ID。

空名单按已确认语义处理：

- `DELETE_LIST + empty`：仍通过 Tablet admission 和 coverage 身份校验后直接返回 `NOOP_VERIFIED`，不读取 tenant 列。
- `KEEP_LIST + empty`：返回 `INVALID_ARGUMENT`，提示应由 FE 使用整分区删除；BE 不把它转成全 Rowset DROP。

### 6.3 任务结果

不扩展通用 `TStatusCode`，新增 Tenant-TTL 专用结果枚举，避免普通代码路径依赖 Tenant-TTL 语义：

```cpp
enum class TenantTtlTaskCode {
    SUCCESS,
    NOOP_VERIFIED,
    INVALID_ARGUMENT,
    TABLET_NOT_FOUND,
    NOT_SUPPORTED,
    TABLET_BUSY,
    TTL_ALREADY_RUNNING,
    REPLICA_NOT_CAUGHT_UP,
    DATA_INVARIANT_VIOLATION,
    SCHEMA_CHANGED,
    STALE_ROWSET,
    CANCELLED,
    INTERNAL_ERROR,
};
```

结果至少包含：

```cpp
enum class TenantTtlRowsetAction {
    VERIFIED_NO_CHANGE, // 所有 Segment 都 KEEP，源 Rowset ID 不变
    DROP,               // 源 Rowset 全量删除，用同 version 空 Rowset 替换
    REWRITE,            // 至少一个 Segment 需 DROP 或 REWRITE，生成非空输出
};

struct TenantTtlRowsetResult {
    Version source_version;
    RowsetId source_rowset_id;
    std::optional<RowsetId> output_rowset_id;
    TenantTtlRowsetAction action;
    int64_t source_rows;
    int64_t kept_rows;
    int64_t deleted_rows;
    int32_t source_segments;
    int32_t linked_segments;
    int32_t dropped_segments;
    int32_t rewritten_segments;
};

struct TenantTtlCompactionResult {
    TenantTtlTaskCode code;
    Status detail_status;
    bool retryable;
    int64_t task_id;
    int64_t tablet_id;
    int64_t partition_id;
    int64_t snapshot_end_version;
    int64_t processed_through_version;
    std::string coverage_digest;
    std::vector<TenantTtlRowsetResult> rowsets;
    int64_t scanned_rows;
    int64_t kept_rows;
    int64_t deleted_rows;
    int64_t linked_bytes;
    int64_t rewritten_bytes;
};
```

Rowset 级动作单独定义，避免与 Segment 级动作混用。

失败发生在 coverage 固定之前时，版本和 coverage 字段为空；固定之后的成功、NOOP 或冲突必须返回实际观测到的 coverage 摘要，便于未来 FE 诊断。

### 6.4 coverage 模型

```cpp
struct TenantTtlCoverageEntry {
    Version version;
    RowsetId expected_rowset_id;
    RowsetSharedPtr source; // immutable strong reference
};

struct TenantTtlCoverage {
    int64_t snapshot_end_version;
    TenantTtlSchemaIdentity schema_identity;
    std::vector<TenantTtlCoverageEntry> entries; // version 升序
    std::string coverage_digest;
};
```

其中 `TenantTtlSchemaIdentity` 定义为：

```cpp
struct TenantTtlSchemaIdentity {
    TabletSchemaCSPtr captured_schema; // 强引用，同时作为任务内 CAS identity
    int64_t schema_id;
    int32_t schema_version;
};
```

请求的 `expected_schema_id + expected_schema_version` 用来识别任务开始前已经过期的 FE 视图。BE 固定 coverage 时持有当前 `TabletSchemaCSPtr`，所有输出 Writer 使用该对象；提交时在 header lock 下保守比较当前 Schema 对象与 `captured_schema` 的指针 identity，并复查 ID/version。任一不同则整体返回可重试 `SCHEMA_CHANGED`。不计算完整 `TabletSchemaPB` fingerprint，也不使用 `schema_hash` 做正确性判断。

### 6.5 Segment 计划和统计

```cpp
enum class SegmentFilterAction { KEEP, DROP, REWRITE };

struct SegmentFilterPlan {
    uint32_t src_segment_id;
    SegmentFilterAction action;
    SparseRangePtr keep_row_ranges; // 仅 REWRITE 必填
    uint32_t source_rows;
    uint32_t kept_rows;
    uint32_t deleted_rows;
};

struct SegmentRuntimeStats {
    int64_t num_rows;
    int64_t total_row_size;
    int64_t accounted_data_size;
    int64_t accounted_index_size;
    int64_t physical_artifact_size;
    std::string encryption_meta;
};

struct SegmentBuildResult {
    uint32_t src_segment_id;
    uint32_t dst_segment_id;
    SegmentRuntimeStats stats;
    std::vector<Artifact> artifacts;
};
```

`accounted_data_size/accounted_index_size` 必须复刻当前 `RowsetWriter/SegmentWriter` 的记账口径，不在本需求中重新定义现有 RowsetMeta 字段。KEEP 统计与同数据全量重写统计的口径一致性必须由测试证明。

## 7. 资格检查和拒绝边界

执行器按以下顺序检查：

1. 请求结构、protocol version、task ID、mode、名单、watermark 和 schema expectation 合法。
2. 按 tablet ID 获取本地 Tablet；校验 partition ID 和 Tablet 仍为 `TABLET_RUNNING`。
3. 拒绝 `tablet->updates() != nullptr` 的 PK Tablet，以及 shared-data/Lake Tablet。
4. 校验 `keys_type == DUP_KEYS`。
5. 按 unique ID 定位业务 tenant 列。首期只支持 nullable/non-nullable `VARCHAR`；列缺失时返回 `DATA_INVARIANT_VIOLATION`，类型不支持时返回 `NOT_SUPPORTED`。
6. 校验请求 `expected_schema_id/expected_schema_version` 与当前 Tablet 一致；请求不接收 `schema_hash`。
7. 获得 admission 和两把锁后固定 coverage。
8. coverage 中每个 Rowset 必须为当前 visible version path、schema 与固定 schema 一致、`NONOVERLAPPING`、没有 delete predicate、partial update 或 DCG，且全部 Segment 可加载。任一不满足则整任务失败，不挑选子集执行。

明确不是资格门禁的条件：

- 是否存在 `tenant_id` 生成列。
- `tenant_id` 是否为 `CAST(tenant AS BIGINT)`。
- 实际排序键是否为 `(tenant_id, recordTimestamp)`。
- 是否存在可用 Short Key、tenant ZoneMap 或定制 Segment 统计。
- coverage 中 Rowset 数量是否大于 1。

## 8. 并发状态、锁顺序和 RAII

### 8.1 Tablet 状态

在 `Tablet` 增加：

```cpp
enum class TenantTtlState { IDLE, PENDING, RUNNING };

struct TenantTtlOwner {
    int64_t task_id;
    TenantTtlPolicyWatermark watermark;
    uint64_t generation;
};
```

并增加仅在 `_compaction_task_lock` 下操作的方法：

```text
try_begin_tenant_ttl(owner)
mark_tenant_ttl_running(owner_generation)
finish_tenant_ttl(owner_generation)
tenant_ttl_state_for_debug()
```

`generation` 是 Tablet 为每次成功 admission 分配的进程内单调递增代次（fencing token），与 FE 逻辑任务身份 `task_id` 不同；同一 task ID 结束后再次准入也必须取得新的 generation。`try_begin_tenant_ttl()` 只在 `IDLE` 时递增内存计数器、写入 owner 并返回 generation；`mark_tenant_ttl_running()` 和 `finish_tenant_ttl()` 必须在 `_compaction_task_lock` 下比较当前 owner generation，匹配时才转换或清除状态，不匹配视为旧执行实例的延迟/重复调用且不得修改当前状态。锁保证“比较并修改”原子，generation 保证调用者仍是当前 owner，避免旧 guard 在 `IDLE -> 新任务 PENDING/RUNNING` 后误清除新任务。

状态、owner 和 generation 计数器均不写入 TabletMeta。任务结束后完全清除 active owner，但同一 Tablet 对象生命周期内不重置 generation 计数器；BE 重启后旧 guard 已不存在，因此计数器可重新初始化。Tablet 不保存 `last_task_id`、digest、tenant 名单或 AppliedProof。

- active owner 的 task ID 与新请求相同：返回 `TTL_ALREADY_RUNNING`。
- active owner 的 task ID 与新请求不同：返回 `TABLET_BUSY`。
- 前次任务已结束：不论 task ID 是否曾使用，都允许重新执行，以正式扫描和 `NOOP_VERIFIED` 收敛。

### 8.2 对普通 Compaction 的最小修改

修改 `Tablet::create_compaction_task()`、`need_compaction()` 和 `force_base_compaction()`：在现有 `_compaction_task_lock` 临界区内增加 `tenant_ttl_state == IDLE` 条件。TTL 已取得 PENDING/RUNNING admission 时，不再为该 Tablet 选择、标记或创建新的普通 Compaction 任务。

不修改：

- `CompactionTask::_try_lock()`。
- Horizontal/Vertical `run_impl()`。
- 普通 `_commit_compaction()`。
- `modify_rowsets_without_lock()`。

TTL 取得 admission 时，在同一 `_compaction_task_lock` 临界区内检查 `_has_running_compaction`；已经先创建的普通任务使 TTL 返回 `TABLET_BUSY`。已经入 CompactionManager 候选/线程池、但尚未调用 `create_compaction_task()` 的延迟任务，会在之后进入 `create_compaction_task()` 时因 TTL 非 IDLE 而无法创建真实 CompactionTask。该时序需用 SyncPoint 测试锁定。

### 8.3 固定锁顺序

```text
1. _compaction_task_lock：只完成 admission 状态转换，立即释放
2. base_lock：unique try-lock
3. cumulative_lock：unique try-lock
4. header lock：仅在固定 coverage 或最终提交时短暂持有
```

禁止在持有 header lock 时再获取 `_compaction_task_lock`，避免与现有“任务锁内执行策略选择、策略读取 Tablet 元数据”的顺序形成反向死锁。

对象构造顺序建议为：admission guard、base guard、cumulative guard、coverage rowset-mark guard、staged-output guard。正常和异常退出时按逆序清理：

```text
清理未提交 staged outputs
清除 coverage Rowset is_compacting
释放 cumulative/base locks
最后在 _compaction_task_lock 下恢复 TTL_IDLE
```

### 8.4 policy watermark 的首期解释

首期 BE 不读取字典，也不持久化已完成 policy watermark。所谓“提交前校验 watermark”落实为：

- 不可变请求对象与 admission owner 中的 watermark 必须一致。
- 当前任务的 admission generation 在执行期间不得变化。
- 严格 Tablet 级 admission 使更高水位任务无法覆盖正在运行任务；它只能得到 busy 并由 FE 稍后重试。

watermark 精确定义为 `{dictionary_id, dictionary_txn_id, evaluation_time_epoch_seconds}`。其中 `dictionary_txn_id` 标识 FE 使用的精确字典刷新版本，`evaluation_time_epoch_seconds` 固定 TTL 计算时点。BE 只校验字段格式和任务内 owner 一致性，不查询 DictionaryCacheManager。

BE 不声称在重启后或两个已经串行完成的任务之间建立全局 policy 版本顺序；该顺序首期仍由 FE 保证。不能在没有持久化协议的情况下把内存比较描述为跨重启的水位证明。

## 9. coverage 捕获与提交设计

### 9.1 coverage 捕获

在 `Tablet` 新增 Tenant-TTL 专用方法，避免任务代码直接访问 `_rs_version_map`：

```cpp
StatusOr<TenantTtlCoverage> Tablet::capture_tenant_ttl_coverage(
        const TenantTtlCompactionRequest& request);
```

该方法在一个 header read lock 临界区中：

1. 读取 `_timestamped_version_tracker` 的最大连续版本上界。
2. 从当前 `_rs_version_map` 解出完整 `[0,B]` 路径；不得回退到 `_stale_rs_version_map`。
3. 检查 version 无空洞、重叠或暂不可读的更高 active Rowset。
4. 按 version 升序记录强引用、version 和 Rowset ID。
5. 持有当前 `TabletSchemaCSPtr`，记录 schema ID/version，并将它作为 coverage 的 schema identity。
6. 检查每个 Rowset 的 eligibility 和 `is_compacting`。
7. 只有全部通过后才统一设置 coverage Rowset 的 `is_compacting=true`。

“检查一部分、标记一部分、后续失败”的实现必须回滚已经设置的标记；优先采用先全量验证、后全量标记。

### 9.2 输出 staging

每个变化源 Rowset创建独立同 version 输出：

```text
rowset_id       = StorageEngine::next_rowset_id()
tablet_uid      = source Tablet UID
tablet/partition/schema = 当前固定 schema
rowset_state    = VISIBLE
version         = source.version
segments_overlap = NONOVERLAPPING
gtid            = source.gtid
is_compaction   = true
```

`StagedRowsetsGuard` 持有已经 build 成功但尚未提交的输出。提交前任意失败时调用 `StorageEngine::add_unused_rowset()` 清理文件和释放 Rowset ID；Writer build 前失败由 Writer/Artifact guard 删除已经创建的文件。

### 9.3 TTL 专用提交 wrapper

新增：

```cpp
Status Tablet::commit_tenant_ttl_rowsets(
        const TenantTtlCoverage& coverage,
        const std::vector<TenantTtlReplacement>& replacements,
        const TenantTtlSchemaIdentity& expected_schema,
        std::vector<RowsetSharedPtr>* replaced_stale_rowsets);
```

该方法内部持有唯一的 header write lock，严格分成两个阶段：

```text
Validation phase（不得修改 TabletMeta）:
    校验 Tablet 仍 RUNNING
    比较当前 TabletSchemaCSPtr 与 captured_schema 的指针 identity
    复查 schema_id/schema_version
    对 coverage 每项：
        current = get_rowset_by_version(expected.version)
        current != null
        current.rowset_id == expected.rowset_id
    校验 replacement source 唯一
    校验 output.version == source.version
    校验 output 已 load、verify 且 Rowset ID 不与当前冲突

Mutation phase（仅全部通过后进入）:
    changed_sources 使用刚从当前 version map 读取并验证的 current 对象
    modify_rowsets_without_lock(all_outputs, changed_sources, &to_replace)
    save_meta(config::skip_schema_in_rowset_meta) 一次
    close changed_sources
```

普通 `modify_rowsets_without_lock()` 保持原样。它会把 changed source 放入 stale map；`to_replace` 中被顶替的旧 stale Rowset 在锁外加入 unused GC。KEEP source 仍在 active version map，不得 close 或进入 stale。

当前 `Tablet::save_meta()` 在持久化失败时采用 fatal 语义而不是返回 `Status`。因此 validation phase 必须完整完成后才能首次修改内存 version map；“内存已改、save_meta 前崩溃”和“save_meta 后、响应前崩溃”由第二批重启恢复测试验证，首期不在本任务中重写 Tablet 持久化事务模型。

### 9.4 NOOP 提交校验

当全 coverage `deleted_rows == 0` 时不调用 Writer。先在 `_compaction_task_lock` 下复查 admission owner 的 task ID、generation 和 policy watermark；由于本任务仍持有 admission guard，校验通过后不会有新 TTL owner 介入。再调用只校验、不修改的专用方法，在一个 header lock 内复查：

- 每个 `(version,rowset_id)`。
- captured schema pointer identity 及 schema ID/version。
- Tablet 状态。

通过后返回：

```text
code = NOOP_VERIFIED
processed_through_version = snapshot_end_version
coverage digest/list = 完整 coverage
```

失败则返回 `STALE_ROWSET/SCHEMA_CHANGED`，不能返回 NOOP。

## 10. TenantTtlRowFilter 详细设计

新增：

```text
be/src/storage/tenant_ttl_row_filter.h
be/src/storage/tenant_ttl_row_filter.cpp
```

核心接口：

```cpp
StatusOr<SegmentFilterPlan> TenantTtlRowFilter::plan_segment(
        const Rowset& rowset,
        uint32_t segment_id,
        int32_t tenant_column_unique_id,
        const TenantFilter& filter,
        MemTracker* mem_tracker,
        CancellationChecker should_cancel);
```

实现步骤：

1. `rowset.load()` 并取得指定 Segment。
2. 根据 unique ID 从固定 Rowset schema 中找到 tenant column ordinal；构造仅含该列的 `Schema`。
3. 创建 `SegmentReadOptions` 和 Segment iterator；首期不提供自定义 predicate/ZoneMap/ShortKey 选择条件。
4. 按 `config::vector_chunk_size` 或内存预算确定 chunk size，只读取 tenant 列。
5. 对每个 Chunk：
   - 先读取 nullable bitmap；NULL 追加到 keep range。
   - 非 NULL 值按精确二进制 VARCHAR 与规范化集合比较。
   - 将连续保留 row ordinal 合并为半开区间，避免逐行 Range 对象膨胀。
   - 同时累计 source/kept/deleted 行数。
6. 校验扫描行数等于 `segment.num_rows()`。
7. 根据保留行数收敛：全保留为 KEEP、零保留为 DROP、其余为 REWRITE。

名单查找使用预构建的只读 hash set，排序 vector 用于稳定响应和测试。名单内容不写入日志，日志只记录名单数量，避免租户信息泄露及大日志。

## 11. VerticalSegmentRewriter 详细设计

新增：

```text
be/src/storage/rowset/vertical_segment_rewriter.h
be/src/storage/rowset/vertical_segment_rewriter.cpp
```

它不是现有 partial-update `SegmentRewriter` 的新分支，而是消费完整保留行范围的独立通用组件。

接口草案：

```cpp
StatusOr<SegmentBuildResult> VerticalSegmentRewriter::rewrite(
        const Rowset& source_rowset,
        uint32_t src_segment_id,
        uint32_t dst_segment_id,
        const SparseRange<rowid_t>& keep_row_ranges,
        const RowsetWriterContext& dst_context,
        MemTracker* mem_tracker,
        CancellationChecker should_cancel);
```

实现顺序：

1. 校验 `0 < keep_rows < source_segment.num_rows()`。
2. 创建目标 `.dat` 的 `WritableFile`；透明加密开启时生成新 encryption meta。
3. 创建唯一 `SegmentWriter(dst_segment_id)`。
4. 调用 `CompactionUtils::split_column_into_groups()`；第一组包含完整实际 sort key，其余列按现有 Vertical Compaction 配置分组。
5. 对第一组调用 `SegmentWriter::init(column_group, true)`；后续组调用 `init(column_group, false)`。
6. 每个列组都使用同一 `SegmentReadOptions::rowid_range_option`，按 Chunk 读取并 `append_chunk()`。
7. 对 CHAR 列复用 `ChunkHelper::padding_char_columns()`；每组完成后 `finalize_columns()`。
8. 记录每组写入行数，所有组必须等于 `keep_row_ranges.span_size()`。
9. 所有列完成后调用一次 `finalize_footer()`，关闭文件并返回完整 `SegmentBuildResult`。
10. 发生取消、内存超限或 I/O 错误时关闭 Writer，由 artifact guard 删除 `.dat`、GIN/Vector 等已经产生的外部 artifact。

只有 footer 和外部索引全部完成后，调用方才可以登记该目标 Segment。禁止把仍在写列组的 Segment 暴露给 Rowset 组装器。

## 12. FilteredRowsetWriter 详细设计

新增：

```text
be/src/storage/rowset/filtered_rowset_writer.h
be/src/storage/rowset/filtered_rowset_writer.cpp
```

`FilteredRowsetWriter final : public RowsetWriter`，只执行通用 `SegmentFilterPlan`，不包含 tenant、TTL、名单或字典逻辑。

### 12.1 处理顺序

```text
for plan ordered by src_segment_id:
    KEEP:
        add_linked_segment(source, src_segment_id, next_dst_segment_id)
        next_dst_segment_id++
    DROP:
        do nothing
    REWRITE:
        VerticalSegmentRewriter::rewrite(..., next_dst_segment_id)
        register completed result
        next_dst_segment_id++

build():
    汇总统计和 encryption meta
    RowsetWriter::build()
```

必须检查计划完整覆盖源 Rowset 的 `[0,num_segments)`，顺序严格递增，不允许重复或缺失源 Segment。

### 12.2 KEEP hard link

首期在 `FilteredRowsetWriter` 内新增只服务该 Writer 的 Segment artifact linker，不重构现有 `Rowset::link_files_to()`，降低普通 shortcut compaction 路径风险。它负责：

- 把源 `segment_{src}.dat` 链接为目标 Rowset 的 `segment_{dst}.dat`。
- 按 TabletSchema index 描述重映射并链接 GIN 目录文件、Vector 文件及当前支持的独立索引 artifact。
- 复制源 `segment_encryption_metas[src]` 到目标 ordinal。
- 每成功创建一个文件/目录立即登记到 artifact guard；后续失败按逆序删除。
- 链接前确认源、目标位于同一支持 hard link 的本地文件系统；首期不做 copy fallback。

### 12.3 运行时统计

首期不修改 `RowsetMetaPB`。实现读侧统计 helper：

```text
Segment::collect_runtime_stats()：
    从已加载 footer、ColumnReader、文件大小重建行数、raw row size、
    数据页/字典页/内嵌索引相关记账值

FilteredRowsetWriter：
    补充 GIN/Vector 外部 artifact 大小和源 Segment encryption meta
```

REWRITE Segment 直接使用 `SegmentWriter::finalize_columns/finalize_footer` 返回值及写入 Chunk 统计。Writer 只累加已经完整登记的 KEEP/REWRITE Segment，DROP 不计入目标统计。

`num_rows`、`total_row_size`、`data_disk_size`、`index_disk_size`、`total_disk_size`、`num_segments`、`segment_encryption_metas` 必须准确。不得按 Segment 数量近似拆分源 Rowset 聚合值，也不得用 0 填充未知统计。

### 12.4 空 Rowset

若源 Rowset所有 Segment 均 DROP，Writer 不创建 Segment artifact，直接构建：

```text
num_rows = 0
num_segments = 0
empty = true
version = source.version
segments_overlap = NONOVERLAPPING
```

空输出仍必须 build/load/verify 并参加最终批量提交。

## 13. EngineTenantTtlCompactionTask 详细设计

新增：

```text
be/src/storage/task/engine_tenant_ttl_compaction_task.h
be/src/storage/task/engine_tenant_ttl_compaction_task.cpp
```

推荐接口：

```cpp
class EngineTenantTtlCompactionTask final : public EngineTask {
public:
    EngineTenantTtlCompactionTask(TenantTtlCompactionRequest request,
                                  MemTracker* parent_tracker);
    Status execute() override;
    const TenantTtlCompactionResult& result() const;
};
```

任务创建 `COMPACTION_TASK` 子 MemTracker，并设置线程局部 tracker。执行步骤：

1. 规范化并验证请求，初始化结果和总计数器。
2. 获取并验证 Tablet。
3. `try_begin_tenant_ttl()`，创建 admission RAII guard。
4. 依次 unique try-lock base/cumulative；失败立即返回 busy。
5. 把状态转为 RUNNING。
6. 固定 coverage 并创建 Rowset mark guard。
7. 按 version 顺序逐 Rowset处理：
   - 按 Segment 顺序调用 `TenantTtlRowFilter`。
   - Rowset 全 KEEP：记录 `VERIFIED_NO_CHANGE`，不创建 Writer。
   - Rowset 有 DROP/REWRITE：创建同 version `FilteredRowsetWriter`，执行全部 Segment 计划，build/load/verify，加入 staged guard。
   - 完成该 Rowset 后释放其 `keep_row_ranges`，控制内存峰值。
8. 校验任务级 `source_rows == kept_rows + deleted_rows`，以及每个变化输出 `output_rows == source_rows - deleted_rows`。
9. 在 `_compaction_task_lock` 下复查 admission owner 的 task ID、generation 和 policy watermark；该步不在 header lock 内取 task lock，避免锁顺序反转。
10. 全 coverage deleted rows 为零：调用 NOOP 身份复查并返回 `NOOP_VERIFIED`。
11. 否则调用 TTL 专用 commit wrapper 一次提交全部变化 Rowset。
12. commit 成功后解除 staged guard，处理 `to_replace` unused Rowset，填充完整结果。
13. 所有退出路径由 RAII 清理 Rowset 标记、锁和 admission。

取消检查放在 Rowset、Segment、Chunk 和列组边界。HTTP 客户端断开不直接决定是否取消已经进入提交准备的任务；无论继续完成还是显式取消，都必须通过同一 RAII 清理路径，不能把资源生命周期绑定在 socket 对象上。

## 14. HTTP 手工入口设计

新增：

```text
be/src/http/action/tenant_ttl_compaction_action.h
be/src/http/action/tenant_ttl_compaction_action.cpp
```

路由：

```text
POST /api/tenant_ttl_compaction/run
Content-Type: application/json
```

Action 只做：JSON 解析、字段类型校验、tenant 名单规范化、构造内部请求、同步执行 `EngineTenantTtlCompactionTask`、序列化结果。它不计算请求 digest，也不得直接选择 Rowset、传入 Rowset/Segment ID、跳过锁或复制 Writer/commit 逻辑。

建议 HTTP 映射：

| Tenant-TTL 结果 | HTTP status |
| --- | --- |
| SUCCESS / NOOP_VERIFIED | 200 |
| INVALID_ARGUMENT / DATA_INVARIANT_VIOLATION | 400 |
| TABLET_NOT_FOUND | 404 |
| STALE_ROWSET / SCHEMA_CHANGED | 409 |
| TABLET_BUSY / TTL_ALREADY_RUNNING / REPLICA_NOT_CAUGHT_UP | 503 |
| NOT_SUPPORTED | 501 |
| INTERNAL_ERROR | 500 |

响应体始终包含稳定的 `code/retryable/message/task_id`；有 coverage 时附带处理水位、coverage digest、Rowset 和统计列表。coverage digest 只是结果中对 `(version,rowset_id)` 清单的紧凑表示，与已删除的请求 digest 无关。

### 14.1 编译隔离

在 `be/CMakeLists.txt` 增加默认 OFF 的 option：

```text
ENABLE_TENANT_TTL_MANUAL_TEST_ENDPOINT=OFF
```

只有显式开启时定义：

```text
STARROCKS_TENANT_TTL_MANUAL_TEST_ENDPOINT
```

以下三处必须同时受宏/option 保护：

1. `be/src/http/CMakeLists.txt` 是否编译 Action 实现文件。
2. `be/src/service/service_be/http_service.cpp` 的 include、实例化和路由注册。
3. `be/test/CMakeLists.txt` 的 HTTP Action 专项测试。

正式 Engine task、过滤器、Writer、Tablet admission 和 commit 不受该宏保护，确保未来 FE 复用同一条生产执行链。

## 15. 代码文件变更清单

### 15.1 新增文件

| 文件 | 职责 |
| --- | --- |
| `be/src/storage/tenant_ttl_compaction_types.h/.cpp` | 请求、结果、名单规范化、coverage/计划公共类型 |
| `be/src/storage/tenant_ttl_row_filter.h/.cpp` | 业务 tenant 单列精扫并生成 SegmentFilterPlan |
| `be/src/storage/rowset/vertical_segment_rewriter.h/.cpp` | 单源 Segment 按 SparseRange 纵向重写 |
| `be/src/storage/rowset/filtered_rowset_writer.h/.cpp` | 通用 KEEP/DROP/REWRITE Rowset 组装、统计和 artifact 清理 |
| `be/src/storage/task/engine_tenant_ttl_compaction_task.h/.cpp` | admission、coverage、多 Rowset staging、原子提交编排 |
| `be/src/http/action/tenant_ttl_compaction_action.h/.cpp` | 特殊宏下的同步手工 HTTP 入口 |
| `be/test/storage/tenant_ttl_row_filter_test.cpp` | 过滤算法单测 |
| `be/test/storage/rowset/vertical_segment_rewriter_test.cpp` | SparseRange 多列组重写测试 |
| `be/test/storage/rowset/filtered_rowset_writer_test.cpp` | 通用 Writer、统计、artifact 和清理测试 |
| `be/test/storage/task/engine_tenant_ttl_compaction_task_test.cpp` | 真实 Tablet、多 Rowset、提交、并发和故障集成测试 |
| `be/test/http/action/tenant_ttl_compaction_action_test.cpp` | HTTP 契约和状态映射测试 |
| `test/sql/test_tenant_ttl_compaction/T/...` | 特殊构建环境的 SQL+HTTP 输入测试 |
| `test/sql/test_tenant_ttl_compaction/R/...` | 归一化后的结果基线 |

### 15.2 修改文件

| 文件 | 修改范围 |
| --- | --- |
| `be/src/storage/tablet.h/.cpp` | TTL 三态/owner、admission、coverage 捕获、TTL 专用 commit wrapper；普通任务创建增加 IDLE 门禁 |
| `be/src/storage/rowset/segment.h/.cpp` | 只新增运行时统计收集 helper，不改变现有读取行为 |
| `be/src/storage/CMakeLists.txt` | 加入 types/filter/engine task 源文件 |
| `be/src/storage/rowset/CMakeLists.txt` | 加入 FilteredRowsetWriter 和 VerticalSegmentRewriter |
| `be/src/http/CMakeLists.txt` | option 开启时加入手工 Action |
| `be/src/service/service_be/http_service.cpp` | 宏保护的 include 和路由注册 |
| `be/CMakeLists.txt` | 默认 OFF 的手工入口 build option 和宏定义 |
| `be/src/util/starrocks_metrics.h/.cpp` | 固定维度的 Tenant-TTL 请求、结果、行数、字节和耗时指标 |
| `be/test/CMakeLists.txt` | 加入新单测/独立集成测试和条件 HTTP 测试 |
| `test/lib/sr_sql_lib.py` | 特殊测试环境复用的 Tablet 定位、HTTP POST 和结果归一化 helper |

## 16. 分轮编码节奏

每一轮必须保持可编译、可独立评审和可回退，不允许先堆积全部实现、最后一次性补测试。

### 第 0 轮：契约、类型和测试基础设施

#### 目标

冻结内部请求/结果、名单规范化、task ID 重试契约和测试 Tablet/Rowset 构造器，不修改真实 Tablet 数据。

#### 详细设计

- 新增 types 文件；不引入请求 SHA-256/digest 逻辑。
- 实现请求校验和结果 code 字符串转换。
- 建立 Tenant-TTL 独立 StorageEngine 测试 fixture：临时 DataDir、DUP_KEYS Tablet、nullable VARCHAR tenant、任意非 tenant 排序键、多 version Rowset、可控制每 Rowset Segment 数量。
- 建立 metadata snapshot helper：记录 version -> Rowset ID、行数、Segment 数、RowsetMeta 聚合统计和目录 artifact。
- 在测试 fixture 中关闭事件驱动后台 Compaction或提高阈值，确保多 Rowset 形态可重复。

#### 对应测试

- DELETE_LIST/KEEP_LIST mode 校验。
- tenant 名单按原始字节序排序、精确去重，不改变匹配语义。
- task ID 必须为正数；同一 task ID 在 HTTP/Agent 重试样例中保持请求内容不变。
- `dictionary_id/dictionary_txn_id/evaluation_time_epoch_seconds` 的必填和边界校验。
- `expected_schema_id/expected_schema_version` 必填；内部请求和 HTTP JSON 均不允许请求方传入 `schema_hash`。
- 内部请求、HTTP JSON 和结果协议均不存在 `request_digest/predicate_digest` 字段。
- DELETE 空名单与 KEEP 空名单协议。
- fixture 能稳定构造单 Rowset、多 Rowset、单/多 Segment和空 Rowset。

#### 完成标准

- 类型层无 HTTP/FE/字典依赖。
- 测试 fixture 连续多次运行获得一致的 Rowset/Segment 布局。
- 尚不存在数据修改入口。

### 第 1 轮：TenantTtlRowFilter 精确单列扫描

#### 目标

把每个源 Segment 的业务 tenant 列转换为正确的 `SegmentFilterPlan`。

#### 详细设计

- 实现 unique ID 找列、单列 Segment iterator、nullable 处理、精确 byte comparison。
- 连续 row ordinal 合并为 SparseRange。
- 加入 cancellation/memory check 和完整行数校验。
- 不增加 tenant_id、ShortKey、自定义 ZoneMap 或时间列读取。

#### 对应测试

- DELETE_LIST：全 KEEP、全 DROP、部分 REWRITE。
- KEEP_LIST：全 KEEP、全 DROP、部分 REWRITE。
- NULL 始终 KEEP。
- `"2"` 与 `"02"`、空字符串、长字符串、多字节字符串。
- 重复名单、多 Chunk、跨 Page/Segment 边界、不连续 keep ranges。
- tenant 列缺失、类型错误、读取损坏、取消和内存错误。
- 通过访问计数或 mock 证明未读取 `recordTimestamp`、`tenant_id` 和字典。

#### 完成标准

- 结果与简单逐行参考实现一致。
- 一个 Segment 的 tenant 列只扫描一遍。
- KEEP/DROP 不携带无用 SparseRange，REWRITE 的范围行数准确。

### 第 2 轮：通用 Segment/Rowset 输出执行层

#### 目标

完成与 TTL 策略解耦的 `VerticalSegmentRewriter` 和 `FilteredRowsetWriter`。

#### 详细设计

- 先实现 REWRITE：单源 Segment、单 `SegmentWriter`、多列组重放同一 SparseRange。
- 再实现 KEEP Segment artifact hard link、DROP omission 和连续目标 ordinal。
- 实现 artifact guard、build 前失败清理和 build 后 staged cleanup 接口。
- 实现 KEEP 运行时统计收集及 REWRITE 统计回传。
- 支持空 Rowset build。
- 输出必须无条件执行 load 和 `Rowset::verify()`；不受普通 `enable_rowset_verify` 开关影响。

#### 对应测试

- SparseRange 多区间在所有列组写入相同行数和值。
- 第一列组包含全部实际 sort key。
- KEEP/REWRITE 交替、DROP 后目标 ordinal 连续、每源 Segment 0/1 输出。
- `.dat` hard link inode/内容一致，目标命名使用新 Rowset ID 和连续 dst ordinal。
- GIN/Vector、透明加密 meta 的链接/重建和 ordinal 重映射。
- 老 Rowset 不含新增 PB 字段仍可处理。
- RowsetMeta 统计与同数据全量重写结果口径一致。
- 所有 Segment DROP 能生成可 load/verify 的空同 version Rowset。
- link、列组写入、finalize、统计和 verify 任一点失败均无残留 artifact/Rowset ID。

#### 完成标准

- Writer 测试完全使用人工 `SegmentFilterPlan`，不依赖 tenant 语义。
- 不修改 `RowsetMetaPB`。
- 不改变现有 Horizontal/Vertical/shortcut writer 测试结果。

### 第 3 轮：Tablet admission、coverage 和原子提交原语

#### 目标

提供 Engine task 可安全使用的 Tablet 级并发与提交基础，但暂不接 HTTP。

#### 详细设计

- 增加 TTL state、当前 owner、generation 和 RAII admission guard；不保存 last task identity。
- 在普通 Compaction 创建/选择入口增加 TTL IDLE 门禁。
- 实现固定顺序的 base/cumulative unique try-lock guard。
- 实现当前 active version path 的完整 coverage 捕获和统一 `is_compacting` 标记。
- 实现 TTL 专用 commit/noop validation wrapper。
- 所有普通 Compaction commit 和 `modify_rowsets_without_lock()` 保持原样。

#### 对应测试

- IDLE -> PENDING -> RUNNING -> IDLE。
- 同一 task ID 结束后再次准入得到更大的 generation；旧 generation 的延迟 `mark/finish` 不得改变新 owner 的 PENDING/RUNNING 状态。
- base 失败、cumulative 失败时立即释放已有锁和 admission。
- active same task ID 返回 `TTL_ALREADY_RUNNING`，active other task ID 返回 `TABLET_BUSY`。
- 已创建普通 Compaction 时 TTL busy；TTL PENDING/RUNNING 时普通任务不能创建。
- 不同 Tablet admission 可以并发。
- coverage 包含完整 `[0,B]` 多 Rowset 路径；空洞、重叠、stale-only 路径拒绝。
- coverage 固定后的新高版本不进入本次列表。
- BE 当前连续版本低于 `fe_observed_max_version` 时返回可重试 `REPLICA_NOT_CAUGHT_UP`；等于或高于时 coverage 仍扩展到 BE 取得互斥后的当前连续上界。
- hand-built staged outputs 的多 Rowset一次提交、一次 save meta。
- 任务开始时 `expected_schema_id/expected_schema_version` 不匹配直接返回 `SCHEMA_CHANGED`；执行期间 captured schema pointer 或 schema ID/version 改变时零修改并返回冲突。
- KEEP Rowset 同样参加 CAS，但不进入 changed_sources。

#### 完成标准

- ThreadSanitizer 友好的状态访问；测试中不存在无锁读写 TTL state。
- 锁获取顺序有注释和 SyncPoint 验证。
- 普通 Compaction 现有单测全部通过。

### 第 4 轮：EngineTenantTtlCompactionTask 多 Rowset闭环

#### 目标

把过滤、Writer、Tablet 原语组合成正式 BE 执行链，完成第一批核心存储测试。

#### 详细设计

- 实现完整 eligibility、内存跟踪、取消、逐 Rowset扫描/构建、staged guard、NOOP 和 commit。
- 一个 Rowset完成后释放其 SparseRange，避免同时保存全 coverage 的逐行范围。
- 增加固定维度日志、trace 和 metrics；禁止把 tenant 值作为 label 或大段输出。
- 结果返回完整 coverage 和每 Rowset动作。

#### 对应测试

单 Rowset参数化矩阵：

```text
DELETE_LIST × KEEP/DROP/REWRITE
KEEP_LIST   × KEEP/DROP/REWRITE
```

多 Rowset `DELETE_LIST(A)`：

| Rowset | 数据 | 预期 |
| --- | --- | --- |
| R1/V1 | B、C、NULL | KEEP，Rowset ID 不变 |
| R2/V2 | A、A | DROP，同 V2 空 Rowset |
| R3/V3 | A、B、NULL | REWRITE，仅保留 B、NULL |

多 Rowset `KEEP_LIST(B)`：

| Rowset | 数据 | 预期 |
| --- | --- | --- |
| R1/V1 | B、NULL | KEEP，Rowset ID 不变 |
| R2/V2 | A、C | DROP，同 V2 空 Rowset |
| R3/V3 | A、B、NULL | REWRITE，仅保留 B、NULL |

同时验证：

- 执行前确有三个独立 Rowset。
- DROP/REWRITE Rowset ID 变化但 version 不变，KEEP Rowset ID 不变。
- TabletReader 读取所有业务列与参考结果一致。
- version 连续，TabletMeta 只保存一次。
- 中间第 N 个输出失败时没有任何源 Rowset 被替换。
- 不同 task ID 重复同谓词返回 `NOOP_VERIFIED`，所有 Rowset ID 不再变化。
- NOOP 不创建 Writer、不分配 Rowset ID、不保存 TabletMeta、不进入 stale/unused。
- 无 `tenant_id`、非指定排序键的表仍然成功。

#### 完成标准

- 第一批中所有内部正确性断言完成。
- 任务仍不能从默认 HTTP 路由触发。
- 执行器接口可由未来 FE adapter 直接构造请求调用。

### 第 5 轮：特殊 HTTP 入口和 SQL+HTTP 端到端

#### 目标

提供首轮手工验证入口并完成用户可见数据闭环，同时保证发布隔离。

#### 详细设计

- 增加默认 OFF build option、条件源文件、条件 include/route。
- Action 使用 RapidJSON 严格解析 body，不接受未知的 bypass/Rowset/Segment 控制字段；调用方提供的 schema hash 不在请求字段白名单内，传入时按未知字段拒绝。
- Action 同步调用正式 Engine task，返回真实结果和 retryable。
- 在 SQL tester 增加专用 helper：按列名解析 `SHOW TABLET`，定位 BE HTTP 地址和 Tablet 信息，POST JSON，归一化动态 ID。
- 专项 SQL 集群提高 Cumulative Compaction 触发阈值并在结束后恢复；调用前通过 Tablet meta/status 确认多 Rowset。

#### 对应测试

- HTTP 合法 DELETE_LIST/KEEP_LIST、非法 JSON、字段缺失、mode 冲突、task ID/watermark 非法、unsupported Tablet、busy 和 stale 映射。
- HTTP 请求携带调用方 schema hash 时按未知字段拒绝；响应如包含 BE 自行读取的 `observed_schema_hash`，仅验证其不参与准入和提交判断。
- HTTP 不能指定 rowset/segment，不能跳过锁/CAS，不能 force。
- 默认构建不编译 Handler，路由 404；特殊构建路由存在。
- SQL+HTTP DELETE_LIST、KEEP_LIST、多 Rowset混合、NULL、不带 tenant_id/指定排序键。
- 不同 task ID 重复谓词：第二次 `NOOP_VERIFIED`，SQL 数据及 Rowset ID 不变。

#### 完成标准

- 第一批最小测试集全部通过。
- 发布构建检查不含手工 Handler 符号和路由字符串。
- HTTP Action 不包含任何存储实现分支。

### 第 6 轮：第一批收口和代码评审修正

#### 目标

完成首期 BE 功能的合入质量门槛，不扩展功能范围。

#### 详细设计

- 本轮不新增业务能力，以“固定回归集 + 资源安全审查 + 故障边界审查”收口第一批实现。
- 把第 4 节不变式逐项映射到测试名称和断言，评审时不允许仅依赖代码推断。
- 对所有 Writer、Rowset 引用、`is_compacting`、TTL admission 和两把 Compaction lock 的退出路径做 RAII 审查。
- 确认新增调用不改变 Horizontal/Vertical/shortcut Compaction 的提交路径和默认 HTTP 构建。

#### 对应测试

- 运行新增目标、现有 RowsetWriter、Horizontal/Vertical Compaction、Tablet 和 HTTP 回归测试。
- 开启 ASAN/UBSAN 跑核心测试。
- 检查所有 Status 返回、资源 guard、取消点、日志敏感信息和 metrics label。
- 通过 failpoint 覆盖第一批要求的第 N 个输出失败、提交前 Rowset ID 变化和 HTTP 断连。
- 对照本文第 4 节逐条检查不变式并记录验证结果。

#### 完成标准

- 第一批测试无跳过项。
- 普通 Compaction 行为和性能路径未被改写。
- 用户再次确认后，首期 BE 编码阶段才可宣告完成。

### 第 7 轮：第二批增强测试和生产化加固

#### 目标

证明并发、故障、重启、多副本和持续运行条件下仍可靠；该轮不阻塞最早的功能验证，但应在正式 FE 联调/生产发布前完成核心项。

#### 详细设计

- 在第 3～5 轮预留的 SyncPoint/failpoint 上构造可重复的并发与崩溃时序，不使用依赖 sleep 的概率性测试。
- 随机属性测试统一使用逐行 oracle，失败时输出 seed 和已规范化输入，但不输出真实 tenant 数据。
- 多副本测试以“逻辑数据和处理水位收敛”为断言，不错误要求各副本 Rowset ID 相同。
- 性能与长稳测试分开记录链接字节和重写字节，以便区分正确性回归与读写放大回归。

#### 对应测试

- 崩溃点：coverage 后、部分/全部输出后、header lock 前、version map 修改/TabletMeta 保存边界、保存后响应前、stale 回收前。
- 深度竞态：TTL/TTL、TTL/Base、TTL/Cumulative、overwrite publish、schema change、clone/repair、Tablet shutdown/drop、不同 Tablet 高并发。
- 全链路错误：Reader、Iterator、tenant 列、hard link、磁盘满、Writer、footer、外部索引、stats、load/verify、meta save。
- 随机属性：随机 Rowset/Segment/tenant/NULL/名单/命中密度，与逐行 oracle 对比并记录 seed。
- ASAN/UBSAN/TSAN 专项。
- 多副本手工编排：部分副本成功、失败、重启和重试收敛；不要求副本 Rowset ID 相同。
- 性能基线：0/1/10/50/90/100% 删除、冷热缓存、短/长名单、单/多 Rowset和 Segment。
- 长稳：循环写入、TTL、普通 Compaction、重启，检查内存、FD、临时/stale 文件、TabletMeta 和状态泄漏。
- HTTP 鲁棒性：超大 body/list、非法编码、重复字段、数值越界、高并发和客户端反复断连。

#### 完成标准

- 崩溃恢复后只存在完整旧状态或完整新状态。
- 确定性竞态和故障注入无部分提交。
- TSAN 无 Tablet TTL 状态机数据竞争。
- 随机属性测试可持续通过并可复现失败。
- 多副本失败重试可收敛。
- `NOOP_VERIFIED` 确认零写入、零新 Rowset、零 TabletMeta 修改。
- 长稳无状态、文件、Rowset ID、内存或 FD 泄漏。

## 17. 指标、日志与诊断

新增指标使用固定 label，不以 tenant、task ID、Tablet ID 作为 label：

```text
tenant_ttl_compaction_requests_total{result=success|noop|busy|conflict|failed}
tenant_ttl_compaction_rows_scanned_total
tenant_ttl_compaction_rows_deleted_total
tenant_ttl_compaction_rowsets_total{action=keep|drop|rewrite}
tenant_ttl_compaction_segments_total{action=keep|drop|rewrite}
tenant_ttl_compaction_linked_bytes_total
tenant_ttl_compaction_rewritten_bytes_total
tenant_ttl_compaction_duration_us
tenant_ttl_compaction_running
```

单任务日志至少包含：task ID、tablet/partition、watermark、schema ID/version、snapshot/processed version、coverage digest、tenant 名单数量、Rowset/Segment动作计数、删除/保留行数、读写字节、耗时和最终 code。tenant 名单本身不输出。

建议 SyncPoint/failpoint 从第一轮实现时就放置在：

- admission 成功后、取锁前。
- base lock 成功、cumulative lock 前。
- coverage 固定和 Rowset 标记后。
- 每个 Rowset计划完成后。
- 每个 staged output build/verify 后。
- 最终 header lock 前。
- 全 coverage 校验后、第一次 metadata mutation 前。
- `modify_rowsets_without_lock()` 后、`save_meta()` 前。
- `save_meta()` 后、HTTP 响应前。

## 18. 风险与控制措施

| 风险 | 控制措施 |
| --- | --- |
| 修改普通 Compaction 准入引入回归 | 只在现有 `_compaction_task_lock` 条件中增加 TTL IDLE 检查；不改普通 task/commit；补现有回归和确定性竞态测试 |
| 多 Rowset部分提交 | 全部 staged/load/verify 后统一 commit；validation/mutation 两阶段；第 N 个输出失败测试 |
| Rowset version 相同但实例已变化 | header lock 内比较每个 version 对应的 Rowset ID；使用重新读取的 current 作为 changed source |
| NOOP 错误覆盖并发变化 | 返回前同一 header lock 内复查完整 coverage 和 schema |
| KEEP hard link 漏掉外部索引或 encryption meta | artifact 枚举、ordinal 重映射和 GIN/Vector/TDE 专项测试 |
| RowsetMeta 统计不准 | 运行时逐 Segment统计；与全量重写口径对比；统计失败整任务不提交 |
| Writer 失败残留文件/Rowset ID | build 前 ArtifactGuard、build 后 StagedRowsetsGuard、failpoint 遍历 |
| 锁顺序死锁 | 固定 task -> base -> cumulative -> header 的分阶段顺序；禁止 header 内取 task lock；SyncPoint/TSAN |
| tenant 名单过大引起内存/日志问题 | 单份规范化 vector + hash set、MemTracker、HTTP body/list 上限、日志只记名单数量 |
| HTTP 风险接口进入发布包 | 源文件、include、路由三重编译隔离；默认 OFF；符号/路由字符串检查 |
| 简要方案与首期语义混淆 | 代码注释引用 tenant-only 已确认边界；不读取时间列或字典；测试证明无依赖 |

## 19. 已确认的实现口径

2026-09-04 已完成以下五项编码前口径对齐：

1. **tenant 类型**：首期只支持 nullable/non-nullable `VARCHAR`；`CHAR`、数值 tenant 和复杂类型返回 `NOT_SUPPORTED`。
2. **schema expectation 与 CAS**：外部请求携带 `expected_schema_id + expected_schema_version + tenant_column_unique_id`，不接收 `schema_hash`。BE 固定 coverage 时持有当前 `TabletSchemaCSPtr`，所有输出使用该 Schema；提交前在 header lock 内比较当前 Schema 对象、schema ID/version 及全部 Rowset ID。如需诊断，BE 自行输出实际 `observed_schema_hash`，它不参与正确性校验。
3. **FE 观测版本**：仅作为“副本至少追到此版本”的下界，不作为 coverage 截断上界；有效 `snapshot_end_version` 始终由 BE 在取得互斥后确定。
4. **task ID 与幂等**：首期不引入 `request_digest/predicate_digest`，也不保存 last task identity。正式任务使用 FE 分配的 Agent Task signature 作为 task ID；重试时 task ID 和请求内容不变。Tablet 只保存当前 `owner_task_id`，结束后不保留历史；重复请求依靠正式扫描和 `NOOP_VERIFIED` 收敛。
5. **policy watermark**：固定为 `{dictionary_id, dictionary_txn_id, evaluation_time_epoch_seconds}`。首期 BE 不读取字典，只校验 watermark 格式及不可变请求/admission owner 的任务内一致性；不持久化完成水位，不判断跨任务或跨重启的新旧顺序，调度顺序由 FE 管理。

第四步编码将严格按第 0～6 轮推进，第 7 轮作为第二批增强测试单独排期。后续若需改变上述任一口径，应先修订本计划的数据结构、状态码和对应测试。

## 20. 计划验收与后续动作

本文件已完成计划评审和口径对齐。第四步编码和测试从后续指令开始，执行顺序为：

```text
开始第四步
  -> 第 0 轮契约/fixture
  -> 第 1 轮过滤
  -> 第 2 轮通用 Writer
  -> 第 3 轮 Tablet 原语
  -> 第 4 轮 Engine task
  -> 第 5 轮 HTTP/SQL+HTTP
  -> 第 6 轮第一批收口
  -> 第 7 轮第二批增强
```

每轮结束必须提交：代码差异、已运行测试、未运行测试及原因、已知风险和下一轮入口条件。
