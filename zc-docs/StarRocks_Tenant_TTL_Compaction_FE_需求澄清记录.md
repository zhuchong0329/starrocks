# StarRocks Tenant-TTL Compaction FE 需求澄清记录

> 本文档用于沉淀 Tenant-TTL Compaction FE 侧逐项讨论并经用户确认的需求与 FE/BE 契约。尚未确认的方案不得作为实施依据。BE 已完成的 Tenant-TTL 能力是 FE 设计的既有边界，FE 侧不得擅自改变 BE 语义。

源码基线：StarRocks `4.0.11-zc_docs`，commit `83bca60feb9aedd35a2d01134d5de4ddbe4afd3a`

建立日期：2026-09-10

当前阶段：FE 需求澄清

文档状态：持续追加，本版仅包含已确认结论

## 1. 文档范围与结论优先级

本轮 FE 澄清已阅读并对照以下背景材料：

- `compaction ttl简要方案.md`
- `StarRocks_Tenant_TTL_Compaction_研发澄清与实施记录.md`
- `StarRocks_Tenant_TTL_Compaction_BE_详细编码计划.md`
- `StarRocks_Tenant_TTL_Compaction_代码Review对齐记录.md`
- `be_tenant_ttl_review.md`
- `StarRocks_Rowset_Tenant_TTL_Rewrite_可行性与代码改造方案.md`
- 用户提供的 ClickHouse `retentionDays` 表结构样例

结论优先级如下：

1. 本文档中标记为“已确认”的 FE 需求。
2. 当前代码中已完成的 BE 契约，以 BE 详细编码计划、最新 Review 对齐结论和当前实现为准。
3. 早期方案、早期 Review 或背景材料。若与前两项冲突，不作为当前 FE 实施依据。

本文档当前只覆盖：

- 业务表结构原则。
- 三列 TTL 策略表和 Dictionary 键语义。
- `compaction_retention_condition` 表属性语法与回退顺序。
- 既有表轻量启用 Tenant-TTL 的 DDL 要求。
- 表绑定和策略命中的可观测性。
- FE 可枚举策略快照的按需启用、对象模型、发布和追平语义。

## 2. 已确定的 FE/BE 边界

状态：已确认

原则：FE 计算策略并物化 tenant 名单，BE 只执行精确过滤和原子 Rowset 替换。

### 2.1 FE 负责的语义

1. FE 基于一个固定、成功发布的 Dictionary 快照和一个固定评估时间，解析每个 tenant 的 `retention_days`。
2. FE 基于时间分区的半开区间 `[partition_lower, partition_upper)` 进行整分区范围证明。
3. 只有当以下条件成立时，FE 才能认定该 tenant 在当前分区中整体过期：

   ```text
   partition_upper <= evaluation_time - retention_days(tenant) * 86400
   ```

4. tenant 的 cutoff 落在分区内部时，FE 必须保留该 tenant，不得要求 BE 在分区内按时间部分删除。
5. FE 将策略结果物化为 `DELETE_LIST` 或 `KEEP_LIST`，并向 BE 下发业务 tenant 字符串名单。
6. 整个分区均可删除时，FE 走 Catalog 分区删除路径，不向 BE 下发空 `KEEP_LIST`。

### 2.2 BE 保持不变的执行契约

1. BE 请求的主要边界包含：
   - `protocol_version`
   - `task_id`
   - `tablet_id`
   - `partition_id`
   - `tenant_column_unique_id`
   - `filter { mode, tenants[] }`
   - `policy_watermark { dictionary_id, dictionary_txn_id, evaluation_time_epoch_seconds }`
   - `expected_schema { schema_id, schema_version }`
   - 可选 `fe_observed_max_version`
2. BE 不读取 Dictionary 内容，不接收 `retention_days`、`recordTimestamp`、cutoff 或 `table_key`。
3. BE 不依赖 `tenant_id`、ShortKey、特定排序键或 `tenant_bucket` 完成正确删除。
4. BE 将业务 `VARCHAR` tenant 的精确谓词下推到 Segment iterator：先复用现有 Segment/Page ZoneMap 做无假阴性的候选范围裁剪，再对未裁剪数据做 tenant 精确字节匹配。最终匹配不做大小写、空格、Unicode 或数字形式归一化；ZoneMap 不可用或被关闭时回退为精确 tenant 扫描，不影响正确性。
5. `DELETE_LIST + empty` 在 BE 中可验证为 NOOP，但 FE 不应下发这类无效任务。
6. `KEEP_LIST + empty` 被 BE 视为非法请求。
7. NULL tenant 在 `DELETE_LIST` 和 `KEEP_LIST` 中都保守保留。
8. BE 在 Tablet 内自行固定完整连续 coverage，对多 Rowset 作单次原子替换，不接受 FE 指定的 Rowset 或 Segment 列表。
9. BE 当前目标是 shared-nothing 本地 OLAP 的非 Primary Key `DUP_KEYS` 表；FE 不得向不受支持的表类型下发 Tenant-TTL 任务。
10. BE 支持 nullable 和 non-nullable `VARCHAR` tenant 列，但 nullable 列中的 NULL 按上述契约始终保留。
11. BE 已有边界支持 `NONOVERLAPPING`、`OVERLAPPING`、`OVERLAP_UNKNOWN` Rowset 及标准零行 delete-predicate Rowset；FE 不应增加与当前 BE 能力冲突的额外限制。

## 3. 业务表结构

### FE-TABLE-001：不物化行级 `retentionDays`

状态：已确认

1. Tenant-TTL 业务表不新增 `retentionDays` 物理列、物化列或生成列。
2. 业务表不新增按写入时策略固化的 `expire_at` 列。
3. ClickHouse `retentionDays MATERIALIZED coalesce(...)` 只作为“策略查找优先级”的参考，不在 StarRocks 历史行中物化 TTL 结果。
4. TTL 策略修改后，FE 在后续评估中使用新的成功快照，历史数据不需要因策略变更而重写一个 TTL 列。

### FE-TABLE-002：正确性不依赖 tenant bucket

状态：已确认

1. FE 首期不强制业务表使用“64 tenant bucket × 日分区”的固定结构。
2. `tenant_bucket` 是缩小单次扫描范围、改善并行调度和隔离租户的性能优化，不是 Tenant-TTL 正确性条件。
3. tenant bucket 可以不存在，也不强制为 64 桶。
4. BE 先利用业务 tenant 列已有的 Segment/Page ZoneMap 裁剪扫描范围，再对未裁剪数据做精确 tenant 过滤；不得把 bucket 哈希值当作 tenant 身份或删除判据。
5. FE 仍必须能根据可信的分区元数据证明 `recordTimestamp` 的分区时间上界；首期具体支持哪些分区表达式和时间粒度，尚待后续单独澄清。

### FE-TABLE-003：保留 tenant 值的表级边界

状态：已确认

1. 字符串 `default` 是 TTL 策略保留值，业务 tenant 禁止使用该值。
2. 对既有表执行启用 Tenant-TTL 的 ALTER DDL 时，FE 不扫描历史数据，因此不证明历史数据中从未出现 `tenant = 'default'`。
3. 保留值禁用属于建表和写入侧业务约束，Tenant-TTL 启用 DDL 不通过全表扫描强制验证。
4. NULL tenant 不是 `default` 策略行；按既有 BE 边界，NULL tenant 始终保留。

## 4. 三列 TTL 策略表与 Dictionary

### FE-DICT-001：三列策略表

状态：已确认

策略源表只包含以下三列：

```sql
CREATE TABLE meta.tenant_ttl_policy (
    tenant         VARCHAR(128) NOT NULL,
    table_name     VARCHAR(256) NOT NULL,
    retention_days INT NOT NULL
)
ENGINE = OLAP
PRIMARY KEY (tenant, table_name)
DISTRIBUTED BY HASH(tenant, table_name);
```

字段契约：

| 列 | 字典角色 | 语义 |
| --- | --- | --- |
| `tenant` | KEY | 业务 tenant；`default` 表示指定逻辑表的默认策略 |
| `table_name` | KEY | `db.table` 形式的逻辑表策略键 |
| `retention_days` | VALUE | 保留天数，必须为正整数 |

约束：

1. `(tenant, table_name)` 唯一，使每个逻辑表可以为不同 tenant 配置不同 TTL。
2. 三列均不允许 NULL。
3. `retention_days` 必须大于 0；具体最大值限制尚待后续确定。
4. `table_name` 不是对当前 StarRocks 物理表的隐式引用，而是可由表属性显式绑定的逻辑策略键。

### FE-DICT-002：Dictionary 定义

状态：已确认

Dictionary 使用两个 KEY 和一个 VALUE：

```sql
USE meta;

CREATE DICTIONARY datalake_tenants_ttl_dict
USING tenant_ttl_policy (
    tenant KEY,
    table_name KEY,
    retention_days VALUE
)
PROPERTIES (
    "dictionary_ignore_failed_refresh" = "true"
);
```

Tenant-TTL 只使用一个原子发布的成功 FE 策略快照。`dictionary_ignore_failed_refresh = true` 用于在普通 Dictionary 自身的新一轮刷新失败时保留上一个成功版本；Tenant-TTL 候选快照的失败隔离和回退由 FE-DICT-006 单独定义。从未有可用 FE 策略快照时，Tenant-TTL 暂停。

### FE-DICT-003：策略表更新与成功快照切换

状态：已确认

1. 向策略源表提交 INSERT、UPSERT、UPDATE 或 DELETE 只会改变源表数据，不直接改变当前已生效的 Dictionary 快照。
2. 策略表更新后，Tenant-TTL 继续使用上一个成功快照，直到新一轮 Dictionary 刷新成功、对应 FE 候选快照完成校验并原子发布。
3. 新快照发布之前，表级 `BindingState` 保持 `ACTIVE`，不因策略源表已变更而自动暂停。
4. 新一轮 Dictionary 刷新和 FE 候选快照都成功后，以新的 Dictionary `txn_id` 和生效时间原子切换到新快照；策略的生效时点是 FE 快照成功发布，不是策略表 DML 提交，也不是普通 Dictionary 单独提交成功。
5. 新一轮 Dictionary 刷新失败或 FE 候选快照失败时，继续使用上一个成功的 Tenant-TTL 快照，不将失败候选的部分内容暴露给 Tenant-TTL。
6. 策略表数据事务 ID 与 Dictionary 刷新 `txn_id` 是两个不同水位；BE 请求中的 `dictionary_txn_id` 始终使用已成功发布的 Dictionary 刷新事务 ID。

当前 StarRocks Dictionary 的周期和手动刷新都会执行策略源对象的全量查询，为新 `txn_id` 构建一份完整候选缓存，成功后再替换旧缓存。现有实现不检查源表是否变化，也不支持只传输变更行的增量刷新。

### FE-DICT-004：保持现有刷新间隔与全量刷新语义

状态：已确认

1. Tenant-TTL 不修改 Dictionary 现有的 `dictionary_refresh_interval` 默认值和调度语义。
2. `dictionary_refresh_interval` 默认值为 `0`；DDL 中配置单位为秒。值小于等于 `0` 时不执行周期自动刷新，正数表示周期刷新间隔。
3. 当刷新间隔为 `0` 时，稳态策略表更新后由用户显式执行 `REFRESH DICTIONARY`；新快照成功发布前，绑定表继续使用旧的成功快照。首次绑定、FE 重启重建和 Tenant-TTL 故障追平属于恢复性触发，可以异步发起额外刷新，但不用于常态探测策略源表变化。
4. 当配置正数刷新间隔时，每次周期触发仍执行现有的全量刷新；即使策略表内容未发生变化，也不跳过该次全量查询、分发与 Cache 重建。
5. Tenant-TTL 首期不增加源表版本检测、不增加“数据未变化则跳过刷新”、不增加增量同步或差量合并能力。
6. 多个业务表绑定同一个 Dictionary 时，Dictionary 每次只执行一次自身的快照刷新，不按绑定业务表数量重复刷新。
7. FE 重启恢复属于单独的可用性触发规则：若被表引用的 Dictionary 尚无可用 FE 策略快照，则不受刷新间隔为 `0` 的限制，立即异步执行一次全量重建；该规则只改变触发时机，不改变现有全量刷新算法。

### FE-DICT-005：现有 Dictionary 刷新边界与按需快照

状态：已确认

1. 普通查询规划时，根 Fragment 原本生成 `ResultSink(MYSQL_PROTOCAL)`；当前 Dictionary 刷新随后在 `DictionaryMgr` 中将其替换为已有的 `DictionaryCacheSink`。
2. `DictionaryCacheSink` 是现有 Dictionary 功能，不是 Tenant-TTL 新增实现。它将完整查询结果写入 BE/CN Dictionary Cache；当前刷新执行只等待 Coordinator 完成，不由 FE 枚举查询结果行。
3. 现有 Dictionary Cache 继续服务普通查询和 `partition_retention_condition`。Tenant-TTL 不复用其 `partitions_meta + dictionary_get()` 内部查询路径，也不改变 BE/CN Dictionary Cache 的既有用途。
4. Tenant-TTL 额外维护 FE 可枚举策略快照，用于构建 `tenant -> retention_days` 显式策略集合；它不重新实现或替换 `DictionaryCacheSink`。
5. FE 可枚举策略快照只对至少被一张表的 `compaction_retention_condition` 引用的 Dictionary 按需启用。
6. 未被 Tenant-TTL 引用的 Dictionary 完全保持现有单 `DictionaryCacheSink` 刷新链路，不增加结果回传、Tenant-TTL 策略校验、FE 策略快照以及相应的查询、网络或内存开销。
7. 多张表绑定同一个 Dictionary 时，只构建和发布一份 Dictionary 级 FE 快照。各表基于该快照使用自己的 `table_key` 和 `default_days` 解析策略，不按绑定表数量重复刷新。
8. FE 可以在 `DictionaryMgr` 中增加按引用关系判断的扩展点，但不得无条件改变所有 Dictionary 的执行计划或运行语义。
9. 具体双路消费的 `ExecPlan` 拓扑尚未确定。普通 `MultiCast + ResultSink` 仍处于同一 Coordinator 故障域，在证明满足本节及 FE-DICT-006 的故障隔离契约前，不作为已确定编码方案。

### FE-DICT-006：Dictionary 与 Tenant-TTL 的单向故障隔离

状态：已确认

Dictionary 刷新和 Tenant-TTL FE 快照使用单向依赖：

```text
Dictionary 失败 -> 阻止对应 Tenant-TTL 新快照发布
Tenant-TTL 失败 -> 不阻止普通 Dictionary 提交
```

具体语义如下：

1. Dictionary 刷新成功，是发布对应 Tenant-TTL FE 快照的必要条件；Tenant-TTL FE 快照采集、构建或校验成功，不是普通 Dictionary 提交成功的必要条件。
2. 对绑定 Dictionary 的刷新事务 `N`，只有现有 Dictionary Cache 提交成功、FE 候选快照采集完整且校验通过时，Tenant-TTL 才能发布 `SnapshotTxnId = N`。
3. Dictionary 事务 `N` 提交失败时，必须丢弃对应 FE 候选快照，不得发布一个未成功提交的 Dictionary 事务水位。
4. Dictionary 事务 `N` 提交成功、但 FE 候选快照失败时：
   - 普通 Dictionary 正常使用事务 `N`，不得因 Tenant-TTL 失败被标记为刷新失败或回滚；
   - 有旧 Tenant-TTL 快照时继续使用旧快照；
   - 从未有过成功 Tenant-TTL 快照时暂停，不下发 Compaction；
   - 不得发布部分采集、部分校验或内容不完整的候选快照。
5. 因此允许普通 Dictionary 的最新成功事务 `N` 暂时领先 Tenant-TTL 当前快照事务 `M`。策略源表或普通 Dictionary 已更新到 `N`，不代表事务 `N` 已对 Tenant-TTL 生效；Tenant-TTL 仍以最后成功发布的 `M` 为有效策略。
6. 后续成功快照可以直接从 `M` 跳到更新事务 `K`，不要求依次补齐中间失败事务。
7. 向 BE 下发任务时，`policy_watermark.dictionary_txn_id` 必须使用 Tenant-TTL 当前实际使用的 `SnapshotTxnId`，不得误用普通 Dictionary 的最新成功事务。
8. `dictionary_ignore_failed_refresh` 仍只控制普通 Dictionary 自身刷新失败时是否回退旧版本。它是布尔开关，不是重试间隔；Tenant-TTL 专属采集错误不得污染 Dictionary 自身的状态和错误信息。

### FE-DICT-007：Tenant-TTL 快照追平与重试

状态：已确认

1. `TenantTtlPolicySnapshotManager` 负责发现 `DictionaryLastSuccessTxnId > SnapshotTxnId` 的版本落后并维护待追平状态。
2. 后续任意一次完整 Dictionary 刷新都同时作为一次追平尝试，包括正数 `dictionary_refresh_interval` 触发的周期刷新、用户执行的 `REFRESH DICTIONARY`、首次绑定、FE 重启重建以及 Tenant-TTL 故障恢复刷新。
3. 不能只依赖 `dictionary_refresh_interval`：其默认值为 `0` 时没有周期刷新。发生可重试的 FE 采集或系统错误后，Manager 必须使用独立的指数退避登记并触发新的完整 Dictionary 刷新，即使 `dictionary_refresh_interval = 0` 也要执行恢复重试。
4. 每次恢复重试产生新的 Dictionary `txn_id`，不冒充或重建已经提交但未被 Tenant-TTL 发布的旧事务。
5. 同一 Dictionary 同时最多存在一个刷新任务。周期刷新或手动刷新先发生时，与待追平任务合并，该次刷新直接作为下一次追平尝试。
6. 成功发布 FE 快照后，清除待重试状态和连续失败计数；Dictionary 被解绑、删除或替换后，取消对应恢复重试。
7. 对确定性的策略数据或契约校验错误，不进行高频自动全量刷新。保留旧快照；没有旧快照时进入 `WAITING_POLICY_SNAPSHOT`。修复数据后，由显式 `REFRESH DICTIONARY` 或下一次正常周期刷新重新尝试。
8. 恢复重试只处理已有绑定的 FE 快照故障，不改变稳态下 `dictionary_refresh_interval = 0` 时策略源表更新仍需显式刷新的语义。
9. 指数退避的初始值、最大值和是否提供独立 FE 配置项留到编码计划阶段确定，不改变本节的行为契约。

### FE-DICT-008：FE 可枚举策略快照对象模型

状态：已确认

Tenant-TTL 使用独立组件管理 FE 快照，不将枚举能力塞入普通 `Dictionary` 对象。

#### `TenantTtlPolicySnapshotManager`

Dictionary 级运行时管理器，负责：

1. 维护哪些 Dictionary 当前被 Tenant-TTL 引用。
2. 管理每个 Dictionary 的当前成功快照、候选快照、版本落后和失败状态。
3. 在对应 Dictionary commit 成功后原子发布候选快照。
4. 管理追平重试，并为表级评估和 `SHOW TENANT TTL STATUS` 提供一致状态。
5. FE 重启后根据持久化表绑定重新构建运行时状态。

#### `TenantTtlPolicySnapshotBuilder`

每次绑定 Dictionary 刷新创建一个 Builder，负责：

1. 消费固定三列 `tenant, table_name, retention_days` 的查询结果。
2. 校验列数、类型、NULL、保留值和 TTL 范围。
3. 构建完整候选快照；任意一行非法时拒绝整个候选，不允许发布部分策略。
4. Builder 失败只记录 Tenant-TTL 错误，不直接修改普通 Dictionary 状态。

具体最大 TTL、快照行数和内存限制仍待后续确定。

#### `TenantTtlPolicySnapshot`

Dictionary 级不可变快照，逻辑结构为：

```text
TenantTtlPolicySnapshot
  dictionaryId
  dictionaryName
  dictionaryTxnId
  snapshotTime
  tablePolicies: Map<tableKey, TablePolicy>
```

快照发布后不可原地修改；新版本完整构建成功后由 Manager 原子替换。

#### `TablePolicy`

一个 `table_key` 在 Dictionary 快照中的不可变策略，逻辑结构为：

```text
TablePolicy
  tableKey
  tenantOverrides: Map<tenant, retentionDays>
  tableDefaultDays: Optional<int>
```

1. `tenantOverrides` 不包含保留 tenant `default`。
2. `tableDefaultDays` 来自 `('default', table_key)`。
3. 表属性第三参数 `default_days` 不存入 `TablePolicy`。多个物理表可以绑定同一个 `table_key`，同时配置不同的属性默认值。
4. 最终解析继续使用 `tenantOverrides[tenant] ?? tableDefaultDays ?? binding.defaultDays`。
5. tenant 和 `table_key` 都精确匹配，不做大小写、空格、Unicode 或数字形式归一化。

### FE-DICT-009：首次绑定自动异步刷新

状态：已确认

1. 某 Dictionary 的 Tenant-TTL 引用数从 `0` 变为 `1` 时，自动异步触发一次带 FE 快照采集的完整 Dictionary 刷新。
2. ALTER DDL 不等待刷新完成，仍然只完成轻量元数据修改。
3. Dictionary 自身没有成功事务时，绑定表进入 `WAITING_DICTIONARY`；Dictionary 已有成功事务但没有 FE 策略快照时，进入 `WAITING_POLICY_SNAPSHOT`。
4. Dictionary 当前没有刷新任务时立即登记异步刷新；已有带 Tenant-TTL 采集的刷新正在运行时复用该任务，不重复提交。
5. 正在运行的刷新若在首次绑定前启动且没有 FE 采集分支，则等待其完成后再触发一次带采集的刷新。
6. 成功发布 FE 快照后，所有使用该 Dictionary 且绑定合法的表转为 `ACTIVE`。
7. 首次异步刷新失败时，按照 FE-DICT-007 的可重试和不可重试规则处理。
8. 该特殊触发只发生在首次引用、FE 重启重建或故障追平场景，不会让未绑定 Dictionary 获得额外刷新行为。

## 5. `compaction_retention_condition` 表属性

### FE-PROP-001：三参数 `dictionary_ttl`

状态：已确认

新属性使用三参数形式：

```sql
"compaction_retention_condition" =
    "dictionary_ttl('datalake_tenants_ttl_dict', 'business.http_log', 180)"
```

三个参数依次为：

| 参数 | 语义 |
| --- | --- |
| `dict_name` | 提供 Tenant-TTL 策略快照的 Dictionary 名称 |
| `table_key` | Dictionary `table_name` 列中的逻辑表策略键 |
| `default_days` | 当成功快照中没有适用记录时的最终默认天数 |

`dictionary_ttl(...)` 是 Tenant-TTL 表属性的受限配置语法，不是对业务表逐行执行的通用 SQL 标量函数。

### FE-PROP-002：`table_key` 是显式逻辑绑定

状态：已确认

1. `table_key` 由 `dictionary_ttl` 显式指定，不由 FE 根据当前物理表名隐式推导。
2. `table_key` 使用 `db.table` 形式，但允许与当前 StarRocks 物理表的 `db.table` 不同。
3. FE 只校验 `table_key` 非空并符合约定格式，不校验对应的物理表是否存在，也不强制它等于当前表名。
4. Dictionary 查找按表达式中的 `table_key` 字符串精确匹配。
5. 多个物理表可以显式绑定同一个 `table_key`，共享同一组 TTL 策略。
6. 物理表重命名不会暂停 TTL，不会自动改写 `table_key`，继续使用原逻辑键。
7. 只有用户通过 ALTER 显式修改表达式，才会切换到新的 `table_key`。

例如，物理表 `archive.http_log_v2` 可以继续使用逻辑策略键 `business.http_log`：

```sql
ALTER TABLE archive.http_log_v2 SET (
    "compaction_retention_condition" =
        "dictionary_ttl('datalake_tenants_ttl_dict', 'business.http_log', 180)"
);
```

### FE-PROP-003：三级策略解析与 fail-closed

状态：已确认

在一个可用的成功快照内，某 tenant 的有效 TTL 按以下顺序解析：

```text
dictionary[(tenant, table_key)]
    ?? dictionary[('default', table_key)]
    ?? default_days
```

| 优先级 | 命中内容 | 解析类型 |
| --- | --- | --- |
| 1 | `(tenant, table_key)` | `TENANT_OVERRIDE` |
| 2 | `('default', table_key)` | `TABLE_DEFAULT` |
| 3 | 表达式第三个参数 | `PROPERTY_DEFAULT` |

fail-closed 语义：

1. 字典快照有效，但整个 `table_key` 不存在：使用 `default_days`，Tenant-TTL 仍然生效。
2. `table_key` 存在专属 tenant 记录，但既不存在当前 tenant，也不存在 `default` 记录：当前 tenant 使用 `default_days`。
3. Tenant-TTL 没有可用的成功 FE 策略快照、Dictionary 被删除、绑定 ID 不匹配或快照无法验证：暂停 Tenant-TTL，不得把它当作“未配置表名”而使用 `default_days`。
4. Dictionary 最新刷新失败或 Tenant-TTL 最新候选快照失败，但仍有可用的上一个成功 FE 快照：继续使用上一个成功快照，不视为字典整体不可用。

## 6. 既有表轻量启用 Tenant-TTL

### FE-DDL-001：必须支持 ALTER 表属性启用

状态：已确认

一个创建时没有配置 Tenant-TTL 的既有表，必须可以通过以下 DDL 启用：

```sql
ALTER TABLE business.http_log SET (
    "compaction_retention_condition" =
        "dictionary_ttl('datalake_tenants_ttl_dict', 'business.http_log', 180)"
);
```

该 DDL 的执行边界为：

1. 解析并校验表属性语法。
2. 基于元数据校验目标表是否满足已确定的 Tenant-TTL 准入边界。
3. 校验 Dictionary 对象和 `tenant KEY, table_name KEY, retention_days VALUE` 结构。
4. 修改并持久化 `TableProperty`，写入 FE EditLog，并更新 FE 内存中的 Tenant-TTL 调度注册信息。
5. 不新增或修改业务列，不修改排序键、分区或分桶。
6. 不创建 Schema Change Job，不复制或改写历史数据，不扫描历史 Rowset。
7. 不在 ALTER DDL 内向 BE 发送 Tenant-TTL 执行 RPC，不等待 Dictionary 刷新，不同步执行 Compaction。
8. DDL 成功只表示属性已配置并通过静态元数据校验；是否已有可用快照、是否命中策略键，必须通过运行时状态查看。
9. DDL 完成后，调度器在后续周期异步评估和执行 Tenant-TTL；这些异步数据操作不属于 ALTER DDL 的同步执行过程。

Dictionary 对象存在且结构合法，但暂无成功快照时，允许 DDL 配置成功。Dictionary 自身尚无成功事务时，表进入 `WAITING_DICTIONARY`；Dictionary 已有成功事务但尚无 FE 策略快照时，表进入 `WAITING_POLICY_SNAPSHOT`。两种状态在可用 FE 快照发布前都不得下发 Tenant-TTL 任务。首次引用该 Dictionary 时，按 FE-DICT-009 自动异步触发一次完整刷新，DDL 本身不等待刷新结果。

## 7. 表绑定和策略命中可观测性

### FE-OBS-001：静态配置与运行时生效状态分离

状态：已确认

1. `SHOW CREATE TABLE` 展示 `compaction_retention_condition` 原始配置，用于确认表已绑定哪个 Dictionary、`table_key` 和 `default_days`。
2. 既有 `SHOW DICTIONARY` 展示 Dictionary 自身的刷新状态，但不能单独回答某个表绑定是否可用、`table_key` 是否命中。
3. FE 需要提供专用的 Tenant-TTL 状态查看语句，同时展示静态绑定、成功快照、表键命中和有效策略来源。
4. “属性已配置”、“策略已解析”和“数据已完成 Compaction”是不同层次的状态，不得使用一个模糊的 `effective=true/false` 合并表达。本节只定义前两个层次；Compaction 执行进度后续单独澄清。

### FE-OBS-002：表级绑定状态

状态：已确认

新增状态查看语句：

```sql
SHOW TENANT TTL STATUS FROM business.http_log;
```

表级输出至少包含：

| 字段 | 语义 |
| --- | --- |
| `PhysicalTable` | 当前查看的 StarRocks 物理表 |
| `Enabled` | 是否配置 `compaction_retention_condition` |
| `BindingState` | 当前绑定运行状态 |
| `DictionaryName` | 属性配置的 Dictionary 名称 |
| `DictionaryId` | 当前验证的 Dictionary ID |
| `TableKey` | 属性显式绑定的逻辑表键 |
| `DefaultDays` | 属性中的最终默认天数 |
| `DictionaryLastSuccessTxnId` | 普通 Dictionary 当前最新成功事务 ID |
| `DictionaryLastSuccessTime` | 普通 Dictionary 当前最新成功事务完成时间 |
| `SnapshotTxnId` | 当前使用的成功 Dictionary 快照事务 ID |
| `SnapshotTime` | 当前 Tenant-TTL 成功快照的发布时间 |
| `LastSnapshotAttemptTxnId` | 最近一次 Tenant-TTL 快照构建尝试对应的 Dictionary 事务 ID |
| `LastSnapshotAttemptTime` | 最近一次 Tenant-TTL 快照构建尝试完成时间 |
| `TableKeyMatch` | 当前成功快照中的表键命中类型 |
| `TableDefaultMatch` | 是否存在 `('default', table_key)` 记录 |
| `ErrorMessage` | 暂停、无效或最新刷新失败的原因 |

`BindingState` 枚举：

| 值 | 语义 |
| --- | --- |
| `DISABLED` | 未配置 Tenant-TTL 属性 |
| `WAITING_DICTIONARY` | 属性已配置，但普通 Dictionary 自身尚无成功事务 |
| `WAITING_POLICY_SNAPSHOT` | 普通 Dictionary 已有成功事务，但当前绑定尚无可用的 FE 策略快照 |
| `ACTIVE` | 存在可用成功快照，可以解析有效 TTL |
| `PAUSED` | 因 Dictionary 被删除、绑定 ID 不匹配、表结构不再满足要求等可诊断原因暂停 |
| `INVALID` | 属性或持久化元数据无法解析 |

`TableKeyMatch` 使用三态枚举：

| 值 | 语义 |
| --- | --- |
| `MATCHED` | 成功快照中至少存在一行 `table_name = table_key` |
| `NOT_FOUND_USE_DEFAULT` | 快照可用，但不存在该 `table_key`；使用属性中的 `default_days` |
| `UNKNOWN` | 没有可用快照，无法判断是否命中；Tenant-TTL 保持暂停 |

`TableKeyMatch = NOT_FOUND_USE_DEFAULT` 不表示 Tenant-TTL 未生效。只要 Dictionary 快照可用且其他准入条件成立，该表仍为 `BindingState = ACTIVE`，并使用表达式的 `default_days`。

`TableDefaultMatch` 需要与 `TableKeyMatch` 分开展示，其枚举为 `MATCHED / NOT_FOUND / UNKNOWN`。例如快照仅包含 `(tenant_a, business.http_log)` 时，`TableKeyMatch = MATCHED`，但 `TableDefaultMatch = NOT_FOUND`；`tenant_a` 使用专属策略，其他未配置 tenant 使用属性默认值。没有可用快照时，`TableDefaultMatch = UNKNOWN`。

当 Dictionary 最新刷新失败或 Tenant-TTL 最新候选快照失败、但上一个成功的 FE 快照仍可用时：

- `BindingState` 保持 `ACTIVE`。
- 命中类型基于上一个成功快照计算。
- `SnapshotTxnId` 和 `SnapshotTime` 保持上一个成功快照的值，不得显示普通 Dictionary 已领先的事务。
- `ErrorMessage` 显示最新失败原因；Tenant-TTL 最近失败信息复用该字段，不增加独立的 `SnapshotError`。

普通 Dictionary 和 Tenant-TTL 快照允许暂时出现版本分叉。`DictionaryLastSuccessTxnId` 展示普通 Dictionary 最新成功事务，`SnapshotTxnId` 展示 Tenant-TTL 实际使用的事务，`LastSnapshotAttemptTxnId` 展示最近一次候选构建所对应的事务，三者不得互相替代。普通 Dictionary 已有成功事务、但 FE 快照从未成功时，`BindingState = WAITING_POLICY_SNAPSHOT`，`SnapshotTxnId` 不得填写普通 Dictionary 的最新事务。

### FE-OBS-003：按 tenant 解析最终策略

状态：已确认

同一状态语句支持可选 tenant 参数：

```sql
SHOW TENANT TTL STATUS
FROM business.http_log
FOR TENANT 'tenant_a';
```

除表级字段外，返回：

| 字段 | 语义 |
| --- | --- |
| `Tenant` | 本次解析的 tenant |
| `EffectiveRetentionDays` | 在当前成功快照下解析得到的最终保留天数 |
| `ResolutionType` | 最终策略来源 |

`ResolutionType` 枚举：

| 值 | 语义 |
| --- | --- |
| `TENANT_OVERRIDE` | 命中 `(tenant, table_key)` |
| `TABLE_DEFAULT` | 未命中 tenant，命中 `('default', table_key)` |
| `PROPERTY_DEFAULT` | 未命中适用的 Dictionary 记录，使用表达式的 `default_days` |
| `UNRESOLVED` | 没有可用成功快照，无法计算有效 TTL |

### FE-OBS-004：Dictionary 与 Tenant-TTL 双水位展示

状态：已确认

1. `SHOW TENANT TTL STATUS` 同时展示普通 Dictionary 最新成功水位、Tenant-TTL 当前有效快照水位和最近一次候选构建水位。
2. 当 `DictionaryLastSuccessTxnId > SnapshotTxnId` 且旧 FE 快照仍可用时，绑定保持 `ACTIVE`；该差值表示 Tenant-TTL 正在使用旧的有效策略并等待追平，不表示 Dictionary 不可用。
3. 当普通 Dictionary 已有成功水位、但 FE 快照从未成功时，使用 `WAITING_POLICY_SNAPSHOT`，与 Dictionary 自身尚未成功的 `WAITING_DICTIONARY` 明确区分。
4. 最近一次候选构建失败的原因复用 `ErrorMessage`；`LastSnapshotAttemptTxnId` 和 `LastSnapshotAttemptTime` 用于定位该错误对应的尝试。
5. FE 重启后上述运行时水位按既有重建语义恢复：重建完成前进入相应等待状态，成功发布新 FE 快照后恢复正常展示。

## 8. 已确认结论清单

| 编号 | 结论 | 状态 |
| --- | --- | --- |
| FE-TABLE-001 | 业务表不新增或物化 `retentionDays` | 已确认 |
| FE-TABLE-002 | 首期不强制 64 tenant bucket × 日分区，tenant bucket 只是性能优化 | 已确认 |
| FE-TABLE-003 | `default` 为 TTL 策略保留 tenant，启用 DDL 不扫描历史数据验证 | 已确认 |
| FE-DICT-001 | 策略表固定为 `tenant, table_name, retention_days` 三列 | 已确认 |
| FE-DICT-002 | Dictionary 使用 `(tenant, table_name)` 复合 KEY 和 `retention_days` VALUE | 已确认 |
| FE-DICT-003 | 策略表更新后继续使用旧成功快照，新快照成功发布时才原子生效 | 已确认 |
| FE-DICT-004 | 保持 Dictionary 默认刷新间隔和全量刷新语义；首次绑定、重启和故障追平是恢复性触发 | 已确认 |
| FE-DICT-005 | FE 可枚举策略快照仅对被 Tenant-TTL 引用的 Dictionary 按需启用，未绑定 Dictionary 完全保持现有链路 | 已确认 |
| FE-DICT-006 | Dictionary 与 Tenant-TTL 使用单向故障隔离，Tenant-TTL 候选失败不阻止普通 Dictionary 提交 | 已确认 |
| FE-DICT-007 | 版本落后由 SnapshotManager 通过正常刷新和独立退避恢复刷新追平，确定性数据错误等待修复后再刷新 | 已确认 |
| FE-DICT-008 | 使用 `TenantTtlPolicySnapshotManager`、`Builder`、不可变 `Snapshot` 和 `TablePolicy` 管理 FE 策略 | 已确认 |
| FE-DICT-009 | Dictionary 首次被 Tenant-TTL 引用时自动异步执行一次带 FE 快照采集的完整刷新 | 已确认 |
| FE-PROP-001 | 表属性使用 `dictionary_ttl(dict_name, table_key, default_days)` | 已确认 |
| FE-PROP-002 | `table_key` 是可与物理表名不同的 `db.table` 逻辑键，重命名不自动改写 | 已确认 |
| FE-PROP-003 | 使用 tenant 专属、逻辑表 default、属性 default 三级回退，Dictionary 不可用时 fail-closed | 已确认 |
| FE-DDL-001 | 既有表可通过轻量 ALTER 启用，DDL 不执行 Schema Change、数据复制、历史扫描或同步 Compaction | 已确认 |
| FE-OBS-001 | 静态配置、策略解析和 Compaction 执行进度分层展示 | 已确认 |
| FE-OBS-002 | 提供表级 Tenant-TTL 状态，明确策略命中类型并区分 Dictionary、FE 快照等待状态 | 已确认 |
| FE-OBS-003 | 支持按 tenant 查看 `EffectiveRetentionDays` 和 `ResolutionType` | 已确认 |
| FE-OBS-004 | 同时展示 Dictionary 最新成功水位、Tenant-TTL 当前快照水位和最近候选构建水位 | 已确认 |

## 9. 待后续澄清项

以下内容尚未完成讨论，不属于已确认实施契约。顺序按当前实现依赖和正确性风险从高到低排列：

1. 同一次 Dictionary 源查询如何形成现有 `DictionaryCacheSink` 与 FE Builder 的双路消费 `ExecPlan`，并在共享 Coordinator 下证明单向故障隔离、完整排空、完成判定和事务提交顺序；普通 `MultiCast + ResultSink` 尚未被确认为最终实现。
2. Dictionary 引用关系和绑定 ID 的持久化结构、FE 启动/Leader 切换时的引用重建、最后一个引用解除后的快照释放，以及 Dictionary drop/recreate 后的暂停和重新绑定 DDL。
3. `TenantTtlPolicySnapshotBuilder` 的完整校验和资源边界：`retention_days` 最大值与溢出保护、重复键、最大行数、最大内存、错误分类，以及恢复重试退避参数。
4. FE 首期具体支持的时间分区列、分区表达式、粒度、时区、边界证明和 `MAXVALUE` 处理。
5. 单轮评估如何固定 `evaluation_time` 和 `SnapshotTxnId`，策略切换期间如何固定任务输入，以及 FE 任务幂等键与 BE `policy_watermark` 的一致性。
6. FE 如何基于 `TablePolicy`、属性默认值和分区时间边界选择 `DELETE_LIST` / `KEEP_LIST`，并证明所选模式的 tenant 补集完整、NULL tenant 保守保留且不会漏删或误删。
7. 整分区删除、部分 tenant Rowset Rewrite 和全保留 NOOP 的调度分流，包括 Catalog 分区删除与 BE Tenant-TTL 任务之间的边界。
8. 多 Partition、Tablet、Replica 的任务编排、并发限制、重试、取消、Leader 切换和失败收敛。
9. 禁用或更新 `compaction_retention_condition` 的 DDL 语法、引用计数切换、在途评估/任务处理和原子生效语义。
10. 新增或重启 BE/CN 时的 Dictionary Cache 补齐方式，以及它与 FE 策略快照水位的关系。
11. `SHOW TENANT TTL STATUS` 的权限模型、输出列类型、NULL/零值展示、错误码和完整语法细节。
12. Tenant-TTL 评估历史、最近调度、最近 Compaction 结果和分区级执行进度的查看接口。
