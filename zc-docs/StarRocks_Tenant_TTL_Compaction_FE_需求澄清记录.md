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
- 时间列、Range/List 分区表达式、时区和分区边界证明。
- CREATE/ALTER 绑定时的静态校验、报错、等待和按分区跳过语义。
- 表绑定和策略命中的可观测性。
- FE 可枚举策略快照的按需启用、提交后导出、对象模型、发布和追平语义。

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
5. FE 仍必须根据可信的分区元数据证明 `recordTimestamp` 的分区时间上界；首期支持的 Range/List 分区表达式、粒度、时区和不可证明分区处理按 FE-TIME-001～005 执行。

### FE-TABLE-003：保留 tenant 值的表级边界

状态：已确认

1. 字符串 `default` 是 TTL 策略保留值，业务 tenant 禁止使用该值。
2. 对既有表执行启用 Tenant-TTL 的 ALTER DDL 时，FE 不扫描历史数据，因此不证明历史数据中从未出现 `tenant = 'default'`。
3. 保留值禁用属于建表和写入侧业务约束，Tenant-TTL 启用 DDL 不通过全表扫描强制验证。
4. NULL tenant 不是 `default` 策略行；按既有 BE 边界，NULL tenant 始终保留。

### FE-TIME-001：业务 tenant 和时间列绑定

状态：已确认

1. 首期业务 tenant 列固定按列名 `tenant` 发现，不为 tenant 列增加 `dictionary_ttl` 第四个参数或独立表属性。
2. 启用 DDL 将当前 `tenant` 列的 FE `ColumnId` 和存储层 Column Unique ID 持久化绑定；后续调度按绑定身份使用，不在每轮评估中重新按名称猜测列。
3. `tenant` 必须是 BE 已支持的 nullable 或 non-nullable `VARCHAR`；`CHAR`、数值、二进制、复杂类型及其他字符串归一化形式不作为首期业务 tenant 列。
4. 时间源列固定为 `recordTimestamp BIGINT`，表示 Unix 秒，不支持毫秒、微秒或其他时间单位的隐式推断。
5. FE 从分区表达式解析 `recordTimestamp`，并持久化它的列身份、时间表达式类型、List 时间分量下标及表达式指纹；表达式或列身份后续不匹配时 fail-closed。
6. 上述表布局绑定与 `TenantTtlDictionaryBinding` 分开持久化，不改变已确认的 Dictionary 三参数绑定结构。

### FE-TIME-002：首期 Range 分区范围

状态：已确认

1. 首期支持单时间分量的 `RANGE(recordTimestamp)`。分区边界按 Unix 秒解释，不涉及时区换算。
2. 首期支持现有语法生成的 `RANGE(from_unixtime(recordTimestamp))`；解析器为该表达式生成的内部 `CAST(... AS DATETIME)` 可被识别，用户额外嵌套的 CAST 或其他函数不属于支持范围。
3. 首期不支持多列 Range，不支持 `date_trunc`、`time_slice`、`str2date`、任意嵌套表达式或从其他列推断 TTL 时间。
4. 只要分区元数据能给出有限、可转换的上界，Range 粒度可以是秒、分钟、小时、天或任意不等宽的有限区间；首期不强制固定日分区。
5. FE 使用 Catalog 中的半开区间 `[partition_lower, partition_upper)`，不根据分区名称或预期粒度重建边界。

### FE-TIME-003：首期 List 分区范围与边界证明

状态：已确认

1. 首期支持单表达式或多表达式 List 中直接使用 Unix 秒 `recordTimestamp`。对整数值 `t`，时间集合为 `[t, t + 1)`；`t + 1` 使用精确算术，溢出时该分区无法证明。
2. 首期支持以下单表达式自动 List 分区：

   ```sql
   PARTITION BY from_unixtime(recordTimestamp, '%Y%m%d')
   ```

3. 首期支持包含一个上述时间分量的多表达式自动 List 分区，例如：

   ```sql
   PARTITION BY (
       tenant_bucket,
       from_unixtime(recordTimestamp, '%Y%m%d')
   )
   ```

4. 多表达式 List 必须恰好包含一个受支持的时间分量；其他分量只能是 `tenant_bucket` 等普通列引用。FE 记录时间分量下标，但不使用其他分量作为 tenant 身份或 TTL 正确性条件。
5. `%Y%m%d` 必须是精确的字面量格式；日期值按严格的 8 位数字和真实日历日期解析，不对 `20260230` 等非法值归一化。
6. 日期 `d` 在绑定时区下转换为 `[d.atStartOfDay(zone), d.plusDays(1).atStartOfDay(zone))`。不使用 `lower + 86400` 构造日粒度上界，以正确处理夏令时的 23/25 小时日界。
7. 一个物理分区包含多个 List 值或 tuple 时，FE 为每个时间值生成区间并保留完整区间集；当前过期证明使用所有区间上界的最大值作为 `partition_upper`。日期之间存在空洞时也只会保守地推迟删除，不会误删。
8. 只有 `partition_upper <= evaluation_time - retention_days * 86400` 时，FE 才能认定该 tenant 在整个 List 分区中已过期。

### FE-TIME-004：声明式分区时区

状态：已确认

1. 使用 `from_unixtime` 的 Range/List 表必须显式配置：

   ```sql
   "compaction_retention_time_zone" = "Asia/Shanghai"
   ```

2. 时区是与 `TenantTtlDictionaryBinding` 分离的表级时间语义，不增加 `dictionary_ttl` 函数参数。
3. FE 不从 CREATE/ALTER 语句的会话时区、FE 系统时区或某次写入任务隐式推导该属性；时区由用户显式指定、校验合法性并以规范化 Zone ID 持久化。
4. `CST`、`PRC` 等已知别名可规范化为 `Asia/Shanghai`；区域时区和当前 offset 相同的固定偏移不自动视为同一时区。
5. 若表同时配置 `dynamic_partition.time_zone`，使用 `from_unixtime` 时两者规范化后必须相等。绑定 DDL 发现不相等时拒绝；因旧元数据回放或后续变更导致不一致时，Tenant-TTL fail-closed。
6. 直接使用 Unix 秒 `recordTimestamp` 的 Range/List 时，时区不参与分区边界证明，运行时语义为 `NOT_APPLICABLE`。
7. `compaction_retention_time_zone` 是管理员声明且 FE 信任的分区时间语义，不是写入约束。启用 DDL 不扫描历史数据，也不验证历史写入时区。
8. 首期不修改 INSERT、Stream Load、Broker Load、Routine Load 或其他写入流程，不检查、改写或拒绝写入任务的时区。管理员负责保证历史和后续数据符合所声明时区；若实际写入使用不同时区，FE 无法从现有分区元数据中发现，且可能破坏时间上界证明。
9. 上述声明式语义是首期不增加写入限制的明确边界；FE 不得对其作出“已自动验证数据时区一致”的可观测性承诺。

### FE-TIME-005：无法证明的分区按分区跳过

状态：已确认

1. Range 分区上界为 `MAXVALUE` 时无法证明有限 `partition_upper`，只跳过该分区。
2. Range 的 `MINVALUE` 下界不必然阻止证明：时间列为 `NOT NULL` 且上界有限时，可仅使用上界证明整体过期；时间列可空时，最小值分区可包含 NULL，因此跳过该分区。
3. List `DEFAULT` 分区、时间分量为 NULL、非法 `%Y%m%d`、tuple 元素数不匹配、整数区间溢出，或同一物理分区的任意时间值无法解释时，跳过整个物理分区。
4. 表级分区表达式已通过准入校验后，个别分区的不可证明性不会使整张表的绑定失败；其他具有有限可信上界的分区继续评估。
5. 当前所有分区均无法证明或表尚无分区时，DDL 仍可成功；这表示当前没有可执行分区，不表示 Dictionary 绑定未生效。

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
| `retention_days` | VALUE | 保留天数；正整数表示有效策略，`0` 表示忽略该行 |

约束：

1. 被 Tenant-TTL 绑定的 Dictionary，其策略源表必须是 OLAP Primary Key 表，且 Primary Key 固定为 `(tenant, table_name)`。该准入限制只适用于 Tenant-TTL 绑定，不改变普通 Dictionary 的源表能力。
2. Primary Key 保证策略查询的逻辑结果中 `(tenant, table_name)` 唯一；对同一键重复写入是 Primary Key 表的 UPSERT，不会产生两条可见策略。Builder 仍会执行重复键防御性校验。
3. 三列均不允许 NULL。
4. `retention_days` 的有效 TTL 范围为正 `INT`，即 `[1, 2147483647]`；`0` 是可容忍的忽略值，负数为非法值。
5. `table_name` 不是对当前 StarRocks 物理表的隐式引用，而是可由表属性显式绑定的逻辑策略键。

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
3. 当刷新间隔为 `0` 时，稳态策略表更新后由用户显式执行 `REFRESH DICTIONARY`；新快照成功发布前，绑定表继续使用旧的成功快照。首次绑定、FE 重启重建，以及新 Leader 没有任何可信 Dictionary 成功事务时的启动重建，可以按已确认规则异步发起一次额外刷新；Tenant-TTL 快照导出失败本身不触发新的策略源表查询。
4. 当配置正数刷新间隔时，每次周期触发仍执行现有的全量刷新；即使策略表内容未发生变化，也不跳过该次全量查询、分发与 Cache 重建。
5. Tenant-TTL 首期不增加源表版本检测、不增加“数据未变化则跳过刷新”、不增加增量同步或差量合并能力。
6. 多个业务表绑定同一个 Dictionary 时，Dictionary 每次只执行一次自身的快照刷新，不按绑定业务表数量重复刷新。
7. FE 重启恢复属于单独的可用性触发规则：若被表引用的 Dictionary 尚无可用 FE 策略快照，则不受刷新间隔为 `0` 的限制，立即异步执行一次全量重建；该规则只改变触发时机，不改变现有全量刷新算法。
8. 首次绑定、FE 重启重建和无可信成功事务的新 Leader 启动重建，都是独立的启动可用性规则，不是导出失败的恢复分支。除这些启动规则外，首期不增加由 Tenant-TTL 导出重试驱动的“恢复性完整 Dictionary 刷新”。

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
9. Tenant-TTL 不在同一 `ExecPlan` 中为现有 `DictionaryCacheSink` 增加第二个结果 Sink，不采用 `MultiCast + ResultSink` 的双终点计划。
10. Tenant-TTL 改为在普通 Dictionary Cache 成功提交后，通过独立的只读内部 RPC 从已提交 Cache 导出 FE 候选快照；具体契约见 FE-DICT-010。

### FE-DICT-006：Dictionary 与 Tenant-TTL 的单向故障隔离

状态：已确认

Dictionary 刷新和 Tenant-TTL FE 快照使用单向依赖：

```text
Dictionary 失败 -> 阻止对应 Tenant-TTL 新快照发布
Tenant-TTL 失败 -> 不阻止普通 Dictionary 提交
```

具体语义如下：

1. Dictionary 刷新成功，是发布对应 Tenant-TTL FE 快照的必要条件；Tenant-TTL FE 快照导出、构建或校验成功，不是普通 Dictionary 提交成功的必要条件。
2. 对绑定 Dictionary 的刷新事务 `N`，只有现有 Dictionary Cache 提交成功、FE 候选快照全量导出完整且校验通过时，Tenant-TTL 才能发布 `SnapshotTxnId = N`。
3. Dictionary 事务 `N` 提交失败时，必须丢弃对应 FE 候选快照，不得发布一个未成功提交的 Dictionary 事务水位。
4. Dictionary 事务 `N` 提交成功、但 FE 候选快照失败时：
   - 普通 Dictionary 正常使用事务 `N`，不得因 Tenant-TTL 失败被标记为刷新失败或回滚；
   - 有旧 Tenant-TTL 快照时继续使用旧快照；
   - 从未有过成功 Tenant-TTL 快照时暂停，不下发 Compaction；
   - 不得发布部分采集、部分校验或内容不完整的候选快照。
5. 因此允许普通 Dictionary 的最新成功事务 `N` 暂时领先 Tenant-TTL 当前快照事务 `M`。策略源表或普通 Dictionary 已更新到 `N`，不代表事务 `N` 已对 Tenant-TTL 生效；Tenant-TTL 仍以最后成功发布的 `M` 为有效策略。
6. 后续成功快照可以直接从 `M` 跳到更新事务 `K`，不要求依次补齐中间失败事务。
7. 向 BE 下发任务时，`policy_watermark.dictionary_txn_id` 必须使用 Tenant-TTL 当前实际使用的 `SnapshotTxnId`，不得误用普通 Dictionary 的最新成功事务。
8. `dictionary_ignore_failed_refresh` 仍只控制普通 Dictionary 自身刷新失败时是否回退旧版本。它是布尔开关，不是重试间隔；Tenant-TTL 专属导出错误不得污染 Dictionary 自身的状态和错误信息。

### FE-DICT-007：Tenant-TTL 快照追平与重试

状态：已确认

1. `TenantTtlPolicySnapshotManager` 负责发现 `DictionaryLastSuccessTxnId > SnapshotTxnId` 的版本落后并维护待追平状态。
2. 被 Tenant-TTL 引用的 Dictionary 每次成功提交后，Manager 都以该 Dictionary 的最新 FE 成功事务作为待导出目标。多个成功事务连续到达时合并到最新目标，不要求依次发布每个中间版本。
3. 发生可重试的导出、网络或 FE 系统错误后，Manager 使用独立的指数退避重试导出当前 `DictionaryLastSuccessTxnId`；该重试不依赖 `dictionary_refresh_interval`，也不首先发起新的策略源表查询。
4. 导出请求的期望版本与节点当前版本不等时，本次不返回其他版本数据。Manager 重新读取 FE 的 `DictionaryLastSuccessTxnId` 并以它作为新目标，不将单个 BE/CN 返回的更高版本直接视为已成功事务。
5. 节点已高于期望版本、低于 FE 成功版本或 Cache 不存在时，先尝试其他参与 Dictionary 刷新的节点；仍无法取得精确目标版本时，保留旧快照或维持 `WAITING_POLICY_SNAPSHOT`，然后退避重试。
6. “版本不可取得”只描述 FE 认定的最新成功事务 `N` 已不在当前可选 BE/CN Cache 中，例如 Cache 已被更高版本替换或不存在。该情形不构成可以推断或采纳其他版本的依据。
7. 可重试错误使用倍增指数退避，名义延迟从 `10 s` 开始，按 `10 s, 20 s, 40 s, ...` 增长，上限为 `10 min`，每次加入 `±20%` jitter。只要当前 FE 仍是 Leader、Dictionary 未被删除且仍有 Tenant-TTL 引用，重试就持续进行。
8. 每次新尝试都重新读取 FE 权威的 `DictionaryLastSuccessTxnId`；若已推进到 `K > N`，直接改为导出 `K`，清零旧目标的连续失败计数并将退避重置到起始档。一次导出尝试最多按 FE-DICT-011 顺序尝试 `2` 台节点。
9. 首期不实现“连续 3 次且至少 5 分钟”等版本不可取得阈值，也不实现由导出失败自动触发的完整 Dictionary 刷新或恢复刷新冷却机制。
10. 正常周期刷新或显式 `REFRESH DICTIONARY` 产生更新的成功事务后，它自然成为新导出目标。若 `dictionary_refresh_interval = 0` 且当前成功事务已永久不可取得，则需要用户显式刷新；在此之前继续使用旧快照，或维持 `WAITING_POLICY_SNAPSHOT`。
11. 对确定性策略数据或契约错误，不对同一事务执行退避重试。具体阻塞、修复和可观测语义见 FE-DICT-014。
12. 成功发布 FE 快照后，清除已覆盖目标的待重试状态和失败计数；Dictionary 被解绑、删除或替换后，取消对应 Tenant-TTL 导出重试。

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

每次完整 Dictionary Cache 导出候选创建一个 Builder，负责：

1. 消费从精确 Dictionary 成功事务导出的固定三列 `tenant, table_name, retention_days`。
2. 校验列数、类型、NULL、保留值和 TTL 范围。
3. 先对完整导出结果执行结构、重复键和取值校验，再过滤 `retention_days = 0` 的可容忍行；零值行不进入发布快照。
4. 构建完整候选快照；除零值行外，任意一行非法时拒绝整个候选，不允许发布部分非零策略。
5. Builder 失败只记录 Tenant-TTL 错误，不直接修改普通 Dictionary 状态。

具体 TTL 范围、零值过滤、重复键和错误处理见 FE-DICT-014；快照行数和内存限制见 FE-DICT-011。

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
发布快照只包含正 `INT` 的有效 TTL 条目，不包含已过滤的 `retention_days = 0` 行。

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

1. 某 Dictionary 的 Tenant-TTL 引用数从 `0` 变为 `1` 时，自动异步触发一次完整 Dictionary 刷新；刷新成功提交后按 FE-DICT-010 导出 FE 候选快照。
2. ALTER DDL 不等待刷新完成，仍然只完成轻量元数据修改。
3. Dictionary 自身没有成功事务时，绑定表进入 `WAITING_DICTIONARY`；Dictionary 已有成功事务但没有 FE 策略快照时，进入 `WAITING_POLICY_SNAPSHOT`。
4. Dictionary 当前没有刷新任务时立即登记异步刷新；已有由该次首次绑定触发的刷新正在运行时复用该任务，不重复提交。
5. 正在运行的普通刷新若在首次绑定前启动，则等待其完成后再触发一次用于首次绑定的完整刷新；不在运行中的旧 `ExecPlan` 上动态增加采集分支。
6. 成功发布 FE 快照后，所有使用该 Dictionary 且绑定合法的表转为 `ACTIVE`。
7. 首次异步刷新失败时，按照 FE-DICT-007 的可重试和不可重试规则处理。
8. 该额外完整刷新触发只发生在首次引用、FE 重启重建，或 FE-DICT-012 规定的新 Leader 无可信成功事务的启动重建场景。它不会让未绑定 Dictionary 获得额外刷新行为，也不作为普通导出失败的自动恢复机制。

### FE-DICT-010：提交后精确版本全量导出

状态：已确认

1. 对被 Tenant-TTL 引用的 Dictionary，现有源查询和 `DictionaryCacheSink` 链路保持不变。只有当该轮 Dictionary 在当次参与的 BE/CN 上成功提交、FE 将其记为普通 Dictionary 成功事务后，才异步启动 Tenant-TTL 快照导出。
2. 导出不重新查询策略源表，而是通过新增的只读内部 RPC 从已提交 Dictionary Cache 中取得全量内容，避免第二次源查询读到不同的数据快照。
3. 现有 `DictionaryCacheSink` 将同一份完整结果分发到当次参与刷新的所有 BE/CN；FE 只需从其中一台节点导出，该节点不可用或版本不匹配时可尝试其他节点，不汇总多台节点的部分数据。
4. 当前代码没有可供 FE 调用的 Cache 全量导出 RPC、可枚举接口、`OPEN_EXPORT` 会话或历史版本保留机制。首期不新增长生命周期的版本固定、跨 RPC Session 或历史 Cache 列表。
5. 每次导出请求必须携带 `dictionary_id` 和 `expected_txn_id=N`。BE/CN 只能在当前 Cache 版本精确等于 `N` 时返回数据，响应同时回显 `actual_txn_id=N`；不做最近版本、向前或向后的隐式替换。
6. 首期采用一次逻辑 RPC 返回完整快照。一个响应内部可包含多个压缩的 Tenant-TTL 类型化策略批次，但不将一个候选快照拆成多个独立 `FETCH` RPC 后直接拼接。
7. BE/CN 可在单次 RPC 执行期间获取并持有现有 Cache `shared_ptr`，保证并发替换 Cache 时的内存安全和本次返回内容一致。该局部引用随 RPC 结束释放，不属于跨 RPC 固定版本。
8. BE/CN 导出当前 Cache 的所有 key/value，并按 Dictionary Schema 解码为 `tenant, table_name, retention_days`。首期不直接返回现有 `ChunkPB`，而使用 FE 可直接解析的类型化 Protobuf 行格式；返回顺序不构成契约。
9. 期望事务 `N` 与节点当前版本不等时，BE/CN 返回明确的版本不匹配以及当前版本，不返回其他版本数据。FE 按 FE-DICT-007 丢弃本次候选并追赶最新的 FE 成功事务。
10. 单个 BE/CN 显示的更高 Cache 版本不能单独证明该 Dictionary 刷新已全局成功；追平目标必须以 FE 的 `DictionaryLastSuccessTxnId` 为权威水位。
11. 若导出 `N` 时精确版本校验成功，则即使导出完成时普通 Dictionary 已推进到 `K > N`，完整且校验通过的 `N` 仍可在不造成快照版本倒退、Dictionary ID 仍匹配的前提下发布；发布后继续追平 `K`。
12. 后续允许扩展为无跨 RPC 版本固定的分页导出。第一页确定 `expected_txn_id=N`，后续每一页都必须携带同一个 `N` 并在 BE/CN 精确校验；任意一页发现版本不再是 `N` 时立即失败，FE 丢弃该候选已收到的所有分页，并以最新 FE 成功事务从头重试。具体格式和游标见 FE-DICT-011。
13. 普通 Dictionary 成功状态和 Cache 不因导出 RPC、候选构建或校验失败而回滚；未被 Tenant-TTL 引用的 Dictionary 不发起该导出。

### FE-DICT-011：导出 RPC 线协议、完整性与资源边界

状态：已确认

#### 首期 Protobuf 结构

首期新增专用的 Tenant-TTL 类型化导出格式，逻辑结构如下；编码时可按工程命名规范微调类名和字段号，但不得改变字段语义：

```protobuf
enum PDictionaryCacheExportOutcome {
    EXPORT_OK = 0;
    VERSION_MISMATCH = 1;
    CACHE_NOT_FOUND = 2;
    SCHEMA_MISMATCH = 3;
    LIMIT_EXCEEDED = 4;
    EXPORT_INTERNAL_ERROR = 5;
}

message PExportDictionaryCacheRequest {
    optional int32 protocol_version = 1 [default = 1];
    optional PUniqueId request_id = 2;
    optional int64 dictionary_id = 3;
    optional int64 expected_txn_id = 4;
    optional int64 max_rows = 5;
    optional int64 max_uncompressed_bytes = 6;
    optional int64 max_response_bytes = 7;
}

message PTenantTtlPolicyEntryPB {
    optional bytes tenant = 1;
    optional bytes table_name = 2;
    optional int32 retention_days = 3;
}

message PTenantTtlPolicyBatchPB {
    repeated PTenantTtlPolicyEntryPB entries = 1;
}

message PCompressedTenantTtlPolicyBatchPB {
    optional int32 sequence = 1;
    optional int64 row_count = 2;
    optional CompressionTypePB compression_type = 3;
    optional int64 uncompressed_size = 4;
    optional fixed32 uncompressed_crc32c = 5;
    optional bytes payload = 6;
}

message PExportDictionaryCacheResult {
    optional StatusPB status = 1;
    optional PDictionaryCacheExportOutcome outcome = 2;
    optional int32 protocol_version = 3;
    optional int64 dictionary_id = 4;
    optional int64 expected_txn_id = 5;
    optional int64 actual_txn_id = 6;
    optional int64 total_row_count = 7;
    optional int32 batch_count = 8;
    optional int64 total_uncompressed_bytes = 9;
    optional int64 total_payload_bytes = 10;
    optional fixed32 content_crc32c = 11;
    optional bool complete = 12;
    repeated PCompressedTenantTtlPolicyBatchPB batches = 13;
}
```

1. 现有 `ChunkPB.data` 是 BE `ProtobufChunkSerde` 的私有列式序列化格式，FE Java 当前没有对应解码器；首期不为该功能新建一套 FE Chunk 解码实现。
2. `tenant` 和 `table_name` 使用 Protobuf `bytes`，保留原始字节并避免 Protobuf UTF-8 校验或 Java 字符串归一化影响精确匹配；`retention_days` 使用 `int32`，其中 `0` 会被 Builder 作为可容忍的忽略行过滤。
3. `StatusPB` 表示 RPC 或服务执行状态，`outcome` 表示 FE 可编程分类的导出结果。可预期的版本竞争返回 `status = OK, outcome = VERSION_MISMATCH`，FE 不通过解析错误文本识别它。
4. `EXPORT_OK + total_row_count = 0 + complete = true` 表示一份完整的空策略快照，与 `CACHE_NOT_FOUND` 明确区分。
5. 请求携带的三个资源上限表示 FE 当次接收能力；BE/CN 取请求上限与本地配置上限的较小值，不允许请求放大服务端安全边界。

#### 压缩与完整性

1. BE/CN 将一批类型化策略行先序列化为 `PTenantTtlPolicyBatchPB`，再将字节写入 `PCompressedTenantTtlPolicyBatchPB.payload`。
2. 默认使用 `SNAPPY`；小批次或压缩比不超过 `1.1` 时使用 `NO_COMPRESSION`。首期不使用现有 BE-to-BE `ChunkPB` 路径默认的 ZSTD，避免为 FE 额外增加 ZSTD 解码路径；FE Core 现有 `snappy-java` 可直接复用。
3. 单批次以 `1 MiB` 未压缩字节数为上限。一次逻辑 RPC 可包含多批，但响应整体仍只有全部成功或全部失败两种结果。
4. FE 只有在以下条件全部成立时才能把响应交给 Builder 发布：
   - `status = OK` 且 `outcome = EXPORT_OK`；
   - 协议版本受支持；
   - `dictionary_id` 与当前绑定 ID 一致；
   - `expected_txn_id = actual_txn_id`；
   - `complete = true`；
   - Batch `sequence` 从 `0` 到 `batch_count - 1` 连续且不重复；
   - 每批解压长度等于 `uncompressed_size`，且该批未压缩字节的 CRC32C 一致；
   - 批次行数、未压缩字节数和 Payload 字节数之和分别等于顶层统计；
   - 按 Batch 序号连接的全部未压缩 Payload 的累计 CRC32C 等于 `content_crc32c`；
   - 所有行通过 Builder 的类型、NULL、保留值、重复键和 TTL 范围校验；`retention_days = 0` 按 FE-DICT-014 校验后过滤，不属于校验失败。
5. 除已明确容忍的零值行外，任一完整性或行级校验失败都丢弃整个候选，不得将已解码的部分批次发布。返回顺序仅用于本次响应的序列、字节数和 CRC 校验，不构成策略语义的排序契约。

#### 首期默认资源与超时边界

| 边界 | 首期默认值 |
| --- | ---: |
| 最大策略行数 | `100,000` |
| 单批次未压缩大小 | `1 MiB` |
| 总未压缩 Payload | `64 MiB` |
| 完整 Protobuf 响应 | `64 MiB` |
| FE Builder 估算内存 | `128 MiB` |
| 同一 Dictionary 并发导出 | `1` |
| 单个 FE 全局并发导出 | `2` |
| 单节点导出 RPC 超时 | `60 s`（默认，可动态修改） |
| 一次导出尝试最多节点数 | `2` |
| 一次导出尝试总墙钟时间 | `2 × 单节点超时`（默认 `120 s`） |

1. 上述上限是 Tenant-TTL FE 快照的独立应用级边界，不继承普通 Dictionary 默认 `2 GiB` 内存上限，也不以 BE 全局 `brpc_max_body_size` 作为功能上限。
2. FE 配置 `tenant_ttl_policy_snapshot_export_rpc_timeout_ms` 的默认值为 `60000`，允许运行时动态修改；新值对修改后新发起的单节点 RPC 生效，不改变已在途请求的 deadline。
3. BE/CN 在遍历和构建响应期间对行数、未压缩字节数和最终 Protobuf `ByteSizeLong` 持续校验；任一限制先到即返回 `LIMIT_EXCEEDED`，不携带部分批次。
4. FE 在解压、构建中间 Map 和冻结不可变快照时持续计算估算内存；超限时丢弃整个候选。
5. `LIMIT_EXCEEDED`、`SCHEMA_MISMATCH` 和确定性行校验错误不对同一事务执行退避重试；需要调整配置、缩减/修复策略数据或修正 Dictionary Schema 后，由显式或正常周期刷新产生新事务再恢复；详见 FE-DICT-014。
6. RPC 超时、节点不可达和内部瞬时错误属于可重试系统错误；保留旧快照，下次按 FE-DICT-007 退避重试时可换节点并重新读取 `DictionaryLastSuccessTxnId`。
7. FE 失去 Leader 身份、Dictionary 被删除/替换或最后一个 Tenant-TTL 引用解除后，忽略迟到的导出响应，不发布快照。

#### 后续无 Session 分页扩展

后续分页请求的逻辑结构为：

```protobuf
message PExportDictionaryCachePageRequest {
    optional int32 protocol_version = 1 [default = 1];
    optional PUniqueId request_id = 2;
    optional int64 dictionary_id = 3;
    optional int64 expected_txn_id = 4;
    optional bytes page_token = 5;
    optional int32 max_page_rows = 6;
    optional int64 max_page_bytes = 7;
}
```

1. 首页的 `page_token` 为空；后续 Token 是不透明编码，至少绑定 `dictionary_id`、`expected_txn_id` 和 `next_offset`，不存放 C++ 原生迭代器、内存地址或其他跨 RPC 进程状态。
2. 同一候选的所有分页固定访问同一台 BE/CN，但不在服务端建立 Session。节点切换或重启时，FE 必须在新节点从首页重新导出。
3. 每一页先精确校验当前 Cache 版本等于首页确定的 `expected_txn_id=N`，再在单次 RPC 内局部持有 Cache `shared_ptr` 并导出该页。任意一页版本不匹配时，丢弃该候选全部已收分页。
4. 在同一节点、同一未变事务下，已提交 Cache 不再原地修改；BE/CN 每页从哈希表起点遍历并跳过 `next_offset` 后继续返回，以无状态方式保证该版本内不重不漏。
5. offset 方案会带来随页数增长的重复遍历，只在首期已确认的 `100,000` 行上限内使用。若未来要支持百万级策略，需重新评估有序物化、短期 Session 或版本保留，不继续放大 offset 重复扫描。
6. 若所有分页均通过版本、序号、行数和 CRC 校验，则完整的 `N` 可按 FE-DICT-010 发布；最后一页返回后普通 Dictionary 变为 `K > N` 不会使已完整取得的 `N` 失效，发布后继续追平 `K`。

### FE-DICT-012：Leader/Follower 分工与切主快照恢复

状态：已确认

#### 现有 `partition_retention_condition` 边界

1. Leader 和 Follower 都持有并回放表属性、Dictionary 对象及 Dictionary 运行状态元数据。
2. `DynamicPartitionScheduler` 只在 Leader 上运行。Follower 不求值 `partition_retention_condition`，不发起 `partitions_meta` 内部查询，也不删除过期分区。
3. 新 Leader 上的现有 Partition TTL 调度器会重新扫描表元数据、注册 TTL 表并重新求值；它不从原 Leader 恢复一份 FE 求值快照。
4. 当 `partition_retention_condition` 中的 `dictionary_get()` 需要 BE/CN 执行时，新 Leader 使用本地回放后的 Dictionary 元数据规划内部查询，将 `dictionary_id` 和当前 `lastSuccessTxnId` 写入执行表达式；Dictionary 数据仍由 BE/CN Cache 提供，不枚举回 FE。

#### Tenant-TTL 的 Leader/Follower 分工

1. Tenant-TTL 沿用现有 Partition TTL 的 Leader-only 执行边界：只有 Leader 可以维护可用的 FE 策略快照、求值分区和 tenant 策略并下发 Tenant-TTL Compaction 任务。
2. Follower 只回放持久化的表绑定和 Dictionary 运行状态；首期不预热、导出、构建或保留 `TenantTtlPolicySnapshot`，不承担相应的网络、校验和内存开销。
3. FE 快照是 Leader 本地运行态，不通过 EditLog 在 FE 之间复制。新 Leader 不得继续使用原 Leader 的快照引用或在途导出结果。

#### 新 Leader 接管流程

1. Follower 晋升时先停止 Replayer 并回放到当前最大 Journal ID，然后基于最终可见的表绑定重建 `dictionary_id -> (db_id, table_id)` 引用关系。
2. 引用重建完成前不得开始 Tenant-TTL 调度；重建完成但对应 FE 快照尚未发布时，仍不得下发 Compaction 任务。
3. 新 Leader 上的 `DictionaryLastSuccessTxnId` 指其在 Follower 期间通过 EditLog 回放获得的、已由 FE 确认成功的 Dictionary 事务，不是从某个 BE/CN Cache 反向推断的版本。
4. 未重启的 Follower 若已回放到 `DictionaryLastSuccessTxnId = N > 0`，新 Leader 先按 FE-DICT-010 尝试精确导出事务 `N`；导出期间对应表为 `WAITING_POLICY_SNAPSHOT`，快照发布后转为 `ACTIVE`。
5. 某台 BE/CN 返回高于 `N` 的当前版本，不证明该版本已完成 FE 成功提交。新 Leader 不得直接采纳该版本，仍按 FE-DICT-007 尝试其他参与节点并退避重试；持续无法精确导出不会触发 Tenant-TTL 专属的完整 Dictionary 刷新。
6. 若 BE/CN 已提交事务 `N`，但原 Leader 在写入对应成功 EditLog 前失效，新 Leader 不将 `N` 视为 FE 已确认成功事务。新 Leader 有更旧可信水位 `M > 0` 时只尝试导出 `M`；没有任何可信成功事务时按下一条的重启重建规则执行一次完整刷新。后续正常周期或手动刷新产生 `K` 时，只有 `K` 成功提交、完整导出并发布后才使用 `K`。
7. FE 进程重启后 Dictionary 运行时字段按现有语义重置。FE 重启或 Leader 切换后无可信成功事务时，新 Leader 不根据 BE/CN Cache 版本推断水位，而是按 FE-DICT-004 和 FE-DICT-009 立即异步执行一次完整刷新；对应表在成功事务建立前为 `WAITING_DICTIONARY`。
8. 新 Leader 在任何时刻都以当前本地已发布的 `SnapshotTxnId` 作为 Tenant-TTL 实际策略水位；不得因 `DictionaryLastSuccessTxnId` 已知就越过快照恢复直接下发任务。

### FE-DICT-013：Dictionary 绑定持久化与引用生命周期

状态：已确认

#### 表绑定的持久化结构

1. 这里的“绑定 ID”是表在 DDL 提交时解析到的现有 `dictionary_id`，首期不再引入独立的全局 `binding_id`。
2. `TableProperty` 新增可选的结构化持久化对象 `TenantTtlDictionaryBinding`，逻辑字段为：

   ```text
   TenantTtlDictionaryBinding {
       format_version
       dictionary_id
       dictionary_name
       table_key
       default_days
   }
   ```

3. `compaction_retention_condition` 字符串仍作为用户可见配置并由 `SHOW CREATE TABLE` 展示；结构化绑定作为运行时对象身份和解析参数的依据。
4. DDL 按 `dictionary_name` 解析当前 Dictionary，校验其三列结构后将当前 `dictionary_id`、名称、`table_key` 和 `default_days` 写入绑定对象。
5. 属性字符串和结构化绑定必须作为同一次表元数据修改写入同一条 EditLog，不允许分别提交。具体编码可在现有表属性修改日志中增加可选绑定对象，但不得仅在 FE 内存中保存 `dictionary_id`。
6. Image 加载和 EditLog replay 直接使用持久化的 `dictionary_id`，不重新按名称解析或自动修复。属性字符串与结构化绑定无法解析或不一致时进入 `INVALID`，不得静默换绑。
7. Tenant-TTL 执行时始终按 `dictionary_id` 查找对象；`dictionary_name` 只用于 DDL、`SHOW CREATE TABLE` 和错误诊断。

#### 运行时引用索引与在线 DDL

1. `TenantTtlPolicySnapshotManager` 使用以下 Leader 本地派生索引管理引用：

   ```text
   dictionary_id -> Set<(db_id, table_id)>
   ```

2. 反向索引和引用数不单独持久化。引用数由 Table ID 集合大小推导，`TableProperty.tenantTtlDictionaryBinding` 是绑定身份的持久化事实来源。
3. 使用 `db_id/table_id` 而不是库表名作为引用元素；物理表重命名不改变引用身份或 `table_key`。
4. 启用、更新、禁用 Tenant-TTL，表删除以及表改绑 Dictionary 时，必须在同一次 FE 元数据操作中切换持久化绑定和运行时引用索引。调度器只能看到切换前或切换后的一致视图，不得基于混合的旧绑定和新引用下发任务。
5. EditLog 是持久化提交点。提交后更新 Leader 内存派生索引，DDL 在该更新完成后才返回；如果 FE 在持久化与内存更新之间失效，新 Leader 按 FE-DICT-012 从表元数据重建索引。
6. 索引的增删必须幂等：对集合添加或删除同一 Table ID 不得重复增减引用数。

#### 最后一个引用解除

1. 禁用属性、删除表或改绑其他 Dictionary 使原 `dictionary_id` 的引用集合变为空时，立即停止由 Tenant-TTL 引入的快照导出和追平重试。
2. Manager 摘除该 Dictionary 的当前 FE 快照、候选 Builder、待追平目标和 Tenant-TTL 最近失败信息；不删除 Dictionary 对象，不清理普通 Dictionary Cache，也不改变 Dictionary 自身的周期刷新。
3. 已取得不可变快照引用的短暂读操作可以完成；Manager 摘除引用后，快照在最后一个读引用释放后回收。
4. 在途导出响应返回时必须重新校验 Leader 身份、Manager generation、Dictionary ID 和引用集合；已无引用时直接丢弃，不得重新发布快照。
5. 同一 Dictionary 以后再次从 `0 -> 1` 产生 Tenant-TTL 引用时，重新按 FE-DICT-009 的首次引用规则异步执行完整刷新。

#### Dictionary drop/recreate 与显式重新绑定

1. 首期保持现有 `DROP DICTIONARY` 可执行语义，不新增 `RESTRICT`/`CASCADE`，不因 Tenant-TTL 引用拒绝 drop，也不自动删除或改写业务表属性。
2. Dictionary 被删除时，Manager 按被删除的 `dictionary_id` 立即废弃对应 FE 快照并取消 Tenant-TTL 专属重试。仍绑定该 ID 的表保留原属性和持久化绑定，进入 `PAUSED`，不使用 `default_days`。
3. 同名 Dictionary 重新 create 时获得新 `dictionary_id`。原表继续绑定旧 ID，不按名称自动换绑，仍为 `PAUSED`。
4. `SHOW CREATE TABLE` 继续展示原表达式。`SHOW TENANT TTL STATUS` 的 `DictionaryId` 展示表持久化的绑定 ID；`ErrorMessage` 区分“绑定 ID 已不存在”和“同名 Dictionary 当前为新 ID”。
5. 首期不增加专用 `REBIND` 语法。用户通过再次执行已确认的轻量 ALTER 属性 DDL 显式重新绑定：

   ```sql
   ALTER TABLE business.http_log SET (
       "compaction_retention_condition" =
           "dictionary_ttl('datalake_tenants_ttl_dict', 'business.http_log', 180)"
   );
   ```

6. 即使表达式字符串与原配置相同，DDL 也必须解析当前同名 Dictionary 的 ID。若该 ID 与持久化绑定 ID 不同，本次 ALTER 是有效元数据变更，不得因字符串相同按 no-op 跳过。
7. 重新绑定时先校验新 Dictionary 结构，然后在一次元数据提交中将表从旧 ID 引用集合切换到新 ID。新 ID 引用数从 `0 -> 1` 时按 FE-DICT-009 触发完整刷新；Dictionary 自身无成功事务时进入 `WAITING_DICTIONARY`，已有成功事务但 FE 快照未发布时进入 `WAITING_POLICY_SNAPSHOT`。
8. 为使 drop 的持久化语义与绑定身份一致，新 `DropDictionaryInfo` 在名称之外增加可选 `dictionary_id`，新 EditLog 优先按 ID 回放并校验名称；旧 EditLog 缺少 ID 时仍按现有名称语义兼容回放。

### FE-DICT-014：Builder 校验、零值过滤与错误恢复

状态：已确认

#### TTL 取值与零值过滤

1. 有效的 `retention_days` 和表属性 `default_days` 范围都是正 `INT`，即 `[1, 2147483647]`。`default_days = 0`、负数或超出 `INT` 范围必须在 DDL 阶段拒绝。
2. 策略行的 `retention_days = 0` 是唯一不会使候选快照失效的非正数情形。Builder 完成响应完整性、必填字段和全量键唯一性校验后，忽略该行并继续发布其余有效策略。
3. 被忽略的 tenant 专属行不构成命中，继续回退到 `('default', table_key)` 或属性 `default_days`；被忽略的 `('default', table_key, 0)` 回退到属性 `default_days`。
4. 某个 `table_key` 只有零值行时，有效 FE 快照中视为该键不存在，`TableKeyMatch = NOT_FOUND_USE_DEFAULT`。导出全部由零值行构成时，可以发布一份有效的空策略快照。
5. 零值行不写入失败型 `ErrorMessage`。Builder 必须聚合记录本次忽略的零值行数，并至少通过 WARN 日志或 metrics 之一暴露；`SHOW TENANT TTL STATUS` 是否增加专用字段留待可观测性语法细节确定。

#### 重复键和行级校验

1. Builder 对导出的所有物理行检查 `(tenant bytes, table_name bytes)` 键唯一性，检查发生在零值行过滤之前。
2. 无论重复行的 `retention_days` 是否相同，都拒绝整份候选，稳定错误码为 `TENANT_TTL_POLICY_DUPLICATE_KEY`。不使用首行、末行或最大/最小 TTL 解决冲突。
3. Tenant-TTL 绑定要求策略源表使用 `(tenant, table_name)` Primary Key，用户对同一键的正常重复写入是 UPSERT。因此，普通 Dictionary Cache 已成功提交后 Builder 仍观察到重复键，表示导出、协议或内部数据完整性违约，不是可静默修正的用户策略。
4. 负 `retention_days`、缺少必填字段、NULL、非法键或其他单行契约违反统归为确定性行校验错误，任意一行出现即拒绝整份候选，不跳过该行。除重复键外的行级错误码精确名称留待 `SHOW TENANT TTL STATUS` 错误码细节统一确定。

#### 天数计算与溢出保护

1. `TablePolicy` 仍以 `int` 保存天数，对外语义不改为秒。计算 cutoff 时转换为 `long`，避免 `INT` 在大于约 `24855` 天时乘以 `86400` 就发生溢出。
2. 按当前已确认的固定日长公式计算：

   ```text
   long retentionDays = parsedInt;
   long retentionSeconds = Math.multiplyExact(retentionDays, 86400L);
   long cutoffEpochSeconds = Math.subtractExact(evaluationTimeEpochSeconds, retentionSeconds);
   ```

3. 不对溢出结果做饱和、最大/最小值钳制或绕回。`Math.*Exact` 异常归为确定性算术错误，当次评估 fail-closed，不下发删除任务。具体错误码名称留待可观测性语法细节确定；时间分区类型、时区和边界证明按 FE-TIME-001～005 执行。

#### 错误分类与恢复行为

| 情形 | 分类/错误码 | 当前快照和恢复行为 |
| --- | --- | --- |
| `retention_days = 0` | 可容忍过滤，非错误 | 忽略该行，候选可继续发布；仅记录聚合诊断计数 |
| 重复键 | `TENANT_TTL_POLICY_DUPLICATE_KEY` | 拒绝候选并阻塞同一事务；检查源表 Primary Key 契约和导出完整性，修复后刷新 |
| 负数、NULL、缺失字段或非法键 | 确定性行校验错误 | 拒绝候选并阻塞同一事务；修复策略数据后显式刷新，或等待下一次周期刷新 |
| Schema 或 Primary Key 不符合三列契约 | `SCHEMA_MISMATCH` | DDL 阶段优先拒绝；运行时发现则阻塞同一事务，修复 Dictionary，如 ID 变化则显式重新绑定 |
| 行数、字节数或 Builder 内存超限 | `LIMIT_EXCEEDED` | 阻塞同一事务；缩减策略数据或调整限制后通过新的 Dictionary 刷新事务恢复 |
| 不支持的协议版本 | 确定性协议错误 | 阻塞同一事务；完成兼容升级并刷新后恢复 |
| `VERSION_MISMATCH` 或 `CACHE_NOT_FOUND` | 可重试版本不可取得 | 保留旧快照或等待，按 FE-DICT-007 换节点、重读最新 FE 成功水位并退避重试 |
| RPC 超时、节点不可达、解压/CRC/响应完整性失败或内部瞬时错误 | 可重试系统错误 | 保留旧快照或等待，按 FE-DICT-007 退避重试 |
| cutoff 精确计算溢出 | 确定性算术错误 | 当次表/分区评估 fail-closed，不下发删除任务，不钳制为可删除结果 |

1. 确定性候选错误记录 `blockedSnapshotTxnId = N` 和稳定错误码，不对同一 `N` 执行自动退避重试。已有成功 FE 快照时继续 `ACTIVE` 并使用旧快照；从未成功时维持 `WAITING_POLICY_SNAPSHOT`。
2. 确定性问题修复后，显式 `REFRESH DICTIONARY` 或下一次正常周期刷新产生 `K > N`，Manager 对 `K` 重新导出和构建。若刷新间隔为 `0`，修复后需要用户显式刷新。
3. 确定性错误和可重试错误都只写入 Tenant-TTL 的最近失败信息，复用 `SHOW TENANT TTL STATUS` 的 `ErrorMessage`、`LastSnapshotAttemptTxnId` 和 `LastSnapshotAttemptTime`，不污染普通 Dictionary 的成功状态和 `ErrorMessage`。

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

`default_days` 必须是 `[1, 2147483647]` 范围内的正 `INT`；`0`、负数或超出 `INT` 范围在 DDL 解析阶段拒绝。

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
        "dictionary_ttl('datalake_tenants_ttl_dict', 'business.http_log', 180)",
    "compaction_retention_time_zone" = "Asia/Shanghai"
);
```

该 DDL 的执行边界为：

1. 解析并校验表属性语法。
2. 基于元数据校验目标表是否满足已确定的 Tenant-TTL 准入边界。
3. 校验 Dictionary 对象、`tenant KEY, table_name KEY, retention_days VALUE` 三列结构，以及其策略源表是 Primary Key 固定为 `(tenant, table_name)` 的 OLAP Primary Key 表。
4. 修改并持久化 `TableProperty`，写入 FE EditLog，并更新 FE 内存中的 Tenant-TTL 调度注册信息。
5. 不新增或修改业务列，不修改排序键、分区或分桶。
6. 不创建 Schema Change Job，不复制或改写历史数据，不扫描历史 Rowset。
7. 不在 ALTER DDL 内向 BE 发送 Tenant-TTL 执行 RPC，不等待 Dictionary 刷新，不同步执行 Compaction。
8. DDL 成功只表示属性已配置并通过静态元数据校验；是否已有可用快照、是否命中策略键，必须通过运行时状态查看。
9. DDL 完成后，调度器在后续周期异步评估和执行 Tenant-TTL；这些异步数据操作不属于 ALTER DDL 的同步执行过程。

Dictionary 对象存在且结构合法，但暂无成功快照时，允许 DDL 配置成功。Dictionary 自身尚无成功事务时，表进入 `WAITING_DICTIONARY`；Dictionary 已有成功事务但尚无 FE 策略快照时，表进入 `WAITING_POLICY_SNAPSHOT`。两种状态在可用 FE 快照发布前都不得下发 Tenant-TTL 任务。首次引用该 Dictionary 时，按 FE-DICT-009 自动异步触发一次完整刷新，DDL 本身不等待刷新结果。

### FE-DDL-002：CREATE/ALTER 绑定的静态校验与报错

状态：已确认

校验原则：只有“DDL 当时能够确定，且会使整张表无法安全执行 Tenant-TTL”的静态问题才拒绝 DDL。Dictionary 暂时不可用、策略尚未命中和个别分区无法证明不属于绑定语法或静态准入错误。

#### 触发完整校验的 DDL

1. `CREATE TABLE` 时同时配置 `compaction_retention_condition`。
2. 首次通过 ALTER 绑定 Tenant-TTL。
3. 修改 Dictionary 名称、`table_key`、`default_days` 或 `compaction_retention_time_zone`。
4. Dictionary drop/recreate 后通过相同表达式显式重新绑定。
5. 同一 ALTER 修改多个相关属性时，FE 必须先合并成候选表属性，然后对修改后的完整配置只做一次校验和一次原子提交。
6. 未配置 `compaction_retention_condition` 的普通 CREATE TABLE 不执行 Tenant-TTL 专属准入校验，不因当前表结构不支持 Tenant-TTL 而阻止建表。

#### 表属性和函数参数错误

1. `compaction_retention_condition` 不是受限的 `dictionary_ttl(dict_name, table_key, default_days)` 三参数形式，或包含额外表达式、非字面量参数。
2. `dict_name` 为空，或当前同名 Dictionary 对象不存在。
3. `table_key` 为空、包含首尾空白，或不符合已确认的 `db.table` 逻辑键格式。FE 不校验该逻辑键对应的物理表是否存在。
4. `default_days` 不是整数字面量，或不在 `[1, 2147483647]` 范围内。这与策略表中可被 Builder 忽略的 `retention_days = 0` 不同；属性 `default_days = 0` 必须拒绝。

#### 目标业务表错误

1. 目标不是本地 shared-nothing 的原生 OLAP 基表，例如 shared-data/Lake 表、外表、物化视图或临时表。
2. 目标表键类型不是 `DUP_KEYS`。
3. ALTER 绑定时表不处于可稳定绑定的正常状态，例如正在执行会改变 tenant/时间列身份或分区表达式的 Schema Change。
4. 不存在列名 `tenant`，该列不是 nullable/non-nullable `VARCHAR`，或无法取得有效的 FE Column ID 和存储层 Column Unique ID。
5. 不存在 `recordTimestamp`，该列不是 `BIGINT`，或已解析的时间分区表达式并非引用当前该列。
6. 表未分区，分区类型不是受支持的 Range/List，或表级分区表达式超出 FE-TIME-002/003 的首期范围。
7. Range 使用多列、多时间分量、任意嵌套函数或不支持的时间函数。
8. List 没有受支持的时间分量、包含多个时间分量、使用非精确 `from_unixtime(recordTimestamp, '%Y%m%d')` 的时间函数，或其他分量不是普通列引用。

#### 时区错误

1. 分区表达式使用 `from_unixtime`，但未显式配置 `compaction_retention_time_zone`。
2. `compaction_retention_time_zone` 不是 StarRocks 可解析的合法时区。
3. 使用 `from_unixtime` 且同时存在 `dynamic_partition.time_zone`，但两者规范化后不相等。
4. 上述校验不扩展为写入时区校验。DDL 不检查历史数据或当前写入任务的时区。

#### Dictionary 和策略源结构错误

1. Dictionary 的 KEY 列数、名称或顺序不是 `(tenant, table_name)`，或 VALUE 不是唯一的 `retention_days`。
2. Dictionary 两个 KEY 不是 `VARCHAR`，或 `retention_days` 不是 `INT`。
3. Dictionary 的可查询对象不是 Internal Catalog 中的 OLAP Primary Key 表，或该源表已不存在。
4. 策略源表不是固定三列 `tenant VARCHAR NOT NULL, table_name VARCHAR NOT NULL, retention_days INT NOT NULL`，或 Primary Key 的列和顺序不是 `(tenant, table_name)`。
5. `dictionary_refresh_interval`、`dictionary_ignore_failed_refresh`、warm-up 等 Dictionary 运行属性不是 Tenant-TTL 绑定的额外静态准入条件；仅按现有 Dictionary 自身规则校验。

#### 原子性和 CREATE 两阶段校验

1. 任意静态校验失败时，CREATE/ALTER 整体失败，不写入部分属性、不持久化部分绑定，不改变 Dictionary 引用索引。
2. CREATE TABLE 在 Analyzer 阶段先使用 `ColumnDef` 和分区 AST 校验名称、类型、表达式和属性；最终 Table/Column ID 分配后，再基于 Catalog 对象复核并构建持久化绑定。
3. ALTER 直接基于已有 Catalog Table 和列身份校验。属性原文、`TenantTtlDictionaryBinding`、tenant/时间列身份和时间分区绑定必须在同一次表元数据修改中写入同一条 EditLog。

### FE-DDL-003：不拒绝 DDL 的运行状态与数据内容

状态：已确认

#### 允许绑定成功的 Dictionary/策略情况

1. Dictionary 对象和静态结构合法，但从未成功刷新、正在刷新、最近刷新失败或被取消。
2. 普通 Dictionary 已有成功事务，但 FE 策略快照尚未建立。
3. 快照中不存在当前 `table_key`、不存在 `('default', table_key)`，或当前策略为空。有效快照中查不到适用记录时按 FE-PROP-003 使用属性 `default_days`。
4. 策略表当前包含负数、重复键、零值或其他数据内容问题。DDL 不同步扫描策略内容；导出后由 FE-DICT-014 的 Builder 区分零值过滤和确定性候选失败。

#### 允许绑定成功的业务表情况

1. 当前表还没有分区，或所有现有分区都因 `MAXVALUE`、`DEFAULT`、NULL 或其他 FE-TIME-005 边界而暂时无法证明。
2. `recordTimestamp` 可空；仅可能容纳 NULL 时间值的对应分区按 FE-TIME-005 跳过。
3. `tenant` 可空；NULL tenant 按 BE 契约始终保留。
4. 不扫描或验证历史数据中是否存在业务 `tenant = 'default'`。
5. 不验证历史或后续写入实际使用的时区，不因写入时区与声明值不同而拒绝写入或绑定。
6. 不存在 `tenant_id` 生成列，tenant 不在排序键/Short Key/分桶键中，不存在 tenant bucket，bucket 数不是 64，或 tenant 列 ZoneMap 不可用。这些均不是正确性准入条件。

#### DDL 成功后的初始状态

| 条件 | 绑定结果 |
| --- | --- |
| Dictionary 对象存在且结构合法，但无成功事务 | `WAITING_DICTIONARY` |
| Dictionary 有成功事务，但无可用 FE 策略快照 | `WAITING_POLICY_SNAPSHOT` |
| 存在可用 FE 策略快照 | `ACTIVE` |
| 当前没有任何可证明分区 | 仍可为 `ACTIVE`，但当轮无可执行分区 |

DDL 提交后 Dictionary 被删除、绑定 ID 不再存在、tenant/时间列身份不匹配、表结构不再符合准入条件，或声明时区与动态分区时区不一致时，进入 `PAUSED` 或 `INVALID` 并 fail-closed，不使用 `default_days` 绕过绑定/结构故障。

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
| FE-TIME-001 | 业务列固定为 `tenant VARCHAR` 和 Unix 秒 `recordTimestamp BIGINT`，绑定列 ID/Unique ID 使用 | 已确认 |
| FE-TIME-002 | Range 支持直接 Unix 秒和 `from_unixtime(recordTimestamp)`，任意有限粒度均按 Catalog 半开区间证明 | 已确认 |
| FE-TIME-003 | List 支持直接 Unix 秒及单/多表达式 `from_unixtime(recordTimestamp, '%Y%m%d')`，以所有值的最大上界证明整体过期 | 已确认 |
| FE-TIME-004 | `compaction_retention_time_zone` 是显式、声明式分区语义；不从会话推导，不修改或限制任何写入流程 | 已确认 |
| FE-TIME-005 | `MAXVALUE`/`DEFAULT`/NULL/非法值等无法证明边界按物理分区跳过，不阻止其他分区 | 已确认 |
| FE-DICT-001 | 策略表固定为 `tenant, table_name, retention_days` 三列 | 已确认 |
| FE-DICT-002 | Dictionary 使用 `(tenant, table_name)` 复合 KEY 和 `retention_days` VALUE | 已确认 |
| FE-DICT-003 | 策略表更新后继续使用旧成功快照，新快照成功发布时才原子生效 | 已确认 |
| FE-DICT-004 | 保持 Dictionary 默认刷新间隔和全量刷新语义；首次绑定、FE 重启和无可信水位的 Leader 启动可触发重建，导出失败不驱动源表刷新 | 已确认 |
| FE-DICT-005 | FE 可枚举策略快照仅对被 Tenant-TTL 引用的 Dictionary 按需启用，不改造现有 `ExecPlan` 为双结果 Sink | 已确认 |
| FE-DICT-006 | Dictionary 与 Tenant-TTL 使用单向故障隔离，Tenant-TTL 候选失败不阻止普通 Dictionary 提交 | 已确认 |
| FE-DICT-007 | 可重试导出错误使用 `10 s` 起步、`10 min` 封顶、`±20%` jitter 的退避，每次追赶 FE 最新成功水位，不自动触发恢复性刷新 | 已确认 |
| FE-DICT-008 | 使用 `TenantTtlPolicySnapshotManager`、`Builder`、不可变 `Snapshot` 和 `TablePolicy` 管理 FE 策略 | 已确认 |
| FE-DICT-009 | Dictionary 首次被 Tenant-TTL 引用时自动异步执行一次完整刷新，成功提交后导出 FE 快照 | 已确认 |
| FE-DICT-010 | 不使用 `MultiCast + ResultSink`；成功提交后从单节点精确版本全量导出，首期单次逻辑 RPC，版本不匹配时以 FE 成功水位重试 | 已确认 |
| FE-DICT-011 | 导出使用 FE 可直接解码的类型化 Protobuf + Snappy，校验完整性并独立限制行数、字节数、内存、并发和超时；后续分页使用同节点精确版本 offset Token | 已确认 |
| FE-DICT-012 | Tenant-TTL 只由 Leader 恢复快照和执行调度；新 Leader 优先精确导出已回放的 FE 成功事务，不采纳 BE/CN 单点更高版本 | 已确认 |
| FE-DICT-013 | 表持久化 `TenantTtlDictionaryBinding` 并按 Dictionary ID 绑定；引用索引由表元数据派生，最后引用解除时释放 FE 快照，drop/recreate 后必须显式 ALTER 重新绑定 | 已确认 |
| FE-DICT-014 | 策略源表必须使用 `(tenant, table_name)` Primary Key；TTL 有效范围为正 `INT`，零值行过滤，重复键和其他非法行拒绝候选，并区分确定性阻塞与可重试错误 | 已确认 |
| FE-PROP-001 | 表属性使用 `dictionary_ttl(dict_name, table_key, default_days)` | 已确认 |
| FE-PROP-002 | `table_key` 是可与物理表名不同的 `db.table` 逻辑键，重命名不自动改写 | 已确认 |
| FE-PROP-003 | 使用 tenant 专属、逻辑表 default、属性 default 三级回退，Dictionary 不可用时 fail-closed | 已确认 |
| FE-DDL-001 | 既有表可通过轻量 ALTER 启用，DDL 不执行 Schema Change、数据复制、历史扫描或同步 Compaction | 已确认 |
| FE-DDL-002 | CREATE/绑定 ALTER 对候选完整配置执行表属性、业务表、tenant/时间列、分区、时区和 Dictionary 静态准入校验，失败时整体拒绝 | 已确认 |
| FE-DDL-003 | Dictionary 暂时状态、策略内容和个别分区不可证明不拒绝 DDL，成功后进入对应等待或活跃状态 | 已确认 |
| FE-OBS-001 | 静态配置、策略解析和 Compaction 执行进度分层展示 | 已确认 |
| FE-OBS-002 | 提供表级 Tenant-TTL 状态，明确策略命中类型并区分 Dictionary、FE 快照等待状态 | 已确认 |
| FE-OBS-003 | 支持按 tenant 查看 `EffectiveRetentionDays` 和 `ResolutionType` | 已确认 |
| FE-OBS-004 | 同时展示 Dictionary 最新成功水位、Tenant-TTL 当前快照水位和最近候选构建水位 | 已确认 |

## 9. 待后续澄清项

以下内容尚未完成讨论，不属于已确认实施契约。顺序按当前实现依赖和正确性风险从高到低排列：

1. 单轮评估如何固定 `evaluation_time` 和 `SnapshotTxnId`，策略切换期间如何固定任务输入，以及 FE 任务幂等键与 BE `policy_watermark` 的一致性。
2. FE 如何基于 `TablePolicy`、属性默认值和分区时间边界选择 `DELETE_LIST` / `KEEP_LIST`，并证明所选模式的 tenant 补集完整、NULL tenant 保守保留且不会漏删或误删。
3. 整分区删除、部分 tenant Rowset Rewrite 和全保留 NOOP 的调度分流，包括 Catalog 分区删除与 BE Tenant-TTL 任务之间的边界。
4. 多 Partition、Tablet、Replica 的任务编排、并发限制、重试、取消、Leader 切换和失败收敛。
5. 禁用 `compaction_retention_condition` 的具体 DDL 清空语法，以及禁用或更新绑定时在途评估和已下发任务的取消、完成或结果拒绝语义。持久化绑定和运行时引用索引的切换语义已由 FE-DICT-013 确认。
6. 新增或重启 BE/CN 时的 Dictionary Cache 补齐方式，以及它与 FE 策略快照水位的关系。
7. `SHOW TENANT TTL STATUS` 的权限模型、输出列类型、NULL/零值忽略计数展示、时间分区绑定与可证明分区计数、错误码和完整语法细节。
8. Tenant-TTL 评估历史、最近调度、最近 Compaction 结果和分区级执行进度的查看接口。
