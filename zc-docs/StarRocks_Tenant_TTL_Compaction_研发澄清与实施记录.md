# StarRocks Tenant-TTL Compaction 研发澄清与实施记录

> 本文档用于沉淀 Tenant-TTL Compaction 从需求澄清、测试用例对齐、编码计划到编码验证的确定性结论，便于后续迁移上下文。可行性分析与总体方案仍以 `StarRocks_Rowset_Tenant_TTL_Rewrite_可行性与代码改造方案.md` 为背景材料；两者冲突时，以本文档中标记为“已确认”的研发结论为准。

源码基线：StarRocks main，commit `9559176fab6e2cb885779f1e7b680133d58d6972`  
建立日期：2026-09-03  
当前阶段：第四步——编码和测试（进行中）
文档状态：持续更新

## 1. 研发阶段

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| 第一步 | 需求澄清 | 已完成 |
| 第二步 | 测试用例对齐 | 已完成 |
| 第三步 | 编码计划文档 | 已完成 |
| 第四步 | 编码和测试 | 进行中 |

Tenant-TTL Compaction 的需求、技术语义、测试边界和详细编码计划已经完成对齐。2026-09-04 开始第四步，按《StarRocks_Tenant_TTL_Compaction_BE_详细编码计划.md》的第 0～4 轮连续实施；每轮完成对应测试后独立提交并推送。第 5 轮 HTTP 手工入口和第 6～7 轮收口/增强测试不在本次连续编码范围内。

当前实施进度：第 0 轮“契约、类型和测试基础设施”准备开始，尚未修改 BE 代码。

## 2. 需求澄清记录

### REQ-6.1-001：首期采用 FE 物化的 tenant-only 过滤谓词

状态：已确认  
确认日期：2026-09-04  
来源：总体方案第 6.1 节  
主题：BE 准入、`recordTimestamp` 职责、字典快照与 tenant 名单表达

#### 问题

FE 下发的 Tenant-TTL Compaction 只携带业务 tenant 过滤条件。`tenant_id` 生成列及 `(tenant_id, recordTimestamp)` 排序前缀是否仍是 BE 准入条件？如果 FE 已经依据字典策略和时间分区证明某些 tenant 在该分区内应当全部删除，BE 是否还需要读取 `recordTimestamp` 并重新计算 cutoff？当待删除 tenant 很多、待保留 tenant 很少时，如何避免下发过大的删除名单？

#### 已确认结论

1. 首期 Tenant-TTL 不检查 `tenant_id` 是否存在、是否为 `CAST(tenant AS BIGINT)`，也不检查排序前缀是否为 `(tenant_id, recordTimestamp)`；不实现基于这些条件选择 ShortKey/ZoneMap 快速路径的逻辑。
2. FE 在固定字典快照和评估时间下，依据时间分区的半开范围证明每个目标 tenant 在该分区内已经整体过期。BE 不读取 `recordTimestamp`，也不重新计算逐行 cutoff。
3. FE 把策略结果物化成业务 tenant 名单。请求使用互斥的 `DELETE_LIST` 或 `KEEP_LIST` 模式，BE 只读取业务 `tenant` 列生成 `keep_row_ranges`。
4. BE 不通过 `DictionaryCacheManager` 重新取得或执行字典内容。`dictionary_id/dictionary_version/evaluation_time` 作为 policy watermark 随请求携带，用于幂等、审计和新旧任务顺序控制。
5. 缺少 `tenant_id`、指定排序键、ShortKey 或定制 Segment 粗判不影响任务合法性；但业务 `tenant` 列不存在、类型无法执行精确比较或 coverage 中的 Rowset 无法读取该列时，BE 无法生成可信过滤结果，必须失败且不提交，不能通过盲目全量 Rewrite 冒充 TTL 成功。

#### FE 的分区级正确性证明

设当前时间分区为半开区间：

```text
partition_time_range = [partition_lower, partition_upper)
cutoff(tenant) = evaluation_time - retention_days(tenant)
```

只有满足以下条件的 tenant，才可以被 FE 认定为在当前分区内整体可删除：

```text
partition_upper <= cutoff(tenant)
```

此时分区内该 tenant 的每一行都满足 `recordTimestamp < cutoff(tenant)`。BE 因而只需判断业务 tenant 是否属于 FE 物化的集合，不需要读取时间列。

这里的“分区部分保留”是不同 tenant 之间的差异：一部分 tenant 在整个分区内均已过期，另一部分 tenant 仍需保留。若某 tenant 的 cutoff 落在分区内部，即 `partition_lower < cutoff(tenant) < partition_upper`，首期不得对该 tenant 做分区内部分时间删除；FE 应将其视为仍需保留，等待整个分区均过期。后续如要处理该场景，再扩展 `tenant + recordTimestamp` 联合谓词，不属于首期范围。

#### DELETE_LIST / KEEP_LIST 请求语义

推荐只使用一个名单字段，并由枚举表达极性，从结构上避免两个字段同时赋值：

```text
enum TenantFilterMode {
    DELETE_LIST,
    KEEP_LIST
}

TenantFilter {
    mode
    tenants[]
}
```

两种模式语义为：

```text
DELETE_LIST:
    tenant in tenants  -> DROP
    other tenant       -> KEEP

KEEP_LIST:
    tenant in tenants  -> KEEP
    other non-NULL tenant -> DROP
    NULL tenant        -> KEEP
```

FE 根据名单大小选择模式：待删除 tenant 较少时使用 `DELETE_LIST`；待删除 tenant 很多但待保留 tenant 较少时使用 `KEEP_LIST`。首期请求正式采用 `TenantFilterMode + tenants`，不定义可以同时出现的 `delete_tenants`、`keep_tenants` 两个字段；缺少 mode、mode 非法或名单字段缺失均返回 `INVALID_ARGUMENT`。

`KEEP_LIST` 是补集删除，FE 必须把所有尚未证明可以整分区删除的 tenant 都放入保留名单，包括 cutoff 落在分区内部、策略无法确定以及默认 TTL 尚未到期的 tenant。`DELETE_LIST` 漏列只会延迟回收；`KEEP_LIST` 漏列会误删，因此后者需要更严格的完整性证明。

首期空名单按以下规则处理：

- `DELETE_LIST + empty` 等价于 NOOP，FE 不应下发任务。
- `KEEP_LIST + empty` 表示分区全部删除，应由 FE 走整分区删除路径，不应下发 Rowset Rewrite。
- DDL 中业务 `tenant` 可为 NULL；首期 NULL 不匹配字符串名单，并在两种模式下都保守 KEEP，除非后续单独定义并证明 NULL tenant 的删除策略。

#### BE 请求与校验边界

建议请求至少包含：

```text
task_id
tablet_id
partition_id
tenant_column_unique_id
TenantFilter { mode, normalized_tenants }
policy_watermark { dictionary_id, dictionary_version, evaluation_time }
schema_fingerprint
expected version / coverage planning information
predicate_digest
```

FE 对名单排序、去重，并将 `filter_mode` 纳入 digest：

```text
predicate_digest = hash(
    partition_id,
    tenant_column_unique_id,
    filter_mode,
    sorted_deduplicated_tenants,
    policy_watermark
)
```

BE 只校验 Tablet/partition、业务 tenant column unique id 与类型、名单模式和 digest、policy watermark、coverage 及提交前 Rowset 身份。相同 `task_id + digest` 是同一任务重试；相同 task id 但 digest 不同必须拒绝。BE 不需要接收 `recordTimestamp` column unique id、cutoff 或字典内容。

#### 对第二步测试对齐的输入

后续测试至少覆盖：`DELETE_LIST` 少量删除、`KEEP_LIST` 少量保留、两种模式得到互补且正确的 `keep_row_ranges`；mode 缺失/非法或名单字段缺失时拒绝；模式进入 predicate digest；相同 task id 不同 digest 拒绝；名单排序和去重不改变 digest；DELETE 空名单不下发/NOOP、KEEP 空名单走分区删除；NULL tenant 在两种模式下均保留；cutoff 等于 partition upper 时允许整 tenant 删除、cutoff 落在分区内部时 FE 不把该 tenant 放入删除补集；BE 不读取 `recordTimestamp`、不访问 DictionaryCache、没有 `tenant_id` 或指定排序键时结果不变；业务 tenant 列缺失或不可读时整任务失败且 TabletMeta 不修改。

### REQ-7.1-001：首期 tenant 精确预扫描后的 KEEP/DROP/REWRITE

状态：已确认  
确认日期：2026-09-04  
来源：总体方案第 7.1 节  
主题：物化 tenant 名单到 SegmentFilterPlan 的决策

#### 问题

首期不实现 `tenant_id`、ShortKey、排序前缀或定制 ZoneMap 粗判的优化选择逻辑后，如何为每个 Segment 生成 KEEP/DROP/REWRITE 计划？

#### 建议结论

首期对 coverage 中每个 Segment 精确扫描业务 `tenant` 列，根据 FE 已物化的 `TenantFilter { mode, tenants }` 生成源 row ordinal 的 `keep_row_ranges`。不读取 `recordTimestamp`、不查询 tenant 字典、不检查 `tenant_id` 和排序键，也不建立新的 ShortKey/ZoneMap 快速路径选择分支。

建议流程为：

```text
keep_ranges = scan_business_tenant_column(segment, tenant_filter)

if keep_ranges.span_size() == segment.num_rows:
    KEEP by hard link
else if keep_ranges.empty():
    DROP by omitting the segment
else:
    REWRITE vertically with keep_ranges
```

精确扫描并不意味着一定需要物理重写。扫描业务 tenant 后仍有三种最终结果：

- 所有行都保留：`KeepRowRanges` 覆盖 `[0, num_rows)`，最终 KEEP 并 hard link。
- 所有行都被 FE 物化名单选中删除：`KeepRowRanges` 为空，最终 DROP。
- 同时存在名单语义下的保留行和删除行：最终 REWRITE，并把相同 `KeepRowRanges` 重放到所有 Vertical 列组。

#### 研发约束

1. Tenant-TTL 行过滤器只消费已物化的 `TenantFilter`，不解析字典或计算时间 cutoff。
2. 每个 Segment 只扫描一次业务 tenant 列，并输出源 row ordinal 的 `SparseRange`。
3. `DELETE_LIST` 与 `KEEP_LIST` 的极性必须在生成 `keep_row_ranges` 时统一转换；`FilteredRowsetWriter` 永远只消费“保留哪些行”。
4. 只有 `0 < keep_row_ranges.span_size() < num_rows` 才创建 `VerticalSegmentRewriter`。
5. 首期不新增 tenant_id/ShortKey/排序键/定制 ZoneMap 优化选择逻辑。底层通用 reader 若透明使用已有索引，不得改变最终结果或成为资格门禁。

#### 对第二步测试对齐的输入

后续测试至少覆盖：DELETE_LIST 精扫后全 KEEP、全 DROP、部分保留；KEEP_LIST 精扫后全 KEEP、全 DROP、部分保留；NULL tenant 保留；两种模式的 `keep_row_ranges` 极性正确；扫描过程不读取 `recordTimestamp`、`tenant_id` 或字典；精扫后全范围/空集不进入 Vertical writer，只有部分范围进入 REWRITE。

### REQ-7.1-002：FilteredRowsetWriter、SparseRange 与逐 Segment 调用顺序

状态：已确认  
确认日期：2026-09-03  
来源：总体方案第 7.2 节处理算法后的说明  
主题：术语、职责边界和调用过程

#### 问题

“mixed writer”具体是什么？`SparseRange` 是什么？“完整 Segment”“所有列组和 footer”“连续 dst ordinal”“0/1 映射”分别对应什么行为，能否把函数调用过程描述得更清晰？

#### 术语澄清

##### mixed writer

“mixed writer”不是当前 StarRocks 中已有的类名，而是总体方案早期对拟新增 `FilteredRowsetWriter` 的非正式简称。“mixed”只表示一个目标 Rowset 可以混合使用三种源 Segment 处理结果：

- KEEP：把已经完整存在的源 Segment 文件 hard link 成新的目标文件名。
- DROP：不为该源 Segment 创建目标文件。
- REWRITE：过滤部分行，重新生成一个完整的目标 Segment。

它不表示把 hard link 文件插入正在写某个列组的 `SegmentWriter`，也不表示在一个 Segment 文件内混合链接列和重写列。为避免误解，后续文档不再单独使用“mixed writer”，统一写成：

> `FilteredRowsetWriter`：与具体过滤策略无关的目标 Rowset 组装器。

该组件负责目标 Segment 编号、文件登记、逐 Segment 统计、加密元数据和最终 RowsetMeta 汇总；具体的多列组写入由 `VerticalSegmentRewriter` 完成。

`FilteredRowsetWriter` 和 `VerticalSegmentRewriter` 均为方案拟新增组件，当前源码中不存在。它们复用的 `SegmentWriter`、`SparseRange`、`SegmentReadOptions` 和 `Rowset::verify` 是现有能力。

##### SparseRange

`SparseRange<rowid_t>` 是当前源码 `be/src/storage/range.h` 中已有的数据结构，用一组互不重叠的半开区间 `[begin, end)` 表示离散 row ordinal 集合。这里的 row ordinal 是行在单个源 Segment 内从 0 开始的位置，不是主键值，也不是跨 Segment 的全局 rowid。

例如一个源 Segment 有 10 行，TTL 判断后保留第 `0、1、2、5、6、9` 行，则：

```text
KeepRowRanges = {[0, 3), [5, 7), [9, 10)}
KeepRowRanges.span_size() = 6
```

- `empty()`：没有任何保留行，最终 DROP。
- `span_size() == segment.num_rows()`：全部行保留，最终 KEEP。
- 其他情况：仅部分行保留，进入 REWRITE。
- `SegmentReadOptions::rowid_range_option = KeepRowRanges`：把保留区间交给 Segment reader。
- `SegmentIterator::_get_row_ranges_by_rowid_range()`：先建立 `[0, num_rows)`，再与传入的 `SparseRange` 求交，只读取这些 row ordinal。

同一个 `KeepRowRanges` 被每个列组重复使用，因此第一个列组读取第 `0、1、2、5、6、9` 行，后续每个 value 列组也读取完全相同的行号和行数。各列最终仍能在目标 Segment 内按行对齐。

#### 建议的职责边界

| 组件/接口 | 是否现有 | 职责 |
| --- | --- | --- |
| `FilteredRowsetWriter` | 拟新增 | 按源顺序组装目标 Rowset，分配连续目标 Segment ordinal，登记完整 Segment 结果 |
| `FilteredRowsetWriter::add_linked_segment` | 拟新增 | hard link 一个完整 KEEP Segment 及外部索引，并登记源/目标 ordinal 和统计 |
| `FilteredRowsetWriter::rewrite_segment_vertically` | 拟新增 | 把单个源 Segment、`KeepRowRanges` 和目标 ordinal 委托给 `VerticalSegmentRewriter` |
| `VerticalSegmentRewriter` | 拟新增 | 为一个源 Segment 创建一个目标 `SegmentWriter`，按列组读取相同 row ordinal，完成该 Segment 后返回结果 |
| `SegmentWriter` | 现有 | 写一个 Segment 文件；支持按列组 `init`、`append_chunk`、`finalize_columns`，最后 `finalize_footer` |
| `SparseRange` | 现有 | 表示单个源 Segment 内要保留的 row ordinal 集合 |
| `Rowset::verify` | 现有 | 输出 Rowset 建成后检查排序；它是最终防线，不验证 TTL 策略本身是否正确 |

#### 单个 REWRITE Segment 的建议调用顺序

```text
FilteredRowsetWriter::rewrite_segment_vertically(src_segment,
                                                 keep_ranges,
                                                 dst_segment_id)
  └─ VerticalSegmentRewriter::rewrite(...)
       ├─ 创建目标 dst_segment_id 的 WritableFile 和 SegmentWriter
       ├─ 切分 column_groups
       │
       ├─ 对第一个列组（必须包含全部 sort key）
       │    ├─ SegmentWriter::init(column_indexes, has_key=true)
       │    ├─ SegmentReadOptions.rowid_range_option = keep_ranges
       │    ├─ iterator.get_next() → SegmentWriter::append_chunk()
       │    └─ SegmentWriter::finalize_columns()
       │
       ├─ 对每个后续 value 列组
       │    ├─ SegmentWriter::init(column_indexes, has_key=false)
       │    ├─ 使用同一个 keep_ranges 创建 reader
       │    ├─ iterator.get_next() → SegmentWriter::append_chunk()
       │    └─ SegmentWriter::finalize_columns()
       │
       ├─ SegmentWriter::finalize_footer()，关闭目标 .dat
       ├─ 校验所有列组写入行数相同
       └─ 返回完整 SegmentBuildResult

FilteredRowsetWriter 登记 SegmentBuildResult
```

这里的“在返回前完成所有列组和 footer”是指：`VerticalSegmentRewriter::rewrite()` 不把仍然打开的 `SegmentWriter` 交回 Rowset 组装器。只有 `.dat` footer、嵌入索引和需要的外部索引全部完成，文件关闭并形成 `SegmentBuildResult` 后，`FilteredRowsetWriter` 才登记这个目标 Segment。

这也是不直接套用当前 `VerticalRowsetWriter` 生命周期的原因之一：现有 `VerticalRowsetWriter` 可以同时持有多个尚未写 footer 的 `SegmentWriter`，最后在 `final_flush()` 中统一调用 `finalize_footer()`；Tenant-TTL 首期则要求一个源 Segment 的目标文件完整结束后再处理下一个源 Segment。

#### 源 Segment 到目标 Segment 的 0/1 映射

“0/1 映射”表示每个源 Segment 最多产生一个目标 Segment：

```text
源 Segment 0：KEEP     → 目标 Segment 0
源 Segment 1：DROP     → 无目标 Segment
源 Segment 2：REWRITE  → 目标 Segment 1
源 Segment 3：KEEP     → 目标 Segment 2
```

目标 ordinal 只在 KEEP 或 REWRITE 成功产生完整 Segment 后递增，因此始终是连续的 `0, 1, 2, ...`。KEEP 时即使源文件名是 `segment_3.dat`，链接到新 Rowset 后也必须按分配结果命名为目标 `segment_2.dat`，并同步重映射外部索引和 encryption meta。

源 Rowset 已是 `NONOVERLAPPING`，严格保持源 Segment 顺序、每个源 Segment 只删除行而不重排、每个源 Segment 最多输出一个 Segment，这三点共同保持目标 Rowset 的全局顺序。最后调用 `Rowset::verify()`，对 NONOVERLAPPING Rowset 的联合迭代结果执行排序检查，防止实现错误破坏顺序。

#### 建议替换总体方案中的原段落

> `FilteredRowsetWriter` 是与具体过滤策略无关的目标 Rowset 组装器，不直接接收 Chunk 或列组。它严格按源 `segment_id` 处理：KEEP 调用 `add_linked_segment()` 登记一个已完整存在的 hard-link Segment；DROP 不产生输出；REWRITE 调用 `rewrite_segment_vertically()`。后者每次只处理一个源 Segment，并委托 `VerticalSegmentRewriter` 创建一个目标 `SegmentWriter`。`VerticalSegmentRewriter` 为每个列组设置同一个 `KeepRowRanges`（`SparseRange<rowid_t>`），通过 `SegmentReadOptions::rowid_range_option` 读取相同的源行号；依次调用 `SegmentWriter::init()`、`append_chunk()` 和 `finalize_columns()`，所有列组完成后调用 `finalize_footer()` 关闭文件，再返回完整的 `SegmentBuildResult`。因此 Rowset 组装器只登记完整 Segment，不会把 hard link 插入尚未完成的列组写入。目标 Segment ordinal 仅在 KEEP/REWRITE 成功后递增；源顺序、每源 Segment 0/1 输出和仅删除不重排共同保持 NONOVERLAPPING，最终再由 `Rowset::verify()` 校验整体排序。

#### 源码依据

- `be/src/storage/range.h:172-224`：`SparseRange` 的区间集合、`empty()`、`span_size()` 和交集语义。
- `be/src/storage/rowset/segment_options.h:94-98`：`SegmentReadOptions::rowid_range_option`。
- `be/src/storage/rowset/segment_iterator.cpp:2650-2696`：初始化全 Segment 行范围并与 `rowid_range_option` 求交。
- `be/src/storage/rowset/segment_writer.h:88-143`：现有 `SegmentWriter` 的 Vertical 写入 API。
- `be/src/storage/rowset/segment_writer.cpp:240-257, 300-364`：首列组必须含全部 sort key；列组索引写入、行数一致性校验和 footer 关闭。
- `be/src/storage/rowset/rowset_writer.cpp:1261-1420`：现有 `VerticalRowsetWriter` 跨列组维护多个 `SegmentWriter`，并在 `final_flush()` 统一完成 footer。
- `be/src/storage/rowset/rowset.cpp:1047-1093`：`Rowset::verify()` 的整体排序检查。

#### 对第二步测试对齐的输入

后续测试至少覆盖：`SparseRange` 多区间重放、各列组行数一致、首列组包含全部 sort key、单源 Segment 只产生 0/1 个输出、DROP 后目标 ordinal 连续、KEEP/REWRITE 交替、外部索引与加密元数据重映射，以及构造错序输出时 `Rowset::verify()` 必须失败。

### REQ-7.1-003：Rowset 输出组件采用通用命名和策略无关接口

状态：已确认  
确认日期：2026-09-03  
来源：`REQ-7.1-002` 中拟新增的 `TenantTtlRowsetWriter`  
主题：为等值、不等值及其他过滤 Compaction 复用 Rowset 输出能力

#### 问题

本次需求由 tenant TTL 驱动，但 Segment 级 KEEP、DROP、部分行重写的机制也可以服务于其他过滤 Compaction。`TenantTtlRowsetWriter` 是否应改用不包含 tenant/TTL 的通用名称？

#### 建议结论

建议将 `TenantTtlRowsetWriter` 重命名为：

```text
FilteredRowsetWriter
```

该名称表达“根据已经计算好的保留行集合生成过滤后的 Rowset”，不绑定 tenant、TTL 或具体谓词类型。仓库当前没有同名类。

不建议命名为 `PredicateRowsetWriter`，因为 Writer 不应解析或执行谓词；不建议使用宽泛的 `RowsetRewriter` 或 `SegmentRewriter`，因为仓库已有 `SegmentRewriter` 服务于 partial update，且无法从名字看出 KEEP hard link、DROP 和部分重写的组合语义；`HybridRowsetWriter`/“mixed writer”则仍然不够明确。

#### 通用接口边界

`FilteredRowsetWriter` 不接收 SQL 表达式、tenant 字典或 TTL cutoff，只接收上层已经归一化的执行计划：

```text
enum class SegmentFilterAction {
    KEEP,
    DROP,
    REWRITE
};

struct SegmentFilterPlan {
    uint32_t src_segment_id;
    SegmentFilterAction action;
    SparseRangePtr keep_row_ranges; // 仅 REWRITE 必填
};

FilteredRowsetWriter::add_linked_segment(...)
FilteredRowsetWriter::rewrite_segment_vertically(...)
FilteredRowsetWriter::build()
```

无论上层条件是 tenant TTL、`column = value`、`column != value`，还是未来受支持的组合谓词，都必须先转成“哪些源 row ordinal 被保留”的 `SparseRange`。Writer 只执行计划，不关心计划由什么谓词产生。

为避免“过滤掉匹配行”还是“只保留匹配行”的极性歧义，接口统一使用 `keep_row_ranges`：

- 删除满足谓词的行：上层计算谓词补集作为 `keep_row_ranges`。
- 仅保留满足谓词的行：上层直接把匹配范围作为 `keep_row_ranges`。
- Writer 永远只解释“这些行要保留”，不解释谓词真假。

#### 分层后的建议命名

| 层次 | 建议组件 | 通用性 |
| --- | --- | --- |
| Tenant-TTL 任务 | `EngineTenantTtlCompactionTask` | Tenant-TTL 专用 |
| Tenant-TTL 决策 | `TenantTtlRowFilter` | Tenant-TTL 专用，消费 FE 物化的 `DELETE_LIST/KEEP_LIST` 并扫描业务 tenant |
| 通用执行计划 | `SegmentFilterPlan` | 策略无关 |
| Rowset 输出 | `FilteredRowsetWriter` | 策略无关，执行 KEEP/DROP/REWRITE |
| 单 Segment 纵向重写 | `VerticalSegmentRewriter` | 策略无关，消费 `keep_row_ranges` |

本次首期只实现 Tenant-TTL 的计划生成器。采用通用 Writer 名称和接口不等于首期同时支持任意 SQL 谓词；其他谓词的语义校验、NULL 规则、表达式执行、索引优化和任务协议应作为后续独立需求。

#### 对已确认结论的影响

`REQ-7.1-002` 中原 `TenantTtlRowsetWriter` 的职责保持不变，类名已统一替换为 `FilteredRowsetWriter`；“Tenant-TTL 目标 Rowset 组装器”相应改为“与具体过滤策略无关的目标 Rowset 组装器”。`VerticalSegmentRewriter`、`SparseRange`、逐源 Segment 0/1 映射和完整 Segment 登记边界均不变化。

#### 对第二步测试对齐的输入

Writer 单元测试应使用与 Tenant-TTL 无关的人工 `SegmentFilterPlan`，验证 KEEP/DROP/REWRITE、连续目标 ordinal、`SparseRange` 重放和异常清理；Tenant-TTL 测试单独验证 FE 已物化的 `DELETE_LIST/KEEP_LIST` 如何通过业务 tenant 精确扫描生成相同计划。这样可以证明执行层没有隐藏的 tenant 或 TTL 依赖。

### REQ-7.4-001：是否必须在 RowsetMetaPB 持久化逐 Segment 统计

状态：已确认  
确认日期：2026-09-03  
来源：总体方案第 7.4 节“逐 Segment 统计是必要元数据”  
主题：部分 Segment hard link 后如何生成准确的 Rowset 级统计

#### 问题

为什么需要在 `RowsetMetaPB` 新增 `repeated SegmentStatsPB`？如果不新增，具体有什么问题无法解决？是否存在完全不修改 `RowsetMetaPB` 的实现方案？

#### 修正后的建议结论

`SegmentStatsPB` 不是 Tenant-TTL/过滤 Compaction 的正确性必需项，首期建议不修改 `RowsetMetaPB`，也不引入 `NEED_SEGMENT_STATS` 兼容门槛。

原方案中只有下面这条判断成立：

> 仅使用源 `RowsetMetaPB` 已持久化的 rowset 级聚合值，无法精确计算“只保留其中若干 Segment”后的聚合值。

但这不等于必须持久化逐 Segment 统计。Segment 文件是自描述的，可以在任务运行时从已加载的 Segment、footer 和文件系统重新收集统计；REWRITE Segment 的统计可以直接从 `SegmentWriter` 获得。持久化 `SegmentStatsPB` 只是减少重复元数据读取的性能优化，不是不可替代的能力。

#### 原方案为什么提出新增字段

当前 `RowsetWriter::build()` 需要写入以下 Rowset 级聚合值：

```text
num_rows
total_row_size
total_disk_size
data_disk_size
index_disk_size
num_segments
segment_encryption_metas
```

现有 `HorizontalRowsetWriter::add_rowset()` 只能链接整个 Rowset，因此可以直接把源 Rowset 的聚合值整体相加。过滤 Compaction 只链接部分 Segment，不能再使用整 Rowset 的 `num_rows/data_disk_size/index_disk_size/total_row_size`。

如果只允许读取 `RowsetMetaPB`，那么确实无法知道被选中 Segment 分别贡献了多少行和字节。新增 `SegmentStatsPB` 可以让这件事变成 O(1) 聚合，并减少运行时 footer/index I/O。这是原方案提出字段的原因。

#### 如果既不新增字段，也不做运行时重算，会出现什么问题

直接沿用源 Rowset 聚合值、按 Segment 数量等比例拆分，或者把缺失值写成 0，都不可接受：

- `num_rows` 错误会破坏 Compaction 输入行数、输出行数和过滤行数的一致性检查。
- `data_disk_size` 错误会影响 SizeTiered/Base Compaction 选取、Tablet footprint、任务统计和容量判断。
- `total_row_size` 错误会影响后续 Horizontal Compaction 的 chunk size/内存估算，严重低估时可能增加内存风险。
- `index_disk_size/total_disk_size` 错误会造成存储统计和监控漂移。

这些聚合字段不负责定位 Segment 文件，也不参与 Segment 数据解码，所以错误值通常不会让新 Rowset 立即不可读；问题主要是校验、调度、资源估算和运维统计不可信。因此不能把“统计不准”描述成“Rowset 无法构建或读取”，但生产实现仍必须生成可信值。

#### 不新增 RowsetMetaPB 字段的推荐方案

新增仅存在于任务执行期间的通用结构，例如：

```text
struct SegmentRuntimeStats {
    int64_t num_rows;
    int64_t segment_file_size;
    int64_t embedded_index_size;
    int64_t external_index_size;
    int64_t raw_row_size;
    std::string encryption_meta;
};

struct SegmentBuildResult {
    uint32_t dst_segment_id;
    SegmentRuntimeStats stats;
    ArtifactList artifacts;
};
```

`FilteredRowsetWriter` 只在内存中累加每个 KEEP/REWRITE Segment 的 `SegmentRuntimeStats`，最后仍然只写现有的 Rowset 级字段。

##### KEEP Segment 的统计来源

| 统计 | 无新增 PB 的来源 |
| --- | --- |
| `num_rows` | 已加载 `Segment::num_rows()`，来自 `SegmentFooterPB.num_rows` |
| `.dat` 文件大小 | `Segment::get_data_size()`；当前该方法实际返回完整 Segment 文件字节数，可使用 `FileInfo.size` 或 `FileSystem::get_file_size()` |
| `raw_row_size` | 递归汇总各 `ColumnReader::total_mem_footprint()`；该值来自未废弃的 `ColumnMetaPB.total_mem_footprint` |
| footer 大小 | `Segment::parse_segment_footer()` 已返回 `footer_length + 12` |
| 压缩数据页大小 | 加载 ordinal index 后递归汇总 `ColumnReader::data_page_footprint()`，并计入 dictionary page |
| 内嵌索引大小 | 根据完整 `.dat` 大小扣除数据页、dictionary page、footer/trailer 后得到，或由统一的 footer/page 统计 helper 直接累加 |
| 外部索引大小 | 按 schema 和源 Segment ordinal 定位 GIN/Vector artifact，使用文件/目录 stat 汇总 |
| 加密元数据 | 现有 `RowsetMetaPB.segment_encryption_metas(src_segment_id)`，按目标 ordinal 复制 |

需要新增一个读侧 helper，例如：

```text
StatusOr<SegmentRuntimeStats> Segment::collect_runtime_stats(
        const TabletSchema& schema,
        bool include_external_indexes);
```

它可以复用已经为分类和 TTL 预扫描打开的 Segment。对于 KEEP Segment，只读取 footer 和必要的 index 元数据，不读取所有业务列数据页。

##### REWRITE Segment 的统计来源

`VerticalSegmentRewriter` 在写入时已经掌握准确值：

- `SegmentWriter::finalize_columns()` 返回本列组索引大小并校验列组行数一致。
- `SegmentWriter::finalize_footer()` 返回完整 Segment 文件大小。
- 保留行数等于 `keep_row_ranges.span_size()`。
- `raw_row_size` 在读取/追加保留 Chunk 时累加。
- 外部索引 artifact 和大小在 writer finalize 时登记。
- encryption meta 在创建目标 `WritableFile/SegmentWriter` 时产生。

因此 REWRITE 路径本来就不需要从持久化的源 Segment 统计推导。

##### Rowset 级汇总

```text
for each completed KEEP or REWRITE SegmentBuildResult:
    num_rows       += stats.num_rows
    total_row_size += stats.raw_row_size
    data_disk_size += stats.segment_file_size
                      - stats.embedded_index_size
                      - stats.external_index_size
    index_disk_size += stats.embedded_index_size
                       + stats.external_index_size
    total_disk_size = data_disk_size + index_disk_size
    num_segments   += 1
```

具体 data/index 口径必须与当前 `SegmentWriter` 的累计方式保持一致；如果现有 standalone index 的记账语义存在历史差异，应在 helper 中复刻当前 writer 语义，而不是借此改变 RowsetMeta 字段定义。

#### 该方案的代价

- 每次过滤 Compaction 都要为 KEEP Segment 收集一次 footer/index/file stat，CPU 和元数据 I/O 高于直接读取持久化 `SegmentStatsPB`。
- 统计 helper 要正确处理复杂列、dictionary page、嵌入索引、GIN/Vector 外部文件和透明加密。
- 任务重启后不能直接恢复内存统计，需要重新收集；但 hard link/rewrite 本身也必须按任务状态重新校验，因此不构成新的正确性问题。

Tenant-TTL 任务本来就要加载 Segment，并扫描业务 tenant 列生成 `keep_row_ranges`。在本地 POSIX、单副本的首期边界内，KEEP Segment 的额外元数据统计通常比引入永久 PB 字段和新老版本兼容逻辑更可控。

#### 其他不新增字段的备选方案

1. 全量重写：所有候选 Segment 都经 `SegmentWriter` 重建，统计天然准确。实现最简单，但完全失去 KEEP hard link 的主要收益，可作为异常 fallback，不建议作为正常路径。
2. 运行时统计缓存：把 `collect_runtime_stats()` 的结果放入进程内 cache，后续任务复用；cache miss 或重启后重新收集。它只能优化推荐方案，不能替代重算逻辑。
3. 近似拆分：按行数或 Segment 数量分摊源 Rowset 聚合值。虽然不改 PB、实现简单，但会污染 Compaction 策略和资源估算，明确不采用。
4. sidecar 统计文件：技术上不改 `RowsetMetaPB`，但仍引入新的持久化格式、生命周期和 GC 一致性问题，比直接扩展 PB 更复杂，不采用。

#### 对总体方案的修订建议

删除以下首期要求：

- 在 `RowsetMetaPB` 新增 `repeated SegmentStatsPB segment_stats`。
- 要求所有新写 Rowset 持久化逐 Segment 统计。
- 老 Rowset 缺失字段时返回 `NEED_SEGMENT_STATS`。
- 为 KEEP/DROP 判断持久化 boundary fingerprint。

替换为：

> 首期不修改 `RowsetMetaPB`。`FilteredRowsetWriter` 使用仅在任务内存中存在的 `SegmentRuntimeStats/SegmentBuildResult` 汇总现有 Rowset 级字段。REWRITE Segment 的统计由 `SegmentWriter` 在 finalize 时返回；KEEP Segment 的统计由 `Segment::collect_runtime_stats()` 从现有 footer、ColumnMeta、文件大小及外部索引 artifact 按需重建。老 Rowset 与新 Rowset 走同一收集路径，不设置 `NEED_SEGMENT_STATS` 门槛。逐 Segment 持久化统计仅作为未来有明确性能数据支持后的可选优化。

#### 源码依据

- `gensrc/proto/olap_file.proto:112-185`：现有 Rowset 级聚合字段与逐 Segment encryption meta。
- `gensrc/proto/segment.proto:204-217`：Segment 顶层 footprint 已废弃，但 footer 仍包含逐列 `ColumnMetaPB`。
- `gensrc/proto/segment.proto:172-196`、`be/src/storage/rowset/column_reader.h:150-154`：未废弃的逐列 `total_mem_footprint` 和数据页 footprint。
- `be/src/storage/rowset/rowset_writer.cpp:145-168`：`build()` 从内存累计值写现有 Rowset 级字段。
- `be/src/storage/rowset/rowset_writer.cpp:758-790`：当前整 Rowset hard link 直接复用源 Rowset 聚合值，说明部分链接需要另一种统计来源。
- `be/src/storage/rowset/segment.cpp:709-714`：从 `FileInfo` 或文件系统读取完整 Segment 文件大小。
- `be/src/storage/rowset/segment.cpp:89-190`：解析 footer 时可以得到 footer 的准确物理长度。
- `be/src/storage/meta_reader.cpp:542-586`：现有代码已递归收集列 `total_mem_footprint`，并通过 ordinal index 汇总压缩数据页大小。
- `be/src/storage/vertical_compaction_task.cpp:170-185`：现有 Vertical Compaction 已直接从 `ColumnReader` 汇总 footprint，而不是依赖逐 Segment RowsetMeta 字段。
- `be/src/storage/rowset/segment_writer.cpp:300-364`：REWRITE 写入路径可在 finalize 阶段取得索引和 Segment 文件大小。

#### 对第二步测试对齐的输入

后续测试至少覆盖：老 Rowset 无任何新增字段仍可完成 KEEP/DROP/REWRITE；运行时统计与同数据全量重写所得 Rowset 级统计一致；复杂列/dictionary page；Bloom/Bitmap/ShortKey；GIN/Vector 外部索引；透明加密；DROP 导致源/目标 ordinal 变化；任务重启后重新收集；统计失败时整任务不提交且清理已生成 artifact。

### REQ-8.1-001：Tablet 级串行、与普通 Compaction 互斥及 Rowset 身份校验的职责边界

状态：已确认  
确认日期：2026-09-04  
来源：总体方案第 6.1 节和第 8.1 节  
主题：`is_compacting`、Tablet 级互斥、完整覆盖语义与 Rowset ID CAS

#### 问题

Tenant-TTL Compaction 会对源 Rowset 设置 `rowset->set_is_compacting(true)`，是否已经足以防止同一 Rowset 并发，从而不再需要提交前比较 Rowset ID？

由 FE 发起的 Tenant-TTL 任务是否必须在同一 Tablet 上串行，并与普通 Base/Cumulative Compaction 互斥，以保证 FE 收到的 `SUCCESS/NOOP` 代表固定任务边界内已经完整处理，而不是跳过一部分 Rowset 后留给下一轮？

#### 已确认结论

1. Tenant-TTL 应对已经成功占有的源 Rowset 设置 `is_compacting=true`，并在所有退出路径上以 RAII 清除；但 `is_compacting` 只是 Rowset 级调度标记，不是互斥锁，也不是原子占有协议。
2. 首期 Tenant-TTL 在同一 Tablet 上必须串行，并与普通 Base/Cumulative Compaction 的执行阶段互斥。这是 FE 任务可以返回准确“完整处理结果”的首期必要约束。
3. 首期必须支持同一 coverage 中存在多个当前可见 Rowset。Rowset 数量大于 1 不是 `NEED_BASE_COMPACTION` 或任务降级理由；任务必须对 coverage 中全部 Rowset 给出结论。
4. 多 Rowset 任务必须“全部构建验证成功后再一次批量提交”。任一 Rowset 失败或身份改变时，整个 coverage 不提交，不允许一个 FE 任务留下部分已应用结果。
5. 不得把 `get_is_compacting()==true`、抢锁失败或提交竞态解释成“跳过该 Rowset 但整个 Tablet 任务仍然成功”。这些情况必须返回可识别的 `TABLET_BUSY/RETRYABLE_CONFLICT/STALE_ROWSET`，由 FE 保持原 policy/evaluation watermark 重试；不能返回 `SUCCESS` 或 `NOOP`。
6. 提交前 Rowset ID 比较仍保留。Tablet 级互斥负责阻止正常执行路径的并发，Rowset ID 比较负责验证锁协议和调度假设在提交时仍然成立。二者不是互相替代关系。
7. Rowset ID 校验只加入 Tenant-TTL 专用提交逻辑，不修改现有 `Tablet::modify_rowsets_without_lock()` 的语义、签名或普通 Horizontal/Vertical Compaction 的调用路径。校验与底层修改必须处于同一个 header lock 临界区。
8. 首期采用非阻塞 try-lock，但不处理 try-lock 饥饿：不实现 `TTL_PENDING` lease、公平队列、优先级提升或 BE 本地排队。抢锁失败即释放已持有资源并返回可重试 `TABLET_BUSY`，由 FE 后续重试；持续繁忙时可能多次失败是首期接受的限制。
9. Tablet 级 `TTL_IDLE / TTL_PENDING / TTL_RUNNING` 状态用于协调任务准入和标识单次请求的执行阶段，不用于替代 Compaction lock、header lock 或 Rowset ID 校验。其中 `TTL_PENDING` 只是从取得 admission 到 try-lock 完成之间的短暂状态，不跨 FE 重试保留。

#### 为什么 `is_compacting` 不等于占有权

当前接口的本质是对 `std::atomic<bool>` 做普通 `store/load`：

```cpp
void set_is_compacting(bool flag) { is_compacting.store(flag); }
bool get_is_compacting() { return is_compacting.load(); }
```

它没有 `compare_exchange`语义。如果两个 FE 任务同时对同一 Rowset 直接执行 `set_is_compacting(true)`，两次调用都会成功，无法判定谁先占有。

普通 Size-Tiered Compaction 之所以可以用该标记排除重叠输入，依赖的不只是 bool 本身，还包括 `Tablet::_compaction_task_lock` 下的任务创建、选择器事先跳过已标记 Rowset，以及 CompactionManager 的任务生命周期。FE 直接下发的 Tenant-TTL 任务如果不显式接入同一并发域，不能仅靠设置 bool 复制这个保证。

`is_compacting` 仍然有必要：它让已有的 Size-Tiered 选择器不再把 TTL 正在处理的 Rowset 选为新任务输入，并为运行状态和测试提供可见标识。但它不能阻止已经选好输入的普通 Compaction，也不覆盖 clone/replication/schema-change 等不以该 bool 作为唯一准入条件的路径。

#### Tablet 级串行与普通 Compaction 互斥的推导

FE 需要为一个 Tenant-TTL 任务给出可审计的终态。因此先定义任务的完整性不变式：

> 对 BE 接受任务后固定的 `coverage` 与 policy/evaluation watermark，`SUCCESS` 表示 coverage 中每一个源 Rowset 都已按该快照完成处理并提交；`NOOP` 表示 coverage 中所有 Rowset 都已经精确证明无需修改或已应用同等/更高水位。锁冲突、`is_compacting` 或未处理 Rowset 不构成 NOOP。

在该不变式下，如果允许两个 TTL 任务并发：

1. TTL-A 标记或处理 R1，TTL-B 可能因 `is_compacting` 跳过 R1。
2. TTL-B 标记或处理 R2，TTL-A 可能跳过 R2。
3. 两个任务即使各自成功改写一部分 Rowset，也没有任何一个结果能向 FE 证明整个 coverage 已完成。
4. 如果两个任务携带不同 policy/evaluation watermark，还会产生水位顺序与结果归属不可审计的问题。

如果允许 TTL 与普通 Compaction 并发：

1. 普通 Compaction 可把 `{R1(V1), R2(V2)}` 改成 `R12([V1,V2])`，直接改变 TTL 任务的 Rowset 集合和 Segment 边界。
2. TTL 若跳过已标记 Rowset，会破坏 coverage 完整性；若继续提交，则必须依赖 Rowset ID/version CAS 让其中一方失败。
3. 仅依赖 CAS 可以避免错误提交，但任务会在完成大量读写后才返回冲突，也无法让本次 FE 任务获得完整成功结果。

因此，对首期“一次 FE 任务必须在明确 coverage 内完整处理”的交付语义，Tablet 级串行和与普通 Compaction 互斥是必要的。未来只有在实现“动态重规划 + 逐 Rowset CAS + coverage 持久化跟踪 + 局部成功可恢复”后，才可以放宽互斥；这不属于首期范围。

#### 固定 coverage 与“处理干净”的精确定义

原文使用的 `coverage_max_version` 容易被理解为“任务回报时 Tablet 的最新版本”。为避免歧义，后续建议改名为：

```text
snapshot_end_version      // BE 固定任务快照时的连续可读版本上界
processed_through_version // 在结果中回传，成功时等于 snapshot_end_version
```

`snapshot_end_version` 是 Rowset 的逻辑 `Version(start,end)` 中的版本号，不是 Rowset ID、Segment ID、policy version 或时间戳。

##### 快照时刻与 coverage 生成规则

BE 按以下时序生成本次 coverage：

1. 验证请求、Tablet 状态和字典策略快照。
2. 取得该 Tablet 的 TTL 串行权，再按固定顺序独占 `base_lock` 与 `cumulative_lock`。
3. 在 header lock 下取当前从版本 0 开始的最大连续可读版本 `B`，记为 `snapshot_end_version=B`。
4. 在同一 header lock 临界区内，解出能够完整覆盖 `[0,B]` 的当前可见 Rowset 路径，并记录每个 Rowset 的 `(version,rowset_id)`。
5. 该精确 Rowset 列表就是本次 `coverage`；`B` 只是逻辑上界，不能单独代替 Rowset 列表。

FE 可以在请求中携带规划时观测到的 max version，用于调试、防止任务逆序或执行严格的 expected-version 协议；但在首期“BE 开始执行时的当前可读快照必须处理完整”语义下，不建议把过时的 FE max version 直接当作有效 coverage 上界。有效边界应由 BE 在取得互斥后固定；否则任务排队期间已经 publish 的 Rowset 会在开始执行前就被排除，再次形成静默遗漏。

概念上可表示为：

```text
snapshot_end_version = B
coverage = [(source_version_0, source_rowset_id_0), ...]
schema_fingerprint
policy_txn_id + evaluation_time
```

例如快照时 Tablet 为：

```text
R0_80  version=[0,80],   rowset_id=A
R81_90 version=[81,90],  rowset_id=B
R91    version=[91,91],  rowset_id=C
```

则：

```text
snapshot_end_version = 91
coverage = [([0,80],A), ([81,90],B), ([91,91],C)]
```

这三个 Rowset 无论最终粗判为 KEEP、DROP 还是需要 REWRITE，都属于 coverage。“属于 coverage”表示必须对其给出可审计结论，不等于每个 Rowset 都必须产生新文件。

##### 什么是 coverage 外的 Rowset

按首期建议，快照时已经可见且属于 `[0,B]` 连续版本路径的 Rowset 必须全部纳入 coverage，不允许为了减少本次工作量人为排除。正常情况下，coverage 外的当前 Rowset 只有以下几类：

1. **快照固定后的新 publish**：例如上述 coverage 固定为 `[0,91]` 后，写入路径又 publish `R92 version=[92,92]`。`R92` 的版本超过 `snapshot_end_version=91`，不属于本次 coverage。
2. **stale/unused Rowset**：它们已经不在当前可读版本路径上，仅为存量查询或 GC 保留，不是 TTL 新任务的输入。
3. **非当前可读路径中的辅助映射**：例如只留在 incremental rowset 映射中、但已被 Compaction 从当前 `_rs_version_map` 可读路径替换的 Rowset。

如果 header lock 内发现 `[0,B]` 存在空洞、重叠、无法解出唯一连续路径，或者存在高于 `B` 但因版本空洞暂时不可读的 active Rowset，不得简单将其当作“coverage 外”并成功。首期应整体返回不可执行/需重试的明确状态，避免 FE 对完成边界产生错误理解。

##### 为什么 coverage 外 Rowset 不设置 `is_compacting`

TTL 只对快照时已经存在的 coverage 源 Rowset 拥有处理权，所以只对这些 Rowset 设置 `is_compacting=true`。快照固定后新 publish 的 `R92` 不是本次读取和改写对象，不应被本任务标记为正在 Compaction。

这不会导致普通 Compaction 在 TTL 执行期间处理 `R92`：TTL 此时仍持有 `base_lock+cumulative_lock` 的独占锁，普通 Compaction 无法进入执行阶段。`is_compacting` 标记限定具体输入身份，Tablet 级锁限定执行并发，两者职责不同。

##### “处理干净”的边界

“处理干净”不能定义为追赶任务执行期间无限到来的新写入，否则持续写入的 Tablet 永远无法完成 TTL 任务。首期定义为：

> 对固定快照 `[0,snapshot_end_version]` 中的所有 coverage Rowset，没有任何 Rowset 因为 busy、`is_compacting`、资源限制或 Rowset 数量而被静默跳过。每个 Rowset 都必须有 KEEP/DROP/REWRITE/ALREADY_APPLIED 等可审计结论，且需要修改的结果已经作为同一批次成功提交。

如果业务要求“截至 FE 收到回调的那一刻，Tablet 所有新写入也必须完成 TTL”，就必须在整个 TTL 读写期间额外阻塞 publish/导入，或实现不断扩展快照的追赶协议。这会带来长时间写入阻塞或任务无法收敛，不建议作为首期语义。

任务结果至少回传 `processed_through_version` 与 coverage Rowset 清单/digest。如果回报时当前 Tablet 已经 publish 到更高版本，FE 可以认定“本次固定快照成功”，但不能将 Tablet 的最新水位标记为已完成。

##### 首期多 Rowset 执行与原子提交语义

首期不限制 coverage 只有一个 consolidated Rowset。只要 coverage 中每个 Rowset 都满足已确认的结构准入条件（例如 DUP_KEYS、schema 一致、单个 Rowset 内 Segment 布局可安全执行 KEEP/DROP/REWRITE），任务必须支持对 coverage 中任意数量的 Rowset 逐个执行 Segment 计划。不得仅因 Rowset 数量大于 1 返回 `NEED_BASE_COMPACTION`。

首期提交单元仍然是整个 Tablet coverage，建议流程为：

```text
staged_outputs = []
per_rowset_results = []

for source in coverage ordered by version:
    classify/scan all segments of source
    if source does not need physical replacement:
        record VERIFIED_NO_CHANGE / ALREADY_APPLIED
    else:
        output = build same-version replacement for source
        load + verify output
        staged_outputs.add(source -> output)

under one header lock:
    verify every (version,rowset_id) in the original coverage
    verify schema fingerprint and policy watermark
    if staged_outputs is empty:
        return NOOP with complete per-rowset results
    collect current changed sources from the just-verified version map
    modify_rowsets_without_lock(all_outputs, current_changed_sources, &to_replace)
    save TabletMeta once

close source references and enter stale/unused GC
return SUCCESS with complete per-rowset results
```

每个需要替换的输出 Rowset 保持对应源 Rowset 的逻辑 version，不要因为本次处理了多个 Rowset 就隐式将它们合并为一个新版本区间。某个源 Rowset 如果全部数据被 DROP，仍必须产生一个同 version 的空输出 Rowset，不能直接删掉该 version 映射而制造版本空洞。

任一源 Rowset 在分类、精确扫描、输出构建、load、verify 或提交前身份校验中失败，必须清理 coverage 内全部 staged outputs 且不修改 TabletMeta。不接受“已处理前 N 个 Rowset，剩余留给下次”的部分提交语义。

##### Rowset ID 校验的实现边界

首期不得为了 Tenant-TTL 修改现有 `Tablet::modify_rowsets_without_lock()` 的行为，也不得给普通 Horizontal/Vertical Compaction 的提交路径增加新的 Rowset ID 校验。Tenant-TTL 应在自己的 commit 方法中，或在只由 Tenant-TTL 调用的专用 wrapper 中实现校验。

推荐提交顺序如下：

```text
under one header lock:
    current_changed_sources = []

    // validation phase: no TabletMeta mutation
    for expected in the entire coverage:
        current = get_rowset_by_version(expected.version)
        require current != null
        require current.rowset_id == expected.source_rowset_id
        if expected needs replacement:
            current_changed_sources.add(current)

    verify schema fingerprint and policy watermark
    verify every staged output has a unique source and the same logical version

    // mutation phase: entered only after the entire coverage passes validation
    modify_rowsets_without_lock(all_outputs, current_changed_sources, &to_replace)
    save TabletMeta once
```

即使某个 coverage Rowset 的结论是 `VERIFIED_NO_CHANGE/ALREADY_APPLIED`，也必须参加提交前身份校验，因为 FE 的成功语义覆盖整个固定快照，而不只是产生输出的 Rowset。传给 `to_delete` 的对象应使用提交时从当前 version map 重新读取并验证过的 `current`，不要使用任务开始时缓存的旧 `RowsetSharedPtr`。

校验不能在 header lock 外完成，也不能在校验后释放锁再调用 `modify_rowsets_without_lock()`，否则校验与修改之间仍存在 TOCTOU 窗口。所有 coverage 身份必须先完成校验，之后才能进行第一次 TabletMeta 修改；不得逐 Rowset 交替执行“校验一个、提交一个”。

如需封装，可以新增只供 TTL 使用、要求调用方持有 header lock 的 `replace_rowsets_for_tenant_ttl_without_lock(...)`，但该方法在完成 TTL 专用校验后仍应复用现有 `modify_rowsets_without_lock()` 完成底层修改，不应复制其 `_rs_version_map`、stale map、TabletMeta、version tracker 和 CompactionManager 更新逻辑。这样既隔离普通 Compaction 路径，也避免两套底层元数据修改逻辑长期分叉。

#### Tablet 级 TTL admission 三态的职责

Tablet 级状态建议定义为：

```text
TTL_IDLE
    --成功取得 Tablet 级 admission-->
TTL_PENDING
    --成功取得 base_lock + cumulative_lock-->
TTL_RUNNING
    --完成或失败，清理 Rowset 标记并释放锁-->
TTL_IDLE

TTL_PENDING
    --任一 try-lock 失败，释放已持有锁和 admission-->
TTL_IDLE
```

这组三态主要解决任务准入和生命周期竞争，不直接解决 Rowset 元数据修改竞争：

1. `TTL_IDLE` 表示没有 TTL 请求占有该 Tablet 的 admission，普通 Compaction 和新的 TTL 请求可以参与调度。
2. `TTL_PENDING` 表示一个 TTL 请求已经取得唯一 admission，但尚未成功取得两把 Compaction lock。普通 Compaction 必须在选择和标记 Rowset 前检查该状态并停止为该 Tablet 创建新任务；其他 TTL 请求也必须返回 busy。该状态关闭“TTL 已通过准入检查，但尚未持有 Compaction lock”期间的新任务创建窗口。
3. `TTL_RUNNING` 表示 TTL 已经取得两把独占 Compaction lock，正在固定 coverage、标记 Rowset、构建输出或提交。其他 TTL 请求应返回 `TTL_ALREADY_RUNNING/TABLET_BUSY`，普通 Compaction 仍不得创建新任务。

仅依赖 `base_lock + cumulative_lock` 可以阻止普通 Compaction 进入执行阶段，但不能天然阻止它提前选择 Rowset、设置 `is_compacting=true`、创建任务并登记到 CompactionManager。普通任务如果到运行时才因抢锁失败，其统一退出逻辑还可能清除输入 Rowset 的 `is_compacting`。因此 admission 状态必须在 Rowset 选择和任务创建之前可见，不能等到 TTL 进入 `TTL_RUNNING` 后才设置。

三态只有与任务创建路径共享同一个同步域时才有正确性意义，首期必须满足：

1. TTL 状态转换与普通 Compaction 任务创建受同一个并发域保护，例如都在 `Tablet::_compaction_task_lock` 下完成检查和更新。
2. 普通 Compaction 在选择、标记 Rowset 和创建任务之前检查 `ttl_state == TTL_IDLE`。
3. `TTL_IDLE -> TTL_PENDING` 必须与“确认没有冲突的已创建/运行任务”原子完成；已经在此之前创建的普通任务不能只靠状态消失，TTL 必须识别该冲突并返回 busy，或确认其已经完全退出。
4. 状态需要同时保存 `owner_task_id`；如用于请求幂等和水位诊断，还应保存 `policy_txn_id/evaluation_time`。只有枚举值而没有 owner，无法可靠区分同一 FE 任务重入与另一个 TTL 任务。
5. 所有成功、失败、取消和异常退出路径都通过 RAII 清理。终止时先清除 coverage Rowset 的 `is_compacting` 并释放 Compaction lock，最后在任务创建同步域内恢复 `TTL_IDLE`，避免新任务在旧资源尚未释放时进入。

`TTL_PENDING` 不承担首期已排除的防饥饿职责。它仅在本次 BE 请求内存在：try-lock 成功便转为 `TTL_RUNNING`；任一 try-lock 失败便立即释放 admission 并回到 `TTL_IDLE`，不会跨 FE 重试保留 lease，也不会为下次重试预留执行顺序。高负载下 TTL 仍可能连续返回 `TABLET_BUSY`。

从纯排他角度，`TTL_IDLE / TTL_ACTIVE` 两态也能表达“是否被 TTL 占用”；保留三态的价值是让 FE 返回、监控、故障清理和测试能够区分“已取得 admission、正在抢锁”和“已取得执行锁、真正运行”。不论采用两态还是三态，都必须具有相同的原子准入和 owner 语义。

职责边界如下：

- admission state：阻止新的 TTL/普通 Compaction 任务进入该 Tablet 的创建与选取阶段，并标识 TTL 生命周期。
- `base_lock + cumulative_lock`：阻止 Compaction 执行阶段并发。
- header lock：保证 coverage 捕获和提交时 Tablet 元数据操作原子。
- Rowset ID 校验：发现 overwrite 等未完全纳入 Compaction lock 域的路径已经替换了源 Rowset。

#### 首期建议锁协议

1. 同一 Tablet 最多一个 Tenant-TTL 任务进入执行阶段。FE 按 Tablet 去重/排队，BE 必须用 Tablet 级 TTL admission state/in-flight registry 做最终裁决，不能只信任 FE 去重。
2. TTL admission state 应与普通 Compaction 的任务创建在同一个并发域内协调，例如受 `Tablet::_compaction_task_lock` 保护。一次 TTL 尝试获得 admission 后，在其抢锁、执行和清理期间，后续自发 Compaction 不应再为该 Tablet 创建新任务；若本次 try-lock 失败，则随已持有锁一起释放 admission，不为下次 FE 重试保留 pending lease。
3. TTL 按与 clone 等现有路径兼容的固定顺序，对 `base_lock` 和 `cumulative_lock` 取独占锁；普通 Base/Cumulative Compaction 持有对应共享锁，因此 TTL 持有两把独占锁时与两类普通 Compaction 都互斥。
4. 两把锁必须使用非阻塞 try-lock。任意一把获取失败时立即释放已经获得的锁，返回可重试 `TABLET_BUSY`，不得持有第一把锁等待第二把锁，也不得在 BE 内长时间 sleep/retry 直到 FE 超时。
5. 两把锁必须在 Rowset 选择之前获取，持有到输出构建、提交或失败清理完成。
6. 取得 Tablet 级互斥后再在 header lock 下固定 coverage 并校验 `is_compacting`。如果发现已有任务标记了 coverage 内的 Rowset，应返回 busy/conflict，不得跳过。
7. 对 coverage 内的源 Rowset 设置 `is_compacting=true`，用 RAII 保证成功、失败、取消和异常退出都清除。
8. 提交时由 Tenant-TTL 专用 commit/wrapper 在 header lock 下比较 coverage 中每个 `(version, rowset_id)`、schema fingerprint 和 policy watermark；只有全部匹配才调用原样保留的 `modify_rowsets_without_lock`。普通 Compaction 路径不变。
9. 任务终止时先清理 Rowset 标记和锁，再清除 BE TTL admission state；该顺序避免新任务在旧任务资源尚未释放时进入。

该协议中，`is_compacting` 用于和既有 Rowset 选择器协作，两把独占 Compaction lock 提供 Tablet 级执行隔离，Rowset ID CAS 是提交前的最终身份校验，coverage/result 协议则向 FE 证明没有静默跳过。

#### 独占两把 Compaction lock 能保证什么

结论需要分成“执行互斥”和“任务准入”两层：

- **对执行互斥足够**：当 TTL 持有 `base_lock` 和 `cumulative_lock` 的独占锁时，后续 Horizontal/Vertical Compaction 在 `CompactionTask::_try_lock()` 中无法取得对应共享锁，不会进入 `run_impl()`，因而不会改写 Tablet Rowset。后续 TTL 任务若也遵守同一独占锁协议，同样无法与当前 TTL 并发执行。
- **对任务准入不足够**：锁只在任务执行时获取，不会自动阻止普通 Compaction 先被选择、创建、标记 Rowset 或登记到 CompactionManager。当前 `CompactionManager::register_task()` 也允许同一 Tablet 登记多个不同任务指针，它不是 Tablet 级排他门。因此仅靠两把锁会出现“任务已创建，到运行时才抢锁失败”的无效调度。

还需要注意 `is_compacting` 只是 bool，没有 owner task id。现有 `CompactionTask::run()` 的统一退出逻辑会对自己的所有输入执行 `set_is_compacting(false)`。如果允许已创建但未取锁的普通任务与 TTL 复用同一 Rowset，失败退出时可能清除不属于它的 TTL 标记。独占锁仍然能保护数据不被并发提交，但调度状态已不可信。所以必须用 Tablet 级 admission gate 阻止后续普通任务被创建，并在首期拒绝/排空已经创建的冲突任务；不能把 Compaction lock 当成唯一调度状态。

#### TTL try-lock 与 FE 重试协议

建议的 BE 伪代码如下：

```text
admission = tablet.try_begin_ttl(task_id, policy_watermark)
if admission == SAME_TASK_RUNNING:
    return TTL_ALREADY_RUNNING(active_task_id, retry_after_ms)
if admission == OTHER_TASK_RUNNING:
    return TABLET_BUSY(active_task_id, retry_after_ms)
// GRANTED 后只为本次尝试持有 admission，不创建跨请求 pending lease

base_guard = unique_try_lock(tablet.base_lock)
if !base_guard.owns_lock():
    release ttl admission
    return TABLET_BUSY(COMPACTION_RUNNING, retry_after_ms)

cumulative_guard = unique_try_lock(tablet.cumulative_lock)
if !cumulative_guard.owns_lock():
    // return 时 base_guard 立即 RAII 释放
    release ttl admission
    return TABLET_BUSY(COMPACTION_RUNNING, retry_after_ms)

fix_coverage_under_header_lock()
mark_coverage_rowsets_compacting()
build_verify_and_commit()
```

FE 把 `TABLET_BUSY/TTL_ALREADY_RUNNING` 视为可重试或继续跟踪的非终态结果，保留原 `task_id/policy_txn_id/evaluation_time` 和完整 coverage 语义，可按 `retry_after_ms` 退避后重试。BE 单次请求不等待长任务锁，因此不会因抢锁导致 FE agent task 超时。

首期明确不提供 try-lock 的无饥饿保证。每次抢锁失败后都释放 admission，不实现 `TTL_PENDING` lease、公平队列、优先级提升或 BE 本地等待队列；高负载下新的自发 Compaction 可能持续抢先，导致 TTL 多次返回 `TABLET_BUSY`。该限制不得通过把 busy 降级成成功或跳过部分 coverage 来掩盖。若后续需要最终执行保证，再单独设计 admission 保留、调度公平性、超时和重启恢复协议。

#### 对总体方案的修订建议

- 删除原“首期只处理一个明确选中的 consolidated Rowset”和请求中 `single_rowset_only` 的限制。首期必须遍历并处理锁内固定 coverage 中的全部当前 Rowset。
- 原“新写入 Rowset 留给下一轮”只能指 coverage 固定之后新 publish、版本高于 `snapshot_end_version` 的 Rowset。锁内快照已存在的其他 Rowset 不属于“新写入”，必须纳入本次 coverage。
- 多 Rowset 输出全部 staged/load/verify 后，必须在同一 header lock 中校验全 coverage 的 `(version,rowset_id)` 并一次修改/保存 TabletMeta；不允许逐 Rowset 提交造成部分成功。
- Rowset ID 校验放在 Tenant-TTL 专用 commit/wrapper 中，并与调用现有 `modify_rowsets_without_lock()` 保持在同一个 header lock 临界区；现有底层函数及普通 Compaction 调用路径不修改。
- FE 去重键需从“同 Tablet 同水位只允许一个 in-flight”收紧为“同 Tablet 任意水位同时只允许一个 Tenant-TTL in-flight”。新的更高水位可排队或在尚未执行时合并/取代低水位，但不与正在执行的任务并发。
- 首期不实现 try-lock 防饥饿机制；抢锁失败释放本次 admission 并返回可重试 busy，不保留 `TTL_PENDING` lease，也不引入 BE 本地排队。
- `TTL_IDLE / TTL_PENDING / TTL_RUNNING` 只表达 Tablet 级准入和单次请求生命周期；状态及 `owner_task_id` 必须在普通 Compaction 任务创建的同一同步域内检查和更新。`TTL_PENDING` 只关闭本次请求取得 admission 后、取得执行锁前的竞争窗口。
- `SUCCESS/NOOP` 结果需要增加 `processed_through_version`、source/output Rowset 清单或等价的 coverage digest，使 FE 可以审计本次完成边界。

#### 源码依据

- `be/src/storage/rowset/rowset.h:318-320,445`：`is_compacting` 是 `std::atomic<bool>` 的 `store/load`，不是 CAS 占有。
- `be/src/storage/tablet.cpp:1681-1693`：普通 Compaction 任务创建受 `_compaction_task_lock` 和 `_has_running_compaction` 管理。
- `be/src/storage/size_tiered_compaction_policy.cpp:68-80,210-223`：Size-Tiered 在任务创建时标记输入 Rowset，选择时跳过已标记 Rowset。
- `be/src/storage/compaction_task.h:236-241`：普通 Base/Cumulative Compaction 执行时持有对应 `shared_lock`。
- `be/src/storage/compaction_task.cpp:80-96,125-131`：普通 Compaction 在运行时才 try-lock，抢锁失败后走统一退出逻辑并清除输入 Rowset 的 `is_compacting`。
- `be/src/storage/compaction_manager.cpp:427-442`：CompactionManager 对同一 Tablet 保存任务集合，不会仅因 Tablet ID 相同就拒绝第二个任务。
- `be/src/storage/task/engine_clone_task.cpp:679-684`：clone 按 base 后 cumulative 的顺序取两把独占锁，证明该互斥模式已被现有代码使用。
- `be/src/storage/compaction_task.h:261-299`：Horizontal/Vertical 共用的提交只校验 version 存在，没有校验 Rowset ID。
- `be/src/storage/tablet.cpp:373-422`、`be/src/storage/tablet_meta.cpp:500-520`：Rowset 和 TabletMeta 删除都以 version 定位，调用者必须保证传入的源 Rowset 仍是当前实例。
- `be/src/storage/txn_manager.cpp:339-351`、`be/src/storage/tablet.cpp:667-679`：overwrite publish 可经 `Tablet::overwrite_rowset()` 修改现有 Rowset 映射，该路径只在 Tablet 内取得 header lock，并不依赖 base/cumulative Compaction lock，因此两把 Compaction lock 不能替代提交前 Rowset ID 校验。

#### 对第二步测试对齐的输入

后续测试至少覆盖：两个不同水位 TTL 任务同 Tablet 并发时只有一个进入执行，后者快速返回可重试 busy 而不等到 FE 超时；`TTL_IDLE -> TTL_PENDING -> TTL_RUNNING -> TTL_IDLE` 正常转换；base 或 cumulative try-lock 失败时从 `TTL_PENDING` 回到 `TTL_IDLE` 且不跨请求保留；同一 `task_id` 重入与不同 `task_id` 冲突能够通过 owner 正确区分；TTL 持有本次 admission 期间不再创建新的自发 Compaction，try-lock 失败时 admission 随即释放；TTL 与 Base/Cumulative Compaction 同时启动时一方返回 busy/retry 且不静默跳过；普通 Compaction 已选择但尚未取锁时 TTL 的处理；base 锁成功但 cumulative 锁失败时立即释放 base 锁和 admission；连续多次 busy 均返回准确可重试状态且不产生部分提交，不要求首期证明无饥饿；任务期间新 publish 版本被正确排除在 coverage 之外；coverage 含 2 个、多个及大量 Rowset 时全部处理；KEEP/DROP/REWRITE 跨 Rowset 交错；某 Rowset 全量 DROP 时以同 version 空 Rowset 保持版本连续；中间任一 Rowset 构建/load/verify 失败时清理全部 staged outputs 且 TabletMeta 零修改；任一 `(version,rowset_id)` 改变时整体不提交；全 coverage 校验与一次批量替换发生在同一个 header lock 临界区；Tenant-TTL 校验不改变普通 Compaction 提交行为；所有输出一次批量替换并仅保存一次 TabletMeta；`SUCCESS/NOOP` 结果 coverage 无遗漏；崩溃/取消/失败后所有 `is_compacting` 标记、owner 和 admission state 都被清除。

### REQ-8.2-001：不持久化 AppliedProof，以执行期零命中保证谓词幂等

#### 问题

除相同 `task_id` 请求的传输幂等外，不同 task ID 可能再次向同一 Tablet 下发相同的 tenant 过滤谓词。是否应在 Tenant-TTL Rewrite 前增加一次存在性查询，或者持久化按 tenant/谓词组织的 `AppliedProof`，以便没有待删除数据时跳过 Rowset 重写？

#### 已确认结论

1. 首期不在 `TabletMetaPB`、`RowsetMetaPB` 或独立 KV 中持久化 `AppliedProof`、tenant 已处理集合或谓词历史。避免 tenant/谓词数量增长导致元数据膨胀、TabletMeta 保存放大、clone/restore/compaction 传播及兼容性连锁问题。
2. 不在正式过滤前执行独立的存在性查询。独立查询若要证明“不存在”，最坏仍需扫描完整 tenant 列；存在数据时还会被正式过滤再次扫描，并且查询快照若不与固定 coverage 绑定还会产生竞态。
3. 固定 coverage 上的正式业务 tenant 单列扫描同时完成谓词判断、`keep_row_ranges` 生成和待删除行计数，不为幂等性增加第二遍数据扫描。
4. “命中数”统一指本次谓词需要删除的非 NULL 行数：
   - `DELETE_LIST`：业务 tenant 位于名单中的非 NULL 行。
   - `KEEP_LIST`：业务 tenant 不在名单中的非 NULL 行。
   - tenant 为 NULL 的行在两种模式下均保留，不计入待删除命中数。
5. 如果全 coverage 的待删除行数为零，则不创建任何 staged output，不生成同 version 替代 Rowset，不调用 `modify_rowsets_without_lock()`，也不为了记录本次 NOOP 保存新的 TabletMeta。任务返回终态成功 `NOOP_VERIFIED`。
6. 返回 `NOOP_VERIFIED` 前，仍必须在同一个 header lock 临界区内重新校验完整 coverage 的每个 `(version,rowset_id)`、schema fingerprint 和本次任务 policy watermark，防止扫描结束后 Rowset 身份变化却错误返回成功。
7. `NOOP_VERIFIED` 必须携带 `processed_through_version` 以及完整 coverage 清单或 digest。它只证明固定的 `[0,snapshot_end_version]` 已经精确检查且零命中，不证明 coverage 固定后新 publish 的更高版本已处理。
8. 如果只有部分 Rowset 命中，零命中的源 Rowset 记录为 `VERIFIED_NO_CHANGE` 且不生成输出，但仍参加全 coverage 的提交前 Rowset ID 校验；只有实际需要 DROP/REWRITE 的源 Rowset 生成替代输出，最后仍按整个 coverage 的原子提交语义处理。

#### 幂等性边界

- 相同 `task_id + predicate_digest` 的重复请求仍按请求级幂等协议处理；相同 task ID 但 digest 不同必须拒绝。
- 不同 task ID 携带相同 tenant 谓词时，删除过滤满足 `F(F(data)) = F(data)`。第一次执行已经删除命中行后，后续任务重新扫描相同 coverage 将得到零命中并返回 `NOOP_VERIFIED`，不会产生新的 Rowset。
- 该方案保证数据结果幂等和无效 Rowset 零重写，但不保证重复请求零扫描。BE 重启、完成回报丢失或不同 task ID 重复下发时，允许重新读取业务 tenant 单列。
- FE 可以把 `NOOP_VERIFIED` 视为本次固定 coverage 的终态成功并记录对应完成边界；BE 不维护随 tenant 数量增长的持久化谓词历史。

#### 资源代价

零命中任务的最坏成本是一遍 coverage 业务 tenant 单列读取、解压和比较，不读取 `recordTimestamp`、`tenant_id`、字典或其他业务列，也不产生 Segment/Rowset 写入、Hard Link、TabletMeta 保存和后续 GC。首期不为降低这次扫描新增 tenant Bloom Filter、tenant_id/Short Key 或定制 ZoneMap 选择逻辑；现有读取链路能够自然利用的通用裁剪不改变本需求边界。

#### 对第二步测试对齐的输入

后续测试至少覆盖：不同 task ID 连续执行相同 DELETE_LIST 时第一次删除、第二次全 coverage 零命中并返回 `NOOP_VERIFIED`；KEEP_LIST 的待删除补集第二次执行同样返回 `NOOP_VERIFIED`；tenant NULL 不计入命中；零命中时不创建 RowsetWriter、不分配新 Rowset ID、不调用 `modify_rowsets_without_lock()`、不保存 TabletMeta 且不进入 stale/unused GC；部分 Rowset 命中时只为变化 Rowset 生成输出但所有 coverage Rowset 均参加提交前身份校验；扫描结束至返回 NOOP 之间注入 Rowset 身份变化时返回 `STALE_ROWSET/RETRYABLE_CONFLICT` 而不是成功；`processed_through_version` 不越过 coverage 固定后新 publish 的版本；重复请求允许重复 tenant 单列扫描但不得发生无效 Rowset 重写。

### REQ-9.1-001：首轮 BE shared-nothing 手工 HTTP 验证入口及发布隔离

#### 背景

首轮开发只完成 BE shared-nothing 执行链路，FE 调度、Agent Task 下发与完成回报尚未接入。为了在单 BE 环境中手工验证真实 Tablet、多 Rowset coverage、tenant 过滤及原子提交，首轮增加一个临时 HTTP 入口。该入口会触发数据文件和 TabletMeta 变更，不允许进入正常发布二进制。

#### 已确认结论

1. 首轮提供独立的同步 `POST /api/tenant_ttl_compaction/run` 手工验证入口，不在现有 `/api/compact` 中增加 `tenant_ttl` 分支，不复用普通手工 Compaction 的进程级 `_running` 开关。
2. HTTP Action 只负责解析、规范化和校验请求，然后构造正式 `TenantTtlCompactionRequest` 并调用与未来 FE Agent Task 相同的 `EngineTenantTtlCompactionTask` 执行链。Action 中不复制 Rowset 选择、Segment 扫描、Writer 或 TabletMeta 提交逻辑。
3. 入口仅支持 shared-nothing 本地非 PK Tablet；对 shared-data、PK Tablet 或其他不在首期范围内的 Tablet 明确返回 `NOT_SUPPORTED`。
4. 请求使用已对齐的正式语义：`tablet_id`、`partition_id`、`task_id`、`tenant_column_unique_id`、`TenantFilterMode + tenants`、`policy_watermark` 和 `predicate_digest`。入口不接受 `rowset_ids/segment_ids`，不提供 `skip_lock`、`skip_rowset_id_check` 或 `force` 等绕过正式门禁的参数。
5. coverage 必须由正式执行器在取得 Tablet admission 及 base/cumulative 独占锁后固定，HTTP 调用者不能选择或排除其中的 Rowset。HTTP 任务必须经过与未来 FE 任务相同的 Tablet 级 TTL 准入、Compaction try-lock、全 coverage `is_compacting`、提交前 Rowset ID 校验及一次性 TabletMeta 提交。
6. 手工入口同步等待本次执行完成，返回 `SUCCESS/NOOP/TABLET_BUSY/TTL_ALREADY_RUNNING/STALE_ROWSET` 等真实任务结果，并返回 `processed_through_version`、coverage 中的 source/output Rowset 清单、行数及 Segment 统计。不得像现有普通 Agent Compaction handler 一样忽略底层执行错误后固定返回成功。

#### 编译宏与发布隔离

1. 手工入口使用专用编译宏，建议命名为 `STARROCKS_TENANT_TTL_MANUAL_TEST_ENDPOINT`。该宏默认不定义，不得复用 `BE_TEST`、`NDEBUG` 或通用 debug 宏作为开关。
2. CMake 中对应的 build option 必须默认为 `OFF`，只有显式构建手工验证版 BE 时才定义该宏。正常开发、CI 发布及 Release 构建均不允许默认打开。
3. HTTP Handler 实现文件的编译、`http_service.cpp` 中的 include/实例化/路由注册都必须受同一个宏保护，不能只在 Handler 内做运行时判断。
4. 普通编译产物中不应包含该 Handler 的符号和路由字符串，访问 `/api/tenant_ttl_compaction/run` 必须返回 404。仅使用默认为 false 的运行时 config 不足以满足这一发布隔离要求。
5. 专用宏只隔离风险 HTTP 入口，不应包围 `EngineTenantTtlCompactionTask`、过滤器、Writer、提交器或 Tablet 准入逻辑。这些是正式功能代码，必须在手工入口和后续 FE Agent Task 之间共用，避免测试一条与发布路径不同的实现。

#### 验收要求

- 使用默认编译参数构建 BE：手工 Handler 不参与链接，路由不存在，请求返回 404。
- 显式打开专用 build option 构建 BE：路由可用，且能对 shared-nothing Tablet 执行 DELETE_LIST 和 KEEP_LIST 任务。
- 验证入口不能指定部分 Rowset；多 Rowset coverage 仍以整体成功、整体 NOOP 或整体失败的语义返回。
- HTTP 请求错误、客户端断开、任务失败及 BE 内部取消均不得遗留 `is_compacting`、Tablet TTL owner/admission、Compaction lock 或 staged outputs。
- 需要有一项构建或 CI 检查防止发布构建误定义 `STARROCKS_TENANT_TTL_MANUAL_TEST_ENDPOINT`。

## 3. 测试用例对齐记录

### TEST-10.1-001：首轮 BE shared-nothing 分层测试与分批测试集

状态：已确认  
确认日期：2026-09-04  
覆盖范围：首轮 BE shared-nothing Tenant-TTL Compaction，不包含正式 FE 下发链路

#### 测试分层

首轮不能只依赖过滤算法单元测试或手工 HTTP 验证。测试按以下层次组织：

1. 纯逻辑单元测试：验证 `DELETE_LIST/KEEP_LIST`、NULL tenant、待删除行计数及 `keep_row_ranges`。
2. BE 存储组件测试：使用真实 Tablet、RowsetWriter、RowsetReader/TabletReader 验证单 Rowset、多 Rowset、版本连续性、原子提交和清理。
3. HTTP Action 契约测试：验证请求解析、正式任务调用、错误映射、结果字段及专用编译宏隔离。
4. SQL+HTTP 混合端到端测试：SQL 负责建表、写入和结果查询，专用 HTTP 入口负责触发首轮尚无 FE 下发链路的 Tenant-TTL Compaction。
5. 并发与故障注入测试：通过 SyncPoint/failpoint 精确制造锁竞争、coverage 变化和输出构建失败，不通过大数据或 `sleep` 猜测竞态。
6. 第二批增强测试：覆盖崩溃恢复、深度竞态、随机属性、Sanitizer、多副本、性能资源和长稳运行。

纯 SQL 在首轮不能独立触发 Tenant-TTL Compaction，因为正式 FE Agent Task 尚未实现；SQL+HTTP 是可行的端到端形式。应提供可复用测试 helper，通过 `SHOW TABLET` 等接口定位目标 Tablet 及 BE endpoint，构造正式请求并归一化动态 task/Rowset ID。该 helper 首次建设成本中等，完成后新增数据用例成本较低。开启手工 HTTP 专用宏的测试需要独立构建或专项 CI，不能依赖正常发布构建。

#### KEEP、DROP、REWRITE 的测试定义

以下分类针对 coverage 中的单个源 Rowset：

- `KEEP`：该源 Rowset 的待删除行数为零，不生成输出 Rowset，原 version 和 Rowset ID 保持不变。若 coverage 中其他 Rowset 发生变化，整个任务仍可提交，不能把单个 Rowset 的 KEEP 等同于整个任务 NOOP。
- `DROP`：该源 Rowset 的全部行都需要删除，必须生成同 version 的空 Rowset，以保持 Tablet 版本连续；不能直接删除该 version。
- `REWRITE`：该源 Rowset 只有部分行需要删除，生成同 version 的新 Rowset，只写入保留行。

只有全 coverage 的所有源 Rowset 都是 KEEP，且提交前完整身份校验仍成立时，整个任务才返回 `NOOP_VERIFIED`。

#### 第一批最小测试集

第一批用于证明功能闭环正确，是开始手工验证和后续开发验收的基础。

##### 1. 过滤与请求语义

- `DELETE_LIST` 和 `KEEP_LIST` 的精确 tenant 匹配。
- 两种模式互斥，缺失或非法 mode/list 被拒绝。
- NULL tenant 在两种模式下始终保留且不计入待删除命中数。
- 多 tenant、重复 tenant、名单顺序变化、零命中、全部命中和部分命中。
- tenant 跨 Chunk/Page/Segment 边界以及精确 VARCHAR 值比较。
- 验证首期扫描不依赖 `tenant_id`、排序键、`recordTimestamp` 或 Dictionary。

##### 2. 单 Rowset

`DELETE_LIST` 和 `KEEP_LIST` 分别覆盖以下三种结果，即 `2 种模式 × 3 种结果` 的参数化矩阵：

- KEEP：零命中并返回 `NOOP_VERIFIED`，不创建 Writer、新 Rowset、TabletMeta 修改或 stale/unused 记录。
- DROP：所有行删除，生成同 version 空 Rowset。
- REWRITE：部分行删除，生成同 version 新 Rowset，并由 TabletReader 校验所有业务列。

##### 3. 多 Rowset 混合 coverage

第一批至少包含两条多 Rowset 核心存储集成用例：一条 `DELETE_LIST`，一条 `KEEP_LIST`。每条用例必须在同一个固定 coverage 内同时出现 KEEP、DROP、REWRITE，且明确包含“含目标 tenant”和“不含目标 tenant”的不同 Rowset。

`DELETE_LIST(A)` 的最小数据模型：

| 源 Rowset | 数据 | 动作 | 元数据预期 |
| --- | --- | --- | --- |
| R1/V1 | B、C、NULL | KEEP | 不生成输出，原 Rowset ID 不变 |
| R2/V2 | A、A | DROP | 生成同 V2 的空 Rowset，使用新 Rowset ID |
| R3/V3 | A、B、NULL | REWRITE | 生成同 V3 的新 Rowset，仅保留 B、NULL |

`KEEP_LIST(B)` 的对称最小数据模型：

| 源 Rowset | 数据 | 动作 | 元数据预期 |
| --- | --- | --- | --- |
| R1/V1 | B、NULL | KEEP | 不生成输出，原 Rowset ID 不变 |
| R2/V2 | A、C | DROP | 生成同 V2 的空 Rowset，使用新 Rowset ID |
| R3/V3 | A、B、NULL | REWRITE | 生成同 V3 的新 Rowset，仅保留 B、NULL |

多 Rowset 用例必须同时验证：

- 执行前实际存在多个独立源 Rowset，不能仅以多次 INSERT 推断；专项集群应阻止后台 Cumulative Compaction 提前合并并在结束后恢复配置。
- KEEP Rowset 不创建输出且 Rowset ID 不变；DROP/REWRITE 使用同 version 的新 Rowset。
- 全部 DROP/REWRITE 输出完成 staged/load/verify 后才允许一次性批量提交，只保存一次 TabletMeta。
- 任一输出失败时所有 staged outputs 被清理，TabletMeta 和全部源 Rowset 保持不变。
- 提交后版本连续，TabletReader/SQL 查询的所有业务列与参考结果一致，NULL tenant 保留。
- coverage 中每个 `(version,rowset_id)` 都参加提交前校验，包括 KEEP/`VERIFIED_NO_CHANGE` Rowset。
- 使用不同 task ID 再执行相同谓词时，返回 `NOOP_VERIFIED`，所有 Rowset ID 不再变化。

##### 4. 提交、幂等与准入

- 提交前 Rowset ID、schema fingerprint 或 policy watermark 任一不匹配时整体不提交。
- coverage 固定后新 publish 的更高版本不进入本次任务，`processed_through_version` 不越界。
- Tablet TTL admission 状态正常转换；同 Tablet 的第二个 TTL 及 Base/Cumulative Compaction 冲突快速返回可重试 busy。
- 不同 Tablet 的 TTL 可以并发，不能误用全局串行锁。
- 任一正常、失败、取消路径都清理 `is_compacting`、owner、admission、Compaction lock 和 staged outputs。

##### 5. HTTP、构建隔离与端到端验证

- HTTP Action 覆盖 JSON 解析、正式字段校验、禁止绕过参数、任务状态映射及 coverage/result 返回字段。
- 显式开启 `STARROCKS_TENANT_TTL_MANUAL_TEST_ENDPOINT` 时接口可用；默认和发布构建中 Handler/路由均不存在且访问返回 404。
- SQL+HTTP 至少覆盖 DELETE_LIST、KEEP_LIST、不同 task ID 重复同谓词三条主线，并包含前述多 Rowset 混合场景。
- SQL 结果只能证明用户可见数据正确，不能替代 BE 存储组件对 Rowset ID、版本、原子提交、stale/unused 和清理行为的断言。

#### 第二批增强测试集

第二批不重复第一批的基本输入输出验证，重点证明并发、故障、重启、多副本和持续运行条件下仍然可靠，并为正式 FE 接入准备验证基础。

##### 1. 崩溃与重启恢复

在 coverage 捕获后、部分/全部输出生成后、header lock 前、Rowset 映射修改与 TabletMeta 保存边界、TabletMeta 保存后响应返回前以及 stale/unused 回收前注入进程退出。重启后验证只能看到完整旧状态或完整新状态，无版本缺口、半提交或不可回收的临时 artifact；已提交但未成功返回的相同谓词重试应收敛到 `NOOP_VERIFIED`。

##### 2. 深度并发竞态

覆盖同 Tablet 的相同/不同 task ID、相同/不同谓词在 PENDING/RUNNING 各阶段竞争；TTL 与 Base/Cumulative Compaction 双向竞争；Tablet shutdown/drop、overwrite publish、clone/repair 或 schema change 与 TTL 提交竞争；coverage 外新版本并发 publish；不同 Tablet 高并发。竞态必须由 SyncPoint/条件变量确定性控制。

##### 3. 全链路故障注入

覆盖 Reader/Iterator 打开、tenant 列读取、Hard Link、临时目录、磁盘空间、Writer add/flush/build、第 N 个 Rowset 输出、load/verify、提交校验、TabletMeta 保存及 HTTP 断连等失败点。所有故障统一验证无部分提交、旧数据可读、版本连续、资源和状态释放且任务可安全重试。

##### 4. 随机与属性测试

随机生成 Rowset/Segment 数量、tenant/NULL 分布、过滤模式、名单和命中密度，与简单逐行参考实现比较。至少验证名单重排/去重不改变结果、同谓词二次执行幂等、没有新数据时连续 DELETE 等价于删除集合并集，以及不同 Rowset/Segment 切分不改变最终逻辑结果。失败日志必须记录可复现随机种子。

##### 5. Sanitizer

使用 ASAN/UBSAN 验证临时文件、Rowset/Reader/Writer 引用生命周期和边界错误；使用 TSAN 重点检查 Tablet TTL state/owner、`is_compacting`、任务创建释放、Rowset 引用和 version map 的数据竞争。Sanitizer 测试适合作为专项或夜间 CI。

##### 6. 多副本专项

对同一 Tablet 的多个 shared-nothing 副本分别下发等价请求，验证逻辑数据、coverage 和处理水位一致，但不要求副本间 Rowset ID 相同。覆盖某个副本失败或在成功提交后响应前重启，随后通过重试使各副本收敛。该测试用于提前暴露未来 FE 编排中的部分成功、超时和重试问题。

##### 7. 性能、资源与长稳

建立 0%、稀疏、密集和 100% 删除比例，以及单/多 Rowset、单/多 Segment、短/长名单、冷热缓存的基线；记录扫描/读写字节、CPU、峰值内存、临时磁盘、锁持有时间和总耗时。硬性验证 `NOOP_VERIFIED` 零写入、零新 Rowset、零 TabletMeta 修改，以及全 DROP 不产生数据 Segment。长稳测试循环执行写入、TTL、普通 Compaction 和 BE 重启，检查内存、文件描述符、临时/stale 文件、TabletMeta、TTL 状态及 `is_compacting` 是否泄漏或卡死。

##### 8. HTTP 鲁棒性和发布安全

覆盖超大 tenant 列表/请求体、非法 JSON/UTF-8、重复字段、未知枚举、数值越界、高并发请求和客户端反复断连；确认只产生稳定错误或可重试 busy，不造成无界排队。发布构建持续检查手工 Handler 符号和路由字符串均不存在。

#### 分批准入边界

- 第一批完成标准：证明过滤结果、单/多 Rowset 的 KEEP/DROP/REWRITE、版本连续、批量原子提交、谓词幂等、Tablet 级互斥、失败清理和 HTTP 编译隔离均正确。
- 第二批核心完成标准：证明崩溃前后只有完整旧/新状态，确定性竞态及故障注入无部分提交，TSAN 无状态机数据竞争，随机属性测试可持续通过，多副本失败重试能够收敛，且无明显资源或长稳泄漏。

## 4. 待澄清问题

按总体方案第 7 节逐项追加。

## 5. 已确认结论索引

- `REQ-6.1-001`：首期由 FE 根据固定字典快照、评估时间和分区上界，物化出互斥的 `DELETE_LIST/KEEP_LIST` tenant 过滤模式；BE 不读取 `recordTimestamp` 或字典，不检查 `tenant_id` 和排序键，只精确扫描业务 tenant。DELETE_LIST 适合少量删除，KEEP_LIST 适合少量保留；filter mode、规范化名单与 policy watermark 共同进入 predicate digest。
- `REQ-7.1-001`：首期对每个 Segment 精确扫描业务 tenant，根据 FE 物化的 `DELETE_LIST/KEEP_LIST` 生成 `keep_row_ranges`，扫描结果再收敛为 KEEP/DROP/REWRITE；不新增 tenant_id、ShortKey、排序键或定制 ZoneMap 优化选择逻辑。
- `REQ-7.1-002`：`FilteredRowsetWriter` 是与具体过滤策略无关的目标 Rowset 组装器；`VerticalSegmentRewriter` 每次完整重写一个 Segment；`SparseRange` 表示源 Segment 内保留的 row ordinal，所有列组重放同一范围。组装器只登记已完成的 Segment，目标 ordinal 连续，最终由 `Rowset::verify()` 检查整体排序。
- `REQ-7.1-003`：原 `TenantTtlRowsetWriter` 正式命名为 `FilteredRowsetWriter`，只消费通用 `SegmentFilterPlan` 和 `keep_row_ranges`，不解析或执行 tenant TTL/SQL 谓词；首期仍只实现 Tenant-TTL 计划生成逻辑。
- `REQ-7.4-001`：首期不修改 `RowsetMetaPB`，不新增 `SegmentStatsPB` 或 `NEED_SEGMENT_STATS` 门槛。KEEP Segment 通过 `Segment::collect_runtime_stats()` 从现有 footer、ColumnMeta 和文件 artifact 运行时收集统计；REWRITE Segment 从 `SegmentWriter` finalize 结果取值；`FilteredRowsetWriter` 仅在内存中汇总并写回现有 Rowset 级字段。
- `REQ-8.1-001`：Tenant-TTL 应设置 `is_compacting`，但该 bool 只是调度标记，不是占有权。首期同 Tablet 的 TTL 任务必须串行；`TTL_IDLE / TTL_PENDING / TTL_RUNNING` 与 `owner_task_id` 在普通 Compaction 任务创建的同一同步域内提供 Tablet 级原子准入，其中 `TTL_PENDING` 只关闭本次请求取得 admission 后、取得两把执行锁前的竞争窗口，不是跨 FE 重试的 lease。Tenant-TTL 通过非阻塞 try-lock 独占 base+cumulative locks 与普通 Compaction 执行互斥；锁冲突释放 admission 并快速返回可重试 busy，不得静默跳过或等到 FE 超时。首期不实现队列或其他 try-lock 防饥饿机制。Coverage 定义为 BE 取得互斥后固定的 `[0,snapshot_end_version]` 连续可读路径及其全部 `(version,rowset_id)` 列表。首期必须支持 coverage 中的多 Rowset，逐 Rowset 完成输出 staged/load/verify 后，由 Tenant-TTL 专用 commit/wrapper 在同一个 header lock 中校验全 coverage 并调用原样保留的 `modify_rowsets_without_lock()` 批量替换，只保存一次 TabletMeta；普通 Compaction 路径不变。任一 Rowset 失败则整体不提交，`SUCCESS/NOOP` 必须证明 coverage 无遗漏。
- `REQ-8.2-001`：首期不持久化 `AppliedProof`、tenant 已处理集合或谓词历史，也不增加独立前置存在性查询。固定 coverage 的正式 tenant 单列扫描同时生成 `keep_row_ranges` 和待删除行计数；全 coverage 零命中时在 header lock 下重新校验所有 `(version,rowset_id)` 后返回终态成功 `NOOP_VERIFIED`，不生成或替换 Rowset、不保存 TabletMeta。不同 task ID 的同谓词允许重新扫描，以数据结果幂等和零无效重写换取无持久化膨胀。
- `REQ-9.1-001`：首轮 BE shared-nothing 增加独立同步 HTTP 手工验证入口，只负责构造正式 Tenant-TTL 请求并复用正式执行链，不并入现有 `/api/compact`。入口实现、链接和路由注册全部受专用 `STARROCKS_TENANT_TTL_MANUAL_TEST_ENDPOINT` 编译宏保护，对应 build option 默认 `OFF`；正常和发布构建中不包含 Handler 符号或路由，访问必须返回 404，不以运行时 config 代替编译隔离。
- `TEST-10.1-001`：测试采用纯逻辑、BE 存储组件、HTTP 契约、SQL+HTTP、确定性并发/故障注入和增强专项的分层结构。第一批必须同时覆盖单 Rowset 与多 Rowset；DELETE_LIST 和 KEEP_LIST 的多 Rowset 用例均须在同一 coverage 内包含目标 tenant 存在/不存在的 Rowset，并同时产生 KEEP、DROP、REWRITE。第二批重点覆盖崩溃恢复、深度竞态、随机属性、Sanitizer、多副本、性能资源和长稳运行。
