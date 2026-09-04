# Compaction 行级 TTL 简要方案

## 1. 方案定位

本方案将行级 TTL 下沉到 StarRocks 存储引擎。在 Compaction 处理 Rowset 和 Segment 时，根据固定策略快照判断每一行是否已经过期；过期行不写入新 Segment，从而在合并或 Rewrite 过程中完成物理回收。

本方案没有独立的 TTL 时间列或预先写入的 `expire_at` 列。每行是否过期由以下信息在任务执行时计算：

- 行中的业务 tenant：`tenant`。
- 行中的业务时间：`recordTimestamp`。
- 策略字典中的 `tenant -> retention_days`；tenant 不在字典中时使用 `dictionary_ttl` 配置的默认保留天数。
- 本次任务固定的策略版本和评估时间。

`tenant_bucket`、生成列 `tenant_id`、`ORDER BY(tenant_id, recordTimestamp)`、ZoneMap 和 ShortKey 都是分区裁决、查询过滤、数据聚簇或 Segment 裁剪的加速手段，不是行级 TTL 正确性的必要输入。行级 TTL 的最终删除判断仍以业务 `tenant` 和 `recordTimestamp` 为准。

## 2. 分区隔离与三态裁决

分区键中引入生成列 `tenant_bucket`，使用 tenant 哈希将数据分到 64 个桶。在尽量控制分区数量的同时获得部分 tenant 隔离性。

当差异化 TTL 的 tenant 数量不多时，FE 根据策略表加载 `tenant -> retention_days`，结合 bucket/day 分区范围，对相关分区做保守三态判定：

1. **全部保留**：分区内数据均未达到过期时间，跳过该分区。
2. **全部删除**：分区内数据均已过期，走分区删除的快速路径。
3. **部分保留**：无法证明全部保留或全部删除，向 BE 下发 Tenant-TTL Compaction，由 BE 执行行级过滤和 Rowset Rewrite。

按 64 个 tenant bucket、保留 365 个日分区估算，分区数量上限为：

```text
64 * 365 = 23,360
```

该数量低于社区建议的 10 万以内安全阈值。

## 3. 表结构示例

以下为方案对应的目标表结构示例：

```sql
CREATE TABLE `dns_log_5_tenant_id_verify` (
    `qr` int(11) NOT NULL DEFAULT "0"
        COMMENT "标志位(请求0，响应1)",

    `queries` varchar(65533) NULL
        COMMENT "查询问题(查询名，多个查询以','分隔)",

    `tenant` varchar(65533) NULL COMMENT "",

    `recordTimestamp` bigint(20) NOT NULL DEFAULT "0" COMMENT "",

    `rawMsg` varchar(65533) NULL COMMENT "",

    `devUId` int(11) NULL COMMENT "",

    `bucketTimestamp` bigint(20) NULL
        AS (floor(`recordTimestamp` / 600)) * 600
        COMMENT "分钟级别int时间戳",

    `tenant_bucket` bigint(20) NULL
        AS (bitand(xx_hash3_64(`tenant`), 63))
        COMMENT "tenant哈希桶，取值范围0-63",

    `tenant_id` bigint(20) NULL
        AS (CAST(`tenant` AS BIGINT))
        COMMENT "tenant数值生成列"
)
ENGINE=OLAP
DUPLICATE KEY(`qr`, `queries`)
PARTITION BY (
    `tenant_bucket`,
    from_unixtime(`recordTimestamp`, '%Y%m%d')
)
DISTRIBUTED BY HASH(`recordTimestamp`) BUCKETS 3
ORDER BY(`tenant_id`, `recordTimestamp`)
PROPERTIES (
    "bloom_filter_columns" = "queries",
    "compression" = "ZSTD(3)",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "1"
);
```

该验证表展示生成排序列和查询优化的目标结构。启用 Tenant-TTL 时还需要配置：

```sql
"compaction_retention_condition" =
    "dictionary_ttl('dns_tenant_retention_dict', 180)"
```

`tenant_bucket` 是分区隔离加速列，不代表 tenant 的业务身份。哈希碰撞不影响行级过滤的正确性。

`tenant_id` 是为规避 VARCHAR Short Key 使用受限而增加的数值生成列。将其放在 `ORDER BY` 第一列，使以 tenant 为条件的查询和 Tenant-TTL Segment 分类有机会利用数值 Short Key。它同样不是业务身份：`CAST(VARCHAR AS BIGINT)` 可能产生 NULL，也可能把不同字符串转换为相同数值，因此 BE 最终仍必须保留或读取业务 `tenant`，按固定字典快照判断该行的保留天数。

## 4. 当前查询裁剪能力

### 4.1 已具备分区裁剪能力

对于查询：

```sql
WHERE tenant = 'xxx'
  AND recordTimestamp >= ...
  AND recordTimestamp < ...
```

当前优化器已经能够通过 `tenant = 'xxx'` 计算相应的 `tenant_bucket`，并结合 `recordTimestamp` 裁剪 bucket/day 分区。示例执行计划显示：

```text
partitionsRatio=2/11
tabletsRatio=6/6
```

这表示分区已裁剪到该 tenant bucket 下相关的两个日期分区；Tablet 数量在保留分区内没有进一步裁剪。

### 4.2 ZoneMap 可过滤，但尚不能利用 Short Key

相关查询当前可以利用 ZoneMap 过滤大量数据，但原始谓词只包含 VARCHAR 业务列 `tenant`，而第一排序列已经调整为生成列 `tenant_id`。在没有派生出 `tenant_id` 谓词时，BE 无法用该排序前缀上的 Short Key 缩小候选范围。

## 5. FE 生成列谓词派生

### 5.1 目标

当业务查询只写：

```sql
WHERE tenant = '2'
```

FE 在确认表结构和生成列表达式满足安全条件后，保留原始谓词并追加：

```sql
tenant = '2' AND tenant_id = 2
```

这样 BE 可以复用现有 Short Key 能力，无需修改 Short Key 的 VARCHAR 编码和比较实现。

### 5.2 新增派生器

建议新增：

```text
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/
GeneratedColumnPredicateDeriver.java
```

核心职责是：

1. 找到第一排序列 `tenant_id`。
2. 确认它是生成列，且表达式是受支持的 `CAST(tenant AS BIGINT)`。
3. 从原始谓词中寻找 `tenant = 常量`、`常量 = tenant` 或常量 `IN`。
4. 使用与生成列表达式一致的语义转换常量。
5. 转换全部成功且没有溢出时，保留原谓词并追加相应的 `tenant_id` 等值或 IN 谓词。

概念伪代码如下：

```java
public static ScalarOperator tryAddDerivedPredicates(
        ScalarOperator predicate,
        LogicalScanOperator scan) {
    // 1. 找到第一排序列 tenant_id。
    // 2. 验证生成表达式为 CAST(tenant AS BIGINT)。
    // 3. 提取 tenant = 常量、常量 = tenant 或常量 IN。
    // 4. 对常量执行相同的 CAST，失败或溢出则不派生。
    // 5. 保留原谓词并追加 tenant_id 谓词。
}
```

必须保留原始 `tenant` 谓词。派生谓词只用于扩大索引过滤能力，不能替代业务谓词；即使 `'02'` 和 `'2'` 都转换为 `tenant_id=2`，原始 `tenant='2'` 仍会排除不相等的业务 tenant，保证查询结果不变。

### 5.3 接入谓词下推

在现有 scan predicate 默认重写完成后接入派生器：

```java
predicates = scalarOperatorRewriter.rewrite(
        predicates,
        ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);

predicates = GeneratedColumnPredicateDeriver.tryAddDerivedPredicates(
        predicates,
        logicalScanOperator);
```

接入位置为：

```text
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/
PushDownPredicateScanRule.java
```

现有 `PruneScanColumnRule` 会把 scan predicate 引用的列加入输出列集合，因此派生谓词引用 `tenant_id` 后，即使业务 SELECT 列表没有选择它，也不会被错误裁掉。对应代码位于：

```text
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/
PruneScanColumnRule.java
```

### 5.4 表属性和首期边界

建议使用显式表属性控制：

```text
"enable_generated_sort_key_predicate_derivation" = "true"
```

只有同时满足以下条件才允许派生：

- 表属性已开启。
- 第一排序列是生成列。
- 生成列表达式属于经过验证的安全形式。
- 首期只支持 `CAST(VARCHAR AS BIGINT)`。
- 原始谓词是等值或常量 IN。
- 所有相关常量转换成功且没有溢出。

首期支持：

```sql
tenant = '2'
'2' = tenant
tenant IN ('2', '10', '20')
```

首期不支持：

```sql
tenant > '2'
tenant LIKE '2%'
tenant = another_column
-- 以及无法安全分解的复杂 OR 表达式
```

不满足派生条件时必须保持原执行语义，只是不使用生成排序列上的 Short Key，不能报错或改变查询结果。

## 6. FE 与 BE 执行流程

FE 按策略表加载 `tenant -> retention_days`，结合 bucket/day 分区值完成三态判定：

- 全部保留：不下发物理删除任务。
- 全部删除：直接删除分区。
- 部分保留：下发携带 tenant 范围、时间范围、策略版本、评估时间和目标数据版本的 Tenant-TTL Compaction。

BE 对任务固定的数据 coverage 执行过滤和 Rowset Rewrite。任务成功后 FE 记录已完成的版本水位；任务失败、数据版本变化或 Rowset 身份变化时，FE 使用相同任务语义幂等重试。

首期必须支持一个 Tablet coverage 内存在多个当前可见 Rowset。BE 对 coverage 中所有 Rowset 逐个完成 Segment 分类和输出构建，所有输出验证成功后一次批量提交 TabletMeta；不允许因为 Rowset 数量大于 1 而拒绝任务，也不允许部分 Rowset 已提交、其余留待下次处理。

## 7. tenant_id 排序带来的 Segment 级优化

将生成列 `tenant_id` 和 `recordTimestamp` 加入排序键，可以规避 VARCHAR Short Key 的限制，并让数值 tenant id 和相近时间的数据尽量聚集，从而提高查询过滤效率、整 Segment KEEP/DROP 的概率并降低 Rewrite 的读写放大。

对于 Segment 互不交叠、且按 `(tenant_id, recordTimestamp)` 聚簇的 DUP_KEYS Rowset，可以应用以下优化：

1. 使用 Segment ZoneMap 和数值 `tenant_id` 范围保守缩小候选 Segment。
2. 在排序、统计信息和 tenant identity 条件足以形成严格证明时，结合精确的首尾 `tenant_id`、时间边界，将 Segment 粗分为 `KEEP / DROP / NEED_EXACT_SCAN`。
3. KEEP Segment 通过 Hard Link 零拷贝复用。
4. DROP Segment 不写入目标 Rowset。
5. 无法整段证明的 Segment 精确扫描 `tenant` 和 `recordTimestamp`；全部保留时收敛为 KEEP，全部过期时收敛为 DROP，部分保留时执行 Vertical Rewrite。

上述条件是性能优化条件，不是 Tenant-TTL 的固定准入条件。表没有生成 `tenant_id`、没有使用 `(tenant_id, recordTimestamp)` 排序、Segment 存在 tenant 交错或缺少可用统计时，BE 必须降级为精确扫描，不能仅因无法使用 Segment 裁剪而拒绝任务。

`CAST(tenant AS BIGINT)` 不是天然的一一映射。对于普通查询，始终保留原始 `tenant` 谓词即可保证派生过滤安全；对于 Tenant-TTL 的整 Segment DROP，若仅凭 `tenant_id` 推断业务 tenant，则必须另外证明 identity 条件。没有该证明时，`tenant_id` 只能缩小候选范围，最终必须扫描业务 `tenant` 后才能做删除决定。

过滤后的行是源 Segment 行序列的子序列，因此不会改变表的实际排序语义。Vertical Rewrite 的首列组需要包含该表真实定义的全部 sort key；当前示例中就是 `tenant_id`、`recordTimestamp`，但 Tenant-TTL 功能本身不要求所有表都采用这组排序键。

一个 Tenant-TTL 任务可以覆盖多个 Rowset；这里的 Segment 级优化在 coverage 内逐 Rowset 应用，并不把任务限制为单个 consolidated Rowset。

## 8. Rowset 原子替换与回收

BE 完成新 Rowset 的构建、同步和校验后，在同一个 header lock 临界区校验固定 coverage 中所有源 Rowset 的 `(version, rowset_id)`，然后批量替换 Tablet 元数据并只保存一次 TabletMeta。

提交前已打开的查询仍可通过旧 Rowset 引用读取旧文件。被替换 Rowset 进入 stale/unused 链路，待引用归零后由 GC 回收。

存算一体部署中，KEEP Segment 可以通过同 Tablet 目录内的 Hard Link 减少复制。Hard Link 是本地文件复用优化，不影响行级 TTL 的判定语义。

## 9. 存算分离适配

分区级三态优化与底层存储形态无关，可同时用于存算一体和存算分离架构。

Rowset Tenant-TTL Rewrite 也适用于存算分离，但需要在 shared-data/Lake Compaction 体系内并行实现，不能直接复用存算一体的 Compaction 代码，原因是当前两套架构使用不同的 Compaction 和元数据提交链路。

存算一体通过 Hard Link 复用 KEEP Segment；存算分离没有相同的本地文件复用条件，应通过对象文件/Segment 的元数据引用复用实现等价优化，并遵守 shared-data 的事务提交、版本发布和垃圾回收协议。
