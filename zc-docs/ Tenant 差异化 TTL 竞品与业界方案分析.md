# Tenant 差异化 TTL 竞品与业界方案分析

## 1. 分析目的

本节重点分析业界针对 **Tenant 差异化 TTL（不同租户不同数据保留周期）** 的实现方案。

分析目标不是简单复制竞品方案，而是：

1. 了解业界在生命周期管理、过期数据检测、物理回收方面的成熟设计；
2. 分析不同方案适用边界和限制；
3. 避免在设计过程中重复踩坑；
4. 为 StarRocks 单表多租户差异化 TTL 设计提供参考。

当前需求特点：

- 多个 Tenant 共享同一张明细表；
- 不按照 Tenant 拆表；
- 不复制数据；
- 不物化 `expire_time` 字段；
- 支持百万级 Tenant；
- Tenant 可以具有不同的数据保留周期；
- 过期数据需要最终完成物理空间回收。

---

# 2. 竞品与业界方案分析

| 架构流派 | 代表系统 | 核心设计哲学 | Tenant 差异化 TTL 策略 | 过期数据清理与物理回收 | 优缺点及典型场景 | 对本方案的借鉴 |
|---|---|---|---|---|---|---|
| 行级条件 TTL + Merge/Compaction 回收 | ClickHouse | 将 TTL 作为 MergeTree 生命周期能力的一部分。TTL 支持根据字段表达式决定数据删除、冷热迁移和历史聚合，由后台 Merge 执行。 | 如果租户数量较少，可以配置多个 TTL 条件，例如普通租户 30 天、高价值租户 180 天。但百万 Tenant 无法展开为百万条 TTL 表达式，通常需要映射到有限 Retention Class，或者写入时计算过期时间。 | 数据过期后不会立即删除，而是在 Merge 时过滤。若整个 Part 或分区全部过期，则可以直接删除整个物理对象，避免逐行重写。 | **优点：**<br>1. 与列存 Compaction 天然结合；<br>2. 不产生大量 DELETE；<br>3. 整体对象过期时回收效率高。<br><br>**缺点：**<br>1. 删除时效依赖 Merge 调度；<br>2. 多 TTL 混合导致部分重写；<br>3. 高基数 Tenant 不适合直接表达。<br><br>**场景：**日志、时序、分析型明细表。 | 借鉴：<br><br>1. Compaction 中执行行级 TTL 判断；<br>2. 整 Segment / Rowset / Partition 过期时快速删除。<br><br>避免：<br><br>1. 百万 Tenant TTL 规则直接展开；<br>2. 依赖 Tenant 物理分区。 |
| 后台分布式扫描 + 批量事务删除 | TiDB、CockroachDB | 数据库内部 TTL Scheduler 周期扫描过期数据，通过批量 DELETE 完成生命周期管理。 | 通过生成列或者过期表达式保存 TTL 信息。动态 Tenant TTL 通常需要提前计算 expire 时间，或者数据库内部维护规则缓存。 | TTL Worker 按 Region、Range 等粒度拆分任务，通过批量事务删除。支持任务暂停、限速、重试和监控。 | **优点：**<br>1. 行级精度高；<br>2. 任务治理能力完善；<br>3. 支持在线控制。<br><br>**缺点：**<br>1. DELETE 产生事务和日志开销；<br>2. 索引维护成本高；<br>3. 删除完成前数据可能仍存在。<br><br>**场景：**订单、Session、中间状态数据。 | 借鉴：<br><br>1. TTL Scheduler；<br>2. 任务拆分；<br>3. checkpoint；<br>4. 限速和失败恢复。<br><br>不采用逐行 SQL DELETE，而复用 StarRocks Delete Predicate / Compaction。 |
| 写入时逐行 TTL + Tombstone | Cassandra | TTL 是写入数据的一部分。数据过期后生成 Tombstone，后续 Compaction 清理。 | 写入时根据 Tenant 查询 TTL，并将 TTL 写入每条记录。不同 Tenant 可以拥有不同 TTL。 | TTL 到期后产生 Tombstone，满足 gc grace 后由 Compaction 删除。 | **优点：**<br>1. 不需要后台扫描规则；<br>2. 支持任意粒度 TTL。<br><br>**缺点：**<br>1. 每条数据携带 TTL 信息；<br>2. Tombstone 增加读取和 Compaction 压力；<br>3. 规则修改难影响历史数据。<br><br>**场景：**KV、缓存、Session。 | 借鉴：<br><br>1. TTL 规则版本化；<br>2. 明确规则修改是否影响历史数据。<br><br>不采用：<br><br>1. 行级 Tombstone；<br>2. 每行保存 TTL 元数据。 |
| 绝对过期时间字段 + TTL Index | MongoDB | 每条文档保存 expireAt，通过 TTL Index 找到过期数据。 | 写入时计算：<br><br>`expireAt = event_time + tenant_ttl`<br><br>不同 Tenant 可以拥有不同 expireAt。 | TTL Monitor 根据索引扫描过期文档，并异步删除。 | **优点：**<br>1. 过期定位准确；<br>2. 支持每行不同生命周期。<br><br>**缺点：**<br>1. 增加字段；<br>2. 增加索引维护；<br>3. TTL 修改需要更新历史数据。<br><br>**场景：**文档、验证码、Session。 | 借鉴：<br><br>1. 绝对过期时间思想；<br>2. 基于元数据快速判断过期。<br><br>不直接增加业务 expire_time 列，可以考虑 Segment / Rowset 元数据。 |
| Tenant 原生规则控制面 + Compactor | Grafana Loki | Tenant 是一级管理对象，通过默认规则、Tenant Override、Selector 和优先级管理生命周期。 | 支持 per-tenant retention override，也支持 Tenant 内不同日志流不同保留周期。 | Compactor 删除过期 Chunk，同时维护索引和删除 Marker，后续异步清理对象。 | **优点：**<br>1. Tenant 生命周期管理成熟；<br>2. 支持规则覆盖；<br>3. 支持运行时修改。<br><br>**缺点：**<br>1. 依赖 Tenant 物理隔离；<br>2. 不适合多个 Tenant 混存在同一个 Segment。<br><br>**场景：**日志、多租户可观测平台。 | 重点借鉴：<br><br>1. Tenant TTL 控制面；<br>2. 默认规则；<br>3. 覆盖规则；<br>4. Rule Version；<br>5. 删除 Marker。 |
| 索引/分区/Segment 生命周期 | Elasticsearch ILM、Apache Doris | 通过删除整个索引、分区或 Segment 实现生命周期管理。 | 通常按照 Tenant 或 Retention Class 建立独立生命周期策略。 | 到期后直接删除整个物理对象。 | **优点：**<br>1. 回收效率最高；<br>2. 不需要扫描数据。<br><br>**缺点：**<br>1. 无法删除对象内部部分 Tenant；<br>2. Tenant 数量大时元数据爆炸。<br><br>**场景：**日志索引、时间分区表。 | 借鉴：<br><br>1. 整 Partition 删除；<br>2. 整 Rowset 删除；<br>3. 整 Segment 删除。<br><br>避免 Tenant 级物理拆分。 |

---

# 3. 综合分析

## 3.1 最接近 StarRocks 的方案

从存储模型来看：

**ClickHouse TTL + MergeTree Merge**

是最接近 StarRocks 的方案。

原因：

- 都是列式存储；
- 都通过后台 Compaction/Merge 完成数据整理；
- 都存在 Segment/Part/Rowset 等物理数据单元；
- 都需要避免大量 DELETE。

因此：

> StarRocks Tenant TTL 最合理方向不是后台 SQL DELETE，而是将 TTL 判断融合到 Compaction 生命周期管理中。

---

## 3.2 最值得借鉴的设计组合

综合业界方案：

| 能力 | 借鉴系统 |
|---|---|
| Tenant 规则管理 | Loki |
| TTL 执行机制 | ClickHouse |
| 任务调度和恢复 | TiDB / CockroachDB |
| 快速物理删除 | Elasticsearch ILM / Doris |
| TTL 版本化思想 | Cassandra |

最终设计方向：

> Loki 式 Tenant Rule Control Plane + ClickHouse 式 Compaction TTL + TiDB 式任务治理 + Doris 式物理快速回收

---

# 4. 关键设计启示

## 4.1 TTL 到期、查询不可见、物理删除需要解耦

业界方案说明：

- ClickHouse：等待 Merge；
- TiDB：等待 TTL Worker；
- MongoDB：等待 TTL Monitor。

因此：

不能仅依赖 Compaction。

推荐：

```
TTL Scheduler
      |
      v
生成 TTL Delete Predicate
      |
      v
Publish Version
      |
      v
TabletReader 查询过滤
      |
      v
Compaction 物理删除
```

形成：

```
逻辑删除
    +
物理回收
```

两个阶段。

---

## 4.2 Tenant TTL 规则不能 DDL 化

不推荐：

```
tenant_001 TTL 30d
tenant_002 TTL 60d
tenant_003 TTL 90d
...
```

原因：

- Tenant 数量可能百万；
- FE 元数据爆炸；
- Schema 变更成本巨大。

推荐：

```
TTL Rule Table

tenant_id
retention_seconds
rule_version
effective_time
status
```

规则作为数据管理。

---

## 4.3 TTL 规则需要版本化

参考 Loki：

每次 TTL Job 固定：

```
rule_version = 10086
evaluation_time = now()
```

任务执行期间不动态读取规则。

避免：

扫描过程中：

```
tenant A:
开始扫描 -> TTL=30天

中途修改:

TTL=90天

导致同一个任务结果不一致
```

---

## 4.4 多级回收路径

推荐：

### Level 1：Partition 快速删除

如果：

```
max_expire_time < now
```

直接删除 Partition。


### Level 2：Rowset 删除

如果：

```
Rowset 全部 Tenant 数据过期
```

直接删除 Rowset。


### Level 3：Segment Compaction

如果：

```
部分 Tenant 数据过期
```

Compaction 重写 Segment。


### Level 4：行级过滤

最终：

```
tenant_id + event_time
```

逐行判断。

---

# 5. 最终建议

Tenant 差异化 TTL 不应该设计成：

- Tenant 独立表；
- Tenant 独立分区；
- Tenant 独立 Tablet；
- Tenant 独立 TTL Job。

这些方案无法支撑百万级租户。

推荐架构：

```
                Tenant TTL Rule Store
                         |
                         |
                  TTL Scheduler
                         |
              Rule Snapshot Version
                         |
        --------------------------------
        |                              |
 Query Visibility                Compaction
        |                              |
Delete Predicate              Physical Reclaim
        |                              |
 TabletReader                  Segment/Rowset Delete
```

核心思想：

> 规则按 Tenant 管理，执行按物理数据组织；逻辑删除保证一致性，Compaction 完成最终回收。

该方案能够同时满足：

- 百万级 Tenant；
- 单表共享存储；
- 不复制数据；
- 不增加业务字段；
- 查询一致性；
- 故障恢复；
- 最终空间回收。