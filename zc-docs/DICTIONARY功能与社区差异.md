# DICTIONARY 功能与社区差异

> 用途：记录本地 StarRocks 4.0.11 与社区后续 DICTIONARY 实现的差异，供 Tenant-TTL 及其他依赖 `DICTIONARY_GET` 的功能在排查问题、升级或决定是否 cherry-pick 社区修复时参考。
>
> 最近核对日期：2026-09-10。

## 1. 结论摘要

社区 `main` 相比本地 4.0.11，已经补充了多项 DICTIONARY 正确性、故障隔离和 FE 元数据一致性修复，但尚未形成新的完整生命周期能力。

已经明显改善的部分包括：

- 修复 `dictionary_refresh_interval` 溢出和禁用自动刷新后被意外重新调度的问题。
- 刷新时跳过不可用 BE/CN，避免单节点故障让整个 DICTIONARY 进入 `CANCELLED`。
- 修复 `dictionary_get` 的假 NULL 拒绝、子表达式错误传播和多 KEY SQL 序列化问题。
- 源表被删除后返回正常语义错误，不再触发 FE NPE。
- DICTIONARY 的 FE 状态更新改用 WAL 成功后再更新内存，增强 Leader/Follower 一致性。
- 修复内部刷新使用的 Warehouse、SessionVariable、线程上下文和 hostname 解析问题。
- `main` 的 `SHOW DICTIONARY` 已接入通用 `WHERE`、`ORDER BY`、`LIMIT` 子句。

截至当前社区 `main`，以下关键能力仍然没有实现：

- 没有 `ALTER DICTIONARY`，五个 DICTIONARY 属性不能在线修改。
- 没有源表或源列的 DDL 反向依赖，删除、改名源表列不会被 DICTIONARY 阻止。
- BE/CN 缓存仍然是纯内存状态，节点重启或重新上线后不会自动回灌。
- `dictionary_refresh_interval = 0` 时，节点或整个实例重启后仍需手工 `REFRESH DICTIONARY`。
- 仍然是全量查询、全量构建，没有增量刷新、CDC 或 watermark。
- 单节点内部替换是原子的，但跨 BE/CN 的刷新提交仍不是集群原子事务。
- `SHOW DICTIONARY` 仍不提供逐节点缓存版本和健康状态。

因此，本地此前关于源表 DDL、BE 重启、`dictionary_refresh_interval = 0`、`dictionary_memory_limit` 在线修改的判断，不会因为社区 `main` 的这些修复而发生根本变化。

## 2. 比较口径

### 2.1 本地基线

- 本地分支：`4.0.11-zc_docs`
- 本地 HEAD：`e9a99fff53b2aeda94169cb2f89a5ed106f5d9a3`
- 官方 `4.0.11` tag 对应 commit：`9559176fab6e2cb885779f1e7b680133d58d6972`
- 本地 DICTIONARY 核心实现、测试和文档与官方 `4.0.11` tag 无差异。

主要本地源码入口：

- `fe/fe-core/src/main/java/com/starrocks/catalog/Dictionary.java`
- `fe/fe-core/src/main/java/com/starrocks/catalog/DictionaryMgr.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/DictionaryAnalyzer.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/ExpressionAnalyzer.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/parser/StarRocks.g4`
- `be/src/storage/dictionary_cache_manager.{h,cpp}`
- `be/src/exprs/dictionary_get_expr.{h,cpp}`

### 2.2 社区比较对象

- 本地可用的 `upstream/main` 快照：`4b9e201b6847721810d2ec0c78799b1ebe31d356`，提交日期为 2026-09-09。
- 2026-09-10 核对的官方 `branch-4.0` HEAD：`859bf1766b5699d0323edd5a6cabd0657d4940f1`。
- 截至核对日，最新已发布 4.0 版本是 4.0.14，发布日期为 2026-08-11：
  - <https://docs.starrocks.io/releasenotes/release-4.0/>

本文严格区分以下三种状态：

1. **已进入社区 `main`**：不代表已经进入 4.0 维护分支。
2. **已进入当前 `branch-4.0`**：可能晚于最新发布 tag，尚未出现在正式 4.0.x 版本中。
3. **已进入已发布 4.0.x**：可以从对应发布 tag 或正式镜像获得。

同时排除了低基数字典、全局字典、Parquet Dictionary Encoding 等名称相近但不属于 `CREATE DICTIONARY` 对象的改动。

## 3. 社区已完成的功能完善和优化

### 3.1 修复刷新周期溢出和意外自动刷新

社区 PR：

- [#76634 Fix dictionary refresh interval overflow and unintended auto refresh](https://github.com/StarRocks/starrocks/pull/76634)
- `main` commit：`2bc2f5c6d127e2793a9aeff4db5db7aa39b22b33`

本地 4.0.11 的问题：

```java
refreshInterval = Integer.parseInt(value) * 1000;
```

- `dictionary_refresh_interval` 的单位从秒转为毫秒时使用 `int` 乘法。
- 大于 2,147,483 秒（约 24.8 天）的配置可能溢出为负数或错误的较小正数。
- `setRefreshing()` 会无条件重设 `nextSchedulableTime`。
- 当 `refresh_interval <= 0` 时，手工刷新或创建时 warm-up 可能把本应禁用的自动刷新重新调度，形成周期刷新循环。

社区 `main` 的修复：

- 先用 `long` 解析秒数，再检查转换成 `int` 毫秒后是否越界。
- 越界时在创建阶段返回明确错误，不再静默溢出。
- `refreshInterval <= 0` 时将 `nextSchedulableTime` 固定为 `Long.MAX_VALUE`。
- `setRefreshing()` 只在 `refreshInterval > 0` 时重新计算下一次刷新时间。
- 可以在 image load/replay 时修复旧版本留下的错误调度时间。

版本状态：

- 已进入 `main`。
- 已进入 `branch-4.1`。
- 面向 `branch-4.0` 的 backport PR [#76693](https://github.com/StarRocks/starrocks/pull/76693) 已关闭、未合入。
- 当前 `branch-4.0` 仍是 4.0.11 的旧调度逻辑。

### 3.2 刷新时跳过不可用 BE/CN

社区 PR：

- [#76496 Skip unavailable nodes when refreshing dictionary cache](https://github.com/StarRocks/starrocks/pull/76496)
- `main` commit：`c84e56b5e51b946fd40bfcd1350b80fa0b25999c`

本地 4.0.11 使用：

```java
getBackends();
getComputeNodes();
```

它会把仍在元数据中但已经宕机、停止或退役的节点加入 BEGIN、REFRESH、COMMIT 广播目标。任一节点 RPC 失败，整次刷新立即失败，DICTIONARY 可能被设置为 `CANCELLED`，进而导致所有查询侧 `dictionary_get` 被 FE 拒绝。

社区 `main` 改成：

```java
getAvailableBackends();
getAvailableComputeNodes();
```

效果：

- 单个不可用节点不再拖垮其他存活节点上的刷新。
- DICTIONARY 可以继续在存活节点上使用。
- 恢复节点只有在**下一次刷新**时才会重新纳入目标集合。

边界：

- 节点在刷新过程中途故障，仍可能让刷新失败。
- 节点恢复不会触发专门的自动补载。
- `dictionary_refresh_interval = 0` 时没有“下一次周期刷新”，仍需手工刷新。

版本状态：仅 `main`，当前 `branch-4.0` 仍使用全部节点。

### 3.3 修复 `dictionary_get` 对 Nullable Key 的假 NULL 拒绝

社区 PR：

- [#76881 Fix dictionary_get false NULL rejection](https://github.com/StarRocks/starrocks/pull/76881)
- `main` commit：`7312882b07639dde3c12e0f8ee5f2f64af72835c`
- 4.0 backport：[#76980](https://github.com/StarRocks/starrocks/pull/76980)

本地 4.0.11 只要 `column->has_null()` 返回 true，就拒绝这个 Key。但 `has_null` 可能是保守缓存标志：即使实际 Null Bitmap 全为 0，仍可能保持 true。例如由 `COALESCE` 等表达式产生的 Nullable 列就可能出现这种情况。

社区修复后：

```cpp
if (column->has_null() && ColumnHelper::count_nulls(column) > 0) {
    // reject
}
```

只有实际包含 NULL 的参数才会被拒绝；确认没有 NULL 后会直接解包 Nullable 数据列。

版本状态：

- 已进入 `main`。
- 已通过 #76980 进入 `branch-4.0`。
- 已出现在 4.0.14 tag。

### 3.4 子表达式错误通过 Status 返回，不再可能 abort BE

社区 PR：

- [#77442 Propagate child expression errors instead of aborting the BE](https://github.com/StarRocks/starrocks/pull/77442)

本地 4.0.11：

```cpp
columns[i] = _children[i]->evaluate(context, ptr);
```

社区后续实现：

```cpp
ASSIGN_OR_RETURN(columns[i], _children[i]->evaluate_checked(context, ptr));
```

子表达式求值失败时会把错误返回给查询，不再通过 unchecked 路径演变为 BE abort。

版本状态：

- 当前 `main` 已包含。
- 当前 `branch-4.0` 已包含。
- 4.0.14 tag 仍使用旧的 unchecked 调用，因此属于 4.0.14 之后进入维护分支、尚未正式发布的修复。

### 3.5 修复多 KEY `dictionary_get` 的 SQL 序列化

相关 Issue 和 PR：

- [#78417 partition_retention_condition serializes null_if_not_exist twice for multi-key dictionary_get](https://github.com/StarRocks/starrocks/issues/78417)
- [#78706 Serialize dictionary_get keys by keySize, not by children count](https://github.com/StarRocks/starrocks/pull/78706)
- `main` commit：`955dd5f46ddd095af4e013325c075676bbaa55ed`
- 4.0 backport：[#78794](https://github.com/StarRocks/starrocks/pull/78794)

问题并不在普通多 KEY lookup 本身，而在“表达式转 SQL，再重新解析”的路径。典型场景包括：

- `partition_retention_condition`
- Tenant-TTL 内部构造、保存并重新解析 TTL predicate
- 其他需要持久化或 explain/deparse `DictionaryGetExpr` 的路径

旧实现通过 `children.size()` 猜测最后一个 child 是否是 `null_if_not_exist`。对于两个或更多 KEY：

- 显式传入 `true/false` 时，布尔参数可能被输出两次。
- 未显式传入布尔参数时，最后一个真实 KEY 可能被误判成可选参数并丢失。

社区修复后使用分析阶段记录的 `keySize` 确定 KEY 的准确边界，再只追加一次 `null_if_not_exist`。

版本状态：

- 已进入 `main`。
- #78794 已进入当前 `branch-4.0`。
- 合入时间晚于 4.0.14，尚不能认为任何已发布的 4.0.x 镜像已经包含。

这项修复与当前 Tenant-TTL 的 `(table_name, tenant)` 复合 KEY 场景直接相关，属于本地 4.0.11 优先级最高的候选 backport。

### 3.6 源表被删除时返回明确错误，不再触发 FE NPE

社区 PR：

- [#71109 Fix NPE in visitDictionaryGetExpr when dictionary backing table is dropped](https://github.com/StarRocks/starrocks/pull/71109)

本地 4.0.11 在确认 `table == null` 后，错误信息仍然调用：

```java
table.getName()
```

因此本来应返回的 `SemanticException` 会变成 `NullPointerException`。

社区改为从 DICTIONARY 元数据中的 `dictionary.getQueryableObject()` 取得源表名，能够返回明确的“源表不存在”错误。

需要特别注意：这只是错误处理修复，不是 DDL 依赖保护。源表和源列仍然可以被删除或重命名。

版本状态：已进入 `main`，当前 `branch-4.0` 仍保留旧的 `table.getName()` 实现。

### 3.7 FE DICTIONARY 元数据和状态改为 WAL 成功后再应用

社区 PR：

- [#66207 Apply memory state only after WAL succeeds](https://github.com/StarRocks/starrocks/pull/66207)
- `main` commit：`80b749ff418077a935481fcf2f6acfe55b787a43`

社区 `main` 将以下 DICTIONARY 操作改成写 WAL 成功后通过 `WALApplier` 更新 Leader 内存：

- CREATE / DROP DICTIONARY
- `nextDictionaryId` / `nextTxnId`
- `REFRESHING`
- `COMMITTING`
- `FINISHED`
- `CANCELLED`
- 上次成功刷新版本和错误信息

主要收益：

- journal 写失败时不会先改变 Leader 内存，降低日志和内存状态分叉风险。
- Leader/Follower 使用增量状态日志回放，增强 FE HA 一致性。
- Leader 切换期间更容易恢复未完成的刷新任务。

这项改动只解决 FE 元数据和状态一致性，不会：

- 持久化 BE/CN 的字典缓存。
- 保证所有 BE/CN 同时提交缓存版本。
- 自动修复单个重启节点的缓存。

这是较大范围的框架性修改，不建议在 4.0.11 上只 cherry-pick 单个 DICTIONARY 文件。

### 3.8 后台刷新上下文修复

相关社区改动：

- [#74385 Fix session variable override caused by SET WAREHOUSE](https://github.com/StarRocks/starrocks/pull/74385)

社区 `main` 的 `DictionaryMgr.RefreshDictionaryCacheWorker` 主要增加了：

- 先设置 background warehouse，再设置刷新任务需要的 SessionVariable。
- 显式关闭 Materialized View Rewrite，避免刷新源查询被不期望地重写。
- 使用 `ConnectContext.bindScope()` 管理线程上下文，避免 ThreadLocal 泄漏或串任务。

本地 4.0.11 是先修改 SessionVariable，最后调用 `setCurrentWarehouse()`。后者会替换 SessionVariable，可能丢掉前面设置的 pipeline、timeout、profile 等内部参数。

版本状态：`main` 和 `branch-4.1` 已修复，当前 `branch-4.0` 仍是旧顺序。

### 3.9 FE 预解析节点 hostname

社区 PR：

- [#78390 Resolve compute node hostnames in FE before shipping them to BE](https://github.com/StarRocks/starrocks/pull/78390)
- `main` commit：`73758c7e7c82e0555a3c92e3a90a5a3e750b8021`

社区 `main` 在 `DictionaryCacheSink` 构造阶段通过 FE DNS Cache 尝试把 BE/CN hostname 解析为 IP，再随刷新 Sink 下发给执行节点。

收益：

- 避免每个执行 BE 在每次刷新时，对每个目标节点重复执行未缓存的 `getaddrinfo`。
- 对 Kubernetes Service hostname 较多的部署，可以减少 DNS 压力和刷新抖动。
- DNS 解析失败时回退为原 hostname，不会比旧行为更差。

### 3.10 `SHOW DICTIONARY` 接入通用过滤语法

本地 4.0.11 grammar：

```antlr
showDictionaryStatement
    : SHOW DICTIONARY qualifiedName?
    ;
```

社区 `main` grammar：

```antlr
showDictionaryStatement
    : SHOW DICTIONARY qualifiedName? showPredicateClauses
    ;
```

因此源码层已经支持在 `SHOW DICTIONARY` 后使用通用的：

- `WHERE`
- `ORDER BY`
- `LIMIT`

该功能当前主要存在于 `main`，社区文档尚未完整同步说明。

## 4. 社区 main 仍未解决的问题

### 4.1 仍无 `ALTER DICTIONARY` 和属性在线修改

社区 `main` 的 DICTIONARY 属性仍然只有：

1. `dictionary_warm_up`
2. `dictionary_memory_limit`
3. `dictionary_refresh_interval`
4. `dictionary_read_latest`
5. `dictionary_ignore_failed_refresh`

grammar 仍然只有：

- CREATE DICTIONARY
- DROP DICTIONARY / DROP DICTIONARY CACHE
- REFRESH DICTIONARY
- SHOW DICTIONARY
- CANCEL REFRESH DICTIONARY

没有 `ALTER DICTIONARY`。因此：

- `dictionary_memory_limit` 不能通过 SQL 在线更新。
- 单纯再次执行 `REFRESH DICTIONARY` 不会读取一个新的 memory limit。
- 需要修改属性时，仍只能 DROP 并重新 CREATE，或者自行扩展 ALTER 语法和持久化逻辑。
- 社区 `main` 对 `dictionary_memory_limit` 的正值/乘法溢出校验也没有发生实质增强。

### 4.2 仍无源表 DDL 反向依赖

`Dictionary` 对象仍然只保存：

- catalog 名称
- database 名称
- `queryableObject` 名称字符串
- KEY 列名称字符串
- VALUE 列名称字符串

它没有保存稳定的 Table ID / Column ID 依赖，Schema Change、Drop Table、Rename Table/Column 等路径也没有调用 `DictionaryMgr` 做依赖校验。

所以以下操作仍可能先成功：

- 删除 `retention_days`。
- 删除或重命名某个 KEY/VALUE 列。
- 删除或重命名源表。
- 修改列类型为 DICTIONARY 不再支持的类型。

错误通常要到下一次 REFRESH 或 `dictionary_get` 分析阶段才暴露。

### 4.3 BE/CN 缓存仍是纯内存状态

社区虽然把 `DictionaryCacheManager` 从 `StorageEngine` 移到了 `ComputeEnv`，但核心状态仍是进程内的 `unordered_map`：

- dictionary ID 到缓存对象
- dictionary ID 到缓存版本
- dictionary ID 到 schema
- 刷新过程中的 mutable cache

没有：

- 本地磁盘持久化。
- BE/CN 启动时重建。
- 节点重新注册时由 FE 自动回灌。
- 查询侧远程 fallback。
- 查询调度时绕开缺少 DICTIONARY cache 的节点。

`DictionaryGetExpr::prepare()` 在目标 BE/CN 上找不到 schema/cache 时仍直接返回：

```text
open dictionary expression failed, there is no cache for dictionary: <id>
```

### 4.4 FE 重启仍会重置 DICTIONARY 运行状态

FE image load 完成后，`DictionaryMgr.gsonPostProcess()` 仍会对每个 DICTIONARY 调用 `resetState()`，把运行状态重置为 `UNINITIALIZED`。

当 `dictionary_refresh_interval = 0` 时，社区 #76634 会正确地把下一次调度时间固定为 `Long.MAX_VALUE`，但这也意味着不会自动刷新。实例整体重启后仍需要管理员手工执行：

```sql
REFRESH DICTIONARY datalake_tenants_ttl_dict;
```

### 4.5 仍然是全量刷新

社区 `Dictionary.buildQuery()` 仍然构造：

```sql
SELECT <all keys>, <all values>
FROM <queryable object>;
```

每次 REFRESH 都会重新扫描全部源数据并在每个目标节点构建完整缓存，没有：

- 增量行更新。
- 基于分区的增量刷新。
- CDC。
- watermark。
- 仅修复某个新上线节点的定向 refresh。

### 4.6 跨节点 COMMIT 仍非原子

BEGIN、刷新查询和 COMMIT 仍通过 FE 对目标 BE/CN 顺序执行 RPC。每个节点内部会原子替换自己的缓存，但 FE 没有集群级二阶段提交保证。

如果前面的节点已 COMMIT，而后续节点 COMMIT 失败，可能出现不同节点持有不同版本。社区 WAL 改造只保证 FE 状态日志一致，不能回滚已经提交的 BE/CN 缓存。

### 4.7 `dictionary_ignore_failed_refresh` 仍有边界

`dictionary_ignore_failed_refresh = true` 主要能处理刷新阶段失败并回到旧 FE 状态，但：

- 进入 COMMITTING 后 `stateBeforeRefresh` 已清除，commit 阶段失败不能可靠回到旧状态。
- 内存超限路径可能清理旧缓存，不能保证旧数据始终可用。
- 跨节点部分 COMMIT 后无法回滚已经替换的节点。

因此该属性不能理解为“任何刷新失败都保证旧缓存完整保留”。

### 4.8 `SHOW DICTIONARY` 的观测能力仍有限

当前输出主要是：

- FE 全局状态。
- 上次刷新时间和错误。
- 从各节点获取的近似内存用量。

它仍不展示或校验：

- 每个节点的实际缓存版本。
- 节点是否缺少 cache/schema。
- refresh transaction 阶段。
- 哪些节点被本次刷新跳过。
- 是否存在跨节点版本分裂。

## 5. 常见场景的当前结论

| 场景 | 本地 4.0.11 | 社区 main | 运维结论 |
|---|---|---|---|
| 删除 VALUE 列 `retention_days` | DDL 通常允许，后续刷新失败 | 仍允许；错误信息略有改善 | 修改源表前必须由业务侧检查 DICTIONARY 依赖 |
| 删除后重新加回同名同类型列 | 可再次手工刷新恢复 | 仍可手工刷新恢复 | 前提是源查询、列类型和数据满足 KEY/VALUE 要求 |
| `refresh_interval=0`，BE/CN 重启 | 节点 cache 丢失，查询可能报错 | 仍然如此 | 手工 REFRESH；社区 #76496 不解决自动回灌 |
| `refresh_interval=0`，手工刷新 | 可能意外重新开启周期刷新 | #76634 已修复 | 4.0.11 如需该语义应 backport #76634 或等价修复 |
| 刷新时一个节点已经宕机 | 整次刷新可能失败并 CANCELLED | #76496 跳过不可用节点 | 4.0.11 建议适配 #76496 |
| 在线更新 `dictionary_memory_limit` | 不支持 | 仍不支持 | DROP/CREATE 或自行实现 ALTER DICTIONARY |
| 多 KEY `dictionary_get` 直接查询 | 基本可工作 | 可工作 | 关注 NULL 和节点 cache 状态 |
| 多 KEY 表达式转 SQL 再解析 | 可能丢 KEY/重复 bool | #78706 已修复 | Tenant-TTL 应优先回补 #78794 |

## 6. 面向本地 4.0.11 的 cherry-pick 建议

### 6.1 P0：与 Tenant-TTL 正确性直接相关

#### A. 多 KEY SQL 序列化修复

- 首选社区 4.0 backport：[#78794](https://github.com/StarRocks/starrocks/pull/78794)
- 来源 PR：[#78706](https://github.com/StarRocks/starrocks/pull/78706)
- 适用条件：Tenant-TTL 的 `partition_retention_condition` 使用多 KEY `dictionary_get`，或者存在任何表达式序列化/反序列化路径。
- 风险：低到中，主要影响 FE 表达式输出和 round-trip 测试。
- 必测：一个、两个、三个 KEY；省略 flag、显式 false、显式 true；TTL predicate 重建后 SQL 参数数量正确。

#### B. Nullable Key 假 NULL 修复

- 首选社区 4.0 backport：[#76980](https://github.com/StarRocks/starrocks/pull/76980)
- 来源 PR：[#76881](https://github.com/StarRocks/starrocks/pull/76881)
- 适用条件：KEY 由 `COALESCE`、`CASE`、CAST 或其他可能产生 Nullable 列的表达式生成。
- 风险：低，官方已经回补并随 4.0.14 发布。

#### C. 子表达式错误传播修复

- 来源 PR：[#77442](https://github.com/StarRocks/starrocks/pull/77442)
- 适用条件：KEY/value 表达式可能在 BE 求值阶段返回错误。
- 风险：低到中；应优先取当前 `branch-4.0` 的适配版本，而不是直接取已经迁移到 `exprs_ext/dict` 的 main 版本。

### 6.2 P1：刷新可用性和边界正确性

#### A. 跳过不可用节点

- 来源 PR：[#76496](https://github.com/StarRocks/starrocks/pull/76496)
- 价值：显著降低单节点故障导致整本 DICTIONARY `CANCELLED` 的概率。
- 风险：低到中。
- 额外要求：必须明确接受“恢复节点在下一次 refresh 前没有 cache”的行为；这不是节点自动恢复方案。

#### B. refresh interval 溢出与禁用调度修复

- 来源 PR：[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- 价值：修复 `interval=0` 被意外重新调度和超大 interval 溢出。
- 风险：中。官方 `branch-4.0` backport #76693 已关闭，说明需要按 4.0.11 的持久化字段和测试结构手工适配，不能默认干净 cherry-pick。

#### C. 源表删除 NPE 修复

- 来源 PR：[#71109](https://github.com/StarRocks/starrocks/pull/71109)
- 价值：把 FE NPE 改成明确的语义错误。
- 风险：低，代码改动很小。
- 注意：不解决源表 DDL 依赖，需要另行设计依赖保护。

### 6.3 P2：环境相关的稳健性修复

#### A. 刷新 ConnectContext 修复

- 来源 PR：[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- 适用条件：非默认 warehouse、共享数据部署、全局 SessionVariable 与内部刷新配置差异较大。
- 建议：拆分审查 warehouse 顺序、关闭 MV Rewrite、`bindScope` 三部分对 4.0.11 的兼容性。

#### B. FE hostname 解析

- 来源 PR：[#78390](https://github.com/StarRocks/starrocks/pull/78390)
- 适用条件：Kubernetes hostname、多 BE/CN、大规模定时刷新或 DNS 压力较高。
- 风险：中，需要确认本地 4.0.11 是否已有兼容的 `DnsCache` 接口。

### 6.4 不建议作为普通 bugfix 单独 cherry-pick

#### FE WAL/Apply 重构

- 来源 PR：[#66207](https://github.com/StarRocks/starrocks/pull/66207)
- 原因：涉及 EditLog、OperationType、replay、WALApplier、DICTIONARY 状态日志和多种元数据写路径。
- 建议：如果确实需要，应作为独立 COMMUNITY-FIX 轮次整体设计、实现和验证，不要只拷贝 `DictionaryMgr` 的局部代码。

#### BE 模块搬迁

- [#75184 Move DictionaryCacheManager from StorageEngine to ComputeEnv](https://github.com/StarRocks/starrocks/pull/75184)
- 相关 Cache Sink/Expr 目录重构。
- 原因：主要是架构解耦，不直接解决本地 4.0.11 的业务故障；单独 backport 代价大、收益小。

## 7. Cherry-pick 前检查清单

每次准备回补社区 DICTIONARY 修复前，至少执行以下检查：

1. 确认修复存在于哪个版本线：`main`、当前 `branch-4.0`，还是已发布 4.0.x。
2. 优先选择官方 `branch-4.0` backport，而不是直接 cherry-pick `main`：main 已经移动 FE grammar、BE DictionaryCacheManager、DictionaryGetExpr 和 Cache Sink 的目录及依赖。
3. 对照当前分支确认修复是否已被其他提交等价实现，避免重复回补。
4. 每个独立社区问题单独提交，不与 Tenant-TTL 实现轮次混合。
5. 按仓库规则使用提交主题：

   ```text
   <type>(<scope>): [COMMUNITY-FIX] <summary>
   ```

6. 提交正文必须包含非空的 `Problem`、`Implementation`、`Compatibility`、`Tests`。
7. 只记录实际执行过的测试；没有执行的相关测试必须写明原因。
8. 在 backport 前后都检查 `git diff`，确认没有混入 `zc-docs` 或其他不相关改动。

建议的最小测试集合：

- FE：`DictionaryMgrTest`
- FE：`ExprToSqlDictionaryGetTest` 或本地等价 round-trip 测试
- BE：`DictionaryCacheManagerTest.dictionary_get_expr*`
- SQL：单 KEY、多 KEY、flag 省略/false/true
- Tenant-TTL：`partition_retention_condition` 中复合 KEY DICTIONARY_GET 的 SQL 重建
- 运维：一个节点停止、恢复、手工 REFRESH、周期 REFRESH
- 重启：FE、单 BE/CN、整个实例，分别覆盖 `refresh_interval=0` 和大于 0

## 8. 源码证据索引

### 8.1 本地 4.0.11

- 五个属性和默认值：`fe/fe-core/src/main/java/com/starrocks/catalog/Dictionary.java:29-109`
- refresh interval 旧解析逻辑：`fe/fe-core/src/main/java/com/starrocks/catalog/Dictionary.java:229-236`
- DICTIONARY SQL 语法：`fe/fe-core/src/main/java/com/starrocks/sql/parser/StarRocks.g4:906-932`
- 全节点刷新目标：`fe/fe-core/src/main/java/com/starrocks/catalog/DictionaryMgr.java:165-176`
- 顺序 RPC、遇错立即返回：`fe/fe-core/src/main/java/com/starrocks/catalog/DictionaryMgr.java:178-205`
- 创建和刷新入口：`fe/fe-core/src/main/java/com/starrocks/catalog/DictionaryMgr.java:208-272`
- 源表不存在的旧 NPE：`fe/fe-core/src/main/java/com/starrocks/sql/analyzer/ExpressionAnalyzer.java:1905-1910`
- BE 查询表达式：`be/src/exprs/dictionary_get_expr.cpp`
- BE 内存缓存管理：`be/src/storage/dictionary_cache_manager.h`

### 8.2 社区 main

- 五个属性、调度和全量查询：
  - <https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/catalog/Dictionary.java>
- DICTIONARY SQL grammar：
  - <https://github.com/StarRocks/starrocks/blob/main/fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4#L946-L972>
- 可用节点选择、WAL 状态和刷新上下文：
  - <https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/catalog/DictionaryMgr.java>
- BE 纯内存缓存 Map：
  - <https://github.com/StarRocks/starrocks/blob/main/be/src/compute_env/dictionary_cache/dictionary_cache_manager.h#L638-L664>
- 目标 BE 缺少 cache 时直接报错：
  - <https://github.com/StarRocks/starrocks/blob/main/be/src/exprs_ext/dict/dictionary_get_expr.cpp#L86-L106>
- 官方 CREATE DICTIONARY 属性文档：
  - <https://github.com/StarRocks/starrocks/blob/main/docs/en/sql-reference/sql-statements/dictionary/CREATE_DICTIONARY.md>

## 9. 维护说明

- 本文是 2026-09-10 的源码和官方 PR 快照，不应把 `main` 的状态永久视为最新结论。
- 每次正式开始新的 DICTIONARY/Tenant-TTL 编码轮次前，应重新检查上述 PR、`branch-4.0` 和最新 release tag。
- 如果社区后续实现 `ALTER DICTIONARY`、节点上线自动 backfill、持久缓存、增量刷新或跨节点原子提交，应优先更新本文“仍未解决的问题”和 cherry-pick 建议。
- 本次文档整理未修改产品代码，也未运行 FE/BE/SQL 测试。
