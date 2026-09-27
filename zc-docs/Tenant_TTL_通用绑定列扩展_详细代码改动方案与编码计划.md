# Tenant-TTL 通用绑定列扩展：详细代码改动方案与编码计划

日期：2026-09-27。静态分析基线：`7e74c2706`（第 043 轮）。

需求依据：[通用绑定列扩展调研与对齐记录](Tenant_TTL_通用绑定列扩展_调研与对齐记录.md)。B01～B06、C01～C07、SHOW 兼容方案、D01 两步变更及 D02 绑定期间禁止绑定列改名均已确认。业务绑定列变更须先解除 TTL 再重新启用，禁止直接换列；解除不等待已下发 BE 任务，允许旧请求晚于新请求执行，不保证副本一致。此前待生效绑定、调度入口切换、全副本收敛及发布冻结方案均撤回；也不再实现改名后的自动绑定延续。E01 已确认使用空条件 SET 解除 TTL，核心需求无待确认项。

第 044～049 轮已实现并独立提交。最终 156 项 Tenant-TTL FE 测试、2 项定向授权测试、55 项 BE 测试、FE Checkstyle/打包、非测试 BE 构建及第 050 轮六项真实三副本验收均通过；具体范围与未执行项见[验收记录](Tenant_TTL_050_通用绑定列扩展_验收记录.md)。

## 1. 范围与交付拆分

第一阶段提供单个 VARCHAR 业务绑定列、自定义 VARCHAR 策略第一键、空字符串专属 TTL、按列身份校验及显式解除/重新启用的生命周期，以及兼容旧入口的通用 SHOW。整数、多列组合、IP 归一化、任意时间列、逐行时间判定、放宽表模型及扩大容量上限不在范围内。

改动分为两部分：

1. **通用列能力。** FE 属性和身份校验、Dictionary 两端校验、空字符串、绑定列改名准入限制、SHOW。BE 继续使用列 Unique ID 和 VARCHAR 名单，过滤与 Rowset 重写算法可复用。
2. **解除后重新绑定。** 新增解除 TTL 的 DDL 路径，清理属性、绑定、引用、旧进度和 FE 旧上下文；重新启用复用现有准入。已下发旧任务自然结束，新旧任务不保证执行顺序。

禁止直接换业务绑定列；新增/解除均在 DDL 提交时生效，实际清理由已有调度触发。第 7 节说明解除、重新启用及旧任务交错的边界。

## 2. 配置、身份与兼容契约

### 2.1 配置入口

```sql
ALTER TABLE business.http_log SET (
    "compaction_retention_condition" =
        "dictionary_ttl('ip_ttl_dict', 'business.http_log', 180)",
    "compaction_retention_key_column" = "dst_ip"
);
```

上述表可以不含 `tenant` 列。对应 Dictionary 的第一 KEY 可以是 `policy_value`，第二 KEY 仍是 `table_name`，VALUE 仍是 `retention_days`。业务列名和策略键名不要求相等。

| 场景 | 处理 |
| --- | --- |
| 首次 CREATE/ALTER 启用，没有新属性 | 按 `tenant` 解析并固定其 ColumnId/Unique ID |
| 首次启用，显式指定属性 | 按正常标识符规则解析实际 VARCHAR 列，保存其规范名称和身份 |
| 已有绑定，只修改 Dictionary、默认天数或时区 | 新属性缺席表示保留原列身份，不重新默认到 tenant |
| 显式属性为空、列不存在、非 VARCHAR | 拒绝；空属性名与合法业务值 `''` 是两件事 |
| 未启用 TTL，只单独设置绑定列属性 | 拒绝无实际 TTL 条件的悬空配置 |
| 指定的列与现有身份相同 | 作为不换列处理，不制造无意义切换 |
| 已有绑定时指定另一列 | 拒绝，提示先解除 TTL，再重新启用；不保存待生效候选 |
| 同名列删后重建、类型不兼容 | 停止采用旧绑定；不得按名称自动修复 |

`dictionary_ttl` 仍严格接受三个参数。VARCHAR 长度不强制相等；TTL 不 CAST、不截断、不 trim、不做大小写或 IP 文本归一化。`default` 仍保留，空字符串是合法专属值，NULL 按已有独立语义处理。

### 2.2 元数据与旧数据读取

保留现有序列化字段 `tenantColumnId`、`tenantColumnUniqueId`，其含义扩展为“实际绑定列身份”；不为名称中含 tenant 就批量改名。旧 binding、旧 journal、旧 image 必须能继续读取。

不为本扩展升级现有 binding 格式，也不新增 BE 绑定代际。继续使用实际列身份和既有 fingerprint；解除时显式使 FE 旧执行上下文失效，不能仅靠解绑前后属性不同。复用现有属性日志中的显式空条件表示解除，区分“无绑定字段的旧日志”和“要求清除绑定的新日志”，不新增日志字段。

旧表没有新列属性时保留原持久化身份；展示可从身份解析当前列名。显式配置使用实际列名；改名须先解除再重新启用，不维护改名中的 TTL 属性同步。解除时清除该表旧完成进度，并继续按现有绑定 fingerprint 复核；额外测试解除后重新绑定相同配置以及 A→B→A，保证旧进度和回报不会误认作新执行结果。无需为此维护一份持久化逐任务历史。

`TenantTtlDictionaryBinding`、导出 Protobuf 和 Agent Thrift 的既有字段编号、类型和协议版本保持不变。内部名称含 tenant 的字段继续表示绑定值/绑定列角色，不做无关的大范围重命名。

## 3. FE 属性、绑定与生命周期改动

本节文件路径以 `fe/fe-core/src/main/java/com/starrocks/` 为前缀。

| 文件 / 方法 | 改动 |
| --- | --- |
| `common/util/PropertyAnalyzer.java` | 增加 `PROPERTIES_COMPACTION_RETENTION_KEY_COLUMN` 常量 |
| `catalog/TableProperty.java` | 解析并保存规范列名；copy、build、Gson 恢复保留属性和绑定身份，不能只增加一个运行时 getter |
| `sql/analyzer/AlterTableClauseAnalyzer.java` | 将新属性加入 TTL 属性组合白名单和 TTL ALTER 分支；保持与其他互斥属性的约束 |
| `server/OlapTableFactory.java` | CREATE 时解析业务列，校验候选配置后一次性保存属性和绑定；处理属性消费，避免被当作未知属性 |
| `server/LocalMetastore.java` | ALTER 合并候选属性；新属性单独变化也触发分析；区分启用、同身份更新、解除与禁止的直接换列；journal/replay 原子保存或清除有效配置 |
| `catalog/TenantTtlBindingAnalyzer.java` | 拆开 DDL 选列与运行时身份复核；前者允许显式名字和首次 tenant 默认，后者只按已保存 ColumnId/Unique ID 验证 |
| `catalog/TenantTtlTableBinding.java` | 保留原列身份字段和格式；复用 copy/equality/hash，必要时增加通用语义访问方法 |
| `tenantttl/policy/TenantTtlEvaluationContext.java` | 初次捕获、发送前复核和完成进度发布均使用身份复核；换列后的 fingerprint 和旧上下文隔离，覆盖 A→B→A |
| `tenantttl/TenantTtlPartitionBoundResolver.java` | 去除间接按 tenant 名称重新选列；保持时间列、分区表达式、时区和分区上界证明不变 |
| `tenantttl/TenantTtlStatusService.java` | 使用同一身份复核，输出实际名字；绑定失效时保留可诊断信息，不能偷偷改绑到同名新列 |
| `persist/ModifyTablePropertyOperationLog.java` | 明确表示解除动作，正常 DDL 与 replay 同步清除属性和绑定；兼容旧日志的缺省字段 |

### 3.1 绑定期间禁止该绑定列重命名

在 `LocalMetastore.renameColumn()` 的正常 DDL 校验中，按持久化 ColumnId/Unique ID 判断目标是否为当前业务绑定列；尚未解除时拒绝，提示先解除 TTL、改名、再重新启用。不能仅检查列名是否 tenant，也不能仅在快照 ACTIVE 或调度开启时拒绝。

其他未绑定列仍按原有规则处理。正常的原子改名与 TTL 启用/解除共享 Catalog 锁下校验，避免检查后被并发绑定。replay 继续重放历史上已经提交的合法列重命名，不在回放时执行新的准入拒绝；不新增 ColumnRenameInfo 字段或修改普通改名语义。

不再实现 TTL 属性随改名更新、绑定迁移或分区表达式 fingerprint 迁移。解除后普通改名由 StarRocks 原有规则决定是否允许，重新启用按新名字完整分析。删列重建、类型不兼容仍按既有绑定失效规则处理，本次不扩大其他 Schema DDL 范围。

### 3.2 运行时共同约束

DDL、Scheduler、PartitionBoundResolver、StatusService 必须复用同一校验逻辑。时间列继续要求 `recordTimestamp BIGINT`；本地 DUP_KEYS、Base Index、无 Rollup/同步 MV 等约束保持不变。Dictionary 引用计数和快照引用只有真实 Dictionary 绑定变化才调整，不因业务列更名无条件触发 Dictionary 刷新。

## 4. 策略源表、Dictionary 与 BE 导出

`TenantTtlBindingAnalyzer.validateDictionarySchema()` 与 `validateDictionarySourceTable()` 改为按角色校验：

1. 源表恰好三列，类型和非空要求依次为 VARCHAR、VARCHAR、INT。
2. 第一列名称可自定义；第二列必须是 `table_name`，第三列必须是 `retention_days`。
3. 本地 PRIMARY KEY 为前两列；Dictionary 前两个 KEY 与源表第一、第二列对应，VALUE 对应第三列。
4. 第一键不能与其他列产生标识符冲突；不能用解除 tenant 名称限制顺便放开列数、顺序、空键列或类型。

BE 修改 `be/src/storage/dictionary_cache_manager.cpp` 的专用导出 Schema 判断：第一列由固定名称 tenant 改为第一键角色，保留其 VARCHAR 类型及其余列约束。继续按 `get_slice()` 读取实际字节。

`gensrc/proto/internal_service.proto` 的导出条目仍使用原 field number 和 `bytes tenant`；它是角色字段，不要求物理键列也叫 tenant。仅此项保留导出协议 v1，不改通用 Dictionary 编码和普通查询行为。

FE 按实际第一键名称和角色约束报错，BE 维持既有 `SCHEMA_MISMATCH` 导出结果，通用 SHOW 同时展示实际策略键列名。FE 放开、旧 BE 未放开时仍明确报 schema 不兼容，不能伪造空快照或按默认 TTL 继续清理。上线需先使相关 BE 具备导出能力，再启用新的策略键名称；混合版本不能被默认当作完整支持。

## 5. 空字符串专属 TTL

三处明确修改点：

| 文件 / 方法 | 修改 |
| --- | --- |
| `tenantttl/policy/TenantTtlPolicySnapshotBuilder.validateRow()` | 允许第一键长度 0，继续拒绝字段缺失/NULL；`table_name` 非空和天数验证不放宽 |
| `tenantttl/policy/TenantTtlPolicyPlanner.resolveTablePolicy()` | 允许大小为 0 的合法 `TenantTtlByteKey`，仍拒绝不存在的键对象 |
| `task/TenantTtlCompactionTask.immutableTenants()` | 允许名单中的零长度元素；保留排序、去重、模式和大小约束 |

验证链路为 Dictionary → 导出 Protobuf → FE 字节键 → 策略指纹 → Thrift `list<binary>` → BE IN 谓词。使用真实序列化往返验证空值存在性，不能只用手工构造 Java/C++ 对象。

BE `tenant_ttl_row_filter.cpp`、`tenant_ttl_compaction_types.cpp` 不因支持空字符串放宽空名单：`[]` 与 `[""]` 必须区分，空 KEEP_LIST 继续拒绝。DELETE/KEEP、NULL、空格、非 ASCII 字节、ZoneMap 全裁剪和默认回退均需要覆盖。长度为 0 的键仍计入条目数，序列化 framing 仍计入字节限制。

## 6. SHOW 双入口

```sql
SHOW TENANT TTL STATUS FROM business.http_log FOR TENANT '';
SHOW COMPACTION TTL STATUS FROM business.http_log FOR VALUE '';
```

两者查询同一个空字符串绑定值；省略 FOR 则是表级状态查询。

### 6.1 解析与执行

`StarRocks.g4` 增加通用分支，严格对应旧入口 `FOR TENANT`、新入口 `FOR VALUE`。已有 COMPACTION/VALUE token 可复用。`AstBuilder` 构造相同 AST 类型，以语句内的 `generic` 标志选择展示格式；旧构造函数默认旧模式。该标志不持久化，与明确排除的历史查询保护标记无关。

`ShowStmtAnalyzer`、`Authorizer` 继续使用相同表解析和 SELECT 授权；`ShowExecutor` 复用同一服务。复用现有 `MutableStatus` 收集状态，再按展示模式投影，不通过硬编码字符串数组下标互相转换。

### 6.2 结果投影设计

| 项目 | 旧入口 | 通用入口 |
| --- | --- | --- |
| 基础字段 | 当前名称、顺序、类型全部保留 | 沿用当前基础字段顺序，在身份字段之前加入 `KeyColumn`、`DictionaryKeyColumn` |
| 业务列身份 | `TenantColumnId`、`TenantColumnUniqueId` | 对应位置改为 `KeyColumnId`、`KeyColumnUniqueId`，类型沿用 |
| 查询值附加字段 | `Tenant`、`EffectiveRetentionDays`、`ResolutionType` | `Value`、`EffectiveRetentionDays`、`ResolutionType`，同顺序、同类型 |
| 来源枚举 | 全部保持原值 | 第一阶段沿用原 ResolutionType 编码并在说明中解释实际绑定含义，不新增另一套策略解析规则 |
| 解除状态 | 沿用未启用时的既有结果格式 | 展示未启用及空绑定列；重新启用后展示实际列，不增加待生效列或切换进度字段 |

`KeyColumn` 从已绑定身份解析；`DictionaryKeyColumn` 从实际 Dictionary 第一 KEY 读取。对象丢失时使用既有空值/错误展示规则，不从相似名称猜测。所有 varchar 元数据长度与数值字段类型在 `ShowResultMetaFactory` 中明确声明，测试同时断言 metadata 和 row shape。

旧入口即使写着 `FOR TENANT`，也查询表实际绑定的 dst_ip/device_name 值。旧入口兼容包括结果列顺序、类型及 ResolutionType 值，不只包含 SQL 能够解析。

## 7. 解除后重新绑定：执行定义与最小改动

### 7.1 DDL 与状态

已有 TTL 绑定时，直接将 `compaction_retention_key_column` 指定为另一列报错；同身份的配置更新继续沿用现有规则。解除仅移除业务表的 TTL 配置，不删除业务表、策略表或 Dictionary。重新启用直接复用 CREATE/ALTER 的分析、持久化和引用注册流程，不引入 PENDING 状态。

E01 已确认：解除入口复用 `ALTER TABLE ... SET ("compaction_retention_condition" = "")`，由 ALTER 分支识别显式空字符串条件并执行解除（只有空白的非法条件不作为解除）；第 049 轮已实现此语法。CREATE 空条件及正常 dictionary_ttl 表达式继续严格校验。业务绑定值 `''` 是合法策略值，与这里解除属性的空条件不混淆。解除请求不与同次重新启用混写，重复解除可幂等成功。

解除在同一 FE 元数据操作中移除 condition/key_column/time_zone 等 TTL 专属属性、两个绑定对象和该表的 Dictionary 引用，清除旧完成进度并使该表已捕获的 FE 上下文失效。共享 Dictionary 上其他业务表的引用保持不变，不删除源数据。

解除 DDL 完成后可以立即重新启用。两步之间没有新 TTL 计划；重新启用失败保持未绑定，不自动恢复旧配置。DDL 生效不等于清理已执行，新配置由后续正常调度读取。

### 7.2 旧任务与新绑定相遇

| 场景 | 已确认行为 |
| --- | --- |
| FE 仍在上一轮流程中 | 不额外启动调度器，旧上下文失效后按现有流程退出；新绑定参与后续评估 |
| 同一 BE/Tablet 的旧任务正在执行 | 新 task ID 返回 TABLET_BUSY，FE 沿用有界重试；旧任务结束后新任务才可能执行 |
| busy/失败直到预算耗尽 | 本轮结束，后续按新配置重新规划，不恢复旧绑定或补齐旧删除 |
| 不同 Tablet、不同副本 | 可以分别执行新旧请求，无全表或跨副本顺序保证 |
| 新任务先执行，旧请求延迟到达 | 旧请求仍可能被接受并按原绑定列删除；不增加绑定代际屏障 |
| 旧回报晚于重新启用 | 不得推进新绑定进度；已失效上下文不能被重新激活 |
| FE/BE 重启 | 恢复真实配置，继续原有任务重启边界，不恢复逐副本旧任务账本 |

同一 Tablet 的执行互斥由现有 `Tablet::try_begin_tenant_ttl()` 负责。互斥不等于按策略新旧排序，新的绑定成功不保证此后只有新策略删除。旧删除不可逆，允许副本差异长期存在；仍需保证每个请求的列身份、过滤谓词、Schema 和单次提交正确。

### 7.3 日志、上下文和进度清理

第 049 轮复用 `ModifyTablePropertyOperationLog.properties` 中显式空的 `compaction_retention_condition` 识别解除，不新增日志字段。缺省/null 绑定字段仍按旧日志规则解释。解除 replay 同步清除属性、绑定、相关进度和运行时引用。

FE 上下文保留现有不可变绑定对象的内存身份；解除清空绑定，重新启用创建新对象。任务等待、发送前检查及进度提交均验证此身份，不增加独立失效标志、持久化 BE 代际或逐任务账本。快速解除再绑定相同配置也会使旧上下文失效。

Catalog、Coordinator、进度管理器使用既有锁序。解除与进度清理以同一条属性日志作为持久化顺序点，live/replay 均清除旧进度，不另写可能交错的进度删除日志。已发出或已被补发器捕获的请求仍可晚到，符合本节契约。

### 7.4 待进一步讨论：解除后的聚合查询保护

用户明确要求本轮不新增持久化 boolean 保护标记。此前“曾启用 TTL 后永久禁止 FE 聚合常量替换”的方案移到澄清文档的待讨论项，本轮不实现，也不借其他历史标记替代。

当前保护仅在有效 TTL 条件/绑定存在时生效；解除后，旧任务仍可能晚到删除，FE 缓存统计也可能滞后。解除后的 COUNT/MIN/MAX 常量替换正确性及可恢复的保护方式仍待进一步讨论。本轮保留现有优化器行为，验收明确区分逐行存储结果和此待讨论的缓存聚合边界，不宣称已解决。

### 7.5 改动文件与范围

| 文件（FE Java 前缀同第 3 节） | 改动 |
| --- | --- |
| `catalog/TableProperty.java` | 清除 TTL 属性/绑定，不新增持久化查询保护或待生效绑定 |
| `server/LocalMetastore.java` | 显式解除分支、禁止直接换列、原子元数据更新及 replay |
| `persist/ModifyTablePropertyOperationLog.java` | 复用现有 properties 的显式空条件表示解除，无须新增字段 |
| `tenantttl/policy/TenantTtlPolicySnapshotManager.java` | 复用移除表引用能力，保留其他表共享引用 |
| `tenantttl/policy/TenantTtlEvaluationContext.java`、`scheduler/TenantTtlRewriteCoordinator.java` | 作废 FE 旧上下文、发送/回报/进度隔离；不撤销 BE 任务 |
| `tenantttl/scheduler/TenantTtlPartitionProgressManager.java` | 解除时清理旧进度，验证日志回放与提交竞态 |
| `tenantttl/TenantTtlStatusService.java` | 解除后展示未启用状态，新建后展示当前绑定，无待切换字段 |

BE 继续复用现有 admission、任务执行、busy 与 Schema 检查；不增加 seal、TabletMeta 绑定代际、发布冻结或通用 ReportHandler 改造。

## 8. 测试设计与通过标准

### 8.1 FE 测试

扩展 `TenantTtlPropertyTest`、`TenantTtlBindingDdlTest`、`TenantTtlEvaluationContextTest`、`TenantTtlPartitionBoundResolverTest` 和状态测试，覆盖：

- 无 tenant 列、存在多个候选列但只绑定一个、双方不同名、VARCHAR 不同长度、非 VARCHAR 拒绝。
- 首次默认 tenant、显式空属性拒绝、其他 ALTER 不重置绑定、旧 image/journal 恢复。
- 绑定列改名拒绝（包括无快照/停调度）、其他列按原规则改名、解除后普通改名再绑定、并发绑定/改名、历史日志回放、同名删建、类型漂移及 A→B→A。
- Dictionary 源表/KEY/VALUE 的全部正反例，错误不能退化成无策略命中。

扩展 SnapshotBuilder、Planner、CompactionTask 测试，验证空字节完整往返、NULL/缺失键仍拒绝、`default`/零天数/空名单语义及指纹区分。扩展 parser、Show AST、ShowResultMetaFactory、StatusService、PrivilegeChecker，精确断言旧结果兼容和新字段映射。

解除/重建专项使用可控时钟和确定性并发栅栏：禁止直接换列、解除幂等、共享引用清理、旧上下文失效、立即重建相同配置、旧回报/进度提交交错、FE 重启、A→B→A、重建失败保持未绑定。覆盖 journal/image 往返、未启用展示；解除后的缓存聚合边界另列待讨论，不计作已修复。

### 8.2 BE 测试

扩展 `dictionary_cache_manager_test`：自定义第一键导出，保持其余 schema 限制，空字符串 Proto presence 与字节值；旧 tenant 导出回归。

扩展 `tenant_ttl_row_filter_test`、`tenant_ttl_compaction_types_test`、`tenant_ttl_compaction_fixture_test`、`engine_tenant_ttl_compaction_task_test`：非 tenant 实际列、正确 UID、空字符串 DELETE/KEEP、NULL、默认遗漏、ZoneMap 全裁剪、多 Rowset、同谓词重复处理、错误 UID/类型拒绝。

沿用 Tablet/Agent/引擎的任务身份、Schema、提交原子性和重复请求测试；增加已下发旧任务晚到完成的组合用例，核对它仅按原请求处理。没有新切换协议，不为不存在的 seal/持久化代际编写测试。

### 8.3 真实三副本验收

复用 `test/sql/test_tenant_ttl_compaction/multireplica/` 基础设施，新建本扩展场景，避免修改现有容量研究脚本或把旧 043 结果计入本次通过数。

建议规模（测试设计，不是已确认性能 SLA）：微型确定性数据覆盖所有边界；每张标准表 12 万行、12 个时间分区、每分区 4 个 Tablet、3 副本；核心正常/失败恢复场景增加到每表 120 万行。绑定值分布包括约千级普通值、空字符串、空格、大小写差异、未配置值与 NULL，不向业务列写入保留值 default。

每个副本查询完整业务列并比较行多重集合，由独立 oracle 根据实际快照、评估时刻、分区上界及已发生的删除历史推导，不能只比较 count/hash 或任务成功数。换列后不能期待已经由旧规则删除的行重新出现。

重点反例：旧列应删、新列应留的一组行，先使一个副本删除，再让另一个副本失败或丢失回报，解除并重新绑定。确认无需等待全部旧 BE 任务结束即可登记新绑定；同一 Tablet 忙碌时新任务重试。另注入新任务先完成、旧请求后到达，允许旧请求按原谓词继续删除。按各副本实际执行的旧/新谓词核对结果，不要求差异自动修复。正常静态数据用例仍核对每个副本的正确结果。

### 8.4 容量与性能

名单条数和序列化字节分别测试 N−1、N、N+1，覆盖 DELETE/KEEP、零长度条目、重复去重及多字节值；不放宽 100000/8 MiB 默认限制，不拆分 KEEP 补集。Dictionary 导出继续验证现有行数、字节与内存限制。

同数据分布比较 tenant 与自定义列的规划时间、FE 内存、BE 读取/重写字节和任务耗时。解除/重建另记录 DDL 耗时、绑定生效到实际调度的间隔及 busy 重试扫描量。没有事先约定指标时报告数据与差异，不编造通过阈值。

## 9. 编码顺序和独立提交安排

基于 043 拆为以下独立轮次，044～049 为实现，050 为已完成的集成验收与文档交付。每轮只包含同一 Tenant-TTL 目的，并带与该变更相应的测试；实际通过项以验收记录为准。

| 建议轮次 | 提交主题示例 | 交付与退出条件 |
| --- | --- | --- |
| 044 | `feat(tenant-ttl): [044] add explicit VARCHAR key binding` | 属性、DDL、身份复核和元数据兼容；已有绑定禁止直接换列 |
| 045 | `feat(tenant-ttl): [045] generalize dictionary key names` | FE 源表/Dictionary 与 BE 导出同步放开第一键名称，两端正反例通过 |
| 046 | `feat(tenant-ttl): [046] support empty string retention keys` | 三处 FE 限制、序列化往返及 BE 精确删除验证 |
| 047 | `feat(tenant-ttl): [047] reject renaming the bound retention column` | DDL 按列身份拒绝绑定列改名，未绑定列不受额外限制，验证 Catalog 锁内并发与历史回放；049 后补解除/改名/重新绑定组合验收 |
| 048 | `feat(tenant-ttl): [048] add generic compaction TTL status` | 双入口、通用投影、旧 metadata/权限兼容 |
| 049 | `feat(tenant-ttl): [049] support unbind and re-enable lifecycle` | 解除 DDL、日志回放、引用/进度/旧上下文清理、立即重新启用；不等待旧 BE 任务，不新增持久化查询保护 |
| 050 | `test(tenant-ttl): [050] verify generic bindings on three replicas` | 真实逐副本数据 oracle、解除/重建交错用例、容量及性能报告，补齐使用说明 |

045 与 046 的 BE 变更分别验证后纳入同一最终功能交付。048 接入通用投影，049 补充未绑定展示及生命周期测试。七个轮次只覆盖本需求，不重新设计已确认的普通调度和多副本一致性语义。

每个实现 commit body 必须含非空 `Problem`、`Implementation`、`Compatibility`、`Tests`。只记录实际执行的测试和结果；未运行项写明原因。非 Tenant-TTL 修复独立提交，社区基线问题使用 `[COMMUNITY-FIX]`，不能搭载进上述轮次。

## 10. 构建、发布与当前核验状态

复用持久卷 `sr-tenant-ttl-4.0-build-cache-arm64`，使用 `/tenant-ttl-workspace`。本次实际唯一写入容器为 `starrocks-tenant-ttl-e2e`，原 `starrocks-tenant-ttl-4.0-build` 保持停止。只同步变化源码；使用前检查 CMakeCache、编译器、架构、build type、sanitizer 与关键开关。

Release 使用 `be/build_Release`，单测按配置使用独立 `be/ut_build_Debug`、`be/ut_build_Release`、`be/ut_build_ASAN`、`be/ut_build_UBSAN`。同一卷只允许一个写入容器，保留现有产物、缓存、output 和有用日志，不清理或重建整个工作区。

先运行相关 FE/BE 单测和 Checkstyle，再执行必要回归与实际三副本矩阵。验证旧 image/journal、新可选字段、重启和新 FE 对旧 BE 导出失败的明确处理。启用自定义列、空字符串策略或使用新增解除日志的表不能未经验证就降级旧二进制；发布说明区分协议字段兼容与旧程序是否理解新业务语义。

当前状态：本阶段编码与验收已完成。查询保护标记排除在本轮实现之外，保留为待进一步讨论项。第 8 节保留测试设计，实际执行项目、构建类型和观测限制以第 050 轮验收记录为准。
