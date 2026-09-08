编译已保持在后台运行，不影响下面的代码讲解。

## 一、总体调用栈

当前 HTTP 入口是一个同步的手工测试入口，调用链如下：

```text
POST /api/tenant_ttl_compaction/run
└── TenantTtlCompactionAction::handle()
    ├── parse_request()
    │   └── normalize_and_validate_tenant_ttl_request()
    ├── EngineTenantTtlCompactionTask::execute()
    │   ├── 获取并校验 Tablet
    │   ├── TenantTtlTabletGuard::try_acquire()
    │   │   ├── Tablet::try_begin_tenant_ttl()
    │   │   ├── try-lock base_lock
    │   │   ├── try-lock cumulative_lock
    │   │   └── Tablet::mark_tenant_ttl_running()
    │   ├── TenantTtlCoverageGuard::capture()
    │   │   └── Tablet::capture_tenant_ttl_coverage()
    │   ├── validate_no_delta_column_groups()
    │   ├── 遍历 coverage 中的所有 Rowset
    │   │   └── 遍历 Rowset 中的所有 Segment
    │   │       └── TenantTtlRowFilter::plan_segment()
    │   │           ├── KEEP
    │   │           ├── DROP
    │   │           └── REWRITE
    │   ├── FilteredRowsetWriter
    │   │   ├── KEEP：Hard Link
    │   │   ├── DROP：不输出 Segment
    │   │   └── REWRITE：VerticalSegmentRewriter::rewrite()
    │   ├── FilteredRowsetWriter::build()
    │   └── Tablet::commit_tenant_ttl_rowsets()
    │       ├── Rowset ID + schema + coverage CAS
    │       ├── modify_rowsets_without_lock()
    │       ├── save_meta()
    │       └── 旧 Rowset 进入 unused GC
    ├── serialize_result()
    └── HttpChannel::send_reply()
```

## 二、特殊 HTTP 入口

### 1. 编译控制

入口默认关闭：

- [be/CMakeLists.txt:99](/Users/zhuchong/Documents/code/starrocks-main/be/CMakeLists.txt:99)

```cmake
option(ENABLE_TENANT_TTL_MANUAL_TEST_ENDPOINT ... OFF)
```

只有显式打开该选项时，才会编译 HTTP Action 并定义特殊宏：

- [be/src/http/CMakeLists.txt:57](/Users/zhuchong/Documents/code/starrocks-main/be/src/http/CMakeLists.txt:57)
- [be/src/http/CMakeLists.txt:66](/Users/zhuchong/Documents/code/starrocks-main/be/src/http/CMakeLists.txt:66)

正常发布编译不会包含这个接口。

### 2. 注册 HTTP 路由

- [http_service.cpp:240](/Users/zhuchong/Documents/code/starrocks-main/be/src/service/service_be/http_service.cpp:240)

注册的路由是：

```text
POST /api/tenant_ttl_compaction/run
```

整段注册逻辑被 `STARROCKS_TENANT_TTL_MANUAL_TEST_ENDPOINT` 宏保护。

### 3. 解析请求

- [tenant_ttl_compaction_action.cpp:94](/Users/zhuchong/Documents/code/starrocks-main/be/src/http/action/tenant_ttl_compaction_action.cpp:94)

`parse_request()` 负责：

- 限制请求体最大为 1 MiB。
- 严格解析 JSON，拒绝未知字段。
- 解析 `task_id`、`tablet_id`、`partition_id`。
- 解析 `tenant_column_unique_id`。
- 解析互斥的 `DELETE_LIST` 或 `KEEP_LIST`。
- 解析 `policy_watermark`。
- 解析 `schema_id + schema_version`。
- 解析可选的 `fe_observed_max_version`。
- 对 tenant 列表排序并去重。

首期请求中没有 `recordTimestamp`、cutoff 或 TTL 天数。BE 接收到的是 FE 已经计算完成的最终业务谓词。

请求模型的公共规范化逻辑位于：

- [tenant_ttl_compaction_types.cpp:21](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_compaction_types.cpp:21)

## 三、HTTP 请求如何进入实际任务

核心入口：

- [tenant_ttl_compaction_action.cpp:291](/Users/zhuchong/Documents/code/starrocks-main/be/src/http/action/tenant_ttl_compaction_action.cpp:291)

关键代码是：

```cpp
EngineTenantTtlCompactionTask task(std::move(request));
(void)task.execute();
HttpChannel::send_reply(...);
```

这意味着当前 HTTP 入口是同步执行：

1. HTTP 工作线程创建任务。
2. 直接调用 `execute()`。
3. 等待整个 Tenant-TTL Compaction 完成。
4. 最后才发送 HTTP 响应。

它不是“提交后台任务后立即返回”的异步接口。

当前 HTTP 入口构造任务时没有传入连接级取消标志。因此，即使客户端请求超时或丢失响应，已经开始的任务也不会因为 HTTP 连接断开而自动取消。

## 四、Tablet 基础准入

主任务函数：

- [engine_tenant_ttl_compaction_task.cpp:309](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:309)

开始阶段依次执行：

1. 请求规范化。
2. 通过 `tablet_id` 查找 Tablet。
3. 校验 Tablet 和 tenant 列。
4. 获取 Tablet 级独占权。
5. 捕获完整 coverage。

Tablet 和 tenant 列检查在：

- [engine_tenant_ttl_compaction_task.cpp:219](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:219)

目前要求：

- `partition_id` 匹配。
- Tablet 为 `RUNNING`。
- 本地 shared-nothing Tablet。
- 非 PK Tablet。
- `DUP_KEYS`。
- `tenant_column_unique_id` 存在。
- tenant 列为 nullable 或 non-nullable `VARCHAR`。

这里不检查：

- `tenant_id` 生成列。
- 排序键。
- Short Key。
- ZoneMap 能力。
- `recordTimestamp`。

因此首期正确性不依赖这些优化条件。

## 五、Tablet 级互斥

入口：

- [tenant_ttl_tablet_guard.cpp:29](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_tablet_guard.cpp:29)

`TenantTtlTabletGuard::try_acquire()` 分三步：

### 1. Tablet 状态准入

调用：

- [tablet.cpp:425](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:425)

`Tablet::try_begin_tenant_ttl()` 在 `_compaction_task_lock` 下：

- 检查是否已有 TTL 任务。
- 检查是否有普通 Compaction 正在运行。
- 将状态从 `IDLE` 变成 `PENDING`。
- 保存临时 owner：`task_id + policy_watermark + generation`。

### 2. 获取 Compaction 锁

按照固定顺序尝试独占：

- `base_lock`
- `cumulative_lock`

对应代码：

- [tenant_ttl_tablet_guard.cpp:47](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_tablet_guard.cpp:47)

任意一个锁拿不到就立即返回 `TABLET_BUSY`，不会在 HTTP 请求里长时间等待锁。

### 3. 进入 RUNNING

- [tablet.cpp:448](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:448)

两把锁都拿到后，状态从 `PENDING` 变成 `RUNNING`。

任务结束时，Guard 自动释放两把锁，并使用 generation 清理对应 owner：

- [tenant_ttl_tablet_guard.cpp:72](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_tablet_guard.cpp:72)
- [tablet.cpp:466](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:466)

## 六、捕获完整 coverage

入口：

- [tenant_ttl_tablet_guard.cpp:91](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_tablet_guard.cpp:91)
- [tablet.cpp:480](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:480)

首期 coverage 定义为：

> 当前 Tablet `_rs_version_map` 中，从版本 0 到当前最大连续版本的全部 active Rowset。

捕获时会：

- 按版本顺序读取所有 active Rowset。
- 要求版本路径连续。
- 要求 Rowset 为 `VISIBLE`。
- 要求 Segment 为 `NONOVERLAPPING`。
- 拒绝 delete predicate、partial update。
- 校验所有 Rowset schema 一致。
- 检查 Rowset 当前没有处于 compacting。
- 加载 Rowset。
- 保存每个 Rowset 的：
  - version
  - `expected_rowset_id`
  - immutable Rowset 引用
- 保存完整 schema identity。
- 生成供诊断使用的 `coverage_digest`。
- 将 coverage 中所有 Rowset 设置为 `is_compacting=true`。

设置 `is_compacting` 的位置：

- [tablet.cpp:586](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:586)

任务结束后统一清除：

- [tablet.cpp:593](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:593)

如果 FE 携带了 `fe_observed_max_version`，BE 还会检查本副本的连续版本是否至少追上 FE 观察到的下界。

## 七、扫描 tenant 并产生 Segment 计划

创建过滤器：

- [engine_tenant_ttl_compaction_task.cpp:389](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:389)

逐 Segment 调用：

- [engine_tenant_ttl_compaction_task.cpp:415](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:415)
- [tenant_ttl_row_filter.cpp:93](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_row_filter.cpp:93)

当前实现只打开 tenant 列并精确扫描，没有使用 Short Key、ZoneMap 或排序键优化。

语义如下：

- `DELETE_LIST`：列表内 tenant 删除，其他 tenant 保留。
- `KEEP_LIST`：列表内 tenant 保留，其他非 NULL tenant 删除。
- tenant 为 NULL：始终保留。

扫描过程中生成需要保留的 Row ID 范围 `SparseRange`，然后将 Segment 分类为：

- `KEEP`：没有命中删除行。
- `DROP`：所有行都命中删除。
- `REWRITE`：一部分保留、一部分删除。

分类代码：

- [tenant_ttl_row_filter.cpp:174](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tenant_ttl_row_filter.cpp:174)

## 八、Rowset 和 Segment 的 KEEP/DROP/REWRITE

主循环支持多个 Rowset：

- [engine_tenant_ttl_compaction_task.cpp:398](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:398)

每个 Rowset 都会遍历其全部 Segment。

### Rowset 完全没有命中

如果一个 Rowset 的 `deleted_rows == 0`：

- 标记为 `VERIFIED_NO_CHANGE`。
- 不生成新 Rowset。
- 不参与元数据替换。

对应代码：

- [engine_tenant_ttl_compaction_task.cpp:439](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:439)

因此多个 Rowset 中，可以只替换包含目标 tenant 的 Rowset，其他 Rowset 保持不变。

### Segment KEEP

入口：

- [filtered_rowset_writer.cpp:299](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/filtered_rowset_writer.cpp:299)

KEEP Segment 通过 Hard Link 复用：

- [filtered_rowset_writer.cpp:236](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/filtered_rowset_writer.cpp:236)

除了 `.dat` Segment 文件，还会复制其外部索引制品，例如倒排索引目录和向量索引文件。

### Segment DROP

- [filtered_rowset_writer.cpp:315](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/filtered_rowset_writer.cpp:315)

不向输出 Rowset 写入该 Segment。

如果一个 Rowset 的全部行都删除，最终仍会构造一个相同 version 的空 Rowset 来替换旧 Rowset，确保 Tablet 的版本链不断裂。结果中的 Rowset action 会标记为 `DROP`。

### Segment REWRITE

- [filtered_rowset_writer.cpp:331](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/filtered_rowset_writer.cpp:331)

调用：

- [vertical_segment_rewriter.cpp:105](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/vertical_segment_rewriter.cpp:105)

主要作用是：

- 根据 TabletSchema 将列拆成 Vertical Compaction 风格的列组。
- 每个列组都使用同一份 `keep_row_ranges`。
- SegmentReader 只读取需要保留的 Row ID。
- 将保留行写入新的 Segment。
- 校验每个列组写出的行数完全一致。
- 最后写入 footer，并统计数据、索引和物理文件大小。

核心 Row ID 范围下推位置：

- [vertical_segment_rewriter.cpp:166](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/vertical_segment_rewriter.cpp:166)

## 九、构造新的同版本 Rowset

输出 Rowset 上下文创建位置：

- [engine_tenant_ttl_compaction_task.cpp:259](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:259)

新的 Rowset：

- 获得全新的 Rowset ID。
- version 与源 Rowset 完全相同。
- 复用捕获到的 schema identity。
- 保留源 Rowset 的 GTID。
- 标记为 `VISIBLE`、`NONOVERLAPPING`、compaction output。

完成所有 Segment 后调用：

- [filtered_rowset_writer.cpp:339](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/filtered_rowset_writer.cpp:339)

`build()` 不只是生成 RowsetMeta，还会重新执行：

- `rowset->load()`
- `rowset->verify()`

提交前如果任意步骤失败，临时输出由 `StagedTenantTtlOutputs` 清理：

- [engine_tenant_ttl_compaction_task.cpp:190](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:190)
- [filtered_rowset_writer.cpp:359](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/filtered_rowset_writer.cpp:359)

## 十、零命中和幂等收敛

全 coverage 扫描结束，如果：

```text
deleted_rows == 0
```

任务会再次校验 coverage，然后返回：

```text
NOOP_VERIFIED
```

对应代码：

- [engine_tenant_ttl_compaction_task.cpp:518](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:518)

此时：

- 不生成新 Rowset。
- 不替换任何 Rowset。
- 不保存 AppliedProof。
- 不持久化 tenant 谓词。
- 不持久化 task ID。

这也是同一 tenant 谓词重试的最终幂等机制：第一次已经删除后，第二次重新扫描会发现零命中并返回 `NOOP_VERIFIED`。

## 十一、提交前 CAS 和 Rowset ID 校验

提交入口：

- [tablet.cpp:651](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:651)

提交分两层校验。

### 1. 输出 Rowset 校验

检查：

- output Rowset ID 不能等于 source Rowset ID。
- output version 必须与 source version 相同。
- replacement 必须属于捕获到的 coverage。
- schema、Tablet UID、partition、GTID 等元数据一致。
- 输出 Rowset 可加载且校验通过。
- 输出行数不大于源 Row数。

### 2. coverage CAS 校验

核心代码：

- [tablet.cpp:601](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:601)

在 Tablet 元数据锁和 schema 锁下重新确认：

- schema shared pointer identity 没有变化。
- schema ID/version 没有变化。
- coverage 版本链仍然连续。
- 每个 version 当前对应的 Rowset 仍然存在。
- 当前 Rowset ID 等于捕获时的 `expected_rowset_id`。

关键判断：

- [tablet.cpp:626](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:626)

```cpp
const auto current = get_rowset_by_version(entry.version);
if (current == nullptr ||
    current->rowset_id() != entry.expected_rowset_id) {
    return STALE_ROWSET;
}
```

这保证即使 version 没变，但该 version 已被另一个流程替换成新的 Rowset，也不会错误覆盖新的数据。

## 十二、原子替换和旧 Rowset 回收

CAS 通过后，Tenant-TTL 专用提交函数才调用：

- [tablet.cpp:374](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/tablet.cpp:374)

`modify_rowsets_without_lock()` 的作用是：

1. 从当前 version 映射中移除旧 Rowset。
2. 加入相同 version 的新 Rowset。
3. 更新 TabletMeta。
4. 将旧版本信息加入 stale version tracker。

它没有被改造成普通 Compaction 的强制 CAS 路径。Tenant-TTL 所需的附加校验被封装在 `commit_tenant_ttl_rowsets()` 外层，因此不改变现有普通 Compaction 的行为。

任务提交完成后：

- 保存一次 TabletMeta。
- 关闭旧输入 Rowset。
- 将被替换 Rowset 放入 `unused_rowset` 链路，等待引用归零后 GC。

任务侧调用位置：

- [engine_tenant_ttl_compaction_task.cpp:531](/Users/zhuchong/Documents/code/starrocks-main/be/src/storage/task/engine_tenant_ttl_compaction_task.cpp:531)

## 十三、HTTP 返回

结果序列化：

- [tenant_ttl_compaction_action.cpp:220](/Users/zhuchong/Documents/code/starrocks-main/be/src/http/action/tenant_ttl_compaction_action.cpp:220)

响应包含：

- `code`
- `retryable`
- `task_id`
- `snapshot_end_version`
- `processed_through_version`
- `coverage_digest`
- 扫描、保留、删除行数
- Hard Link 与重写字节数
- 每个源 Rowset 的 version、Rowset ID、action
- 实际 output Rowset ID
- Segment KEEP/DROP/REWRITE 计数

HTTP 状态映射位于：

- [tenant_ttl_compaction_action.cpp:194](/Users/zhuchong/Documents/code/starrocks-main/be/src/http/action/tenant_ttl_compaction_action.cpp:194)

典型映射：

- `SUCCESS`、`NOOP_VERIFIED` → 200
- 参数或数据不变量错误 → 400
- Rowset/schema 已变化 → 409
- Tablet 忙、已有 TTL、未追上版本 → 503
- 内部错误 → 500

需要特别注意：HTTP 超时只意味着调用方没有收到结果，不代表任务失败或回滚。调用方重试时：

- 原任务仍在运行：通常返回 `TTL_ALREADY_RUNNING`。
- 原任务已经成功：重新扫描并以 `NOOP_VERIFIED` 收敛。
- 原任务在提交前失败：临时文件被清理，重试重新执行。
- 原任务已经提交但响应丢失：重试不会再生成新的 Rowset。