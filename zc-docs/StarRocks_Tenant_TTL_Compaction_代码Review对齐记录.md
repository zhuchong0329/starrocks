# StarRocks Tenant-TTL Compaction 代码 Review 对齐记录

> 用途：单独记录第 6 轮完成后、第 7 轮开始前的代码 review 问题、已确认结论、待实施修改及验证要求。  
> 建立日期：2026-09-08  
> 当前状态：持续追加；已确认 review 项按详细编码计划的独立轮次实现和验证。
> 约束：本文件只记录 review 修正，不重复需求澄清文档中的完整背景。

## Review 项状态约定

- `讨论中`：问题已提出，结论尚未确认，不得据此修改实现。
- `已确认，待实现`：设计已经对齐，可进入后续集中修正。
- `已实现，待验证`：代码已经修改，尚未完成约定测试。
- `已完成`：代码、测试和文档均已收口。

## REVIEW-001：Tenant-TTL 与 Storage Migration 的互斥

状态：已确认，待实现  
确认日期：2026-09-08  
涉及代码：`TenantTtlTabletGuard`、`EngineStorageMigrationTask`、`Tablet::check_migrate()`

### 1. Review 问题

当前 Tenant-TTL 已通过 Tablet admission 串行同 Tablet 的 TTL 任务，并排他获取 `base_lock`、`cumulative_lock`，但没有参与 `_migration_lock` 协议。

Storage migration 会先捕获旧 Rowset，释放 `_migration_lock` 后复制文件，最终重新取得锁并检查最大结束版本。Tenant-TTL 删除业务行时使用同 version Rowset 替换，不会改变最大版本，因此 migration 的版本检查无法发现这次替换。

可能出现以下竞态：

```text
Migration                              Tenant-TTL
捕获旧 Rowset R、end_version=N
释放 migration 写锁并复制 R
                                       过滤 R，生成同 version 的 R'
                                       提交 R'，返回 SUCCESS
重新取得 migration 写锁
只检查 max_version 仍为 N，校验通过
使用旧 R 创建并切换到新 Tablet
```

最终结果是 FE 已收到 Tenant-TTL 成功，但迁移后的 Tablet 又包含旧数据，破坏 `SUCCESS` 的任务语义。

### 2. 源码依据

- `be/src/storage/task/engine_storage_migration_task.cpp:105-216`：migration 在 `_migration_lock` 排他临界区捕获 Rowset 并设置 `is_migrating=true`。
- `be/src/storage/task/engine_storage_migration_task.cpp:223-235`：复制文件时释放锁，完成阶段重新取得排他锁。
- `be/src/storage/task/engine_storage_migration_task.cpp:324-345`：非 PK migration 完成前只比较最大结束版本，不比较完整 `(version,rowset_id)`。
- `be/src/storage/tablet.cpp:1265-1275`：`Tablet::check_migrate()` 同时检查 `is_migrating()` 和 TabletManager 中的当前 Tablet 对象身份。
- `be/src/storage/tenant_ttl_tablet_guard.cpp:47-60`：当前 Tenant-TTL 已对 base/cumulative locks 使用排他 try-lock，但尚未获取 migration 共享锁。

### 3. 已确认结论

Tenant-TTL 必须参与 `_migration_lock` 协议。完整锁模式如下：

| 锁或状态 | Tenant-TTL 获取方式 | 作用 |
| --- | --- | --- |
| Tablet TTL admission | `_compaction_task_lock` 同步域内取得唯一 owner | 串行同 Tablet TTL，并阻止后续普通 Compaction 创建 |
| `_migration_lock` | 共享、非阻塞 try-lock | 阻止 storage migration 在 TTL 执行期间开始或完成 Tablet 切换 |
| `base_lock` | 排他、非阻塞 try-lock | 阻止 Base/对应普通 Compaction 执行 |
| `cumulative_lock` | 排他、非阻塞 try-lock | 阻止 Cumulative/对应普通 Compaction 执行 |

`base_lock` 和 `cumulative_lock` 对 Tenant-TTL 必须保持排他模式。普通 Compaction 只取得其中一把共享锁，Tenant-TTL 需要同时阻塞两类 Compaction，因此不能把 TTL 的两把锁改成共享模式。当前实现已经使用 `std::unique_lock`，这一点不需要改变，只需通过测试继续锁定。

### 4. 固定获取与释放顺序

获取顺序固定为：

```text
TTL admission
  -> migration_lock(shared try-lock)
  -> Tablet::check_migrate(tablet)
  -> base_lock(unique try-lock)
  -> cumulative_lock(unique try-lock)
  -> TTL_PENDING 转为 TTL_RUNNING
```

释放顺序固定为：

```text
清理 coverage Rowset 的 is_compacting
  -> cumulative_lock
  -> base_lock
  -> migration_lock
  -> 清除 TTL admission/owner
```

三把锁必须在捕获 coverage 之前全部获得，并持有到扫描、staged output 构建、完整 CAS 提交以及失败清理全部结束。只在开始时检查 migration 状态后立即释放共享锁不能建立互斥。

### 5. 为什么必须使用 `Tablet::check_migrate()`

取得 migration 共享锁后不能只检查 `is_migrating()`。

可能存在下面的对象身份竞态：

1. Tenant-TTL 从 TabletManager 取得旧 Tablet 指针。
2. migration 完成 Tablet 切换，并把旧对象的 `is_migrating` 恢复为 false。
3. Tenant-TTL 随后取得旧对象上的 migration 共享锁。
4. 如果只检查 `is_migrating()==false`，任务会继续修改已经失效的旧 Tablet，并可能错误返回成功。

`Tablet::check_migrate(tablet)` 还会比较传入对象与 TabletManager 当前对象是否相同，因此能够拒绝该场景。发现对象已被替换后，当前任务不得中途切换到新 Tablet 继续执行，而应返回可重试状态，让 FE 重新下发并从最新 Tablet 重新固定 coverage。

### 6. 失败语义

以下情况统一返回可重试 `TABLET_BUSY`：

- migration 处于初始或最终排他临界区，TTL 无法取得共享锁。
- migration 正在无锁复制阶段，TTL 取得共享锁但 `check_migrate()` 发现 `is_migrating=true`。
- migration 已完成切换，TTL 持有的 Tablet 不再是 TabletManager 当前对象。
- 后续 base/cumulative 任一排他 try-lock 失败。

任一失败都必须释放本次已经取得的锁和 admission，不在 BE 内等待、sleep 或循环抢锁，不改变首期“不处理 try-lock 饥饿”的结论。

### 7. 为什么 Rowset ID CAS 不能替代 migration 锁

Rowset ID CAS 只证明 Tenant-TTL 提交瞬间 active version map 仍对应任务开始时捕获的 Rowset。Migration 可能已经在更早时复制了旧 Rowset，并在 TTL 成功后仅用最大版本完成校验。

因此：

- Rowset ID CAS 防止 Tenant-TTL 覆盖其他并发结果。
- migration 共享锁防止迁移遗漏 Tenant-TTL 已经成功提交的逻辑删除。

两者解决不同方向的竞态，必须同时保留。

### 8. 待实施修改

1. 在 `TenantTtlTabletGuard` 中增加生命周期覆盖完整任务的 `std::shared_lock<std::shared_mutex> _migration_lock`。
2. `try_acquire()` 在取得 TTL admission 后，先对 `_migration_lock` 执行共享 try-lock。
3. 取得共享锁后调用 `Tablet::check_migrate(_tablet)`。
4. migration 冲突或 Tablet 对象已失效时，返回 `TABLET_BUSY` 并通过统一 `release()` 清理。
5. 保持现有 `base_lock`、`cumulative_lock` 的 `std::unique_lock` 排他模式。
6. `release()` 按 `cumulative -> base -> migration -> admission` 释放；coverage guard 必须仍先于 tablet guard 析构。

### 9. 验证要求

至少补充以下确定性测试：

1. migration 排他锁被占用时，TTL 快速返回 `TABLET_BUSY`，状态恢复为 `TTL_IDLE`。
2. `is_migrating=true`、但 migration 共享锁可获得时，`Tablet::check_migrate()` 仍拒绝 TTL。
3. TTL 为 `RUNNING` 时，migration 无法取得 `_migration_lock` 排他锁。
4. TTL 已取得 migration 共享锁后，base 或 cumulative try-lock 失败会释放 migration 锁和 admission。
5. Tenant-TTL 持有的旧 Tablet 已被 TabletManager 替换时，不修改旧对象并返回可重试状态。
6. 正常成功、取消、构建失败和提交冲突路径均不泄漏 migration 共享锁。
7. 现有 Base/Cumulative 双向准入测试继续证明 Tenant-TTL 对两把 Compaction lock 使用排他模式。

### 10. 范围边界

本项只修复 storage migration 与 Tenant-TTL 的竞态。Clone、schema change 等同样获取 migration 共享锁的流程不会因此与 Tenant-TTL 自动互斥；是否需要额外协调，后续按各自逻辑数据语义单独提出 review 项并对齐。

## REVIEW-005（P0）：用 reader 引用保护 coverage Rowset 的 Segment 生命周期

状态：已确认，待实现

确认日期：2026-09-08

实施轮次：第 8 轮（优先于 overlap、delete predicate 和 generation 修正）

涉及代码：`Tablet::capture_tenant_ttl_coverage()`、`Tablet::release_tenant_ttl_coverage()`、`TenantTtlCoverageGuard`、`EngineTenantTtlCompactionTask`

### 1. Review 问题

当前 coverage 捕获只对源 Rowset 执行 `load()` 并设置 `is_compacting=true`，后续却会通过 `entry.source->segments()[segment_id]` 直接访问 Segment。

`load()` 只保证调用完成时 Rowset 已加载，不会在后续读取期间固定 `_segments`；`is_compacting` 只阻止普通 Compaction 重复选中该 Rowset，MetadataCache 淘汰和其他 `close()` 调用不检查该标记。当 `_refs_by_reader == 0` 时，`Rowset::close()` 可以调用 `do_close()` 清空 `_segments`。

因此可能出现：

```text
Tenant-TTL                         MetadataCache / other close path
rowset->load()
is_compacting = true
                                   rowset->close()
                                   refs_by_reader == 0
                                   _segments.clear()
segments()[segment_id]
```

此时既可能越界访问空 vector，也可能与并发 `clear()` 形成数据竞争。`shared_ptr<Rowset>` 只保证 Rowset 对象本身不析构，不保证其内部 `_segments` 保持加载。

### 2. 源码依据

- `be/src/storage/rowset/metadata_cache.cpp`：MetadataCache value 淘汰时调用 `rowset->close()`。
- `be/src/storage/rowset/rowset.h`、`rowset.cpp`：`close()` 在 reader 引用为零时执行 `do_close()`，后者清空 `_segments`；有 reader 引用时则进入延迟卸载状态。
- `be/src/storage/tablet_reader.cpp`：普通 TabletReader 在逐个 `load()` 前先对全部输入调用 `Rowset::acquire_readers()`，`close()` 时统一 `release_readers()`。
- `be/src/storage/rowset/rowset_merger.cpp`：直接 Segment iterator 路径使用 `RowsetReleaseGuard`执行同类生命周期保护。
- `be/src/storage/tablet.cpp`、`be/src/storage/task/engine_tenant_ttl_compaction_task.cpp`：当前 Tenant-TTL 缺少 reader 引用，但在整个规划、读取、link/rewrite 和提交阶段持续访问源 Segment。

### 3. 已确认结论

coverage 中全部源 Rowset 必须同时持有两种独立保护：

- `is_compacting=true`：用于 Compaction 调度与选择互斥。
- reader 引用：用于阻止 `close()` 在读取期间真正卸载 Segment。

安全顺序固定为：

```text
捕获并验证 coverage 元数据
  -> Rowset::acquire_readers(all_sources)
  -> 逐个 rowset->load()
  -> 全部成功后设置 is_compacting=true
  -> 规划 / 读取 / link / rewrite / CAS 提交或 NOOP 复查
  -> 清除 is_compacting
  -> Rowset::release_readers(all_sources)
```

`TenantTtlCoverageGuard` 在 coverage 成功后同时拥有 compacting 标记和 reader 引用，并在所有数据访问结束后统一释放。引用必须在任何 `load()` 之前批量取得，不能在已经 load 完成后才补充。

提交路径对 changed source 调用 `close()` 与该方案不冲突：只要 coverage guard 仍持有 reader 引用，`close()` 只能记录延迟卸载；在 guard 最终释放引用后才能真正清理 Segment。

### 4. 失败原子性与释放要求

1. 在批量 acquire 后任意 Rowset `load()` 失败，必须释放本批全部 reader 引用，清空未成功的 coverage，不得遗留 `is_compacting`。
2. 成功、`NOOP_VERIFIED`、取消、读/写失败、提交 CAS 冲突和 TabletMeta 保存失败等所有出口都必须恢复 reader 引用计数。
3. coverage guard 必须在 tablet guard 释放 migration/base/cumulative/admission 之前析构，避免读保护尚未清理时放开 Tablet 级互斥。
4. 本修复不将 reader 引用写入 TabletMeta，不改变 Rowset ID、version、stale/unused 生命周期或普通 Compaction 代码。

### 5. 验证要求

1. coverage 成功后主动对源 Rowset 调用 `close()` 模拟 MetadataCache 淘汰，验证 `_segments` 未被提前清空、Tenant-TTL 可继续读取并正确完成。
2. 单 Rowset 和多 Rowset coverage 均验证 reader 引用在 guard 持有期增加，退出后恢复到执行前值。
3. 批量 acquire 后中途 `load()` 失败，验证全部源 Rowset 无引用泄漏。
4. 覆盖正常成功、`NOOP_VERIFIED`、取消、Writer 失败、Rowset ID CAS 冲突和 TabletMeta 保存失败的释放断言。
5. 现有 coverage 完整性、KEEP/DROP/REWRITE、stale/unused 回收和普通 Compaction 回归测试继续通过。

### 6. 范围边界

本项只修复源 Rowset 在 Tenant-TTL 整个使用期内的 Segment 卸载竞态，不在本轮处理 Segment overlap、delete predicate、generation 语义常量或第二批增强测试。

## REVIEW-002：支持 Segment `OVERLAPPING` / `OVERLAP_UNKNOWN` Rowset

状态：已确认，待实现  
确认日期：2026-09-08  
涉及代码：`Tablet::capture_tenant_ttl_coverage()`、`FilteredRowsetWriter`、`make_output_context()`、`Tablet::commit_tenant_ttl_rowsets()`

### 1. Review 问题

当前 Tenant-TTL 在捕获 coverage 时要求 Rowset 的 `segments_overlap` 必须为 `NONOVERLAPPING`，并将替换 Rowset 的输出强制标记为 `NONOVERLAPPING`。这会直接拒绝由多个键范围可能相交 Segment 组成的 `OVERLAPPING` 或 `OVERLAP_UNKNOWN` Rowset。

`OVERLAPPING` 表示同一 Rowset 内多个 Segment 的键范围可能相交；`OVERLAP_UNKNOWN` 表示缺少足够证据确认 Segment 是否重叠。两者都不表示 Segment 不可读或数据已损坏。当前 Tenant-TTL 只支持 `DUP_KEYS`，每行是否保留只由该行 tenant 决定，不涉及 `UNIQUE_KEYS`/`AGG_KEYS` 的去重或聚合语义。因此可以对每个 Segment 独立执行精确过滤，无需先将全部 Segment 合并成全局有序流。

### 2. 已确认结论

`OVERLAPPING` 和 `OVERLAP_UNKNOWN` 都不再是 Tenant-TTL 的准入失败条件。对这两类 Rowset 使用相同的现有逐 Segment 精确过滤路径：

1. 逐个 Segment 扫描业务 tenant 列，构建基于物理 row-id 的 KEEP/DROP/REWRITE 计划。
2. KEEP Segment 继续通过 Hard Link 复用。
3. DROP Segment 不写入替换 Rowset。
4. REWRITE Segment 按保留 row-id ranges 重写。
5. 不在 Tenant-TTL 内启动前置普通 Compaction，不返回 `TABLET_BUSY` 等待 FE 通过后续重试完成过滤。
6. 不强制走 Merge Iterator 来生成 `NONOVERLAPPING` Rowset。
7. Tenant-TTL 在本次任务内直接完成过滤和同 version 替换，正常返回 `SUCCESS` 或 `NOOP_VERIFIED`。

对 `DUP_KEYS` 的行级 tenant 谓词，逐 Segment 过滤与先 merge 后过滤的保留行集合相同。该结论依赖当前 Tenant-TTL 的 `DUP_KEYS` 门禁，不延伸到其他 keys type。StarRocks 现有 `RowsetMeta::is_segments_overlapping()` 对多 Segment、single-version Rowset 也将 `OVERLAP_UNKNOWN` 与 `OVERLAPPING` 一样作为可能重叠处理。

### 3. 输出 overlap 属性规则

过滤不会扩大 Segment 的键范围，但仅凭过滤结果不重新计算所有 Segment 的精确边界关系。因此输出采用保守标记：

- 源 Rowset 为 `OVERLAPPING`，且输出仍有两个或以上 Segment 时，输出保持 `OVERLAPPING`。
- 源 Rowset 为 `OVERLAP_UNKNOWN`，且输出仍有两个或以上 Segment 时，输出保持 `OVERLAP_UNKNOWN`；不在没有额外证据时改成 `OVERLAPPING` 或 `NONOVERLAPPING`。
- 过滤后只剩一个或零个 Segment 时，输出可标记为 `NONOVERLAPPING`。
- 即使过滤后的多个 Segment 实际已经不交叉，继续保留 `OVERLAPPING` 也是安全的保守结果；它只可能增加后续 Compaction 成本，不影响查询正确性。
- 后续普通 Cumulative Compaction 可以按现有机制将该 Rowset 整理为 `NONOVERLAPPING`，但这不是 Tenant-TTL 成功的前置条件。

### 4. 不选择前置 Compaction 的原因

Tenant-TTL 执行时已经占有 Tablet admission，并排他持有 base/cumulative locks。如果在发现 `OVERLAPPING` 后发起普通 Compaction，必须先释放 TTL 的 coverage、锁和 admission，才能避免自身互斥。这会将一个 TTL 请求拆成两个非原子的异步阶段，并引入：

- 普通 Compaction 选取范围与调度时机不确定。
- FE 重试前预处理不一定完成。
- 重复 TTL 请求可能重复触发预处理。
- 任务成功语义需要依赖 FE 多轮重试才能收敛。

这与已确认的“Tenant-TTL 应在本次任务中干净完成”语义不符，因此不采用。

### 5. Merge Iterator 的定位

使用 `new_heap_merge_iterator()` 合并可能 overlapping 的 Segment 在技术上可行，普通 `DUP_KEYS` Compaction 也使用该路径。但它会迫使所有保留行和全部列重新写入，无法保留 KEEP Segment 的 Hard Link 优化，并扩大实现与测试范围。

因此 Merge Iterator 不作为本轮支持 `OVERLAPPING` / `OVERLAP_UNKNOWN` 的必需正确性路径。未来如果需要“Tenant-TTL 同时消除 Segment overlap”，可以作为独立的可选优化评估。

### 6. 待实施修改

1. 移除 coverage 捕获对 `OVERLAPPING` 和 `OVERLAP_UNKNOWN` Rowset 的准入拒绝。
2. 使输出 `RowsetWriterContext` 按源 Rowset 保守设置 overlap 属性，而不是固定设为 `NONOVERLAPPING`。
3. 修改 Tenant-TTL 提交校验：校验输出 overlap 属性与源 Rowset 和输出 Segment 数的组合是否合法，不再无条件要求 `NONOVERLAPPING`。
4. 保持现有 coverage Rowset ID CAS、schema identity CAS、行数守恒、取消、内存限制和 staged output 清理语义不变。
5. 对多 Segment 输出要求原样继承源 Rowset 的 `OVERLAPPING` 或 `OVERLAP_UNKNOWN`；对零或单 Segment 输出允许归一化为 `NONOVERLAPPING`。

### 7. 验证要求

至少覆盖以下确定性测试：

1. 一个 `OVERLAPPING` Rowset 含多个键范围相交的 Segment，目标 tenant 分布在多个 Segment 中，TTL 后精确删除所有目标行。
2. 同一 `OVERLAPPING` Rowset 内同时覆盖 Segment KEEP、DROP、REWRITE，校验行数守恒和最终数据集合。
3. 输出保留两个或以上 Segment 时，Rowset 标记仍为 `OVERLAPPING`。
4. 过滤后输出仅有一个或零个 Segment 时，Rowset 标记为 `NONOVERLAPPING`。
5. 全 coverage 零命中时返回 `NOOP_VERIFIED`，不生成替换 Rowset，源 overlap 元数据不变。
6. 重复执行相同 tenant 谓词时收敛到 `NOOP_VERIFIED`，不再次重写。
7. 校验 KEEP Segment 仍通过 Hard Link 复用，并且替换后 Rowset 可正常读取、可被后续普通 Cumulative Compaction 正常处理。
8. 使用多 Segment `OVERLAP_UNKNOWN` Rowset 重复执行上述正确性、KEEP/DROP/REWRITE 和可读性测试，输出仍为多 Segment 时校验保持 `OVERLAP_UNKNOWN`。
9. `OVERLAP_UNKNOWN` Rowset 过滤后只剩零或一个 Segment 时，校验输出归一化为 `NONOVERLAPPING`。

## REVIEW-003：支持 coverage 中存在 delete predicate

状态：已确认，待实现  
确认日期：2026-09-08  
涉及代码：`PushHandler::_delete_convert()`、`Tablet::capture_tenant_ttl_coverage()`、`DeletePredicates`、`Tablet::commit_tenant_ttl_rowsets()`

### 1. Review 问题

当前 Tenant-TTL 只要发现 coverage 内某个 Rowset 带有 delete predicate，就将整个任务拒绝为 `NOT_SUPPORTED`。但 shared-nothing、非 PK Tablet 的正常 DELETE 会生成一个专用的零行 predicate Rowset。这个 Rowset 仅承载 delete predicate 及其 version，本身不承载需要执行 Tenant-TTL 过滤的数据行。

如果仅因 coverage 中存在这类标准 predicate Rowset 就拒绝 Tenant-TTL，存在过普通 DELETE 的 Tablet 将无法执行 tenant 物理回收。

### 2. delete predicate 的版本语义

delete predicate 不是只与某个物理 Rowset ID 绑定。读取时根据 predicate version 和数据 Rowset 的 end version 决定作用范围。例如：

```text
V0：数据 Rowset
V1：数据 Rowset
V2：delete predicate Rowset
V3：数据 Rowset
```

V2 的 predicate 会作用于 V0、V1，不作用于 V3。Tenant-TTL 将 V0、V1 替换为同 version 的 V0′、V1′，同时保持 V2 不变时，predicate 对替换后 Rowset 的作用范围不变，不会导致已逻辑删除的数据复活。

### 3. 已确认结论

Tenant-TTL 首期支持 coverage 中存在标准零行 delete-predicate Rowset，但不在 Tenant-TTL 中消费、物化或清理该 predicate：

1. coverage 准入允许 `has_delete_predicate=true` 且 `num_rows=0` 的 Rowset。
2. predicate Rowset 保持原 Rowset ID、version 和全部 predicate 元数据不变，不生成 replacement。
3. 任务结果中将该 Rowset 记录为 `VERIFIED_NO_CHANGE`，其 `source_rows`、`kept_rows`、`deleted_rows` 均为零。
4. predicate Rowset 仍是完整 coverage 的一部分，提交前必须参与 version + Rowset ID CAS，不得因为不生成 replacement 而排除在一致性校验之外。
5. 普通数据 Rowset 仍扫描原始物理 tenant 列，只应用 Tenant-TTL 的 `DELETE_LIST` 或 `KEEP_LIST`，不把现有 delete predicate 并入 TTL 过滤器。
6. TabletMeta 中的 delete predicate 集合保持不变；后续普通 Compaction 继续负责将 predicate 物化到输出数据并按现有机制清理 predicate 元数据。

### 4. 为什么 TTL 扫描不应应用现有 delete predicate

Tenant-TTL 的目标是物理删除指定 tenant 的行。某些目标 tenant 行可能已被普通 delete predicate 逻辑隐藏，但其物理数据仍然存在。

如果 Tenant-TTL 只扫描应用了 delete predicate 后的逻辑可见行，就可能对这些仍然存在的物理行返回 `NOOP_VERIFIED`。因此 TTL 必须继续按 Segment 扫描原始 tenant 值，确保命中的 tenant 数据真正被物理移除。已有 delete predicate 保持独立生效，两者不会互相导致数据复活。

### 5. 非零行 predicate-bearing Rowset

当前 shared-nothing、非 PK 的正常 DELETE 路径会先构建零行 Rowset，再附加 delete predicate。普通 Compaction 会消费 predicate 并生成不带 predicate 的数据 Rowset；Shortcut Compaction 也明确避免直接复用含 predicate 的输入。因此正常生产路径不应生成：

```text
has_delete_predicate = true
num_rows > 0
```

如果 Tenant-TTL 遇到该组合，不得跳过该 Rowset 后返回成功，也不在首期猜测如何替换和维护 predicate 生命周期。统一返回非重试的 `DATA_INVARIANT_VIOLATION`，并在错误信息中输出 tablet ID、version、Rowset ID 和行数便于诊断。

这是对非标准元数据、定制代码、不兼容历史数据或存储损坏的防御性保护，正常环境不应命中。

### 6. 为什么不复制或替换 predicate Rowset

delete predicate 同时出现在 RowsetMeta 和 TabletMeta 管理的 predicate 集合中。旧 predicate Rowset 进入 stale 后，后续 GC 会按其 version 删除 TabletMeta 中的 predicate。

如果 Tenant-TTL 只把 predicate 复制到同 version 新 Rowset，但没有改造 stale GC 的 predicate 所有权语义，旧 Rowset 的延迟 GC 仍可能删除正在使用的 TabletMeta predicate，导致逻辑删除失效。因此首期必须将标准 predicate Rowset 作为不可替换的 coverage 成员保留。

### 7. 待实施修改

1. 拆分 coverage 中对 delete predicate 和 partial update 的统一拒绝逻辑；partial update 门禁保持不变。
2. 对 predicate-bearing Rowset 校验 `num_rows == 0`；不满足时返回 `DATA_INVARIANT_VIOLATION`。
3. 零行 predicate Rowset 加入 coverage 和 CAS，但跳过 Tenant-TTL Segment 计划、writer 创建和 replacement 生成。
4. 提交前继续验证 predicate Rowset 的 version + Rowset ID，并确保它仍未被替换。
5. 不向数据 Rowset 的 Segment 读取器传入 Tablet delete predicates，不改动 TabletMeta predicate 集合。
6. 任务日志和结果区分零行 predicate Rowset 的 `VERIFIED_NO_CHANGE` 与数据 Rowset 的 KEEP/DROP/REWRITE。

### 8. 验证要求

至少覆盖以下确定性测试：

1. coverage 内同时包含多个数据 Rowset 和一个零行 predicate Rowset，Tenant-TTL 能正常执行。
2. TTL 前后 predicate Rowset 的 Rowset ID、version、delete predicate 内容和 TabletMeta predicate 集合保持不变。
3. predicate 作用于 TTL 替换前的早期数据 Rowset；TTL 替换后查询结果不得复活已逻辑删除的行。
4. 目标 tenant 行已被 delete predicate 逻辑隐藏但物理仍存在时，Tenant-TTL 仍删除其物理行，不得误返 `NOOP_VERIFIED`。
5. 零命中 TTL 返回 `NOOP_VERIFIED` 时，predicate Rowset 和 TabletMeta predicate 集合不变，不生成新 Rowset。
6. 人工构造 `has_delete_predicate=true && num_rows>0` Rowset，任务在任何输出构建前返回 `DATA_INVARIANT_VIOLATION`，不改动 TabletMeta 或 Rowset 文件。
7. Tenant-TTL 完成后再运行普通 Compaction，predicate 能被正常物化和清理，最终查询结果正确。
8. predicate Rowset 的 Rowset ID 在提交前变化时，完整 coverage CAS 拒绝提交，所有 staged output 被清理。

## REVIEW-004：用语义常量表示无效 generation

状态：已确认，待实现  
确认日期：2026-09-08  
涉及代码：`tenant_ttl_compaction_types.h`、`Tablet`、`TenantTtlTabletGuard` 及 generation 相关测试

### 1. Review 问题

Tenant-TTL 将 generation 作为 fencing token，防止延迟析构的旧 guard 清理新任务状态。当前多个数据结构和 Tablet 成员直接使用数字 `0` 初始化 generation，回绕判断和测试也直接与 `0` 比较。

`0` 在这里不是普通起始计数，而是被保留为“无效、未取得所有权”的 generation 哨兵值。继续分散使用字面量 `0` 会隐藏这一安全语义。

### 2. 已确认结论

在 `tenant_ttl_compaction_types.h` 的 `starrocks` 命名空间定义编译期常量：

```cpp
inline constexpr uint64_t kInvalidTenantTtlGeneration = 0;
```

常量名使用 `Invalid` 而不是 `Initial`，因为它的核心约束是：

- 未取得 Tenant-TTL admission 的 owner/guard/result 使用该值。
- 任何有效 Tenant-TTL owner 都不能使用该值。
- Tablet generation 计数器初始化为该值，首次成功 admission 先递增再分配。
- `uint64_t` 计数器回绕到该值时必须继续递增并跳过它。

### 3. 待实施修改

使用 `kInvalidTenantTtlGeneration` 统一替换以下 generation 字面量：

1. `TenantTtlOwner::generation` 的默认值。
2. `TenantTtlAdmissionResult::generation` 的默认值。
3. `Tablet::_tenant_ttl_generation` 的初始值。
4. `TenantTtlTabletGuard::_generation` 的初始值。
5. `Tablet::try_begin_tenant_ttl()` 中计数器回绕的判断值。
6. generation 相关单元测试中用于表示“有效 generation”的断言。

这是纯语义化重构，不修改 generation 的分配、匹配、状态转换或延迟 guard fencing 逻辑。

### 4. 验证要求

1. admission 成功返回的 generation 必须不等于 `kInvalidTenantTtlGeneration`。
2. 默认构造的 `TenantTtlOwner`、`TenantTtlAdmissionResult` 和未持有 admission 的 guard 均使用无效 generation。
3. 保留现有 fencing 测试：旧 generation 不得将新 owner 从 `PENDING`/`RUNNING` 改回 `IDLE`。
4. 如果通过测试入口模拟计数器回绕，回绕后第一个分配值必须跳过 `kInvalidTenantTtlGeneration`。

## REVIEW-006：复用业务 tenant ZoneMap 并返回精确 rowid

状态：已确认，待实现

确认日期：2026-09-08

实施轮次：第 12 轮

涉及代码：`TenantTtlRowFilter`、`SegmentReadOptions`、`PredicateTree`、`ZonemapPredicatesRewriter`、`EngineTenantTtlCompactionTask` 及相关 metrics/tests

### 1. Review 问题

当前 `TenantTtlRowFilter` 仅以 tenant 单列 Schema 打开 Segment iterator，`SegmentReadOptions` 没有设置 `pred_tree`、`pred_tree_for_zone_map`、`ranges` 或 `short_key_ranges`。因此底层虽然已有 Segment/Page ZoneMap 能力，但没有可供裁剪的谓词，每个 Segment 都必须读取全部业务 tenant 值。

### 2. 已确认范围

第 12 轮只实现以下能力：

1. 对业务 `tenant` 列构造精确谓词。
2. 将谓词交给现有 Segment iterator，复用已有 Segment/Page ZoneMap。
3. 使用 `ChunkIterator::get_next(chunk, &rowids)` 获得谓词命中行的物理 row ordinal。
4. 按 `DELETE_LIST` / `KEEP_LIST` 极性将 rowid 转为现有 `keep_row_ranges`，后续 KEEP/DROP/REWRITE 和 Writer 语义不变。
5. 增加能区分 coverage 逻辑行数、实际 tenant 读取行数和 ZoneMap 裁剪行数的观测指标。

本轮明确不实现：

- `tenant_id` 生成列识别或校验。
- `CAST(VARCHAR AS BIGINT)` 常量派生。
- Short Key、`SeekRange`、`short_key_ranges` 或排序键候选范围。
- FE→BE 请求字段扩展。
- 仅根据 `tenant_id` 或 ZoneMap 粗粒度元数据直接判定业务 tenant 身份。

### 3. 谓词与 rowid 极性

```text
DELETE_LIST:
    predicate = tenant IN delete_list
    returned rowids = rows to DROP
    keep_row_ranges = complement(returned rowids, [0, num_rows))

KEEP_LIST:
    predicate = tenant IN keep_list OR tenant IS NULL
    returned rowids = rows to KEEP
    keep_row_ranges = returned rowids
```

nullable tenant 在 `KEEP_LIST` 中必须包含 `IS NULL` 分支，保证 NULL tenant 始终保留。`DELETE_LIST` 的 `IN` 谓词不匹配 NULL，因此 NULL 自然位于补集并被保留。

ZoneMap 只用于排除不可能命中的 Segment/Page，不替代精确 PredicateTree 计算。如果 tenant ZoneMap 不存在、被配置关闭或选择性不足，Segment iterator 必须自然退化为精确 tenant 扫描，任务不得因无法利用 ZoneMap 而失败。

### 4. EOF 和完整性语义

设置 `pred_tree_for_zone_map` 后，对一个 `num_rows > 0` 的 Segment，`segment->new_iterator()` 或读取返回 EOF 可能是 Segment 级 ZoneMap 证明候选集为空，不再统一视为数据损坏：

- `DELETE_LIST`：无待删除行，Segment 收敛为 KEEP。
- `KEEP_LIST`：无待保留行，Segment 收敛为 DROP。

对 iterator 真正返回的 rowid，必须校验严格递增、不重复且小于 `segment->num_rows()`。最终仍须校验 `kept_rows + deleted_rows == source_rows`，任何不平衡或非法 rowid 都不得进入 Writer/提交。

### 5. Metrics 口径

不改变现有 `tenant_ttl_compaction_rows_scanned_total` 的历史口径：它继续表示本任务 coverage 中经过逻辑判定的源行数，不改成裁剪后的物理读取数，避免破坏现有看板语义。

新增固定名称、无 tablet/tenant 高基数 label 的累计指标：

- `tenant_ttl_compaction_tenant_rows_read_total`：取自 `OlapReaderStatistics::raw_rows_read`，表示 ZoneMap 之后真正从 tenant 列读取的原始行数。
- `tenant_ttl_compaction_rows_pruned_by_segment_zonemap_total`：取自 `segment_stats_filtered`。
- `tenant_ttl_compaction_rows_pruned_by_page_zonemap_total`：取自 `rows_stats_filtered`。

同步增加同口径 trace counters 和单任务终态日志字段，但不记录 tenant 名单内容。各统计项必须使用底层 `OlapReaderStatistics` 的原始定义；如果它们因执行阶段或谓词过滤而不能无重叠地组成 coverage 行数，则不伪造守恒等式，而是在测试中锁定每个指标的实际口径。

### 6. 待实施修改

1. `TenantTtlRowFilter` 为两种 mode 构造 PredicateTree，并通过 `ZonemapPredicatesRewriter` 产生 `pred_tree_for_zone_map`。
2. 使用带 rowid 的 iterator `get_next()`，将命中 rowid 合并为半开 `SparseRange`。
3. 区分 DELETE 补集与 KEEP 直接集，保持 NULL tenant 语义。
4. 修正带谓词读取下的 EOF 处理。
5. 汇总每个 Segment 的 `OlapReaderStatistics`，记录新 metrics、trace counters 和不含 tenant 内容的终态日志。
6. 不改动 `FilteredRowsetWriter`、VerticalSegmentRewriter、Tablet 提交、FE/HTTP 请求协议或普通 Compaction 路径。

### 7. 验证要求

1. `DELETE_LIST`：Segment ZoneMap 零候选收敛 KEEP，精确全命中收敛 DROP，部分命中收敛 REWRITE。
2. `KEEP_LIST`：Segment ZoneMap 零候选收敛 DROP，精确全命中收敛 KEEP，部分命中收敛 REWRITE。
3. nullable tenant 的 NULL 在 DELETE/KEEP 两种模式下均保留。
4. 构造多 Page Segment，证明 Page ZoneMap 实际减少 `raw_rows_read`，且 rowid/`keep_row_ranges` 与逐行 oracle 一致。
5. 禁用 ZoneMap 或构造 ZoneMap 无选择性数据，证明自然退化的结果不变。
6. 非法、重复、无序或越界 rowid 在构建 Writer 前失败；正常路径行数守恒。
7. 新 metrics 在成功、NOOP 和失败路径按实际已扫描工作累计，不产生高基数 label，旧 metric 口径不变。
8. 现有单/多 Rowset、KEEP/DROP/REWRITE、`NOOP_VERIFIED`、取消、CAS 冲突和 HTTP 回归继续通过。

### 8. 范围边界

本项是可退化的读放大优化，不改变 Tenant-TTL 资格门禁、请求语义、coverage、Rowset ID CAS 或提交原子性。仅凭 ZoneMap 不对业务 tenant 做强身份判定；精确 PredicateTree 始终是最终行级判断。Short Key 和 `tenant_id` 派生另行对齐，不属于第 12 轮。
