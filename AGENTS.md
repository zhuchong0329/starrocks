# Query Corruption Tolerance — Repository Instructions

## 0. 当前阶段与授权边界

本 worktree 只用于本地 OLAP 查询文件损坏容错，不是 Tenant-TTL。

截至 2026-09-20：需求/设计/实施规划 v1.1，QCT-001～005 已独立提交推送，QCT-006 实现、功能验收与性能评估已完成，随本轮独立提交交付。整查询全部不可读也返回 Warning，不实现可用输入保护。性能有非零代价，原始测量和观测回退必须保留；不改变执行流程、不新增等待仍是约束。

**最终状态优先于下文历史过程记录**：没有正在运行的构建、性能或故障测试；本任务 1 FE / 3 BE 均已停止，全部注入文件恢复原始摘要。不要操作历史 PID，也不要因为旧段落写“构建中”而重建或清理缓存。

- FE 45 项与 package、BE 14 项随机重复 20 轮、验证工具最终 46 项通过；真实 MySQL/两种 JDBC/HTTP 文件故障与 343,512 条 A/B/C 对照样本完成。
- 产品 `conf/fe.conf` 已设 true，Java 默认仍 false；滚动升级保持 false，所有 FE/BE 同版本后再启用。
- 生产 FE/BE 产物准确标识 QCT-005 `45cacacf0b02b747234d602dc9fb1d5c405efb51`，原版标识 `9559176fab6e2cb885779f1e7b680133d58d6972`。本轮只加配置/测试/工具/文档，不修改生产 Java/C++。卷内 `src` Git HEAD 仍为 QCT-005，加本轮测试/配置改动；不冒称二进制来自 QCT-006。后续同步最终提交时核验生产差异、必要时增量生成版本/打包，不全量 clean；矩阵工具会拒绝源码 SHA 与产物不一致。
- 真实未覆盖项：全 BE UT、sanitizer、双 FE 转发拓扑、生产长期压力/全部表模型。FE 转发已有 mock 回归。性能存在部分并发吞吐/P99 回退，不宣称零影响或生产 SLO 已认证。
- 末次数据盘可用约 108 GiB，验收无新增 OOM。原始日志、JFR、测试数据、故障备份和编译产物全部保留。

先阅读：

- [需求文档](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/需求文档.md)
- [技术设计与性能影响评估](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/设计文档.md)
- [实施规划](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/实施规划.md)
- [最终验收](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/QCT-006-验收.md)
- [实际性能与限制](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/QCT-006-性能评估.md)

当前已获编码、测试、逐轮提交推送授权；未获发 PR、force push、清理缓存或改动 Tenant-TTL 的授权。

## 1. 准确的 worktree 与基线

| 项目 | 查询容错（本任务） | Tenant-TTL（受保护，不属于本任务） |
| --- | --- | --- |
| 宿主机目录 | `/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance` | `/Users/zhuchong/Documents/code/starrocks-main` |
| 分支 | `codex/query-corruption-tolerance` | 建环境时为 `4.0.11-zc_docs`，以后可能变化，使用前核对 |
| 本功能起点 | 本地 `refs/heads/4.0.11`：`9559176fab6e2cb885779f1e7b680133d58d6972` | 不作为本功能源码基线 |
| 编译容器 | `starrocks-query-corruption-4.0-build` | `starrocks-tenant-ttl-4.0-build`，另有 `starrocks-tenant-ttl-e2e` 使用旧卷 |
| 专属持久卷 | `sr-query-corruption-4.0-build-cache-arm64` | `sr-tenant-ttl-4.0-build-cache-arm64` |
| 卷内根目录 | `/query-corruption-workspace` | `/tenant-ttl-workspace` |

已核实本地 4.0.11 提交等于社区 4.0.11 标签解引用提交与当时的 branch-4.0.11 提交。最初提及的 dev-4.0.11 未查到，用户明确批准本地 4.0.11。不要偷偷换到 main、最新 branch-4.0 或 TTL 分支。

即使工具默认 cwd 仍指向 starrocks-main，处理本任务也必须显式指定本 worktree。worktree 共用 Git 对象库是正常现象，不代表可以共用编译树。不要切换或重置其他 worktree。

## 2. 强制覆盖 Tenant-TTL 的默认编译复用规则

用户对此任务明确要求另起 Docker 环境，不污染 Tenant-TTL 编译缓存。因此，其他目录 AGENTS 中“新编译容器默认挂载 TTL 卷”的要求不适用于本 worktree。

- 本功能任何编译/测试容器不得挂载 `sr-tenant-ttl-4.0-build-cache-arm64`，即使只为节省首次编译时间也不允许。
- 不从 TTL 卷复制 build 目录、target、output、CMakeCache 或已修改源码；不将新产物写入 TTL 运行目录。
- 可以共享已存在镜像的只读基础层，不能共享可写编译缓存。
- 不为本功能启动、清理、替换 TTL 编译容器，不打断 TTL 工作来释放资源。
- 需要 x86_64 等不同架构时另建有明确标签的持久卷，不能覆盖本 arm64 缓存，也不能借用 TTL 卷。

## 3. 独立编译环境与历史过程（最终状态见第 0 节）

以下按时间保留从 2026-09-19 建环境到验收的过程；初建时仅确认工具，最终两套生产构建均已成功。

- 容器：`starrocks-query-corruption-4.0-build`。
- 初始容器 ID：`045e139bc2d1e6c0dc5de6be6875c98776caa7aa7a50e1de54dd76d77ee7636b`。替换容器后应更新记录，不以旧 ID 强制匹配。
- 镜像来源：已有 `starrocks/dev-env-ubuntu:4.0-latest`，arm64。
- 固定 image ID：`sha256:98326df55743ab4f081fcba9fa9cb31b7ff5ee06d69ddd54c11d7fae9cbbaeb2`。标签可变，后续不得只凭标签推断工具链一致。
- 资源上限：6 CPU、16 GiB RAM。工作目录 `/query-corruption-workspace`，主命令 `sleep infinity`，无宿主机发布端口。
- 唯一可写数据挂载：`sr-query-corruption-4.0-build-cache-arm64` → `/query-corruption-workspace`。
- 源码挂载：宿主机本 worktree → `/host-workspace`，只读。
- 卷与容器标签记录 purpose=query-corruption-tolerance、baseline=9559176fab6e2cb885779f1e7b680133d58d6972；卷另标 architecture=arm64。
- 已检查 GCC 12.3.0、Java 17.0.15、Maven 3.6.3、CMake 3.22.1、ccache、lld、镜像内 thirdparty 目录及 LLVM 静态库。没有确认 clang++ 可用，不假设 Clang 构建已准备好。
- QCT-001 已初始化 /query-corruption-workspace/src、独立 Maven/ccache/logs；FE 构建及 10 项资格测试通过，BE ut_build_Release 首次全量构建中。详见 QCT-001-验收.md，不将构建中写成成功。
- 容器新增 ninja-build 1.10.1。FE 增量命令必须包含 -Dmaven.clean.skip=true，避免现有 POM 的 initialize/auto-clean 删除编译资产。
- QCT-002/003 的 BE 实现已联合进入首次构建，分别验收后再独立提交。优先目标为 `query_corruption_test`，复用正常 BE_TEST 库，仅链接本功能涉及的三组测试；原 `starrocks_test` 不删除、不清理。
- 已知 `be/src/exec/join/join_hash_map.cpp` 单个 GCC 进程可占约 10～11 GiB。曾在较高并行度与 FE 构建重叠时发生一次 OOM，保留对象后恢复。常规最多 3 个编译任务；该大文件编译时暂停 Ninja 新任务调度，让其他编译自然结束，待大文件完成后恢复。不要直接以 6 CPU 上限设置 `-j6`。FE Maven 使用 `MAVEN_OPTS=-Xmx2g`，与大型 C++ 编译错峰。
- 当前 UT 构建还遇到 `persistent_index.cpp` 等高内存单元，已启用本次 Ninja 专属的内存守护：单编译器 RSS 超过约 5 GiB 时暂停新任务派发，待在途编译/汇编结束后恢复。日志 `logs/QCT-003/compiler-memory-guard.log`；守护随这次 Ninja 退出而结束，不会自动接管后续构建。在守护活跃时不要另运行独立的 SIGSTOP/SIGCONT 错峰控制器，避免相互提前恢复；后续重新构建先检查进程和实际内存。
- 新增 clang-format-14，只通过 `git clang-format-14 --diff` 检查改动行。不要运行会在缺少 origin/main 时全仓格式化的脚本。
- 测试客户端位于 `/query-corruption-workspace/tools/python` 独立 venv，固定 PyMySQL 1.1.1；不向宿主 Python 或 Tenant-TTL 环境安装依赖。
- 专用 `cache/ccache` 的容量上限已从默认 5 GiB 调整为 20 GiB，避免首次 UT/生产对照构建过早淘汰；未清理任何缓存。配置文件在该专用缓存目录，不影响其他任务。
- 2026-09-19 11:20 UTC：首次 BE UT 构建/链接及最终回归已通过。聚焦 `query_corruption_test` 14 项随机重复 20 轮均通过；独立扫描 5 项、统计/Exchange 9 项。QCT-002/003 已独立提交推送；原 Exchange 夹具缺失准备和 RPC 环境的修复为独立 COMMUNITY-FIX，不改生产代码。
- UT 链接峰值 RSS 约 15 GiB，接近容器 16 GiB 上限，链接期间不要并行 Maven 或另一链接。首次测试因句柄软上限 1024 退出；使用本进程 `ulimit -n 65535`。测试夹具排查中的无限填充另发生一次 OOM；当前累计 OOM 为 2（一次早期编译、一次旧测试循环），修复后 20 轮通过。
- UT Ninja 及其守护已经退出。2026-09-19 11:38 UTC 启动新的生产构建：`src/be/build_Release`、目标 `starrocks_be`、`-j3`；日志 `logs/QCT-006/feature-production-build.log`。该次 CMake PID 48585 / Ninja PID 48590 有专属内存守护，单编译器 RSS 超过约 5 GiB 时只暂停 Ninja 派发，待在途 cc1plus/as 结束后恢复；不要同时手动恢复它。PID 仅是当时记录，后续先核验进程身份；守护不自动接管新构建。
- 容器独立仓库已同步到 QCT-005 `45cacacf0b02b747234d602dc9fb1d5c405efb51`，生产源码与该提交一致；记录 `logs/QCT-006/source-round005-status.log`。新版 FE 已完成带正确版本信息的 package，日志 `fe-round005-package.log`；不使用早期 UNKNOWN 版本产物作交付证明。
- 2026-09-19 13:43 UTC：新版生产 BE 首次构建成功，`src/be/output/lib/starrocks_be` 约 2.2 GiB；实际 `--version` 为 `4.0.11-QCT-round005-45cacacf0b02b747234d602dc9fb1d5c405efb51`、RELEASE、aarch64。动态依赖均找到；日志 `feature-production-build.log` 和 `feature-production-control.log`。上述旧 Ninja 48590/守护已退出，不再操作这些旧 PID。首次生产构建约 125 分钟，无新增 OOM，须保留完整增量资产。
- 2026-09-19 13:45:59 UTC：原版生产构建启动，日志 `baseline-production-build.log` / `baseline-production-control.log`。原版 native 生成版本已修正为 `4.0.11-QCT-baseline` / `9559176fab6e2cb885779f1e7b680133d58d6972`，日志 `baseline-build-version.log`，不是 UNKNOWN。
- **分时测试已结束（14:38:21 UTC）**：13:50:34 暂停原版守护 75538 和 Ninja 76223、在途编译全部结束后，完成新版真实文件故障测试。所有注入文件已逐清单核验恢复原 SHA-256，四个测试节点均已停止；证据 `logs/QCT-006/physical-restoration-build-resume.log`。核验父子关系/命令/工作目录后，已先恢复 Ninja 76223，再恢复守护 75538；CMake 76218。**原版构建当前正在运行，不再是人工暂停状态**，不要再次无条件 SIGCONT 或启动第二个控制器。PID 仅为当时身份记录，后续先核验；原版构建会话 98216 可能随会话压缩不可用，以持久日志和实际进程为准。构建期间不启动测试集群/Maven，性能测量必须等所有编译结束。
- 已启动本次性能接续进程，会话 19513，日志 `logs/QCT-006/performance-build-gate.log`：等待上述原版 CMake 退出、确认原版生产 BE 存在并读取版本后，顺序运行 `process-restart-r1`（A/B/C 每用例一次、无预热，不称 OS 冷缓存），以及 `warm-page-r1`（A/B/C）和 `warm-page-r2`（C/B/A）的 200 次/工作线程矩阵。`tools/query_corruption/matrix.py` 会再次拒绝活动/暂停编译和未恢复文件；只停启本 QCT 四个节点并保留全部数据/日志。**不要同时手动启动另一矩阵、测试节点或新构建**；先检查此日志和实际进程。每个矩阵结束自动停止本测试节点。该记录只是已排队，尚不是执行成功或性能验收结论。
- **以上构建/接续状态已更新**：原版 BE 于 2026-09-19 16:29 UTC 构建成功，实际版本 `4.0.11-QCT-baseline-9559176fab6e2cb885779f1e7b680133d58d6972`、RELEASE、aarch64，约 2.2 GiB。CMake 76218、Ninja 76223 和守护已退出，不再操作这些历史 PID。没有新增 OOM。
- 首次性能接续会话 19513 已失败退出：`process-restart-r1` 完成原版 4 个首轮样本，切 B 时测试启动器在 exec/退出窗口读取 procfs 遇到竞态；不是查询容错错误。工具已增加有界启动身份重试、退出 ESRCH 处理，并补单测（共 44 项，`tool-tests-v19.log`）。未登记的本测试 BE 108837 已核验路径/环境/身份后 SIGTERM 正常停止，没有强杀；全部文件恢复，证据 `orphan-startup-recovery.log`。保留首次不完整性能记录，不作为完整 A/B/C 对照。
- **当前运行（16:35:02 UTC）**：会话 12432，日志 `logs/QCT-006/performance-matrices-v2.log`，依次运行 `process-restart-r2`、`warm-page-r1`、`warm-page-r2`，参数同前。不要并行启动补充故障脚本、其他矩阵、节点或构建；先核验此日志和真实进程。`supplement.py` 已准备并通过工具单测，但必须等性能矩阵结束再运行。
- **上述会话已于 16:53:55 UTC 成功结束**：两轮完整矩阵 132,000 个有效健康样本，计划均一致；结果在 `warm-page-combined-summary.json`。随后 `physical-supplement-r2` 于 16:58:46 UTC 完成跨三 BE 固定构建侧 JOIN、同 BE 复用 CTE、真参数 JDBC、半填充 Chunk、magic+Query Cache、故障并发健康回归。所有文件恢复原摘要、四节点停止。r1 在故障注入前遇 SHOW TABLET 文本编号断言，工具已修正；未改生产实现。
- **当前运行（约 17:00 UTC）**：会话 76420，日志 `logs/QCT-006/performance-extra-v1.log`，顺序执行 `page-cache-off-r1`（A/B/C、MySQL、缓存 OFF、并发 1/4、三个用例各 200 次、预热 50）、`focused-r1`（A/B/C）和 `focused-r2`（C/B/A；两协议、Query Cache 开/关、并发 1/4、小 LIMIT/空结果/明细分页各 500 次、预热 200）。禁止与其他测试/构建并行启动节点。工具已去掉固定 round005 字符串，使用实际源码完整 SHA 验证 BE；最新工具 45 项通过，`tool-tests-v21.log`。
- 已排队串行跟进，会话 34931，日志 `logs/QCT-006/performance-followup-v1.log`：等待上述父进程 131347（start_ticks=4609450）退出，并核对 `focused-r2` 全部 24 子组存在最终 summary，才运行 `page-cache-off-r2`（C/B/A、500 次、预热 200）和 `interference-warm-r1`（C-on、各阶段预热 2,000 次、健康有效样本各 1,000 次，中段并发单线程故障流）。前者用于复核首轮 ON 并发吞吐约 -7% 的差异；后者用于排除首次并发故障观察明显的预热趋势。**这个进程当前仅等待，不与 76420 并行采样；不要另起相同矩阵或测试节点。**两者完成后仍需检查结果、更新文档/产品配置、提交推送 QCT-006。
- **最终收尾**：76420 于 17:24:07 UTC、34931 于 17:30:19 UTC 成功结束；随后独立 FE JFR 诊断也成功结束并停止四节点，没有遗留节点/控制器。最终证据 `final-evidence-audit.log`，46 项工具/配置测试 `tool-tests-final.log`。上面所有“当前/等待/运行中”均为历史时点，不再表示活跃任务。

### 3.1 原版性能对照资产

2026-09-19 已在本任务专属卷内创建 `/query-corruption-workspace/baseline-src`，为容器内独立 Git 仓库的 detached worktree，固定 `9559176fab6e2cb885779f1e7b680133d58d6972`。它不是宿主 Tenant-TTL checkout，也不从 TTL 复制编译产物。

- 原版 A 生产目录：`/query-corruption-workspace/baseline-src/be/build_Release`；原版 FE package 和生产 BE 均已成功。
- 新版 B/OFF、C/ON 生产目录 `/query-corruption-workspace/src/be/build_Release` 已首次构建成功，与 UT 分离。两者使用同一新版二进制，按实际 FE 启动配置分别测量。原版/新版的 1,266 个生产编译单元在规范化源码目录后命令一致；这不是性能通过结论，真实对照测量正在进行。
- A/B/C 生产配置统一 Release、GCC 12、MAKE_TEST=OFF、JIT=ON、STARCACHE=ON、STAROS/TENANN/AVX/SSE/BMI/故障注入=OFF、WITH_COMPRESS=ON、WITH_RELATIVE_SRC_PATH=ON；编译前再次核验 CMakeCache 和 compile_commands，不能用 UT 二进制冒充生产对照。
- 生产构建 ccache 环境为本任务 `cache/ccache`；`CCACHE_BASEDIR` 指向各自源码根、`CCACHE_NOHASHDIR=true`，配合 CMake file-prefix-map 规范化目录。绝不使用 TTL cache。
- 基线配置日志：`logs/QCT-006/baseline-source-setup.log`、`baseline-cmake.log`。版本、源码清单和二进制摘要仍须在实际打包、测量前记录。
- 创建前空间检查：宿主约 505 GiB 可用，VM 数据盘约 170 GiB 可用。本任务 src 约 4.4 GiB，Maven/ccache 合计约 3 GiB；仍需在实际生产构建和数据扩展前复查。

初始化布局：`/query-corruption-workspace/src` 为独立源码副本，`cache/ccache`、`cache/maven`、`logs/QCT-NNN`、`runtime`、`test-data` 都在此新卷下。

宿主 worktree 的 `.git` 文件包含指向主仓库的绝对路径，禁止盲目复制或符号链接到容器。采用本功能分支 Git bundle 等方式初始化独立 Git 元数据；记录编译时源码 SHA/未提交差异，并核验产物版本标识。后续只同步实际改动源码，不全量清空重拷。

## 4. 虚拟机容量与运行恢复记录

- Colima default：aarch64，8 CPU，24 GiB RAM，vz / virtiofs。
- 用户批准磁盘增加 100 GiB，已由 350 GiB 扩至 450 GiB。
- 扩容前数据盘可用约 84 GiB；扩容后 `/var/lib/docker` 文件系统约 443 GiB、已用约 247 GiB、可用约 178 GiB。文件系统显示值与虚拟磁盘标称值不相同。
- 宿主扩容前可用约 466 GiB；卷按实际写入消耗宿主存储，450 GiB 不表示已全部物理占用。
- 旧 TTL 卷检查时约 97.65 GB，没有清理。新卷不设独立容量配额，仍与其他容器共用 VM 数据盘。
- Colima 保留原 starrocks-main 可写挂载，新增本 worktree 只读挂载；Docker 当前上下文已恢复为原 default。

扩容需要重启 VM。原运行的 `sr-tools-r01-ch23`、`sr-exporter-types-review`、`starrocks-tenant-ttl-e2e`、`starrocks-build-codex-20260819-2325` 已恢复运行；原本停止的 TTL 编译容器未启动。

临时 exec 服务不会随容器自动恢复。本次使用原目录/二进制恢复 TTL FE/BE，并恢复其原地址 172.17.0.2；已通过 SELECT 1 和 SHOW FRONTENDS/BACKENDS Alive=true 检查。ClickHouse 使用原 `/tmp/config.xml` 恢复且 SELECT 1 通过。没有重新初始化数据库、导入数据或重跑一次性工具测试。只存在内存中的动态配置不保证保持，不据历史日志擅自恢复可能过时的值。

上述 VM 重启是本次扩容授权，不是未来随时重启/扩容的授权。

每次首次全量构建、新增 UT/sanitizer 构建或扩大集成数据前，检查宿主空间、VM 数据盘、卷占用和可用内存。建议保留至少 30 GiB 操作余量；预测空间不足时暂停与用户对齐，不擅自清理或再次扩容。

## 5. 构建资产保护

- 本功能只允许一个运行容器写同一编译树；换容器先停止旧写入者，挂载同一个专属新卷。
- 生产目录 `/query-corruption-workspace/src/be/build_Release`；UT 使用独立 `be/ut_build_Release`、`be/ut_build_Debug`、`be/ut_build_ASAN`、`be/ut_build_UBSAN`，只按需要创建。
- 重用前读取 CMakeCache 和实际编译参数，核对架构、编译器、构建类型、sanitizer 和特性开关。
- 保留 CMake 生成文件、对象、依赖、二进制、FE target、output、Maven/ccache 和测试日志；禁止常规更新全量 clean、删除式全树同步。
- 执行 FE 脚本前检查隐式 Maven clean，使用经核验的跳过 clean 方式保留增量状态。
- 不删除本功能卷、TTL 卷或任何既有 build/ut_build 目录；不运行 docker volume prune、包含卷的 broad prune、colima delete。
- 如需清理，先报告准确路径、大小、配置、最后有效状态与重建成本，获用户批准后才操作。
- 容器上限不是并行编译推荐值。首次用保守并行度，检查实际内存，不能为提速挤占其他任务。

## 6. 功能边界提醒

- 代码默认 false，验收后产品 fe.conf 设置 true。
- 仅用户只读 SELECT、本地 shared-nothing OLAP、已接入 Pipeline/协议。
- 仅吞独立存储 Reader prepare/open/get_next 明确物理 Corruption；公共准备/共享拆分及其他错误保持严格。
- 失败 Chunk 丢弃，只结束失败任务；不设 tablet 共享停止标记、不修复文件、不切副本。
- 单侧全坏、整查询全坏均允许结果或空结果 + Warning；不实现成功读取证据、逐输入健康矩阵或全坏错误。
- MySQL/JDBC Warning 与标准 HTTP 末尾 partial_result/warnings 都在第一阶段，先 MySQL 后 HTTP。
- 未接入路径原样严格，包括短路/非 Pipeline、HTTP raw、混合外部表；不强制慢路径或禁用优化。
- 不改变计划、调度、EOS、LIMIT 和取消流程；不新增诊断 RPC/同步确认/等待。实际采用的容错通过既有通道标识，不等待无关后台收尾。
- Query Cache 沿当前实现；B 方案第二阶段。明确告知历史污染及后续漏标风险。
- 性能目标尽可能小，编码完成后集中评估，不要求绝对零开销。健康路径不新增读盘/逐行统计/共享热计数；不把静态判断当实测。
- 验收必须记录实际运行项目、失败及未测；不得为了宣称 QCT-006 完成而虚构测试或缩小覆盖不说明。

## 7. 轮次、提交与验证

建议独立提交格式 `<type>(query-corruption): [QCT-NNN] <summary>`。与 Tenant-TTL 的轮次命名分离；每轮一个提交，不混无关更改。

正文必须含非空 `Problem`、`Implementation`、`Compatibility`、`Tests`。如实写已运行/未运行测试和原因，不将工具链存在写成编译通过，不将 mock 单测写成真实坏盘验证。

社区基线问题单独记录、单独对齐和提交，不混入容错功能轮次。用户授权每轮验收后提交并推送 origin/codex/query-corruption-tolerance；不 force push、不自动发 PR。

故障注入只允许针对本功能明确建立的测试数据，不触碰 TTL 卷和用户数据，不执行真实 RAID 故障操作。
