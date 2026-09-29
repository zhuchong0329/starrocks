# Query Corruption Tolerance — Repository Instructions

## 0. 当前阶段与授权边界

本 worktree 只用于本地 OLAP 查询文件损坏容错，不是 Tenant-TTL。

**2026-09-22 / 客户端与 CPU 隔离实验最终状态（覆盖下列所有“进行中”记录）**：会话 87960、59640 均 exit 0；共 30 组 / 300000 正式查询，另 219910 预热。原共享环境四对 P/T 吞吐 +74.05%、P99 -49.66%；独立 CPU 两对仍 +32.53%/-29.01%，确认原单进程客户端限制。随后六块 ABC 的 A/B/C QPS=150.698/149.728/150.596，完成 P99=43.680/44.696/45.697 ms；ON/A 吞吐 -0.068%、P99 +4.62%，探索性区间宽且含 0，不能证明零成本/尾延迟等效。四进程客户端合计约 0.63～0.67 核，2 核预算、测量无限流；仍是共享 VM，不是独占物理硬件。报告 `zc-docs/query-corruption-tolerance/QCT-007-客户端瓶颈与隔离复验.md`，轻量证据 `qct007-client-performance-20260922/`；原始记录留在两个专属卷。5 项新编排 + 46 项工具 + 4 项统计/顺序单测通过，逐请求审计通过；没有生产编译/源码更改/提交推送或补丁更新。

**当前准确环境**：FE/三个 BE 全部停止，`starrocks-query-corruption-perf-client` 已停止；QCT 构建容器本体仍运行，ID/启动时间/挂载/6 CPU 配额/16 GiB 均保持原样。空参数 `docker update --cpuset-cpus ""` 未生效，收尾审计发现临时 `0-5` 残留；随后明确更新为 **`0-7`**，已实测有效 CPU 范围 `0-7`、`cpu.max=600000 100000`。这是恢复全部当前 8 个 VM CPU 的执行范围，但 HostConfig.CpusetCpus 从原空串变为显式 `0-7`；全 HostConfig 对比仅此差异。将来若增加 VM CPU 数，需同步调整掩码；不要冒称配置字节级完全复原，也不需为清空字段重建容器。失败恢复记录和最终 `environment-audit.json` 均保留。专属卷约 104 GiB 可用、服务端 OOM 仍为历史 2，客户端无 OOM；全部 segment/故障备份均未变化。新只读客户端容器/卷保留作证据，不用于编译；Tenant-TTL 完全未操作。所有测试已结束，不要按下面历史 PID/脚本启动重复任务。

**2026-09-22 / CPU 隔离复验进行中（最新）**：首阶段会话 87960 已 exit 0，07:49:34～08:14:15 UTC 完成 8 组/80000 正式样本；四对 P/T 吞吐几何平均 +74.05%、P99 -49.66%、客户端 CPU/查询 -60.54%。全部数据摘要未变、无新增 OOM，首阶段节点已停。只读审计 `logs/QCT-007/client-architecture-20260922-r1/audit-summary.json`。

第二阶段会话 **59640** 已启动，宿主控制器 `/private/tmp/qct-client-perf.YpMWTT/split_suite.py`，主日志同目录 `split-suite-20260922-r1.log`，服务端脚本 `logs/QCT-007/split-suite-20260922.py`。QCT 服务端临时 CPU 集合 `0-5`（原值为空，结束必须恢复），仍 6 CPU/16 GiB；新增纯客户端容器 `starrocks-query-corruption-perf-client`，2 CPU/2 GiB、CPU 集合 `6-7`，共享 QCT PID/network namespace 以保持原地址并读进程计数。客户端对 QCT 卷只读挂载；新持久卷 `sr-query-corruption-client-perf-arm64` 挂 `/qct-client-workspace`，只存脚本/结果，不编译、不挂 Tenant-TTL。先 CPU 隔离 TP/PT 两对，再多进程 ABC/ACB/BAC/BCA/CAB/CBA，共 22 组/220000 正式样本。服务端清单/设计/最终状态在 `logs/QCT-007/split-client-20260922-r1/`；客户端逐条结果在其新卷 `results/split-client-20260922-r1/`。不要启动重复矩阵或并行构建。控制器 finally 停客户端和四节点并恢复 QCT CPU 集合；完成后须核验，不能只相信代码。其他容器、生产代码、编译缓存未改，不提交推送。进展报告 `zc-docs/query-corruption-tolerance/QCT-007-客户端瓶颈与隔离复验.md` 尚为进行中。

**2026-09-22 / 客户端瓶颈对照进行中（优先于以下历史状态）**：用户批准先排查客户端。会话 87960，专属卷脚本 `logs/QCT-007/client-perf-20260922.py`，主日志 `client-perf-20260922-r1.log`，证据 `client-architecture-20260922-r1/`。仅社区基线，单进程四线程 T 与四进程各一线程 P，固定总并发 4、同一 detail_page SQL/完整消费 1000 行、DOP=2、Query Cache OFF、页缓存 ON；4 对按 TP/PT/PT/TP 共 8 组，每组全新 FE/3BE/client、60 秒预热、10000 正式样本。新增 5 项编排自测和原工具 46 项通过。开始前专属卷约 106 GiB 可用、OOM 历史 2、无活跃 QCT 构建/节点。尚未完成或判定瓶颈；不启动重复矩阵、不并行构建、不操作 Tenant-TTL。生产代码与缓存未改，不提交推送。客户端仍与服务端共享 6 CPU 容器配额，暂不宣称独立资源隔离。所有工作进程 CPU 与采样前后服务端资源均记录，写样本在测量结束后完成。

**2026-09-21 / 单场景独立重启复验最终状态（覆盖下一段“进行中”历史记录）**：会话 80240 已 exit 0，12:44:12～13:55:58 UTC 完成 22 组、220,000 正式样本及 128,350 预热。88 个不同 FE/BE 进程身份、22 个客户端 PID，21 个 segment SHA、所有计划和 tablet 指纹均一致。四节点和编排均已停止，OOM 仍为历史 2，卷约 106 GiB 可用。六块配对几何平均 ON/A 吞吐 -1.49%、P99 +0.24%、P50 +3.29%，探索性区间包含 0；未复现此前约 21% 的 P99 回退，但不能证明零开销或全部归因顺序，客户端近一核仍限制归因。报告 `zc-docs/query-corruption-tolerance/QCT-007-单场景重启性能复验.md`；本地轻量证据 `qct007-isolated-performance-20260921/`；121 MiB 原始样本在卷内 `logs/QCT-007/isolated-detail-20260921-r1/`。生产源码、编译缓存和 Tenant-TTL 未改，无提交推送。不要操作本轮历史 PID 或启动重复矩阵。

**2026-09-21 / 单场景独立重启复验进行中（优先于下文已结束记录）**：用户批准排查顺序影响。运行会话 80240；专属容器中脚本 `logs/QCT-007/isolated-perf-20260921.py`、主日志 `isolated-perf-20260921.log`、证据目录 `isolated-detail-20260921-r1/`。每组全新 FE/3BE/客户端进程；MySQL detail_page、并发 4、Query Cache OFF、BE 页缓存 ON、DOP=2；固定预热 60 秒、每组 10,000 有效样本。22 组为前后各两次 A/A 和 ABC/ACB/BAC/BCA/CAB/CBA 六个完整对照块。不启动重复矩阵、不并行构建、不操作其他容器。A=9559176、B/C=46ca14950；保留生产源码、所有构建和旧证据。4 项新编排自测和原有 46 项工具测试通过；开始前卷可用约 107 GiB，无活跃 QCT 节点/构建，历史 OOM 仍为 2。每轮核对物理 segment SHA、tablet 放置/版本、执行计划和全新进程身份，任何变化停止并保留证据。本段仅记录已启动，不表示验收完成。

**2026-09-21 / QCT-007 性能复验授权（优先于下面的暂缓记录）**：用户现已明确要求重跑之前的性能测试。对照 A=社区 9559176、B=46ca14950 OFF、C=同提交 ON；复用本任务专属容器、持久卷和数据，不修改业务代码，不动 Tenant-TTL。空间约 108 GiB，故障备份全部恢复。容器 src 已保留工作文件、同步 Git HEAD/index 至 46ca14950，生产及测试工具与提交一致；仅旧文档同步差异保留。FE/BE 增量编译及 FE 41 项、工具 46 项回归通过（09:48 UTC），两者版本均为实际 46ca14950，不再使用未提交包。BE 增量构建 24 步，非全量 clean。

- **最终状态：全部进程已结束，不再运行测试**。主矩阵会话 89124 于 10:34 UTC 成功结束；补充会话 65548 的页缓存复测成功，但首次故障并发在预热中遇健康连接包序号错误，已自动恢复文件并停节点；原样复跑会话 50848 于约 10:47 UTC 成功，最终审计通过。不要操作上述历史 PID/会话或从下文旧“运行中”记录启动重复任务。361,512 条有效健康对照、123 子组、22 类计划及 1,266 个编译命令一致性核验完成；无新增 OOM，四个节点全部停止，所有故障文件恢复，约 107 GiB 可用。
- 正常查询仍有局部吞吐/P99 回退：完整 MySQL 混合负载并发 4/缓存 OFF 的 C/A 吞吐 -1.15%/-3.39%；重点分页合并 P99 A/B/C=75.024/83.653/90.921 ms。页缓存关闭首轮约 -15% 未在三次后续复测中重复同幅度，不能删除异常。故障复跑 3,000 健康样本通过，中段 909 故障请求；首次协议错误前有 BE 重启后的存活检测取消/重试证据，根因尚未闭环，不能写成所有功能一次通过。
- 新报告 `zc-docs/query-corruption-tolerance/QCT-007-性能评估.md`；宿主证据目录 `qct007-performance-20260921/`。原始矩阵仍在专属卷 `logs/QCT-006/qct007-20260921-*`（工具固定输出根），脚本/主日志/审计在 `logs/QCT-007/perf-*`。保留旧数据、所有缓存、编译资产及失败证据。不修改生产代码、不推送、不更新已有 patch 或 production-review；Tenant-TTL 环境不变。

**2026-09-21 / QCT-007 最新状态（优先于下文 QCT-006 历史记录）**：用户确认删除 EXPLAIN/Pipeline/扫描源/远端 Runtime Filter 资格检查，保留 OUTFILE、DefaultCoordinator instanceof 及其他既定边界；允许混合源局部容错、已容忍损坏的诊断也可丢失。QueryCorruptionWarning.beginStatement 增加 null 快速返回，SHOW 行为不变。需求/设计/规划已更新 v1.2。

- 本轮仅新增一个独立提交，FE 增量编译/package 与 41 项功能回归通过，0 失败/错误/跳过；详见 `zc-docs/query-corruption-tolerance/QCT-007-验收.md`。
- 用户明确要求性能验收等待后续命令：不要自动启动性能矩阵/JFR，不复用 QCT-006 数据声称新版性能通过。无 BE 生产修改，本轮未重编 BE、未重新注入物理故障。
- 仅同步 6 个 FE 生产/测试源文件到已有专属卷，复用缓存；FE target jar 已被本轮增量编译更新，不能继续冒称 QCT-005 旧对照产物。卷内 src Git 元数据仍为 QCT-005，含本轮未提交 FE 源码；本轮 jar 仅作编译测试产物，不冒称已经按新提交正式发版。未启动或修改物理测试集群配置/数据，BE 与 baseline-src 原版产物不变；测试启动器直接读取 src 的 jar，未来运行前必须重新核对版本与源码清单。
- 日志 `logs/QCT-007/fe-tests-package-r1.log`、`fe-tests-package-r2.log`、`final-artifact-audit.log`；两次同组 41 项均通过。首包版本 UNKNOWN，复验包明确标识 `4.0.11-QCT-007-worktree` / `22a4478221d94b27e24d006a36f3b15bf962aac2-QCT007-uncommitted`，不是正式提交包。空间约 108 GiB，无遗留活跃测试 Java/BE/Ninja。本轮不推送、不发 PR，不更新历史补丁包或 production-review worktree；后续按新命令处理。

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
- 仅用户 SELECT 入口请求启用，OUTFILE/写入/内部任务排除；实际错误转换仍只在已接入本地 shared-nothing OLAP 独立 Reader。
- 仅吞独立存储 Reader prepare/open/get_next 明确物理 Corruption；公共准备/共享拆分及其他错误保持严格。
- 失败 Chunk 丢弃，只结束失败任务；不设 tablet 共享停止标记、不修复文件、不切副本。
- 单侧全坏、整查询全坏均允许结果或空结果；FE 收到诊断才附 Warning，已容忍损坏也可无标识。不实现成功读取证据、逐输入健康矩阵或全坏错误。
- MySQL/JDBC Warning 与标准 HTTP 末尾 partial_result/warnings 都在第一阶段，先 MySQL 后 HTTP。
- 短路/Arrow/HTTP raw 保持排除；不再预检 Pipeline/扫描源/远端过滤，混合源允许局部容错，未接入 Reader 保持原错误。EXPLAIN ANALYZE 可容错；不强制慢路径或禁用优化。
- 不改变计划、调度、EOS、LIMIT 和取消流程；不新增诊断 RPC/同步确认/等待。实际采用的容错通过既有通道标识，不等待无关后台收尾。
- Query Cache 沿当前实现；B 方案第二阶段。明确告知历史污染及后续漏标风险。
- 性能目标尽可能小，编码完成后集中评估，不要求绝对零开销。健康路径不新增读盘/逐行统计/共享热计数；不把静态判断当实测。
- 验收必须记录实际运行项目、失败及未测；不得为了宣称 QCT-006 完成而虚构测试或缩小覆盖不说明。

## 7. 轮次、提交与验证

建议独立提交格式 `<type>(query-corruption): [QCT-NNN] <summary>`。与 Tenant-TTL 的轮次命名分离；每轮一个提交，不混无关更改。

正文必须含非空 `Problem`、`Implementation`、`Compatibility`、`Tests`。如实写已运行/未运行测试和原因，不将工具链存在写成编译通过，不将 mock 单测写成真实坏盘验证。

社区基线问题单独记录、单独对齐和提交，不混入容错功能轮次。用户授权每轮验收后提交并推送 origin/codex/query-corruption-tolerance；不 force push、不自动发 PR。

故障注入只允许针对本功能明确建立的测试数据，不触碰 TTL 卷和用户数据，不执行真实 RAID 故障操作。
