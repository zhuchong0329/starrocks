# Query Corruption Tolerance — Repository Instructions

## 0. 当前阶段与授权边界

本 worktree 只用于本地 OLAP 查询文件损坏容错，不是 Tenant-TTL。

截至 2026-09-19：需求/设计/实施规划 v1.0 已对齐，用户已授权连续实施至 QCT-006，每轮完成必要确认测试、单元测试与验收后独立提交并推送 origin。整查询全部不可读也返回 Warning，不实现可用输入保护。性能改为尽量小的影响，集中评估在编码完成后；保留现有执行流程、不新增等待仍是约束。

先阅读：

- [需求文档](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/需求文档.md)
- [技术设计与性能影响评估](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/设计文档.md)
- [实施规划](/Users/zhuchong/Documents/code/starrocks-query-corruption-tolerance/zc-docs/query-corruption-tolerance/实施规划.md)

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

## 3. 已创建的独立编译环境

记录时间：2026-09-19。状态是“容器已建立、基础工具已检查”，不是“已编译成功”。

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
