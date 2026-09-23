# 第 035～038 轮：完整轮次与有限重试

需求确认日期：2026-09-23。依据：FE 澄清文档 FE-SCHED-009～013、详细编码计划第 19 节。用户已授权文档完成后直接编码、编译和验收。

## 1. 实现边界

- 一个 FE 调度线程对本轮起点的有限绑定表集合逐表执行；每张表固定自己的 evaluation_time 和策略快照，处理完本轮任务后才等待默认 600 秒。
- 每 Replica 默认最多 30 次尝试（含首次），明确可恢复失败间隔 10 秒；从首次检查起累计最多 3600 秒，超时后本轮不再尝试该任务，继续后续目标。等待按 1 秒检查，配置在每 Replica 开始时固定。
- 保留普通 `ReportHandler` 补发，不改其代码；同一业务任务的补发/重试请求不变。未知结果等待不会重新获得一小时，超时不代表 BE 已停止。
- 不保存跨轮部分成功执行对象；完整成功事实仍唯一保存在现有进度 Manager 的 Map，并通过已有 EditLog/Image 持久化。所有 Catalog Replica 成功后才推进，非 quorum，版本取最小值。
- 四类确定性错误保留轻量 BLOCKED 身份/错误/恢复条件；单纯事务号、时钟或 task ID 更新不解除阻塞。角色变化关闭本地任务、失效旧代际。
- 移除旧超时全局独占及额外逻辑分区 DROP 屏障，但保留 Catalog 写锁内身份复核、BE 状态/锁/提交校验。没有增加 BE 取消或修改 BE 产品协议。

SHOW 的任务计数是 FE 视角：`RunningReplicaTasks` 只统计当前跟踪的执行，超时清理后为 0 不能证明旧 BE 任务已停止；`PendingRewritePartitions` 是当前表已进入本轮派发列表的剩余计划，不是整个集群尚未扫描工作的总量。诊断摘要和轻量 BLOCKED 判据分离，显示截断不解除阻塞。

## 2. 构建环境与命令

复用 `starrocks-tenant-ttl-e2e`（`starrocks/dev-env-ubuntu:4.0-latest`），挂载持久卷 `sr-tenant-ttl-4.0-build-cache-arm64:/tenant-ttl-workspace`；原 `starrocks-tenant-ttl-4.0-build` 处于停止状态，不启动第二个卷写入者。

BE 复用 `be/ut_build_Debug`。实际 CMakeCache/flags 已核对：Debug、MAKE_TEST=ON、gcc 12、ccache、USE_AVX2=ON、WITH_STARCACHE=ON、WITH_TENANN=OFF，`-O0 -g -ggdb -gdwarf-4`。不是 Release，也没有 clean。该缓存早于已有公共配置头更新，因此本次是范围较大的合法增量重编。

FE 测试命令（容器内）：

```bash
export STARROCKS_HOME=/tenant-ttl-workspace
source /tenant-ttl-workspace/env.sh
cd /tenant-ttl-workspace/fe
mvn -pl fe-core -am -Dmaven.clean.skip=true -Dcheckstyle.skip \
  -Dtest='*TenantTtl*Test,ReportHandlerTest,LeaderImplTest,DictionaryMgrTest,AgentTaskTest,AgentTaskQueueSignatureCollisionTest,DynamicPartitionSchedulerTest' \
  -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -T 1 test
mvn -pl fe-core -DskipTests checkstyle:check
```

BE 构建命令：

```bash
cd /tenant-ttl-workspace
cmake --build be/ut_build_Debug --target engine_tenant_ttl_compaction_task_test -j4
```

FE 编译完成后确认 8 CPU/约 23 GiB 内存仍有余量，将 BE 并发调整为 6。先终止原 make，核实遗留进程均为 zombie、不再写入，再使用相同目录续编；保留已完成对象，不 clean。原日志中的 Terminated 是此次主动调整，不是编译错误；续编日志为 `round038-be-build-j6.log`。

```bash
cmake --build be/ut_build_Debug --target engine_tenant_ttl_compaction_task_test -j6
```

仅同步本轮变化的源文件。未删除 target、CMakeCache、对象文件、链接产物或 output。FE 打包/部署时保留旧 jar，不使用会清空 output/fe/lib 的安装步骤。

BE 的 `tablet.cpp`、`tablet_manager.cpp`、`engine_tenant_ttl_compaction_task.cpp`、`config.h` 和新增测试文件，已通过主机/容器 SHA-256 逐项比对一致。测试运行时需设置以下库路径，已用 ldd 确认 JVM/jemalloc 可解析；测试初始化器自己创建临时存储目录，不使用运行集群的数据目录：

```bash
export STARROCKS_HOME=/tenant-ttl-workspace
export UDF_RUNTIME_DIR=/tenant-ttl-workspace/lib/udf-runtime
export LOG_DIR=/tenant-ttl-workspace/log
export STARROCKS_TEST_BINARY_DIR=/tenant-ttl-workspace/be/ut_build_Debug/test
export LD_LIBRARY_PATH=/var/local/thirdparty/installed/jemalloc/lib-shared:/lib/jvm/java-17-openjdk/lib/server:/tenant-ttl-workspace/lib/hadoop/native
```

直接执行测试时也必须设置 `UDF_RUNTIME_DIR`：首次调用遗漏该变量，在配置初始化阶段退出（未执行用例），保留日志 `round038-be-engine.log`；补齐后使用 `-v2` 日志运行成功。没有运行测试包装脚本中的清理步骤。

```bash
cd /tenant-ttl-workspace
timeout 240s be/ut_build_Debug/test/storage/engine_tenant_ttl_compaction_task_test \
  --gtest_color=no --gtest_output=xml:/tenant-ttl-workspace/round038-be-engine-v2.xml
timeout 240s be/ut_build_Debug/test/storage/engine_tenant_ttl_compaction_task_test \
  --gtest_color=no \
  --gtest_filter='*DropBeforeExecution*:*DropAfterOutputStaging*:*DropImmediatelyBeforeCommit*:*CommitHoldingHeaderLock*' \
  --gtest_repeat=20 --gtest_output=xml:/tenant-ttl-workspace/round038-be-races-repeat.xml
```

第二个命令的 XML 仅呈现最后一轮，完整 20 轮以 `round038-be-races-repeat.log` 为准。编译中出现原有 CLucene 源文件的 `-Wmaybe-uninitialized` 警告，没有编译错误；本轮不混入该非 Tenant-TTL 修复。

其余四个目标在相同环境中依次增量编译、运行：

```bash
cmake --build be/ut_build_Debug --target \
  tenant_ttl_compaction_types_test tenant_ttl_row_filter_test \
  tenant_ttl_compaction_fixture_test tenant_ttl_tablet_primitives_test -j6
for task_target in tenant_ttl_compaction_types_test tenant_ttl_row_filter_test \
    tenant_ttl_compaction_fixture_test tenant_ttl_tablet_primitives_test; do
  timeout 240s "be/ut_build_Debug/test/storage/$task_target" \
    --gtest_color=no \
    --gtest_output="xml:/tenant-ttl-workspace/round038-be-$task_target.xml" \
    > "/tenant-ttl-workspace/round038-be-$task_target.log" 2>&1 || break
done
```

## 3. 自动化验证记录

| 阶段 | 实际结果/状态 | 容器持久卷日志 |
| --- | --- | --- |
| 035 预算单测 | 6 项通过 | `round035-fe-budget-test.log` |
| 036 全部 Tenant-TTL | 102 项通过，0 failures/errors/skipped | `round036-fe-full-v2.log` |
| 036 Checkstyle | 0 violations | `round036-checkstyle-v2.log` |
| 037 首次专项回归 | 51 项中 4 failures、1 error；新增多副本测试未维护倒排索引，清理异常污染后续测试，已修正测试构造 | `round037-fe-fault-test.log` |
| 037 扩展全套回归 | 158 项通过，0 failures/errors/skipped | `round037-fe-full.log` |
| 037 Checkstyle | 0 violations | `round037-checkstyle.log` |
| 038 FE Maven package | BUILD SUCCESS，4 分 13 秒；显式跳过 clean | `round038-fe-package.log` |
| BE Debug engine 目标增量构建 | 成功，保留 Debug UT 缓存 | `round038-be-build-j6.log` |
| BE engine 全套 | 26 项通过（含 6 个参数化用例及 4 个新增交错用例） | `round038-be-engine-v2.log` / `.xml` |
| BE 4 个删除/提交交错用例重复运行 | 20 轮全通过，即 80 次执行，不另算 80 个不同用例 | `round038-be-races-repeat.log` / `.xml` |
| BE 其余四目标增量构建 | 全部成功 | `round038-be-related-build.log` |
| BE types / row filter / fixture / tablet primitives | 分别 5 / 10 / 2 / 10 项全部通过 | `round038-be-<目标名>.log` / `.xml` |

本次 BE 共 53 个不同用例通过；另有 80 次交错重复执行。五个测试源文件均已核对主机/容器 SHA-256 一致。FE 生产代码在 158 项回归及 package 之后没有再修改；038 的代码变化仅增强 BE 测试锁探测。

新增覆盖包含次数/累计预算、未知提交、不改变原请求的补发、迟到报告、全 Replica 非 quorum、语义不变的快照切换、策略变化阻止旧进度、BLOCKED 恢复、1001 表扫描、新绑定下一轮、同轮多表推进、角色代际失效、Catalog 锁保护初次进度发布以及 4 个 BE drop/commit 交错场景。

## 4. MySQL 手工复测步骤

仅用于本地验证集群 `127.0.0.1:9030`，root 无密码。所有脚本采用独立库 `tenant_ttl_r038_20260923`，不会 DROP/TRUNCATE 既有库；测试库绑定 TTL 后的行删除/分区删除是测试预期行为。再次完整复测必须换新的库名和全局 Dictionary 名。

工作目录为仓库根目录。保留 `--skip-comments`，避免客户端把单独注释发送为 SQL。

### 4.1 建策略、Dictionary、两张表并绑定

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round038_setup.sql
```

策略为 default=10、short=2、long=20，table_key=`round038.events`，属性默认值=30。两张表均为日粒度自动 List、4 bucket、1 Replica，先各写入 16 行再绑定。脚本记录绑定前分区/Tablet、SHOW CREATE 和三个生效配置。主键策略更新、Dictionary 刷新、FE 快照导出和 TTL 调度彼此异步；短暂 WAITING 不等于失败。

### 4.2 首轮收敛与无事件重访

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round038_verify.sql
```

| 场景 | 数据日期 | 每表初始行数 | 每表预期行数 | 预期剩余 tenant |
| --- | --- | ---: | ---: | --- |
| FE_NOOP | 今天 | 4 | 4 | short、long、other、NULL |
| DELETE_LIST | 5 天前 | 4 | 3 | long、other、NULL |
| KEEP_LIST | 15 天前 | 4 | 2 | long、NULL |
| DROP | 40 天前 | 4 | 0 | 业务分区从 Catalog 消失 |

每表合计 9 行；NULL 保留上限为 max(10,2,20)=20 天。每表两个 Rewrite 分区、每分区四个 Tablet，共 16 个 Replica 任务应在一轮连续推进，而不是各隔 600 秒。结合 BE 日志的 task/tablet/时间和 SHOW 完成进度核验，不只核对最终行数。

自动 List 的空占位分区没有可证明边界，仍显示 fail-closed；不为改善 SHOW 聚合展示而放宽边界规则。再次经过无事件轮次，应保持行数/完整进度，且不新增这些 Tablet 的 Tenant-TTL 执行日志。

### 4.3 迟到写入补偿

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round038_late_write.sql
```

仅 A 的 5 天前分区新增一行已过期 short。下一轮应重验该物理分区全部 Replica，A 回到 9 行，B 无新任务；不能只凭昨天已成功就永久跳过新数据。

### 4.4 策略变更

```bash
mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table \
  < zc-docs/tenant_ttl_round038_policy_update.sql
```

long 从 20 调整到 3 后 REFRESH。旧快照允许继续使用到新快照成功发布；收敛后两表各 6 行：今天 4 行、5 天前只剩 other/NULL 两行、15 天前分区删除。NULL 的最长保留期随新策略变为 10 天。

## 5. 保留集群 FE 升级

2026-09-23 服务端 UTC 15:33 启动新 FE，仅替换 FE jar；未重启 BE，未删除既有库。

- 运行目录 `/tenant-ttl-workspace/tenant-ttl-e2e-runtime-20260911-1457`。
- 旧运行 jar 保留为 `fe/lib/starrocks-fe.jar.pre-038-20260923`；旧 output jar 也保留同样后缀。
- 停机元数据备份 `/tenant-ttl-workspace/round038-fe-meta-pre-upgrade.tar.gz`，约 24 MiB。
- 新 jar SHA-256：`9e04f98b297a252adba615d353bd46a4ad001b83849318302c84397a5ff20969`。
- 旧 jar SHA-256：`d5f55fedca7bd8f568e9089ac87f47aeec2b9a546e6347d87e12a4a9d22ade43`。
- BE PID 903 未变，二进制 SHA-256：`d245c04c4a0b21c599c59a1ec1de2c661f3d09995e170a3d0a626b2512588380`。
- stop 脚本因旧 Java 的 zombie 进程仍响应 kill -0 而打印 45 秒超时；备份前通过 ps 确认原 FE 已是 Z、不是仍在写元数据的运行进程。新 FE Alive=true、Role=LEADER。
- 现场 `ADMIN SHOW FRONTEND CONFIG` 确认新默认 `tenant_ttl_scheduler_interval_seconds=600`、`tenant_ttl_agent_task_max_attempts=30`。升级前周期为 60 秒，未通过 fe.conf 设置临时值。

## 6. 当前交付状态与未完成验收

035～037 实现和自动化回归完成；038 的 FE 打包、BE Debug 增量构建、53 项存储回归及关键交错重复验证完成，已保留 SQL 复测脚本，但**不宣称真实 SQL 闭环验收已经完成**。

SQL setup 命令连续两次因自动权限审核超时而未执行，已向用户请求确认；没有生成 setup 输出文件。本次独立库尚未通过这些脚本创建，表内行数、任务时间、无事件重访、迟到写入、策略切换以及 FE 重启后的真实进度恢复，均没有本轮 SQL 证据。第 4 节全部是可复测动作与预期，不是执行结果。

真实多 FE 切主、多 BE 副本故障、混合版本、BE/CN 重启 Dictionary Cache 补齐、取消和 DELETE_LIST 超限分片不在本次本地单节点集群验收范围内。存储竞态测试验证既有 BE 状态和锁边界；没有据此声称 BE 已实现取消或任何新增协议。
