# 内网导入与验收步骤

本交付是一份 git format-patch 格式的大补丁：生产代码、配置、协议定义以及 FE/BE 测试一起导入。两个协议集成测试已经包含。推荐通过 git am 生成一个迁移提交，不要逐个复制文件。

## 1. 先核对适用范围

- 排他基线：9559176fab6e2cb885779f1e7b680133d58d6972（本地 refs/heads/4.0.11）。
- 包含终点：22a4478221d94b27e24d006a36f3b15bf962aac2。
- 文件：45 个，新增 1,859 行、删除 22 行。具体见 FILELIST.tsv。
- 补丁文件：0001-feat-query-corruption-add-local-OLAP-scan-tolerance.patch。
- 不包含 Tenant-TTL、zc-docs、Markdown 文档、独立故障注入/性能工具、驱动 jar 或编译产物。
- 原始各轮提交信息见 ORIGIN_COMMITS.txt；本补丁是新汇总的迁移提交，不保留原来的逐轮提交粒度。原分支历史没有被改写。
- 若内网已导入之前的“仅生产代码”版本，不要再全量应用本补丁；应先做差异分析，另行制作增量补丁。

内网需要有截至基线的等价代码。内网提交 SHA 可以不同，但不能仅凭分支名、提交标题或“也是 4.0.11”就认为完全等价。内网定制会导致冲突，甚至在无冲突时存在语义差异。此次只在外网明确基线验证，尚未在内网分支验证。

## 2. 传输、校验和解压

将压缩包及旁边的 .tar.gz.sha256 文件一起带入内网。不要把解压目录放到待修改的源码目录中，也不要覆盖已有交付包。

在两个文件所在的目录执行（以下为 Linux；macOS 用 shasum -a 256 -c 替代 sha256sum -c）：

~~~bash
sha256sum -c qct-production-tests-4.0.11-20260920-211659.tar.gz.sha256
tar -tzf qct-production-tests-4.0.11-20260920-211659.tar.gz

# 解压到新建目录；不要使用已存在的目录覆盖旧包。
mkdir qct-import-20260920
tar -xzf qct-production-tests-4.0.11-20260920-211659.tar.gz -C qct-import-20260920
cd qct-import-20260920/qct-production-tests-4.0.11-20260920-211659
sha256sum -c SHA256SUMS
~~~

任一校验失败，停止，不应用。SHA-256 用于检测传输损坏；不代替可信传输渠道或签名。

## 3. 准备干净的迁移分支

把以下占位值替换为实际值；不要原样执行含“实际”字样的路径。以下命令应逐段执行并检查退出状态，失败就停止。

~~~bash
QCT_REPO='/实际路径/内网StarRocks仓库'
QCT_PACKAGE='/实际解压路径/qct-production-tests-4.0.11-20260920-211659'
QCT_TARGET='实际内网项目开发分支名'
QCT_IMPORT='codex/query-corruption-tolerance-import-20260920'
QCT_PATCH="$QCT_PACKAGE/0001-feat-query-corruption-add-local-OLAP-scan-tolerance.patch"

cd "$QCT_REPO"
git status
git status --short
git branch --show-current
git log -1 --format=fuller
~~~

工作区和暂存区必须干净，且不能有进行中的 am/rebase/merge。不自动 stash，不 reset，不覆盖现有改动。如该迁移分支已存在，换一个未占用的名字，不复用不明状态的分支。

确认本地目标分支已由你按内网流程更新、代码基线正确后：

~~~bash
git switch "$QCT_TARGET"
git status
QCT_BASE=$(git rev-parse HEAD)
git switch -c "$QCT_IMPORT"
git config user.name
git config user.email
git apply --stat "$QCT_PATCH"
git apply --summary "$QCT_PATCH"
git apply --check "$QCT_PATCH"
~~~

最后一个检查失败时先分析冲突。静态可应用不等于语义正确，也不等于编译通过。不要自动访问外网拉取代码补齐内网仓库。

## 4. 推荐路线：git am

在上面的干净迁移分支执行一次：

~~~bash
git am "$QCT_PATCH"
~~~

这会创建一个提交。补丁作者是汇总提交使用的 Patch Verification <patch-verification@localhost>；原实施提交的作者和正文在 ORIGIN_COMMITS.txt 中原样保留。git am 的 committer 是你的内网 Git 身份。若内网要求迁移提交直接使用内部作者身份，可改用第 6 节的 git apply 路线；两条路线只选一条。

### 发生冲突

~~~bash
git status
git am --show-current-patch=diff
# 对照目标分支、补丁和 FILELIST.tsv，人工解决冲突。
# 仅添加确认过的文件，不要 git add . 混入其他改动。
git add 实际已解决的文件路径一 实际已解决的文件路径二
git am --continue
~~~

不要默认使用 --skip，否则会漏掉这份大补丁。不要使用 --reject 留下一部分已应用的生产修改后继续构建。

如决定放弃本次尚未完成的 am，先保存需要保留的冲突处理笔记，再执行：

~~~bash
git am --abort
~~~

它会撤销当前这批 am 及冲突处理，回到开始应用时的状态。成功导入后已经没有 am 状态，不能用这个命令回滚已完成提交。

需要三方合并时，可以在干净分支、未进行 am 的状态选择如下替代命令，不要在普通 am 已成功后再运行：

~~~bash
git am --3way "$QCT_PATCH"
~~~

补丁包含完整 blob 标识，但内网本地必须有对应原始 blob。缺失时三方合并仍可能失败；--3way 不是自动解决全部冲突，也不会为你完成语义适配。

## 5. 核对提交和差异

仅在 am 成功后执行：

~~~bash
git status
git log --oneline "$QCT_BASE"..HEAD
git rev-list --count "$QCT_BASE"..HEAD
git diff --stat "$QCT_BASE" HEAD
git diff --numstat "$QCT_BASE" HEAD
git diff --name-only "$QCT_BASE" HEAD
git diff --check "$QCT_BASE" HEAD
git show --format=fuller --no-patch HEAD
~~~

未经适配的基线上预期：1 个新提交、45 个文件、+1859/-22，与 FILELIST.tsv 一致，没有 zc-docs 或 .md 文件。可用下式检查被排除的文件仍未变化：

~~~bash
git diff --exit-code "$QCT_BASE" HEAD -- zc-docs ':(icase,glob)**/*.md'
~~~

外网精确基线应用后的完整 Git tree 为：
4f0f87a489250890de13f9df0f61f376039e4535

只有整个内网起点与该基线相同、且未人工适配，才应要求整棵 tree 相等。有内网定制时，应检查新增差异，不要为了 tree 相同覆盖内部代码。FILELIST.tsv 同时提供每个文件的起点/终点 blob 供排查。

## 6. 可选路线：git apply 后自行提交

仅在第 4 节尚未执行，或已明确终止该次导入并回到干净起点时使用。不要与 git am 重复应用。

~~~bash
git apply --check "$QCT_PATCH"
git apply --index "$QCT_PATCH"
git diff --cached --stat
git diff --cached --check
git status
git commit -F "$QCT_PACKAGE/COMMIT_MESSAGE.txt"
~~~

该路线应用同一代码差异，但不保留邮件里的作者/作者日期，使用你当前配置的内部 Git 身份。它也不会重建原始逐轮历史。检查不要混入原有暂存项。

## 7. 构建与 FE 回归

协议定义有变更，必须按内网既有 StarRocks 4.0.11 构建流程重新生成 Thrift/Protobuf 并构建 FE 和 BE，不能只部署 Java 或只复制改动过的源码文件。本包不提供编译好的二进制和驱动包。

复用专用、配置兼容的增量构建目录，不执行 clean、删除 build 目录或清理 Docker volume。不要使用 Tenant-TTL 的构建容器/缓存；编译器、架构、Build Type、sanitizer、关键特性开关不一致时使用另一个独立持久化缓存。改动源码只做增量同步。

在完成内网 FE 构建环境准备、生成代码和依赖准备后，从仓库根目录执行：

~~~bash
mvn -f fe/pom.xml -pl fe-core -am test \
  -Dmaven.clean.skip=true \
  -Dfe_ut_parallel=1 \
  -Dtest='QueryCorruptionPolicyTest,QueryCorruptionWarningTest,QueryStateTest,MysqlEofPacketTest,MysqlOkPacketTest,LeaderOpExecutorMockTest,ConnectProcessorTest#testResetConnection,QueryCorruptionJdbcTest,QueryCorruptionJsonTest,QueryCorruptionHttpSenderTest,QueryCorruptionHttpTest,QueryCorruptionPlanTest' \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dqct.mysql.driver.jar=/实际路径/mysql-connector-j-8.4.0.jar
~~~

历史对应精选 FE 验收为 45 项，无失败/错误/跳过。迁移后必须查看 fe/fe-core/target/surefire-reports 中本次报告，不能仅看到 BUILD SUCCESS 就验收：上述 reactor 选项允许某些模块没有匹配测试，不应掩盖真正的 0 用例执行。

- 需要 Java 17 及仓库既有的 JUnit/JMockit 等测试配置，不能用裸 javac/java 代替 Maven 测试初始化。
- MariaDB Connector/J 使用项目已有依赖；没有新增运行期 JDBC 驱动依赖。
- MySQL Connector/J 用于测试，jar 不在本补丁中。通过内网合规制品渠道准备已验证的 8.4.0，传入绝对路径。不传 qct.mysql.driver.jar 会跳过该用例；路径错误/类缺失则会失败。
- 内网离线环境需事先准备项目已有 Maven 依赖与生成工具；不要从不可信网站临时下载驱动。

### 只执行两个协议集成测试

~~~bash
mvn -f fe/pom.xml -pl fe-core -am test \
  -Dmaven.clean.skip=true \
  -Dfe_ut_parallel=1 \
  -Dtest='QueryCorruptionJdbcTest,QueryCorruptionHttpTest' \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dqct.mysql.driver.jar=/实际路径/mysql-connector-j-8.4.0.jar
~~~

预期是 2 个 JDBC + 3 个 HTTP 测试方法，共 5 项；提供正确 jar 时应没有跳过。不传 jar 时其中 1 项 Connector/J 用例跳过，不能写成“两种驱动都通过”。

这是按仓库现有 FE JUnit 5 / Maven Surefire / PseudoCluster 框架新增的自动化测试。测试启动临时 FE/PseudoCluster，使用随机本地端口、真实 JDBC/HTTP socket，但 BE 返回/诊断是模拟的。不需要启动真实 C++ BE，也不会人为损坏磁盘。它们不是外置手工脚本，不是仓库 SQL regression-test 的 .sql/.result 用例，更不能单独证明真实坏页已经被扫描层正确容忍。

## 8. BE 回归

三个相关测试源仍属于标准 starrocks_test；补丁额外提供 EXCLUDE_FROM_ALL 的 query_corruption_test 聚焦目标，不改变默认全量目标。

首先检查已有 CMakeCache.txt，确认 MAKE_TEST、Release/Debug、sanitizer、编译器、架构与重要特性匹配。以下路径仅示范 Release，不能对一个实际 ASAN/Debug 缓存直接套用。

可以在独立测试 checkout 使用标准入口：

~~~bash
BUILD_TYPE=Release ./run-be-ut.sh -j 2 --module starrocks_test \
  --gtest_filter='OlapScanOperatorTest.*:ExchangePassThroughTest.*:QueryStatisticsTest.*'
~~~

注意：run-be-ut.sh 默认 Build Type 是 ASAN，不是 Release；需要明确 BUILD_TYPE。该脚本会配置构建并清理/重新准备其测试数据与 UDF 测试目录，因此只在专属测试环境运行，不拿生产数据目录或其他任务环境做测试。脚本参数也可能改变 CMake 特性，运行前与既有缓存核对。若 FD 硬上限允许，按测试环境要求提高打开文件数；不要无条件把 ulimit 报错当成测试结果。

如果已有配置完成且参数兼容的专属 Release UT 环境，可以只构建聚焦目标：

~~~bash
cmake --build be/ut_build_Release --target query_corruption_test --parallel 2
~~~

按现有 BE UT 启动环境准备 STARROCKS_HOME、JAVA_HOME、动态库路径、日志和测试数据目录后，再运行：

~~~bash
be/ut_build_Release/test/query_corruption_test \
  --gtest_filter='OlapScanOperatorTest.*:ExchangePassThroughTest.*:QueryStatisticsTest.*' \
  --gtest_shuffle --gtest_repeat=20 --gtest_break_on_failure
~~~

最后的二进制命令依赖已准备的 BE UT 环境，不是任意 shell 下独立可用的完整启动器。历史结果为 14 个不同测试、随机顺序重复 20 轮全部通过；不是 280 个不同用例。内网应另保留本次构建/运行日志，并补做项目要求的回归。

## 9. 配置、部署和客户端验收

- Java Config 默认 false；本补丁的产品 conf/fe.conf 明确设置 true。
- 它是 FE 启动配置，不是 SET SESSION 参数，不是本次新增的热修改开关。修改实际部署使用的 FE 配置后按维护流程重启。
- 混合版本升级期间所有 FE 显式设为 false；FE/BE 全部升级后再评估开启。可选协议字段不等于已经验证了旧新混跑的完整容错语义。
- 安装包模板不会自动修改既有部署配置。检查所有 FE 实际配置来源，避免有的节点开、有的关。
- 开启前客户端必须接入部分结果提示；空结果也可能是被容忍的损坏，不能按“没有行就是没有日志”解释。

MySQL/JDBC：完整消费 ResultSet 后、同 Statement 执行下一条业务 SQL 前，读取 Statement.getWarnings()；检查 errorCode=9000、消息含 SR_QUERY_PARTIAL_RESULT。命令行客户端同连接执行 SHOW WARNINGS。不新增结果列或额外结果集；客户端若忽略 Warning，就可能把不完整结果当成正常结果。

HTTP：只支持标准 SQL NDJSON，读取到正常完成的末尾 statistics 记录；同层 partial_result=true，warnings 包含 code=SR_QUERY_PARTIAL_RESULT 和 message。资格通过且未检测到损坏时为 false/空数组；关闭或不支持时保持旧格式。不能只检查 HTTP 200，必须确认流正常结束；raw 模式不启用容错。

真实坏页验证仅在独立测试集群、合成数据、完整备份且可恢复的文件上按批准流程做；本包没有携带故障注入工具。不要直接修改生产 segment 或 RAID 来“验收”。历史物理测试不等于你们内网部署已通过。

## 10. 已知风险与上线门槛

1. 第一阶段 Query Cache 沿用原实现。部分结果可能进入缓存，后续命中可能没有 Warning；关闭容错也不会自动清理历史缓存污染。本补丁不解决这个风险。
2. 未增加全量文件巡检。未读到坏页/坏列、LIMIT 提前结束或查询结束后才产生的错误，不保证出现在最终 Warning 中；没有 Warning 不代表磁盘完整。
3. 性能评估已观察到健康查询的非零开销和部分吞吐/P99 回退，不能承诺无影响；内网生产 SLO 需另测。详见 MR_DESCRIPTION.md。
4. 公共准备、共享初始化、不支持的计划/协议、非 Corruption 错误仍按原行为失败。
5. 不进行自动副本修复、标坏隔离、跨查询状态保存或数据恢复。

验收至少包括：FE/BE 构建和以上测试、配置 OFF/ON 健康查询、两协议 Warning 生命周期、内网已有定制兼容性、代表性负载性能；记录实际执行项和未执行项。全部通过后再按内网流程人工推送迁移分支并创建 MR，本次外网打包没有替你向任何 remote 推送。

如已提交后需要撤回代码，按项目流程对实际迁移提交执行 git revert 并重新构建/发布；不要使用 reset --hard 或强推。临时关闭启动开关能停止新的容错执行，但不能恢复缺失数据或撤回历史查询/缓存影响。
