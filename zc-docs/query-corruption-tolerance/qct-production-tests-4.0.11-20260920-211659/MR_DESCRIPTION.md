# MR 标题

feat(query-corruption): 支持本地 OLAP 查询损坏容错及 MySQL/HTTP 部分结果提示

## 背景与目标

日志明细检索场景中，业务接受文件损坏时返回不完整、甚至为空的结果，但要求明确提示。现有独立扫描遇到 Corruption 会使查询失败；本 MR 提供受控容错能力，不改变已有查询规划、调度、LIMIT 提前结束和取消流程。

这是“查询容错”而非“文件修复”：不会自动寻找其他副本、重建 tablet、隔离坏副本或增加后台完整性巡检。共享准备等尚未安全接入的路径仍保持严格失败。

## 交付边界

基于本地社区 4.0.11 起点 9559176fab6e2cb885779f1e7b680133d58d6972，汇总至 22a4478221d94b27e24d006a36f3b15bf962aac2 的已确认生产和测试差异。

| 分类 | 文件数 | 新增 | 删除 |
| --- | ---: | ---: | ---: |
| 生产代码、协议、配置 | 31 | 346 | 9 |
| BE 单元测试 | 3 | 405 | 12 |
| FE 单元/计划/mock 测试 | 8 | 731 | 1 |
| FE JDBC/HTTP 协议集成测试 | 2 | 366 | 0 |
| BE 测试构建配置 | 1 | 11 | 0 |
| 合计 | 45 | 1859 | 22 |

本 MR 导入的是一个新的汇总迁移提交。QCT-001～006 的来源及独立 COMMUNITY-FIX 原始提交元数据在 ORIGIN_COMMITS.txt 中保留，未改写源分支历史。QCT-000 为纯文档，无代码进入补丁。独立 COMMUNITY-FIX 只让 Exchange 测试夹具自包含，没有引入无关生产修复。

不包含 Tenant-TTL、文档/AGENTS、独立性能与故障注入工具、驱动 jar 或构建产物。详细路径和 blob 标识见 FILELIST.tsv。

## 一、开关和资格控制（FE）

新增 FE 启动配置 enable_query_corruption_tolerance：

- Config.java 中默认 false。
- 产品 conf/fe.conf 模板设为 true。
- 不提供 SET SESSION 控制，不声明热生效；部署时需修改实际 FE 配置并按维护流程重启。

QueryCorruptionPolicy 在用户查询执行入口判定资格，不为开启容错改写执行计划：

- 仅面向用户的只读 SELECT，实际扫描节点全部是本地 shared-nothing OLAP，走受支持的 Pipeline 和结果输出。
- 视图/CTE 按实际物理扫描与执行计划判断，不以 SQL 表名直接放行。
- 排除内部任务、写入、SELECT INTO OUTFILE、EXPLAIN、无真实本地 OLAP 扫描、湖/外部/混合扫描、Arrow、HTTP raw、短路和非 Pipeline 路径。
- 带远端运行时过滤控制依赖的计划保持严格；不会为了容错关闭过滤器或强制短路回退到普通 Pipeline。

资格结果通过可选查询参数传给 BE。新字段使用可选/默认值形式，但混合版本的端到端语义没有因此获得保证，滚动升级时必须先关闭。

主要文件：Config.java、QueryCorruptionPolicy.java、StmtExecutor.java、InternalService.thrift。

## 二、独立扫描任务容错（BE）

在 OlapChunkSource 的独立 Reader prepare/open 和读取 next 的错误分支识别 Status::Corruption。只有开关生效且错误类型正确时：

1. 将当前独立扫描任务转为 EOF。
2. 丢弃发生错误的整个当前 Chunk，包含该 Chunk 已读入的部分行。
3. 保留此前已经成功交付的 Chunk。
4. 记录查询内诊断标志，其他并行任务照常执行；其他任务若自己遇到同类损坏，再自行停止。

不设置 tablet 级全局停止标志，不增加跨查询持久化状态；共享版本捕获、公共准备、共享拆分等错误不在容错范围。IO、NotFound、取消、超时、内存不足等非 Corruption 不被吞掉，也不靠匹配报错字符串泛化为文件损坏。

全部输入都不可读时同样允许完成并提示 Warning：明细可能零行，聚合可能生成 0/NULL，LEFT JOIN 可能产生右侧为 NULL 的行。结果不仅可能是少行，也可能在聚合/JOIN 语义下“不准确”，因此客户端提示不能只写“缺少部分记录”。

主要文件：olap_chunk_source.cpp/.h。

## 三、诊断传递（BE → FE）

QueryContext 持有查询内粘性原子标志。诊断只从 false 变成 true，不负责停止其他任务。

跨 Fragment/BE 时，标志附在现有 Exchange 的数据/EOS 消息中；检测到损坏后向实际目的端发送的后续消息带标志。接收端在数据/EOS 对下游可见之前合并，根结果的现有最终统计携带诊断交给 FE。

不依赖抽样的 Profile/audit statistics 来保证传递，不新增诊断 RPC、ACK、最终屏障或等待其他 BE 的回收机制。局部 pass-through 与远端通路均有回归。取消/提前完成后不追溯已结束结果，因此不能把 Warning 当成全库完整性证明。

主要文件：query_context.*、sink_buffer.*、data_stream_mgr.cpp、data_stream_recvr.*、query_statistics.*、data.proto、internal_service.proto。

## 四、MySQL/JDBC 协议

- 保持结果列和结果集数量不变。
- 最终结果结束包的 warning_count 与会话 Warning 一致。
- 同连接 SHOW WARNINGS 展示一条 Warning：代码 9000，消息标识 SR_QUERY_PARTIAL_RESULT，含 query_id；不暴露文件绝对路径。
- JDBC 通过 Statement.getWarnings() 获取；需先完整消费结果并在下一条业务 SQL 前读取。
- 文本有界，会话/连接复用时清理；SHOW WARNINGS 重读不消费该诊断，SHOW ERRORS 不把它当错误。
- 若最终查询仍失败，错误优先，不以 Warning 伪装成功。
- FE 转发的返回结构携带 Warning，转发逻辑有 mock 回归。

相关文件：QueryCorruptionWarning.java、ConnectContext.java、QueryState.java、ConnectProcessor.java、MysqlEofPacket.java、ShowExecutor.java、ShowWarningStmt.java、AstBuilder.java、LeaderOpExecutor.java、FrontendService.thrift。

示例：客户端在查询完成后，得到 code=9000、消息含 SR_QUERY_PARTIAL_RESULT 的 SQLWarning。此处不新增客户端业务 SDK，产品接口/页面仍须主动将 Warning 转成自己的显式提示。

## 五、HTTP SQL 协议

标准 SQL NDJSON 在正常结束的末尾 statistics 记录中增加同级字段。示意结构如下（统计对象内容仍为原字段）：

~~~json
{
  "statistics": {},
  "partial_result": true,
  "warnings": [
    {
      "code": "SR_QUERY_PARTIAL_RESULT",
      "message": "SR_QUERY_PARTIAL_RESULT: result may be incomplete or inaccurate ... query_id=..."
    }
  ]
}
~~~

以上为结构示意，不是逐字响应样本。资格通过但健康的请求为 partial_result=false、warnings=[]；开关关闭或资格不通过时不新增字段。raw 模式保持严格，不改变其流格式。

HTTP 200 不能单独代表查询完整完成，调用方仍须读到正常末尾记录并检查流结束；原有错误和断流不被伪装成部分成功。keep-alive 后续请求不得继承上一条诊断。

主要文件：ExecuteSqlAction.java、HttpResultSender.java、JsonSerializer.java。

## 六、测试改动与执行方式

### 自动化资产

FE 单元/计划/mock 覆盖资格判断、Warning 生命周期、EOF/OK、转发与 reset、HTTP 序列化/发送、已有计划不被改写。

BE 单测覆盖独立扫描 Corruption、非 Corruption 严格行为、失败 Chunk 丢弃、统计标志以及 Exchange/pass-through 传播。三个测试源继续纳入标准 starrocks_test；新增可选 query_corruption_test 聚焦目标，EXCLUDE_FROM_ALL，不要求日常完整构建多链接一个测试程序。

两个协议集成测试一并交付：

- QueryCorruptionJdbcTest：MariaDB 与 MySQL Connector/J，共 2 个方法；真实 JDBC socket、text/server-prepared、带参数 prepared、Warning 获取/重读/清理、错误优先及关闭开关。
- QueryCorruptionHttpTest：共 3 个方法，标准末尾标识、健康/关闭、raw 等协议和连接生命周期。
- 使用现有 FE JUnit 5 / Maven Surefire / PseudoCluster 框架，不是另外一套手动测试平台。BE 结果/诊断为模拟，不能替代真实 segment 故障验证。
- Connector/J jar 为测试侧外部输入；不传 -Dqct.mysql.driver.jar 时该方法跳过。测试不把 jar 加入生产依赖或本补丁。
- MariaDB 复用已有项目依赖，没有修改 POM 增加驱动。

完整命令和前置环境见 APPLY.md。两个协议类完整执行应为 5 项、无失败/错误/跳过。只得到 BUILD SUCCESS 或把 1 项 SKIPPED 计为“已测两驱动”，均不满足验收。

### 源分支历史验证（不是本次重新执行）

1. FE 精选 45 项通过，无失败、错误或跳过；FE package 成功。
2. BE 聚焦 14 项随机顺序重复 20 轮通过；这是 14 个不同用例，不是 280 个不同用例。
3. 独立 1 FE / 3 BE 测试集群真实文件故障：page CRC、magic、footer；初始/后段损坏；全部不可读；多 tablet；同 tablet 多任务；失败 Chunk 半填充不泄漏；共享阶段与开关关闭保持严格。
4. 真实客户端：Connector/J 8.4.0、MariaDB Connector/J 3.3.2 的 text/server-prepared，MySQL Warning 与 HTTP 标识、同连接复用。
5. 跨 3 BE 的固定构建侧 LEFT/INNER JOIN，broadcast/shuffle；复用 CTE 实际 MultiCast 只覆盖同 BE，不宣称远端 CTE 全覆盖。
6. LIMIT、取消的有界回归；Query Cache 开启的故障场景验证了能结束，未认证缓存完整性。
7. 生产 FE/BE 基线与功能版使用独立 Release 构建；没有拿 UT 二进制冒充性能基线。

历史物理故障仅注入专用合成数据，不涉及用户 RAID/生产文件；文件备份按 SHA-256 恢复并重新读取，测试节点已停止。原始日志留在专用构建/测试资产中，不随此次代码包交付。独立工具另有 46 项单测，但工具不在本补丁中，不将它们计入本 MR 自动化测试覆盖。

### 本次打包验证（已执行）

- 从精确基线在独立临时仓库 git am 成功，新增 1 个迁移提交。
- 另一个独立 worktree 上 git apply --index 成功。
- 两条路线的完整 tree 都与期望汇总 tree 相等。
- 所选文件与冻结终点一致，范围外文件保持基线。
- 差异路径无 zc-docs/Markdown，git diff --check 通过。
- 包内 SHA-256、归档文件列表及独立解压校验，详见 VERIFICATION.txt。

此次没有重新编译、没有重新执行上述历史测试，没有连接内网验证，也没有提交或推送源分支的新修改。

### 尚未覆盖

完整 BE UT/sanitizer、真正双 FE 转发拓扑、全部表模型/复杂控制依赖组合、生产长期压力与全部硬件平台。真实故障主场景为 Duplicate Key 日志明细，Primary Key 只用于健康短路/计划/性能对照。FE 转发当前只有 mock 覆盖。内网目标分支的冲突、编译、测试及性能须在迁移后另验收。

## 七、性能影响与结论

没有新增额外 RPC、等待其他 BE 最终诊断或改写原有执行流程，但“流程不变”不等于“CPU 成本为零”：

- OFF：有关闭判断及少量状态维护；BE 错误处理新增逻辑主要在原错误分支，不逐行检查数据完整性。
- ON、健康：FE 检查资格；Exchange/结果路径有布尔状态读取、条件判断和统计处理。它们是按计划/消息/结果边界执行，不是按每一行额外扫描。
- ON、损坏：额外记录标志/日志并传递诊断，停止当前任务；可能少读数据，因此故障查询的耗时不能直接拿来证明正常查询无开销。
- 不强制将原短路查询切回 Pipeline；不为容错关闭运行时过滤；不增加提前 LIMIT 的完成屏障。

历史健康数据 A/B/C：A=社区基线，B=新代码 OFF，C=新代码 ON。共享 ARM64 VM、1 FE/3 BE、6 CPU/16 GiB、合成约百万日志，累计 343,512 个有效健康对照样本；这些不是文件损坏时的性能样本。

| 观察 | 结果与限制 |
| --- | --- |
| focused 单客户端、Query Cache OFF | 六类用例 C/A 完成时延中位数增加约 0.058～0.222 ms，约 0.9%～2.5%；不能外推所有请求 |
| MySQL 并发 4、Query Cache OFF | 两轮吞吐 A=275.53/279.52，B=274.91/265.77，C=266.29/259.78 QPS；C/A 约下降 3.4%/7.1% |
| 部分明细分页尾延迟 | 对照组 P99 观察到 76.469 → 87.204 ms 的增加 |
| 判断 | 已观察到健康查询的非零开销和部分回退；共享 VM、有限重复与顺序噪声限制因果归因，不能全归因于某个布尔判断，也不能忽略回退 |

部分 OFF 组也有波动/回退，不能断言只有遇到文件损坏时才受影响。FE JFR 未见明显资格判断热点也不等于“零成本”。本 MR 不虚构百分比验收线，不声明满足生产 SLO；内网合并/上线应以代表性数据、实际并发和硬件重新评估。

## 八、已接受限制与风险

1. 第一阶段 Query Cache 不做完整性修复或绕过。容错产生的部分结果可能污染缓存；后续命中可能没有 Warning，甚至关闭容错后仍受先前缓存内容影响。关闭开关不自动清除历史污染。第二阶段再处理完整性标志和缓存传播。
2. 没有新增文件全扫描。没读到坏 page/列、LIMIT 提前结束，不保证发现损坏；没有 Warning 不代表所有副本健康。
3. 不等待查询完成之后才到达的诊断，不追溯已结束请求；只报告当前支持通路实际传到正常结果结束处的诊断。
4. 允许零行、聚合合成结果、外连接 NULL 扩展，必须明确提示“可能不完整或不准确”，不能保证结果是精确子集。
5. 不受支持的路径保持原严格错误，开关开启不意味着所有查询都保证成功。
6. 只是查询层降级能力，不替代故障告警、存储修复和人工运维。

## 九、发布、回退与内网验收

发布顺序：审查差异 → 生成协议代码并构建 FE/BE → 单测/协议测试 → 独立测试集群验证 → 内网性能评估 → 客户端提示接入 → 按项目维护流程发布。

混合版本期间所有 FE 显式 false，全部节点完成升级后再开启。产品配置模板 true 不会自动更新已有部署；应核查每台 FE 实际配置并重启。严禁直接用 Tenant-TTL 缓存编译本功能分支。

回退：关闭启动开关并按维护流程重启 FE 可停止新的容错执行，但不会恢复数据或撤销历史缓存影响。需要撤回实现时，对实际迁移提交做正常 revert、重新构建并按兼容发布流程部署，不 reset/强推共享分支。

内网验收清单（本包未代执行）：

- [ ] 基线等价且全部 45 个文件经人工审查，内部定制保留。
- [ ] FE/BE 及协议生成成功，相关测试报告无遗漏。
- [ ] 两个协议集成类共 5 项实际执行，Connector/J 未跳过。
- [ ] OFF/ON 健康与受支持 Corruption 的结果/Warning 符合约定。
- [ ] 共享准备、其他错误和不支持路径保持严格。
- [ ] 客户端正确展示 Warning/HTTP 字段，不仅检查请求成功状态。
- [ ] Query Cache 已知风险被业务明确接受或按独立运维决策控制。
- [ ] 代表性负载性能达到内部标准；不以源分支测量替代。
- [ ] 所有 FE 实际配置、版本一致性及回退方案经确认。
