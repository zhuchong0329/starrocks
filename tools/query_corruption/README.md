# QCT 独立验证工具

这些工具只用于本任务专门建立的测试集群，不是生产修复工具，也不操作 RAID。
运行前阅读仓库 `AGENTS.md` 的环境隔离与缓存保护规则。

## 可重复性能矩阵

`matrix.py --series <新名称> --variants A-baseline B-off C-on` 会按顺序切换专用测试节点，默认分别测试 MySQL 的八类查询和 HTTP 的小 LIMIT/空结果/明细分页，Query Cache 开/关、并发 1/4。它不会创建数据；先完成 fixtures 和真实故障验收。原版/新版生产构建必须全部完成，所有注入文件必须恢复原摘要。存在活动或暂停的编译器/Ninja/Maven 时拒绝开始，避免干扰性能与超内存。

每次切换保留配置历史，记录实际 BE 版本、源码 SHA、BE/FE/依赖库/CMake/配置摘要和节点身份。每组保存原始 JSONL、计划、资源快照、执行参数和日志；已有 series 或测量文件不覆盖。运行结束只停止本测试集群，不删数据。用不同 series 反向排列 variants 做复测；`--page-cache` 是存储页缓存，`--query-cache` 是另一层缓存。`--cases`、`--concurrency`、`--repetitions`、`--warmup` 可明确控制范围；默认每个工作线程每个用例 200 个有效样本。工具拒绝有 Warning 或结果行数不符的健康性能样本。

原版/新版实际二进制必须对应卷内源码提交；源码提交变化后需先核验或重建产物，不用修改 variant 标签冒充新版本。

点查用例显式在 SQL hint 中设置 `enable_short_circuit=true`（4.0.11 的会话初值为 false），确保衡量原有短路路径而非普通 Pipeline 点查；A/B/C 使用同一 SQL。其余参数与实际 EXPLAIN 及会话设置保存在原始记录中。

性能模板对三种版本统一设置 `tablet_sched_disable_balance=true`，避免后台均衡改变 tablet 放置；每次启动后的实际 tablet/backend/version 映射写入构建证据。这是专用测试配置，不修改产品调度配置。

`summarize.py <series目录> [另一个series目录]` 只读汇总已完成 JSONL，核对样本数量、健康行数/Warning、存储摘要与原始样本重算结果，并输出逐轮吞吐、CPU/RSS/缓存计数差、相同 SQL 的计划摘要以及跨轮合并时延。缺失指标保持 null，不当作零；不输出预设通过阈值。原始资源快照另保留容器 CPU quota/throttling 与内存/OOM 计数。重复轮次须保留各自统计；合并样本不能代替独立复测，少量首轮样本也不能用于稳定的 P99 判断。

## 专用运行节点

`runtime.py prepare B-off` 只接受本任务卷内的生产 Release BE 和已打包 FE，首次建立有标记的 `qct-cluster`；切换 A/B/C 必须先停止所有测试节点，旧配置、启动记录和全部数据保留。原版 A 来自 `baseline-src`，B/C 来自 `src`；不要在节点运行时重新构建其二进制。

依次用 `runtime.py start fe`、`start be0`、`start be1`、`start be2` 启动，`status <节点>` 检查进程身份，`stop <节点>` 仅发送 SIGTERM 并等待最多 30 秒，不强杀、不清理。进程启动成功不代表服务已就绪，还须检查日志、SQL 连通性和三台 BE 的 Alive 状态。工具不会注册 BE，也不会自动执行 fixtures.sql。所有地址限于容器内，MySQL 19303、HTTP 19300，BE heartbeat 19402/19502/19602。

启动器只为自身及子进程提高文件句柄软上限（目标 65535，不超过已有硬上限）；硬上限不足 60000 时拒绝启动。不会修改 Docker/VM 的全局配置或其他进程。BE UT 也须在自己的测试进程使用至少 60000 个句柄，默认 1024 会在测试开始前被 StorageEngine 拒绝。

这个简化运行环境仅用于本地 OLAP 验证，不包含外部连接器/UDF 的完整发行包。A/B/C 使用相同启动参数。每轮取证还需记录实际二进制/配置摘要和 CMake 参数；variant 标签本身不是版本证明。

故障测试默认关闭存储页缓存。性能热页测试可在所有测试节点停止后使用 `runtime.py prepare B-off --page-cache`（A/C 同理），三个测试 BE 均开启页缓存，`selection.json` 显式记录；与 benchmark 的 `--cache`（Query Cache）是两种不同设置。重新启动只清除本任务进程内缓存，不代表清除了操作系统缓存。禁止为本测试执行全 VM 的 drop_caches 或干扰 Tenant-TTL 服务。

## 文件故障

`fault_file.py` 只允许操作 `/query-corruption-workspace/runtime/qct-cluster` 下的非符号链接 segment，要求目录含 `.qct-test-cluster` 标记且显式确认 tablet ID。单文件限制 64 MiB。完整备份、SHA-256 与恢复清单保存在专属 `test-data/qct-backups`，不自动删除证据。

```bash
python3 tools/query_corruption/fault_file.py inject \
  /query-corruption-workspace/runtime/qct-cluster/be0/storage/data/1/123/456/rowset_0.dat \
  --confirm-test-tablet 123 --kind page
python3 tools/query_corruption/fault_file.py restore \
  /query-corruption-workspace/test-data/qct-backups/实际返回的清单.json
```

示例路径仅说明格式，必须从测试集群实际 tablet/segment 元数据确认，不能照抄或指向用户数据。

- `page`：翻转 footer 之前指定偏移（默认 0）的一个字节；实际错误类型须由 BE 验证，工具本身不证明触发 checksum。
- `magic`：修改末尾 `D0R1` 的一个字节。
- `footer`：把 footer 的第一个 protobuf tag 改为非法的 0。
- 关闭测试 compaction；magic/footer 注入和恢复前停止相应测试 BE，重启使 segment 元数据缓存失效。page 测试关闭页缓存。
- 恢复先校验备份和目标 inode/内容，拒绝覆盖已被替换或额外修改的文件；不得通过绕过检查强行恢复。

需要读中故障时，先用生产 BE 的 `meta_tool --operation=show_segment_footer --file=实际测试segment` 保存健康 footer JSON，再用 `segment_pages.py 实际测试segment --confirm-test-tablet 实际ID --footer-json 实际JSON` 只读列出 ordinal index 的数据页偏移。工具仅支持本基线标量列的未压缩 ordinal index；不是全格式解析器或 CRC 健康检查。将选定的后续页偏移显式传给 `fault_file.py --page-offset`，记录其 first_ordinal，并以实际查询和 BE checksum 日志确认命中，不能靠猜测文件中点宣称覆盖读中损坏。

## 性能

`probe.py mysql|http healthy|partial|strict SELECT_SQL` 对真实测试集群执行只读断言，记录计划（MySQL）、结束包 Warning / SHOW WARNINGS、HTTP 完整结束与末尾字段、行数及有界样本。`--reuse-healthy` 验证同连接诊断清理；HTTP 同时核对 connectionId。`--split` 显式选择测试用强制共享拆分，`--cache` 打开测试会话 Query Cache，`--profile` 保留用于核对 MorselsCount、节点分布的详细 Profile（后续按实际 query ID 从专用 FE `/api/profile` 获取）。它不注入故障、不创建数据、不修改 FE 配置；期望值必须结合真实文件故障和实际计划选择，不能把一次无 Warning 当作磁盘完整性证明。输出应保存到新的 QCT-006 日志。功能 Profile 设置不用于性能计时。

`JdbcProbe.java` 是额外的真实 JDBC 客户端，固定连接有标记集群的 19303 端口，不模拟 BE；参数为 `mysql|mariadb text|prepared healthy|partial|strict SELECT_SQL`。只允许单条 SELECT，prepared 可带一个绑定为 0 的占位符。它显式关闭本测试的短路、共享拆分、远端过滤和 Query Cache，以验证已接入路径；不能把这些测试设置当作产品功能改变计划。编译/运行时用测试专属的驱动 jar，不加入产品依赖。实际运行输出应保存在 QCT-006 日志，Java 编译通过本身不等于真实 JDBC 故障测试通过。

`supplement.py --series <新名称> --pages-json <已保存的页定位JSON> --confirm-test-tablet <实际ID>` 补充固定构建侧 broadcast/shuffle JOIN、复用 CTE、真实 JDBC 参数绑定、Chunk 内第二个数据页损坏、未读列不触发额外校验，以及初始化损坏时的 Query Cache 完成性。它核验当前 segment 摘要与页快照相同，并再次核对 `qct_faults.one_tablet` 的实际 tablet ID。故障前后停启对应测试 BE，`finally` 恢复原文件并保留备份；结束停止四个测试节点。**只能在性能矩阵结束后单独运行**，不能与编译或其他集群测试并行。

该补充脚本还记录“健康请求单独运行 / 并发故障请求 / 再次单独运行”的各 1,000 个健康延迟样本；故障流单工作线程且请求间隔至少 10 ms。它衡量有限故障流对同集群的实际干扰，包含额外查询本身的资源开销，不能把差值全部归因于诊断字段，也不能代替生产长稳压测。

`benchmark.py` 默认使用独立安装的 PyMySQL 1.1.1，固定访问同容器 `127.0.0.1:19303` 的有标记测试集群，只执行 SELECT/EXPLAIN、SHOW VARIABLES 和会话参数设置，不创建或修改表。先准备 `qct_perf.events(k,message)`、主键表 `points(k,message)`、小表 `dimension(k)`。

`--protocol http` 改用同一测试 FE 的 19300 标准 SQL 入口，复用 HTTP keep-alive 连接，按 NDJSON 消费数据并验证最终 statistics 和正常传输结束。首行时间包含客户端 JSON 解码，空结果取完整结束时间；缺失末尾记录、HTTP 错误、流截断或健康性能表意外返回 partial 都不能成为成功样本。HTTP 的 Query Cache / DOP 通过请求 sessionVariables 设置；元数据中的 SHOW VARIABLES 和 EXPLAIN 来自同设置的 MySQL 控制连接，不能冒称为 HTTP 会话自身的快照。两种协议分别比较 A/B/C，不把协议间客户端解码成本当作本功能开销。

默认场景同时包含 LIMIT 10 和倒序明细页 LIMIT 1000，后者实际向客户端传输明细文本；不只用 COUNT/SUM 代表日志检索响应成本。

在全部功能编码完成之后，分别运行 A 原版 / B 新版关闭 / C 新版开启。`--variant` 只是记录标签，不会替你切换服务或配置，必须先核实产物和配置。`--build-manifest` 应包含真实源码版本、未提交差异、编译参数、二进制摘要与实际 FE/BE 配置。

采样前会核对标签与 `selection.json` 及四个存活节点的启动记录完全一致，拒绝只改标签而未切换节点的测量。此检查仍不能替代二进制摘要和运行配置的核验。

```bash
/query-corruption-workspace/tools/python/bin/python tools/query_corruption/benchmark.py \
  --variant B-off --build-manifest /query-corruption-workspace/logs/QCT-006/B-off-build.json \
  --output /query-corruption-workspace/logs/QCT-006/B-off-cache-off-c1-run1.jsonl \
  --concurrency 1 --warmup 20 --repetitions 200
```

每次使用新输出文件，工具拒绝覆盖历史测量。保留计划、实际 SHOW VARIABLES 会话快照、随机执行顺序参数、首行/完成延迟原始样本、行数和 Warning 数。分位数使用 nearest-rank；客户端预热后统一开始采样，以首个样本开始到最后一个样本完成的墙钟窗口计算 `measured_queries_per_second`，不能用各查询时延之和计算并发吞吐。另保留明确标注“含预热”的全过程吞吐；两者不能混淆。这个客户端采样起点同步不在 SQL 内，也不是产品增加执行屏障。

比较需核验数据/计划/设置一致，交错多轮运行识别环境漂移。容器内共用 CPU/磁盘的结果有噪声，200 样本的 P99 只能作为有限样本观察。工具在计时区间外记录四个真实进程的 CPU 累计时间、RSS/HWM，以及 FE/BE 原始 metrics（获取失败显式记录 unavailable）；指标差值包含预热、后台活动和采样边界噪声，不能冒充单 SQL Profile。缓存是否真正命中、冷页状态仍需核实。普通 JOIN 与测试 SQL 显式关闭远端过滤的 JOIN 分列，避免把不适用容错的计划误当作 ON 覆盖。

工具自身单元测试：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/query_corruption -p 'test_*.py' -v
```

这些测试只证明工具边界与统计逻辑，不等于 BE 故障容错或性能验收。
