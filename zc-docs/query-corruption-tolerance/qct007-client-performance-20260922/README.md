# QCT-007 客户端模型与 CPU 隔离实验的轻量证据

日期：2026-09-22。执行状态以相邻的 [报告](../QCT-007-客户端瓶颈与隔离复验.md) 为准，不把本目录存在解释为全部测量完成。

最终状态：30 组 / 300000 正式样本与两部分原始记录审计完成。`shared-audit-summary.json` / `split-audit-summary.json` 是各阶段结果；`environment-audit.json` 记录恢复后的精确状态。所有查询节点和客户端停止，无生产代码改动。

本目录只保存编排/统计脚本、设计、轻量汇总、测试日志及环境记录。原始逐请求数据在专属 Docker 持久卷，未删除，也不把大量原始样本写入 Git。

## 实验数据位置

1. 同一原 QCT 容器下，T=单进程四线程与 P=四进程单线程的四对社区基线对照：
   - 容器：`starrocks-query-corruption-4.0-build`。
   - 卷：`sr-query-corruption-4.0-build-cache-arm64`。
   - 目录：`/query-corruption-workspace/logs/QCT-007/client-architecture-20260922-r1/`。
2. 客户端/服务器 CPU 集合与配额隔离：
   - 服务端仍为上述 QCT 容器，清单/设计/文件指纹位于 `logs/QCT-007/split-client-20260922-r1/`。
   - 客户端容器：`starrocks-query-corruption-perf-client`。
   - 客户端卷：`sr-query-corruption-client-perf-arm64`，挂载 `/qct-client-workspace`。
   - 客户端逐请求证据：`/qct-client-workspace/results/split-client-20260922-r1/`。
   - 两对 TP/PT 基线控制后，按 ABC/ACB/BAC/BCA/CAB/CBA 六块复验。

这些脚本只针对带有专属标记的 QCT 测试集群，不能当作生产压测/启停脚本直接运行。它们拒绝覆盖旧证据、拒绝活动编译/未恢复故障，并在退出时停止本任务节点。不运行时不要根据历史 PID 发送信号。

## 统计口径

- 每组 60 秒共同预热、四个工作者各 2500 正式请求，总并发/连接数固定 4。
- 逐请求完整读取 1000 行及最终包，计时复用已提交的 `benchmark.execute`；没有截断结果或改用 COUNT。
- 聚合文件中的 `sample` 为正式请求，`warmup` 不计入正式 QPS；`*-worker-*.jsonl` 是同一批样本的工作者副本，不能重复累计。
- QPS 用全部正式请求的单调时间总窗口计算，不能将四个线程 QPS 简单相加或用时延相加代替墙钟时间。
- 版本/客户端差异按运行块配对，不能把单 SQL 当独立环境复验。几何平均单组 P99 不等于合并请求后重新算 P99。
- CPU/查询来自实际进程 CPU 差值；多进程汇总全部四个 PID，单进程不重复算四遍。另有工作者测量区间的线程 CPU 交叉检查。
- CPU 隔离时同时记录客户端与服务端 cgroup。共享 VM/宿主的限制仍存在，不能宣称物理机器独占。

## 离线审计

`analyze_client.py` 只读取记录，验证样本数/行数/Warning、QPS/P99 重算、源清单摘要、进程重启、计划与放置一致，以及最终文件未变/节点停止。`--output` 使用排他新建，不能覆盖旧汇总。

同容器实验：

```bash
/query-corruption-workspace/tools/python/bin/python analyze_client.py \
  /query-corruption-workspace/logs/QCT-007/client-architecture-20260922-r1
```

隔离实验在客户端容器中读取两个卷：

```bash
/query-corruption-workspace/tools/python/bin/python analyze_client.py \
  /query-corruption-workspace/logs/QCT-007/split-client-20260922-r1 \
  --results-root /qct-client-workspace/results/split-client-20260922-r1
```

只有未完成时的临时观察才用 `--partial`，不能据此声明整套验收完成。最终还需独立检查容器停止、服务端原 CPU 设置恢复、无新增 OOM 和生产 Git 差异未变化。

## 已知收尾问题与复用注意

历史 `split_suite.py` 原样保留，包含一次实际发生的恢复缺陷：空 CPU 集合参数未能清除 Docker 的 `0-5`，控制器虽 exit 0，但 `split-environment-after.json` 中恢复标志为 false。后续独立审计发现并明确设置 `0-7`，恢复全部当前 VM CPU；6 核预算、16 GiB、容器身份/挂载均未变，唯一 HostConfig 差异为原空串变显式 `0-7`。`export_environment.py` 如实校验并记录这个差异，没有改写首次失败记录。

不要原封不动重复执行旧编排：证据目录已存在，当前 CPU 配置也不同，脚本会拒绝。若另开实验，应使用新的系列名、按当前真实配置设置恢复目标，并在完成后读回核验，不以 Docker 命令成功代替资源恢复成功。将来扩 VM CPU 数时需更新显式 `0-7`。无需为恢复空字段重建已保存编译资产的容器。
