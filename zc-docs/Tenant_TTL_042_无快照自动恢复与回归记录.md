# Tenant-TTL 第 042 轮：无快照自动恢复与回归记录

日期：2026-09-25。实施前 HEAD：`439c74292`（041）。对应 FE-DICT-016，用户已确认全部计划及两档真实数据规模。

## 实现与并发边界

- 无可用 FE 快照时，完整候选轮次全部明确缺失/版本不符可首次申请恢复；包含网络不确定时需同一目标连续至少 3 个完整失败轮次且持续至少 60 秒。完整性/内部错误不触发源刷新；确定性错误继续阻塞。
- 空集合、全部离线不积累可触发源刷新的失败证据；节点重新上线先导出。保留旧快照时不新增恢复刷新。
- 沿用普通 Dictionary 刷新队列、源查询、事务和导出。首次绑定/启动请求也经过按 ID 的原子准入；已有任务合并等待，失败进入冷却重试。第一次绑定前已有刷新时，保留其完成后额外刷新一次的契约。
- `tenant_ttl_policy_snapshot_auto_recover_enabled=true`；`tenant_ttl_policy_snapshot_recovery_cooldown_seconds=300`。后者从所跟踪刷新完成起按单调时间计算，非正值使用 300 秒并诊断。最多一个逻辑轻量检查 timer；开关重新开启可自动重评估。RPC 排队仍可能延长检查时间。
- 新增本地刷新 ID 区分迟到完成回调与当前任务，不把本地 ID 当作成功事务，不持久化它。去重同时核验 FE 成功水位、已排队/运行任务及刷新状态；发布合法快照才算策略恢复。
- SHOW 沿用原列数和类型，在有界 ErrorMessage 中增加阶段、目标、实际已访问/候选数、刷新结果、下次检查和冷却剩余时间。

锁路径核查：SnapshotManager monitor → DictionaryMgr lock；DictionaryMgr 的 drop 和刷新完成回调在释放自身锁后进入 SnapshotManager。条件准入只登记本地队列与身份，不做 RPC、源查询或等待 Journal；由普通调度器设置 REFRESHING、提交日志并启动 worker。初次准入使用已持有的 Manager 生命周期锁作为资格消费屏障，不另传可在锁外失效的布尔 token。Catalog 节点枚举、RPC 和 Builder 在 Manager 临界区外执行。

普通非 Tenant-TTL 刷新的既有调度和结果语义不变；未修改 BE/协议、TTL 调度并发或 Replica 完成规则。集群级性能与真实数据正确性须由 043 验收，不以本轮单测替代。

## 实际验证

复用 `starrocks-tenant-ttl-e2e` 及持久卷，Java 17.0.15 / Maven 3.6.3；仅同步变化文件，没有 clean 或重建整个工作区。

1. 041 旧行为上的两项红灯均失败：明确缺失不会申请恢复、启动刷新失败后没有恢复 timer。日志/XML 为 `/tenant-ttl-workspace/round042-fe-red.*`。
2. 初次定向 42 项全部通过，日志 `round042-fe-initial.log`。
3. `*TenantTtl*Test,DictionaryMgrTest,ConfigTest,LeaderImplTest,ReportHandlerTest` 共 172 项：171 通过、0 failures/errors、1 skipped。跳过的是 `ConfigTest.testMutableConfig`，原测试因容器环境不可执行持久化而触发 assumption；不当作通过。日志 `round042-fe-regression.log`。
4. 最后修正提前成功/确定性阻塞时的候选访问诊断计数，补充第三节点阻塞应显示 `3/4` 的断言；Manager/Recovery/Status 定向 33 项全部通过。日志 `round042-fe-diagnostic.log`。其余 FE 生产代码在 172 项回归后未改动。
5. Checkstyle 首次发现一项测试 import 顺序问题，修正后最终 0 violations。最终增量 FE package 成功；日志 `round042-checkstyle-final.log`、`round042-fe-package-final.log`。`git diff --check` 通过。

验证包含明确缺失、第三节点命中、网络双阈值、CRC/内部错误、事务改变、动态开关、299/300 秒边界、长时间在途刷新、墙钟回拨、无效冷却值、迟到回调、原子并发准入、已有手动/COMMITTING 任务、初次请求异常、解绑/drop/新 Leader timer 失效等。单调时钟用可控测试时钟验证，真实 60/300 秒门槛仍由 043 补齐。

BE 使用实际配置核对过的 `be/ut_build_Debug`（Debug、MAKE_TEST=ON、GCC 12.3、无 sanitizer、WITH_STARCACHE=ON、WITH_TENANN=OFF），正在增量补齐相关目标并执行存储/Dictionary/Agent 回归；尚未将正在运行或未运行的目标记为通过。BE 相关最终结果并入后续 043 验收记录。

未执行全 FE 回归，也尚未执行真实 3 FE/3 BE 的 E01～E10 或 12 万/120 万行逐副本验收；总体验收仍待 043。保留所有构建树、缓存、产物及原始日志，未混入既有用户修改。
