# 社区测试夹具修复：ExchangePassThroughTest

日期：2026-09-19。独立于 QCT 功能的测试修复，不改生产代码。

## 问题及定位

4.0.11 原夹具直接创建接收器，但没有注册 query 级 pass-through buffer；单独运行首个旧用例时在 `PassThroughChunkBuffer::get_or_create_channel` 空指针崩溃。补齐后发现旧用例只调 `prepare`、未调当前算子的 `prepare_local_state`，因此通道未初始化。即使数据走本地 pass-through，元数据仍须通过 RPC 通知接收端；旧夹具没有启动对应服务，不能依赖其他 UT 的进程状态。

原无限填充循环在这些前置条件不成立时不能保证退出。排查中出现一次测试进程 OOM，原日志保留；它不是生产查询的容错路径 OOM。

## 修复

- 成对初始化/销毁 pass-through buffer，补齐原三个用例的算子本地准备。
- 为该 endpoint 的 stub pool 临时安装本地测试桩，经过真实 `DataStreamMgr`/接收队列；元数据发送后，在发送锁释放时执行 RPC 完成回调。接收队列仍真实控制背压。
- 保存并恢复 stub pool，避免跨测试污染；不打开固定网络监听端口。
- 检查 push 状态、将填充循环限制为 4096 块并显式验证达到背压，失败时不无限分配。

## 实际测试

在 QCT 专属 arm64 Release BE_TEST 联合构建中，原三个 Exchange 用例全部通过；包含后续 QCT 用例的 9 项传播回归全部通过。随后整个聚焦目标 14 项按随机顺序重复 20 轮（280 次用例执行）全部通过，exit 0；这不是 280 个独立用例，也不是完整 BE UT 套件。

证据：`/query-corruption-workspace/logs/QCT-003/propagation-unit-tests-v5.log`、`.xml`、`be-combined-repeat20.log`、`.xml`。旧失败记录 v1～v4 保留。修改行 clang-format-14 检查通过。

本次没有另建原版 BE UT 全量构建；“社区继承”依据固定基线的原夹具代码和实际崩溃调用链。本提交只包含夹具修复，不包含 QCT 的开关、诊断字段、新增传播用例或生产逻辑。
