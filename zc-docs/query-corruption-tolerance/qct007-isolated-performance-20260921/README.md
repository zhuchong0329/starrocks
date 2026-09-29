# 单场景重启复验证据

本目录保存 2026-09-21 分页查询独立重启实验的编排脚本、复核脚本、统计汇总和运行审计。生产代码未修改。

完整逐请求正式/预热样本仍保留在 QCT 专属持久卷：

```text
/query-corruption-workspace/logs/QCT-007/isolated-detail-20260921-r1/
```

它们不是缺失或被删除，也不与旧混合负载样本合并。本地目录只复制轻量证据，避免将大量原始采样文件加入 Git 工作区。

只读重算命令（不启动数据库、不修改样本）：

```sh
docker exec starrocks-query-corruption-4.0-build \
  /query-corruption-workspace/tools/python/bin/python \
  /query-corruption-workspace/logs/QCT-007/analyze-isolated-20260921.py \
  /query-corruption-workspace/logs/QCT-007/isolated-detail-20260921-r1
```

不要直接重跑 `suite` 并复用旧 series；脚本会拒绝覆盖已有目录。未来执行新实验必须使用新的 series，重新核验资源和产物，并遵守仓库 AGENTS 中的 QCT / Tenant-TTL 隔离约束。
