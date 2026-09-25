# Tenant-TTL 第 041 轮：完整节点遍历与回归记录

日期：2026-09-25。实施前 HEAD：`9c45758a7`。对应 FE-DICT-015；用户已确认 041～043 计划及每表 12 万/120 万行两档真实验收规模。

## 变更

快照导出从每次固定访问排序前两台，改为对当前 Catalog 候选集合分片遍历，每片最多两台。未访问完时在队尾续执行，全部候选失败后才指数退避。跨事务保留公平游标；每片核验 Catalog 节点 ID、地址及在线状态，注销/替换节点不继续使用旧地址，新节点在下轮加入。参与刷新列表不再限制可用候选范围。

成功快照仍需完整 Builder 校验及生命周期核验；确定性错误停止当前事务；旧快照继续可用。未改 BE、导出协议、资源上限、普通 Dictionary 刷新或 TTL Replica 完成规则。无快照自动完整刷新属于 042，本轮尚未实现。

## 实际测试

复用唯一运行的 `starrocks-tenant-ttl-e2e` 和 `sr-tenant-ttl-4.0-build-cache-arm64`，仅同步本轮两个 Java 文件。Java 17.0.15 / Maven 3.6.3；不执行 clean、不重建容器、不删除缓存或产物。

1. 旧实现红灯：`TenantTtlPolicySnapshotManagerTest` 11 项，原有 8 项通过，新增 3 项失败，0 errors/skipped。分别证明第三节点无即时机会、五节点未完成即退避、事务推进重新偏向前两台。日志及 XML：持久卷 `/tenant-ttl-workspace/round041-fe-red.log`、`round041-fe-red.xml`。
2. 修复后定向回归：`TenantTtlPolicySnapshotManagerTest,TenantTtlPolicySnapshotBuilderTest,TenantTtlStatusServiceTest` 共 29 项，全部通过。Manager 16 项包括节点注销/新增、ID/地址变化、无节点/全部离线、队尾让出和解绑失效、第三节点确定性阻塞。日志：`round041-fe-green.log`。
3. 全部 `*TenantTtl*Test` 选择器：129 项，0 failures/errors/skipped。包含既有快照、Builder、状态、调度、DDL 及聚合防护等匹配该命名的测试；不等于全 FE 回归。日志：`round041-fe-regression.log`。
4. FE Checkstyle：0 violations；`git diff --check` 通过。日志：`round041-checkstyle.log`。

执行入口：容器中设置 `STARROCKS_HOME=/tenant-ttl-workspace`、`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64`，加载 `env.sh` 后进入 `fe`。Maven 使用 `-pl fe-core -am -Dmaven.clean.skip=true -Dcheckstyle.skip -Dtest='<上述选择器>' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -T 1 test`；Checkstyle 使用 `mvn -pl fe-core -DskipTests checkstyle:check`。

未执行 FE package、BE 编译/单测或真实集群 SQL/故障注入：本轮只完成导出遍历，package 和相关 BE 回归将在 042 完成后执行，真实 3 FE/3 BE、多副本逐行验收属于 043。129 项 FE 测试不替代真实容灾验收，不宣称此次自动恢复需求整体已交付。

## 后续

042 实现无快照恢复分类、刷新去重、冷却及动态开关；043 按已确认 E01～E10 和两档数据规模完成真实逐副本验收。已有用户未提交的 035～038 记录、SQL、design/path 内容不纳入本轮提交。
