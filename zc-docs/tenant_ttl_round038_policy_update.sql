-- 迟到数据收敛后执行；只变更业务策略，不变更绑定。
-- long 从 20 天降为 3 天；NULL 的最长保留期变为 max(default=10,short=2,long=3)=10。
-- 收敛后两表各 6 行：NOOP=4、DELETE_LIST 分区仅 other/NULL=2、15 天分区整体删除。
USE tenant_ttl_r038_20260923;
INSERT INTO ttl_policy VALUES ('long', 'round038.events', 3);
REFRESH DICTIONARY tenant_ttl_r038_dict;
SHOW TENANT TTL STATUS FROM events_a;
SHOW TENANT TTL STATUS FROM events_b;
