-- 只读，可重复执行；初始收敛预期两表各 9 行：NOOP=4、DELETE_LIST=3、KEEP_LIST=2、DROP=0。
USE tenant_ttl_r038_20260923;
SET time_zone = 'Asia/Shanghai';
SELECT now() AS observation_time;
SELECT 'a' AS tbl, scenario, count(*) AS rows_count,
       group_concat(ifnull(tenant, '<NULL>') ORDER BY ifnull(tenant, '<NULL>')) AS tenants
FROM events_a GROUP BY scenario
UNION ALL
SELECT 'b', scenario, count(*),
       group_concat(ifnull(tenant, '<NULL>') ORDER BY ifnull(tenant, '<NULL>'))
FROM events_b GROUP BY scenario ORDER BY tbl, scenario;
SHOW PARTITIONS FROM events_a;
SHOW PARTITIONS FROM events_b;
SHOW TENANT TTL STATUS FROM events_a;
SHOW TENANT TTL STATUS FROM events_b;
SHOW ALTER TABLE COLUMN FROM tenant_ttl_r038_20260923;
