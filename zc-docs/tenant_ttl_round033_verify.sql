USE tenant_ttl_r033_20260916;
SET time_zone = 'Asia/Shanghai';
SELECT 'single' AS table_kind, scenario, coalesce(tenant, '<NULL>') AS tenant, count(*) AS rows_count
FROM events_single GROUP BY scenario, tenant
UNION ALL
SELECT 'multi_cast', scenario, coalesce(tenant, '<NULL>'), count(*)
FROM events_multi_cast GROUP BY scenario, tenant
ORDER BY table_kind, scenario, tenant;
-- Each table: noop=4, delete_list=3, keep_list=2, drop=0, total=9.
SHOW PARTITIONS FROM events_single;
SHOW PARTITIONS FROM events_multi_cast;
SHOW TABLET FROM events_single;
SHOW TENANT TTL STATUS FROM events_single;
SHOW TENANT TTL STATUS FROM events_multi_cast;
SHOW ALTER TABLE COLUMN FROM tenant_ttl_r033_20260916;
SHOW DICTIONARY;
