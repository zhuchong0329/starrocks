-- 首次执行；重复验收请先将库名和全局 Dictionary 名替换为新的唯一名称。
-- mysql -h127.0.0.1 -P9030 -uroot --skip-comments --verbose --table < 本文件
-- 无 DROP/TRUNCATE；绑定 TTL 后预期由调度器删除本测试库的过期数据。
CREATE DATABASE tenant_ttl_r038_20260923;
USE tenant_ttl_r038_20260923;
SET time_zone = 'Asia/Shanghai';

CREATE TABLE ttl_policy (
    tenant VARCHAR(128) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    retention_days INT NOT NULL
) PRIMARY KEY (tenant, table_name)
DISTRIBUTED BY HASH(tenant) BUCKETS 1
PROPERTIES ('replication_num' = '1');

INSERT INTO ttl_policy VALUES
    ('default', 'round038.events', 10),
    ('short', 'round038.events', 2),
    ('long', 'round038.events', 20);

CREATE DICTIONARY tenant_ttl_r038_dict
USING ttl_policy (tenant KEY, table_name KEY, retention_days VALUE)
PROPERTIES ('dictionary_refresh_interval' = '0', 'dictionary_ignore_failed_refresh' = 'true');

CREATE TABLE events_a (
    tenant VARCHAR(128) NULL,
    recordTimestamp BIGINT NOT NULL,
    scenario VARCHAR(16) NOT NULL
) DUPLICATE KEY(tenant, recordTimestamp)
PARTITION BY date_trunc('day', from_unixtime(recordTimestamp))
DISTRIBUTED BY HASH(tenant) BUCKETS 4
PROPERTIES ('replication_num' = '1');

CREATE TABLE events_b LIKE events_a;

INSERT INTO events_a
SELECT t.tenant, unix_timestamp(date_sub(curdate(), INTERVAL s.age DAY)) + 43200, s.scenario
FROM (SELECT 'short' AS tenant UNION ALL SELECT 'long' UNION ALL SELECT 'other' UNION ALL SELECT NULL) t
CROSS JOIN (SELECT 0 AS age, 'noop' AS scenario UNION ALL SELECT 5, 'delete_list'
            UNION ALL SELECT 15, 'keep_list' UNION ALL SELECT 40, 'drop') s;
INSERT INTO events_b (tenant, recordTimestamp, scenario)
SELECT tenant, recordTimestamp, scenario FROM events_a;

SELECT 'before_binding_a' AS step, scenario, count(*) AS rows_count
FROM events_a GROUP BY scenario ORDER BY scenario;
SELECT 'before_binding_b' AS step, scenario, count(*) AS rows_count
FROM events_b GROUP BY scenario ORDER BY scenario;
SHOW PARTITIONS FROM events_a;
SHOW TABLET FROM events_a;
SHOW TABLET FROM events_b;

ALTER TABLE events_a SET (
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r038_dict', 'round038.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);
ALTER TABLE events_b SET (
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r038_dict', 'round038.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);
REFRESH DICTIONARY tenant_ttl_r038_dict;
SHOW CREATE TABLE events_a;
SHOW TENANT TTL STATUS FROM events_a;
SHOW TENANT TTL STATUS FROM events_b;
ADMIN SHOW FRONTEND CONFIG LIKE 'tenant_ttl_scheduler_interval_seconds';
ADMIN SHOW FRONTEND CONFIG LIKE 'tenant_ttl_agent_task_max_attempts';
ADMIN SHOW FRONTEND CONFIG LIKE 'tenant_ttl_agent_task_soft_timeout_seconds';
