-- 首次执行；复测请统一替换三个脚本的库名和全局 Dictionary 名。
-- 无 DROP/TRUNCATE，不修改全局配置。此步尚不绑定 TTL，便于先预热旧聚合缓存。
CREATE DATABASE tenant_ttl_r039_20260924;
USE tenant_ttl_r039_20260924;
SET time_zone = 'Asia/Shanghai';

CREATE TABLE ttl_policy (
    tenant VARCHAR(128) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    retention_days INT NOT NULL
) PRIMARY KEY (tenant, table_name)
DISTRIBUTED BY HASH(tenant) BUCKETS 1
PROPERTIES ('replication_num' = '1');
INSERT INTO ttl_policy VALUES
    ('default', 'round039.events', 10),
    ('short', 'round039.events', 2),
    ('long', 'round039.events', 20);
CREATE DICTIONARY tenant_ttl_r039_dict
USING ttl_policy (tenant KEY, table_name KEY, retention_days VALUE)
PROPERTIES ('dictionary_refresh_interval' = '0', 'dictionary_ignore_failed_refresh' = 'true');

CREATE TABLE events (
    tenant VARCHAR(128) NULL,
    recordTimestamp BIGINT NOT NULL,
    scenario VARCHAR(16) NOT NULL,
    metric BIGINT NOT NULL,
    optional_metric BIGINT NULL
) DUPLICATE KEY(tenant, recordTimestamp)
PARTITION BY date_trunc('day', from_unixtime(recordTimestamp))
DISTRIBUTED BY HASH(tenant) BUCKETS 2
PROPERTIES ('replication_num' = '1');
CREATE TABLE rewrite_only LIKE events;
CREATE TABLE rewrite_empty LIKE events;
CREATE TABLE control LIKE events;

INSERT INTO events
SELECT tenant, unix_timestamp(date_sub(curdate(), INTERVAL age DAY)) + 43200,
       scenario, metric, if(tenant IS NULL, NULL, metric)
FROM (
    SELECT t.tenant, s.age, s.scenario,
           CASE s.age
               WHEN 0 THEN 10 + t.n
               WHEN 5 THEN if(t.n = 1, -1000, 19 + t.n)
               WHEN 15 THEN CASE t.n WHEN 1 THEN -2000 WHEN 3 THEN 2000 ELSE 29 + t.n END
               ELSE CASE t.n WHEN 1 THEN -3000 ELSE 1000 + 1000 * t.n END
           END AS metric
    FROM (SELECT 'short' AS tenant, 1 AS n UNION ALL SELECT 'long', 2
          UNION ALL SELECT 'other', 3 UNION ALL SELECT NULL, 4) t
    CROSS JOIN (SELECT 0 AS age, 'noop' AS scenario UNION ALL SELECT 5, 'delete_list'
                UNION ALL SELECT 15, 'keep_list' UNION ALL SELECT 40, 'drop') s
) source_rows;
INSERT INTO rewrite_only SELECT * FROM events WHERE scenario IN ('delete_list', 'keep_list');
INSERT INTO rewrite_empty SELECT * FROM events WHERE scenario = 'delete_list' AND tenant = 'short';
INSERT INTO rewrite_empty SELECT * FROM events WHERE scenario = 'delete_list' AND tenant = 'short';
INSERT INTO control SELECT * FROM rewrite_only;

-- 绑定前：events=16/-3000/5000；rewrite_only=8/-2000/2000；rewrite_empty=2/-1000/-1000。
-- control 永远不绑定 TTL，始终为 8/-2000/2000。
SELECT now() AS observation_time;
SELECT 'events' AS tbl, count(*) AS n, min(metric) AS lo, max(metric) AS hi FROM events
UNION ALL SELECT 'rewrite_only', count(*), min(metric), max(metric) FROM rewrite_only
UNION ALL SELECT 'rewrite_empty', count(*), min(metric), max(metric) FROM rewrite_empty
UNION ALL SELECT 'control', count(*), min(metric), max(metric) FROM control;
-- 多次执行可预热异步 MIN/MAX；FE COUNT 常量还需满足原有统计新鲜度条件。
EXPLAIN SELECT count(*), min(metric), max(metric) FROM rewrite_only;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM control;
SHOW DICTIONARY tenant_ttl_r039_dict;
