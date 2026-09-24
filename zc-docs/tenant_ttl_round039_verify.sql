-- 只读，可重复运行；收敛前允许真实数据尚未删完，须结合任务/分区日志判断。
USE tenant_ttl_r039_20260924;
SET time_zone = 'Asia/Shanghai';
SELECT now() AS observation_time;
-- 收敛后：events=9/11/33；rewrite_only=5/21/33；rewrite_empty=0/NULL/NULL；control 不变。
SELECT 'events' AS tbl, count(*) AS n, min(metric) AS lo, max(metric) AS hi FROM events
UNION ALL SELECT 'rewrite_only', count(*), min(metric), max(metric) FROM rewrite_only
UNION ALL SELECT 'rewrite_empty', count(*), min(metric), max(metric) FROM rewrite_empty
UNION ALL SELECT 'control', count(*), min(metric), max(metric) FROM control;
SELECT scenario, count(*) AS n, min(metric) AS lo, max(metric) AS hi
FROM events GROUP BY scenario ORDER BY scenario;
SELECT tenant, scenario, metric, optional_metric FROM rewrite_only ORDER BY scenario, metric;
SELECT count(optional_metric), count(DISTINCT metric) FROM rewrite_only;
SELECT tenant, scenario, metric FROM rewrite_empty ORDER BY scenario, metric;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM rewrite_only;
EXPLAIN SELECT count(*) FROM rewrite_empty;
EXPLAIN SELECT count(optional_metric) FROM rewrite_only;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM control;
SHOW TENANT TTL STATUS FROM events;
SHOW TENANT TTL STATUS FROM rewrite_only;
SHOW TENANT TTL STATUS FROM rewrite_empty;
SHOW PARTITIONS FROM rewrite_only;
