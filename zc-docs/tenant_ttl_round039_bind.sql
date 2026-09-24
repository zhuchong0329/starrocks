-- 在保存绑定前聚合/EXPLAIN 后执行；此步授权调度器删除本独立测试库的过期数据。
USE tenant_ttl_r039_20260924;
SET time_zone = 'Asia/Shanghai';
ALTER TABLE events SET (
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r039_dict', 'round039.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);
ALTER TABLE rewrite_only SET (
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r039_dict', 'round039.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);
ALTER TABLE rewrite_empty SET (
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r039_dict', 'round039.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);
REFRESH DICTIONARY tenant_ttl_r039_dict;
SELECT now() AS observation_time;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM rewrite_only;
EXPLAIN SELECT count(*), min(metric), max(metric) FROM control;
SHOW TENANT TTL STATUS FROM events;
SHOW TENANT TTL STATUS FROM rewrite_only;
SHOW TENANT TTL STATUS FROM rewrite_empty;
