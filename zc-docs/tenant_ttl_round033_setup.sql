-- Run with mysql --verbose; use new database/dictionary names for each repeat.
-- No DROP/TRUNCATE: preserve existing data and this verification fixture.
CREATE DATABASE tenant_ttl_r033_20260916;
USE tenant_ttl_r033_20260916;
SET time_zone = 'Asia/Shanghai';

CREATE TABLE ttl_policy (
    tenant VARCHAR(128) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    retention_days INT NOT NULL
) PRIMARY KEY (tenant, table_name)
DISTRIBUTED BY HASH(tenant) BUCKETS 1
PROPERTIES ('replication_num' = '1');

INSERT INTO ttl_policy VALUES
    ('default', 'round033.events', 10),
    ('short', 'round033.events', 2),
    ('long', 'round033.events', 20);

CREATE DICTIONARY tenant_ttl_r033_dict
USING ttl_policy (tenant KEY, table_name KEY, retention_days VALUE)
PROPERTIES ('dictionary_refresh_interval' = '0', 'dictionary_ignore_failed_refresh' = 'true');

-- ALTER path: record partition/tablet IDs and rows before binding.
CREATE TABLE events_single (
    tenant VARCHAR(128) NULL,
    recordTimestamp BIGINT NOT NULL,
    scenario VARCHAR(16) NOT NULL
) DUPLICATE KEY(tenant, recordTimestamp)
PARTITION BY date_trunc('day', from_unixtime(recordTimestamp))
DISTRIBUTED BY HASH(tenant) BUCKETS 1
PROPERTIES ('replication_num' = '1');

INSERT INTO events_single
SELECT t.tenant, unix_timestamp(date_sub(curdate(), INTERVAL s.age DAY)) + 43200, s.scenario
FROM (SELECT 'short' AS tenant UNION ALL SELECT 'long' UNION ALL SELECT 'other' UNION ALL SELECT NULL) t
CROSS JOIN (SELECT 0 AS age, 'noop' AS scenario UNION ALL SELECT 5, 'delete_list'
            UNION ALL SELECT 15, 'keep_list' UNION ALL SELECT 40, 'drop') s;

SELECT 'before_binding' AS step, scenario, count(*) AS rows_count
FROM events_single GROUP BY scenario ORDER BY scenario;
SHOW PARTITIONS FROM events_single;
SHOW TABLET FROM events_single;

-- CREATE path, multi-column automatic List + equivalent explicit CAST.
CREATE TABLE events_multi_cast (
    tenant VARCHAR(128) NULL,
    recordTimestamp BIGINT NOT NULL,
    tenant_bucket INT NOT NULL,
    scenario VARCHAR(16) NOT NULL
) DUPLICATE KEY(tenant, recordTimestamp)
PARTITION BY (tenant_bucket, date_trunc('day', CAST(from_unixtime(recordTimestamp) AS DATETIME)))
DISTRIBUTED BY HASH(tenant) BUCKETS 1
PROPERTIES (
    'replication_num' = '1',
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r033_dict', 'round033.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);
INSERT INTO events_multi_cast (tenant, recordTimestamp, tenant_bucket, scenario)
SELECT tenant, recordTimestamp, 7, scenario FROM events_single;

ALTER TABLE events_single SET (
    'compaction_retention_condition' = "dictionary_ttl('tenant_ttl_r033_dict', 'round033.events', 30)",
    'compaction_retention_time_zone' = 'Asia/Shanghai'
);

SHOW CREATE TABLE events_single;
SHOW CREATE TABLE events_multi_cast;
SHOW ALTER TABLE COLUMN FROM tenant_ttl_r033_20260916;
SHOW TENANT TTL STATUS FROM events_single;
SHOW TENANT TTL STATUS FROM events_multi_cast;
