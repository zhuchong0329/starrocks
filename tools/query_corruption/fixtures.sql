-- Copyright 2021-present StarRocks, Inc. All rights reserved.
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- https://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Only load into the dedicated marked QCT test cluster, after all three BEs are alive.
-- Intentionally no DROP/TRUNCATE/IF NOT EXISTS: refuse accidental reuse of existing databases.
CREATE DATABASE qct_faults;
CREATE TABLE qct_faults.one_tablet(k BIGINT, message VARCHAR(256))
DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO qct_faults.one_tablet
SELECT generate_series, concat('qct-', cast(generate_series AS VARCHAR), '-', repeat('local-log-message-', 8))
FROM TABLE(generate_series(1, 32768));

CREATE TABLE qct_faults.multiple_tablets(k BIGINT, message VARCHAR(256))
DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 6 PROPERTIES('replication_num'='1');
INSERT INTO qct_faults.multiple_tablets SELECT * FROM qct_faults.one_tablet;

CREATE TABLE qct_faults.healthy(k BIGINT, message VARCHAR(256))
DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO qct_faults.healthy SELECT * FROM qct_faults.one_tablet WHERE k <= 8;

-- Keep performance data independent from every injected fault.
CREATE DATABASE qct_perf;
CREATE TABLE qct_perf.events(k BIGINT, message VARCHAR(256))
DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 6 PROPERTIES('replication_num'='1');
INSERT INTO qct_perf.events
SELECT generate_series, concat('qct-', cast(generate_series AS VARCHAR), '-', repeat('local-log-message-', 8))
FROM TABLE(generate_series(1, 1000000));

CREATE TABLE qct_perf.points(k BIGINT NOT NULL, message VARCHAR(256))
PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 6 PROPERTIES('replication_num'='1');
INSERT INTO qct_perf.points SELECT * FROM qct_perf.events WHERE k <= 10000;

CREATE TABLE qct_perf.dimension(k BIGINT NOT NULL)
DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO qct_perf.dimension SELECT generate_series FROM TABLE(generate_series(0, 7));
