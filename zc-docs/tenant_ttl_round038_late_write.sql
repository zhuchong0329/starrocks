-- 初始收敛并观察无事件轮次后只执行一次；预期 A 的迟到 short 被补偿删除，B 不重发。
USE tenant_ttl_r038_20260923;
SET time_zone = 'Asia/Shanghai';
INSERT INTO events_a VALUES
    ('short', unix_timestamp(date_sub(curdate(), INTERVAL 5 DAY)) + 43201, 'delete_list');
SELECT 'after_late_write' AS step, scenario, count(*) AS rows_count
FROM events_a GROUP BY scenario ORDER BY scenario;
SHOW TENANT TTL STATUS FROM events_a;
