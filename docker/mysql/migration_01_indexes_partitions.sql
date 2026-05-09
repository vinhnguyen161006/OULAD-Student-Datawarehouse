-- Migration 01 — apply indexes + monitoring_log partitioning to live DB.
-- An toàn: idempotent — bỏ qua nếu index/partition đã tồn tại.
-- Chạy:  docker compose exec -T mysql mysql -uroot -prootpassword < docker/mysql/migration_01_indexes_partitions.sql

USE student_dwh;

-- 1) Index trên Fact_Performance
--    MySQL không có CREATE INDEX IF NOT EXISTS → dùng information_schema để check.

SET @idx_exists := (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = 'student_dwh'
      AND table_name   = 'Fact_Performance'
      AND index_name   = 'idx_fact_risk_group'
);
SET @sql := IF(@idx_exists = 0,
    'CREATE INDEX idx_fact_risk_group ON Fact_Performance(risk_group)',
    'SELECT "idx_fact_risk_group already exists" AS msg'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @idx_exists := (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = 'student_dwh'
      AND table_name   = 'Fact_Performance'
      AND index_name   = 'idx_fact_final_result'
);
SET @sql := IF(@idx_exists = 0,
    'CREATE INDEX idx_fact_final_result ON Fact_Performance(final_result)',
    'SELECT "idx_fact_final_result already exists" AS msg'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2) monitoring_log: đổi PK + add indexes + partition.
--    Bước này yêu cầu bảng chưa partition. Check trước khi đụng.

SET @already_partitioned := (
    SELECT COUNT(*) FROM information_schema.partitions
    WHERE table_schema = 'student_dwh'
      AND table_name   = 'monitoring_log'
      AND partition_name IS NOT NULL
);

-- 2a) Composite PK + indexes (chỉ chạy nếu chưa partition — vì lần đầu tiên).
SET @sql := IF(@already_partitioned = 0,
    'ALTER TABLE monitoring_log
        DROP PRIMARY KEY,
        MODIFY checked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        ADD PRIMARY KEY (id, checked_at),
        ADD INDEX idx_mon_checked_at (checked_at),
        ADD INDEX idx_mon_status_checked (status, checked_at)',
    'SELECT "monitoring_log already partitioned, skip PK/index step" AS msg'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2b) RANGE partition theo quý
SET @sql := IF(@already_partitioned = 0,
    "ALTER TABLE monitoring_log
        PARTITION BY RANGE (TO_DAYS(checked_at)) (
            PARTITION p_2025      VALUES LESS THAN (TO_DAYS('2026-01-01')),
            PARTITION p_2026_q1   VALUES LESS THAN (TO_DAYS('2026-04-01')),
            PARTITION p_2026_q2   VALUES LESS THAN (TO_DAYS('2026-07-01')),
            PARTITION p_2026_q3   VALUES LESS THAN (TO_DAYS('2026-10-01')),
            PARTITION p_2026_q4   VALUES LESS THAN (TO_DAYS('2027-01-01')),
            PARTITION p_2027      VALUES LESS THAN (TO_DAYS('2028-01-01')),
            PARTITION p_max       VALUES LESS THAN MAXVALUE
        )",
    'SELECT "monitoring_log already partitioned" AS msg'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 3) Verify
SELECT 'Indexes on Fact_Performance' AS section;
SELECT index_name, column_name, seq_in_index
FROM information_schema.statistics
WHERE table_schema = 'student_dwh' AND table_name = 'Fact_Performance'
ORDER BY index_name, seq_in_index;

SELECT 'Indexes on monitoring_log' AS section;
SELECT index_name, column_name, seq_in_index
FROM information_schema.statistics
WHERE table_schema = 'student_dwh' AND table_name = 'monitoring_log'
ORDER BY index_name, seq_in_index;

SELECT 'Partitions on monitoring_log' AS section;
SELECT partition_name, partition_method, partition_expression, partition_description, table_rows
FROM information_schema.partitions
WHERE table_schema = 'student_dwh' AND table_name = 'monitoring_log'
ORDER BY partition_ordinal_position;
