-- Student Data Warehouse — MySQL Init Script
-- Auto-executes on MySQL container startup
-- Architecture: Bronze (MinIO) → Silver (MinIO Parquet) → DWH (MySQL) → Mart (dbt)
-- Note: No stg_* tables - Staging layer resides in MinIO Silver as Parquet

CREATE DATABASE IF NOT EXISTS student_dwh
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS student_data_mart
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE student_dwh;

-- Star Schema: Dim_Time → Dim_Course → Dim_Student → Dim_Assessment → Fact_Performance

CREATE TABLE IF NOT EXISTS Dim_Time (
    time_key           INT AUTO_INCREMENT PRIMARY KEY,
    code_presentation  VARCHAR(10) UNIQUE NOT NULL,
    year               INT         NOT NULL,
    semester_type      VARCHAR(5)  NOT NULL,
    presentation_label VARCHAR(20) NOT NULL
);

INSERT IGNORE INTO Dim_Time (code_presentation, year, semester_type, presentation_label) VALUES
    ('2013J', 2013, 'J', 'Kỳ 1 - 2013'),
    ('2013B', 2013, 'B', 'Kỳ 2 - 2013'),
    ('2014J', 2014, 'J', 'Kỳ 1 - 2014'),
    ('2014B', 2014, 'B', 'Kỳ 2 - 2014');

CREATE TABLE IF NOT EXISTS Dim_Course (
    course_key                 INT AUTO_INCREMENT PRIMARY KEY,
    code_module                VARCHAR(10) NOT NULL,
    code_presentation          VARCHAR(10) NOT NULL,
    module_presentation_length INT,
    UNIQUE KEY uq_course (code_module, code_presentation)
);

CREATE TABLE IF NOT EXISTS Dim_Student (
    student_key          INT AUTO_INCREMENT PRIMARY KEY,
    id_student           INT UNIQUE NOT NULL,
    gender               VARCHAR(5),
    region               VARCHAR(60),
    highest_education    VARCHAR(60),
    imd_band             VARCHAR(15),
    age_band             VARCHAR(10),
    disability           VARCHAR(5),
    num_of_prev_attempts INT,
    studied_credits      INT
);

CREATE TABLE IF NOT EXISTS Dim_Assessment (
    assessment_key  INT AUTO_INCREMENT PRIMARY KEY,
    id_assessment   INT UNIQUE NOT NULL,
    code_module     VARCHAR(10),
    assessment_type VARCHAR(10),
    weight          FLOAT,
    day_due         INT
);

CREATE TABLE IF NOT EXISTS Fact_Performance (
    fact_id         INT AUTO_INCREMENT PRIMARY KEY,
    student_key     INT        NOT NULL,
    course_key      INT        NOT NULL,
    assessment_key  INT        NOT NULL,
    time_key        INT        NOT NULL,
    score           FLOAT,
    total_clicks    BIGINT,
    final_result    VARCHAR(15),
    score_vs_avg    FLOAT,
    risk_group      VARCHAR(10),
    INDEX idx_fact_risk_group   (risk_group),
    INDEX idx_fact_final_result (final_result),
    CONSTRAINT fk_fact_student     FOREIGN KEY (student_key)     REFERENCES Dim_Student(student_key),
    CONSTRAINT fk_fact_course      FOREIGN KEY (course_key)      REFERENCES Dim_Course(course_key),
    CONSTRAINT fk_fact_assessment  FOREIGN KEY (assessment_key)  REFERENCES Dim_Assessment(assessment_key),
    CONSTRAINT fk_fact_time        FOREIGN KEY (time_key)        REFERENCES Dim_Time(time_key)
);

-- monitoring_log: composite PK (id, checked_at) cho phép RANGE partition theo checked_at.
-- AUTO_INCREMENT vẫn dùng được vì id nằm leftmost trong PK.
CREATE TABLE IF NOT EXISTS monitoring_log (
    id          INT AUTO_INCREMENT,
    check_name  VARCHAR(100) NOT NULL,
    status      VARCHAR(10)  NOT NULL,
    detail      TEXT,
    checked_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, checked_at),
    INDEX idx_mon_checked_at      (checked_at),
    INDEX idx_mon_status_checked  (status, checked_at)
)
PARTITION BY RANGE (TO_DAYS(checked_at)) (
    PARTITION p_2025      VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p_2026_q1   VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p_2026_q2   VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p_2026_q3   VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p_2026_q4   VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p_2027      VALUES LESS THAN (TO_DAYS('2028-01-01')),
    PARTITION p_max       VALUES LESS THAN MAXVALUE
);
