-- ============================================================
-- Student Data Warehouse — MySQL Init Script
-- Tự động chạy khi MySQL container khởi động lần đầu
--
-- Kiến trúc 4 lớp: Bronze (MinIO) → Silver (MinIO Parquet)
--   → DWH (MySQL Star Schema) → Mart (dbt)
-- Không có bảng stg_* — Staging layer nằm trong MinIO Silver.
-- ============================================================

CREATE DATABASE IF NOT EXISTS student_dwh
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS student_data_mart
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE student_dwh;

-- ============================================================
-- DWH LAYER — Star Schema
-- Thứ tự tạo: Dim_Time → Dim_Course → Dim_Student →
--             Dim_Assessment → Fact_Performance
-- ============================================================

CREATE TABLE IF NOT EXISTS Dim_Time (
    time_key           INT AUTO_INCREMENT PRIMARY KEY,
    code_presentation  VARCHAR(10) UNIQUE NOT NULL,
    year               INT         NOT NULL,
    semester_type      VARCHAR(5)  NOT NULL,   -- J = Jan start, B = Feb-Oct start
    presentation_label VARCHAR(20) NOT NULL
);

-- 4 bản ghi cố định — insert khi chưa tồn tại
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
    assessment_type VARCHAR(10),   -- TMA / CMA / Exam
    weight          FLOAT,
    day_due         INT            -- NULL = Exam (ngày thi cuối kỳ)
);

CREATE TABLE IF NOT EXISTS Fact_Performance (
    fact_id         INT AUTO_INCREMENT PRIMARY KEY,
    student_key     INT        NOT NULL,
    course_key      INT        NOT NULL,
    time_key        INT        NOT NULL,
    avg_score       FLOAT,
    total_clicks    BIGINT,
    num_submissions INT,
    final_result    VARCHAR(15),   -- Pass / Fail / Withdrawn / Distinction
    score_vs_avg    FLOAT,         -- avg_score - AVG cùng code_presentation
    risk_group      VARCHAR(10),   -- Low (>=70) / Medium (50-69) / High (<50)
    CONSTRAINT fk_fact_student FOREIGN KEY (student_key) REFERENCES Dim_Student(student_key),
    CONSTRAINT fk_fact_course  FOREIGN KEY (course_key)  REFERENCES Dim_Course(course_key),
    CONSTRAINT fk_fact_time    FOREIGN KEY (time_key)    REFERENCES Dim_Time(time_key)
);

-- ============================================================
-- MONITORING — ghi kết quả health checks từ DAG 05
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring_log (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    check_name  VARCHAR(100) NOT NULL,
    status      VARCHAR(10)  NOT NULL,   -- OK / WARN / FAIL
    detail      TEXT,
    checked_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);
