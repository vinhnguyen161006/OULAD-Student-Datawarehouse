{{ config(materialized='table') }}

-- Mục đích: Tổng hợp điểm trung bình và số lượng sinh viên theo từng môn học và học kỳ
SELECT
    c.code_module,
    t.code_presentation,
    f.final_result,
    COUNT(f.student_key) as total_students,
    AVG(f.avg_score) as average_score,
    SUM(f.total_clicks) as total_vle_interactions
FROM {{ source('student_dwh', 'Fact_Performance') }} f
JOIN {{ source('student_dwh', 'Dim_Course') }} c 
    ON f.course_key = c.course_key
JOIN {{ source('student_dwh', 'Dim_Time') }} t 
    ON f.time_key = t.time_key
GROUP BY 
    c.code_module,
    t.code_presentation,
    f.final_result