{{ config(materialized='table') }}

-- Mục đích: Liệt kê các sinh viên thuộc nhóm 'High' risk để có biện pháp can thiệp sớm
SELECT
    s.id_student,
    s.gender,
    s.region,
    c.code_module,
    t.code_presentation,
    f.avg_score,
    f.total_clicks,
    f.num_submissions,
    f.risk_group
FROM {{ source('student_dwh', 'Fact_Performance') }} f
JOIN {{ source('student_dwh', 'Dim_Student') }} s 
    ON f.student_key = s.student_key
JOIN {{ source('student_dwh', 'Dim_Course') }} c 
    ON f.course_key = c.course_key
JOIN {{ source('student_dwh', 'Dim_Time') }} t 
    ON f.time_key = t.time_key
WHERE f.risk_group = 'High'