{{ config(materialized='table') }}

SELECT
    s.gender,
    COUNT(DISTINCT s.id_student) AS total_students,
    AVG(f.avg_score) AS average_score,
    SUM(CASE WHEN f.final_result IN ('Pass', 'Distinction') THEN 1 ELSE 0 END) / COUNT(f.fact_id) * 100 AS pass_rate_percent
FROM {{ source('student_dwh', 'Fact_Performance') }} f
JOIN {{ source('student_dwh', 'Dim_Student') }} s
    ON f.student_key = s.student_key
GROUP BY 
    s.gender