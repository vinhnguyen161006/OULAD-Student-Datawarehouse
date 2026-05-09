{{ config(materialized='table') }}

SELECT
    gender,
    assessment_type,
    code_presentation,
    presentation_label,
    COUNT(DISTINCT id_student) as num_students,
    COUNT(*) as num_submissions,
    ROUND(AVG(score), 2) as avg_score,
    ROUND(MIN(score), 2) as min_score,
    ROUND(MAX(score), 2) as max_score,
    ROUND(STDDEV(score), 2) as stddev_score
FROM {{ ref('stg_fact_performance') }}
WHERE score IS NOT NULL
GROUP BY gender, assessment_type, code_presentation, presentation_label
ORDER BY code_presentation, gender, assessment_type
