{{ config(materialized='table') }}

SELECT
    highest_education,
    assessment_type,
    code_presentation,
    presentation_label,
    COUNT(DISTINCT id_student) as num_students,
    COUNT(*) as num_submissions,
    ROUND(AVG(score), 2) as avg_score,
    ROUND(AVG(CASE WHEN final_result = 'Pass' THEN 1 ELSE 0 END) * 100, 2) as pass_rate_pct,
    ROUND(AVG(CASE WHEN final_result = 'Distinction' THEN 1 ELSE 0 END) * 100, 2) as distinction_rate_pct,
    ROUND(AVG(CASE WHEN final_result = 'Withdrawn' THEN 1 ELSE 0 END) * 100, 2) as withdrawn_rate_pct
FROM {{ ref('stg_fact_performance') }}
GROUP BY highest_education, assessment_type, code_presentation, presentation_label
ORDER BY code_presentation, highest_education, assessment_type
