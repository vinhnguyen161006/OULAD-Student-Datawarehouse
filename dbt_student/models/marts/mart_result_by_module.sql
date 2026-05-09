{{ config(materialized='table') }}

SELECT
    code_module,
    code_presentation,
    presentation_label,
    final_result,
    COUNT(DISTINCT id_student) as num_students,
    COUNT(*) as num_submissions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY code_module, code_presentation), 2) as pct_submissions
FROM {{ ref('stg_fact_performance') }}
GROUP BY code_module, code_presentation, presentation_label, final_result
ORDER BY code_module, code_presentation, final_result
