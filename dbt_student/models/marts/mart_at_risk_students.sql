{{ config(materialized='table') }}

SELECT
    id_student,
    gender,
    region,
    highest_education,
    age_band,
    disability,
    code_module,
    code_presentation,
    presentation_label,
    COUNT(*) as num_high_risk_submissions,
    ROUND(AVG(score), 2) as avg_score,
    ROUND(AVG(total_clicks), 2) as avg_total_clicks,
    MAX(final_result) as final_result
FROM {{ ref('stg_fact_performance') }}
WHERE risk_group = 'High'
GROUP BY id_student, gender, region, highest_education, age_band, disability, 
         code_module, code_presentation, presentation_label
ORDER BY id_student, code_presentation
