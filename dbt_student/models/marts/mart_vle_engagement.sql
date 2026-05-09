{{ config(materialized='table') }}

SELECT
    risk_group,
    code_presentation,
    presentation_label,
    COUNT(DISTINCT id_student) as num_students,
    COUNT(*) as num_submissions,
    ROUND(AVG(total_clicks), 2) as avg_total_clicks,
    ROUND(AVG(score), 2) as avg_score,
    ROUND(STDDEV(total_clicks), 2) as stddev_clicks,
    ROUND(
        (AVG(total_clicks * score) - AVG(total_clicks) * AVG(score))
        / NULLIF(STDDEV_POP(total_clicks) * STDDEV_POP(score), 0),
        3
    ) as corr_clicks_score
FROM {{ ref('stg_fact_performance') }}
WHERE score IS NOT NULL
GROUP BY risk_group, code_presentation, presentation_label
ORDER BY code_presentation,
         CASE WHEN risk_group = 'Low' THEN 1 WHEN risk_group = 'Medium' THEN 2 ELSE 3 END
