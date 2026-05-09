{{ config(materialized='table') }}

WITH engagement_buckets AS (
    SELECT
        f.student_key,
        f.avg_score,
        f.total_clicks,
        f.final_result,
        -- Chia nhóm tương tác dựa trên số click
        CASE
            WHEN f.total_clicks IS NULL OR f.total_clicks < 500 THEN 'Low (0 - 500)'
            WHEN f.total_clicks >= 500 AND f.total_clicks < 2000 THEN 'Medium (500 - 2000)'
            ELSE 'High (2000+)'
        END AS engagement_level
    FROM {{ source('student_dwh', 'Fact_Performance') }} f
)

SELECT
    engagement_level,
    COUNT(*) AS total_records,
    AVG(avg_score) AS average_score,
    SUM(CASE WHEN final_result IN ('Pass', 'Distinction') THEN 1 ELSE 0 END) / COUNT(*) * 100 AS pass_rate_percent
FROM engagement_buckets
GROUP BY 
    engagement_level
ORDER BY
    CASE engagement_level
        WHEN 'Low (0 - 500)' THEN 1
        WHEN 'Medium (500 - 2000)' THEN 2
        ELSE 3
    END