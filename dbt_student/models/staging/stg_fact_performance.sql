{{ config(materialized='view') }}

SELECT
    fp.fact_id,
    fp.student_key,
    fp.course_key,
    fp.assessment_key,
    fp.time_key,
    fp.score,
    fp.total_clicks,
    fp.final_result,
    fp.score_vs_avg,
    fp.risk_group,
    ds.id_student,
    ds.gender,
    ds.region,
    ds.highest_education,
    ds.imd_band,
    ds.age_band,
    ds.disability,
    ds.num_of_prev_attempts,
    ds.studied_credits,
    
    dc.code_module,
    dc.code_presentation,
    dc.module_presentation_length,
    
    da.id_assessment,
    da.assessment_type,
    da.weight,
    da.day_due,
    
    dt.year,
    dt.semester_type,
    dt.presentation_label
    
FROM {{ source('student_dwh', 'Fact_Performance') }} fp
INNER JOIN {{ source('student_dwh', 'Dim_Student') }} ds
    ON fp.student_key = ds.student_key
INNER JOIN {{ source('student_dwh', 'Dim_Course') }} dc
    ON fp.course_key = dc.course_key
INNER JOIN {{ source('student_dwh', 'Dim_Assessment') }} da
    ON fp.assessment_key = da.assessment_key
INNER JOIN {{ source('student_dwh', 'Dim_Time') }} dt
    ON fp.time_key = dt.time_key
