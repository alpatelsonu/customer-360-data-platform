{{ config(
    materialized = 'view',
    tags = ["gold"]

)}}

SELECT
    cohort_month,
    month_num,
    retention_percent
FROM {{ ref('gold_cohort_retention')}}
ORDER BY cohort_month, month_num