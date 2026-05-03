{{ config(
    materialized = 'table',
    tags = ["gold"]
)}}


SELECT 
    cohort_month,
    "0" AS month_0,
    "1" AS month_1,
    "2" AS month_2,
    "3" AS month_3,
    "4" AS month_4
FROM (
        SELECT 
            *
        FROM {{ ref('gold_cohort_retention')}}
        PIVOT (
            MAX(retention_percent)
            FOR month_num in (0,1,2,3,4)
        )
)
ORDER BY cohort_month


-- SELECT
--     cohort_month,
--     MAX(CASE WHEN month_number = 0 THEN retention_pct END) AS month_0,
--     MAX(CASE WHEN month_number = 1 THEN retention_pct END) AS month_1,
--     MAX(CASE WHEN month_number = 2 THEN retention_pct END) AS month_2
-- FROM gold_cohort_retention
-- GROUP BY cohort_month