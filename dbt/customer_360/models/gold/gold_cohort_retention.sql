--“Cohort analysis groups users by their initial interaction period and tracks their behavior over time to measure retention and engagement.”

{{  config(
    materialized = 'table',
    tags = ["gold"]
)}}

WITH events AS (

    SELECT 
        unified_customer_id,
        user_id,
        DATE_TRUNC('MONTH', event_timestamp) AS activity_month
    FROM {{ ref('silver_user_events_pit') }}
),

first_activity AS (
    
    SELECT 
        unified_customer_id,
        MIN(activity_month) AS cohort_month
    FROM events
    GROUP BY unified_customer_id
),

cohort_activity AS (

    SELECT 
        e.unified_customer_id,
        f.cohort_month,
        e.activity_month,
        DATEDIFF('MONTH', cohort_month, activity_month) AS month_num
    FROM events e 
    INNER JOIN first_activity f
        ON e.unified_customer_id = f.unified_customer_id
),

cohort_size_agg AS (

    SELECT
        cohort_month,
        COUNT(DISTINCT unified_customer_id) AS cohort_size
    FROM cohort_activity
    GROUP BY cohort_month
),

cohort_retention AS (

    SELECT 
        cohort_month,
        month_num,
        COUNT(DISTINCT unified_customer_id) as active_user_count
    FROM cohort_activity 
    GROUP BY cohort_month, month_num
),

cohort_retention_details AS (

    SELECT 
        cr.cohort_month,
        cr.month_num,
        cr.active_user_count,
        cs.cohort_size,
        ROUND(  1.0 * cr.active_user_count /  cs.cohort_size , 3) as retention_percent
    FROM cohort_retention cr
    INNER JOIN cohort_size_agg cs 
        ON cr.cohort_month = cs.cohort_month
)

SELECT 
    *
FROM cohort_retention_details
ORDER BY cohort_month, month_num


