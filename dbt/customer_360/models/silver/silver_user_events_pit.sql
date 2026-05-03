{{ config(
    materialized = 'table',
    tags = ["downstream"]

)}}

WITH pit_identity AS (
    SELECT 
        user_id,
        unified_customer_id,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to
    FROM {{ ref('silver_identity_resolved') }}
),

latest_identity AS (
    SELECT *
    FROM {{ ref('silver_identity_current') }}
)

SELECT

    e.*,
    COALESCE(
        pit.unified_customer_id,
        latest.unified_customer_id
    ) AS unified_customer_id
FROM {{ ref('bronze_user_events') }} e
LEFT JOIN pit_identity pit
    ON e.user_id = pit.user_id
   AND e.event_timestamp >= pit.valid_from
   AND e.event_timestamp < COALESCE(pit.valid_to, '9999-12-31')
LEFT JOIN latest_identity latest
    ON e.user_id = latest.user_id