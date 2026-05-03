{{ config(
    materialized = 'table',
    tags = ["downstream"]

)}}

WITH events AS (
    SELECT 
        *
    FROM {{ ref('bronze_user_events') }}
),

identity AS (
    SELECT 
        *
    FROM {{ ref('silver_identity_current') }}
)

SELECT 
    e.*,
    i.unified_customer_id
FROM events e 
LEFT JOIN identity i 
    ON e.user_id = i.user_id