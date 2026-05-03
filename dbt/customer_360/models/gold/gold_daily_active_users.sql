{{  config(
    materialized = 'table',
    tags = ["gold"]
)}}

WITH events AS (
    SELECT
        e.user_id,
        e.event_timestamp,
        e.unified_customer_id
    FROM {{ ref('silver_user_events_pit')}} e
)

SELECT 
    DATE_TRUNC('DAY', event_timestamp) AS event_date,
    COUNT(DISTINCT unified_customer_id) AS dau_unique,
    COUNT(DISTINCT user_id) AS dau
FROM events
GROUP BY event_date
ORDER BY event_date