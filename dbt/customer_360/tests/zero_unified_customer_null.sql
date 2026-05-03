SELECT *
FROM {{ ref('silver_user_events_pit') }}
WHERE unified_customer_id IS NULL;