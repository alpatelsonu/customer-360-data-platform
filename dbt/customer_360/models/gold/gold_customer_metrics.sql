{{  config(
    materialzied = 'table',
    tags = ["gold"]
)}}

WITH orders_enriched AS (
    SELECT
        i.unified_customer_id,
        o.order_id,
        o.order_amount,
        o.order_timestamp
    FROM {{ ref('bronze_orders') }} o
    LEFT JOIN {{  ref('silver_identity_current') }} i
        ON  o.user_id = i.user_id
        -- AND o.order_timestamp >= i.dbt_valid_from AND o.order_timestamp < COALESCE(i.dbt_valid_to, '9999-12-31')
)

SELECT 
    unified_customer_id,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_revenue,
    MIN(order_timestamp) AS first_order,
    MAX(order_timestamp) AS last_order,
    DATEDIFF('DAY', MIN(order_timestamp) , MAX(order_timestamp)) AS customer_lifecycle_days,
    DATEDIFF('DAY', MAX(order_timestamp), current_timestamp) AS days_since_last_order
FROM orders_enriched
GROUP BY unified_customer_id
ORDER BY unified_customer_id

