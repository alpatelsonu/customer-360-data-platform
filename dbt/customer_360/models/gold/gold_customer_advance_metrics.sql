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
),

customer_agg AS (
SELECT 
    unified_customer_id,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_revenue,
    MIN(order_timestamp) AS first_order,
    MAX(order_timestamp) AS last_order,
FROM orders_enriched
GROUP BY unified_customer_id
),

final AS (
    SELECT 
        unified_customer_id,
        total_orders,
        total_revenue,
        --AOV
        ROUND(total_revenue/NULLIF(total_orders,0), 2) AS avg_order_value,
        --Lifetime
        DATEDIFF('DAY', first_order, last_order) AS customer_lifetime_in_days,
        -- REPEAT CUSTOMER
        CASE WHEN total_orders > 1 then 1
            ELSE 0
        END AS is_repeat_customer

    FROM customer_agg
),

segment AS (
    SELECT
        *,
        CASE 
            WHEN total_revenue > 1000 THEN 'High Value'
            WHEN total_revenue >= 500 THEN 'Mid Value'
            ELSE 'Low Value'
        END AS value_segment,

        CASE
            WHEN total_orders = 1 THEN 'One Timer'
            WHEN total_orders <= 3 THEN 'Repeat'
            ELSE 'Loyal'
        END AS frequency_segment
    FROM final
)


SELECT 
    *
FROM segment

