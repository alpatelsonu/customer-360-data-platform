{{ config(
    materialized='view',
    tags = ["gold"]
)}}


SELECT 
    value_segment as Segment,
    AVG(is_repeat_customer) AS repeat_rate

FROM {{ ref('gold_customer_advance_metrics')}}
GROUP BY value_segment
ORDER BY value_segment
