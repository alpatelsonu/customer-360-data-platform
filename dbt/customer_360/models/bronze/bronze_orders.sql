{{  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    tags = ["upsteam"]

)}}

WITH source AS (
    SELECT 
        order_id,
        user_id,
        order_amount,
        order_status,
        order_timestamp,
        order_priority,
        ingestion_timestamp,
        source_file
    FROM {{ source('raw', 'raw_orders')}}
),

typed AS (
    SELECT 
        CAST(order_id AS STRING) AS order_id,
        CAST(user_id AS STRING) AS user_id,
        CAST(order_amount AS INTEGER) AS order_amount,
        TRIM(order_status) AS order_status,
        CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
        CAST(TRIM(order_priority) AS STRING) AS order_priority,
        CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
        CAST(source_file AS STRING) AS source_file
    FROM source
),

deduplicated AS (
    SELECT * FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY ingestion_timestamp desc) AS rnk
        FROM typed 
        WHERE order_id IS NOT NULL
    ) WHERE rnk = 1
)

SELECT 
    order_id,
    user_id,
    order_amount,
    order_status,
    order_timestamp,
    order_priority,
    ingestion_timestamp,
    source_file
FROM deduplicated

{% if is_incremental() %}
WHERE ingestion_timestamp > (
    SELECT 
        COALESCE(MAX(ingestion_timestamp), '1990-01-01')
    FROM {{ this }}
)
{% endif %}