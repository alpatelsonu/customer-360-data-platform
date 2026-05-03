-- late data awareness, history is preserved in raw , bronze is latest per business key
{{  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge',
    tags = ["upsteam"]

) }}

WITH source AS (
    SELECT 
        event_id,
        event_type,
        user_id,
        device_id,
        session_id,
        product_id,
        event_timestamp,
        ingestion_timestamp,
        source_file
    FROM {{ source('raw', 'raw_user_events')    }}
),

typed AS (

    SELECT
        CAST(event_id AS STRING) AS event_id,
        LOWER(TRIM(event_type)) AS event_type,
        CAST(user_id AS STRING) AS user_id,
        CAST(device_id AS STRING) AS device_id,
        CAST(session_id AS STRING) AS session_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
        CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
        source_file
    FROM source
),

deduplicated AS (

    SELECT * FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY ingestion_timestamp desc) AS rnk
        FROM typed
        WHERE event_id IS NOT NULL
    )
    WHERE rnk=1
)

SELECT 
    event_id,
    event_type,
    user_id,
    device_id,
    session_id,
    product_id,
    event_timestamp,
    ingestion_timestamp,
    source_file
FROM deduplicated

{%  if is_incremental() %}
WHERE ingestion_timestamp > (
    SELECT 
    COALESCE(MAX(ingestion_timestamp), '1990-01-01')    
    FROM {{ this }}
)
{%  endif   %}