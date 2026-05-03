{{
    config(
        materialized = 'incremental',
        tags = ["upsteam"]
    )
}}

WITH source AS (
    SELECT
        old_user_id,
        new_user_id,
        effective_from,
        ingestion_timestamp,
        source_file
    FROM {{ source('raw','raw_identity_map')}}
),

typed AS (
    SELECT 
        CAST(old_user_id AS STRING) AS old_user_id,
        CAST(new_user_id AS STRING) AS new_user_id,
        CAST(effective_from AS TIMESTAMP) AS effective_from,
        CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
        CAST(source_file AS STRING) AS source_file
    FROM source
),

deduplicated AS (
    SELECT * FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY old_user_id, new_user_id ORDER BY ingestion_timestamp desc) AS rnk
        FROM typed
        WHERE old_user_id IS NOT NULL AND new_user_id IS NOT NULL
    )
)

SELECT
    old_user_id,
    new_user_id,
    effective_from,
    ingestion_timestamp,
    source_file
FROM deduplicated

{%  if is_incremental() %}
WHERE ingestion_timestamp > (
    SELECT
        COALESCE(MAX(ingestion_timestamp), '1990-01-01')
    FROM {{ this }}
)

{% endif %}