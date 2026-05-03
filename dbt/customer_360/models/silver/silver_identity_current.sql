{{ config(materialized='table',
    tags = ["upsteam"]

) }}
-- rebuild entire chain from bronze
WITH RECURSIVE identity_chain AS (

    -- Step 1: direct edges
    SELECT
        old_user_id,
        new_user_id,
        effective_from
    FROM {{ ref('bronze_identity_map') }}

    UNION ALL

    -- Step 2: recursive expansion (multi-hop)
    SELECT
        ic.old_user_id,
        im.new_user_id,
        im.effective_from
    FROM identity_chain ic
    JOIN {{ ref('bronze_identity_map') }} im
        ON ic.new_user_id = im.old_user_id

),

-- Step 3: pick latest reachable node
final_mapping AS (

    SELECT
        old_user_id AS user_id,
        new_user_id AS unified_customer_id,
        effective_from,
        ROW_NUMBER() OVER (
            PARTITION BY old_user_id
            ORDER BY effective_from DESC   -- replace with effective_from if available
        ) AS rn
    FROM identity_chain

),

-- Step 4: keep only final mapping
resolved_old_ids AS (

    SELECT
        user_id,
        unified_customer_id,
        effective_from
    FROM final_mapping
    WHERE rn = 1

),

-- Step 5: terminal nodes map to themselves
terminal_nodes AS (

    SELECT DISTINCT
        im.new_user_id AS user_id,
        im.new_user_id AS unified_customer_id,
        im.effective_from
    FROM {{ ref('bronze_identity_map') }} im
    LEFT JOIN {{ ref('bronze_identity_map') }} im2
        ON im.new_user_id = im2.old_user_id
    WHERE im2.old_user_id IS NULL

),

-- Step 6: include users with no swaps
base_users AS (

    SELECT CAST(user_id AS STRING) AS user_id
    FROM {{ source('raw', 'customer_base') }}

    UNION
    -- simulated new_user_id without adding in base customer , In real base customer is all universe of customers 
    SELECT new_user_id AS user_id 
    FROM {{ ref('bronze_identity_map')}}

),

final_identity AS (

    SELECT
        u.user_id,
        COALESCE(m.unified_customer_id, u.user_id) AS unified_customer_id,
        COALESCE(m.effective_from, CAST('2024-01-01' AS TIMESTAMP)) as effective_from 
    FROM base_users u
    LEFT JOIN (

        -- union of resolved mappings
        SELECT * FROM resolved_old_ids
        UNION
        SELECT * FROM terminal_nodes

    ) m
    ON u.user_id = m.user_id

)

SELECT * FROM final_identity