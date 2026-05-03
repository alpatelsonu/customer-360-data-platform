{% snapshot silver_identity_resolved %}

{{
  config(
    target_schema='SILVER',
    unique_key='user_id',
    strategy='timestamp',
    updated_at='effective_from',
    invalidate_hard_deletes=False 
  )
}}

SELECT
    user_id,
    unified_customer_id,
    effective_from
FROM {{ ref('silver_identity_current') }}

{% endsnapshot %}



--  uses current timestamp to set dbt_valid_from/to, uses unified_customer_id to compare
-- {{
--   config(
--     target_schema='SILVER',
--     unique_key='user_id',
--     strategy='check',
--     check_cols=['unified_customer_id'],
--     invalidate_hard_deletes=False 
--   )
-- }}