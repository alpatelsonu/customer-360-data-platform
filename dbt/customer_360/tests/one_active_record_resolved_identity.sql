SELECT 
    user_id
FROM {{ ref('silver_identity_resolved')}}
WHERE dbt_valid_to IS NULL
GROUP BY user_id
HAVING COUNT(user_id) > 1