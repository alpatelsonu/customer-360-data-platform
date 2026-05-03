-- ensure no two same users swapped with each other A -> A 

SELECT 
    *
FROM {{ ref('bronze_identity_map') }}
WHERE old_user_id = new_user_id