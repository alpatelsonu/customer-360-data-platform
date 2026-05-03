-- verify if same user is mapped to two different identities A(old_user_id) -> B and A(old_user_id) -> C

SELECT 
    old_user_id
FROM {{ ref('bronze_identity_map') }}
GROUP BY old_user_id
HAVING COUNT(DISTINCT new_user_id) > 1