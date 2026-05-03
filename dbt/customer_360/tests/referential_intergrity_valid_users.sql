-- events customer must be long to customer base or new swaps
SELECT e.user_id
FROM {{ ref('bronze_user_events') }} e
LEFT JOIN {{ source('raw', 'customer_base') }} c
  ON e.user_id = c.user_id
LEFT JOIN {{ ref('bronze_identity_map') }} i
  ON e.user_id = i.new_user_id
WHERE c.user_id IS NULL
  AND i.new_user_id IS NULL


-- SELECT e.*
-- FROM {{ ref('bronze_user_events') }} e
-- LEFT JOIN {{ ref('bronze_identity_map') }} i_old
--   ON e.user_id = i_old.old_user_id
-- LEFT JOIN {{ ref('bronze_identity_map') }} i_new
--   ON e.user_id = i_new.new_user_id

-- WHERE NOT (
--     -- Case 1: old_user_id valid before effective date
--     (i_old.old_user_id IS NOT NULL AND e.event_timestamp < i_old.effective_from)

--     OR

--     -- Case 2: new_user_id valid after effective date
--     (i_new.new_user_id IS NOT NULL AND e.event_timestamp >= i_new.effective_from)

--     OR

--     -- Case 3: user with no identity change
--     (i_old.old_user_id IS NULL AND i_new.new_user_id IS NULL)
-- )