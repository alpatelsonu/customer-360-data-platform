-- tests duplicate user events in bronze

SELECT 
    event_id
FROM {{ ref('bronze_user_events')   }}
GROUP BY event_id
HAVING COUNT(event_id) > 1