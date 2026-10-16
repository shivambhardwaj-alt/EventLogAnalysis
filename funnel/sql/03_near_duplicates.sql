

UPDATE clean_events
SET is_near_duplicate = 0
WHERE user_id IN (SELECT user_id FROM _affected_users);

WITH ordered AS (
    SELECT event_id,
           ts_epoch - LAG(ts_epoch) OVER (
               PARTITION BY user_id, event_name, COALESCE(product_id, '')
               ORDER BY ts_epoch, raw_id
           ) AS seconds_since_same_action
    FROM clean_events
    WHERE user_id IN (SELECT user_id FROM _affected_users)
)
UPDATE clean_events
SET is_near_duplicate = 1
WHERE event_id IN (
    SELECT event_id FROM ordered
    WHERE seconds_since_same_action <= :near_dup_window_s
);
