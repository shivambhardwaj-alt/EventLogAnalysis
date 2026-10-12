-- =====================================================================
-- STAGE 3: near-duplicate detection (double clicks / double submits).
-- Same user + same event + same product within :near_dup_window_s seconds
-- of the previous identical action => flagged, kept for audit, but
-- excluded from sessions and funnels.
-- Recomputed for affected users only; a late event can change who is
-- "previous", so flags are reset first.
-- Params: :near_dup_window_s
-- =====================================================================

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
