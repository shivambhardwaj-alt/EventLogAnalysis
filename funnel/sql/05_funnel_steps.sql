-- name: reset
DELETE FROM funnel_session_steps
WHERE user_id IN (SELECT user_id FROM _affected_users);

-- name: first_step
INSERT INTO funnel_session_steps (session_id, user_id, step_index, step_name, reached_epoch)
SELECT e.session_id, e.user_id, :step_index, :step_name, MIN(e.ts_epoch)
FROM clean_events AS e
WHERE e.user_id IN (SELECT user_id FROM _affected_users)
  AND e.session_id IS NOT NULL
  AND e.event_name = :step_name
GROUP BY e.session_id, e.user_id;

-- name: next_step
INSERT INTO funnel_session_steps (session_id, user_id, step_index, step_name, reached_epoch)
SELECT e.session_id, e.user_id, :step_index, :step_name, MIN(e.ts_epoch)
FROM clean_events         AS e
JOIN funnel_session_steps AS prev
  ON prev.session_id = e.session_id
 AND prev.step_index = :step_index - 1
WHERE e.user_id IN (SELECT user_id FROM _affected_users)
  AND e.event_name = :step_name
  AND e.ts_epoch >= prev.reached_epoch
GROUP BY e.session_id, e.user_id;