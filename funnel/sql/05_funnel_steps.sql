-- =====================================================================
-- STAGE 5: strict, ordered, per-session funnel.
-- This file holds three named blocks; the Python orchestrator runs
-- `reset` once, `first_step` once, then `next_step` for every further
-- step in the configured funnel (so the funnel is data, not code).
-- A session reaches step k only if it reached step k-1 first and the
-- step-k event happens at or after that moment, in the SAME session.
-- =====================================================================

-- name: reset
DELETE FROM funnel_session_steps
WHERE user_id IN (SELECT user_id FROM _affected_users);

-- name: first_step
-- Params: :step_index, :step_name
INSERT INTO funnel_session_steps (session_id, user_id, step_index, step_name, reached_epoch)
SELECT e.session_id, e.user_id, :step_index, :step_name, MIN(e.ts_epoch)
FROM clean_events AS e
WHERE e.user_id IN (SELECT user_id FROM _affected_users)
  AND e.session_id IS NOT NULL
  AND e.event_name = :step_name
GROUP BY e.session_id, e.user_id;

-- name: next_step
-- Params: :step_index, :step_name
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
