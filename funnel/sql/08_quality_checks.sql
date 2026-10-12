-- =====================================================================
-- Data-quality assertions (dbt-style tests).  Each named query returns a
-- single number: the count of violating rows.  0 = pass.
-- They run after every pipeline run and are shown in the report.
-- =====================================================================

-- name: every_raw_row_accounted_for
-- each raw row is in exactly one of: clean / duplicate / quarantine
SELECT COUNT(*) FROM raw_events r
WHERE (  (SELECT COUNT(*) FROM clean_events      c WHERE c.raw_id = r.raw_id)
       + (SELECT COUNT(*) FROM duplicate_events  d WHERE d.raw_id = r.raw_id)
       + (SELECT COUNT(*) FROM quarantine_events q WHERE q.raw_id = r.raw_id)) <> 1;

-- name: no_null_required_fields
SELECT COUNT(*) FROM clean_events
WHERE user_id IS NULL OR event_name IS NULL OR ts_epoch IS NULL OR event_ts IS NULL;

-- name: analysable_events_have_session
SELECT COUNT(*) FROM clean_events
WHERE is_near_duplicate = 0 AND session_id IS NULL;

-- name: near_duplicates_have_no_session
SELECT COUNT(*) FROM clean_events
WHERE is_near_duplicate = 1 AND session_id IS NOT NULL;

-- name: session_event_counts_match
SELECT COUNT(*) FROM sessions s
WHERE s.event_count <> (SELECT COUNT(*) FROM clean_events c WHERE c.session_id = s.session_id);

-- name: no_gap_inside_session_exceeds_threshold
-- Params: :gap_seconds
SELECT COUNT(*) FROM (
    SELECT ts_epoch - LAG(ts_epoch) OVER (PARTITION BY session_id ORDER BY ts_epoch, raw_id) AS gap
    FROM clean_events WHERE session_id IS NOT NULL
) WHERE gap > :gap_seconds;

-- name: sessions_do_not_overlap
SELECT COUNT(*) FROM (
    SELECT start_epoch,
           LAG(start_epoch + duration_seconds) OVER (PARTITION BY user_id ORDER BY start_epoch) AS prev_end
    FROM sessions
) WHERE start_epoch <= prev_end;

-- name: funnel_is_monotonic
SELECT COUNT(*) FROM funnel_metrics
WHERE prev_sessions IS NOT NULL AND sessions_reached > prev_sessions;

-- name: funnel_steps_reached_in_order
SELECT COUNT(*) FROM funnel_session_steps cur
LEFT JOIN funnel_session_steps prv
  ON prv.session_id = cur.session_id AND prv.step_index = cur.step_index - 1
WHERE cur.step_index > 1
  AND (prv.session_id IS NULL OR cur.reached_epoch < prv.reached_epoch);

-- name: event_timestamps_in_valid_range
-- Params: :min_epoch
SELECT COUNT(*) FROM clean_events WHERE ts_epoch < :min_epoch;
