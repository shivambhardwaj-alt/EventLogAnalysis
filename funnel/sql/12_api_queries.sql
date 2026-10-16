WITH s AS (
    SELECT session_id
    FROM sessions
    WHERE (:device      IS NULL OR device       = :device)
      AND (:channel     IS NULL OR channel      = :channel)
      AND (:country     IS NULL OR country      = :country)
      AND (:app_version IS NULL OR app_version  = :app_version)
      AND (:date_from   IS NULL OR session_date >= :date_from)
      AND (:date_to     IS NULL OR session_date <= :date_to)
),
reached AS (
    SELECT f.step_index, COUNT(*) AS n
    FROM funnel_session_steps AS f
    JOIN s ON s.session_id = f.session_id
    GROUP BY f.step_index
),
b AS (
    SELECT d.step_index, d.step_name,
           COALESCE(r.n, 0) AS n,
           LAG(COALESCE(r.n, 0))         OVER (ORDER BY d.step_index) AS prev_n,
           FIRST_VALUE(COALESCE(r.n, 0)) OVER (ORDER BY d.step_index) AS first_n
    FROM dim_funnel_steps AS d
    LEFT JOIN reached AS r ON r.step_index = d.step_index
)
SELECT step_index, step_name, n AS sessions_reached, prev_n AS prev_sessions,
       CASE WHEN prev_n  > 0 THEN ROUND(1.0 * n / prev_n,  6) END AS step_conversion,
       CASE WHEN first_n > 0 THEN ROUND(1.0 * n / first_n, 6) END AS overall_conversion,
       (SELECT COUNT(*) FROM s) AS sessions_in_filter
FROM b
ORDER BY step_index;

SELECT c.session_id, c.event_ts, c.event_name, c.device, c.app_version, c.product_id, c.price,
       c.is_near_duplicate
FROM clean_events AS c
WHERE c.user_id = :user_id
ORDER BY c.ts_epoch, c.raw_id
LIMIT 500;