

DELETE FROM funnel_metrics;

INSERT INTO funnel_metrics (
    step_index, step_name, sessions_reached, prev_sessions, step_conversion,
    overall_conversion, drop_off_sessions, drop_off_rate, median_seconds_from_prev
)
WITH reached AS (
    SELECT step_index, COUNT(*) AS n
    FROM funnel_session_steps
    GROUP BY step_index
),
base AS (
    SELECT d.step_index,
           d.step_name,
           COALESCE(r.n, 0)                                                 AS n,
           LAG(COALESCE(r.n, 0))         OVER (ORDER BY d.step_index)        AS prev_n,
           FIRST_VALUE(COALESCE(r.n, 0)) OVER (ORDER BY d.step_index)        AS first_n
    FROM dim_funnel_steps AS d
    LEFT JOIN reached     AS r ON r.step_index = d.step_index
),
deltas AS (
    SELECT cur.step_index, cur.reached_epoch - prv.reached_epoch AS dt
    FROM funnel_session_steps AS cur
    JOIN funnel_session_steps AS prv
      ON prv.session_id = cur.session_id
     AND prv.step_index = cur.step_index - 1
),
ranked AS (
    SELECT step_index, dt,
           ROW_NUMBER() OVER (PARTITION BY step_index ORDER BY dt) AS rn,
           COUNT(*)     OVER (PARTITION BY step_index)             AS cnt
    FROM deltas
),
medians AS (
    SELECT step_index, AVG(dt) AS median_s
    FROM ranked
    WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
    GROUP BY step_index
)
SELECT b.step_index,
       b.step_name,
       b.n,
       b.prev_n,
       CASE WHEN b.prev_n > 0  THEN ROUND(1.0 * b.n / b.prev_n, 6)  END,
       CASE WHEN b.first_n > 0 THEN ROUND(1.0 * b.n / b.first_n, 6) END,
       CASE WHEN b.prev_n IS NOT NULL THEN b.prev_n - b.n END,
       CASE WHEN b.prev_n > 0  THEN ROUND(1.0 - 1.0 * b.n / b.prev_n, 6) END,
       ROUND(m.median_s, 3)
FROM base         AS b
LEFT JOIN medians AS m ON m.step_index = b.step_index
ORDER BY b.step_index;
