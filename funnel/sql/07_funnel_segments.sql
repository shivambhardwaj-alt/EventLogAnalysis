-- =====================================================================
-- STAGE 7: funnel per segment (device / channel / country ...).
-- {dimension} is substituted by Python from a strict allow-list of
-- column names (identifiers cannot be bound as SQL parameters); every
-- value is still passed as a bound parameter.
-- Params: :dimension
-- =====================================================================

INSERT INTO funnel_segments (
    dimension, segment_value, step_index, step_name, sessions_reached, prev_sessions, step_conversion
)
WITH seg AS (
    SELECT s.session_id, s.{dimension} AS v
    FROM sessions AS s
),
reached AS (
    SELECT seg.v, f.step_index, COUNT(*) AS n
    FROM funnel_session_steps AS f
    JOIN seg ON seg.session_id = f.session_id
    GROUP BY seg.v, f.step_index
),
grid AS (
    SELECT vals.v, d.step_index, d.step_name
    FROM (SELECT DISTINCT v FROM reached) AS vals
    CROSS JOIN dim_funnel_steps AS d
),
b AS (
    SELECT g.v, g.step_index, g.step_name,
           COALESCE(r.n, 0) AS n,
           LAG(COALESCE(r.n, 0)) OVER (PARTITION BY g.v ORDER BY g.step_index) AS prev_n
    FROM grid AS g
    LEFT JOIN reached AS r ON r.v = g.v AND r.step_index = g.step_index
)
SELECT :dimension, v, step_index, step_name, n, prev_n,
       CASE WHEN prev_n > 0 THEN ROUND(1.0 * n / prev_n, 6) END
FROM b;
