

DELETE FROM daily_funnel;

INSERT INTO daily_funnel (session_date, step_index, step_name, sessions_reached, prev_sessions, step_conversion)
WITH reached AS (
    SELECT s.session_date, f.step_index, COUNT(*) AS n
    FROM funnel_session_steps AS f
    JOIN sessions             AS s ON s.session_id = f.session_id
    GROUP BY s.session_date, f.step_index
),
days AS (
    SELECT DISTINCT session_date FROM reached
),
grid AS (
    SELECT days.session_date, d.step_index, d.step_name
    FROM days CROSS JOIN dim_funnel_steps AS d
),
b AS (
    SELECT g.session_date, g.step_index, g.step_name,
           COALESCE(r.n, 0) AS n,
           LAG(COALESCE(r.n, 0)) OVER (PARTITION BY g.session_date ORDER BY g.step_index) AS prev_n
    FROM grid AS g
    LEFT JOIN reached AS r ON r.session_date = g.session_date AND r.step_index = g.step_index
)
SELECT session_date, step_index, step_name, n, prev_n,
       CASE WHEN prev_n > 0 THEN ROUND(1.0 * n / prev_n, 6) END
FROM b;
