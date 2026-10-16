

DROP TABLE IF EXISTS temp._session_assign;

CREATE TEMP TABLE _session_assign AS
WITH events AS (
    SELECT event_id, raw_id, user_id, event_name, ts_epoch, device, country, channel, app_version
    FROM clean_events
    WHERE is_near_duplicate = 0
      AND user_id IN (SELECT user_id FROM _affected_users)
),
gaps AS (
    SELECT events.*,
           ts_epoch - LAG(ts_epoch) OVER (PARTITION BY user_id ORDER BY ts_epoch, raw_id) AS gap_s
    FROM events
),
flagged AS (
    SELECT gaps.*,
           CASE WHEN gap_s IS NULL OR gap_s > :gap_seconds THEN 1 ELSE 0 END AS is_new_session
    FROM gaps
),
numbered AS (
    SELECT flagged.*,
           SUM(is_new_session) OVER (
               PARTITION BY user_id ORDER BY ts_epoch, raw_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS session_seq
    FROM flagged
)
SELECT numbered.*,
       user_id || '#' || CAST(CAST(ROUND(MIN(ts_epoch) OVER (PARTITION BY user_id, session_seq) * 1000) AS INTEGER) AS TEXT)
           AS session_id,
       ROW_NUMBER() OVER (PARTITION BY user_id, session_seq ORDER BY ts_epoch, raw_id)       AS pos,
       COUNT(*)     OVER (PARTITION BY user_id, session_seq)                                AS n_events
FROM numbered;

CREATE INDEX temp.ix_session_assign ON _session_assign (event_id);

UPDATE clean_events
SET session_id = NULL
WHERE user_id IN (SELECT user_id FROM _affected_users);

UPDATE clean_events
SET session_id = a.session_id
FROM _session_assign AS a
WHERE a.event_id = clean_events.event_id;

DELETE FROM sessions
WHERE user_id IN (SELECT user_id FROM _affected_users);

INSERT INTO sessions (
    session_id, user_id, session_seq, session_start, session_end, start_epoch,
    duration_seconds, event_count, device, country, channel, app_version, session_date,
    entry_event, exit_event
)
SELECT session_id,
       user_id,
       session_seq,
       strftime('%Y-%m-%dT%H:%M:%fZ', MIN(ts_epoch), 'unixepoch'),
       strftime('%Y-%m-%dT%H:%M:%fZ', MAX(ts_epoch), 'unixepoch'),
       MIN(ts_epoch),
       ROUND(MAX(ts_epoch) - MIN(ts_epoch), 3),
       COUNT(*),
       MAX(CASE WHEN pos = 1        THEN device     END),
       MAX(CASE WHEN pos = 1        THEN country    END),
       MAX(CASE WHEN pos = 1        THEN channel    END),
       MAX(CASE WHEN pos = 1        THEN app_version END),
       date(MIN(ts_epoch), 'unixepoch'),
       MAX(CASE WHEN pos = 1        THEN event_name END),
       MAX(CASE WHEN pos = n_events THEN event_name END)
FROM _session_assign
GROUP BY session_id, user_id, session_seq;
