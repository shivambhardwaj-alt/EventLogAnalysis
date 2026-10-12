-- =====================================================================
-- STAGE 2: exact de-duplication on event_id.
-- A duplicate can be inside this batch (client retry) or against events
-- already loaded by an earlier run (retry that arrived a day later).
-- Winner = earliest arrival (lowest raw_id) - deterministic on reruns.
--   kind = 'exact'       same id, same user/event/timestamp
--   kind = 'conflicting' same id but different content (a producer bug;
--                        counted separately so it can be alerted on)
-- Params: :run_id
-- =====================================================================

INSERT INTO duplicate_events (raw_id, run_id, event_id, kind)
WITH candidates AS (
    SELECT s.raw_id, s.event_id, s.user_id, s.event_name, s.ts_epoch,
           ROW_NUMBER() OVER (PARTITION BY s.event_id ORDER BY s.raw_id) AS rn,
           EXISTS (SELECT 1 FROM clean_events c WHERE c.event_id = s.event_id) AS seen_before
    FROM stg_events AS s
    JOIN _batch     AS b ON b.raw_id = s.raw_id
    WHERE s.reject_reason IS NULL
),
winners AS (
    SELECT c.event_id, c.user_id, c.event_name, c.ts_epoch
    FROM clean_events AS c
    WHERE c.event_id IN (SELECT event_id FROM candidates WHERE seen_before)
    UNION ALL
    SELECT event_id, user_id, event_name, ts_epoch
    FROM candidates
    WHERE rn = 1 AND NOT seen_before
)
SELECT k.raw_id,
       :run_id,
       k.event_id,
       CASE WHEN k.user_id = w.user_id
             AND k.event_name = w.event_name
             AND k.ts_epoch = w.ts_epoch
            THEN 'exact' ELSE 'conflicting' END
FROM candidates AS k
JOIN winners    AS w ON w.event_id = k.event_id
WHERE k.seen_before OR k.rn > 1;

INSERT INTO clean_events (
    event_id, raw_id, run_id, user_id, event_name, ts_epoch, event_ts,
    device, country, channel, app_version, product_id, price
)
SELECT s.event_id, s.raw_id, :run_id, s.user_id, s.event_name, s.ts_epoch, s.event_ts,
       s.device, s.country, s.channel, s.app_version, s.product_id, s.price
FROM stg_events AS s
JOIN _batch     AS b ON b.raw_id = s.raw_id
WHERE s.reject_reason IS NULL
  AND NOT EXISTS (SELECT 1 FROM duplicate_events d WHERE d.raw_id = s.raw_id)
ORDER BY s.raw_id;

-- Users whose history changed in this batch: only they need their
-- near-duplicate flags, sessions and funnel steps recomputed.
DELETE FROM _affected_users;

INSERT OR IGNORE INTO _affected_users (user_id)
SELECT DISTINCT c.user_id
FROM clean_events AS c
JOIN _batch       AS b ON b.raw_id = c.raw_id;
