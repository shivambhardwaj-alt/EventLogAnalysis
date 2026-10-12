-- =====================================================================
-- STAGE 1: parse, type-cast, normalise and validate the current batch.
-- Input : raw_events rows listed in temp table _batch
-- Output: one stg_events row per raw row (reject_reason NULL = valid)
--         + quarantine_events for every rejected row
-- Params: :run_id, :min_epoch, :future_tolerance_s
-- Nothing is ever dropped silently: a raw row ends up either in
-- clean_events, duplicate_events or quarantine_events.
-- =====================================================================

INSERT INTO stg_events (
    raw_id, run_id, event_id, user_id, event_name, ts_epoch, event_ts,
    device, country, channel, app_version, product_id, price, reject_reason
)
WITH src AS (
    SELECT r.raw_id,
           r.raw_payload            AS p,
           r.ingested_at_epoch      AS ingested_epoch,
           json_valid(r.raw_payload) AS is_json
    FROM raw_events AS r
    JOIN _batch     AS b ON b.raw_id = r.raw_id
),
extracted AS (
    -- json_extract is only evaluated on valid JSON (CASE is lazy in SQLite)
    SELECT raw_id, ingested_epoch, is_json,
        CASE WHEN is_json THEN json_type(p)                              END AS root_type,
        CASE WHEN is_json THEN json_extract(p, '$.__parse_error__')      END AS parse_error,
        CASE WHEN is_json THEN json_extract(p, '$.event_id')             END AS v_event_id,
        CASE WHEN is_json THEN json_extract(p, '$.user_id')              END AS v_user_id,
        CASE WHEN is_json THEN json_extract(p, '$.event_name')           END AS v_event_name,
        CASE WHEN is_json THEN json_extract(p, '$.ts')                   END AS v_ts,
        CASE WHEN is_json THEN json_extract(p, '$.device')               END AS v_device,
        CASE WHEN is_json THEN json_extract(p, '$.country')              END AS v_country,
        CASE WHEN is_json THEN json_extract(p, '$.channel')              END AS v_channel,
        CASE WHEN is_json THEN json_extract(p, '$.app_version')          END AS v_app_version,
        CASE WHEN is_json THEN json_extract(p, '$.properties.product_id') END AS v_product_id,
        CASE WHEN is_json THEN json_extract(p, '$.properties.price')     END AS v_price
    FROM src
),
typed AS (
    SELECT raw_id, ingested_epoch, is_json, root_type, parse_error,

        -- ids: numbers become text, blanks become NULL
        NULLIF(TRIM(CAST(v_event_id AS TEXT)), '') AS event_id,
        CASE WHEN LOWER(TRIM(CAST(v_user_id AS TEXT))) IN ('', 'null', 'none', 'nan', 'undefined')
             THEN NULL
             ELSE TRIM(CAST(v_user_id AS TEXT)) END AS user_id,

        -- "Add To Cart" / "add-to-cart" / " ADD_TO_CART " -> add_to_cart
        NULLIF(LOWER(REPLACE(REPLACE(TRIM(CAST(v_event_name AS TEXT)), ' ', '_'), '-', '_')), '') AS event_name,

        -- timestamps: epoch seconds, epoch millis (number or numeric string),
        -- or ISO-8601 with Z / +hh:mm offsets.  Everything becomes UTC epoch.
        ROUND(CASE
            WHEN typeof(v_ts) IN ('integer', 'real')
                THEN CASE WHEN v_ts > 1e11 THEN v_ts / 1000.0 ELSE v_ts * 1.0 END
            WHEN typeof(v_ts) = 'text' AND TRIM(v_ts) <> '' AND TRIM(v_ts) NOT GLOB '*[^0-9.]*'
                THEN CASE WHEN CAST(TRIM(v_ts) AS REAL) > 1e11
                          THEN CAST(TRIM(v_ts) AS REAL) / 1000.0
                          ELSE CAST(TRIM(v_ts) AS REAL) END
            WHEN typeof(v_ts) = 'text'
                THEN (julianday(TRIM(v_ts)) - 2440587.5) * 86400.0
        END, 3) AS ts_epoch,

        -- categorical cleaning with explicit "unknown" buckets instead of NULLs
        CASE
            WHEN LOWER(TRIM(v_device)) IN ('mobile', 'desktop', 'tablet') THEN LOWER(TRIM(v_device))
            WHEN LOWER(TRIM(v_device)) IN ('ios', 'android', 'iphone', 'phone') THEN 'mobile'
            WHEN LOWER(TRIM(v_device)) IN ('ipad') THEN 'tablet'
            WHEN LOWER(TRIM(v_device)) IN ('web', 'pc', 'mac') THEN 'desktop'
            ELSE 'unknown'
        END AS device,
        CASE WHEN LENGTH(TRIM(v_country)) = 2 AND UPPER(TRIM(v_country)) GLOB '[A-Z][A-Z]'
             THEN UPPER(TRIM(v_country)) ELSE 'ZZ' END AS country,
        COALESCE(NULLIF(LOWER(TRIM(v_channel)), ''), 'unknown') AS channel,
        COALESCE(NULLIF(LOWER(TRIM(CAST(v_app_version AS TEXT))), ''), 'unknown') AS app_version,
        NULLIF(TRIM(CAST(v_product_id AS TEXT)), '') AS product_id,

        -- price: numbers kept, numeric strings cast, junk / negatives -> NULL
        CASE
            WHEN typeof(v_price) IN ('integer', 'real') AND v_price >= 0 THEN v_price * 1.0
            WHEN typeof(v_price) = 'text' AND TRIM(v_price) <> ''
                 AND TRIM(v_price) NOT GLOB '*[^0-9.]*'
                 AND TRIM(v_price) NOT GLOB '*.*.*'          THEN CAST(TRIM(v_price) AS REAL)
            ELSE NULL
        END AS price
    FROM extracted
)
SELECT
    t.raw_id,
    :run_id,
    t.event_id,
    t.user_id,
    t.event_name,
    t.ts_epoch,
    CASE WHEN t.ts_epoch IS NOT NULL
         THEN strftime('%Y-%m-%dT%H:%M:%fZ', t.ts_epoch, 'unixepoch') END,
    t.device,
    t.country,
    t.channel,
    t.app_version,
    t.product_id,
    t.price,
    -- first failing rule wins; order = most fundamental problem first
    CASE
        WHEN NOT t.is_json                         THEN 'malformed_json'
        WHEN t.parse_error IS NOT NULL             THEN CAST(t.parse_error AS TEXT)
        WHEN t.root_type <> 'object'               THEN 'not_a_json_object'
        WHEN t.event_id IS NULL                    THEN 'missing_event_id'
        WHEN t.user_id IS NULL                     THEN 'missing_user_id'
        WHEN t.event_name IS NULL                  THEN 'missing_event_name'
        WHEN NOT EXISTS (SELECT 1 FROM dim_event_types d WHERE d.event_name = t.event_name)
                                                   THEN 'unknown_event_type'
        WHEN t.ts_epoch IS NULL                    THEN 'invalid_timestamp'
        WHEN t.ts_epoch < :min_epoch               THEN 'timestamp_too_old'
        WHEN t.ts_epoch > t.ingested_epoch + :future_tolerance_s
                                                   THEN 'timestamp_in_future'
        ELSE NULL
    END
FROM typed AS t
ORDER BY t.raw_id;

INSERT INTO quarantine_events (raw_id, run_id, reason, raw_payload)
SELECT s.raw_id, s.run_id, s.reject_reason, r.raw_payload
FROM stg_events AS s
JOIN _batch     AS b ON b.raw_id = s.raw_id
JOIN raw_events AS r ON r.raw_id = s.raw_id
WHERE s.reject_reason IS NOT NULL;
