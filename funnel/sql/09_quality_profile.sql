


WITH j AS (
    SELECT r.raw_id, r.raw_payload AS p
    FROM raw_events r
    JOIN clean_events c ON c.raw_id = r.raw_id
)
SELECT
    SUM(json_type(p, '$.ts') IN ('integer', 'real'))                          AS ts_epoch_number,
    SUM(json_type(p, '$.ts') = 'text' AND json_extract(p, '$.ts') NOT GLOB '*[^0-9.]*')
                                                                              AS ts_epoch_string,
    SUM(json_type(p, '$.ts') = 'text' AND json_extract(p, '$.ts') GLOB '*[+-][0-9][0-9]:[0-9][0-9]')
                                                                              AS ts_with_utc_offset,
    SUM(json_extract(p, '$.event_name') <> c.event_name)                      AS event_name_normalised,
    SUM(json_type(p, '$.properties.price') = 'text')                          AS price_string_cast,
    SUM(json_type(p, '$.properties.price') IS NOT NULL AND c.price IS NULL)   AS price_invalid_nulled,
    SUM(json_type(p, '$.user_id') = 'integer')                                AS user_id_number_cast,
    SUM(c.device = 'unknown')                                                 AS device_unknown,
    SUM(LOWER(COALESCE(json_extract(p, '$.device'), '')) <> c.device AND c.device <> 'unknown')
                                                                              AS device_mapped,
    SUM(c.country = 'ZZ')                                                     AS country_unknown,
    SUM(c.channel = 'unknown')                                                AS channel_unknown
FROM j JOIN clean_events c ON c.raw_id = j.raw_id;
