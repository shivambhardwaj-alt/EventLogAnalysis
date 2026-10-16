

DELETE FROM schema_drift;

INSERT INTO schema_drift (field, rows, first_seen_file, first_seen_run, example_value)
WITH objs AS (
    SELECT r.raw_id, r.run_id, r.file_hash, r.raw_payload AS p
    FROM raw_events AS r
    WHERE json_valid(r.raw_payload)
      AND json_type(r.raw_payload) = 'object'
      AND json_extract(r.raw_payload, '$.__parse_error__') IS NULL
),
top_level AS (
    SELECT o.raw_id, o.run_id, o.file_hash, j.key AS field, j.value
    FROM objs AS o, json_each(o.p) AS j
    WHERE j.key NOT IN (SELECT value FROM json_each(:contract_fields))
),
nested AS (
    SELECT o.raw_id, o.run_id, o.file_hash, 'properties.' || j.key AS field, j.value
    FROM objs AS o, json_each(o.p, '$.properties') AS j
    WHERE json_type(o.p, '$.properties') = 'object'
      AND j.key NOT IN (SELECT value FROM json_each(:contract_property_fields))
),
all_fields AS (
    SELECT * FROM top_level
    UNION ALL
    SELECT * FROM nested
),
ranked AS (
    SELECT all_fields.*,
           ROW_NUMBER() OVER (PARTITION BY field ORDER BY raw_id) AS rn,
           COUNT(*)     OVER (PARTITION BY field)                 AS cnt
    FROM all_fields
)
SELECT r.field, r.cnt, f.file_name, r.run_id, SUBSTR(CAST(r.value AS TEXT), 1, 80)
FROM ranked AS r
JOIN ingest_files AS f ON f.file_hash = r.file_hash
WHERE r.rn = 1;
