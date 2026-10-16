

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  TEXT PRIMARY KEY,
    started_at              TEXT NOT NULL,
    finished_at             TEXT,
    mode                    TEXT NOT NULL CHECK (mode IN ('incremental', 'full_refresh')),
    status                  TEXT NOT NULL CHECK (status IN ('running', 'success', 'no_new_data', 'failed')),
    files_ingested          INTEGER NOT NULL DEFAULT 0,
    raw_rows_ingested       INTEGER NOT NULL DEFAULT 0,
    rows_processed          INTEGER NOT NULL DEFAULT 0,
    rows_accepted           INTEGER NOT NULL DEFAULT 0,
    rows_quarantined        INTEGER NOT NULL DEFAULT 0,
    exact_duplicates        INTEGER NOT NULL DEFAULT 0,
    conflicting_duplicates  INTEGER NOT NULL DEFAULT 0,
    users_recomputed        INTEGER NOT NULL DEFAULT 0,
    error                   TEXT
);


CREATE TABLE IF NOT EXISTS pipeline_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS ingest_files (
    file_hash    TEXT PRIMARY KEY,              
    file_name    TEXT NOT NULL,
    file_format  TEXT NOT NULL CHECK (file_format IN ('jsonl', 'csv')),
    line_count   INTEGER NOT NULL,
    blank_lines  INTEGER NOT NULL,
    run_id       TEXT NOT NULL,
    ingested_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_events (
    raw_id             INTEGER PRIMARY KEY,     
    run_id             TEXT NOT NULL,
    file_hash          TEXT NOT NULL REFERENCES ingest_files (file_hash),
    line_no            INTEGER NOT NULL,
    raw_payload        TEXT NOT NULL,           
    ingested_at_epoch  REAL NOT NULL,
    UNIQUE (file_hash, line_no)
);


CREATE TABLE IF NOT EXISTS dim_event_types (
    event_name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_funnel_steps (
    step_index INTEGER PRIMARY KEY,
    step_name  TEXT NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS stg_events (
    raw_id         INTEGER PRIMARY KEY REFERENCES raw_events (raw_id),
    run_id         TEXT NOT NULL,
    event_id       TEXT,
    user_id        TEXT,
    event_name     TEXT,
    ts_epoch       REAL,
    event_ts       TEXT,
    device         TEXT,
    country        TEXT,
    channel        TEXT,
    app_version    TEXT,
    product_id     TEXT,
    price          REAL,
    reject_reason  TEXT
);

CREATE TABLE IF NOT EXISTS quarantine_events (
    raw_id       INTEGER PRIMARY KEY REFERENCES raw_events (raw_id),
    run_id       TEXT NOT NULL,
    reason       TEXT NOT NULL,
    raw_payload  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_quarantine_reason ON quarantine_events (reason);

CREATE TABLE IF NOT EXISTS duplicate_events (
    raw_id    INTEGER PRIMARY KEY REFERENCES raw_events (raw_id),
    run_id    TEXT NOT NULL,
    event_id  TEXT NOT NULL,
    kind      TEXT NOT NULL CHECK (kind IN ('exact', 'conflicting'))
);

CREATE TABLE IF NOT EXISTS clean_events (
    event_id           TEXT PRIMARY KEY,        
    raw_id             INTEGER NOT NULL UNIQUE REFERENCES raw_events (raw_id),
    run_id             TEXT NOT NULL,
    user_id            TEXT NOT NULL,
    event_name         TEXT NOT NULL REFERENCES dim_event_types (event_name),
    ts_epoch           REAL NOT NULL,
    event_ts           TEXT NOT NULL,
    device             TEXT NOT NULL,
    country            TEXT NOT NULL,
    channel            TEXT NOT NULL,
    app_version        TEXT NOT NULL,
    product_id         TEXT,
    price              REAL CHECK (price IS NULL OR price >= 0),
    is_near_duplicate  INTEGER NOT NULL DEFAULT 0 CHECK (is_near_duplicate IN (0, 1)),
    session_id         TEXT
);
CREATE INDEX IF NOT EXISTS ix_clean_user_ts ON clean_events (user_id, ts_epoch);
CREATE INDEX IF NOT EXISTS ix_clean_session ON clean_events (session_id, event_name, ts_epoch);


CREATE TABLE IF NOT EXISTS sessions (
    session_id        TEXT PRIMARY KEY,
    user_id           TEXT NOT NULL,
    session_seq       INTEGER NOT NULL,
    session_start     TEXT NOT NULL,
    session_end       TEXT NOT NULL,
    start_epoch       REAL NOT NULL,
    duration_seconds  REAL NOT NULL,
    event_count       INTEGER NOT NULL,
    device            TEXT NOT NULL,
    country           TEXT NOT NULL,
    channel           TEXT NOT NULL,
    app_version       TEXT NOT NULL,
    session_date      TEXT NOT NULL,
    entry_event       TEXT NOT NULL,
    exit_event        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sessions_user ON sessions (user_id);
CREATE INDEX IF NOT EXISTS ix_sessions_date ON sessions (session_date);

CREATE TABLE IF NOT EXISTS funnel_session_steps (
    session_id     TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    step_index     INTEGER NOT NULL,
    step_name      TEXT NOT NULL,
    reached_epoch  REAL NOT NULL,
    PRIMARY KEY (session_id, step_index)
);
CREATE INDEX IF NOT EXISTS ix_fss_user ON funnel_session_steps (user_id);
CREATE INDEX IF NOT EXISTS ix_fss_step ON funnel_session_steps (step_index);

CREATE TABLE IF NOT EXISTS funnel_metrics (
    step_index               INTEGER PRIMARY KEY,
    step_name                TEXT NOT NULL,
    sessions_reached         INTEGER NOT NULL,
    prev_sessions            INTEGER,
    step_conversion          REAL,
    overall_conversion       REAL,
    drop_off_sessions        INTEGER,
    drop_off_rate            REAL,
    median_seconds_from_prev REAL
);

CREATE TABLE IF NOT EXISTS funnel_segments (
    dimension         TEXT NOT NULL,
    segment_value     TEXT NOT NULL,
    step_index        INTEGER NOT NULL,
    step_name         TEXT NOT NULL,
    sessions_reached  INTEGER NOT NULL,
    prev_sessions     INTEGER,
    step_conversion   REAL,
    PRIMARY KEY (dimension, segment_value, step_index)
);


CREATE TABLE IF NOT EXISTS daily_funnel (
    session_date      TEXT NOT NULL,
    step_index        INTEGER NOT NULL,
    step_name         TEXT NOT NULL,
    sessions_reached  INTEGER NOT NULL,
    prev_sessions     INTEGER,
    step_conversion   REAL,
    PRIMARY KEY (session_date, step_index)
);

CREATE TABLE IF NOT EXISTS funnel_anomalies (
    session_date         TEXT NOT NULL,
    step_index           INTEGER NOT NULL,
    step_name            TEXT NOT NULL,
    day_conversion       REAL NOT NULL,
    baseline_conversion  REAL NOT NULL,
    relative_change      REAL NOT NULL,
    day_sessions         INTEGER NOT NULL,
    baseline_sessions    INTEGER NOT NULL,
    p_value              REAL NOT NULL,
    PRIMARY KEY (session_date, step_index)
);


CREATE TABLE IF NOT EXISTS schema_drift (
    field             TEXT PRIMARY KEY,
    rows              INTEGER NOT NULL,
    first_seen_file   TEXT NOT NULL,
    first_seen_run    TEXT NOT NULL,
    example_value     TEXT
);


CREATE TABLE IF NOT EXISTS task_runs (
    run_id         TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    task           TEXT NOT NULL,
    status         TEXT NOT NULL,
    duration_ms    REAL NOT NULL,
    rows_affected  INTEGER,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_key   TEXT PRIMARY KEY,          
    run_id      TEXT NOT NULL,
    severity    TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    kind        TEXT NOT NULL,
    message     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    delivered   TEXT NOT NULL               -- comma list of channels it was sent to
);


CREATE TABLE IF NOT EXISTS erased_users (
    user_hash   TEXT PRIMARY KEY,
    erased_at   TEXT NOT NULL,
    rows_removed INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS ai_explanations (
    context_hash  TEXT NOT NULL,
    provider      TEXT NOT NULL,
    model         TEXT NOT NULL,
    step_name     TEXT NOT NULL,
    explanation   TEXT NOT NULL,
    status        TEXT NOT NULL,
    warnings      TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (context_hash, provider, model)
);
