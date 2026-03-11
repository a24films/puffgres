-- Convert all TEXT timestamp columns to INTEGER (epoch milliseconds).
-- SQLite does not support ALTER COLUMN, so we recreate each table.

PRAGMA foreign_keys = OFF;

-- configs
CREATE TABLE configs_new (
    name TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    transform_hash TEXT,
    applied_at INTEGER NOT NULL,
    tombstone_applied_at INTEGER,
    namespace_prefix TEXT
);

INSERT INTO configs_new (name, namespace, content_hash, transform_hash, applied_at, tombstone_applied_at, namespace_prefix)
SELECT name, namespace, content_hash, transform_hash,
       CAST(strftime('%s', applied_at) AS INTEGER) * 1000,
       CASE WHEN tombstone_applied_at IS NOT NULL
            THEN CAST(strftime('%s', tombstone_applied_at) AS INTEGER) * 1000
            ELSE NULL END,
       namespace_prefix
FROM configs;

DROP TABLE configs;
ALTER TABLE configs_new RENAME TO configs;

-- streaming_checkpoints
CREATE TABLE streaming_checkpoints_new (
    config_name TEXT PRIMARY KEY NOT NULL,
    lsn INTEGER NOT NULL,
    events_processed INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
);

INSERT INTO streaming_checkpoints_new (config_name, lsn, events_processed, updated_at)
SELECT config_name, lsn, events_processed,
       CAST(strftime('%s', updated_at) AS INTEGER) * 1000
FROM streaming_checkpoints;

DROP TABLE streaming_checkpoints;
ALTER TABLE streaming_checkpoints_new RENAME TO streaming_checkpoints;

-- backfill_progress
CREATE TABLE backfill_progress_new (
    config_name TEXT PRIMARY KEY NOT NULL,
    last_id TEXT,
    total_rows INTEGER,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    started_at INTEGER,
    completed_at INTEGER,
    error_message TEXT,
    watermark_lsn INTEGER,
    FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
);

INSERT INTO backfill_progress_new (config_name, last_id, total_rows, processed_rows, status, started_at, completed_at, error_message, watermark_lsn)
SELECT config_name, last_id, total_rows, processed_rows, status,
       CASE WHEN started_at IS NOT NULL
            THEN CAST(strftime('%s', started_at) AS INTEGER) * 1000
            ELSE NULL END,
       CASE WHEN completed_at IS NOT NULL
            THEN CAST(strftime('%s', completed_at) AS INTEGER) * 1000
            ELSE NULL END,
       error_message, watermark_lsn
FROM backfill_progress;

DROP TABLE backfill_progress;
ALTER TABLE backfill_progress_new RENAME TO backfill_progress;

-- dlq
CREATE TABLE dlq_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    config_name TEXT NOT NULL,
    lsn INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    doc_id TEXT,
    error_message TEXT NOT NULL,
    error_kind TEXT NOT NULL CHECK (error_kind IN ('retryable', 'permanent')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    last_retry_at INTEGER,
    permanent_at INTEGER,
    FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
);

INSERT INTO dlq_new (id, config_name, lsn, event_json, doc_id, error_message, error_kind, retry_count, created_at, last_retry_at, permanent_at)
SELECT id, config_name, lsn, event_json, doc_id, error_message, error_kind, retry_count,
       CAST(strftime('%s', created_at) AS INTEGER) * 1000,
       CASE WHEN last_retry_at IS NOT NULL
            THEN CAST(strftime('%s', last_retry_at) AS INTEGER) * 1000
            ELSE NULL END,
       CASE WHEN permanent_at IS NOT NULL
            THEN CAST(strftime('%s', permanent_at) AS INTEGER) * 1000
            ELSE NULL END
FROM dlq;

DROP TABLE dlq;
ALTER TABLE dlq_new RENAME TO dlq;
CREATE INDEX idx_dlq_config_name ON dlq(config_name);
CREATE INDEX idx_dlq_error_kind ON dlq(error_kind);

PRAGMA foreign_keys = ON;
