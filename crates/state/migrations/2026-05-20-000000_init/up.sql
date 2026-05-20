-- Initial puffgres state schema. Tables are created without schema
-- qualification; the caller sets `search_path` to the configured schema
-- (default: puffgres) before running migrations.

CREATE TABLE configs (
    name TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    transform_hash TEXT,
    applied_at BIGINT NOT NULL,
    tombstone_applied_at BIGINT,
    namespace_prefix TEXT
);

CREATE TABLE streaming_checkpoints (
    config_name TEXT PRIMARY KEY NOT NULL REFERENCES configs(name) ON DELETE CASCADE,
    lsn PG_LSN NOT NULL,
    events_processed BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL
);

CREATE TABLE backfill_progress (
    config_name TEXT PRIMARY KEY NOT NULL REFERENCES configs(name) ON DELETE CASCADE,
    last_id TEXT,
    total_rows BIGINT,
    processed_rows BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    started_at BIGINT,
    completed_at BIGINT,
    error_message TEXT,
    watermark_lsn PG_LSN
);

CREATE TABLE dlq (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    config_name TEXT NOT NULL REFERENCES configs(name) ON DELETE CASCADE,
    lsn PG_LSN NOT NULL,
    doc_id TEXT,
    operation TEXT,
    error_message TEXT NOT NULL,
    error_kind TEXT NOT NULL CHECK (error_kind IN ('retryable', 'permanent')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    last_retry_at BIGINT,
    permanent_at BIGINT
);

CREATE INDEX idx_dlq_config_name ON dlq(config_name);
CREATE INDEX idx_dlq_error_kind ON dlq(error_kind);
