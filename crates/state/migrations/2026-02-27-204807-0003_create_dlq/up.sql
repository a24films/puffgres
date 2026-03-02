CREATE TABLE dlq (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    config_name TEXT NOT NULL,
    lsn INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    doc_id TEXT,
    error_message TEXT NOT NULL,
    error_kind TEXT NOT NULL CHECK (error_kind IN ('retryable', 'permanent')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_retry_at TEXT,
    permanent_at TEXT,
    FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
);
CREATE INDEX idx_dlq_config_name ON dlq(config_name);
CREATE INDEX idx_dlq_error_kind ON dlq(error_kind);
