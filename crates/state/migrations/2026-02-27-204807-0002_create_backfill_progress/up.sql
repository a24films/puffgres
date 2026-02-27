CREATE TABLE backfill_progress (
    config_name TEXT PRIMARY KEY NOT NULL,
    last_id TEXT,
    total_rows INTEGER,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    started_at TEXT,
    completed_at TEXT,
    error_message TEXT,
    watermark_lsn INTEGER,
    FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
);
