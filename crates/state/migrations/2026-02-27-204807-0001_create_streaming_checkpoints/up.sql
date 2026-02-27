CREATE TABLE streaming_checkpoints (
    config_name TEXT PRIMARY KEY NOT NULL,
    lsn INTEGER NOT NULL,
    events_processed INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
);
