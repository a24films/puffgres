CREATE TABLE configs (
    name TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL,
    namespace TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    transform_hash TEXT,
    applied_at TEXT NOT NULL
);
