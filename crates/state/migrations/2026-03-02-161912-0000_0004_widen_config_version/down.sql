PRAGMA foreign_keys = OFF;

CREATE TABLE configs_new (
    name TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL,
    namespace TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    transform_hash TEXT,
    applied_at TEXT NOT NULL
);

INSERT INTO configs_new SELECT * FROM configs;
DROP TABLE configs;
ALTER TABLE configs_new RENAME TO configs;

PRAGMA foreign_keys = ON;
