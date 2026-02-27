-- SQLite stores all integers as up to 8 bytes regardless of declared type,
-- so existing data is unaffected. This migration changes the declared column
-- type from INTEGER to BIGINT so that Diesel generates i64 (BigInt) mappings
-- instead of i32 (Integer), preserving versions above 2,147,483,647.

PRAGMA foreign_keys = OFF;

CREATE TABLE configs_new (
    name TEXT PRIMARY KEY NOT NULL,
    version BIGINT NOT NULL,
    namespace TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    transform_hash TEXT,
    applied_at TEXT NOT NULL
);

INSERT INTO configs_new SELECT * FROM configs;
DROP TABLE configs;
ALTER TABLE configs_new RENAME TO configs;

PRAGMA foreign_keys = ON;
