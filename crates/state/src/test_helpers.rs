use chrono::Utc;

use crate::{ConfigRecord, StateDb};

pub fn setup_test_db() -> (tempfile::TempDir, StateDb) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = StateDb::open(&path).unwrap();
    (dir, db)
}

pub fn sample_config(name: &str) -> ConfigRecord {
    ConfigRecord {
        name: name.to_string(),
        namespace: name.to_string(),
        content_hash: "abc123".to_string(),
        transform_hash: None,
        applied_at: Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    }
}
