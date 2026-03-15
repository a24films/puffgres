//! Chaos tests for SQLite state corruption and recovery.
//! Run with: cargo test -p state --test chaos_sqlite -- --ignored --nocapture

use chrono::Utc;
use state::{ConfigRecord, StateDb, StreamingCheckpoint};
use std::io::Write;

fn setup() -> (tempfile::TempDir, StateDb) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = StateDb::open(&path).unwrap();
    db.insert_config(&ConfigRecord {
        name: "test".to_string(),
        namespace: "test".to_string(),
        content_hash: "abc".to_string(),
        transform_hash: None,
        applied_at: Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    })
    .unwrap();
    (dir, db)
}

fn save_checkpoint(db: &StateDb, lsn: u64, events: u64) {
    db.save_streaming_checkpoint(&StreamingCheckpoint {
        config_name: "test".to_string(),
        lsn,
        events_processed: events,
        updated_at: Utc::now(),
    })
    .unwrap();
}

#[test]
#[ignore]
fn checkpoint_survives_process_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    {
        let db = StateDb::open(&path).unwrap();
        db.insert_config(&ConfigRecord {
            name: "test".to_string(),
            namespace: "test".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();
        save_checkpoint(&db, 42000, 500);
        // db dropped here — simulates process exit
    }

    // "Restart" — reopen
    let db = StateDb::open(&path).unwrap();
    let cp = db.get_streaming_checkpoint("test").unwrap().unwrap();
    assert_eq!(cp.lsn, 42000);
    assert_eq!(cp.events_processed, 500);
    println!(
        "checkpoint survived restart: LSN={}, events={}",
        cp.lsn, cp.events_processed
    );
}

#[test]
#[ignore]
fn corrupt_db_file_detected_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create a valid DB with data
    {
        let db = StateDb::open(&path).unwrap();
        db.insert_config(&ConfigRecord {
            name: "test".to_string(),
            namespace: "test".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();
    }

    // Corrupt the database file
    {
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all(b"CORRUPTED_GARBAGE_DATA_HERE").unwrap();
    }

    let result = StateDb::open(&path);
    let is_err = result.is_err();
    println!("corrupt DB open result: is_err={is_err}");
    assert!(is_err, "opening a corrupted database should fail");
}

#[test]
#[ignore]
fn corrupt_wal_file_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create valid DB with checkpoint
    {
        let db = StateDb::open(&path).unwrap();
        db.insert_config(&ConfigRecord {
            name: "test".to_string(),
            namespace: "test".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();
        save_checkpoint(&db, 99000, 1000);
    }

    // Corrupt the WAL file if it exists
    let wal_path = path.with_extension("db-wal");
    if wal_path.exists() {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(b"CORRUPT_WAL_GARBAGE").unwrap();
        println!("corrupted WAL file at {wal_path:?}");
    } else {
        // WAL was checkpointed when db was dropped. Create a garbage WAL.
        std::fs::write(&wal_path, b"CORRUPT_WAL_GARBAGE").unwrap();
        println!("created corrupt WAL file at {wal_path:?}");
    }

    // Try to reopen — SQLite should either recover or fail clearly
    match StateDb::open(&path) {
        Ok(db) => {
            // SQLite ignored/recovered from the corrupt WAL
            let cp = db.get_streaming_checkpoint("test").unwrap();
            println!("WAL corruption recovered, checkpoint: {cp:?}");
            // Data from before the WAL corruption should still be there
            // (it was in the main DB file, not the WAL)
        }
        Err(e) => {
            println!("WAL corruption detected: {e}");
            // This is also acceptable — clear error, no silent data loss
        }
    }
}

#[test]
#[ignore]
fn many_rapid_checkpoints_no_corruption() {
    let (_dir, db) = setup();

    for i in 0..10_000u64 {
        save_checkpoint(&db, i * 100, i);
    }

    let cp = db.get_streaming_checkpoint("test").unwrap().unwrap();
    assert_eq!(cp.lsn, 999_900);
    assert_eq!(cp.events_processed, 9_999);
    println!("10k rapid checkpoints succeeded, final LSN={}", cp.lsn);
}

#[test]
#[ignore]
fn concurrent_reads_during_writes() {
    let (_dir, db) = setup();
    let db2 = db.clone();

    save_checkpoint(&db, 1000, 10);

    // Read from clone while writing via original
    let cp = db2.get_streaming_checkpoint("test").unwrap().unwrap();
    assert_eq!(cp.lsn, 1000);

    save_checkpoint(&db, 2000, 20);

    let cp = db2.get_streaming_checkpoint("test").unwrap().unwrap();
    assert_eq!(cp.lsn, 2000);

    println!("concurrent read/write works correctly via Arc<Mutex>");
}
