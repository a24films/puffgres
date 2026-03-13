use criterion::{Criterion, criterion_group, criterion_main};
use state::{ConfigRecord, StateDb, StreamingCheckpoint};

fn setup_db() -> (tempfile::TempDir, StateDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("bench.db")).unwrap();
    db.insert_config(&ConfigRecord {
        name: "bench".to_string(),
        namespace: "bench".to_string(),
        content_hash: "abc".to_string(),
        transform_hash: None,
        applied_at: chrono::Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    })
    .unwrap();
    (dir, db)
}

fn bench_checkpoint_save(c: &mut Criterion) {
    let (_dir, db) = setup_db();

    c.bench_function("save_streaming_checkpoint", |b| {
        let mut lsn = 0u64;
        b.iter(|| {
            lsn += 1;
            db.save_streaming_checkpoint(&StreamingCheckpoint {
                config_name: "bench".to_string(),
                lsn,
                events_processed: lsn * 100,
                updated_at: chrono::Utc::now(),
            })
            .unwrap();
        })
    });
}

fn bench_checkpoint_read(c: &mut Criterion) {
    let (_dir, db) = setup_db();
    db.save_streaming_checkpoint(&StreamingCheckpoint {
        config_name: "bench".to_string(),
        lsn: 1000,
        events_processed: 50000,
        updated_at: chrono::Utc::now(),
    })
    .unwrap();

    c.bench_function("get_streaming_checkpoint", |b| {
        b.iter(|| {
            db.get_streaming_checkpoint("bench").unwrap();
        })
    });
}

fn bench_checkpoint_save_read_cycle(c: &mut Criterion) {
    let (_dir, db) = setup_db();

    c.bench_function("save_then_read_checkpoint", |b| {
        let mut lsn = 0u64;
        b.iter(|| {
            lsn += 1;
            db.save_streaming_checkpoint(&StreamingCheckpoint {
                config_name: "bench".to_string(),
                lsn,
                events_processed: lsn * 100,
                updated_at: chrono::Utc::now(),
            })
            .unwrap();
            db.get_streaming_checkpoint("bench").unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_checkpoint_save,
    bench_checkpoint_read,
    bench_checkpoint_save_read_cycle,
);
criterion_main!(benches);
