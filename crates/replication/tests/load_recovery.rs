//! Load test: crash recovery and checkpoint correctness.
//! Run with: cargo test -p replication --test load_recovery -- --ignored --nocapture

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use chrono::Utc;
use pgwire_replication::{Lsn, ReplicationEvent};
use replication::stream::{BatchResult, ReplicationStream};
use replication::{ReplicationTransport, Result};
use state::{ConfigRecord, Store, StreamingCheckpoint};

struct LazyTransport {
    events_per_txn: usize,
    num_txns: usize,
    start_txn_index: usize,
    state: TxState,
    txn_cursor: usize,
    txn_index: usize,
    relation_sent: bool,
    acked: Mutex<Vec<u64>>,
}

enum TxState {
    Ready,
    InTxn,
    Done,
}

impl LazyTransport {
    fn new(events_per_txn: usize, num_txns: usize) -> Self {
        Self::starting_at(events_per_txn, num_txns, 0)
    }

    fn starting_at(events_per_txn: usize, num_txns: usize, start_txn_index: usize) -> Self {
        Self {
            events_per_txn,
            num_txns,
            start_txn_index,
            state: TxState::Ready,
            txn_cursor: 0,
            txn_index: start_txn_index,
            relation_sent: false,
            acked: Mutex::new(Vec::new()),
        }
    }

    fn lsn_for_txn(txn_index: usize) -> u64 {
        ((txn_index + 1) * 1_000) as u64
    }
}

impl ReplicationTransport for LazyTransport {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        if !self.relation_sent {
            self.relation_sent = true;
            return Ok(Some(relation_event()));
        }
        match self.state {
            TxState::Done => Ok(None),
            TxState::Ready => {
                if self.txn_index >= self.start_txn_index + self.num_txns {
                    self.state = TxState::Done;
                    return Ok(None);
                }
                self.txn_cursor = 0;
                self.state = TxState::InTxn;
                Ok(Some(ReplicationEvent::Begin {
                    final_lsn: Lsn(0),
                    xid: (self.txn_index + 1) as u32,
                    commit_time_micros: 0,
                }))
            }
            TxState::InTxn => {
                if self.txn_cursor < self.events_per_txn {
                    self.txn_cursor += 1;
                    Ok(Some(insert_event_with_id(self.txn_index, self.txn_cursor)))
                } else {
                    let lsn = Self::lsn_for_txn(self.txn_index);
                    self.txn_index += 1;
                    self.state = TxState::Ready;
                    Ok(Some(ReplicationEvent::Commit {
                        lsn: Lsn(lsn),
                        end_lsn: Lsn(lsn),
                        commit_time_micros: 0,
                    }))
                }
            }
        }
    }

    fn update_applied_lsn(&self, lsn: Lsn) {
        self.acked.lock().unwrap().push(lsn.0);
    }
}

fn relation_event() -> ReplicationEvent {
    let mut buf = BytesMut::new();
    buf.put_u8(b'R');
    buf.put_u32(1);
    buf.put_slice(b"public\0test\0");
    buf.put_u8(b'd');
    buf.put_u16(2);
    buf.put_u8(1);
    buf.put_slice(b"id\0");
    buf.put_u32(23);
    buf.put_i32(-1);
    buf.put_u8(0);
    buf.put_slice(b"value\0");
    buf.put_u32(25);
    buf.put_i32(-1);
    ReplicationEvent::XLogData {
        wal_start: Lsn(0),
        wal_end: Lsn(0),
        server_time_micros: 0,
        data: buf.freeze(),
    }
}

fn insert_event_with_id(txn_index: usize, event_index: usize) -> ReplicationEvent {
    let id_str = format!("{txn_index:06}_{event_index:06}");
    let id_bytes = id_str.as_bytes();

    let mut buf = BytesMut::new();
    buf.put_u8(b'I');
    buf.put_u32(1);
    buf.put_u8(b'N');
    buf.put_u16(2);
    buf.put_u8(b't');
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u32(id_bytes.len() as u32);
    buf.put_slice(id_bytes);
    buf.put_u8(b't');
    buf.put_u32(5);
    buf.put_slice(b"hello");
    ReplicationEvent::XLogData {
        wal_start: Lsn(0),
        wal_end: Lsn(0),
        server_time_micros: 0,
        data: buf.freeze(),
    }
}

use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres as PgImage;
use tokio::sync::OnceCell;

struct LoadRecoveryPg {
    _container: ContainerAsync<PgImage>,
    database_url: String,
}

static SHARED_PG: OnceCell<LoadRecoveryPg> = OnceCell::const_new();
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn shared_pg() -> &'static LoadRecoveryPg {
    SHARED_PG
        .get_or_init(|| async {
            let container = PgImage::default()
                .with_tag("17-alpine")
                .start()
                .await
                .expect("failed to start postgres testcontainer");
            let host = container.get_host().await.unwrap();
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
            LoadRecoveryPg {
                _container: container,
                database_url,
            }
        })
        .await
}

async fn setup_store() -> ((), Store) {
    let pg = shared_pg().await;
    let n = SCHEMA_COUNTER.fetch_add(1, Ordering::SeqCst);
    let schema = format!("load_recovery_{n}");
    let db = Store::connect(&pg.database_url, &schema).await.unwrap();
    db.insert_config(&ConfigRecord {
        name: "test".to_string(),
        namespace: "test".to_string(),
        content_hash: "abc".to_string(),
        transform_hash: None,
        applied_at: Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    })
    .await
    .unwrap();
    ((), db)
}

async fn save_checkpoint(db: &Store, lsn: u64, events: u64) {
    db.save_streaming_checkpoint(&StreamingCheckpoint {
        config_name: "test".to_string(),
        lsn,
        events_processed: events,
        updated_at: Utc::now(),
    })
    .await
    .unwrap();
}

fn collect_event_ids(batch: &replication::stream::StreamingBatch) -> Vec<String> {
    batch
        .events
        .iter()
        .filter_map(|e| {
            e.new_tuple.as_ref().and_then(|t| {
                t.columns.first().and_then(|c| {
                    c.as_bytes()
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .map(String::from)
                })
            })
        })
        .collect()
}

#[tokio::test]
#[ignore]
async fn crash_mid_stream_resume_from_checkpoint() {
    let num_txns = 100;
    let events_per_txn = 100;
    let crash_after_txn = 50;

    let (_dir, db) = setup_store().await;

    // Phase 1: process first 50 transactions, checkpointing after each
    let transport = LazyTransport::new(events_per_txn, num_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let mut phase1_ids: Vec<String> = Vec::new();
    let mut txns_processed = 0;
    let mut total_events = 0u64;

    while txns_processed < crash_after_txn {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                phase1_ids.extend(collect_event_ids(&b));
                total_events += b.events.len() as u64;
                save_checkpoint(&db, b.ack_lsn, total_events).await;
                stream.ack();
                txns_processed += 1;
            }
            Some(_) => {}
            None => break,
        }
    }

    let checkpoint = db.get_streaming_checkpoint("test").await.unwrap().unwrap();
    println!(
        "Phase 1: {txns_processed} txns, {} events, LSN={}",
        phase1_ids.len(),
        checkpoint.lsn
    );

    // "Crash" — drop the stream
    drop(stream);

    // Phase 2: resume from checkpoint
    let resume_txn = crash_after_txn; // LazyTransport starting_at
    let remaining_txns = num_txns - crash_after_txn;
    let transport = LazyTransport::starting_at(events_per_txn, remaining_txns, resume_txn);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let start = Instant::now();
    let mut phase2_ids: Vec<String> = Vec::new();

    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                phase2_ids.extend(collect_event_ids(&b));
                total_events += b.events.len() as u64;
                save_checkpoint(&db, b.ack_lsn, total_events).await;
                stream.ack();
            }
            Some(_) => {}
            None => break,
        }
    }
    let recovery_time = start.elapsed();

    println!(
        "Phase 2: {} events, recovery={recovery_time:.2?}",
        phase2_ids.len()
    );

    // Verify no gaps
    let all_ids: HashSet<&str> = phase1_ids
        .iter()
        .chain(&phase2_ids)
        .map(|s| s.as_str())
        .collect();
    let expected_total = num_txns * events_per_txn;
    assert_eq!(
        all_ids.len(),
        expected_total,
        "expected {expected_total} unique events, got {}",
        all_ids.len()
    );

    // Verify no overlap (since we resumed at exact boundary)
    let phase1_set: HashSet<&str> = phase1_ids.iter().map(|s| s.as_str()).collect();
    let phase2_set: HashSet<&str> = phase2_ids.iter().map(|s| s.as_str()).collect();
    let overlap = phase1_set.intersection(&phase2_set).count();
    println!("Overlap: {overlap} events (expected 0 for clean crash at txn boundary)");
    assert_eq!(overlap, 0);
}

#[tokio::test]
#[ignore]
async fn crash_during_sub_batched_transaction_replays_full_txn() {
    let total_events = 100_000;
    let sub_batch_size = 10_000;
    let crash_after_sub_batches = 5;

    let transport = LazyTransport::new(total_events, 1);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, Some(sub_batch_size));

    let mut phase1_events = 0u64;
    let mut sub_batches_seen = 0;

    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::SubBatch(sb)) => {
                phase1_events += sb.events.len() as u64;
                sub_batches_seen += 1;
                if sub_batches_seen >= crash_after_sub_batches {
                    break; // "crash" mid-transaction
                }
            }
            Some(BatchResult::Batch(_)) => break,
            _ => {}
        }
    }

    println!(
        "Phase 1: {phase1_events} events across {sub_batches_seen} sub-batches (no checkpoint)"
    );
    drop(stream);

    // Since sub-batches don't checkpoint, the entire transaction replays.
    let transport = LazyTransport::new(total_events, 1);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, Some(sub_batch_size));

    let mut phase2_events = 0u64;
    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::SubBatch(sb)) => phase2_events += sb.events.len() as u64,
            Some(BatchResult::Batch(b)) => {
                phase2_events += b.events.len() as u64;
                stream.ack();
            }
            _ => break,
        }
    }

    println!("Phase 2: {phase2_events} events (full replay)");
    assert_eq!(phase2_events, total_events as u64);
}

#[tokio::test]
#[ignore]
async fn rapid_crash_loop_every_transaction() {
    let num_txns = 20;
    let events_per_txn = 50;
    let (_dir, db) = setup_store().await;

    let mut all_ids: Vec<String> = Vec::new();
    let mut total_events = 0u64;

    for i in 0..num_txns {
        let transport = LazyTransport::starting_at(events_per_txn, 1, i);
        let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                all_ids.extend(collect_event_ids(&b));
                total_events += b.events.len() as u64;
                save_checkpoint(&db, b.ack_lsn, total_events).await;
                stream.ack();
            }
            _ => panic!("expected batch for txn {i}"),
        }

        // Verify checkpoint
        let cp = db.get_streaming_checkpoint("test").await.unwrap().unwrap();
        assert_eq!(cp.lsn, LazyTransport::lsn_for_txn(i));

        drop(stream); // "crash"
    }

    let unique: HashSet<&str> = all_ids.iter().map(|s| s.as_str()).collect();
    let expected = num_txns * events_per_txn;
    println!(
        "Rapid crash loop: {num_txns} restarts, {expected} total events, {} unique",
        unique.len()
    );
    assert_eq!(unique.len(), expected);
    assert_eq!(total_events, expected as u64);
}
