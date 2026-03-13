//! OOM boundary sweep: measure memory scaling without sub-batching,
//! verify TransactionTooLarge fires at exact boundaries, and check for leaks.
//! Run with: cargo test -p replication --test load_oom_boundary -- --ignored --nocapture

use std::sync::Mutex;
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use pgwire_replication::{Lsn, ReplicationEvent};
use replication::stream::{BatchResult, ReplicationStream};
use replication::{ReplicationTransport, Result};

struct LazyTransport {
    events_per_txn: usize,
    num_txns: usize,
    state: TransportState,
    txn_cursor: usize,
    txn_index: usize,
    relation_sent: bool,
    acked: Mutex<Vec<u64>>,
}

enum TransportState {
    Ready,
    InTxn,
    Done,
}

impl LazyTransport {
    fn new(events_per_txn: usize, num_txns: usize) -> Self {
        Self {
            events_per_txn,
            num_txns,
            state: TransportState::Ready,
            txn_cursor: 0,
            txn_index: 0,
            relation_sent: false,
            acked: Mutex::new(Vec::new()),
        }
    }
}

impl ReplicationTransport for LazyTransport {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        if !self.relation_sent {
            self.relation_sent = true;
            return Ok(Some(relation_event()));
        }
        match self.state {
            TransportState::Done => Ok(None),
            TransportState::Ready => {
                if self.txn_index >= self.num_txns {
                    self.state = TransportState::Done;
                    return Ok(None);
                }
                self.txn_cursor = 0;
                self.state = TransportState::InTxn;
                Ok(Some(ReplicationEvent::Begin {
                    final_lsn: Lsn(0),
                    xid: (self.txn_index + 1) as u32,
                    commit_time_micros: 0,
                }))
            }
            TransportState::InTxn => {
                if self.txn_cursor < self.events_per_txn {
                    self.txn_cursor += 1;
                    Ok(Some(insert_event()))
                } else {
                    let lsn = ((self.txn_index + 1) * 1_000_000) as u64;
                    self.txn_index += 1;
                    self.state = TransportState::Ready;
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
    buf.put_u16(1);
    buf.put_u8(1);
    buf.put_slice(b"id\0");
    buf.put_u32(23);
    buf.put_i32(-1);
    ReplicationEvent::XLogData {
        wal_start: Lsn(0),
        wal_end: Lsn(0),
        server_time_micros: 0,
        data: buf.freeze(),
    }
}

fn insert_event() -> ReplicationEvent {
    let mut buf = BytesMut::new();
    buf.put_u8(b'I');
    buf.put_u32(1);
    buf.put_u8(b'N');
    buf.put_u16(1);
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

fn current_rss_bytes() -> usize {
    use sysinfo::{Pid, System};
    let pid = Pid::from_u32(std::process::id());
    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid).map_or(0, |p| p.memory() as usize)
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::test]
#[ignore]
async fn rss_scaling_sweep() {
    println!(
        "\n{:<12} {:>10} {:>10} {:>12}",
        "Events", "RSS (MB)", "Delta", "Bytes/event"
    );
    println!("{}", "-".repeat(48));

    for &n in &[10_000usize, 100_000, 1_000_000, 5_000_000] {
        let baseline = current_rss_bytes();
        let transport = LazyTransport::new(n, 1);
        let mut stream = ReplicationStream::from_transport(transport, n + 1, None);

        let mut total = 0u64;
        loop {
            match stream.recv_batch().await.unwrap() {
                Some(BatchResult::Batch(b)) => {
                    total += b.events.len() as u64;
                    stream.ack();
                }
                Some(_) => {}
                None => break,
            }
        }

        let peak = current_rss_bytes();
        let delta = peak.saturating_sub(baseline);
        let per_event = if total > 0 { delta / total as usize } else { 0 };

        println!(
            "{:<12} {:>10.1} {:>10.1} {:>12}",
            n,
            mb(peak),
            mb(delta),
            per_event
        );

        assert_eq!(total, n as u64);
    }
}

#[tokio::test]
#[ignore]
async fn transaction_too_large_boundary() {
    println!(
        "\n{:<10} {:<12} {:<12} {:<10}",
        "Limit", "At limit", "Over limit", "Ack LSN"
    );
    println!("{}", "-".repeat(46));

    for &limit in &[100usize, 1_000, 10_000, 100_000] {
        // Exactly at limit: should produce Batch
        let transport = LazyTransport::new(limit, 1);
        let mut stream = ReplicationStream::from_transport(transport, limit, None);
        let mut got_batch = false;
        loop {
            match stream.recv_batch().await.unwrap() {
                Some(BatchResult::Batch(b)) => {
                    assert_eq!(b.events.len(), limit);
                    got_batch = true;
                    stream.ack();
                }
                Some(_) => {}
                None => break,
            }
        }
        assert!(got_batch, "limit={limit}: expected Batch");

        // One over limit: should produce TransactionTooLarge
        let transport = LazyTransport::new(limit + 1, 1);
        let mut stream = ReplicationStream::from_transport(transport, limit, None);
        let mut got_too_large = false;
        let mut ack_lsn = 0u64;
        loop {
            match stream.recv_batch().await.unwrap() {
                Some(BatchResult::TransactionTooLarge {
                    ack_lsn: lsn,
                    event_count,
                }) => {
                    assert_eq!(event_count, limit);
                    ack_lsn = lsn;
                    got_too_large = true;
                    stream.ack();
                }
                Some(_) => {}
                None => break,
            }
        }
        assert!(got_too_large, "limit={limit}: expected TransactionTooLarge");
        assert!(ack_lsn > 0);

        println!(
            "{:<10} {:<12} {:<12} {:<10}",
            limit, "Batch", "TooLarge", ack_lsn
        );
    }
}

#[tokio::test]
#[ignore]
async fn rapid_small_transactions_flat_memory() {
    let num_txns = 50_000usize;
    let baseline = current_rss_bytes();

    let transport = LazyTransport::new(1, num_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let start = Instant::now();
    let mut batch_count = 0u64;
    let mut peak_rss = baseline;

    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(_)) => {
                batch_count += 1;
                stream.ack();
                if batch_count % 10_000 == 0 {
                    let rss = current_rss_bytes();
                    if rss > peak_rss {
                        peak_rss = rss;
                    }
                }
            }
            Some(_) => {}
            None => break,
        }
    }

    let elapsed = start.elapsed();
    let final_rss = current_rss_bytes();
    if final_rss > peak_rss {
        peak_rss = final_rss;
    }

    println!("--- rapid_small_transactions ---");
    println!("  Transactions: {batch_count}");
    println!("  Elapsed:      {elapsed:.2?}");
    println!(
        "  Throughput:   {:.0} txns/sec",
        batch_count as f64 / elapsed.as_secs_f64()
    );
    println!("  Baseline RSS: {:.1} MB", mb(baseline));
    println!("  Peak RSS:     {:.1} MB", mb(peak_rss));
    println!(
        "  RSS delta:    {:.1} MB",
        mb(peak_rss.saturating_sub(baseline))
    );

    assert_eq!(batch_count, num_txns as u64);
    if baseline > 0 && peak_rss > 0 {
        let delta = mb(peak_rss.saturating_sub(baseline));
        assert!(delta < 50.0, "RSS grew by {delta:.1} MB — possible leak");
    }
}
