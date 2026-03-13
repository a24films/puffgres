//! Load test for sub-batching at scale.
//! Run with: cargo test -p replication --test load_sub_batch -- --ignored --nocapture

use std::sync::Mutex;
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use pgwire_replication::{Lsn, ReplicationEvent};
use replication::stream::{BatchResult, ReplicationStream};
use replication::{ReplicationTransport, Result};

struct LazyTransport {
    events_per_txn: usize,
    num_txns: usize,
    state: LazyState,
    txn_cursor: usize,
    txn_index: usize,
    relation_sent: bool,
    acked: Mutex<Vec<u64>>,
}

enum LazyState {
    Ready,
    InTxn,
    Done,
}

impl LazyTransport {
    fn new(events_per_txn: usize, num_txns: usize) -> Self {
        Self {
            events_per_txn,
            num_txns,
            state: LazyState::Ready,
            txn_cursor: 0,
            txn_index: 0,
            relation_sent: false,
            acked: Mutex::new(Vec::new()),
        }
    }

    fn lsn_for_txn(&self, txn_index: usize) -> u64 {
        ((txn_index + 1) * 1_000_000) as u64
    }
}

impl ReplicationTransport for LazyTransport {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        if !self.relation_sent {
            self.relation_sent = true;
            return Ok(Some(relation_event()));
        }

        match self.state {
            LazyState::Done => Ok(None),
            LazyState::Ready => {
                if self.txn_index >= self.num_txns {
                    self.state = LazyState::Done;
                    return Ok(None);
                }
                self.txn_cursor = 0;
                self.state = LazyState::InTxn;
                Ok(Some(ReplicationEvent::Begin {
                    final_lsn: Lsn(0),
                    xid: (self.txn_index + 1) as u32,
                    commit_time_micros: 0,
                }))
            }
            LazyState::InTxn => {
                if self.txn_cursor < self.events_per_txn {
                    self.txn_cursor += 1;
                    Ok(Some(insert_event()))
                } else {
                    let lsn = self.lsn_for_txn(self.txn_index);
                    self.txn_index += 1;
                    self.state = LazyState::Ready;
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

struct DrainStats {
    total_events: u64,
    sub_batch_count: u64,
    batch_count: u64,
    last_ack_lsn: u64,
    peak_rss_bytes: usize,
}

async fn drain_stream(
    stream: &mut ReplicationStream<LazyTransport>,
    sample_interval: usize,
) -> DrainStats {
    let mut stats = DrainStats {
        total_events: 0,
        sub_batch_count: 0,
        batch_count: 0,
        last_ack_lsn: 0,
        peak_rss_bytes: current_rss_bytes(),
    };
    let mut calls = 0u64;

    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::SubBatch(sb)) => {
                stats.total_events += sb.events.len() as u64;
                stats.sub_batch_count += 1;
            }
            Some(BatchResult::Batch(b)) => {
                stats.total_events += b.events.len() as u64;
                stats.batch_count += 1;
                stats.last_ack_lsn = b.ack_lsn;
                stream.ack();
            }
            Some(BatchResult::SchemaChanged(_)) => {}
            Some(BatchResult::TransactionTooLarge { ack_lsn, .. }) => {
                stats.last_ack_lsn = ack_lsn;
                stream.ack();
            }
            None => break,
        }

        calls += 1;
        if sample_interval > 0 && calls % sample_interval as u64 == 0 {
            let rss = current_rss_bytes();
            if rss > stats.peak_rss_bytes {
                stats.peak_rss_bytes = rss;
            }
        }
    }

    let rss = current_rss_bytes();
    if rss > stats.peak_rss_bytes {
        stats.peak_rss_bytes = rss;
    }
    stats
}

fn print_stats(name: &str, stats: &DrainStats, elapsed: std::time::Duration, baseline_rss: usize) {
    let events_per_sec = stats.total_events as f64 / elapsed.as_secs_f64();
    println!("--- {name} ---");
    println!("  Total events:  {}", stats.total_events);
    println!("  Sub-batches:   {}", stats.sub_batch_count);
    println!("  Batches:       {}", stats.batch_count);
    println!("  Elapsed:       {elapsed:.2?}");
    println!("  Throughput:    {events_per_sec:.0} events/sec");
    println!("  Peak RSS:      {:.1} MB", mb(stats.peak_rss_bytes));
    if baseline_rss > 0 && stats.peak_rss_bytes > 0 {
        println!(
            "  RSS delta:     {:.1} MB",
            mb(stats.peak_rss_bytes.saturating_sub(baseline_rss))
        );
    }
}

fn assert_rss_bounded(stats: &DrainStats, baseline_rss: usize, max_delta_mb: f64) {
    if baseline_rss > 0 && stats.peak_rss_bytes > 0 {
        let delta_mb = mb(stats.peak_rss_bytes.saturating_sub(baseline_rss));
        assert!(
            delta_mb < max_delta_mb,
            "RSS grew by {delta_mb:.1} MB (limit: {max_delta_mb} MB)"
        );
    }
}

#[tokio::test]
#[ignore]
async fn sub_batch_1m_events() {
    let transport = LazyTransport::new(1_000_000, 1);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, Some(10_000));

    let start = Instant::now();
    let stats = drain_stream(&mut stream, 10).await;
    print_stats("sub_batch_1m_events", &stats, start.elapsed(), 0);

    assert_eq!(stats.total_events, 1_000_000);
    assert!(stats.sub_batch_count > 0);
    assert_eq!(stats.batch_count, 1);
}

#[tokio::test]
#[ignore]
async fn sub_batch_10m_events() {
    let baseline_rss = current_rss_bytes();
    let transport = LazyTransport::new(10_000_000, 1);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, Some(100_000));

    let start = Instant::now();
    let stats = drain_stream(&mut stream, 10).await;
    print_stats(
        "sub_batch_10m_events",
        &stats,
        start.elapsed(),
        baseline_rss,
    );

    assert_eq!(stats.total_events, 10_000_000);
    assert!(stats.sub_batch_count >= 99);
    assert_eq!(stats.batch_count, 1);
    assert_rss_bounded(&stats, baseline_rss, 500.0);
}

#[tokio::test]
#[ignore]
async fn sub_batch_100m_events() {
    let baseline_rss = current_rss_bytes();
    let transport = LazyTransport::new(100_000_000, 1);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, Some(100_000));

    let start = Instant::now();
    let stats = drain_stream(&mut stream, 100).await;
    print_stats(
        "sub_batch_100m_events",
        &stats,
        start.elapsed(),
        baseline_rss,
    );

    assert_eq!(stats.total_events, 100_000_000);
    assert!(stats.sub_batch_count >= 999);
    assert_eq!(stats.batch_count, 1);
    assert_eq!(stats.last_ack_lsn, 1_000_000);
    assert_rss_bounded(&stats, baseline_rss, 500.0);
}

#[tokio::test]
#[ignore]
async fn many_small_transactions_no_leak() {
    let num_txns: usize = 100_000;
    let events_per_txn: usize = 10;
    let baseline_rss = current_rss_bytes();

    let transport = LazyTransport::new(events_per_txn, num_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let start = Instant::now();
    let stats = drain_stream(&mut stream, 1000).await;
    let elapsed = start.elapsed();
    let txns_per_sec = num_txns as f64 / elapsed.as_secs_f64();

    print_stats("many_small_transactions", &stats, elapsed, baseline_rss);
    println!("  Txn throughput: {txns_per_sec:.0} txns/sec");

    assert_eq!(stats.total_events, (num_txns * events_per_txn) as u64);
    assert_eq!(stats.batch_count, num_txns as u64);
    assert_eq!(stats.sub_batch_count, 0);
    assert_rss_bounded(&stats, baseline_rss, 100.0);
}

#[tokio::test]
#[ignore]
async fn transaction_too_large_clears_memory() {
    let baseline_rss = current_rss_bytes();
    let transport = LazyTransport::new(1_000_000, 1);
    let mut stream = ReplicationStream::from_transport(transport, 100_000, None);

    let stats = drain_stream(&mut stream, 10).await;
    print_stats(
        "transaction_too_large_clears_memory",
        &stats,
        std::time::Duration::ZERO,
        baseline_rss,
    );

    assert_eq!(stats.total_events, 0);
    assert_eq!(stats.batch_count, 0);
    assert!(stats.last_ack_lsn > 0);
    assert_rss_bounded(&stats, baseline_rss, 200.0);
}
