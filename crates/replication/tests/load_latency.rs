//! Latency percentile tests for the replication stream.
//! Run with: cargo test -p replication --release --test load_latency -- --ignored --nocapture

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

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn print_latency_table(label: &str, latencies_us: &mut Vec<f64>) {
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = latencies_us.len();
    let mean = latencies_us.iter().sum::<f64>() / n as f64;

    println!("\n--- {label} ({n} samples) ---");
    println!(
        "{:<8} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "", "mean", "p50", "p95", "p99", "p99.9"
    );
    println!("{}", "-".repeat(62));
    println!(
        "{:<8} {:>9.1}\u{00b5}s {:>9.1}\u{00b5}s {:>9.1}\u{00b5}s {:>9.1}\u{00b5}s {:>9.1}\u{00b5}s",
        "latency",
        mean,
        percentile(latencies_us, 50.0),
        percentile(latencies_us, 95.0),
        percentile(latencies_us, 99.0),
        percentile(latencies_us, 99.9),
    );
}

#[tokio::test]
#[ignore]
async fn batch_latency_small_transactions() {
    let num_txns = 100_000;
    let events_per_txn = 10;

    let transport = LazyTransport::new(events_per_txn, num_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let mut latencies_us = Vec::with_capacity(num_txns);

    loop {
        let t0 = Instant::now();
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                latencies_us.push(elapsed_us);
                assert_eq!(b.events.len(), events_per_txn);
                stream.ack();
            }
            Some(_) => {}
            None => break,
        }
    }

    print_latency_table("small transactions (10 events)", &mut latencies_us);
    assert_eq!(latencies_us.len(), num_txns);

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99 = percentile(&latencies_us, 99.0);
    assert!(
        p99 < 1000.0,
        "p99 latency {p99:.1}\u{00b5}s exceeds 1ms — unexpected for synthetic workload"
    );
}

#[tokio::test]
#[ignore]
async fn batch_latency_medium_transactions() {
    let num_txns = 10_000;
    let events_per_txn = 1_000;

    let transport = LazyTransport::new(events_per_txn, num_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let mut latencies_us = Vec::with_capacity(num_txns);

    loop {
        let t0 = Instant::now();
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                latencies_us.push(elapsed_us);
                assert_eq!(b.events.len(), events_per_txn);
                stream.ack();
            }
            Some(_) => {}
            None => break,
        }
    }

    print_latency_table("medium transactions (1K events)", &mut latencies_us);
    assert_eq!(latencies_us.len(), num_txns);
}

#[tokio::test]
#[ignore]
async fn sub_batch_latency() {
    let total_events = 5_000_000;
    let sub_batch_size = 10_000;

    let transport = LazyTransport::new(total_events, 1);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, Some(sub_batch_size));

    let mut latencies_us = Vec::with_capacity(total_events / sub_batch_size + 1);

    loop {
        let t0 = Instant::now();
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::SubBatch(sb)) => {
                let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                latencies_us.push(elapsed_us);
                assert_eq!(sb.events.len(), sub_batch_size);
            }
            Some(BatchResult::Batch(_b)) => {
                let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                latencies_us.push(elapsed_us);
                stream.ack();
            }
            Some(_) => {}
            None => break,
        }
    }

    print_latency_table("sub-batches (10K events each)", &mut latencies_us);
    assert!(latencies_us.len() >= total_events / sub_batch_size);
}

#[tokio::test]
#[ignore]
async fn latency_vs_batch_size() {
    println!(
        "\n{:<12} {:>10} {:>10} {:>10} {:>10}",
        "Batch size", "p50", "p95", "p99", "throughput"
    );
    println!("{}", "-".repeat(56));

    for &events_per_txn in &[1, 10, 100, 1_000, 10_000] {
        let num_txns = 100_000 / events_per_txn.max(1);
        let transport = LazyTransport::new(events_per_txn, num_txns);
        let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

        let mut latencies_us = Vec::with_capacity(num_txns);
        let mut total_events = 0u64;
        let wall_start = Instant::now();

        loop {
            let t0 = Instant::now();
            match stream.recv_batch().await.unwrap() {
                Some(BatchResult::Batch(b)) => {
                    let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                    latencies_us.push(elapsed_us);
                    total_events += b.events.len() as u64;
                    stream.ack();
                }
                Some(_) => {}
                None => break,
            }
        }

        let wall_elapsed = wall_start.elapsed();
        let throughput = total_events as f64 / wall_elapsed.as_secs_f64();

        latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "{:<12} {:>9.1}\u{00b5}s {:>9.1}\u{00b5}s {:>9.1}\u{00b5}s {:>8.0} ev/s",
            events_per_txn,
            percentile(&latencies_us, 50.0),
            percentile(&latencies_us, 95.0),
            percentile(&latencies_us, 99.0),
            throughput,
        );
    }
}
