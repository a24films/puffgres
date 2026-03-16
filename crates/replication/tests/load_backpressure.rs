//! Backpressure tests: measure throughput degradation when the consumer is slower than the producer.
//! Run with: cargo test -p replication --release --test load_backpressure -- --ignored --nocapture

use std::sync::Mutex;
use std::time::{Duration, Instant};

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

/// Simulate a slow consumer by sleeping between batch acks.
/// Measures how throughput degrades and whether memory stays bounded.
#[tokio::test]
#[ignore]
async fn slow_consumer_throughput_degradation() {
    let events_per_txn = 100;
    let num_txns = 10_000;

    println!(
        "\n{:<14} {:>12} {:>14} {:>10} {:>10}",
        "Consumer delay", "Elapsed", "Throughput", "Peak RSS", "RSS delta"
    );
    println!("{}", "-".repeat(64));

    for &delay_us in &[0u64, 10, 50, 100, 500] {
        let baseline_rss = current_rss_bytes();
        let transport = LazyTransport::new(events_per_txn, num_txns);
        let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

        let mut total_events = 0u64;
        let mut peak_rss = baseline_rss;
        let start = Instant::now();

        loop {
            match stream.recv_batch().await.unwrap() {
                Some(BatchResult::Batch(b)) => {
                    total_events += b.events.len() as u64;
                    stream.ack();

                    if delay_us > 0 {
                        tokio::time::sleep(Duration::from_micros(delay_us)).await;
                    }

                    if total_events % 100_000 == 0 {
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

        let throughput = total_events as f64 / elapsed.as_secs_f64();
        let delta = mb(peak_rss.saturating_sub(baseline_rss));

        println!(
            "{:<14} {:>12.2?} {:>12.0} ev/s {:>8.1} MB {:>8.1} MB",
            format!("{delay_us}\u{00b5}s"),
            elapsed,
            throughput,
            mb(peak_rss),
            delta,
        );

        assert_eq!(total_events, (events_per_txn * num_txns) as u64);
    }
}

/// Verify memory stays bounded when consumer pauses for extended periods mid-stream.
/// Simulates a consumer that stalls (e.g. waiting on a downstream write) periodically.
#[tokio::test]
#[ignore]
async fn stall_recovery_memory_bounded() {
    let events_per_txn = 100;
    let num_txns = 50_000;
    let stall_every = 10_000; // stall every N batches
    let stall_duration = Duration::from_millis(100);

    let baseline_rss = current_rss_bytes();
    let transport = LazyTransport::new(events_per_txn, num_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    let mut total_events = 0u64;
    let mut batch_count = 0u64;
    let mut peak_rss = baseline_rss;
    let mut stall_count = 0u64;
    let start = Instant::now();

    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                total_events += b.events.len() as u64;
                batch_count += 1;
                stream.ack();

                if batch_count % stall_every as u64 == 0 {
                    tokio::time::sleep(stall_duration).await;
                    stall_count += 1;
                }

                if batch_count % 5_000 == 0 {
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
    let delta = mb(peak_rss.saturating_sub(baseline_rss));

    println!("--- stall recovery ---");
    println!("  Duration:    {elapsed:.2?}");
    println!("  Events:      {total_events}");
    println!("  Batches:     {batch_count}");
    println!("  Stalls:      {stall_count} x {stall_duration:?}");
    println!(
        "  Throughput:  {:.0} events/sec",
        total_events as f64 / elapsed.as_secs_f64()
    );
    println!("  Peak RSS:    {:.1} MB", mb(peak_rss));
    println!("  RSS delta:   {delta:.1} MB");

    assert_eq!(total_events, (events_per_txn * num_txns) as u64);
    assert!(
        delta < 100.0,
        "RSS grew by {delta:.1} MB during stalls — memory not bounded"
    );
}

/// Burst-then-drain: feed a large backlog and measure how quickly we catch up.
#[tokio::test]
#[ignore]
async fn burst_catchup_latency() {
    let burst_txns = 50_000;
    let events_per_txn = 10;

    let transport = LazyTransport::new(events_per_txn, burst_txns);
    let mut stream = ReplicationStream::from_transport(transport, usize::MAX, None);

    // Drain as fast as possible and measure time-to-clear
    let start = Instant::now();
    let mut total_events = 0u64;
    let mut batch_count = 0u64;

    // Track per-10K-batch throughput windows
    let mut window_start = Instant::now();
    let mut window_events = 0u64;
    let mut windows = Vec::new();

    loop {
        match stream.recv_batch().await.unwrap() {
            Some(BatchResult::Batch(b)) => {
                total_events += b.events.len() as u64;
                window_events += b.events.len() as u64;
                batch_count += 1;
                stream.ack();

                if batch_count % 10_000 == 0 {
                    let window_elapsed = window_start.elapsed();
                    let window_throughput = window_events as f64 / window_elapsed.as_secs_f64();
                    windows.push((batch_count, window_throughput));
                    window_start = Instant::now();
                    window_events = 0;
                }
            }
            Some(_) => {}
            None => break,
        }
    }

    let elapsed = start.elapsed();
    let overall_throughput = total_events as f64 / elapsed.as_secs_f64();

    println!("--- burst catchup ---");
    println!("  Backlog:      {burst_txns} txns ({total_events} events)");
    println!("  Drain time:   {elapsed:.2?}");
    println!("  Throughput:   {overall_throughput:.0} events/sec");
    println!();

    println!("  {:<12} {:>14}", "Batch window", "Throughput");
    println!("  {}", "-".repeat(28));
    for (batch, throughput) in &windows {
        println!("  {:<12} {:>12.0} ev/s", format!("0-{batch}"), throughput);
    }

    // Throughput should not drop significantly across windows
    if windows.len() >= 2 {
        let first = windows[0].1;
        let last = windows[windows.len() - 1].1;
        let ratio = last / first;
        println!("\n  First/last window ratio: {ratio:.2}");
        assert!(
            ratio > 0.5,
            "Throughput dropped to {:.0}% of initial — degradation too steep",
            ratio * 100.0
        );
    }
}
