use std::collections::VecDeque;
use std::sync::Mutex;

use bytes::{BufMut, BytesMut};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pgwire_replication::{Lsn, ReplicationEvent};
use replication::stream::{BatchResult, ReplicationStream};
use replication::{ReplicationTransport, Result};

struct MockTransport {
    events: VecDeque<Result<Option<ReplicationEvent>>>,
    acked: Mutex<Vec<u64>>,
}

impl MockTransport {
    fn new(events: Vec<Result<Option<ReplicationEvent>>>) -> Self {
        Self {
            events: events.into(),
            acked: Mutex::new(Vec::new()),
        }
    }
}

impl ReplicationTransport for MockTransport {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        self.events.pop_front().unwrap_or(Ok(None))
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
    buf.put_u32(2);
    buf.put_slice(b"42");
    ReplicationEvent::XLogData {
        wal_start: Lsn(0),
        wal_end: Lsn(0),
        server_time_micros: 0,
        data: buf.freeze(),
    }
}

fn build_txn(n: usize, lsn: u64) -> Vec<Result<Option<ReplicationEvent>>> {
    let mut v = Vec::with_capacity(n + 2);
    v.push(Ok(Some(ReplicationEvent::Begin {
        final_lsn: Lsn(0),
        xid: 1,
        commit_time_micros: 0,
    })));
    for _ in 0..n {
        v.push(Ok(Some(insert_event())));
    }
    v.push(Ok(Some(ReplicationEvent::Commit {
        lsn: Lsn(lsn),
        end_lsn: Lsn(lsn),
        commit_time_micros: 0,
    })));
    v
}

fn bench_recv_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("stream");

    for n in [10, 100, 1000] {
        group.bench_function(format!("recv_batch_{n}_events"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut all = vec![Ok(Some(relation_event()))];
                    all.extend(build_txn(n, 100));
                    let mut stream =
                        ReplicationStream::from_transport(MockTransport::new(all), 1_000_000, None);
                    black_box(stream.recv_batch().await.unwrap());
                })
            })
        });
    }

    group.bench_function("recv_batch_1000_sub100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut all = vec![Ok(Some(relation_event()))];
                all.extend(build_txn(1000, 100));
                let mut stream = ReplicationStream::from_transport(
                    MockTransport::new(all),
                    1_000_000,
                    Some(100),
                );
                loop {
                    match stream.recv_batch().await.unwrap() {
                        Some(BatchResult::Batch(_)) | None => break,
                        Some(_) => continue,
                    }
                }
            })
        })
    });

    group.finish();
}

criterion_group!(benches, bench_recv_batch);
criterion_main!(benches);
