use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use replication::decoder;

fn cstring(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

fn tuple(cols: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
    let mut buf = (cols.len() as u16).to_be_bytes().to_vec();
    for &(tag, data) in cols {
        buf.extend_from_slice(tag);
        if let Some(d) = data {
            buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
            buf.extend_from_slice(d);
        }
    }
    buf
}

fn make_begin() -> Bytes {
    let mut buf = vec![b'B'];
    buf.extend_from_slice(&100u64.to_be_bytes());
    buf.extend_from_slice(&200i64.to_be_bytes());
    buf.extend_from_slice(&42u32.to_be_bytes());
    Bytes::from(buf)
}

fn make_relation(ncols: usize) -> Bytes {
    let mut buf = vec![b'R'];
    buf.extend_from_slice(&16384u32.to_be_bytes());
    buf.extend_from_slice(&cstring("public"));
    buf.extend_from_slice(&cstring("users"));
    buf.push(b'd');
    buf.extend_from_slice(&(ncols as u16).to_be_bytes());
    for i in 0..ncols {
        buf.push(if i == 0 { 1 } else { 0 });
        buf.extend_from_slice(&cstring(&format!("col_{i}")));
        buf.extend_from_slice(&23u32.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes());
    }
    Bytes::from(buf)
}

fn make_insert(ncols: usize, value_len: usize) -> Bytes {
    let value = "x".repeat(value_len);
    let cols: Vec<(&[u8], Option<&[u8]>)> = (0..ncols)
        .map(|_| (b"t" as &[u8], Some(value.as_bytes())))
        .collect();
    let td = tuple(&cols);
    let mut buf = vec![b'I'];
    buf.extend_from_slice(&16384u32.to_be_bytes());
    buf.push(b'N');
    buf.extend_from_slice(&td);
    Bytes::from(buf)
}

fn make_commit() -> Bytes {
    let mut buf = vec![b'C', 0];
    buf.extend_from_slice(&100u64.to_be_bytes());
    buf.extend_from_slice(&200u64.to_be_bytes());
    buf.extend_from_slice(&300i64.to_be_bytes());
    Bytes::from(buf)
}

fn bench_decode_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("decoder");

    let begin = make_begin();
    group.bench_function("begin", |b| {
        b.iter(|| decoder::decode(black_box(begin.clone())))
    });

    let commit = make_commit();
    group.bench_function("commit", |b| {
        b.iter(|| decoder::decode(black_box(commit.clone())))
    });

    let relation_5 = make_relation(5);
    group.bench_function("relation_5cols", |b| {
        b.iter(|| decoder::decode(black_box(relation_5.clone())))
    });

    let relation_50 = make_relation(50);
    group.bench_function("relation_50cols", |b| {
        b.iter(|| decoder::decode(black_box(relation_50.clone())))
    });

    let insert_5_short = make_insert(5, 10);
    group.bench_function("insert_5cols_10bytes", |b| {
        b.iter(|| decoder::decode(black_box(insert_5_short.clone())))
    });

    let insert_5_long = make_insert(5, 1000);
    group.bench_function("insert_5cols_1kb", |b| {
        b.iter(|| decoder::decode(black_box(insert_5_long.clone())))
    });

    let insert_50 = make_insert(50, 100);
    group.bench_function("insert_50cols_100bytes", |b| {
        b.iter(|| decoder::decode(black_box(insert_50.clone())))
    });

    group.finish();
}

fn bench_decode_batch(c: &mut Criterion) {
    // Simulate decoding a realistic batch: 100 inserts with 10 columns each
    let messages: Vec<Bytes> = (0..100).map(|_| make_insert(10, 50)).collect();

    c.bench_function("batch_100_inserts_10cols", |b| {
        b.iter(|| {
            for msg in &messages {
                let _ = decoder::decode(black_box(msg.clone()));
            }
        })
    });
}

criterion_group!(benches, bench_decode_messages, bench_decode_batch);
criterion_main!(benches);
