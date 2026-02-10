//! pgoutput binary protocol decoder.
//!
//! Parses raw WAL data from pgwire-replication's `XLogData.data` into typed
//! messages. We write the decoder ourselves because `postgres-protocol` does
//! not expose pgoutput parsing in its public API, and `pg-walstream` requires
//! `libpq-sys` which we avoid.
//!
//! The protocol is straightforward: each message starts with a type byte
//! followed by fixed-width and null-terminated fields in network byte order.
//! We parse with `bytes::Buf` directly — no wrapper crate needed.

use bytes::{Buf, Bytes};

use crate::event::{ColumnValue, TupleData};
use crate::relation::{ColumnInfo, RelationInfo, ReplicaIdentity};
use crate::{ReplicationError, Result};

/// A decoded pgoutput protocol message.
#[derive(Debug)]
pub enum WalMessage {
    Begin(Begin),
    Commit(Commit),
    Relation(RelationInfo),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate(Truncate),
    /// Origin, Type, and LogicalDecodingMessage are uncommon in CDC pipelines.
    /// We skip their contents rather than error on them.
    Other(u8),
}

#[derive(Debug)]
pub struct Begin {
    pub final_lsn: u64,
    /// Microseconds since 2000-01-01 00:00:00 UTC.
    pub timestamp: i64,
    pub xid: u32,
}

#[derive(Debug)]
pub struct Commit {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    /// Microseconds since 2000-01-01 00:00:00 UTC.
    pub timestamp: i64,
}

#[derive(Debug)]
pub struct Insert {
    pub relation_id: u32,
    pub tuple: TupleData,
}

#[derive(Debug)]
pub struct Update {
    pub relation_id: u32,
    pub old_tuple: Option<TupleData>,
    pub new_tuple: TupleData,
}

#[derive(Debug)]
pub struct Delete {
    pub relation_id: u32,
    pub old_tuple: TupleData,
}

#[derive(Debug)]
pub struct Truncate {
    pub option_bits: u8,
    pub relation_ids: Vec<u32>,
}

/// Decode a single pgoutput message from raw WAL bytes.
pub fn decode(mut data: Bytes) -> Result<WalMessage> {
    if !data.has_remaining() {
        return Err(ReplicationError::Decoder("empty message".into()));
    }

    let tag = data.get_u8();
    match tag {
        b'B' => decode_begin(&mut data),
        b'C' => decode_commit(&mut data),
        b'R' => decode_relation(&mut data),
        b'I' => decode_insert(&mut data),
        b'U' => decode_update(&mut data),
        b'D' => decode_delete(&mut data),
        b'T' => decode_truncate(&mut data),
        // Origin ('O'), Type ('Y'), LogicalDecodingMessage ('M') are rare
        // in CDC pipelines. Skip rather than error.
        other => Ok(WalMessage::Other(other)),
    }
}

// ---------------------------------------------------------------------------
// Message decoders
// ---------------------------------------------------------------------------

fn decode_begin(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 20)?;
    Ok(WalMessage::Begin(Begin {
        final_lsn: buf.get_u64(),
        timestamp: buf.get_i64(),
        xid: buf.get_u32(),
    }))
}

fn decode_commit(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 25)?;
    Ok(WalMessage::Commit(Commit {
        flags: buf.get_u8(),
        commit_lsn: buf.get_u64(),
        end_lsn: buf.get_u64(),
        timestamp: buf.get_i64(),
    }))
}

fn decode_relation(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 4)?;
    let id = buf.get_u32();
    let namespace = read_cstring(buf)?;
    let name = read_cstring(buf)?;
    need(buf, 3)?;
    let replica_identity = ReplicaIdentity::from_pgoutput_byte(buf.get_u8());
    let ncols = buf.get_u16() as usize;

    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        need(buf, 1)?;
        let flags = buf.get_u8();
        let col_name = read_cstring(buf)?;
        need(buf, 8)?;
        columns.push(ColumnInfo {
            part_of_key: flags & 1 != 0,
            name: col_name,
            type_oid: buf.get_u32(),
            type_modifier: buf.get_i32(),
        });
    }

    Ok(WalMessage::Relation(RelationInfo {
        id,
        namespace,
        name,
        replica_identity,
        columns,
    }))
}

fn decode_insert(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 5)?;
    let relation_id = buf.get_u32();
    expect_marker(buf, b'N')?;
    let tuple = read_tuple(buf)?;
    Ok(WalMessage::Insert(Insert { relation_id, tuple }))
}

fn decode_update(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 5)?;
    let relation_id = buf.get_u32();
    let marker = buf.get_u8();

    let (old_tuple, new_tuple) = match marker {
        // 'K' = old key, 'O' = old full row — both followed by 'N' + new tuple.
        b'K' | b'O' => {
            let old = read_tuple(buf)?;
            need(buf, 1)?;
            expect_marker(buf, b'N')?;
            (Some(old), read_tuple(buf)?)
        }
        // 'N' = no old data, just new tuple.
        b'N' => (None, read_tuple(buf)?),
        _ => {
            return Err(ReplicationError::Decoder(format!(
                "unexpected update marker: 0x{marker:02X}"
            )));
        }
    };

    Ok(WalMessage::Update(Update {
        relation_id,
        old_tuple,
        new_tuple,
    }))
}

fn decode_delete(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 5)?;
    let relation_id = buf.get_u32();
    let marker = buf.get_u8();
    match marker {
        b'K' | b'O' => Ok(WalMessage::Delete(Delete {
            relation_id,
            old_tuple: read_tuple(buf)?,
        })),
        _ => Err(ReplicationError::Decoder(format!(
            "unexpected delete marker: 0x{marker:02X}"
        ))),
    }
}

fn decode_truncate(buf: &mut Bytes) -> Result<WalMessage> {
    need(buf, 5)?;
    let nrels = buf.get_u32() as usize;
    let option_bits = buf.get_u8();
    need(buf, nrels * 4)?;
    let relation_ids = (0..nrels).map(|_| buf.get_u32()).collect();
    Ok(WalMessage::Truncate(Truncate {
        option_bits,
        relation_ids,
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn need(buf: &Bytes, n: usize) -> Result<()> {
    if buf.remaining() < n {
        Err(ReplicationError::Decoder(format!(
            "need {n} bytes, {} remaining",
            buf.remaining()
        )))
    } else {
        Ok(())
    }
}

fn expect_marker(buf: &mut Bytes, expected: u8) -> Result<()> {
    let got = buf.get_u8();
    if got != expected {
        Err(ReplicationError::Decoder(format!(
            "expected marker '{}', got 0x{got:02X}",
            expected as char
        )))
    } else {
        Ok(())
    }
}

/// Read a null-terminated UTF-8 string.
fn read_cstring(buf: &mut Bytes) -> Result<String> {
    let chunk = buf.chunk();
    let pos = chunk
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ReplicationError::Decoder("unterminated string".into()))?;
    let s = std::str::from_utf8(&chunk[..pos])
        .map_err(|e| ReplicationError::Decoder(format!("invalid UTF-8: {e}")))?
        .to_owned();
    buf.advance(pos + 1);
    Ok(s)
}

/// Read a TupleData (column count + per-column values).
fn read_tuple(buf: &mut Bytes) -> Result<TupleData> {
    need(buf, 2)?;
    let ncols = buf.get_u16() as usize;
    let mut columns = Vec::with_capacity(ncols);

    for _ in 0..ncols {
        need(buf, 1)?;
        let col = match buf.get_u8() {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::Unchanged,
            b't' => {
                need(buf, 4)?;
                let len = buf.get_u32() as usize;
                need(buf, len)?;
                ColumnValue::Text(buf.copy_to_bytes(len))
            }
            b'b' => {
                need(buf, 4)?;
                let len = buf.get_u32() as usize;
                need(buf, len)?;
                ColumnValue::Binary(buf.copy_to_bytes(len))
            }
            other => {
                return Err(ReplicationError::Decoder(format!(
                    "unexpected column type: 0x{other:02X}"
                )));
            }
        };
        columns.push(col);
    }

    Ok(TupleData { columns })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cstring(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    fn tuple_bytes(cols: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(cols.len() as u16).to_be_bytes());
        for &(tag, data) in cols {
            buf.extend_from_slice(tag);
            if let Some(d) = data {
                buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
                buf.extend_from_slice(d);
            }
        }
        buf
    }

    #[test]
    fn empty_message_errors() {
        assert!(decode(Bytes::new()).is_err());
    }

    #[test]
    fn unknown_tag_returns_other() {
        let msg = decode(Bytes::from_static(&[0xFF])).unwrap();
        assert!(matches!(msg, WalMessage::Other(0xFF)));
    }

    #[test]
    fn begin() {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&100u64.to_be_bytes());
        buf.extend_from_slice(&200i64.to_be_bytes());
        buf.extend_from_slice(&42u32.to_be_bytes());

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Begin(b) => {
                assert_eq!(b.final_lsn, 100);
                assert_eq!(b.timestamp, 200);
                assert_eq!(b.xid, 42);
            }
            _ => panic!("expected Begin"),
        }
    }

    #[test]
    fn commit() {
        let mut buf = vec![b'C', 0];
        buf.extend_from_slice(&100u64.to_be_bytes());
        buf.extend_from_slice(&200u64.to_be_bytes());
        buf.extend_from_slice(&300i64.to_be_bytes());

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Commit(c) => {
                assert_eq!(c.commit_lsn, 100);
                assert_eq!(c.end_lsn, 200);
                assert_eq!(c.timestamp, 300);
            }
            _ => panic!("expected Commit"),
        }
    }

    #[test]
    fn relation() {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.extend_from_slice(&cstring("public"));
        buf.extend_from_slice(&cstring("users"));
        buf.push(b'd');
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.push(1); // part of key
        buf.extend_from_slice(&cstring("id"));
        buf.extend_from_slice(&23u32.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes());

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Relation(r) => {
                assert_eq!(r.id, 16384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert!(r.columns[0].part_of_key);
                assert_eq!(r.columns[0].type_oid, 23);
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn insert() {
        let td = tuple_bytes(&[(b"t", Some(b"42")), (b"t", Some(b"alice"))]);
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Insert(ins) => {
                assert_eq!(ins.relation_id, 16384);
                assert_eq!(ins.tuple.columns.len(), 2);
                assert_eq!(ins.tuple.columns[0].as_bytes().unwrap(), b"42");
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn update_without_old() {
        let td = tuple_bytes(&[(b"t", Some(b"new"))]);
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Update(u) => {
                assert!(u.old_tuple.is_none());
                assert_eq!(u.new_tuple.columns[0].as_bytes().unwrap(), b"new");
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn update_with_old_key() {
        let old = tuple_bytes(&[(b"t", Some(b"old"))]);
        let new = tuple_bytes(&[(b"t", Some(b"new"))]);
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'K');
        buf.extend_from_slice(&old);
        buf.push(b'N');
        buf.extend_from_slice(&new);

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Update(u) => {
                assert!(u.old_tuple.is_some());
                assert_eq!(u.old_tuple.unwrap().columns[0].as_bytes().unwrap(), b"old");
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn delete() {
        let td = tuple_bytes(&[(b"t", Some(b"42"))]);
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'K');
        buf.extend_from_slice(&td);

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Delete(d) => {
                assert_eq!(d.relation_id, 16384);
                assert_eq!(d.old_tuple.columns[0].as_bytes().unwrap(), b"42");
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn truncate() {
        let mut buf = vec![b'T'];
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.push(0x01); // CASCADE
        buf.extend_from_slice(&100u32.to_be_bytes());
        buf.extend_from_slice(&200u32.to_be_bytes());

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Truncate(t) => {
                assert_eq!(t.option_bits, 0x01);
                assert_eq!(t.relation_ids, vec![100, 200]);
            }
            _ => panic!("expected Truncate"),
        }
    }

    #[test]
    fn null_and_unchanged_columns() {
        let td = tuple_bytes(&[(b"n", None), (b"u", None), (b"t", Some(b"hi"))]);
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);

        match decode(Bytes::from(buf)).unwrap() {
            WalMessage::Insert(ins) => {
                assert!(ins.tuple.columns[0].is_null());
                assert!(ins.tuple.columns[1].is_unchanged());
                assert_eq!(ins.tuple.columns[2].as_bytes().unwrap(), b"hi");
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn truncated_begin_errors() {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&100u64.to_be_bytes());
        // missing timestamp + xid
        assert!(decode(Bytes::from(buf)).is_err());
    }
}
