use std::time::Duration;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};

use crate::connection::parse_connection_string;
use crate::decoder::{self, WalMessage};
use crate::error::SchemaChanged;
use crate::event::{Operation, RowEvent};
use crate::relation::RelationCache;
use crate::{ReplicationError, Result};

/// Abstraction over the replication protocol client, enabling test mocks.
pub trait ReplicationTransport {
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<ReplicationEvent>>> + Send;
    fn update_applied_lsn(&self, lsn: Lsn);
}

impl ReplicationTransport for ReplicationClient {
    async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        ReplicationClient::recv(self)
            .await
            .map_err(|e| ReplicationError::Stream(e.to_string()))
    }

    fn update_applied_lsn(&self, lsn: Lsn) {
        ReplicationClient::update_applied_lsn(self, lsn);
    }
}

pub struct ReplicationStreamConfig {
    pub connection_string: String,
    pub slot_name: String,
    pub publication_name: String,
    pub start_lsn: Option<u64>,
    pub status_interval: Duration,
    /// Maximum number of events per transaction. Transactions exceeding this
    /// limit are dropped and returned as `BatchResult::TransactionTooLarge`.
    /// Defaults to 1,000,000.
    pub max_transaction_events: Option<usize>,
}

/// All row events from a single committed transaction.
#[derive(Debug)]
pub struct StreamingBatch {
    pub events: Vec<RowEvent>,
    /// Commit LSN for this transaction — used for checkpointing and ack.
    pub ack_lsn: u64,
}

/// The result of receiving the next batch from the replication stream.
/// Schema changes are signaled here rather than as errors, since they
/// are not failures — just signals to reconnect with fresh metadata.
#[derive(Debug)]
pub enum BatchResult {
    /// A committed transaction batch.
    Batch(StreamingBatch),
    /// A tracked relation's schema changed. The caller should tear down
    /// and reconnect with fresh schema metadata.
    SchemaChanged(SchemaChanged),
    /// A transaction exceeded `max_transaction_events`. The caller should
    /// skip this transaction (log + DLQ) and advance past it.
    TransactionTooLarge {
        /// Commit LSN — must be acked to advance past the skipped transaction.
        ack_lsn: u64,
        /// Number of events that were in the transaction before it was dropped.
        event_count: usize,
    },
}

/// Default maximum events per transaction (1 million).
const DEFAULT_MAX_TRANSACTION_EVENTS: usize = 1_000_000;

struct TransactionState {
    events: Vec<RowEvent>,
    /// Set to `true` when this transaction exceeded the event limit and was truncated.
    too_large: bool,
}

/// Buffers row events per transaction via `recv_batch`.
pub struct ReplicationStream<T: ReplicationTransport = ReplicationClient> {
    client: T,
    relation_cache: RelationCache,
    current_txn: Option<TransactionState>,
    pending_lsn: Option<Lsn>,
    max_transaction_events: usize,
}

impl ReplicationStream {
    pub async fn connect(config: ReplicationStreamConfig) -> Result<Self> {
        let parsed = parse_connection_string(&config.connection_string)?;

        let tls = match parsed.sslmode.as_deref() {
            Some("require") => TlsConfig::require(),
            Some("verify-ca") => TlsConfig::verify_ca(None),
            Some("verify-full") => TlsConfig::verify_full(None),
            _ => TlsConfig::disabled(),
        };

        let mut repl_config = ReplicationConfig::new(
            parsed.host,
            parsed.user,
            parsed.password,
            parsed.database,
            config.slot_name,
            config.publication_name,
        )
        .with_port(parsed.port)
        .with_tls(tls)
        .with_status_interval(config.status_interval);

        if let Some(lsn) = config.start_lsn {
            repl_config = repl_config.with_start_lsn(Lsn(lsn));
        }

        let client = ReplicationClient::connect(repl_config)
            .await
            .map_err(|e| ReplicationError::Connection(e.to_string()))?;

        Ok(Self {
            client,
            relation_cache: RelationCache::new(),
            current_txn: None,
            pending_lsn: None,
            max_transaction_events: config
                .max_transaction_events
                .unwrap_or(DEFAULT_MAX_TRANSACTION_EVENTS),
        })
    }
}

impl<T: ReplicationTransport> ReplicationStream<T> {
    /// Returns the next committed transaction as a batch, or `None` on stream end.
    /// This function itself does **not** ack the batch to Postgres, because it may fail.
    /// Instead, the `ack` function is called by the parent afterwards.
    ///
    /// If a tracked relation's schema changes, returns `BatchResult::SchemaChanged`
    /// instead of an error, since schema changes are not failures.
    pub async fn recv_batch(&mut self) -> Result<Option<BatchResult>> {
        loop {
            let event = match self.client.recv().await {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            };

            match event {
                ReplicationEvent::Begin { .. } => {
                    self.current_txn = Some(TransactionState {
                        events: Vec::new(),
                        too_large: false,
                    });
                }

                ReplicationEvent::XLogData { data, .. } => {
                    let msg = decoder::decode(data)?;

                    match msg {
                        WalMessage::Relation(info) => {
                            if self.relation_cache.schema_changed(&info) {
                                let signal = SchemaChanged {
                                    relation_id: info.id,
                                    namespace: info.namespace.clone(),
                                    name: info.name.clone(),
                                };
                                self.relation_cache.insert(info);
                                // Signal the caller to tear down and reconnect with a
                                // fresh stream. Postgres replication slots retain all
                                // un-acked WAL, so no messages are lost on reconnect.
                                return Ok(Some(BatchResult::SchemaChanged(signal)));
                            }
                            self.relation_cache.insert(info);
                        }
                        WalMessage::Insert(ins) => {
                            if let Some(txn) = &mut self.current_txn {
                                if txn.too_large {
                                    // Already marked — skip remaining events
                                } else if txn.events.len() >= self.max_transaction_events {
                                    txn.too_large = true;
                                    txn.events.clear();
                                } else {
                                    txn.events.push(RowEvent {
                                        relation_id: ins.relation_id,
                                        operation: Operation::Insert,
                                        new_tuple: Some(ins.tuple),
                                        old_tuple: None,
                                    });
                                }
                            }
                        }
                        WalMessage::Update(upd) => {
                            if let Some(txn) = &mut self.current_txn {
                                if txn.too_large {
                                    // Already marked — skip remaining events
                                } else if txn.events.len() >= self.max_transaction_events {
                                    txn.too_large = true;
                                    txn.events.clear();
                                } else {
                                    txn.events.push(RowEvent {
                                        relation_id: upd.relation_id,
                                        operation: Operation::Update,
                                        new_tuple: Some(upd.new_tuple),
                                        old_tuple: upd.old_tuple,
                                    });
                                }
                            }
                        }
                        WalMessage::Delete(del) => {
                            if let Some(txn) = &mut self.current_txn {
                                if txn.too_large {
                                    // Already marked — skip remaining events
                                } else if txn.events.len() >= self.max_transaction_events {
                                    txn.too_large = true;
                                    txn.events.clear();
                                } else {
                                    txn.events.push(RowEvent {
                                        relation_id: del.relation_id,
                                        operation: Operation::Delete,
                                        new_tuple: None,
                                        old_tuple: Some(del.old_tuple),
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }

                ReplicationEvent::Commit { end_lsn, .. } => {
                    if let Some(txn) = self.current_txn.take() {
                        self.pending_lsn = Some(end_lsn);
                        if txn.too_large {
                            return Ok(Some(BatchResult::TransactionTooLarge {
                                ack_lsn: end_lsn.0,
                                event_count: self.max_transaction_events,
                            }));
                        }
                        return Ok(Some(BatchResult::Batch(StreamingBatch {
                            events: txn.events,
                            ack_lsn: end_lsn.0,
                        })));
                    }
                }

                ReplicationEvent::StoppedAt { .. } => return Ok(None),

                ReplicationEvent::KeepAlive { .. } | ReplicationEvent::Message { .. } => {}
            }
        }
    }

    /// Advances the replication slot after last batch has been processed.
    /// ONLY called after events from `recv_batch` have been successfully applied.
    pub fn ack(&mut self) {
        if let Some(lsn) = self.pending_lsn.take() {
            self.client.update_applied_lsn(lsn);
        }
    }

    /// Returns the next decoded WAL message without transaction batching.
    /// Auto-acks commits to prevent WAL buildup. Updates the relation cache
    /// for Relation messages. Intended for debug/observation use cases.
    pub async fn recv_raw(&mut self) -> Result<Option<WalMessage>> {
        loop {
            let event = match self.client.recv().await {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            };

            match event {
                ReplicationEvent::XLogData { data, .. } => {
                    let msg = decoder::decode(data)?;
                    if let WalMessage::Relation(ref info) = msg {
                        self.relation_cache.insert(info.clone());
                    }
                    return Ok(Some(msg));
                }
                ReplicationEvent::Commit { end_lsn, .. } => {
                    self.client.update_applied_lsn(end_lsn);
                }
                ReplicationEvent::StoppedAt { .. } => return Ok(None),
                _ => {}
            }
        }
    }

    pub fn relation_cache(&self) -> &RelationCache {
        &self.relation_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::ColumnInfo;
    use bytes::{BufMut, BytesMut};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Helper to unwrap a `BatchResult::Batch` from `recv_batch`.
    fn expect_batch(result: Option<BatchResult>) -> StreamingBatch {
        match result.expect("expected Some, got None") {
            BatchResult::Batch(b) => b,
            other => panic!("expected Batch, got: {other:?}"),
        }
    }

    struct MockTransport {
        events: VecDeque<Result<Option<ReplicationEvent>>>,
        acked_lsns: Mutex<Vec<u64>>,
    }

    impl MockTransport {
        fn new(events: Vec<Result<Option<ReplicationEvent>>>) -> Self {
            Self {
                events: events.into(),
                acked_lsns: Mutex::new(Vec::new()),
            }
        }

        fn acked_lsns(&self) -> Vec<u64> {
            self.acked_lsns.lock().unwrap().clone()
        }
    }

    impl ReplicationTransport for MockTransport {
        async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
            self.events.pop_front().unwrap_or(Ok(None))
        }

        fn update_applied_lsn(&self, lsn: Lsn) {
            self.acked_lsns.lock().unwrap().push(lsn.0);
        }
    }

    fn make_stream(
        events: Vec<Result<Option<ReplicationEvent>>>,
    ) -> ReplicationStream<MockTransport> {
        ReplicationStream {
            client: MockTransport::new(events),
            relation_cache: RelationCache::new(),
            current_txn: None,
            pending_lsn: None,
            max_transaction_events: DEFAULT_MAX_TRANSACTION_EVENTS,
        }
    }

    fn make_stream_with_limit(
        events: Vec<Result<Option<ReplicationEvent>>>,
        max_events: usize,
    ) -> ReplicationStream<MockTransport> {
        ReplicationStream {
            client: MockTransport::new(events),
            relation_cache: RelationCache::new(),
            current_txn: None,
            pending_lsn: None,
            max_transaction_events: max_events,
        }
    }

    #[test]
    fn config_construction() {
        let config = ReplicationStreamConfig {
            connection_string: "postgresql://localhost/mydb".to_string(),
            slot_name: "my_slot".to_string(),
            publication_name: "my_pub".to_string(),
            start_lsn: Some(0x1234),
            status_interval: Duration::from_secs(10),
            max_transaction_events: None,
        };
        assert_eq!(config.slot_name, "my_slot");
        assert_eq!(config.start_lsn, Some(0x1234));
    }

    #[tokio::test]
    async fn test_recv_batch_does_not_ack() {
        let mut stream = make_stream(vec![
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 1,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(100),
                end_lsn: Lsn(200),
                commit_time_micros: 0,
            })),
        ]);

        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert!(batch.events.is_empty());
        assert_eq!(batch.ack_lsn, 200);
        assert!(stream.client.acked_lsns().is_empty());
    }

    #[tokio::test]
    async fn test_ack_after_successful_processing() {
        let mut stream = make_stream(vec![
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 1,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(100),
                end_lsn: Lsn(200),
                commit_time_micros: 0,
            })),
        ]);

        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.ack_lsn, 200);
        // simulate successful processing, then ack
        stream.ack();
        assert_eq!(stream.client.acked_lsns(), vec![200]);
    }

    #[tokio::test]
    async fn test_no_ack_on_processing_failure() {
        let mut stream = make_stream(vec![
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 1,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(100),
                end_lsn: Lsn(200),
                commit_time_micros: 0,
            })),
        ]);

        let _batch = expect_batch(stream.recv_batch().await.unwrap());
        // simulate a processing failure — don't call ack
        assert!(stream.client.acked_lsns().is_empty());
    }

    #[tokio::test]
    async fn test_streaming_batch_has_correct_ack_lsn() {
        let mut stream = make_stream(vec![
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 1,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(500),
                end_lsn: Lsn(1000),
                commit_time_micros: 0,
            })),
            // Second transaction with different LSN
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 2,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(1500),
                end_lsn: Lsn(2000),
                commit_time_micros: 0,
            })),
        ]);

        let batch1 = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch1.ack_lsn, 1000);
        stream.ack();

        let batch2 = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch2.ack_lsn, 2000);
        stream.ack();

        assert_eq!(stream.client.acked_lsns(), vec![1000, 2000]);
    }

    /// Build raw pgoutput bytes for a Relation message.
    fn encode_relation(
        id: u32,
        namespace: &str,
        name: &str,
        columns: &[ColumnInfo],
    ) -> bytes::Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'R'); // tag
        buf.put_u32(id);
        buf.put_slice(namespace.as_bytes());
        buf.put_u8(0); // null terminator
        buf.put_slice(name.as_bytes());
        buf.put_u8(0); // null terminator
        buf.put_u8(b'd'); // replica identity = Default
        buf.put_u16(columns.len() as u16);
        for col in columns {
            buf.put_u8(if col.part_of_key { 1 } else { 0 });
            buf.put_slice(col.name.as_bytes());
            buf.put_u8(0);
            buf.put_u32(col.type_oid);
            buf.put_i32(col.type_modifier);
        }
        buf.freeze()
    }

    #[tokio::test]
    async fn test_schema_change_returns_error() {
        let col_id = ColumnInfo {
            part_of_key: true,
            name: "id".to_string(),
            type_oid: 23,
            type_modifier: -1,
        };
        let col_email = ColumnInfo {
            part_of_key: false,
            name: "email".to_string(),
            type_oid: 25,
            type_modifier: -1,
        };

        // First relation message: table with just "id"
        let rel_v1 = encode_relation(1, "public", "users", std::slice::from_ref(&col_id));
        // Second relation message: same table with "id" + "email" (schema change)
        let rel_v2 = encode_relation(1, "public", "users", &[col_id, col_email]);

        let mut stream = make_stream(vec![
            // First transaction: establishes the relation in cache
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 1,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::XLogData {
                wal_start: Lsn(0),
                wal_end: Lsn(0),
                server_time_micros: 0,
                data: rel_v1,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(100),
                end_lsn: Lsn(200),
                commit_time_micros: 0,
            })),
            // Second transaction: schema change on same relation
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 2,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::XLogData {
                wal_start: Lsn(200),
                wal_end: Lsn(200),
                server_time_micros: 0,
                data: rel_v2,
            })),
        ]);

        // First batch succeeds (relation cached)
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.ack_lsn, 200);

        // Second recv returns SchemaChanged signal (not an error)
        match stream.recv_batch().await.unwrap().unwrap() {
            BatchResult::SchemaChanged(sc) => {
                assert_eq!(sc.relation_id, 1);
            }
            other => panic!("expected SchemaChanged, got: {other:?}"),
        };

        // Cache was still updated with the new schema
        let cached = stream.relation_cache().get(1).unwrap();
        assert_eq!(cached.columns.len(), 2);
        assert_eq!(cached.columns[1].name, "email");
    }

    #[tokio::test]
    async fn test_same_relation_no_schema_change() {
        let col_id = ColumnInfo {
            part_of_key: true,
            name: "id".to_string(),
            type_oid: 23,
            type_modifier: -1,
        };

        // Same relation message sent twice (no schema change)
        let rel = encode_relation(1, "public", "users", std::slice::from_ref(&col_id));
        let rel2 = encode_relation(1, "public", "users", std::slice::from_ref(&col_id));

        let mut stream = make_stream(vec![
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 1,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::XLogData {
                wal_start: Lsn(0),
                wal_end: Lsn(0),
                server_time_micros: 0,
                data: rel,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(100),
                end_lsn: Lsn(200),
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid: 2,
                commit_time_micros: 0,
            })),
            Ok(Some(ReplicationEvent::XLogData {
                wal_start: Lsn(200),
                wal_end: Lsn(200),
                server_time_micros: 0,
                data: rel2,
            })),
            Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(300),
                end_lsn: Lsn(400),
                commit_time_micros: 0,
            })),
        ]);

        // Both batches should succeed — no schema change
        let batch1 = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch1.ack_lsn, 200);
        let batch2 = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch2.ack_lsn, 400);
    }

    /// Build raw pgoutput bytes for an Insert message with one text column.
    fn encode_insert(relation_id: u32, value: &str) -> bytes::Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'I');
        buf.put_u32(relation_id);
        buf.put_u8(b'N'); // new tuple marker
        buf.put_u16(1); // 1 column
        buf.put_u8(b't'); // text type
        buf.put_u32(value.len() as u32);
        buf.put_slice(value.as_bytes());
        buf.freeze()
    }

    #[tokio::test]
    async fn test_transaction_too_large() {
        // Limit to 2 events
        let insert1 = encode_insert(1, "a");
        let insert2 = encode_insert(1, "b");
        let insert3 = encode_insert(1, "c"); // This triggers the limit

        let mut stream = make_stream_with_limit(
            vec![
                Ok(Some(ReplicationEvent::Begin {
                    final_lsn: Lsn(0),
                    xid: 1,
                    commit_time_micros: 0,
                })),
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data: insert1,
                })),
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data: insert2,
                })),
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data: insert3,
                })),
                Ok(Some(ReplicationEvent::Commit {
                    lsn: Lsn(100),
                    end_lsn: Lsn(200),
                    commit_time_micros: 0,
                })),
            ],
            2,
        );

        match stream.recv_batch().await.unwrap().unwrap() {
            BatchResult::TransactionTooLarge {
                ack_lsn,
                event_count,
            } => {
                assert_eq!(ack_lsn, 200);
                assert_eq!(event_count, 2);
            }
            other => panic!("expected TransactionTooLarge, got: {other:?}"),
        }

        // Should still be able to ack and advance past it
        stream.ack();
        assert_eq!(stream.client.acked_lsns(), vec![200]);
    }

    #[tokio::test]
    async fn test_transaction_within_limit() {
        let insert1 = encode_insert(1, "a");
        let insert2 = encode_insert(1, "b");

        let mut stream = make_stream_with_limit(
            vec![
                Ok(Some(ReplicationEvent::Begin {
                    final_lsn: Lsn(0),
                    xid: 1,
                    commit_time_micros: 0,
                })),
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data: insert1,
                })),
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data: insert2,
                })),
                Ok(Some(ReplicationEvent::Commit {
                    lsn: Lsn(100),
                    end_lsn: Lsn(200),
                    commit_time_micros: 0,
                })),
            ],
            2,
        );

        // 2 events with limit 2 should succeed
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.ack_lsn, 200);
    }
}
