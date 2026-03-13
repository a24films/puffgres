use std::collections::HashMap;
use std::sync::Arc;
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
    /// Defaults to 1,000,000. Ignored when `sub_batch_size` is set.
    pub max_transaction_events: Option<usize>,
    /// When set, yield sub-batches of this size during large transactions instead
    /// of buffering the entire transaction in memory. The commit message finalizes
    /// the group. This gives backpressure for free — the pipeline processes chunks
    /// as they arrive rather than waiting for the full transaction to buffer.
    pub sub_batch_size: Option<usize>,
    /// Columns watched per table (`schema.table` → column names).
    /// Schema changes that only add columns NOT in this set are silently accepted.
    /// If a table has no entry (or the entry is empty), all column changes trigger
    /// a `SchemaChanged` signal.
    pub watched_columns: HashMap<String, Vec<String>>,
}

/// All row events from a single committed transaction (or the final chunk
/// of a streamed transaction when sub-batching is enabled).
#[derive(Debug)]
pub struct StreamingBatch {
    pub events: Vec<RowEvent>,
    /// Commit LSN for this transaction — used for checkpointing and ack.
    pub ack_lsn: u64,
    /// Transaction ID (xid). When preceded by `SubBatch`es with the same ID,
    /// this is the final chunk that commits the group.
    pub transaction_id: u64,
    /// Commit timestamp as microseconds since 2000-01-01 00:00:00 UTC.
    /// Used for computing replication lag.
    pub commit_time_micros: i64,
}

/// A chunk of events from an in-progress transaction, yielded before commit
/// when sub-batch streaming is enabled. Process immediately for throughput,
/// but don't checkpoint or ack until the final `Batch` arrives.
#[derive(Debug)]
pub struct StreamingSubBatch {
    pub events: Vec<RowEvent>,
    /// Transaction ID for grouping sub-batches together.
    pub transaction_id: u64,
}

/// The result of receiving the next batch from the replication stream.
/// Schema changes are signaled here rather than as errors, since they
/// are not failures — just signals to reconnect with fresh metadata.
#[derive(Debug)]
pub enum BatchResult {
    /// A committed transaction batch (or the final chunk of a streamed transaction).
    Batch(StreamingBatch),
    /// A sub-batch from an in-progress transaction (not yet committed).
    /// Only emitted when `sub_batch_size` is configured. Process events
    /// immediately but defer checkpointing until the corresponding `Batch`.
    SubBatch(StreamingSubBatch),
    /// A tracked relation's schema changed. The caller should tear down
    /// and reconnect with fresh schema metadata.
    SchemaChanged(SchemaChanged),
    /// A transaction exceeded `max_transaction_events`. The caller should
    /// skip this transaction (log + DLQ) and advance past it.
    /// Only emitted when `sub_batch_size` is NOT configured.
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
    xid: u64,
    /// Total events seen in this transaction (including already-yielded sub-batches).
    total_events: usize,
    /// Set to `true` when this transaction exceeded the event limit and was truncated.
    /// Only used when sub-batching is disabled.
    too_large: bool,
}

/// Buffers row events per transaction via `recv_batch`.
pub struct ReplicationStream<T: ReplicationTransport = ReplicationClient> {
    client: T,
    relation_cache: RelationCache,
    current_txn: Option<TransactionState>,
    pending_lsn: Option<Lsn>,
    max_transaction_events: usize,
    sub_batch_size: Option<usize>,
    watched_columns: HashMap<String, Vec<String>>,
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
            sub_batch_size: config.sub_batch_size,
            watched_columns: config.watched_columns,
        })
    }
}

impl<T: ReplicationTransport> ReplicationStream<T> {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_transport(
        client: T,
        max_transaction_events: usize,
        sub_batch_size: Option<usize>,
    ) -> Self {
        Self {
            client,
            relation_cache: RelationCache::new(),
            current_txn: None,
            pending_lsn: None,
            max_transaction_events,
            sub_batch_size,
            watched_columns: HashMap::new(),
        }
    }

    /// Push an event into the current transaction, returning a `SubBatch` if the
    /// sub-batch threshold is reached.
    fn push_event(&mut self, event: RowEvent) -> Option<BatchResult> {
        let txn = self.current_txn.as_mut()?;

        if txn.too_large {
            return None;
        }

        if let Some(sub_batch_size) = self.sub_batch_size {
            txn.events.push(event);
            txn.total_events += 1;
            if txn.events.len() >= sub_batch_size {
                let events = std::mem::take(&mut txn.events);
                return Some(BatchResult::SubBatch(StreamingSubBatch {
                    events,
                    transaction_id: txn.xid,
                }));
            }
        } else if txn.events.len() >= self.max_transaction_events {
            txn.too_large = true;
            txn.events.clear();
        } else {
            txn.events.push(event);
            txn.total_events += 1;
        }

        None
    }

    /// Returns the next committed transaction as a batch, or `None` on stream end.
    /// This function itself does **not** ack the batch to Postgres, because it may fail.
    /// Instead, the `ack` function is called by the parent afterwards.
    ///
    /// When `sub_batch_size` is configured, may return `BatchResult::SubBatch` for
    /// in-progress transactions. The final `BatchResult::Batch` on commit carries
    /// the remaining events and the ack LSN.
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
                ReplicationEvent::Begin { xid, .. } => {
                    self.current_txn = Some(TransactionState {
                        events: Vec::new(),
                        xid: u64::from(xid),
                        total_events: 0,
                        too_large: false,
                    });
                }

                ReplicationEvent::XLogData { data, .. } => {
                    let msg = decoder::decode(data)?;

                    match msg {
                        WalMessage::Relation(info) => {
                            let key = format!("{}.{}", info.namespace, info.name);
                            let is_breaking = if let Some(cols) = self.watched_columns.get(&key) {
                                self.relation_cache.schema_changed_for_columns(&info, cols)
                            } else {
                                self.relation_cache.schema_changed(&info)
                            };

                            // Even a "non-breaking" schema change is unsafe if we already
                            // buffered rows for this relation in the current transaction.
                            // Those rows were encoded with the OLD column layout and would
                            // be misinterpreted when decoded with the new schema.
                            let has_buffered_rows = self.current_txn.as_ref().is_some_and(|txn| {
                                txn.events.iter().any(|e| e.relation_id == info.id)
                            });
                            let columns_changed = self.relation_cache.schema_changed(&info);

                            if is_breaking || (columns_changed && has_buffered_rows) {
                                let signal = SchemaChanged {
                                    relation_id: info.id,
                                    namespace: info.namespace.clone(),
                                    name: info.name.clone(),
                                };
                                self.relation_cache.insert(info);
                                return Ok(Some(BatchResult::SchemaChanged(signal)));
                            }
                            self.relation_cache.insert(info);
                        }
                        WalMessage::Insert(ins) => {
                            if let Some(sub_batch) = self.push_event(RowEvent {
                                relation_id: ins.relation_id,
                                operation: Operation::Insert,
                                new_tuple: Some(Arc::new(ins.tuple)),
                                old_tuple: None,
                            }) {
                                return Ok(Some(sub_batch));
                            }
                        }
                        WalMessage::Update(upd) => {
                            if let Some(sub_batch) = self.push_event(RowEvent {
                                relation_id: upd.relation_id,
                                operation: Operation::Update,
                                new_tuple: Some(Arc::new(upd.new_tuple)),
                                old_tuple: upd.old_tuple.map(Arc::new),
                            }) {
                                return Ok(Some(sub_batch));
                            }
                        }
                        WalMessage::Delete(del) => {
                            if let Some(sub_batch) = self.push_event(RowEvent {
                                relation_id: del.relation_id,
                                operation: Operation::Delete,
                                new_tuple: None,
                                old_tuple: Some(Arc::new(del.old_tuple)),
                            }) {
                                return Ok(Some(sub_batch));
                            }
                        }
                        _ => {}
                    }
                }

                ReplicationEvent::Commit {
                    end_lsn,
                    commit_time_micros,
                    ..
                } => {
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
                            transaction_id: txn.xid,
                            commit_time_micros,
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
            sub_batch_size: None,
            watched_columns: HashMap::new(),
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
            sub_batch_size: None,
            watched_columns: HashMap::new(),
        }
    }

    fn make_stream_with_sub_batching(
        events: Vec<Result<Option<ReplicationEvent>>>,
        sub_batch_size: usize,
    ) -> ReplicationStream<MockTransport> {
        ReplicationStream {
            client: MockTransport::new(events),
            relation_cache: RelationCache::new(),
            current_txn: None,
            pending_lsn: None,
            max_transaction_events: DEFAULT_MAX_TRANSACTION_EVENTS,
            sub_batch_size: Some(sub_batch_size),
            watched_columns: HashMap::new(),
        }
    }

    fn expect_sub_batch(result: Option<BatchResult>) -> StreamingSubBatch {
        match result.expect("expected Some, got None") {
            BatchResult::SubBatch(sb) => sb,
            other => panic!("expected SubBatch, got: {other:?}"),
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
            sub_batch_size: None,
            watched_columns: HashMap::new(),
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

    fn make_stream_with_watched(
        events: Vec<Result<Option<ReplicationEvent>>>,
        watched_columns: HashMap<String, Vec<String>>,
    ) -> ReplicationStream<MockTransport> {
        ReplicationStream {
            client: MockTransport::new(events),
            relation_cache: RelationCache::new(),
            current_txn: None,
            pending_lsn: None,
            max_transaction_events: DEFAULT_MAX_TRANSACTION_EVENTS,
            watched_columns,
            sub_batch_size: None,
        }
    }

    #[tokio::test]
    async fn test_nonbreaking_schema_change_mid_transaction_forces_reconnect() {
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

        // v1: just "id"
        let rel_v1 = encode_relation(1, "public", "users", std::slice::from_ref(&col_id));
        // v2: "id" + "email" (additive, non-breaking for watched=["id"])
        let rel_v2 = encode_relation(1, "public", "users", &[col_id.clone(), col_email]);
        let insert = encode_insert(1, "42");

        let mut watched = HashMap::new();
        watched.insert("public.users".to_string(), vec!["id".to_string()]);

        let mut stream = make_stream_with_watched(
            vec![
                // First transaction: seed the relation cache
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
                // Second transaction: insert with old schema, then schema change
                Ok(Some(ReplicationEvent::Begin {
                    final_lsn: Lsn(0),
                    xid: 2,
                    commit_time_micros: 0,
                })),
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(200),
                    wal_end: Lsn(200),
                    server_time_micros: 0,
                    data: insert,
                })),
                // Relation update mid-transaction (additive: adds "email")
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(300),
                    wal_end: Lsn(300),
                    server_time_micros: 0,
                    data: rel_v2,
                })),
                Ok(Some(ReplicationEvent::Commit {
                    lsn: Lsn(400),
                    end_lsn: Lsn(500),
                    commit_time_micros: 0,
                })),
            ],
            watched,
        );

        // First batch: seeds the cache
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.ack_lsn, 200);

        // Second batch: even though "email" is not watched, the relation change
        // mid-transaction with buffered rows must force SchemaChanged to avoid
        // decoding pre-Relation rows with the wrong column layout.
        match stream.recv_batch().await.unwrap().unwrap() {
            BatchResult::SchemaChanged(sc) => {
                assert_eq!(sc.relation_id, 1);
            }
            other => panic!("expected SchemaChanged, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_nonbreaking_schema_change_without_buffered_rows_is_safe() {
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

        let rel_v1 = encode_relation(1, "public", "users", std::slice::from_ref(&col_id));
        let rel_v2 = encode_relation(1, "public", "users", &[col_id.clone(), col_email]);

        let mut watched = HashMap::new();
        watched.insert("public.users".to_string(), vec!["id".to_string()]);

        let mut stream = make_stream_with_watched(
            vec![
                // First transaction: seed the cache
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
                // Second transaction: schema change BEFORE any rows (safe)
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
                Ok(Some(ReplicationEvent::Commit {
                    lsn: Lsn(300),
                    end_lsn: Lsn(400),
                    commit_time_micros: 0,
                })),
            ],
            watched,
        );

        // First batch: seeds the cache
        let batch1 = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch1.ack_lsn, 200);

        // Second batch: non-breaking change with no buffered rows → safe, no SchemaChanged
        let batch2 = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch2.ack_lsn, 400);
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

    /// Helper: build a list of N insert XLogData events.
    fn n_inserts(n: usize) -> Vec<Result<Option<ReplicationEvent>>> {
        (0..n)
            .map(|i| {
                let data = encode_insert(1, &format!("row-{i}"));
                Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data,
                }))
            })
            .collect()
    }

    #[tokio::test]
    async fn test_sub_batch_yields_chunks_then_commit() {
        // 5 events with sub_batch_size=2 → 2 sub-batches (2 each) + 1 final batch (1 remaining)
        let mut events = vec![Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 42,
            commit_time_micros: 0,
        }))];
        events.extend(n_inserts(5));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(100),
            end_lsn: Lsn(200),
            commit_time_micros: 0,
        })));

        let mut stream = make_stream_with_sub_batching(events, 2);

        // First sub-batch: 2 events
        let sb1 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb1.events.len(), 2);
        assert_eq!(sb1.transaction_id, 42);

        // Second sub-batch: 2 events
        let sb2 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb2.events.len(), 2);
        assert_eq!(sb2.transaction_id, 42);

        // Final batch on commit: 1 remaining event
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.ack_lsn, 200);
        assert_eq!(batch.transaction_id, 42);

        // No ack until caller calls ack()
        assert!(stream.client.acked_lsns().is_empty());
        stream.ack();
        assert_eq!(stream.client.acked_lsns(), vec![200]);
    }

    #[tokio::test]
    async fn test_sub_batch_exact_multiple() {
        // 4 events with sub_batch_size=2 → 2 sub-batches + final batch with 0 events
        let mut events = vec![Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 10,
            commit_time_micros: 0,
        }))];
        events.extend(n_inserts(4));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(100),
            end_lsn: Lsn(500),
            commit_time_micros: 0,
        })));

        let mut stream = make_stream_with_sub_batching(events, 2);

        let sb1 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb1.events.len(), 2);

        let sb2 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb2.events.len(), 2);

        // Final batch: 0 remaining events (exact multiple)
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.events.len(), 0);
        assert_eq!(batch.ack_lsn, 500);
        assert_eq!(batch.transaction_id, 10);
    }

    #[tokio::test]
    async fn test_sub_batch_small_transaction_no_sub_batches() {
        // 1 event with sub_batch_size=10 → no sub-batches, just a normal batch
        let mut events = vec![Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 99,
            commit_time_micros: 0,
        }))];
        events.extend(n_inserts(1));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(100),
            end_lsn: Lsn(300),
            commit_time_micros: 0,
        })));

        let mut stream = make_stream_with_sub_batching(events, 10);

        // Should get a normal batch, no sub-batches
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.ack_lsn, 300);
    }

    #[tokio::test]
    async fn test_sub_batch_large_transaction_many_chunks() {
        // 2_000_005 events with sub_batch_size=1_000_000 → 2 sub-batches + final batch
        let total_events = 2_000_005;
        let sub_batch_size = 1_000_000;

        let mut events = vec![Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 777,
            commit_time_micros: 0,
        }))];
        events.extend(n_inserts(total_events));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(100),
            end_lsn: Lsn(9999),
            commit_time_micros: 0,
        })));

        let mut stream = make_stream_with_sub_batching(events, sub_batch_size);

        // First sub-batch: 1M events
        let sb1 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb1.events.len(), sub_batch_size);
        assert_eq!(sb1.transaction_id, 777);

        // Second sub-batch: 1M events
        let sb2 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb2.events.len(), sub_batch_size);
        assert_eq!(sb2.transaction_id, 777);

        // Final batch: 5 remaining events
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.events.len(), 5);
        assert_eq!(batch.ack_lsn, 9999);
        assert_eq!(batch.transaction_id, 777);

        stream.ack();
        assert_eq!(stream.client.acked_lsns(), vec![9999]);
    }

    #[tokio::test]
    async fn test_sub_batch_transaction_id_preserved() {
        // Two transactions, each with sub-batches — verify IDs are distinct
        let mut events = vec![Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 100,
            commit_time_micros: 0,
        }))];
        events.extend(n_inserts(3));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(100),
            end_lsn: Lsn(200),
            commit_time_micros: 0,
        })));
        events.push(Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 200,
            commit_time_micros: 0,
        })));
        events.extend(n_inserts(3));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(300),
            end_lsn: Lsn(400),
            commit_time_micros: 0,
        })));

        let mut stream = make_stream_with_sub_batching(events, 2);

        // Txn 100: sub-batch + final
        let sb = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb.transaction_id, 100);
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.transaction_id, 100);
        stream.ack();

        // Txn 200: sub-batch + final
        let sb = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb.transaction_id, 200);
        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.transaction_id, 200);
        stream.ack();

        assert_eq!(stream.client.acked_lsns(), vec![200, 400]);
    }

    #[tokio::test]
    async fn test_sub_batch_total_events_tracked() {
        // Verify total_events counts across sub-batches
        let mut events = vec![Ok(Some(ReplicationEvent::Begin {
            final_lsn: Lsn(0),
            xid: 1,
            commit_time_micros: 0,
        }))];
        events.extend(n_inserts(7));
        events.push(Ok(Some(ReplicationEvent::Commit {
            lsn: Lsn(100),
            end_lsn: Lsn(200),
            commit_time_micros: 0,
        })));

        let mut stream = make_stream_with_sub_batching(events, 3);

        let sb1 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb1.events.len(), 3);

        let sb2 = expect_sub_batch(stream.recv_batch().await.unwrap());
        assert_eq!(sb2.events.len(), 3);

        let batch = expect_batch(stream.recv_batch().await.unwrap());
        assert_eq!(batch.events.len(), 1);

        // Total across all: 3 + 3 + 1 = 7
    }

    mod proptests {
        use super::*;

        fn build_txn_events(
            n_events: usize,
            xid: u32,
            commit_lsn: u64,
        ) -> Vec<Result<Option<ReplicationEvent>>> {
            let mut events = Vec::new();
            events.push(Ok(Some(ReplicationEvent::Begin {
                final_lsn: Lsn(0),
                xid,
                commit_time_micros: 0,
            })));
            for _ in 0..n_events {
                let mut buf = BytesMut::new();
                buf.put_u8(b'I');
                buf.put_u32(1); // relation_id
                buf.put_u8(b'N');
                buf.put_u16(1);
                buf.put_u8(b't');
                let val = b"42";
                buf.put_u32(val.len() as u32);
                buf.put_slice(val);
                events.push(Ok(Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(0),
                    wal_end: Lsn(0),
                    server_time_micros: 0,
                    data: buf.freeze(),
                })));
            }
            events.push(Ok(Some(ReplicationEvent::Commit {
                lsn: Lsn(commit_lsn),
                end_lsn: Lsn(commit_lsn),
                commit_time_micros: 0,
            })));
            events
        }

        #[tokio::test]
        async fn sub_batch_total_equals_txn_size() {
            // For various event counts and sub-batch sizes, verify that
            // the total events across all sub-batches + final batch == n_events.
            for n_events in [0, 1, 2, 5, 10, 50, 100] {
                for sub_batch_size in [1, 2, 3, 7, 10, 50, 100, 200] {
                    let events = build_txn_events(n_events, 1, 1000);
                    let mut all_events = vec![Ok(Some(ReplicationEvent::XLogData {
                        wal_start: Lsn(0),
                        wal_end: Lsn(0),
                        server_time_micros: 0,
                        data: {
                            let mut buf = BytesMut::new();
                            buf.put_u8(b'R');
                            buf.put_u32(1); // relation_id
                            buf.put_slice(b"public\0");
                            buf.put_slice(b"test\0");
                            buf.put_u8(b'd');
                            buf.put_u16(1);
                            buf.put_u8(1);
                            buf.put_slice(b"id\0");
                            buf.put_u32(23);
                            buf.put_i32(-1);
                            buf.freeze()
                        },
                    }))];
                    all_events.extend(events);

                    let mut stream = ReplicationStream {
                        client: MockTransport::new(all_events),
                        relation_cache: RelationCache::new(),
                        current_txn: None,
                        pending_lsn: None,
                        max_transaction_events: DEFAULT_MAX_TRANSACTION_EVENTS,
                        sub_batch_size: Some(sub_batch_size),
                        watched_columns: HashMap::new(),
                    };

                    let mut total = 0usize;
                    loop {
                        match stream.recv_batch().await.unwrap() {
                            Some(BatchResult::SubBatch(sb)) => {
                                assert!(
                                    sb.events.len() <= sub_batch_size,
                                    "sub-batch exceeded size: {} > {} (n={}, sub={})",
                                    sb.events.len(),
                                    sub_batch_size,
                                    n_events,
                                    sub_batch_size,
                                );
                                total += sb.events.len();
                            }
                            Some(BatchResult::Batch(b)) => {
                                total += b.events.len();
                                break;
                            }
                            Some(other) => panic!("unexpected: {other:?}"),
                            None => break,
                        }
                    }

                    assert_eq!(
                        total, n_events,
                        "total mismatch for n_events={n_events}, sub_batch_size={sub_batch_size}"
                    );
                }
            }
        }

        #[tokio::test]
        async fn max_transaction_events_drops_correctly() {
            // Verify that when a transaction exceeds the limit, we get
            // TransactionTooLarge with the correct ack_lsn.
            for limit in [1, 2, 5, 10] {
                for n_events in [limit + 1, limit * 2, limit * 10] {
                    let mut all_events = vec![Ok(Some(ReplicationEvent::XLogData {
                        wal_start: Lsn(0),
                        wal_end: Lsn(0),
                        server_time_micros: 0,
                        data: {
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
                            buf.freeze()
                        },
                    }))];
                    all_events.extend(build_txn_events(n_events, 1, 999));

                    let mut stream = ReplicationStream {
                        client: MockTransport::new(all_events),
                        relation_cache: RelationCache::new(),
                        current_txn: None,
                        pending_lsn: None,
                        max_transaction_events: limit,
                        sub_batch_size: None,
                        watched_columns: HashMap::new(),
                    };

                    match stream.recv_batch().await.unwrap() {
                        Some(BatchResult::TransactionTooLarge { ack_lsn, .. }) => {
                            assert_eq!(
                                ack_lsn, 999,
                                "ack_lsn mismatch for limit={limit}, n={n_events}"
                            );
                        }
                        other => panic!(
                            "expected TransactionTooLarge for limit={limit}, n={n_events}, got {other:?}"
                        ),
                    }
                }
            }
        }
    }
}
