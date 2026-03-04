use std::time::Duration;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};

use crate::connection::parse_connection_string;
use crate::decoder::{self, WalMessage};
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
}

/// All row events from a single committed transaction.
#[derive(Debug)]
pub struct StreamingBatch {
    pub events: Vec<RowEvent>,
    /// Commit LSN for this transaction — used for checkpointing and ack.
    pub ack_lsn: u64,
}

struct TransactionState {
    events: Vec<RowEvent>,
}

/// Buffers row events per transaction via `recv_batch`.
pub struct ReplicationStream<T: ReplicationTransport = ReplicationClient> {
    client: T,
    relation_cache: RelationCache,
    current_txn: Option<TransactionState>,
    pending_lsn: Option<Lsn>,
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
        })
    }
}

impl<T: ReplicationTransport> ReplicationStream<T> {
    /// Returns the next committed transaction as a batch, or `None` on stream end.
    /// This function itself does **not** ack the batch to Postgres, because it may fail.
    /// Instead, the `ack` function is called by the parent afterwards.
    pub async fn recv_batch(&mut self) -> Result<Option<StreamingBatch>> {
        loop {
            let event = match self.client.recv().await {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            };

            match event {
                ReplicationEvent::Begin { .. } => {
                    self.current_txn = Some(TransactionState { events: Vec::new() });
                }

                ReplicationEvent::XLogData { data, .. } => {
                    let msg = decoder::decode(data)?;

                    match msg {
                        WalMessage::Relation(info) => {
                            if self.relation_cache.schema_changed(&info) {
                                let err = ReplicationError::SchemaChanged {
                                    relation_id: info.id,
                                    namespace: info.namespace.clone(),
                                    name: info.name.clone(),
                                };
                                self.relation_cache.insert(info);
                                // Return an error to signal the caller to tear down and
                                // reconnect with a fresh stream. This is not a fatal error:
                                // Postgres replication slots retain all un-acked WAL, so
                                // when we reconnect from the last checkpointed LSN, we will
                                // receive all events between the disconnect and the present
                                // (including any from this transaction we haven't consumed).
                                return Err(err);
                            }
                            self.relation_cache.insert(info);
                        }
                        WalMessage::Insert(ins) => {
                            if let Some(txn) = &mut self.current_txn {
                                txn.events.push(RowEvent {
                                    relation_id: ins.relation_id,
                                    operation: Operation::Insert,
                                    new_tuple: Some(ins.tuple),
                                    old_tuple: None,
                                });
                            }
                        }
                        WalMessage::Update(upd) => {
                            if let Some(txn) = &mut self.current_txn {
                                txn.events.push(RowEvent {
                                    relation_id: upd.relation_id,
                                    operation: Operation::Update,
                                    new_tuple: Some(upd.new_tuple),
                                    old_tuple: upd.old_tuple,
                                });
                            }
                        }
                        WalMessage::Delete(del) => {
                            if let Some(txn) = &mut self.current_txn {
                                txn.events.push(RowEvent {
                                    relation_id: del.relation_id,
                                    operation: Operation::Delete,
                                    new_tuple: None,
                                    old_tuple: Some(del.old_tuple),
                                });
                            }
                        }
                        _ => {}
                    }
                }

                ReplicationEvent::Commit { end_lsn, .. } => {
                    if let Some(txn) = self.current_txn.take() {
                        self.pending_lsn = Some(end_lsn);
                        return Ok(Some(StreamingBatch {
                            events: txn.events,
                            ack_lsn: end_lsn.0,
                        }));
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
        }
    }

    #[test]
    fn test_config_construction() {
        let config = ReplicationStreamConfig {
            connection_string: "postgresql://localhost/mydb".to_string(),
            slot_name: "my_slot".to_string(),
            publication_name: "my_pub".to_string(),
            start_lsn: Some(0x1234),
            status_interval: Duration::from_secs(10),
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

        let batch = stream.recv_batch().await.unwrap().unwrap();
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

        let batch = stream.recv_batch().await.unwrap().unwrap();
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

        let _batch = stream.recv_batch().await.unwrap().unwrap();
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

        let batch1 = stream.recv_batch().await.unwrap().unwrap();
        assert_eq!(batch1.ack_lsn, 1000);
        stream.ack();

        let batch2 = stream.recv_batch().await.unwrap().unwrap();
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
        let rel_v1 = encode_relation(1, "public", "users", &[col_id.clone()]);
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
        let batch = stream.recv_batch().await.unwrap().unwrap();
        assert_eq!(batch.ack_lsn, 200);

        // Second batch returns SchemaChanged error
        let result = stream.recv_batch().await;
        assert!(
            matches!(
                result,
                Err(ReplicationError::SchemaChanged { relation_id: 1, .. })
            ),
            "expected SchemaChanged error, got: {result:?}",
        );

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
        let rel = encode_relation(1, "public", "users", &[col_id.clone()]);
        let rel2 = encode_relation(1, "public", "users", &[col_id]);

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
        let batch1 = stream.recv_batch().await.unwrap().unwrap();
        assert_eq!(batch1.ack_lsn, 200);
        let batch2 = stream.recv_batch().await.unwrap().unwrap();
        assert_eq!(batch2.ack_lsn, 400);
    }
}
