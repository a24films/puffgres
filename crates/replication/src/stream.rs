use std::time::Duration;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};

use crate::connection::parse_connection_string;
use crate::decoder::{self, WalMessage};
use crate::event::{Operation, RowEvent};
use crate::relation::RelationCache;
use crate::{ReplicationError, Result};

pub struct ReplicationStreamConfig {
    pub connection_string: String,
    pub slot_name: String,
    pub publication_name: String,
    pub start_lsn: Option<u64>,
    pub status_interval: Duration,
}

/// All row events from a single committed transaction.
pub struct StreamingBatch {
    pub events: Vec<RowEvent>,
    pub ack_lsn: u64,
}

struct TransactionState {
    events: Vec<RowEvent>,
}

/// Buffers row events per transaction via `recv_batch`.
pub struct ReplicationStream {
    client: ReplicationClient,
    relation_cache: RelationCache,
    current_txn: Option<TransactionState>,
}

impl ReplicationStream {
    pub async fn connect(config: ReplicationStreamConfig) -> Result<Self> {
        let parsed = parse_connection_string(&config.connection_string)?;

        let mut repl_config = ReplicationConfig::new(
            parsed.host,
            parsed.user,
            parsed.password,
            parsed.database,
            config.slot_name,
            config.publication_name,
        )
        .with_port(parsed.port)
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
        })
    }

    /// Returns the next committed transaction as a batch, or None on stream end.
    pub async fn recv_batch(&mut self) -> Result<Option<StreamingBatch>> {
        loop {
            let event = match self.client.recv().await {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(None),
                Err(e) => return Err(ReplicationError::Stream(e.to_string())),
            };

            match event {
                ReplicationEvent::Begin { .. } => {
                    self.current_txn = Some(TransactionState { events: Vec::new() });
                }

                ReplicationEvent::XLogData { data, .. } => {
                    let msg = decoder::decode(data)?;

                    match msg {
                        WalMessage::Relation(info) => {
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
                        let ack_lsn = end_lsn.0;
                        self.client.update_applied_lsn(end_lsn);
                        return Ok(Some(StreamingBatch {
                            events: txn.events,
                            ack_lsn,
                        }));
                    }
                }

                ReplicationEvent::StoppedAt { .. } => return Ok(None),

                ReplicationEvent::KeepAlive { .. } | ReplicationEvent::Message { .. } => {}
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

    #[test]
    fn config_construction() {
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
}
