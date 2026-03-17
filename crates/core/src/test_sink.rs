//! In-process mock sink that tracks writes by namespace for testing the
//! pipeline without calling out to real turbopuffer.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{Action, BackfillSink, CoreError, DocumentId};

#[derive(Debug, Clone)]
pub struct RecordedWrite {
    pub namespace: String,
    pub actions: Vec<Action>,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct FailureConfig {
    /// Fail every Nth write (0 = never fail).
    pub fail_every_n: usize,
    /// Return transient errors (retryable) vs permanent.
    pub transient: bool,
    /// Artificial latency per write.
    pub latency: Option<Duration>,
    /// Fail after this many total writes (0 = no limit).
    pub fail_after: usize,
}

pub struct MetricsSink {
    writes: Arc<Mutex<Vec<RecordedWrite>>>,
    write_count: AtomicU64,
    action_count: AtomicU64,
    error_count: AtomicU64,
    call_count: AtomicUsize,
    failure_config: FailureConfig,
}

impl MetricsSink {
    pub fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(Vec::new())),
            write_count: AtomicU64::new(0),
            action_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            call_count: AtomicUsize::new(0),
            failure_config: FailureConfig::default(),
        }
    }

    pub fn with_failure(mut self, config: FailureConfig) -> Self {
        self.failure_config = config;
        self
    }

    pub fn writes(&self) -> Vec<RecordedWrite> {
        self.writes.lock().expect("lock poisoned").clone()
    }

    pub fn writes_for(&self, namespace: &str) -> Vec<RecordedWrite> {
        self.writes
            .lock()
            .expect("lock poisoned")
            .iter()
            .filter(|w| w.namespace == namespace)
            .cloned()
            .collect()
    }

    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::SeqCst)
    }

    pub fn action_count(&self) -> u64 {
        self.action_count.load(Ordering::SeqCst)
    }

    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::SeqCst)
    }

    pub fn upserted_ids(&self) -> Vec<DocumentId> {
        self.writes
            .lock()
            .expect("lock poisoned")
            .iter()
            .flat_map(|w| {
                w.actions.iter().filter_map(|a| match a {
                    Action::Upsert { id, .. } => Some(id.clone()),
                    _ => None,
                })
            })
            .collect()
    }

    pub fn deleted_ids(&self) -> Vec<DocumentId> {
        self.writes
            .lock()
            .expect("lock poisoned")
            .iter()
            .flat_map(|w| {
                w.actions.iter().filter_map(|a| match a {
                    Action::Delete { id } => Some(id.clone()),
                    _ => None,
                })
            })
            .collect()
    }

    pub fn namespace_stats(&self) -> HashMap<String, NamespaceStats> {
        let writes = self.writes.lock().expect("lock poisoned");
        let mut stats: HashMap<String, NamespaceStats> = HashMap::new();
        for w in writes.iter() {
            let entry = stats.entry(w.namespace.clone()).or_default();
            entry.write_calls += 1;
            for action in &w.actions {
                match action {
                    Action::Upsert { .. } => entry.upserts += 1,
                    Action::Delete { .. } => entry.deletes += 1,
                    Action::Skip => entry.skips += 1,
                }
            }
        }
        stats
    }

    fn should_fail(&self) -> bool {
        let call = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.failure_config.fail_after > 0 && call > self.failure_config.fail_after {
            return true;
        }
        if self.failure_config.fail_every_n > 0 {
            return call % self.failure_config.fail_every_n == 0;
        }
        false
    }
}

#[derive(Debug, Clone, Default)]
pub struct NamespaceStats {
    pub write_calls: u64,
    pub upserts: u64,
    pub deletes: u64,
    pub skips: u64,
}

impl BackfillSink for MetricsSink {
    fn write<'a>(
        &'a self,
        namespace: &'a str,
        actions: &'a [Action],
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(latency) = self.failure_config.latency {
                tokio::time::sleep(latency).await;
            }

            self.write_count.fetch_add(1, Ordering::SeqCst);

            if self.should_fail() {
                self.error_count.fetch_add(1, Ordering::SeqCst);
                return Err(CoreError::pipeline_transient(
                    "MetricsSink: injected failure".to_string(),
                    self.failure_config.transient,
                ));
            }

            self.action_count
                .fetch_add(actions.len() as u64, Ordering::SeqCst);
            self.writes
                .lock()
                .expect("lock poisoned")
                .push(RecordedWrite {
                    namespace: namespace.to_string(),
                    actions: actions.to_vec(),
                    timestamp: Instant::now(),
                });

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_upsert(id: u64) -> Action {
        Action::Upsert {
            id: DocumentId::Uint(id),
            document: json!({"test": true}),
            vector: None,
            distance_metric: None,
            schema: None,
        }
    }

    #[tokio::test]
    async fn records_writes() {
        let sink = MetricsSink::new();
        let actions = vec![sample_upsert(1), sample_upsert(2)];

        sink.write("ns1", &actions).await.unwrap();
        sink.write(
            "ns2",
            &[Action::Delete {
                id: DocumentId::Uint(3),
            }],
        )
        .await
        .unwrap();

        assert_eq!(sink.write_count(), 2);
        assert_eq!(sink.action_count(), 3);
        assert_eq!(sink.writes_for("ns1").len(), 1);
        assert_eq!(sink.writes_for("ns2").len(), 1);
        assert_eq!(sink.upserted_ids().len(), 2);
        assert_eq!(sink.deleted_ids().len(), 1);
    }

    #[tokio::test]
    async fn namespace_stats() {
        let sink = MetricsSink::new();
        sink.write("a", &[sample_upsert(1), sample_upsert(2)])
            .await
            .unwrap();
        sink.write(
            "b",
            &[Action::Delete {
                id: DocumentId::Uint(3),
            }],
        )
        .await
        .unwrap();
        sink.write("a", &[Action::Skip]).await.unwrap();

        let stats = sink.namespace_stats();
        assert_eq!(stats["a"].write_calls, 2);
        assert_eq!(stats["a"].upserts, 2);
        assert_eq!(stats["a"].skips, 1);
        assert_eq!(stats["b"].deletes, 1);
    }

    #[tokio::test]
    async fn fail_every_n() {
        let sink = MetricsSink::new().with_failure(FailureConfig {
            fail_every_n: 2,
            transient: true,
            ..Default::default()
        });

        assert!(sink.write("ns", &[sample_upsert(1)]).await.is_ok());
        assert!(sink.write("ns", &[sample_upsert(2)]).await.is_err());
        assert!(sink.write("ns", &[sample_upsert(3)]).await.is_ok());
        assert!(sink.write("ns", &[sample_upsert(4)]).await.is_err());

        assert_eq!(sink.error_count(), 2);
        assert_eq!(sink.action_count(), 2);
    }

    #[tokio::test]
    async fn fail_after_n() {
        let sink = MetricsSink::new().with_failure(FailureConfig {
            fail_after: 2,
            transient: false,
            ..Default::default()
        });

        assert!(sink.write("ns", &[sample_upsert(1)]).await.is_ok());
        assert!(sink.write("ns", &[sample_upsert(2)]).await.is_ok());
        assert!(sink.write("ns", &[sample_upsert(3)]).await.is_err());
        assert!(sink.write("ns", &[sample_upsert(4)]).await.is_err());

        assert_eq!(sink.error_count(), 2);
    }
}
