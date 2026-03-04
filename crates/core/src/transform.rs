use async_trait::async_trait;

use crate::{Action, CoreError, DocumentId};
use replication::RowEvent;

/// Converts raw replication events into pipeline [`Action`]s.
///
/// # Error handling
///
/// Return `Err(CoreError)` only for fatal failures (e.g. transformer service
/// unreachable). Per-event errors (bad data, missing columns, etc.) should be
/// handled internally — log the error and return [`Action::Skip`] for that
/// event so the rest of the batch can proceed.
#[async_trait]
pub trait Transformer: Send + Sync {
    async fn transform_batch(
        &self,
        events: &[(&RowEvent, DocumentId)],
    ) -> Result<Vec<Action>, CoreError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use replication::{Operation, TupleData};
    use serde_json::json;

    struct NoopTransformer;

    #[async_trait]
    impl Transformer for NoopTransformer {
        async fn transform_batch(
            &self,
            events: &[(&RowEvent, DocumentId)],
        ) -> Result<Vec<Action>, CoreError> {
            Ok(events
                .iter()
                .map(|(event, id)| {
                    // Per-event error handling: skip events with no tuple data
                    // instead of failing the whole batch.
                    if event.new_tuple.is_none() && event.old_tuple.is_none() {
                        return Action::Skip;
                    }
                    match event.operation {
                        Operation::Delete => Action::Delete { id: id.clone() },
                        _ => Action::Upsert {
                            id: id.clone(),
                            document: json!({}),
                            vector: None,
                            distance_metric: None,
                            schema: None,
                        },
                    }
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn test_transform_batch() {
        let transformer: Box<dyn Transformer> = Box::new(NoopTransformer);

        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(TupleData { columns: vec![] }),
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let actions = transformer.transform_batch(&[(&event, id)]).await.unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Upsert { .. }));
    }

    #[tokio::test]
    async fn test_bad_event_skips_without_failing_batch() {
        let transformer: Box<dyn Transformer> = Box::new(NoopTransformer);

        let good_event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(TupleData { columns: vec![] }),
            old_tuple: None,
        };
        let bad_event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: None,
            old_tuple: None,
        };

        let actions = transformer
            .transform_batch(&[
                (&good_event, DocumentId::Uint(1)),
                (&bad_event, DocumentId::Uint(2)),
                (&good_event, DocumentId::Uint(3)),
            ])
            .await
            .unwrap();

        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::Upsert { .. }));
        assert!(matches!(actions[1], Action::Skip));
        assert!(matches!(actions[2], Action::Upsert { .. }));
    }
}
