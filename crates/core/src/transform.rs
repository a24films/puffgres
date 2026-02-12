use async_trait::async_trait;

use crate::{Action, CoreError, DocumentId};
use replication::RowEvent;

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
                .map(|(event, id)| match event.operation {
                    Operation::Delete => Action::Delete { id: id.clone() },
                    _ => Action::Upsert {
                        id: id.clone(),
                        document: json!({}),
                        vector: None,
                        distance_metric: None,
                    },
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn trait_compiles_and_works() {
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
}
