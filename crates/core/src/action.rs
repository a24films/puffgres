use serde_json::Value;

use crate::DocumentId;

// Action and DocumentId are the *processed* counterparts of the replication
// crate's RowEvent and Operation types. Where RowEvent/Operation represent raw
// Postgres WAL changes (relation_id, raw ColumnValue bytes, insert/update/delete),
// Action/DocumentId represent what the pipeline should *do* about those changes
// after extracting a typed primary key and serializing columns to JSON. These
// actions are sent to the transform.

/// A pipeline action derived from a replication RowEvent.
#[derive(Debug, Clone)]
pub enum Action {
    /// Insert or update a document. Collapses Insert and Update operations
    /// since downstream vector stores treat them identically.
    Upsert {
        id: DocumentId,
        document: Value,
        vector: Option<Vec<f32>>,
    },
    /// Delete a document by its id.
    Delete { id: DocumentId },
    /// Skip this event (e.g. filtered out by transform).
    Skip,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn action_upsert() {
        let action = Action::Upsert {
            id: DocumentId::Uint(1),
            document: json!({"name": "test"}),
            vector: Some(vec![0.1, 0.2, 0.3]),
        };
        assert!(matches!(action, Action::Upsert { .. }));
    }

    #[test]
    fn action_upsert_no_vector() {
        let action = Action::Upsert {
            id: DocumentId::Uint(1),
            document: json!({"name": "test"}),
            vector: None,
        };
        if let Action::Upsert { vector, .. } = &action {
            assert!(vector.is_none());
        }
    }

    #[test]
    fn action_delete() {
        let action = Action::Delete {
            id: DocumentId::String("abc".to_string()),
        };
        assert!(matches!(action, Action::Delete { .. }));
    }

    #[test]
    fn action_skip() {
        let action = Action::Skip;
        assert!(matches!(action, Action::Skip));
    }
}
