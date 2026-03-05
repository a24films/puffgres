use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;

use async_trait::async_trait;
use config::IdType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::transform::Transformer;
use crate::{Action, CoreError, DocumentId};
use replication::{Operation, RowEvent};

// Wire types for the JS boundary. These are the JSON shapes that cross the
// subprocess stdin/stdout. They're intentionally decoupled from the internal
// Action/RowEvent types so the JS contract can evolve independently.

/// Event serialized to JSON and written to the transform script's stdin.
#[derive(Serialize)]
struct JsEvent {
    operation: &'static str,
    id: Value,
    columns: Vec<Option<String>>,
}

/// Action deserialized from the transform script's stdout.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum JsAction {
    Upsert {
        id: Value,
        document: Value,
        #[serde(default)]
        vector: Option<Vec<f32>>,
        #[serde(default)]
        distance_metric: Option<String>,
        #[serde(default)]
        schema: Option<HashMap<String, Value>>,
    },
    Delete {
        id: Value,
    },
    Skip {},
}

/// A [`Transformer`] that delegates to a user-supplied TypeScript/JavaScript
/// file by spawning `pnpx tsx <script>` as a subprocess. Events are serialized
/// as a JSON array to stdin; the script writes a JSON array of actions to
/// stdout.
pub struct JsTransformer {
    script_path: PathBuf,
    id_type: IdType,
    /// When set, reindexes WAL tuple columns to match the generated schema order.
    /// Each entry is a WAL column index; the output columns are emitted in this order.
    /// Computed from config.columns + table column ordinals.
    column_reindex: Option<Vec<usize>>,
}

impl JsTransformer {
    pub fn new(script_path: PathBuf, id_type: IdType) -> Self {
        Self {
            script_path,
            id_type,
            column_reindex: None,
        }
    }

    /// Create a transformer with a column reindex mapping.
    ///
    /// `column_reindex` maps from schema.ts column order to WAL tuple positions.
    /// For example, if config specifies `["name", "id"]` and the table has columns
    /// `[id, name, email]` (ordinals 0, 1, 2), then `column_reindex = [1, 0]`.
    pub fn with_column_reindex(
        script_path: PathBuf,
        id_type: IdType,
        column_reindex: Vec<usize>,
    ) -> Self {
        Self {
            script_path,
            id_type,
            column_reindex: Some(column_reindex),
        }
    }

    fn serialize_events(&self, events: &[(&RowEvent, DocumentId)]) -> Result<String, CoreError> {
        let js_events: Vec<JsEvent> = events
            .iter()
            .map(|(event, id)| {
                let operation = match event.operation {
                    Operation::Insert => "insert",
                    Operation::Update => "update",
                    Operation::Delete => "delete",
                };

                let id_value = match id {
                    DocumentId::Uint(n) => Value::Number((*n).into()),
                    DocumentId::Int(n) => Value::Number((*n).into()),
                    DocumentId::Uuid(u) => Value::String(u.to_string()),
                    DocumentId::String(s) => Value::String(s.clone()),
                };

                // Prefer new_tuple (insert/update), fall back to old_tuple (delete).
                let tuple = event.new_tuple.as_ref().or(event.old_tuple.as_ref());
                let columns: Vec<Option<String>> = tuple
                    .map(|t| {
                        let col_to_string = |col: &replication::ColumnValue| {
                            col.as_bytes()
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .map(|s| s.to_string())
                        };
                        if let Some(reindex) = &self.column_reindex {
                            reindex
                                .iter()
                                .map(|&i| t.columns.get(i).and_then(|col| col_to_string(col)))
                                .collect()
                        } else {
                            t.columns.iter().map(|col| col_to_string(col)).collect()
                        }
                    })
                    .unwrap_or_default();

                JsEvent {
                    operation,
                    id: id_value,
                    columns,
                }
            })
            .collect();

        serde_json::to_string(&js_events)
            .map_err(|e| CoreError::Pipeline(format!("failed to serialize events: {e}")))
    }

    fn parse_actions(&self, output: &str) -> Result<Vec<Action>, CoreError> {
        let js_actions: Vec<JsAction> = serde_json::from_str(output)
            .map_err(|e| CoreError::Pipeline(format!("failed to parse transform output: {e}")))?;

        js_actions
            .into_iter()
            .map(|action| match action {
                JsAction::Upsert {
                    id,
                    document,
                    vector,
                    distance_metric,
                    schema,
                } => {
                    let doc_id = DocumentId::from_value(&id, &self.id_type)?;
                    Ok(Action::Upsert {
                        id: doc_id,
                        document,
                        vector,
                        distance_metric,
                        schema,
                    })
                }
                JsAction::Delete { id } => {
                    let doc_id = DocumentId::from_value(&id, &self.id_type)?;
                    Ok(Action::Delete { id: doc_id })
                }
                JsAction::Skip {} => Ok(Action::Skip),
            })
            .collect()
    }
}

#[async_trait]
impl Transformer for JsTransformer {
    async fn transform_batch(
        &self,
        events: &[(&RowEvent, DocumentId)],
    ) -> Result<Vec<Action>, CoreError> {
        let input = self.serialize_events(events)?;

        let mut child = Command::new("pnpx")
            .arg("tsx")
            .arg(&self.script_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| CoreError::Pipeline(format!("failed to spawn pnpx tsx: {e}")))?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| CoreError::Pipeline("failed to open stdin".to_string()))?;
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(|e| CoreError::Pipeline(format!("failed to write to stdin: {e}")))?;
        drop(stdin);

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| CoreError::Pipeline(format!("failed to wait for pnpx tsx: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CoreError::Pipeline(format!(
                "transform exited with {}: {stderr}",
                output.status
            )));
        }

        let stdout = String::from_utf8(output.stdout).map_err(|e| {
            CoreError::Pipeline(format!("transform output is not valid utf-8: {e}"))
        })?;

        self.parse_actions(&stdout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use config::IdType;
    use replication::{ColumnValue, TupleData};
    use serde_json::json;

    fn make_transformer() -> JsTransformer {
        JsTransformer::new(PathBuf::from("transform.ts"), IdType::Uint)
    }

    fn make_event(op: Operation, cols: Vec<&str>) -> RowEvent {
        RowEvent {
            relation_id: 1,
            operation: op,
            new_tuple: Some(TupleData {
                columns: cols
                    .into_iter()
                    .map(|s| ColumnValue::Text(Bytes::from(s.to_string())))
                    .collect(),
            }),
            old_tuple: None,
        }
    }

    // -- serialization format --

    #[test]
    fn serialize_insert_event() {
        let t = make_transformer();
        let event = make_event(Operation::Insert, vec!["1", "hello"]);
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["operation"], "insert");
        assert_eq!(parsed[0]["id"], 1);
        assert_eq!(parsed[0]["columns"], json!(["1", "hello"]));
    }

    #[test]
    fn serialize_delete_uses_old_tuple() {
        let t = make_transformer();
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Delete,
            new_tuple: None,
            old_tuple: Some(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from("42"))],
            }),
        };
        let id = DocumentId::Uint(42);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["operation"], "delete");
        assert_eq!(parsed[0]["columns"], json!(["42"]));
    }

    #[test]
    fn serialize_null_columns() {
        let t = make_transformer();
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from("a")),
                    ColumnValue::Null,
                    ColumnValue::Text(Bytes::from("c")),
                ],
            }),
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!(["a", null, "c"]));
    }

    #[test]
    fn serialize_no_tuple_gives_empty_columns() {
        let t = make_transformer();
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Delete,
            new_tuple: None,
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!([]));
    }

    #[test]
    fn serialize_string_id() {
        let t = JsTransformer::new(PathBuf::from("t.ts"), IdType::String);
        let event = make_event(Operation::Insert, vec!["val"]);
        let id = DocumentId::String("abc".to_string());

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["id"], "abc");
    }

    #[test]
    fn serialize_uuid_id() {
        let t = JsTransformer::new(PathBuf::from("t.ts"), IdType::Uuid);
        let event = make_event(Operation::Insert, vec!["val"]);
        let uuid: uuid::Uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let id = DocumentId::Uuid(uuid);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["id"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn serialize_batch() {
        let t = make_transformer();
        let e1 = make_event(Operation::Insert, vec!["a"]);
        let e2 = make_event(Operation::Update, vec!["b"]);

        let json_str = t
            .serialize_events(&[(&e1, DocumentId::Uint(1)), (&e2, DocumentId::Uint(2))])
            .unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["operation"], "insert");
        assert_eq!(parsed[1]["operation"], "update");
    }

    // -- parse valid output --

    #[test]
    fn parse_upsert_action() {
        let t = make_transformer();
        let output = r#"[{"type":"upsert","id":1,"document":{"name":"test"}}]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Upsert {
                id,
                document,
                vector,
                ..
            } => {
                assert_eq!(*id, DocumentId::Uint(1));
                assert_eq!(*document, json!({"name": "test"}));
                assert!(vector.is_none());
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[test]
    fn parse_upsert_with_vector() {
        let t = make_transformer();
        let output = r#"[{"type":"upsert","id":1,"document":{},"vector":[0.1,0.2,0.3]}]"#;
        let actions = t.parse_actions(output).unwrap();

        match &actions[0] {
            Action::Upsert { vector, .. } => {
                assert_eq!(vector.as_ref().unwrap(), &vec![0.1, 0.2, 0.3]);
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[test]
    fn parse_delete_action() {
        let t = make_transformer();
        let output = r#"[{"type":"delete","id":42}]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Delete { id } => assert_eq!(*id, DocumentId::Uint(42)),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_skip_action() {
        let t = make_transformer();
        let output = r#"[{"type":"skip"}]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Skip));
    }

    #[test]
    fn parse_mixed_actions() {
        let t = make_transformer();
        let output = r#"[
            {"type":"upsert","id":1,"document":{"a":1}},
            {"type":"skip"},
            {"type":"delete","id":2}
        ]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::Upsert { .. }));
        assert!(matches!(actions[1], Action::Skip));
        assert!(matches!(actions[2], Action::Delete { .. }));
    }

    // -- handle transform errors --

    #[test]
    fn parse_invalid_json() {
        let t = make_transformer();
        let err = t.parse_actions("not json").unwrap_err();
        assert!(err.to_string().contains("failed to parse transform output"));
    }

    #[test]
    fn parse_unknown_action_type() {
        let t = make_transformer();
        let err = t.parse_actions(r#"[{"type":"explode"}]"#).unwrap_err();
        assert!(err.to_string().contains("failed to parse transform output"));
    }

    #[test]
    fn parse_missing_document_field() {
        let t = make_transformer();
        let err = t
            .parse_actions(r#"[{"type":"upsert","id":1}]"#)
            .unwrap_err();
        assert!(err.to_string().contains("failed to parse transform output"));
    }

    #[test]
    fn parse_invalid_id_type() {
        let t = make_transformer(); // expects Uint
        let err = t
            .parse_actions(r#"[{"type":"upsert","id":"not-a-number","document":{}}]"#)
            .unwrap_err();
        assert!(err.to_string().contains("cannot parse"));
    }

    // -- column reindex --

    #[test]
    fn serialize_with_column_reindex() {
        // Table has columns [id, name, email] in WAL order (indices 0, 1, 2).
        // Config specifies ["name", "id"], so reindex = [1, 0].
        let t = JsTransformer::with_column_reindex(
            PathBuf::from("transform.ts"),
            IdType::Uint,
            vec![1, 0],
        );
        let event = make_event(Operation::Insert, vec!["42", "alice", "alice@example.com"]);
        let id = DocumentId::Uint(42);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        // Should output columns in config order: [name, id]
        assert_eq!(parsed[0]["columns"], json!(["alice", "42"]));
    }

    #[test]
    fn serialize_with_column_reindex_handles_nulls() {
        let t = JsTransformer::with_column_reindex(
            PathBuf::from("transform.ts"),
            IdType::Uint,
            vec![2, 0],
        );
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from("1")),
                    ColumnValue::Null,
                    ColumnValue::Null,
                ],
            }),
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!([null, "1"]));
    }
}
