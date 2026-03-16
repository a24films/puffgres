use config::{Config, IdType};
use replication::RelationInfo;

use crate::{CoreError, DocumentId, RowEvent};

// Mapping is the bridge between a Config (a TOML declaring which tables we index)
// and the replication stream. The Router references Mappings so that when a RowEvent
// arrives, it can resolve the relation and find which configs care about that table.

pub struct Mapping {
    pub name: String,
    pub namespace: String,
    pub source_schema: String,
    pub source_table: String,
    pub id_column: String,
    pub id_type: IdType,
    pub columns: Option<Vec<String>>,
}

impl Mapping {
    pub fn from_config(config: &Config) -> Self {
        Self {
            name: config.name.clone(),
            namespace: config.namespace.clone(),
            source_schema: config.source.schema.clone(),
            source_table: config.source.table.clone(),
            id_column: config.id.column.clone(),
            id_type: config.id.id_type.clone(),
            columns: config.columns.clone(),
        }
    }

    pub fn matches(&self, relation: &RelationInfo) -> bool {
        self.source_schema == relation.namespace && self.source_table == relation.name
    }

    // DocumentId::from_value handles JSON value → typed id. This method is the layer
    // before that: raw WAL event → find the id column by name → pull bytes from the
    // tuple → hand off to from_value.
    pub fn extract_id(
        &self,
        event: &RowEvent,
        relation: &RelationInfo,
    ) -> Result<DocumentId, CoreError> {
        let col_index = relation
            .columns
            .iter()
            .position(|c| c.name == self.id_column)
            .ok_or_else(|| {
                CoreError::pipeline(format!(
                    "id column \"{}\" not found in relation \"{}.{}\"",
                    self.id_column, relation.namespace, relation.name
                ))
            })?;

        // Prefer new_tuple (Insert/Update), fall back to old_tuple (Delete).
        let tuple = event
            .new_tuple
            .as_ref()
            .or(event.old_tuple.as_ref())
            .ok_or_else(|| CoreError::pipeline("row event has no tuple data".to_string()))?;

        let col_value = tuple.columns.get(col_index).ok_or_else(|| {
            CoreError::pipeline(format!(
                "column index {col_index} out of bounds (tuple has {} columns)",
                tuple.columns.len()
            ))
        })?;

        let bytes = col_value.as_bytes().ok_or_else(|| {
            let reason = if col_value.is_unchanged() {
                "unchanged (REPLICA IDENTITY may not be FULL)"
            } else {
                "null"
            };
            CoreError::pipeline(format!("id column \"{}\" is {}", self.id_column, reason))
        })?;

        let text = std::str::from_utf8(bytes)
            .map_err(|e| CoreError::pipeline(format!("id column is not valid utf-8: {e}")))?;

        DocumentId::from_text(text, &self.id_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use replication::{ColumnInfo, ColumnValue, Operation, ReplicaIdentity, TupleData};
    use std::sync::Arc;

    fn load_fixture(name: &str) -> Config {
        let path = format!("tests/fixtures/{name}.toml");
        toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn test_relation() -> RelationInfo {
        RelationInfo {
            id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnInfo {
                    part_of_key: true,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnInfo {
                    part_of_key: false,
                    name: "name".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    #[test]
    fn from_config_fields() {
        let mapping = Mapping::from_config(&load_fixture("valid"));

        assert_eq!(mapping.name, "users");
        assert_eq!(mapping.namespace, "users");
        assert_eq!(mapping.source_schema, "public");
        assert_eq!(mapping.source_table, "users");
        assert_eq!(mapping.id_column, "id");
        assert_eq!(mapping.id_type, IdType::Uint);
        assert!(mapping.columns.is_none());
    }

    #[test]
    fn matches_correct_schema_table() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        assert!(mapping.matches(&test_relation()));
    }

    #[test]
    fn no_match_wrong_schema() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        let mut rel = test_relation();
        rel.namespace = "other".to_string();
        assert!(!mapping.matches(&rel));
    }

    #[test]
    fn no_match_wrong_table() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        let mut rel = test_relation();
        rel.name = "orders".to_string();
        assert!(!mapping.matches(&rel));
    }

    #[test]
    fn extract_id_insert() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        let event = RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from_static(b"42")),
                    ColumnValue::Text(Bytes::from_static(b"alice")),
                ],
            })),
            old_tuple: None,
        };

        let id = mapping.extract_id(&event, &test_relation()).unwrap();
        assert_eq!(id, DocumentId::Uint(42));
    }

    #[test]
    fn extract_id_delete_uses_old_tuple() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        let event = RowEvent {
            relation_id: 16384,
            operation: Operation::Delete,
            new_tuple: None,
            old_tuple: Some(Arc::new(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from_static(b"99")),
                    ColumnValue::Text(Bytes::from_static(b"bob")),
                ],
            })),
        };

        let id = mapping.extract_id(&event, &test_relation()).unwrap();
        assert_eq!(id, DocumentId::Uint(99));
    }

    #[test]
    fn extract_id_missing_column() {
        let mut config = load_fixture("valid");
        config.id.column = "missing_col".to_string();
        let mapping = Mapping::from_config(&config);

        let event = RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from_static(b"1"))],
            })),
            old_tuple: None,
        };

        let err = mapping.extract_id(&event, &test_relation()).unwrap_err();
        assert!(err.to_string().contains("missing_col"));
    }

    #[test]
    fn extract_id_no_tuple_data() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        let event = RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: None,
            old_tuple: None,
        };

        let err = mapping.extract_id(&event, &test_relation()).unwrap_err();
        assert!(err.to_string().contains("no tuple data"));
    }

    #[test]
    fn extract_id_null_column() {
        let mapping = Mapping::from_config(&load_fixture("valid"));
        let event = RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![
                    ColumnValue::Null,
                    ColumnValue::Text(Bytes::from_static(b"alice")),
                ],
            })),
            old_tuple: None,
        };

        let err = mapping.extract_id(&event, &test_relation()).unwrap_err();
        assert!(err.to_string().contains("is null"));
    }
}
