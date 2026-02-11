use config::IdType;
use derive_more::Display;
use serde_json::Value;

use crate::CoreError;

/// A typed document identifier extracted from a row's primary key column.
#[derive(Debug, Clone, PartialEq, Display)]
pub enum DocumentId {
    Uint(u64),
    Int(i64),
    Uuid(uuid::Uuid),
    String(String),
}

impl DocumentId {
    /// Parse a JSON value into a DocumentId according to the configured IdType.
    pub fn from_value(value: &Value, id_type: &IdType) -> Result<Self, CoreError> {
        match id_type {
            IdType::Uint => match value {
                Value::Number(n) => n.as_u64().map(DocumentId::Uint).ok_or_else(|| {
                    CoreError::Pipeline(format!("expected unsigned integer, got {value}"))
                }),
                Value::String(s) => s
                    .parse::<u64>()
                    .map(DocumentId::Uint)
                    .map_err(|e| CoreError::Pipeline(format!("cannot parse \"{s}\" as uint: {e}"))),
                _ => Err(CoreError::Pipeline(format!(
                    "expected uint-compatible value, got {value}"
                ))),
            },
            IdType::Int => match value {
                Value::Number(n) => n.as_i64().map(DocumentId::Int).ok_or_else(|| {
                    CoreError::Pipeline(format!("expected signed integer, got {value}"))
                }),
                Value::String(s) => s
                    .parse::<i64>()
                    .map(DocumentId::Int)
                    .map_err(|e| CoreError::Pipeline(format!("cannot parse \"{s}\" as int: {e}"))),
                _ => Err(CoreError::Pipeline(format!(
                    "expected int-compatible value, got {value}"
                ))),
            },
            IdType::Uuid => match value {
                Value::String(s) => s
                    .parse::<uuid::Uuid>()
                    .map(DocumentId::Uuid)
                    .map_err(|e| CoreError::Pipeline(format!("cannot parse \"{s}\" as uuid: {e}"))),
                _ => Err(CoreError::Pipeline(format!(
                    "expected uuid string, got {value}"
                ))),
            },
            IdType::String => match value {
                Value::String(s) => Ok(DocumentId::String(s.clone())),
                Value::Number(n) => Ok(DocumentId::String(n.to_string())),
                _ => Err(CoreError::Pipeline(format!(
                    "expected string-compatible value, got {value}"
                ))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uint_from_number() {
        let id = DocumentId::from_value(&json!(42), &IdType::Uint).unwrap();
        assert_eq!(id, DocumentId::Uint(42));
    }

    #[test]
    fn uint_from_string() {
        let id = DocumentId::from_value(&json!("123"), &IdType::Uint).unwrap();
        assert_eq!(id, DocumentId::Uint(123));
    }

    #[test]
    fn uint_rejects_negative() {
        let result = DocumentId::from_value(&json!(-1), &IdType::Uint);
        assert!(result.is_err());
    }

    #[test]
    fn uint_rejects_non_numeric_string() {
        let result = DocumentId::from_value(&json!("abc"), &IdType::Uint);
        assert!(result.is_err());
    }

    #[test]
    fn int_from_number() {
        let id = DocumentId::from_value(&json!(-7), &IdType::Int).unwrap();
        assert_eq!(id, DocumentId::Int(-7));
    }

    #[test]
    fn int_from_string() {
        let id = DocumentId::from_value(&json!("-99"), &IdType::Int).unwrap();
        assert_eq!(id, DocumentId::Int(-99));
    }

    #[test]
    fn uuid_from_string() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = DocumentId::from_value(&json!(uuid_str), &IdType::Uuid).unwrap();
        assert_eq!(id, DocumentId::Uuid(uuid_str.parse().unwrap()));
    }

    #[test]
    fn uuid_rejects_invalid() {
        let result = DocumentId::from_value(&json!("not-a-uuid"), &IdType::Uuid);
        assert!(result.is_err());
    }

    #[test]
    fn uuid_rejects_number() {
        let result = DocumentId::from_value(&json!(42), &IdType::Uuid);
        assert!(result.is_err());
    }

    #[test]
    fn string_from_string() {
        let id = DocumentId::from_value(&json!("hello"), &IdType::String).unwrap();
        assert_eq!(id, DocumentId::String("hello".to_string()));
    }

    #[test]
    fn string_from_number() {
        let id = DocumentId::from_value(&json!(42), &IdType::String).unwrap();
        assert_eq!(id, DocumentId::String("42".to_string()));
    }

    #[test]
    fn string_rejects_object() {
        let result = DocumentId::from_value(&json!({"a": 1}), &IdType::String);
        assert!(result.is_err());
    }

    #[test]
    fn to_string_uint() {
        assert_eq!(DocumentId::Uint(42).to_string(), "42");
    }

    #[test]
    fn to_string_int() {
        assert_eq!(DocumentId::Int(-7).to_string(), "-7");
    }

    #[test]
    fn to_string_uuid() {
        let u: uuid::Uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        assert_eq!(
            DocumentId::Uuid(u).to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn to_string_string() {
        assert_eq!(DocumentId::String("hello".to_string()).to_string(), "hello");
    }
}
