use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use rs_puff::DistanceMetric;
use rs_puff::params::WriteParams;
use serde_json::Value;

use puffgres_core::{Action, BackfillSink, CoreError, DocumentId};

use crate::PuffError;

pub use rs_puff::params::{IncludeAttributes, QueryParams};
pub use rs_puff::responses::{NamespaceSummary, NamespacesResponse, QueryResponse};
pub use rs_puff::{Order, RankBy};

pub struct TurbopufferClient {
    inner: rs_puff::Client,
}

impl TurbopufferClient {
    pub fn new(api_key: String, region: Option<String>) -> Result<Self, PuffError> {
        let client = match region {
            Some(r) => rs_puff::Client::with_region(&api_key, &r),
            None => rs_puff::Client::new(&api_key),
        };
        Ok(Self { inner: client })
    }

    pub fn with_base_url(api_key: String, base_url: String) -> Self {
        Self {
            inner: rs_puff::Client::with_base_url(api_key, base_url),
        }
    }

    pub fn from_env() -> Result<Self, PuffError> {
        let client = rs_puff::Client::from_env().map_err(|e| PuffError::Client(e.to_string()))?;
        Ok(Self { inner: client })
    }

    pub async fn send_batch(&self, namespace: &str, actions: &[Action]) -> Result<(), PuffError> {
        let mut upsert_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut deletes: Vec<Value> = Vec::new();
        let mut batch_distance_metric: Option<DistanceMetric> = None;
        let mut batch_schema: Option<HashMap<String, Value>> = None;

        for action in actions {
            match action {
                Action::Upsert {
                    id,
                    document,
                    vector,
                    distance_metric,
                    schema,
                } => {
                    let parsed = distance_metric.as_deref().and_then(parse_distance_metric);
                    match (&batch_distance_metric, &parsed) {
                        (None, _) => batch_distance_metric = parsed,
                        (Some(existing), Some(new)) if existing != new => {
                            return Err(PuffError::Client(format!(
                                "mixed distance metrics in batch: {:?} and {:?}",
                                existing, new,
                            )));
                        }
                        _ => {}
                    }
                    if let Some(s) = schema {
                        match &batch_schema {
                            None => batch_schema = Some(s.clone()),
                            Some(existing) if existing != s => {
                                return Err(PuffError::Client(
                                    "mixed schemas in batch".to_string(),
                                ));
                            }
                            _ => {}
                        }
                    }
                    let mut row = match document {
                        Value::Object(map) => map
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect::<HashMap<String, Value>>(),
                        other => {
                            return Err(PuffError::Client(format!(
                                "document must be a JSON object, got {}",
                                match other {
                                    Value::Array(_) => "array",
                                    Value::String(_) => "string",
                                    Value::Number(_) => "number",
                                    Value::Bool(_) => "bool",
                                    Value::Null => "null",
                                    Value::Object(_) => unreachable!(),
                                }
                            )));
                        }
                    };
                    row.insert("id".to_string(), id_to_value(id));
                    if let Some(vec) = vector {
                        row.insert(
                            "vector".to_string(),
                            Value::Array(vec.iter().map(|f| json_f32(*f)).collect()),
                        );
                    }
                    upsert_rows.push(row);
                }
                Action::Delete { id } => {
                    deletes.push(id_to_value(id));
                }
                Action::Skip => {}
            }
        }

        if upsert_rows.is_empty() && deletes.is_empty() {
            return Ok(());
        }

        let params = WriteParams {
            upsert_rows: if upsert_rows.is_empty() {
                None
            } else {
                Some(upsert_rows)
            },
            deletes: if deletes.is_empty() {
                None
            } else {
                Some(deletes)
            },
            distance_metric: batch_distance_metric,
            schema: batch_schema,
            ..Default::default()
        };

        self.inner.namespace(namespace).write(params).await?;
        Ok(())
    }

    pub async fn query(
        &self,
        namespace: &str,
        params: QueryParams,
    ) -> Result<QueryResponse, PuffError> {
        let result = self.inner.namespace(namespace).query(params).await?;
        Ok(result)
    }

    pub async fn list_namespaces(
        &self,
        prefix: Option<String>,
        cursor: Option<String>,
    ) -> Result<NamespacesResponse, PuffError> {
        let params = rs_puff::NamespacesParams {
            prefix,
            cursor,
            ..Default::default()
        };
        let result = self.inner.namespaces(params).await?;
        Ok(result)
    }

    pub async fn delete_namespace(&self, namespace: &str) -> Result<(), PuffError> {
        self.inner.namespace(namespace).delete_all().await?;
        Ok(())
    }
}

impl BackfillSink for TurbopufferClient {
    fn write<'a>(
        &'a self,
        namespace: &'a str,
        actions: &'a [Action],
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send + 'a>> {
        Box::pin(async move {
            self.send_batch(namespace, actions).await.map_err(|e| {
                let puff_err: PuffError = e.into();
                CoreError::pipeline_transient(puff_err.to_string(), puff_err.is_transient())
            })
        })
    }
}

fn id_to_value(id: &DocumentId) -> Value {
    match id {
        DocumentId::Uint(n) => Value::Number((*n).into()),
        DocumentId::Int(n) => Value::Number((*n).into()),
        DocumentId::Uuid(u) => Value::String(u.to_string()),
        DocumentId::String(s) => Value::String(s.clone()),
    }
}

fn parse_distance_metric(s: &str) -> Option<DistanceMetric> {
    match s {
        "cosine_distance" => Some(DistanceMetric::CosineDistance),
        "euclidean_squared" => Some(DistanceMetric::EuclideanSquared),
        _ => None,
    }
}

fn json_f32(f: f32) -> Value {
    serde_json::Number::from_f64(f as f64)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn client_construction() {
        let client = TurbopufferClient::new("test-key".to_string(), None);
        assert!(client.is_ok());
    }

    #[test]
    fn client_construction_with_region() {
        let client = TurbopufferClient::new("test-key".to_string(), Some("us-east-1".to_string()));
        assert!(client.is_ok());
    }

    #[test]
    fn id_to_value_uint() {
        let val = id_to_value(&DocumentId::Uint(42));
        assert_eq!(val, json!(42));
    }

    #[test]
    fn id_to_value_int() {
        let val = id_to_value(&DocumentId::Int(-7));
        assert_eq!(val, json!(-7));
    }

    #[test]
    fn id_to_value_uuid() {
        let u: uuid::Uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let val = id_to_value(&DocumentId::Uuid(u));
        assert_eq!(val, json!("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn id_to_value_string() {
        let val = id_to_value(&DocumentId::String("abc".to_string()));
        assert_eq!(val, json!("abc"));
    }

    #[test]
    fn json_f32_normal() {
        let val = json_f32(1.5);
        assert_eq!(val, json!(1.5));
    }

    #[test]
    fn json_f32_nan() {
        let val = json_f32(f32::NAN);
        assert_eq!(val, Value::Null);
    }

    #[tokio::test]
    async fn send_batch_rejects_mixed_distance_metrics() {
        let client = TurbopufferClient::new("test-key".to_string(), None).unwrap();
        let actions = vec![
            Action::Upsert {
                id: DocumentId::Uint(1),
                document: json!({"title": "doc a"}),
                vector: Some(vec![0.1, 0.2, 0.3]),
                distance_metric: Some("cosine_distance".to_string()),
                schema: None,
            },
            Action::Upsert {
                id: DocumentId::Uint(2),
                document: json!({"title": "doc b"}),
                vector: Some(vec![0.4, 0.5, 0.6]),
                distance_metric: Some("euclidean_squared".to_string()),
                schema: None,
            },
        ];
        let result = client.send_batch("ns", &actions).await;
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("mixed distance metrics"),
            "expected mixed distance metrics error, got: {msg}",
        );
    }

    #[tokio::test]
    #[ignore] // requires real API key
    async fn integration_send_batch() {
        let client = TurbopufferClient::from_env().expect("TURBOPUFFER_API_KEY must be set");
        let actions = vec![
            Action::Upsert {
                id: DocumentId::Uint(1),
                document: json!({"title": "test doc"}),
                vector: Some(vec![0.1, 0.2, 0.3]),
                distance_metric: Some("cosine_distance".to_string()),
                schema: None,
            },
            Action::Delete {
                id: DocumentId::Uint(2),
            },
            Action::Skip,
        ];
        let result = client.send_batch("puffgres-test", &actions).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // requires real API key
    async fn integration_delete_namespace() {
        let client = TurbopufferClient::from_env().expect("TURBOPUFFER_API_KEY must be set");
        let result = client.delete_namespace("puffgres-test").await;
        assert!(result.is_ok());
    }
}
