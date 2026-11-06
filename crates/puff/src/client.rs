use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

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
    /// Last distance metric written to each namespace. turbopuffer rejects any
    /// write to a namespace with vectors that omits `distance_metric`, including
    /// delete-only batches and upserts without a vector. The transform only sets
    /// the metric on rows it embeds, so we remember it and attach it to every
    /// later write for that namespace.
    namespace_metrics: Mutex<HashMap<String, DistanceMetric>>,
}

impl TurbopufferClient {
    pub fn new(api_key: String, region: Option<String>) -> Result<Self, PuffError> {
        let client = match region {
            Some(r) => rs_puff::Client::with_region(&api_key, &r),
            None => rs_puff::Client::new(&api_key),
        };
        Ok(Self::from_inner(client))
    }

    pub fn from_env() -> Result<Self, PuffError> {
        let client = rs_puff::Client::from_env().map_err(|e| PuffError::Client(e.to_string()))?;
        Ok(Self::from_inner(client))
    }

    fn from_inner(inner: rs_puff::Client) -> Self {
        Self {
            inner,
            namespace_metrics: Mutex::new(HashMap::new()),
        }
    }

    fn known_metric(&self, namespace: &str) -> Option<DistanceMetric> {
        self.namespace_metrics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(namespace)
            .copied()
    }

    fn remember_metric(&self, namespace: &str, metric: DistanceMetric) {
        self.namespace_metrics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(namespace.to_string(), metric);
    }

    pub async fn send_batch(&self, namespace: &str, actions: &[Action]) -> Result<(), PuffError> {
        let Some(params) = build_write_params(actions, self.known_metric(namespace))? else {
            return Ok(());
        };
        let metric = params.distance_metric;

        self.inner.namespace(namespace).write(params).await?;

        if let Some(m) = metric {
            self.remember_metric(namespace, m);
        }
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

/// Convert a batch of actions into turbopuffer write params.
///
/// Returns `Ok(None)` when the batch has nothing to write. `known_metric` is the
/// metric previously written to this namespace; it is used when no upsert in
/// the batch sets one.
fn build_write_params(
    actions: &[Action],
    known_metric: Option<DistanceMetric>,
) -> Result<Option<WriteParams>, PuffError> {
    let mut upsert_rows: Vec<HashMap<String, Value>> = Vec::new();
    let mut deletes: Vec<Value> = Vec::new();
    let mut batch_distance_metric: Option<DistanceMetric> = None;
    let mut batch_schema: Option<HashMap<String, Value>> = None;
    let mut first_vector_id: Option<&DocumentId> = None;

    for action in actions {
        match action {
            Action::Upsert {
                id,
                document,
                vector,
                distance_metric,
                schema,
            } => {
                if let Some(raw) = distance_metric.as_deref() {
                    let new = parse_distance_metric(raw).ok_or_else(|| {
                        PuffError::Client(format!(
                            "invalid distance_metric {raw:?} for document {}: \
                             expected \"cosine_distance\" or \"euclidean_squared\"",
                            id_to_value(id),
                        ))
                    })?;
                    match batch_distance_metric {
                        None => batch_distance_metric = Some(new),
                        Some(existing) if existing != new => {
                            return Err(PuffError::Client(format!(
                                "mixed distance metrics in batch: {:?} and {:?}",
                                existing, new,
                            )));
                        }
                        _ => {}
                    }
                }
                if let Some(s) = schema {
                    match &batch_schema {
                        None => batch_schema = Some(s.clone()),
                        Some(existing) if existing != s => {
                            return Err(PuffError::Client("mixed schemas in batch".to_string()));
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
                    first_vector_id.get_or_insert(id);
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
        return Ok(None);
    }

    let distance_metric = batch_distance_metric.or(known_metric);
    if let (Some(id), None) = (first_vector_id, distance_metric) {
        return Err(PuffError::Client(format!(
            "transform returned a vector for document {} without distance_metric: \
             set distance_metric to \"cosine_distance\" or \"euclidean_squared\" \
             on every upsert",
            id_to_value(id),
        )));
    }

    Ok(Some(WriteParams {
        upsert_rows: (!upsert_rows.is_empty()).then_some(upsert_rows),
        deletes: (!deletes.is_empty()).then_some(deletes),
        distance_metric,
        schema: batch_schema,
        ..Default::default()
    }))
}

fn id_to_value(id: &DocumentId) -> Value {
    match id {
        DocumentId::Uint(n) => Value::Number((*n).into()),
        DocumentId::Int(n) => Value::Number((*n).into()),
        DocumentId::Uuid(u) => Value::String(u.to_string()),
        DocumentId::String(s) => Value::String(s.clone()),
    }
}

/// Whether `s` is a distance metric turbopuffer accepts.
pub fn is_valid_distance_metric(s: &str) -> bool {
    parse_distance_metric(s).is_some()
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

    fn upsert(id: u64, vector: Option<Vec<f32>>, metric: Option<&str>) -> Action {
        Action::Upsert {
            id: DocumentId::Uint(id),
            document: json!({"title": "doc"}),
            vector,
            distance_metric: metric.map(str::to_string),
            schema: None,
        }
    }

    #[test]
    fn build_rejects_unknown_distance_metric() {
        let actions = vec![upsert(1, Some(vec![0.1]), Some("cosine"))];
        let msg = build_write_params(&actions, None).unwrap_err().to_string();
        assert!(msg.contains("invalid distance_metric \"cosine\""), "{msg}");
    }

    #[test]
    fn build_rejects_vector_without_metric() {
        let actions = vec![upsert(7, Some(vec![0.1]), None)];
        let err = build_write_params(&actions, None).unwrap_err();
        assert!(!err.is_transient());
        let msg = err.to_string();
        assert!(msg.contains("document 7 without distance_metric"), "{msg}");
    }

    #[test]
    fn build_uses_metric_from_any_upsert_in_batch() {
        let actions = vec![
            upsert(1, None, None),
            upsert(2, Some(vec![0.1]), Some("cosine_distance")),
        ];
        let params = build_write_params(&actions, None).unwrap().unwrap();
        assert_eq!(params.distance_metric, Some(DistanceMetric::CosineDistance));
    }

    #[test]
    fn build_falls_back_to_known_metric() {
        // Delete-only batch and upserts without vectors still need the metric
        // once the namespace has vectors.
        let deletes = vec![Action::Delete {
            id: DocumentId::Uint(1),
        }];
        let params = build_write_params(&deletes, Some(DistanceMetric::CosineDistance))
            .unwrap()
            .unwrap();
        assert_eq!(params.distance_metric, Some(DistanceMetric::CosineDistance));

        let no_vector = vec![upsert(2, None, None)];
        let params = build_write_params(&no_vector, Some(DistanceMetric::EuclideanSquared))
            .unwrap()
            .unwrap();
        assert_eq!(
            params.distance_metric,
            Some(DistanceMetric::EuclideanSquared)
        );

        let vector_no_metric = vec![upsert(3, Some(vec![0.1]), None)];
        let params = build_write_params(&vector_no_metric, Some(DistanceMetric::CosineDistance))
            .unwrap()
            .unwrap();
        assert_eq!(params.distance_metric, Some(DistanceMetric::CosineDistance));
    }

    #[test]
    fn build_without_vectors_or_known_metric_omits_metric() {
        let actions = vec![upsert(1, None, None)];
        let params = build_write_params(&actions, None).unwrap().unwrap();
        assert_eq!(params.distance_metric, None);
    }

    #[test]
    fn build_empty_batch_is_none() {
        assert!(build_write_params(&[Action::Skip], None).unwrap().is_none());
    }

    #[test]
    fn remembers_metric_per_namespace() {
        let client = TurbopufferClient::new("test-key".to_string(), None).unwrap();
        assert_eq!(client.known_metric("a"), None);
        client.remember_metric("a", DistanceMetric::CosineDistance);
        assert_eq!(
            client.known_metric("a"),
            Some(DistanceMetric::CosineDistance)
        );
        assert_eq!(client.known_metric("b"), None);
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
