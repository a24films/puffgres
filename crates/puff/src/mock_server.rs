//! Mock turbopuffer HTTP server for chaos/integration tests.
//!
//! Accepts writes and stores them in memory. Can also simulate server errors
//! with 500s, trigger rate limiting with 429s, or add artificial delay — all
//! switchable at runtime via the `/test/chaos` endpoint. This lets us test the
//! pipeline's resilience to turbopuffer itself having flaky behavior.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct MockTurbopufferServer {
    state: Arc<ServerState>,
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Sender<bool>,
}

#[derive(Debug)]
struct ServerState {
    store: RwLock<HashMap<String, Vec<WriteRecord>>>,
    chaos: RwLock<ChaosConfig>,
    stats: RwLock<ServerStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WriteRecord {
    pub upsert_count: usize,
    pub delete_count: usize,
    pub body: Value,
}

#[derive(Debug, Clone, Default)]
pub struct ChaosConfig {
    pub mode: ChaosMode,
    pub remaining: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum ChaosMode {
    #[default]
    Healthy,
    Error500,
    RateLimit429,
    SlowResponse(Duration),
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ServerStats {
    pub total_writes: u64,
    pub total_errors: u64,
    pub total_bytes: u64,
}

#[derive(Deserialize)]
struct WriteBody {
    upsert_rows: Option<Vec<Value>>,
    deletes: Option<Vec<Value>>,
}

#[derive(Deserialize)]
struct ChaosRequest {
    mode: String,
    remaining: Option<usize>,
    delay_ms: Option<u64>,
}

impl MockTurbopufferServer {
    pub async fn start() -> Self {
        let state = Arc::new(ServerState {
            store: RwLock::new(HashMap::new()),
            chaos: RwLock::new(ChaosConfig::default()),
            stats: RwLock::new(ServerStats::default()),
        });

        let app = Router::new()
            .route("/v2/namespaces/{ns}", post(handle_write))
            .route("/test/records/{ns}", get(handle_get_records))
            .route("/test/chaos", post(handle_set_chaos))
            .route("/test/stats", get(handle_get_stats))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    shutdown_rx.changed().await.ok();
                })
                .await
                .ok();
        });

        Self {
            state,
            addr,
            shutdown: shutdown_tx,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn records(&self, namespace: &str) -> Vec<WriteRecord> {
        self.state
            .store
            .read()
            .await
            .get(namespace)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn total_upserts(&self, namespace: &str) -> usize {
        self.records(namespace)
            .await
            .iter()
            .map(|r| r.upsert_count)
            .sum()
    }

    pub async fn set_chaos(&self, config: ChaosConfig) {
        *self.state.chaos.write().await = config;
    }

    pub async fn stats(&self) -> ServerStats {
        self.state.stats.read().await.clone()
    }

    pub fn stop(self) {
        self.shutdown.send(true).ok();
    }
}

async fn apply_chaos(state: &ServerState) -> Option<axum::response::Response> {
    let mut chaos = state.chaos.write().await;

    if chaos.mode == ChaosMode::Healthy {
        return None;
    }

    if let Some(ref mut remaining) = chaos.remaining {
        if *remaining == 0 {
            chaos.mode = ChaosMode::Healthy;
            return None;
        }
        *remaining -= 1;
    }

    state.stats.write().await.total_errors += 1;

    match &chaos.mode {
        ChaosMode::Healthy => None,
        ChaosMode::Error500 => Some(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"simulated server error"}"#,
            )
                .into_response(),
        ),
        ChaosMode::RateLimit429 => Some(
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(axum::http::header::RETRY_AFTER, "1")],
                r#"{"error":"rate limited"}"#,
            )
                .into_response(),
        ),
        ChaosMode::SlowResponse(delay) => {
            let delay = *delay;
            drop(chaos);
            tokio::time::sleep(delay).await;
            None
        }
    }
}

async fn handle_write(
    Path(ns): Path<String>,
    State(state): State<Arc<ServerState>>,
    body: String,
) -> impl IntoResponse {
    if let Some(err_response) = apply_chaos(&state).await {
        return err_response;
    }

    let parsed: WriteBody = match serde_json::from_str(&body) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!(r#"{{"error":"invalid json: {e}"}}"#),
            )
                .into_response();
        }
    };

    let upsert_count = parsed.upsert_rows.as_ref().map_or(0, |v| v.len());
    let delete_count = parsed.deletes.as_ref().map_or(0, |v| v.len());
    let body_value: Value = serde_json::from_str(&body).unwrap_or_default();

    let record = WriteRecord {
        upsert_count,
        delete_count,
        body: body_value,
    };

    state
        .store
        .write()
        .await
        .entry(ns)
        .or_default()
        .push(record);

    {
        let mut stats = state.stats.write().await;
        stats.total_writes += 1;
        #[allow(clippy::cast_possible_truncation)]
        {
            stats.total_bytes += body.len() as u64;
        }
    }

    let rows_affected = upsert_count + delete_count;
    (
        StatusCode::OK,
        format!(r#"{{"rows_affected":{rows_affected}}}"#),
    )
        .into_response()
}

async fn handle_get_records(
    Path(ns): Path<String>,
    State(state): State<Arc<ServerState>>,
) -> impl IntoResponse {
    let records = state
        .store
        .read()
        .await
        .get(&ns)
        .cloned()
        .unwrap_or_default();
    axum::Json(records).into_response()
}

async fn handle_set_chaos(
    State(state): State<Arc<ServerState>>,
    axum::Json(req): axum::Json<ChaosRequest>,
) -> impl IntoResponse {
    let mode = match req.mode.as_str() {
        "healthy" => ChaosMode::Healthy,
        "500" => ChaosMode::Error500,
        "429" => ChaosMode::RateLimit429,
        "slow" => ChaosMode::SlowResponse(Duration::from_millis(req.delay_ms.unwrap_or(1000))),
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!(r#"{{"error":"unknown mode: {other}"}}"#),
            )
                .into_response();
        }
    };

    *state.chaos.write().await = ChaosConfig {
        mode,
        remaining: req.remaining,
    };

    (StatusCode::OK, r#"{"status":"chaos updated"}"#).into_response()
}

async fn handle_get_stats(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let stats = state.stats.read().await.clone();
    axum::Json(stats).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn healthy_write_and_query() {
        let server = MockTurbopufferServer::start().await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{}/v2/namespaces/test_ns", server.url()))
            .json(&serde_json::json!({
                "upsert_rows": [{"id": 1, "title": "hello"}],
                "deletes": [2]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let records = server.records("test_ns").await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].upsert_count, 1);
        assert_eq!(records[0].delete_count, 1);
        assert_eq!(server.total_upserts("test_ns").await, 1);

        let stats = server.stats().await;
        assert_eq!(stats.total_writes, 1);

        server.stop();
    }

    #[tokio::test]
    async fn chaos_500_then_recover() {
        let server = MockTurbopufferServer::start().await;
        let client = reqwest::Client::new();

        server
            .set_chaos(ChaosConfig {
                mode: ChaosMode::Error500,
                remaining: Some(2),
            })
            .await;

        let body = serde_json::json!({"upsert_rows": [{"id": 1}]});
        let url = format!("{}/v2/namespaces/ns", server.url());

        let r1 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r1.status(), 500);

        let r2 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 500);

        // remaining exhausted, back to healthy
        let r3 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r3.status(), 200);

        assert_eq!(server.records("ns").await.len(), 1);
        assert_eq!(server.stats().await.total_errors, 2);

        server.stop();
    }

    #[tokio::test]
    async fn chaos_429() {
        let server = MockTurbopufferServer::start().await;
        let client = reqwest::Client::new();

        server
            .set_chaos(ChaosConfig {
                mode: ChaosMode::RateLimit429,
                remaining: Some(1),
            })
            .await;

        let url = format!("{}/v2/namespaces/ns", server.url());
        let body = serde_json::json!({"upsert_rows": [{"id": 1}]});

        let r1 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r1.status(), 429);
        assert!(r1.headers().contains_key("retry-after"));

        let r2 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 200);

        server.stop();
    }

    #[tokio::test]
    async fn multiple_namespaces() {
        let server = MockTurbopufferServer::start().await;
        let client = reqwest::Client::new();

        let body = serde_json::json!({"upsert_rows": [{"id": 1}]});
        client
            .post(format!("{}/v2/namespaces/ns_a", server.url()))
            .json(&body)
            .send()
            .await
            .unwrap();
        client
            .post(format!("{}/v2/namespaces/ns_b", server.url()))
            .json(&body)
            .send()
            .await
            .unwrap();
        client
            .post(format!("{}/v2/namespaces/ns_a", server.url()))
            .json(&body)
            .send()
            .await
            .unwrap();

        assert_eq!(server.records("ns_a").await.len(), 2);
        assert_eq!(server.records("ns_b").await.len(), 1);
        assert_eq!(server.stats().await.total_writes, 3);

        server.stop();
    }
}
