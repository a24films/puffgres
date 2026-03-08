use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        Html, IntoResponse, Json,
        sse::{Event, Sse},
    },
    routing::{delete, get, post},
};
use puff::TurbopufferClient;
use puff::client::{IncludeAttributes, Order, QueryParams, RankBy};
use replication::{ColumnValue, ReplicationStream, ReplicationStreamConfig, WalMessage};
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

const UI_HTML: &str = include_str!("ui.html");

struct AppState {
    client: TurbopufferClient,
    replication_tx: Option<broadcast::Sender<String>>,
}

pub async fn run(
    client: TurbopufferClient,
    port: u16,
    replication_config: Option<ReplicationStreamConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let replication_tx = if let Some(config) = replication_config {
        let (tx, _rx) = broadcast::channel(1024);
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            run_replication_loop(config, tx_clone).await;
        });
        Some(tx)
    } else {
        None
    };

    let state = Arc::new(AppState {
        client,
        replication_tx,
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/env", get(get_env))
        .route("/api/namespaces", get(list_namespaces))
        .route("/api/namespaces/{name}", delete(delete_namespace))
        .route("/api/query", post(query))
        .route("/api/replication/events", get(replication_events))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    eprintln!("Debug UI running at http://localhost:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_replication_loop(config: ReplicationStreamConfig, tx: broadcast::Sender<String>) {
    loop {
        match run_replication_stream(&config, &tx).await {
            Ok(()) => {
                let _ = tx.send(
                    serde_json::json!({"type": "status", "message": "Stream ended"}).to_string(),
                );
                break;
            }
            Err(e) => {
                let msg = e.to_string();
                eprintln!("Replication stream error: {msg}");
                let _ = tx.send(serde_json::json!({"type": "error", "message": msg}).to_string());
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn run_replication_stream(
    config: &ReplicationStreamConfig,
    tx: &broadcast::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream_config = ReplicationStreamConfig {
        connection_string: config.connection_string.clone(),
        slot_name: config.slot_name.clone(),
        publication_name: config.publication_name.clone(),
        start_lsn: config.start_lsn,
        status_interval: config.status_interval,
        max_transaction_events: None,
        watched_columns: HashMap::new(),
    };

    let mut stream = ReplicationStream::connect(stream_config).await?;
    let _ = tx.send(
        serde_json::json!({"type": "status", "message": "Connected to Postgres replication"})
            .to_string(),
    );

    loop {
        let msg = match stream.recv_raw().await? {
            Some(msg) => msg,
            None => return Ok(()),
        };

        let event_json = match &msg {
            WalMessage::Begin(info) => {
                serde_json::json!({
                    "type": "begin",
                    "xid": info.xid,
                    "lsn": format_lsn(info.final_lsn),
                })
            }
            WalMessage::Commit(info) => {
                serde_json::json!({
                    "type": "commit",
                    "lsn": format_lsn(info.end_lsn),
                })
            }
            WalMessage::Relation(info) => {
                let cols: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
                serde_json::json!({
                    "type": "relation",
                    "table": format!("{}.{}", info.namespace, info.name),
                    "columns": cols,
                })
            }
            WalMessage::Insert(ins) => {
                let relation = stream.relation_cache().get(ins.relation_id);
                serde_json::json!({
                    "type": "insert",
                    "table": format_table(relation),
                    "new": format_tuple(&ins.tuple, relation),
                })
            }
            WalMessage::Update(upd) => {
                let relation = stream.relation_cache().get(upd.relation_id);
                serde_json::json!({
                    "type": "update",
                    "table": format_table(relation),
                    "new": format_tuple(&upd.new_tuple, relation),
                    "old": upd.old_tuple.as_ref().map(|t| format_tuple(t, relation)),
                })
            }
            WalMessage::Delete(del) => {
                let relation = stream.relation_cache().get(del.relation_id);
                serde_json::json!({
                    "type": "delete",
                    "table": format_table(relation),
                    "old": format_tuple(&del.old_tuple, relation),
                })
            }
            WalMessage::Truncate(trunc) => {
                let tables: Vec<String> = trunc
                    .relation_ids
                    .iter()
                    .map(|id| format_table(stream.relation_cache().get(*id)))
                    .collect();
                serde_json::json!({
                    "type": "truncate",
                    "tables": tables,
                })
            }
            WalMessage::Other(tag) => {
                serde_json::json!({
                    "type": "other",
                    "tag": *tag,
                })
            }
        };

        let _ = tx.send(event_json.to_string());
    }
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

fn format_table(relation: Option<&replication::RelationInfo>) -> String {
    relation
        .map(|r| format!("{}.{}", r.namespace, r.name))
        .unwrap_or_else(|| "?".to_string())
}

fn format_tuple(
    tuple: &replication::TupleData,
    relation: Option<&replication::RelationInfo>,
) -> serde_json::Value {
    let columns: Vec<serde_json::Value> = tuple
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let name = relation
                .and_then(|r| r.columns.get(i))
                .map(|c| c.name.as_str())
                .unwrap_or("?");
            let value = match col {
                ColumnValue::Text(b) => String::from_utf8_lossy(b).to_string(),
                ColumnValue::Binary(b) => format!("(binary: {} bytes)", b.len()),
                ColumnValue::Null => "NULL".to_string(),
                ColumnValue::Unchanged => "(unchanged)".to_string(),
            };
            serde_json::json!({"name": name, "value": value})
        })
        .collect();
    serde_json::json!(columns)
}

async fn replication_events(
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let tx = state
        .replication_tx
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let rx = tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|r: Result<String, _>| r.ok())
        .map(|msg| Ok::<_, Infallible>(Event::default().data(msg)));
    Ok(Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn index() -> Html<&'static str> {
    Html(UI_HTML)
}

async fn get_env(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "turbopuffer_namespace_prefix": std::env::var("TURBOPUFFER_NAMESPACE_PREFIX").ok(),
        "replication_enabled": state.replication_tx.is_some(),
    }))
}

#[derive(Deserialize)]
struct NamespacesQuery {
    prefix: Option<String>,
    cursor: Option<String>,
}

async fn list_namespaces(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NamespacesQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let result = state
        .client
        .list_namespaces(params.prefix, params.cursor)
        .await
        .map_err(|e| {
            let msg = format!(
                "{}. You need an API key with admin permissions to list namespaces.",
                e,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": msg })),
            )
        })?;

    let namespaces: Vec<serde_json::Value> = result
        .namespaces
        .iter()
        .map(|ns| serde_json::json!({ "id": ns.id }))
        .collect();

    Ok(Json(serde_json::json!({
        "namespaces": namespaces,
        "next_cursor": result.next_cursor,
    })))
}

async fn delete_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    state.client.delete_namespace(&name).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
    })?;

    Ok(Json(serde_json::json!({ "success": true })))
}

#[derive(Deserialize)]
struct QueryBody {
    namespace: String,
    limit: Option<u64>,
}

async fn query(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QueryBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let params = QueryParams {
        rank_by: Some(RankBy::Attribute {
            attr: "id".to_string(),
            order: Order::Asc,
        }),
        top_k: Some(body.limit.unwrap_or(10)),
        include_attributes: Some(IncludeAttributes::All(true)),
        ..Default::default()
    };

    let result = state
        .client
        .query(&body.namespace, params)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
        })?;

    Ok(Json(serde_json::json!({ "rows": result.rows })))
}
