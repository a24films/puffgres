use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::{delete, get, post},
};
use puff::TurbopufferClient;
use puff::client::{IncludeAttributes, Order, QueryParams, RankBy};
use serde::Deserialize;

const UI_HTML: &str = include_str!("ui.html");

pub async fn run(client: TurbopufferClient, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(client);
    let app = Router::new()
        .route("/", get(index))
        .route("/api/env", get(get_env))
        .route("/api/namespaces", get(list_namespaces))
        .route("/api/namespaces/{name}", delete(delete_namespace))
        .route("/api/query", post(query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    eprintln!("Debug UI running at http://localhost:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(UI_HTML)
}

async fn get_env() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "turbopuffer_namespace_prefix": std::env::var("TURBOPUFFER_NAMESPACE_PREFIX").ok(),
    }))
}

#[derive(Deserialize)]
struct NamespacesQuery {
    prefix: Option<String>,
    cursor: Option<String>,
}

async fn list_namespaces(
    State(client): State<Arc<TurbopufferClient>>,
    Query(params): Query<NamespacesQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let result = client
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
    State(client): State<Arc<TurbopufferClient>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    client.delete_namespace(&name).await.map_err(|e| {
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
    State(client): State<Arc<TurbopufferClient>>,
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

    let result = client.query(&body.namespace, params).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
    })?;

    Ok(Json(serde_json::json!({ "rows": result.rows })))
}
