use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{Html, Json},
    routing::get,
};
use state::StateDb;

const UI_HTML: &str = include_str!("ui.html");

struct AppState {
    db: StateDb,
}

pub async fn run(db: StateDb, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = Arc::new(AppState { db });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/state", get(get_state))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    eprintln!("Inspect server running at http://0.0.0.0:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(UI_HTML)
}

async fn get_state(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let db = &state.db;
    let err = |e: state::StateError| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());

    let configs = db.list_configs().await.map_err(err)?;
    let checkpoints = db.list_streaming_checkpoints().await.map_err(err)?;
    let (dlq_retryable, dlq_permanent) = db.dlq_count_by_kind(None).await.map_err(err)?;
    let dlq_entries = db.list_dlq_entries(None, 50).await.map_err(err)?;

    let mut backfills = Vec::new();
    for config in &configs {
        if let Some(p) = db.get_backfill_progress(&config.name).await.map_err(err)? {
            backfills.push(serde_json::json!({
                "config_name": p.config_name,
                "status": p.status.to_string(),
                "processed_rows": p.processed_rows,
                "total_rows": p.total_rows,
                "last_id": p.last_id,
                "started_at": p.started_at.map(|dt| dt.to_rfc3339()),
                "completed_at": p.completed_at.map(|dt| dt.to_rfc3339()),
                "error_message": p.error_message,
            }));
        }
    }

    Ok(Json(serde_json::json!({
        "state_schema": state.db.schema(),
        "configs": configs.iter().map(|c| serde_json::json!({
            "name": c.name,
            "namespace": c.namespace,
            "content_hash": c.content_hash,
            "transform_hash": c.transform_hash,
            "applied_at": c.applied_at.to_rfc3339(),
            "tombstoned": c.tombstone_applied_at.is_some(),
            "tombstone_applied_at": c.tombstone_applied_at.map(|dt| dt.to_rfc3339()),
            "namespace_prefix": c.namespace_prefix,
        })).collect::<Vec<_>>(),
        "streaming_checkpoints": checkpoints.iter().map(|cp| serde_json::json!({
            "config_name": cp.config_name,
            "lsn": format!("{:X}/{:X}", cp.lsn >> 32, cp.lsn & 0xFFFF_FFFF),
            "events_processed": cp.events_processed,
            "updated_at": cp.updated_at.to_rfc3339(),
        })).collect::<Vec<_>>(),
        "backfills": backfills,
        "dlq": serde_json::json!({
            "retryable": dlq_retryable,
            "permanent": dlq_permanent,
            "recent_entries": dlq_entries.iter().map(|e| serde_json::json!({
                "id": e.id,
                "config_name": e.config_name,
                "operation": e.operation.as_ref().map(|o| o.to_string()),
                "error_message": e.error_message,
                "error_kind": format!("{:?}", e.error_kind),
                "retry_count": e.retry_count,
                "created_at": e.created_at.to_rfc3339(),
            })).collect::<Vec<_>>(),
        }),
    })))
}
