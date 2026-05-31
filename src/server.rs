use crate::search;
use crate::storage::Store;
use crate::transport;
use anyhow::Result;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::io::Cursor;
use std::net::SocketAddr;

#[derive(Clone)]
struct AppState {
    store: Store,
}

#[derive(Serialize)]
struct Health {
    ok: bool,
}

pub async fn serve(store: Store, addr: SocketAddr) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/heads", get(heads))
        .route("/export", get(export_jsonl))
        .route("/import", post(import_jsonl))
        .with_state(AppState { store });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("serving sync API on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

async fn heads(State(state): State<AppState>) -> Result<Json<crate::storage::ArchiveStats>, ServerError> {
    Ok(Json(state.store.stats()?))
}

async fn export_jsonl(State(state): State<AppState>) -> Result<Response, ServerError> {
    let mut body = Vec::new();
    transport::export_jsonl(&state.store, &mut body)?;
    Ok((
        [(header::CONTENT_TYPE, "application/x-ndjson; charset=utf-8")],
        body,
    )
        .into_response())
}

async fn import_jsonl(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ServerError> {
    let stats = transport::import_jsonl_reader(&state.store, Cursor::new(body))?;
    let projected = search::refresh(&state.store)?;
    Ok(Json(serde_json::json!({
        "inserted": stats.inserted,
        "duplicates": stats.duplicates,
        "projected_events": projected
    })))
}

struct ServerError(anyhow::Error);

impl<E> From<E> for ServerError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("{}\n", self.0),
        )
            .into_response()
    }
}

