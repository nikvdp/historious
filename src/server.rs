use crate::search;
use crate::storage::{RecentResultRefInput, Store};
use crate::transport;
use anyhow::{bail, Result};
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    store: Store,
    machine_id: String,
    default_search_mode: search::SearchMode,
    embedder: Option<Arc<dyn crate::embed::Embedder>>,
    embedder_degraded_reason: Option<String>,
}

#[derive(Serialize)]
struct Health {
    ok: bool,
}

pub async fn serve(
    store: Store,
    addr: SocketAddr,
    machine_id: String,
    default_search_mode: search::SearchMode,
    embedder: crate::embed::EmbedderConfig,
) -> Result<()> {
    let (embedder, embedder_degraded_reason) = match embedder.load() {
        Ok(embedder) => (Some(Arc::from(embedder)), None),
        Err(err) => (None, Some(err.to_string())),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/heads", get(heads))
        .route("/search", get(search_endpoint))
        .route("/export", get(export_jsonl))
        .route("/import", post(import_jsonl))
        .with_state(AppState {
            store,
            machine_id,
            default_search_mode,
            embedder,
            embedder_degraded_reason,
        });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("serving sync API on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

async fn heads(
    State(state): State<AppState>,
) -> Result<Json<crate::storage::ArchiveStats>, ServerError> {
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

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: Option<String>,
    limit: Option<usize>,
    sort: Option<String>,
    mode: Option<String>,
    corpus: Option<String>,
    recency_bias: Option<f64>,
    after: Option<String>,
    before: Option<String>,
    machine: Option<String>,
    hostname: Option<String>,
    format: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServerSearchOutput {
    query: String,
    options: ServerSearchOptions,
    degraded_reason: Option<String>,
    results: Vec<ServerSearchResult>,
}

#[derive(Debug, Serialize)]
struct ServerSearchOptions {
    limit: usize,
    sort: String,
    mode: String,
    corpus: String,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    machine: Option<String>,
    hostname: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServerSearchResult {
    history_item_id: Option<String>,
    match_type: search::MatchType,
    event_id: String,
    session_id: String,
    machine_id: String,
    source_kind: String,
    tier: Option<String>,
    kind: String,
    score: f64,
    lexical_rank: Option<usize>,
    semantic_rank: Option<usize>,
    occurred_at: Option<DateTime<Utc>>,
    session_title: Option<String>,
    workspace_values: Vec<String>,
    snippet: String,
}

async fn search_endpoint(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Response, ServerError> {
    let query = params.q.unwrap_or_default();
    if query.trim().is_empty() {
        return Err(anyhow::anyhow!("search query is required").into());
    }
    let limit = params.limit.unwrap_or(25);
    let sort = parse_sort(params.sort.as_deref())?;
    let sort_name = sort_name(sort).to_string();
    let mode = parse_mode(params.mode.as_deref())?.unwrap_or(state.default_search_mode);
    let corpus = parse_corpus(params.corpus.as_deref())?;
    let recency_bias = params.recency_bias.unwrap_or(0.0);
    let after = parse_rfc3339_opt(params.after.as_deref(), "after")?;
    let before = parse_rfc3339_opt(params.before.as_deref(), "before")?;
    let options = search::SearchOptions::new(limit, sort, recency_bias)
        .with_mode(mode)
        .with_corpus(corpus.clone())
        .with_time_window(after, before)
        .with_machine_filter(params.machine.clone(), params.hostname.clone());
    let response = search::search(
        &state.store,
        &query,
        options,
        state.embedder.as_deref(),
        state.embedder_degraded_reason.clone(),
    )?;
    if params.format.as_deref() == Some("fzf") {
        let refs = state
            .store
            .record_recent_result_refs(&recent_ref_inputs(&response.results))?;
        return Ok((
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            server_fzf_rows(&response.results, &refs),
        )
            .into_response());
    }
    let output = ServerSearchOutput {
        query,
        options: ServerSearchOptions {
            limit,
            sort: sort_name,
            mode: mode.as_str().to_string(),
            corpus: corpus.as_csv(),
            recency_bias: recency_bias.clamp(0.0, 1.0),
            after,
            before,
            machine: params.machine,
            hostname: params.hostname,
        },
        degraded_reason: response.degraded_reason,
        results: response
            .results
            .into_iter()
            .map(ServerSearchResult::from)
            .collect(),
    };
    Ok(Json(output).into_response())
}

async fn import_jsonl(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ServerError> {
    let stats = transport::import_jsonl_reader(&state.store, Cursor::new(body))?;
    let projected = search::refresh(&state.store)?;
    let embeddings = search::refresh_embeddings(
        &state.store,
        &state.machine_id,
        state.embedder.as_deref(),
        state.embedder_degraded_reason.clone(),
    )?;
    Ok(Json(serde_json::json!({
        "inserted": stats.inserted,
        "duplicates": stats.duplicates,
        "vectors_indexed": stats.vectors_indexed,
        "embedded": embeddings.embedded,
        "embedding_vectors_indexed": embeddings.vectors_indexed,
        "embedding_degraded_reason": embeddings.degraded_reason,
        "indexed_events": projected
    })))
}

impl From<search::SearchResult> for ServerSearchResult {
    fn from(result: search::SearchResult) -> Self {
        Self {
            history_item_id: result.history_item_id,
            match_type: result.match_type,
            event_id: result.event_id,
            session_id: result.session_id,
            machine_id: result.machine_id,
            source_kind: result.source_kind,
            tier: result.tier,
            kind: result.kind,
            score: result.score,
            lexical_rank: result.lexical_rank,
            semantic_rank: result.semantic_rank,
            occurred_at: result.occurred_at,
            session_title: result.session_title,
            workspace_values: result.workspace_values,
            snippet: result.snippet,
        }
    }
}

fn parse_corpus(value: Option<&str>) -> Result<search::SearchCorpus> {
    value
        .map(search::SearchCorpus::parse)
        .unwrap_or_else(|| Ok(search::SearchCorpus::default()))
}

fn parse_sort(value: Option<&str>) -> Result<search::SortMode> {
    match value.unwrap_or("relevance") {
        "relevance" => Ok(search::SortMode::Relevance),
        "newest" => Ok(search::SortMode::Newest),
        "oldest" => Ok(search::SortMode::Oldest),
        value => bail!("unsupported sort mode: {value}"),
    }
}

fn parse_mode(value: Option<&str>) -> Result<Option<search::SearchMode>> {
    match value {
        None => Ok(None),
        Some("hybrid") => Ok(Some(search::SearchMode::Hybrid)),
        Some("lexical") => Ok(Some(search::SearchMode::Lexical)),
        Some("semantic") => Ok(Some(search::SearchMode::Semantic)),
        Some(value) => bail!("unsupported search mode: {value}"),
    }
}

fn sort_name(sort: search::SortMode) -> &'static str {
    match sort {
        search::SortMode::Relevance => "relevance",
        search::SortMode::Newest => "newest",
        search::SortMode::Oldest => "oldest",
    }
}

fn parse_rfc3339_opt(value: Option<&str>, name: &str) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|err| anyhow::anyhow!("invalid {name} timestamp: {err}"))
        })
        .transpose()
}

fn recent_ref_inputs(results: &[search::SearchResult]) -> Vec<RecentResultRefInput> {
    results
        .iter()
        .map(|result| RecentResultRefInput {
            event_id: result.event_id.clone(),
            session_id: result.session_id.clone(),
            source_kind: result.source_kind.clone(),
            occurred_at: result.occurred_at,
            preview: result.snippet.clone(),
        })
        .collect()
}

fn server_fzf_rows(results: &[search::SearchResult], refs: &[String]) -> String {
    let mut rows = String::new();
    for (idx, result) in results.iter().enumerate() {
        rows.push_str(&server_fzf_row(result, refs.get(idx).map(String::as_str)));
        rows.push('\n');
    }
    rows
}

fn server_fzf_row(result: &search::SearchResult, ref_id: Option<&str>) -> String {
    [
        clean_fzf_field(ref_id.unwrap_or("-")),
        clean_fzf_field(&result.source_kind),
        clean_fzf_field(match result.match_type {
            search::MatchType::Lexical => "lexical",
            search::MatchType::Semantic => "semantic",
            search::MatchType::Hybrid => "hybrid",
        }),
        clean_fzf_field(
            &result
                .occurred_at
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        clean_fzf_field(&result.snippet),
        clean_fzf_field(&result.session_id),
        clean_fzf_field(&result.event_id),
        clean_fzf_field(result.session_title.as_deref().unwrap_or("")),
        clean_fzf_field(&result.workspace_values.join(" ")),
        clean_fzf_field(&result.machine_id),
        clean_fzf_field(result.history_item_id.as_deref().unwrap_or("")),
        clean_fzf_field(server_fzf_open_mode_flag(result)),
    ]
    .join("\t")
}

fn server_fzf_open_mode_flag(result: &search::SearchResult) -> &'static str {
    match result.tier.as_deref() {
        Some("conversation") => "",
        _ => "--full",
    }
}

fn clean_fzf_field(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fzf_rows_include_visible_short_ref_and_hidden_ids() {
        let result = search::SearchResult {
            history_item_id: Some("hi_1".to_string()),
            match_type: search::MatchType::Hybrid,
            event_id: "sc_1234567890abcdef".to_string(),
            session_id: "session_1".to_string(),
            machine_id: "machine_devbox_123".to_string(),
            source_kind: "codex".to_string(),
            tier: Some("conversation".to_string()),
            kind: "user".to_string(),
            score: 0.25,
            lexical_rank: Some(1),
            semantic_rank: Some(2),
            occurred_at: None,
            session_title: Some("Planning Session".to_string()),
            workspace_values: vec!["/tmp/workspace".to_string()],
            snippet: "line one\nline two".to_string(),
        };

        let row = server_fzf_row(&result, Some("ab3f"));
        let fields = row.split('\t').collect::<Vec<_>>();

        assert_eq!(fields.len(), 12);
        assert_eq!(fields[0], "ab3f");
        assert_eq!(fields[1], "codex");
        assert_eq!(fields[2], "hybrid");
        assert_eq!(fields[4], "line one line two");
        assert_eq!(fields[5], "session_1");
        assert_eq!(fields[6], "sc_1234567890abcdef");
        assert_eq!(fields[7], "Planning Session");
        assert_eq!(fields[8], "/tmp/workspace");
        assert_eq!(fields[9], "machine_devbox_123");
        assert_eq!(fields[10], "hi_1");
        assert_eq!(fields[11], "");
    }
}
