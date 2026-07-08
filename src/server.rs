use crate::search;
use crate::storage::{RecentResultRefInput, Store};
use crate::transport;
use anyhow::{bail, Result};
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
struct AppState {
    store: Store,
    machine_id: String,
    default_search_mode: search::SearchMode,
    embeddings_enabled: bool,
    embedder: Arc<RwLock<ServerEmbedderState>>,
}

struct ServerEmbedderState {
    embedder: Option<Arc<dyn crate::embed::Embedder>>,
    degraded_reason: Option<String>,
}

impl ServerEmbedderState {
    fn loading() -> Self {
        Self {
            embedder: None,
            degraded_reason: Some(
                "query embedder is still loading; using lexical search only".to_string(),
            ),
        }
    }

    fn ready(embedder: Box<dyn crate::embed::Embedder>) -> Self {
        Self {
            embedder: Some(Arc::from(embedder)),
            degraded_reason: None,
        }
    }

    fn degraded(reason: String) -> Self {
        Self {
            embedder: None,
            degraded_reason: Some(reason),
        }
    }

    fn disabled() -> Self {
        Self {
            embedder: None,
            degraded_reason: None,
        }
    }

    fn snapshot(&self) -> (Option<Arc<dyn crate::embed::Embedder>>, Option<String>) {
        (self.embedder.clone(), self.degraded_reason.clone())
    }
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
    let embeddings_enabled = !embedder.is_disabled();
    let embedder_state = Arc::new(RwLock::new(if embeddings_enabled {
        ServerEmbedderState::loading()
    } else {
        ServerEmbedderState::disabled()
    }));
    if embeddings_enabled {
        load_embedder_in_background(embedder, Arc::clone(&embedder_state));
    }
    let app = Router::new()
        .route("/health", get(health))
        .route("/heads", get(heads))
        .route("/search", get(search_endpoint))
        .route("/show", get(show_endpoint))
        .route("/transcript", get(transcript_endpoint))
        .route("/export", get(export_jsonl))
        .route("/import", post(import_jsonl))
        .with_state(AppState {
            store,
            machine_id,
            default_search_mode,
            embeddings_enabled,
            embedder: embedder_state,
        });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("serving sync API on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

fn load_embedder_in_background(
    config: crate::embed::EmbedderConfig,
    state: Arc<RwLock<ServerEmbedderState>>,
) {
    let loader_state = Arc::clone(&state);
    let spawn_result = std::thread::Builder::new()
        .name("histo-embedder-loader".to_string())
        .spawn(move || {
            let next_state = match config.load() {
                Ok(embedder) => ServerEmbedderState::ready(embedder),
                Err(err) => ServerEmbedderState::degraded(err.to_string()),
            };
            let mut guard = loader_state
                .write()
                .expect("server embedder state lock poisoned");
            *guard = next_state;
        });
    if let Err(err) = spawn_result {
        let mut guard = state.write().expect("server embedder state lock poisoned");
        *guard = ServerEmbedderState::degraded(format!("starting query embedder loader: {err}"));
    }
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
    transport::export_jsonl_with_options(
        &state.store,
        transport::ExportOptions {
            include_embeddings: state.embeddings_enabled,
            ..Default::default()
        },
        &mut body,
    )?;
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
    project: Option<String>,
    machine: Option<String>,
    hostname: Option<String>,
    show_duplicates: Option<bool>,
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
    project: Option<String>,
    machine: Option<String>,
    hostname: Option<String>,
    show_duplicates: bool,
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
    duplicate_group: Vec<search::DuplicateSearchMember>,
}

#[derive(Debug, Deserialize)]
struct ShowParams {
    event: Option<String>,
    before: Option<usize>,
    after: Option<usize>,
    full: Option<bool>,
    verbose: Option<bool>,
    color: Option<String>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TranscriptParams {
    session: Option<String>,
    at: Option<String>,
    full: Option<bool>,
    verbose: Option<bool>,
    color: Option<String>,
    format: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServerViewOutput {
    view: &'static str,
    session_id: String,
    event_id: Option<String>,
    full: bool,
    content: String,
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
    let show_duplicates = params.show_duplicates.unwrap_or(false);
    let after = parse_rfc3339_opt(params.after.as_deref(), "after")?;
    let before = parse_rfc3339_opt(params.before.as_deref(), "before")?;
    let options = search::SearchOptions::new(limit, sort, recency_bias)
        .with_mode(mode)
        .with_corpus(corpus.clone())
        .with_show_duplicates(show_duplicates)
        .with_time_window(after, before)
        .with_machine_filter(params.machine.clone(), params.hostname.clone())
        .with_workspace_scope(params.project.clone());
    let (embedder, embedder_degraded_reason) = state
        .embedder
        .read()
        .expect("server embedder state lock poisoned")
        .snapshot();
    let response = search::search(
        &state.store,
        &query,
        options,
        embedder.as_deref(),
        embedder_degraded_reason,
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
            project: params.project,
            machine: params.machine,
            hostname: params.hostname,
            show_duplicates,
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

async fn show_endpoint(
    State(state): State<AppState>,
    Query(params): Query<ShowParams>,
) -> Result<Response, ServerError> {
    let event_id = required_query(params.event, "event")?;
    let before = params.before.unwrap_or(3);
    let after = params.after.unwrap_or(5);
    let full = params.full.unwrap_or(false);
    let verbose = params.verbose.unwrap_or(false);
    let color = parse_color_param(params.color.as_deref())?;
    let rendered = if full {
        let context = state
            .store
            .events_around_event(&event_id, before, after)?
            .ok_or_else(|| ServerError::not_found(format!("event not found: {event_id}")))?;
        let metadata = view_metadata_for_event(&state.store, &context.target_event, verbose)?;
        let content = crate::transcript::render_context(&context, &metadata, color);
        ServerViewOutput {
            view: "show",
            session_id: context.session.id,
            event_id: Some(context.target_event.id),
            full,
            content,
        }
    } else {
        let context = state
            .store
            .history_items_around_event(&event_id, before, after)?
            .ok_or_else(|| ServerError::not_found(format!("event not found: {event_id}")))?;
        let metadata = if let Some(event) = &context.target_event {
            view_metadata_for_event(&state.store, event, verbose)?
        } else {
            view_metadata_for_session(&state.store, &context.session, None, verbose)?
        };
        let output_event_id = context.target_event.as_ref().map(|event| event.id.clone());
        let content = crate::transcript::render_history_context(&context, &metadata, color);
        ServerViewOutput {
            view: "show",
            session_id: context.session.id,
            event_id: output_event_id,
            full,
            content,
        }
    };
    view_response(rendered, params.format.as_deref())
}

async fn transcript_endpoint(
    State(state): State<AppState>,
    Query(params): Query<TranscriptParams>,
) -> Result<Response, ServerError> {
    let session_id = required_query(params.session, "session")?;
    let full = params.full.unwrap_or(false);
    let verbose = params.verbose.unwrap_or(false);
    let color = parse_color_param(params.color.as_deref())?;
    let session = state
        .store
        .session_by_id(&session_id)?
        .ok_or_else(|| ServerError::not_found(format!("session not found: {session_id}")))?;
    let target_event = params
        .at
        .as_deref()
        .map(|event_id| {
            state
                .store
                .event_by_id(event_id)?
                .ok_or_else(|| ServerError::not_found(format!("event not found: {event_id}")))
        })
        .transpose()?;
    if let Some(event) = &target_event {
        if event.session_id != session.id {
            return Err(ServerError::bad_request(format!(
                "event {} belongs to session {}, not {}",
                event.id, event.session_id, session.id
            )));
        }
    }

    let target_event_id = target_event.as_ref().map(|event| event.id.clone());
    let metadata =
        view_metadata_for_session(&state.store, &session, target_event.as_ref(), verbose)?;
    let content = if full {
        let events = state.store.events_for_session(&session.id)?;
        crate::transcript::render_session(
            &session,
            &events,
            target_event_id.as_deref(),
            &metadata,
            color,
        )
    } else if let Some(event_id) = target_event_id.as_deref() {
        let context = state
            .store
            .history_items_around_event(event_id, usize::MAX / 4, usize::MAX / 4)?
            .ok_or_else(|| ServerError::not_found(format!("event not found: {event_id}")))?;
        crate::transcript::render_history_session(&context, &metadata, color)
    } else {
        let context = state
            .store
            .history_items_for_transcript_session(&session.id)?
            .ok_or_else(|| ServerError::not_found(format!("session not found: {}", session.id)))?;
        crate::transcript::render_history_session(&context, &metadata, color)
    };
    view_response(
        ServerViewOutput {
            view: "transcript",
            session_id: session.id,
            event_id: target_event_id,
            full,
            content,
        },
        params.format.as_deref(),
    )
}

async fn import_jsonl(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ServerError> {
    let import_options = transport::ImportOptions {
        include_embeddings: state.embeddings_enabled,
        refresh_vector_projection: state.embeddings_enabled,
    };
    let stats = transport::import_jsonl_reader_with_options_and_import_progress(
        &state.store,
        Cursor::new(body),
        import_options,
        |_| {},
    )?;
    let projected = if state.embeddings_enabled {
        search::refresh(&state.store)?
    } else {
        search::refresh_text(&state.store)?
    };
    let (embedder, embedder_degraded_reason) = state
        .embedder
        .read()
        .expect("server embedder state lock poisoned")
        .snapshot();
    let embeddings = if state.embeddings_enabled {
        search::refresh_embeddings(
            &state.store,
            &state.machine_id,
            embedder.as_deref(),
            embedder_degraded_reason,
        )?
    } else {
        search::EmbeddingRefresh::disabled()
    };
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
            duplicate_group: result.duplicate_group,
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

fn required_query(value: Option<String>, name: &str) -> Result<String, ServerError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ServerError::bad_request(format!("{name} is required")))
}

fn parse_color_param(value: Option<&str>) -> Result<bool, ServerError> {
    match value.unwrap_or("never") {
        "always" | "true" | "1" => Ok(true),
        "never" | "false" | "0" | "" => Ok(false),
        value => Err(ServerError::bad_request(format!(
            "unsupported color value: {value}"
        ))),
    }
}

fn view_response(output: ServerViewOutput, format: Option<&str>) -> Result<Response, ServerError> {
    match format.unwrap_or("text") {
        "text" | "" => Ok((
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            output.content,
        )
            .into_response()),
        "json" => Ok(Json(output).into_response()),
        value => Err(ServerError::bad_request(format!(
            "unsupported view format: {value}"
        ))),
    }
}

fn view_metadata_for_event(
    store: &Store,
    event: &crate::archive::EventRecord,
    verbose: bool,
) -> Result<crate::transcript::ViewMetadata> {
    let source = if verbose {
        store.source_by_id(&event.source_id)?
    } else {
        None
    };
    let raw_artifact = if verbose {
        event
            .raw_artifact_hash
            .as_deref()
            .map(|hash| store.raw_artifact_summary_by_hash(hash))
            .transpose()?
            .flatten()
    } else {
        None
    };
    Ok(crate::transcript::ViewMetadata {
        ref_id: store.recent_ref_for_event_id(&event.id)?,
        source,
        raw_artifact,
        verbose,
    })
}

fn view_metadata_for_session(
    store: &Store,
    session: &crate::archive::SessionRecord,
    target_event: Option<&crate::archive::EventRecord>,
    verbose: bool,
) -> Result<crate::transcript::ViewMetadata> {
    if let Some(event) = target_event {
        return view_metadata_for_event(store, event, verbose);
    }
    let source = if verbose {
        store.source_by_id(&session.source_id)?
    } else {
        None
    };
    Ok(crate::transcript::ViewMetadata {
        ref_id: None,
        source,
        raw_artifact: None,
        verbose,
    })
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

#[derive(Debug)]
struct ServerError {
    status: StatusCode,
    error: anyhow::Error,
}

impl ServerError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: anyhow::anyhow!(message.into()),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error: anyhow::anyhow!(message.into()),
        }
    }
}

impl<E> From<E> for ServerError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: value.into(),
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        (self.status, format!("{}\n", self.error)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{stable_hash, ArchiveRecord, EventRecord, SessionRecord, SourceRecord};
    use axum::body::to_bytes;
    use axum::extract::{Query, State};
    use serde_json::json;

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
            duplicate_group: Vec::new(),
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

    #[tokio::test]
    async fn show_endpoint_renders_clean_remote_event_context() {
        let (_dir, state) = fixture_state();

        let response = show_endpoint(
            State(state),
            Query(ShowParams {
                event: Some("event_two".to_string()),
                before: Some(1),
                after: Some(1),
                full: Some(false),
                verbose: Some(false),
                color: Some("never".to_string()),
                format: None,
            }),
        )
        .await
        .expect("show response");
        let status = response.status();
        let body = response_text(response).await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("# Transcript"));
        assert!(body.contains("## Assistant"));
        assert!(body.contains("· selected"));
        assert!(body.contains("remote target text"));
        assert!(body.contains("remote before text"));
        assert!(body.contains("remote after text"));
    }

    #[tokio::test]
    async fn transcript_endpoint_renders_full_remote_session() {
        let (_dir, state) = fixture_state();

        let response = transcript_endpoint(
            State(state),
            Query(TranscriptParams {
                session: Some("session_remote".to_string()),
                at: Some("event_two".to_string()),
                full: Some(true),
                verbose: Some(false),
                color: Some("never".to_string()),
                format: None,
            }),
        )
        .await
        .expect("transcript response");
        let status = response.status();
        let body = response_text(response).await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("# Transcript"));
        assert!(body.contains("## #2 codex assistant"));
        assert!(body.contains("· selected"));
        assert!(body.contains("remote target text"));
        assert!(body.contains("## #1 codex user"));
        assert!(body.contains("## #3 codex assistant"));
    }

    #[tokio::test]
    async fn show_endpoint_reports_missing_event_as_non_success() {
        let (_dir, state) = fixture_state();

        let err = show_endpoint(
            State(state),
            Query(ShowParams {
                event: Some("missing_event".to_string()),
                before: None,
                after: None,
                full: None,
                verbose: None,
                color: None,
                format: None,
            }),
        )
        .await
        .expect_err("missing event should fail");
        let response = err.into_response();
        let status = response.status();
        let body = response_text(response).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("event not found: missing_event"));
    }

    async fn response_text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        String::from_utf8(bytes.to_vec()).expect("utf8 response")
    }

    fn fixture_state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = SourceRecord {
            id: "source_remote".to_string(),
            kind: "codex".to_string(),
            identity: "source_remote".to_string(),
            path: Some("/tmp/remote.jsonl".to_string()),
            first_seen_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            hash: stable_hash(&("source_remote", "source")).expect("source hash"),
        };
        let session = SessionRecord {
            id: "session_remote".to_string(),
            source_id: source.id.clone(),
            machine_id: "machine_remote".to_string(),
            source_kind: "codex".to_string(),
            external_id: "agent_session_remote".to_string(),
            title: Some("Remote fixture".to_string()),
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({}),
            hash: stable_hash(&("session_remote", "session")).expect("session hash"),
        };
        let event_one = fixture_event(
            "event_one",
            &session,
            &source,
            1,
            "user",
            "remote before text",
        );
        let event_two = fixture_event(
            "event_two",
            &session,
            &source,
            2,
            "assistant",
            "remote target text",
        );
        let event_three = fixture_event(
            "event_three",
            &session,
            &source,
            3,
            "assistant",
            "remote after text",
        );
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(event_one),
                ArchiveRecord::Event(event_two),
                ArchiveRecord::Event(event_three),
            ])
            .expect("import records");
        search::refresh(&store).expect("refresh search index");
        (
            dir,
            AppState {
                store,
                machine_id: "machine_remote".to_string(),
                default_search_mode: search::SearchMode::Lexical,
                embeddings_enabled: false,
                embedder: Arc::new(RwLock::new(ServerEmbedderState::disabled())),
            },
        )
    }

    fn fixture_event(
        id: &str,
        session: &SessionRecord,
        source: &SourceRecord,
        ordinal: i64,
        role: &str,
        content: &str,
    ) -> EventRecord {
        EventRecord {
            id: id.to_string(),
            session_id: session.id.clone(),
            source_id: source.id.clone(),
            machine_id: session.machine_id.clone(),
            source_kind: source.kind.clone(),
            ordinal,
            event_type: "message".to_string(),
            role: Some(role.to_string()),
            content: content.to_string(),
            raw_artifact_hash: None,
            occurred_at: None,
            metadata: json!({
                "search_indexable": true,
                "search_kind": role,
                "search_text": content
            }),
            hash: stable_hash(&(id, "event")).expect("event hash"),
        }
    }
}
