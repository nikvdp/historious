use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, SessionRecord,
};
use crate::config::SourceConfigs;
use crate::source::{
    AdapterConcurrency, PreparedImport, SearchSegment, SemanticPolicy, SourceAdapter,
    SourceAdapterRegistry, SourceCandidate, SourceCheckpointUpsert, SourceSyncContext,
    SourceUpsert,
};
use crate::storage::{ImportDelta, SourceCheckpointFingerprint, SourceFileStatus, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags, Transaction, TransactionBehavior};
use serde::Serialize;
use serde_json::Map;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::thread;
use walkdir::WalkDir;

#[derive(Debug, Clone, Default, Serialize)]
pub struct UpdateStats {
    pub files_seen: usize,
    pub skipped_unchanged: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub errors: usize,
    #[serde(skip)]
    pub delta: ImportDelta,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct OpencodeUsageBackfill {
    pub scanned: usize,
    pub updated: usize,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    pub max_files: Option<usize>,
    pub source_selection: SourceSelection,
    pub sources: SourceConfigs,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SourceSelection {
    sources: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionClass {
    Interactive,
    Subagent,
    Automation,
    Unknown,
}

impl SessionClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Subagent => "subagent",
            Self::Automation => "automation",
            Self::Unknown => "unknown",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionRelationshipKind {
    Subagent,
    Fork,
    None,
}

impl SessionRelationshipKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Subagent => "subagent",
            Self::Fork => "fork",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionRelationshipHint {
    pub parent_external_id: Option<String>,
    pub relationship: SessionRelationshipKind,
    pub rule: &'static str,
}

pub(crate) fn resolve_session_relationship(
    source_kind: &str,
    _session_external_id: &str,
    session_metadata: &Value,
    _event_contents: &[&str],
) -> SessionRelationshipHint {
    if source_kind == "claude_code" {
        if let Some(parent_external_id) = claude_subagent_parent(session_metadata) {
            return SessionRelationshipHint {
                parent_external_id: Some(parent_external_id),
                relationship: SessionRelationshipKind::Subagent,
                rule: "claude.subagent_path",
            };
        }
    }

    if source_kind == "opencode" {
        if let Some(parent_external_id) = session_metadata
            .get("opencode_parent_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|parent_id| !parent_id.is_empty())
        {
            return SessionRelationshipHint {
                parent_external_id: Some(parent_external_id.to_string()),
                relationship: SessionRelationshipKind::Subagent,
                rule: "opencode.parent_id",
            };
        }
    }

    let rule = match source_kind {
        "pi_agent" => "pi_agent.capture_gap",
        "hermes" => "hermes.capture_gap",
        _ => "default.none",
    };
    SessionRelationshipHint {
        parent_external_id: None,
        relationship: SessionRelationshipKind::None,
        rule,
    }
}

fn claude_subagent_parent(session_metadata: &Value) -> Option<String> {
    let path = Path::new(session_metadata.get("path")?.as_str()?);
    let file_name = path.file_name()?.to_str()?;
    if !file_name.starts_with("agent-") || path.parent()?.file_name()?.to_str()? != "subagents" {
        return None;
    }
    let parent_external_id = path.parent()?.parent()?.file_name()?.to_str()?;
    is_uuid_text(parent_external_id).then(|| parent_external_id.to_string())
}

fn is_uuid_text(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

pub(crate) fn codex_subagent_paths(event_content: &str) -> Vec<String> {
    if !event_content.contains("subagent_notification") {
        return Vec::new();
    }

    let mut paths = Vec::new();
    if let Ok(value) = serde_json::from_str::<Value>(event_content) {
        collect_codex_agent_paths(&value, &mut paths);
    }
    collect_embedded_agent_paths(event_content, &mut paths);
    paths.sort();
    paths.dedup();
    paths
}

fn collect_codex_agent_paths(value: &Value, paths: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(path) = map.get("agent_path").and_then(Value::as_str) {
                paths.push(path.to_string());
            }
            for child in map.values() {
                collect_codex_agent_paths(child, paths);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_codex_agent_paths(item, paths);
            }
        }
        Value::String(text) => collect_embedded_agent_paths(text, paths),
        _ => {}
    }
}

fn collect_embedded_agent_paths(text: &str, paths: &mut Vec<String>) {
    let key = "\"agent_path\"";
    let mut offset = 0;
    while let Some(index) = text[offset..].find(key) {
        let key_end = offset + index + key.len();
        let Some(colon) = text[key_end..].find(':') else {
            break;
        };
        let value = text[key_end + colon + 1..].trim_start();
        if let Some(path) = json_string_prefix(value) {
            paths.push(path);
        }
        offset = key_end;
    }
}

fn json_string_prefix(value: &str) -> Option<String> {
    if !value.starts_with('"') {
        return None;
    }
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate().skip(1) {
        match (byte, escaped) {
            (b'"', false) => return serde_json::from_str(&value[..=index]).ok(),
            (b'\\', false) => escaped = true,
            _ => escaped = false,
        }
    }
    None
}

#[derive(Debug, Clone)]
pub(crate) struct UsageEvent {
    pub content: String,
    pub metadata: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SessionUsage {
    pub models: Vec<String>,
    pub primary_model: Option<String>,
    pub input_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
}

pub(crate) fn extract_session_usage(source_kind: &str, events: &[UsageEvent]) -> SessionUsage {
    let mut models = HashMap::<String, usize>::new();
    let mut input = 0i64;
    let mut cached = 0i64;
    let mut output = 0i64;
    let mut usage_seen = false;
    let mut opencode_messages = BTreeSet::new();

    for event in events {
        let content = serde_json::from_str::<Value>(&event.content).ok();
        match source_kind {
            "codex" => {
                let Some(value) = content.as_ref() else {
                    continue;
                };
                if let Some(model) = string_at(value, &["payload", "model"]) {
                    *models.entry(model).or_default() += 1;
                }
                if let Some(total) = value
                    .pointer("/payload/info/total_token_usage")
                    .and_then(Value::as_object)
                {
                    usage_seen = true;
                    input = input.max(json_i64(total.get("input_tokens")));
                    cached = cached.max(json_i64(total.get("cached_input_tokens")));
                    output = output.max(json_i64(total.get("output_tokens")));
                }
            }
            "claude_code" => {
                let Some(value) = content.as_ref() else {
                    continue;
                };
                if let Some(model) = string_at(value, &["message", "model"]) {
                    *models.entry(model).or_default() += 1;
                }
                if let Some(usage) = value.pointer("/message/usage").and_then(Value::as_object) {
                    usage_seen = true;
                    input = input.saturating_add(json_i64(usage.get("input_tokens")));
                    cached = cached.saturating_add(json_i64(usage.get("cache_read_input_tokens")));
                    output = output.saturating_add(json_i64(usage.get("output_tokens")));
                }
            }
            "pi_agent" => {
                let Some(value) = content.as_ref() else {
                    continue;
                };
                if let Some(model) = string_at(value, &["message", "model"]) {
                    *models.entry(model).or_default() += 1;
                }
                if let Some(usage) = value.pointer("/message/usage").and_then(Value::as_object) {
                    usage_seen = true;
                    input = input.saturating_add(json_i64(usage.get("input")));
                    cached = cached.saturating_add(json_i64(usage.get("cacheRead")));
                    output = output.saturating_add(json_i64(usage.get("output")));
                }
            }
            "opencode" => {
                let Some(message_id) = event
                    .metadata
                    .get("opencode_message_id")
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if !opencode_messages.insert(message_id.to_string()) {
                    continue;
                }
                if let Some(model) = event
                    .metadata
                    .get("opencode_model_id")
                    .and_then(Value::as_str)
                {
                    *models.entry(model.to_string()).or_default() += 1;
                }
                if let Some(usage) = event
                    .metadata
                    .get("opencode_tokens")
                    .and_then(Value::as_object)
                {
                    usage_seen = true;
                    input = input.saturating_add(json_i64(usage.get("input")));
                    cached = cached.saturating_add(
                        usage
                            .get("cache")
                            .and_then(Value::as_object)
                            .map(|cache| json_i64(cache.get("read")))
                            .unwrap_or(0),
                    );
                    output = output.saturating_add(json_i64(usage.get("output")));
                }
            }
            _ => {}
        }
    }

    let primary_model = models
        .iter()
        .max_by(|(left_model, left_count), (right_model, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_model.cmp(left_model))
        })
        .map(|(model, _)| model.clone());
    let mut models = models.into_keys().collect::<Vec<_>>();
    models.sort();
    SessionUsage {
        models,
        primary_model,
        input_tokens: usage_seen.then_some(input),
        cached_input_tokens: usage_seen.then_some(cached),
        output_tokens: usage_seen.then_some(output),
    }
}

fn json_i64(value: Option<&Value>) -> i64 {
    value.and_then(Value::as_i64).unwrap_or(0).max(0)
}

pub(crate) fn classify_session(
    source_kind: &str,
    session_metadata: &Value,
    event_contents: &[&str],
) -> SessionClass {
    match source_kind {
        "codex" => classify_codex_session(event_contents),
        "opencode" => classify_opencode_session(session_metadata),
        "claude_code" => classify_claude_session(event_contents),
        _ => SessionClass::Unknown,
    }
}

pub(crate) fn classify_event(source_kind: &str, event_content: &str) -> SessionClass {
    match source_kind {
        "claude_code" => classify_claude_event(event_content),
        _ => SessionClass::Unknown,
    }
}

impl SourceSelection {
    pub fn parse(values: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut sources = BTreeSet::new();
        for value in values {
            for part in value.split(',') {
                let source = part.trim();
                if source.is_empty() {
                    continue;
                }
                sources.insert(source.to_ascii_lowercase());
            }
        }
        Ok(Self { sources })
    }

    pub fn single(source: impl Into<String>) -> Result<Self> {
        Self::parse([source.into()])
    }

    fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    fn includes_adapter(&self, adapter_kind: &str) -> bool {
        self.is_empty()
            || self.sources.contains(adapter_kind)
            || (self.sources.contains("agent_logs") && is_local_transcript_kind(adapter_kind))
    }

    fn matches_candidate(&self, adapter_kind: &str, candidate_kind: &str) -> bool {
        self.is_empty()
            || self.sources.contains(adapter_kind)
            || self.sources.contains(candidate_kind)
            || (self.sources.contains("agent_logs") && is_local_transcript_kind(candidate_kind))
    }
}

#[derive(Debug, Clone)]
pub struct UpdateSourceSummary {
    pub kind: String,
    pub found_files: usize,
    pub selected_files: usize,
}

#[derive(Debug, Clone)]
pub struct UpdateChangedSourceSummary {
    pub kind: String,
    pub changed_files: usize,
}

#[derive(Debug, Clone)]
pub enum UpdateProgress {
    Discovering {
        sources: Vec<String>,
    },
    Discovered {
        sources: Vec<UpdateSourceSummary>,
        selected_files: usize,
    },
    Processing {
        adapter_kind: String,
        kind: String,
        path: PathBuf,
        file_index: usize,
        total_files: usize,
        source_file_index: usize,
        source_file_count: usize,
        stats: UpdateStats,
    },
    PreparingImports {
        changed_files: usize,
        sources: Vec<UpdateChangedSourceSummary>,
        stats: UpdateStats,
    },
    ImportingFile {
        adapter_kind: String,
        kind: String,
        path: PathBuf,
        changed_file_index: usize,
        changed_file_count: usize,
        stats: UpdateStats,
    },
    ImportedFile {
        adapter_kind: String,
        kind: String,
        path: PathBuf,
        changed_file_index: usize,
        changed_file_count: usize,
        stats: UpdateStats,
    },
    CompletedFile {
        adapter_kind: String,
        kind: String,
        path: PathBuf,
        file_index: usize,
        total_files: usize,
        source_file_index: usize,
        source_file_count: usize,
        stats: UpdateStats,
    },
}

#[derive(Debug, Clone)]
struct SourceRoot {
    path: PathBuf,
    extensions: &'static [&'static str],
}

#[derive(Debug, Clone)]
struct LocalTranscriptSource {
    kind: &'static str,
    roots: Vec<SourceRoot>,
}

#[derive(Debug, Clone)]
struct ParsedLine {
    ordinal: i64,
    value: Value,
    byte_offset: usize,
    byte_len: usize,
    content: String,
    content_compacted: bool,
    search: SearchSegment,
    role: Option<String>,
    event_type: String,
    occurred_at: Option<DateTime<Utc>>,
    external_session_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct NativeTitleIndex {
    titles: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceIdentity {
    workspace_path: String,
    workspace_root: String,
    cwd: Option<String>,
    git_repo: Option<String>,
    git_branch: Option<String>,
    source: String,
    confidence: String,
}

pub fn update_local_with_progress(
    store: &Store,
    machine_id: &str,
    options: UpdateOptions,
    mut progress: impl FnMut(&UpdateProgress),
) -> Result<UpdateStats> {
    update_local_with_progress_and_cancel(store, machine_id, options, &mut progress, || false)
}

pub fn update_local_with_progress_and_cancel(
    store: &Store,
    machine_id: &str,
    options: UpdateOptions,
    mut progress: impl FnMut(&UpdateProgress),
    should_cancel: impl Fn() -> bool,
) -> Result<UpdateStats> {
    let mut stats = UpdateStats::default();
    let mut candidates = Vec::new();
    let mut source_summaries = Vec::new();
    let registry = built_in_source_adapters(&options)?;
    progress(&UpdateProgress::Discovering {
        sources: registry
            .iter()
            .map(|adapter| adapter.kind().to_string())
            .collect(),
    });
    let discoveries = discover_selected_sources(&registry, &options, &should_cancel)?;
    for discovery in discoveries {
        match discovery.result {
            Ok(discovered) => {
                for candidate in discovered {
                    if should_cancel() {
                        return Ok(stats);
                    }
                    if !options
                        .source_selection
                        .matches_candidate(discovery.adapter_kind, &candidate.kind)
                    {
                        continue;
                    }
                    push_found_source_file(&mut source_summaries, &candidate.kind);
                    candidates.push(candidate);
                }
            }
            Err(err) => {
                tracing::debug!(
                    "failed to discover {} sources: {err:#}",
                    discovery.adapter_kind
                );
                stats.errors += 1;
            }
        }
    }
    candidates.sort_by(|left, right| right.modified.cmp(&left.modified));
    if let Some(max_files) = options.max_files {
        candidates.truncate(max_files);
    }
    mark_selected_source_files(&mut source_summaries, &candidates);
    progress(&UpdateProgress::Discovered {
        sources: source_summaries
            .iter()
            .map(|source| UpdateSourceSummary {
                kind: source.kind.to_string(),
                found_files: source.found_files,
                selected_files: source.selected_files,
            })
            .collect(),
        selected_files: candidates.len(),
    });

    let native_titles = NativeTitleIndex::load();
    refresh_existing_native_titles(store, &native_titles)?;
    let total_files = candidates.len();
    let mut source_seen = Vec::new();
    let mut pending_imports = Vec::new();
    let source_file_statuses = precompute_local_source_file_statuses(store, &candidates)?;
    let context = SourceSyncContext::new(store).with_source_file_statuses(&source_file_statuses);
    for (idx, candidate) in candidates.into_iter().enumerate() {
        if should_cancel() {
            return Ok(stats);
        }
        let kind = candidate.kind.clone();
        let path = candidate.progress_path();
        stats.files_seen += 1;
        let source_file_index = increment_source_seen(&mut source_seen, &kind);
        let source_file_count = source_summaries
            .iter()
            .find(|source| source.kind == kind)
            .map(|source| source.selected_files)
            .unwrap_or(0);
        progress(&UpdateProgress::Processing {
            adapter_kind: candidate.adapter_kind.to_string(),
            kind: kind.to_string(),
            path: path.clone(),
            file_index: idx + 1,
            total_files,
            source_file_index,
            source_file_count,
            stats: stats.clone(),
        });
        if should_cancel() {
            return Ok(stats);
        }
        let Some(adapter) = registry
            .iter()
            .find(|adapter| adapter.kind() == candidate.adapter_kind)
        else {
            tracing::debug!(
                "source adapter {} disappeared during update",
                candidate.adapter_kind
            );
            stats.errors += 1;
            progress(&UpdateProgress::CompletedFile {
                adapter_kind: candidate.adapter_kind.to_string(),
                kind: kind.to_string(),
                path,
                file_index: idx + 1,
                total_files,
                source_file_index,
                source_file_count,
                stats: stats.clone(),
            });
            continue;
        };
        if adapter.is_current(&context, &candidate)? {
            stats.skipped_unchanged += 1;
            progress(&UpdateProgress::CompletedFile {
                adapter_kind: candidate.adapter_kind.to_string(),
                kind: kind.to_string(),
                path,
                file_index: idx + 1,
                total_files,
                source_file_index,
                source_file_count,
                stats: stats.clone(),
            });
            continue;
        }
        pending_imports.push(PendingImport {
            order: idx,
            adapter_kind: candidate.adapter_kind,
            candidate,
            kind,
            path,
        });
    }

    if !pending_imports.is_empty() {
        let changed_sources = changed_source_summaries(&pending_imports);
        progress(&UpdateProgress::PreparingImports {
            changed_files: pending_imports.len(),
            sources: changed_sources,
            stats: stats.clone(),
        });
    }
    let prepared_imports = prepare_pending_imports(
        &registry,
        &context,
        machine_id,
        pending_imports,
        &should_cancel,
    )?;
    let changed_file_count = prepared_imports.len();
    for (changed_idx, prepared) in prepared_imports.into_iter().enumerate() {
        if should_cancel() {
            return Ok(stats);
        }
        progress(&UpdateProgress::ImportingFile {
            adapter_kind: prepared.adapter_kind.to_string(),
            kind: prepared.kind.clone(),
            path: prepared.path.clone(),
            changed_file_index: changed_idx + 1,
            changed_file_count,
            stats: stats.clone(),
        });
        match prepared.result {
            Ok(import) => match import.commit(store) {
                Ok(delta) => {
                    stats.inserted += delta.inserted;
                    stats.duplicates += delta.duplicates;
                    stats.delta.merge(delta.delta);
                }
                Err(err) => {
                    tracing::debug!("failed to ingest {}: {err:#}", prepared.path.display());
                    stats.errors += 1;
                }
            },
            Err(err) => {
                tracing::debug!("failed to ingest {}: {err:#}", prepared.path.display());
                stats.errors += 1;
            }
        }
        progress(&UpdateProgress::ImportedFile {
            adapter_kind: prepared.adapter_kind.to_string(),
            kind: prepared.kind,
            path: prepared.path,
            changed_file_index: changed_idx + 1,
            changed_file_count,
            stats: stats.clone(),
        });
    }
    Ok(stats)
}

fn precompute_local_source_file_statuses(
    store: &Store,
    candidates: &[SourceCandidate],
) -> Result<HashMap<String, SourceFileStatus>> {
    let fingerprints = candidates
        .iter()
        .filter(|candidate| is_local_transcript_kind(&candidate.kind))
        .filter_map(|candidate| {
            let cursor = local_transcript_checkpoint_cursor_for_candidate(candidate).ok()?;
            Some(SourceCheckpointFingerprint {
                source_kind: candidate.kind.clone(),
                source_identity: candidate.identity.clone(),
                cursor,
            })
        })
        .collect::<Vec<_>>();
    store.source_checkpoint_statuses(&fingerprints)
}

struct AdapterDiscovery {
    adapter_kind: &'static str,
    result: Result<Vec<SourceCandidate>>,
}

fn discover_selected_sources(
    registry: &SourceAdapterRegistry,
    options: &UpdateOptions,
    should_cancel: &impl Fn() -> bool,
) -> Result<Vec<AdapterDiscovery>> {
    let adapters = registry
        .iter()
        .filter(|adapter| options.source_selection.includes_adapter(adapter.kind()))
        .collect::<Vec<_>>();
    if adapters.is_empty() || should_cancel() {
        return Ok(Vec::new());
    }

    thread::scope(|scope| {
        let handles = adapters
            .into_iter()
            .map(|adapter| {
                let adapter_kind = adapter.kind();
                (
                    adapter_kind,
                    scope.spawn(move || AdapterDiscovery {
                        adapter_kind,
                        result: adapter.discover(),
                    }),
                )
            })
            .collect::<Vec<_>>();

        let mut discoveries = Vec::with_capacity(handles.len());
        for (adapter_kind, handle) in handles {
            if should_cancel() {
                return Ok(discoveries);
            }
            match handle.join() {
                Ok(discovery) => discoveries.push(discovery),
                Err(_) => discoveries.push(AdapterDiscovery {
                    adapter_kind,
                    result: Err(anyhow::anyhow!("source discovery worker panicked")),
                }),
            }
        }
        Ok(discoveries)
    })
}

#[derive(Clone)]
struct PendingImport {
    order: usize,
    adapter_kind: &'static str,
    candidate: SourceCandidate,
    kind: String,
    path: PathBuf,
}

struct PreparedPendingImport {
    order: usize,
    adapter_kind: &'static str,
    kind: String,
    path: PathBuf,
    result: Result<PreparedImport>,
}

fn prepare_pending_imports(
    registry: &SourceAdapterRegistry,
    context: &SourceSyncContext<'_>,
    machine_id: &str,
    pending_imports: Vec<PendingImport>,
    should_cancel: &impl Fn() -> bool,
) -> Result<Vec<PreparedPendingImport>> {
    if pending_imports.is_empty() || should_cancel() {
        return Ok(Vec::new());
    }

    let mut groups: Vec<(&'static str, Vec<PendingImport>)> = Vec::new();
    for pending in pending_imports {
        if let Some((_, items)) = groups
            .iter_mut()
            .find(|(adapter_kind, _)| *adapter_kind == pending.adapter_kind)
        {
            items.push(pending);
        } else {
            groups.push((pending.adapter_kind, vec![pending]));
        }
    }

    let mut prepared = Vec::new();
    for (adapter_kind, imports) in groups {
        if should_cancel() {
            break;
        }
        let Some(adapter) = registry
            .iter()
            .find(|adapter| adapter.kind() == adapter_kind)
        else {
            prepared.extend(imports.into_iter().map(|pending| PreparedPendingImport {
                order: pending.order,
                adapter_kind: pending.adapter_kind,
                kind: pending.kind,
                path: pending.path,
                result: Err(anyhow::anyhow!(
                    "source adapter {} disappeared during import preparation",
                    pending.adapter_kind
                )),
            }));
            continue;
        };
        let concurrency = adapter.concurrency().normalized().prepare;
        for batch in imports.chunks(concurrency) {
            if should_cancel() {
                break;
            }
            prepared.extend(prepare_pending_import_batch(
                adapter,
                context,
                machine_id,
                batch.to_vec(),
                should_cancel,
            )?);
        }
    }
    prepared.sort_by_key(|prepared| prepared.order);
    Ok(prepared)
}

fn prepare_pending_import_batch(
    adapter: &dyn SourceAdapter,
    context: &SourceSyncContext<'_>,
    machine_id: &str,
    pending_imports: Vec<PendingImport>,
    should_cancel: &impl Fn() -> bool,
) -> Result<Vec<PreparedPendingImport>> {
    thread::scope(|scope| {
        let handles = pending_imports
            .into_iter()
            .map(|pending| {
                let fallback = pending.clone();
                (
                    fallback,
                    scope.spawn(move || {
                        let result =
                            adapter.prepare_import(&context, machine_id, &pending.candidate);
                        PreparedPendingImport {
                            order: pending.order,
                            adapter_kind: pending.adapter_kind,
                            kind: pending.kind,
                            path: pending.path,
                            result,
                        }
                    }),
                )
            })
            .collect::<Vec<_>>();

        let mut prepared = Vec::with_capacity(handles.len());
        for (fallback, handle) in handles {
            if should_cancel() {
                return Ok(prepared);
            }
            match handle.join() {
                Ok(import) => prepared.push(import),
                Err(_) => prepared.push(PreparedPendingImport {
                    order: fallback.order,
                    adapter_kind: fallback.adapter_kind,
                    kind: fallback.kind,
                    path: fallback.path,
                    result: Err(anyhow::anyhow!("source import preparation worker panicked")),
                }),
            }
        }
        Ok(prepared)
    })
}

pub fn update_source_path_with_progress_and_cancel(
    store: &Store,
    machine_id: &str,
    kind: &str,
    path: &Path,
    mut progress: impl FnMut(&UpdateProgress),
    should_cancel: impl Fn() -> bool,
) -> Result<UpdateStats> {
    let mut stats = UpdateStats::default();
    if should_cancel() {
        return Ok(stats);
    }
    let native_titles = NativeTitleIndex::load();
    refresh_existing_native_titles(store, &native_titles)?;
    if should_cancel() {
        return Ok(stats);
    }

    let path = path.to_path_buf();
    stats.files_seen += 1;
    progress(&UpdateProgress::Processing {
        adapter_kind: kind.to_string(),
        kind: kind.to_string(),
        path: path.clone(),
        file_index: 1,
        total_files: 1,
        source_file_index: 1,
        source_file_count: 1,
        stats: stats.clone(),
    });
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) => {
            tracing::debug!("failed to read metadata for {}: {err}", path.display());
            stats.errors += 1;
            progress(&UpdateProgress::CompletedFile {
                adapter_kind: kind.to_string(),
                kind: kind.to_string(),
                path,
                file_index: 1,
                total_files: 1,
                source_file_index: 1,
                source_file_count: 1,
                stats: stats.clone(),
            });
            return Ok(stats);
        }
    };
    if should_cancel() {
        return Ok(stats);
    }

    let size = metadata.len();
    let mtime_ms = file_mtime_ms(&metadata);
    let path_text = path.to_string_lossy().to_string();
    let file_status = store.source_file_status(&path_text, size, mtime_ms)?;
    if kind != "opencode" && file_status.raw_current && !file_status.needs_workspace_refresh {
        stats.skipped_unchanged += 1;
    } else {
        let context = SourceSyncContext::new(store);
        match prepare_file_import(&context, machine_id, kind, &path, &native_titles)
            .and_then(|prepared| prepared.commit(store))
        {
            Ok(delta) => {
                stats.inserted += delta.inserted;
                stats.duplicates += delta.duplicates;
                stats.delta.merge(delta.delta);
            }
            Err(err) => {
                tracing::debug!("failed to ingest {}: {err:#}", path.display());
                stats.errors += 1;
            }
        }
    }
    progress(&UpdateProgress::CompletedFile {
        adapter_kind: kind.to_string(),
        kind: kind.to_string(),
        path,
        file_index: 1,
        total_files: 1,
        source_file_index: 1,
        source_file_count: 1,
        stats: stats.clone(),
    });
    Ok(stats)
}

#[derive(Debug, Clone)]
struct MutableSourceSummary {
    kind: String,
    found_files: usize,
    selected_files: usize,
}

fn push_found_source_file(summaries: &mut Vec<MutableSourceSummary>, kind: &str) {
    if let Some(summary) = summaries.iter_mut().find(|summary| summary.kind == kind) {
        summary.found_files += 1;
    } else {
        summaries.push(MutableSourceSummary {
            kind: kind.to_string(),
            found_files: 1,
            selected_files: 0,
        });
    }
}

fn mark_selected_source_files(
    summaries: &mut [MutableSourceSummary],
    candidates: &[SourceCandidate],
) {
    for summary in summaries.iter_mut() {
        summary.selected_files = 0;
    }
    for candidate in candidates {
        if let Some(summary) = summaries
            .iter_mut()
            .find(|summary| summary.kind == candidate.kind)
        {
            summary.selected_files += 1;
        }
    }
}

fn increment_source_seen(seen: &mut Vec<MutableSourceSummary>, kind: &str) -> usize {
    if let Some(summary) = seen.iter_mut().find(|summary| summary.kind == kind) {
        summary.found_files += 1;
        summary.found_files
    } else {
        seen.push(MutableSourceSummary {
            kind: kind.to_string(),
            found_files: 1,
            selected_files: 0,
        });
        1
    }
}

fn changed_source_summaries(pending_imports: &[PendingImport]) -> Vec<UpdateChangedSourceSummary> {
    let mut summaries: Vec<UpdateChangedSourceSummary> = Vec::new();
    for pending in pending_imports {
        if let Some(summary) = summaries
            .iter_mut()
            .find(|summary| summary.kind == pending.kind)
        {
            summary.changed_files += 1;
        } else {
            summaries.push(UpdateChangedSourceSummary {
                kind: pending.kind.clone(),
                changed_files: 1,
            });
        }
    }
    summaries
}

fn built_in_source_adapters(options: &UpdateOptions) -> Result<SourceAdapterRegistry> {
    let native_titles = NativeTitleIndex::load();
    let mut registry = SourceAdapterRegistry::new();
    for source in local_transcript_sources() {
        if !options.source_selection.includes_adapter(source.kind) {
            continue;
        }
        registry = registry.register(LocalTranscriptAdapter {
            kind: source.kind,
            roots: source.roots,
            native_titles: native_titles.clone(),
        })?;
    }
    if options.source_selection.includes_adapter("treechat") {
        if let Some(adapter) = crate::treechat::TreechatAdapter::from_config(
            &options.sources.treechat,
            selected_treechat_source(&options.source_selection),
        )? {
            return registry.register(adapter);
        }
    }
    Ok(registry)
}

fn selected_treechat_source(selection: &SourceSelection) -> Option<&'static str> {
    selection.sources.contains("treechat").then_some("treechat")
}

fn is_local_transcript_kind(kind: &str) -> bool {
    matches!(
        kind,
        "codex" | "claude_code" | "pi_agent" | "openclaw" | "hermes" | "opencode"
    )
}

struct LocalTranscriptAdapter {
    kind: &'static str,
    roots: Vec<SourceRoot>,
    native_titles: NativeTitleIndex,
}

impl SourceAdapter for LocalTranscriptAdapter {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn concurrency(&self) -> AdapterConcurrency {
        if self.kind() == "opencode" {
            AdapterConcurrency::new(1, 1)
        } else {
            AdapterConcurrency::new(1, 4)
        }
    }

    fn discover(&self) -> Result<Vec<SourceCandidate>> {
        let mut candidates = Vec::new();
        for root in &self.roots {
            if !root.path.exists() {
                continue;
            }
            for entry in WalkDir::new(&root.path)
                .follow_links(false)
                .into_iter()
                .filter_entry(|entry| !is_hidden_noise(entry.path()))
            {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => {
                        tracing::debug!(
                            "skipping unreadable entry under {}: {err}",
                            root.path.display()
                        );
                        continue;
                    }
                };
                if !entry.file_type().is_file() || !has_extension(entry.path(), root.extensions) {
                    continue;
                }
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        tracing::debug!(
                            "failed to read metadata for {}: {err}",
                            entry.path().display()
                        );
                        continue;
                    }
                };
                let modified = metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|duration| duration.as_millis() as i128)
                    .unwrap_or(0);
                let path = entry.path().to_path_buf();
                candidates.push(SourceCandidate {
                    adapter_kind: self.kind(),
                    kind: self.kind().to_string(),
                    identity: path.to_string_lossy().to_string(),
                    path: Some(path),
                    modified,
                    size: Some(metadata.len()),
                    mtime_ms: file_mtime_ms(&metadata),
                });
            }
        }
        Ok(candidates)
    }

    fn is_current(
        &self,
        context: &SourceSyncContext<'_>,
        candidate: &SourceCandidate,
    ) -> Result<bool> {
        let cursor = local_transcript_checkpoint_cursor_for_candidate(candidate)?;
        let status = context.source_checkpoint_status(self.kind(), &candidate.identity, &cursor)?;
        Ok(status.raw_current && !status.needs_workspace_refresh)
    }

    fn prepare_import(
        &self,
        context: &SourceSyncContext<'_>,
        machine_id: &str,
        candidate: &SourceCandidate,
    ) -> Result<PreparedImport> {
        let path = candidate
            .path
            .as_deref()
            .with_context(|| format!("source candidate {} has no path", candidate.identity))?;
        let prepared =
            prepare_file_import(context, machine_id, self.kind(), path, &self.native_titles)?;
        Ok(prepared.with_checkpoint(local_transcript_checkpoint_upsert(self.kind(), candidate)?))
    }
}

fn local_transcript_sources() -> Vec<LocalTranscriptSource> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    vec![
        LocalTranscriptSource {
            kind: "codex",
            roots: vec![
                SourceRoot {
                    path: home.join(".codex/sessions"),
                    extensions: &["jsonl"],
                },
                SourceRoot {
                    path: home.join(".codex/archived_sessions"),
                    extensions: &["jsonl"],
                },
            ],
        },
        LocalTranscriptSource {
            kind: "claude_code",
            roots: vec![SourceRoot {
                path: home.join(".claude/projects"),
                extensions: &["jsonl"],
            }],
        },
        LocalTranscriptSource {
            kind: "pi_agent",
            roots: vec![SourceRoot {
                path: home.join(".pi/agent/sessions"),
                extensions: &["jsonl"],
            }],
        },
        LocalTranscriptSource {
            kind: "openclaw",
            roots: vec![SourceRoot {
                path: home.join(".openclaw"),
                extensions: &["jsonl"],
            }],
        },
        LocalTranscriptSource {
            kind: "hermes",
            roots: vec![SourceRoot {
                path: home.join(".hermes/sessions"),
                extensions: &["json", "jsonl"],
            }],
        },
        LocalTranscriptSource {
            kind: "opencode",
            roots: vec![SourceRoot {
                path: home.join(".local/share/opencode/opencode.db"),
                extensions: &["db"],
            }],
        },
    ]
}

fn opencode_checkpoint_cursor(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for path in opencode_sqlite_fingerprint_paths(path) {
        let label = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        match fs::metadata(&path) {
            Ok(metadata) => parts.push(format!(
                "{}:{}:{}",
                label,
                metadata.len(),
                file_mtime_ms(&metadata)
                    .map(|mtime| mtime.to_string())
                    .unwrap_or_else(|| "none".to_string())
            )),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                parts.push(format!("{label}:missing"));
            }
            Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
        }
    }
    Ok(parts.join("|"))
}

fn opencode_sqlite_fingerprint_paths(path: &Path) -> Vec<PathBuf> {
    let path_text = path.to_string_lossy();
    vec![
        path.to_path_buf(),
        PathBuf::from(format!("{path_text}-wal")),
        PathBuf::from(format!("{path_text}-shm")),
    ]
}

fn local_transcript_checkpoint_cursor_for_candidate(candidate: &SourceCandidate) -> Result<String> {
    let path = candidate
        .path
        .as_deref()
        .with_context(|| format!("source candidate {} has no path", candidate.identity))?;
    local_transcript_checkpoint_cursor(&candidate.kind, path, candidate.size, candidate.mtime_ms)
}

fn local_transcript_checkpoint_cursor(
    kind: &str,
    path: &Path,
    size: Option<u64>,
    mtime_ms: Option<i64>,
) -> Result<String> {
    if kind == "opencode" {
        return opencode_checkpoint_cursor(path);
    }
    Ok(format!(
        "file_metadata_v1:{}:{}",
        size.map(|size| size.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        mtime_ms
            .map(|mtime| mtime.to_string())
            .unwrap_or_else(|| "none".to_string())
    ))
}

fn local_transcript_checkpoint_upsert(
    kind: &str,
    candidate: &SourceCandidate,
) -> Result<SourceCheckpointUpsert> {
    let path = candidate
        .path
        .as_deref()
        .with_context(|| format!("source candidate {} has no path", candidate.identity))?;
    let cursor =
        local_transcript_checkpoint_cursor(kind, path, candidate.size, candidate.mtime_ms)?;
    let strategy = if kind == "opencode" {
        "sqlite_file_trio_metadata_v1"
    } else {
        "file_metadata_v1"
    };
    Ok(SourceCheckpointUpsert {
        source_kind: kind.to_string(),
        source_identity: candidate.identity.clone(),
        cursor: Some(cursor),
        metadata: json!({
            "strategy": strategy,
            "path": candidate.identity,
            "size": candidate.size,
            "mtime_ms": candidate.mtime_ms,
        }),
    })
}

fn prepare_file_import(
    _context: &SourceSyncContext<'_>,
    machine_id: &str,
    kind: &str,
    path: &Path,
    native_titles: &NativeTitleIndex,
) -> Result<PreparedImport> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let raw_hash = blake3_hex(&bytes);
    let path_text = path.to_string_lossy().to_string();
    let source_id = stable_id(&["source", kind, &path_text]);
    let source_upsert = SourceUpsert {
        id: source_id.clone(),
        kind: kind.to_string(),
        identity: path_text.clone(),
        path: Some(path_text.clone()),
    };

    let mut records = Vec::new();

    if kind == "opencode" {
        return prepare_opencode_db_import(machine_id, path, &path_text, &source_id, source_upsert);
    }

    let lines = if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
        let text = String::from_utf8_lossy(&bytes);
        parse_jsonl(&text)
    } else {
        let text = String::from_utf8_lossy(&bytes);
        parse_json_file(&text)
    };
    let lines = lines.with_context(|| format!("parsing {}", path.display()))?;
    let external_session_id = lines
        .iter()
        .find_map(|line| line.external_session_id.clone())
        .unwrap_or_else(|| file_stem(path));
    let session_id = stable_id(&["session", kind, &path_text, &external_session_id]);
    let title = session_title(kind, &external_session_id, &lines, native_titles);
    let workspace = session_workspace(kind, path, &lines);
    let session_hash = stable_hash(&(kind, &source_id, &external_session_id, &path_text))?;
    records.push(ArchiveRecord::Session(SessionRecord {
        id: session_id.clone(),
        source_id: source_id.clone(),
        machine_id: machine_id.to_string(),
        source_kind: kind.to_string(),
        external_id: external_session_id.clone(),
        title,
        status: "open".to_string(),
        started_at: lines.iter().find_map(|line| line.occurred_at),
        updated_at: lines.iter().rev().find_map(|line| line.occurred_at),
        metadata: session_metadata(&path_text, workspace.as_ref()),
        hash: session_hash,
    }));

    for line in lines {
        if line.content.trim().is_empty() {
            continue;
        }
        let line_hash = stable_hash(&line.value)?;
        let event_id = stable_id(&[
            "event",
            kind,
            &session_id,
            &line.ordinal.to_string(),
            &line_hash,
        ]);
        let byte_offset = line.byte_offset;
        let byte_len = line.byte_len;
        let mut metadata = Map::new();
        metadata.insert("byte_offset".to_string(), json!(byte_offset));
        metadata.insert("byte_len".to_string(), json!(byte_len));
        metadata.insert("source_file_hash".to_string(), json!(raw_hash.clone()));
        metadata.insert(
            "capture_fidelity".to_string(),
            Value::String("normalized_local_log".to_string()),
        );
        if kind == "claude_code" {
            if let Some(relationship) = claude_event_relationship_metadata(&line.value) {
                metadata.insert("claude_relationship".to_string(), relationship);
            }
        }
        if line.content_compacted {
            metadata.insert("content_compacted".to_string(), Value::Bool(true));
            metadata.insert(
                "content_compaction".to_string(),
                json!({
                    "strategy": "skipped_event_summary_v1",
                    "raw_payload_preserved": false
                }),
            );
        }
        line.search.apply_compat_metadata(&mut metadata);
        records.push(ArchiveRecord::Event(EventRecord {
            id: event_id,
            session_id: session_id.clone(),
            source_id: source_id.clone(),
            machine_id: machine_id.to_string(),
            source_kind: kind.to_string(),
            ordinal: line.ordinal,
            event_type: line.event_type,
            role: line.role,
            content: line.content,
            raw_artifact_hash: None,
            occurred_at: line.occurred_at,
            metadata: Value::Object(metadata),
            hash: line_hash,
        }));
    }
    Ok(PreparedImport::archive(vec![source_upsert], records))
}

fn classify_codex_session(event_contents: &[&str]) -> SessionClass {
    for content in event_contents {
        let Ok(value) = serde_json::from_str::<Value>(content) else {
            continue;
        };
        let payload = value.get("payload").unwrap_or(&value);
        if let Some(thread_source) = string_at(payload, &["thread_source"])
            .or_else(|| string_at(payload, &["source", "thread_source"]))
        {
            return match thread_source.as_str() {
                "user" => SessionClass::Interactive,
                "subagent" => SessionClass::Subagent,
                "automation" => SessionClass::Automation,
                _ => SessionClass::Unknown,
            };
        }
        if payload
            .get("source")
            .and_then(Value::as_object)
            .is_some_and(|source| source.contains_key("subagent"))
        {
            return SessionClass::Subagent;
        }
        match string_at(payload, &["originator"]).as_deref() {
            Some("codex_exec") => return SessionClass::Automation,
            Some("codex_chatgpt_ios_remote") => return SessionClass::Interactive,
            _ => {}
        }
        if string_at(payload, &["base_instructions"])
            .or_else(|| string_at(payload, &["base_instructions", "text"]))
            .is_some_and(|instructions| is_reviewer_instructions(&instructions))
        {
            return SessionClass::Automation;
        }
    }
    SessionClass::Interactive
}

fn is_reviewer_instructions(instructions: &str) -> bool {
    instructions
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("you are acting as a reviewer")
}

fn prepare_opencode_db_import(
    machine_id: &str,
    path: &Path,
    path_text: &str,
    source_id: &str,
    source_upsert: SourceUpsert,
) -> Result<PreparedImport> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening OpenCode database {}", path.display()))?;
    let mut records = Vec::new();

    let mut sessions = conn.prepare(
        "SELECT id, project_id, parent_id, slug, directory, title, version,
                time_created, time_updated, time_archived
         FROM session
         ORDER BY time_created, id",
    )?;
    let session_rows = sessions.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, Option<i64>>(9)?,
        ))
    })?;

    for session in session_rows {
        let (
            external_session_id,
            project_id,
            parent_id,
            slug,
            directory,
            title,
            version,
            time_created,
            time_updated,
            time_archived,
        ) = session?;
        let session_id = stable_id(&["session", "opencode", path_text, &external_session_id]);
        let workspace = workspace_from_opencode_directory(&directory);
        let session_hash = stable_hash(&(
            "opencode",
            source_id,
            &external_session_id,
            path_text,
            &title,
            time_updated,
        ))?;
        records.push(ArchiveRecord::Session(SessionRecord {
            id: session_id.clone(),
            source_id: source_id.to_string(),
            machine_id: machine_id.to_string(),
            source_kind: "opencode".to_string(),
            external_id: external_session_id.clone(),
            title: clean_optional_title(&title),
            status: if time_archived.is_some() {
                "archived".to_string()
            } else {
                "open".to_string()
            },
            started_at: unix_millis_to_utc(time_created),
            updated_at: unix_millis_to_utc(time_updated),
            metadata: opencode_session_metadata(
                path_text,
                workspace.as_ref(),
                &project_id,
                parent_id.as_deref(),
                &slug,
                &version,
            ),
            hash: session_hash,
        }));

        let mut parts = conn.prepare(
            "SELECT m.id, m.time_created, m.data, p.id, p.time_created, p.data
             FROM message m
             JOIN part p ON p.message_id = m.id
             WHERE m.session_id = ?1
             ORDER BY m.time_created, m.id, p.time_created, p.id",
        )?;
        let part_rows = parts.query_map([&external_session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let mut ordinal = 0i64;
        for part in part_rows {
            let (message_id, message_time, message_data, part_id, part_time, part_data) = part?;
            let message_value: Value = serde_json::from_str(&message_data)
                .with_context(|| format!("parsing OpenCode message {message_id}"))?;
            let part_value: Value = serde_json::from_str(&part_data)
                .with_context(|| format!("parsing OpenCode part {part_id}"))?;
            let role = string_at(&message_value, &["role"])
                .map(|role| role.to_ascii_lowercase())
                .unwrap_or_else(|| "event".to_string());
            let part_type = string_at(&part_value, &["type"]).unwrap_or_else(|| "part".to_string());
            if part_type != "text" || (role != "user" && role != "assistant") {
                continue;
            }
            let Some(content) = string_at(&part_value, &["text"])
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
            else {
                continue;
            };
            let event_id = stable_id(&["event", "opencode", &session_id, &message_id, &part_id]);
            let event_hash = stable_hash(&(
                "opencode_part",
                &message_id,
                &part_id,
                &role,
                &part_type,
                &content,
            ))?;
            let search = SearchSegment::indexed(
                "conversation",
                role.clone(),
                content.clone(),
                SemanticPolicy::Required,
            )
            .with_provenance("opencode_part_text")
            .with_stable_part(part_id.clone());
            let mut metadata = json!({
                "capture_fidelity": "normalized_opencode_sqlite",
                "parser": "opencode_sqlite_v1",
                "opencode_session_id": external_session_id.clone(),
                "opencode_message_id": message_id.clone(),
                "opencode_part_id": part_id.clone(),
                "opencode_part_type": part_type.clone(),
            })
            .as_object()
            .cloned()
            .unwrap_or_default();
            metadata.extend(opencode_usage_metadata(&message_value));
            search.apply_compat_metadata(&mut metadata);
            records.push(ArchiveRecord::Event(EventRecord {
                id: event_id,
                session_id: session_id.clone(),
                source_id: source_id.to_string(),
                machine_id: machine_id.to_string(),
                source_kind: "opencode".to_string(),
                ordinal,
                event_type: part_type.clone(),
                role: Some(role.clone()),
                content: content.clone(),
                raw_artifact_hash: None,
                occurred_at: unix_millis_to_utc(part_time)
                    .or_else(|| unix_millis_to_utc(message_time)),
                metadata: Value::Object(metadata),
                hash: event_hash,
            }));
            ordinal += 1;
        }
    }

    Ok(PreparedImport::archive(vec![source_upsert], records))
}

fn opencode_usage_metadata(message: &Value) -> Map<String, Value> {
    let mut metadata = Map::new();
    if let Some(model_id) = message.get("modelID").and_then(Value::as_str) {
        metadata.insert("opencode_model_id".to_string(), json!(model_id));
    }
    if let Some(provider_id) = message.get("providerID").and_then(Value::as_str) {
        metadata.insert("opencode_provider_id".to_string(), json!(provider_id));
    }
    if let Some(tokens) = message.get("tokens") {
        metadata.insert("opencode_tokens".to_string(), tokens.clone());
    }
    metadata
}

pub(crate) fn backfill_default_opencode_usage(store: &Store) -> Result<OpencodeUsageBackfill> {
    let path = home_dir()
        .context("cannot locate the home directory for the OpenCode database")?
        .join(".local/share/opencode/opencode.db");
    backfill_opencode_usage(store, &path)
}

fn backfill_opencode_usage(store: &Store, path: &Path) -> Result<OpencodeUsageBackfill> {
    const BATCH_SIZE: i64 = 500;

    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening OpenCode database {}", path.display()))?;
    let event_ids = opencode_event_ids(store)?;
    let mut outcome = OpencodeUsageBackfill::default();
    let mut message_rowid = 0i64;
    let mut part_rowid = 0i64;

    loop {
        let batch = {
            let mut stmt = conn.prepare(
                "SELECT m.rowid, p.rowid, m.id, p.id, m.data
                 FROM message m
                 JOIN part p ON p.message_id = m.id
                 WHERE (m.rowid > ?1 OR (m.rowid = ?1 AND p.rowid > ?2))
                   AND json_extract(m.data, '$.role') = 'assistant'
                   AND json_extract(p.data, '$.type') = 'text'
                 ORDER BY m.rowid, p.rowid
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![message_rowid, part_rowid, BATCH_SIZE], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        if batch.is_empty() {
            break;
        }
        let last = batch.last().expect("non-empty OpenCode usage batch");
        message_rowid = last.0;
        part_rowid = last.1;
        outcome.scanned += batch.len();

        outcome.updated += store.with_conn(|histo| {
            let tx = Transaction::new_unchecked(histo, TransactionBehavior::Immediate)
                .context("starting OpenCode usage backfill batch")?;
            let mut updated = 0usize;
            for (_, _, message_id, part_id, message_data) in &batch {
                let Some(event_id) = event_ids.get(&(message_id.clone(), part_id.clone())) else {
                    continue;
                };
                let message: Value = serde_json::from_str(message_data)
                    .with_context(|| format!("parsing OpenCode message {message_id}"))?;
                let patch = Value::Object(opencode_usage_metadata(&message));
                if patch.as_object().is_none_or(Map::is_empty) {
                    continue;
                }
                let patch = patch.to_string();
                updated += tx.execute(
                    "UPDATE events
                     SET metadata_json = json_patch(metadata_json, ?1)
                     WHERE id = ?2
                       AND metadata_json != json_patch(metadata_json, ?1)",
                    params![patch, event_id],
                )?;
            }
            tx.commit().context("committing OpenCode usage backfill batch")?;
            Ok(updated)
        })?;
    }

    Ok(outcome)
}

fn opencode_event_ids(store: &Store) -> Result<HashMap<(String, String), String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id,
                    json_extract(metadata_json, '$.opencode_message_id'),
                    json_extract(metadata_json, '$.opencode_part_id')
             FROM events INDEXED BY idx_events_source
             WHERE source_id IN (SELECT id FROM sources WHERE kind = 'opencode')
               AND json_type(metadata_json, '$.opencode_message_id') = 'text'
               AND json_type(metadata_json, '$.opencode_part_id') = 'text'",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
                row.get::<_, String>(0)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<HashMap<_, _>>>()
            .map_err(Into::into)
    })
}

fn clean_optional_title(title: &str) -> Option<String> {
    let title = title.trim();
    if title.is_empty() {
        None
    } else {
        Some(title.to_string())
    }
}

fn unix_millis_to_utc(millis: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(millis)
}

fn workspace_from_opencode_directory(directory: &str) -> Option<WorkspaceIdentity> {
    let cwd = normalize_path_text(directory)?;
    let workspace_root = git_root_for(&cwd).unwrap_or_else(|| cwd.clone());
    Some(WorkspaceIdentity {
        workspace_path: workspace_root.clone(),
        workspace_root,
        cwd: Some(cwd),
        git_repo: None,
        git_branch: None,
        source: "opencode.session.directory".to_string(),
        confidence: "direct".to_string(),
    })
}

fn opencode_session_metadata(
    path_text: &str,
    workspace: Option<&WorkspaceIdentity>,
    project_id: &str,
    parent_id: Option<&str>,
    slug: &str,
    version: &str,
) -> Value {
    let mut metadata = session_metadata(path_text, workspace);
    if let Value::Object(map) = &mut metadata {
        map.insert("parser".to_string(), json!("opencode_sqlite_v1"));
        map.insert(
            "capture_fidelity".to_string(),
            json!("native_opencode_sqlite"),
        );
        map.insert("opencode_project_id".to_string(), json!(project_id));
        map.insert("opencode_parent_id".to_string(), json!(parent_id));
        map.insert("opencode_slug".to_string(), json!(slug));
        map.insert("opencode_version".to_string(), json!(version));
    }
    metadata
}

fn classify_opencode_session(session_metadata: &Value) -> SessionClass {
    if session_metadata
        .get("opencode_parent_id")
        .and_then(Value::as_str)
        .is_some_and(|parent_id| !parent_id.trim().is_empty())
    {
        SessionClass::Subagent
    } else {
        SessionClass::Interactive
    }
}

impl NativeTitleIndex {
    fn load() -> Self {
        let mut index = Self::default();
        let Some(home) = home_dir() else {
            return index;
        };
        index.load_jsonl_titles(
            &home.join(".codex/session_index.jsonl"),
            "codex",
            &["id"],
            &["thread_name"],
        );
        index.load_jsonl_titles(
            &home.join(".claude/history.jsonl"),
            "claude_code",
            &["sessionId"],
            &["display"],
        );
        index
    }

    fn insert(&mut self, kind: &str, external_session_id: &str, title: &str) {
        let title = normalize_title(title);
        if title.is_empty() {
            return;
        }
        self.titles
            .insert(native_title_key(kind, external_session_id), title);
    }

    fn get(&self, kind: &str, external_session_id: &str) -> Option<String> {
        self.titles
            .get(&native_title_key(kind, external_session_id))
            .cloned()
    }

    fn iter(&self) -> impl Iterator<Item = (&str, &str, &str)> {
        self.titles.iter().filter_map(|(key, title)| {
            let (kind, external_session_id) = key.split_once('\0')?;
            Some((kind, external_session_id, title.as_str()))
        })
    }

    fn load_jsonl_titles(
        &mut self,
        path: &Path,
        kind: &str,
        id_path: &[&str],
        title_path: &[&str],
    ) {
        let Ok(text) = fs::read_to_string(path) else {
            return;
        };
        for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Some(id) = string_at(&value, id_path) else {
                continue;
            };
            let Some(title) = string_at(&value, title_path) else {
                continue;
            };
            self.insert(kind, &id, &title);
        }
    }
}

fn refresh_existing_native_titles(
    store: &Store,
    native_titles: &NativeTitleIndex,
) -> Result<usize> {
    let mut changed = 0;
    for (kind, external_session_id, title) in native_titles.iter() {
        changed += store.update_session_title_for_external_id(kind, external_session_id, title)?;
    }
    Ok(changed)
}

fn native_title_key(kind: &str, external_session_id: &str) -> String {
    format!("{kind}\0{external_session_id}")
}

fn session_title(
    kind: &str,
    external_session_id: &str,
    lines: &[ParsedLine],
    native_titles: &NativeTitleIndex,
) -> Option<String> {
    native_titles
        .get(kind, external_session_id)
        .filter(|title| !is_rejected_title_content(title))
        .or_else(|| inline_session_title(kind, lines))
        .or_else(|| fallback_session_title(lines))
}

fn inline_session_title(kind: &str, lines: &[ParsedLine]) -> Option<String> {
    match kind {
        "pi_agent" => pi_session_info_title(lines),
        _ => None,
    }
}

fn pi_session_info_title(lines: &[ParsedLine]) -> Option<String> {
    lines
        .iter()
        .rev()
        .find(|line| line.event_type == "session_info")
        .and_then(|line| string_at(&line.value, &["name"]))
        .and_then(|name| normalized_nonempty_title(&name))
        .filter(|title| !is_rejected_title_content(title))
}

fn fallback_session_title(lines: &[ParsedLine]) -> Option<String> {
    lines
        .iter()
        .find(|line| is_human_line(line) && line_title_candidate(line).is_some())
        .or_else(|| {
            lines
                .iter()
                .filter(|line| !is_instruction_event_line(line))
                .find(|line| line_title_candidate(line).is_some())
        })
        .and_then(line_title_candidate)
}

fn is_human_line(line: &ParsedLine) -> bool {
    line.role.as_deref() == Some("user") || line.event_type == "user"
}

fn title_candidate(content: &str) -> Option<String> {
    if is_rejected_title_content(content) {
        return None;
    }
    normalized_nonempty_title(content)
}

fn line_title_candidate(line: &ParsedLine) -> Option<String> {
    let mut parts = Vec::new();
    collect_conversation_text(&line.value, &mut parts);
    parts
        .iter()
        .find_map(|part| title_candidate(part))
        .or_else(|| title_candidate(&line.search.text))
        .or_else(|| title_candidate(&line.content))
}
fn normalized_nonempty_title(content: &str) -> Option<String> {
    let title = normalize_title(content);
    if title.is_empty() {
        None
    } else {
        Some(title.chars().take(100).collect())
    }
}

fn normalize_title(content: &str) -> String {
    let first_line = content
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    first_line.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_bootstrap_content(content: &str) -> bool {
    let content = content.trim_start();
    content.starts_with("# AGENTS.md instructions")
        || content.starts_with("<INSTRUCTIONS>")
        || content.starts_with("<environment_context>")
}

/// Returns true when content looks like instruction/system/policy text
/// or generic session metadata rather than concise user-facing conversation
/// content, making it a poor transcript title candidate.
fn is_rejected_title_content(content: &str) -> bool {
    if is_bootstrap_content(content) {
        return true;
    }
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return true;
    }
    if looks_like_session_metadata(trimmed) {
        return true;
    }
    if looks_like_instruction_block(trimmed) {
        return true;
    }
    false
}

/// Returns true when a parsed line is an instruction/system event that
/// should never be used as a title source.
fn is_instruction_event_line(line: &ParsedLine) -> bool {
    if is_instruction_role(line.role.as_deref()) {
        return true;
    }
    let event_type = line.event_type.to_ascii_lowercase();
    matches!(
        event_type.as_str(),
        "system" | "developer" | "system_prompt" | "instructions"
    )
}

fn looks_like_instruction_block(content: &str) -> bool {
    let lower = content.to_ascii_lowercase();
    // Markdown header-style instruction blocks
    lower.starts_with("# instructions")
        || lower.starts_with("# policy")
        || lower.starts_with("# system")
        || lower.starts_with("# guidelines")
        || lower.starts_with("# rules")
        // Common instruction/preamble markers at the start of the text
        || lower.starts_with("you are ")
        || lower.starts_with("your task ")
        || lower.starts_with("your role ")
        || lower.starts_with("instructions:")
        || lower.starts_with("follow these ")
        || lower.starts_with("policy:")
        || lower.starts_with("system prompt:")
        || lower.starts_with("<system>")
        || lower.starts_with("<developer>")
        // Long text (>200 chars) with a high density of instruction phrases
        // is likely bootstrap/policy text rather than a user message.
        || (content.len() > 200 && instruction_marker_density(&lower) >= 3)
}

fn instruction_marker_density(lower: &str) -> usize {
    let markers = [
        "you are",
        "your task",
        "your role",
        "instructions",
        "follow these",
        "policy",
        "guidelines",
        "must ",
        "should ",
        "always ",
        "never ",
        "do not",
        "ensure",
        "required",
        "preamble",
        "bootstrap",
    ];
    markers.iter().filter(|marker| lower.contains(*marker)).count()
}

fn looks_like_session_metadata(content: &str) -> bool {
    let trimmed = content.trim();
    // JSON-like metadata dumps are not useful titles
    (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
        || trimmed.contains("\"sessionId\"")
        || trimmed.contains("\"session_id\"")
}

fn session_metadata(path_text: &str, workspace: Option<&WorkspaceIdentity>) -> Value {
    let mut metadata = Map::new();
    metadata.insert("path".to_string(), json!(path_text));
    metadata.insert("capture_fidelity".to_string(), json!("exact_local_log"));
    metadata.insert("parser".to_string(), json!("generic_json_event_v1"));
    if let Some(workspace) = workspace {
        metadata.insert(
            "workspace_path".to_string(),
            json!(&workspace.workspace_path),
        );
        metadata.insert(
            "workspace_root".to_string(),
            json!(&workspace.workspace_root),
        );
        metadata.insert(
            "workspace_confidence".to_string(),
            json!(&workspace.confidence),
        );
        metadata.insert("workspace_source".to_string(), json!(&workspace.source));
        if let Some(cwd) = &workspace.cwd {
            metadata.insert("cwd".to_string(), json!(cwd));
        }
        if let Some(git_repo) = &workspace.git_repo {
            metadata.insert("git_repo".to_string(), json!(git_repo));
        }
        if let Some(git_branch) = &workspace.git_branch {
            metadata.insert("git_branch".to_string(), json!(git_branch));
        }
        metadata.insert(
            "workspace".to_string(),
            json!({
                "path": &workspace.workspace_path,
                "root": &workspace.workspace_root,
                "cwd": &workspace.cwd,
                "git_repo": &workspace.git_repo,
                "git_branch": &workspace.git_branch,
                "source": &workspace.source,
                "confidence": &workspace.confidence
            }),
        );
    }
    Value::Object(metadata)
}

fn session_workspace(
    kind: &str,
    source_path: &Path,
    lines: &[ParsedLine],
) -> Option<WorkspaceIdentity> {
    lines
        .iter()
        .find_map(|line| workspace_from_value(&line.value))
        .or_else(|| workspace_from_source_path(kind, source_path))
}

fn workspace_from_value(value: &Value) -> Option<WorkspaceIdentity> {
    let (raw_path, source) = workspace_path_candidate(value)?;
    let cwd = normalize_path_text(&raw_path)?;
    let workspace_root = git_root_for(&cwd).unwrap_or_else(|| cwd.clone());
    Some(WorkspaceIdentity {
        workspace_path: workspace_root.clone(),
        workspace_root,
        cwd: Some(cwd),
        git_repo: git_repo_candidate(value),
        git_branch: git_branch_candidate(value),
        source,
        confidence: "direct".to_string(),
    })
}

fn classify_claude_session(event_contents: &[&str]) -> SessionClass {
    let values = event_contents
        .iter()
        .filter_map(|content| serde_json::from_str::<Value>(content).ok())
        .collect::<Vec<_>>();

    if values.iter().any(|value| {
        string_at(value, &["entrypoint"])
            .or_else(|| string_at(value, &["payload", "entrypoint"]))
            .is_some_and(|entrypoint| {
                let entrypoint = entrypoint.to_ascii_lowercase();
                entrypoint.contains("sdk") || entrypoint.contains("headless")
            })
    }) {
        return SessionClass::Automation;
    }

    let user_events = values
        .iter()
        .filter(|value| claude_event_role(value).as_deref() == Some("user"))
        .collect::<Vec<_>>();
    if !user_events.is_empty()
        && user_events
            .iter()
            .all(|value| value.get("isSidechain").and_then(Value::as_bool) == Some(true))
    {
        return SessionClass::Subagent;
    }
    if !user_events.is_empty()
        || values.iter().any(|value| {
            string_at(value, &["entrypoint"])
                .or_else(|| string_at(value, &["payload", "entrypoint"]))
                .is_some_and(|entrypoint| {
                    matches!(entrypoint.to_ascii_lowercase().as_str(), "claude-desktop" | "cli")
                })
        })
    {
        SessionClass::Interactive
    } else {
        SessionClass::Unknown
    }
}

fn claude_event_relationship_metadata(value: &Value) -> Option<Value> {
    let uuid = value.get("uuid").and_then(Value::as_str);
    let parent_uuid = value.get("parentUuid").and_then(Value::as_str);
    let is_sidechain = value
        .get("isSidechain")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let task_tool_use = claude_has_task_tool_use(value);
    (uuid.is_some() || parent_uuid.is_some() || is_sidechain || task_tool_use).then(|| {
        json!({
            "uuid": uuid,
            "parent_uuid": parent_uuid,
            "is_sidechain": is_sidechain,
            "task_tool_use": task_tool_use
        })
    })
}

fn claude_has_task_tool_use(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            let is_task = map.get("type").and_then(Value::as_str) == Some("tool_use")
                && map
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name.eq_ignore_ascii_case("task"));
            is_task || map.values().any(claude_has_task_tool_use)
        }
        Value::Array(items) => items.iter().any(claude_has_task_tool_use),
        _ => false,
    }
}

fn classify_claude_event(event_content: &str) -> SessionClass {
    serde_json::from_str::<Value>(event_content)
        .ok()
        .filter(|value| claude_event_role(value).as_deref() == Some("user"))
        .and_then(|value| value.get("isSidechain").and_then(Value::as_bool))
        .map(|sidechain| {
            if sidechain {
                SessionClass::Subagent
            } else {
                SessionClass::Interactive
            }
        })
        .unwrap_or(SessionClass::Unknown)
}

fn claude_event_role(value: &Value) -> Option<String> {
    string_at(value, &["role"])
        .or_else(|| string_at(value, &["message", "role"]))
        .or_else(|| string_at(value, &["type"]))
        .map(|role| role.to_ascii_lowercase())
}

fn workspace_path_candidate(value: &Value) -> Option<(String, String)> {
    for (path, source) in [
        (&["cwd"][..], "cwd"),
        (
            &["current_working_directory"][..],
            "current_working_directory",
        ),
        (&["workspace"][..], "workspace"),
        (&["workspace_path"][..], "workspace_path"),
        (&["project_path"][..], "project_path"),
        (&["repo_path"][..], "repo_path"),
        (&["payload", "cwd"][..], "payload.cwd"),
        (
            &["payload", "current_working_directory"][..],
            "payload.current_working_directory",
        ),
        (&["payload", "workspace"][..], "payload.workspace"),
        (&["payload", "workspace_path"][..], "payload.workspace_path"),
        (&["payload", "project_path"][..], "payload.project_path"),
        (&["payload", "repo_path"][..], "payload.repo_path"),
        (&["payload", "git", "root"][..], "payload.git.root"),
        (
            &["turn_context", "payload", "cwd"][..],
            "turn_context.payload.cwd",
        ),
        (&["git", "root"][..], "git.root"),
    ] {
        if let Some(value) = string_at(value, path).filter(|text| looks_like_path(text)) {
            return Some((value, source.to_string()));
        }
    }
    None
}

fn git_repo_candidate(value: &Value) -> Option<String> {
    for path in [
        &["git_repo"][..],
        &["git_remote"][..],
        &["repository_url"][..],
        &["repo_url"][..],
        &["payload", "git_repo"][..],
        &["payload", "git_remote"][..],
        &["payload", "repository_url"][..],
        &["payload", "repo_url"][..],
        &["payload", "git", "remote"][..],
        &["payload", "git", "repository_url"][..],
        &["git", "remote"][..],
        &["git", "repository_url"][..],
    ] {
        if let Some(value) = string_at(value, path).filter(|text| !text.trim().is_empty()) {
            return Some(value);
        }
    }
    None
}

fn git_branch_candidate(value: &Value) -> Option<String> {
    for path in [
        &["gitBranch"][..],
        &["git_branch"][..],
        &["branch"][..],
        &["payload", "gitBranch"][..],
        &["payload", "git_branch"][..],
        &["payload", "branch"][..],
        &["payload", "git", "branch"][..],
        &["git", "branch"][..],
    ] {
        if let Some(value) = string_at(value, path).filter(|text| !text.trim().is_empty()) {
            return Some(value);
        }
    }
    None
}

fn workspace_from_source_path(kind: &str, source_path: &Path) -> Option<WorkspaceIdentity> {
    if kind != "claude_code" {
        return None;
    }
    let project_dir = source_path.parent()?.file_name()?.to_str()?;
    let decoded = decode_claude_project_dir(project_dir)?;
    let workspace_path = normalize_path_text(&decoded).unwrap_or(decoded);
    Some(WorkspaceIdentity {
        workspace_path: workspace_path.clone(),
        workspace_root: workspace_path,
        cwd: None,
        git_repo: None,
        git_branch: None,
        source: "source_path.claude_project_dir".to_string(),
        confidence: "inferred".to_string(),
    })
}

fn decode_claude_project_dir(name: &str) -> Option<String> {
    let stripped = name.strip_prefix('-')?;
    if stripped.is_empty() {
        return None;
    }
    Some(format!("/{}", stripped.replace('-', "/")))
}

fn normalize_path_text(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let expanded = raw
        .strip_prefix("~/")
        .and_then(|rest| home_dir().map(|home| home.join(rest)))
        .unwrap_or_else(|| PathBuf::from(raw));
    if let Ok(canonical) = expanded.canonicalize() {
        return Some(canonical.to_string_lossy().to_string());
    }
    Some(clean_path(&expanded).to_string_lossy().to_string())
}

fn git_root_for(path: &str) -> Option<String> {
    let mut cursor = PathBuf::from(path);
    loop {
        if cursor.join(".git").exists() {
            return Some(cursor.to_string_lossy().to_string());
        }
        if !cursor.pop() {
            return None;
        }
    }
}

fn clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            _ => out.push(component.as_os_str()),
        }
    }
    out
}

fn looks_like_path(text: &str) -> bool {
    let text = text.trim();
    text.starts_with('/') || text.starts_with("~/")
}

fn parse_jsonl(text: &str) -> Result<Vec<ParsedLine>> {
    parse_jsonl_with_start(text, 0, 0)
}

fn parse_jsonl_with_start(
    text: &str,
    starting_ordinal: i64,
    starting_byte_offset: usize,
) -> Result<Vec<ParsedLine>> {
    let mut out = Vec::new();
    let mut offset = starting_byte_offset;
    for (idx, raw_line) in text.split_inclusive('\n').enumerate() {
        let byte_len = raw_line.len();
        let raw_line = raw_line.trim();
        if raw_line.is_empty() {
            offset += byte_len;
            continue;
        }
        let value: Value = serde_json::from_str(raw_line)
            .with_context(|| format!("parsing JSONL line {}", idx + 1))?;
        out.push(parsed_line(
            starting_ordinal.saturating_add(idx as i64),
            value,
            offset,
            byte_len,
        ));
        offset += byte_len;
    }
    Ok(out)
}

fn parse_json_file(text: &str) -> Result<Vec<ParsedLine>> {
    let value: Value = serde_json::from_str(text)?;
    if let Some(items) = value.as_array() {
        Ok(items
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, value)| parsed_line(idx as i64, value, 0, text.len()))
            .collect())
    } else {
        Ok(vec![parsed_line(0, value, 0, text.len())])
    }
}

fn parsed_line(ordinal: i64, value: Value, byte_offset: usize, byte_len: usize) -> ParsedLine {
    let role = string_at(&value, &["role"])
        .or_else(|| string_at(&value, &["message", "role"]))
        .or_else(|| string_at(&value, &["payload", "role"]));
    let event_type = string_at(&value, &["type"])
        .or_else(|| string_at(&value, &["payload", "type"]))
        .unwrap_or_else(|| role.clone().unwrap_or_else(|| "event".to_string()));
    let search = derive_search_segment(&value, role.as_deref(), &event_type);
    let (content, content_compacted) =
        event_content_for_search(&value, role.as_deref(), &event_type, &search);
    let occurred_at = string_at(&value, &["timestamp"])
        .or_else(|| string_at(&value, &["created_at"]))
        .or_else(|| string_at(&value, &["message", "created_at"]))
        .and_then(|text| parse_time(&text));
    let external_session_id = string_at(&value, &["sessionId"])
        .or_else(|| string_at(&value, &["session_id"]))
        .or_else(|| string_at(&value, &["payload", "id"]));
    ParsedLine {
        ordinal,
        value,
        byte_offset,
        byte_len,
        content,
        content_compacted,
        search,
        role,
        event_type,
        occurred_at,
        external_session_id,
    }
}

fn derive_search_segment(value: &Value, role: Option<&str>, event_type: &str) -> SearchSegment {
    let normalized_role = role.map(str::to_ascii_lowercase);
    let search_role = normalized_role.as_deref().or_else(|| match event_type {
        "user" | "assistant" => Some(event_type),
        _ => None,
    });

    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        let mut parts = Vec::new();
        for message in messages {
            let role = string_at(message, &["role"])
                .map(|role| role.to_ascii_lowercase())
                .unwrap_or_default();
            if role == "user" || role == "assistant" {
                collect_message_text(message, &mut parts);
            }
        }
        return projection_from_parts(parts, "conversation", "no conversation text in messages");
    }

    match search_role {
        Some("user") | Some("assistant") => {
            let mut parts = Vec::new();
            collect_conversation_text(value, &mut parts);
            projection_from_parts(
                parts,
                search_role.unwrap_or("conversation"),
                "no conversation text",
            )
        }
        Some("tool") => SearchSegment::skipped("none", "tool event"),
        Some("system") | Some("developer") => SearchSegment::skipped("none", "instruction event"),
        _ => {
            if looks_like_tool_event(value, event_type) {
                SearchSegment::skipped("none", "tool event")
            } else {
                SearchSegment::skipped("none", "non-message event")
            }
        }
    }
}

fn projection_from_parts(parts: Vec<String>, kind: &str, empty_reason: &str) -> SearchSegment {
    let text = normalize_parts(parts);
    if text.is_empty() {
        SearchSegment::skipped("none", empty_reason)
    } else {
        SearchSegment::indexed("conversation", kind, text, SemanticPolicy::Required)
            .with_provenance("message_text")
    }
}

fn event_content_for_search(
    value: &Value,
    role: Option<&str>,
    event_type: &str,
    search: &SearchSegment,
) -> (String, bool) {
    if search.is_searchable() {
        return (search.text.clone(), false);
    }
    let normalized = normalized_event_content(value)
        .unwrap_or_else(|| compact_event_summary(value, role, event_type, search));
    (normalized, false)
}

fn normalized_event_content(value: &Value) -> Option<String> {
    serde_json::to_string(value)
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn compact_event_summary(
    value: &Value,
    role: Option<&str>,
    event_type: &str,
    search: &SearchSegment,
) -> String {
    let label = if is_instruction_role(role) {
        "instruction"
    } else if looks_like_thinking_event(value, event_type) {
        "thinking"
    } else if looks_like_compaction_event(value, event_type) {
        "compaction"
    } else if looks_like_tool_event(value, event_type) {
        "tool"
    } else {
        event_type.trim()
    };
    let label = if label.is_empty() {
        "non-message"
    } else {
        label
    };
    let reason = search
        .skip_reason
        .as_deref()
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or("not indexed");
    format!("[{label} event omitted from projection: {reason}; raw payload unavailable]")
}

fn collect_conversation_text(value: &Value, parts: &mut Vec<String>) {
    if let Some(message) = value.get("message") {
        collect_message_text(message, parts);
    }
    if let Some(payload) = value.get("payload") {
        collect_message_text(payload, parts);
    }
    collect_message_text(value, parts);
}

fn collect_message_text(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if is_toolish_object(value) {
                return;
            }
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                push_text(parts, text);
            }
            if let Some(content) = map.get("content") {
                collect_content_text(content, parts);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_message_text(item, parts);
            }
        }
        Value::String(text) => push_text(parts, text),
        _ => {}
    }
}

fn collect_content_text(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::String(text) => push_text(parts, text),
        Value::Array(items) => {
            for item in items {
                if is_toolish_object(item) {
                    continue;
                }
                collect_content_text(item, parts);
            }
        }
        Value::Object(map) => {
            if is_toolish_object(value) {
                return;
            }
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                push_text(parts, text);
            }
            if let Some(content) = map.get("content") {
                collect_content_text(content, parts);
            }
        }
        _ => {}
    }
}

fn looks_like_tool_event(value: &Value, event_type: &str) -> bool {
    let event_type = event_type.to_ascii_lowercase();
    event_type.contains("tool")
        || event_type.contains("function_call")
        || event_type.contains("exec")
        || is_toolish_object(value)
}

fn looks_like_thinking_event(value: &Value, event_type: &str) -> bool {
    let event_type = event_type.to_ascii_lowercase();
    event_type.contains("thinking")
        || event_type.contains("reasoning")
        || value_contains_kind(value, &["thinking", "reasoning"])
}

fn looks_like_compaction_event(value: &Value, event_type: &str) -> bool {
    let event_type = event_type.to_ascii_lowercase();
    event_type.contains("compact") || value_contains_kind(value, &["compact", "compaction"])
}

fn is_instruction_role(role: Option<&str>) -> bool {
    role.map(str::to_ascii_lowercase)
        .is_some_and(|role| role == "system" || role == "developer")
}

fn is_toolish_object(value: &Value) -> bool {
    let Some(map) = value.as_object() else {
        return false;
    };
    let kind = map
        .get("type")
        .or_else(|| map.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    kind.contains("tool")
        || kind.contains("function_call")
        || kind.contains("tool_result")
        || kind.contains("thinking")
        || kind.contains("reasoning")
        || kind.contains("compact")
        || kind == "bash"
        || map.contains_key("tool_use_id")
        || map.contains_key("toolUseResult")
        || map.contains_key("stdout")
        || map.contains_key("stderr")
}

fn value_contains_kind(value: &Value, needles: &[&str]) -> bool {
    match value {
        Value::Object(map) => {
            let kind_matches = ["type", "name", "kind"].iter().any(|key| {
                map.get(*key)
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase)
                    .is_some_and(|kind| needles.iter().any(|needle| kind.contains(needle)))
            });
            kind_matches
                || map
                    .values()
                    .any(|child| value_contains_kind(child, needles))
        }
        Value::Array(items) => items.iter().any(|item| value_contains_kind(item, needles)),
        _ => false,
    }
}

fn normalize_parts(parts: Vec<String>) -> String {
    parts
        .into_iter()
        .map(|part| part.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn push_text(parts: &mut Vec<String>, text: &str) {
    let text = text.trim();
    if !text.is_empty() {
        parts.push(text.to_string());
    }
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str().map(ToOwned::to_owned)
}

fn parse_time(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("unknown-session")
        .to_string()
}

fn has_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            extensions
                .iter()
                .any(|candidate| ext.eq_ignore_ascii_case(candidate))
        })
        .unwrap_or(false)
}

fn is_hidden_noise(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name == ".git" || name == "node_modules" || name == "target")
        .unwrap_or(false)
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn file_mtime_ms(metadata: &fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;

    #[test]
    fn classifies_codex_session_signals() {
        for (source, expected) in [
            ("user", SessionClass::Interactive),
            ("subagent", SessionClass::Subagent),
            ("automation", SessionClass::Automation),
        ] {
            let content = json!({
                "type": "session_meta",
                "payload": {"source": {"thread_source": source}}
            })
            .to_string();
            assert_eq!(
                classify_session("codex", &json!({}), &[content.as_str()]),
                expected
            );
        }

        let direct_thread_source =
            json!({"payload": {"thread_source": "subagent"}}).to_string();
        assert_eq!(
            classify_session(
                "codex",
                &json!({}),
                &[direct_thread_source.as_str()]
            ),
            SessionClass::Subagent
        );

        for (payload, expected) in [
            (
                json!({"source": {"subagent": {"thread_spawn": {"agent_nickname": "reviewer"}}}}),
                SessionClass::Subagent,
            ),
            (json!({"originator": "codex_exec"}), SessionClass::Automation),
            (
                json!({"originator": "codex_chatgpt_ios_remote"}),
                SessionClass::Interactive,
            ),
            (
                json!({"base_instructions": {"text": "You are acting as a reviewer for these changes."}}),
                SessionClass::Automation,
            ),
        ] {
            let content = json!({"type": "session_meta", "payload": payload}).to_string();
            assert_eq!(
                classify_session("codex", &json!({}), &[content.as_str()]),
                expected
            );
        }

        assert_eq!(
            classify_session("codex", &json!({}), &[]),
            SessionClass::Interactive
        );
    }

    #[test]
    fn extracts_codex_subagent_paths_from_embedded_and_structured_notifications() {
        let embedded = r#"<subagent_notification>{"agent_path":"child-one"}</subagent_notification>"#;
        assert_eq!(codex_subagent_paths(embedded), vec!["child-one"]);

        let structured = json!({
            "type": "message",
            "content": "<subagent_notification>",
            "payload": {"agent_path": "child-two"}
        })
        .to_string();
        assert_eq!(codex_subagent_paths(&structured), vec!["child-two"]);
        assert!(codex_subagent_paths(r#"{"agent_path":"not-a-notification"}"#).is_empty());
    }

    #[test]
    fn classifies_opencode_parent_sessions_as_subagents() {
        assert_eq!(
            classify_session(
                "opencode",
                &json!({"opencode_parent_id": "ses_parent"}),
                &[]
            ),
            SessionClass::Subagent
        );
        assert_eq!(
            classify_session("opencode", &json!({"opencode_parent_id": null}), &[]),
            SessionClass::Interactive
        );

        let relationship = resolve_session_relationship(
            "opencode",
            "ses_child",
            &json!({"opencode_parent_id": "ses_parent"}),
            &[],
        );
        assert_eq!(relationship.parent_external_id.as_deref(), Some("ses_parent"));
        assert_eq!(relationship.relationship, SessionRelationshipKind::Subagent);
        assert_eq!(relationship.rule, "opencode.parent_id");

        let none = resolve_session_relationship(
            "opencode",
            "ses_root",
            &json!({"opencode_parent_id": null}),
            &[],
        );
        assert_eq!(none.parent_external_id, None);
        assert_eq!(none.relationship, SessionRelationshipKind::None);
    }

    #[test]
    fn classifies_claude_sidechains_and_entrypoints() {
        let sidechain_one = json!({
            "type": "user",
            "isSidechain": true,
            "message": {"role": "user", "content": "review this"}
        })
        .to_string();
        let sidechain_two = json!({
            "type": "user",
            "isSidechain": true,
            "message": {"role": "user", "content": "continue"}
        })
        .to_string();
        assert_eq!(
            classify_session(
                "claude_code",
                &json!({}),
                &[sidechain_one.as_str(), sidechain_two.as_str()]
            ),
            SessionClass::Subagent
        );
        assert_eq!(
            classify_event("claude_code", &sidechain_one),
            SessionClass::Subagent
        );

        let interactive = json!({
            "type": "user",
            "isSidechain": false,
            "message": {"role": "user", "content": "hello"}
        })
        .to_string();
        assert_eq!(
            classify_session(
                "claude_code",
                &json!({}),
                &[sidechain_one.as_str(), interactive.as_str()]
            ),
            SessionClass::Interactive
        );
        assert_eq!(
            classify_event("claude_code", &interactive),
            SessionClass::Interactive
        );

        let sdk = json!({"entrypoint": "sdk-ts"}).to_string();
        assert_eq!(
            classify_session("claude_code", &json!({}), &[sdk.as_str()]),
            SessionClass::Automation
        );
        let desktop = json!({"entrypoint": "claude-desktop"}).to_string();
        assert_eq!(
            classify_session("claude_code", &json!({}), &[desktop.as_str()]),
            SessionClass::Interactive
        );

        let relationship = resolve_session_relationship(
            "claude_code",
            "agent-child",
            &json!({
                "path": "/logs/123e4567-e89b-12d3-a456-426614174000/subagents/agent-child.jsonl"
            }),
            &[],
        );
        assert_eq!(
            relationship.parent_external_id.as_deref(),
            Some("123e4567-e89b-12d3-a456-426614174000")
        );
        assert_eq!(relationship.relationship, SessionRelationshipKind::Subagent);
        assert_eq!(relationship.rule, "claude.subagent_path");

        let unrelated = resolve_session_relationship(
            "claude_code",
            "regular",
            &json!({"path": "/logs/regular.jsonl"}),
            &[],
        );
        assert_eq!(unrelated.relationship, SessionRelationshipKind::None);

        let inline = claude_event_relationship_metadata(&json!({
            "uuid": "task-message",
            "parentUuid": "user-message",
            "isSidechain": false,
            "message": {"content": [{"type": "tool_use", "name": "Task"}]}
        }))
        .expect("Claude relationship metadata");
        assert_eq!(inline["uuid"], "task-message");
        assert_eq!(inline["parent_uuid"], "user-message");
        assert_eq!(inline["is_sidechain"], false);
        assert_eq!(inline["task_tool_use"], true);
    }

    #[test]
    fn leaves_unmarked_providers_unknown() {
        assert_eq!(
            classify_session("pi_agent", &json!({}), &[]),
            SessionClass::Unknown
        );
        assert_eq!(
            classify_event("hermes", "{}"),
            SessionClass::Unknown
        );
    }

    #[test]
    fn extracts_codex_cumulative_max_and_primary_model() {
        let events = vec![
            usage_event(json!({"payload": {"model": "gpt-5.4"}})),
            usage_event(json!({"payload": {"model": "gpt-5.4"}})),
            usage_event(json!({"payload": {"model": "gpt-5.5"}})),
            usage_event(json!({
                "payload": {"info": {"total_token_usage": {
                    "input_tokens": 100,
                    "cached_input_tokens": 20,
                    "output_tokens": 30
                }}}
            })),
            usage_event(json!({
                "payload": {"info": {"total_token_usage": {
                    "input_tokens": 180,
                    "cached_input_tokens": 40,
                    "output_tokens": 55
                }}}
            })),
        ];

        let usage = extract_session_usage("codex", &events);
        assert_eq!(usage.models, vec!["gpt-5.4", "gpt-5.5"]);
        assert_eq!(usage.primary_model.as_deref(), Some("gpt-5.4"));
        assert_eq!(usage.input_tokens, Some(180));
        assert_eq!(usage.cached_input_tokens, Some(40));
        assert_eq!(usage.output_tokens, Some(55));
    }

    #[test]
    fn sums_claude_and_pi_usage_and_deduplicates_opencode_messages() {
        let claude = extract_session_usage(
            "claude_code",
            &[
                usage_event(json!({"message": {"model": "claude-opus", "usage": {
                    "input_tokens": 10, "cache_read_input_tokens": 3, "output_tokens": 5
                }}})),
                usage_event(json!({"message": {"model": "claude-opus", "usage": {
                    "input_tokens": 20, "cache_read_input_tokens": 7, "output_tokens": 9
                }}})),
            ],
        );
        assert_eq!(claude.input_tokens, Some(30));
        assert_eq!(claude.cached_input_tokens, Some(10));
        assert_eq!(claude.output_tokens, Some(14));

        let pi = extract_session_usage(
            "pi_agent",
            &[usage_event(json!({"message": {"model": "glm-5", "usage": {
                "input": 90, "cacheRead": 30, "output": 12
            }}}))],
        );
        assert_eq!(pi.input_tokens, Some(90));
        assert_eq!(pi.cached_input_tokens, Some(30));
        assert_eq!(pi.output_tokens, Some(12));

        let metadata = json!({
            "opencode_message_id": "msg_1",
            "opencode_model_id": "kimi-k2",
            "opencode_tokens": {"input": 50, "output": 8, "cache": {"read": 11}}
        });
        let opencode = extract_session_usage(
            "opencode",
            &[
                UsageEvent {
                    content: "part one".to_string(),
                    metadata: metadata.clone(),
                },
                UsageEvent {
                    content: "part two".to_string(),
                    metadata,
                },
            ],
        );
        assert_eq!(opencode.models, vec!["kimi-k2"]);
        assert_eq!(opencode.input_tokens, Some(50));
        assert_eq!(opencode.cached_input_tokens, Some(11));
        assert_eq!(opencode.output_tokens, Some(8));
    }

    fn usage_event(value: Value) -> UsageEvent {
        UsageEvent {
            content: value.to_string(),
            metadata: json!({}),
        }
    }

    #[test]
    fn indexes_user_message_text() {
        let line = parsed_line(
            0,
            json!({
                "type": "message",
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": "search this exact request"}]
                }
            }),
            0,
            1,
        );

        assert!(line.search.is_searchable());
        assert_eq!(line.search.kind, "user");
        assert_eq!(line.search.text, "search this exact request");
    }

    #[test]
    fn does_not_index_tool_output_text() {
        let line = parsed_line(
            0,
            json!({
                "type": "tool_result",
                "role": "tool",
                "content": [{"type": "text", "text": "noisy command output"}],
                "tool_use_id": "toolu_123"
            }),
            0,
            1,
        );

        assert!(!line.search.is_searchable());
        assert!(line.search.text.is_empty());
        assert_eq!(line.search.skip_reason.as_deref(), Some("tool event"));
    }

    #[test]
    fn indexes_hermes_message_arrays_without_tools() {
        let line = parsed_line(
            0,
            json!({
                "session_id": "session-1",
                "messages": [
                    {"role": "system", "content": "do not index system text"},
                    {"role": "user", "content": "human question"},
                    {"role": "tool", "content": "tool answer"},
                    {"role": "assistant", "content": "assistant answer"}
                ]
            }),
            0,
            1,
        );

        assert!(line.search.is_searchable());
        assert_eq!(line.search.kind, "conversation");
        assert_eq!(line.search.text, "human question\nassistant answer");
    }

    #[test]
    fn extracts_workspace_from_codex_payload_cwd() {
        let temp = tempfile::tempdir().expect("temp dir");
        let repo = temp.path().join("repo");
        let nested = repo.join("subdir");
        fs::create_dir_all(repo.join(".git")).expect("create git dir");
        fs::create_dir_all(&nested).expect("create nested dir");

        let workspace = workspace_from_value(&json!({
            "payload": {
                "cwd": nested,
                "git": {
                    "branch": "main",
                    "remote": "git@example.com:example/repo.git"
                }
            }
        }))
        .expect("workspace");
        let expected_repo = repo.canonicalize().expect("canonical repo");
        let expected_nested = nested.canonicalize().expect("canonical nested");

        assert_eq!(
            workspace.cwd.as_deref(),
            Some(expected_nested.to_str().unwrap())
        );
        assert_eq!(workspace.workspace_root, expected_repo.to_string_lossy());
        assert_eq!(workspace.workspace_path, expected_repo.to_string_lossy());
        assert_eq!(workspace.git_branch.as_deref(), Some("main"));
        assert_eq!(
            workspace.git_repo.as_deref(),
            Some("git@example.com:example/repo.git")
        );
        assert_eq!(workspace.source, "payload.cwd");
        assert_eq!(workspace.confidence, "direct");
    }

    #[test]
    fn extracts_workspace_from_claude_top_level_cwd() {
        let temp = tempfile::tempdir().expect("temp dir");
        let workspace_path = temp.path().join("claude-project");
        fs::create_dir_all(&workspace_path).expect("create workspace");

        let workspace = workspace_from_value(&json!({
            "cwd": workspace_path,
            "gitBranch": "feature-sync"
        }))
        .expect("workspace");
        let expected_workspace = workspace_path.canonicalize().expect("canonical workspace");

        assert_eq!(
            workspace.workspace_path,
            expected_workspace.to_string_lossy().to_string()
        );
        assert_eq!(workspace.git_branch.as_deref(), Some("feature-sync"));
        assert_eq!(workspace.source, "cwd");
    }

    #[test]
    fn session_title_prefers_native_title() {
        let mut native_titles = NativeTitleIndex::default();
        native_titles.insert("codex", "session-1", "Native Codex Title");
        let lines = vec![parsed_line(
            0,
            json!({
                "session_id": "session-1",
                "type": "message",
                "role": "user",
                "content": "fallback user request"
            }),
            0,
            1,
        )];

        assert_eq!(
            session_title("codex", "session-1", &lines, &native_titles).as_deref(),
            Some("Native Codex Title")
        );
    }

    #[test]
    fn fallback_session_title_skips_bootstrap_content() {
        let native_titles = NativeTitleIndex::default();
        let lines = vec![
            parsed_line(
                0,
                json!({
                    "session_id": "session-1",
                    "type": "user",
                    "content": "# AGENTS.md instructions\n<INSTRUCTIONS>..."
                }),
                0,
                1,
            ),
            parsed_line(
                1,
                json!({
                    "session_id": "session-1",
                    "type": "user",
                    "content": "Make thread titles recognizable\nwith more detail"
                }),
                0,
                1,
            ),
        ];

        assert_eq!(
            session_title("codex", "session-1", &lines, &native_titles).as_deref(),
            Some("Make thread titles recognizable")
        );
    }

    #[test]
    fn pi_session_title_prefers_latest_session_info_name() {
        let native_titles = NativeTitleIndex::default();
        let lines = vec![
            parsed_line(
                0,
                json!({
                    "type": "session",
                    "id": "pi-session-1",
                    "cwd": "/tmp/repo"
                }),
                0,
                1,
            ),
            parsed_line(
                1,
                json!({
                    "type": "session_info",
                    "name": "Old Pi Name"
                }),
                0,
                1,
            ),
            parsed_line(
                2,
                json!({
                    "type": "message",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "fallback user request"}]
                    }
                }),
                0,
                1,
            ),
            parsed_line(
                3,
                json!({
                    "type": "session_info",
                    "name": "Renamed Pi Session"
                }),
                0,
                1,
            ),
        ];

        assert_eq!(
            session_title("pi_agent", "pi-session-1", &lines, &native_titles).as_deref(),
            Some("Renamed Pi Session")
        );
    }

    #[test]
    fn pi_session_title_falls_back_after_cleared_session_info_name() {
        let native_titles = NativeTitleIndex::default();
        let lines = vec![
            parsed_line(
                0,
                json!({
                    "type": "session_info",
                    "name": ""
                }),
                0,
                1,
            ),
            parsed_line(
                1,
                json!({
                    "type": "message",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "first real pi prompt"}]
                    }
                }),
                0,
                1,
            ),
        ];

        assert_eq!(
            session_title("pi_agent", "pi-session-1", &lines, &native_titles).as_deref(),
            Some("first real pi prompt")
        );
    }

    #[test]
    fn claude_session_title_skips_instruction_text_for_user_message() {
        let native_titles = NativeTitleIndex::default();
        let lines = vec![
            parsed_line(
                0,
                json!({
                    "session_id": "claude-1",
                    "type": "user",
                    "role": "user",
                    "content": "You are a helpful coding assistant. Follow these instructions carefully. Your task is to assist with code review and debugging. You must ensure all code follows the project guidelines and style rules. Always use descriptive variable names. Never commit directly to main.",
                    "timestamp": "2026-06-03T00:00:00Z"
                }),
                0,
                1,
            ),
            parsed_line(
                1,
                json!({
                    "session_id": "claude-1",
                    "type": "message",
                    "role": "user",
                    "content": "Help me debug the auth flow",
                    "timestamp": "2026-06-03T00:00:01Z"
                }),
                0,
                1,
            ),
        ];

        assert_eq!(
            session_title("claude_code", "claude-1", &lines, &native_titles).as_deref(),
            Some("Help me debug the auth flow")
        );
    }

    #[test]
    fn pi_session_title_skips_instruction_text_for_user_message() {
        let native_titles = NativeTitleIndex::default();
        let lines = vec![
            parsed_line(
                0,
                json!({
                    "type": "session_info",
                    "name": "You are a helpful coding assistant with tool access. Follow the project guidelines carefully."
                }),
                0,
                1,
            ),
            parsed_line(
                1,
                json!({
                    "type": "message",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "first real pi prompt"}]
                    }
                }),
                0,
                1,
            ),
        ];

        assert_eq!(
            session_title("pi_agent", "pi-session-1", &lines, &native_titles).as_deref(),
            Some("first real pi prompt")
        );
    }

    #[test]
    fn claude_session_title_skips_system_role_line() {
        let native_titles = NativeTitleIndex::default();
        let lines = vec![
            parsed_line(
                0,
                json!({
                    "session_id": "claude-sys",
                    "type": "system",
                    "role": "system",
                    "content": "System prompt: you must follow all project policies.",
                    "timestamp": "2026-06-03T00:00:00Z"
                }),
                0,
                1,
            ),
            parsed_line(
                1,
                json!({
                    "session_id": "claude-sys",
                    "type": "message",
                    "role": "user",
                    "content": "Refactor the config loader",
                    "timestamp": "2026-06-03T00:00:01Z"
                }),
                0,
                1,
            ),
        ];

        assert_eq!(
            session_title("claude_code", "claude-sys", &lines, &native_titles).as_deref(),
            Some("Refactor the config loader")
        );
    }

    #[test]
    fn instruction_native_title_is_rejected_for_fallback() {
        let mut native_titles = NativeTitleIndex::default();
        native_titles.insert(
            "claude_code",
            "claude-bad-native",
            "You are a helpful assistant. Follow these instructions.",
        );
        let lines = vec![parsed_line(
            0,
            json!({
                "session_id": "claude-bad-native",
                "type": "message",
                "role": "user",
                "content": "actual user question",
                "timestamp": "2026-06-03T00:00:00Z"
            }),
            0,
            1,
        )];

        assert_eq!(
            session_title("claude_code", "claude-bad-native", &lines, &native_titles)
                .as_deref(),
            Some("actual user question")
        );
    }

    #[test]
    fn clean_native_title_is_preserved() {
        let mut native_titles = NativeTitleIndex::default();
        native_titles.insert("claude_code", "claude-clean", "Clean Claude Title");
        let lines = vec![parsed_line(
            0,
            json!({
                "session_id": "claude-clean",
                "type": "user",
                "role": "user",
                "content": "fallback user request",
                "timestamp": "2026-06-03T00:00:00Z"
            }),
            0,
            1,
        )];

        assert_eq!(
            session_title("claude_code", "claude-clean", &lines, &native_titles).as_deref(),
            Some("Clean Claude Title")
        );
    }

    #[test]
    fn falls_back_to_claude_project_directory() {
        let source_path = Path::new(
            "/home/example/.claude/projects/-home-example-workspace-project-alpha/session.jsonl",
        );

        let workspace = workspace_from_source_path("claude_code", source_path).expect("workspace");

        assert_eq!(
            workspace.workspace_path,
            "/home/example/workspace/project/alpha"
        );
        assert_eq!(workspace.cwd, None);
        assert_eq!(workspace.source, "source_path.claude_project_dir");
        assert_eq!(workspace.confidence, "inferred");
    }

    #[test]
    fn ingests_opencode_sqlite_sessions_with_native_titles_and_text_parts() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let db_path = temp.path().join("opencode.db");
        let conn = Connection::open(&db_path).expect("open fixture db");
        conn.execute_batch(
            r#"
            CREATE TABLE session (
              id text PRIMARY KEY,
              project_id text NOT NULL,
              parent_id text,
              slug text NOT NULL,
              directory text NOT NULL,
              title text NOT NULL,
              version text NOT NULL,
              time_created integer NOT NULL,
              time_updated integer NOT NULL,
              time_archived integer
            );
            CREATE TABLE message (
              id text PRIMARY KEY,
              session_id text NOT NULL,
              time_created integer NOT NULL,
              time_updated integer NOT NULL,
              data text NOT NULL
            );
            CREATE TABLE part (
              id text PRIMARY KEY,
              message_id text NOT NULL,
              session_id text NOT NULL,
              time_created integer NOT NULL,
              time_updated integer NOT NULL,
              data text NOT NULL
            );
            "#,
        )
        .expect("create schema");
        conn.execute(
            "INSERT INTO session
             (id, project_id, parent_id, slug, directory, title, version, time_created, time_updated, time_archived)
             VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
            (
                "ses_fixture",
                "project_fixture",
                "native-title-slug",
                temp.path().to_string_lossy().as_ref(),
                "Native OpenCode Title",
                "1.2.3",
                1_775_640_000_000i64,
                1_775_640_002_000i64,
            ),
        )
        .expect("insert session");
        for (message_id, role, time) in [
            ("msg_user", "user", 1_775_640_000_100i64),
            ("msg_assistant", "assistant", 1_775_640_001_000i64),
        ] {
            let data = if role == "assistant" {
                json!({
                    "role": role,
                    "time": {"created": time},
                    "modelID": "gpt-5.4",
                    "providerID": "openai",
                    "tokens": {
                        "input": 120,
                        "output": 30,
                        "reasoning": 10,
                        "cache": {"read": 40, "write": 5}
                    }
                })
            } else {
                json!({"role": role, "time": {"created": time}})
            };
            conn.execute(
                "INSERT INTO message (id, session_id, time_created, time_updated, data)
                 VALUES (?1, 'ses_fixture', ?2, ?2, ?3)",
                (
                    message_id,
                    time,
                    data.to_string(),
                ),
            )
            .expect("insert message");
        }
        for (part_id, message_id, time, data) in [
            (
                "part_user",
                "msg_user",
                1_775_640_000_200i64,
                json!({"type": "text", "text": "user asks for OpenCode ingestion"}),
            ),
            (
                "part_assistant",
                "msg_assistant",
                1_775_640_001_200i64,
                json!({"type": "text", "text": "assistant explains the OpenCode parser"}),
            ),
            (
                "part_tool",
                "msg_assistant",
                1_775_640_001_300i64,
                json!({"type": "tool", "tool": "bash", "state": {"output": "do not index"}}),
            ),
        ] {
            conn.execute(
                "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
                 VALUES (?1, ?2, 'ses_fixture', ?3, ?3, ?4)",
                (part_id, message_id, time, data.to_string()),
            )
            .expect("insert part");
        }
        drop(conn);

        let native_titles = NativeTitleIndex::default();
        let context = SourceSyncContext::new(&store);
        let imported = prepare_file_import(
            &context,
            "machine_fixture",
            "opencode",
            &db_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("ingest opencode");
        let path_text = db_path.to_string_lossy().to_string();
        let session_id = stable_id(&["session", "opencode", &path_text, "ses_fixture"]);
        let session = store
            .session_by_id(&session_id)
            .expect("load session")
            .expect("session");
        let events = store.events_for_session(&session_id).expect("load events");

        assert_eq!(imported.inserted, 3);
        assert_eq!(session.title.as_deref(), Some("Native OpenCode Title"));
        assert_eq!(session.source_kind, "opencode");
        assert_eq!(session.metadata["parser"], "opencode_sqlite_v1");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].role.as_deref(), Some("user"));
        assert_eq!(events[0].content, "user asks for OpenCode ingestion");
        assert_eq!(events[1].role.as_deref(), Some("assistant"));
        assert_eq!(events[1].content, "assistant explains the OpenCode parser");
        assert_eq!(events[1].metadata["opencode_model_id"], "gpt-5.4");
        assert_eq!(events[1].metadata["opencode_provider_id"], "openai");
        assert_eq!(events[1].metadata["opencode_tokens"]["input"], 120);
        assert_eq!(events[1].metadata["opencode_tokens"]["cache"]["read"], 40);
    }

    #[test]
    fn backfills_opencode_usage_without_changing_event_identity_or_checkpoint() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let db_path = temp.path().join("opencode.db");
        let conn = Connection::open(&db_path).expect("open fixture db");
        conn.execute_batch(
            r#"
            CREATE TABLE message (
              id text PRIMARY KEY,
              session_id text NOT NULL,
              data text NOT NULL
            );
            CREATE TABLE part (
              id text PRIMARY KEY,
              message_id text NOT NULL,
              data text NOT NULL
            );
            "#,
        )
        .expect("create usage schema");
        conn.execute(
            "INSERT INTO message (id, session_id, data) VALUES ('msg_old', 'ses_old', ?1)",
            [json!({
                "role": "assistant",
                "modelID": "claude-opus-4-6",
                "providerID": "anthropic",
                "tokens": {
                    "input": 90,
                    "output": 20,
                    "reasoning": 4,
                    "cache": {"read": 30, "write": 2}
                }
            })
            .to_string()],
        )
        .expect("insert assistant message");
        conn.execute(
            "INSERT INTO part (id, message_id, data) VALUES ('part_old', 'msg_old', ?1)",
            [json!({"type": "text", "text": "legacy assistant answer"}).to_string()],
        )
        .expect("insert assistant part");
        drop(conn);

        let event = EventRecord {
            id: "event_old".to_string(),
            session_id: "session_old".to_string(),
            source_id: "source_old".to_string(),
            machine_id: "machine_old".to_string(),
            source_kind: "opencode".to_string(),
            ordinal: 0,
            event_type: "text".to_string(),
            role: Some("assistant".to_string()),
            content: "legacy assistant answer".to_string(),
            raw_artifact_hash: None,
            occurred_at: None,
            metadata: json!({
                "opencode_message_id": "msg_old",
                "opencode_part_id": "part_old"
            }),
            hash: "event_hash_old".to_string(),
        };
        let now = Utc::now();
        store
            .import_records(&[
                ArchiveRecord::Source(crate::archive::SourceRecord {
                    id: "source_old".to_string(),
                    kind: "opencode".to_string(),
                    identity: db_path.to_string_lossy().to_string(),
                    path: Some(db_path.to_string_lossy().to_string()),
                    first_seen_at: now,
                    updated_at: now,
                    hash: "source_hash_old".to_string(),
                }),
                ArchiveRecord::Event(event.clone()),
            ])
            .expect("insert legacy event");
        let identity = db_path.to_string_lossy().to_string();
        store
            .upsert_source_checkpoint(
                "opencode",
                &identity,
                Some("fixture-checkpoint"),
                &json!({"kept": true}),
            )
            .expect("store checkpoint");

        let first = backfill_opencode_usage(&store, &db_path).expect("backfill usage");
        assert_eq!(first.scanned, 1);
        assert_eq!(first.updated, 1);
        let enriched = store
            .events_for_session("session_old")
            .expect("load enriched event")
            .pop()
            .expect("enriched event");
        assert_eq!(enriched.id, event.id);
        assert_eq!(enriched.hash, event.hash);
        assert_eq!(enriched.content, event.content);
        assert_eq!(enriched.metadata["opencode_model_id"], "claude-opus-4-6");
        assert_eq!(enriched.metadata["opencode_provider_id"], "anthropic");
        assert_eq!(enriched.metadata["opencode_tokens"]["output"], 20);
        assert_eq!(
            store
                .source_checkpoint("opencode", &identity)
                .expect("load checkpoint")
                .and_then(|checkpoint| checkpoint.cursor),
            Some("fixture-checkpoint".to_string())
        );

        let second = backfill_opencode_usage(&store, &db_path).expect("rerun backfill");
        assert_eq!(second.scanned, 1);
        assert_eq!(second.updated, 0);
    }

    #[test]
    fn opencode_adapter_skips_when_sqlite_checkpoint_matches() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let db_path = temp.path().join("opencode.db");
        let wal_path = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));
        let shm_path = PathBuf::from(format!("{}-shm", db_path.to_string_lossy()));
        fs::write(&db_path, b"db").expect("write db");
        fs::write(&wal_path, b"wal").expect("write wal");
        fs::write(&shm_path, b"shm").expect("write shm");
        let identity = db_path.to_string_lossy().to_string();
        let cursor = opencode_checkpoint_cursor(&db_path).expect("checkpoint cursor");
        store
            .upsert_source_checkpoint("opencode", &identity, Some(&cursor), &json!({}))
            .expect("store checkpoint");
        let adapter = LocalTranscriptAdapter {
            kind: "opencode",
            roots: Vec::new(),
            native_titles: NativeTitleIndex::default(),
        };
        let candidate = SourceCandidate {
            adapter_kind: "opencode",
            kind: "opencode".to_string(),
            identity,
            path: Some(db_path.clone()),
            modified: 0,
            size: None,
            mtime_ms: None,
        };
        let context = SourceSyncContext::new(&store);

        assert!(adapter
            .is_current(&context, &candidate)
            .expect("matching checkpoint"));

        fs::write(&wal_path, b"wal changed").expect("change wal");
        assert!(!adapter
            .is_current(&context, &candidate)
            .expect("changed checkpoint"));
    }

    #[test]
    fn local_transcript_adapter_uses_cached_source_file_status() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let identity = temp.path().join("cached-status.json");
        let identity = identity.to_string_lossy().to_string();
        let adapter = LocalTranscriptAdapter {
            kind: "hermes",
            roots: Vec::new(),
            native_titles: NativeTitleIndex::default(),
        };
        let candidate = SourceCandidate {
            adapter_kind: "hermes",
            kind: "hermes".to_string(),
            identity: identity.clone(),
            path: Some(PathBuf::from(&identity)),
            modified: 0,
            size: Some(123),
            mtime_ms: Some(456),
        };
        let mut statuses = HashMap::new();
        statuses.insert(
            identity,
            SourceFileStatus {
                raw_current: true,
                needs_workspace_refresh: false,
            },
        );
        let context = SourceSyncContext::new(&store).with_source_file_statuses(&statuses);

        assert!(adapter
            .is_current(&context, &candidate)
            .expect("cached source status"));
    }

    #[test]
    fn session_metadata_includes_flat_and_nested_workspace_fields() {
        let workspace = WorkspaceIdentity {
            workspace_path: "/repo".to_string(),
            workspace_root: "/repo".to_string(),
            cwd: Some("/repo/subdir".to_string()),
            git_repo: Some("git@example.com:example/repo.git".to_string()),
            git_branch: Some("main".to_string()),
            source: "payload.cwd".to_string(),
            confidence: "direct".to_string(),
        };

        let metadata = session_metadata("/logs/session.jsonl", Some(&workspace));

        assert_eq!(metadata["workspace_path"], "/repo");
        assert_eq!(metadata["workspace_root"], "/repo");
        assert_eq!(metadata["cwd"], "/repo/subdir");
        assert_eq!(metadata["git_branch"], "main");
        assert_eq!(metadata["workspace"]["path"], "/repo");
        assert_eq!(metadata["workspace"]["source"], "payload.cwd");
    }

    #[test]
    fn append_like_ingest_reports_small_deltas_for_changed_file() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let log_path = temp.path().join("session.jsonl");
        let first_log = fixture_line("session-1", "first question");
        let first_hash = blake3_hex(first_log.as_bytes());
        fs::write(&log_path, &first_log).expect("write first log");

        let native_titles = NativeTitleIndex::default();
        let context = SourceSyncContext::new(&store);
        let first = prepare_file_import(
            &context,
            "machine_fixture",
            "codex",
            &log_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("first ingest");

        assert_eq!(first.inserted, 2);
        assert_eq!(first.duplicates, 0);
        assert_eq!(first.delta.inserted_events.len(), 1);
        assert_eq!(first.delta.touched_sessions.len(), 1);

        let second_log = fixture_line("session-1", "second question");
        fs::write(&log_path, format!("{}{}", first_log, second_log))
            .expect("append second log line");
        let context = SourceSyncContext::new(&store);
        let second = prepare_file_import(
            &context,
            "machine_fixture",
            "codex",
            &log_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("second ingest");
        let stats = store.stats().expect("stats");
        let path_text = log_path.to_string_lossy().to_string();
        let session_id = stable_id(&["session", "codex", &path_text, "session-1"]);
        let events = store.events_for_session(&session_id).expect("events");

        assert_eq!(second.inserted, 1);
        assert_eq!(second.duplicates, 2);
        assert_eq!(second.delta.inserted_events.len(), 1);
        assert_eq!(second.delta.touched_events.len(), 1);
        assert_eq!(second.delta.touched_sessions.len(), 1);
        assert!(second.delta.touched_paths.is_empty());
        assert_eq!(stats.raw_artifacts, 0);
        assert_eq!(stats.sessions, 1);
        assert_eq!(stats.events, 2);
        assert_eq!(raw_object_count(&store), 0);
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.raw_artifact_hash.is_none()));
        assert_eq!(
            events[0].metadata["source_file_hash"].as_str(),
            Some(first_hash.as_str())
        );
        assert!(events[0].metadata["raw_manifest_hash"].is_null());
        assert!(events[0].metadata["raw_object_hash"].is_null());
        assert_eq!(events[0].metadata["byte_offset"].as_u64(), Some(0));
        assert_eq!(
            events[0].metadata["byte_len"].as_u64(),
            Some(first_log.len() as u64)
        );
        let session = store
            .session_by_id(&session_id)
            .expect("session lookup")
            .expect("session exists");
        let rendered = crate::transcript::render_session(
            &session,
            &events,
            None,
            &crate::transcript::ViewMetadata::default(),
            false,
        );
        assert!(rendered.contains("first question"));
        assert!(rendered.contains("second question"));
    }

    #[test]
    fn non_prefix_rewrite_keeps_prior_manifest_objects() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let log_path = temp.path().join("session.jsonl");
        let first_log = fixture_line("session-1", "first question");
        fs::write(&log_path, &first_log).expect("write first log");

        let native_titles = NativeTitleIndex::default();
        let context = SourceSyncContext::new(&store);
        prepare_file_import(
            &context,
            "machine_fixture",
            "codex",
            &log_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("first ingest");

        let rewritten_log = format!(
            "{}{}",
            fixture_line("session-1", "rewritten first question"),
            fixture_line("session-1", "second question")
        );
        fs::write(&log_path, &rewritten_log).expect("rewrite log");
        let context = SourceSyncContext::new(&store);
        prepare_file_import(
            &context,
            "machine_fixture",
            "codex",
            &log_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("second ingest");
        let stats = store.stats().expect("stats");
        let path_text = log_path.to_string_lossy().to_string();
        let session_id = stable_id(&["session", "codex", &path_text, "session-1"]);
        let events = store.events_for_session(&session_id).expect("events");

        assert_eq!(stats.raw_artifacts, 0);
        assert_eq!(stats.sessions, 1);
        assert_eq!(stats.events, 2);
        assert_eq!(raw_object_count(&store), 0);
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.raw_artifact_hash.is_none()));
        assert!(events.iter().any(|event| event.content == "first question"));
        assert!(events
            .iter()
            .any(|event| event.content == "second question"));
    }

    #[test]
    fn forked_jsonl_copy_reuses_prefix_raw_object() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let first_path = temp.path().join("first.jsonl");
        let fork_path = temp.path().join("fork.jsonl");
        let shared = fixture_line("session-1", "shared question");
        let first_log = format!("{}{}", shared, fixture_line("session-1", "first tail"));
        let fork_log = format!("{}{}", shared, fixture_line("session-1", "fork tail"));
        fs::write(&first_path, &first_log).expect("write first log");
        fs::write(&fork_path, &fork_log).expect("write fork log");

        let native_titles = NativeTitleIndex::default();
        let context = SourceSyncContext::new(&store);
        prepare_file_import(
            &context,
            "machine_fixture",
            "codex",
            &first_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("first ingest");
        let context = SourceSyncContext::new(&store);
        prepare_file_import(
            &context,
            "machine_fixture",
            "codex",
            &fork_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("fork ingest");

        let stats = store.stats().expect("stats");
        let first_path_text = first_path.to_string_lossy().to_string();
        let fork_path_text = fork_path.to_string_lossy().to_string();
        let first_session_id = stable_id(&["session", "codex", &first_path_text, "session-1"]);
        let fork_session_id = stable_id(&["session", "codex", &fork_path_text, "session-1"]);
        let first_events = store
            .events_for_session(&first_session_id)
            .expect("first events");
        let fork_events = store
            .events_for_session(&fork_session_id)
            .expect("fork events");

        assert_eq!(stats.raw_artifacts, 0);
        assert_eq!(raw_object_count(&store), 0);
        assert_eq!(first_events.len(), 2);
        assert_eq!(fork_events.len(), 2);
        assert!(first_events
            .iter()
            .all(|event| event.raw_artifact_hash.is_none()));
        assert!(fork_events
            .iter()
            .all(|event| event.raw_artifact_hash.is_none()));
        assert!(fork_events
            .iter()
            .any(|event| event.content == "shared question"));
        assert!(fork_events.iter().any(|event| event.content == "fork tail"));
    }

    #[test]
    fn non_jsonl_import_export_preserves_normalized_event_content() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let log_path = temp.path().join("session.json");
        let raw_json = json!({
            "session_id": "session-json",
            "type": "message",
            "role": "user",
            "content": "json array fallback stays whole-file",
            "timestamp": "2026-06-03T00:00:00Z"
        })
        .to_string();
        fs::write(&log_path, &raw_json).expect("write json log");
        let native_titles = NativeTitleIndex::default();
        let context = SourceSyncContext::new(&store);

        prepare_file_import(
            &context,
            "machine_fixture",
            "hermes",
            &log_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("ingest json");

        assert_eq!(store.stats().expect("stats").raw_artifacts, 0);
        assert!(store
            .latest_raw_manifest_for_path("hermes", &log_path.to_string_lossy())
            .expect("manifest lookup")
            .is_none());
        let path_text = log_path.to_string_lossy().to_string();
        let session_id = stable_id(&["session", "hermes", &path_text, "session-json"]);
        let events = store.events_for_session(&session_id).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].content, "json array fallback stays whole-file");
        assert!(events[0].raw_artifact_hash.is_none());

        let exported = store.export_records().expect("export records");
        assert!(!exported
            .iter()
            .any(|record| matches!(record, ArchiveRecord::RawArtifact(_))));

        let imported_dir = tempfile::tempdir().expect("import temp dir");
        let imported = Store::open(imported_dir.path()).expect("open import store");
        imported
            .import_records(&exported)
            .expect("import exported records");
        let imported_events = imported
            .events_for_session(&session_id)
            .expect("imported events");
        assert_eq!(imported_events.len(), 1);
        assert_eq!(
            imported_events[0].content,
            "json array fallback stays whole-file"
        );
    }

    #[test]
    fn pi_tool_heavy_events_store_compact_summaries_and_project_conversation() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let log_path = temp.path().join("pi-session.jsonl");
        let lines = [
            json!({
                "session_id": "session-pi",
                "type": "message",
                "role": "user",
                "content": "please inspect the failing deploy",
                "timestamp": "2026-06-03T00:00:00Z"
            }),
            json!({
                "session_id": "session-pi",
                "type": "message",
                "role": "assistant",
                "content": "I will check the deploy logs.",
                "timestamp": "2026-06-03T00:00:01Z"
            }),
            json!({
                "session_id": "session-pi",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "thinking", "text": "large private reasoning payload"}],
                "timestamp": "2026-06-03T00:00:02Z"
            }),
            json!({
                "session_id": "session-pi",
                "type": "toolResult",
                "content": "very large tool output that should stay out of projection",
                "timestamp": "2026-06-03T00:00:03Z"
            }),
            json!({
                "session_id": "session-pi",
                "type": "compaction",
                "content": "large compaction payload that should stay out of projection",
                "timestamp": "2026-06-03T00:00:04Z"
            }),
        ]
        .into_iter()
        .map(|value| format!("{value}\n"))
        .collect::<String>();
        fs::write(&log_path, lines).expect("write pi fixture");
        let native_titles = NativeTitleIndex::default();
        let context = SourceSyncContext::new(&store);

        prepare_file_import(
            &context,
            "machine_fixture",
            "pi_agent",
            &log_path,
            &native_titles,
        )
        .and_then(|prepared| prepared.commit(&store))
        .expect("ingest pi fixture");
        store
            .refresh_history_items()
            .expect("refresh history items");

        let path_text = log_path.to_string_lossy().to_string();
        let session_id = stable_id(&["session", "pi_agent", &path_text, "session-pi"]);
        let events = store.events_for_session(&session_id).expect("events");

        assert_eq!(events.len(), 5);
        assert_eq!(events[0].content, "please inspect the failing deploy");
        assert_eq!(events[1].content, "I will check the deploy logs.");
        assert!(events[2]
            .content
            .contains("large private reasoning payload"));
        assert!(events[3].content.contains("very large tool output"));
        assert!(events[4].content.contains("large compaction payload"));
        for event in &events[2..] {
            assert!(event.metadata["content_compacted"].is_null());
            assert!(event.metadata["raw_object_hash"].is_null());
            let items = store
                .history_items_for_event(&event.id)
                .expect("history items");
            assert!(items.iter().any(|item| item.tier == "raw"));
        }
        assert!(store
            .history_items_for_event(&events[3].id)
            .expect("tool result history")
            .iter()
            .any(|item| item.tier == "tool" && item.kind == "tool_result"));

        assert_eq!(
            tier_kinds(
                &store
                    .history_items_for_event(&events[0].id)
                    .expect("user history")
            ),
            vec![("conversation", "user"), ("raw", "user")]
        );
        assert_eq!(
            tier_kinds(
                &store
                    .history_items_for_event(&events[1].id)
                    .expect("assistant history")
            ),
            vec![("conversation", "assistant"), ("raw", "assistant")]
        );
    }

    #[test]
    fn source_summaries_track_found_and_selected_files_by_kind() {
        let mut summaries = Vec::new();
        push_found_source_file(&mut summaries, "codex");
        push_found_source_file(&mut summaries, "hermes");
        push_found_source_file(&mut summaries, "codex");
        let candidates = vec![
            SourceCandidate {
                adapter_kind: "codex",
                modified: 3,
                kind: "codex".to_string(),
                identity: "codex-1.jsonl".to_string(),
                path: Some(PathBuf::from("codex-1.jsonl")),
                size: None,
                mtime_ms: None,
            },
            SourceCandidate {
                adapter_kind: "hermes",
                modified: 2,
                kind: "hermes".to_string(),
                identity: "hermes-1.json".to_string(),
                path: Some(PathBuf::from("hermes-1.json")),
                size: None,
                mtime_ms: None,
            },
        ];

        mark_selected_source_files(&mut summaries, &candidates);

        let codex = summaries
            .iter()
            .find(|summary| summary.kind == "codex")
            .expect("codex summary");
        let hermes = summaries
            .iter()
            .find(|summary| summary.kind == "hermes")
            .expect("hermes summary");
        assert_eq!(codex.found_files, 2);
        assert_eq!(codex.selected_files, 1);
        assert_eq!(hermes.found_files, 1);
        assert_eq!(hermes.selected_files, 1);
    }

    #[test]
    fn source_selection_matches_specific_sources_and_agent_logs_alias() {
        let codex = SourceSelection::single("codex").expect("codex selection");
        let agent_logs = SourceSelection::single("agent_logs").expect("agent logs selection");
        let treechat = SourceSelection::single("treechat").expect("treechat selection");

        assert!(codex.matches_candidate("codex", "codex"));
        assert!(agent_logs.matches_candidate("codex", "codex"));
        assert!(agent_logs.matches_candidate("opencode", "opencode"));
        assert!(treechat.matches_candidate("treechat", "treechat"));
        assert!(!agent_logs.matches_candidate("treechat", "treechat"));
        assert!(!codex.matches_candidate("hermes", "hermes"));
    }

    #[test]
    fn source_selection_accepts_repeated_and_comma_separated_sources() {
        let selection =
            SourceSelection::parse(["codex, claude_code".to_string(), "treechat".to_string()])
                .expect("selection");

        assert!(selection.includes_adapter("codex"));
        assert!(selection.includes_adapter("claude_code"));
        assert!(selection.includes_adapter("treechat"));
        assert!(!selection.includes_adapter("hermes"));
    }

    #[test]
    fn source_selection_skips_treechat_discovery_for_other_sources() {
        let mut sources = SourceConfigs::default();
        sources.treechat.enabled = true;
        let options = UpdateOptions {
            max_files: None,
            source_selection: SourceSelection::single("codex").expect("selection"),
            sources,
        };

        let registry = built_in_source_adapters(&options).expect("registry");
        let kinds = registry
            .iter()
            .map(|adapter| adapter.kind())
            .collect::<Vec<_>>();

        assert_eq!(kinds, vec!["codex"]);
    }

    #[test]
    fn local_transcript_adapter_discovers_candidates_with_source_adapter_kind() {
        let temp = tempfile::tempdir().expect("temp dir");
        let log_path = temp.path().join("session.jsonl");
        fs::write(&log_path, fixture_line("session-1", "hello from codex")).expect("write fixture");
        let adapter = LocalTranscriptAdapter {
            kind: "codex",
            roots: vec![SourceRoot {
                path: temp.path().to_path_buf(),
                extensions: &["jsonl"],
            }],
            native_titles: NativeTitleIndex::default(),
        };

        let candidates = adapter.discover().expect("discover candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].adapter_kind, "codex");
        assert_eq!(candidates[0].kind, "codex");
        assert_eq!(candidates[0].path.as_deref(), Some(log_path.as_path()));
    }

    fn fixture_line(session_id: &str, text: &str) -> String {
        format!(
            "{}\n",
            json!({
                "session_id": session_id,
                "type": "message",
                "role": "user",
                "content": text,
                "timestamp": "2026-06-03T00:00:00Z"
            })
        )
    }

    fn raw_object_count(store: &Store) -> i64 {
        store
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM raw_objects", [], |row| row.get(0))
                    .map_err(Into::into)
            })
            .expect("raw object count")
    }

    fn tier_kinds(items: &[crate::storage::HistoryItemRecord]) -> Vec<(&str, &str)> {
        items
            .iter()
            .map(|item| (item.tier.as_str(), item.kind.as_str()))
            .collect()
    }
}
