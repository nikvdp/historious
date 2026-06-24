use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, RawArtifact, SessionRecord,
};
use crate::config::SourceConfigs;
use crate::source::{
    AdapterConcurrency, PreparedImport, SearchSegment, SemanticPolicy, SourceAdapter,
    SourceAdapterRegistry, SourceCandidate, SourceCheckpointUpsert, SourceSyncContext,
    SourceUpsert,
};
use crate::storage::{ImportDelta, SourceFileFingerprint, SourceFileStatus, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags};
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
pub enum UpdateProgress {
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

#[derive(Debug, Clone)]
struct AppendImportPlan {
    previous_size: usize,
    external_session_id: String,
    next_ordinal: i64,
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
            file_index: idx + 1,
            total_files,
            source_file_index,
            source_file_count,
        });
    }

    let prepared_imports = prepare_pending_imports(
        &registry,
        &context,
        machine_id,
        pending_imports,
        &should_cancel,
    )?;
    for prepared in prepared_imports {
        if should_cancel() {
            return Ok(stats);
        }
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
        progress(&UpdateProgress::CompletedFile {
            adapter_kind: prepared.adapter_kind.to_string(),
            kind: prepared.kind,
            path: prepared.path,
            file_index: prepared.file_index,
            total_files: prepared.total_files,
            source_file_index: prepared.source_file_index,
            source_file_count: prepared.source_file_count,
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
        .filter(|candidate| {
            is_local_transcript_kind(&candidate.kind) && candidate.kind.as_str() != "opencode"
        })
        .filter_map(|candidate| {
            Some(SourceFileFingerprint {
                path: candidate.identity.clone(),
                size: candidate.size?,
                mtime_ms: candidate.mtime_ms,
            })
        })
        .collect::<Vec<_>>();
    store.source_file_statuses(&fingerprints)
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
    file_index: usize,
    total_files: usize,
    source_file_index: usize,
    source_file_count: usize,
}

struct PreparedPendingImport {
    order: usize,
    adapter_kind: &'static str,
    kind: String,
    path: PathBuf,
    file_index: usize,
    total_files: usize,
    source_file_index: usize,
    source_file_count: usize,
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
                file_index: pending.file_index,
                total_files: pending.total_files,
                source_file_index: pending.source_file_index,
                source_file_count: pending.source_file_count,
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
                            file_index: pending.file_index,
                            total_files: pending.total_files,
                            source_file_index: pending.source_file_index,
                            source_file_count: pending.source_file_count,
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
                    file_index: fallback.file_index,
                    total_files: fallback.total_files,
                    source_file_index: fallback.source_file_index,
                    source_file_count: fallback.source_file_count,
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
        if self.kind() == "opencode" {
            let path = candidate
                .path
                .as_deref()
                .with_context(|| format!("source candidate {} has no path", candidate.identity))?;
            let cursor = opencode_checkpoint_cursor(path)?;
            let checkpoint = context
                .store
                .source_checkpoint(self.kind(), &candidate.identity)?;
            return Ok(checkpoint
                .and_then(|checkpoint| checkpoint.cursor)
                .is_some_and(|stored| stored == cursor));
        }
        let size = candidate.size.unwrap_or(0);
        let status = context.source_file_status(&candidate.identity, size, candidate.mtime_ms)?;
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
        if self.kind() == "opencode" {
            return Ok(prepared.with_checkpoint(SourceCheckpointUpsert {
                source_kind: self.kind().to_string(),
                source_identity: candidate.identity.clone(),
                cursor: Some(opencode_checkpoint_cursor(path)?),
                metadata: json!({
                    "strategy": "sqlite_file_trio_metadata_v1",
                    "path": candidate.identity,
                }),
            }));
        }
        Ok(prepared)
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

fn prepare_file_import(
    context: &SourceSyncContext<'_>,
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

    let metadata =
        fs::metadata(path).with_context(|| format!("reading metadata {}", path.display()))?;
    let mtime_ms = file_mtime_ms(&metadata);
    let mut records = Vec::new();
    let raw_artifact = RawArtifact {
        hash: raw_hash.clone(),
        source_id: source_id.clone(),
        path: path_text.clone(),
        size: bytes.len() as u64,
        mtime_ms,
        media_type: media_type(path),
        content: bytes.clone(),
        first_seen_at: Utc::now(),
    };
    records.push(ArchiveRecord::RawArtifact(raw_artifact.clone()));

    if kind == "opencode" {
        return prepare_opencode_db_import(
            machine_id,
            path,
            &path_text,
            &source_id,
            source_upsert,
            raw_artifact,
        );
    }

    let append_plan = append_import_plan(context, kind, path, &path_text, &bytes)?;
    let lines = if let Some(plan) = &append_plan {
        let suffix = String::from_utf8_lossy(&bytes[plan.previous_size..]);
        parse_jsonl_with_start(suffix.as_ref(), plan.next_ordinal, plan.previous_size)
    } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
        let text = String::from_utf8_lossy(&bytes);
        parse_jsonl(&text)
    } else {
        let text = String::from_utf8_lossy(&bytes);
        parse_json_file(&text)
    };
    let lines = lines.with_context(|| format!("parsing {}", path.display()))?;
    let external_session_id = append_plan
        .as_ref()
        .map(|plan| plan.external_session_id.clone())
        .or_else(|| {
            lines
                .iter()
                .find_map(|line| line.external_session_id.clone())
        })
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
        let mut metadata = json!({
            "raw_artifact_hash": raw_hash.clone(),
            "byte_offset": byte_offset,
            "byte_len": byte_len,
            "capture_fidelity": "exact_local_log",
        })
        .as_object()
        .cloned()
        .unwrap_or_default();
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
            raw_artifact_hash: Some(raw_hash.clone()),
            occurred_at: line.occurred_at,
            metadata: Value::Object(metadata),
            hash: line_hash,
        }));
    }
    Ok(PreparedImport::archive(vec![source_upsert], records))
}

fn append_import_plan(
    context: &SourceSyncContext<'_>,
    kind: &str,
    path: &Path,
    path_text: &str,
    bytes: &[u8],
) -> Result<Option<AppendImportPlan>> {
    if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") || kind == "opencode" {
        return Ok(None);
    }

    let Some(previous) = context
        .store
        .latest_raw_artifact_summary_for_path(path_text)?
    else {
        return Ok(None);
    };
    let Ok(previous_size) = usize::try_from(previous.size) else {
        return Ok(None);
    };
    if previous_size == 0 || previous_size >= bytes.len() {
        return Ok(None);
    }
    if bytes.get(previous_size.saturating_sub(1)) != Some(&b'\n') {
        return Ok(None);
    }
    if blake3_hex(&bytes[..previous_size]) != previous.hash {
        return Ok(None);
    }

    let Some(external_session_id) = context
        .store
        .latest_session_external_id_for_source_path(kind, path_text)?
    else {
        return Ok(None);
    };
    let session_id = stable_id(&["session", kind, path_text, &external_session_id]);
    let Some(max_ordinal) = context.store.max_event_ordinal_for_session(&session_id)? else {
        return Ok(None);
    };

    Ok(Some(AppendImportPlan {
        previous_size,
        external_session_id,
        next_ordinal: max_ordinal.saturating_add(1),
    }))
}

fn prepare_opencode_db_import(
    machine_id: &str,
    path: &Path,
    path_text: &str,
    source_id: &str,
    source_upsert: SourceUpsert,
    raw_artifact: RawArtifact,
) -> Result<PreparedImport> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening OpenCode database {}", path.display()))?;
    let mut records = vec![ArchiveRecord::RawArtifact(raw_artifact.clone())];

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
                "raw_artifact_hash": raw_artifact.hash.clone(),
                "capture_fidelity": "native_opencode_sqlite",
                "parser": "opencode_sqlite_v1",
                "opencode_session_id": external_session_id.clone(),
                "opencode_message_id": message_id.clone(),
                "opencode_part_id": part_id.clone(),
                "opencode_part_type": part_type.clone(),
            })
            .as_object()
            .cloned()
            .unwrap_or_default();
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
                raw_artifact_hash: Some(raw_artifact.hash.clone()),
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
}

fn fallback_session_title(lines: &[ParsedLine]) -> Option<String> {
    lines
        .iter()
        .find(|line| is_human_line(line) && title_candidate(&line.content).is_some())
        .or_else(|| {
            lines.iter().find(|line| {
                !is_bootstrap_content(&line.content) && title_candidate(&line.content).is_some()
            })
        })
        .and_then(|line| title_candidate(&line.content))
}

fn is_human_line(line: &ParsedLine) -> bool {
    line.role.as_deref() == Some("user") || line.event_type == "user"
}

fn title_candidate(content: &str) -> Option<String> {
    if is_bootstrap_content(content) {
        return None;
    }
    normalized_nonempty_title(content)
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
    let content = extract_text(&value).unwrap_or_else(|| value.to_string());
    let search = derive_search_segment(&value, role.as_deref(), &event_type);
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
        || kind == "bash"
        || map.contains_key("tool_use_id")
        || map.contains_key("toolUseResult")
        || map.contains_key("stdout")
        || map.contains_key("stderr")
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

fn extract_text(value: &Value) -> Option<String> {
    let mut parts = Vec::new();
    collect_text(value, &mut parts);
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn collect_text(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for key in [
                "text", "content", "output", "input", "command", "stdout", "stderr",
            ] {
                if let Some(child) = map.get(key) {
                    collect_text(child, parts);
                }
            }
            if let Some(message) = map.get("message") {
                collect_text(message, parts);
            }
            if let Some(payload) = map.get("payload") {
                collect_text(payload, parts);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text(item, parts);
            }
        }
        Value::String(text) => {
            let text = text.trim();
            if !text.is_empty() {
                parts.push(text.to_string());
            }
        }
        _ => {}
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

fn media_type(path: &Path) -> String {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("json") => "application/json".to_string(),
        Some("jsonl") => "application/jsonl".to_string(),
        _ => "application/octet-stream".to_string(),
    }
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
                    "content": "# AGENTS.md instructions for /tmp/repo\n<INSTRUCTIONS>..."
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
            conn.execute(
                "INSERT INTO message (id, session_id, time_created, time_updated, data)
                 VALUES (?1, 'ses_fixture', ?2, ?2, ?3)",
                (
                    message_id,
                    time,
                    json!({"role": role, "time": {"created": time}}).to_string(),
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

        assert_eq!(imported.inserted, 4);
        assert_eq!(session.title.as_deref(), Some("Native OpenCode Title"));
        assert_eq!(session.source_kind, "opencode");
        assert_eq!(session.metadata["parser"], "opencode_sqlite_v1");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].role.as_deref(), Some("user"));
        assert_eq!(events[0].content, "user asks for OpenCode ingestion");
        assert_eq!(events[1].role.as_deref(), Some("assistant"));
        assert_eq!(events[1].content, "assistant explains the OpenCode parser");
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
        let identity = temp.path().join("cached-status.jsonl");
        let identity = identity.to_string_lossy().to_string();
        let adapter = LocalTranscriptAdapter {
            kind: "codex",
            roots: Vec::new(),
            native_titles: NativeTitleIndex::default(),
        };
        let candidate = SourceCandidate {
            adapter_kind: "codex",
            kind: "codex".to_string(),
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
        let metadata = fs::metadata(&log_path).expect("first metadata");
        let current = store
            .raw_artifact_is_current(
                &log_path.to_string_lossy(),
                metadata.len(),
                file_mtime_ms(&metadata),
            )
            .expect("freshness check");

        assert_eq!(first.inserted, 3);
        assert_eq!(first.duplicates, 0);
        assert_eq!(first.delta.inserted_events.len(), 1);
        assert_eq!(first.delta.touched_sessions.len(), 1);
        assert!(current);

        fs::write(
            &log_path,
            format!(
                "{}{}",
                first_log,
                fixture_line("session-1", "second question")
            ),
        )
        .expect("append second log line");
        let full_log = fs::read(&log_path).expect("read full log");
        let full_hash = blake3_hex(&full_log);
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

        assert_eq!(second.inserted, 2);
        assert_eq!(second.duplicates, 1);
        assert_eq!(second.delta.inserted_events.len(), 1);
        assert_eq!(second.delta.touched_events.len(), 1);
        assert_eq!(second.delta.touched_sessions.len(), 1);
        assert_eq!(second.delta.touched_paths.len(), 1);
        assert_eq!(stats.raw_artifacts, 1);
        assert_eq!(stats.sessions, 1);
        assert_eq!(stats.events, 2);
        assert!(!store.raw_artifact_blob_exists(&first_hash));
        assert!(store.raw_artifact_blob_exists(&full_hash));
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|event| event.raw_artifact_hash.as_deref() == Some(full_hash.as_str())));
        assert_eq!(
            events[0].metadata["raw_artifact_hash"].as_str(),
            Some(full_hash.as_str())
        );
        assert_eq!(events[0].metadata["byte_offset"].as_u64(), Some(0));
        assert_eq!(
            events[0].metadata["byte_len"].as_u64(),
            Some(first_log.len() as u64)
        );
        let kept_raw = store
            .read_raw_artifact_blob(&full_hash)
            .expect("read kept raw");
        assert_eq!(&kept_raw[..first_log.len()], first_log.as_bytes());
    }

    #[test]
    fn non_prefix_rewrite_keeps_prior_raw_artifact() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = Store::open(temp.path()).expect("open store");
        let log_path = temp.path().join("session.jsonl");
        let first_log = fixture_line("session-1", "first question");
        let first_hash = blake3_hex(first_log.as_bytes());
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
        let rewritten_hash = blake3_hex(rewritten_log.as_bytes());
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

        assert_eq!(stats.raw_artifacts, 2);
        assert_eq!(stats.sessions, 1);
        assert_eq!(stats.events, 2);
        assert!(store.raw_artifact_blob_exists(&first_hash));
        assert!(store.raw_artifact_blob_exists(&rewritten_hash));
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
}
