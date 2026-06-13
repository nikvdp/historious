use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, RawArtifact, SessionRecord,
};
use crate::storage::{ImportDelta, ImportStats, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use serde_json::Map;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
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
    pub source: Option<String>,
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
        kind: String,
        path: PathBuf,
        file_index: usize,
        total_files: usize,
        source_file_index: usize,
        source_file_count: usize,
        stats: UpdateStats,
    },
    CompletedFile {
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
    kind: &'static str,
    path: PathBuf,
    extensions: &'static [&'static str],
}

#[derive(Debug, Clone)]
struct UpdateCandidate {
    modified: i128,
    kind: &'static str,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct ParsedLine {
    ordinal: i64,
    value: Value,
    byte_offset: usize,
    byte_len: usize,
    content: String,
    search_text: String,
    search_kind: String,
    search_indexable: bool,
    search_skip_reason: Option<String>,
    role: Option<String>,
    event_type: String,
    occurred_at: Option<DateTime<Utc>>,
    external_session_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SearchProjection {
    text: String,
    kind: String,
    indexable: bool,
    skip_reason: Option<String>,
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
    let mut stats = UpdateStats::default();
    let mut candidates = Vec::new();
    let mut source_summaries = Vec::new();
    for root in discover_roots() {
        if options
            .source
            .as_deref()
            .is_some_and(|source| source != root.kind)
        {
            continue;
        }
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
                    stats.errors += 1;
                    continue;
                }
            };
            if !entry.file_type().is_file() || !has_extension(entry.path(), root.extensions) {
                continue;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as i128)
                .unwrap_or(0);
            push_found_source_file(&mut source_summaries, root.kind);
            candidates.push(UpdateCandidate {
                modified,
                kind: root.kind,
                path: entry.path().to_path_buf(),
            });
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
    for (idx, candidate) in candidates.into_iter().enumerate() {
        let kind = candidate.kind;
        let path = candidate.path;
        stats.files_seen += 1;
        let source_file_index = increment_source_seen(&mut source_seen, kind);
        let source_file_count = source_summaries
            .iter()
            .find(|source| source.kind == kind)
            .map(|source| source.selected_files)
            .unwrap_or(0);
        progress(&UpdateProgress::Processing {
            kind: kind.to_string(),
            path: path.clone(),
            file_index: idx + 1,
            total_files,
            source_file_index,
            source_file_count,
            stats: stats.clone(),
        });
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::debug!("failed to read metadata for {}: {err}", path.display());
                stats.errors += 1;
                progress(&UpdateProgress::CompletedFile {
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
        };
        let size = metadata.len();
        let mtime_ms = file_mtime_ms(&metadata);
        let path_text = path.to_string_lossy().to_string();
        let file_status = store.source_file_status(&path_text, size, mtime_ms)?;
        if kind != "opencode" && file_status.raw_current && !file_status.needs_workspace_refresh {
            stats.skipped_unchanged += 1;
            progress(&UpdateProgress::CompletedFile {
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
        match ingest_file(store, machine_id, kind, &path, &native_titles) {
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
        progress(&UpdateProgress::CompletedFile {
            kind: kind.to_string(),
            path,
            file_index: idx + 1,
            total_files,
            source_file_index,
            source_file_count,
            stats: stats.clone(),
        });
    }
    Ok(stats)
}

#[derive(Debug, Clone)]
struct MutableSourceSummary {
    kind: &'static str,
    found_files: usize,
    selected_files: usize,
}

fn push_found_source_file(summaries: &mut Vec<MutableSourceSummary>, kind: &'static str) {
    if let Some(summary) = summaries.iter_mut().find(|summary| summary.kind == kind) {
        summary.found_files += 1;
    } else {
        summaries.push(MutableSourceSummary {
            kind,
            found_files: 1,
            selected_files: 0,
        });
    }
}

fn mark_selected_source_files(
    summaries: &mut [MutableSourceSummary],
    candidates: &[UpdateCandidate],
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

fn increment_source_seen(seen: &mut Vec<MutableSourceSummary>, kind: &'static str) -> usize {
    if let Some(summary) = seen.iter_mut().find(|summary| summary.kind == kind) {
        summary.found_files += 1;
        summary.found_files
    } else {
        seen.push(MutableSourceSummary {
            kind,
            found_files: 1,
            selected_files: 0,
        });
        1
    }
}

fn discover_roots() -> Vec<SourceRoot> {
    let mut roots = Vec::new();
    if let Some(home) = home_dir() {
        roots.push(SourceRoot {
            kind: "codex",
            path: home.join(".codex/sessions"),
            extensions: &["jsonl"],
        });
        roots.push(SourceRoot {
            kind: "codex",
            path: home.join(".codex/archived_sessions"),
            extensions: &["jsonl"],
        });
        roots.push(SourceRoot {
            kind: "claude_code",
            path: home.join(".claude/projects"),
            extensions: &["jsonl"],
        });
        roots.push(SourceRoot {
            kind: "pi_agent",
            path: home.join(".pi/agent/sessions"),
            extensions: &["jsonl"],
        });
        roots.push(SourceRoot {
            kind: "openclaw",
            path: home.join(".openclaw"),
            extensions: &["jsonl"],
        });
        roots.push(SourceRoot {
            kind: "hermes",
            path: home.join(".hermes/sessions"),
            extensions: &["json", "jsonl"],
        });
        roots.push(SourceRoot {
            kind: "opencode",
            path: home.join(".local/share/opencode/opencode.db"),
            extensions: &["db"],
        });
    }
    roots
}

fn ingest_file(
    store: &Store,
    machine_id: &str,
    kind: &str,
    path: &Path,
    native_titles: &NativeTitleIndex,
) -> Result<ImportStats> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let raw_hash = blake3_hex(&bytes);
    let path_text = path.to_string_lossy().to_string();
    let source_id = stable_id(&["source", kind, &path_text]);
    store.upsert_source(&source_id, kind, &path_text, Some(&path_text))?;

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
        return ingest_opencode_db(
            store,
            machine_id,
            path,
            &path_text,
            &source_id,
            raw_artifact,
        );
    }

    let text = String::from_utf8_lossy(&bytes);
    let lines = if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
        parse_jsonl(&text)
    } else {
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
            metadata: json!({
                "raw_artifact_hash": raw_hash.clone(),
                "byte_offset": byte_offset,
                "byte_len": byte_len,
                "capture_fidelity": "exact_local_log",
                "search_indexable": line.search_indexable,
                "search_kind": line.search_kind,
                "search_text": line.search_text,
                "search_skip_reason": line.search_skip_reason
            }),
            hash: line_hash,
        }));
    }
    store.import_archive_records(&records)
}

fn ingest_opencode_db(
    store: &Store,
    machine_id: &str,
    path: &Path,
    path_text: &str,
    source_id: &str,
    raw_artifact: RawArtifact,
) -> Result<ImportStats> {
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
                metadata: json!({
                    "raw_artifact_hash": raw_artifact.hash.clone(),
                    "capture_fidelity": "native_opencode_sqlite",
                    "parser": "opencode_sqlite_v1",
                    "opencode_session_id": external_session_id.clone(),
                    "opencode_message_id": message_id.clone(),
                    "opencode_part_id": part_id.clone(),
                    "opencode_part_type": part_type.clone(),
                    "search_indexable": true,
                    "search_kind": role.clone(),
                    "search_text": content.clone(),
                    "search_skip_reason": null
                }),
                hash: event_hash,
            }));
            ordinal += 1;
        }
    }

    store.import_archive_records(&records)
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
        .or_else(|| fallback_session_title(lines))
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
    content.starts_with("# AGENTS.md instructions for ")
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
    let mut out = Vec::new();
    let mut offset = 0usize;
    for (idx, raw_line) in text.split_inclusive('\n').enumerate() {
        let byte_len = raw_line.len();
        let raw_line = raw_line.trim();
        if raw_line.is_empty() {
            offset += byte_len;
            continue;
        }
        let value: Value = serde_json::from_str(raw_line)
            .with_context(|| format!("parsing JSONL line {}", idx + 1))?;
        out.push(parsed_line(idx as i64, value, offset, byte_len));
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
    let search = derive_search_projection(&value, role.as_deref(), &event_type);
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
        search_text: search.text,
        search_kind: search.kind,
        search_indexable: search.indexable,
        search_skip_reason: search.skip_reason,
        role,
        event_type,
        occurred_at,
        external_session_id,
    }
}

fn derive_search_projection(
    value: &Value,
    role: Option<&str>,
    event_type: &str,
) -> SearchProjection {
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
        Some("tool") => SearchProjection::skipped("tool event"),
        Some("system") | Some("developer") => SearchProjection::skipped("instruction event"),
        _ => {
            if looks_like_tool_event(value, event_type) {
                SearchProjection::skipped("tool event")
            } else {
                SearchProjection::skipped("non-message event")
            }
        }
    }
}

impl SearchProjection {
    fn skipped(reason: &str) -> Self {
        Self {
            text: String::new(),
            kind: "none".to_string(),
            indexable: false,
            skip_reason: Some(reason.to_string()),
        }
    }
}

fn projection_from_parts(parts: Vec<String>, kind: &str, empty_reason: &str) -> SearchProjection {
    let text = normalize_parts(parts);
    if text.is_empty() {
        SearchProjection::skipped(empty_reason)
    } else {
        SearchProjection {
            text,
            kind: kind.to_string(),
            indexable: true,
            skip_reason: None,
        }
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

        assert!(line.search_indexable);
        assert_eq!(line.search_kind, "user");
        assert_eq!(line.search_text, "search this exact request");
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

        assert!(!line.search_indexable);
        assert!(line.search_text.is_empty());
        assert_eq!(line.search_skip_reason.as_deref(), Some("tool event"));
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

        assert!(line.search_indexable);
        assert_eq!(line.search_kind, "conversation");
        assert_eq!(line.search_text, "human question\nassistant answer");
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
        let source_path =
            Path::new("/home/example/.claude/projects/-home-example-workspace-project-alpha/session.jsonl");

        let workspace = workspace_from_source_path("claude_code", source_path).expect("workspace");

        assert_eq!(workspace.workspace_path, "/home/example/workspace/project/alpha");
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
        let imported = ingest_file(
            &store,
            "machine_fixture",
            "opencode",
            &db_path,
            &native_titles,
        )
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
        fs::write(&log_path, fixture_line("session-1", "first question")).expect("write first log");

        let native_titles = NativeTitleIndex::default();
        let first = ingest_file(
            &store,
            "machine_fixture",
            "codex",
            &log_path,
            &native_titles,
        )
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
                fixture_line("session-1", "first question"),
                fixture_line("session-1", "second question")
            ),
        )
        .expect("append second log line");
        let second = ingest_file(
            &store,
            "machine_fixture",
            "codex",
            &log_path,
            &native_titles,
        )
        .expect("second ingest");
        let stats = store.stats().expect("stats");

        assert_eq!(second.inserted, 2);
        assert_eq!(second.duplicates, 2);
        assert_eq!(second.delta.inserted_events.len(), 1);
        assert_eq!(second.delta.touched_events.len(), 1);
        assert_eq!(second.delta.touched_sessions.len(), 1);
        assert_eq!(second.delta.touched_paths.len(), 1);
        assert_eq!(stats.raw_artifacts, 2);
        assert_eq!(stats.sessions, 1);
        assert_eq!(stats.events, 2);
    }

    #[test]
    fn source_summaries_track_found_and_selected_files_by_kind() {
        let mut summaries = Vec::new();
        push_found_source_file(&mut summaries, "codex");
        push_found_source_file(&mut summaries, "hermes");
        push_found_source_file(&mut summaries, "codex");
        let candidates = vec![
            UpdateCandidate {
                modified: 3,
                kind: "codex",
                path: PathBuf::from("codex-1.jsonl"),
            },
            UpdateCandidate {
                modified: 2,
                kind: "hermes",
                path: PathBuf::from("hermes-1.json"),
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
