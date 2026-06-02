use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, RawArtifact, SessionRecord,
};
use crate::storage::{ImportStats, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Map;
use serde_json::{json, Value};
use std::fs;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone, Default)]
pub struct UpdateStats {
    pub files_seen: usize,
    pub skipped_unchanged: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub errors: usize,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    pub max_files: Option<usize>,
    pub source: Option<String>,
}

#[derive(Debug, Clone)]
struct SourceRoot {
    kind: &'static str,
    path: PathBuf,
    extensions: &'static [&'static str],
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

pub fn update_local(
    store: &Store,
    machine_id: &str,
    options: UpdateOptions,
) -> Result<UpdateStats> {
    let mut stats = UpdateStats::default();
    let mut candidates = Vec::new();
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
            candidates.push((modified, root.kind, entry.path().to_path_buf()));
        }
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0));
    let iter: Box<dyn Iterator<Item = (i128, &'static str, PathBuf)>> =
        if let Some(max_files) = options.max_files {
            Box::new(candidates.into_iter().take(max_files))
        } else {
            Box::new(candidates.into_iter())
        };
    for (_, kind, path) in iter {
        stats.files_seen += 1;
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::debug!("failed to read metadata for {}: {err}", path.display());
                stats.errors += 1;
                continue;
            }
        };
        let size = metadata.len();
        let mtime_ms = file_mtime_ms(&metadata);
        let path_text = path.to_string_lossy().to_string();
        let raw_current = store.raw_artifact_is_current(&path_text, size, mtime_ms)?;
        let needs_workspace_refresh =
            raw_current && store.session_workspace_metadata_missing_for_path(&path_text)?;
        if raw_current && !needs_workspace_refresh {
            stats.skipped_unchanged += 1;
            continue;
        }
        match ingest_file(store, machine_id, kind, &path) {
            Ok(delta) => {
                stats.inserted += delta.inserted;
                stats.duplicates += delta.duplicates;
            }
            Err(err) => {
                tracing::debug!("failed to ingest {}: {err:#}", path.display());
                stats.errors += 1;
            }
        }
    }
    Ok(stats)
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
    }
    roots
}

fn ingest_file(store: &Store, machine_id: &str, kind: &str, path: &Path) -> Result<ImportStats> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let raw_hash = blake3_hex(&bytes);
    let path_text = path.to_string_lossy().to_string();
    let source_id = stable_id(&["source", kind, &path_text]);
    store.upsert_source(&source_id, kind, &path_text, Some(&path_text))?;

    let metadata =
        fs::metadata(path).with_context(|| format!("reading metadata {}", path.display()))?;
    let mtime_ms = file_mtime_ms(&metadata);
    let mut records = Vec::new();
    records.push(ArchiveRecord::RawArtifact(RawArtifact {
        hash: raw_hash.clone(),
        source_id: source_id.clone(),
        path: path_text.clone(),
        size: bytes.len() as u64,
        mtime_ms,
        media_type: media_type(path),
        content: bytes.clone(),
        first_seen_at: Utc::now(),
    }));

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
    let title = lines
        .iter()
        .find(|line| line.role.as_deref() == Some("user") && !line.content.trim().is_empty())
        .or_else(|| lines.iter().find(|line| !line.content.trim().is_empty()))
        .map(|line| snippet(&line.content, 100));
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
    store.import_records(&records)
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

fn snippet(input: &str, max_chars: usize) -> String {
    let input = input.split_whitespace().collect::<Vec<_>>().join(" ");
    input.chars().take(max_chars).collect()
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
}
