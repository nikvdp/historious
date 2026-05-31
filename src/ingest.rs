use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, RawArtifact, SessionRecord,
};
use crate::storage::{ImportStats, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone, Default)]
pub struct UpdateStats {
    pub files_seen: usize,
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
    role: Option<String>,
    event_type: String,
    occurred_at: Option<DateTime<Utc>>,
    external_session_id: Option<String>,
}

pub fn update_local(store: &Store, machine_id: &str, options: UpdateOptions) -> Result<UpdateStats> {
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
                    tracing::debug!("skipping unreadable entry under {}: {err}", root.path.display());
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
    let iter: Box<dyn Iterator<Item = (i128, &'static str, PathBuf)>> = if let Some(max_files) = options.max_files {
        Box::new(candidates.into_iter().take(max_files))
    } else {
        Box::new(candidates.into_iter())
    };
    for (_, kind, path) in iter {
        stats.files_seen += 1;
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

    let metadata = fs::metadata(path).with_context(|| format!("reading metadata {}", path.display()))?;
    let mtime_ms = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64);
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
        metadata: json!({
            "path": path_text,
            "capture_fidelity": "exact_local_log",
            "parser": "generic_json_event_v1"
        }),
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
                "capture_fidelity": "exact_local_log"
            }),
            hash: line_hash,
        }));
    }
    store.import_records(&records)
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
    let content = extract_text(&value).unwrap_or_else(|| value.to_string());
    let role = string_at(&value, &["role"])
        .or_else(|| string_at(&value, &["message", "role"]))
        .or_else(|| string_at(&value, &["payload", "role"]));
    let event_type = string_at(&value, &["type"])
        .or_else(|| string_at(&value, &["payload", "type"]))
        .unwrap_or_else(|| role.clone().unwrap_or_else(|| "event".to_string()));
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
        role,
        event_type,
        occurred_at,
        external_session_id,
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
            for key in ["text", "content", "output", "input", "command", "stdout", "stderr"] {
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
        .map(|ext| extensions.iter().any(|candidate| ext.eq_ignore_ascii_case(candidate)))
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
