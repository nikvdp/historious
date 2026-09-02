use crate::archive::{blake3_hex, stable_id, EventRecord, SessionRecord};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillHashBasis {
    ReturnedDocumentBytes,
    NormalizedToolOutput,
    EmbeddedDocumentBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillCoverage {
    Complete,
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillConfidence {
    High,
    Medium,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillLoadKind {
    NativeRead,
    ShellRead,
    EmbeddedContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillObservation {
    pub id: String,
    pub session_id: String,
    pub event_id: String,
    pub result_event_id: String,
    pub tool_call_id: Option<String>,
    pub machine_id: String,
    pub source_kind: String,
    pub skill_name: String,
    pub locator: String,
    pub content_hash: Option<String>,
    pub hash_basis: Option<SkillHashBasis>,
    pub coverage: SkillCoverage,
    pub confidence: SkillConfidence,
    pub load_kind: SkillLoadKind,
    pub workspace: Option<String>,
    pub repository: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillProjectionState {
    Missing,
    Stale,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillProjectionStatus {
    pub state: SkillProjectionState,
    pub observation_count: usize,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillMaintenanceMode {
    Rebuild,
    Incremental,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillMaintenanceOutcome {
    pub mode: SkillMaintenanceMode,
    pub processed_sessions: usize,
    pub total_sessions: usize,
    pub observation_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageFilter {
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    pub project: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageTotals {
    pub locators: usize,
    pub skills: usize,
    pub unique_sessions: usize,
    pub loads: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageConfidenceCounts {
    pub high: usize,
    pub medium: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageVersion {
    pub content_hash: Option<String>,
    pub loads: usize,
    pub unique_sessions: usize,
    pub hash_bases: Vec<SkillHashBasis>,
    pub complete_loads: usize,
    pub partial_loads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageEvidence {
    pub session_id: String,
    pub event_id: String,
    pub result_event_id: String,
    pub observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageAggregate {
    pub locator: String,
    pub skill_name: String,
    pub unique_sessions: usize,
    pub loads: usize,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub source_kinds: Vec<String>,
    pub workspaces: Vec<String>,
    pub repositories: Vec<String>,
    pub confidence: SkillUsageConfidenceCounts,
    pub versions: Vec<SkillUsageVersion>,
    pub evidence: Vec<SkillUsageEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageOutput {
    pub filters: SkillUsageFilter,
    pub totals: SkillUsageTotals,
    pub records: Vec<SkillUsageAggregate>,
}

impl SkillHashBasis {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ReturnedDocumentBytes => "returned_document_bytes",
            Self::NormalizedToolOutput => "normalized_tool_output",
            Self::EmbeddedDocumentBytes => "embedded_document_bytes",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "returned_document_bytes" => Some(Self::ReturnedDocumentBytes),
            "normalized_tool_output" => Some(Self::NormalizedToolOutput),
            "embedded_document_bytes" => Some(Self::EmbeddedDocumentBytes),
            _ => None,
        }
    }
}

impl SkillCoverage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
        }
    }
}

impl SkillConfidence {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
        }
    }
}

impl SkillLoadKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NativeRead => "native_read",
            Self::ShellRead => "shell_read",
            Self::EmbeddedContext => "embedded_context",
        }
    }
}

#[derive(Debug, Clone)]
struct SkillTarget {
    name: String,
    locator: String,
    partial: bool,
}

#[derive(Debug, Clone)]
struct PendingRead {
    target: SkillTarget,
    call_event_id: String,
    call_id: String,
    load_kind: SkillLoadKind,
    confidence: SkillConfidence,
}

#[derive(Debug)]
enum ToolSignal {
    Call {
        call_id: String,
        name: String,
        arguments: Value,
    },
    Result {
        call_id: String,
        success: bool,
        text: Option<RecoveredText>,
        truncated: bool,
    },
}

#[derive(Debug, Clone)]
struct RecoveredText {
    text: String,
    basis: SkillHashBasis,
}

pub fn detect_skill_observations(
    session: &SessionRecord,
    events: &[EventRecord],
) -> Vec<SkillObservation> {
    let mut ordered: Vec<&EventRecord> = events.iter().collect();
    ordered.sort_by_key(|event| event.ordinal);

    let mut pending = HashMap::<String, PendingRead>::new();
    let mut completed = HashSet::<String>::new();
    let mut observations = Vec::new();

    for event in ordered {
        let Ok(value) = serde_json::from_str::<Value>(&event.content) else {
            continue;
        };

        detect_embedded_documents(session, event, &value, &mut observations);

        for signal in tool_signals(&value) {
            match signal {
                ToolSignal::Call {
                    call_id,
                    name,
                    arguments,
                } => {
                    if completed.contains(&call_id) || pending.contains_key(&call_id) {
                        continue;
                    }
                    if let Some(read) =
                        pending_read(session, event, call_id.clone(), &name, &arguments)
                    {
                        pending.insert(call_id, read);
                    }
                }
                ToolSignal::Result {
                    call_id,
                    success,
                    text,
                    truncated,
                } => {
                    if completed.contains(&call_id) {
                        continue;
                    }
                    let Some(read) = pending.remove(&call_id) else {
                        continue;
                    };
                    completed.insert(call_id);
                    if !success || (read.load_kind == SkillLoadKind::ShellRead && text.is_none()) {
                        continue;
                    }
                    observations.push(observation_from_result(
                        session, event, read, text, truncated,
                    ));
                }
            }
        }
    }

    observations
}

fn pending_read(
    session: &SessionRecord,
    event: &EventRecord,
    call_id: String,
    tool_name: &str,
    arguments: &Value,
) -> Option<PendingRead> {
    if is_native_reader(tool_name) {
        let path = argument_string(arguments, &["path", "file_path", "filePath", "uri"])?;
        let argument_partial = arguments.get("offset").is_some()
            || arguments.get("limit").is_some()
            || arguments.get("line_start").is_some()
            || arguments.get("line_end").is_some();
        let mut target = skill_target(session, path)?;
        target.partial |= argument_partial;
        return Some(PendingRead {
            target,
            call_event_id: event.id.clone(),
            call_id,
            load_kind: SkillLoadKind::NativeRead,
            confidence: SkillConfidence::High,
        });
    }

    if is_shell_runner(tool_name) {
        let command = argument_string(arguments, &["command", "cmd", "input"])
            .or_else(|| arguments.as_str())?;
        let path = shell_cat_path(command)?;
        let target = skill_target(session, &path)?;
        return Some(PendingRead {
            target,
            call_event_id: event.id.clone(),
            call_id,
            load_kind: SkillLoadKind::ShellRead,
            confidence: SkillConfidence::Medium,
        });
    }

    None
}

fn observation_from_result(
    session: &SessionRecord,
    result_event: &EventRecord,
    read: PendingRead,
    text: Option<RecoveredText>,
    truncated: bool,
) -> SkillObservation {
    let coverage = if read.target.partial || truncated || text.is_none() {
        SkillCoverage::Partial
    } else {
        SkillCoverage::Complete
    };
    let recovered_name = text
        .as_ref()
        .and_then(|recovered| frontmatter_name(&recovered.text));
    let (content_hash, hash_basis) = if coverage == SkillCoverage::Complete {
        text.as_ref()
            .map(|recovered| {
                (
                    Some(blake3_hex(recovered.text.as_bytes())),
                    Some(recovered.basis),
                )
            })
            .unwrap_or((None, None))
    } else {
        (None, None)
    };

    let locator = read.target.locator;
    SkillObservation {
        id: stable_id(&[
            "skill_observation",
            &session.id,
            &read.call_event_id,
            &read.call_id,
            &locator,
        ]),
        session_id: session.id.clone(),
        event_id: read.call_event_id,
        result_event_id: result_event.id.clone(),
        tool_call_id: Some(read.call_id),
        machine_id: session.machine_id.clone(),
        source_kind: session.source_kind.clone(),
        skill_name: recovered_name.unwrap_or(read.target.name),
        locator,
        content_hash,
        hash_basis,
        coverage,
        confidence: read.confidence,
        load_kind: read.load_kind,
        workspace: session_workspace(session),
        repository: session_git_repo(session).map(normalize_git_remote),
        observed_at: result_event.occurred_at,
    }
}

fn detect_embedded_documents(
    session: &SessionRecord,
    event: &EventRecord,
    value: &Value,
    observations: &mut Vec<SkillObservation>,
) {
    if !is_instruction_event(event) {
        return;
    }

    let mut ordinal = 0usize;
    walk_objects(value, &mut |object| {
        let path = object
            .get("skill_uri")
            .or_else(|| object.get("skill_path"))
            .and_then(Value::as_str)
            .or_else(|| {
                let kind = object
                    .get("kind")
                    .or_else(|| object.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                (kind == "skill" || kind == "skill_document")
                    .then(|| object.get("path").and_then(Value::as_str))
                    .flatten()
            });
        let content = object
            .get("content")
            .or_else(|| object.get("text"))
            .and_then(Value::as_str);
        let (Some(path), Some(content)) = (path, content) else {
            return;
        };
        if object_is_truncated(object) {
            return;
        }
        let Some(target) = skill_target(session, path) else {
            return;
        };
        let Some(name) = frontmatter_name(content).or(Some(target.name)) else {
            return;
        };
        let locator = target.locator;
        let reference = ordinal.to_string();
        ordinal += 1;
        observations.push(SkillObservation {
            id: stable_id(&[
                "skill_observation",
                &session.id,
                &event.id,
                &reference,
                &locator,
            ]),
            session_id: session.id.clone(),
            event_id: event.id.clone(),
            result_event_id: event.id.clone(),
            tool_call_id: None,
            machine_id: session.machine_id.clone(),
            source_kind: session.source_kind.clone(),
            skill_name: name,
            locator,
            content_hash: Some(blake3_hex(content.as_bytes())),
            hash_basis: Some(SkillHashBasis::EmbeddedDocumentBytes),
            coverage: SkillCoverage::Complete,
            confidence: SkillConfidence::High,
            load_kind: SkillLoadKind::EmbeddedContext,
            workspace: session_workspace(session),
            repository: session_git_repo(session).map(normalize_git_remote),
            observed_at: event.occurred_at,
        });
    });
}

fn tool_signals(value: &Value) -> Vec<ToolSignal> {
    let mut signals = Vec::new();
    collect_tool_signals(value, &mut signals);
    signals
}

fn collect_tool_signals(value: &Value, signals: &mut Vec<ToolSignal>) {
    match value {
        Value::Object(object) => {
            if let Some(call) = tool_call_signal(object) {
                signals.push(call);
            } else if let Some(result) = tool_result_signal(object) {
                signals.push(result);
            }
            for child in object.values() {
                collect_tool_signals(child, signals);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_tool_signals(item, signals);
            }
        }
        _ => {}
    }
}

fn tool_call_signal(object: &Map<String, Value>) -> Option<ToolSignal> {
    let event_type = object
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let nested_function = object.get("function").and_then(Value::as_object);
    let is_call = event_type.contains("toolcall")
        || event_type.contains("tool_call")
        || event_type == "tool_use"
        || event_type == "function_call"
        || event_type == "custom_tool_call"
        || (event_type == "function" && nested_function.is_some() && object.contains_key("id"));
    if !is_call {
        return None;
    }

    let source = nested_function.unwrap_or(object);
    let name = source.get("name")?.as_str()?.to_string();
    let call_id = object
        .get("call_id")
        .or_else(|| object.get("id"))?
        .as_str()?
        .to_string();
    let arguments = source
        .get("arguments")
        .or_else(|| source.get("input"))
        .cloned()
        .unwrap_or(Value::Null);
    let arguments = parse_json_string(arguments);
    Some(ToolSignal::Call {
        call_id,
        name,
        arguments,
    })
}

fn tool_result_signal(object: &Map<String, Value>) -> Option<ToolSignal> {
    let event_type = object
        .get("type")
        .or_else(|| object.get("role"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_result = event_type.contains("tool_result")
        || event_type.contains("toolresult")
        || event_type.contains("function_call_output")
        || event_type.contains("custom_tool_call_output")
        || event_type == "mcp_tool_call_end"
        || event_type == "tool";
    if !is_result {
        return None;
    }

    let call_id = object
        .get("call_id")
        .or_else(|| object.get("toolCallId"))
        .or_else(|| object.get("tool_use_id"))
        .or_else(|| object.get("tool_call_id"))?
        .as_str()?
        .to_string();
    let text = recover_result_text(object);
    let truncated = object_is_truncated(object)
        || text
            .as_ref()
            .is_some_and(|recovered| text_has_truncation_marker(&recovered.text));
    Some(ToolSignal::Result {
        call_id,
        success: !object_failed(object),
        text,
        truncated,
    })
}

fn recover_result_text(object: &Map<String, Value>) -> Option<RecoveredText> {
    if let Some(text) =
        value_at(object, &["details", "displayContent", "text"]).and_then(Value::as_str)
    {
        return Some(RecoveredText {
            text: text.to_string(),
            basis: SkillHashBasis::ReturnedDocumentBytes,
        });
    }
    if let Some(text) = value_at(object, &["details", "cells"])
        .and_then(Value::as_array)
        .and_then(|cells| cells.last())
        .and_then(|cell| cell.get("output"))
        .and_then(Value::as_str)
    {
        return Some(RecoveredText {
            text: text.to_string(),
            basis: SkillHashBasis::ReturnedDocumentBytes,
        });
    }
    if let Some(text) = object.get("stdout").and_then(Value::as_str) {
        return Some(RecoveredText {
            text: text.to_string(),
            basis: SkillHashBasis::ReturnedDocumentBytes,
        });
    }
    if let Some(text) = value_at(object, &["result", "Ok", "content"])
        .and_then(text_from_value)
        .or_else(|| object.get("output").and_then(text_from_value))
        .or_else(|| object.get("content").and_then(text_from_value))
    {
        return Some(normalize_tool_output(text));
    }
    None
}

fn normalize_tool_output(text: String) -> RecoveredText {
    let lines: Vec<&str> = text.lines().collect();
    let anchored = lines.first().is_some_and(|line| {
        let line = line.trim();
        line.starts_with('[') && line.ends_with(']') && line.contains('#')
    });
    let content_lines = if anchored { &lines[1..] } else { &lines[..] };
    let numbered = !content_lines.is_empty()
        && content_lines
            .iter()
            .filter(|line| !line.is_empty())
            .all(|line| strip_numbered_prefix(line).is_some());
    if !anchored && !numbered {
        return RecoveredText {
            text,
            basis: SkillHashBasis::ReturnedDocumentBytes,
        };
    }

    let mut normalized = String::new();
    for (index, line) in content_lines.iter().enumerate() {
        if index > 0 {
            normalized.push('\n');
        }
        normalized.push_str(strip_numbered_prefix(line).unwrap_or(line));
    }
    if text.ends_with('\n') {
        normalized.push('\n');
    }
    RecoveredText {
        text: normalized,
        basis: SkillHashBasis::NormalizedToolOutput,
    }
}

fn strip_numbered_prefix(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let digit_count = trimmed.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    let rest = &trimmed[digit_count..];
    rest.strip_prefix(':').or_else(|| rest.strip_prefix('→'))
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let parts: Vec<&str> = items
                .iter()
                .filter_map(|item| {
                    item.as_str()
                        .or_else(|| item.get("text").and_then(Value::as_str))
                })
                .collect();
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        Value::Object(object) => object
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

fn object_failed(object: &Map<String, Value>) -> bool {
    if object
        .get("is_error")
        .or_else(|| object.get("isError"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        return true;
    }
    if object.get("success").and_then(Value::as_bool) == Some(false) {
        return true;
    }
    if object
        .get("exit_code")
        .or_else(|| object.get("exitCode"))
        .and_then(Value::as_i64)
        .is_some_and(|code| code != 0)
    {
        return true;
    }
    if object.get("Err").is_some() {
        return true;
    }
    object.values().any(|value| match value {
        Value::Object(child) => object_failed(child),
        Value::Array(items) => items.iter().filter_map(Value::as_object).any(object_failed),
        _ => false,
    })
}

fn object_is_truncated(object: &Map<String, Value>) -> bool {
    if object.get("truncated").and_then(Value::as_bool) == Some(true)
        || object.get("content_compacted").and_then(Value::as_bool) == Some(true)
    {
        return true;
    }
    let total = object
        .get("totalBytes")
        .or_else(|| object.get("total_bytes"))
        .and_then(Value::as_u64);
    let output = object
        .get("outputBytes")
        .or_else(|| object.get("output_bytes"))
        .and_then(Value::as_u64);
    if matches!((total, output), (Some(total), Some(output)) if output < total) {
        return true;
    }
    if object.values().any(|value| match value {
        Value::Object(child) => object_is_truncated(child),
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_object)
            .any(object_is_truncated),
        _ => false,
    }) {
        return true;
    }
    false
}

fn text_has_truncation_marker(text: &str) -> bool {
    let tail = text
        .trim_end()
        .rsplit_once('\n')
        .map(|(_, tail)| tail)
        .unwrap_or(text)
        .trim()
        .to_ascii_lowercase();
    tail.starts_with("[output truncated")
        || tail.starts_with("[truncated")
        || tail.ends_with("lines omitted]")
}

fn skill_target(session: &SessionRecord, raw_path: &str) -> Option<SkillTarget> {
    let (path, selector_partial) = split_read_selector(raw_path.trim());
    if let Some(name) = skill_uri_name(path) {
        return Some(SkillTarget {
            locator: format!("skill://{name}"),
            name,
            partial: selector_partial,
        });
    }

    let path = absolute_skill_path(session, path)?;
    let name = skill_path_name(&path)?;
    let workspace_root = session_workspace_root(session).map(PathBuf::from);
    let repository = session_git_repo(session).map(normalize_git_remote);
    let locator = match (workspace_root.as_deref(), repository) {
        (Some(root), Some(remote)) => path
            .strip_prefix(root)
            .ok()
            .map(|relative| format!("{remote}#{}", slash_path(relative)))
            .unwrap_or_else(|| local_locator(&session.machine_id, &path)),
        _ => local_locator(&session.machine_id, &path),
    };
    Some(SkillTarget {
        name,
        locator,
        partial: selector_partial,
    })
}

fn split_read_selector(path: &str) -> (&str, bool) {
    let Some((base, selector)) = path.rsplit_once(':') else {
        return (path, false);
    };
    if selector == "raw" {
        return (base, false);
    }
    let selector = selector.strip_prefix("raw:").unwrap_or(selector);
    let recognized = !selector.is_empty()
        && selector
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '-' | ',' | '+'));
    if recognized {
        (base, true)
    } else {
        (path, false)
    }
}

fn skill_uri_name(path: &str) -> Option<String> {
    let name = path.strip_prefix("skill://")?;
    (!name.is_empty()
        && !name.contains('/')
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_')))
    .then(|| name.to_string())
}

fn skill_path_name(path: &Path) -> Option<String> {
    let components: Vec<String> = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect();
    if components.len() < 3 || components.last().map(String::as_str) != Some("SKILL.md") {
        return None;
    }
    let skills_index = components.len() - 3;
    if components[skills_index] != "skills" {
        return None;
    }
    let name = &components[skills_index + 1];
    (!name.is_empty()).then(|| name.clone())
}

fn absolute_skill_path(session: &SessionRecord, path: &str) -> Option<PathBuf> {
    let path = Path::new(path);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(relative) = path.to_str().and_then(|value| value.strip_prefix("~/")) {
        inferred_home(session)?.join(relative)
    } else {
        let base = session
            .metadata
            .get("cwd")
            .and_then(Value::as_str)
            .or_else(|| session_workspace_root(session))?;
        Path::new(base).join(path)
    };
    Some(clean_path(&absolute))
}

fn inferred_home(session: &SessionRecord) -> Option<PathBuf> {
    let path = session
        .metadata
        .get("cwd")
        .and_then(Value::as_str)
        .or_else(|| session_workspace_root(session))?;
    let mut components = Path::new(path).components();
    let root = components.next()?;
    let home_parent = components.next()?;
    let user = components.next()?;
    let parent = home_parent.as_os_str().to_string_lossy();
    ((parent == "Users" || parent == "home") && matches!(root, Component::RootDir)).then(|| {
        PathBuf::from("/")
            .join(home_parent.as_os_str())
            .join(user.as_os_str())
    })
}

fn clean_path(path: &Path) -> PathBuf {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                cleaned.pop();
            }
            other => cleaned.push(other.as_os_str()),
        }
    }
    cleaned
}

fn local_locator(machine_id: &str, path: &Path) -> String {
    format!("machine://{machine_id}{}", slash_path(path))
}

fn slash_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn normalize_git_remote(remote: &str) -> String {
    let mut remote = remote
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .to_string();
    if let Some((user_host, path)) = remote.split_once(':') {
        if user_host.contains('@') && !user_host.contains("//") {
            let host = user_host.rsplit('@').next().unwrap_or(user_host);
            return format!("https://{host}/{}", path.trim_start_matches('/'));
        }
    }
    if let Some(rest) = remote.strip_prefix("ssh://") {
        let rest = rest
            .rsplit_once('@')
            .map(|(_, value)| value)
            .unwrap_or(rest);
        remote = format!("https://{rest}");
    }
    remote
}

fn shell_cat_path(command: &str) -> Option<String> {
    let words = shell_words(command)?;
    let (program, arguments) = words.split_first()?;
    if Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        != Some("cat")
    {
        return None;
    }
    let arguments = arguments
        .strip_prefix(&["--".to_string()])
        .unwrap_or(arguments);
    (arguments.len() == 1).then(|| arguments[0].clone())
}

fn shell_words(command: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            word.push(character);
            escaped = false;
            continue;
        }
        match (quote, character) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('\''), _) | (Some('"'), _) => word.push(character),
            (None, '\\') => escaped = true,
            (None, '\'' | '"') => quote = Some(character),
            (None, character) if character.is_whitespace() => {
                if !word.is_empty() {
                    words.push(std::mem::take(&mut word));
                }
            }
            (None, '|' | '&' | ';' | '>' | '<' | '$' | '`') => return None,
            (None, _) => word.push(character),
            _ => unreachable!(),
        }
    }
    if quote.is_some() || escaped {
        return None;
    }
    if !word.is_empty() {
        words.push(word);
    }
    Some(words)
}

fn is_native_reader(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(name.as_str(), "read" | "read_file" | "_fetch_file") || name.ends_with("__read")
}

fn is_shell_runner(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(name.as_str(), "bash" | "shell" | "exec" | "exec_command") || name.ends_with("__bash")
}

fn argument_string<'a>(arguments: &'a Value, keys: &[&str]) -> Option<&'a str> {
    let object = arguments.as_object()?;
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
}

fn parse_json_string(value: Value) -> Value {
    match value {
        Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
        other => other,
    }
}

fn value_at<'a>(object: &'a Map<String, Value>, path: &[&str]) -> Option<&'a Value> {
    let mut value = object.get(*path.first()?)?;
    for key in &path[1..] {
        value = value.get(*key)?;
    }
    Some(value)
}

fn frontmatter_name(content: &str) -> Option<String> {
    let mut lines = content.lines();
    if lines.next()?.trim() != "---" {
        return None;
    }
    for line in lines {
        let line = line.trim();
        if line == "---" {
            break;
        }
        if let Some(name) = line.strip_prefix("name:") {
            let name = name.trim().trim_matches(['\'', '"']);
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    None
}

fn session_workspace(session: &SessionRecord) -> Option<String> {
    session
        .metadata
        .get("workspace_path")
        .or_else(|| session.metadata.get("workspace_root"))
        .or_else(|| session.metadata.get("cwd"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn session_workspace_root(session: &SessionRecord) -> Option<&str> {
    session
        .metadata
        .get("workspace_root")
        .or_else(|| session.metadata.get("workspace_path"))
        .and_then(Value::as_str)
}

fn session_git_repo(session: &SessionRecord) -> Option<&str> {
    session
        .metadata
        .get("git_repo")
        .or_else(|| session.metadata.get("git_remote"))
        .and_then(Value::as_str)
}

fn is_instruction_event(event: &EventRecord) -> bool {
    event
        .role
        .as_deref()
        .is_some_and(|role| matches!(role.to_ascii_lowercase().as_str(), "system" | "developer"))
        || {
            let event_type = event.event_type.to_ascii_lowercase();
            event_type.contains("instruction")
                || event_type.contains("context")
                || event_type == "system"
                || event_type == "developer"
        }
}

fn walk_objects(value: &Value, callback: &mut impl FnMut(&Map<String, Value>)) {
    match value {
        Value::Object(object) => {
            callback(object);
            for child in object.values() {
                walk_objects(child, callback);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk_objects(item, callback);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    const BODY: &str = "---\nname: observed-skill\n---\n\n# Skill\n";

    fn session(source_kind: &str) -> SessionRecord {
        SessionRecord {
            id: format!("session-{source_kind}"),
            source_id: format!("source-{source_kind}"),
            machine_id: "machine-a".to_string(),
            source_kind: source_kind.to_string(),
            external_id: "external".to_string(),
            title: None,
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({
                "cwd": "/workspace/repo",
                "workspace_path": "/workspace/repo",
                "workspace_root": "/workspace/repo",
                "git_repo": "git@github.com:example/repo.git"
            }),
            hash: "session-hash".to_string(),
        }
    }

    fn event(session: &SessionRecord, id: &str, ordinal: i64, value: Value) -> EventRecord {
        EventRecord {
            id: id.to_string(),
            session_id: session.id.clone(),
            source_id: session.source_id.clone(),
            machine_id: session.machine_id.clone(),
            source_kind: session.source_kind.clone(),
            ordinal,
            event_type: value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("event")
                .to_string(),
            role: value
                .get("role")
                .or_else(|| value.get("message").and_then(|message| message.get("role")))
                .and_then(Value::as_str)
                .map(str::to_string),
            content: serde_json::to_string(&value).unwrap(),
            raw_artifact_hash: None,
            occurred_at: Utc.timestamp_opt(1_700_000_000 + ordinal, 0).single(),
            metadata: json!({}),
            hash: format!("hash-{id}"),
        }
    }

    fn assert_success(observation: &SkillObservation, session: &SessionRecord, call: &str) {
        assert_eq!(observation.session_id, session.id);
        assert_eq!(observation.event_id, "call");
        assert_eq!(observation.result_event_id, "result");
        assert_eq!(observation.tool_call_id.as_deref(), Some(call));
        assert_eq!(observation.machine_id, "machine-a");
        assert_eq!(observation.source_kind, session.source_kind);
        assert_eq!(observation.skill_name, "observed-skill");
        assert_eq!(
            observation.locator,
            "https://github.com/example/repo#skills/path-skill/SKILL.md"
        );
        assert_eq!(observation.content_hash, Some(blake3_hex(BODY.as_bytes())));
        assert_eq!(
            observation.hash_basis,
            Some(SkillHashBasis::ReturnedDocumentBytes)
        );
        assert_eq!(observation.coverage, SkillCoverage::Complete);
        assert_eq!(observation.confidence, SkillConfidence::High);
        assert_eq!(observation.load_kind, SkillLoadKind::NativeRead);
        assert_eq!(observation.workspace.as_deref(), Some("/workspace/repo"));
        assert_eq!(
            observation.repository.as_deref(),
            Some("https://github.com/example/repo")
        );
        assert_eq!(
            observation.observed_at,
            Utc.timestamp_opt(1_700_000_001, 0).single()
        );
    }

    #[test]
    fn skill_usage_detects_supported_native_read_shapes() {
        let cases = [
            (
                "codex",
                json!({"type":"response_item","payload":{"type":"function_call","call_id":"call-1","name":"read_file","arguments":"{\"path\":\"/workspace/repo/skills/path-skill/SKILL.md\"}"}}),
                json!({"type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":BODY}}),
                "call-1",
            ),
            (
                "claude_code",
                json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"call-2","name":"Read","input":{"file_path":"/workspace/repo/skills/path-skill/SKILL.md"}}]}}),
                json!({"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-2","content":BODY}]}}),
                "call-2",
            ),
            (
                "omp",
                json!({"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-3","name":"read","arguments":{"path":"/workspace/repo/skills/path-skill/SKILL.md"}}]}}),
                json!({"type":"message","message":{"role":"toolResult","toolCallId":"call-3","toolName":"read","content":[{"type":"text","text":"rendered"}],"details":{"displayContent":{"text":BODY}},"isError":false}}),
                "call-3",
            ),
            (
                "pi_agent",
                json!({"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":"call-4","name":"read","arguments":{"path":"/workspace/repo/skills/path-skill/SKILL.md"}}]}}),
                json!({"type":"message","message":{"role":"toolResult","toolCallId":"call-4","toolName":"read","content":[{"type":"text","text":BODY}],"isError":false}}),
                "call-4",
            ),
        ];

        for (source, call, result, call_id) in cases {
            let session = session(source);
            let observations = detect_skill_observations(
                &session,
                &[
                    event(&session, "result", 1, result),
                    event(&session, "call", 0, call),
                ],
            );
            assert_eq!(observations.len(), 1, "source {source}");
            assert_success(&observations[0], &session, call_id);
        }
    }

    #[test]
    fn skill_usage_pairs_hermes_messages_inside_one_event() {
        let session = session("hermes");
        let combined = event(
            &session,
            "combined",
            0,
            json!({
                "messages": [
                    {"role":"assistant","tool_calls":[{"id":"call-h","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"skill://hermes-skill\"}"}}]},
                    {"role":"tool","tool_call_id":"call-h","content":BODY}
                ]
            }),
        );
        let observations = detect_skill_observations(&session, &[combined]);
        assert_eq!(observations.len(), 1);
        let observation = &observations[0];
        assert_eq!(observation.event_id, "combined");
        assert_eq!(observation.result_event_id, "combined");
        assert_eq!(observation.skill_name, "observed-skill");
        assert_eq!(observation.locator, "skill://hermes-skill");
        assert_eq!(observation.content_hash, Some(blake3_hex(BODY.as_bytes())));
        assert_eq!(observation.coverage, SkillCoverage::Complete);
    }

    #[test]
    fn skill_usage_counts_only_successful_narrow_shell_reads() {
        let session = session("omp");
        let observations = detect_skill_observations(
            &session,
            &[
                event(
                    &session,
                    "call",
                    0,
                    json!({"type":"toolCall","id":"shell-1","name":"bash","arguments":{"command":"cat -- '/workspace/repo/skills/path-skill/SKILL.md'"}}),
                ),
                event(
                    &session,
                    "result",
                    1,
                    json!({"role":"toolResult","toolCallId":"shell-1","content":BODY,"isError":false}),
                ),
                event(
                    &session,
                    "search-call",
                    2,
                    json!({"type":"toolCall","id":"search-1","name":"bash","arguments":{"command":"grep SKILL.md ."}}),
                ),
                event(
                    &session,
                    "search-result",
                    3,
                    json!({"role":"toolResult","toolCallId":"search-1","content":"skills/path-skill/SKILL.md","isError":false}),
                ),
                event(
                    &session,
                    "failed-call",
                    4,
                    json!({"type":"toolCall","id":"shell-2","name":"bash","arguments":{"command":"cat /workspace/repo/skills/path-skill/SKILL.md"}}),
                ),
                event(
                    &session,
                    "failed-result",
                    5,
                    json!({"role":"toolResult","toolCallId":"shell-2","content":"permission denied","isError":true}),
                ),
            ],
        );
        assert_eq!(observations.len(), 1);
        let observation = &observations[0];
        assert_eq!(observation.event_id, "call");
        assert_eq!(observation.load_kind, SkillLoadKind::ShellRead);
        assert_eq!(observation.confidence, SkillConfidence::Medium);
        assert_eq!(observation.content_hash, Some(blake3_hex(BODY.as_bytes())));
    }

    #[test]
    fn skill_usage_marks_partial_reads_without_guessing_a_version() {
        let session = session("omp");
        let observations = detect_skill_observations(
            &session,
            &[
                event(
                    &session,
                    "call",
                    0,
                    json!({"type":"toolCall","id":"partial","name":"read","arguments":{"path":"skill://partial-skill:1-20"}}),
                ),
                event(
                    &session,
                    "result",
                    1,
                    json!({"role":"toolResult","toolCallId":"partial","content":BODY,"details":{"truncation":{"truncated":true,"totalBytes":100,"outputBytes":40}},"isError":false}),
                ),
            ],
        );
        assert_eq!(observations.len(), 1);
        let observation = &observations[0];
        assert_eq!(observation.locator, "skill://partial-skill");
        assert_eq!(observation.coverage, SkillCoverage::Partial);
        assert_eq!(observation.content_hash, None);
        assert_eq!(observation.hash_basis, None);
    }

    #[test]
    fn skill_usage_excludes_mentions_searches_diffs_and_failed_reads() {
        for source in [
            "codex",
            "claude_code",
            "opencode",
            "pi_agent",
            "omp",
            "hermes",
        ] {
            let session = session(source);
            let events = [
                event(
                    &session,
                    "prose",
                    0,
                    json!({"role":"user","content":"please read skill://mentioned"}),
                ),
                event(
                    &session,
                    "search",
                    1,
                    json!({"type":"toolCall","id":"search","name":"grep","arguments":{"pattern":"SKILL.md","path":"skills"}}),
                ),
                event(
                    &session,
                    "search-output",
                    2,
                    json!({"role":"toolResult","toolCallId":"search","content":"skills/mentioned/SKILL.md"}),
                ),
                event(
                    &session,
                    "diff",
                    3,
                    json!({"role":"assistant","content":"diff --git a/skills/mentioned/SKILL.md b/skills/mentioned/SKILL.md"}),
                ),
                event(
                    &session,
                    "failed",
                    4,
                    json!({"type":"toolCall","id":"failed","name":"read","arguments":{"path":"skill://failed"}}),
                ),
                event(
                    &session,
                    "failed-output",
                    5,
                    json!({"role":"toolResult","toolCallId":"failed","content":"not found","isError":true}),
                ),
            ];
            assert!(
                detect_skill_observations(&session, &events).is_empty(),
                "source {source}"
            );
        }
    }

    #[test]
    fn skill_usage_deduplicates_repeated_call_representations() {
        let session = session("codex");
        let events = [
            event(
                &session,
                "call",
                0,
                json!({"type":"function_call","call_id":"same","name":"read_file","arguments":{"path":"skill://dedup"}}),
            ),
            event(
                &session,
                "call-copy",
                1,
                json!({"type":"function_call","call_id":"same","name":"read_file","arguments":{"path":"skill://dedup"}}),
            ),
            event(
                &session,
                "result",
                2,
                json!({"type":"function_call_output","call_id":"same","output":BODY}),
            ),
            event(
                &session,
                "result-copy",
                3,
                json!({"type":"function_call_output","call_id":"same","output":BODY}),
            ),
        ];
        let observations = detect_skill_observations(&session, &events);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].event_id, "call");
    }

    #[test]
    fn skill_usage_detects_complete_provenanced_context_documents() {
        let mut session = session("codex");
        let mut embedded = event(
            &session,
            "context",
            0,
            json!({"type":"developer","role":"developer","context":{"kind":"skill_document","path":"skill://embedded","content":BODY}}),
        );
        embedded.role = Some("developer".to_string());
        session.source_kind = "codex".to_string();
        let observations = detect_skill_observations(&session, &[embedded]);
        assert_eq!(observations.len(), 1);
        let observation = &observations[0];
        assert_eq!(observation.locator, "skill://embedded");
        assert_eq!(observation.skill_name, "observed-skill");
        assert_eq!(observation.load_kind, SkillLoadKind::EmbeddedContext);
        assert_eq!(
            observation.hash_basis,
            Some(SkillHashBasis::EmbeddedDocumentBytes)
        );
        assert_eq!(observation.content_hash, Some(blake3_hex(BODY.as_bytes())));
    }
}
