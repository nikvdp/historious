use crate::archive::EventRecord;
use serde_json::{Map, Value};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolEvent {
    pub subordinal: i64,
    pub signal: ToolSignal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ToolSignal {
    Call {
        call_id: Option<String>,
        name: String,
        arguments: Value,
    },
    Result {
        call_id: Option<String>,
        status: ToolStatus,
        text: Option<RecoveredText>,
        truncated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolStatus {
    Success,
    Failure,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveredText {
    pub text: String,
    pub normalized: bool,
}

pub(crate) fn normalize(event: &EventRecord) -> Vec<ToolEvent> {
    let mut collector = Collector::default();
    if let Ok(value) = serde_json::from_str::<Value>(&event.content) {
        collect_root(&value, &mut collector);
        if collector.signals.is_empty() && event_allows_untyped_payload(event) {
            collect_untyped_payload(&value, event, &mut collector);
        }
    } else if is_legacy_chunk_result(event) {
        collector.emit(ToolSignal::Result {
            call_id: None,
            status: ToolStatus::Unknown,
            text: Some(RecoveredText {
                text: event.content.trim().to_string(),
                normalized: false,
            }),
            truncated: text_has_truncation_marker(&event.content),
        });
    }
    collector
        .signals
        .into_iter()
        .enumerate()
        .map(|(subordinal, signal)| ToolEvent {
            subordinal: subordinal as i64,
            signal,
        })
        .collect()
}

fn event_allows_untyped_payload(event: &EventRecord) -> bool {
    let event_type = event.event_type.to_ascii_lowercase();
    let role = event
        .role
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    role == "tool"
        || role == "toolcall"
        || role == "tool_call"
        || role == "toolresult"
        || role == "tool_result"
        || event_type.contains("tool_result")
        || event_type.contains("toolresult")
        || event_type.contains("function_call")
        || event_type == "response_item"
}

fn collect_untyped_payload(value: &Value, event: &EventRecord, collector: &mut Collector) {
    let object = value.as_object();
    let payload = object
        .and_then(|object| object.get("payload"))
        .and_then(Value::as_object)
        .or(object);
    let Some(payload) = payload else {
        return;
    };
    let event_type = event.event_type.to_ascii_lowercase();
    let role = event
        .role
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let result_provenance =
        role.contains("result") || event_type.contains("result") || event_type.contains("output");
    if !result_provenance {
        if let Some(name) = string_at(payload, &["name", "tool", "toolName"]) {
            let arguments = first_value(payload, &["arguments", "input", "args", "command"])
                .cloned()
                .unwrap_or(Value::Null);
            collector.emit(ToolSignal::Call {
                call_id: string_at(
                    payload,
                    &[
                        "call_id",
                        "callId",
                        "toolCallId",
                        "tool_call_id",
                        "tool_use_id",
                        "id",
                    ],
                ),
                name,
                arguments: preserve_cwd(parse_json_string(arguments), &[payload]),
            });
        }
    }
    if result_provenance
        && ["output", "result", "content", "stdout", "stderr"]
            .iter()
            .any(|key| payload.contains_key(*key))
    {
        let text = recover_result_text(payload);
        let truncated = object_is_truncated(payload)
            || text
                .as_ref()
                .is_some_and(|text| text_has_truncation_marker(&text.text));
        collector.emit(ToolSignal::Result {
            call_id: string_at(
                payload,
                &[
                    "call_id",
                    "callId",
                    "toolCallId",
                    "tool_call_id",
                    "tool_use_id",
                    "id",
                ],
            ),
            status: result_status(payload, result_provenance),
            text,
            truncated,
        });
    }
}

fn is_legacy_chunk_result(event: &EventRecord) -> bool {
    let text = event.content.trim();
    if !(text.starts_with("Chunk ID:") && text.contains("\nOutput:")) {
        return false;
    }
    let event_type = event.event_type.to_ascii_lowercase();
    let role = event
        .role
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    role == "tool"
        || role == "toolresult"
        || role == "tool_result"
        || event_type == "response_item"
        || event_type.contains("tool_result")
        || event_type.contains("function_call")
        || event_type.contains("exec")
}

#[derive(Default)]
struct Collector {
    signals: Vec<ToolSignal>,
    seen: HashSet<String>,
}

impl Collector {
    fn emit(&mut self, signal: ToolSignal) {
        let key = signal_key(&signal);
        if self.seen.insert(key) {
            self.signals.push(signal);
        }
    }
}

fn signal_key(signal: &ToolSignal) -> String {
    match signal {
        ToolSignal::Call {
            call_id,
            name,
            arguments,
        } => format!(
            "call\0{}\0{}\0{}",
            call_id.as_deref().unwrap_or_default(),
            name,
            serde_json::to_string(arguments).unwrap_or_default()
        ),
        ToolSignal::Result {
            call_id,
            status,
            text,
            truncated,
        } => format!(
            "result\0{}\0{:?}\0{}\0{}",
            call_id.as_deref().unwrap_or_default(),
            status,
            text.as_ref()
                .map(|text| text.text.as_str())
                .unwrap_or_default(),
            truncated
        ),
    }
}

fn collect_root(value: &Value, collector: &mut Collector) {
    match value {
        Value::Object(object) => collect_root_object(object, collector),
        // A top-level array is a known archive envelope shape for a batch of
        // provider records. Each item still has to identify its own schema.
        Value::Array(items) => {
            for item in items {
                if item.is_object() {
                    collect_root(item, collector);
                }
            }
        }
        _ => {}
    }
}

fn collect_root_object(object: &Map<String, Value>, collector: &mut Collector) {
    let kind = object_kind(object);
    if kind.contains("compact") || kind.contains("summary") {
        return;
    }

    if let Some(payload) = object.get("payload").and_then(Value::as_object) {
        if kind == "response_item" || is_codex_payload(payload) {
            collect_codex_payload(payload, collector);
        }
    }

    if let Some(signal) = parse_call(object) {
        collector.emit(signal);
    }
    if let Some(signal) = parse_result(object) {
        collector.emit(signal);
    }
    if is_opencode_part(object) {
        collect_opencode_part(object, collector);
    }

    {
        if let Some(message) = object.get("message").and_then(Value::as_object) {
            collect_message(message, collector);
        }
        if let Some(messages) = object.get("messages").and_then(Value::as_array) {
            for message in messages {
                if let Some(message) = message.as_object() {
                    collect_message(message, collector);
                }
            }
        }
        if let Some(parts) = object.get("parts").and_then(Value::as_array) {
            collect_parts(parts, collector, true);
        }
        if has_message_shape(object) {
            collect_message(object, collector);
        }
    }
}

fn collect_codex_payload(object: &Map<String, Value>, collector: &mut Collector) {
    if let Some(signal) = parse_call(object) {
        collector.emit(signal);
    }
    if let Some(signal) = parse_result(object) {
        collector.emit(signal);
    }
}

fn collect_message(object: &Map<String, Value>, collector: &mut Collector) {
    let role = object
        .get("role")
        .or_else(|| object.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let allow_calls = !matches!(
        role.as_str(),
        "user" | "tool" | "toolresult" | "tool_result"
    );
    if allow_calls {
        if let Some(signal) = parse_call(object) {
            collector.emit(signal);
        }
    }
    if let Some(signal) = parse_result(object) {
        collector.emit(signal);
    }
    if allow_calls && is_opencode_part(object) {
        collect_opencode_part(object, collector);
    }

    if allow_calls {
        if let Some(calls) = object
            .get("tool_calls")
            .or_else(|| object.get("toolCalls"))
            .and_then(Value::as_array)
        {
            for call in calls {
                collect_tool_call(call, collector);
            }
        }
    }
    if let Some(content) = object.get("content") {
        collect_content(content, collector, allow_calls);
    }
    if let Some(parts) = object.get("parts").and_then(Value::as_array) {
        collect_parts(parts, collector, allow_calls);
    }
}

fn collect_tool_call(value: &Value, collector: &mut Collector) {
    let Some(object) = value.as_object() else {
        return;
    };
    if let Some(signal) = parse_hermes_call(object).or_else(|| parse_call(object)) {
        collector.emit(signal);
    }
}

fn collect_content(value: &Value, collector: &mut Collector, allow_calls: bool) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_content_item(item, collector, allow_calls);
            }
        }
        Value::Object(_) => collect_content_item(value, collector, allow_calls),
        // Strings are message text, never an operation source. In particular,
        // do not parse quoted JSON transcripts supplied as user content.
        Value::String(_) | Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn collect_content_item(value: &Value, collector: &mut Collector, allow_calls: bool) {
    let Some(object) = value.as_object() else {
        return;
    };
    if allow_calls {
        if let Some(signal) = parse_call(object) {
            collector.emit(signal);
        }
    }
    if let Some(signal) = parse_result(object) {
        collector.emit(signal);
    }
    if allow_calls && is_opencode_part(object) {
        collect_opencode_part(object, collector);
    }
    if allow_calls {
        if let Some(calls) = object
            .get("tool_calls")
            .or_else(|| object.get("toolCalls"))
            .and_then(Value::as_array)
        {
            for call in calls {
                collect_tool_call(call, collector);
            }
        }
    }
    // A provider may wrap a content block in one additional message object;
    // descend only through that named structural container.
    if object_kind(object) == "message" {
        if let Some(content) = object.get("content") {
            collect_content(content, collector, allow_calls);
        }
    }
}

fn collect_parts(parts: &[Value], collector: &mut Collector, allow_calls: bool) {
    for part in parts {
        let Some(object) = part.as_object() else {
            continue;
        };
        if allow_calls {
            if let Some(signal) = parse_call(object) {
                collector.emit(signal);
            }
        }
        if let Some(signal) = parse_result(object) {
            collector.emit(signal);
        }
        if allow_calls && is_opencode_part(object) {
            collect_opencode_part(object, collector);
        }
    }
}

fn collect_opencode_part(object: &Map<String, Value>, collector: &mut Collector) {
    let kind = object_kind(object);
    if kind != "tool" && !kind.contains("tool_call") && !kind.contains("tool_result") {
        return;
    }
    let state = object.get("state").and_then(Value::as_object);
    let name = string_at(object, &["tool", "name", "toolName"]);
    let call_id = string_at(
        object,
        &["callID", "callId", "toolCallId", "tool_call_id", "id"],
    );

    if let Some(name) = name {
        let arguments = state
            .and_then(|state| first_value(state, &["input", "arguments", "args"]))
            .or_else(|| first_value(object, &["arguments", "input", "args"]))
            .cloned()
            .unwrap_or(Value::Null);
        let argument_sources = state
            .map(|state| vec![state, object])
            .unwrap_or_else(|| vec![object]);
        collector.emit(ToolSignal::Call {
            call_id: call_id.clone(),
            name,
            arguments: preserve_cwd(parse_json_string(arguments), &argument_sources),
        });
    }

    let Some(state) = state else {
        return;
    };
    let has_result = ["output", "result", "error", "stderr", "stdout", "status"]
        .iter()
        .any(|key| state.contains_key(*key));
    if !has_result {
        return;
    }
    let text = recover_result_text(state);
    let truncated = object_is_truncated(state)
        || text
            .as_ref()
            .is_some_and(|text| text_has_truncation_marker(&text.text));
    collector.emit(ToolSignal::Result {
        call_id,
        status: result_status(state, false),
        text,
        truncated,
    });
}

fn parse_call(object: &Map<String, Value>) -> Option<ToolSignal> {
    let kind = object_kind(object);
    let nested_function = object.get("function").and_then(Value::as_object);
    let is_call = is_call_kind(&kind)
        || (kind == "function" && nested_function.is_some())
        || (kind == "tool" && object.contains_key("arguments"));
    if !is_call {
        return None;
    }

    let source = nested_function.unwrap_or(object);
    let name = string_at(source, &["name", "tool", "toolName"])
        .or_else(|| string_at(object, &["name", "tool", "toolName"]))?;
    let call_id = string_at(
        object,
        &[
            "call_id",
            "callId",
            "toolCallId",
            "tool_call_id",
            "tool_use_id",
            "id",
        ],
    );
    let arguments = first_value(source, &["arguments", "input", "args", "parameters"])
        .or_else(|| first_value(object, &["arguments", "input", "args", "parameters"]))
        .cloned()
        .unwrap_or(Value::Null);
    Some(ToolSignal::Call {
        call_id,
        name,
        arguments: preserve_cwd(parse_json_string(arguments), &[source, object]),
    })
}

fn parse_hermes_call(object: &Map<String, Value>) -> Option<ToolSignal> {
    let function = object.get("function").and_then(Value::as_object)?;
    let name = string_at(function, &["name"])?;
    let call_id = string_at(
        object,
        &["id", "call_id", "callId", "toolCallId", "tool_call_id"],
    );
    let arguments = first_value(function, &["arguments", "input", "args"])
        .or_else(|| first_value(object, &["arguments", "input", "args"]))
        .cloned()
        .unwrap_or(Value::Null);
    Some(ToolSignal::Call {
        call_id,
        name,
        arguments: preserve_cwd(parse_json_string(arguments), &[function, object]),
    })
}

fn parse_result(object: &Map<String, Value>) -> Option<ToolSignal> {
    let kind = object_kind(object);
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let result_kind = is_result_kind(&kind)
        || matches!(role.as_str(), "tool" | "toolresult" | "tool_result")
        || (kind == "tool"
            && object.keys().any(|key| {
                matches!(
                    key.as_str(),
                    "output" | "result" | "content" | "stdout" | "stderr" | "error"
                )
            }));
    if !result_kind {
        return None;
    }
    if kind == "tool" && object.contains_key("state") {
        return None;
    }

    let call_id = string_at(
        object,
        &[
            "call_id",
            "callId",
            "toolCallId",
            "tool_call_id",
            "tool_use_id",
        ],
    );
    let text = recover_result_text(object);
    let truncated = object_is_truncated(object)
        || text
            .as_ref()
            .is_some_and(|text| text_has_truncation_marker(&text.text));
    let provider_contract =
        is_result_kind(&kind) || matches!(role.as_str(), "tool" | "toolresult" | "tool_result");
    Some(ToolSignal::Result {
        call_id,
        status: result_status(object, provider_contract),
        text,
        truncated,
    })
}

fn object_kind(object: &Map<String, Value>) -> String {
    object
        .get("type")
        .or_else(|| object.get("role"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn has_message_shape(object: &Map<String, Value>) -> bool {
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    role.as_deref().is_some_and(|role| {
        matches!(
            role,
            "assistant" | "user" | "tool" | "toolresult" | "tool_result"
        )
    }) || object.contains_key("tool_calls")
        || object.contains_key("toolCalls")
        || (object.contains_key("content") && object.contains_key("role"))
}

fn is_codex_payload(object: &Map<String, Value>) -> bool {
    object
        .get("type")
        .and_then(Value::as_str)
        .map(|kind| {
            let kind = kind.to_ascii_lowercase();
            is_call_kind(&kind) || is_result_kind(&kind)
        })
        .unwrap_or(false)
}

fn is_call_kind(kind: &str) -> bool {
    kind.contains("toolcall")
        || kind.contains("tool_call")
        || matches!(
            kind,
            "tool_use" | "function_call" | "custom_tool_call" | "function"
        )
}

fn is_result_kind(kind: &str) -> bool {
    kind.contains("tool_result")
        || kind.contains("toolresult")
        || kind.contains("function_call_output")
        || kind.contains("custom_tool_call_output")
        || kind == "mcp_tool_call_end"
}

fn is_opencode_part(object: &Map<String, Value>) -> bool {
    let kind = object_kind(object);
    kind == "tool"
        || kind.contains("tool_call")
        || kind.contains("tool_result")
        || (object.contains_key("state") && object.contains_key("tool"))
}

fn result_status(object: &Map<String, Value>, provider_contract: bool) -> ToolStatus {
    if explicit_failure(object) {
        return ToolStatus::Failure;
    }
    if explicit_success(object) || provider_contract {
        return ToolStatus::Success;
    }
    ToolStatus::Unknown
}

fn explicit_failure(object: &Map<String, Value>) -> bool {
    for key in ["is_error", "isError", "failed", "failure"] {
        if object.get(key).and_then(Value::as_bool) == Some(true) {
            return true;
        }
    }
    if object.get("success").and_then(Value::as_bool) == Some(false)
        || object.get("ok").and_then(Value::as_bool) == Some(false)
    {
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
    if object
        .get("error")
        .or_else(|| object.get("Err"))
        .is_some_and(|value| !value.is_null() && value != &Value::Bool(false))
    {
        return true;
    }
    object
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "error" | "failed" | "failure" | "aborted" | "cancelled" | "canceled"
            )
        })
}

fn explicit_success(object: &Map<String, Value>) -> bool {
    for key in ["is_error", "isError"] {
        if object.get(key).and_then(Value::as_bool) == Some(false) {
            return true;
        }
    }
    if object.get("success").and_then(Value::as_bool) == Some(true)
        || object.get("ok").and_then(Value::as_bool) == Some(true)
    {
        return true;
    }
    if object
        .get("exit_code")
        .or_else(|| object.get("exitCode"))
        .and_then(Value::as_i64)
        == Some(0)
    {
        return true;
    }
    object
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "ok" | "success" | "succeeded" | "complete" | "completed" | "done"
            )
        })
}

fn recover_result_text(object: &Map<String, Value>) -> Option<RecoveredText> {
    if let Some(text) =
        value_at(object, &["details", "displayContent", "text"]).and_then(Value::as_str)
    {
        return Some(RecoveredText {
            text: text.to_string(),
            normalized: false,
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
            normalized: false,
        });
    }
    for key in ["stdout", "stderr"] {
        if let Some(text) = object.get(key).and_then(Value::as_str) {
            return Some(RecoveredText {
                text: text.to_string(),
                normalized: false,
            });
        }
    }
    if let Some(text) = value_at(object, &["result", "Ok", "content"])
        .and_then(text_from_value)
        .or_else(|| object.get("output").and_then(text_from_value))
        .or_else(|| object.get("result").and_then(text_from_value))
        .or_else(|| object.get("content").and_then(text_from_value))
        .or_else(|| object.get("toolUseResult").and_then(text_from_value))
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
            normalized: false,
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
        normalized: true,
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

fn object_is_truncated(object: &Map<String, Value>) -> bool {
    if [
        "truncated",
        "content_compacted",
        "partial",
        "incomplete",
        "isPartial",
    ]
    .iter()
    .any(|key| object.get(*key).and_then(Value::as_bool) == Some(true))
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
    ["details", "state", "result", "toolUseResult", "truncation"]
        .iter()
        .filter_map(|key| object.get(*key).and_then(Value::as_object))
        .any(object_is_truncated)
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

fn first_value<'a>(object: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| object.get(*key))
}

fn string_at(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    first_value(object, keys)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn value_at<'a>(object: &'a Map<String, Value>, path: &[&str]) -> Option<&'a Value> {
    let mut value = object.get(*path.first()?)?;
    for key in &path[1..] {
        value = value.get(*key)?;
    }
    Some(value)
}

fn parse_json_string(value: Value) -> Value {
    match value {
        Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
        other => other,
    }
}

fn preserve_cwd(mut arguments: Value, sources: &[&Map<String, Value>]) -> Value {
    let cwd = sources.iter().find_map(|source| {
        first_value(source, &["cwd", "current_working_directory"]).and_then(Value::as_str)
    });
    let Some(cwd) = cwd else {
        return arguments;
    };
    if let Value::Object(object) = &mut arguments {
        object
            .entry("cwd".to_string())
            .or_insert_with(|| Value::String(cwd.to_string()));
    }
    arguments
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(value: Value) -> EventRecord {
        EventRecord {
            id: "event".to_string(),
            session_id: "session".to_string(),
            source_id: "source".to_string(),
            machine_id: "machine".to_string(),
            source_kind: "omp".to_string(),
            ordinal: 0,
            event_type: "message".to_string(),
            role: None,
            content: value.to_string(),
            raw_artifact_hash: None,
            occurred_at: None,
            metadata: json!({}),
            hash: "hash".to_string(),
        }
    }

    #[test]
    fn normalized_tool_nested_omp_call_write_result_is_exact_and_ordered() {
        let content = json!({
            "type": "message",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "toolCall",
                        "id": "write-1",
                        "name": "write",
                        "arguments": {
                            "path": "notes/file.md",
                            "content": "exact\nbytes",
                            "cwd": "/repo"
                        }
                    }
                ]
            },
            "providerPayload": {
                "type": "toolCall",
                "id": "write-1",
                "name": "write",
                "arguments": {"path": "wrong"}
            },
            "summary": "toolCall read skill://quoted"
        });
        let events = normalize(&event(content));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subordinal, 0);
        match &events[0].signal {
            ToolSignal::Call {
                call_id,
                name,
                arguments,
            } => {
                assert_eq!(call_id.as_deref(), Some("write-1"));
                assert_eq!(name, "write");
                assert_eq!(arguments["content"], "exact\nbytes");
                assert_eq!(arguments["cwd"], "/repo");
            }
            ToolSignal::Result { .. } => panic!("expected call"),
        }
    }

    #[test]
    fn normalized_tool_equivalent_provider_results_are_successful() {
        let values = [
            json!({"type":"response_item","payload":{"type":"function_call_output","call_id":"x","output":"body"}}),
            json!({"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"body"}]}}),
            json!({"messages":[{"role":"tool","tool_call_id":"x","content":"body"}]}),
            json!({"type":"message","message":{"role":"toolResult","toolCallId":"x","content":"body"}}),
        ];
        for value in values {
            let events = normalize(&event(value));
            assert_eq!(events.len(), 1);
            match &events[0].signal {
                ToolSignal::Result {
                    call_id,
                    status,
                    text,
                    truncated,
                } => {
                    assert_eq!(call_id.as_deref(), Some("x"));
                    assert_eq!(*status, ToolStatus::Success);
                    assert_eq!(text.as_ref().map(|text| text.text.as_str()), Some("body"));
                    assert!(!truncated);
                }
                ToolSignal::Call { .. } => panic!("expected result"),
            }
        }
    }

    #[test]
    fn normalized_tool_failure_unknown_and_truncation_remain_distinct() {
        let value = json!({
            "messages": [
                {"role":"tool","tool_call_id":"failed","content":"no","is_error":true},
                {"type":"tool","call_id":"unknown","output":"maybe"},
                {"role":"tool","tool_call_id":"partial","content":"1: line\n2: [output truncated]","truncated":true}
            ]
        });
        let events = normalize(&event(value));
        assert_eq!(events.len(), 3);
        let statuses = events
            .iter()
            .map(|event| match &event.signal {
                ToolSignal::Result {
                    status, truncated, ..
                } => (*status, *truncated),
                ToolSignal::Call { .. } => panic!("expected result"),
            })
            .collect::<Vec<_>>();
        assert_eq!(statuses[0], (ToolStatus::Failure, false));
        assert_eq!(statuses[1], (ToolStatus::Unknown, false));
        assert_eq!(statuses[2], (ToolStatus::Success, true));
    }

    #[test]
    fn normalized_tool_hermes_call_and_quoted_operations_do_not_duplicate() {
        let value = json!({
            "messages": [
                {"role":"assistant","tool_calls":[{"id":"read-1","type":"function","function":{"name":"read","arguments":"{\"path\":\"skill://one\"}"}}]},
                {"role":"tool","tool_call_id":"read-1","content":"---\nname: one\n---"},
                {"role":"user","content":"{\"type\":\"toolCall\",\"id\":\"quoted\",\"name\":\"read\"}"}
            ],
            "providerPayload": {"messages": [{"role":"assistant","tool_calls":[{"id":"read-1","type":"function","function":{"name":"read","arguments":"{}"}}]}]}
        });
        let events = normalize(&event(value));
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].signal, ToolSignal::Call { .. }));
        assert!(matches!(events[1].signal, ToolSignal::Result { .. }));
    }
}
