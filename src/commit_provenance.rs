use crate::archive::{stable_id, EventRecord, SessionRecord};
use crate::tool_events::{normalize, ToolSignal, ToolStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Component, Path, PathBuf};

mod commands;
pub(crate) mod query;
pub(crate) mod store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CommitEvidence {
    pub id: String,
    pub session_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub event_id: String,
    pub result_event_id: String,
    pub call_id: String,
    pub cwd: Option<String>,
    pub sha: String,
    pub subject: Option<String>,
    pub message: Option<String>,
    pub normalized_message: Option<String>,
    pub message_event_id: Option<String>,
    pub message_result_event_id: Option<String>,
    pub occurred_at: Option<DateTime<Utc>>,
}
#[derive(Debug, Clone)]
struct WriteState {
    content: Option<String>,
    message_event_id: Option<String>,
    message_result_event_id: Option<String>,
    generation: u64,
}

#[derive(Debug, Clone)]
struct MessageSnapshot {
    content: Option<String>,
    message_event_id: Option<String>,
    message_result_event_id: Option<String>,
    source_call_event_id: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingCommit {
    call_event_id: String,
    call_id: String,
    cwd: Option<String>,
    message: MessageSnapshot,
    followed_by_command: bool,
}
#[derive(Debug, Clone)]
enum PendingKind {
    NativeWrite {
        path: Option<String>,
        content: Option<String>,
        generation: u64,
    },
    Shell {
        commits: Vec<PendingCommit>,
    },
}

#[derive(Debug, Clone)]
struct PendingCall {
    call_id: Option<String>,
    call_event_id: String,
    kind: PendingKind,
}

#[derive(Debug, Clone)]
struct CreationHeader {
    sha: String,
    subject: String,
}

/// Detect commit creations represented by normalized tool calls and results.
///
/// This deliberately never executes shell text or consults the local Git
/// repository.  All paths and messages come from the archived tool events.
pub(crate) fn detect(session: &SessionRecord, events: &[EventRecord]) -> Vec<CommitEvidence> {
    let mut ordered: Vec<&EventRecord> = events
        .iter()
        .filter(|event| event.session_id == session.id)
        .collect();
    ordered.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
    });

    let session_cwd = session_cwd(session);
    let mut writes = HashMap::<String, WriteState>::new();
    let mut write_generation = 0u64;
    let mut pending = VecDeque::<PendingCall>::new();
    let mut completed_ids = HashSet::<String>::new();
    let mut evidence_by_sha = HashMap::<String, CommitEvidence>::new();
    for event in ordered {
        for tool_event in normalize(event) {
            match tool_event.signal {
                ToolSignal::Call {
                    call_id: Some(call_id),
                    name,
                    arguments,
                } => {
                    if call_id.trim().is_empty()
                        || completed_ids.contains(&call_id)
                        || pending
                            .iter()
                            .any(|pending| pending.call_id.as_deref() == Some(call_id.as_str()))
                    {
                        continue;
                    }
                    let call_cwd = effective_cwd(&session_cwd, &arguments);
                    let Some(kind) = begin_call(
                        event,
                        &call_id,
                        &name,
                        &arguments,
                        call_cwd,
                        &mut writes,
                        &mut write_generation,
                    ) else {
                        continue;
                    };
                    pending.push_back(PendingCall {
                        call_id: Some(call_id),
                        call_event_id: event.id.clone(),
                        kind,
                    });
                }
                ToolSignal::Result {
                    call_id: Some(call_id),
                    status,
                    text,
                    truncated,
                } if !call_id.trim().is_empty() => {
                    let Some(pending_call) = take_pending(&mut pending, &call_id) else {
                        continue;
                    };
                    completed_ids.insert(call_id);
                    finish_call(
                        session,
                        &pending_call,
                        event,
                        status,
                        text.as_ref().map(|text| text.text.as_str()),
                        truncated,
                        &mut writes,
                        &mut evidence_by_sha,
                    );
                }
                _ => {}
            }
        }
    }

    let mut evidence: Vec<_> = evidence_by_sha.into_values().collect();
    evidence.sort_by(|left, right| {
        left.occurred_at
            .cmp(&right.occurred_at)
            .then_with(|| left.event_id.cmp(&right.event_id))
            .then_with(|| left.result_event_id.cmp(&right.result_event_id))
            .then_with(|| left.sha.cmp(&right.sha))
    });
    evidence
}
fn begin_call(
    event: &EventRecord,
    call_id: &str,
    name: &str,
    arguments: &Value,
    call_cwd: Option<String>,
    writes: &mut HashMap<String, WriteState>,
    write_generation: &mut u64,
) -> Option<PendingKind> {
    if is_shell_runner(name) {
        // Shell parsing below never interprets opaque code execution.
        let command =
            argument_string(arguments, &["command", "cmd", "input"]).or_else(|| arguments.as_str());
        let Some(command) = command else {
            // An opaque shell/eval call is an unknown mutation boundary.  It
            // cannot be a commit authority, but it must not leave stale -F
            // content available to a later invocation.
            invalidate_writes(writes, write_generation);
            return Some(PendingKind::Shell {
                commits: Vec::new(),
            });
        };
        let mut commits = Vec::new();
        for action in commands::analyze(command, call_cwd.as_deref()) {
            match action {
                commands::Action::Write { path, content } => {
                    *write_generation += 1;
                    let generation = *write_generation;
                    let Some(path) = resolve_path(&path, call_cwd.as_deref()) else {
                        invalidate_writes(writes, write_generation);
                        continue;
                    };
                    writes.insert(
                        path,
                        WriteState {
                            content,
                            message_event_id: Some(event.id.clone()),
                            message_result_event_id: None,
                            generation,
                        },
                    );
                }
                commands::Action::Commit {
                    cwd,
                    message,
                    followed_by_command,
                } => {
                    let message =
                        snapshot_message(message, writes, cwd.as_deref(), event.id.as_str());
                    commits.push(PendingCommit {
                        call_event_id: event.id.clone(),
                        call_id: call_id.to_string(),
                        cwd,
                        message,
                        followed_by_command,
                    });
                }
                commands::Action::InvalidateWrites => invalidate_writes(writes, write_generation),
            }
        }
        return Some(PendingKind::Shell { commits });
    }

    if matches!(
        name.to_ascii_lowercase().as_str(),
        "eval" | "python" | "javascript"
    ) {
        invalidate_writes(writes, write_generation);
        return None;
    }
    if !is_native_writer(name) {
        return None;
    }

    let path = argument_string(
        arguments,
        &[
            "path",
            "file_path",
            "filePath",
            "filename",
            "fileName",
            "uri",
            "target",
        ],
    )
    .and_then(|path| resolve_path(path, call_cwd.as_deref()));
    let normalized_name = name.to_ascii_lowercase();
    let content = if normalized_name.contains("edit") || normalized_name.contains("patch") {
        None
    } else {
        write_content(arguments)
    };

    *write_generation += 1;
    let generation = *write_generation;
    if let Some(path) = path.as_ref() {
        // A write is an overwrite barrier as soon as it is invoked.  A later
        // successful, complete result replaces this unknown state with the
        // exact argument bytes.
        writes.insert(
            path.clone(),
            WriteState {
                content: None,
                message_event_id: Some(event.id.clone()),
                message_result_event_id: None,
                generation,
            },
        );
    } else {
        invalidate_writes(writes, write_generation);
    }

    Some(PendingKind::NativeWrite {
        path,
        content,
        generation,
    })
}

fn finish_call(
    session: &SessionRecord,
    pending: &PendingCall,
    result_event: &EventRecord,
    status: ToolStatus,
    text: Option<&str>,
    truncated: bool,
    writes: &mut HashMap<String, WriteState>,
    evidence_by_sha: &mut HashMap<String, CommitEvidence>,
) {
    match &pending.kind {
        PendingKind::NativeWrite {
            path,
            content,
            generation,
        } => {
            if status == ToolStatus::Success && !truncated {
                if let (Some(path), Some(content)) = (path, content) {
                    if writes
                        .get(path)
                        .is_some_and(|write| write.generation == *generation)
                    {
                        writes.insert(
                            path.clone(),
                            WriteState {
                                content: Some(content.clone()),
                                message_event_id: Some(pending.call_event_id.clone()),
                                message_result_event_id: Some(result_event.id.clone()),
                                generation: *generation,
                            },
                        );
                    }
                }
            }
        }
        PendingKind::Shell { commits } => {
            let shell_write_complete = status == ToolStatus::Success && !truncated;
            for write in writes.values_mut() {
                if write.message_event_id.as_deref() == Some(pending.call_event_id.as_str())
                    && write.message_result_event_id.is_none()
                {
                    if shell_write_complete {
                        write.message_result_event_id = Some(result_event.id.clone());
                    } else {
                        // A shell write that did not complete is an
                        // overwrite barrier, never a source of full -F
                        // bytes for a later invocation.
                        write.content = None;
                    }
                }
            }
            let Some(text) = text else {
                return;
            };
            let headers = parse_creation_headers(text);
            if headers.is_empty() || commits.is_empty() {
                return;
            }
            let mut used = HashSet::<usize>::new();
            for header in headers {
                let Some(index) = choose_commit(&commits, &used, &header.subject) else {
                    continue;
                };
                used.insert(index);
                let commit = &commits[index];
                if status == ToolStatus::Failure && !commit.followed_by_command {
                    continue;
                }
                let mut message = commit.message.clone();
                if message.content.is_some()
                    && message.message_result_event_id.is_none()
                    && message.source_call_event_id.as_deref()
                        == Some(commit.call_event_id.as_str())
                {
                    // Inline messages and literal heredoc writes are sourced
                    // by the invocation; the paired result is their citation
                    // for the completed command operation.
                    message.message_result_event_id = Some(result_event.id.clone());
                }
                let subject = (!header.subject.is_empty()).then(|| header.subject.clone());
                let MessageSnapshot {
                    content,
                    message_event_id,
                    message_result_event_id,
                    ..
                } = message;
                let (message, message_event_id, message_result_event_id) =
                    if let Some(message) = content {
                        (Some(message), message_event_id, message_result_event_id)
                    } else {
                        // A creation header is only a subject-level
                        // observation.  Do not present it as a recovered
                        // full -m/-F message.
                        (None, None, None)
                    };
                let normalized_message = message.as_deref().map(normalize_message);
                let id = stable_id(&[
                    "commit_evidence",
                    &session.id,
                    &commit.call_event_id,
                    &result_event.id,
                    &header.sha,
                ]);
                let evidence = CommitEvidence {
                    id,
                    session_id: session.id.clone(),
                    machine_id: session.machine_id.clone(),
                    source_kind: session.source_kind.clone(),
                    event_id: commit.call_event_id.clone(),
                    result_event_id: result_event.id.clone(),
                    call_id: commit.call_id.clone(),
                    cwd: commit.cwd.clone(),
                    sha: header.sha.clone(),
                    subject,
                    message,
                    normalized_message,
                    message_event_id,
                    message_result_event_id,
                    occurred_at: result_event.occurred_at,
                };
                // A repeated provider representation can produce a different
                // event citation for the same commit.  Keep one logical
                // observation per SHA within this session while retaining the
                // first real citations.
                evidence_by_sha.entry(header.sha).or_insert(evidence);
            }
        }
    }
}

fn choose_commit(commits: &[PendingCommit], used: &HashSet<usize>, subject: &str) -> Option<usize> {
    // Prefer a subject match so output from a multi-commit command is paired
    // with the invocation that actually supplied that message.
    if let Some(index) = commits.iter().enumerate().find_map(|(index, commit)| {
        if used.contains(&index) {
            return None;
        }
        let expected = commit
            .message
            .content
            .as_deref()
            .and_then(message_subject)
            .map(normalize_message);
        expected
            .as_deref()
            .filter(|expected| expected == &normalize_message(subject))
            .map(|_| index)
    }) {
        return Some(index);
    }

    // If a known source exists but does not match this header, leave the
    // header available for a later matching one.  Unknown/partial sources can
    // only be paired by deterministic action order.
    let has_unmatched_known = commits
        .iter()
        .enumerate()
        .any(|(index, commit)| !used.contains(&index) && commit.message.content.is_some());
    commits
        .iter()
        .enumerate()
        .find(|(index, commit)| {
            !used.contains(index) && (!has_unmatched_known || commit.message.content.is_none())
        })
        .map(|(index, _)| index)
}

fn parse_creation_headers(text: &str) -> Vec<CreationHeader> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            let inner = line.strip_prefix('[')?.split_once(']')?;
            let header = inner.0;
            let subject = inner.1.trim_start();
            let sha = header.split_whitespace().last()?;
            if sha.len() < 4
                || sha.len() > 64
                || !sha.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit())
            {
                return None;
            }
            Some(CreationHeader {
                sha: sha.to_string(),
                subject: subject.to_string(),
            })
        })
        .collect()
}

fn snapshot_message(
    source: commands::MessageSource,
    writes: &HashMap<String, WriteState>,
    cwd: Option<&str>,
    call_event_id: &str,
) -> MessageSnapshot {
    match source {
        commands::MessageSource::Inline(content) => MessageSnapshot {
            content: Some(content),
            message_event_id: Some(call_event_id.to_string()),
            message_result_event_id: None,
            source_call_event_id: Some(call_event_id.to_string()),
        },
        commands::MessageSource::File(path) => {
            let path = resolve_path(&path, cwd);
            path.and_then(|path| writes.get(&path)).map_or_else(
                || MessageSnapshot {
                    content: None,
                    message_event_id: None,
                    message_result_event_id: None,
                    source_call_event_id: None,
                },
                |write| MessageSnapshot {
                    content: write
                        .content
                        .as_ref()
                        .filter(|_| {
                            write.message_result_event_id.is_some()
                                || write.message_event_id.as_deref() == Some(call_event_id)
                        })
                        .cloned(),
                    message_event_id: write.message_event_id.clone(),
                    message_result_event_id: write.message_result_event_id.clone(),
                    source_call_event_id: write.message_event_id.clone(),
                },
            )
        }
        commands::MessageSource::Unknown => MessageSnapshot {
            content: None,
            message_event_id: None,
            message_result_event_id: None,
            source_call_event_id: None,
        },
    }
}

fn invalidate_writes(writes: &mut HashMap<String, WriteState>, generation: &mut u64) {
    *generation += 1;
    writes.clear();
}

fn take_pending(pending: &mut VecDeque<PendingCall>, result_call_id: &str) -> Option<PendingCall> {
    let index = pending
        .iter()
        .position(|pending| pending.call_id.as_deref() == Some(result_call_id))?;
    pending.remove(index)
}

fn is_shell_runner(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "bash"
            | "sh"
            | "zsh"
            | "shell"
            | "exec"
            | "exec_command"
            | "run_command"
            | "run_shell_command"
            | "terminal"
    ) || name.ends_with("__bash")
}
fn is_native_writer(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase().replace(['-', '.'], "_");
    matches!(
        normalized.as_str(),
        "write"
            | "write_file"
            | "writefile"
            | "write_text"
            | "write_text_file"
            | "file_write"
            | "save_file"
            | "create_file"
            | "edit_file"
            | "replace_file"
            | "update_file"
            | "overwrite_file"
            | "put_file"
            | "apply_patch"
            | "patch_file"
            | "edit"
    ) || normalized.ends_with("_write")
}

fn write_content(arguments: &Value) -> Option<String> {
    for object in std::iter::once(arguments).chain(arguments.get("input")) {
        let Some(object) = object.as_object() else {
            continue;
        };
        for key in [
            "content",
            "contents",
            "text",
            "data",
            "body",
            "new_content",
            "newContent",
        ] {
            if let Some(value) = object.get(key) {
                return value.as_str().map(ToOwned::to_owned);
            }
        }
    }
    None
}
fn argument_string<'a>(arguments: &'a Value, keys: &[&str]) -> Option<&'a str> {
    let object = arguments.as_object()?;
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
}

fn effective_cwd(session_cwd: &Option<String>, arguments: &Value) -> Option<String> {
    let requested = arguments.as_object().and_then(|object| {
        [
            "cwd",
            "working_directory",
            "workingDirectory",
            "workdir",
            "directory",
            "dir",
        ]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
    });
    match requested {
        Some(cwd) => resolve_path(cwd, session_cwd.as_deref()),
        None => session_cwd.clone(),
    }
}

fn session_cwd(session: &SessionRecord) -> Option<String> {
    let object = session.metadata.as_object()?;
    ["cwd", "workspace_path", "workspace_root"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .and_then(|path| resolve_path(path, None))
}

fn resolve_path(path: &str, cwd: Option<&str>) -> Option<String> {
    if path.trim().is_empty() {
        return None;
    }
    let path = Path::new(path);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let cwd = Path::new(cwd?);
        if !cwd.is_absolute() {
            return None;
        }
        cwd.join(path)
    };
    lexical_absolute(&absolute)
}

fn lexical_absolute(path: &Path) -> Option<String> {
    if !path.is_absolute() {
        return None;
    }
    let mut components = Vec::<String>::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                components.pop();
            }
            Component::Normal(component) => {
                components.push(component.to_string_lossy().into_owned())
            }
            Component::Prefix(prefix) => {
                components.push(prefix.as_os_str().to_string_lossy().into_owned())
            }
        }
    }
    let mut output = PathBuf::from("/");
    for component in components {
        output.push(component);
    }
    Some(output.to_string_lossy().into_owned())
}

fn message_subject(message: &str) -> Option<&str> {
    message
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
}

fn normalize_message(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn session(id: &str) -> SessionRecord {
        SessionRecord {
            id: id.to_string(),
            source_id: format!("source-{id}"),
            machine_id: "machine-test".to_string(),
            source_kind: "synthetic".to_string(),
            external_id: id.to_string(),
            title: None,
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({"cwd":"/workspace/repo"}),
            hash: format!("hash-{id}"),
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
                .or_else(|| value.get("message").and_then(|value| value.get("role")))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            content: serde_json::to_string(&value).unwrap(),
            raw_artifact_hash: None,
            occurred_at: Utc.timestamp_opt(1_700_000_000 + ordinal, 0).single(),
            metadata: json!({}),
            hash: format!("event-hash-{id}"),
        }
    }

    fn shell_call(session: &SessionRecord, id: &str, ordinal: i64, command: &str) -> EventRecord {
        event(
            session,
            id,
            ordinal,
            json!({
                "type":"tool_call",
                "call_id":id,
                "name":"bash",
                "arguments":{"command":command}
            }),
        )
    }

    fn shell_result(
        session: &SessionRecord,
        id: &str,
        ordinal: i64,
        output: &str,
        is_error: bool,
    ) -> EventRecord {
        event(
            session,
            id,
            ordinal,
            json!({
                "type":"tool_result",
                "call_id":id,
                "output":output,
                "isError":is_error
            }),
        )
    }

    fn native_write(
        session: &SessionRecord,
        id: &str,
        ordinal: i64,
        path: &str,
        content: &str,
    ) -> EventRecord {
        event(
            session,
            id,
            ordinal,
            json!({
                "type":"tool_call",
                "call_id":id,
                "name":"write_file",
                "arguments":{"path":path,"content":content}
            }),
        )
    }

    fn native_result(
        session: &SessionRecord,
        id: &str,
        ordinal: i64,
        is_error: bool,
        truncated: bool,
    ) -> EventRecord {
        event(
            session,
            id,
            ordinal,
            json!({
                "type":"tool_result",
                "call_id":id,
                "output":"ok",
                "isError":is_error,
                "truncated":truncated
            }),
        )
    }

    #[test]
    fn commit_evidence_recovers_native_write_file_message() {
        let session = session("native-file");
        let evidence = detect(
            &session,
            &[
                native_write(&session, "write", 0, "/tmp/message", "Subject\n\nBody\n"),
                native_result(&session, "write", 1, false, false),
                shell_call(&session, "commit", 2, "git commit -F /tmp/message"),
                shell_result(
                    &session,
                    "commit",
                    3,
                    "[main abc1234] Subject\n 1 file changed\n",
                    false,
                ),
            ],
        );
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].subject.as_deref(), Some("Subject"));
        assert_eq!(evidence[0].message.as_deref(), Some("Subject\n\nBody\n"));
        assert_eq!(
            evidence[0].normalized_message.as_deref(),
            Some("Subject Body")
        );
        assert_eq!(evidence[0].message_event_id.as_deref(), Some("write"));
        assert_eq!(
            evidence[0].message_result_event_id.as_deref(),
            Some("write")
        );
        assert_eq!(evidence[0].call_id, "commit");
    }

    #[test]
    fn commit_evidence_overwrite_barrier_blocks_old_file_message() {
        let session = session("overwrite");
        let evidence = detect(
            &session,
            &[
                native_write(&session, "write-1", 0, "/tmp/message", "Old subject\n"),
                native_result(&session, "write-1", 1, false, false),
                native_write(&session, "write-2", 2, "/tmp/message", "New subject\n"),
                shell_call(&session, "commit", 3, "git commit -F /tmp/message"),
                shell_result(&session, "commit", 4, "[main abc1234] New subject\n", false),
            ],
        );
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].subject.as_deref(), Some("New subject"));
        assert!(evidence[0].message.is_none());
        assert!(evidence[0].normalized_message.is_none());
        assert!(evidence[0].message_event_id.is_none());
    }

    #[test]
    fn commit_evidence_failed_or_truncated_write_cannot_supply_full_message() {
        let session = session("incomplete-write");
        let evidence = detect(
            &session,
            &[
                native_write(
                    &session,
                    "write",
                    0,
                    "/tmp/message",
                    "Never use this body\n",
                ),
                native_result(&session, "write", 1, true, false),
                shell_call(&session, "commit", 2, "git commit -F /tmp/message"),
                shell_result(
                    &session,
                    "commit",
                    3,
                    "[main abc1234] Subject only\n",
                    false,
                ),
            ],
        );
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].subject.as_deref(), Some("Subject only"));
        assert!(evidence[0].message.is_none());
        assert!(evidence[0].normalized_message.is_none());
        assert!(evidence[0].message_event_id.is_none());

        let evidence = detect(
            &session,
            &[
                native_write(
                    &session,
                    "write-t",
                    0,
                    "/tmp/message",
                    "Never use this body\n",
                ),
                native_result(&session, "write-t", 1, false, true),
                shell_call(&session, "commit-t", 2, "git commit -F /tmp/message"),
                shell_result(
                    &session,
                    "commit-t",
                    3,
                    "[main abc1235] Subject only\n",
                    false,
                ),
            ],
        );
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].subject.as_deref(), Some("Subject only"));
        assert!(evidence[0].message.is_none());
    }

    #[test]
    fn commit_evidence_uncompleted_shell_write_and_partial_edit_stay_unknown() {
        let session = session("uncertain-writes");
        let pending_write = detect(
            &session,
            &[
                shell_call(&session, "write", 0, "printf '%s' 'Subject' > /tmp/message"),
                shell_call(&session, "commit", 1, "git commit -F /tmp/message"),
                shell_result(&session, "commit", 2, "[main abc1234] Subject", false),
                shell_result(&session, "write", 3, "", false),
            ],
        );
        assert_eq!(pending_write.len(), 1);
        assert!(pending_write[0].message.is_none());
        let partial_edit = detect(
            &session,
            &[
                native_write(&session, "write", 0, "/tmp/message", "Old body"),
                native_result(&session, "write", 1, false, false),
                event(
                    &session,
                    "edit",
                    2,
                    json!({
                        "type":"tool_call","call_id":"edit","name":"edit_file",
                        "arguments":{"path":"/tmp/message","new_content":"replacement fragment"}
                    }),
                ),
                native_result(&session, "edit", 3, false, false),
                shell_call(&session, "commit", 4, "git commit -F /tmp/message"),
                shell_result(&session, "commit", 5, "[main def5678] New subject", false),
            ],
        );
        assert_eq!(partial_edit.len(), 1);
        assert!(partial_edit[0].message.is_none());
    }
    #[test]
    fn commit_evidence_ignores_failed_noop_and_log_mentions() {
        let session = session("distractors");
        let evidence = detect(
            &session,
            &[
                shell_call(&session, "log", 0, "git log --oneline"),
                shell_result(&session, "log", 1, "abc1234 previous subject", false),
                shell_call(
                    &session,
                    "noop",
                    2,
                    "git commit --allow-empty -m 'No commit'",
                ),
                shell_result(&session, "noop", 3, "nothing to commit", false),
                shell_call(&session, "failed", 4, "git commit -m 'Failed'"),
                shell_result(&session, "failed", 5, "fatal: rejected", true),
            ],
        );
        assert!(evidence.is_empty());
    }

    #[test]
    fn commit_evidence_failed_standalone_header_is_not_creation_proof() {
        let session = session("failed-header");
        let failed = detect(
            &session,
            &[
                shell_call(
                    &session,
                    "single",
                    0,
                    "git commit -m 'Subject && condition'",
                ),
                shell_result(
                    &session,
                    "single",
                    1,
                    "[main abc1234] Subject && condition\nfatal: rejected",
                    true,
                ),
            ],
        );
        assert!(failed.is_empty());
        let trailing_failure = detect(
            &session,
            &[
                shell_call(&session, "chain", 0, "git commit -m 'Subject' && false"),
                shell_result(
                    &session,
                    "chain",
                    1,
                    "[main abc1234] Subject\nlater command failed",
                    true,
                ),
            ],
        );
        assert_eq!(trailing_failure.len(), 1);
        assert_eq!(trailing_failure[0].sha, "abc1234");
    }

    #[test]
    fn commit_evidence_is_session_scoped_for_same_message_filename() {
        let first = session("first");
        let second = session("second");
        let mut events = vec![
            native_write(&first, "write-1", 0, "/tmp/shared-message", "First\n"),
            native_result(&first, "write-1", 1, false, false),
            shell_call(&first, "commit-1", 2, "git commit -F /tmp/shared-message"),
            shell_result(&first, "commit-1", 3, "[main abc1234] First\n", false),
            shell_call(&second, "commit-2", 0, "git commit -F /tmp/shared-message"),
            shell_result(&second, "commit-2", 1, "[main def5678] Second\n", false),
        ];
        let first_evidence = detect(&first, &events);
        assert_eq!(first_evidence.len(), 1);
        assert_eq!(first_evidence[0].message.as_deref(), Some("First\n"));
        events.retain(|event| event.session_id == second.id);
        let second_evidence = detect(&second, &events);
        assert_eq!(second_evidence.len(), 1);
        assert_eq!(second_evidence[0].message, None);
        assert_eq!(second_evidence[0].subject.as_deref(), Some("Second"));
    }

    #[test]
    fn commit_evidence_handles_multiple_commits_and_trailing_failure() {
        let session = session("multiple");
        let command = "git commit -m 'First subject' && git commit -m 'Second subject' && false";
        let evidence = detect(
            &session,
            &[
                shell_call(&session, "multi", 0, command),
                shell_result(
                    &session,
                    "multi",
                    1,
                    "[main abc1234] First subject\n[main def5678] Second subject\nfalse: failed\n",
                    true,
                ),
            ],
        );
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence[0].message.as_deref(), Some("First subject"));
        assert_eq!(evidence[1].message.as_deref(), Some("Second subject"));
    }

    #[test]
    fn commit_evidence_dynamic_git_directory_is_not_the_session_repository() {
        let session = session("unknown-repository");
        let evidence = detect(
            &session,
            &[
                shell_call(
                    &session,
                    "commit",
                    0,
                    "git -C \"$TARGET\" commit -m 'Subject'",
                ),
                shell_result(&session, "commit", 1, "[main abc1234] Subject", false),
            ],
        );
        assert_eq!(evidence.len(), 1);
        assert!(evidence[0].cwd.is_none());
    }

    #[test]
    fn commit_evidence_omp_and_non_omp_envelopes_are_equivalent() {
        let session = session("envelopes");
        let call = event(
            &session,
            "call",
            0,
            json!({
                "type":"message",
                "message":{"role":"assistant","content":[{"type":"toolCall","id":"c","name":"bash","arguments":{"command":"git commit -m 'Subject'"}}]}
            }),
        );
        let result = event(
            &session,
            "result",
            1,
            json!({
                "type":"message",
                "message":{"role":"toolResult","toolCallId":"c","content":"[main abc1234] Subject\n","isError":false}
            }),
        );
        let codex = detect(
            &session,
            &[
                shell_call(&session, "call", 0, "git commit -m 'Subject'"),
                shell_result(&session, "call", 1, "[main abc1234] Subject\n", false),
            ],
        );
        let omp = detect(&session, &[call, result]);
        assert_eq!(codex.len(), 1);
        assert_eq!(omp.len(), 1);
        assert_eq!(codex[0].sha, omp[0].sha);
        assert_eq!(codex[0].message, omp[0].message);
        assert_eq!(codex[0].normalized_message, omp[0].normalized_message);
    }
}
