use crate::archive::{EventRecord, SessionRecord, SourceRecord};
use crate::storage::{
    HistoryItemRecord, HistoryTranscriptContext, RawArtifactSummary, TranscriptContext,
};
use chrono::{DateTime, Local, Utc};

#[derive(Debug, Clone, Default)]
pub struct ViewMetadata {
    pub ref_id: Option<String>,
    pub source: Option<SourceRecord>,
    pub raw_artifact: Option<RawArtifactSummary>,
    pub verbose: bool,
}

pub fn render_context(context: &TranscriptContext, metadata: &ViewMetadata, color: bool) -> String {
    let mut out = String::new();
    push_view_header(
        &mut out,
        "Show",
        &context.session,
        Some(&context.target_event),
        metadata,
    );
    out.push('\n');
    for (idx, event) in context.events.iter().enumerate() {
        push_event(
            &mut out,
            event,
            idx == context.target_index,
            color,
            metadata.verbose,
            RenderMode::Preview,
        );
    }
    out
}

pub fn render_history_context(
    context: &HistoryTranscriptContext,
    metadata: &ViewMetadata,
    color: bool,
) -> String {
    let mut out = String::new();
    push_view_header(
        &mut out,
        "Transcript",
        &context.session,
        context.target_event.as_ref(),
        metadata,
    );
    out.push('\n');
    if context.omitted_target {
        push_omitted_target(&mut out, color);
    }
    for (idx, item) in context.items.iter().enumerate() {
        push_history_item(
            &mut out,
            item,
            context.target_index == Some(idx),
            color,
            metadata.verbose,
        );
    }
    out
}

pub fn render_session(
    session: &SessionRecord,
    events: &[EventRecord],
    target_event_id: Option<&str>,
    metadata: &ViewMetadata,
    color: bool,
) -> String {
    let mut out = String::new();
    let target_event =
        target_event_id.and_then(|event_id| events.iter().find(|event| event.id == event_id));
    push_view_header(&mut out, "Transcript", session, target_event, metadata);
    out.push('\n');
    for event in events {
        push_event(
            &mut out,
            event,
            target_event_id == Some(event.id.as_str()),
            color,
            metadata.verbose,
            RenderMode::Full,
        );
    }
    out
}

pub fn render_history_session(
    context: &HistoryTranscriptContext,
    metadata: &ViewMetadata,
    color: bool,
) -> String {
    render_history_context(context, metadata, color)
}

pub fn render_history_items(items: &[HistoryItemRecord], color: bool, verbose: bool) -> String {
    let mut out = String::new();
    for item in items {
        push_history_item(&mut out, item, false, color, verbose);
    }
    out
}

fn push_view_header(
    out: &mut String,
    label: &str,
    session: &SessionRecord,
    target_event: Option<&EventRecord>,
    metadata: &ViewMetadata,
) {
    out.push_str(label);
    out.push('\n');
    if let Some(ref_id) = &metadata.ref_id {
        push_field(out, "ref", ref_id);
    }
    out.push_str("source: ");
    out.push_str(&session.source_kind);
    if metadata.verbose {
        if let Some(source) = &metadata.source {
            out.push_str(" identity:");
            out.push_str(&source.identity);
            if let Some(path) = &source.path {
                out.push_str(" path:");
                out.push_str(path);
            }
        }
    }
    out.push('\n');
    if let Some(when) = target_event
        .and_then(|event| event.occurred_at)
        .or(session.updated_at)
        .or(session.started_at)
    {
        if metadata.verbose {
            push_field(out, "when", &when.to_rfc3339());
        } else {
            push_field(out, "when", &format_local_timestamp_seconds(when));
        }
    }
    if let Some(title) = &session.title {
        out.push_str("title: ");
        out.push_str(title);
        out.push('\n');
    }
    push_field(out, "provider_thread", &session.external_id);
    push_field(out, "histo_session", &session.id);
    if let Some(event) = target_event {
        push_field(out, "event", &format!("#{}", event.ordinal));
    }
    if metadata.verbose {
        push_field(out, "source_id", &session.source_id);
        push_field(out, "machine_id", &session.machine_id);
        push_field(out, "session_hash", &session.hash);
        if let Some(event) = target_event {
            push_field(out, "super_event", &event.id);
            push_field(out, "event_hash", &event.hash);
            push_field(out, "event_type", &event.event_type);
            if let Some(role) = &event.role {
                push_field(out, "role", role);
            }
            if let Some(raw_hash) = &event.raw_artifact_hash {
                push_field(out, "raw_artifact", raw_hash);
            }
        }
        if let Some(raw) = &metadata.raw_artifact {
            push_field(out, "raw_hash", &raw.hash);
            push_field(out, "raw_path", &raw.path);
            push_field(out, "raw_media_type", &raw.media_type);
            push_field(out, "raw_size", &raw.size.to_string());
            push_field(out, "raw_first_seen", &raw.first_seen_at.to_rfc3339());
        }
    }
}

fn push_field(out: &mut String, label: &str, value: &str) {
    out.push_str(label);
    out.push_str(": ");
    out.push_str(value);
    out.push('\n');
}

#[derive(Clone, Copy)]
enum RenderMode {
    Preview,
    Full,
}

fn push_event(
    out: &mut String,
    event: &EventRecord,
    target: bool,
    color: bool,
    verbose: bool,
    mode: RenderMode,
) {
    let marker = if target { "=> " } else { "   " };
    if target && color {
        out.push_str("\x1b[1;36m");
    }
    out.push_str(marker);
    if verbose {
        out.push_str("event:");
        out.push_str(&event.id);
        out.push_str(" histo_session:");
        out.push_str(&event.session_id);
        out.push_str(" ordinal:");
        out.push_str(&event.ordinal.to_string());
        out.push_str(" source:");
        out.push_str(&event.source_kind);
        out.push_str(" type:");
        out.push_str(&event.event_type);
        if let Some(role) = &event.role {
            out.push_str(" role:");
            out.push_str(role);
        }
        if let Some(raw_hash) = &event.raw_artifact_hash {
            out.push_str(" raw:");
            out.push_str(raw_hash);
        }
        if let Some(occurred_at) = event.occurred_at {
            out.push_str(" when:");
            out.push_str(&occurred_at.to_rfc3339());
        }
    } else {
        out.push('#');
        out.push_str(&event.ordinal.to_string());
        out.push(' ');
        out.push_str(&event.source_kind);
        if let Some(role) = &event.role {
            out.push(' ');
            out.push_str(role);
        }
        if let Some(occurred_at) = event.occurred_at {
            out.push(' ');
            out.push_str(&format_local_timestamp_minutes(occurred_at));
        }
    }
    if target && color {
        out.push_str("\x1b[0m");
    }
    out.push('\n');
    if should_compact_preview_event(event, target, mode) {
        out.push_str(
            "[non-message event omitted from context preview; use transcript for full raw transcript]",
        );
    } else {
        out.push_str(&event.content);
    }
    out.push_str("\n\n");
}

fn push_history_item(
    out: &mut String,
    item: &HistoryItemRecord,
    target: bool,
    color: bool,
    verbose: bool,
) {
    let marker = if target { "=> " } else { "   " };
    if target && color {
        out.push_str("\x1b[1;36m");
    }
    out.push_str(marker);
    if verbose {
        out.push_str("history_item:");
        out.push_str(&item.id);
        out.push_str(" event:");
        out.push_str(&item.event_id);
        out.push_str(" histo_session:");
        out.push_str(&item.session_id);
        out.push_str(" ordinal:");
        out.push_str(&item.ordinal.to_string());
        out.push_str(" tier:");
        out.push_str(&item.tier);
        out.push_str(" kind:");
        out.push_str(&item.kind);
        if let Some(occurred_at) = item.occurred_at {
            out.push_str(" when:");
            out.push_str(&occurred_at.to_rfc3339());
        }
    } else {
        out.push('#');
        out.push_str(&item.ordinal.to_string());
        out.push(' ');
        out.push_str(&item.source_kind);
        out.push(' ');
        out.push_str(&item.kind);
        if let Some(occurred_at) = item.occurred_at {
            out.push(' ');
            out.push_str(&format_local_timestamp_minutes(occurred_at));
        }
    }
    if target && color {
        out.push_str("\x1b[0m");
    }
    out.push('\n');
    out.push_str(&item.text);
    out.push_str("\n\n");
}

fn push_omitted_target(out: &mut String, color: bool) {
    if color {
        out.push_str("\x1b[1;36m");
    }
    out.push_str(
        "=> [target event omitted from clean transcript; use --full to inspect raw event]",
    );
    if color {
        out.push_str("\x1b[0m");
    }
    out.push_str("\n\n");
}

fn should_compact_preview_event(event: &EventRecord, target: bool, mode: RenderMode) -> bool {
    matches!(mode, RenderMode::Preview)
        && !target
        && event.role.is_none()
        && (event.content.trim_start().starts_with('{') || event.content.chars().count() > 1200)
}

fn format_local_timestamp_seconds(value: DateTime<Utc>) -> String {
    value
        .with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn format_local_timestamp_minutes(value: DateTime<Utc>) -> String {
    value
        .with_timezone(&Local)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{stable_hash, EventRecord, SessionRecord};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn context_marks_target_and_preserves_full_content() {
        let session = fixture_session();
        let target = fixture_event("event_two", 2, "middle full content\nwith newline");
        let context = TranscriptContext {
            session,
            target_event: target.clone(),
            events: vec![
                fixture_event("event_one", 1, "first"),
                target,
                fixture_event("event_three", 3, "third"),
            ],
            target_index: 1,
        };

        let rendered = render_context(&context, &ViewMetadata::default(), false);

        assert!(rendered.contains("Show"));
        assert!(rendered.contains("provider_thread: external_session_test"));
        assert!(rendered.contains("histo_session: session_test"));
        assert!(rendered.contains("event: #2"));
        assert!(rendered.contains("=> #2 codex assistant"));
        assert!(rendered.contains("middle full content\nwith newline"));
    }

    #[test]
    fn context_compacts_neighboring_raw_events() {
        let session = fixture_session();
        let context = TranscriptContext {
            session,
            target_event: fixture_event("event_two", 2, "target"),
            events: vec![
                fixture_raw_event("event_raw", 1, "{\"payload\":{\"arguments\":\"large\"}}"),
                fixture_event("event_two", 2, "target"),
            ],
            target_index: 1,
        };

        let rendered = render_context(&context, &ViewMetadata::default(), false);

        assert!(rendered.contains("#1 codex"));
        assert!(rendered.contains("non-message event omitted"));
        assert!(!rendered.contains("\"arguments\""));
        assert!(rendered.contains("target"));
    }

    #[test]
    fn history_context_renders_clean_items_and_omitted_target_marker() {
        let session = fixture_session();
        let target = fixture_event("event_raw", 2, "{\"payload\":\"hidden\"}");
        let context = HistoryTranscriptContext {
            session,
            target_event: Some(target),
            items: vec![
                fixture_history_item("item_user", "event_user", 1, "user", "please fix this"),
                fixture_history_item(
                    "item_assistant",
                    "event_assistant",
                    3,
                    "assistant",
                    "I fixed it",
                ),
            ],
            target_index: None,
            omitted_target: true,
        };

        let rendered = render_history_context(&context, &ViewMetadata::default(), false);

        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("target event omitted from clean transcript"));
        assert!(rendered.contains("#1 codex user"));
        assert!(rendered.contains("please fix this"));
        assert!(rendered.contains("#3 codex assistant"));
        assert!(rendered.contains("I fixed it"));
        assert!(!rendered.contains("{\"payload\":\"hidden\"}"));
    }

    #[test]
    fn session_renderer_marks_requested_event() {
        let session = fixture_session();
        let rendered = render_session(
            &session,
            &[
                fixture_event("event_one", 1, "first"),
                fixture_event("event_two", 2, "second"),
            ],
            Some("event_two"),
            &ViewMetadata::default(),
            false,
        );

        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("   #1 codex"));
        assert!(rendered.contains("=> #2 codex"));
    }

    #[test]
    fn verbose_headers_include_internal_origin_fields() {
        let session = fixture_session();
        let event = fixture_raw_event("event_two", 2, "target");
        let context = TranscriptContext {
            session,
            target_event: event.clone(),
            events: vec![event],
            target_index: 0,
        };
        let rendered = render_context(
            &context,
            &ViewMetadata {
                ref_id: Some("ab3f".to_string()),
                verbose: true,
                ..ViewMetadata::default()
            },
            false,
        );

        assert!(rendered.contains("ref: ab3f"));
        assert!(rendered.contains("provider_thread: external_session_test"));
        assert!(rendered.contains("histo_session: session_test"));
        assert!(rendered.contains("super_event: event_two"));
        assert!(rendered.contains("event_type: message"));
        assert!(rendered.contains("=> event:event_two histo_session:session_test ordinal:2"));
    }

    fn fixture_session() -> SessionRecord {
        SessionRecord {
            id: "session_test".to_string(),
            source_id: "source_test".to_string(),
            machine_id: "machine_test".to_string(),
            source_kind: "codex".to_string(),
            external_id: "external_session_test".to_string(),
            title: Some("Fixture Session".to_string()),
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({}),
            hash: "session_hash".to_string(),
        }
    }

    fn fixture_event(id: &str, ordinal: i64, content: &str) -> EventRecord {
        EventRecord {
            id: id.to_string(),
            session_id: "session_test".to_string(),
            source_id: "source_test".to_string(),
            machine_id: "machine_test".to_string(),
            source_kind: "codex".to_string(),
            ordinal,
            event_type: "message".to_string(),
            role: Some("assistant".to_string()),
            content: content.to_string(),
            raw_artifact_hash: None,
            occurred_at: Some(Utc::now()),
            metadata: json!({}),
            hash: stable_hash(&(id, ordinal, content)).expect("event hash"),
        }
    }

    fn fixture_raw_event(id: &str, ordinal: i64, content: &str) -> EventRecord {
        let mut event = fixture_event(id, ordinal, content);
        event.role = None;
        event
    }

    fn fixture_history_item(
        id: &str,
        event_id: &str,
        ordinal: i64,
        kind: &str,
        text: &str,
    ) -> HistoryItemRecord {
        HistoryItemRecord {
            id: id.to_string(),
            event_id: event_id.to_string(),
            session_id: "session_test".to_string(),
            source_id: "source_test".to_string(),
            machine_id: "machine_test".to_string(),
            source_kind: "codex".to_string(),
            ordinal,
            subordinal: 0,
            tier: "conversation".to_string(),
            kind: kind.to_string(),
            text: text.to_string(),
            text_hash: stable_hash(&(id, text)).expect("text hash"),
            occurred_at: None,
            lexical_indexable: true,
            semantic_policy: "required".to_string(),
            metadata: json!({}),
            hash: stable_hash(&(id, event_id, text)).expect("history item hash"),
        }
    }
}
