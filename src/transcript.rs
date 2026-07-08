use crate::archive::{EventRecord, SessionRecord, SourceRecord};
use crate::storage::{
    HistoryItemRecord, HistoryTranscriptContext, RawArtifactSummary, TranscriptContext,
};
use chrono::{DateTime, Local, Utc};

#[derive(Debug, Clone)]
pub struct ViewMetadata {
    pub ref_id: Option<String>,
    pub source: Option<SourceRecord>,
    pub raw_artifact: Option<RawArtifactSummary>,
    pub verbose: bool,
    pub timestamps: bool,
}

impl Default for ViewMetadata {
    fn default() -> Self {
        Self {
            ref_id: None,
            source: None,
            raw_artifact: None,
            verbose: false,
            timestamps: true,
        }
    }
}

pub fn render_context(context: &TranscriptContext, metadata: &ViewMetadata, color: bool) -> String {
    let mut out = String::new();
    push_markdown_header(
        &mut out,
        "Show",
        &context.session,
        Some(&context.target_event),
        metadata,
    );
    for (idx, event) in context.events.iter().enumerate() {
        push_event(
            &mut out,
            event,
            idx == context.target_index,
            color,
            metadata.verbose,
            metadata.timestamps,
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
    push_markdown_header(
        &mut out,
        "Transcript",
        &context.session,
        context.target_event.as_ref(),
        metadata,
    );
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
            metadata.timestamps,
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
    push_markdown_header(&mut out, "Transcript", session, target_event, metadata);
    for event in events {
        push_event(
            &mut out,
            event,
            target_event_id == Some(event.id.as_str()),
            color,
            metadata.verbose,
            metadata.timestamps,
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

pub fn render_history_items(
    items: &[HistoryItemRecord],
    color: bool,
    verbose: bool,
    timestamps: bool,
) -> String {
    let mut out = String::new();
    for item in items {
        push_history_item(&mut out, item, false, color, verbose, timestamps);
    }
    out
}

/// Render a single clean history item as Markdown, without a transcript-level header.
/// Used by slice selectors (--only, --last, --last-answer) that print one item to stdout.
pub fn render_single_history_item(
    item: &HistoryItemRecord,
    color: bool,
    verbose: bool,
    timestamps: bool,
) -> String {
    let mut out = String::new();
    push_history_item(&mut out, item, false, color, verbose, timestamps);
    out
}

fn push_markdown_header(
    out: &mut String,
    label: &str,
    session: &SessionRecord,
    target_event: Option<&EventRecord>,
    metadata: &ViewMetadata,
) {
    out.push_str("# ");
    out.push_str(label);
    out.push_str("\n\n");
    if let Some(ref_id) = &metadata.ref_id {
        push_meta_line(out, "ref", ref_id);
    }
    push_meta_line(out, "source", &session.source_kind);
    if metadata.verbose {
        if let Some(source) = &metadata.source {
            let mut detail = format!("identity:{}", source.identity);
            if let Some(path) = &source.path {
                detail.push_str(" path:");
                detail.push_str(path);
            }
            push_meta_line(out, "source_detail", &detail);
        }
    }
    if let Some(when) = target_event
        .and_then(|event| event.occurred_at)
        .or(session.updated_at)
        .or(session.started_at)
    {
        if metadata.timestamps {
            if metadata.verbose {
                push_meta_line(out, "when", &when.to_rfc3339());
            } else {
                push_meta_line(out, "when", &format_local_timestamp_seconds(when));
            }
        }
    }
    if let Some(title) = &session.title {
        push_meta_line(out, "title", title);
    }
    push_meta_line(out, "provider_thread", &session.external_id);
    push_meta_line(out, "histo_session", &session.id);
    if let Some(event) = target_event {
        push_meta_line(out, "event", &format!("#{}", event.ordinal));
    }
    if metadata.verbose {
        push_meta_line(out, "source_id", &session.source_id);
        push_meta_line(out, "machine_id", &session.machine_id);
        push_meta_line(out, "session_hash", &session.hash);
        if let Some(event) = target_event {
            push_meta_line(out, "super_event", &event.id);
            push_meta_line(out, "event_hash", &event.hash);
            push_meta_line(out, "event_type", &event.event_type);
            if let Some(role) = &event.role {
                push_meta_line(out, "role", role);
            }
            if let Some(raw_hash) = &event.raw_artifact_hash {
                push_meta_line(out, "raw_artifact", raw_hash);
            }
        }
        if let Some(raw) = &metadata.raw_artifact {
            push_meta_line(out, "raw_hash", &raw.hash);
            push_meta_line(out, "raw_path", &raw.path);
            push_meta_line(out, "raw_media_type", &raw.media_type);
            push_meta_line(out, "raw_size", &raw.size.to_string());
            push_meta_line(out, "raw_first_seen", &raw.first_seen_at.to_rfc3339());
        }
    }
    out.push('\n');
}

fn push_meta_line(out: &mut String, label: &str, value: &str) {
    out.push_str("- **");
    out.push_str(label);
    out.push_str(":** ");
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
    timestamps: bool,
    mode: RenderMode,
) {
    let mut heading = String::from("## #");
    heading.push_str(&event.ordinal.to_string());
    heading.push(' ');
    heading.push_str(&event.source_kind);
    if let Some(role) = &event.role {
        heading.push(' ');
        heading.push_str(role);
    }
    if timestamps {
        if let Some(occurred_at) = event.occurred_at {
            heading.push_str(" · ");
            heading.push_str(&format_local_timestamp_minutes(occurred_at));
        }
    }
    if target {
        heading.push_str(" · selected");
    }
    if target && color {
        out.push_str("\x1b[1;36m");
        out.push_str(&heading);
        out.push_str("\x1b[0m");
    } else {
        out.push_str(&heading);
    }
    out.push('\n');
    if verbose {
        out.push('\n');
        push_meta_line(out, "event", &event.id);
        push_meta_line(out, "histo_session", &event.session_id);
        push_meta_line(out, "ordinal", &event.ordinal.to_string());
        push_meta_line(out, "source", &event.source_kind);
        push_meta_line(out, "type", &event.event_type);
        if let Some(role) = &event.role {
            push_meta_line(out, "role", role);
        }
        if let Some(raw_hash) = &event.raw_artifact_hash {
            push_meta_line(out, "raw", raw_hash);
        }
        if let Some(occurred_at) = event.occurred_at {
            push_meta_line(out, "when", &occurred_at.to_rfc3339());
        }
    }
    out.push('\n');
    if should_compact_preview_event(event, target, mode) {
        out.push_str(
            "> [non-message event omitted from context preview; use transcript for full raw transcript]",
        );
    } else if is_json_content(&event.content) {
        out.push_str("```json\n");
        out.push_str(&event.content);
        out.push_str("\n```");
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
    timestamps: bool,
) {
    let kind_display = capitalize_kind(&item.kind);
    let heading = build_item_heading(&kind_display, item.occurred_at, target, timestamps);
    if target && color {
        out.push_str("\x1b[1;36m");
        out.push_str(&heading);
        out.push_str("\x1b[0m");
    } else {
        out.push_str(&heading);
    }
    out.push('\n');
    if verbose {
        out.push('\n');
        push_meta_line(out, "history_item", &item.id);
        push_meta_line(out, "event", &item.event_id);
        push_meta_line(out, "histo_session", &item.session_id);
        push_meta_line(out, "ordinal", &item.ordinal.to_string());
        push_meta_line(out, "subordinal", &item.subordinal.to_string());
        push_meta_line(out, "tier", &item.tier);
        push_meta_line(out, "kind", &item.kind);
        if let Some(occurred_at) = item.occurred_at {
            push_meta_line(out, "when", &occurred_at.to_rfc3339());
        }
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
        "> [target event omitted from clean transcript; use --full to inspect raw event]",
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

fn is_json_content(content: &str) -> bool {
    content.trim_start().starts_with('{')
}

fn capitalize_kind(kind: &str) -> String {
    let mut chars = kind.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn build_item_heading(
    kind: &str,
    occurred_at: Option<DateTime<Utc>>,
    selected: bool,
    timestamps: bool,
) -> String {
    let mut h = String::from("## ");
    h.push_str(kind);
    if timestamps {
        if let Some(when) = occurred_at {
            h.push_str(" · ");
            h.push_str(&format_local_timestamp_minutes(when));
        }
    }
    if selected {
        h.push_str(" · selected");
    }
    h
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
        assert!(rendered.contains("# Show"));
        assert!(rendered.contains("**provider_thread:** external_session_test"));
        assert!(rendered.contains("**histo_session:** session_test"));
        assert!(rendered.contains("**event:** #2"));
        assert!(rendered.contains("## #2 codex assistant"));
        assert!(rendered.contains("· selected"));
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

        assert!(rendered.contains("## #1 codex"));
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

        assert!(rendered.contains("# Transcript"));
        assert!(rendered.contains("target event omitted from clean transcript"));
        assert!(rendered.contains("## User"));
        assert!(rendered.contains("please fix this"));
        assert!(rendered.contains("## Assistant"));
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

        assert!(rendered.contains("# Transcript"));
        assert!(rendered.contains("## #1 codex assistant"));
        assert!(rendered.contains("## #2 codex assistant"));
        assert!(rendered.contains("· selected"));
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

        assert!(rendered.contains("**ref:** ab3f"));
        assert!(rendered.contains("**provider_thread:** external_session_test"));
        assert!(rendered.contains("**histo_session:** session_test"));
        assert!(rendered.contains("**super_event:** event_two"));
        assert!(rendered.contains("**event_type:** message"));
        assert!(rendered.contains("**event:** event_two"));
        assert!(rendered.contains("**ordinal:** 2"));
    }

    #[test]
    fn history_context_omits_timestamps_when_disabled() {
        let session = fixture_session();
        let item_with_ts = {
            let mut item = fixture_history_item(
                "item_user",
                "event_user",
                1,
                "user",
                "hello",
            );
            item.occurred_at = Some(Utc::now());
            item
        };
        let context = HistoryTranscriptContext {
            session,
            target_event: None,
            items: vec![item_with_ts],
            target_index: None,
            omitted_target: false,
        };
        let rendered_with = render_history_context(
            &context,
            &ViewMetadata {
                timestamps: true,
                ..ViewMetadata::default()
            },
            false,
        );
        assert!(rendered_with.contains("## User ·"));

        let rendered_without = render_history_context(
            &context,
            &ViewMetadata {
                timestamps: false,
                ..ViewMetadata::default()
            },
            false,
        );
        assert!(rendered_without.contains("## User\n"));
        assert!(!rendered_without.contains("## User ·"));
    }

    #[test]
    fn event_omits_timestamps_when_disabled() {
        let session = fixture_session();
        let event = fixture_event("event_two", 2, "target");
        let context = TranscriptContext {
            session,
            target_event: event.clone(),
            events: vec![event],
            target_index: 0,
        };
        let rendered = render_context(
            &context,
            &ViewMetadata {
                timestamps: false,
                ..ViewMetadata::default()
            },
            false,
        );
        assert!(rendered.contains("## #2 codex assistant"));
        assert!(rendered.contains("· selected"));
        assert!(!rendered.contains("**when:**"));
        // Verify no timestamp date pattern in the heading (only · selected should appear)
        assert!(!rendered.contains("## #2 codex assistant · 20"));
    }

    #[test]
    fn render_single_history_item_produces_markdown_without_header() {
        let item = fixture_history_item(
            "item_assistant",
            "event_assistant",
            3,
            "assistant",
            "Here is the answer",
        );
        let rendered = render_single_history_item(&item, false, false, true);
        assert!(rendered.contains("## Assistant"));
        assert!(rendered.contains("Here is the answer"));
        assert!(!rendered.contains("# Transcript"));
        assert!(!rendered.contains("# Show"));
    }

    #[test]
    fn render_history_items_renders_multiple_items() {
        let items = vec![
            fixture_history_item("item_user", "event_user", 1, "user", "question"),
            fixture_history_item(
                "item_assistant",
                "event_assistant",
                2,
                "assistant",
                "answer",
            ),
        ];
        let rendered = render_history_items(&items, false, false, true);
        assert!(rendered.contains("## User"));
        assert!(rendered.contains("question"));
        assert!(rendered.contains("## Assistant"));
        assert!(rendered.contains("answer"));
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
