use crate::archive::{EventRecord, SessionRecord};
use crate::storage::TranscriptContext;

pub fn render_context(context: &TranscriptContext, color: bool) -> String {
    let mut out = String::new();
    push_session_header(&mut out, &context.session, "Context");
    out.push_str("target: event:");
    out.push_str(&context.target_event.id);
    out.push('\n');
    out.push('\n');
    for (idx, event) in context.events.iter().enumerate() {
        push_event(
            &mut out,
            event,
            idx == context.target_index,
            color,
            RenderMode::Preview,
        );
    }
    out
}

pub fn render_session(
    session: &SessionRecord,
    events: &[EventRecord],
    target_event_id: Option<&str>,
    color: bool,
) -> String {
    let mut out = String::new();
    push_session_header(&mut out, session, "Transcript");
    out.push('\n');
    for event in events {
        push_event(
            &mut out,
            event,
            target_event_id == Some(event.id.as_str()),
            color,
            RenderMode::Full,
        );
    }
    out
}

fn push_session_header(out: &mut String, session: &SessionRecord, label: &str) {
    out.push_str(label);
    out.push('\n');
    out.push_str("session: ");
    out.push_str(&session.id);
    out.push('\n');
    out.push_str("source: ");
    out.push_str(&session.source_kind);
    out.push('\n');
    if let Some(title) = &session.title {
        out.push_str("title: ");
        out.push_str(title);
        out.push('\n');
    }
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
    mode: RenderMode,
) {
    let marker = if target { "=> " } else { "   " };
    if target && color {
        out.push_str("\x1b[1;36m");
    }
    out.push_str(marker);
    out.push_str("event:");
    out.push_str(&event.id);
    out.push_str(" ordinal:");
    out.push_str(&event.ordinal.to_string());
    out.push_str(" source:");
    out.push_str(&event.source_kind);
    if let Some(role) = &event.role {
        out.push_str(" role:");
        out.push_str(role);
    }
    if let Some(occurred_at) = event.occurred_at {
        out.push_str(" when:");
        out.push_str(&occurred_at.to_rfc3339());
    }
    if target && color {
        out.push_str("\x1b[0m");
    }
    out.push('\n');
    if should_compact_preview_event(event, target, mode) {
        out.push_str("[non-message event omitted from context preview; use show for full raw transcript]");
    } else {
        out.push_str(&event.content);
    }
    out.push_str("\n\n");
}

fn should_compact_preview_event(event: &EventRecord, target: bool, mode: RenderMode) -> bool {
    matches!(mode, RenderMode::Preview)
        && !target
        && event.role.is_none()
        && (event.content.trim_start().starts_with('{') || event.content.chars().count() > 1200)
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

        let rendered = render_context(&context, false);

        assert!(rendered.contains("Context"));
        assert!(rendered.contains("session: session_test"));
        assert!(rendered.contains("=> event:event_two ordinal:2"));
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

        let rendered = render_context(&context, false);

        assert!(rendered.contains("event:event_raw"));
        assert!(rendered.contains("non-message event omitted"));
        assert!(!rendered.contains("\"arguments\""));
        assert!(rendered.contains("target"));
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
            false,
        );

        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("   event:event_one"));
        assert!(rendered.contains("=> event:event_two"));
    }

    fn fixture_session() -> SessionRecord {
        SessionRecord {
            id: "session_test".to_string(),
            source_id: "source_test".to_string(),
            machine_id: "machine_test".to_string(),
            source_kind: "codex".to_string(),
            external_id: "session_test".to_string(),
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
}
