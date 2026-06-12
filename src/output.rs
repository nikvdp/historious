use anyhow::Error;
use serde::Serialize;
use std::io::{self, Write};
use std::time::Instant;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct EnvelopeOptions {
    pub warnings: Vec<String>,
    pub hints: Vec<String>,
    pub degraded_reason: Option<String>,
    pub started_at: Option<Instant>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct SuccessEnvelope<T: Serialize> {
    pub success: bool,
    pub command: String,
    pub schema_version: u32,
    pub data: T,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded_reason: Option<String>,
    pub metadata: OutputMetadata,
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub success: bool,
    pub command: String,
    pub schema_version: u32,
    pub error: ErrorPayload,
    pub metadata: OutputMetadata,
}

#[derive(Debug, Serialize)]
pub struct ErrorPayload {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OutputMetadata {
    pub version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_ms: Option<u128>,
}

#[allow(dead_code)]
pub fn success_envelope<T: Serialize>(
    command: impl Into<String>,
    data: T,
    options: EnvelopeOptions,
) -> SuccessEnvelope<T> {
    SuccessEnvelope {
        success: true,
        command: command.into(),
        schema_version: SCHEMA_VERSION,
        data,
        warnings: options.warnings,
        hints: options.hints,
        degraded_reason: options.degraded_reason,
        metadata: metadata(options.started_at),
    }
}

pub fn error_envelope(
    command: impl Into<String>,
    error: &Error,
    kind: Option<String>,
    hint: Option<String>,
    started_at: Option<Instant>,
) -> ErrorEnvelope {
    ErrorEnvelope {
        success: false,
        command: command.into(),
        schema_version: SCHEMA_VERSION,
        error: ErrorPayload {
            message: format!("{error:#}"),
            kind,
            hint,
        },
        metadata: metadata(started_at),
    }
}

#[allow(dead_code)]
pub fn write_success<T: Serialize>(
    command: impl Into<String>,
    data: T,
    options: EnvelopeOptions,
) -> anyhow::Result<()> {
    write_json(&success_envelope(command, data, options))
}

pub fn write_error(
    command: impl Into<String>,
    error: &Error,
    kind: Option<String>,
    hint: Option<String>,
    started_at: Option<Instant>,
) -> anyhow::Result<()> {
    write_json(&error_envelope(command, error, kind, hint, started_at))
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, value)?;
    writeln!(handle)?;
    handle.flush()?;
    Ok(())
}

fn metadata(started_at: Option<Instant>) -> OutputMetadata {
    OutputMetadata {
        version: env!("CARGO_PKG_VERSION"),
        execution_ms: started_at.map(|instant| instant.elapsed().as_millis()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use serde_json::json;

    #[test]
    fn success_envelope_has_stable_shape() {
        let envelope = success_envelope(
            "search",
            json!({"results": []}),
            EnvelopeOptions {
                hints: vec!["histo show <ref> --json".to_string()],
                degraded_reason: Some("none".to_string()),
                ..EnvelopeOptions::default()
            },
        );
        let value = serde_json::to_value(envelope).expect("serialize");

        assert_eq!(value["success"], true);
        assert_eq!(value["command"], "search");
        assert_eq!(value["schema_version"], SCHEMA_VERSION);
        assert_eq!(value["data"]["results"], json!([]));
        assert_eq!(value["hints"][0], "histo show <ref> --json");
        assert_eq!(value["degraded_reason"], "none");
        assert_eq!(value["metadata"]["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn error_envelope_has_stable_shape() {
        let err = anyhow!("event/ref not found: nope");
        let envelope = error_envelope(
            "show",
            &err,
            Some("not_found".to_string()),
            Some("Run `histo search <query> --json` first.".to_string()),
            None,
        );
        let value = serde_json::to_value(envelope).expect("serialize");

        assert_eq!(value["success"], false);
        assert_eq!(value["command"], "show");
        assert_eq!(value["schema_version"], SCHEMA_VERSION);
        assert_eq!(value["error"]["message"], "event/ref not found: nope");
        assert_eq!(value["error"]["kind"], "not_found");
        assert_eq!(
            value["error"]["hint"],
            "Run `histo search <query> --json` first."
        );
    }
}
