use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ARCHIVE_SCHEMA: &str = "super-cass.archive.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawArtifact {
    pub hash: String,
    pub source_id: String,
    pub path: String,
    pub size: u64,
    pub mtime_ms: Option<i64>,
    pub media_type: String,
    pub content: Vec<u8>,
    pub first_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: String,
    pub source_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub external_id: String,
    pub title: Option<String>,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: String,
    pub session_id: String,
    pub source_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub ordinal: i64,
    pub event_type: String,
    pub role: Option<String>,
    pub content: String,
    pub raw_artifact_hash: Option<String>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum ArchiveRecord {
    RawArtifact(RawArtifact),
    Session(SessionRecord),
    Event(EventRecord),
}

impl ArchiveRecord {
    pub fn id(&self) -> &str {
        match self {
            Self::RawArtifact(record) => &record.hash,
            Self::Session(record) => &record.id,
            Self::Event(record) => &record.id,
        }
    }

    pub fn hash(&self) -> &str {
        match self {
            Self::RawArtifact(record) => &record.hash,
            Self::Session(record) => &record.hash,
            Self::Event(record) => &record.hash,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveEnvelope {
    pub schema: String,
    pub id: String,
    pub hash: String,
    pub producer: String,
    pub produced_at: DateTime<Utc>,
    #[serde(flatten)]
    pub record: ArchiveRecord,
}

impl ArchiveEnvelope {
    pub fn new(record: ArchiveRecord) -> Self {
        Self {
            schema: ARCHIVE_SCHEMA.to_string(),
            id: record.id().to_string(),
            hash: record.hash().to_string(),
            producer: format!("super-cass/{}", env!("CARGO_PKG_VERSION")),
            produced_at: Utc::now(),
            record,
        }
    }
}

pub fn blake3_hex(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub fn stable_hash<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(blake3_hex(&bytes))
}

pub fn stable_id(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("sc_{}", hasher.finalize().to_hex())
}

