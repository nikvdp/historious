use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ARCHIVE_SCHEMA: &str = "historious.archive.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawArtifact {
    pub hash: String,
    pub source_id: String,
    pub path: String,
    pub size: u64,
    pub mtime_ms: Option<i64>,
    pub media_type: String,
    #[serde(with = "base64_bytes")]
    pub content: Vec<u8>,
    pub first_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    pub id: String,
    pub kind: String,
    pub identity: String,
    pub path: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub hash: String,
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
pub struct SearchUnitRecord {
    pub id: String,
    pub event_id: String,
    pub session_id: String,
    pub source_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub role: Option<String>,
    pub search_kind: String,
    pub text: String,
    pub text_hash: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingRecord {
    pub id: String,
    pub unit_id: String,
    pub text_hash: String,
    pub model_id: String,
    pub dims: u32,
    pub vector_hash: String,
    #[serde(with = "base64_bytes")]
    pub vector: Vec<u8>,
    pub producer_machine_id: String,
    pub embedded_at: DateTime<Utc>,
    pub metadata: Value,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum ArchiveRecord {
    Source(SourceRecord),
    RawArtifact(RawArtifact),
    Session(SessionRecord),
    Event(EventRecord),
    SearchUnit(SearchUnitRecord),
    Embedding(EmbeddingRecord),
}

impl ArchiveRecord {
    pub fn id(&self) -> &str {
        match self {
            Self::Source(record) => &record.id,
            Self::RawArtifact(record) => &record.hash,
            Self::Session(record) => &record.id,
            Self::Event(record) => &record.id,
            Self::SearchUnit(record) => &record.id,
            Self::Embedding(record) => &record.id,
        }
    }

    pub fn hash(&self) -> &str {
        match self {
            Self::Source(record) => &record.hash,
            Self::RawArtifact(record) => &record.hash,
            Self::Session(record) => &record.hash,
            Self::Event(record) => &record.hash,
            Self::SearchUnit(record) => &record.hash,
            Self::Embedding(record) => &record.hash,
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
            producer: format!("historious/{}", env!("CARGO_PKG_VERSION")),
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

mod base64_bytes {
    use base64::Engine;
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BytesVisitor)
    }

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a base64 string")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_str(&value)
        }
    }
}
