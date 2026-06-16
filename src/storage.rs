use crate::archive::{
    ArchiveRecord, EmbeddingRecord, EventRecord, RawArtifact, SearchUnitRecord, SessionRecord,
    SourceRecord,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{
    named_params, params, params_from_iter, types::Value as SqlValue, Connection,
    OptionalExtension, Transaction, TransactionBehavior,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

const RECENT_RESULT_REF_LIMIT: usize = 10_000;
const SQLITE_BIND_CHUNK_SIZE: usize = 500;
const SQLITE_BUSY_TIMEOUT_MS: u64 = 4_000;
const SEMANTIC_EMBEDDING_MIN_TEXT_CHARS: usize = 80;

#[derive(Debug, Clone)]
pub struct Store {
    db_path: PathBuf,
    blob_dir: PathBuf,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SourceCheckpoint {
    pub source_kind: String,
    pub source_identity: String,
    pub cursor: Option<String>,
    pub metadata: Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ImportStats {
    pub inserted: usize,
    pub duplicates: usize,
    pub vectors_indexed: usize,
    #[serde(skip)]
    pub delta: ImportDelta,
}

#[derive(Debug, Clone, Default)]
pub struct ImportDelta {
    pub inserted_sources: Vec<String>,
    pub inserted_raw_artifacts: Vec<String>,
    pub inserted_sessions: Vec<String>,
    pub inserted_events: Vec<String>,
    pub inserted_search_units: Vec<String>,
    pub inserted_embeddings: Vec<String>,
    pub touched_paths: Vec<String>,
    pub touched_sessions: Vec<String>,
    pub touched_events: Vec<String>,
    pub touched_search_units: Vec<String>,
    pub touched_embeddings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportDeltaMode {
    Full,
    InsertedOnly,
}

impl ImportDelta {
    pub fn merge(&mut self, other: ImportDelta) {
        extend_unique(&mut self.inserted_sources, other.inserted_sources);
        extend_unique(
            &mut self.inserted_raw_artifacts,
            other.inserted_raw_artifacts,
        );
        extend_unique(&mut self.inserted_sessions, other.inserted_sessions);
        extend_unique(&mut self.inserted_events, other.inserted_events);
        extend_unique(&mut self.inserted_search_units, other.inserted_search_units);
        extend_unique(&mut self.inserted_embeddings, other.inserted_embeddings);
        extend_unique(&mut self.touched_paths, other.touched_paths);
        extend_unique(&mut self.touched_sessions, other.touched_sessions);
        extend_unique(&mut self.touched_events, other.touched_events);
        extend_unique(&mut self.touched_search_units, other.touched_search_units);
        extend_unique(&mut self.touched_embeddings, other.touched_embeddings);
    }

    pub fn search_index_event_ids(&self) -> Vec<String> {
        let mut event_ids = self.inserted_events.clone();
        extend_unique(&mut event_ids, self.touched_events.clone());
        event_ids
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ArchiveStats {
    pub sources: u64,
    pub raw_artifacts: u64,
    pub sessions: u64,
    pub events: u64,
    pub history_items: u64,
    pub search_units: u64,
    pub embeddings: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PruneFilter {
    pub sources: Vec<String>,
    pub sessions: Vec<String>,
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    pub workspace_scope: Option<String>,
    pub machine_id: Option<String>,
    pub machine_id_prefix: Option<String>,
}

impl PruneFilter {
    pub fn has_selector(&self) -> bool {
        !self.sources.is_empty()
            || !self.sessions.is_empty()
            || self.after.is_some()
            || self.before.is_some()
            || self
                .workspace_scope
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            || self
                .machine_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            || self
                .machine_id_prefix
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PrunePlan {
    pub session_ids: Vec<String>,
    pub sessions: u64,
    pub events: u64,
    pub history_items: u64,
    pub search_units: u64,
    pub embeddings: u64,
    pub event_embeddings: u64,
    pub vector_rows: u64,
    pub recent_result_refs: u64,
    pub raw_artifacts: u64,
    pub raw_blob_bytes: u64,
    pub sources: u64,
    #[serde(skip)]
    pub raw_artifact_hashes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PruneOutcome {
    pub plan: PrunePlan,
    pub raw_blobs_deleted: usize,
    pub raw_blob_bytes_deleted: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SourceFileStatus {
    pub raw_current: bool,
    pub needs_workspace_refresh: bool,
}

#[derive(Debug, Clone)]
pub struct SourceFileFingerprint {
    pub path: String,
    pub size: u64,
    pub mtime_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct EventForProjection {
    pub id: String,
    pub session_id: String,
    pub source_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub role: Option<String>,
    pub search_kind: String,
    pub content: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub text_hash: String,
    pub fts_indexed: bool,
}

#[derive(Debug, Clone)]
pub struct HistoryItemRecord {
    pub id: String,
    pub event_id: String,
    pub session_id: String,
    pub source_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub ordinal: i64,
    pub subordinal: i64,
    pub tier: String,
    pub kind: String,
    pub text: String,
    pub text_hash: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub lexical_indexable: bool,
    pub semantic_policy: String,
    pub metadata: Value,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub history_item_id: Option<String>,
    pub event_id: String,
    pub session_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub tier: Option<String>,
    pub search_kind: String,
    pub content: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub session_title: Option<String>,
    pub workspace_values: Vec<String>,
    pub rank: usize,
}

#[derive(Debug, Clone)]
pub struct HistoryItemForEmbedding {
    pub id: String,
    pub text: String,
    pub text_hash: String,
    pub cursor: HistoryItemEmbeddingCursor,
}

#[derive(Debug, Clone)]
pub struct HistoryItemEmbeddingCursor {
    pub occurred_at_key: String,
    pub session_id: String,
    pub ordinal: i64,
    pub subordinal: i64,
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct VectorSearchRow {
    pub event_id: String,
    pub history_item_id: String,
    pub session_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub tier: String,
    pub search_kind: String,
    pub content: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub session_title: Option<String>,
    pub workspace_values: Vec<String>,
    pub distance: f64,
    pub rank: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadSortMode {
    Newest,
    Oldest,
}

#[derive(Debug, Clone)]
pub struct ThreadListOptions {
    pub limit: usize,
    pub sort: ThreadSortMode,
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    pub workspace_scope: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ThreadRow {
    pub session: SessionRecord,
    pub event_count: u64,
    pub first_event_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub workspace_path: Option<String>,
    pub workspace_values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TranscriptContext {
    pub session: SessionRecord,
    pub target_event: EventRecord,
    pub events: Vec<EventRecord>,
    pub target_index: usize,
}

#[derive(Debug, Clone)]
pub struct HistoryTranscriptContext {
    pub session: SessionRecord,
    pub target_event: Option<EventRecord>,
    pub items: Vec<HistoryItemRecord>,
    pub target_index: Option<usize>,
    pub omitted_target: bool,
}

#[derive(Debug, Clone)]
pub struct RawArtifactSummary {
    pub hash: String,
    pub path: String,
    pub size: u64,
    pub media_type: String,
    pub first_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RecentResultRefInput {
    pub event_id: String,
    pub session_id: String,
    pub source_kind: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub preview: String,
}

#[derive(Debug, Clone, Default)]
pub struct ArchiveExportFilter {
    pub sources: Vec<String>,
    pub workspaces: Vec<String>,
    pub sessions: Vec<String>,
    pub since: Option<DateTime<Utc>>,
}

impl ArchiveExportFilter {
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
            && self.workspaces.is_empty()
            && self.sessions.is_empty()
            && self.since.is_none()
    }
}

#[derive(Debug)]
struct SessionFilterRow {
    id: String,
    source_kind: String,
    started_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    latest_event_at: Option<DateTime<Utc>>,
    metadata: Value,
}

#[derive(Debug)]
struct PruneSessionFilterRow {
    id: String,
    source_kind: String,
    machine_id: String,
    started_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    latest_event_at: Option<DateTime<Utc>>,
    metadata: Value,
}

impl Store {
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("creating data dir {}", data_dir.display()))?;
        let db_path = data_dir.join("historious.db");
        let blob_dir = data_dir.join("blobs");
        std::fs::create_dir_all(&blob_dir)
            .with_context(|| format!("creating blob dir {}", blob_dir.display()))?;
        let store = Self { db_path, blob_dir };
        store.with_conn(|conn| {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            migrate(conn)
        })?;
        Ok(store)
    }

    pub fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        load_sqlite_vec();
        let conn = Connection::open(&self.db_path)
            .with_context(|| format!("opening database {}", self.db_path.display()))?;
        conn.busy_timeout(Duration::from_millis(SQLITE_BUSY_TIMEOUT_MS))?;
        f(&conn)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    #[cfg(test)]
    pub fn raw_artifact_blob_exists(&self, hash: &str) -> bool {
        blob_path(&self.blob_dir, hash).exists()
    }

    pub fn missing_raw_artifact_blob_hashes(
        &self,
        filter: &ArchiveExportFilter,
    ) -> Result<Vec<String>> {
        let session_ids = if filter.is_empty() {
            Vec::new()
        } else {
            self.session_ids_for_export_filter(filter)?
        };
        self.with_conn(|conn| {
            let hashes = if filter.is_empty() {
                raw_artifact_hashes(conn)?
            } else {
                raw_artifact_hashes_for_session_ids(conn, &session_ids)?
            };
            Ok(hashes
                .into_iter()
                .filter(|hash| !blob_path(&self.blob_dir, hash).exists())
                .collect())
        })
    }

    pub fn read_raw_artifact_blob(&self, hash: &str) -> Result<Vec<u8>> {
        self.with_conn(|conn| {
            let exists: i64 = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM raw_artifacts WHERE hash = ?1)",
                params![hash],
                |row| row.get(0),
            )?;
            if exists == 0 {
                bail!("raw artifact metadata not found: {hash}");
            }
            read_blob(&self.blob_dir, hash)
        })
    }

    pub fn write_raw_artifact_blob(&self, hash: &str, content: &[u8]) -> Result<bool> {
        let actual = crate::archive::blake3_hex(content);
        if actual != hash {
            bail!("raw artifact blob hash mismatch: expected {hash}, got {actual}");
        }
        self.with_conn(|conn| {
            let exists: i64 = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM raw_artifacts WHERE hash = ?1)",
                params![hash],
                |row| row.get(0),
            )?;
            if exists == 0 {
                bail!("raw artifact metadata not found: {hash}");
            }
            let already_present = blob_path(&self.blob_dir, hash).exists();
            if !already_present {
                write_blob(&self.blob_dir, hash, content)?;
            }
            Ok(!already_present)
        })
    }

    pub fn upsert_source(
        &self,
        id: &str,
        kind: &str,
        identity: &str,
        path: Option<&str>,
    ) -> Result<()> {
        self.with_conn(|conn| {
            let now = Utc::now();
            let source = SourceRecord {
                id: id.to_string(),
                kind: kind.to_string(),
                identity: identity.to_string(),
                path: path.map(ToOwned::to_owned),
                first_seen_at: now,
                updated_at: now,
                hash: crate::archive::stable_hash(&(id, kind, identity, path))?,
            };
            insert_source(conn, &source)?;
            Ok(())
        })
    }

    #[allow(dead_code)]
    pub fn source_checkpoint(
        &self,
        source_kind: &str,
        source_identity: &str,
    ) -> Result<Option<SourceCheckpoint>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT source_kind, source_identity, cursor, metadata_json, updated_at
                 FROM source_checkpoints
                 WHERE source_kind = ?1 AND source_identity = ?2",
                params![source_kind, source_identity],
                row_source_checkpoint,
            )
            .optional()
            .map_err(Into::into)
        })
    }

    #[allow(dead_code)]
    pub fn upsert_source_checkpoint(
        &self,
        source_kind: &str,
        source_identity: &str,
        cursor: Option<&str>,
        metadata: &Value,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO source_checkpoints
                 (source_kind, source_identity, cursor, metadata_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(source_kind, source_identity) DO UPDATE SET
                   cursor = excluded.cursor,
                   metadata_json = excluded.metadata_json,
                   updated_at = excluded.updated_at",
                params![
                    source_kind,
                    source_identity,
                    cursor,
                    metadata.to_string(),
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    #[cfg(test)]
    pub fn import_record(&self, record: &ArchiveRecord) -> Result<ImportStats> {
        self.import_records(std::slice::from_ref(record))
    }

    pub fn import_records(&self, records: &[ArchiveRecord]) -> Result<ImportStats> {
        self.import_records_with_delta_mode(records, ImportDeltaMode::Full)
    }

    pub fn import_archive_records(&self, records: &[ArchiveRecord]) -> Result<ImportStats> {
        self.import_records_with_delta_mode(records, ImportDeltaMode::InsertedOnly)
    }

    fn import_records_with_delta_mode(
        &self,
        records: &[ArchiveRecord],
        delta_mode: ImportDeltaMode,
    ) -> Result<ImportStats> {
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                let mut stats = ImportStats::default();
                for record in records {
                    let inserted = match record {
                        ArchiveRecord::Source(source) => insert_source(&tx, source)?,
                        ArchiveRecord::RawArtifact(raw) => {
                            insert_raw_artifact(&tx, raw, &self.blob_dir)?
                        }
                        ArchiveRecord::Session(session) => insert_session(&tx, session)?,
                        ArchiveRecord::Event(event) => insert_event(&tx, event)?,
                        ArchiveRecord::SearchUnit(unit) => insert_search_unit(&tx, unit)?,
                        ArchiveRecord::Embedding(embedding) => insert_embedding(&tx, embedding)?,
                    };
                    if inserted {
                        stats.inserted += 1;
                    } else {
                        stats.duplicates += 1;
                    }
                    record_delta(record, inserted, delta_mode, &mut stats.delta);
                }
                Ok(stats)
            })
        })
    }

    pub fn export_records(&self) -> Result<Vec<ArchiveRecord>> {
        self.export_records_with_raw_content(true)
    }

    pub fn export_records_with_raw_content(
        &self,
        include_raw_content: bool,
    ) -> Result<Vec<ArchiveRecord>> {
        self.with_conn(|conn| {
            let mut records = Vec::new();
            {
                let mut stmt = conn.prepare(
                    "SELECT id, kind, identity, path, first_seen_at, updated_at, hash
                     FROM sources ORDER BY id",
                )?;
                let rows = stmt.query_map([], row_source)?;
                for row in rows {
                    records.push(ArchiveRecord::Source(row?));
                }
            }
            {
                let mut stmt = conn.prepare(
                    "SELECT hash, source_id, path, size, mtime_ms, media_type, content, first_seen_at
                     FROM raw_artifacts ORDER BY first_seen_at, hash",
                )?;
                let rows = stmt.query_map([], row_raw_artifact)?;
                for row in rows {
                    let mut raw = row?;
                    if include_raw_content && raw.content.is_empty() {
                        raw.content = read_blob(&self.blob_dir, &raw.hash)?;
                    }
                    records.push(ArchiveRecord::RawArtifact(raw));
                }
            }
            {
                let mut stmt = conn.prepare(
                    "SELECT id, source_id, machine_id, source_kind, external_id, title, status,
                            started_at, updated_at, metadata_json, hash
                     FROM sessions ORDER BY id",
                )?;
                let rows = stmt.query_map([], row_session)?;
                for row in rows {
                    records.push(ArchiveRecord::Session(row?));
                }
            }
            {
                let mut stmt = conn.prepare(
                    "SELECT id, session_id, source_id, machine_id, source_kind, ordinal,
                            event_type, role, content, raw_artifact_hash, occurred_at,
                            metadata_json, hash
                     FROM events ORDER BY session_id, ordinal, id",
                )?;
                let rows = stmt.query_map([], row_event)?;
                for row in rows {
                    records.push(ArchiveRecord::Event(row?));
                }
            }
            {
                let mut stmt = conn.prepare(
                    "SELECT id, event_id, session_id, source_id, machine_id, source_kind, role,
                            search_kind, text, text_hash, occurred_at, metadata_json, hash
                     FROM search_units ORDER BY session_id, id",
                )?;
                let rows = stmt.query_map([], row_search_unit)?;
                for row in rows {
                    records.push(ArchiveRecord::SearchUnit(row?));
                }
            }
            {
                let mut stmt = conn.prepare(
                    "SELECT id, unit_id, text_hash, model_id, dims, vector_hash, vector,
                            producer_machine_id, embedded_at, metadata_json, hash
                     FROM embeddings ORDER BY model_id, unit_id",
                )?;
                let rows = stmt.query_map([], row_embedding)?;
                for row in rows {
                    records.push(ArchiveRecord::Embedding(row?));
                }
            }
            Ok(records)
        })
    }

    pub fn export_records_for_session_ids(
        &self,
        session_ids: &[String],
    ) -> Result<Vec<ArchiveRecord>> {
        self.export_records_for_session_ids_with_raw_content(session_ids, true)
    }

    pub fn export_records_for_session_ids_with_raw_content(
        &self,
        session_ids: &[String],
        include_raw_content: bool,
    ) -> Result<Vec<ArchiveRecord>> {
        let session_ids = normalized_ids(session_ids);
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_conn(|conn| {
            let mut records = Vec::new();
            let placeholders = placeholders(session_ids.len());
            {
                let sql = format!(
                    "SELECT id, kind, identity, path, first_seen_at, updated_at, hash
                     FROM sources
                     WHERE id IN (
                       SELECT source_id FROM sessions WHERE id IN ({placeholders})
                       UNION
                       SELECT source_id FROM events WHERE session_id IN ({placeholders})
                       UNION
                       SELECT source_id FROM search_units WHERE session_id IN ({placeholders})
                       UNION
                       SELECT source_id FROM raw_artifacts
                       WHERE hash IN (
                         SELECT raw_artifact_hash FROM events
                         WHERE session_id IN ({placeholders}) AND raw_artifact_hash IS NOT NULL
                       )
                     )
                     ORDER BY id"
                );
                let params = repeated_id_params(&session_ids, 4);
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map(params_from_iter(params), row_source)?;
                for row in rows {
                    records.push(ArchiveRecord::Source(row?));
                }
            }
            {
                let sql = format!(
                    "SELECT hash, source_id, path, size, mtime_ms, media_type, content, first_seen_at
                     FROM raw_artifacts
                     WHERE hash IN (
                       SELECT raw_artifact_hash FROM events
                       WHERE session_id IN ({placeholders}) AND raw_artifact_hash IS NOT NULL
                     )
                     ORDER BY first_seen_at, hash"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(params_from_iter(session_ids.iter().map(String::as_str)), row_raw_artifact)?;
                for row in rows {
                    let mut raw = row?;
                    if include_raw_content && raw.content.is_empty() {
                        raw.content = read_blob(&self.blob_dir, &raw.hash)?;
                    }
                    records.push(ArchiveRecord::RawArtifact(raw));
                }
            }
            {
                let sql = format!(
                    "SELECT id, source_id, machine_id, source_kind, external_id, title, status,
                            started_at, updated_at, metadata_json, hash
                     FROM sessions
                     WHERE id IN ({placeholders})
                     ORDER BY id"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(params_from_iter(session_ids.iter().map(String::as_str)), row_session)?;
                for row in rows {
                    records.push(ArchiveRecord::Session(row?));
                }
            }
            {
                let sql = format!(
                    "SELECT id, session_id, source_id, machine_id, source_kind, ordinal,
                            event_type, role, content, raw_artifact_hash, occurred_at,
                            metadata_json, hash
                     FROM events
                     WHERE session_id IN ({placeholders})
                     ORDER BY session_id, ordinal, id"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(params_from_iter(session_ids.iter().map(String::as_str)), row_event)?;
                for row in rows {
                    records.push(ArchiveRecord::Event(row?));
                }
            }
            {
                let sql = format!(
                    "SELECT id, event_id, session_id, source_id, machine_id, source_kind, role,
                            search_kind, text, text_hash, occurred_at, metadata_json, hash
                     FROM search_units
                     WHERE session_id IN ({placeholders})
                     ORDER BY session_id, id"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(params_from_iter(session_ids.iter().map(String::as_str)), row_search_unit)?;
                for row in rows {
                    records.push(ArchiveRecord::SearchUnit(row?));
                }
            }
            {
                let sql = format!(
                    "SELECT e.id, e.unit_id, e.text_hash, e.model_id, e.dims, e.vector_hash, e.vector,
                            e.producer_machine_id, e.embedded_at, e.metadata_json, e.hash
                     FROM embeddings e
                     JOIN history_items hi ON hi.id = e.unit_id
                     WHERE hi.session_id IN ({placeholders})
                     ORDER BY e.model_id, e.unit_id"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(params_from_iter(session_ids.iter().map(String::as_str)), row_embedding)?;
                for row in rows {
                    records.push(ArchiveRecord::Embedding(row?));
                }
            }
            Ok(records)
        })
    }

    #[allow(dead_code)]
    pub fn event_ids_for_hash(&self, hash: &str) -> Result<Vec<String>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id FROM events WHERE hash = ?1 ORDER BY id")?;
            let rows = stmt.query_map(params![hash], |row| row.get(0))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn session_ids_for_export_filter(
        &self,
        filter: &ArchiveExportFilter,
    ) -> Result<Vec<String>> {
        if filter.is_empty() {
            return Ok(Vec::new());
        }
        let sources = normalized_string_set(&filter.sources);
        let workspaces = normalized_string_set(&filter.workspaces);
        let sessions = normalized_string_set(&filter.sessions);
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT s.id,
                        s.source_kind,
                        s.started_at,
                        s.updated_at,
                        MAX(e.occurred_at),
                        s.metadata_json
                 FROM sessions s
                 LEFT JOIN events e ON e.session_id = s.id
                 GROUP BY s.id
                 ORDER BY s.id",
            )?;
            let rows = stmt.query_map([], |row| {
                let metadata: String = row.get(5)?;
                Ok(SessionFilterRow {
                    id: row.get(0)?,
                    source_kind: row.get(1)?,
                    started_at: parse_opt_dt(row.get(2)?),
                    updated_at: parse_opt_dt(row.get(3)?),
                    latest_event_at: parse_opt_dt(row.get(4)?),
                    metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                let row = row?;
                if session_matches_export_filter(
                    &row,
                    &sources,
                    &workspaces,
                    &sessions,
                    filter.since,
                ) {
                    out.push(row.id);
                }
            }
            Ok(out)
        })
    }

    pub fn stats(&self) -> Result<ArchiveStats> {
        self.with_conn(|conn| {
            Ok(ArchiveStats {
                sources: count(conn, "sources")?,
                raw_artifacts: count(conn, "raw_artifacts")?,
                sessions: count(conn, "sessions")?,
                events: count(conn, "events")?,
                history_items: count(conn, "history_items")?,
                search_units: count(conn, "search_units")?,
                embeddings: count(conn, "embeddings")?,
            })
        })
    }

    pub fn prune_plan(&self, filter: &PruneFilter) -> Result<PrunePlan> {
        self.with_conn(|conn| {
            let session_ids = prune_session_ids(conn, filter)?;
            prepare_prune_scope(conn, &session_ids)?;
            prune_plan_from_scope(conn, session_ids)
        })
    }

    pub fn prune(&self, filter: &PruneFilter) -> Result<PruneOutcome> {
        let (plan, raw_hashes) = self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                let session_ids = prune_session_ids(tx, filter)?;
                prepare_prune_scope(tx, &session_ids)?;
                let plan = prune_plan_from_scope(tx, session_ids)?;
                if plan.sessions == 0 {
                    return Ok((plan, Vec::new()));
                }
                let raw_hashes = plan.raw_artifact_hashes.clone();
                delete_prune_scope(tx)?;
                Ok((plan, raw_hashes))
            })
        })?;
        let (raw_blobs_deleted, raw_blob_bytes_deleted) =
            remove_raw_artifact_blobs(&self.blob_dir, &raw_hashes)?;
        Ok(PruneOutcome {
            plan,
            raw_blobs_deleted,
            raw_blob_bytes_deleted,
        })
    }

    pub fn vacuum(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute_batch("VACUUM")
                .context("compacting Historious SQLite database")?;
            Ok(())
        })
    }

    pub fn refresh_history_items(&self) -> Result<usize> {
        self.refresh_history_items_with_progress(|_, _| {})
    }

    pub fn refresh_history_items_with_progress(
        &self,
        mut progress: impl FnMut(usize, usize),
    ) -> Result<usize> {
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                tx.execute("DELETE FROM history_items_fts", [])?;
                tx.execute("DELETE FROM history_items_conversation_fts", [])?;
                tx.execute("DELETE FROM history_items", [])?;
                let total_events = count(tx, "events")? as usize;
                progress(0, total_events);
                let mut stmt = tx.prepare(
                    "SELECT id, session_id, source_id, machine_id, source_kind, ordinal,
                            event_type, role, content, raw_artifact_hash, occurred_at,
                            metadata_json, hash
                     FROM events
                     ORDER BY session_id, ordinal, id",
                )?;
                let rows = stmt.query_map([], row_event)?;
                let mut processed_events = 0usize;
                for row in rows {
                    processed_events += 1;
                    for item in history_items_from_event(&row?)? {
                        insert_history_item(&tx, &item)?;
                    }
                    if processed_events % 1_000 == 0 {
                        progress(processed_events, total_events);
                    }
                }
                if total_events > 0 || processed_events > 0 {
                    progress(processed_events, total_events);
                }
                drop(stmt);
                let count = count(tx, "history_items")? as usize;
                update_projection_status(tx, "history_items_v1", count)?;
                update_projection_status(tx, "history_items_conversation_fts_v1", count)?;
                Ok(count)
            })
        })
    }

    #[allow(dead_code)]
    pub fn refresh_history_items_for_events(&self, event_ids: &[String]) -> Result<usize> {
        self.refresh_history_items_for_events_with_progress(event_ids, |_, _| {})
    }

    pub fn refresh_history_items_for_events_with_progress(
        &self,
        event_ids: &[String],
        mut progress: impl FnMut(usize, usize),
    ) -> Result<usize> {
        let event_ids = normalized_ids(event_ids);
        if event_ids.is_empty() {
            return self.with_conn(|conn| {
                if let Some(count) = projection_status_count(conn, "history_items_v1")? {
                    Ok(count)
                } else {
                    Ok(count(conn, "history_items")? as usize)
                }
            });
        }
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                prepare_temp_id_scope(tx, "temp_history_item_event_ids", &event_ids)?;
                let total_events: usize = tx.query_row(
                    "SELECT COUNT(*)
                     FROM temp_history_item_event_ids scope
                     CROSS JOIN events e
                     WHERE e.id = scope.id",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                progress(0, total_events);
                let replaced_count: usize = tx.query_row(
                    "SELECT COUNT(*)
                     FROM history_items
                     WHERE event_id IN (SELECT id FROM temp_history_item_event_ids)",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                if replaced_count > 0 {
                    tx.execute(
                        "DELETE FROM history_items_fts
                         WHERE event_id IN (SELECT id FROM temp_history_item_event_ids)",
                        [],
                    )?;
                    tx.execute(
                        "DELETE FROM history_items_conversation_fts
                         WHERE event_id IN (SELECT id FROM temp_history_item_event_ids)",
                        [],
                    )?;
                    tx.execute(
                        "DELETE FROM history_items
                         WHERE event_id IN (SELECT id FROM temp_history_item_event_ids)",
                        [],
                    )?;
                }
                let mut stmt = tx.prepare(
                    "SELECT e.id, e.session_id, e.source_id, e.machine_id, e.source_kind,
                            e.ordinal, e.event_type, e.role, e.content, e.raw_artifact_hash,
                            e.occurred_at, e.metadata_json, e.hash
                     FROM temp_history_item_event_ids scope
                     CROSS JOIN events e
                     WHERE e.id = scope.id
                     ORDER BY e.session_id, e.ordinal, e.id",
                )?;
                let rows = stmt.query_map([], row_event)?;
                let mut inserted_count = 0usize;
                let mut processed_events = 0usize;
                for row in rows {
                    processed_events += 1;
                    for item in history_items_from_event(&row?)? {
                        if insert_history_item(&tx, &item)? {
                            inserted_count += 1;
                        }
                    }
                    if processed_events % 1_000 == 0 {
                        progress(processed_events, total_events);
                    }
                }
                if total_events > 0 || processed_events > 0 {
                    progress(processed_events, total_events);
                }
                drop(stmt);
                let count =
                    if let Some(current_count) = projection_status_count(tx, "history_items_v1")? {
                        current_count
                            .saturating_sub(replaced_count)
                            .saturating_add(inserted_count)
                    } else {
                        count(tx, "history_items")? as usize
                    };
                update_projection_status(tx, "history_items_v1", count)?;
                update_projection_status(tx, "history_items_conversation_fts_v1", count)?;
                Ok(count)
            })
        })
    }

    pub fn history_items_projection_ready(&self) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(projection_status_ready(conn, "history_items_v1")?
                && projection_status_ready(conn, "history_items_conversation_fts_v1")?)
        })
    }

    #[cfg(test)]
    pub fn history_items_for_event(&self, event_id: &str) -> Result<Vec<HistoryItemRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, event_id, session_id, source_id, machine_id, source_kind,
                        ordinal, subordinal, tier, kind, text, text_hash, occurred_at,
                        lexical_indexable, semantic_policy, metadata_json, hash
                 FROM history_items
                 WHERE event_id = ?1
                 ORDER BY subordinal, id",
            )?;
            let rows = stmt.query_map(params![event_id], row_history_item)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    #[cfg(test)]
    pub fn raw_artifact_is_current(
        &self,
        path: &str,
        size: u64,
        mtime_ms: Option<i64>,
    ) -> Result<bool> {
        self.with_conn(|conn| raw_artifact_is_current(conn, path, size, mtime_ms))
    }

    pub fn source_file_status(
        &self,
        path: &str,
        size: u64,
        mtime_ms: Option<i64>,
    ) -> Result<SourceFileStatus> {
        self.with_conn(|conn| {
            let raw_current = raw_artifact_is_current(conn, path, size, mtime_ms)?;
            let needs_workspace_refresh =
                raw_current && session_workspace_metadata_missing_for_path(conn, path)?;
            Ok(SourceFileStatus {
                raw_current,
                needs_workspace_refresh,
            })
        })
    }

    pub fn source_file_statuses(
        &self,
        files: &[SourceFileFingerprint],
    ) -> Result<HashMap<String, SourceFileStatus>> {
        if files.is_empty() {
            return Ok(HashMap::new());
        }
        self.with_conn(|conn| {
            prepare_temp_source_file_status_scope(conn, files)?;
            let mut stmt = conn.prepare(
                "SELECT scope.path,
                        CASE
                          WHEN raw.size = scope.size
                           AND (
                             raw.mtime_ms = scope.mtime_ms
                             OR (raw.mtime_ms IS NULL AND scope.mtime_ms IS NULL)
                           )
                          THEN 1
                          ELSE 0
                        END AS raw_current,
                        CASE
                          WHEN raw.size = scope.size
                           AND (
                             raw.mtime_ms = scope.mtime_ms
                             OR (raw.mtime_ms IS NULL AND scope.mtime_ms IS NULL)
                           )
                          THEN EXISTS(
                            SELECT 1
                            FROM sessions
                            WHERE json_extract(metadata_json, '$.path') = scope.path
                              AND json_extract(metadata_json, '$.workspace_path') IS NULL
                          )
                          ELSE 0
                        END AS needs_workspace_refresh
                 FROM temp_source_file_status_scope scope
                 LEFT JOIN raw_artifacts raw
                   ON raw.rowid = (
                     SELECT latest.rowid
                     FROM raw_artifacts latest
                     WHERE latest.path = scope.path
                     ORDER BY latest.first_seen_at DESC
                     LIMIT 1
                   )",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    SourceFileStatus {
                        raw_current: row.get::<_, i64>(1)? != 0,
                        needs_workspace_refresh: row.get::<_, i64>(2)? != 0,
                    },
                ))
            })?;
            let mut out = HashMap::with_capacity(files.len());
            for row in rows {
                let (path, status) = row?;
                out.insert(path, status);
            }
            Ok(out)
        })
    }

    #[cfg(test)]
    fn raw_artifact_current_query_plan(&self) -> Result<String> {
        self.with_conn(|conn| {
            query_plan(
                conn,
                "EXPLAIN QUERY PLAN
                 SELECT size, mtime_ms
                 FROM raw_artifacts
                 WHERE path = ?1
                 ORDER BY first_seen_at DESC
                 LIMIT 1",
                ["/tmp/fixture.jsonl"],
            )
        })
    }

    #[cfg(test)]
    fn search_index_missing_rows_query_plan(&self) -> Result<String> {
        self.with_conn(|conn| {
            query_plan(
                conn,
                "EXPLAIN QUERY PLAN
                 SELECT e.id
                 FROM events e
                 LEFT JOIN event_embeddings emb
                   ON emb.event_id = e.id AND emb.model = ?1
                 WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                   AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                   AND emb.event_id IS NULL
                 ORDER BY e.session_id, e.ordinal, e.id",
                ["hash-embed-v1"],
            )
        })
    }

    #[cfg(test)]
    fn workspace_refresh_query_plan(&self) -> Result<String> {
        self.with_conn(|conn| {
            query_plan(
                conn,
                "EXPLAIN QUERY PLAN
                 SELECT EXISTS(
                   SELECT 1
                   FROM sessions
                   WHERE json_extract(metadata_json, '$.path') = ?1
                     AND json_extract(metadata_json, '$.workspace_path') IS NULL
                 )",
                ["/tmp/fixture.jsonl"],
            )
        })
    }

    #[cfg(test)]
    fn incremental_history_items_event_lookup_query_plan(
        &self,
        event_ids: &[String],
    ) -> Result<String> {
        self.with_conn(|conn| {
            prepare_temp_id_scope(conn, "temp_history_item_event_ids", event_ids)?;
            query_plan(
                conn,
                "EXPLAIN QUERY PLAN
                 SELECT e.id, e.session_id, e.source_id, e.machine_id, e.source_kind,
                        e.ordinal, e.event_type, e.role, e.content, e.raw_artifact_hash,
                        e.occurred_at, e.metadata_json, e.hash
                 FROM temp_history_item_event_ids scope
                 CROSS JOIN events e
                 WHERE e.id = scope.id
                 ORDER BY e.session_id, e.ordinal, e.id",
                [],
            )
        })
    }

    #[cfg(test)]
    pub fn session_workspace_metadata_missing_for_path(&self, path: &str) -> Result<bool> {
        self.with_conn(|conn| session_workspace_metadata_missing_for_path(conn, path))
    }

    pub fn refresh_search_index(
        &self,
        model: &str,
        dims: usize,
        embed: impl Fn(&str) -> Vec<f32>,
    ) -> Result<usize> {
        self.refresh_search_index_with_progress(model, dims, embed, |_, _, _| {})
    }

    pub fn refresh_search_index_with_progress(
        &self,
        model: &str,
        dims: usize,
        embed: impl Fn(&str) -> Vec<f32>,
        mut progress: impl FnMut(&'static str, usize, usize),
    ) -> Result<usize> {
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                prepare_temp_events_fts_scope(tx)?;
                let total_index_rows: usize = tx.query_row(
                    "SELECT COUNT(*)
                     FROM events e
                     LEFT JOIN event_embeddings emb
                       ON emb.event_id = e.id AND emb.model = ?1
                     LEFT JOIN temp_events_fts_event_ids fts
                       ON fts.id = e.id
                     WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                       AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                       AND (emb.event_id IS NULL OR fts.id IS NULL)",
                    params![model],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                progress("search_rows", 0, total_index_rows);
                let mut stmt = tx.prepare(
                    "SELECT e.id,
                        e.session_id,
                        e.source_id,
                        e.machine_id,
                        e.source_kind,
                        e.role,
                        json_extract(e.metadata_json, '$.search_kind'),
                        json_extract(e.metadata_json, '$.search_text'),
                        e.occurred_at,
                        fts.id IS NOT NULL
                 FROM events e
                 LEFT JOIN event_embeddings emb
                   ON emb.event_id = e.id AND emb.model = ?1
                 LEFT JOIN temp_events_fts_event_ids fts
                   ON fts.id = e.id
                 WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                   AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                   AND (emb.event_id IS NULL OR fts.id IS NULL)
                 ORDER BY e.session_id, e.ordinal, e.id",
                )?;
                let rows = stmt.query_map(params![model], |row| {
                    let content: String = row.get(7)?;
                    Ok(EventForProjection {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        source_id: row.get(2)?,
                        machine_id: row.get(3)?,
                        source_kind: row.get(4)?,
                        role: row.get(5)?,
                        search_kind: row.get(6)?,
                        text_hash: crate::archive::blake3_hex(content.as_bytes()),
                        content,
                        occurred_at: parse_opt_dt(row.get(8)?),
                        fts_indexed: row.get(9)?,
                    })
                })?;
                let mut indexed_rows = 0usize;
                for row in rows {
                    insert_search_index_rows(&tx, &row?, model, dims, &embed)?;
                    indexed_rows += 1;
                    if indexed_rows % 1_000 == 0 {
                        progress("search_rows", indexed_rows, total_index_rows);
                    }
                }
                if total_index_rows > 0 || indexed_rows > 0 {
                    progress("search_rows", indexed_rows, total_index_rows);
                }
                drop(stmt);

                let total_unit_rows: usize = tx.query_row(
                    "SELECT COUNT(*)
                     FROM events e
                     LEFT JOIN search_units su
                       ON su.event_id = e.id
                     WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                       AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                       AND su.event_id IS NULL",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                progress("search_units", 0, total_unit_rows);
                let mut missing_unit_stmt = tx.prepare(
                    "SELECT e.id,
                        e.session_id,
                        e.source_id,
                        e.machine_id,
                        e.source_kind,
                        e.role,
                        json_extract(e.metadata_json, '$.search_kind'),
                        json_extract(e.metadata_json, '$.search_text'),
                        e.occurred_at,
                        0
                 FROM events e
                 LEFT JOIN search_units su
                   ON su.event_id = e.id
                 WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                   AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                   AND su.event_id IS NULL
                 ORDER BY e.session_id, e.ordinal, e.id",
                )?;
                let missing_unit_rows = missing_unit_stmt.query_map([], |row| {
                    let content: String = row.get(7)?;
                    Ok(EventForProjection {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        source_id: row.get(2)?,
                        machine_id: row.get(3)?,
                        source_kind: row.get(4)?,
                        role: row.get(5)?,
                        search_kind: row.get(6)?,
                        text_hash: crate::archive::blake3_hex(content.as_bytes()),
                        content,
                        occurred_at: parse_opt_dt(row.get(8)?),
                        fts_indexed: row.get(9)?,
                    })
                })?;
                let mut projected_units = 0usize;
                for row in missing_unit_rows {
                    let event = row?;
                    let unit_id =
                        crate::archive::stable_id(&["search_unit", &event.id, &event.text_hash]);
                    let unit_hash = crate::archive::stable_hash(&(
                        &unit_id,
                        &event.id,
                        &event.text_hash,
                        &event.content,
                        &event.search_kind,
                    ))?;
                    let unit = SearchUnitRecord {
                        id: unit_id,
                        event_id: event.id.clone(),
                        session_id: event.session_id.clone(),
                        source_id: event.source_id.clone(),
                        machine_id: event.machine_id.clone(),
                        source_kind: event.source_kind.clone(),
                        role: event.role.clone(),
                        search_kind: event.search_kind.clone(),
                        text: event.content.clone(),
                        text_hash: event.text_hash.clone(),
                        occurred_at: event.occurred_at,
                        metadata: serde_json::json!({
                            "derived_from": "event.search_text",
                            "indexer": "search_unit_v1"
                        }),
                        hash: unit_hash,
                    };
                    insert_search_unit(&tx, &unit)?;
                    projected_units += 1;
                    if projected_units % 1_000 == 0 {
                        progress("search_units", projected_units, total_unit_rows);
                    }
                }
                if total_unit_rows > 0 || projected_units > 0 {
                    progress("search_units", projected_units, total_unit_rows);
                }
                let indexed_events = count_indexed_events(&tx, model)?;
                update_projection_status(&tx, "search_rrf_v1", indexed_events)?;
                drop(missing_unit_stmt);
                Ok(indexed_events)
            })
        })
    }

    pub fn refresh_search_text_index_with_progress(
        &self,
        mut progress: impl FnMut(&'static str, usize, usize),
    ) -> Result<usize> {
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                prepare_temp_events_fts_scope(tx)?;
                let total_index_rows: usize = tx.query_row(
                    "SELECT COUNT(*)
                     FROM events e
                     LEFT JOIN search_units su
                       ON su.event_id = e.id
                     LEFT JOIN temp_events_fts_event_ids fts
                       ON fts.id = e.id
                     WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                       AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                       AND (su.event_id IS NULL OR fts.id IS NULL)",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                progress("search_rows", 0, total_index_rows);
                let mut stmt = tx.prepare(
                    "SELECT e.id,
                        e.session_id,
                        e.source_id,
                        e.machine_id,
                        e.source_kind,
                        e.role,
                        json_extract(e.metadata_json, '$.search_kind'),
                        json_extract(e.metadata_json, '$.search_text'),
                        e.occurred_at,
                        fts.id IS NOT NULL
                 FROM events e
                 LEFT JOIN search_units su
                   ON su.event_id = e.id
                 LEFT JOIN temp_events_fts_event_ids fts
                   ON fts.id = e.id
                 WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                   AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                   AND (su.event_id IS NULL OR fts.id IS NULL)
                 ORDER BY e.session_id, e.ordinal, e.id",
                )?;
                let rows = stmt.query_map([], |row| {
                    let content: String = row.get(7)?;
                    Ok(EventForProjection {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        source_id: row.get(2)?,
                        machine_id: row.get(3)?,
                        source_kind: row.get(4)?,
                        role: row.get(5)?,
                        search_kind: row.get(6)?,
                        text_hash: crate::archive::blake3_hex(content.as_bytes()),
                        content,
                        occurred_at: parse_opt_dt(row.get(8)?),
                        fts_indexed: row.get(9)?,
                    })
                })?;
                let mut indexed_rows = 0usize;
                for row in rows {
                    insert_search_text_index_rows(&tx, &row?)?;
                    indexed_rows += 1;
                    if indexed_rows % 1_000 == 0 {
                        progress("search_rows", indexed_rows, total_index_rows);
                    }
                }
                if total_index_rows > 0 || indexed_rows > 0 {
                    progress("search_rows", indexed_rows, total_index_rows);
                }
                drop(stmt);

                let indexed_events = count_text_indexed_events(&tx)?;
                update_projection_status(&tx, "search_rrf_v1", indexed_events)?;
                Ok(indexed_events)
            })
        })
    }

    #[allow(dead_code)]
    pub fn refresh_search_index_for_events(
        &self,
        model: &str,
        dims: usize,
        event_ids: &[String],
        embed: impl Fn(&str) -> Vec<f32>,
    ) -> Result<usize> {
        self.refresh_search_index_for_events_with_progress(model, dims, event_ids, embed, |_, _| {})
    }

    pub fn refresh_search_index_for_events_with_progress(
        &self,
        model: &str,
        dims: usize,
        event_ids: &[String],
        embed: impl Fn(&str) -> Vec<f32>,
        mut progress: impl FnMut(usize, usize),
    ) -> Result<usize> {
        let event_ids = normalized_ids(event_ids);
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                prepare_temp_events_fts_scope(tx)?;
                let mut projected_events = 0;
                if !event_ids.is_empty() {
                    prepare_temp_id_scope(&tx, "temp_search_index_event_ids", &event_ids)?;
                    let total_events: usize = tx.query_row(
                        "SELECT COUNT(*)
                         FROM temp_search_index_event_ids scope
                         CROSS JOIN events e
                         WHERE e.id = scope.id
                           AND json_extract(e.metadata_json, '$.search_indexable') = 1
                           AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0",
                        [],
                        |row| row.get::<_, i64>(0),
                    )? as usize;
                    let mut stmt = tx.prepare(
                        "SELECT e.id,
                            e.session_id,
                            e.source_id,
                            e.machine_id,
                            e.source_kind,
                            e.role,
                            json_extract(e.metadata_json, '$.search_kind'),
                            json_extract(e.metadata_json, '$.search_text'),
                            e.occurred_at,
                            fts.id IS NOT NULL,
                            su.event_id IS NOT NULL,
                            emb.event_id IS NOT NULL
                     FROM temp_search_index_event_ids scope
                     CROSS JOIN events e
                     LEFT JOIN temp_events_fts_event_ids fts
                       ON fts.id = e.id
                     LEFT JOIN search_units su
                       ON su.event_id = e.id
                     LEFT JOIN event_embeddings emb
                       ON emb.event_id = e.id AND emb.model = ?1
                     WHERE e.id = scope.id
                       AND json_extract(e.metadata_json, '$.search_indexable') = 1
                       AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                     ORDER BY scope.id",
                    )?;
                    let rows = stmt.query_map(params![model], |row| {
                        let content: String = row.get(7)?;
                        let fts_indexed: bool = row.get(9)?;
                        let unit_indexed: bool = row.get(10)?;
                        let embedding_indexed: bool = row.get(11)?;
                        Ok((
                            EventForProjection {
                                id: row.get(0)?,
                                session_id: row.get(1)?,
                                source_id: row.get(2)?,
                                machine_id: row.get(3)?,
                                source_kind: row.get(4)?,
                                role: row.get(5)?,
                                search_kind: row.get(6)?,
                                text_hash: crate::archive::blake3_hex(content.as_bytes()),
                                content,
                                occurred_at: parse_opt_dt(row.get(8)?),
                                fts_indexed,
                            },
                            fts_indexed && unit_indexed && embedding_indexed,
                        ))
                    })?;
                    let mut newly_complete_events = 0usize;
                    for row in rows {
                        let (event, complete_before) = row?;
                        insert_search_index_rows(&tx, &event, model, dims, &embed)?;
                        if !complete_before {
                            newly_complete_events += 1;
                        }
                        projected_events += 1;
                        progress(projected_events, total_events);
                    }
                    if total_events > 0 {
                        progress(projected_events, total_events);
                    }
                    drop(stmt);
                    let indexed_events = indexed_count_after_incremental_refresh(
                        &tx,
                        newly_complete_events,
                        || count_indexed_events(&tx, model),
                    )?;
                    update_projection_status(&tx, "search_rrf_v1", indexed_events)?;
                    return Ok(projected_events);
                }
                Ok(projected_events)
            })
        })
    }

    pub fn refresh_search_text_index_for_events_with_progress(
        &self,
        event_ids: &[String],
        mut progress: impl FnMut(usize, usize),
    ) -> Result<usize> {
        let event_ids = normalized_ids(event_ids);
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                prepare_temp_events_fts_scope(tx)?;
                let mut projected_events = 0;
                if !event_ids.is_empty() {
                    prepare_temp_id_scope(&tx, "temp_search_index_event_ids", &event_ids)?;
                    let total_events: usize = tx.query_row(
                        "SELECT COUNT(*)
                         FROM temp_search_index_event_ids scope
                         CROSS JOIN events e
                         WHERE e.id = scope.id
                           AND json_extract(e.metadata_json, '$.search_indexable') = 1
                           AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0",
                        [],
                        |row| row.get::<_, i64>(0),
                    )? as usize;
                    let mut stmt = tx.prepare(
                        "SELECT e.id,
                            e.session_id,
                            e.source_id,
                            e.machine_id,
                            e.source_kind,
                            e.role,
                            json_extract(e.metadata_json, '$.search_kind'),
                            json_extract(e.metadata_json, '$.search_text'),
                            e.occurred_at,
                            fts.id IS NOT NULL,
                            su.event_id IS NOT NULL
                     FROM temp_search_index_event_ids scope
                     CROSS JOIN events e
                     LEFT JOIN temp_events_fts_event_ids fts
                       ON fts.id = e.id
                     LEFT JOIN search_units su
                       ON su.event_id = e.id
                     WHERE e.id = scope.id
                       AND json_extract(e.metadata_json, '$.search_indexable') = 1
                       AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                     ORDER BY scope.id",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        let content: String = row.get(7)?;
                        let fts_indexed: bool = row.get(9)?;
                        let unit_indexed: bool = row.get(10)?;
                        Ok((
                            EventForProjection {
                                id: row.get(0)?,
                                session_id: row.get(1)?,
                                source_id: row.get(2)?,
                                machine_id: row.get(3)?,
                                source_kind: row.get(4)?,
                                role: row.get(5)?,
                                search_kind: row.get(6)?,
                                text_hash: crate::archive::blake3_hex(content.as_bytes()),
                                content,
                                occurred_at: parse_opt_dt(row.get(8)?),
                                fts_indexed,
                            },
                            fts_indexed && unit_indexed,
                        ))
                    })?;
                    let mut newly_complete_events = 0usize;
                    for row in rows {
                        let (event, complete_before) = row?;
                        insert_search_text_index_rows(&tx, &event)?;
                        if !complete_before {
                            newly_complete_events += 1;
                        }
                        projected_events += 1;
                        progress(projected_events, total_events);
                    }
                    if total_events > 0 {
                        progress(projected_events, total_events);
                    }
                    drop(stmt);
                    let indexed_events = indexed_count_after_incremental_refresh(
                        &tx,
                        newly_complete_events,
                        || count_text_indexed_events(&tx),
                    )?;
                    update_projection_status(&tx, "search_rrf_v1", indexed_events)?;
                    return Ok(projected_events);
                }
                Ok(projected_events)
            })
        })
    }

    pub fn search_index_needs_repair(&self, model: &str) -> Result<bool> {
        self.with_conn(|conn| search_index_needs_repair(conn, model))
    }

    pub fn search_text_index_needs_repair(&self) -> Result<bool> {
        self.with_conn(search_text_index_needs_repair)
    }

    pub fn search_fts(
        &self,
        query: &str,
        tiers: &[&str],
        limit: usize,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        machine_id: Option<&str>,
        machine_id_prefix: Option<&str>,
        workspace_scope: Option<&str>,
    ) -> Result<Vec<SearchRow>> {
        let fts_query = fts_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }
        if tiers.is_empty() {
            return Ok(Vec::new());
        }
        let after = opt_dt(after);
        let before = opt_dt(before);
        let tier_placeholders = vec!["?"; tiers.len()].join(", ");
        self.with_conn(|conn| {
            let use_conversation_fts = tiers_are_only_conversation(tiers);
            let fts_table = if use_conversation_fts {
                "history_items_conversation_fts"
            } else {
                "history_items_fts"
            };
            let workspace_filter = positional_workspace_scope_sql();
            let snippet_column = if use_conversation_fts { 4 } else { 5 };
            let tier_clause = if use_conversation_fts {
                String::new()
            } else {
                format!("AND hi.tier IN ({tier_placeholders})")
            };
            let sql = format!(
                "SELECT {fts_table}.item_id,
                        {fts_table}.event_id,
                        {fts_table}.session_id,
                        e.machine_id,
                        e.source_kind,
                        hi.tier,
                        hi.kind,
                        snippet({fts_table}, {snippet_column}, '', '', '...', 24),
                        hi.occurred_at,
                        s.title,
                        s.metadata_json
                 FROM {fts_table}
                 JOIN history_items hi ON hi.id = {fts_table}.item_id
                 JOIN events e ON e.id = {fts_table}.event_id
                 LEFT JOIN sessions s ON s.id = {fts_table}.session_id
                 WHERE {fts_table} MATCH ?
                   AND (? IS NULL OR hi.occurred_at >= ?)
                   AND (? IS NULL OR hi.occurred_at < ?)
                   AND (? IS NULL OR e.machine_id = ?)
                   AND (? IS NULL OR substr(e.machine_id, 1, length(?)) = ?)
                   AND (? IS NULL OR {workspace_filter})
                   {tier_clause}
                 ORDER BY bm25({fts_table})
                 LIMIT ?"
            );
            let mut values = vec![
                SqlValue::Text(fts_query),
                opt_sql_text(after.clone()),
                opt_sql_text(after),
                opt_sql_text(before.clone()),
                opt_sql_text(before),
                opt_sql_text(machine_id.map(str::to_string)),
                opt_sql_text(machine_id.map(str::to_string)),
                opt_sql_text(machine_id_prefix.map(str::to_string)),
                opt_sql_text(machine_id_prefix.map(str::to_string)),
                opt_sql_text(machine_id_prefix.map(str::to_string)),
            ];
            push_workspace_scope_filter_params(&mut values, workspace_scope);
            if !use_conversation_fts {
                values.extend(tiers.iter().map(|tier| SqlValue::Text((*tier).to_string())));
            }
            values.push(SqlValue::Integer(limit as i64));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), |row| {
                Ok(SearchRow {
                    history_item_id: Some(row.get(0)?),
                    event_id: row.get(1)?,
                    session_id: row.get(2)?,
                    machine_id: row.get(3)?,
                    source_kind: row.get(4)?,
                    tier: Some(row.get(5)?),
                    search_kind: row.get(6)?,
                    content: row.get(7)?,
                    occurred_at: parse_opt_dt(row.get(8)?),
                    session_title: row.get(9)?,
                    workspace_values: session_workspace_values(&parse_metadata_json(
                        row.get::<_, Option<String>>(10)?,
                    )),
                    rank: 0,
                })
            })?;
            let mut out = Vec::new();
            for (idx, row) in rows.enumerate() {
                let mut row = row?;
                row.rank = idx + 1;
                out.push(row);
            }
            Ok(out)
        })
    }

    pub fn session_by_id(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT id, source_id, machine_id, source_kind, external_id, title, status,
                        started_at, updated_at, metadata_json, hash
                 FROM sessions
                 WHERE id = ?1",
                params![session_id],
                row_session,
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn sessions_by_external_id(&self, external_id: &str) -> Result<Vec<SessionRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, source_id, machine_id, source_kind, external_id, title, status,
                        started_at, updated_at, metadata_json, hash
                 FROM sessions
                 WHERE external_id = ?1
                 ORDER BY source_kind, id",
            )?;
            let rows = stmt.query_map(params![external_id], row_session)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn source_by_id(&self, source_id: &str) -> Result<Option<SourceRecord>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT id, kind, identity, path, first_seen_at, updated_at, hash
                 FROM sources
                 WHERE id = ?1",
                params![source_id],
                row_source,
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn event_by_id(&self, event_id: &str) -> Result<Option<EventRecord>> {
        self.with_conn(|conn| event_by_id(conn, event_id))
    }

    pub fn raw_artifact_summary_by_hash(&self, hash: &str) -> Result<Option<RawArtifactSummary>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT hash, path, size, media_type, first_seen_at
                 FROM raw_artifacts
                 WHERE hash = ?1",
                params![hash],
                |row| {
                    Ok(RawArtifactSummary {
                        hash: row.get(0)?,
                        path: row.get(1)?,
                        size: row.get::<_, u64>(2)?,
                        media_type: row.get(3)?,
                        first_seen_at: parse_dt(row.get(4)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn search_unit_by_id(&self, unit_id: &str) -> Result<Option<SearchUnitRecord>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT id, event_id, session_id, source_id, machine_id, source_kind, role,
                        search_kind, text, text_hash, occurred_at, metadata_json, hash
                 FROM search_units
                 WHERE id = ?1",
                params![unit_id],
                row_search_unit,
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn events_for_session(&self, session_id: &str) -> Result<Vec<EventRecord>> {
        self.with_conn(|conn| events_for_session(conn, session_id))
    }

    pub fn list_threads(&self, options: &ThreadListOptions) -> Result<Vec<ThreadRow>> {
        self.with_conn(|conn| {
            let last_activity = thread_last_activity_sql();
            let workspace_filter = thread_workspace_scope_sql();
            let order = match options.sort {
                ThreadSortMode::Newest => {
                    "last_activity_at IS NULL ASC, last_activity_at DESC, s.id DESC"
                }
                ThreadSortMode::Oldest => {
                    "last_activity_at IS NULL DESC, last_activity_at ASC, s.id ASC"
                }
            };
            let sql = format!(
                "SELECT s.id,
                        s.source_id,
                        s.machine_id,
                        s.source_kind,
                        s.external_id,
                        s.title,
                        s.status,
                        s.started_at,
                        s.updated_at,
                        s.metadata_json,
                        s.hash,
                        COALESCE(a.event_count, 0),
                        a.first_event_at,
                        a.last_event_at,
                        {last_activity} AS last_activity_at
                 FROM sessions s
                 LEFT JOIN session_activity a ON a.session_id = s.id
                 WHERE (:after IS NULL OR {last_activity} >= :after)
                   AND (:before IS NULL OR {last_activity} < :before)
                   AND (:scope IS NULL OR {workspace_filter})
                 ORDER BY {order}
                 LIMIT :limit"
            );
            let after = options.after.map(|dt| dt.to_rfc3339());
            let before = options.before.map(|dt| dt.to_rfc3339());
            let scope = options
                .workspace_scope
                .as_deref()
                .map(|scope| scope.trim_end_matches('/'))
                .filter(|scope| !scope.is_empty());
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(
                named_params! {
                    ":after": after,
                    ":before": before,
                    ":scope": scope,
                    ":limit": options.limit as i64,
                },
                |row| {
                    let metadata_text: String = row.get(9)?;
                    let metadata = serde_json::from_str(&metadata_text).unwrap_or(Value::Null);
                    let session = SessionRecord {
                        id: row.get(0)?,
                        source_id: row.get(1)?,
                        machine_id: row.get(2)?,
                        source_kind: row.get(3)?,
                        external_id: row.get(4)?,
                        title: row.get(5)?,
                        status: row.get(6)?,
                        started_at: parse_opt_dt(row.get(7)?),
                        updated_at: parse_opt_dt(row.get(8)?),
                        metadata: metadata.clone(),
                        hash: row.get(10)?,
                    };
                    let first_event_at = parse_opt_dt(row.get(12)?);
                    let last_event_at = parse_opt_dt(row.get(13)?);
                    let last_activity_at = parse_opt_dt(row.get(14)?);
                    let workspace_values = session_workspace_values(&metadata);
                    Ok(ThreadRow {
                        session,
                        event_count: row.get::<_, i64>(11)?.max(0) as u64,
                        first_event_at,
                        last_event_at,
                        last_activity_at,
                        workspace_path: primary_workspace_value(&metadata),
                        workspace_values,
                    })
                },
            )?;

            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn update_session_title_for_external_id(
        &self,
        source_kind: &str,
        external_id: &str,
        title: &str,
    ) -> Result<usize> {
        let title = title.trim();
        if title.is_empty() {
            return Ok(0);
        }
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE sessions
                 SET title = ?3
                 WHERE source_kind = ?1
                   AND external_id = ?2
                   AND coalesce(title, '') != ?3",
                params![source_kind, external_id, title],
            )
            .map_err(Into::into)
        })
    }

    pub fn events_around_event(
        &self,
        event_id: &str,
        before: usize,
        after: usize,
    ) -> Result<Option<TranscriptContext>> {
        self.with_conn(|conn| {
            let Some(target_event) = event_by_id(conn, event_id)? else {
                return Ok(None);
            };
            let Some(session) = session_by_id(conn, &target_event.session_id)? else {
                return Ok(None);
            };
            let session_events = events_for_session(conn, &target_event.session_id)?;
            let Some(target_index) = session_events
                .iter()
                .position(|candidate| candidate.id == target_event.id)
            else {
                return Ok(None);
            };
            let start = target_index.saturating_sub(before);
            let end = (target_index + after + 1).min(session_events.len());
            Ok(Some(TranscriptContext {
                session,
                target_event,
                events: session_events[start..end].to_vec(),
                target_index: target_index - start,
            }))
        })
    }

    pub fn history_items_for_transcript_session(
        &self,
        session_id: &str,
    ) -> Result<Option<HistoryTranscriptContext>> {
        self.with_conn(|conn| {
            let Some(session) = session_by_id(conn, session_id)? else {
                return Ok(None);
            };
            let items = conversation_history_items_for_session(conn, session_id)?;
            Ok(Some(HistoryTranscriptContext {
                session,
                target_event: None,
                items,
                target_index: None,
                omitted_target: false,
            }))
        })
    }

    pub fn history_items_around_event(
        &self,
        event_id: &str,
        before: usize,
        after: usize,
    ) -> Result<Option<HistoryTranscriptContext>> {
        self.with_conn(|conn| {
            let Some(target_event) = event_by_id(conn, event_id)? else {
                return Ok(None);
            };
            let Some(session) = session_by_id(conn, &target_event.session_id)? else {
                return Ok(None);
            };
            let all_items = conversation_history_items_for_session(conn, &target_event.session_id)?;
            let target_index = all_items
                .iter()
                .position(|candidate| candidate.event_id == target_event.id);
            let anchor_index = target_index.or_else(|| {
                all_items
                    .iter()
                    .position(|candidate| candidate.ordinal >= target_event.ordinal)
                    .or_else(|| all_items.len().checked_sub(1))
            });
            let items = if let Some(anchor_index) = anchor_index {
                let start = anchor_index.saturating_sub(before);
                let end = (anchor_index + after + 1).min(all_items.len());
                all_items[start..end].to_vec()
            } else {
                Vec::new()
            };
            let target_index = target_index.and_then(|target_index| {
                anchor_index.and_then(|anchor_index| {
                    let start = anchor_index.saturating_sub(before);
                    target_index
                        .checked_sub(start)
                        .filter(|idx| *idx < items.len())
                })
            });
            Ok(Some(HistoryTranscriptContext {
                session,
                target_event: Some(target_event),
                items,
                target_index,
                omitted_target: target_index.is_none(),
            }))
        })
    }

    pub fn record_recent_result_refs(
        &self,
        results: &[RecentResultRefInput],
    ) -> Result<Vec<String>> {
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                let mut refs = Vec::with_capacity(results.len());
                for result in results {
                    refs.push(upsert_recent_result_ref(&tx, result)?);
                }
                prune_recent_result_refs(&tx, RECENT_RESULT_REF_LIMIT)?;
                Ok(refs)
            })
        })
    }

    pub fn event_id_for_recent_ref(&self, ref_id: &str) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT event_id
                 FROM recent_result_refs
                 WHERE ref = ?1",
                params![ref_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn recent_ref_for_event_id(&self, event_id: &str) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT ref
                 FROM recent_result_refs
                 WHERE event_id = ?1",
                params![event_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn history_items_missing_required_embedding(
        &self,
        model_id: &str,
        limit: usize,
    ) -> Result<Vec<HistoryItemForEmbedding>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT hi.id,
                        hi.text,
                        hi.text_hash,
                        COALESCE(hi.occurred_at, ''),
                        hi.session_id,
                        hi.ordinal,
                        hi.subordinal
                 FROM history_items hi
                 LEFT JOIN embeddings e
                   ON e.unit_id = hi.id
                  AND e.text_hash = hi.text_hash
                   AND e.model_id = ?1
                 WHERE e.id IS NULL
                   AND hi.tier = 'conversation'
                   AND hi.semantic_policy = 'required'
                   AND length(trim(hi.text)) >= ?2
                 ORDER BY COALESCE(hi.occurred_at, ''),
                          hi.session_id,
                          hi.ordinal,
                          hi.subordinal,
                          hi.id
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![
                    model_id,
                    SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64,
                    limit as i64
                ],
                |row| {
                    Ok(HistoryItemForEmbedding {
                        id: row.get(0)?,
                        text: row.get(1)?,
                        text_hash: row.get(2)?,
                        cursor: HistoryItemEmbeddingCursor {
                            occurred_at_key: row.get(3)?,
                            session_id: row.get(4)?,
                            ordinal: row.get(5)?,
                            subordinal: row.get(6)?,
                            id: row.get(0)?,
                        },
                    })
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn history_items_missing_required_embedding_after(
        &self,
        model_id: &str,
        after: &HistoryItemEmbeddingCursor,
        limit: usize,
    ) -> Result<Vec<HistoryItemForEmbedding>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT hi.id,
                        hi.text,
                        hi.text_hash,
                        COALESCE(hi.occurred_at, ''),
                        hi.session_id,
                        hi.ordinal,
                        hi.subordinal
                 FROM history_items hi
                 LEFT JOIN embeddings e
                   ON e.unit_id = hi.id
                  AND e.text_hash = hi.text_hash
                   AND e.model_id = ?1
                 WHERE e.id IS NULL
                   AND hi.tier = 'conversation'
                   AND hi.semantic_policy = 'required'
                   AND length(trim(hi.text)) >= ?2
                   AND (
                     COALESCE(hi.occurred_at, ''),
                     hi.session_id,
                     hi.ordinal,
                     hi.subordinal,
                     hi.id
                   ) > (?3, ?4, ?5, ?6, ?7)
                 ORDER BY COALESCE(hi.occurred_at, ''),
                          hi.session_id,
                          hi.ordinal,
                          hi.subordinal,
                          hi.id
                 LIMIT ?8",
            )?;
            let rows = stmt.query_map(
                params![
                    model_id,
                    SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64,
                    &after.occurred_at_key,
                    &after.session_id,
                    after.ordinal,
                    after.subordinal,
                    &after.id,
                    limit as i64
                ],
                |row| {
                    Ok(HistoryItemForEmbedding {
                        id: row.get(0)?,
                        text: row.get(1)?,
                        text_hash: row.get(2)?,
                        cursor: HistoryItemEmbeddingCursor {
                            occurred_at_key: row.get(3)?,
                            session_id: row.get(4)?,
                            ordinal: row.get(5)?,
                            subordinal: row.get(6)?,
                            id: row.get(0)?,
                        },
                    })
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn history_items_missing_required_embedding_for_events(
        &self,
        model_id: &str,
        event_ids: &[String],
        limit: usize,
    ) -> Result<Vec<HistoryItemForEmbedding>> {
        let event_ids = normalized_ids(event_ids);
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_conn(|conn| {
            prepare_temp_id_scope(conn, "temp_delta_event_ids", &event_ids)?;
            let mut stmt = conn.prepare(
                "SELECT hi.id,
                        hi.text,
                        hi.text_hash,
                        COALESCE(hi.occurred_at, ''),
                        hi.session_id,
                        hi.ordinal,
                        hi.subordinal
                 FROM history_items hi
                 LEFT JOIN temp_delta_event_ids event_scope
                   ON event_scope.id = hi.event_id
                 LEFT JOIN embeddings e
                   ON e.unit_id = hi.id
                  AND e.text_hash = hi.text_hash
                  AND e.model_id = ?
                 WHERE e.id IS NULL
                   AND event_scope.id IS NOT NULL
                   AND hi.tier = 'conversation'
                   AND hi.semantic_policy = 'required'
                   AND length(trim(hi.text)) >= ?
                 ORDER BY COALESCE(hi.occurred_at, ''),
                          hi.session_id,
                          hi.ordinal,
                          hi.subordinal,
                          hi.id
                 LIMIT ?",
            )?;
            let rows = stmt.query_map(
                params![
                    model_id,
                    SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64,
                    limit as i64
                ],
                |row| {
                    Ok(HistoryItemForEmbedding {
                        id: row.get(0)?,
                        text: row.get(1)?,
                        text_hash: row.get(2)?,
                        cursor: HistoryItemEmbeddingCursor {
                            occurred_at_key: row.get(3)?,
                            session_id: row.get(4)?,
                            ordinal: row.get(5)?,
                            subordinal: row.get(6)?,
                            id: row.get(0)?,
                        },
                    })
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn history_items_need_required_embedding(&self, model_id: &str) -> Result<bool> {
        self.with_conn(|conn| history_items_need_required_embedding(conn, model_id))
    }

    pub fn history_items_missing_required_embedding_count(&self, model_id: &str) -> Result<usize> {
        self.with_conn(|conn| history_items_missing_required_embedding_count(conn, model_id))
    }

    pub fn refresh_vector_projection(&self) -> Result<usize> {
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                tx.execute("DELETE FROM vec_embeddings_384", [])?;
                let inserted = tx.execute(
                    "INSERT INTO vec_embeddings_384(rowid, embedding)
                     SELECT e.rowid, e.vector
                     FROM embeddings e
                     JOIN history_items hi ON hi.id = e.unit_id
                     WHERE e.dims = 384
                       AND hi.semantic_policy != 'never'",
                    [],
                )?;
                Ok(inserted)
            })
        })
    }

    pub fn refresh_vector_projection_for_embeddings(
        &self,
        embedding_ids: &[String],
    ) -> Result<usize> {
        let embedding_ids = normalized_ids(embedding_ids);
        self.with_conn(|conn| {
            with_immediate_write_tx(conn, |tx| {
                let mut inserted = 0;
                for chunk in embedding_ids.chunks(SQLITE_BIND_CHUNK_SIZE) {
                    let placeholders = placeholders(chunk.len());
                    let delete_sql = format!(
                        "DELETE FROM vec_embeddings_384
                     WHERE rowid IN (
                       SELECT rowid FROM embeddings WHERE id IN ({placeholders})
                     )"
                    );
                    tx.execute(
                        &delete_sql,
                        params_from_iter(chunk.iter().map(String::as_str)),
                    )?;
                    let insert_sql = format!(
                        "INSERT INTO vec_embeddings_384(rowid, embedding)
                     SELECT e.rowid, e.vector
                     FROM embeddings e
                     JOIN history_items hi ON hi.id = e.unit_id
                     WHERE e.dims = 384
                       AND hi.semantic_policy != 'never'
                       AND e.id IN ({placeholders})"
                    );
                    inserted += tx.execute(
                        &insert_sql,
                        params_from_iter(chunk.iter().map(String::as_str)),
                    )?;
                }
                Ok(inserted)
            })
        })
    }

    pub fn vector_projection_needs_repair(&self) -> Result<bool> {
        self.with_conn(vector_projection_needs_repair)
    }

    pub fn vector_search(
        &self,
        model_id: &str,
        query_vector: &[f32],
        tiers: &[&str],
        limit: usize,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        machine_id: Option<&str>,
        machine_id_prefix: Option<&str>,
        workspace_scope: Option<&str>,
    ) -> Result<Vec<VectorSearchRow>> {
        if query_vector.len() != 384 || tiers.is_empty() {
            return Ok(Vec::new());
        }
        let after = opt_dt(after);
        let before = opt_dt(before);
        let tier_placeholders = vec!["?"; tiers.len()].join(", ");
        self.with_conn(|conn| {
            let workspace_filter = positional_workspace_scope_sql();
            let sql = format!(
                "SELECT hi.event_id,
                        hi.id,
                        hi.session_id,
                        hi.machine_id,
                        hi.source_kind,
                        hi.tier,
                        hi.kind,
                        hi.text,
                        hi.occurred_at,
                        s.title,
                        s.metadata_json,
                        vec_embeddings_384.distance
                 FROM vec_embeddings_384
                 JOIN embeddings e ON e.rowid = vec_embeddings_384.rowid
                 JOIN history_items hi ON hi.id = e.unit_id
                 LEFT JOIN sessions s ON s.id = hi.session_id
                 WHERE vec_embeddings_384.embedding MATCH ?
                   AND k = ?
                   AND e.model_id = ?
                   AND (? IS NULL OR hi.occurred_at >= ?)
                   AND (? IS NULL OR hi.occurred_at < ?)
                   AND (? IS NULL OR hi.machine_id = ?)
                   AND (? IS NULL OR substr(hi.machine_id, 1, length(?)) = ?)
                   AND (? IS NULL OR {workspace_filter})
                   AND hi.semantic_policy != 'never'
                   AND length(trim(hi.text)) >= ?
                   AND hi.tier IN ({tier_placeholders})
                 ORDER BY vec_embeddings_384.distance"
            );
            let mut values = vec![
                SqlValue::Blob(f32_vector_to_blob(query_vector)),
                SqlValue::Integer(limit as i64),
                SqlValue::Text(model_id.to_string()),
                opt_sql_text(after.clone()),
                opt_sql_text(after),
                opt_sql_text(before.clone()),
                opt_sql_text(before),
                opt_sql_text(machine_id.map(str::to_string)),
                opt_sql_text(machine_id.map(str::to_string)),
                opt_sql_text(machine_id_prefix.map(str::to_string)),
                opt_sql_text(machine_id_prefix.map(str::to_string)),
                opt_sql_text(machine_id_prefix.map(str::to_string)),
            ];
            push_workspace_scope_filter_params(&mut values, workspace_scope);
            values.push(SqlValue::Integer(SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64));
            values.extend(tiers.iter().map(|tier| SqlValue::Text((*tier).to_string())));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), |row| {
                Ok(VectorSearchRow {
                    event_id: row.get(0)?,
                    history_item_id: row.get(1)?,
                    session_id: row.get(2)?,
                    machine_id: row.get(3)?,
                    source_kind: row.get(4)?,
                    tier: row.get(5)?,
                    search_kind: row.get(6)?,
                    content: row.get(7)?,
                    occurred_at: parse_opt_dt(row.get(8)?),
                    session_title: row.get(9)?,
                    workspace_values: session_workspace_values(&parse_metadata_json(
                        row.get::<_, Option<String>>(10)?,
                    )),
                    distance: row.get(11)?,
                    rank: 0,
                })
            })?;
            let mut out = Vec::new();
            for (idx, row) in rows.enumerate() {
                let mut row = row?;
                row.rank = idx + 1;
                out.push(row);
            }
            Ok(out)
        })
    }
}

fn load_sqlite_vec() {
    use rusqlite::ffi::sqlite3_auto_extension;
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| unsafe {
        sqlite3_auto_extension(Some(std::mem::transmute(
            sqlite_vec::sqlite3_vec_init as *const (),
        )));
    });
}

fn with_immediate_write_tx<T>(
    conn: &Connection,
    f: impl FnOnce(&Transaction<'_>) -> Result<T>,
) -> Result<T> {
    let tx =
        Transaction::new_unchecked(conn, TransactionBehavior::Immediate).with_context(|| {
            format!(
                "starting SQLite write transaction after waiting up to {SQLITE_BUSY_TIMEOUT_MS}ms"
            )
        })?;
    let value = f(&tx)?;
    tx.commit().context("committing SQLite write transaction")?;
    Ok(value)
}

fn prune_session_ids(conn: &Connection, filter: &PruneFilter) -> Result<Vec<String>> {
    let sources = normalized_string_set(&filter.sources);
    let sessions = normalized_string_set(&filter.sessions);
    let workspace_scope = filter
        .workspace_scope
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let machine_id = filter
        .machine_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let machine_id_prefix = filter
        .machine_id_prefix
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let mut stmt = conn.prepare(
        "SELECT s.id,
                s.source_kind,
                s.machine_id,
                s.started_at,
                s.updated_at,
                a.last_event_at,
                s.metadata_json
         FROM sessions s
         LEFT JOIN session_activity a ON a.session_id = s.id
         ORDER BY s.id",
    )?;
    let rows = stmt.query_map([], |row| {
        let metadata: String = row.get(6)?;
        Ok(PruneSessionFilterRow {
            id: row.get(0)?,
            source_kind: row.get(1)?,
            machine_id: row.get(2)?,
            started_at: parse_opt_dt(row.get(3)?),
            updated_at: parse_opt_dt(row.get(4)?),
            latest_event_at: parse_opt_dt(row.get(5)?),
            metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        let row = row?;
        if prune_session_matches(
            &row,
            &sources,
            &sessions,
            workspace_scope,
            machine_id,
            machine_id_prefix,
            filter.after,
            filter.before,
        ) {
            out.push(row.id);
        }
    }
    Ok(out)
}

fn prepare_prune_scope(conn: &Connection, session_ids: &[String]) -> Result<()> {
    prepare_temp_id_scope(conn, "temp_prune_session_ids", session_ids)?;
    let event_ids = collect_text_column(
        conn,
        "SELECT id
         FROM events
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
    )?;
    prepare_temp_id_scope(conn, "temp_prune_event_ids", &event_ids)?;
    let unit_ids = collect_text_column(
        conn,
        "SELECT id
         FROM history_items
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)
         UNION
         SELECT id
         FROM search_units
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
    )?;
    prepare_temp_id_scope(conn, "temp_prune_unit_ids", &unit_ids)?;
    let raw_hashes = collect_text_column(
        conn,
        "SELECT DISTINCT e.raw_artifact_hash
         FROM events e
         JOIN temp_prune_session_ids scope ON scope.id = e.session_id
         WHERE e.raw_artifact_hash IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM events keep
             LEFT JOIN temp_prune_session_ids keep_scope ON keep_scope.id = keep.session_id
             WHERE keep.raw_artifact_hash = e.raw_artifact_hash
               AND keep_scope.id IS NULL
           )
         ORDER BY e.raw_artifact_hash",
    )?;
    prepare_temp_id_scope(conn, "temp_prune_raw_hashes", &raw_hashes)?;
    let source_ids = collect_text_column(
        conn,
        "SELECT DISTINCT source_id
         FROM (
           SELECT source_id FROM sessions
           WHERE id IN (SELECT id FROM temp_prune_session_ids)
           UNION
           SELECT source_id FROM events
           WHERE session_id IN (SELECT id FROM temp_prune_session_ids)
           UNION
           SELECT source_id FROM search_units
           WHERE session_id IN (SELECT id FROM temp_prune_session_ids)
           UNION
           SELECT source_id FROM raw_artifacts
           WHERE hash IN (SELECT id FROM temp_prune_raw_hashes)
         )
         ORDER BY source_id",
    )?;
    prepare_temp_id_scope(conn, "temp_prune_source_ids", &source_ids)?;
    Ok(())
}

fn prune_plan_from_scope(conn: &Connection, session_ids: Vec<String>) -> Result<PrunePlan> {
    let raw_artifact_hashes =
        collect_text_column(conn, "SELECT id FROM temp_prune_raw_hashes ORDER BY id")?;
    let raw_blob_bytes: u64 = conn.query_row(
        "SELECT COALESCE(SUM(size), 0)
         FROM raw_artifacts
         WHERE hash IN (SELECT id FROM temp_prune_raw_hashes)",
        [],
        |row| row.get::<_, i64>(0),
    )? as u64;
    Ok(PrunePlan {
        sessions: session_ids.len() as u64,
        events: count_prune_events(conn)?,
        history_items: count_prune_history_items(conn)?,
        search_units: count_prune_search_units(conn)?,
        embeddings: count_prune_embeddings(conn)?,
        event_embeddings: count_prune_event_embeddings(conn)?,
        vector_rows: count_prune_vector_rows(conn)?,
        recent_result_refs: count_prune_recent_refs(conn)?,
        raw_artifacts: count_prune_raw_artifacts(conn)?,
        raw_blob_bytes,
        sources: count_prune_sources(conn)?,
        session_ids,
        raw_artifact_hashes,
    })
}

fn delete_prune_scope(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM vec_embeddings_384
         WHERE rowid IN (
           SELECT e.rowid
           FROM embeddings e
           JOIN temp_prune_unit_ids scope ON scope.id = e.unit_id
         )",
        [],
    )?;
    conn.execute(
        "DELETE FROM embeddings
         WHERE unit_id IN (SELECT id FROM temp_prune_unit_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM event_embeddings
         WHERE event_id IN (SELECT id FROM temp_prune_event_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM events_fts
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM history_items_fts
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM history_items_conversation_fts
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM recent_result_refs
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)
            OR event_id IN (SELECT id FROM temp_prune_event_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM history_items
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM search_units
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM events
         WHERE id IN (SELECT id FROM temp_prune_event_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM session_activity
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM sessions
         WHERE id IN (SELECT id FROM temp_prune_session_ids)",
        [],
    )?;
    conn.execute(
        "DELETE FROM raw_artifacts
         WHERE hash IN (SELECT id FROM temp_prune_raw_hashes)",
        [],
    )?;
    conn.execute(
        "DELETE FROM sources
         WHERE id IN (SELECT id FROM temp_prune_source_ids)
           AND NOT EXISTS (SELECT 1 FROM sessions WHERE source_id = sources.id)
           AND NOT EXISTS (SELECT 1 FROM events WHERE source_id = sources.id)
           AND NOT EXISTS (SELECT 1 FROM search_units WHERE source_id = sources.id)
           AND NOT EXISTS (SELECT 1 FROM raw_artifacts WHERE source_id = sources.id)",
        [],
    )?;
    let history_count = count(conn, "history_items")? as usize;
    update_projection_status(conn, "history_items_v1", history_count)?;
    update_projection_status(conn, "history_items_conversation_fts_v1", history_count)?;
    update_projection_status(conn, "search_rrf_v1", count(conn, "search_units")? as usize)?;
    Ok(())
}

fn remove_raw_artifact_blobs(blob_dir: &Path, hashes: &[String]) -> Result<(usize, u64)> {
    let mut deleted = 0;
    let mut bytes = 0;
    for hash in hashes {
        let path = blob_path(blob_dir, hash);
        if !path.exists() {
            continue;
        }
        let size = std::fs::metadata(&path)
            .with_context(|| format!("reading blob metadata {}", path.display()))?
            .len();
        std::fs::remove_file(&path)
            .with_context(|| format!("removing raw artifact blob {}", path.display()))?;
        deleted += 1;
        bytes += size;
    }
    Ok((deleted, bytes))
}

fn collect_text_column(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn count_prune_events(conn: &Connection) -> Result<u64> {
    count_prune_sql(conn, "SELECT COUNT(*) FROM temp_prune_event_ids")
}

fn count_prune_history_items(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM history_items
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
    )
}

fn count_prune_search_units(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM search_units
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)",
    )
}

fn count_prune_embeddings(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM embeddings
         WHERE unit_id IN (SELECT id FROM temp_prune_unit_ids)",
    )
}

fn count_prune_event_embeddings(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM event_embeddings
         WHERE event_id IN (SELECT id FROM temp_prune_event_ids)",
    )
}

fn count_prune_vector_rows(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM vec_embeddings_384 v
         JOIN embeddings e ON e.rowid = v.rowid
         JOIN temp_prune_unit_ids scope ON scope.id = e.unit_id",
    )
}

fn count_prune_recent_refs(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM recent_result_refs
         WHERE session_id IN (SELECT id FROM temp_prune_session_ids)
            OR event_id IN (SELECT id FROM temp_prune_event_ids)",
    )
}

fn count_prune_raw_artifacts(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM raw_artifacts
         WHERE hash IN (SELECT id FROM temp_prune_raw_hashes)",
    )
}

fn count_prune_sources(conn: &Connection) -> Result<u64> {
    count_prune_sql(
        conn,
        "SELECT COUNT(*)
         FROM temp_prune_source_ids candidate
         WHERE NOT EXISTS (
           SELECT 1
           FROM sessions s
           LEFT JOIN temp_prune_session_ids scope ON scope.id = s.id
           WHERE s.source_id = candidate.id AND scope.id IS NULL
         )
           AND NOT EXISTS (
             SELECT 1
             FROM events e
             LEFT JOIN temp_prune_session_ids scope ON scope.id = e.session_id
             WHERE e.source_id = candidate.id AND scope.id IS NULL
           )
           AND NOT EXISTS (
             SELECT 1
             FROM search_units su
             LEFT JOIN temp_prune_session_ids scope ON scope.id = su.session_id
             WHERE su.source_id = candidate.id AND scope.id IS NULL
           )
           AND NOT EXISTS (
             SELECT 1
             FROM raw_artifacts raw
             LEFT JOIN temp_prune_raw_hashes scope ON scope.id = raw.hash
             WHERE raw.source_id = candidate.id AND scope.id IS NULL
           )",
    )
}

fn count_prune_sql(conn: &Connection, sql: &str) -> Result<u64> {
    let count: i64 = conn.query_row(sql, [], |row| row.get(0))?;
    Ok(count as u64)
}

fn session_by_id(conn: &Connection, session_id: &str) -> Result<Option<SessionRecord>> {
    conn.query_row(
        "SELECT id, source_id, machine_id, source_kind, external_id, title, status,
                started_at, updated_at, metadata_json, hash
         FROM sessions
         WHERE id = ?1",
        params![session_id],
        row_session,
    )
    .optional()
    .map_err(Into::into)
}

fn event_by_id(conn: &Connection, event_id: &str) -> Result<Option<EventRecord>> {
    conn.query_row(
        "SELECT id, session_id, source_id, machine_id, source_kind, ordinal,
                event_type, role, content, raw_artifact_hash, occurred_at, metadata_json, hash
         FROM events
         WHERE id = ?1",
        params![event_id],
        row_event,
    )
    .optional()
    .map_err(Into::into)
}

fn events_for_session(conn: &Connection, session_id: &str) -> Result<Vec<EventRecord>> {
    let mut stmt = conn.prepare(
        "SELECT id, session_id, source_id, machine_id, source_kind, ordinal,
                event_type, role, content, raw_artifact_hash, occurred_at, metadata_json, hash
         FROM events
         WHERE session_id = ?1
         ORDER BY ordinal, id",
    )?;
    let rows = stmt.query_map(params![session_id], row_event)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn raw_artifact_hashes(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT hash FROM raw_artifacts ORDER BY first_seen_at, hash")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn raw_artifact_hashes_for_session_ids(
    conn: &Connection,
    session_ids: &[String],
) -> Result<Vec<String>> {
    let session_ids = normalized_ids(session_ids);
    if session_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = placeholders(session_ids.len());
    let sql = format!(
        "SELECT hash
         FROM raw_artifacts
         WHERE hash IN (
           SELECT raw_artifact_hash FROM events
           WHERE session_id IN ({placeholders}) AND raw_artifact_hash IS NOT NULL
         )
         ORDER BY first_seen_at, hash"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params_from_iter(session_ids.iter().map(String::as_str)),
        |row| row.get(0),
    )?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn upsert_recent_result_ref(conn: &Connection, result: &RecentResultRefInput) -> Result<String> {
    let now = Utc::now().to_rfc3339();
    if let Some(existing_ref) = conn
        .query_row(
            "SELECT ref
             FROM recent_result_refs
             WHERE event_id = ?1",
            params![result.event_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    {
        if !valid_recent_ref_shape(&existing_ref) {
            conn.execute(
                "DELETE FROM recent_result_refs WHERE event_id = ?1",
                params![result.event_id],
            )?;
        } else {
            conn.execute(
                "UPDATE recent_result_refs
             SET session_id = ?2,
                 source_kind = ?3,
                 occurred_at = ?4,
                 preview = ?5,
                 last_seen_at = ?6,
                 hit_count = hit_count + 1
             WHERE event_id = ?1",
                params![
                    result.event_id,
                    result.session_id,
                    result.source_kind,
                    opt_dt(result.occurred_at),
                    result.preview,
                    now
                ],
            )?;
            return Ok(existing_ref);
        }
    }

    let hash = crate::archive::blake3_hex(result.event_id.as_bytes());
    let seed = hash.strip_prefix("blake3:").unwrap_or(&hash);
    for len in 4..=16 {
        let candidate = seed[..len].to_string();
        let existing_event = conn
            .query_row(
                "SELECT event_id
                 FROM recent_result_refs
                 WHERE ref = ?1",
                params![candidate],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if existing_event.as_deref() == Some(result.event_id.as_str()) {
            return Ok(candidate);
        }
        if existing_event.is_some() {
            continue;
        }
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO recent_result_refs
             (ref, event_id, session_id, source_kind, occurred_at, preview,
              first_seen_at, last_seen_at, hit_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, 1)",
            params![
                candidate,
                result.event_id,
                result.session_id,
                result.source_kind,
                opt_dt(result.occurred_at),
                result.preview,
                now
            ],
        )?;
        if inserted > 0 {
            return Ok(candidate);
        }
    }
    bail!(
        "could not allocate recent search ref for event {}",
        result.event_id
    )
}

fn valid_recent_ref_shape(ref_id: &str) -> bool {
    (4..=16).contains(&ref_id.len())
        && ref_id
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
}

fn prune_recent_result_refs(conn: &Connection, limit: usize) -> Result<()> {
    conn.execute(
        "DELETE FROM recent_result_refs
         WHERE ref NOT IN (
           SELECT ref
           FROM recent_result_refs
           ORDER BY last_seen_at DESC, ref ASC
           LIMIT ?1
         )",
        params![limit as i64],
    )?;
    Ok(())
}

fn normalized_ids(ids: &[String]) -> Vec<String> {
    let mut ids = ids
        .iter()
        .filter(|id| !id.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

fn normalized_string_set(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn session_matches_export_filter(
    row: &SessionFilterRow,
    sources: &HashSet<String>,
    workspaces: &HashSet<String>,
    sessions: &HashSet<String>,
    since: Option<DateTime<Utc>>,
) -> bool {
    if !sessions.is_empty() && !sessions.contains(&row.id) {
        return false;
    }
    if !sources.is_empty() && !sources.contains(&row.source_kind) {
        return false;
    }
    if !workspaces.is_empty()
        && !session_workspace_values(&row.metadata).iter().any(|value| {
            workspaces
                .iter()
                .any(|workspace| path_matches_scope(value, workspace))
        })
    {
        return false;
    }
    if let Some(since) = since {
        let latest = [row.updated_at, row.latest_event_at, row.started_at]
            .into_iter()
            .flatten()
            .max();
        if latest.is_none_or(|latest| latest < since) {
            return false;
        }
    }
    true
}

fn prune_session_matches(
    row: &PruneSessionFilterRow,
    sources: &HashSet<String>,
    sessions: &HashSet<String>,
    workspace_scope: Option<&str>,
    machine_id: Option<&str>,
    machine_id_prefix: Option<&str>,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
) -> bool {
    if !sessions.is_empty() && !sessions.contains(&row.id) {
        return false;
    }
    if !sources.is_empty() && !sources.contains(&row.source_kind) {
        return false;
    }
    if let Some(machine_id) = machine_id {
        if row.machine_id != machine_id {
            return false;
        }
    }
    if let Some(machine_id_prefix) = machine_id_prefix {
        if !row.machine_id.starts_with(machine_id_prefix) {
            return false;
        }
    }
    if let Some(workspace_scope) = workspace_scope {
        if !session_workspace_values(&row.metadata)
            .iter()
            .any(|value| path_matches_scope(value, workspace_scope))
        {
            return false;
        }
    }
    let latest = [row.updated_at, row.latest_event_at, row.started_at]
        .into_iter()
        .flatten()
        .max();
    if let Some(after) = after {
        if latest.is_none_or(|latest| latest < after) {
            return false;
        }
    }
    if let Some(before) = before {
        if latest.is_none_or(|latest| latest >= before) {
            return false;
        }
    }
    true
}

fn session_workspace_values(metadata: &Value) -> Vec<String> {
    let mut values = Vec::new();
    for key in ["workspace_path", "workspace_root", "cwd"] {
        if let Some(value) = metadata.get(key).and_then(Value::as_str) {
            values.push(value.to_string());
        }
    }
    if let Some(workspace) = metadata.get("workspace") {
        for key in ["path", "root", "cwd"] {
            if let Some(value) = workspace.get(key).and_then(Value::as_str) {
                values.push(value.to_string());
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

fn parse_metadata_json(text: Option<String>) -> Value {
    text.and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or(Value::Null)
}

fn primary_workspace_value(metadata: &Value) -> Option<String> {
    metadata
        .get("workspace_path")
        .and_then(Value::as_str)
        .or_else(|| {
            metadata
                .get("workspace")
                .and_then(|workspace| workspace.get("path"))
                .and_then(Value::as_str)
        })
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn thread_last_activity_sql() -> &'static str {
    "NULLIF(MAX(COALESCE(a.last_event_at, ''), COALESCE(s.updated_at, ''), COALESCE(s.started_at, '')), '')"
}

fn thread_workspace_scope_sql() -> String {
    [
        "json_extract(s.metadata_json, '$.workspace_path')",
        "json_extract(s.metadata_json, '$.workspace_root')",
        "json_extract(s.metadata_json, '$.cwd')",
        "json_extract(s.metadata_json, '$.workspace.path')",
        "json_extract(s.metadata_json, '$.workspace.root')",
        "json_extract(s.metadata_json, '$.workspace.cwd')",
    ]
    .into_iter()
    .map(thread_workspace_value_matches_scope_sql)
    .collect::<Vec<_>>()
    .join(" OR ")
}

fn thread_workspace_value_matches_scope_sql(value_expr: &str) -> String {
    format!(
        "(rtrim(COALESCE({value_expr}, ''), '/') = :scope
          OR substr(rtrim(COALESCE({value_expr}, ''), '/'), 1, length(:scope) + 1) = :scope || '/')"
    )
}

fn positional_workspace_scope_sql() -> String {
    [
        "json_extract(s.metadata_json, '$.workspace_path')",
        "json_extract(s.metadata_json, '$.workspace_root')",
        "json_extract(s.metadata_json, '$.cwd')",
        "json_extract(s.metadata_json, '$.workspace.path')",
        "json_extract(s.metadata_json, '$.workspace.root')",
        "json_extract(s.metadata_json, '$.workspace.cwd')",
    ]
    .into_iter()
    .map(positional_workspace_value_matches_scope_sql)
    .collect::<Vec<_>>()
    .join(" OR ")
}

fn positional_workspace_value_matches_scope_sql(value_expr: &str) -> String {
    format!(
        "(rtrim(COALESCE({value_expr}, ''), '/') = ?
          OR substr(rtrim(COALESCE({value_expr}, ''), '/'), 1, length(?) + 1) = ? || '/')"
    )
}

fn push_workspace_scope_filter_params(values: &mut Vec<SqlValue>, scope: Option<&str>) {
    values.push(opt_sql_text(scope.map(str::to_string)));
    for _ in 0..6 {
        values.push(opt_sql_text(scope.map(str::to_string)));
        values.push(opt_sql_text(scope.map(str::to_string)));
        values.push(opt_sql_text(scope.map(str::to_string)));
    }
}

fn path_matches_scope(value: &str, scope: &str) -> bool {
    let value = value.trim_end_matches('/');
    let scope = scope.trim_end_matches('/');
    value == scope
        || value
            .strip_prefix(scope)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn placeholders(count: usize) -> String {
    std::iter::repeat("?")
        .take(count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn prepare_temp_id_scope(conn: &Connection, table: &str, ids: &[String]) -> Result<()> {
    match table {
        "temp_search_index_event_ids"
        | "temp_delta_event_ids"
        | "temp_delta_search_unit_ids"
        | "temp_history_item_event_ids"
        | "temp_prune_session_ids"
        | "temp_prune_event_ids"
        | "temp_prune_unit_ids"
        | "temp_prune_raw_hashes"
        | "temp_prune_source_ids" => {}
        _ => bail!("unsupported temporary id scope table: {table}"),
    }
    conn.execute(
        &format!("CREATE TEMP TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY) WITHOUT ROWID"),
        [],
    )?;
    conn.execute(&format!("DELETE FROM {table}"), [])?;
    for chunk in ids.chunks(SQLITE_BIND_CHUNK_SIZE) {
        let values = std::iter::repeat("(?)")
            .take(chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute(
            &format!("INSERT OR IGNORE INTO {table} (id) VALUES {values}"),
            params_from_iter(chunk.iter().map(String::as_str)),
        )?;
    }
    Ok(())
}

fn prepare_temp_source_file_status_scope(
    conn: &Connection,
    files: &[SourceFileFingerprint],
) -> Result<()> {
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS temp_source_file_status_scope
         (path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ms INTEGER) WITHOUT ROWID",
        [],
    )?;
    conn.execute("DELETE FROM temp_source_file_status_scope", [])?;
    for chunk in files.chunks(SQLITE_BIND_CHUNK_SIZE) {
        let values = std::iter::repeat("(?, ?, ?)")
            .take(chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let mut params = Vec::with_capacity(chunk.len() * 3);
        for file in chunk {
            params.push(SqlValue::Text(file.path.clone()));
            params.push(SqlValue::Integer(file.size as i64));
            params.push(match file.mtime_ms {
                Some(mtime_ms) => SqlValue::Integer(mtime_ms),
                None => SqlValue::Null,
            });
        }
        conn.execute(
            &format!(
                "INSERT OR REPLACE INTO temp_source_file_status_scope
                 (path, size, mtime_ms) VALUES {values}"
            ),
            params_from_iter(params),
        )?;
    }
    Ok(())
}

fn prepare_temp_events_fts_scope(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS temp_events_fts_event_ids
         (id TEXT PRIMARY KEY) WITHOUT ROWID",
        [],
    )?;
    conn.execute("DELETE FROM temp_events_fts_event_ids", [])?;
    conn.execute(
        "INSERT OR IGNORE INTO temp_events_fts_event_ids (id)
         SELECT event_id FROM events_fts",
        [],
    )?;
    Ok(())
}

fn repeated_id_params(ids: &[String], repeats: usize) -> Vec<&str> {
    let mut out = Vec::with_capacity(ids.len() * repeats);
    for _ in 0..repeats {
        for id in ids {
            out.push(id.as_str());
        }
    }
    out
}

fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sources (
          id TEXT PRIMARY KEY,
          kind TEXT NOT NULL,
          identity TEXT NOT NULL,
          path TEXT,
          first_seen_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          hash TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS raw_artifacts (
          hash TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          path TEXT NOT NULL,
          size INTEGER NOT NULL,
          mtime_ms INTEGER,
          media_type TEXT NOT NULL,
          content BLOB NOT NULL,
          first_seen_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_raw_artifacts_source
          ON raw_artifacts(source_id);

        CREATE INDEX IF NOT EXISTS idx_raw_artifacts_path_first_seen
          ON raw_artifacts(path, first_seen_at DESC);

        CREATE TABLE IF NOT EXISTS sessions (
          id TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          external_id TEXT NOT NULL,
          title TEXT,
          status TEXT NOT NULL,
          started_at TEXT,
          updated_at TEXT,
          metadata_json TEXT NOT NULL,
          hash TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_hash
          ON sessions(hash);

        CREATE INDEX IF NOT EXISTS idx_sessions_source
          ON sessions(source_id);

        CREATE INDEX IF NOT EXISTS idx_sessions_workspace_path
          ON sessions(json_extract(metadata_json, '$.workspace_path'));

        CREATE INDEX IF NOT EXISTS idx_sessions_metadata_path_workspace
          ON sessions(
            json_extract(metadata_json, '$.path'),
            json_extract(metadata_json, '$.workspace_path')
          );

        CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          ordinal INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          role TEXT,
          content TEXT NOT NULL,
          raw_artifact_hash TEXT,
          occurred_at TEXT,
          metadata_json TEXT NOT NULL,
          hash TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_events_session_ordinal
          ON events(session_id, ordinal);

        CREATE INDEX IF NOT EXISTS idx_events_hash
          ON events(hash);

        CREATE INDEX IF NOT EXISTS idx_events_source
          ON events(source_id);

        CREATE INDEX IF NOT EXISTS idx_events_raw_artifact_hash
          ON events(raw_artifact_hash);

        CREATE TABLE IF NOT EXISTS session_activity (
          session_id TEXT PRIMARY KEY,
          event_count INTEGER NOT NULL DEFAULT 0,
          first_event_at TEXT,
          last_event_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_session_activity_last_event
          ON session_activity(last_event_at);

        CREATE TABLE IF NOT EXISTS history_items (
          id TEXT PRIMARY KEY,
          event_id TEXT NOT NULL,
          session_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          ordinal INTEGER NOT NULL,
          subordinal INTEGER NOT NULL,
          tier TEXT NOT NULL,
          kind TEXT NOT NULL,
          text TEXT NOT NULL,
          text_hash TEXT NOT NULL,
          occurred_at TEXT,
          lexical_indexable INTEGER NOT NULL,
          semantic_policy TEXT NOT NULL,
          metadata_json TEXT NOT NULL,
          hash TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_history_items_event
          ON history_items(event_id, subordinal);

        CREATE INDEX IF NOT EXISTS idx_history_items_session_order
          ON history_items(session_id, ordinal, subordinal);

        CREATE INDEX IF NOT EXISTS idx_history_items_tier_kind
          ON history_items(tier, kind);

        CREATE INDEX IF NOT EXISTS idx_history_items_text_hash
          ON history_items(text_hash);

        CREATE INDEX IF NOT EXISTS idx_history_items_required_embedding_order
          ON history_items(
            tier,
            semantic_policy,
            COALESCE(occurred_at, ''),
            session_id,
            ordinal,
            subordinal,
            id
          )
          WHERE tier = 'conversation' AND semantic_policy = 'required';

        CREATE VIRTUAL TABLE IF NOT EXISTS history_items_fts USING fts5(
          item_id UNINDEXED,
          event_id UNINDEXED,
          session_id UNINDEXED,
          tier UNINDEXED,
          kind UNINDEXED,
          text,
          tokenize = 'porter unicode61'
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS history_items_conversation_fts USING fts5(
          item_id UNINDEXED,
          event_id UNINDEXED,
          session_id UNINDEXED,
          kind UNINDEXED,
          text,
          tokenize = 'porter unicode61'
        );

        CREATE TABLE IF NOT EXISTS search_units (
          id TEXT PRIMARY KEY,
          event_id TEXT NOT NULL,
          session_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          role TEXT,
          search_kind TEXT NOT NULL,
          text TEXT NOT NULL,
          text_hash TEXT NOT NULL,
          occurred_at TEXT,
          metadata_json TEXT NOT NULL,
          hash TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_search_units_event
          ON search_units(event_id);

        CREATE INDEX IF NOT EXISTS idx_search_units_session
          ON search_units(session_id);

        CREATE INDEX IF NOT EXISTS idx_search_units_source
          ON search_units(source_id);

        CREATE INDEX IF NOT EXISTS idx_search_units_text_hash
          ON search_units(text_hash);

        CREATE TABLE IF NOT EXISTS embeddings (
          id TEXT PRIMARY KEY,
          unit_id TEXT NOT NULL,
          text_hash TEXT NOT NULL,
          model_id TEXT NOT NULL,
          dims INTEGER NOT NULL,
          vector_hash TEXT NOT NULL,
          vector BLOB NOT NULL,
          producer_machine_id TEXT NOT NULL,
          embedded_at TEXT NOT NULL,
          metadata_json TEXT NOT NULL,
          hash TEXT NOT NULL,
          UNIQUE(unit_id, text_hash, model_id)
        );

        CREATE INDEX IF NOT EXISTS idx_embeddings_model
          ON embeddings(model_id);

        CREATE INDEX IF NOT EXISTS idx_embeddings_unit
          ON embeddings(unit_id);

        CREATE INDEX IF NOT EXISTS idx_embeddings_text_hash
          ON embeddings(text_hash);

        CREATE INDEX IF NOT EXISTS idx_embeddings_vector_hash
          ON embeddings(vector_hash);

        CREATE TABLE IF NOT EXISTS projection_status (
          projection_name TEXT PRIMARY KEY,
          input_high_watermark TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL,
          last_error TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_checkpoints (
          source_kind TEXT NOT NULL,
          source_identity TEXT NOT NULL,
          cursor TEXT,
          metadata_json TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (source_kind, source_identity)
        );

        CREATE INDEX IF NOT EXISTS idx_source_checkpoints_kind_updated
          ON source_checkpoints(source_kind, updated_at);

        CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
          event_id UNINDEXED,
          session_id UNINDEXED,
          source_kind UNINDEXED,
          content,
          tokenize = 'porter unicode61'
        );

        CREATE TABLE IF NOT EXISTS event_embeddings (
          event_id TEXT NOT NULL,
          model TEXT NOT NULL,
          dims INTEGER NOT NULL,
          vector_json TEXT NOT NULL,
          PRIMARY KEY (event_id, model)
        );

        CREATE TABLE IF NOT EXISTS recent_result_refs (
          ref TEXT PRIMARY KEY,
          event_id TEXT NOT NULL UNIQUE,
          session_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          occurred_at TEXT,
          preview TEXT NOT NULL,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          hit_count INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_recent_result_refs_last_seen
          ON recent_result_refs(last_seen_at);

        CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings_384
          USING vec0(embedding float[384]);
        ",
    )?;
    backfill_missing_session_activity(conn)?;
    Ok(())
}

fn backfill_missing_session_activity(conn: &Connection) -> Result<()> {
    let missing = conn.query_row(
        "SELECT EXISTS(
           SELECT 1
           FROM sessions s
           LEFT JOIN session_activity a ON a.session_id = s.id
           WHERE a.session_id IS NULL
           LIMIT 1
         )",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !missing {
        return Ok(());
    }

    conn.execute(
        "INSERT OR IGNORE INTO session_activity
         (session_id, event_count, first_event_at, last_event_at)
         SELECT s.id,
                COUNT(e.id),
                MIN(e.occurred_at),
                MAX(e.occurred_at)
         FROM sessions s
         LEFT JOIN events e ON e.session_id = s.id
         LEFT JOIN session_activity a ON a.session_id = s.id
         WHERE a.session_id IS NULL
         GROUP BY s.id",
        [],
    )?;
    Ok(())
}

pub fn f32_vector_to_blob(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * std::mem::size_of::<f32>());
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

#[cfg(test)]
pub fn f32_vector_from_blob(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        bail!("invalid f32 vector byte length {}", bytes.len());
    }
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

fn fts_query(input: &str) -> String {
    input
        .split_whitespace()
        .filter_map(|term| {
            let cleaned: String = term
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
                .collect();
            if cleaned.is_empty() {
                None
            } else if cleaned.chars().count() >= 4 {
                Some(format!("\"{}\"*", cleaned.replace('"', "\"\"")))
            } else {
                Some(format!("\"{}\"", cleaned.replace('"', "\"\"")))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn insert_source(conn: &Connection, source: &SourceRecord) -> Result<bool> {
    ensure_same_hash(conn, "sources", "id", &source.id, &source.hash)?;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO sources
         (id, kind, identity, path, first_seen_at, updated_at, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            source.id,
            source.kind,
            source.identity,
            source.path,
            source.first_seen_at.to_rfc3339(),
            source.updated_at.to_rfc3339(),
            source.hash
        ],
    )?;
    Ok(changed > 0)
}

fn insert_raw_artifact(conn: &Connection, raw: &RawArtifact, blob_dir: &Path) -> Result<bool> {
    ensure_same_hash(conn, "raw_artifacts", "hash", &raw.hash, &raw.hash)?;
    if !raw.content.is_empty() {
        write_blob(blob_dir, &raw.hash, &raw.content)?;
    }
    let changed = conn.execute(
        "INSERT OR IGNORE INTO raw_artifacts
         (hash, source_id, path, size, mtime_ms, media_type, content, first_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            raw.hash,
            raw.source_id,
            raw.path,
            raw.size,
            raw.mtime_ms,
            raw.media_type,
            Vec::<u8>::new(),
            raw.first_seen_at.to_rfc3339()
        ],
    )?;
    Ok(changed > 0)
}

fn write_blob(blob_dir: &Path, hash: &str, content: &[u8]) -> Result<()> {
    let path = blob_path(blob_dir, hash);
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating blob shard {}", parent.display()))?;
    }
    std::fs::write(&path, content).with_context(|| format!("writing blob {}", path.display()))
}

fn read_blob(blob_dir: &Path, hash: &str) -> Result<Vec<u8>> {
    let path = blob_path(blob_dir, hash);
    std::fs::read(&path).with_context(|| format!("reading blob {}", path.display()))
}

fn blob_path(blob_dir: &Path, hash: &str) -> PathBuf {
    let clean = hash.strip_prefix("blake3:").unwrap_or(hash);
    let shard = clean.get(0..2).unwrap_or("xx");
    blob_dir.join(shard).join(clean)
}

fn insert_session(conn: &Connection, session: &SessionRecord) -> Result<bool> {
    ensure_same_hash(conn, "sessions", "id", &session.id, &session.hash)?;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO sessions
         (id, source_id, machine_id, source_kind, external_id, title, status,
          started_at, updated_at, metadata_json, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            session.id,
            session.source_id,
            session.machine_id,
            session.source_kind,
            session.external_id,
            session.title,
            session.status,
            opt_dt(session.started_at),
            opt_dt(session.updated_at),
            session.metadata.to_string(),
            session.hash
        ],
    )?;
    if changed == 0 {
        update_session_title(conn, session)?;
        enrich_session_metadata(conn, session)?;
    } else {
        ensure_session_activity_row(conn, &session.id)?;
    }
    Ok(changed > 0)
}

fn update_session_title(conn: &Connection, session: &SessionRecord) -> Result<()> {
    let Some(title) = session
        .title
        .as_deref()
        .filter(|title| !title.trim().is_empty())
    else {
        return Ok(());
    };
    conn.execute(
        "UPDATE sessions
         SET title = ?2
         WHERE id = ?1
           AND coalesce(title, '') != ?2",
        params![session.id, title],
    )?;
    Ok(())
}

fn enrich_session_metadata(conn: &Connection, session: &SessionRecord) -> Result<()> {
    if !metadata_has_workspace(&session.metadata) {
        return Ok(());
    }
    conn.execute(
        "UPDATE sessions
         SET metadata_json = ?2
         WHERE id = ?1
           AND json_extract(metadata_json, '$.workspace_path') IS NULL",
        params![session.id, session.metadata.to_string()],
    )?;
    Ok(())
}

fn metadata_has_workspace(metadata: &Value) -> bool {
    metadata
        .get("workspace_path")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn ensure_session_activity_row(conn: &Connection, session_id: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO session_activity
         (session_id, event_count, first_event_at, last_event_at)
         VALUES (?1, 0, NULL, NULL)",
        params![session_id],
    )?;
    Ok(())
}

fn insert_event(conn: &Connection, event: &EventRecord) -> Result<bool> {
    ensure_same_hash(conn, "events", "id", &event.id, &event.hash)?;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO events
         (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
          role, content, raw_artifact_hash, occurred_at, metadata_json, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            event.id,
            event.session_id,
            event.source_id,
            event.machine_id,
            event.source_kind,
            event.ordinal,
            event.event_type,
            event.role,
            event.content,
            event.raw_artifact_hash,
            opt_dt(event.occurred_at),
            event.metadata.to_string(),
            event.hash
        ],
    )?;
    if changed > 0 {
        update_session_activity_for_event(conn, event)?;
    }
    Ok(changed > 0)
}

fn update_session_activity_for_event(conn: &Connection, event: &EventRecord) -> Result<()> {
    let occurred_at = opt_dt(event.occurred_at);
    conn.execute(
        "INSERT INTO session_activity
         (session_id, event_count, first_event_at, last_event_at)
         VALUES (?1, 1, ?2, ?2)
         ON CONFLICT(session_id) DO UPDATE SET
           event_count = session_activity.event_count + 1,
           first_event_at = CASE
             WHEN excluded.first_event_at IS NULL THEN session_activity.first_event_at
             WHEN session_activity.first_event_at IS NULL THEN excluded.first_event_at
             WHEN excluded.first_event_at < session_activity.first_event_at THEN excluded.first_event_at
             ELSE session_activity.first_event_at
           END,
           last_event_at = CASE
             WHEN excluded.last_event_at IS NULL THEN session_activity.last_event_at
             WHEN session_activity.last_event_at IS NULL THEN excluded.last_event_at
             WHEN excluded.last_event_at > session_activity.last_event_at THEN excluded.last_event_at
             ELSE session_activity.last_event_at
           END",
        params![event.session_id, occurred_at],
    )?;
    Ok(())
}

fn insert_history_item(conn: &Connection, item: &HistoryItemRecord) -> Result<bool> {
    ensure_same_hash(conn, "history_items", "id", &item.id, &item.hash)?;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO history_items
         (id, event_id, session_id, source_id, machine_id, source_kind, ordinal, subordinal,
          tier, kind, text, text_hash, occurred_at, lexical_indexable, semantic_policy,
          metadata_json, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        params![
            item.id,
            item.event_id,
            item.session_id,
            item.source_id,
            item.machine_id,
            item.source_kind,
            item.ordinal,
            item.subordinal,
            item.tier,
            item.kind,
            item.text,
            item.text_hash,
            opt_dt(item.occurred_at),
            item.lexical_indexable as i64,
            item.semantic_policy,
            item.metadata.to_string(),
            item.hash
        ],
    )?;
    if changed > 0 && item.lexical_indexable {
        conn.execute(
            "INSERT INTO history_items_fts (item_id, event_id, session_id, tier, kind, text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                item.id,
                item.event_id,
                item.session_id,
                item.tier,
                item.kind,
                item.text
            ],
        )?;
        if item.tier == "conversation" {
            conn.execute(
                "INSERT INTO history_items_conversation_fts (item_id, event_id, session_id, kind, text)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    item.id,
                    item.event_id,
                    item.session_id,
                    item.kind,
                    item.text
                ],
            )?;
        }
    }
    Ok(changed > 0)
}

fn insert_search_unit(conn: &Connection, unit: &SearchUnitRecord) -> Result<bool> {
    ensure_same_hash(conn, "search_units", "id", &unit.id, &unit.hash)?;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO search_units
         (id, event_id, session_id, source_id, machine_id, source_kind, role, search_kind,
          text, text_hash, occurred_at, metadata_json, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            unit.id,
            unit.event_id,
            unit.session_id,
            unit.source_id,
            unit.machine_id,
            unit.source_kind,
            unit.role,
            unit.search_kind,
            unit.text,
            unit.text_hash,
            opt_dt(unit.occurred_at),
            unit.metadata.to_string(),
            unit.hash
        ],
    )?;
    Ok(changed > 0)
}

fn insert_search_index_rows(
    conn: &Connection,
    event: &EventForProjection,
    model: &str,
    dims: usize,
    embed: &impl Fn(&str) -> Vec<f32>,
) -> Result<()> {
    let inserted_unit = insert_search_text_unit(conn, event)?;
    let vector = embed(&event.content);
    let inserted_embedding = conn.execute(
        "INSERT OR IGNORE INTO event_embeddings (event_id, model, dims, vector_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            event.id,
            model,
            dims as i64,
            serde_json::to_string(&vector)?
        ],
    )? > 0;
    if inserted_unit || inserted_embedding || !event.fts_indexed {
        insert_event_fts_row(conn, event)?;
    }
    Ok(())
}

fn insert_search_text_index_rows(conn: &Connection, event: &EventForProjection) -> Result<()> {
    let inserted_unit = insert_search_text_unit(conn, event)?;
    if inserted_unit || !event.fts_indexed {
        insert_event_fts_row(conn, event)?;
    }
    Ok(())
}

fn insert_search_text_unit(conn: &Connection, event: &EventForProjection) -> Result<bool> {
    let unit_id = crate::archive::stable_id(&["search_unit", &event.id, &event.text_hash]);
    let unit_hash = crate::archive::stable_hash(&(
        &unit_id,
        &event.id,
        &event.text_hash,
        &event.content,
        &event.search_kind,
    ))?;
    let unit = SearchUnitRecord {
        id: unit_id,
        event_id: event.id.clone(),
        session_id: event.session_id.clone(),
        source_id: event.source_id.clone(),
        machine_id: event.machine_id.clone(),
        source_kind: event.source_kind.clone(),
        role: event.role.clone(),
        search_kind: event.search_kind.clone(),
        text: event.content.clone(),
        text_hash: event.text_hash.clone(),
        occurred_at: event.occurred_at,
        metadata: serde_json::json!({
            "derived_from": "event.search_text",
            "indexer": "search_unit_v1"
        }),
        hash: unit_hash,
    };
    insert_search_unit(conn, &unit)
}

fn insert_event_fts_row(conn: &Connection, event: &EventForProjection) -> Result<()> {
    conn.execute(
        "INSERT INTO events_fts (event_id, session_id, source_kind, content)
         VALUES (?1, ?2, ?3, ?4)",
        params![event.id, event.session_id, event.source_kind, event.content],
    )?;
    Ok(())
}

fn insert_embedding(conn: &Connection, embedding: &EmbeddingRecord) -> Result<bool> {
    ensure_same_hash(conn, "embeddings", "id", &embedding.id, &embedding.hash)?;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO embeddings
         (id, unit_id, text_hash, model_id, dims, vector_hash, vector,
          producer_machine_id, embedded_at, metadata_json, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            embedding.id,
            embedding.unit_id,
            embedding.text_hash,
            embedding.model_id,
            embedding.dims as i64,
            embedding.vector_hash,
            embedding.vector,
            embedding.producer_machine_id,
            embedding.embedded_at.to_rfc3339(),
            embedding.metadata.to_string(),
            embedding.hash
        ],
    )?;
    Ok(changed > 0)
}

fn ensure_same_hash(
    conn: &Connection,
    table: &str,
    id_col: &str,
    id: &str,
    hash: &str,
) -> Result<()> {
    let sql = format!("SELECT hash FROM {table} WHERE {id_col} = ?1");
    let existing: Option<String> = conn
        .query_row(&sql, params![id], |row| row.get(0))
        .optional()?;
    if let Some(existing) = existing {
        if existing != hash {
            bail!("record {id} already exists with different hash");
        }
    }
    Ok(())
}

fn count(conn: &Connection, table: &str) -> Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
    Ok(count as u64)
}

fn tiers_are_only_conversation(tiers: &[&str]) -> bool {
    tiers.len() == 1 && tiers[0] == "conversation"
}

fn search_index_needs_repair(conn: &Connection, model: &str) -> Result<bool> {
    let search_units: i64 =
        conn.query_row("SELECT COUNT(*) FROM search_units", [], |row| row.get(0))?;
    let embeddings: i64 = conn.query_row(
        "SELECT COUNT(*) FROM event_embeddings WHERE model = ?1",
        params![model],
        |row| row.get(0),
    )?;

    if embeddings < search_units {
        return Ok(true);
    }
    if let Some(indexed_events) = projection_status_count(conn, "search_rrf_v1")? {
        let indexed_events = indexed_events as i64;
        if search_units > 0 && indexed_events == search_units {
            return Ok(false);
        }
        if indexed_events < search_units {
            return Ok(true);
        }
    }

    let fts_events: i64 =
        conn.query_row("SELECT COUNT(*) FROM events_fts", [], |row| row.get(0))?;
    if search_units > 0 && search_units == fts_events && search_units <= embeddings {
        return Ok(false);
    }
    if fts_events < search_units {
        return Ok(true);
    }

    let indexable = indexable_event_count(conn)?;
    Ok(search_units < indexable || fts_events < indexable || embeddings < indexable)
}

fn search_text_index_needs_repair(conn: &Connection) -> Result<bool> {
    let search_units: i64 =
        conn.query_row("SELECT COUNT(*) FROM search_units", [], |row| row.get(0))?;

    if let Some(indexed_events) = projection_status_count(conn, "search_rrf_v1")? {
        let indexed_events = indexed_events as i64;
        if search_units > 0 && indexed_events == search_units {
            return Ok(false);
        }
        if indexed_events < search_units {
            return Ok(true);
        }
    }

    let fts_events: i64 =
        conn.query_row("SELECT COUNT(*) FROM events_fts", [], |row| row.get(0))?;
    if search_units > 0 && search_units == fts_events {
        return Ok(false);
    }
    if fts_events < search_units {
        return Ok(true);
    }

    let indexable = indexable_event_count(conn)?;
    Ok(search_units < indexable || fts_events < indexable)
}

fn indexable_event_count(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*)
         FROM events
         WHERE json_extract(metadata_json, '$.search_indexable') = 1
           AND length(trim(json_extract(metadata_json, '$.search_text'))) > 0",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn history_items_need_required_embedding(conn: &Connection, model_id: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
           SELECT 1
           FROM history_items hi
           LEFT JOIN embeddings e
             ON e.unit_id = hi.id
            AND e.text_hash = hi.text_hash
            AND e.model_id = ?1
           WHERE e.id IS NULL
             AND hi.tier = 'conversation'
             AND hi.semantic_policy = 'required'
             AND length(trim(hi.text)) >= ?2
           LIMIT 1
         )",
        params![model_id, SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn history_items_missing_required_embedding_count(
    conn: &Connection,
    model_id: &str,
) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM history_items hi
         LEFT JOIN embeddings e
           ON e.unit_id = hi.id
          AND e.text_hash = hi.text_hash
          AND e.model_id = ?1
         WHERE e.id IS NULL
           AND hi.tier = 'conversation'
           AND hi.semantic_policy = 'required'
           AND length(trim(hi.text)) >= ?2",
        params![model_id, SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64],
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

fn vector_projection_needs_repair(conn: &Connection) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
           SELECT 1
           FROM embeddings e
           JOIN history_items hi ON hi.id = e.unit_id
           LEFT JOIN vec_embeddings_384 v
             ON v.rowid = e.rowid
           WHERE e.dims = 384
             AND hi.semantic_policy != 'never'
             AND v.rowid IS NULL
           LIMIT 1
         )",
        [],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn record_delta(
    record: &ArchiveRecord,
    inserted: bool,
    mode: ImportDeltaMode,
    delta: &mut ImportDelta,
) {
    if mode == ImportDeltaMode::InsertedOnly && !inserted {
        return;
    }
    let track_touched = mode == ImportDeltaMode::Full || inserted;
    match record {
        ArchiveRecord::Source(source) => {
            if inserted {
                push_unique(&mut delta.inserted_sources, source.id.clone());
            }
        }
        ArchiveRecord::RawArtifact(raw) => {
            if track_touched {
                push_unique(&mut delta.touched_paths, raw.path.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_raw_artifacts, raw.hash.clone());
            }
        }
        ArchiveRecord::Session(session) => {
            if track_touched {
                push_unique(&mut delta.touched_sessions, session.id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_sessions, session.id.clone());
            }
        }
        ArchiveRecord::Event(event) => {
            if track_touched {
                push_unique(&mut delta.touched_events, event.id.clone());
                push_unique(&mut delta.touched_sessions, event.session_id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_events, event.id.clone());
            }
        }
        ArchiveRecord::SearchUnit(unit) => {
            if track_touched {
                push_unique(&mut delta.touched_search_units, unit.id.clone());
                push_unique(&mut delta.touched_events, unit.event_id.clone());
                push_unique(&mut delta.touched_sessions, unit.session_id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_search_units, unit.id.clone());
            }
        }
        ArchiveRecord::Embedding(embedding) => {
            if track_touched {
                push_unique(&mut delta.touched_embeddings, embedding.id.clone());
                push_unique(&mut delta.touched_search_units, embedding.unit_id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_embeddings, embedding.id.clone());
            }
        }
    }
}

fn extend_unique(target: &mut Vec<String>, values: Vec<String>) {
    if target.len().saturating_mul(values.len()) > 1024 {
        let mut seen = target.iter().cloned().collect::<HashSet<_>>();
        for value in values {
            if seen.insert(value.clone()) {
                target.push(value);
            }
        }
        return;
    }
    for value in values {
        push_unique(target, value);
    }
}

fn push_unique(target: &mut Vec<String>, value: String) {
    if !target.contains(&value) {
        target.push(value);
    }
}

fn raw_artifact_is_current(
    conn: &Connection,
    path: &str,
    size: u64,
    mtime_ms: Option<i64>,
) -> Result<bool> {
    let existing: Option<(i64, Option<i64>)> = conn
        .query_row(
            "SELECT size, mtime_ms
             FROM raw_artifacts
             WHERE path = ?1
             ORDER BY first_seen_at DESC
             LIMIT 1",
            params![path],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(existing
        .map(|(stored_size, stored_mtime)| stored_size == size as i64 && stored_mtime == mtime_ms)
        .unwrap_or(false))
}

fn session_workspace_metadata_missing_for_path(conn: &Connection, path: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
           SELECT 1
           FROM sessions
           WHERE json_extract(metadata_json, '$.path') = ?1
             AND json_extract(metadata_json, '$.workspace_path') IS NULL
         )",
        params![path],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

#[cfg(test)]
fn query_plan<const N: usize>(conn: &Connection, sql: &str, params: [&str; N]) -> Result<String> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params_from_iter(params), |row| row.get::<_, String>(3))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out.join("\n"))
}

fn count_indexed_events(conn: &Connection, model: &str) -> Result<usize> {
    let fts_count: i64 = conn.query_row("SELECT COUNT(*) FROM events_fts", [], |row| row.get(0))?;
    let embedding_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM event_embeddings WHERE model = ?1",
        params![model],
        |row| row.get(0),
    )?;
    Ok(fts_count.min(embedding_count) as usize)
}

fn count_text_indexed_events(conn: &Connection) -> Result<usize> {
    let fts_count: i64 = conn.query_row("SELECT COUNT(*) FROM events_fts", [], |row| row.get(0))?;
    let unit_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM search_units", [], |row| row.get(0))?;
    Ok(fts_count.min(unit_count) as usize)
}

fn indexed_count_after_incremental_refresh(
    conn: &Connection,
    newly_complete_events: usize,
    fallback: impl FnOnce() -> Result<usize>,
) -> Result<usize> {
    if let Some(current_count) = projection_status_count(conn, "search_rrf_v1")? {
        Ok(current_count.saturating_add(newly_complete_events))
    } else {
        fallback()
    }
}

fn projection_status_ready(conn: &Connection, name: &str) -> Result<bool> {
    let ready: i64 = conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM projection_status
           WHERE projection_name = ?1
             AND status = 'ready'
         )",
        params![name],
        |row| row.get(0),
    )?;
    Ok(ready != 0)
}

fn projection_status_count(conn: &Connection, name: &str) -> Result<Option<usize>> {
    let count = conn
        .query_row(
            "SELECT input_high_watermark
             FROM projection_status
             WHERE projection_name = ?1
               AND status = 'ready'",
            params![name],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(count.and_then(|value| value.parse::<usize>().ok()))
}

fn update_projection_status(conn: &Connection, name: &str, indexed_events: usize) -> Result<()> {
    conn.execute(
        "INSERT INTO projection_status
         (projection_name, input_high_watermark, status, last_error, updated_at)
         VALUES (?1, ?2, 'ready', NULL, ?3)
         ON CONFLICT(projection_name) DO UPDATE SET
           input_high_watermark = excluded.input_high_watermark,
           status = excluded.status,
           last_error = NULL,
           updated_at = excluded.updated_at",
        params![name, indexed_events.to_string(), Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn history_items_from_event(event: &EventRecord) -> Result<Vec<HistoryItemRecord>> {
    let mut items = Vec::new();

    if let Some((kind, text)) = conversation_history_text(event) {
        items.push(build_history_item(
            event,
            0,
            "conversation",
            &kind,
            text,
            true,
            "required",
            serde_json::json!({
                "derived_from": "event.search_text",
                "projector": "history_items_v2"
            }),
        )?);
    }

    if let Some((kind, text)) = tool_history_text(event) {
        items.push(build_history_item(
            event,
            10,
            "tool",
            &kind,
            &text,
            true,
            "opportunistic",
            serde_json::json!({
                "derived_from": "event.content",
                "projector": "history_items_v2"
            }),
        )?);
    }

    let raw_text = event.content.trim();
    if !raw_text.is_empty() {
        items.push(build_history_item(
            event,
            100,
            "raw",
            &raw_history_kind(event),
            raw_text,
            true,
            "never",
            serde_json::json!({
                "derived_from": "event.content",
                "projector": "history_items_v2"
            }),
        )?);
    }

    Ok(items)
}

fn conversation_history_text(event: &EventRecord) -> Option<(String, &str)> {
    let search_indexable = event
        .metadata
        .get("search_indexable")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let text = event
        .metadata
        .get("search_text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())?;
    if !search_indexable || is_instruction_text(event, text) {
        return None;
    }
    let kind = event
        .metadata
        .get("search_kind")
        .and_then(Value::as_str)
        .or(event.role.as_deref())
        .unwrap_or("unknown")
        .to_ascii_lowercase();
    match kind.as_str() {
        "user" | "assistant" => Some((kind, text)),
        "conversation" => {
            let kind = event
                .role
                .as_deref()
                .map(str::to_ascii_lowercase)
                .filter(|role| role == "user" || role == "assistant")
                .unwrap_or_else(|| "conversation".to_string());
            Some((kind, text))
        }
        "thinking" | "reasoning" => Some(("thinking".to_string(), text)),
        _ => None,
    }
}

fn tool_history_text(event: &EventRecord) -> Option<(String, String)> {
    let text = event.content.trim();
    if text.is_empty() || is_encrypted_payload_text(text) {
        return None;
    }
    if let Some(value) = parse_json_value(text) {
        if let Some((kind, text)) = tool_history_text_from_json(&value) {
            return Some((kind, text));
        }
    }
    if text.starts_with("Chunk ID:") && text.contains("\nOutput:") {
        return Some(("tool_result".to_string(), text.to_string()));
    }
    None
}

fn tool_history_text_from_json(value: &Value) -> Option<(String, String)> {
    let payload = value.get("payload").unwrap_or(value);
    let name = payload
        .get("name")
        .or_else(|| payload.get("tool"))
        .and_then(Value::as_str);
    let arguments = payload
        .get("arguments")
        .or_else(|| payload.get("input"))
        .or_else(|| payload.get("command"));
    if let Some(arguments) = arguments {
        let mut text = String::new();
        if let Some(name) = name {
            text.push_str(name);
            text.push('\n');
        }
        if let Some(arguments) = arguments.as_str() {
            text.push_str(arguments.trim());
        } else {
            text.push_str(&arguments.to_string());
        }
        let text = text.trim();
        if !text.is_empty() {
            return Some(("tool_call".to_string(), text.to_string()));
        }
    }

    for key in ["stdout", "stderr", "output", "result"] {
        if let Some(value) = payload.get(key) {
            let text = value
                .as_str()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| value.to_string());
            let text = text.trim();
            if !text.is_empty() {
                return Some(("tool_result".to_string(), text.to_string()));
            }
        }
    }
    None
}

fn parse_json_value(text: &str) -> Option<Value> {
    serde_json::from_str(text).ok()
}

fn raw_history_kind(event: &EventRecord) -> String {
    event
        .role
        .as_deref()
        .or(Some(event.event_type.as_str()))
        .unwrap_or("raw")
        .to_ascii_lowercase()
}

fn is_encrypted_payload_text(text: &str) -> bool {
    text.contains("\"encrypted_content\"")
}

fn is_instruction_text(event: &EventRecord, text: &str) -> bool {
    let role = event
        .role
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if role == "system" || role == "developer" {
        return true;
    }
    let lower = text.trim_start().to_ascii_lowercase();
    lower.starts_with("# agents.md instructions")
        || lower.starts_with("<instructions>")
        || lower.starts_with("<permissions instructions>")
        || lower.contains("<instructions>")
}

fn build_history_item(
    event: &EventRecord,
    subordinal: i64,
    tier: &str,
    kind: &str,
    text: &str,
    lexical_indexable: bool,
    semantic_policy: &str,
    metadata: Value,
) -> Result<HistoryItemRecord> {
    let text_hash = crate::archive::blake3_hex(text.as_bytes());
    let subordinal_text = subordinal.to_string();
    let id = crate::archive::stable_id(&[
        "history_item",
        &event.id,
        &subordinal_text,
        tier,
        kind,
        &text_hash,
    ]);
    let hash = crate::archive::stable_hash(&(
        &id,
        &event.id,
        event.ordinal,
        subordinal,
        tier,
        kind,
        &text_hash,
        text,
        lexical_indexable,
        semantic_policy,
        &metadata,
    ))?;
    Ok(HistoryItemRecord {
        id,
        event_id: event.id.clone(),
        session_id: event.session_id.clone(),
        source_id: event.source_id.clone(),
        machine_id: event.machine_id.clone(),
        source_kind: event.source_kind.clone(),
        ordinal: event.ordinal,
        subordinal,
        tier: tier.to_string(),
        kind: kind.to_string(),
        text: text.to_string(),
        text_hash,
        occurred_at: event.occurred_at,
        lexical_indexable,
        semantic_policy: semantic_policy.to_string(),
        metadata,
        hash,
    })
}

fn row_raw_artifact(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawArtifact> {
    Ok(RawArtifact {
        hash: row.get(0)?,
        source_id: row.get(1)?,
        path: row.get(2)?,
        size: row.get::<_, i64>(3)? as u64,
        mtime_ms: row.get(4)?,
        media_type: row.get(5)?,
        content: row.get(6)?,
        first_seen_at: parse_dt(row.get::<_, String>(7)?),
    })
}

fn row_source(row: &rusqlite::Row<'_>) -> rusqlite::Result<SourceRecord> {
    Ok(SourceRecord {
        id: row.get(0)?,
        kind: row.get(1)?,
        identity: row.get(2)?,
        path: row.get(3)?,
        first_seen_at: parse_dt(row.get::<_, String>(4)?),
        updated_at: parse_dt(row.get::<_, String>(5)?),
        hash: row.get(6)?,
    })
}

#[allow(dead_code)]
fn row_source_checkpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<SourceCheckpoint> {
    let metadata_json: String = row.get(3)?;
    Ok(SourceCheckpoint {
        source_kind: row.get(0)?,
        source_identity: row.get(1)?,
        cursor: row.get(2)?,
        metadata: serde_json::from_str(&metadata_json).unwrap_or(Value::Null),
        updated_at: parse_dt(row.get(4)?),
    })
}

fn row_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    let metadata: String = row.get(9)?;
    Ok(SessionRecord {
        id: row.get(0)?,
        source_id: row.get(1)?,
        machine_id: row.get(2)?,
        source_kind: row.get(3)?,
        external_id: row.get(4)?,
        title: row.get(5)?,
        status: row.get(6)?,
        started_at: parse_opt_dt(row.get(7)?),
        updated_at: parse_opt_dt(row.get(8)?),
        metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
        hash: row.get(10)?,
    })
}

fn row_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRecord> {
    let metadata: String = row.get(11)?;
    Ok(EventRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        source_id: row.get(2)?,
        machine_id: row.get(3)?,
        source_kind: row.get(4)?,
        ordinal: row.get(5)?,
        event_type: row.get(6)?,
        role: row.get(7)?,
        content: row.get(8)?,
        raw_artifact_hash: row.get(9)?,
        occurred_at: parse_opt_dt(row.get(10)?),
        metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
        hash: row.get(12)?,
    })
}

fn row_search_unit(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchUnitRecord> {
    let metadata: String = row.get(11)?;
    Ok(SearchUnitRecord {
        id: row.get(0)?,
        event_id: row.get(1)?,
        session_id: row.get(2)?,
        source_id: row.get(3)?,
        machine_id: row.get(4)?,
        source_kind: row.get(5)?,
        role: row.get(6)?,
        search_kind: row.get(7)?,
        text: row.get(8)?,
        text_hash: row.get(9)?,
        occurred_at: parse_opt_dt(row.get(10)?),
        metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
        hash: row.get(12)?,
    })
}

fn row_history_item(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryItemRecord> {
    let metadata: String = row.get(15)?;
    Ok(HistoryItemRecord {
        id: row.get(0)?,
        event_id: row.get(1)?,
        session_id: row.get(2)?,
        source_id: row.get(3)?,
        machine_id: row.get(4)?,
        source_kind: row.get(5)?,
        ordinal: row.get(6)?,
        subordinal: row.get(7)?,
        tier: row.get(8)?,
        kind: row.get(9)?,
        text: row.get(10)?,
        text_hash: row.get(11)?,
        occurred_at: parse_opt_dt(row.get(12)?),
        lexical_indexable: row.get::<_, i64>(13)? != 0,
        semantic_policy: row.get(14)?,
        metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
        hash: row.get(16)?,
    })
}

fn conversation_history_items_for_session(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<HistoryItemRecord>> {
    let mut stmt = conn.prepare(
        "SELECT id, event_id, session_id, source_id, machine_id, source_kind, ordinal, subordinal,
                tier, kind, text, text_hash, occurred_at, lexical_indexable, semantic_policy,
                metadata_json, hash
         FROM history_items
         WHERE session_id = ?1
           AND tier = 'conversation'
         ORDER BY ordinal, subordinal, id",
    )?;
    let rows = stmt.query_map(params![session_id], row_history_item)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn row_embedding(row: &rusqlite::Row<'_>) -> rusqlite::Result<EmbeddingRecord> {
    let metadata: String = row.get(9)?;
    Ok(EmbeddingRecord {
        id: row.get(0)?,
        unit_id: row.get(1)?,
        text_hash: row.get(2)?,
        model_id: row.get(3)?,
        dims: row.get::<_, i64>(4)? as u32,
        vector_hash: row.get(5)?,
        vector: row.get(6)?,
        producer_machine_id: row.get(7)?,
        embedded_at: parse_dt(row.get::<_, String>(8)?),
        metadata: serde_json::from_str(&metadata).unwrap_or(Value::Null),
        hash: row.get(10)?,
    })
}

fn opt_dt(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(|dt| dt.to_rfc3339())
}

fn opt_sql_text(value: Option<String>) -> SqlValue {
    value.map(SqlValue::Text).unwrap_or(SqlValue::Null)
}

fn parse_opt_dt(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .and_then(|text| DateTime::parse_from_rfc3339(&text).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn parse_dt(value: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{
        stable_hash, stable_id, EmbeddingRecord, EventRecord, RawArtifact, SearchUnitRecord,
        SessionRecord, SourceRecord,
    };
    use serde_json::json;

    #[test]
    fn sqlite_vec_search_returns_synced_embedding_without_fts_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit_with_kind(
            "user conversation about distributed memories with enough context for semantic retrieval",
            "user",
        );
        let embedding = fixture_embedding(&unit, unit_vector(0));
        store
            .import_records(&[
                ArchiveRecord::Source(fixture_source("source_vector")),
                ArchiveRecord::Session(fixture_session("session_vector", "source_vector")),
                ArchiveRecord::Event(fixture_event_with_text_kind(
                    "event_vector",
                    "session_vector",
                    "source_vector",
                    1,
                    None,
                    "event_hash_vector",
                    &unit.text,
                    "user",
                )),
                ArchiveRecord::Embedding(embedding),
            ])
            .expect("import records");
        store
            .refresh_history_items()
            .expect("refresh history items");
        store.refresh_vector_projection().expect("refresh vectors");

        let hits = store
            .vector_search(
                "fixture-semantic-384",
                &unit_vector(0),
                &["conversation"],
                5,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("vector search");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].event_id, "event_vector");
        assert_eq!(hits[0].history_item_id, unit.id);
        assert_eq!(hits[0].session_id, "session_vector");
        assert_eq!(hits[0].source_kind, "codex");
        assert_eq!(hits[0].tier, "conversation");
        assert_eq!(hits[0].search_kind, "user");
        assert_eq!(
            hits[0].content,
            "user conversation about distributed memories with enough context for semantic retrieval"
        );
        assert!(hits[0].distance <= 0.001);
    }

    #[test]
    fn vector_projection_refresh_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit_with_kind(
            "idempotent vector projection with enough user context for semantic retrieval in local history",
            "user",
        );
        let embedding = fixture_embedding(&unit, unit_vector(2));
        store
            .import_records(&[
                ArchiveRecord::Source(fixture_source("source_vector")),
                ArchiveRecord::Session(fixture_session("session_vector", "source_vector")),
                ArchiveRecord::Event(fixture_event_with_text_kind(
                    "event_vector",
                    "session_vector",
                    "source_vector",
                    1,
                    None,
                    "event_hash_vector",
                    &unit.text,
                    "user",
                )),
                ArchiveRecord::Embedding(embedding),
            ])
            .expect("import records");
        store
            .refresh_history_items()
            .expect("refresh history items");

        assert_eq!(store.refresh_vector_projection().expect("first refresh"), 1);
        assert_eq!(
            store.refresh_vector_projection().expect("second refresh"),
            1
        );
        let hits = store
            .vector_search(
                "fixture-semantic-384",
                &unit_vector(2),
                &["conversation"],
                5,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("vector search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].history_item_id, unit.id);
    }

    #[test]
    fn vector_projection_refresh_chunks_large_embedding_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let mut records = vec![
            ArchiveRecord::Source(fixture_source("source_vector")),
            ArchiveRecord::Session(fixture_session("session_vector", "source_vector")),
        ];
        for idx in 0..(SQLITE_BIND_CHUNK_SIZE * 3) {
            let event_id = format!("event_vector_{idx}");
            let text = format!("chunked vector projection {idx}");
            let unit = fixture_search_unit_for_event(&event_id, &text, "user");
            records.push(ArchiveRecord::Event(fixture_event_with_text_kind(
                &event_id,
                "session_vector",
                "source_vector",
                idx as i64,
                None,
                &format!("event_hash_vector_{idx}"),
                &text,
                "user",
            )));
            records.push(ArchiveRecord::Embedding(fixture_embedding(
                &unit,
                unit_vector(idx % 384),
            )));
        }
        let embedding_ids = records
            .iter()
            .map(|record| record.id().to_string())
            .collect::<Vec<_>>();
        store.import_records(&records).expect("import embeddings");
        store
            .refresh_history_items()
            .expect("refresh history items");

        let indexed = store
            .refresh_vector_projection_for_embeddings(&embedding_ids)
            .expect("refresh projection");

        assert_eq!(indexed, SQLITE_BIND_CHUNK_SIZE * 3);
    }

    #[test]
    fn semantic_embedding_queue_includes_required_conversation_items() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let assistant = fixture_search_unit_for_event(
            "event_vector_assistant",
            "assistant progress update with plenty of words that should become a required conversation embedding for semantic retrieval in readable history",
            "assistant",
        );
        let short_user =
            fixture_search_unit_for_event("event_vector_short_user", "short user request", "user");
        let long_user = fixture_search_unit_for_event(
            "event_vector_long_user",
            "user wants to find payment failures in agent history with enough surrounding context",
            "user",
        );
        let agents_instructions = fixture_search_unit_for_event(
            "event_vector_agents",
            "# AGENTS.md instructions for /tmp/repo <INSTRUCTIONS> Guidelines for interaction with enough surrounding context to otherwise qualify",
            "user",
        );
        let mut records = vec![
            ArchiveRecord::Source(fixture_source("source_vector")),
            ArchiveRecord::Session(fixture_session("session_vector", "source_vector")),
        ];
        for (idx, unit) in [&assistant, &short_user, &agents_instructions, &long_user]
            .iter()
            .enumerate()
        {
            records.push(ArchiveRecord::Event(fixture_event_with_text_kind(
                &unit.event_id,
                "session_vector",
                "source_vector",
                idx as i64,
                None,
                &format!("event_hash_queue_{idx}"),
                &unit.text,
                &unit.search_kind,
            )));
        }
        store.import_records(&records).expect("import events");
        store
            .refresh_history_items()
            .expect("refresh history items");

        let missing = store
            .history_items_missing_required_embedding("fixture-semantic-384", 10)
            .expect("missing embeddings");

        assert_eq!(missing.len(), 2);
        assert!(missing.iter().any(|item| item.id == assistant.id));
        assert!(missing.iter().any(|item| item.id == long_user.id));

        let first_page = store
            .history_items_missing_required_embedding("fixture-semantic-384", 1)
            .expect("first missing embedding page");
        let second_page = store
            .history_items_missing_required_embedding_after(
                "fixture-semantic-384",
                &first_page[0].cursor,
                10,
            )
            .expect("next missing embedding page");
        assert_eq!(second_page.len(), 1);
        assert_ne!(second_page[0].id, first_page[0].id);

        let plan = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "EXPLAIN QUERY PLAN
                     SELECT hi.id
                     FROM history_items hi
                     LEFT JOIN embeddings e
                       ON e.unit_id = hi.id
                      AND e.text_hash = hi.text_hash
                      AND e.model_id = ?1
                     WHERE e.id IS NULL
                       AND hi.tier = 'conversation'
                       AND hi.semantic_policy = 'required'
                       AND length(trim(hi.text)) >= ?2
                     ORDER BY COALESCE(hi.occurred_at, ''),
                              hi.session_id,
                              hi.ordinal,
                              hi.subordinal,
                              hi.id
                     LIMIT ?3",
                )?;
                let rows = stmt.query_map(
                    params![
                        "fixture-semantic-384",
                        SEMANTIC_EMBEDDING_MIN_TEXT_CHARS as i64,
                        10_i64
                    ],
                    |row| row.get::<_, String>(3),
                )?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            })
            .expect("query plan");
        assert!(
            plan.iter()
                .any(|line| line.contains("idx_history_items_required_embedding_order")),
            "missing embedding query should use the ordered partial index: {plan:?}"
        );

        assert_eq!(
            store
                .history_items_missing_required_embedding_count("fixture-semantic-384")
                .expect("missing count"),
            2
        );
        assert!(store
            .history_items_need_required_embedding("fixture-semantic-384")
            .expect("needs embedding"));

        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &assistant,
                unit_vector(4),
            )))
            .expect("import assistant embedding");
        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &long_user,
                unit_vector(4),
            )))
            .expect("import embedding");
        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &agents_instructions,
                unit_vector(4),
            )))
            .expect("import instruction embedding");
        store
            .refresh_vector_projection()
            .expect("refresh vector projection");

        let hits = store
            .vector_search(
                "fixture-semantic-384",
                &unit_vector(4),
                &["conversation"],
                5,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("vector search");

        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|hit| hit.history_item_id == long_user.id));
        assert!(hits.iter().any(|hit| hit.history_item_id == assistant.id));

        assert_eq!(
            store
                .history_items_missing_required_embedding_count("fixture-semantic-384")
                .expect("missing count"),
            0
        );
        assert!(!store
            .history_items_need_required_embedding("fixture-semantic-384")
            .expect("needs embedding"));
    }

    #[test]
    fn f32_vector_blob_round_trip_preserves_values() {
        let vector = vec![1.0, -0.25, 0.5];
        let blob = f32_vector_to_blob(&vector);
        let decoded = f32_vector_from_blob(&blob).expect("decode vector");
        assert_eq!(decoded, vector);
    }

    #[test]
    fn repeated_import_dedupes_same_archive_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let records = fixture_archive_records();

        let first = store.import_records(&records).expect("first import");
        let second = store.import_records(&records).expect("second import");

        assert_eq!(first.inserted, records.len());
        assert_eq!(first.duplicates, 0);
        assert_eq!(first.delta.inserted_events, vec!["event_vector"]);
        assert_eq!(first.delta.inserted_search_units.len(), 1);
        assert_eq!(first.delta.inserted_embeddings.len(), 1);
        assert_eq!(first.delta.touched_sessions, vec!["session_vector"]);
        assert_eq!(first.delta.touched_events, vec!["event_vector"]);
        assert_eq!(second.inserted, 0);
        assert_eq!(second.duplicates, records.len());
        assert!(second.delta.inserted_events.is_empty());
        assert_eq!(second.delta.touched_sessions, vec!["session_vector"]);
        assert_eq!(second.delta.touched_events, vec!["event_vector"]);
        assert_eq!(second.delta.touched_search_units.len(), 1);
        assert_eq!(second.delta.touched_embeddings.len(), 1);
    }

    #[test]
    fn list_threads_uses_activity_projection_for_scope_dates_and_counts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_threads");
        let mut today = fixture_session("session_today", &source.id);
        today.metadata = json!({"workspace_path": "/tmp/repo"});
        let mut nested = fixture_session("session_nested", &source.id);
        nested.metadata = json!({"workspace": {"path": "/tmp/repo/nested"}});
        let mut old = fixture_session("session_old", &source.id);
        old.metadata = json!({"workspace_path": "/tmp/repo"});
        let mut outside = fixture_session("session_outside", &source.id);
        outside.metadata = json!({"workspace_path": "/tmp/other"});

        let mut today_first = fixture_event(
            "event_today_first",
            &today.id,
            &source.id,
            1,
            None,
            "event_hash_today_first",
        );
        today_first.occurred_at = Some(dt("2026-06-06T03:00:00Z"));
        let mut today_last = fixture_event(
            "event_today_last",
            &today.id,
            &source.id,
            2,
            None,
            "event_hash_today_last",
        );
        today_last.occurred_at = Some(dt("2026-06-06T04:00:00Z"));
        let mut nested_event = fixture_event(
            "event_nested",
            &nested.id,
            &source.id,
            1,
            None,
            "event_hash_nested",
        );
        nested_event.occurred_at = Some(dt("2026-06-06T05:00:00Z"));
        let mut old_event =
            fixture_event("event_old", &old.id, &source.id, 1, None, "event_hash_old");
        old_event.occurred_at = Some(dt("2026-06-05T05:00:00Z"));
        let mut outside_event = fixture_event(
            "event_outside",
            &outside.id,
            &source.id,
            1,
            None,
            "event_hash_outside",
        );
        outside_event.occurred_at = Some(dt("2026-06-06T06:00:00Z"));

        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(today.clone()),
                ArchiveRecord::Session(nested.clone()),
                ArchiveRecord::Session(old),
                ArchiveRecord::Session(outside),
                ArchiveRecord::Event(today_first),
                ArchiveRecord::Event(today_last),
                ArchiveRecord::Event(nested_event),
                ArchiveRecord::Event(old_event),
                ArchiveRecord::Event(outside_event),
            ])
            .expect("import thread records");

        let rows = store
            .list_threads(&ThreadListOptions {
                limit: 10,
                sort: ThreadSortMode::Newest,
                after: Some(dt("2026-06-06T00:00:00Z")),
                before: Some(dt("2026-06-07T00:00:00Z")),
                workspace_scope: Some("/tmp/repo".to_string()),
            })
            .expect("list threads");

        assert_eq!(
            rows.iter()
                .map(|row| row.session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["session_nested", "session_today"]
        );
        assert_eq!(rows[0].event_count, 1);
        assert_eq!(rows[1].event_count, 2);
        assert_eq!(rows[1].first_event_at, Some(dt("2026-06-06T03:00:00Z")));
        assert_eq!(rows[1].last_event_at, Some(dt("2026-06-06T04:00:00Z")));
    }

    #[test]
    fn history_items_repair_projects_search_text_without_crossing_events() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_history_items");
        let session = fixture_session("session_history_items", &source.id);
        let mut user = fixture_event(
            "event_history_user",
            &session.id,
            &source.id,
            1,
            None,
            "event_hash_history_user",
        );
        user.role = Some("user".to_string());
        user.metadata = json!({
            "search_indexable": true,
            "search_kind": "user",
            "search_text": "please fix the transcript viewer"
        });
        let mut assistant = fixture_event(
            "event_history_assistant",
            &session.id,
            &source.id,
            2,
            None,
            "event_hash_history_assistant",
        );
        assistant.role = Some("assistant".to_string());
        assistant.metadata = json!({
            "search_indexable": true,
            "search_kind": "assistant",
            "search_text": "I will inspect the transcript renderer"
        });
        let mut skipped = fixture_event(
            "event_history_skipped",
            &session.id,
            &source.id,
            3,
            None,
            "event_hash_history_skipped",
        );
        skipped.metadata = json!({
            "search_indexable": false,
            "search_kind": "none",
            "search_text": ""
        });

        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(user.clone()),
                ArchiveRecord::Event(assistant.clone()),
                ArchiveRecord::Event(skipped.clone()),
            ])
            .expect("import records");

        assert_eq!(store.refresh_history_items().expect("full repair"), 5);
        let user_items = store
            .history_items_for_event(&user.id)
            .expect("user history items");
        let assistant_items = store
            .history_items_for_event(&assistant.id)
            .expect("assistant history items");
        let skipped_items = store
            .history_items_for_event(&skipped.id)
            .expect("skipped history items");

        assert_eq!(user_items.len(), 2);
        assert_eq!(assistant_items.len(), 2);
        assert_eq!(skipped_items.len(), 1);
        assert_eq!(
            tier_kinds(&user_items),
            vec![("conversation", "user"), ("raw", "user")]
        );
        assert_eq!(
            tier_kinds(&assistant_items),
            vec![("conversation", "assistant"), ("raw", "assistant")]
        );
        assert_eq!(tier_kinds(&skipped_items), vec![("raw", "assistant")]);
        let conversation_user = user_items
            .iter()
            .find(|item| item.tier == "conversation")
            .expect("conversation user item");
        assert_eq!(conversation_user.event_id, user.id);
        assert_eq!(conversation_user.session_id, "session_history_items");
        assert_eq!(conversation_user.ordinal, 1);
        assert_eq!(conversation_user.subordinal, 0);
        assert_eq!(conversation_user.kind, "user");
        assert_eq!(conversation_user.text, "please fix the transcript viewer");
        assert!(conversation_user.lexical_indexable);
        assert_eq!(conversation_user.semantic_policy, "required");

        let stable_id = conversation_user.id.clone();
        assert_eq!(
            store
                .refresh_history_items_for_events(std::slice::from_ref(&user.id))
                .expect("event repair"),
            5
        );
        let repaired_user_items = store
            .history_items_for_event(&user.id)
            .expect("repaired user history items");
        assert_eq!(repaired_user_items.len(), 2);
        let repaired_conversation_user = repaired_user_items
            .iter()
            .find(|item| item.tier == "conversation")
            .expect("repaired conversation user item");
        assert_eq!(repaired_conversation_user.id, stable_id);
    }

    #[test]
    fn history_item_projector_separates_bad_codex_payload_shapes() {
        let source = fixture_source("source_bad_codex_shapes");
        let session = fixture_session("session_bad_codex_shapes", &source.id);

        let mut session_meta = fixture_event(
            "event_session_meta",
            &session.id,
            &source.id,
            1,
            None,
            "event_hash_session_meta",
        );
        session_meta.event_type = "session_meta".to_string();
        session_meta.role = None;
        session_meta.content =
            r#"{"payload":{"base_instructions":{"text":"You are Codex"}}}"#.to_string();
        session_meta.metadata = json!({
            "search_indexable": false,
            "search_kind": "none",
            "search_text": ""
        });

        let mut agents = fixture_event(
            "event_agents",
            &session.id,
            &source.id,
            2,
            None,
            "event_hash_agents",
        );
        agents.role = Some("user".to_string());
        agents.content = "# AGENTS.md instructions for /tmp/repo\n<INSTRUCTIONS>".to_string();
        agents.metadata = json!({
            "search_indexable": true,
            "search_kind": "user",
            "search_text": agents.content
        });

        let mut user = fixture_event(
            "event_real_user",
            &session.id,
            &source.id,
            3,
            None,
            "event_hash_real_user",
        );
        user.role = Some("user".to_string());
        user.content = "please make transcript output readable".to_string();
        user.metadata = json!({
            "search_indexable": true,
            "search_kind": "user",
            "search_text": user.content
        });

        let mut assistant = fixture_event(
            "event_real_assistant",
            &session.id,
            &source.id,
            4,
            None,
            "event_hash_real_assistant",
        );
        assistant.role = Some("assistant".to_string());
        assistant.content = "I will inspect the renderer and projection.".to_string();
        assistant.metadata = json!({
            "search_indexable": true,
            "search_kind": "assistant",
            "search_text": assistant.content
        });

        let mut tool_call = fixture_event(
            "event_tool_call",
            &session.id,
            &source.id,
            5,
            None,
            "event_hash_tool_call",
        );
        tool_call.role = None;
        tool_call.event_type = "response_item".to_string();
        tool_call.content = r#"{"payload":{"name":"exec_command","arguments":"{\"cmd\":\"cargo test\"}","type":"function_call"}}"#.to_string();
        tool_call.metadata = json!({
            "search_indexable": false,
            "search_kind": "none",
            "search_text": ""
        });

        let mut tool_result = fixture_event(
            "event_tool_result",
            &session.id,
            &source.id,
            6,
            None,
            "event_hash_tool_result",
        );
        tool_result.role = None;
        tool_result.event_type = "response_item".to_string();
        tool_result.content =
            "Chunk ID: abc\nWall time: 0.1 seconds\nOutput:\nerror[E0425]: missing value"
                .to_string();
        tool_result.metadata = json!({
            "search_indexable": false,
            "search_kind": "none",
            "search_text": ""
        });

        let mut encrypted = fixture_event(
            "event_encrypted",
            &session.id,
            &source.id,
            7,
            None,
            "event_hash_encrypted",
        );
        encrypted.role = None;
        encrypted.event_type = "response_item".to_string();
        encrypted.content =
            r#"{"payload":{"content":null,"encrypted_content":"gAAAA..."}}"#.to_string();
        encrypted.metadata = json!({
            "search_indexable": false,
            "search_kind": "none",
            "search_text": ""
        });

        assert_eq!(
            tier_kinds(&history_items_from_event(&session_meta).expect("session meta")),
            vec![("raw", "session_meta")]
        );
        assert_eq!(
            tier_kinds(&history_items_from_event(&agents).expect("agents")),
            vec![("raw", "user")]
        );
        assert_eq!(
            tier_kinds(&history_items_from_event(&user).expect("user")),
            vec![("conversation", "user"), ("raw", "user")]
        );
        assert_eq!(
            tier_kinds(&history_items_from_event(&assistant).expect("assistant")),
            vec![("conversation", "assistant"), ("raw", "assistant")]
        );
        assert_eq!(
            tier_kinds(&history_items_from_event(&tool_call).expect("tool call")),
            vec![("tool", "tool_call"), ("raw", "response_item")]
        );
        assert_eq!(
            tier_kinds(&history_items_from_event(&tool_result).expect("tool result")),
            vec![("tool", "tool_result"), ("raw", "response_item")]
        );
        assert_eq!(
            tier_kinds(&history_items_from_event(&encrypted).expect("encrypted")),
            vec![("raw", "response_item")]
        );
    }

    #[test]
    fn history_item_projector_honors_roleless_conversation_segments() {
        let source = fixture_source("source_roleless_conversation");
        let session = fixture_session("session_roleless_conversation", &source.id);
        let mut event = fixture_event(
            "event_roleless_conversation",
            &session.id,
            &source.id,
            1,
            None,
            "event_hash_roleless_conversation",
        );
        event.role = None;
        event.event_type = "event".to_string();
        event.content =
            r#"{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]}"#
                .to_string();
        event.metadata = json!({
            "search_indexable": true,
            "search_kind": "conversation",
            "search_text": "hello\nhi"
        });

        let items = history_items_from_event(&event).expect("history items");

        assert_eq!(
            tier_kinds(&items),
            vec![("conversation", "conversation"), ("raw", "event")]
        );
        assert_eq!(items[0].text, "hello\nhi");
        assert_eq!(items[0].semantic_policy, "required");
    }

    #[test]
    fn clean_transcript_context_marks_hidden_raw_target_as_omitted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_clean_transcript");
        let session = fixture_session("session_clean_transcript", &source.id);
        let mut user = fixture_event(
            "event_clean_user",
            &session.id,
            &source.id,
            1,
            None,
            "event_hash_clean_user",
        );
        user.role = Some("user".to_string());
        user.content = "please make transcript output readable".to_string();
        user.metadata = json!({
            "search_indexable": true,
            "search_kind": "user",
            "search_text": user.content
        });
        let mut hidden = fixture_event(
            "event_clean_hidden",
            &session.id,
            &source.id,
            2,
            None,
            "event_hash_clean_hidden",
        );
        hidden.role = Some("user".to_string());
        hidden.content = "# AGENTS.md instructions for /tmp/repo <INSTRUCTIONS>".to_string();
        hidden.metadata = json!({
            "search_indexable": true,
            "search_kind": "user",
            "search_text": hidden.content
        });
        let mut assistant = fixture_event(
            "event_clean_assistant",
            &session.id,
            &source.id,
            3,
            None,
            "event_hash_clean_assistant",
        );
        assistant.role = Some("assistant".to_string());
        assistant.content = "I will keep the readable turns visible.".to_string();
        assistant.metadata = json!({
            "search_indexable": true,
            "search_kind": "assistant",
            "search_text": assistant.content
        });
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(user),
                ArchiveRecord::Event(hidden),
                ArchiveRecord::Event(assistant),
            ])
            .expect("import clean transcript records");
        store
            .refresh_history_items()
            .expect("refresh history items");

        let context = store
            .history_items_around_event("event_clean_hidden", 1, 1)
            .expect("clean context")
            .expect("context");

        assert!(context.omitted_target);
        assert_eq!(context.target_index, None);
        assert_eq!(
            context
                .items
                .iter()
                .map(|item| item.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["event_clean_user", "event_clean_assistant"]
        );
    }

    #[test]
    fn update_hot_path_query_plans_are_visible() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_query_plan");
        let session = fixture_session("session_query_plan", &source.id);
        let event = fixture_event(
            "event_query_plan",
            &session.id,
            &source.id,
            1,
            Some("user"),
            "event_hash_query_plan",
        );
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(event.clone()),
            ])
            .expect("import query plan fixture");

        let raw_plan = store
            .raw_artifact_current_query_plan()
            .expect("raw artifact plan");
        let workspace_plan = store
            .workspace_refresh_query_plan()
            .expect("workspace refresh plan");
        let index_plan = store
            .search_index_missing_rows_query_plan()
            .expect("search index plan");
        let history_plan = store
            .incremental_history_items_event_lookup_query_plan(&[event.id])
            .expect("history item event lookup plan");

        assert!(
            raw_plan.contains("SEARCH raw_artifacts"),
            "unexpected raw artifact freshness plan:\n{raw_plan}"
        );
        assert!(
            workspace_plan.contains("SEARCH sessions"),
            "unexpected workspace refresh plan:\n{workspace_plan}"
        );
        assert!(
            index_plan.contains("SCAN e") || index_plan.contains("SCAN events"),
            "unexpected search index missing-row plan:\n{index_plan}"
        );
        assert!(
            history_plan.contains("SCAN scope"),
            "incremental history projection should start from scoped event ids:\n{history_plan}"
        );
        assert!(
            history_plan.contains("SEARCH e"),
            "incremental history projection should look up events by id:\n{history_plan}"
        );
    }

    #[test]
    fn incremental_text_index_refresh_advances_projection_status_from_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_incremental_index_status");
        let session = fixture_session("session_incremental_index_status", &source.id);
        let first = fixture_event(
            "event_incremental_index_status_first",
            &session.id,
            &source.id,
            1,
            None,
            "event_hash_incremental_index_status_first",
        );
        let second = fixture_event(
            "event_incremental_index_status_second",
            &session.id,
            &source.id,
            2,
            None,
            "event_hash_incremental_index_status_second",
        );
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(first.clone()),
                ArchiveRecord::Event(second.clone()),
            ])
            .expect("import events");

        assert_eq!(
            store
                .refresh_search_text_index_for_events_with_progress(
                    std::slice::from_ref(&first.id),
                    |_, _| {}
                )
                .expect("index first event"),
            1
        );
        let first_count = store
            .with_conn(|conn| projection_status_count(conn, "search_rrf_v1"))
            .expect("projection status")
            .expect("ready projection status");
        assert_eq!(first_count, 1);

        assert_eq!(
            store
                .refresh_search_text_index_for_events_with_progress(
                    &[first.id.clone(), second.id.clone()],
                    |_, _| {}
                )
                .expect("index first and second event"),
            2
        );
        let second_count = store
            .with_conn(|conn| projection_status_count(conn, "search_rrf_v1"))
            .expect("projection status")
            .expect("ready projection status");
        assert_eq!(second_count, 2);
    }

    #[test]
    fn search_text_repair_check_uses_ready_status_and_detects_stale_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_text_repair_status");
        let session = fixture_session("session_text_repair_status", &source.id);
        let event = fixture_event(
            "event_text_repair_status",
            &session.id,
            &source.id,
            1,
            None,
            "event_hash_text_repair_status",
        );
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(event.clone()),
            ])
            .expect("import event");
        store
            .refresh_search_text_index_for_events_with_progress(
                std::slice::from_ref(&event.id),
                |_, _| {},
            )
            .expect("index event");

        assert!(!store
            .search_text_index_needs_repair()
            .expect("ready status should be healthy"));

        store
            .with_conn(|conn| update_projection_status(conn, "search_rrf_v1", 0))
            .expect("make status stale");
        assert!(store
            .search_text_index_needs_repair()
            .expect("stale status should need repair"));

        store
            .with_conn(|conn| {
                conn.execute(
                    "DELETE FROM projection_status WHERE projection_name = ?1",
                    params!["search_rrf_v1"],
                )?;
                Ok(())
            })
            .expect("remove status");
        assert!(!store
            .search_text_index_needs_repair()
            .expect("missing status should fall back to full check"));
    }

    #[test]
    fn batched_source_file_statuses_match_individual_statuses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_batched_status");
        let current_path = "/tmp/batched-current.jsonl";
        let changed_path = "/tmp/batched-changed.jsonl";
        let missing_path = "/tmp/batched-missing.jsonl";
        let mut current_raw = fixture_raw_artifact(&source.id);
        current_raw.hash = "raw_batched_current".to_string();
        current_raw.path = current_path.to_string();
        current_raw.size = 10;
        current_raw.mtime_ms = Some(100);
        let mut changed_raw = fixture_raw_artifact(&source.id);
        changed_raw.hash = "raw_batched_changed".to_string();
        changed_raw.path = changed_path.to_string();
        changed_raw.size = 20;
        changed_raw.mtime_ms = Some(200);
        let mut legacy_session = fixture_session("session_batched_status", &source.id);
        legacy_session.metadata = json!({"path": current_path});
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::RawArtifact(current_raw),
                ArchiveRecord::RawArtifact(changed_raw),
                ArchiveRecord::Session(legacy_session),
            ])
            .expect("import fixtures");
        let fingerprints = vec![
            SourceFileFingerprint {
                path: current_path.to_string(),
                size: 10,
                mtime_ms: Some(100),
            },
            SourceFileFingerprint {
                path: changed_path.to_string(),
                size: 21,
                mtime_ms: Some(200),
            },
            SourceFileFingerprint {
                path: missing_path.to_string(),
                size: 1,
                mtime_ms: None,
            },
        ];

        let batched = store
            .source_file_statuses(&fingerprints)
            .expect("batched statuses");

        for fingerprint in &fingerprints {
            let individual = store
                .source_file_status(&fingerprint.path, fingerprint.size, fingerprint.mtime_ms)
                .expect("individual status");
            let batched_status = batched
                .get(&fingerprint.path)
                .copied()
                .expect("batched status");
            assert_eq!(batched_status.raw_current, individual.raw_current);
            assert_eq!(
                batched_status.needs_workspace_refresh,
                individual.needs_workspace_refresh
            );
        }
        assert!(batched[current_path].raw_current);
        assert!(batched[current_path].needs_workspace_refresh);
        assert!(!batched[changed_path].raw_current);
        assert!(!batched[missing_path].raw_current);
    }

    #[test]
    fn same_id_with_different_hash_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let mut source = fixture_source("source_conflict");
        store
            .import_record(&ArchiveRecord::Source(source.clone()))
            .expect("first import");
        source.hash = "blake3:different".to_string();

        let err = store
            .import_record(&ArchiveRecord::Source(source))
            .expect_err("conflicting hash should fail");

        assert!(err
            .to_string()
            .contains("already exists with different hash"));
    }

    #[test]
    fn event_ids_for_hash_finds_canonical_duplicates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let hash = stable_hash(&json!({"message": "same"})).expect("hash");
        let event_a = fixture_event("event_a", "session_a", "source_a", 1, None, &hash);
        let event_b = fixture_event("event_b", "session_b", "source_b", 1, None, &hash);
        store
            .import_records(&[ArchiveRecord::Event(event_b), ArchiveRecord::Event(event_a)])
            .expect("import events");

        let ids = store.event_ids_for_hash(&hash).expect("hash lookup");

        assert_eq!(ids, vec!["event_a".to_string(), "event_b".to_string()]);
    }

    #[test]
    fn session_scoped_export_includes_dependency_closure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let records = fixture_archive_records();
        let other_session = fixture_session("session_other", "source_other");
        store
            .import_records(&records)
            .expect("import selected records");
        store
            .refresh_history_items()
            .expect("refresh history items");
        store
            .import_records(&[
                ArchiveRecord::Source(fixture_source("source_other")),
                ArchiveRecord::Session(other_session),
            ])
            .expect("import other session");

        let exported = store
            .export_records_for_session_ids(&["session_vector".to_string()])
            .expect("scoped export");

        assert!(record_id_exists(&exported, "source_vector"));
        assert!(record_id_exists(&exported, "session_vector"));
        assert!(record_id_exists(&exported, "event_vector"));
        assert!(record_id_exists(&exported, "raw_fixture"));
        assert!(record_id_exists(&exported, "session_vector"));
        assert!(exported.iter().any(|record| match record {
            ArchiveRecord::SearchUnit(unit) =>
                unit.id == fixture_search_unit("conversation about distributed memories").id,
            _ => false,
        }));
        assert!(exported
            .iter()
            .any(|record| matches!(record, ArchiveRecord::Embedding(_))));
        assert!(!record_id_exists(&exported, "session_other"));
    }

    #[test]
    fn duplicate_session_import_enriches_missing_workspace_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let mut legacy = fixture_session("session_legacy", "source_legacy");
        legacy.metadata = json!({"path": "/tmp/legacy.jsonl"});
        let mut enriched = legacy.clone();
        enriched.metadata = json!({
            "path": "/tmp/legacy.jsonl",
            "workspace_path": "/tmp/repo",
            "workspace_root": "/tmp/repo"
        });

        store
            .import_record(&ArchiveRecord::Session(legacy))
            .expect("legacy import");
        assert!(store
            .session_workspace_metadata_missing_for_path("/tmp/legacy.jsonl")
            .expect("missing workspace"));
        let stats = store
            .import_record(&ArchiveRecord::Session(enriched))
            .expect("enriched duplicate import");

        assert_eq!(stats.inserted, 0);
        assert_eq!(stats.duplicates, 1);
        assert!(!store
            .session_workspace_metadata_missing_for_path("/tmp/legacy.jsonl")
            .expect("workspace refreshed"));
        let records = store.export_records().expect("export records");
        let metadata = records
            .iter()
            .find_map(|record| match record {
                ArchiveRecord::Session(session) if session.id == "session_legacy" => {
                    Some(&session.metadata)
                }
                _ => None,
            })
            .expect("session metadata");
        assert_eq!(metadata["workspace_path"], "/tmp/repo");
    }

    #[test]
    fn duplicate_session_import_updates_stale_title() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let mut stale = fixture_session("session_title", "source_title");
        stale.title = Some("# AGENTS.md instructions for /tmp/repo <INSTRUCTIONS>".to_string());
        let mut improved = stale.clone();
        improved.title = Some("Fix thread listing titles".to_string());

        store
            .import_record(&ArchiveRecord::Session(stale))
            .expect("stale import");
        let stats = store
            .import_record(&ArchiveRecord::Session(improved))
            .expect("improved duplicate import");
        let session = store
            .session_by_id("session_title")
            .expect("session lookup")
            .expect("session exists");

        assert_eq!(stats.inserted, 0);
        assert_eq!(stats.duplicates, 1);
        assert_eq!(session.title.as_deref(), Some("Fix thread listing titles"));
    }

    #[test]
    fn transcript_lookup_returns_ordered_session_events_and_context() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = fixture_source("source_transcript");
        let session = fixture_session("session_transcript", &source.id);
        let mut first = fixture_event("event_first", &session.id, &source.id, 10, None, "hash_1");
        first.content = "first message".to_string();
        let mut middle = fixture_event("event_middle", &session.id, &source.id, 20, None, "hash_2");
        middle.content = "middle message".to_string();
        let mut last = fixture_event("event_last", &session.id, &source.id, 30, None, "hash_3");
        last.content = "last message".to_string();
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session.clone()),
                ArchiveRecord::Event(last),
                ArchiveRecord::Event(first.clone()),
                ArchiveRecord::Event(middle.clone()),
            ])
            .expect("import transcript");

        let loaded_session = store
            .session_by_id(&session.id)
            .expect("session lookup")
            .expect("session exists");
        let loaded_event = store
            .event_by_id(&middle.id)
            .expect("event lookup")
            .expect("event exists");
        let events = store
            .events_for_session(&session.id)
            .expect("session events");
        let context = store
            .events_around_event(&middle.id, 1, 1)
            .expect("context lookup")
            .expect("context exists");

        assert_eq!(loaded_session.id, session.id);
        assert_eq!(loaded_event.content, "middle message");
        assert_eq!(
            events
                .iter()
                .map(|event| event.id.as_str())
                .collect::<Vec<_>>(),
            vec!["event_first", "event_middle", "event_last"]
        );
        assert_eq!(context.session.id, session.id);
        assert_eq!(context.target_event.id, middle.id);
        assert_eq!(context.target_index, 1);
        assert_eq!(
            context
                .events
                .iter()
                .map(|event| event.content.as_str())
                .collect::<Vec<_>>(),
            vec!["first message", "middle message", "last message"]
        );
    }

    #[test]
    fn transcript_lookup_handles_missing_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");

        assert!(store
            .session_by_id("missing_session")
            .expect("session lookup")
            .is_none());
        assert!(store
            .event_by_id("missing_event")
            .expect("event lookup")
            .is_none());
        assert!(store
            .search_unit_by_id("missing_unit")
            .expect("unit lookup")
            .is_none());
        assert!(store
            .events_around_event("missing_event", 2, 2)
            .expect("context lookup")
            .is_none());
    }

    #[test]
    fn search_unit_lookup_resolves_viewer_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let records = fixture_archive_records();
        let expected_unit = records.iter().find_map(|record| match record {
            ArchiveRecord::SearchUnit(unit) => Some(unit.clone()),
            _ => None,
        });
        store.import_records(&records).expect("import records");

        let unit = store
            .search_unit_by_id(&expected_unit.as_ref().expect("fixture unit").id)
            .expect("unit lookup")
            .expect("unit exists");

        assert_eq!(unit.event_id, "event_vector");
        assert_eq!(unit.session_id, "session_vector");
    }

    #[test]
    fn viewer_origin_lookups_expose_source_raw_and_recent_ref() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let records = fixture_archive_records();
        store.import_records(&records).expect("import records");
        store
            .record_recent_result_refs(&[fixture_recent_ref_input("event_vector")])
            .expect("record ref");

        let source = store
            .source_by_id("source_vector")
            .expect("source lookup")
            .expect("source exists");
        let raw = store
            .raw_artifact_summary_by_hash("raw_fixture")
            .expect("raw lookup")
            .expect("raw exists");
        let ref_id = store
            .recent_ref_for_event_id("event_vector")
            .expect("ref lookup")
            .expect("ref exists");

        assert_eq!(source.identity, "source_vector");
        assert_eq!(raw.path, "/tmp/session.jsonl");
        assert_eq!(raw.media_type, "application/jsonl");
        assert_eq!(ref_id.len(), 4);
    }

    #[test]
    fn recent_result_refs_reuse_event_mapping() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let input = fixture_recent_ref_input("event_recent_a");

        let first = store
            .record_recent_result_refs(std::slice::from_ref(&input))
            .expect("first refs");
        let second = store
            .record_recent_result_refs(std::slice::from_ref(&input))
            .expect("second refs");
        let event_id = store
            .event_id_for_recent_ref(&first[0])
            .expect("lookup ref")
            .expect("ref exists");

        assert_eq!(first, second);
        assert_eq!(first[0].len(), 4);
        assert_eq!(event_id, "event_recent_a");
    }

    #[test]
    fn recent_result_refs_allocate_distinct_refs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let inputs = vec![
            fixture_recent_ref_input("event_recent_a"),
            fixture_recent_ref_input("event_recent_b"),
        ];

        let refs = store.record_recent_result_refs(&inputs).expect("refs");

        assert_eq!(refs.len(), 2);
        assert_ne!(refs[0], refs[1]);
        assert_eq!(
            store
                .event_id_for_recent_ref(&refs[1])
                .expect("lookup ref")
                .expect("ref exists"),
            "event_recent_b"
        );
    }

    fn record_id_exists(records: &[ArchiveRecord], id: &str) -> bool {
        records.iter().any(|record| record.id() == id)
    }

    fn tier_kinds(items: &[HistoryItemRecord]) -> Vec<(&str, &str)> {
        items
            .iter()
            .map(|item| (item.tier.as_str(), item.kind.as_str()))
            .collect()
    }

    fn dt(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("fixture datetime")
            .with_timezone(&Utc)
    }

    fn fixture_archive_records() -> Vec<ArchiveRecord> {
        let unit = fixture_search_unit("conversation about distributed memories");
        let embedding = fixture_embedding(&unit, unit_vector(0));
        let raw = fixture_raw_artifact("source_vector");
        vec![
            ArchiveRecord::Source(fixture_source("source_vector")),
            ArchiveRecord::RawArtifact(raw.clone()),
            ArchiveRecord::Session(fixture_session("session_vector", "source_vector")),
            ArchiveRecord::Event(fixture_event(
                "event_vector",
                "session_vector",
                "source_vector",
                1,
                Some(&raw.hash),
                "event_hash_vector",
            )),
            ArchiveRecord::SearchUnit(unit),
            ArchiveRecord::Embedding(embedding),
        ]
    }

    fn fixture_source(id: &str) -> SourceRecord {
        SourceRecord {
            id: id.to_string(),
            kind: "codex".to_string(),
            identity: id.to_string(),
            path: Some(format!("/tmp/{id}.jsonl")),
            first_seen_at: Utc::now(),
            updated_at: Utc::now(),
            hash: stable_hash(&(id, "source")).expect("source hash"),
        }
    }

    fn fixture_raw_artifact(source_id: &str) -> RawArtifact {
        RawArtifact {
            hash: "raw_fixture".to_string(),
            source_id: source_id.to_string(),
            path: "/tmp/session.jsonl".to_string(),
            size: 7,
            mtime_ms: Some(1),
            media_type: "application/jsonl".to_string(),
            content: b"fixture".to_vec(),
            first_seen_at: Utc::now(),
        }
    }

    fn fixture_session(id: &str, source_id: &str) -> SessionRecord {
        SessionRecord {
            id: id.to_string(),
            source_id: source_id.to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            external_id: id.to_string(),
            title: Some("fixture session".to_string()),
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({"workspace_path": "/tmp/repo"}),
            hash: stable_hash(&(id, source_id, "session")).expect("session hash"),
        }
    }

    fn fixture_event(
        id: &str,
        session_id: &str,
        source_id: &str,
        ordinal: i64,
        raw_artifact_hash: Option<&str>,
        hash: &str,
    ) -> EventRecord {
        fixture_event_with_text_kind(
            id,
            session_id,
            source_id,
            ordinal,
            raw_artifact_hash,
            hash,
            "conversation about distributed memories",
            "assistant",
        )
    }

    fn fixture_event_with_text_kind(
        id: &str,
        session_id: &str,
        source_id: &str,
        ordinal: i64,
        raw_artifact_hash: Option<&str>,
        hash: &str,
        text: &str,
        search_kind: &str,
    ) -> EventRecord {
        EventRecord {
            id: id.to_string(),
            session_id: session_id.to_string(),
            source_id: source_id.to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            ordinal,
            event_type: "message".to_string(),
            role: Some(search_kind.to_string()),
            content: text.to_string(),
            raw_artifact_hash: raw_artifact_hash.map(ToOwned::to_owned),
            occurred_at: None,
            metadata: json!({
                "fixture": true,
                "search_indexable": true,
                "search_kind": search_kind,
                "search_text": text
            }),
            hash: hash.to_string(),
        }
    }

    fn fixture_search_unit(text: &str) -> SearchUnitRecord {
        fixture_search_unit_with_kind(text, "assistant")
    }

    fn fixture_search_unit_with_kind(text: &str, search_kind: &str) -> SearchUnitRecord {
        fixture_search_unit_for_event("event_vector", text, search_kind)
    }

    fn fixture_search_unit_for_event(
        event_id: &str,
        text: &str,
        search_kind: &str,
    ) -> SearchUnitRecord {
        let text_hash = crate::archive::blake3_hex(text.as_bytes());
        let id = stable_id(&[
            "history_item",
            event_id,
            "0",
            "conversation",
            search_kind,
            &text_hash,
        ]);
        let hash = stable_hash(&(&id, event_id, &text_hash, text)).expect("unit hash");
        SearchUnitRecord {
            id,
            event_id: event_id.to_string(),
            session_id: "session_vector".to_string(),
            source_id: "source_vector".to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            role: Some(search_kind.to_string()),
            search_kind: search_kind.to_string(),
            text: text.to_string(),
            text_hash,
            occurred_at: None,
            metadata: json!({"fixture": true}),
            hash,
        }
    }

    fn fixture_recent_ref_input(event_id: &str) -> RecentResultRefInput {
        RecentResultRefInput {
            event_id: event_id.to_string(),
            session_id: "session_recent".to_string(),
            source_kind: "codex".to_string(),
            occurred_at: None,
            preview: format!("preview for {event_id}"),
        }
    }

    fn fixture_embedding(unit: &SearchUnitRecord, vector: Vec<f32>) -> EmbeddingRecord {
        let vector_blob = f32_vector_to_blob(&vector);
        let vector_hash = crate::archive::blake3_hex(&vector_blob);
        let id = stable_id(&[
            "embedding",
            &unit.id,
            &unit.text_hash,
            "fixture-semantic-384",
        ]);
        let hash =
            stable_hash(&(&id, &unit.id, &unit.text_hash, &vector_hash)).expect("embedding hash");
        EmbeddingRecord {
            id,
            unit_id: unit.id.clone(),
            text_hash: unit.text_hash.clone(),
            model_id: "fixture-semantic-384".to_string(),
            dims: 384,
            vector_hash,
            vector: vector_blob,
            producer_machine_id: "machine_b".to_string(),
            embedded_at: Utc::now(),
            metadata: json!({"fixture": true}),
            hash,
        }
    }

    fn unit_vector(index: usize) -> Vec<f32> {
        let mut vector = vec![0.0; 384];
        vector[index] = 1.0;
        vector
    }
}
