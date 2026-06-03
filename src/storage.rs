use crate::archive::{
    ArchiveRecord, EmbeddingRecord, EventRecord, RawArtifact, SearchUnitRecord, SessionRecord,
    SourceRecord,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const RECENT_RESULT_REF_LIMIT: usize = 10_000;
const SQLITE_BIND_CHUNK_SIZE: usize = 500;

#[derive(Debug, Clone)]
pub struct Store {
    db_path: PathBuf,
    blob_dir: PathBuf,
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
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ArchiveStats {
    pub sources: u64,
    pub raw_artifacts: u64,
    pub sessions: u64,
    pub events: u64,
    pub search_units: u64,
    pub embeddings: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SourceFileStatus {
    pub raw_current: bool,
    pub needs_workspace_refresh: bool,
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
}

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub event_id: String,
    pub session_id: String,
    pub source_kind: String,
    pub content: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub session_title: Option<String>,
    pub rank: usize,
}

#[derive(Debug, Clone)]
pub struct SearchUnitForEmbedding {
    pub id: String,
    pub text: String,
    pub text_hash: String,
}

#[derive(Debug, Clone)]
pub struct VectorSearchRow {
    pub event_id: String,
    pub unit_id: String,
    pub session_id: String,
    pub source_kind: String,
    pub content: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub session_title: Option<String>,
    pub distance: f64,
    pub rank: usize,
}

#[derive(Debug, Clone)]
pub struct TranscriptContext {
    pub session: SessionRecord,
    pub target_event: EventRecord,
    pub events: Vec<EventRecord>,
    pub target_index: usize,
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

impl Store {
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("creating data dir {}", data_dir.display()))?;
        let db_path = data_dir.join("super-cass.db");
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
            let tx = conn.unchecked_transaction()?;
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
            tx.commit()?;
            Ok(stats)
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
                     JOIN search_units su ON su.id = e.unit_id
                     WHERE su.session_id IN ({placeholders})
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
                search_units: count(conn, "search_units")?,
                embeddings: count(conn, "embeddings")?,
            })
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
    pub fn session_workspace_metadata_missing_for_path(&self, path: &str) -> Result<bool> {
        self.with_conn(|conn| session_workspace_metadata_missing_for_path(conn, path))
    }

    pub fn refresh_search_index(
        &self,
        model: &str,
        dims: usize,
        embed: impl Fn(&str) -> Vec<f32>,
    ) -> Result<usize> {
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let mut stmt = tx.prepare(
                "SELECT e.id,
                        e.session_id,
                        e.source_id,
                        e.machine_id,
                        e.source_kind,
                        e.role,
                        json_extract(e.metadata_json, '$.search_kind'),
                        json_extract(e.metadata_json, '$.search_text'),
                        e.occurred_at
                 FROM events e
                 LEFT JOIN event_embeddings emb
                   ON emb.event_id = e.id AND emb.model = ?1
                 WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                   AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                   AND emb.event_id IS NULL
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
                })
            })?;
            for row in rows {
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
                tx.execute(
                    "INSERT INTO events_fts (event_id, session_id, source_kind, content)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![event.id, event.session_id, event.source_kind, event.content],
                )?;
                let vector = embed(&event.content);
                tx.execute(
                    "INSERT OR IGNORE INTO event_embeddings (event_id, model, dims, vector_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        event.id,
                        model,
                        dims as i64,
                        serde_json::to_string(&vector)?
                    ],
                )?;
            }
            drop(stmt);

            let mut missing_unit_stmt = tx.prepare(
                "SELECT e.id,
                        e.session_id,
                        e.source_id,
                        e.machine_id,
                        e.source_kind,
                        e.role,
                        json_extract(e.metadata_json, '$.search_kind'),
                        json_extract(e.metadata_json, '$.search_text'),
                        e.occurred_at
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
                })
            })?;
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
            }
            let indexed_events = count_indexed_events(&tx, model)?;
            update_projection_status(&tx, "search_rrf_v1", indexed_events)?;
            drop(missing_unit_stmt);
            tx.commit()?;
            Ok(indexed_events)
        })
    }

    pub fn refresh_search_index_for_events(
        &self,
        model: &str,
        dims: usize,
        event_ids: &[String],
        embed: impl Fn(&str) -> Vec<f32>,
    ) -> Result<usize> {
        let event_ids = normalized_ids(event_ids);
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            if !event_ids.is_empty() {
                let placeholders = placeholders(event_ids.len());
                let sql = format!(
                    "SELECT e.id,
                            e.session_id,
                            e.source_id,
                            e.machine_id,
                            e.source_kind,
                            e.role,
                            json_extract(e.metadata_json, '$.search_kind'),
                            json_extract(e.metadata_json, '$.search_text'),
                            e.occurred_at
                     FROM events e
                     WHERE e.id IN ({placeholders})
                       AND json_extract(e.metadata_json, '$.search_indexable') = 1
                       AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                     ORDER BY e.session_id, e.ordinal, e.id",
                );
                let mut stmt = tx.prepare(&sql)?;
                let rows = stmt.query_map(
                    params_from_iter(event_ids.iter().map(String::as_str)),
                    |row| {
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
                        })
                    },
                )?;
                for row in rows {
                    insert_search_index_rows(&tx, &row?, model, dims, &embed)?;
                }
                drop(stmt);
            }
            let indexed_events = count_indexed_events(&tx, model)?;
            update_projection_status(&tx, "search_rrf_v1", indexed_events)?;
            tx.commit()?;
            Ok(indexed_events)
        })
    }

    pub fn search_fts(
        &self,
        query: &str,
        limit: usize,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Result<Vec<SearchRow>> {
        let fts_query = fts_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }
        let after = opt_dt(after);
        let before = opt_dt(before);
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT events_fts.event_id,
                        events_fts.session_id,
                        events_fts.source_kind,
                        snippet(events_fts, 3, '', '', '...', 24),
                        e.occurred_at,
                        s.title
                 FROM events_fts
                 JOIN events e ON e.id = events_fts.event_id
                 LEFT JOIN sessions s ON s.id = events_fts.session_id
                 WHERE events_fts MATCH ?1
                   AND (?2 IS NULL OR e.occurred_at >= ?2)
                   AND (?3 IS NULL OR e.occurred_at < ?3)
                 ORDER BY bm25(events_fts)
                 LIMIT ?4",
            )?;
            let rows = stmt.query_map(params![fts_query, after, before, limit as i64], |row| {
                Ok(SearchRow {
                    event_id: row.get(0)?,
                    session_id: row.get(1)?,
                    source_kind: row.get(2)?,
                    content: row.get(3)?,
                    occurred_at: parse_opt_dt(row.get(4)?),
                    session_title: row.get(5)?,
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

    pub fn record_recent_result_refs(
        &self,
        results: &[RecentResultRefInput],
    ) -> Result<Vec<String>> {
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let mut refs = Vec::with_capacity(results.len());
            for result in results {
                refs.push(upsert_recent_result_ref(&tx, result)?);
            }
            prune_recent_result_refs(&tx, RECENT_RESULT_REF_LIMIT)?;
            tx.commit()?;
            Ok(refs)
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

    pub fn search_units_missing_embedding(
        &self,
        model_id: &str,
        limit: usize,
    ) -> Result<Vec<SearchUnitForEmbedding>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT su.id, su.text, su.text_hash
                 FROM search_units su
                 LEFT JOIN embeddings e
                   ON e.unit_id = su.id
                  AND e.text_hash = su.text_hash
                  AND e.model_id = ?1
                 WHERE e.id IS NULL
                 ORDER BY su.occurred_at, su.session_id, su.id
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![model_id, limit as i64], |row| {
                Ok(SearchUnitForEmbedding {
                    id: row.get(0)?,
                    text: row.get(1)?,
                    text_hash: row.get(2)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn search_units_missing_embedding_for_delta(
        &self,
        model_id: &str,
        event_ids: &[String],
        unit_ids: &[String],
        limit: usize,
    ) -> Result<Vec<SearchUnitForEmbedding>> {
        let event_ids = normalized_ids(event_ids);
        let unit_ids = normalized_ids(unit_ids);
        if event_ids.is_empty() && unit_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_conn(|conn| {
            let event_placeholders = placeholders(event_ids.len());
            let unit_placeholders = placeholders(unit_ids.len());
            let scope = match (event_ids.is_empty(), unit_ids.is_empty()) {
                (false, false) => {
                    format!(
                        "(su.event_id IN ({event_placeholders}) OR su.id IN ({unit_placeholders}))"
                    )
                }
                (false, true) => format!("su.event_id IN ({event_placeholders})"),
                (true, false) => format!("su.id IN ({unit_placeholders})"),
                (true, true) => unreachable!(),
            };
            let sql = format!(
                "SELECT su.id, su.text, su.text_hash
                 FROM search_units su
                 LEFT JOIN embeddings e
                   ON e.unit_id = su.id
                  AND e.text_hash = su.text_hash
                  AND e.model_id = ?
                 WHERE e.id IS NULL
                   AND {scope}
                 ORDER BY su.occurred_at, su.session_id, su.id
                 LIMIT ?",
            );
            let mut params = Vec::with_capacity(2 + event_ids.len() + unit_ids.len());
            params.push(model_id.to_string());
            params.extend(event_ids);
            params.extend(unit_ids);
            params.push(limit.to_string());
            let mut stmt = conn.prepare(&sql)?;
            let rows =
                stmt.query_map(params_from_iter(params.iter().map(String::as_str)), |row| {
                    Ok(SearchUnitForEmbedding {
                        id: row.get(0)?,
                        text: row.get(1)?,
                        text_hash: row.get(2)?,
                    })
                })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn refresh_vector_projection(&self) -> Result<usize> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM vec_embeddings_384", [])?;
            let inserted = conn.execute(
                "INSERT INTO vec_embeddings_384(rowid, embedding)
                 SELECT rowid, vector
                 FROM embeddings
                 WHERE dims = 384",
                [],
            )?;
            Ok(inserted)
        })
    }

    pub fn refresh_vector_projection_for_embeddings(
        &self,
        embedding_ids: &[String],
    ) -> Result<usize> {
        let embedding_ids = normalized_ids(embedding_ids);
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
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
                     SELECT rowid, vector
                     FROM embeddings
                     WHERE dims = 384
                       AND id IN ({placeholders})"
                );
                tx.execute(
                    &insert_sql,
                    params_from_iter(chunk.iter().map(String::as_str)),
                )?;
            }
            let count = count_vec_embeddings(&tx)?;
            tx.commit()?;
            Ok(count)
        })
    }

    pub fn vector_search(
        &self,
        model_id: &str,
        query_vector: &[f32],
        limit: usize,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Result<Vec<VectorSearchRow>> {
        if query_vector.len() != 384 {
            return Ok(Vec::new());
        }
        let after = opt_dt(after);
        let before = opt_dt(before);
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT su.event_id,
                        e.unit_id,
                        su.session_id,
                        su.source_kind,
                        su.text,
                        su.occurred_at,
                        s.title,
                        vec_embeddings_384.distance
                 FROM vec_embeddings_384
                 JOIN embeddings e ON e.rowid = vec_embeddings_384.rowid
                 JOIN search_units su ON su.id = e.unit_id
                 LEFT JOIN sessions s ON s.id = su.session_id
                 WHERE vec_embeddings_384.embedding MATCH ?1
                   AND k = ?2
                   AND e.model_id = ?3
                   AND (?4 IS NULL OR su.occurred_at >= ?4)
                   AND (?5 IS NULL OR su.occurred_at < ?5)
                 ORDER BY vec_embeddings_384.distance",
            )?;
            let rows = stmt.query_map(
                params![
                    f32_vector_to_blob(query_vector),
                    limit as i64,
                    model_id,
                    after,
                    before
                ],
                |row| {
                    Ok(VectorSearchRow {
                        event_id: row.get(0)?,
                        unit_id: row.get(1)?,
                        session_id: row.get(2)?,
                        source_kind: row.get(3)?,
                        content: row.get(4)?,
                        occurred_at: parse_opt_dt(row.get(5)?),
                        session_title: row.get(6)?,
                        distance: row.get(7)?,
                        rank: 0,
                    })
                },
            )?;
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

        CREATE INDEX IF NOT EXISTS idx_events_raw_artifact_hash
          ON events(raw_artifact_hash);

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
        enrich_session_metadata(conn, session)?;
    }
    Ok(changed > 0)
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
    insert_search_unit(conn, &unit)?;

    let fts_exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM events_fts WHERE event_id = ?1)",
        params![event.id],
        |row| row.get(0),
    )?;
    if fts_exists == 0 {
        conn.execute(
            "INSERT INTO events_fts (event_id, session_id, source_kind, content)
             VALUES (?1, ?2, ?3, ?4)",
            params![event.id, event.session_id, event.source_kind, event.content],
        )?;
    }

    let vector = embed(&event.content);
    conn.execute(
        "INSERT OR IGNORE INTO event_embeddings (event_id, model, dims, vector_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            event.id,
            model,
            dims as i64,
            serde_json::to_string(&vector)?
        ],
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

fn count_vec_embeddings(conn: &Connection) -> Result<usize> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM vec_embeddings_384", [], |row| {
        row.get(0)
    })?;
    Ok(count as usize)
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
    match record {
        ArchiveRecord::Source(source) => {
            if inserted {
                push_unique(&mut delta.inserted_sources, source.id.clone());
            }
        }
        ArchiveRecord::RawArtifact(raw) => {
            if mode == ImportDeltaMode::Full {
                push_unique(&mut delta.touched_paths, raw.path.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_raw_artifacts, raw.hash.clone());
            }
        }
        ArchiveRecord::Session(session) => {
            if mode == ImportDeltaMode::Full {
                push_unique(&mut delta.touched_sessions, session.id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_sessions, session.id.clone());
            }
        }
        ArchiveRecord::Event(event) => {
            if mode == ImportDeltaMode::Full {
                push_unique(&mut delta.touched_events, event.id.clone());
                push_unique(&mut delta.touched_sessions, event.session_id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_events, event.id.clone());
            }
        }
        ArchiveRecord::SearchUnit(unit) => {
            if mode == ImportDeltaMode::Full {
                push_unique(&mut delta.touched_search_units, unit.id.clone());
                push_unique(&mut delta.touched_events, unit.event_id.clone());
                push_unique(&mut delta.touched_sessions, unit.session_id.clone());
            }
            if inserted {
                push_unique(&mut delta.inserted_search_units, unit.id.clone());
            }
        }
        ArchiveRecord::Embedding(embedding) => {
            if mode == ImportDeltaMode::Full {
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
        let unit = fixture_search_unit("conversation about distributed memories");
        let embedding = fixture_embedding(&unit, unit_vector(0));
        store
            .import_records(&[
                ArchiveRecord::SearchUnit(unit.clone()),
                ArchiveRecord::Embedding(embedding),
            ])
            .expect("import records");
        store.refresh_vector_projection().expect("refresh vectors");

        let hits = store
            .vector_search("fixture-semantic-384", &unit_vector(0), 5, None, None)
            .expect("vector search");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].event_id, "event_vector");
        assert_eq!(hits[0].unit_id, unit.id);
        assert_eq!(hits[0].session_id, "session_vector");
        assert_eq!(hits[0].source_kind, "codex");
        assert_eq!(hits[0].content, "conversation about distributed memories");
        assert!(hits[0].distance <= 0.001);
    }

    #[test]
    fn vector_projection_refresh_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit("idempotent vector projection");
        let embedding = fixture_embedding(&unit, unit_vector(2));
        store
            .import_records(&[
                ArchiveRecord::SearchUnit(unit.clone()),
                ArchiveRecord::Embedding(embedding),
            ])
            .expect("import records");

        assert_eq!(store.refresh_vector_projection().expect("first refresh"), 1);
        assert_eq!(
            store.refresh_vector_projection().expect("second refresh"),
            1
        );
        let hits = store
            .vector_search("fixture-semantic-384", &unit_vector(2), 5, None, None)
            .expect("vector search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].unit_id, unit.id);
    }

    #[test]
    fn vector_projection_refresh_chunks_large_embedding_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let records = (0..(SQLITE_BIND_CHUNK_SIZE * 3))
            .map(|idx| {
                let unit = fixture_search_unit(&format!("chunked vector projection {idx}"));
                ArchiveRecord::Embedding(fixture_embedding(&unit, unit_vector(idx % 384)))
            })
            .collect::<Vec<_>>();
        let embedding_ids = records
            .iter()
            .map(|record| record.id().to_string())
            .collect::<Vec<_>>();
        store.import_records(&records).expect("import embeddings");

        let indexed = store
            .refresh_vector_projection_for_embeddings(&embedding_ids)
            .expect("refresh projection");

        assert_eq!(indexed, records.len());
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
    fn update_hot_path_query_plans_are_visible() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");

        let raw_plan = store
            .raw_artifact_current_query_plan()
            .expect("raw artifact plan");
        let workspace_plan = store
            .workspace_refresh_query_plan()
            .expect("workspace refresh plan");
        let index_plan = store
            .search_index_missing_rows_query_plan()
            .expect("search index plan");

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
        EventRecord {
            id: id.to_string(),
            session_id: session_id.to_string(),
            source_id: source_id.to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            ordinal,
            event_type: "message".to_string(),
            role: Some("assistant".to_string()),
            content: "conversation about distributed memories".to_string(),
            raw_artifact_hash: raw_artifact_hash.map(ToOwned::to_owned),
            occurred_at: None,
            metadata: json!({"fixture": true}),
            hash: hash.to_string(),
        }
    }

    fn fixture_search_unit(text: &str) -> SearchUnitRecord {
        let text_hash = crate::archive::blake3_hex(text.as_bytes());
        let id = stable_id(&["search_unit", "event_vector", &text_hash]);
        let hash = stable_hash(&(&id, "event_vector", &text_hash, text)).expect("unit hash");
        SearchUnitRecord {
            id,
            event_id: "event_vector".to_string(),
            session_id: "session_vector".to_string(),
            source_id: "source_vector".to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            role: Some("assistant".to_string()),
            search_kind: "assistant".to_string(),
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
