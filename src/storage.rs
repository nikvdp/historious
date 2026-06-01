use crate::archive::{
    ArchiveRecord, EmbeddingRecord, EventRecord, RawArtifact, SearchUnitRecord, SessionRecord,
    SourceRecord,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Store {
    db_path: PathBuf,
    blob_dir: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    pub inserted: usize,
    pub duplicates: usize,
    pub vectors_projected: usize,
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
    pub rank: usize,
}

#[derive(Debug, Clone)]
pub struct VectorSearchRow {
    pub event_id: String,
    pub unit_id: String,
    pub session_id: String,
    pub source_kind: String,
    pub content: String,
    pub distance: f64,
    pub rank: usize,
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

    pub fn import_record(&self, record: &ArchiveRecord) -> Result<ImportStats> {
        self.import_records(std::slice::from_ref(record))
    }

    pub fn import_records(&self, records: &[ArchiveRecord]) -> Result<ImportStats> {
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
            }
            tx.commit()?;
            Ok(stats)
        })
    }

    pub fn export_records(&self) -> Result<Vec<ArchiveRecord>> {
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
                    if raw.content.is_empty() {
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

    pub fn raw_artifact_is_current(
        &self,
        path: &str,
        size: u64,
        mtime_ms: Option<i64>,
    ) -> Result<bool> {
        self.with_conn(|conn| {
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
                .map(|(stored_size, stored_mtime)| {
                    stored_size == size as i64 && stored_mtime == mtime_ms
                })
                .unwrap_or(false))
        })
    }

    pub fn refresh_search_projection(
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
                        "projection": "search_unit_v1"
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
                    "INSERT INTO event_embeddings (event_id, model, dims, vector_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        event.id,
                        model,
                        dims as i64,
                        serde_json::to_string(&vector)?
                    ],
                )?;
            }
            let projected_events = count_projected_events(&tx, model)?;
            tx.execute(
                "INSERT INTO projection_status
                 (projection_name, input_high_watermark, status, last_error, updated_at)
                 VALUES ('search_rrf_v1', ?1, 'ready', NULL, ?2)
                 ON CONFLICT(projection_name) DO UPDATE SET
                   input_high_watermark = excluded.input_high_watermark,
                   status = excluded.status,
                   last_error = NULL,
                   updated_at = excluded.updated_at",
                params![projected_events.to_string(), Utc::now().to_rfc3339()],
            )?;
            drop(stmt);
            tx.commit()?;
            Ok(projected_events)
        })
    }

    pub fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<SearchRow>> {
        let fts_query = fts_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT event_id, session_id, source_kind, snippet(events_fts, 3, '[', ']', '...', 24)
                 FROM events_fts
                 WHERE events_fts MATCH ?1
                 ORDER BY bm25(events_fts)
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![fts_query, limit as i64], |row| {
                Ok(SearchRow {
                    event_id: row.get(0)?,
                    session_id: row.get(1)?,
                    source_kind: row.get(2)?,
                    content: row.get(3)?,
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

    pub fn refresh_vector_projection(&self) -> Result<usize> {
        self.with_conn(|conn| {
            let inserted = conn.execute(
                "INSERT OR IGNORE INTO vec_embeddings_384(rowid, embedding)
                 SELECT rowid, vector
                 FROM embeddings
                 WHERE dims = 384",
                [],
            )?;
            Ok(inserted)
        })
    }

    pub fn vector_search(
        &self,
        model_id: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorSearchRow>> {
        if query_vector.len() != 384 {
            return Ok(Vec::new());
        }
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT su.event_id,
                        e.unit_id,
                        su.session_id,
                        su.source_kind,
                        su.text,
                        vec_embeddings_384.distance
                 FROM vec_embeddings_384
                 JOIN embeddings e ON e.rowid = vec_embeddings_384.rowid
                 JOIN search_units su ON su.id = e.unit_id
                 WHERE vec_embeddings_384.embedding MATCH ?1
                   AND k = ?2
                   AND e.model_id = ?3
                 ORDER BY vec_embeddings_384.distance",
            )?;
            let rows = stmt.query_map(
                params![f32_vector_to_blob(query_vector), limit as i64, model_id],
                |row| {
                    Ok(VectorSearchRow {
                        event_id: row.get(0)?,
                        unit_id: row.get(1)?,
                        session_id: row.get(2)?,
                        source_kind: row.get(3)?,
                        content: row.get(4)?,
                        distance: row.get(5)?,
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
    write_blob(blob_dir, &raw.hash, &raw.content)?;
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
    Ok(changed > 0)
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

fn count_projected_events(conn: &Connection, model: &str) -> Result<usize> {
    let fts_count: i64 = conn.query_row("SELECT COUNT(*) FROM events_fts", [], |row| row.get(0))?;
    let embedding_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM event_embeddings WHERE model = ?1",
        params![model],
        |row| row.get(0),
    )?;
    Ok(fts_count.min(embedding_count) as usize)
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
    use crate::archive::{stable_hash, stable_id, EmbeddingRecord, SearchUnitRecord};
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
            .vector_search("fixture-semantic-384", &unit_vector(0), 5)
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
    fn f32_vector_blob_round_trip_preserves_values() {
        let vector = vec![1.0, -0.25, 0.5];
        let blob = f32_vector_to_blob(&vector);
        let decoded = f32_vector_from_blob(&blob).expect("decode vector");
        assert_eq!(decoded, vector);
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
