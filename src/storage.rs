use crate::archive::{ArchiveRecord, EventRecord, RawArtifact, SessionRecord, SourceRecord};
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
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ArchiveStats {
    pub sources: u64,
    pub raw_artifacts: u64,
    pub sessions: u64,
    pub events: u64,
}

#[derive(Debug, Clone)]
pub struct EventForProjection {
    pub id: String,
    pub session_id: String,
    pub source_kind: String,
    pub content: String,
    pub fts_present: bool,
    pub embedding_present: bool,
}

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub event_id: String,
    pub session_id: String,
    pub source_kind: String,
    pub content: String,
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
                        e.source_kind,
                        json_extract(e.metadata_json, '$.search_text'),
                        fts.event_id IS NOT NULL,
                        emb.event_id IS NOT NULL
                 FROM events e
                 LEFT JOIN events_fts fts ON fts.event_id = e.id
                 LEFT JOIN event_embeddings emb
                   ON emb.event_id = e.id AND emb.model = ?1
                 WHERE json_extract(e.metadata_json, '$.search_indexable') = 1
                   AND length(trim(json_extract(e.metadata_json, '$.search_text'))) > 0
                   AND (fts.event_id IS NULL OR emb.event_id IS NULL)
                 ORDER BY e.session_id, e.ordinal, e.id",
            )?;
            let rows = stmt.query_map(params![model], |row| {
                Ok(EventForProjection {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    source_kind: row.get(2)?,
                    content: row.get(3)?,
                    fts_present: row.get(4)?,
                    embedding_present: row.get(5)?,
                })
            })?;
            for row in rows {
                let event = row?;
                if !event.fts_present {
                    tx.execute(
                        "INSERT INTO events_fts (event_id, session_id, source_kind, content)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![event.id, event.session_id, event.source_kind, event.content],
                    )?;
                }
                if !event.embedding_present {
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

    pub fn all_embeddings(&self, model: &str) -> Result<Vec<(String, Vec<f32>)>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT event_id, vector_json FROM event_embeddings WHERE model = ?1")?;
            let rows = stmt.query_map(params![model], |row| {
                let json: String = row.get(1)?;
                let vector: Vec<f32> = serde_json::from_str(&json).unwrap_or_default();
                Ok((row.get(0)?, vector))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn load_projected_search_rows(&self, ids: &[String]) -> Result<Vec<SearchRow>> {
        self.with_conn(|conn| {
            let mut out = Vec::new();
            let mut stmt = conn.prepare(
                "SELECT event_id, session_id, source_kind, content
                 FROM events_fts
                 WHERE event_id = ?1",
            )?;
            for id in ids {
                if let Some(row) = stmt
                    .query_row(params![id], |row| {
                        Ok(SearchRow {
                            event_id: row.get(0)?,
                            session_id: row.get(1)?,
                            source_kind: row.get(2)?,
                            content: row.get(3)?,
                            rank: 0,
                        })
                    })
                    .optional()?
                {
                    out.push(row);
                }
            }
            Ok(out)
        })
    }
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
        ",
    )?;
    Ok(())
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
            } else {
                Some(format!("\"{}\"*", cleaned.replace('"', "\"\"")))
            }
        })
        .collect::<Vec<_>>()
        .join(" OR ")
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
    let count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM event_embeddings emb
         WHERE emb.model = ?1
           AND EXISTS (
             SELECT 1 FROM events_fts fts WHERE fts.event_id = emb.event_id
           )",
        params![model],
        |row| row.get(0),
    )?;
    Ok(count as usize)
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
