use crate::ingest::{self, SessionClass};
use crate::provenance;
use crate::storage::Store;
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub const MESSAGE_PROVENANCE_PROJECTION: &str = "message_provenance";
pub const MESSAGE_PROVENANCE_VERSION: u32 = 2;
pub const SESSION_FACTS_PROJECTION: &str = "session_facts";
pub const SESSION_FACTS_VERSION: u32 = 1;

const PROJECTIONS: [Projection; 2] = [
    Projection {
        name: MESSAGE_PROVENANCE_PROJECTION,
        version: MESSAGE_PROVENANCE_VERSION,
        table: "message_provenance",
    },
    Projection {
        name: SESSION_FACTS_PROJECTION,
        version: SESSION_FACTS_VERSION,
        table: "session_facts",
    },
];

#[derive(Debug, Clone, Copy)]
struct Projection {
    name: &'static str,
    version: u32,
    table: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectionFreshness {
    pub name: &'static str,
    pub stale: bool,
    pub building: bool,
    pub version: u32,
    pub stored_version: Option<u32>,
    pub input_rowid: i64,
    pub stored_input_rowid: Option<i64>,
    pub new_event_rows: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectionState {
    version: u32,
    input_rowid: i64,
    row_count: u64,
}

pub fn rebuild_all(
    store: &Store,
    mut progress: impl FnMut(&'static str, usize, usize),
) -> Result<Vec<ProjectionFreshness>> {
    let total = PROJECTIONS.len();
    for (index, projection) in PROJECTIONS.iter().copied().enumerate() {
        progress(projection.name, index, total);
        rebuild_projection(store, projection)?;
        progress(projection.name, index + 1, total);
    }
    freshness(store)
}

pub fn freshness(store: &Store) -> Result<Vec<ProjectionFreshness>> {
    PROJECTIONS
        .iter()
        .copied()
        .map(|projection| projection_freshness(store, projection))
        .collect()
}

pub fn is_stale(store: &Store) -> Result<bool> {
    Ok(freshness(store)?.iter().any(|status| status.stale))
}

fn rebuild_projection(store: &Store, projection: Projection) -> Result<()> {
    let input_rowid = store.with_conn(max_event_rowid)?;
    set_projection_building(store, projection, input_rowid)?;

    let result = if projection.name == MESSAGE_PROVENANCE_PROJECTION {
        rebuild_message_provenance(store)
    } else {
        clear_projection(store, projection)
    };

    match result {
        Ok(()) => set_projection_ready(store, projection, input_rowid),
        Err(error) => {
            let _ = set_projection_failed(store, projection, input_rowid, &error.to_string());
            Err(error)
        }
    }
}

fn clear_projection(store: &Store, projection: Projection) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting analytics projection batch")?;
        tx.execute(&format!("DELETE FROM {}", projection.table), [])?;
        tx.commit().context("committing analytics projection batch")
    })
}

fn rebuild_message_provenance(store: &Store) -> Result<()> {
    const BATCH_SIZE: i64 = 10_000;

    clear_projection(store, PROJECTIONS[0])?;
    let repeated_templates = repeated_template_hashes(store)?;
    let mut session_classes = HashMap::new();
    let mut last_rowid = 0i64;

    loop {
        let batch = store.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT hi.rowid, hi.id, hi.session_id, hi.source_kind, hi.text,
                        hi.text_hash, hi.occurred_at, s.metadata_json, e.content
                 FROM history_items hi
                 JOIN sessions s ON s.id = hi.session_id
                 JOIN events e ON e.id = hi.event_id
                 WHERE hi.rowid > ?1
                   AND hi.tier = 'conversation'
                   AND hi.kind = 'user'
                 ORDER BY hi.rowid
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![last_rowid, BATCH_SIZE], |row| {
                Ok(ProvenanceInput {
                    rowid: row.get(0)?,
                    item_id: row.get(1)?,
                    session_id: row.get(2)?,
                    source_kind: row.get(3)?,
                    text: row.get(4)?,
                    text_hash: row.get(5)?,
                    occurred_at: row.get(6)?,
                    session_metadata: row.get(7)?,
                    event_content: row.get(8)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })?;
        if batch.is_empty() {
            break;
        }
        last_rowid = batch.last().expect("non-empty provenance batch").rowid;

        let mut rows = Vec::with_capacity(batch.len());
        for input in batch {
            let session_class = if let Some(class) = session_classes.get(&input.session_id) {
                *class
            } else {
                let metadata = serde_json::from_str::<Value>(&input.session_metadata)
                    .unwrap_or_else(|_| serde_json::json!({}));
                let class = classify_stored_session(
                    store,
                    &input.session_id,
                    &input.source_kind,
                    &metadata,
                )?;
                session_classes.insert(input.session_id.clone(), class);
                class
            };
            let event_class = ingest::classify_event(&input.source_kind, &input.event_content);
            let classification = provenance::classify_message(
                &input.text,
                repeated_templates.contains(&input.text_hash),
                session_class,
                event_class,
            );
            rows.push(ProvenanceRow {
                item_id: input.item_id,
                session_id: input.session_id,
                source_kind: input.source_kind,
                authored_by: classification.authored_by,
                sentiment_usable: classification.sentiment_usable,
                rule: classification.rule,
                occurred_at: input.occurred_at,
            });
        }
        insert_provenance_batch(store, &rows)?;
    }
    Ok(())
}

fn repeated_template_hashes(store: &Store) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT text_hash
             FROM history_items
             WHERE tier = 'conversation'
               AND kind = 'user'
               AND length(text) > 200
             GROUP BY text_hash
             HAVING COUNT(DISTINCT session_id) > 3",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<HashSet<_>>>()
            .map_err(Into::into)
    })
}

fn classify_stored_session(
    store: &Store,
    session_id: &str,
    source_kind: &str,
    metadata: &Value,
) -> Result<SessionClass> {
    let contents = match source_kind {
        "codex" => session_event_contents(store, session_id, Some("session_meta"))?,
        "claude_code" => session_event_contents(store, session_id, None)?,
        _ => Vec::new(),
    };
    let contents = contents.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(ingest::classify_session(source_kind, metadata, &contents))
}

fn session_event_contents(
    store: &Store,
    session_id: &str,
    event_type: Option<&str>,
) -> Result<Vec<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT content
             FROM events INDEXED BY idx_events_session_ordinal
             WHERE session_id = ?1
               AND (?2 IS NULL OR event_type = ?2)
             ORDER BY ordinal",
        )?;
        let rows = stmt.query_map(params![session_id, event_type], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn insert_provenance_batch(store: &Store, rows: &[ProvenanceRow<'_>]) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting message provenance batch")?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO message_provenance
                 (item_id, session_id, source_kind, authored_by, sentiment_usable, rule, occurred_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for row in rows {
                stmt.execute(params![
                    row.item_id,
                    row.session_id,
                    row.source_kind,
                    row.authored_by,
                    row.sentiment_usable,
                    row.rule,
                    row.occurred_at,
                ])?;
            }
        }
        tx.commit().context("committing message provenance batch")
    })
}

struct ProvenanceInput {
    rowid: i64,
    item_id: String,
    session_id: String,
    source_kind: String,
    text: String,
    text_hash: String,
    occurred_at: Option<String>,
    session_metadata: String,
    event_content: String,
}

struct ProvenanceRow<'a> {
    item_id: String,
    session_id: String,
    source_kind: String,
    authored_by: &'a str,
    sentiment_usable: &'a str,
    rule: &'a str,
    occurred_at: Option<String>,
}

fn projection_freshness(store: &Store, projection: Projection) -> Result<ProjectionFreshness> {
    store.with_conn(|conn| {
        let input_rowid = max_event_rowid(conn)?;
        let row_count = table_count(conn, projection.table)?;
        let stored = conn
            .query_row(
                "SELECT status, input_high_watermark
                 FROM projection_status
                 WHERE projection_name = ?1",
                params![projection.name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let (status, state) = stored
            .as_ref()
            .map(|(status, state)| {
                (
                    Some(status.as_str()),
                    serde_json::from_str::<ProjectionState>(state).ok(),
                )
            })
            .unwrap_or((None, None));
        let building = status == Some("building");
        let stale = status != Some("ready")
            || state.as_ref().map(|state| state.version) != Some(projection.version)
            || state
                .as_ref()
                .is_none_or(|state| input_rowid > state.input_rowid);
        let stored_input_rowid = state.as_ref().map(|state| state.input_rowid);

        Ok(ProjectionFreshness {
            name: projection.name,
            stale,
            building,
            version: projection.version,
            stored_version: state.as_ref().map(|state| state.version),
            input_rowid,
            stored_input_rowid,
            new_event_rows: stored_input_rowid
                .map(|stored| input_rowid.saturating_sub(stored) as u64)
                .unwrap_or(input_rowid.max(0) as u64),
            row_count,
        })
    })
}

fn max_event_rowid(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT COALESCE(MAX(rowid), 0) FROM events", [], |row| {
        row.get(0)
    })
    .context("reading analytics input high-water mark")
}

fn table_count(conn: &Connection, table: &str) -> Result<u64> {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    })
    .map(|count| count.max(0) as u64)
    .with_context(|| format!("counting {table} rows"))
}

fn set_projection_building(store: &Store, projection: Projection, input_rowid: i64) -> Result<()> {
    set_projection_status(store, projection, input_rowid, "building", None, 0)
}

fn set_projection_ready(store: &Store, projection: Projection, input_rowid: i64) -> Result<()> {
    let row_count = store.with_conn(|conn| table_count(conn, projection.table))?;
    set_projection_status(store, projection, input_rowid, "ready", None, row_count)
}

fn set_projection_failed(
    store: &Store,
    projection: Projection,
    input_rowid: i64,
    error: &str,
) -> Result<()> {
    set_projection_status(store, projection, input_rowid, "failed", Some(error), 0)
}

fn set_projection_status(
    store: &Store,
    projection: Projection,
    input_rowid: i64,
    status: &str,
    error: Option<&str>,
    row_count: u64,
) -> Result<()> {
    let state = serde_json::to_string(&ProjectionState {
        version: projection.version,
        input_rowid,
        row_count,
    })?;
    store.with_conn(|conn| {
        conn.execute(
            "INSERT INTO projection_status
             (projection_name, input_high_watermark, status, last_error, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(projection_name) DO UPDATE SET
               input_high_watermark = excluded.input_high_watermark,
               status = excluded.status,
               last_error = excluded.last_error,
               updated_at = excluded.updated_at",
            params![
                projection.name,
                state,
                status,
                error,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provenance_rebuild_classifies_every_user_conversation_item() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status, metadata_json, hash)
                    VALUES
                      ('session_sub', 'source', 'machine', 'codex', 'sub', 'open', '{}', 'session_sub_hash'),
                      ('session_human', 'source', 'machine', 'codex', 'human', 'open', '{}', 'session_human_hash');

                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, metadata_json, hash)
                    VALUES
                      ('event_meta', 'session_sub', 'source', 'machine', 'codex', 0, 'session_meta',
                       NULL, '{"payload":{"thread_source":"subagent"}}', '{}', 'event_meta_hash'),
                      ('event_sub', 'session_sub', 'source', 'machine', 'codex', 1, 'message',
                       'user', 'please review this', '{}', 'event_sub_hash'),
                      ('event_abort', 'session_human', 'source', 'machine', 'codex', 0, 'message',
                       'user', '<turn_aborted>stopped</turn_aborted>', '{}', 'event_abort_hash'),
                      ('event_image', 'session_human', 'source', 'machine', 'codex', 1, 'message',
                       'user', '<image name="photo.png">my caption', '{}', 'event_image_hash');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, lexical_indexable, semantic_policy,
                       metadata_json, hash)
                    VALUES
                      ('item_sub', 'event_sub', 'session_sub', 'source', 'machine', 'codex', 1,
                       0, 'conversation', 'user', 'please review this', 'hash_sub', 1, 'required', '{}', 'item_sub_hash'),
                      ('item_abort', 'event_abort', 'session_human', 'source', 'machine', 'codex', 0,
                       0, 'conversation', 'user', '<turn_aborted>stopped</turn_aborted>', 'hash_abort', 1, 'required', '{}', 'item_abort_hash'),
                      ('item_image', 'event_image', 'session_human', 'source', 'machine', 'codex', 1,
                       0, 'conversation', 'user', '<image name="photo.png">my caption', 'hash_image', 1, 'required', '{}', 'item_image_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert provenance fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild provenance");
        let rows = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT item_id, authored_by, sentiment_usable, rule
                     FROM message_provenance
                     ORDER BY item_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
            .expect("load provenance rows");

        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            (
                "item_abort".to_string(),
                "harness".to_string(),
                "no".to_string(),
                "tag.turn_aborted".to_string()
            )
        );
        assert_eq!(rows[1].1, "human");
        assert_eq!(rows[1].2, "strip_wrapper");
        assert_eq!(rows[2].1, "agent");
        assert_eq!(rows[2].3, "session.subagent");
    }

    #[test]
    fn rebuild_tracks_versions_and_new_event_staleness() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        insert_event(&store, "event-1", 1);

        rebuild_all(&store, |_, _, _| {}).expect("rebuild projections");
        assert!(!is_stale(&store).expect("freshness after rebuild"));
        let statuses = freshness(&store).expect("projection statuses");
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].stored_version, Some(MESSAGE_PROVENANCE_VERSION));
        assert_eq!(statuses[1].stored_version, Some(SESSION_FACTS_VERSION));

        insert_event(&store, "event-2", 2);
        let statuses = freshness(&store).expect("stale projection statuses");
        assert!(statuses.iter().all(|status| status.stale));
        assert!(statuses.iter().all(|status| status.new_event_rows == 1));

        rebuild_all(&store, |_, _, _| {}).expect("rebuild stale projections");
        assert!(!is_stale(&store).expect("freshness after second rebuild"));

        let bumped = Projection {
            version: MESSAGE_PROVENANCE_VERSION + 1,
            ..PROJECTIONS[0]
        };
        assert!(
            projection_freshness(&store, bumped)
                .expect("version bump freshness")
                .stale
        );
    }

    fn insert_event(store: &Store, id: &str, ordinal: i64) {
        store
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO events
                     (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                      role, content, occurred_at, metadata_json, hash)
                     VALUES (?1, 'session', 'source', 'machine', 'codex', ?2, 'message',
                             'user', 'hello', '2026-07-12T00:00:00Z', '{}', ?1)",
                    params![id, ordinal],
                )?;
                Ok(())
            })
            .expect("insert fixture event");
    }
}
