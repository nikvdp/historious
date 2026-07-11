use crate::storage::Store;
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

pub const MESSAGE_PROVENANCE_PROJECTION: &str = "message_provenance";
pub const MESSAGE_PROVENANCE_VERSION: u32 = 1;
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

    let result = store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting analytics projection batch")?;
        tx.execute(&format!("DELETE FROM {}", projection.table), [])?;
        tx.commit().context("committing analytics projection batch")
    });

    match result {
        Ok(()) => set_projection_ready(store, projection, input_rowid),
        Err(error) => {
            let _ = set_projection_failed(store, projection, input_rowid, &error.to_string());
            Err(error)
        }
    }
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
    fn rebuild_tracks_versions_and_new_event_staleness() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        insert_event(&store, "event-1", 1);

        rebuild_all(&store, |_, _, _| {}).expect("rebuild projections");
        assert!(!is_stale(&store).expect("freshness after rebuild"));
        let statuses = freshness(&store).expect("projection statuses");
        assert_eq!(statuses.len(), 2);
        assert!(statuses
            .iter()
            .all(|status| status.stored_version == Some(1)));

        insert_event(&store, "event-2", 2);
        let statuses = freshness(&store).expect("stale projection statuses");
        assert!(statuses.iter().all(|status| status.stale));
        assert!(statuses.iter().all(|status| status.new_event_rows == 1));

        rebuild_all(&store, |_, _, _| {}).expect("rebuild stale projections");
        assert!(!is_stale(&store).expect("freshness after second rebuild"));

        let bumped = Projection {
            version: 2,
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
