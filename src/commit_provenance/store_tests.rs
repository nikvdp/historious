use super::*;
use crate::archive::{ArchiveRecord, SourceRecord};
use serde_json::json;
use std::cell::Cell;

fn seed(store: &Store, id: &str, sha: &str, message: &str) {
    let source_id = format!("source-{id}");
    let source = SourceRecord {
        id: source_id.clone(),
        kind: "omp".into(),
        identity: source_id.clone(),
        path: None,
        first_seen_at: Utc::now(),
        updated_at: Utc::now(),
        hash: source_id.clone(),
    };
    let session = SessionRecord {
        id: id.into(),
        source_id: source_id.clone(),
        machine_id: "machine-a".into(),
        source_kind: "omp".into(),
        external_id: id.into(),
        title: None,
        status: "closed".into(),
        started_at: None,
        updated_at: None,
        metadata: json!({"cwd":"/repo"}),
        hash: id.into(),
    };
    let values = [
        json!({"type":"toolCall","id":"commit","name":"bash","arguments":{"command":format!("git commit -m '{}'", message)}}),
        json!({"role":"toolResult","toolCallId":"commit","isError":false,"content":[{"type":"text","text":format!("[main {sha}] {message}\n 1 file changed")}]}),
    ];
    let mut records = vec![
        ArchiveRecord::Source(source),
        ArchiveRecord::Session(session),
    ];
    for (ordinal, value) in values.into_iter().enumerate() {
        let event_id = format!("{id}-{ordinal}");
        records.push(ArchiveRecord::Event(EventRecord {
            id: event_id.clone(),
            session_id: id.into(),
            source_id: source_id.clone(),
            machine_id: "machine-a".into(),
            source_kind: "omp".into(),
            ordinal: ordinal as i64,
            event_type: "message".into(),
            role: None,
            content: value.to_string(),
            raw_artifact_hash: None,
            occurred_at: None,
            metadata: json!({}),
            hash: event_id,
        }));
    }
    store.import_records(&records).unwrap();
}

fn refresh(store: &Store) -> MaintenanceOutcome {
    maintain(store, |_| {}, || false).unwrap()
}

fn lookup(store: &Store, machine: &str, sha: &str, message: &str, limit: usize) -> CandidatePage {
    store
        .with_conn(|conn| candidates(conn, machine, &["/repo".into()], sha, message, limit))
        .unwrap()
}

#[test]
fn commit_projection_initial_empty_and_reopened_current_are_read_only_fast_paths() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.with_conn(status).unwrap().state,
        ProjectionState::Missing
    );
    assert_eq!(refresh(&store).evidence_count, 0);
    seed(&store, "first", "abc1234", "Preserve original context");
    seed(&store, "second", "def5678", "Recover indexed evidence");
    let first = refresh(&store);
    assert_eq!(first.processed_sessions, 2);
    assert_eq!(first.evidence_count, 2);
    let before = store.with_conn(status).unwrap();
    let reopened = Store::open(dir.path()).unwrap();
    let current = refresh(&reopened);
    assert_eq!(current.processed_sessions, 0);
    assert_eq!(current.evidence_count, 2);
    assert_eq!(before, reopened.with_conn(status).unwrap());
    let readonly =
        Connection::open_with_flags(store.db_path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(status(&readonly).unwrap(), before);
    let page = candidates(
        &readonly,
        "machine-a",
        &["/repo".into()],
        "abc12340000000000000000000000000000000000",
        "",
        10,
    )
    .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].session_id, "first");
    assert_eq!(
        readonly
            .query_row("SELECT total_changes()", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn commit_projection_lookup_prioritizes_hash_and_scopes_message_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    seed(&store, "first", "abc1234", "Preserve original context");
    seed(&store, "second", "def5678", "Recover indexed evidence");
    refresh(&store);
    let direct = lookup(
        &store,
        "machine-a",
        "abc12340000000000000000000000000000000000",
        "Recover indexed evidence",
        1,
    );
    assert_eq!(direct.records[0].session_id, "first");
    assert!(direct.truncated);
    let message = lookup(&store, "machine-a", "", "Recover indexed evidence", 10);
    assert_eq!(message.records[0].session_id, "second");
    let fuzzy = lookup(&store, "machine-a", "", "Recover indexed history", 10);
    assert!(fuzzy.records.iter().any(|row| row.session_id == "second"));
    assert!(lookup(
        &store,
        "machine-b",
        "abc1234",
        "Recover indexed evidence",
        10
    )
    .records
    .is_empty());
    assert!(store
        .with_conn(|conn| candidates(
            conn,
            "machine-a",
            &["/other".into()],
            "abc1234",
            "Recover indexed evidence",
            10
        ))
        .unwrap()
        .records
        .is_empty());
}

#[test]
fn commit_projection_interruption_preserves_snapshot_and_recovers_dirty_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    seed(&store, "first", "abc1234", "Preserve original context");
    refresh(&store);
    let before = store.with_conn(status).unwrap();
    seed(&store, "second", "def5678", "Recover indexed evidence");
    let cancel = Cell::new(false);
    let failed = maintain(
        &store,
        |progress| {
            if progress.phase == "publishing" {
                cancel.set(true);
            }
        },
        || cancel.get(),
    );
    assert!(failed.is_err());
    let stale = store.with_conn(status).unwrap();
    assert_eq!(stale.state, ProjectionState::Stale);
    assert!(stale.has_snapshot);
    assert_eq!(stale.evidence_count, 1);
    assert_eq!(stale.updated_at, before.updated_at);
    assert_eq!(
        lookup(&store, "machine-a", "abc1234", "", 10).records.len(),
        1
    );
    assert!(lookup(&store, "machine-a", "def5678", "", 10)
        .records
        .is_empty());
    let resumed = refresh(&store);
    assert_eq!(resumed.processed_sessions, 1);
    assert_eq!(resumed.evidence_count, 2);
}

#[test]
fn commit_projection_concurrent_mutation_rejects_publication_without_writer_lock() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    seed(&store, "first", "abc1234", "Preserve original context");
    refresh(&store);
    seed(&store, "second", "def5678", "Recover indexed evidence");
    let changed = Cell::new(false);
    let result = maintain(
        &store,
        |progress| {
            if progress.phase == "sessions" && !changed.replace(true) {
                store
                    .with_conn(|conn| {
                        conn.execute(
                            "UPDATE sessions SET machine_id = 'machine-b' WHERE id = 'second'",
                            [],
                        )?;
                        Ok(())
                    })
                    .unwrap();
            }
        },
        || false,
    );
    assert!(result.unwrap_err().to_string().contains("input changed"));
    assert_eq!(store.with_conn(status).unwrap().evidence_count, 1);
    let resumed = refresh(&store);
    assert_eq!(resumed.processed_sessions, 1);
    assert_eq!(resumed.evidence_count, 2);
    assert!(lookup(&store, "machine-a", "def5678", "", 10)
        .records
        .is_empty());
    assert_eq!(
        lookup(&store, "machine-b", "def5678", "", 10).records.len(),
        1
    );
    store
        .with_conn(|conn| {
            conn.execute("DELETE FROM events WHERE session_id = 'second'", [])?;
            conn.execute("DELETE FROM sessions WHERE id = 'second'", [])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(refresh(&store).evidence_count, 1);
    assert!(lookup(
        &store,
        "machine-b",
        "def5678",
        "Recover indexed evidence",
        10
    )
    .records
    .is_empty());
}
