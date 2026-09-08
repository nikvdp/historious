use super::{detect, CommitEvidence};
use crate::archive::{EventRecord, SessionRecord};
use crate::storage::Store;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension, Row,
    Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

const PROJECTION_NAME: &str = "commit_evidence_v1";
const DETECTOR_REVISION: &str = "1";
const MAX_SHA_LEN: usize = 64;
const MIN_SHA_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct Progress {
    pub(crate) phase: &'static str,
    pub(crate) current: usize,
    pub(crate) total: usize,
    pub(crate) evidence: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MaintenanceOutcome {
    pub(crate) processed_sessions: usize,
    pub(crate) total_sessions: usize,
    pub(crate) evidence_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectionState {
    Missing,
    Stale,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectionStatus {
    pub(crate) state: ProjectionState,
    pub(crate) has_snapshot: bool,
    pub(crate) evidence_count: usize,
    pub(crate) updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CandidatePage {
    pub(crate) records: Vec<CommitEvidence>,
    pub(crate) truncated: bool,
}

/// Install the commit-evidence projection schema and mutation journal.
///
/// The storage migration creates the archive tables before calling this hook.
/// Keeping this hook idempotent also lets older databases acquire the journal
/// without rebuilding their archive.
pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS commit_evidence (
          id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          event_id TEXT NOT NULL,
          result_event_id TEXT NOT NULL,
          call_id TEXT NOT NULL,
          cwd TEXT,
          sha TEXT NOT NULL,
          sha_key TEXT NOT NULL,
          subject TEXT,
          message TEXT,
          normalized_message TEXT,
          message_event_id TEXT,
          message_result_event_id TEXT,
          occurred_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_commit_evidence_machine_sha
          ON commit_evidence(machine_id, sha_key);
        CREATE INDEX IF NOT EXISTS idx_commit_evidence_sha
          ON commit_evidence(sha_key);
        CREATE INDEX IF NOT EXISTS idx_commit_evidence_machine_message
          ON commit_evidence(machine_id, normalized_message);
        CREATE INDEX IF NOT EXISTS idx_commit_evidence_machine_cwd
          ON commit_evidence(machine_id, cwd);
        CREATE INDEX IF NOT EXISTS idx_commit_evidence_session
          ON commit_evidence(session_id);

        CREATE VIRTUAL TABLE IF NOT EXISTS commit_evidence_fts USING fts5(
          evidence_id UNINDEXED,
          machine_id UNINDEXED,
          cwd UNINDEXED,
          subject,
          normalized_message,
          tokenize = 'porter unicode61'
        );

        CREATE TABLE IF NOT EXISTS commit_projection_input (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          revision INTEGER NOT NULL,
          updated_at TEXT NOT NULL
        );
        INSERT OR IGNORE INTO commit_projection_input
          (singleton, revision, updated_at)
          VALUES (1, 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

        CREATE TABLE IF NOT EXISTS commit_projection_dirty (
          session_id TEXT PRIMARY KEY,
          revision INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS projection_status (
          projection_name TEXT PRIMARY KEY,
          input_high_watermark TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL,
          last_error TEXT,
          updated_at TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS commit_projection_sessions_insert
        AFTER INSERT ON sessions
        BEGIN
          UPDATE commit_projection_input
             SET revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT NEW.id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          UPDATE projection_status
             SET status = 'stale', last_error = NULL
           WHERE projection_name = 'commit_evidence_v1';
        END;

        CREATE TRIGGER IF NOT EXISTS commit_projection_sessions_update
        AFTER UPDATE ON sessions
        WHEN OLD.source_id IS NOT NEW.source_id
          OR OLD.machine_id IS NOT NEW.machine_id
          OR OLD.source_kind IS NOT NEW.source_kind
          OR OLD.external_id IS NOT NEW.external_id
          OR OLD.title IS NOT NEW.title
          OR OLD.status IS NOT NEW.status
          OR OLD.started_at IS NOT NEW.started_at
          OR OLD.updated_at IS NOT NEW.updated_at
          OR OLD.metadata_json IS NOT NEW.metadata_json
          OR OLD.hash IS NOT NEW.hash
        BEGIN
          UPDATE commit_projection_input
             SET revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT NEW.id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          UPDATE projection_status
             SET status = 'stale', last_error = NULL
           WHERE projection_name = 'commit_evidence_v1';
        END;

        CREATE TRIGGER IF NOT EXISTS commit_projection_sessions_delete
        AFTER DELETE ON sessions
        BEGIN
          UPDATE commit_projection_input
             SET revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT OLD.id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          UPDATE projection_status
             SET status = 'stale', last_error = NULL
           WHERE projection_name = 'commit_evidence_v1';
        END;

        CREATE TRIGGER IF NOT EXISTS commit_projection_events_insert
        AFTER INSERT ON events
        BEGIN
          UPDATE commit_projection_input
             SET revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT NEW.session_id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          UPDATE projection_status
             SET status = 'stale', last_error = NULL
           WHERE projection_name = 'commit_evidence_v1';
        END;

        CREATE TRIGGER IF NOT EXISTS commit_projection_events_update
        AFTER UPDATE ON events
        WHEN OLD.session_id IS NOT NEW.session_id
          OR OLD.source_id IS NOT NEW.source_id
          OR OLD.machine_id IS NOT NEW.machine_id
          OR OLD.source_kind IS NOT NEW.source_kind
          OR OLD.ordinal IS NOT NEW.ordinal
          OR OLD.event_type IS NOT NEW.event_type
          OR OLD.role IS NOT NEW.role
          OR OLD.content IS NOT NEW.content
          OR OLD.raw_artifact_hash IS NOT NEW.raw_artifact_hash
          OR OLD.occurred_at IS NOT NEW.occurred_at
          OR OLD.metadata_json IS NOT NEW.metadata_json
          OR OLD.hash IS NOT NEW.hash
        BEGIN
          UPDATE commit_projection_input
             SET revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT OLD.session_id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT NEW.session_id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          UPDATE projection_status
             SET status = 'stale', last_error = NULL
           WHERE projection_name = 'commit_evidence_v1';
        END;

        CREATE TRIGGER IF NOT EXISTS commit_projection_events_delete
        AFTER DELETE ON events
        BEGIN
          UPDATE commit_projection_input
             SET revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE singleton = 1;
          INSERT OR REPLACE INTO commit_projection_dirty(session_id, revision)
            SELECT OLD.session_id, revision
              FROM commit_projection_input
             WHERE singleton = 1;
          UPDATE projection_status
             SET status = 'stale', last_error = NULL
           WHERE projection_name = 'commit_evidence_v1';
        END;
        ",
    )?;

    // A database opened before this projection existed has no journal rows.
    // Queueing is deliberately outside the mutation triggers: the existing
    // archive is one logical generation, not one revision per historical row.
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM projection_status WHERE projection_name = ?1)",
        [PROJECTION_NAME],
        |row| row.get::<_, bool>(0),
    )? {
        conn.execute(
            "INSERT OR IGNORE INTO commit_projection_dirty(session_id, revision)
             SELECT s.id, i.revision
               FROM sessions s
               CROSS JOIN commit_projection_input i
              WHERE i.singleton = 1",
            [],
        )?;
    }
    Ok(())
}

/// Return the committed projection state without repairing or marking it.
pub(crate) fn status(conn: &Connection) -> Result<ProjectionStatus> {
    if !table_exists(conn, "commit_evidence")?
        || !table_exists(conn, "commit_evidence_fts")?
        || !table_exists(conn, "commit_projection_input")?
        || !table_exists(conn, "commit_projection_dirty")?
        || !table_exists(conn, "projection_status")?
    {
        return Ok(missing_status());
    }

    let row = conn
        .query_row(
            "SELECT input_high_watermark, status, updated_at
               FROM projection_status
              WHERE projection_name = ?1",
            params![PROJECTION_NAME],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let evidence_count = row
        .as_ref()
        .and_then(|(watermark, _, _)| {
            watermark
                .split(':')
                .nth(1)
                .and_then(|count| count.parse::<usize>().ok())
        })
        .unwrap_or(0);
    let Some((watermark, status, updated_at)) = row else {
        return Ok(ProjectionStatus {
            state: ProjectionState::Missing,
            has_snapshot: false,
            evidence_count,
            updated_at: None,
        });
    };
    let compatible = watermark.split(':').nth(2) == Some(DETECTOR_REVISION);
    Ok(ProjectionStatus {
        state: if !compatible {
            ProjectionState::Missing
        } else if status == "ready" {
            ProjectionState::Ready
        } else {
            ProjectionState::Stale
        },
        has_snapshot: compatible,
        evidence_count,
        updated_at: (!updated_at.is_empty()).then_some(updated_at),
    })
}

/// Maintain dirty sessions without holding a main-database writer lock while
/// detection runs. The temporary staging tables are connection-local, and the
/// final transaction only swaps rows after checking the input generation.
pub(crate) fn maintain(
    store: &Store,
    mut progress: impl FnMut(Progress),
    should_cancel: impl Fn() -> bool,
) -> Result<MaintenanceOutcome> {
    progress(Progress {
        phase: "starting",
        current: 0,
        total: 0,
        evidence: 0,
    });
    store.with_conn(|conn| {
        let result = maintain_on_connection(conn, &mut progress, &should_cancel);
        if let Err(error) = &result {
            let _ = mark_stale(conn, Some(&format!("{error:#}")));
        }
        result
    })
}

fn maintain_on_connection(
    conn: &Connection,
    progress: &mut impl FnMut(Progress),
    should_cancel: &impl Fn() -> bool,
) -> Result<MaintenanceOutcome> {
    let current_revision = input_revision(conn)?;
    let existing_status = status(conn)?;
    let existing_count = existing_status.evidence_count;
    let watermark = existing_status_watermark(conn)?;
    let all_sessions = existing_status.state == ProjectionState::Missing
        || !existing_status.has_snapshot
        || (existing_status.state == ProjectionState::Ready && watermark != Some(current_revision));

    let session_ids = if all_sessions {
        session_ids(conn)?
    } else {
        dirty_session_ids(conn)?
    };
    let total_sessions = session_ids.len();
    progress(Progress {
        phase: "starting",
        current: 0,
        total: total_sessions,
        evidence: existing_count,
    });

    if total_sessions == 0 && existing_status.has_snapshot && watermark == Some(current_revision) {
        progress(Progress {
            phase: "complete",
            current: 0,
            total: 0,
            evidence: existing_count,
        });
        return Ok(MaintenanceOutcome {
            processed_sessions: 0,
            total_sessions: 0,
            evidence_count: existing_count,
        });
    }
    prepare_staging(conn, &session_ids)?;
    let mut processed_sessions = 0usize;
    let mut staged_evidence = 0usize;
    let mut insert_staging = conn.prepare(
        "INSERT OR REPLACE INTO temp.commit_evidence_staging
         (id, session_id, machine_id, source_kind, event_id, result_event_id, call_id,
          cwd, sha, sha_key, subject, message, normalized_message,
          message_event_id, message_result_event_id, occurred_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
    )?;
    let mut session_stmt = conn.prepare(
        "SELECT s.id, s.source_id, s.machine_id, s.source_kind, s.external_id, s.title, s.status,
                s.started_at, s.updated_at, s.metadata_json, s.hash
         FROM temp.commit_evidence_sessions d JOIN sessions s ON s.id = d.id ORDER BY s.id",
    )?;
    let mut session_rows = session_stmt.query_map([], session_from_row)?.peekable();
    let mut event_stmt = conn.prepare(
        "SELECT e.id, e.session_id, e.source_id, e.machine_id, e.source_kind, e.ordinal,
                e.event_type, e.role, e.content, e.raw_artifact_hash, e.occurred_at, e.metadata_json, e.hash
         FROM temp.commit_evidence_sessions d CROSS JOIN events e ON e.session_id = d.id
         JOIN sessions s ON s.id = e.session_id ORDER BY e.session_id, e.ordinal, e.id",
    )?;
    let mut event_rows = event_stmt.query_map([], event_from_row)?.peekable();

    for session_id in &session_ids {
        if should_cancel() {
            bail!("commit evidence maintenance interrupted");
        }
        let session = match session_rows.peek() {
            Some(Ok(session)) if session.id == *session_id => session_rows.next().transpose()?,
            Some(Err(_)) => session_rows.next().transpose()?,
            _ => None,
        };
        if let Some(session) = session {
            let mut events = Vec::new();
            while event_rows.peek().is_some_and(|row| {
                row.as_ref()
                    .map(|event| event.session_id == *session_id)
                    .unwrap_or(true)
            }) {
                events.push(event_rows.next().expect("peeked event")?);
            }
            for evidence in detect(&session, &events) {
                let sha_key = evidence.sha.to_ascii_lowercase();
                insert_staging.execute(params![
                    evidence.id,
                    evidence.session_id,
                    evidence.machine_id,
                    evidence.source_kind,
                    evidence.event_id,
                    evidence.result_event_id,
                    evidence.call_id,
                    evidence.cwd,
                    evidence.sha,
                    sha_key,
                    evidence.subject,
                    evidence.message,
                    evidence.normalized_message,
                    evidence.message_event_id,
                    evidence.message_result_event_id,
                    evidence.occurred_at.map(|value| value.to_rfc3339()),
                ])?;
                staged_evidence += 1;
            }
        }
        processed_sessions += 1;
        progress(Progress {
            phase: "sessions",
            current: processed_sessions,
            total: total_sessions,
            evidence: staged_evidence,
        });
    }
    drop(insert_staging);
    drop(event_rows);
    drop(session_rows);
    drop(event_stmt);
    drop(session_stmt);

    if should_cancel() {
        bail!("commit evidence maintenance interrupted before commit");
    }
    progress(Progress {
        phase: "publishing",
        current: 0,
        total: 1,
        evidence: staged_evidence,
    });
    if should_cancel() {
        bail!("commit evidence maintenance interrupted before publication");
    }
    let evidence_count = publish(conn, &session_ids, all_sessions, current_revision)?;
    progress(Progress {
        phase: "publishing",
        current: 1,
        total: 1,
        evidence: evidence_count,
    });
    progress(Progress {
        phase: "complete",
        current: total_sessions,
        total: total_sessions,
        evidence: evidence_count,
    });
    Ok(MaintenanceOutcome {
        processed_sessions,
        total_sessions,
        evidence_count,
    })
}

fn publish(
    conn: &Connection,
    session_ids: &[String],
    replace_all: bool,
    input_revision_at_start: i64,
) -> Result<usize> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
        .context("starting commit evidence publication transaction")?;
    let current_revision = input_revision_tx(&tx)?;
    if current_revision != input_revision_at_start {
        tx.rollback()
            .context("rolling back changed commit evidence generation")?;
        bail!("commit evidence input changed before publication");
    }

    if replace_all {
        tx.execute("DELETE FROM commit_evidence_fts", [])?;
        tx.execute("DELETE FROM commit_evidence", [])?;
    } else if !session_ids.is_empty() {
        tx.execute(
            "DELETE FROM commit_evidence_fts
              WHERE rowid IN (
                SELECT e.rowid
                  FROM commit_evidence e
                  JOIN temp.commit_evidence_sessions scope
                    ON scope.id = e.session_id
              )",
            [],
        )?;
        tx.execute(
            "DELETE FROM commit_evidence
              WHERE session_id IN (SELECT id FROM temp.commit_evidence_sessions)",
            [],
        )?;
    }

    tx.execute(
        "INSERT INTO commit_evidence
         SELECT id, session_id, machine_id, source_kind, event_id, result_event_id, call_id,
                cwd, sha, sha_key, subject, message, normalized_message,
                message_event_id, message_result_event_id, occurred_at
           FROM temp.commit_evidence_staging",
        [],
    )?;
    tx.execute(
        "INSERT INTO commit_evidence_fts
         (rowid, evidence_id, machine_id, cwd, subject, normalized_message)
         SELECT e.rowid, e.id, e.machine_id, e.cwd, COALESCE(e.subject, ''),
                COALESCE(e.normalized_message, '')
           FROM commit_evidence e JOIN temp.commit_evidence_staging s ON s.id = e.id",
        [],
    )?;

    let evidence_count = tx.query_row("SELECT COUNT(*) FROM commit_evidence", [], |row| {
        row.get::<_, i64>(0)
    })? as usize;
    let now = Utc::now().to_rfc3339();
    tx.execute(
        "INSERT INTO projection_status
         (projection_name, input_high_watermark, status, last_error, updated_at)
         VALUES (?1, ?2, 'ready', NULL, ?3)
         ON CONFLICT(projection_name) DO UPDATE SET
           input_high_watermark = excluded.input_high_watermark,
           status = excluded.status,
           last_error = NULL,
           updated_at = excluded.updated_at",
        params![
            PROJECTION_NAME,
            format!("{input_revision_at_start}:{evidence_count}:{DETECTOR_REVISION}"),
            now
        ],
    )?;
    tx.execute(
        "DELETE FROM commit_projection_dirty
          WHERE revision <= ?1",
        params![input_revision_at_start],
    )?;
    tx.commit()
        .context("committing commit evidence projection")?;
    Ok(evidence_count)
}

fn prepare_staging(conn: &Connection, session_ids: &[String]) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TEMP TABLE IF NOT EXISTS commit_evidence_staging (
          id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          machine_id TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          event_id TEXT NOT NULL,
          result_event_id TEXT NOT NULL,
          call_id TEXT NOT NULL,
          cwd TEXT,
          sha TEXT NOT NULL,
          sha_key TEXT NOT NULL,
          subject TEXT,
          message TEXT,
          normalized_message TEXT,
          message_event_id TEXT,
          message_result_event_id TEXT,
          occurred_at TEXT
        );
        DELETE FROM temp.commit_evidence_staging;
        CREATE TEMP TABLE IF NOT EXISTS commit_evidence_sessions (
          id TEXT PRIMARY KEY
        ) WITHOUT ROWID;
        DELETE FROM temp.commit_evidence_sessions;
        ",
    )?;
    for session_id in session_ids {
        conn.execute(
            "INSERT OR IGNORE INTO temp.commit_evidence_sessions(id) VALUES (?1)",
            params![session_id],
        )?;
    }
    Ok(())
}

pub(crate) fn candidates(
    conn: &Connection,
    machine_id: &str,
    roots: &[String],
    sha: &str,
    message: &str,
    limit: usize,
) -> Result<CandidatePage> {
    if limit == 0
        || !table_exists(conn, "commit_evidence")?
        || !table_exists(conn, "commit_evidence_fts")?
    {
        return Ok(CandidatePage {
            records: Vec::new(),
            truncated: false,
        });
    }
    let roots = roots
        .iter()
        .map(|root| {
            let root = root.trim();
            if root == "/" {
                "/"
            } else {
                root.trim_end_matches('/')
            }
        })
        .filter(|root| !root.is_empty())
        .collect::<Vec<_>>();
    if roots.is_empty() {
        return Ok(CandidatePage {
            records: Vec::new(),
            truncated: false,
        });
    }

    let scope_sql = scope_sql("e", roots.len());
    let normalized = normalize_message(message);
    let sha_prefixes = sha_prefixes(sha);
    let mut exact = Vec::new();
    let mut seen = HashSet::new();
    let mut truncated = false;
    for hash_lookup in [true, false] {
        if (hash_lookup && sha_prefixes.is_empty()) || (!hash_lookup && normalized.is_empty()) {
            continue;
        }
        let (index, predicate) = if hash_lookup {
            (
                "idx_commit_evidence_machine_sha",
                format!("e.sha_key IN ({})", placeholders(sha_prefixes.len())),
            )
        } else {
            (
                "idx_commit_evidence_machine_message",
                "e.normalized_message = ?".to_string(),
            )
        };
        let sql = format!(
            "SELECT {EVIDENCE_COLUMNS} FROM commit_evidence e INDEXED BY {index}
             WHERE e.machine_id = ? AND ({scope_sql}) AND ({predicate})
             ORDER BY e.occurred_at IS NULL, e.occurred_at DESC, e.id LIMIT ?"
        );
        let mut values = vec![SqlValue::Text(machine_id.to_string())];
        push_scope_values(&mut values, &roots);
        if hash_lookup {
            values.extend(sha_prefixes.iter().cloned().map(SqlValue::Text));
        } else {
            values.push(SqlValue::Text(normalized.clone()));
        }
        values.push(SqlValue::Integer(
            limit.saturating_add(1).min(i64::MAX as usize) as i64,
        ));
        let mut stmt = conn.prepare(&sql)?;
        for row in stmt.query_map(params_from_iter(values), evidence_from_row)? {
            let record = row?;
            if !seen.insert(record.id.clone()) {
                continue;
            }
            if exact.len() == limit {
                truncated = true;
                break;
            }
            exact.push(record);
        }
        if truncated {
            break;
        }
    }
    if exact.len() == limit {
        return Ok(CandidatePage {
            records: exact,
            truncated,
        });
    }

    let remaining = limit - exact.len();
    let fts_query = fts_query(message);
    if fts_query.is_empty() {
        return Ok(CandidatePage {
            records: exact,
            truncated,
        });
    }
    let mut fuzzy_sql = format!(
        "SELECT {EVIDENCE_COLUMNS}
           FROM commit_evidence_fts
           JOIN commit_evidence e ON e.id = commit_evidence_fts.evidence_id
          WHERE commit_evidence_fts MATCH ?
            AND e.machine_id = ?
            AND ({scope_sql})",
    );
    if !seen.is_empty() {
        fuzzy_sql.push_str(&format!(" AND e.id NOT IN ({})", placeholders(seen.len())));
    }
    fuzzy_sql.push_str(" ORDER BY bm25(commit_evidence_fts), e.id LIMIT ?");
    let mut fuzzy_values = vec![
        SqlValue::Text(fts_query),
        SqlValue::Text(machine_id.to_string()),
    ];
    push_scope_values(&mut fuzzy_values, &roots);
    for id in &seen {
        fuzzy_values.push(SqlValue::Text(id.clone()));
    }
    fuzzy_values.push(SqlValue::Integer(remaining.saturating_add(1) as i64));

    let mut fuzzy_stmt = conn.prepare(&fuzzy_sql)?;
    let fuzzy_rows = fuzzy_stmt.query_map(params_from_iter(fuzzy_values), evidence_from_row)?;
    for row in fuzzy_rows {
        let record = row?;
        if !seen.insert(record.id.clone()) {
            continue;
        }
        if exact.len() == limit {
            truncated = true;
            break;
        }
        exact.push(record);
    }
    Ok(CandidatePage {
        records: exact,
        truncated,
    })
}

const EVIDENCE_COLUMNS: &str = "e.id, e.session_id, e.machine_id, e.source_kind, e.event_id,
       e.result_event_id, e.call_id, e.cwd, e.sha, e.subject, e.message,
       e.normalized_message, e.message_event_id, e.message_result_event_id, e.occurred_at";

fn scope_sql(alias: &str, roots: usize) -> String {
    (0..roots)
        .map(|_| format!("({alias}.cwd = ? OR {alias}.cwd LIKE ? ESCAPE '\\')"))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn push_scope_values(values: &mut Vec<SqlValue>, roots: &[&str]) {
    for root in roots {
        let prefix = if *root == "/" {
            "/".to_string()
        } else {
            format!("{root}/")
        };
        values.push(SqlValue::Text((*root).to_string()));
        values.push(SqlValue::Text(format!("{}%", escape_like(&prefix))));
    }
}

fn escape_like(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn sha_prefixes(sha: &str) -> Vec<String> {
    let sha = sha.trim().to_ascii_lowercase();
    if !(MIN_SHA_LEN..=MAX_SHA_LEN).contains(&sha.len())
        || !sha.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Vec::new();
    }
    (MIN_SHA_LEN..=sha.len())
        .map(|length| sha[..length].to_string())
        .collect()
}

fn normalize_message(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn fts_query(input: &str) -> String {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-')
        .filter(|term| !term.is_empty())
        .take(32)
        .map(|term| {
            let term = term.replace('"', "\"\"");
            if term.chars().count() >= 4 {
                format!("\"{term}\"*")
            } else {
                format!("\"{term}\"")
            }
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn evidence_from_row(row: &Row<'_>) -> rusqlite::Result<CommitEvidence> {
    Ok(CommitEvidence {
        id: row.get(0)?,
        session_id: row.get(1)?,
        machine_id: row.get(2)?,
        source_kind: row.get(3)?,
        event_id: row.get(4)?,
        result_event_id: row.get(5)?,
        call_id: row.get(6)?,
        cwd: row.get(7)?,
        sha: row.get(8)?,
        subject: row.get(9)?,
        message: row.get(10)?,
        normalized_message: row.get(11)?,
        message_event_id: row.get(12)?,
        message_result_event_id: row.get(13)?,
        occurred_at: parse_datetime(row.get(14)?),
    })
}

fn session_from_row(row: &Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        id: row.get(0)?,
        source_id: row.get(1)?,
        machine_id: row.get(2)?,
        source_kind: row.get(3)?,
        external_id: row.get(4)?,
        title: row.get(5)?,
        status: row.get(6)?,
        started_at: parse_datetime(row.get(7)?),
        updated_at: parse_datetime(row.get(8)?),
        metadata: metadata_from_row(row, 9)?,
        hash: row.get(10)?,
    })
}

fn event_from_row(row: &Row<'_>) -> rusqlite::Result<EventRecord> {
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
        occurred_at: parse_datetime(row.get(10)?),
        metadata: metadata_from_row(row, 11)?,
        hash: row.get(12)?,
    })
}

fn metadata_from_row(row: &Row<'_>, column: usize) -> rusqlite::Result<Value> {
    let text: String = row.get(column)?;
    serde_json::from_str(&text).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn session_ids(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id FROM sessions ORDER BY id")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn dirty_session_ids(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT session_id
           FROM commit_projection_dirty
          ORDER BY session_id",
    )?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn input_revision(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT revision FROM commit_projection_input WHERE singleton = 1",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn input_revision_tx(tx: &Transaction<'_>) -> Result<i64> {
    tx.query_row(
        "SELECT revision FROM commit_projection_input WHERE singleton = 1",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn existing_status_watermark(conn: &Connection) -> Result<Option<i64>> {
    let value = conn
        .query_row(
            "SELECT input_high_watermark
               FROM projection_status
              WHERE projection_name = ?1",
            params![PROJECTION_NAME],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(value.and_then(|value| value.split(':').next()?.parse::<i64>().ok()))
}
fn mark_stale(conn: &Connection, error: Option<&str>) -> Result<()> {
    if !table_exists(conn, "projection_status")? {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO projection_status
         (projection_name, input_high_watermark, status, last_error, updated_at)
         VALUES (?1, '', 'stale', ?2, ?3)
         ON CONFLICT(projection_name) DO UPDATE SET
           status = 'stale',
           last_error = excluded.last_error",
        params![PROJECTION_NAME, error, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn missing_status() -> ProjectionStatus {
    ProjectionStatus {
        state: ProjectionState::Missing,
        has_snapshot: false,
        evidence_count: 0,
        updated_at: None,
    }
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM sqlite_master
            WHERE (type = 'table' OR type = 'view') AND name = ?1
         )",
        params![table],
        |row| row.get::<_, bool>(0),
    )
    .map_err(Into::into)
}

fn parse_datetime(value: Option<String>) -> Option<DateTime<Utc>> {
    value.and_then(|value| {
        DateTime::parse_from_rfc3339(&value)
            .ok()
            .map(|value| value.with_timezone(&Utc))
    })
}

fn placeholders(count: usize) -> String {
    std::iter::repeat("?")
        .take(count)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
