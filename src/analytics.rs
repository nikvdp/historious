use crate::archive::stable_id;
use crate::ingest::{self, SessionClass};
use crate::provenance;
use crate::storage::{ImportDelta, Store};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::functions::{Aggregate, Context as SqlContext, FunctionFlags};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub const MESSAGE_PROVENANCE_PROJECTION: &str = "message_provenance";
pub const MESSAGE_PROVENANCE_VERSION: u32 = 8;
pub const SESSION_RELATIONSHIPS_PROJECTION: &str = "session_relationships";
pub const SESSION_RELATIONSHIPS_VERSION: u32 = 8;
pub const SESSION_FACTS_PROJECTION: &str = "session_facts";
pub const SESSION_FACTS_VERSION: u32 = 3;
pub const REPORT_SNAPSHOT_PROJECTION: &str = "report_snapshot";
pub const REPORT_SNAPSHOT_VERSION: u32 = 8;
pub(crate) const REPORT_PROJECTION_COUNT: usize = 4;

const PROJECTIONS: [Projection; REPORT_PROJECTION_COUNT] = [
    Projection {
        name: SESSION_RELATIONSHIPS_PROJECTION,
        version: SESSION_RELATIONSHIPS_VERSION,
        table: "session_relationships",
    },
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
    Projection {
        name: REPORT_SNAPSHOT_PROJECTION,
        version: REPORT_SNAPSHOT_VERSION,
        table: "report_snapshot",
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
    #[serde(skip)]
    pub(crate) status: Option<String>,
    pub version: u32,
    pub stored_version: Option<u32>,
    pub input_rowid: i64,
    pub stored_input_rowid: Option<i64>,
    pub new_event_rows: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReportRefreshOutcome {
    pub refreshed: bool,
    pub full_rebuild: bool,
    pub affected_sessions: usize,
    pub affected_events: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ReportRefreshProgress {
    pub phase: &'static str,
    pub completed: usize,
    pub total: usize,
    pub detail: String,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportRefreshStatus {
    pub stale: bool,
    pub snapshot_missing: bool,
    pub prerequisites_invalid: bool,
}


#[derive(Debug, Clone)]
pub struct AuditCount {
    pub authored_by: String,
    pub sentiment_usable: String,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct AuditRuleCount {
    pub rule: String,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct AuditSample {
    pub source_kind: String,
    pub occurred_at: Option<String>,
    pub workspace_path: Option<String>,
    pub authored_by: String,
    pub sentiment_usable: String,
    pub rule: String,
    pub preview: String,
}

#[derive(Debug, Clone)]
pub struct ProvenanceAudit {
    pub buckets: Vec<AuditCount>,
    pub rules: Vec<AuditRuleCount>,
    pub samples: Vec<AuditSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectionState {
    version: u32,
    input_rowid: i64,
    row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RebuildProgress {
    Started {
        projection: &'static str,
        completed: usize,
        total: usize,
    },
    Detail {
        projection: &'static str,
        completed: usize,
        total: usize,
        detail: String,
    },
    Completed {
        projection: &'static str,
        completed: usize,
        total: usize,
    },
}

#[allow(dead_code)]
pub fn rebuild_all(
    store: &Store,
    mut progress: impl FnMut(&'static str, usize, usize),
) -> Result<Vec<ProjectionFreshness>> {
    rebuild_all_with_progress(store, |event| match event {
        RebuildProgress::Started {
            projection,
            completed,
            total,
        }
        | RebuildProgress::Completed {
            projection,
            completed,
            total,
        } => progress(projection, completed, total),
        RebuildProgress::Detail { .. } => {}
    })
}

pub(crate) fn rebuild_all_with_progress(
    store: &Store,
    mut progress: impl FnMut(RebuildProgress),
) -> Result<Vec<ProjectionFreshness>> {
    let total = PROJECTIONS.len();
    for (index, projection) in PROJECTIONS.iter().copied().enumerate() {
        progress(RebuildProgress::Started {
            projection: projection.name,
            completed: index,
            total,
        });
        rebuild_projection(store, projection, |detail| {
            progress(RebuildProgress::Detail {
                projection: projection.name,
                completed: index,
                total,
                detail,
            });
        })?;
        progress(RebuildProgress::Completed {
            projection: projection.name,
            completed: index + 1,
            total,
        });
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

pub fn report_snapshot_freshness(store: &Store) -> Result<ProjectionFreshness> {
    projection_freshness(store, PROJECTIONS[3])
}
pub(crate) fn report_refresh_status(store: &Store) -> Result<ReportRefreshStatus> {
    store.with_conn(|conn| {
        let input_rowid = max_event_rowid(conn)?;
        let mut stale = false;
        let mut prerequisites_invalid = false;
        for (index, projection) in PROJECTIONS.into_iter().enumerate() {
            let stored = conn
                .query_row(
                    "SELECT status, input_high_watermark
                     FROM projection_status
                     WHERE projection_name = ?1",
                    params![projection.name],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let state = stored
                .as_ref()
                .and_then(|(_, state)| serde_json::from_str::<ProjectionState>(state).ok());
            let invalid = stored.as_ref().map(|(status, _)| status.as_str()) != Some("ready")
                || state.as_ref().map(|state| state.version) != Some(projection.version);
            if index < 3 {
                prerequisites_invalid |= invalid;
            }
            stale |= invalid
                || state
                    .as_ref()
                    .is_none_or(|state| input_rowid > state.input_rowid);
        }
        let snapshot_missing = conn.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM report_snapshot WHERE singleton = 1)",
            [],
            |row| row.get(0),
        )?;
        Ok(ReportRefreshStatus {
            stale,
            snapshot_missing,
            prerequisites_invalid,
        })
    })
}


pub(crate) fn report_refresh_prior_hashes(
    store: &Store,
    delta: &ImportDelta,
    mut progress: impl FnMut(usize, usize),
) -> Result<HashSet<String>> {
    let mut event_ids = delta
        .touched_events
        .iter()
        .chain(&delta.repaired_events)
        .map(String::as_str)
        .collect::<Vec<_>>();
    event_ids.sort_unstable();
    event_ids.dedup();
    let total = event_ids.len();
    if total == 0 {
        return Ok(HashSet::new());
    }
    progress(0, total);

    store.with_conn(|conn| {
        let mut hashes = HashSet::new();
        let mut processed = 0;
        for batch in event_ids.chunks(500) {
            let placeholders = vec!["?"; batch.len()].join(", ");
            let mut stmt = conn.prepare(&format!(
                "SELECT DISTINCT text_hash FROM history_items
                 WHERE event_id IN ({placeholders})
                   AND tier = 'conversation' AND kind = 'user' AND length(text) > 200"
            ))?;
            let rows = stmt.query_map(rusqlite::params_from_iter(batch.iter().copied()), |row| {
                row.get::<_, String>(0)
            })?;
            for hash in rows {
                hashes.insert(hash?);
            }
            processed += batch.len();
            progress(processed, total);
        }
        Ok(hashes)
    })
}

#[cfg(test)]
fn refresh_report_after_update_with_progress(
    store: &Store,
    delta: &ImportDelta,
    progress: impl FnMut(ReportRefreshProgress),
) -> Result<ReportRefreshOutcome> {
    refresh_report_after_update_with_prior_hashes(store, delta, &HashSet::new(), progress)
}

pub(crate) fn refresh_report_after_update_with_prior_hashes(
    store: &Store,
    delta: &ImportDelta,
    prior_candidate_hashes: &HashSet<String>,
    progress: impl FnMut(ReportRefreshProgress),
) -> Result<ReportRefreshOutcome> {
    refresh_report_with_progress(store, delta, prior_candidate_hashes, false, progress)
}

pub(crate) fn refresh_report_on_demand_with_progress(
    store: &Store,
    refresh_snapshot: bool,
    progress: impl FnMut(ReportRefreshProgress),
) -> Result<ReportRefreshOutcome> {
    refresh_report_with_progress(
        store,
        &ImportDelta::default(),
        &HashSet::new(),
        refresh_snapshot,
        progress,
    )
}

fn refresh_report_with_progress(
    store: &Store,
    delta: &ImportDelta,
    prior_candidate_hashes: &HashSet<String>,
    refresh_snapshot: bool,
    mut progress: impl FnMut(ReportRefreshProgress),
) -> Result<ReportRefreshOutcome> {
    let statuses = freshness(store)?;
    let delta_empty = delta.inserted_sessions.is_empty()
        && delta.touched_sessions.is_empty()
        && delta.inserted_events.is_empty()
        && delta.touched_events.is_empty()
        && delta.repaired_events.is_empty();
    if !refresh_snapshot
        && statuses.iter().all(|status| !status.stale)
        && (delta_empty || !delta.repaired_events.is_empty())
    {
        return Ok(ReportRefreshOutcome {
            refreshed: false,
            full_rebuild: false,
            affected_sessions: 0,
            affected_events: 0,
        });
    }

    let captured_input_rowid = store.with_conn(max_event_rowid)?;
    let invalid_state = statuses.iter().take(3).any(|status| {
        status.status.as_deref() != Some("ready")
            || status.stored_version != Some(status.version)
            || (status.stale && status.new_event_rows == 0)
    });
    let mut event_ids = delta
        .inserted_events
        .iter()
        .chain(&delta.touched_events)
        .chain(&delta.repaired_events)
        .cloned()
        .collect::<HashSet<_>>();
    let mut unmapped_event_ids = event_ids.clone();
    let mut session_ids = delta
        .inserted_sessions
        .iter()
        .chain(&delta.touched_sessions)
        .cloned()
        .collect::<HashSet<_>>();

    if invalid_state {
        event_ids = all_event_ids(store, captured_input_rowid)?;
        session_ids = all_session_ids(store)?;
        unmapped_event_ids.clear();
    } else if statuses.iter().any(|status| status.stale) {
        let catch_up_after = statuses
            .iter()
            .filter_map(|status| status.stored_input_rowid)
            .min()
            .unwrap_or(0);
        let catch_up = events_after_watermark(store, catch_up_after, captured_input_rowid)?;
        for (event_id, session_id) in catch_up {
            unmapped_event_ids.remove(&event_id);
            event_ids.insert(event_id);
            session_ids.insert(session_id);
        }
    }
    session_ids.extend(session_ids_for_events(store, &unmapped_event_ids)?);

    let relationship_fallback = invalid_state
        || !delta.inserted_sessions.is_empty()
        || relationship_sensitive_scope(store, &session_ids, &event_ids)?;
    let mut provenance_sessions = if invalid_state || relationship_fallback {
        all_session_ids(store)?
    } else {
        widen_provenance_sessions(store, &session_ids, &event_ids, prior_candidate_hashes)?
    };
    if provenance_sessions.is_empty() {
        provenance_sessions.extend(session_ids.iter().cloned());
    }
    let affected_sessions = session_ids.union(&provenance_sessions).count();
    let relationship_row_count = statuses[0].row_count as usize;

    let relationship_work = if relationship_fallback {
        provenance_sessions.len().max(1)
    } else {
        1
    };
    let fact_work = session_ids.len().max(1);
    let provenance_work = if relationship_fallback {
        total_conversation_messages(store)?
    } else {
        provenance_message_count(store, &provenance_sessions)?
    }
    .max(1);
    let report_work = 1;
    let total = relationship_work + fact_work + provenance_work + report_work;
    let mut completed = 0usize;

    progress(ReportRefreshProgress {
        phase: "relationship",
        completed,
        total,
        detail: if relationship_fallback {
            "rebuilding relationship-sensitive inputs".to_string()
        } else {
            "advancing unchanged relationships".to_string()
        },
    });
    if relationship_fallback {
        run_projection_refresh(store, PROJECTIONS[0], captured_input_rowid, || {
            rebuild_session_relationships_with_progress(store, |processed, relationship_total| {
                completed = processed.min(relationship_work);
                progress(ReportRefreshProgress {
                    phase: "relationship",
                    completed,
                    total,
                    detail: format!(
                        "resolved {processed}/{relationship_total} session relationships"
                    ),
                });
            })
        })?;
    } else {
        run_projection_refresh(store, PROJECTIONS[0], captured_input_rowid, || {
            Ok(relationship_row_count)
        })?;
    }
    completed = relationship_work;
    progress(ReportRefreshProgress {
        phase: "relationship",
        completed,
        total,
        detail: "relationships ready".to_string(),
    });

    let facts_start = completed;
    progress(ReportRefreshProgress {
        phase: "session_facts",
        completed,
        total,
        detail: format!("refreshing {} affected sessions", session_ids.len()),
    });
    run_projection_refresh(store, PROJECTIONS[2], captured_input_rowid, || {
        if invalid_state {
            rebuild_session_facts_with_progress(store, |processed| {
                completed = facts_start + processed.min(fact_work);
                progress(ReportRefreshProgress {
                    phase: "session_facts",
                    completed,
                    total,
                    detail: format!("refreshed {processed}/{} session facts", session_ids.len()),
                });
            })
        } else {
            refresh_session_facts_scoped(store, &session_ids, |processed| {
                completed = facts_start + processed;
                progress(ReportRefreshProgress {
                    phase: "session_facts",
                    completed,
                    total,
                    detail: format!("refreshed {processed}/{} session facts", session_ids.len()),
                });
            })?;
            store.with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map(|count| count.max(0) as usize)
                .map_err(Into::into)
            })
        }
    })?;
    completed = facts_start + fact_work;
    progress(ReportRefreshProgress {
        phase: "session_facts",
        completed,
        total,
        detail: "session facts ready".to_string(),
    });

    let provenance_start = completed;
    progress(ReportRefreshProgress {
        phase: "provenance",
        completed,
        total,
        detail: format!("classifying {provenance_work} affected messages"),
    });
    run_projection_refresh(store, PROJECTIONS[1], captured_input_rowid, || {
        if relationship_fallback {
            rebuild_message_provenance(store, |detail| {
                if let Some(processed) = classified_message_progress(&detail) {
                    completed = provenance_start + processed.min(provenance_work);
                }
                progress(ReportRefreshProgress {
                    phase: "provenance",
                    completed,
                    total,
                    detail,
                });
            })
        } else {
            refresh_message_provenance_scoped(
                store,
                &provenance_sessions,
                prior_candidate_hashes,
                |processed| {
                    completed = provenance_start + processed.min(provenance_work);
                    progress(ReportRefreshProgress {
                        phase: "provenance",
                        completed,
                        total,
                        detail: format!(
                            "classified {processed}/{provenance_work} affected messages"
                        ),
                    });
                },
            )?;
            Ok(total_conversation_messages(store)?)
        }
    })?;
    completed = provenance_start + provenance_work;
    progress(ReportRefreshProgress {
        phase: "provenance",
        completed,
        total,
        detail: "provenance ready".to_string(),
    });

    let report_start = completed;
    set_projection_building(store, PROJECTIONS[3], captured_input_rowid)?;
    let report_result = crate::report::rebuild_snapshot_with_progress(store, |event| {
        let scaled = event
            .completed
            .saturating_mul(report_work)
            / event.total.max(1);
        completed = completed
            .max(report_start + scaled)
            .min(report_start + report_work);
        progress(ReportRefreshProgress {
            phase: "report_snapshot",
            completed,
            total,
            detail: event.detail,
        });
    });
    match report_result {
        Ok(()) => set_projection_ready(store, PROJECTIONS[3], captured_input_rowid, 1)?,
        Err(error) => {
            let _ = set_projection_failed(
                store,
                PROJECTIONS[3],
                captured_input_rowid,
                &error.to_string(),
            );
            return Err(error);
        }
    }
    completed = total;
    progress(ReportRefreshProgress {
        phase: "report_snapshot",
        completed,
        total,
        detail: "report snapshot ready".to_string(),
    });

    Ok(ReportRefreshOutcome {
        refreshed: true,
        full_rebuild: invalid_state || relationship_fallback,
        affected_sessions,
        affected_events: event_ids.len(),
    })
}

fn run_projection_refresh(
    store: &Store,
    projection: Projection,
    input_rowid: i64,
    action: impl FnOnce() -> Result<usize>,
) -> Result<()> {
    set_projection_building(store, projection, input_rowid)?;
    match action() {
        Ok(row_count) => set_projection_ready(store, projection, input_rowid, row_count),
        Err(error) => {
            let _ = set_projection_failed(store, projection, input_rowid, &error.to_string());
            Err(error)
        }
    }
}

fn classified_message_progress(detail: &str) -> Option<usize> {
    detail
        .strip_prefix("classifying ")
        .or_else(|| detail.strip_prefix("classified "))?
        .split_once('/')?
        .0
        .parse()
        .ok()
}

pub fn audit_provenance(
    store: &Store,
    bucket: Option<&str>,
    rule: Option<&str>,
    limit: usize,
) -> Result<ProvenanceAudit> {
    store.with_conn(|conn| {
        let buckets = {
            let mut stmt = conn.prepare(
                "SELECT authored_by, sentiment_usable, COUNT(*)
                 FROM message_provenance
                 GROUP BY authored_by, sentiment_usable
                 ORDER BY authored_by, sentiment_usable",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(AuditCount {
                    authored_by: row.get(0)?,
                    sentiment_usable: row.get(1)?,
                    count: row.get::<_, i64>(2)?.max(0) as u64,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let rules = {
            let mut stmt = conn.prepare(
                "SELECT rule, COUNT(*)
                 FROM message_provenance
                 GROUP BY rule
                 ORDER BY COUNT(*) DESC, rule",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(AuditRuleCount {
                    rule: row.get(0)?,
                    count: row.get::<_, i64>(1)?.max(0) as u64,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let samples = {
            let mut stmt = conn.prepare(
                "SELECT p.source_kind, p.occurred_at, s.metadata_json, p.authored_by,
                        p.sentiment_usable, p.rule, hi.text
                 FROM message_provenance p
                 JOIN history_items hi ON hi.id = p.item_id
                 JOIN sessions s ON s.id = p.session_id
                 WHERE (?1 IS NULL OR p.authored_by = ?1)
                   AND (?2 IS NULL OR p.rule = ?2)
                 ORDER BY random()
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![bucket, rule, limit.min(200) as i64], |row| {
                let metadata = row.get::<_, String>(2)?;
                let metadata = serde_json::from_str::<Value>(&metadata)
                    .unwrap_or_else(|_| serde_json::json!({}));
                Ok(AuditSample {
                    source_kind: row.get(0)?,
                    occurred_at: row.get(1)?,
                    workspace_path: workspace_path(&metadata),
                    authored_by: row.get(3)?,
                    sentiment_usable: row.get(4)?,
                    rule: row.get(5)?,
                    preview: collapsed_preview(&row.get::<_, String>(6)?, 200),
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(ProvenanceAudit {
            buckets,
            rules,
            samples,
        })
    })
}

fn collapsed_preview(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        collapsed
    } else {
        let mut preview = collapsed
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        preview.push('…');
        preview
    }
}

fn rebuild_projection(
    store: &Store,
    projection: Projection,
    mut progress: impl FnMut(String),
) -> Result<()> {
    let input_rowid = store.with_conn(max_event_rowid)?;
    set_projection_building(store, projection, input_rowid)?;

    let result: Result<usize> = match projection.name {
        SESSION_RELATIONSHIPS_PROJECTION => rebuild_session_relationships_with_detailed_progress(
            store,
            |processed, total, detail| {
                progress(detail.unwrap_or_else(|| {
                    format!("resolved {processed}/{total} session relationships")
                }));
            },
        ),
        MESSAGE_PROVENANCE_PROJECTION => rebuild_message_provenance(store, &mut progress),
        SESSION_FACTS_PROJECTION => {
            let total = store.with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map(|count| count.max(0) as usize)
                .map_err(Into::into)
            })?;
            progress(format!("refreshed 0/{total} session facts"));
            rebuild_session_facts_with_progress(store, |processed| {
                progress(format!("refreshed {processed}/{total} session facts"));
            })
        }
        REPORT_SNAPSHOT_PROJECTION => {
            crate::report::rebuild_snapshot_with_progress(store, |event| {
                progress(event.detail);
            })?;
            Ok(1)
        }
        _ => {
            clear_projection(store, projection)?;
            Ok(0)
        }
    };

    match result {
        Ok(row_count) => set_projection_ready(store, projection, input_rowid, row_count),
        Err(error) => {
            let _ = set_projection_failed(store, projection, input_rowid, &error.to_string());
            Err(error)
        }
    }
}

fn rebuild_session_relationships_with_progress(
    store: &Store,
    mut progress: impl FnMut(usize, usize),
) -> Result<usize> {
    rebuild_session_relationships_with_detailed_progress(store, |processed, total, detail| {
        if detail.is_none() {
            progress(processed, total);
        }
    })
}

fn rebuild_session_relationships_with_detailed_progress(
    store: &Store,
    mut progress: impl FnMut(usize, usize, Option<String>),
) -> Result<usize> {
    const SESSION_BATCH_SIZE: usize = 500;

    let sessions = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT rowid, id, machine_id, source_kind, external_id, metadata_json
             FROM sessions
             ORDER BY rowid",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RelationshipSession {
                rowid: row.get(0)?,
                session_id: row.get(1)?,
                machine_id: row.get(2)?,
                source_kind: row.get(3)?,
                external_id: row.get(4)?,
                metadata_json: row.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    let mut session_ids = HashMap::<(String, String, String), Vec<String>>::new();
    for session in &sessions {
        session_ids
            .entry((
                session.machine_id.clone(),
                session.source_kind.clone(),
                session.external_id.clone(),
            ))
            .or_default()
            .push(session.session_id.clone());
    }

    let mut codex_parents = HashMap::<(String, String), (String, bool)>::new();
    let mut codex_hashes = HashMap::<String, Vec<String>>::new();
    let mut hints = Vec::with_capacity(sessions.len());
    let mut parents = HashMap::with_capacity(sessions.len());
    let mut inline_relationships = Vec::new();
    let mut event_overrides = Vec::new();
    let total = sessions.len();
    let mut scanned = 0usize;
    progress(
        scanned,
        total,
        Some(format!("scanning {scanned}/{total} sessions for relationships")),
    );
    for session_batch in sessions.chunks(SESSION_BATCH_SIZE) {
        let events = store.with_conn(|conn| load_relationship_event_batch(conn, session_batch))?;
        scanned += session_batch.len();
        progress(
            scanned,
            total,
            Some(format!("scanning {scanned}/{total} sessions for relationships")),
        );
        for session in session_batch {
            let session_events = events
                .get(&session.session_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let metadata = serde_json::from_str::<Value>(&session.metadata_json)
                .unwrap_or_else(|_| serde_json::json!({}));
            let hint = ingest::resolve_session_relationship(
                &session.source_kind,
                &session.external_id,
                &metadata,
                &[],
            );
            let parent_session_id = hint.parent_external_id.as_ref().and_then(|external_id| {
                relationship_parent_session_id(session, external_id, &session_ids)
            });
            if session.source_kind == "codex" {
                for content in session_events
                    .iter()
                    .filter_map(|event| event.notification.as_deref())
                {
                    for child_external_id in ingest::codex_subagent_paths(content) {
                        codex_parents
                            .entry((session.machine_id.clone(), child_external_id))
                            .and_modify(|(existing_parent_id, collision)| {
                                if existing_parent_id != &session.session_id {
                                    *collision = true;
                                }
                            })
                            .or_insert_with(|| (session.session_id.clone(), false));
                    }
                }
                if hint.relationship == ingest::SessionRelationshipKind::None {
                    codex_hashes.insert(
                        session.session_id.clone(),
                        session_events
                            .iter()
                            .filter_map(|event| event.hash.clone())
                            .collect(),
                    );
                }
            }
            if session.source_kind == "claude_code"
                && hint.relationship == ingest::SessionRelationshipKind::None
            {
                let (relationships, overrides) =
                    claude_inline_relationships(&session.session_id, session_events)?;
                inline_relationships.extend(relationships);
                event_overrides.extend(overrides);
            }
            parents.insert(session.session_id.clone(), parent_session_id.clone());
            hints.push((hint, parent_session_id));
            progress(hints.len(), total, None);
        }
    }

    for (session, (hint, parent_session_id)) in sessions.iter().zip(hints.iter_mut()) {
        if session.source_kind != "codex" {
            continue;
        }
        if let Some((codex_parent_session_id, collision)) =
            codex_parents.get(&(session.machine_id.clone(), session.external_id.clone()))
        {
            *parent_session_id = Some(codex_parent_session_id.clone());
            hint.relationship = ingest::SessionRelationshipKind::Subagent;
            hint.rule = if *collision {
                "codex.subagent_notification.collision"
            } else {
                "codex.subagent_notification"
            };
            parents.insert(session.session_id.clone(), parent_session_id.clone());
        }
    }

    let fork_parents = codex_fork_parents(&sessions, &hints, &codex_hashes)?;
    for (session, (hint, parent_session_id)) in sessions.iter().zip(hints.iter_mut()) {
        if let Some(fork_parent_id) = fork_parents.get(&session.session_id) {
            *parent_session_id = Some(fork_parent_id.clone());
            hint.relationship = ingest::SessionRelationshipKind::Fork;
            hint.rule = "codex.shared_prefix";
            parents.insert(session.session_id.clone(), Some(fork_parent_id.clone()));
        }
    }
    for relationship in &inline_relationships {
        parents.insert(
            relationship.session_id.clone(),
            relationship.parent_session_id.clone(),
        );
    }

    let resolved_at = Utc::now().to_rfc3339();
    let mut rows = sessions
        .into_iter()
        .zip(hints)
        .map(|(session, (hint, parent_session_id))| {
            Ok(SessionRelationshipRow {
                root_session_id: root_session_id(&session.session_id, &parents)?,
                session_id: session.session_id,
                parent_session_id,
                relationship: hint.relationship.as_str(),
                rule: hint.rule,
                resolved_at: resolved_at.as_str(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for relationship in inline_relationships {
        rows.push(SessionRelationshipRow {
            root_session_id: root_session_id(&relationship.session_id, &parents)?,
            session_id: relationship.session_id,
            parent_session_id: relationship.parent_session_id,
            relationship: relationship.relationship.as_str(),
            rule: relationship.rule,
            resolved_at: resolved_at.as_str(),
        });
    }

    let row_count = rows.len();
    replace_session_relationships(store, &rows, &event_overrides)?;
    Ok(row_count)
}

fn relationship_parent_session_id(
    session: &RelationshipSession,
    parent_external_id: &str,
    session_ids: &HashMap<(String, String, String), Vec<String>>,
) -> Option<String> {
    let exact_key = (
        session.machine_id.clone(),
        session.source_kind.clone(),
        parent_external_id.to_string(),
    );
    if let Some(parent_id) = session_ids.get(&exact_key).and_then(|ids| {
        ids.iter()
            .find(|id| id.as_str() != session.session_id)
            .cloned()
    }) {
        return Some(parent_id);
    }
    if session.source_kind != "omp" || parent_external_id.contains('/') {
        return None;
    }

    let suffix = format!("_{parent_external_id}");
    let mut matches = session_ids
        .iter()
        .filter(|((machine_id, source_kind, external_id), _)| {
            machine_id == &session.machine_id
                && source_kind == "omp"
                && external_id.ends_with(&suffix)
        })
        .flat_map(|(_, ids)| ids)
        .filter(|id| id.as_str() != session.session_id);
    let parent_id = matches.next()?.clone();
    matches.next().is_none().then_some(parent_id)
}

fn load_relationship_event_batch(
    conn: &Connection,
    sessions: &[RelationshipSession],
) -> Result<HashMap<String, Vec<RelationshipEvent>>> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS relationship_session_scope (
           session_id TEXT PRIMARY KEY,
           source_kind TEXT NOT NULL
         ) WITHOUT ROWID;
         DELETE FROM temp.relationship_session_scope;",
    )?;
    {
        let mut insert = conn.prepare(
            "INSERT INTO temp.relationship_session_scope (session_id, source_kind)
             VALUES (?1, ?2)",
        )?;
        for session in sessions {
            insert.execute(params![session.session_id, session.source_kind])?;
        }
    }
    let mut stmt = conn.prepare(
        "SELECT e.session_id, e.id,
                CASE WHEN scope.source_kind = 'codex'
                           AND instr(e.content, 'subagent_notification') > 0
                     THEN e.content END,
                CASE WHEN scope.source_kind = 'claude_code' THEN e.metadata_json END,
                CASE WHEN scope.source_kind = 'codex' THEN e.hash END
         FROM temp.relationship_session_scope scope
         CROSS JOIN events e INDEXED BY idx_events_session_ordinal
           ON e.session_id = scope.session_id
         WHERE scope.source_kind IN ('codex', 'claude_code')
         ORDER BY e.session_id, e.ordinal",
    )?;
    let rows = stmt.query_map([], |row| {
        let inline = row
            .get::<_, Option<String>>(3)?
            .map(|metadata| {
                let metadata = serde_json::from_str::<Value>(&metadata)
                    .unwrap_or_else(|_| serde_json::json!({}));
                let relationship = metadata.get("claude_relationship");
                Ok::<_, rusqlite::Error>(InlineClaudeEvent {
                    event_id: row.get(1)?,
                    uuid: relationship
                        .and_then(|value| value.get("uuid"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    parent_uuid: relationship
                        .and_then(|value| value.get("parent_uuid"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    is_sidechain: relationship
                        .and_then(|value| value.get("is_sidechain"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    task_tool_use: relationship
                        .and_then(|value| value.get("task_tool_use"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
            })
            .transpose()?;
        Ok((
            row.get::<_, String>(0)?,
            RelationshipEvent {
                notification: row.get(2)?,
                hash: row.get(4)?,
                inline,
            },
        ))
    })?;
    let mut events = HashMap::<String, Vec<RelationshipEvent>>::new();
    for row in rows {
        let (session_id, event) = row?;
        events.entry(session_id).or_default().push(event);
    }
    Ok(events)
}

fn codex_fork_parents(
    sessions: &[RelationshipSession],
    hints: &[(ingest::SessionRelationshipHint, Option<String>)],
    hashes: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, String>> {
    const MIN_SHARED_EVENTS: usize = 8;

    let candidates = sessions
        .iter()
        .zip(hints)
        .filter(|(session, (hint, _))| {
            session.source_kind == "codex"
                && hint.relationship == ingest::SessionRelationshipKind::None
        })
        .map(|(session, _)| session)
        .collect::<Vec<_>>();
    let mut groups = HashMap::<(String, Vec<String>), Vec<&RelationshipSession>>::new();
    for session in candidates {
        let session_hashes = hashes
            .get(&session.session_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if session_hashes.len() <= MIN_SHARED_EVENTS {
            continue;
        }
        groups
            .entry((
                session.machine_id.clone(),
                session_hashes[..MIN_SHARED_EVENTS].to_vec(),
            ))
            .or_default()
            .push(session);
    }

    let mut parents = HashMap::new();
    for mut group in groups.into_values().filter(|group| group.len() > 1) {
        group.sort_by_key(|session| session.rowid);
        for (child_index, child) in group.iter().enumerate().skip(1) {
            let child_hashes = &hashes[&child.session_id];
            let mut best_parent = None;
            let mut best_length = 0;
            let mut tied = false;
            for parent in &group[..child_index] {
                if parent.external_id == child.external_id {
                    continue;
                }
                let parent_hashes = &hashes[&parent.session_id];
                let shared = parent_hashes
                    .iter()
                    .zip(child_hashes)
                    .take_while(|(left, right)| left == right)
                    .count();
                if shared < MIN_SHARED_EVENTS
                    || shared == parent_hashes.len()
                    || shared == child_hashes.len()
                {
                    continue;
                }
                if shared > best_length {
                    best_parent = Some(parent.session_id.clone());
                    best_length = shared;
                    tied = false;
                } else if shared == best_length {
                    tied = true;
                }
            }
            if !tied {
                if let Some(parent_id) = best_parent {
                    parents.insert(child.session_id.clone(), parent_id);
                }
            }
        }
    }
    Ok(parents)
}

fn claude_inline_relationships(
    parent_session_id: &str,
    relationship_events: &[RelationshipEvent],
) -> Result<(Vec<InlineRelationship>, Vec<EventSessionOverride>)> {
    let events = relationship_events
        .iter()
        .filter_map(|event| event.inline.as_ref())
        .collect::<Vec<_>>();
    if !events.iter().any(|event| event.is_sidechain) {
        return Ok((Vec::new(), Vec::new()));
    }

    let by_uuid = events
        .iter()
        .filter_map(|event| event.uuid.as_ref().map(|uuid| (uuid.as_str(), event)))
        .collect::<HashMap<_, _>>();
    let mut roots = HashMap::<String, Vec<String>>::new();
    for event in events.iter().filter(|event| event.is_sidechain) {
        let mut current = event;
        let mut visited = HashSet::new();
        while let Some(parent) = current
            .parent_uuid
            .as_deref()
            .and_then(|uuid| by_uuid.get(uuid).copied())
            .filter(|parent| parent.is_sidechain)
        {
            if !visited.insert(current.event_id.as_str()) {
                bail!("cycle in Claude inline sidechain at {}", current.event_id);
            }
            current = parent;
        }
        let root_key = current
            .uuid
            .clone()
            .unwrap_or_else(|| current.event_id.clone());
        roots.entry(root_key).or_default().push(event.event_id.clone());
    }

    let mut relationships = Vec::with_capacity(roots.len());
    let mut overrides = Vec::new();
    for (root_uuid, event_ids) in roots {
        let root = by_uuid.get(root_uuid.as_str()).copied();
        let mut ancestor_uuid = root.and_then(|event| event.parent_uuid.as_deref());
        let mut visited = HashSet::new();
        let mut linked = false;
        while let Some(uuid) = ancestor_uuid {
            if !visited.insert(uuid) {
                bail!("cycle in Claude inline parent chain at {uuid}");
            }
            let Some(ancestor) = by_uuid.get(uuid).copied() else {
                break;
            };
            if ancestor.task_tool_use && !ancestor.is_sidechain {
                linked = true;
                break;
            }
            ancestor_uuid = ancestor.parent_uuid.as_deref();
        }

        let synthetic_session_id = stable_id(&[
            "session_relationship",
            "claude_inline",
            parent_session_id,
            &root_uuid,
        ]);
        for event_id in event_ids {
            overrides.push(EventSessionOverride {
                event_id,
                session_id: synthetic_session_id.clone(),
            });
        }
        relationships.push(InlineRelationship {
            session_id: synthetic_session_id,
            parent_session_id: linked.then(|| parent_session_id.to_string()),
            relationship: if linked {
                ingest::SessionRelationshipKind::Subagent
            } else {
                ingest::SessionRelationshipKind::None
            },
            rule: if linked {
                "claude.inline_sidechain"
            } else {
                "claude.inline_orphan"
            },
        });
    }
    Ok((relationships, overrides))
}


fn root_session_id(session_id: &str, parents: &HashMap<String, Option<String>>) -> Result<String> {
    let mut current = session_id;
    let mut visited = HashSet::new();
    while let Some(parent) = parents.get(current).and_then(Option::as_deref) {
        if !visited.insert(current) {
            bail!("cycle in session relationships at {current}");
        }
        current = parent;
    }
    Ok(current.to_string())
}

fn replace_session_relationships(
    store: &Store,
    rows: &[SessionRelationshipRow<'_>],
    overrides: &[EventSessionOverride],
) -> Result<()> {
    store.with_conn(|conn| {
        conn.pragma_update(None, "temp_store", "FILE")?;
        conn.execute_batch(
            "DROP TABLE IF EXISTS temp.session_relationships_rebuild;
             DROP TABLE IF EXISTS temp.event_session_overrides_rebuild;
             CREATE TEMP TABLE session_relationships_rebuild (
               session_id TEXT PRIMARY KEY,
               parent_session_id TEXT,
               root_session_id TEXT NOT NULL,
               relationship TEXT NOT NULL,
               rule TEXT NOT NULL,
               resolved_at TEXT NOT NULL
             ) WITHOUT ROWID;
             CREATE TEMP TABLE event_session_overrides_rebuild (
               event_id TEXT PRIMARY KEY,
               session_id TEXT NOT NULL
             ) WITHOUT ROWID;",
        )?;
        {
            let mut insert = conn.prepare(
                "INSERT INTO temp.session_relationships_rebuild
                 (session_id, parent_session_id, root_session_id, relationship, rule, resolved_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for row in rows {
                insert.execute(params![
                    row.session_id,
                    row.parent_session_id,
                    row.root_session_id,
                    row.relationship,
                    row.rule,
                    row.resolved_at,
                ])?;
            }
        }
        {
            let mut insert = conn.prepare(
                "INSERT INTO temp.event_session_overrides_rebuild (event_id, session_id)
                 VALUES (?1, ?2)",
            )?;
            for row in overrides {
                insert.execute(params![row.event_id, row.session_id])?;
            }
        }

        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting relationship replacement")?;
        tx.execute("DELETE FROM session_relationships", [])?;
        tx.execute("DELETE FROM event_session_overrides", [])?;
        tx.execute(
            "INSERT INTO session_relationships
             (session_id, parent_session_id, root_session_id, relationship, rule, resolved_at)
             SELECT session_id, parent_session_id, root_session_id, relationship, rule, resolved_at
             FROM temp.session_relationships_rebuild",
            [],
        )?;
        tx.execute(
            "INSERT INTO event_session_overrides (event_id, session_id)
             SELECT event_id, session_id FROM temp.event_session_overrides_rebuild",
            [],
        )?;
        tx.commit().context("committing relationship replacement")
    })
}

struct RelationshipSession {
    rowid: i64,
    session_id: String,
    machine_id: String,
    source_kind: String,
    external_id: String,
    metadata_json: String,
}

struct SessionRelationshipRow<'a> {
    session_id: String,
    parent_session_id: Option<String>,
    root_session_id: String,
    relationship: &'a str,
    rule: &'a str,
    resolved_at: &'a str,
}

struct RelationshipEvent {
    notification: Option<String>,
    hash: Option<String>,
    inline: Option<InlineClaudeEvent>,
}

struct InlineClaudeEvent {
    event_id: String,
    uuid: Option<String>,
    parent_uuid: Option<String>,
    is_sidechain: bool,
    task_tool_use: bool,
}

struct InlineRelationship {
    session_id: String,
    parent_session_id: Option<String>,
    relationship: ingest::SessionRelationshipKind,
    rule: &'static str,
}

struct EventSessionOverride {
    event_id: String,
    session_id: String,
}

fn all_event_ids(store: &Store, through_rowid: i64) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare("SELECT id FROM events WHERE rowid <= ?1")?;
        let rows = stmt.query_map([through_rowid], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<HashSet<_>>>()
            .map_err(Into::into)
    })
}

fn all_session_ids(store: &Store) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare("SELECT id FROM sessions")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<HashSet<_>>>()
            .map_err(Into::into)
    })
}

fn events_after_watermark(
    store: &Store,
    after_rowid: i64,
    through_rowid: i64,
) -> Result<Vec<(String, String)>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id, session_id FROM events
             WHERE rowid > ?1 AND rowid <= ?2
             ORDER BY rowid",
        )?;
        let rows = stmt.query_map(params![after_rowid, through_rowid], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn session_ids_for_events(
    store: &Store,
    event_ids: &HashSet<String>,
) -> Result<HashSet<String>> {
    if event_ids.is_empty() {
        return Ok(HashSet::new());
    }
    let mut event_ids = event_ids.iter().map(String::as_str).collect::<Vec<_>>();
    event_ids.sort_unstable();
    store.with_conn(|conn| {
        let mut session_ids = HashSet::new();
        for batch in event_ids.chunks(500) {
            let placeholders = vec!["?"; batch.len()].join(", ");
            let mut stmt = conn.prepare(&format!(
                "SELECT DISTINCT session_id FROM events WHERE id IN ({placeholders})"
            ))?;
            let rows = stmt.query_map(rusqlite::params_from_iter(batch.iter().copied()), |row| {
                row.get::<_, String>(0)
            })?;
            for session_id in rows {
                session_ids.insert(session_id?);
            }
        }
        Ok(session_ids)
    })
}

fn relationship_sensitive_scope(
    store: &Store,
    session_ids: &HashSet<String>,
    event_ids: &HashSet<String>,
) -> Result<bool> {
    store.with_conn(|conn| {
        let mut session_stmt = conn.prepare(
            "SELECT s.source_kind, s.external_id, s.metadata_json,
                    sr.relationship, sr.rule, parent.external_id
             FROM sessions s
             LEFT JOIN session_relationships sr ON sr.session_id = s.id
             LEFT JOIN sessions parent ON parent.id = sr.parent_session_id
             WHERE s.id = ?1",
        )?;
        for session_id in session_ids {
            let stored = session_stmt
                .query_row([session_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })
                .optional()?;
            let Some((source_kind, external_id, metadata_json, relationship, rule, parent)) =
                stored
            else {
                continue;
            };
            if matches!(source_kind.as_str(), "codex" | "claude_code") {
                return Ok(true);
            }
            if matches!(source_kind.as_str(), "opencode" | "omp") {
                let metadata = serde_json::from_str::<Value>(&metadata_json)
                    .unwrap_or_else(|_| serde_json::json!({}));
                let hint = ingest::resolve_session_relationship(
                    &source_kind,
                    &external_id,
                    &metadata,
                    &[],
                );
                let parent_matches = match hint.parent_external_id.as_deref() {
                    None => parent.is_none(),
                    Some(expected) => parent.as_deref().is_some_and(|actual| {
                        actual == expected || (source_kind == "omp" && actual.ends_with(expected))
                    }),
                };
                if relationship.as_deref() != Some(hint.relationship.as_str())
                    || rule.as_deref() != Some(hint.rule)
                    || !parent_matches
                {
                    return Ok(true);
                }
            }
        }
        let mut event_stmt = conn.prepare("SELECT source_kind FROM events WHERE id = ?1")?;
        for event_id in event_ids {
            let source_kind = event_stmt
                .query_row([event_id], |row| row.get::<_, String>(0))
                .optional()?;
            if source_kind
                .as_deref()
                .is_some_and(|kind| matches!(kind, "codex" | "claude_code"))
            {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

fn widen_provenance_sessions(
    store: &Store,
    touched: &HashSet<String>,
    event_ids: &HashSet<String>,
    prior_candidate_hashes: &HashSet<String>,
) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut affected = touched.clone();
        let edges = {
            let mut stmt = conn.prepare(
                "SELECT session_id, parent_session_id
                 FROM session_relationships
                 WHERE parent_session_id IS NOT NULL",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        loop {
            let before = affected.len();
            for (child, parent) in &edges {
                if affected.contains(child) || affected.contains(parent) {
                    affected.insert(child.clone());
                    affected.insert(parent.clone());
                }
            }
            if affected.len() == before {
                break;
            }
        }

        let mut touched_candidate_hashes = prior_candidate_hashes.clone();
        let mut event_ids = event_ids.iter().map(String::as_str).collect::<Vec<_>>();
        event_ids.sort_unstable();
        for batch in event_ids.chunks(500) {
            let placeholders = vec!["?"; batch.len()].join(", ");
            let mut stmt = conn.prepare(&format!(
                "SELECT DISTINCT text_hash FROM history_items INDEXED BY idx_history_items_event
                 WHERE event_id IN ({placeholders})
                   AND tier = 'conversation' AND kind = 'user' AND length(text) > 200"
            ))?;
            let rows = stmt.query_map(rusqlite::params_from_iter(batch.iter().copied()), |row| {
                row.get::<_, String>(0)
            })?;
            for hash in rows {
                touched_candidate_hashes.insert(hash?);
            }
        }

        let mut sessions_stmt = conn.prepare(
            "SELECT DISTINCT session_id FROM history_items WHERE text_hash = ?1",
        )?;
        for hash in touched_candidate_hashes {
            let sessions = sessions_stmt.query_map([hash], |row| row.get::<_, String>(0))?;
            for session_id in sessions {
                affected.insert(session_id?);
            }
        }
        Ok(affected)
    })
}

fn provenance_message_count(store: &Store, session_ids: &HashSet<String>) -> Result<usize> {
    if session_ids.is_empty() {
        return Ok(0);
    }
    let mut session_ids = session_ids.iter().map(String::as_str).collect::<Vec<_>>();
    session_ids.sort_unstable();
    store.with_conn(|conn| {
        let mut total = 0usize;
        for batch in session_ids.chunks(500) {
            let placeholders = vec!["?"; batch.len()].join(", ");
            let count = conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM history_items INDEXED BY idx_history_items_session_order
                     WHERE session_id IN ({placeholders})
                       AND tier = 'conversation' AND kind IN ('user', 'assistant')"
                ),
                rusqlite::params_from_iter(batch.iter().copied()),
                |row| row.get::<_, i64>(0),
            )?;
            total = total.saturating_add(count.max(0) as usize);
        }
        Ok(total)
    })
}

fn total_conversation_messages(store: &Store) -> Result<usize> {
    store.with_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM history_items
             WHERE tier = 'conversation' AND kind IN ('user', 'assistant')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as usize)
        .map_err(Into::into)
    })
}



const SESSION_FACT_BATCH_SIZE: usize = 100;

fn rebuild_session_facts_with_progress(
    store: &Store,
    mut progress: impl FnMut(usize),
) -> Result<usize> {

    store.with_conn(|conn| {
        conn.pragma_update(None, "temp_store", "FILE")?;
        conn.execute_batch(
            "DROP TABLE IF EXISTS temp.session_facts_rebuild;
             CREATE TEMP TABLE session_facts_rebuild (
               session_id TEXT PRIMARY KEY,
               source_kind TEXT NOT NULL,
               workspace_path TEXT,
               session_class TEXT NOT NULL,
               models_json TEXT NOT NULL,
               primary_model TEXT,
               input_tokens INTEGER,
               cached_input_tokens INTEGER,
               output_tokens INTEGER,
               event_count INTEGER NOT NULL,
               user_message_count INTEGER NOT NULL,
               first_event_at TEXT,
               last_event_at TEXT,
               duration_secs INTEGER
             );",
        )?;

        let mut last_rowid = 0i64;
        let mut processed = 0usize;
        let mut last_progress = Instant::now();
        loop {
            let session_ids = {
                let mut stmt = conn.prepare(
                    "SELECT rowid, id
                     FROM sessions
                     WHERE rowid > ?1
                     ORDER BY rowid
                     LIMIT ?2",
                )?;
                let rows = stmt.query_map(
                    params![last_rowid, SESSION_FACT_BATCH_SIZE as i64],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            if session_ids.is_empty() {
                break;
            }
            last_rowid = session_ids
                .last()
                .expect("non-empty session facts batch")
                .0;
            let session_ids = session_ids
                .into_iter()
                .map(|(_, session_id)| session_id)
                .collect::<Vec<_>>();
            let (inputs, events) = load_session_fact_batch(conn, &session_ids)?;
            let rows = project_session_fact_batch(
                inputs,
                events,
                processed,
                &mut progress,
                &mut last_progress,
            )?;
            insert_session_fact_rows(
                conn,
                "INSERT INTO temp.session_facts_rebuild
                 (session_id, source_kind, workspace_path, session_class, models_json,
                  primary_model, input_tokens, cached_input_tokens, output_tokens, event_count,
                  user_message_count, first_event_at, last_event_at, duration_secs)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                &rows,
            )?;
            processed += rows.len();
            progress(processed);
            last_progress = Instant::now();
        }

        let staged = conn
            .query_row("SELECT COUNT(*) FROM temp.session_facts_rebuild", [], |row| {
                row.get::<_, i64>(0)
            })
            .context("counting staged session facts")?;
        let staged = usize::try_from(staged).context("invalid staged session fact count")?;
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting session facts replacement")?;
        tx.execute("DELETE FROM session_facts", [])?;
        tx.execute(
            "INSERT INTO session_facts
             (session_id, source_kind, workspace_path, session_class, models_json,
              primary_model, input_tokens, cached_input_tokens, output_tokens, event_count,
              user_message_count, first_event_at, last_event_at, duration_secs)
             SELECT session_id, source_kind, workspace_path, session_class, models_json,
                    primary_model, input_tokens, cached_input_tokens, output_tokens, event_count,
                    user_message_count, first_event_at, last_event_at, duration_secs
             FROM temp.session_facts_rebuild",
            [],
        )?;
        tx.commit().context("committing session facts replacement")?;
        Ok(staged)
    })
}

fn refresh_session_facts_scoped(
    store: &Store,
    session_ids: &HashSet<String>,
    mut progress: impl FnMut(usize),
) -> Result<()> {
    let mut session_ids = session_ids.iter().collect::<Vec<_>>();
    session_ids.sort_unstable();
    store.with_conn(|conn| {
        let mut processed = 0usize;
        let mut last_progress = Instant::now();
        for batch in session_ids.chunks(SESSION_FACT_BATCH_SIZE) {
            let (inputs, events) = load_session_fact_batch(conn, batch)?;
            let rows = project_session_fact_batch(
                inputs,
                events,
                processed,
                &mut progress,
                &mut last_progress,
            )?;
            insert_session_facts_batch(conn, &rows)?;
            processed += rows.len();
            progress(processed);
            last_progress = Instant::now();
        }
        Ok(())
    })
}

fn load_session_fact_batch<S: AsRef<str>>(
    conn: &Connection,
    session_ids: &[S],
) -> Result<(Vec<SessionFactInput>, SessionEventFacts)> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS session_fact_scope (
           session_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM temp.session_fact_scope;",
    )?;
    {
        let mut insert = conn.prepare(
            "INSERT INTO temp.session_fact_scope (session_id) VALUES (?1)",
        )?;
        for session_id in session_ids {
            insert.execute([session_id.as_ref()])?;
        }
    }

    let inputs = {
        let mut stmt = conn.prepare(
            "WITH user_message_counts AS (
               SELECT hi.session_id, COUNT(*) AS user_message_count
               FROM temp.session_fact_scope scope
               CROSS JOIN history_items hi INDEXED BY idx_history_items_session_order
                 ON hi.session_id = scope.session_id
               WHERE hi.tier = 'conversation' AND hi.kind = 'user'
               GROUP BY hi.session_id
             )
             SELECT s.id, s.source_kind, s.metadata_json, sa.event_count,
                    COALESCE(sa.first_event_at, s.started_at),
                    COALESCE(sa.last_event_at, s.updated_at),
                    COALESCE(users.user_message_count, 0)
             FROM temp.session_fact_scope scope
             CROSS JOIN sessions s ON s.id = scope.session_id
             LEFT JOIN session_activity sa ON sa.session_id = s.id
             LEFT JOIN user_message_counts users ON users.session_id = s.id
             ORDER BY s.rowid",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SessionFactInput {
                session_id: row.get(0)?,
                source_kind: row.get(1)?,
                metadata_json: row.get(2)?,
                event_count: row.get::<_, Option<i64>>(3)?.map(|count| count.max(0)),
                first_event_at: row.get(4)?,
                last_event_at: row.get(5)?,
                user_message_count: row.get::<_, i64>(6)?.max(0),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let facts = session_usage_events(conn)?;
    Ok((inputs, facts))
}

fn session_usage_events(conn: &Connection) -> Result<SessionEventFacts> {
    conn.create_aggregate_function::<CodexSessionFactAccumulator, _, String>(
        "codex_session_fact",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        CodexSessionFactAggregate,
    )?;

    let mut facts = SessionEventFacts::new();
    {
        let mut stmt = conn.prepare(
            "SELECT e.session_id, codex_session_fact(e.content, e.ordinal)
             FROM temp.session_fact_scope scope
             CROSS JOIN sessions s ON s.id = scope.session_id AND s.source_kind = 'codex'
             CROSS JOIN events e INDEXED BY idx_events_session_ordinal
               ON e.session_id = scope.session_id
             GROUP BY e.session_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (session_id, serialized) = row?;
            let compact = serde_json::from_str::<CodexSessionFactOutput>(&serialized)?;
            facts.insert(
                session_id,
                SessionEventFact {
                    event_row_count: compact.event_row_count,
                    classified_from_events: Some(match compact.session_class.as_str() {
                        "interactive" => SessionClass::Interactive,
                        "subagent" => SessionClass::Subagent,
                        "automation" => SessionClass::Automation,
                        _ => SessionClass::Unknown,
                    }),
                    usage: ingest::SessionUsage {
                        models: compact.models,
                        primary_model: compact.primary_model,
                        input_tokens: compact.input_tokens,
                        cached_input_tokens: compact.cached_input_tokens,
                        output_tokens: compact.output_tokens,
                    },
                    legacy_events: Vec::new(),
                },
            );
        }
    }

    let mut stmt = conn.prepare(
        "SELECT e.session_id, e.content, e.metadata_json
         FROM temp.session_fact_scope scope
         CROSS JOIN sessions s ON s.id = scope.session_id AND s.source_kind <> 'codex'
         CROSS JOIN events e INDEXED BY idx_events_session_ordinal
           ON e.session_id = scope.session_id
         ORDER BY e.session_id, e.ordinal",
    )?;
    let rows = stmt.query_map([], |row| {
        let metadata = row.get::<_, String>(2)?;
        Ok((
            row.get::<_, String>(0)?,
            ingest::UsageEvent {
                content: row.get(1)?,
                metadata: serde_json::from_str(&metadata)
                    .unwrap_or_else(|_| serde_json::json!({})),
            },
        ))
    })?;
    for row in rows {
        let (session_id, event) = row?;
        let fact = facts.entry(session_id).or_default();
        fact.event_row_count += 1;
        fact.legacy_events.push(event);
    }
    Ok(facts)
}

fn project_session_fact_batch(
    inputs: Vec<SessionFactInput>,
    mut facts: SessionEventFacts,
    processed: usize,
    progress: &mut impl FnMut(usize),
    last_progress: &mut Instant,
) -> Result<Vec<SessionFactRow>> {
    let mut rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        let metadata = serde_json::from_str::<Value>(&input.metadata_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let fact = facts.remove(&input.session_id).unwrap_or_default();
        let (session_class, usage) = if input.source_kind == "codex" {
            (
                fact.classified_from_events
                    .unwrap_or(SessionClass::Interactive),
                fact.usage,
            )
        } else {
            let event_contents = fact
                .legacy_events
                .iter()
                .map(|event| event.content.as_str())
                .collect::<Vec<_>>();
            (
                ingest::classify_session(&input.source_kind, &metadata, &event_contents),
                ingest::extract_session_usage(&input.source_kind, &fact.legacy_events),
            )
        };
        rows.push(SessionFactRow {
            session_id: input.session_id,
            source_kind: input.source_kind,
            workspace_path: workspace_path(&metadata),
            session_class: session_class.as_str(),
            models_json: serde_json::to_string(&usage.models)?,
            primary_model: usage.primary_model,
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            output_tokens: usage.output_tokens,
            event_count: input.event_count.unwrap_or(fact.event_row_count),
            user_message_count: input.user_message_count,
            duration_secs: duration_secs(
                input.first_event_at.as_deref(),
                input.last_event_at.as_deref(),
            ),
            first_event_at: input.first_event_at,
            last_event_at: input.last_event_at,
        });
        if last_progress.elapsed() >= Duration::from_secs(1) {
            progress(processed + rows.len());
            *last_progress = Instant::now();
        }
    }
    Ok(rows)
}

fn workspace_path(metadata: &Value) -> Option<String> {
    metadata
        .get("workspace_path")
        .and_then(Value::as_str)
        .or_else(|| metadata.pointer("/workspace/path").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn duration_secs(first: Option<&str>, last: Option<&str>) -> Option<i64> {
    let first = DateTime::parse_from_rfc3339(first?).ok()?;
    let last = DateTime::parse_from_rfc3339(last?).ok()?;
    Some((last - first).num_seconds().max(0))
}

fn insert_session_facts_batch(conn: &Connection, rows: &[SessionFactRow]) -> Result<()> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
        .context("starting session facts batch")?;
    insert_session_fact_rows(
        &tx,
        "INSERT INTO session_facts
         (session_id, source_kind, workspace_path, session_class, models_json,
          primary_model, input_tokens, cached_input_tokens, output_tokens, event_count,
          user_message_count, first_event_at, last_event_at, duration_secs)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(session_id) DO UPDATE SET
           source_kind = excluded.source_kind,
           workspace_path = excluded.workspace_path,
           session_class = excluded.session_class,
           models_json = excluded.models_json,
           primary_model = excluded.primary_model,
           input_tokens = excluded.input_tokens,
           cached_input_tokens = excluded.cached_input_tokens,
           output_tokens = excluded.output_tokens,
           event_count = excluded.event_count,
           user_message_count = excluded.user_message_count,
           first_event_at = excluded.first_event_at,
           last_event_at = excluded.last_event_at,
           duration_secs = excluded.duration_secs",
        rows,
    )?;
    tx.commit().context("committing session facts batch")
}

fn insert_session_fact_rows(
    conn: &Connection,
    sql: &str,
    rows: &[SessionFactRow],
) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    for row in rows {
        stmt.execute(params![
            row.session_id,
            row.source_kind,
            row.workspace_path,
            row.session_class,
            row.models_json,
            row.primary_model,
            row.input_tokens,
            row.cached_input_tokens,
            row.output_tokens,
            row.event_count,
            row.user_message_count,
            row.first_event_at,
            row.last_event_at,
            row.duration_secs,
        ])?;
    }
    Ok(())
}

#[derive(Default)]
struct CodexSessionFactAccumulator {
    event_row_count: i64,
    first_class: Option<(i64, SessionClass)>,
    model_counts: HashMap<String, u64>,
    usage_seen: bool,
    input_tokens: i64,
    cached_input_tokens: i64,
    output_tokens: i64,
}

struct CodexSessionFactAggregate;

impl Aggregate<CodexSessionFactAccumulator, String> for CodexSessionFactAggregate {
    fn init(&self, _context: &mut SqlContext<'_>) -> rusqlite::Result<CodexSessionFactAccumulator> {
        Ok(CodexSessionFactAccumulator::default())
    }

    fn step(
        &self,
        context: &mut SqlContext<'_>,
        accumulator: &mut CodexSessionFactAccumulator,
    ) -> rusqlite::Result<()> {
        let content = context.get::<String>(0)?;
        let ordinal = context.get::<i64>(1)?;
        let fact = ingest::codex_event_fact(&content);
        accumulator.event_row_count += 1;
        if let Some(class) = fact.class_signal {
            if accumulator
                .first_class
                .is_none_or(|(first_ordinal, _)| ordinal < first_ordinal)
            {
                accumulator.first_class = Some((ordinal, class));
            }
        }
        if let Some(model) = fact.model {
            *accumulator.model_counts.entry(model).or_default() += 1;
        }
        if fact.usage_seen {
            accumulator.usage_seen = true;
            accumulator.input_tokens = accumulator.input_tokens.max(fact.input_tokens);
            accumulator.cached_input_tokens = accumulator
                .cached_input_tokens
                .max(fact.cached_input_tokens);
            accumulator.output_tokens = accumulator.output_tokens.max(fact.output_tokens);
        }
        Ok(())
    }

    fn finalize(
        &self,
        _context: &mut SqlContext<'_>,
        accumulator: Option<CodexSessionFactAccumulator>,
    ) -> rusqlite::Result<String> {
        let accumulator = accumulator.unwrap_or_default();
        let usage = ingest::session_usage_from_aggregates(
            accumulator.model_counts,
            accumulator.usage_seen,
            accumulator.input_tokens,
            accumulator.cached_input_tokens,
            accumulator.output_tokens,
        );
        serde_json::to_string(&CodexSessionFactOutput {
            event_row_count: accumulator.event_row_count,
            session_class: accumulator
                .first_class
                .map(|(_, class)| class)
                .unwrap_or(SessionClass::Interactive)
                .as_str()
                .to_owned(),
            models: usage.models,
            primary_model: usage.primary_model,
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            output_tokens: usage.output_tokens,
        })
        .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
    }
}

#[derive(Deserialize, Serialize)]
struct CodexSessionFactOutput {
    event_row_count: i64,
    session_class: String,
    models: Vec<String>,
    primary_model: Option<String>,
    input_tokens: Option<i64>,
    cached_input_tokens: Option<i64>,
    output_tokens: Option<i64>,
}

#[derive(Default)]
struct SessionEventFact {
    event_row_count: i64,
    classified_from_events: Option<SessionClass>,
    usage: ingest::SessionUsage,
    legacy_events: Vec<ingest::UsageEvent>,
}

type SessionEventFacts = HashMap<String, SessionEventFact>;

struct SessionFactInput {
    session_id: String,
    source_kind: String,
    metadata_json: String,
    event_count: Option<i64>,
    user_message_count: i64,
    first_event_at: Option<String>,
    last_event_at: Option<String>,
}

struct SessionFactRow {
    session_id: String,
    source_kind: String,
    workspace_path: Option<String>,
    session_class: &'static str,
    models_json: String,
    primary_model: Option<String>,
    input_tokens: Option<i64>,
    cached_input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    event_count: i64,
    user_message_count: i64,
    first_event_at: Option<String>,
    last_event_at: Option<String>,
    duration_secs: Option<i64>,
}

fn clear_projection(store: &Store, projection: Projection) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting analytics projection batch")?;
        tx.execute(&format!("DELETE FROM {}", projection.table), [])?;
        tx.commit().context("committing analytics projection batch")
    })
}

fn repeated_template_hashes_scoped(
    store: &Store,
    session_ids: &HashSet<String>,
    prior_candidate_hashes: &HashSet<String>,
) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut candidate_stmt = conn.prepare(
            "SELECT DISTINCT text_hash FROM history_items
             WHERE session_id = ?1 AND tier = 'conversation' AND kind = 'user'
               AND length(text) > 200",
        )?;
        let mut candidates = prior_candidate_hashes.clone();
        for session_id in session_ids {
            let hashes = candidate_stmt.query_map([session_id], |row| row.get::<_, String>(0))?;
            for hash in hashes {
                candidates.insert(hash?);
            }
        }
        let mut threshold_stmt = conn.prepare(
            "SELECT COUNT(DISTINCT hi.session_id),
                    COUNT(DISTINCT COALESCE(
                      json_extract(s.metadata_json, '$.workspace_path'),
                      json_extract(s.metadata_json, '$.path')
                    ))
             FROM history_items hi
             JOIN sessions s ON s.id = hi.session_id
             WHERE hi.text_hash = ?1 AND hi.tier = 'conversation'
               AND hi.kind = 'user' AND length(hi.text) > 200",
        )?;
        let mut repeated = HashSet::new();
        for hash in candidates {
            let (sessions, workspaces) = threshold_stmt.query_row([hash.as_str()], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?;
            if sessions > 3 && workspaces > 3 {
                repeated.insert(hash);
            }
        }
        Ok(repeated)
    })
}

fn inherited_parent_items_scoped(
    store: &Store,
    session_ids: &HashSet<String>,
) -> Result<HashSet<(String, String, String)>> {
    store.with_conn(|conn| {
        let mut edge_stmt = conn.prepare(
            "SELECT session_id, parent_session_id
             FROM session_relationships
             WHERE relationship = 'subagent' AND parent_session_id IS NOT NULL
               AND (session_id = ?1 OR parent_session_id = ?1)",
        )?;
        let mut edges = HashSet::new();
        for session_id in session_ids {
            let rows = edge_stmt.query_map([session_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for edge in rows {
                edges.insert(edge?);
            }
        }
        let mut parent_stmt = conn.prepare(
            "SELECT kind, text_hash FROM history_items
             WHERE session_id = ?1 AND tier = 'conversation'
               AND kind IN ('user', 'assistant')",
        )?;
        let mut by_parent = HashMap::<String, Vec<(String, String)>>::new();
        for parent_id in edges.iter().map(|(_, parent)| parent).collect::<HashSet<_>>() {
            let rows = parent_stmt.query_map([parent_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            by_parent.insert(
                parent_id.to_string(),
                rows.collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        let mut inherited = HashSet::new();
        for (child_id, parent_id) in edges {
            if let Some(items) = by_parent.get(&parent_id) {
                for (kind, text_hash) in items {
                    inherited.insert((child_id.clone(), kind.clone(), text_hash.clone()));
                }
            }
        }
        Ok(inherited)
    })
}

fn refresh_message_provenance_scoped(
    store: &Store,
    session_ids: &HashSet<String>,
    prior_candidate_hashes: &HashSet<String>,
    mut progress: impl FnMut(usize),
) -> Result<()> {
    let repeated_templates =
        repeated_template_hashes_scoped(store, session_ids, prior_candidate_hashes)?;
    let inherited_parent_items = inherited_parent_items_scoped(store, session_ids)?;
    let mut session_classes = HashMap::new();
    let mut sorted_session_ids = session_ids.iter().cloned().collect::<Vec<_>>();
    sorted_session_ids.sort_unstable();

    let mut processed = 0usize;
    let mut projected = Vec::new();
    for session_id in &sorted_session_ids {
        let inputs = store.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT hi.rowid, hi.id, hi.session_id, hi.source_kind, hi.kind, hi.text,
                        hi.text_hash, hi.occurred_at, s.metadata_json,
                        COALESCE(sr.relationship, 'none'), eso.event_id IS NOT NULL
                 FROM history_items hi
                 JOIN sessions s ON s.id = hi.session_id
                 LEFT JOIN event_session_overrides eso ON eso.event_id = hi.event_id
                 LEFT JOIN session_relationships sr
                   ON sr.session_id = COALESCE(eso.session_id, hi.session_id)
                 WHERE hi.session_id = ?1
                   AND hi.tier = 'conversation'
                   AND hi.kind IN ('user', 'assistant')
                 ORDER BY hi.rowid",
            )?;
            let rows = stmt.query_map([session_id.as_str()], |row| {
                Ok(ProvenanceInput {
                    rowid: row.get(0)?,
                    item_id: row.get(1)?,
                    session_id: row.get(2)?,
                    source_kind: row.get(3)?,
                    message_kind: row.get(4)?,
                    text: row.get(5)?,
                    text_hash: row.get(6)?,
                    occurred_at: row.get(7)?,
                    session_metadata: row.get(8)?,
                    relationship: row.get(9)?,
                    event_session_overridden: row.get(10)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })?;
        for input in inputs {
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
            let relationship = if !input.event_session_overridden
                && input.relationship == "subagent"
                && inherited_parent_items.contains(&(
                    input.session_id.clone(),
                    input.message_kind.clone(),
                    input.text_hash.clone(),
                ))
            {
                "none"
            } else {
                input.relationship.as_str()
            };
            let classification = provenance::classify_message(
                &input.text,
                &input.message_kind,
                repeated_templates.contains(&input.text_hash),
                relationship,
                session_class,
            );
            projected.push(ProvenanceRow {
                item_id: input.item_id,
                session_id: input.session_id,
                source_kind: input.source_kind,
                authored_by: classification.authored_by,
                sentiment_usable: classification.sentiment_usable,
                rule: classification.rule,
                occurred_at: input.occurred_at,
            });
            processed += 1;
            if processed % 500 == 0 {
                progress(processed);
            }
        }
    }
    if processed % 500 != 0 {
        progress(processed);
    }

    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting scoped provenance replacement")?;
        {
            let mut delete =
                tx.prepare("DELETE FROM message_provenance WHERE session_id = ?1")?;
            for session_id in &sorted_session_ids {
                delete.execute([session_id.as_str()])?;
            }
        }
        {
            let mut insert = tx.prepare(
                "INSERT INTO message_provenance
                 (item_id, session_id, source_kind, authored_by, sentiment_usable, rule, occurred_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(item_id) DO UPDATE SET
                   session_id = excluded.session_id,
                   source_kind = excluded.source_kind,
                   authored_by = excluded.authored_by,
                   sentiment_usable = excluded.sentiment_usable,
                   rule = excluded.rule,
                   occurred_at = excluded.occurred_at",
            )?;
            for row in &projected {
                insert.execute(params![
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
        tx.commit().context("committing scoped provenance replacement")
    })
}

fn rebuild_message_provenance(
    store: &Store,
    mut progress: impl FnMut(String),
) -> Result<usize> {
    const BATCH_SIZE: i64 = 500;

    progress("finding repeated message templates".to_string());
    let repeated_templates = repeated_template_hashes(store)?;
    let total_messages = store.with_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM history_items
             WHERE tier = 'conversation'
               AND kind IN ('user', 'assistant')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as usize)
        .map_err(Into::into)
    })?;
    store.with_conn(|conn| {
        progress("preparing provenance staging table".to_string());
        conn.pragma_update(None, "temp_store", "FILE")?;
        progress("loading inherited parent messages".to_string());
        inherited_parent_items(conn, |processed, total| {
            progress(format!(
                "loaded {processed}/{total} inherited parent messages"
            ));
        })?;
        conn.execute_batch(
            "DROP TABLE IF EXISTS temp.message_provenance_rebuild;
             CREATE TEMP TABLE message_provenance_rebuild (
               item_id TEXT NOT NULL,
               session_id TEXT NOT NULL,
               source_kind TEXT NOT NULL,
               authored_by TEXT NOT NULL,
               sentiment_usable TEXT NOT NULL,
               rule TEXT NOT NULL,
               occurred_at TEXT
             );",
        )?;
        let mut insert_stmt = conn.prepare(
            "INSERT INTO temp.message_provenance_rebuild
             (item_id, session_id, source_kind, authored_by, sentiment_usable, rule, occurred_at)
             SELECT json_extract(value, '$.item_id'),
                    json_extract(value, '$.session_id'),
                    json_extract(value, '$.source_kind'),
                    json_extract(value, '$.authored_by'),
                    json_extract(value, '$.sentiment_usable'),
                    json_extract(value, '$.rule'),
                    json_extract(value, '$.occurred_at')
             FROM json_each(?1)",
        )?;
        let mut session_classes = HashMap::new();
        let mut user_rowid = 0i64;
        let mut assistant_rowid = 0i64;
        let mut users_done = false;
        let mut assistants_done = false;
        let mut processed = 0usize;
        progress(format!("classifying {processed}/{total_messages} messages"));
        let mut last_progress = Instant::now();
        let mut select_stmt = conn.prepare(
            "SELECT hi.rowid, hi.id, hi.session_id, hi.source_kind, hi.kind, hi.text,
                    hi.text_hash, hi.occurred_at, s.metadata_json,
                    CASE WHEN eso.event_id IS NULL AND sr.relationship = 'subagent'
                              AND EXISTS (
                                SELECT 1 FROM temp.inherited_parent_items inherited
                                WHERE inherited.parent_session_id = sr.parent_session_id
                                  AND inherited.kind = hi.kind
                                  AND inherited.text_hash = hi.text_hash
                              )
                         THEN 'none' ELSE COALESCE(sr.relationship, 'none') END,
                    eso.event_id IS NOT NULL
             FROM history_items hi INDEXED BY idx_history_items_tier_kind
             JOIN sessions s ON s.id = hi.session_id
             LEFT JOIN event_session_overrides eso ON eso.event_id = hi.event_id
             LEFT JOIN session_relationships sr
               ON sr.session_id = COALESCE(eso.session_id, hi.session_id)
             WHERE hi.tier = 'conversation' AND hi.kind = ?1 AND hi.rowid > ?2
             ORDER BY hi.rowid
             LIMIT ?3",
        )?;
        loop {
            let mut batch = Vec::with_capacity((BATCH_SIZE * 2) as usize);
            if !users_done {
                let rows = select_stmt.query_map(params!["user", user_rowid, BATCH_SIZE], |row| {
                    Ok(ProvenanceInput {
                        rowid: row.get(0)?,
                        item_id: row.get(1)?,
                        session_id: row.get(2)?,
                        source_kind: row.get(3)?,
                        message_kind: row.get(4)?,
                        text: row.get(5)?,
                        text_hash: row.get(6)?,
                        occurred_at: row.get(7)?,
                        session_metadata: row.get(8)?,
                        relationship: row.get(9)?,
                        event_session_overridden: row.get(10)?,
                    })
                })?;
                let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                users_done = rows.is_empty();
                if let Some(last) = rows.last() {
                    user_rowid = last.rowid;
                }
                batch.extend(rows);
            }
            if !assistants_done {
                let rows = select_stmt.query_map(
                    params!["assistant", assistant_rowid, BATCH_SIZE],
                    |row| {
                        Ok(ProvenanceInput {
                            rowid: row.get(0)?,
                            item_id: row.get(1)?,
                            session_id: row.get(2)?,
                            source_kind: row.get(3)?,
                            message_kind: row.get(4)?,
                            text: row.get(5)?,
                            text_hash: row.get(6)?,
                            occurred_at: row.get(7)?,
                            session_metadata: row.get(8)?,
                            relationship: row.get(9)?,
                            event_session_overridden: row.get(10)?,
                        })
                    },
                )?;
                let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                assistants_done = rows.is_empty();
                if let Some(last) = rows.last() {
                    assistant_rowid = last.rowid;
                }
                batch.extend(rows);
            }
            if batch.is_empty() {
                break;
            }

            let batch_len = batch.len();
            let mut rows = Vec::with_capacity(batch.len());
            for input in batch {
                let session_class = if let Some(class) = session_classes.get(&input.session_id) {
                    *class
                } else {
                    let metadata = serde_json::from_str::<Value>(&input.session_metadata)
                        .unwrap_or_else(|_| serde_json::json!({}));
                    let class = classify_stored_session_with_conn(
                        conn,
                        &input.session_id,
                        &input.source_kind,
                        &metadata,
                    )?;
                    session_classes.insert(input.session_id.clone(), class);
                    class
                };
                let relationship = input.relationship.as_str();
                let classification = provenance::classify_message(
                    &input.text,
                    &input.message_kind,
                    repeated_templates.contains(&input.text_hash),
                    relationship,
                    session_class,
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
                if last_progress.elapsed() >= Duration::from_secs(1) {
                    progress(format!(
                        "classifying {}/{} messages",
                        processed + rows.len(),
                        total_messages
                    ));
                    last_progress = Instant::now();
                }
            }
            progress(format!(
                "classified {}/{} messages",
                processed + batch_len,
                total_messages
            ));
            insert_stmt.execute([serde_json::to_string(&rows)?])?;
            processed += batch_len;
            progress(format!("staged {processed}/{total_messages} messages"));
            last_progress = Instant::now();
        }
        drop(select_stmt);
        drop(insert_stmt);
        progress(format!("indexing {total_messages} classified messages"));
        conn.execute(
            "CREATE INDEX temp.idx_message_provenance_rebuild_item_id
             ON message_provenance_rebuild(item_id)",
            [],
        )?;
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting message provenance replacement")?;
        tx.execute("DELETE FROM message_provenance", [])?;
        const STORE_BATCH: usize = 5_000;
        let mut stored = 0usize;
        let mut last_item_id: Option<String> = None;
        progress(format!("storing {stored}/{total_messages} classified messages"));
        loop {
            let item_ids = {
                let mut stmt = tx.prepare(
                    "SELECT item_id
                     FROM temp.message_provenance_rebuild
                     WHERE (?1 IS NULL OR item_id > ?1)
                     ORDER BY item_id
                     LIMIT ?2",
                )?;
                let rows = stmt.query_map(
                    params![last_item_id.as_deref(), STORE_BATCH as i64],
                    |row| row.get::<_, String>(0),
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            if item_ids.is_empty() {
                break;
            }
            let batch_end = item_ids.last().expect("non-empty provenance store batch");
            tx.execute(
                "INSERT INTO message_provenance
                 (item_id, session_id, source_kind, authored_by, sentiment_usable, rule, occurred_at)
                 SELECT item_id, session_id, source_kind, authored_by, sentiment_usable, rule,
                        occurred_at
                 FROM temp.message_provenance_rebuild
                 WHERE (?1 IS NULL OR item_id > ?1) AND item_id <= ?2
                 ORDER BY item_id",
                params![last_item_id.as_deref(), batch_end],
            )?;
            stored += item_ids.len();
            last_item_id = Some(batch_end.clone());
            progress(format!("storing {stored}/{total_messages} classified messages"));
        }
        progress("committing message provenance".to_string());
        tx.commit().context("committing message provenance rebuild")?;
        Ok(processed)
    })
}

fn inherited_parent_items(
    conn: &Connection,
    mut progress: impl FnMut(usize, usize),
) -> Result<()> {
    const MESSAGE_BATCH_SIZE: i64 = 500;

    conn.execute_batch(
        "DROP TABLE IF EXISTS temp.inherited_parent_items;
         DROP TABLE IF EXISTS temp.inherited_parent_scope;
         DROP TABLE IF EXISTS temp.inherited_parent_message_batch;
         CREATE TEMP TABLE inherited_parent_items (
           parent_session_id TEXT NOT NULL,
           kind TEXT NOT NULL,
           text_hash TEXT NOT NULL,
           PRIMARY KEY (parent_session_id, kind, text_hash)
         ) WITHOUT ROWID;
         CREATE TEMP TABLE inherited_parent_scope (
           parent_session_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         CREATE TEMP TABLE inherited_parent_message_batch (
           parent_session_id TEXT NOT NULL,
           ordinal INTEGER NOT NULL,
           subordinal INTEGER NOT NULL,
           history_rowid INTEGER NOT NULL,
           kind TEXT NOT NULL,
           text_hash TEXT NOT NULL
         );
         INSERT INTO temp.inherited_parent_scope (parent_session_id)
         SELECT DISTINCT parent_session_id
         FROM session_relationships
         WHERE relationship = 'subagent' AND parent_session_id IS NOT NULL;",
    )?;
    let total = conn
        .query_row(
            "SELECT COUNT(*)
             FROM temp.inherited_parent_scope scope
             CROSS JOIN history_items hi INDEXED BY idx_history_items_session_order
               ON hi.session_id = scope.parent_session_id
             WHERE hi.tier = 'conversation' AND hi.kind IN ('user', 'assistant')",
            [],
            |row| row.get::<_, i64>(0),
        )?
        .max(0) as usize;
    let Some(mut last_parent) = conn.query_row(
        "SELECT MIN(parent_session_id) FROM temp.inherited_parent_scope",
        [],
        |row| row.get::<_, Option<String>>(0),
    )? else {
        progress(0, total);
        return Ok(());
    };
    let mut last_ordinal = i64::MIN;
    let mut last_subordinal = i64::MIN;
    let mut last_rowid = i64::MIN;
    let mut processed = 0usize;
    progress(processed, total);
    loop {
        conn.execute("DELETE FROM temp.inherited_parent_message_batch", [])?;
        let loaded = conn.execute(
            "INSERT INTO temp.inherited_parent_message_batch
             (parent_session_id, ordinal, subordinal, history_rowid, kind, text_hash)
             SELECT hi.session_id, hi.ordinal, hi.subordinal, hi.rowid, hi.kind, hi.text_hash
             FROM temp.inherited_parent_scope scope
             CROSS JOIN history_items hi INDEXED BY idx_history_items_session_order
               ON hi.session_id = scope.parent_session_id
             WHERE hi.tier = 'conversation' AND hi.kind IN ('user', 'assistant')
               AND scope.parent_session_id >= ?1
               AND (hi.session_id, hi.ordinal, hi.subordinal, hi.rowid)
                     > (?1, ?2, ?3, ?4)
             ORDER BY hi.session_id, hi.ordinal, hi.subordinal, hi.rowid
             LIMIT ?5",
            params![
                last_parent.as_str(),
                last_ordinal,
                last_subordinal,
                last_rowid,
                MESSAGE_BATCH_SIZE,
            ],
        )?;
        if loaded == 0 {
            break;
        }
        conn.execute(
            "INSERT OR IGNORE INTO temp.inherited_parent_items
             (parent_session_id, kind, text_hash)
             SELECT parent_session_id, kind, text_hash
             FROM temp.inherited_parent_message_batch",
            [],
        )?;
        (last_parent, last_ordinal, last_subordinal, last_rowid) = conn.query_row(
            "SELECT parent_session_id, ordinal, subordinal, history_rowid
             FROM temp.inherited_parent_message_batch
             ORDER BY parent_session_id DESC, ordinal DESC, subordinal DESC, history_rowid DESC
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        processed += loaded;
        progress(processed, total);
    }
    Ok(())
}

fn repeated_template_hashes(store: &Store) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT hi.text_hash
             FROM history_items hi
             JOIN sessions s ON s.id = hi.session_id
             WHERE hi.tier = 'conversation'
               AND hi.kind = 'user'
               AND length(hi.text) > 200
             GROUP BY hi.text_hash
             HAVING COUNT(DISTINCT hi.session_id) > 3
                AND COUNT(DISTINCT COALESCE(
                      json_extract(s.metadata_json, '$.workspace_path'),
                      json_extract(s.metadata_json, '$.path')
                    )) > 3",
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
    store.with_conn(|conn| {
        classify_stored_session_with_conn(conn, session_id, source_kind, metadata)
    })
}

fn classify_stored_session_with_conn(
    conn: &Connection,
    session_id: &str,
    source_kind: &str,
    metadata: &Value,
) -> Result<SessionClass> {
    let contents = match source_kind {
        "codex" => session_event_contents_with_conn(conn, session_id, Some("session_meta"))?,
        "claude_code" => session_event_contents_with_conn(conn, session_id, None)?,
        _ => Vec::new(),
    };
    let contents = contents.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(ingest::classify_session(source_kind, metadata, &contents))
}


fn session_event_contents_with_conn(
    conn: &Connection,
    session_id: &str,
    event_type: Option<&str>,
) -> Result<Vec<String>> {
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
}


struct ProvenanceInput {
    rowid: i64,
    item_id: String,
    session_id: String,
    source_kind: String,
    message_kind: String,
    text: String,
    text_hash: String,
    occurred_at: Option<String>,
    session_metadata: String,
    relationship: String,
    event_session_overridden: bool,
}

#[derive(Serialize)]
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
            status: status.map(ToOwned::to_owned),
            version: projection.version,
            stored_version: state.as_ref().map(|state| state.version),
            input_rowid,
            stored_input_rowid,
            new_event_rows: stored_input_rowid
                .map(|stored| input_rowid.saturating_sub(stored) as u64)
                .unwrap_or(input_rowid.max(0) as u64),
            row_count: state.as_ref().map_or(0, |state| state.row_count),
        })
    })
}

fn max_event_rowid(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT COALESCE(MAX(rowid), 0) FROM events", [], |row| {
        row.get(0)
    })
    .context("reading analytics input high-water mark")
}


fn set_projection_building(store: &Store, projection: Projection, input_rowid: i64) -> Result<()> {
    set_projection_status(store, projection, input_rowid, "building", None, 0)
}

fn set_projection_ready(
    store: &Store,
    projection: Projection,
    input_rowid: i64,
    row_count: usize,
) -> Result<()> {
    set_projection_status(
        store,
        projection,
        input_rowid,
        "ready",
        None,
        row_count as u64,
    )
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
    fn audit_preview_collapses_lines_and_caps_characters() {
        assert_eq!(collapsed_preview("one\n  two\tthree", 50), "one two three");
        assert_eq!(collapsed_preview("abcdef", 5), "abcd…");
    }

    #[test]
    fn session_relationships_rebuilds_provider_defaults_idempotently() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       metadata_json, hash)
                    VALUES
                      ('session_codex', 'source_codex', 'machine', 'codex', 'codex', 'open',
                       '{}', 'session_codex_hash'),
                      ('session_pi', 'source_pi', 'machine', 'pi_agent', 'pi', 'open',
                       '{}', 'session_pi_hash'),
                      ('session_hermes', 'source_hermes', 'machine', 'hermes', 'hermes', 'open',
                       '{}', 'session_hermes_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert session relationship fixtures");

        for _ in 0..2 {
            rebuild_all(&store, |_, _, _| {}).expect("rebuild relationships");
        }
        let rows = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT session_id, parent_session_id, root_session_id, relationship, rule
                     FROM session_relationships
                     ORDER BY session_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
            .expect("load session relationships");

        assert_eq!(rows.len(), 3);
        for (session_id, parent_session_id, root_session_id, relationship, _) in &rows {
            assert_eq!(parent_session_id, &None);
            assert_eq!(root_session_id, session_id);
            assert_eq!(relationship, "none");
        }
        assert_eq!(rows[0].4, "default.none");
        assert_eq!(rows[1].4, "hermes.capture_gap");
        assert_eq!(rows[2].4, "pi_agent.capture_gap");
    }

    #[test]
    fn relationship_roots_walk_parent_chains_and_reject_cycles() {
        let parents = HashMap::from([
            ("root".to_string(), None),
            ("child".to_string(), Some("root".to_string())),
            ("grandchild".to_string(), Some("child".to_string())),
        ]);
        assert_eq!(
            root_session_id("grandchild", &parents).expect("resolve root"),
            "root"
        );

        let cycle = HashMap::from([
            ("one".to_string(), Some("two".to_string())),
            ("two".to_string(), Some("one".to_string())),
        ]);
        assert!(root_session_id("one", &cycle).is_err());
    }

    #[test]
    fn codex_notifications_link_children_and_surface_parent_collisions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       metadata_json, hash)
                    VALUES
                      ('parent_one', 'source_parent_one', 'machine', 'codex', 'parent-one',
                       'open', '{}', 'parent_one_hash'),
                      ('parent_two', 'source_parent_two', 'machine', 'codex', 'parent-two',
                       'open', '{}', 'parent_two_hash'),
                      ('child_single', 'source_child_single', 'machine', 'codex', 'child-single',
                       'open', '{}', 'child_single_hash'),
                      ('child_collision', 'source_child_collision', 'machine', 'codex',
                       'child-collision', 'open', '{}', 'child_collision_hash');

                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, metadata_json, hash)
                    VALUES
                      ('notify_single', 'parent_one', 'source_parent_one', 'machine', 'codex', 0,
                       'message', 'user',
                       '<subagent_notification>{"agent_path":"child-single"}</subagent_notification>',
                       '{}', 'notify_single_hash'),
                      ('notify_collision_first', 'parent_one', 'source_parent_one', 'machine',
                       'codex', 1, 'message', 'user',
                       '<subagent_notification>{"agent_path":"child-collision"}</subagent_notification>',
                       '{}', 'notify_collision_first_hash'),
                      ('notify_collision_second', 'parent_two', 'source_parent_two', 'machine',
                       'codex', 0, 'message', 'user',
                       '<subagent_notification>{"agent_path":"child-collision"}</subagent_notification>',
                       '{}', 'notify_collision_second_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert Codex relationship fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild Codex relationships");
        let rows = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT session_id, parent_session_id, root_session_id, relationship, rule
                     FROM session_relationships
                     WHERE session_id LIKE 'child_%'
                     ORDER BY session_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
            .expect("load Codex relationships");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "child_collision");
        assert_eq!(
            (rows[0].1.as_str(), rows[0].2.as_str(), rows[0].3.as_str()),
            ("parent_one", "parent_one", "subagent")
        );
        assert_eq!(rows[0].4, "codex.subagent_notification.collision");
        assert_eq!(rows[1].0, "child_single");
        assert_eq!(
            (rows[1].1.as_str(), rows[1].2.as_str(), rows[1].3.as_str()),
            ("parent_one", "parent_one", "subagent")
        );
        assert_eq!(rows[1].4, "codex.subagent_notification");
    }

    #[test]
    fn codex_shared_prefix_links_forks_without_overriding_subagents() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                for (session_id, external_id) in [
                    ("fork_parent", "parent-external"),
                    ("fork_child", "fork-external"),
                    ("fork_subagent", "subagent-external"),
                ] {
                    conn.execute(
                        "INSERT INTO sessions
                         (id, source_id, machine_id, source_kind, external_id, status,
                          metadata_json, hash)
                         VALUES (?1, ?2, 'machine', 'codex', ?3, 'open', '{}', ?4)",
                        params![
                            session_id,
                            format!("source_{session_id}"),
                            external_id,
                            format!("{session_id}_hash")
                        ],
                    )?;
                    for ordinal in 0..8 {
                        conn.execute(
                            "INSERT INTO events
                             (id, session_id, source_id, machine_id, source_kind, ordinal,
                              event_type, role, content, metadata_json, hash)
                             VALUES (?1, ?2, ?3, 'machine', 'codex', ?4, 'message', 'user',
                                     'shared', '{}', ?5)",
                            params![
                                format!("{session_id}_event_{ordinal}"),
                                session_id,
                                format!("source_{session_id}"),
                                ordinal,
                                format!("shared_hash_{ordinal}")
                            ],
                        )?;
                    }
                    conn.execute(
                        "INSERT INTO events
                         (id, session_id, source_id, machine_id, source_kind, ordinal,
                          event_type, role, content, metadata_json, hash)
                         VALUES (?1, ?2, ?3, 'machine', 'codex', 8, 'message', 'assistant',
                                 'diverged', '{}', ?4)",
                        params![
                            format!("{session_id}_tail"),
                            session_id,
                            format!("source_{session_id}"),
                            format!("{session_id}_tail_hash")
                        ],
                    )?;
                }
                conn.execute(
                    "INSERT INTO events
                     (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                      role, content, metadata_json, hash)
                     VALUES ('fork_notification', 'fork_parent', 'source_fork_parent', 'machine',
                             'codex', 9, 'message', 'user', ?1, '{}', 'fork_notification_hash')",
                    ["<subagent_notification>{\"agent_path\":\"subagent-external\"}</subagent_notification>"],
                )?;
                Ok(())
            })
            .expect("insert Codex fork fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild Codex fork relationships");
        let rows = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT session_id, parent_session_id, relationship, rule
                     FROM session_relationships
                     WHERE session_id IN ('fork_child', 'fork_subagent')
                     ORDER BY session_id",
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
            .expect("load Codex fork relationships");

        assert_eq!(rows[0].0, "fork_child");
        assert_eq!(rows[0].1, "fork_parent");
        assert_eq!(rows[0].2, "fork");
        assert_eq!(rows[0].3, "codex.shared_prefix");
        assert_eq!(rows[1].0, "fork_subagent");
        assert_eq!(rows[1].1, "fork_parent");
        assert_eq!(rows[1].2, "subagent");
        assert_eq!(rows[1].3, "codex.subagent_notification");
    }

    #[test]
    fn opencode_parent_metadata_resolves_to_the_native_parent_session() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       metadata_json, hash)
                    VALUES
                      ('opencode_parent', 'source', 'machine', 'opencode', 'ses_parent', 'open',
                       '{"opencode_parent_id":null}', 'opencode_parent_hash'),
                      ('opencode_child', 'source', 'machine', 'opencode', 'ses_child', 'open',
                       '{"opencode_parent_id":"ses_parent"}', 'opencode_child_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert OpenCode relationship fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild OpenCode relationships");
        let relationship = store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT parent_session_id, root_session_id, relationship, rule
                     FROM session_relationships
                     WHERE session_id = 'opencode_child'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(Into::into)
            })
            .expect("load OpenCode relationship");

        assert_eq!(relationship.0, "opencode_parent");
        assert_eq!(relationship.1, "opencode_parent");
        assert_eq!(relationship.2, "subagent");
        assert_eq!(relationship.3, "opencode.parent_id");
    }

    #[test]
    fn omp_relationships_resolve_qualified_subagents_and_bare_forks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       metadata_json, hash)
                    VALUES
                      ('omp_parent', 'source_parent', 'machine', 'omp',
                       '2026-07-13T00-00-00-000Z_parent-id', 'open',
                       '{"omp_parent_external_id":null,"omp_relationship":"none"}',
                       'omp_parent_hash'),
                      ('omp_subagent', 'source_subagent', 'machine', 'omp',
                       '2026-07-13T00-00-00-000Z_parent-id/Reviewer', 'open',
                       '{"omp_parent_external_id":"2026-07-13T00-00-00-000Z_parent-id","omp_relationship":"subagent"}',
                       'omp_subagent_hash'),
                      ('omp_fork', 'source_fork', 'machine', 'omp',
                       '2026-07-13T00-01-00-000Z_fork-id', 'open',
                       '{"omp_parent_external_id":"parent-id","omp_relationship":"fork"}',
                       'omp_fork_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert OMP relationship fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild OMP relationships");
        let relationships = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT session_id, parent_session_id, root_session_id, relationship, rule
                     FROM session_relationships
                     WHERE session_id IN ('omp_subagent', 'omp_fork')
                     ORDER BY session_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
            .expect("load OMP relationships");

        assert_eq!(relationships.len(), 2);
        for relationship in &relationships {
            assert_eq!(relationship.1, "omp_parent");
            assert_eq!(relationship.2, "omp_parent");
        }
        assert_eq!(relationships[0].0, "omp_fork");
        assert_eq!(relationships[0].3, "fork");
        assert_eq!(relationships[0].4, "omp.parent_session");
        assert_eq!(relationships[1].0, "omp_subagent");
        assert_eq!(relationships[1].3, "subagent");
        assert_eq!(relationships[1].4, "omp.artifact_path");
    }

    #[test]
    fn claude_subagent_path_resolves_to_the_parent_directory_session() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       metadata_json, hash)
                    VALUES
                      ('claude_parent', 'source_parent', 'machine', 'claude_code',
                       '123e4567-e89b-12d3-a456-426614174000', 'open',
                       '{"path":"/logs/parent.jsonl"}', 'claude_parent_hash'),
                      ('claude_child', 'source_child', 'machine', 'claude_code',
                       '123e4567-e89b-12d3-a456-426614174000', 'open',
                       '{"path":"/logs/123e4567-e89b-12d3-a456-426614174000/subagents/agent-child.jsonl"}',
                       'claude_child_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert Claude path relationship fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild Claude relationships");
        let relationship = store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT parent_session_id, root_session_id, relationship, rule
                     FROM session_relationships
                     WHERE session_id = 'claude_child'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(Into::into)
            })
            .expect("load Claude path relationship");

        assert_eq!(relationship.0, "claude_parent");
        assert_eq!(relationship.1, "claude_parent");
        assert_eq!(relationship.2, "subagent");
        assert_eq!(relationship.3, "claude.subagent_path");
    }

    #[test]
    fn claude_inline_sidechains_project_synthetic_children_and_orphans() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       metadata_json, hash)
                    VALUES
                      ('claude_inline_parent', 'source', 'machine', 'claude_code', 'parent',
                       'open', '{"path":"/logs/parent.jsonl"}', 'claude_inline_parent_hash');

                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, metadata_json, hash)
                    VALUES
                      ('task_event', 'claude_inline_parent', 'source', 'machine', 'claude_code', 0,
                       'assistant', 'assistant', 'task',
                       '{"claude_relationship":{"uuid":"task","parent_uuid":null,"is_sidechain":false,"task_tool_use":true}}',
                       'task_event_hash'),
                      ('sidechain_root', 'claude_inline_parent', 'source', 'machine', 'claude_code', 1,
                       'user', 'user', 'root',
                       '{"claude_relationship":{"uuid":"root","parent_uuid":"task","is_sidechain":true,"task_tool_use":false}}',
                       'sidechain_root_hash'),
                      ('sidechain_child', 'claude_inline_parent', 'source', 'machine', 'claude_code', 2,
                       'assistant', 'assistant', 'child',
                       '{"claude_relationship":{"uuid":"child","parent_uuid":"root","is_sidechain":true,"task_tool_use":false}}',
                       'sidechain_child_hash'),
                      ('sidechain_orphan', 'claude_inline_parent', 'source', 'machine', 'claude_code', 3,
                       'user', 'user', 'orphan',
                       '{"claude_relationship":{"uuid":"orphan","parent_uuid":"missing","is_sidechain":true,"task_tool_use":false}}',
                       'sidechain_orphan_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert Claude inline fixtures");

        for _ in 0..2 {
            rebuild_all(&store, |_, _, _| {}).expect("rebuild Claude inline relationships");
        }
        let relationships = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT parent_session_id, root_session_id, relationship, rule
                     FROM session_relationships
                     WHERE rule LIKE 'claude.inline_%'
                     ORDER BY rule",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
            .expect("load Claude inline relationships");
        let overrides = store
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM event_session_overrides", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(Into::into)
            })
            .expect("count Claude event overrides");

        assert_eq!(relationships.len(), 2);
        assert_eq!(relationships[0].0, None);
        assert!(relationships[0].1.starts_with("sc_"));
        assert_eq!(relationships[0].2, "none");
        assert_eq!(relationships[0].3, "claude.inline_orphan");
        assert_eq!(
            relationships[1],
            (
                Some("claude_inline_parent".to_string()),
                "claude_inline_parent".to_string(),
                "subagent".to_string(),
                "claude.inline_sidechain".to_string()
            )
        );
        assert_eq!(overrides, 3);
    }

    #[test]
    fn duplicate_templates_must_repeat_across_workspaces_not_only_forks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                for index in 0..4 {
                    for (prefix, text_hash, workspace) in [
                        ("wide", "hash_wide", format!("/repo/{index}")),
                        ("fork", "hash_fork", "/repo/shared".to_string()),
                    ] {
                        let session_id = format!("{prefix}_session_{index}");
                        conn.execute(
                            "INSERT INTO sessions
                             (id, source_id, machine_id, source_kind, external_id, status,
                              metadata_json, hash)
                             VALUES (?1, 'source', 'machine', 'codex', ?1, 'open', ?2, ?3)",
                            params![
                                session_id,
                                serde_json::json!({"workspace_path": workspace}).to_string(),
                                format!("{prefix}_session_hash_{index}")
                            ],
                        )?;
                        conn.execute(
                            "INSERT INTO history_items
                             (id, event_id, session_id, source_id, machine_id, source_kind,
                              ordinal, subordinal, tier, kind, text, text_hash,
                              lexical_indexable, semantic_policy, metadata_json, hash)
                             VALUES (?1, ?2, ?3, 'source', 'machine', 'codex', 0, 0,
                                     'conversation', 'user', ?4, ?5, 1, 'required', '{}', ?6)",
                            params![
                                format!("{prefix}_item_{index}"),
                                format!("{prefix}_event_{index}"),
                                session_id,
                                "template text ".repeat(20),
                                text_hash,
                                format!("{prefix}_item_hash_{index}")
                            ],
                        )?;
                    }
                }
                Ok(())
            })
            .expect("insert duplicate template fixtures");

        let hashes = repeated_template_hashes(&store).expect("find repeated templates");
        assert!(hashes.contains("hash_wide"));
        assert!(!hashes.contains("hash_fork"));
    }

    #[test]
    fn session_facts_rebuild_projects_usage_activity_and_workspace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       started_at, updated_at, metadata_json, hash)
                    VALUES
                      ('session_usage', 'source', 'machine', 'opencode', 'usage', 'open',
                       '2026-07-12T00:00:00Z', '2026-07-12T00:00:10Z',
                       '{"workspace_path":"/repo/project","opencode_parent_id":null}',
                       'session_usage_hash');

                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, occurred_at, metadata_json, hash)
                    VALUES
                      ('usage_event_1', 'session_usage', 'source', 'machine', 'opencode', 0, 'text',
                       'assistant', 'answer one', '2026-07-12T00:00:00Z',
                       '{"opencode_message_id":"msg_1","opencode_model_id":"kimi-k2","opencode_tokens":{"input":100,"output":20,"cache":{"read":30}}}',
                       'usage_event_hash_1'),
                      ('usage_event_2', 'session_usage', 'source', 'machine', 'opencode', 1, 'text',
                       'assistant', 'answer two', '2026-07-12T00:00:10Z',
                       '{"opencode_message_id":"msg_2","opencode_model_id":"kimi-k2","opencode_tokens":{"input":50,"output":10,"cache":{"read":5}}}',
                       'usage_event_hash_2');

                    INSERT OR REPLACE INTO session_activity
                      (session_id, event_count, first_event_at, last_event_at)
                    VALUES
                      ('session_usage', 2, '2026-07-12T00:00:00Z', '2026-07-12T00:00:10Z');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, lexical_indexable, semantic_policy,
                       metadata_json, hash)
                    VALUES
                      ('usage_user_item', 'usage_event_1', 'session_usage', 'source', 'machine',
                       'opencode', 0, 0, 'conversation', 'user', 'question', 'usage_user_hash',
                       1, 'required', '{}', 'usage_user_item_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert session facts fixtures");

        rebuild_all(&store, |_, _, _| {}).expect("rebuild session facts");
        let facts = store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT workspace_path, session_class, models_json, primary_model,
                            input_tokens, cached_input_tokens, output_tokens, event_count,
                            user_message_count, duration_secs
                     FROM session_facts
                     WHERE session_id = 'session_usage'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, i64>(7)?,
                            row.get::<_, i64>(8)?,
                            row.get::<_, i64>(9)?,
                        ))
                    },
                )
                .map_err(Into::into)
            })
            .expect("load session facts");

        assert_eq!(facts.0, "/repo/project");
        assert_eq!(facts.1, "interactive");
        assert_eq!(facts.2, "[\"kimi-k2\"]");
        assert_eq!(facts.3, "kimi-k2");
        assert_eq!((facts.4, facts.5, facts.6), (150, 35, 30));
        assert_eq!((facts.7, facts.8, facts.9), (2, 1, 10));
    }

    #[test]
    fn failed_session_facts_swap_preserves_previous_rows() {
        let (_dir, store) = current_refresh_store();
        let load_facts = || {
            store
                .with_conn(|conn| {
                    let mut stmt = conn.prepare(
                        "SELECT session_id, event_count, user_message_count
                         FROM session_facts ORDER BY session_id",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(Into::into)
                })
                .expect("load session facts")
        };
        let before = load_facts();
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER fail_session_facts_insert
                     BEFORE INSERT ON session_facts
                     BEGIN SELECT RAISE(FAIL, 'forced session facts failure'); END;",
                )?;
                Ok(())
            })
            .expect("install session facts failure trigger");

        let error = rebuild_session_facts_with_progress(&store, |_| {})
            .expect_err("session facts replacement should fail");

        assert!(error.to_string().contains("forced session facts failure"));
        assert_eq!(load_facts(), before);
    }

    #[test]
    fn provenance_rebuild_uses_relationships_and_includes_assistant_items() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status, metadata_json, hash)
                    VALUES
                      ('session_parent', 'source', 'machine', 'codex', 'parent', 'open', '{}', 'session_parent_hash'),
                      ('session_sub', 'source', 'machine', 'codex', 'sub', 'open', '{}', 'session_sub_hash'),
                      ('session_heuristic', 'source', 'machine', 'codex', 'heuristic', 'open', '{}', 'session_heuristic_hash'),
                      ('session_human', 'source', 'machine', 'codex', 'human', 'open', '{}', 'session_human_hash');

                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, metadata_json, hash)
                    VALUES
                      ('event_notification', 'session_parent', 'source', 'machine', 'codex', 0,
                       'message', 'user',
                       '<subagent_notification>{"agent_path":"sub"}</subagent_notification>',
                       '{}', 'event_notification_hash'),
                      ('event_parent_copy', 'session_parent', 'source', 'machine', 'codex', 1,
                       'message', 'user', 'inherited human turn', '{}', 'event_parent_copy_hash'),
                      ('event_meta', 'session_sub', 'source', 'machine', 'codex', 0, 'session_meta',
                       NULL, '{"payload":{"thread_source":"subagent"}}', '{}', 'event_meta_hash'),
                      ('event_sub', 'session_sub', 'source', 'machine', 'codex', 7, 'message',
                       'user', 'inherited human turn', '{}', 'event_sub_hash'),
                      ('event_sub_divergent', 'session_sub', 'source', 'machine', 'codex', 8,
                       'message', 'user', 'child task prompt', '{}', 'event_sub_divergent_hash'),
                      ('event_heuristic_meta', 'session_heuristic', 'source', 'machine', 'codex', 0,
                       'session_meta', NULL, '{"payload":{"thread_source":"subagent"}}',
                       '{}', 'event_heuristic_meta_hash'),
                      ('event_heuristic_user', 'session_heuristic', 'source', 'machine', 'codex', 1,
                       'message', 'user', 'real human turn', '{}', 'event_heuristic_user_hash'),
                      ('event_abort', 'session_human', 'source', 'machine', 'codex', 0, 'message',
                       'user', '<turn_aborted>stopped</turn_aborted>', '{}', 'event_abort_hash'),
                      ('event_image', 'session_human', 'source', 'machine', 'codex', 1, 'message',
                       'user', '<image name="photo.png">my caption', '{}', 'event_image_hash'),
                      ('event_assistant', 'session_human', 'source', 'machine', 'codex', 2, 'message',
                       'assistant', 'helpful answer', '{}', 'event_assistant_hash');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, lexical_indexable, semantic_policy,
                       metadata_json, hash)
                    VALUES
                      ('item_parent_copy', 'event_parent_copy', 'session_parent', 'source',
                       'machine', 'codex', 1, 0, 'conversation', 'user', 'inherited human turn',
                       'hash_inherited', 1, 'required', '{}', 'item_parent_copy_hash'),
                      ('item_sub', 'event_sub', 'session_sub', 'source', 'machine', 'codex', 7,
                       0, 'conversation', 'user', 'inherited human turn', 'hash_inherited',
                       1, 'required', '{}', 'item_sub_hash'),
                      ('item_sub_divergent', 'event_sub_divergent', 'session_sub', 'source',
                       'machine', 'codex', 8, 0, 'conversation', 'user', 'child task prompt',
                       'hash_divergent', 1, 'required', '{}', 'item_sub_divergent_hash'),
                      ('item_heuristic', 'event_heuristic_user', 'session_heuristic', 'source',
                       'machine', 'codex', 1, 0, 'conversation', 'user', 'real human turn',
                       'hash_heuristic', 1, 'required', '{}', 'item_heuristic_hash'),
                      ('item_abort', 'event_abort', 'session_human', 'source', 'machine', 'codex', 0,
                       0, 'conversation', 'user', '<turn_aborted>stopped</turn_aborted>', 'hash_abort', 1, 'required', '{}', 'item_abort_hash'),
                      ('item_image', 'event_image', 'session_human', 'source', 'machine', 'codex', 1,
                       0, 'conversation', 'user', '<image name="photo.png">my caption', 'hash_image', 1, 'required', '{}', 'item_image_hash'),
                      ('item_assistant', 'event_assistant', 'session_human', 'source', 'machine',
                       'codex', 2, 0, 'conversation', 'assistant', 'helpful answer',
                       'hash_assistant', 1, 'required', '{}', 'item_assistant_hash');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert provenance fixtures");

        let mut rebuild_progress = Vec::new();
        rebuild_all_with_progress(&store, |event| rebuild_progress.push(event))
            .expect("rebuild provenance");
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

        let provenance_details = rebuild_progress
            .iter()
            .filter_map(|event| match event {
                RebuildProgress::Detail {
                    projection,
                    detail,
                    ..
                } if *projection == MESSAGE_PROVENANCE_PROJECTION => Some(detail.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(provenance_details.contains(&"preparing provenance staging table"));
        assert!(provenance_details.contains(&"finding repeated message templates"));
        assert!(provenance_details.contains(&"loading inherited parent messages"));
        assert!(provenance_details
            .iter()
            .any(|detail| detail.starts_with("indexing ")));
        assert!(provenance_details
            .iter()
            .any(|detail| detail.starts_with("storing 0/")));
        assert!(provenance_details.contains(&"committing message provenance"));
        let classified = provenance_details
            .iter()
            .filter_map(|detail| {
                detail
                    .strip_prefix("classifying ")
                    .or_else(|| detail.strip_prefix("classified "))?
                    .strip_suffix(" messages")?
                    .split_once('/')?
                    .0
                    .parse::<usize>()
                    .ok()
            })
            .collect::<Vec<_>>();
        assert_eq!(classified, vec![0, 7]);
        assert!(classified.windows(2).all(|window| window[0] <= window[1]));
        assert!(provenance_details
            .iter()
            .any(|detail| detail == &"staged 7/7 messages"));

        assert_eq!(rows.len(), 7);
        assert_eq!(
            rows[0],
            (
                "item_abort".to_string(),
                "harness".to_string(),
                "no".to_string(),
                "tag.turn_aborted".to_string()
            )
        );
        assert_eq!(rows[1].1, "assistant");
        assert_eq!(rows[1].2, "no");
        assert_eq!(rows[1].3, "message.assistant");
        assert_eq!(rows[2].1, "human");
        assert_eq!(rows[2].3, "default.human");
        assert_eq!(rows[3].1, "human");
        assert_eq!(rows[3].2, "strip_wrapper");
        assert_eq!(rows[4].1, "human");
        assert_eq!(rows[5].1, "human");
        assert_eq!(rows[5].3, "default.human");
        assert_eq!(rows[6].1, "agent");
        assert_eq!(rows[6].3, "relationship.subagent");
    }

    #[test]
    fn failed_provenance_rebuild_preserves_previous_rows() {
        let (_dir, store) = current_refresh_store();
        let load_rows = || {
            store
                .with_conn(|conn| {
                    let mut stmt = conn.prepare(
                        "SELECT item_id, authored_by, rule
                         FROM message_provenance
                         ORDER BY item_id",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(Into::into)
                })
                .expect("load provenance rows")
        };
        let before = load_rows();
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER fail_provenance_rebuild
                     BEFORE INSERT ON message_provenance
                     BEGIN
                       SELECT RAISE(ABORT, 'fixture provenance insert failure');
                     END;",
                )?;
                Ok(())
            })
            .expect("install failing provenance trigger");

        let error = rebuild_message_provenance(&store, |_| {})
            .expect_err("provenance replacement should fail");

        assert!(error.to_string().contains("fixture provenance insert failure"));
        assert_eq!(load_rows(), before);
    }

    #[test]
    fn provenance_staging_does_not_hold_the_main_writer_lock() {
        let (_dir, store) = current_refresh_store();
        let mut wrote_during_staging = false;

        rebuild_message_provenance(&store, |detail| {
            if !wrote_during_staging && detail.starts_with("classifying 0/") {
                store
                    .with_conn(|conn| {
                        conn.execute_batch(
                            "CREATE TABLE provenance_writer_probe (id INTEGER PRIMARY KEY);",
                        )?;
                        Ok(())
                    })
                    .expect("write through a second connection during staging");
                wrote_during_staging = true;
            }
        })
        .expect("rebuild provenance without holding the main writer");

        assert!(wrote_during_staging);
    }

    #[test]
    fn classified_progress_accepts_active_and_completed_batches() {
        assert_eq!(
            classified_message_progress("classifying 500/1000 messages"),
            Some(500)
        );
        assert_eq!(
            classified_message_progress("classified 500/1000 messages"),
            Some(500)
        );
    }

    #[test]
    fn relationship_progress_reports_only_resolved_sessions() {
        let (_dir, store) = current_refresh_store();
        let mut progress = Vec::new();

        rebuild_session_relationships_with_progress(&store, |processed, total| {
            progress.push((processed, total));
        })
        .expect("rebuild relationships with progress");

        assert!(!progress.is_empty());
        let total = progress[0].1;
        assert!(total > 0);
        assert!(progress.iter().all(|(processed, current_total)|
            *current_total == total && processed <= current_total));
        assert!(progress
            .windows(2)
            .all(|window| window[0].0 <= window[1].0));
    }

    #[test]
    fn failed_relationship_swap_preserves_relationships_and_overrides() {
        let (_dir, store) = current_refresh_store();
        store
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO event_session_overrides (event_id, session_id)
                     VALUES ('target_event_1', 'session_target')",
                    [],
                )?;
                Ok(())
            })
            .expect("seed event override");
        let load_rows = || {
            store
                .with_conn(|conn| {
                    let relationships = {
                        let mut stmt = conn.prepare(
                            "SELECT session_id, parent_session_id, root_session_id, relationship, rule
                             FROM session_relationships ORDER BY session_id",
                        )?;
                        let rows = stmt.query_map([], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Option<String>>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                            ))
                        })?;
                        rows.collect::<rusqlite::Result<Vec<_>>>()?
                    };
                    let overrides = {
                        let mut stmt = conn.prepare(
                            "SELECT event_id, session_id FROM event_session_overrides
                             ORDER BY event_id",
                        )?;
                        let rows = stmt.query_map([], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                        })?;
                        rows.collect::<rusqlite::Result<Vec<_>>>()?
                    };
                    Ok((relationships, overrides))
                })
                .expect("load relationship projections")
        };
        let before = load_rows();
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER fail_relationship_insert
                     BEFORE INSERT ON session_relationships
                     BEGIN SELECT RAISE(FAIL, 'forced relationship failure'); END;",
                )?;
                Ok(())
            })
            .expect("install relationship failure trigger");

        let error = rebuild_session_relationships_with_progress(&store, |_, _| {})
            .expect_err("relationship replacement should fail");

        assert!(error.to_string().contains("forced relationship failure"));
        assert_eq!(load_rows(), before);
    }

    #[test]
    fn incremental_report_refresh_updates_only_the_touched_session() {
        let (_dir, store) = current_refresh_store();
        let delta = append_target_turn(&store);
        let mut progress = Vec::new();

        let outcome = refresh_report_after_update_with_progress(&store, &delta, |event| {
            progress.push((event.completed, event.total));
        })
        .expect("refresh scoped report projections");

        assert!(outcome.refreshed);
        assert!(!outcome.full_rebuild);
        assert_eq!(outcome.affected_sessions, 1);
        assert_eq!(outcome.affected_events, 1);
        assert!(!progress.is_empty());
        assert!(progress
            .windows(2)
            .all(|window| window[0].0 <= window[1].0 && window[0].1 <= window[1].1));
        assert!(progress.iter().all(|(completed, total)| completed <= total));
        assert_eq!(progress.last(), Some(&(progress[0].1, progress[0].1)));

        let (target_facts, untouched_facts, provenance_count, new_author) = store
            .with_conn(|conn| {
                Ok((
                    conn.query_row(
                        "SELECT event_count, user_message_count FROM session_facts
                         WHERE session_id = 'session_target'",
                        [],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                    )?,
                    conn.query_row(
                        "SELECT event_count, user_message_count FROM session_facts
                         WHERE session_id = 'session_untouched'",
                        [],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                    )?,
                    conn.query_row("SELECT COUNT(*) FROM message_provenance", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    conn.query_row(
                        "SELECT authored_by FROM message_provenance
                         WHERE item_id = 'target_item_2'",
                        [],
                        |row| row.get::<_, String>(0),
                    )?,
                ))
            })
            .expect("load incrementally refreshed projections");
        assert_eq!(target_facts, (2, 2));
        assert_eq!(untouched_facts, (1, 1));
        assert_eq!(provenance_count, 3);
        assert_eq!(new_author, "human");

        let report = snapshot_report(&store);
        assert_eq!(report.totals.sessions, 2);
        assert_eq!(report.totals.events, 3);
        assert_eq!(report.totals.human_turns, 3);
        assert_projections_current(&store);
    }

    #[test]
    fn empty_delta_leaves_a_current_report_snapshot_unchanged() {
        let (_dir, store) = current_refresh_store();
        let delta = append_target_turn(&store);
        refresh_report_after_update_with_progress(&store, &delta, |_| {})
            .expect("perform initial scoped refresh");
        let generated_at = snapshot_report(&store).generated_at;

        let outcome = refresh_report_after_update_with_progress(
            &store,
            &ImportDelta::default(),
            |_| {},
        )
        .expect("skip current report refresh");

        assert!(!outcome.refreshed);
        assert!(!outcome.full_rebuild);
        assert_eq!(outcome.affected_sessions, 0);
        assert_eq!(outcome.affected_events, 0);
        assert_eq!(snapshot_report(&store).generated_at, generated_at);
        assert_projections_current(&store);
    }
    #[test]
    fn report_refresh_status_tracks_empty_current_and_stale_stores() {
        let empty_dir = tempfile::tempdir().expect("empty tempdir");
        let empty_store = Store::open(empty_dir.path()).expect("open empty store");
        let empty = report_refresh_status(&empty_store).expect("empty refresh status");
        assert!(empty.stale);
        assert!(empty.snapshot_missing);
        assert!(empty.prerequisites_invalid);

        let (_dir, store) = current_refresh_store();
        let current = report_refresh_status(&store).expect("current refresh status");
        assert!(!current.stale);
        assert!(!current.snapshot_missing);
        assert!(!current.prerequisites_invalid);

        append_target_turn(&store);
        let stale = report_refresh_status(&store).expect("stale refresh status");
        assert!(stale.stale);
        assert!(!stale.snapshot_missing);
        assert!(!stale.prerequisites_invalid);

        store
            .with_conn(|conn| {
                conn.execute("DELETE FROM report_snapshot", [])?;
                Ok(())
            })
            .expect("remove snapshot");
        assert!(
            report_refresh_status(&store)
                .expect("missing snapshot status")
                .snapshot_missing
        );
    }



    #[test]
    fn missing_snapshot_with_current_prerequisites_avoids_full_fallback() {
        let (_dir, store) = current_refresh_store();
        store
            .with_conn(|conn| {
                conn.execute("DELETE FROM report_snapshot", [])?;
                conn.execute(
                    "DELETE FROM projection_status WHERE projection_name = ?1",
                    [REPORT_SNAPSHOT_PROJECTION],
                )?;
                Ok(())
            })
            .expect("remove report snapshot state");
        let status = report_refresh_status(&store).expect("missing snapshot status");
        assert!(status.stale);
        assert!(status.snapshot_missing);
        assert!(!status.prerequisites_invalid);

        let outcome = refresh_report_on_demand_with_progress(&store, true, |_| {})
            .expect("rebuild only missing report state");

        assert!(outcome.refreshed);
        assert!(!outcome.full_rebuild);
        assert!(!report_refresh_status(&store)
            .expect("restored snapshot status")
            .stale);
    }

    #[test]
    fn repaired_delta_skips_a_report_snapshot_already_rebuilt_by_repair() {
        let (_dir, store) = current_refresh_store();
        let generated_at = snapshot_report(&store).generated_at;
        let delta = ImportDelta {
            repaired_events: vec!["target_event_1".to_string()],
            touched_sessions: vec!["session_target".to_string()],
            ..ImportDelta::default()
        };

        let outcome = refresh_report_after_update_with_progress(&store, &delta, |_| {})
            .expect("skip duplicate repaired report refresh");

        assert!(!outcome.refreshed);
        assert_eq!(snapshot_report(&store).generated_at, generated_at);
        assert_projections_current(&store);
    }

    #[test]
    fn empty_delta_catches_up_a_snapshot_stale_by_a_later_event() {
        let (_dir, store) = current_refresh_store();
        append_target_turn(&store);
        let stale = freshness(&store).expect("load stale projection statuses");
        assert!(stale.iter().all(|status| status.stale));
        assert!(stale.iter().all(|status| status.new_event_rows == 1));

        let outcome = refresh_report_after_update_with_progress(
            &store,
            &ImportDelta::default(),
            |_| {},
        )
        .expect("catch up stale report without import delta");

        assert!(outcome.refreshed);
        assert!(!outcome.full_rebuild);
        assert_eq!(outcome.affected_sessions, 1);
        assert_eq!(outcome.affected_events, 1);
        let report = snapshot_report(&store);
        assert_eq!(report.totals.events, 3);
        assert_eq!(report.totals.human_turns, 3);
        assert_projections_current(&store);
    }

    #[test]
    fn failed_projection_forces_a_complete_report_refresh() {
        let (_dir, store) = current_refresh_store();
        let failed_at = store
            .with_conn(max_event_rowid)
            .expect("load projection watermark");
        set_projection_failed(
            &store,
            PROJECTIONS[1],
            failed_at,
            "fixture projection failure",
        )
        .expect("mark provenance projection failed");
        store
            .with_conn(|conn| {
                conn.execute(
                    "DELETE FROM session_facts WHERE session_id = 'session_untouched'",
                    [],
                )?;
                conn.execute(
                    "DELETE FROM message_provenance WHERE session_id = 'session_untouched'",
                    [],
                )?;
                Ok(())
            })
            .expect("make partial projections observable");
        let delta = append_target_turn(&store);

        let outcome = refresh_report_after_update_with_progress(&store, &delta, |_| {})
            .expect("fall back to complete report refresh");

        assert!(outcome.refreshed);
        assert!(outcome.full_rebuild);
        assert_eq!(outcome.affected_sessions, 2);
        assert_eq!(outcome.affected_events, 3);
        let (fact_count, provenance_count) = store
            .with_conn(|conn| {
                Ok((
                    conn.query_row("SELECT COUNT(*) FROM session_facts", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    conn.query_row("SELECT COUNT(*) FROM message_provenance", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                ))
            })
            .expect("load fully restored projections");
        assert_eq!(fact_count, 2);
        assert_eq!(provenance_count, 3);
        let report = snapshot_report(&store);
        assert_eq!(report.totals.sessions, 2);
        assert_eq!(report.totals.events, 3);
        assert_eq!(report.totals.human_turns, 3);
        assert_projections_current(&store);
    }

    #[test]
    fn scoped_refresh_reclassifies_peers_when_a_template_hash_disappears() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let template = "shared generated template ".repeat(12);
        store
            .with_conn(|conn| {
                for index in 0..4 {
                    let session_id = format!("template_session_{index}");
                    let event_id = format!("template_event_{index}");
                    let item_id = format!("template_item_{index}");
                    conn.execute(
                        "INSERT INTO sessions
                         (id, source_id, machine_id, source_kind, external_id, status,
                          metadata_json, hash)
                         VALUES (?1, 'source', 'machine', 'hermes', ?1, 'open', ?2, ?3)",
                        params![
                            session_id,
                            format!(r#"{{"workspace_path":"/repo/{index}"}}"#),
                            format!("template_session_hash_{index}")
                        ],
                    )?;
                    conn.execute(
                        "INSERT INTO events
                         (id, session_id, source_id, machine_id, source_kind, ordinal,
                          event_type, role, content, occurred_at, metadata_json, hash)
                         VALUES (?1, ?2, 'source', 'machine', 'hermes', 0, 'message', 'user',
                                 ?3, '2026-07-12T00:00:00Z', '{}', ?4)",
                        params![
                            event_id,
                            session_id,
                            template,
                            format!("template_event_hash_{index}")
                        ],
                    )?;
                    conn.execute(
                        "INSERT INTO session_activity
                         (session_id, event_count, first_event_at, last_event_at)
                         VALUES (?1, 1, '2026-07-12T00:00:00Z', '2026-07-12T00:00:00Z')",
                        [session_id.as_str()],
                    )?;
                    conn.execute(
                        "INSERT INTO history_items
                         (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                          subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                          semantic_policy, metadata_json, hash)
                         VALUES (?1, ?2, ?3, 'source', 'machine', 'hermes', 0, 0,
                                 'conversation', 'user', ?4, 'shared_template_hash',
                                 '2026-07-12T00:00:00Z', 1, 'required', '{}', ?5)",
                        params![
                            item_id,
                            event_id,
                            session_id,
                            template,
                            format!("template_item_hash_{index}")
                        ],
                    )?;
                }
                Ok(())
            })
            .expect("insert repeated template fixtures");
        rebuild_all(&store, |_, _, _| {}).expect("seed repeated template projections");

        let delta = ImportDelta {
            touched_events: vec!["template_event_0".to_string()],
            touched_sessions: vec!["template_session_0".to_string()],
            ..ImportDelta::default()
        };
        let prior_hashes = report_refresh_prior_hashes(&store, &delta, |_, _| {})
            .expect("capture replaced template hashes");
        assert_eq!(prior_hashes, HashSet::from(["shared_template_hash".to_string()]));
        store
            .with_conn(|conn| {
                conn.execute(
                    "DELETE FROM history_items WHERE event_id = 'template_event_0'",
                    [],
                )?;
                conn.execute(
                    "INSERT INTO history_items
                     (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                      subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                      semantic_policy, metadata_json, hash)
                     VALUES ('replacement_item', 'template_event_0', 'template_session_0',
                             'source', 'machine', 'hermes', 0, 0, 'conversation', 'user',
                             'short human message', 'replacement_hash',
                             '2026-07-12T00:00:00Z', 1, 'required', '{}', 'replacement_item_hash')",
                    [],
                )?;
                Ok(())
            })
            .expect("replace derived history item");

        let outcome = refresh_report_after_update_with_prior_hashes(
            &store,
            &delta,
            &prior_hashes,
            |_| {},
        )
        .expect("refresh removed template peers");
        assert!(!outcome.full_rebuild);
        assert_eq!(outcome.affected_sessions, 4);
        let authors = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT authored_by FROM message_provenance ORDER BY session_id",
                )?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            })
            .expect("load reclassified provenance");
        assert_eq!(authors, vec!["human"; 4]);
        assert_projections_current(&store);
    }

    #[test]
    fn provenance_message_count_respects_the_requested_sessions() {
        let (_dir, store) = current_refresh_store();
        append_target_turn(&store);

        assert_eq!(
            provenance_message_count(&store, &HashSet::new()).expect("count empty scope"),
            0
        );
        assert_eq!(
            provenance_message_count(
                &store,
                &HashSet::from(["session_target".to_string()]),
            )
            .expect("count target scope"),
            2
        );
        assert_eq!(
            provenance_message_count(
                &store,
                &HashSet::from([
                    "session_target".to_string(),
                    "session_untouched".to_string(),
                ]),
            )
            .expect("count full scope"),
            3
        );
    }

    #[test]
    fn prior_hash_capture_reports_batched_progress() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let delta = ImportDelta {
            touched_events: (0..10_742).map(|index| format!("event_{index}")).collect(),
            repaired_events: vec!["event_0".to_string()],
            ..ImportDelta::default()
        };
        let mut progress = Vec::new();

        let hashes = report_refresh_prior_hashes(&store, &delta, |completed, total| {
            progress.push((completed, total));
        })
        .expect("capture prior hashes in batches");

        assert!(hashes.is_empty());
        assert_eq!(progress.first(), Some(&(0, 10_742)));
        assert_eq!(progress.last(), Some(&(10_742, 10_742)));
        assert_eq!(progress.len(), 23);
        assert!(progress.windows(2).all(|window| {
            window[0].0 < window[1].0
                && window[1].0 - window[0].0 <= 500
                && window[0].1 == 10_742
                && window[1].1 == 10_742
        }));
    }

    fn current_refresh_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO sessions
                      (id, source_id, machine_id, source_kind, external_id, status,
                       started_at, updated_at, metadata_json, hash)
                    VALUES
                      ('session_target', 'source', 'machine', 'hermes', 'target', 'open',
                       '2026-07-12T00:00:00Z', '2026-07-12T00:00:00Z',
                       '{"workspace_path":"/repo/target"}', 'session_target_hash'),
                      ('session_untouched', 'source', 'machine', 'hermes', 'untouched', 'open',
                       '2026-07-12T00:00:00Z', '2026-07-12T00:00:00Z',
                       '{"workspace_path":"/repo/untouched"}', 'session_untouched_hash');

                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, occurred_at, metadata_json, hash)
                    VALUES
                      ('target_event_1', 'session_target', 'source', 'machine', 'hermes', 0,
                       'message', 'user', 'target question one', '2026-07-12T00:00:00Z',
                       '{}', 'target_event_hash_1'),
                      ('untouched_event_1', 'session_untouched', 'source', 'machine', 'hermes', 0,
                       'message', 'user', 'untouched question', '2026-07-12T00:00:00Z',
                       '{}', 'untouched_event_hash_1');

                    INSERT INTO session_activity
                      (session_id, event_count, first_event_at, last_event_at)
                    VALUES
                      ('session_target', 1, '2026-07-12T00:00:00Z', '2026-07-12T00:00:00Z'),
                      ('session_untouched', 1, '2026-07-12T00:00:00Z', '2026-07-12T00:00:00Z');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('target_item_1', 'target_event_1', 'session_target', 'source', 'machine',
                       'hermes', 0, 0, 'conversation', 'user', 'target question one',
                       'target_text_hash_1', '2026-07-12T00:00:00Z', 1, 'required', '{}',
                       'target_item_hash_1'),
                      ('untouched_item_1', 'untouched_event_1', 'session_untouched', 'source',
                       'machine', 'hermes', 0, 0, 'conversation', 'user', 'untouched question',
                       'untouched_text_hash_1', '2026-07-12T00:00:00Z', 1, 'required', '{}',
                       'untouched_item_hash_1');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert report refresh fixtures");
        rebuild_all(&store, |_, _, _| {}).expect("seed current report projections");
        (dir, store)
    }

    fn append_target_turn(store: &Store) -> ImportDelta {
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO events
                      (id, session_id, source_id, machine_id, source_kind, ordinal, event_type,
                       role, content, occurred_at, metadata_json, hash)
                    VALUES
                      ('target_event_2', 'session_target', 'source', 'machine', 'hermes', 1,
                       'message', 'user', 'target question two', '2026-07-12T00:01:00Z',
                       '{}', 'target_event_hash_2');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('target_item_2', 'target_event_2', 'session_target', 'source', 'machine',
                       'hermes', 1, 0, 'conversation', 'user', 'target question two',
                       'target_text_hash_2', '2026-07-12T00:01:00Z', 1, 'required', '{}',
                       'target_item_hash_2');

                    UPDATE session_activity
                    SET event_count = 2, last_event_at = '2026-07-12T00:01:00Z'
                    WHERE session_id = 'session_target';
                    "#,
                )?;
                Ok(())
            })
            .expect("append target event and history item");
        ImportDelta {
            inserted_events: vec!["target_event_2".to_string()],
            touched_sessions: vec!["session_target".to_string()],
            ..ImportDelta::default()
        }
    }

    fn snapshot_report(store: &Store) -> crate::report::UsageReport {
        let json = store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT report_json FROM report_snapshot WHERE singleton = 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(Into::into)
            })
            .expect("load report snapshot JSON");
        serde_json::from_str(&json).expect("parse report snapshot JSON")
    }

    fn assert_projections_current(store: &Store) {
        let statuses = freshness(store).expect("load current projection statuses");
        assert!(statuses.iter().all(|status| !status.stale));
        assert!(statuses
            .iter()
            .all(|status| status.stored_input_rowid == Some(status.input_rowid)));
    }

    #[test]
    fn rebuild_tracks_versions_and_new_event_staleness() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        insert_event(&store, "event-1", 1);

        rebuild_all(&store, |_, _, _| {}).expect("rebuild projections");
        assert!(!is_stale(&store).expect("freshness after rebuild"));
        let statuses = freshness(&store).expect("projection statuses");
        assert_eq!(statuses.len(), 4);
        assert_eq!(
            statuses[0].stored_version,
            Some(SESSION_RELATIONSHIPS_VERSION)
        );
        assert_eq!(statuses[1].stored_version, Some(MESSAGE_PROVENANCE_VERSION));
        assert_eq!(statuses[2].stored_version, Some(SESSION_FACTS_VERSION));
        assert_eq!(statuses[3].stored_version, Some(REPORT_SNAPSHOT_VERSION));

        insert_event(&store, "event-2", 2);
        let statuses = freshness(&store).expect("stale projection statuses");
        assert!(statuses.iter().all(|status| status.stale));
        assert!(statuses.iter().all(|status| status.new_event_rows == 1));

        rebuild_all(&store, |_, _, _| {}).expect("rebuild stale projections");
        assert!(!is_stale(&store).expect("freshness after second rebuild"));

        let bumped = Projection {
            version: MESSAGE_PROVENANCE_VERSION + 1,
            ..PROJECTIONS[1]
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
