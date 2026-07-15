use crate::archive::stable_id;
use crate::ingest::{self, SessionClass};
use crate::provenance;
use crate::storage::{ImportDelta, Store};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub const MESSAGE_PROVENANCE_PROJECTION: &str = "message_provenance";
pub const MESSAGE_PROVENANCE_VERSION: u32 = 8;
pub const SESSION_RELATIONSHIPS_PROJECTION: &str = "session_relationships";
pub const SESSION_RELATIONSHIPS_VERSION: u32 = 8;
pub const SESSION_FACTS_PROJECTION: &str = "session_facts";
pub const SESSION_FACTS_VERSION: u32 = 2;
pub const REPORT_SNAPSHOT_PROJECTION: &str = "report_snapshot";
pub const REPORT_SNAPSHOT_VERSION: u32 = 8;

const PROJECTIONS: [Projection; 4] = [
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

pub(crate) fn report_refresh_prior_hashes(
    store: &Store,
    delta: &ImportDelta,
) -> Result<HashSet<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT DISTINCT text_hash FROM history_items
             WHERE event_id = ?1 AND tier = 'conversation' AND kind = 'user'
               AND length(text) > 200",
        )?;
        let mut hashes = HashSet::new();
        for event_id in delta.touched_events.iter().chain(&delta.repaired_events) {
            let rows = stmt.query_map([event_id], |row| row.get::<_, String>(0))?;
            for hash in rows {
                hashes.insert(hash?);
            }
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
    mut progress: impl FnMut(ReportRefreshProgress),
) -> Result<ReportRefreshOutcome> {
    let statuses = freshness(store)?;
    let delta_empty = delta.inserted_sessions.is_empty()
        && delta.touched_sessions.is_empty()
        && delta.inserted_events.is_empty()
        && delta.touched_events.is_empty()
        && delta.repaired_events.is_empty();
    if statuses.iter().all(|status| !status.stale)
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
    let invalid_state = statuses.iter().any(|status| {
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
    let mut session_ids = delta
        .inserted_sessions
        .iter()
        .chain(&delta.touched_sessions)
        .cloned()
        .collect::<HashSet<_>>();

    if invalid_state {
        event_ids = all_event_ids(store, captured_input_rowid)?;
        session_ids = all_session_ids(store)?;
    } else if statuses.iter().any(|status| status.stale) {
        let catch_up_after = statuses
            .iter()
            .filter_map(|status| status.stored_input_rowid)
            .min()
            .unwrap_or(0);
        let catch_up = events_after_watermark(store, catch_up_after, captured_input_rowid)?;
        for (event_id, session_id) in catch_up {
            event_ids.insert(event_id);
            session_ids.insert(session_id);
        }
    }
    session_ids.extend(session_ids_for_events(store, &event_ids)?);

    let relationship_fallback = invalid_state
        || !delta.inserted_sessions.is_empty()
        || relationship_sensitive_scope(store, &session_ids, &event_ids)?;
    let mut provenance_sessions = if invalid_state || relationship_fallback {
        all_session_ids(store)?
    } else {
        widen_provenance_sessions(store, &session_ids, prior_candidate_hashes)?
    };
    if provenance_sessions.is_empty() {
        provenance_sessions.extend(session_ids.iter().cloned());
    }
    let affected_sessions = session_ids.union(&provenance_sessions).count();

    let relationship_work = if relationship_fallback {
        all_session_ids(store)?.len().max(1)
    } else {
        1
    };
    let fact_work = session_ids.len().max(1);
    let provenance_work = provenance_message_count(store, &provenance_sessions)?.max(1);
    let report_work = 15 + total_conversation_messages(store)?;
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
        run_projection_refresh(store, PROJECTIONS[0], captured_input_rowid, || Ok(()))?;
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
            )
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
        Ok(()) => set_projection_ready(store, PROJECTIONS[3], captured_input_rowid)?,
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
    action: impl FnOnce() -> Result<()>,
) -> Result<()> {
    set_projection_building(store, projection, input_rowid)?;
    match action() {
        Ok(()) => set_projection_ready(store, projection, input_rowid),
        Err(error) => {
            let _ = set_projection_failed(store, projection, input_rowid, &error.to_string());
            Err(error)
        }
    }
}

fn classified_message_progress(detail: &str) -> Option<usize> {
    detail
        .strip_prefix("classifying ")?
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

    let result = match projection.name {
        SESSION_RELATIONSHIPS_PROJECTION => rebuild_session_relationships(store),
        MESSAGE_PROVENANCE_PROJECTION => rebuild_message_provenance(store, &mut progress),
        SESSION_FACTS_PROJECTION => rebuild_session_facts(store),
        REPORT_SNAPSHOT_PROJECTION => crate::report::rebuild_snapshot(store),
        _ => clear_projection(store, projection),
    };

    match result {
        Ok(()) => set_projection_ready(store, projection, input_rowid),
        Err(error) => {
            let _ = set_projection_failed(store, projection, input_rowid, &error.to_string());
            Err(error)
        }
    }
}

fn rebuild_session_relationships(store: &Store) -> Result<()> {
    rebuild_session_relationships_with_progress(store, |_, _| {})
}

fn rebuild_session_relationships_with_progress(
    store: &Store,
    mut progress: impl FnMut(usize, usize),
) -> Result<()> {
    let projection = PROJECTIONS[0];
    clear_projection(store, projection)?;
    clear_event_session_overrides(store)?;

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

    let codex_notifications = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT s.machine_id, e.session_id, e.content
             FROM events e
             JOIN sessions s ON s.id = e.session_id
             WHERE e.source_kind = 'codex'
               AND e.content LIKE '%subagent_notification%'
             ORDER BY e.rowid",
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
    })?;
    let mut codex_parents = HashMap::<(String, String), (String, bool)>::new();
    for (machine_id, parent_session_id, content) in codex_notifications {
        for child_external_id in ingest::codex_subagent_paths(&content) {
            codex_parents
                .entry((machine_id.clone(), child_external_id))
                .and_modify(|(existing_parent_id, collision)| {
                    if existing_parent_id != &parent_session_id {
                        *collision = true;
                    }
                })
                .or_insert_with(|| (parent_session_id.clone(), false));
        }
    }

    let mut hints = Vec::with_capacity(sessions.len());
    let mut parents = HashMap::with_capacity(sessions.len());
    let mut inline_relationships = Vec::new();
    let mut event_overrides = Vec::new();
    let total = sessions.len();
    for (index, session) in sessions.iter().enumerate() {
        let metadata = serde_json::from_str::<Value>(&session.metadata_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let event_contents = session_event_contents(store, &session.session_id, None)?;
        let event_contents = event_contents
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let hint = ingest::resolve_session_relationship(
            &session.source_kind,
            &session.external_id,
            &metadata,
            &event_contents,
        );
        let mut parent_session_id = hint.parent_external_id.as_ref().and_then(|external_id| {
            relationship_parent_session_id(session, external_id, &session_ids)
        });
        let mut hint = hint;
        if session.source_kind == "codex" {
            if let Some((codex_parent_session_id, collision)) =
                codex_parents.get(&(session.machine_id.clone(), session.external_id.clone()))
            {
                parent_session_id = Some(codex_parent_session_id.clone());
                hint.relationship = ingest::SessionRelationshipKind::Subagent;
                hint.rule = if *collision {
                    "codex.subagent_notification.collision"
                } else {
                    "codex.subagent_notification"
                };
            }
        }
        if session.source_kind == "claude_code"
            && hint.relationship == ingest::SessionRelationshipKind::None
        {
            let (relationships, overrides) =
                claude_inline_relationships(store, &session.session_id)?;
            inline_relationships.extend(relationships);
            event_overrides.extend(overrides);
        }
        parents.insert(session.session_id.clone(), parent_session_id.clone());
        hints.push((hint, parent_session_id));
        progress(index + 1, total);
    }
    let fork_parents = codex_fork_parents(store, &sessions, &hints)?;
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

    for batch in rows.chunks(500) {
        insert_session_relationships_batch(store, batch)?;
    }
    for batch in event_overrides.chunks(500) {
        insert_event_session_overrides_batch(store, batch)?;
    }
    Ok(())
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

fn codex_fork_parents(
    store: &Store,
    sessions: &[RelationshipSession],
    hints: &[(ingest::SessionRelationshipHint, Option<String>)],
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
        let prefix = session_event_hashes(store, &session.session_id, MIN_SHARED_EVENTS + 1)?;
        if prefix.len() <= MIN_SHARED_EVENTS {
            continue;
        }
        groups
            .entry((
                session.machine_id.clone(),
                prefix[..MIN_SHARED_EVENTS].to_vec(),
            ))
            .or_default()
            .push(session);
    }

    let mut parents = HashMap::new();
    for mut group in groups.into_values().filter(|group| group.len() > 1) {
        group.sort_by_key(|session| session.rowid);
        let mut hashes = HashMap::new();
        for session in &group {
            hashes.insert(
                session.session_id.as_str(),
                session_event_hashes(store, &session.session_id, i64::MAX as usize)?,
            );
        }
        for (child_index, child) in group.iter().enumerate().skip(1) {
            let child_hashes = &hashes[child.session_id.as_str()];
            let mut best_parent = None;
            let mut best_length = 0;
            let mut tied = false;
            for parent in &group[..child_index] {
                if parent.external_id == child.external_id {
                    continue;
                }
                let parent_hashes = &hashes[parent.session_id.as_str()];
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

fn session_event_hashes(store: &Store, session_id: &str, limit: usize) -> Result<Vec<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT hash
             FROM events INDEXED BY idx_events_session_ordinal
             WHERE session_id = ?1
             ORDER BY ordinal
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![session_id, limit.min(i64::MAX as usize) as i64], |row| {
            row.get(0)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn claude_inline_relationships(
    store: &Store,
    parent_session_id: &str,
) -> Result<(Vec<InlineRelationship>, Vec<EventSessionOverride>)> {
    let events = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id, metadata_json
             FROM events INDEXED BY idx_events_session_ordinal
             WHERE session_id = ?1
             ORDER BY ordinal",
        )?;
        let rows = stmt.query_map([parent_session_id], |row| {
            let metadata = row.get::<_, String>(1)?;
            let metadata = serde_json::from_str::<Value>(&metadata)
                .unwrap_or_else(|_| serde_json::json!({}));
            let relationship = metadata.get("claude_relationship");
            Ok(InlineClaudeEvent {
                event_id: row.get(0)?,
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
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
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

fn clear_event_session_overrides(store: &Store) -> Result<()> {
    store.with_conn(|conn| {
        conn.execute("DELETE FROM event_session_overrides", [])?;
        Ok(())
    })
}

fn insert_event_session_overrides_batch(
    store: &Store,
    rows: &[EventSessionOverride],
) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting event session override batch")?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO event_session_overrides (event_id, session_id) VALUES (?1, ?2)",
            )?;
            for row in rows {
                stmt.execute(params![row.event_id, row.session_id])?;
            }
        }
        tx.commit().context("committing event session override batch")
    })
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

fn insert_session_relationships_batch(
    store: &Store,
    rows: &[SessionRelationshipRow<'_>],
) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting session relationships batch")?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO session_relationships
                 (session_id, parent_session_id, root_session_id, relationship, rule, resolved_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for row in rows {
                stmt.execute(params![
                    row.session_id,
                    row.parent_session_id,
                    row.root_session_id,
                    row.relationship,
                    row.rule,
                    row.resolved_at,
                ])?;
            }
        }
        tx.commit()
            .context("committing session relationships batch")
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
    store.with_conn(|conn| {
        let mut stmt = conn.prepare("SELECT session_id FROM events WHERE id = ?1")?;
        let mut session_ids = HashSet::new();
        for event_id in event_ids {
            if let Some(session_id) = stmt
                .query_row([event_id], |row| row.get::<_, String>(0))
                .optional()?
            {
                session_ids.insert(session_id);
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

        let mut hash_stmt = conn.prepare(
            "SELECT DISTINCT text_hash FROM history_items
             WHERE session_id = ?1 AND tier = 'conversation' AND kind = 'user'
               AND length(text) > 200",
        )?;
        let mut touched_candidate_hashes = prior_candidate_hashes.clone();
        for session_id in touched {
            let hashes = hash_stmt.query_map([session_id], |row| row.get::<_, String>(0))?;
            for hash in hashes {
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
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) FROM history_items
             WHERE session_id = ?1 AND tier = 'conversation'
               AND kind IN ('user', 'assistant')",
        )?;
        let mut total = 0usize;
        for session_id in session_ids {
            let count = stmt.query_row([session_id], |row| row.get::<_, i64>(0))?;
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


fn rebuild_session_facts(store: &Store) -> Result<()> {
    rebuild_session_facts_with_progress(store, |_| {})
}

fn rebuild_session_facts_with_progress(
    store: &Store,
    mut progress: impl FnMut(usize),
) -> Result<()> {
    const BATCH_SIZE: i64 = 500;

    clear_projection(store, PROJECTIONS[2])?;
    let mut last_rowid = 0i64;
    let mut processed = 0usize;
    loop {
        let batch = store.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT s.rowid, s.id, s.source_kind, s.metadata_json,
                        COALESCE(sa.event_count,
                          (SELECT COUNT(*) FROM events e
                           WHERE e.session_id = s.id)),
                        COALESCE(sa.first_event_at, s.started_at),
                        COALESCE(sa.last_event_at, s.updated_at),
                        (SELECT COUNT(*)
                         FROM history_items hi INDEXED BY idx_history_items_session_order
                         WHERE hi.session_id = s.id
                           AND hi.tier = 'conversation'
                           AND hi.kind = 'user')
                 FROM sessions s
                 LEFT JOIN session_activity sa ON sa.session_id = s.id
                 WHERE s.rowid > ?1
                 ORDER BY s.rowid
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![last_rowid, BATCH_SIZE], |row| {
                Ok(SessionFactInput {
                    rowid: row.get(0)?,
                    session_id: row.get(1)?,
                    source_kind: row.get(2)?,
                    metadata_json: row.get(3)?,
                    event_count: row.get::<_, i64>(4)?.max(0),
                    first_event_at: row.get(5)?,
                    last_event_at: row.get(6)?,
                    user_message_count: row.get::<_, i64>(7)?.max(0),
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })?;
        if batch.is_empty() {
            break;
        }
        last_rowid = batch.last().expect("non-empty session facts batch").rowid;

        let mut rows = Vec::with_capacity(batch.len());
        for input in batch {
            let metadata = serde_json::from_str::<Value>(&input.metadata_json)
                .unwrap_or_else(|_| serde_json::json!({}));
            let events = session_usage_events(store, &input.session_id)?;
            let event_contents = events
                .iter()
                .map(|event| event.content.as_str())
                .collect::<Vec<_>>();
            let session_class =
                ingest::classify_session(&input.source_kind, &metadata, &event_contents);
            let usage = ingest::extract_session_usage(&input.source_kind, &events);
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
                event_count: input.event_count,
                user_message_count: input.user_message_count,
                duration_secs: duration_secs(
                    input.first_event_at.as_deref(),
                    input.last_event_at.as_deref(),
                ),
                first_event_at: input.first_event_at,
                last_event_at: input.last_event_at,
            });
        }
        insert_session_facts_batch(store, &rows)?;
        processed += rows.len();
        progress(processed);
    }
    Ok(())
}

fn refresh_session_facts_scoped(
    store: &Store,
    session_ids: &HashSet<String>,
    mut progress: impl FnMut(usize),
) -> Result<()> {
    let mut session_ids = session_ids.iter().collect::<Vec<_>>();
    session_ids.sort_unstable();
    let mut processed = 0usize;
    for batch in session_ids.chunks(500) {
        let mut rows = Vec::with_capacity(batch.len());
        for session_id in batch {
            let input = store.with_conn(|conn| {
                conn.query_row(
                    "SELECT s.rowid, s.id, s.source_kind, s.metadata_json,
                            COALESCE(sa.event_count,
                              (SELECT COUNT(*) FROM events e WHERE e.session_id = s.id)),
                            COALESCE(sa.first_event_at, s.started_at),
                            COALESCE(sa.last_event_at, s.updated_at),
                            (SELECT COUNT(*) FROM history_items hi
                             WHERE hi.session_id = s.id AND hi.tier = 'conversation'
                               AND hi.kind = 'user')
                     FROM sessions s
                     LEFT JOIN session_activity sa ON sa.session_id = s.id
                     WHERE s.id = ?1",
                    [session_id.as_str()],
                    |row| {
                        Ok(SessionFactInput {
                            rowid: row.get(0)?,
                            session_id: row.get(1)?,
                            source_kind: row.get(2)?,
                            metadata_json: row.get(3)?,
                            event_count: row.get::<_, i64>(4)?.max(0),
                            first_event_at: row.get(5)?,
                            last_event_at: row.get(6)?,
                            user_message_count: row.get::<_, i64>(7)?.max(0),
                        })
                    },
                )
                .optional()
                .map_err(Into::into)
            })?;
            let Some(input) = input else {
                continue;
            };
            let metadata = serde_json::from_str::<Value>(&input.metadata_json)
                .unwrap_or_else(|_| serde_json::json!({}));
            let events = session_usage_events(store, &input.session_id)?;
            let event_contents = events
                .iter()
                .map(|event| event.content.as_str())
                .collect::<Vec<_>>();
            let session_class =
                ingest::classify_session(&input.source_kind, &metadata, &event_contents);
            let usage = ingest::extract_session_usage(&input.source_kind, &events);
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
                event_count: input.event_count,
                user_message_count: input.user_message_count,
                duration_secs: duration_secs(
                    input.first_event_at.as_deref(),
                    input.last_event_at.as_deref(),
                ),
                first_event_at: input.first_event_at,
                last_event_at: input.last_event_at,
            });
        }
        insert_session_facts_batch(store, &rows)?;
        processed += batch.len();
        progress(processed);
    }
    Ok(())
}

fn session_usage_events(store: &Store, session_id: &str) -> Result<Vec<ingest::UsageEvent>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT content, metadata_json
             FROM events INDEXED BY idx_events_session_ordinal
             WHERE session_id = ?1
             ORDER BY ordinal",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            let metadata = row.get::<_, String>(1)?;
            Ok(ingest::UsageEvent {
                content: row.get(0)?,
                metadata: serde_json::from_str(&metadata).unwrap_or_else(|_| serde_json::json!({})),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
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

fn insert_session_facts_batch(store: &Store, rows: &[SessionFactRow<'_>]) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting session facts batch")?;
        {
            let mut stmt = tx.prepare(
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
            )?;
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
        }
        tx.commit().context("committing session facts batch")
    })
}

struct SessionFactInput {
    rowid: i64,
    session_id: String,
    source_kind: String,
    metadata_json: String,
    event_count: i64,
    user_message_count: i64,
    first_event_at: Option<String>,
    last_event_at: Option<String>,
}

struct SessionFactRow<'a> {
    session_id: String,
    source_kind: String,
    workspace_path: Option<String>,
    session_class: &'a str,
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

fn rebuild_message_provenance(store: &Store, mut progress: impl FnMut(String)) -> Result<()> {
    const BATCH_SIZE: i64 = 1_000;

    progress("clearing previous provenance rows".to_string());
    clear_projection(store, PROJECTIONS[1])?;
    progress("finding repeated message templates".to_string());
    let repeated_templates = repeated_template_hashes(store)?;
    progress("loading inherited parent messages".to_string());
    let inherited_parent_items = inherited_parent_items(store)?;
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
    let mut session_classes = HashMap::new();
    let mut last_rowid = 0i64;
    let mut processed = 0usize;
    progress(format!("classifying {processed}/{total_messages} messages"));
    loop {
        let batch = store.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT hi.rowid, hi.id, hi.session_id, hi.source_kind, hi.kind, hi.text,
                        hi.text_hash, hi.occurred_at, s.metadata_json,
                        COALESCE(sr.relationship, 'none'),
                        eso.event_id IS NOT NULL
                 FROM history_items hi
                 JOIN sessions s ON s.id = hi.session_id
                 LEFT JOIN event_session_overrides eso ON eso.event_id = hi.event_id
                 LEFT JOIN session_relationships sr
                   ON sr.session_id = COALESCE(eso.session_id, hi.session_id)
                 WHERE hi.rowid > ?1
                   AND hi.tier = 'conversation'
                   AND hi.kind IN ('user', 'assistant')
                 ORDER BY hi.rowid
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![last_rowid, BATCH_SIZE], |row| {
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
        if batch.is_empty() {
            break;
        }
        last_rowid = batch.last().expect("non-empty provenance batch").rowid;

        let batch_len = batch.len();
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
        processed += batch_len;
        progress(format!("classifying {processed}/{total_messages} messages"));
    }
    Ok(())
}

fn inherited_parent_items(store: &Store) -> Result<HashSet<(String, String, String)>> {
    store.with_conn(|conn| {
        let edges = {
            let mut stmt = conn.prepare(
                "SELECT session_id, parent_session_id
             FROM session_relationships
             WHERE relationship = 'subagent'
               AND parent_session_id IS NOT NULL",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<rusqlite::Result<Vec<(String, String)>>>()?
        };
        let parent_ids = edges
            .iter()
            .map(|(_, parent_id)| parent_id.clone())
            .collect::<HashSet<_>>();
        let mut by_parent = HashMap::<String, Vec<(String, String)>>::new();
        let mut stmt = conn.prepare(
            "SELECT kind, text_hash
             FROM history_items INDEXED BY idx_history_items_session_order
             WHERE session_id = ?1
               AND tier = 'conversation'
               AND kind IN ('user', 'assistant')",
        )?;
        for parent_id in parent_ids {
            let rows = stmt.query_map([parent_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))?;
            by_parent.insert(
                parent_id,
                rows.collect::<rusqlite::Result<Vec<(String, String)>>>()?,
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
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(item_id) DO UPDATE SET
                   session_id = excluded.session_id,
                   source_kind = excluded.source_kind,
                   authored_by = excluded.authored_by,
                   sentiment_usable = excluded.sentiment_usable,
                   rule = excluded.rule,
                   occurred_at = excluded.occurred_at",
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
    message_kind: String,
    text: String,
    text_hash: String,
    occurred_at: Option<String>,
    session_metadata: String,
    relationship: String,
    event_session_overridden: bool,
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
            status: status.map(ToOwned::to_owned),
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
        assert!(provenance_details.contains(&"clearing previous provenance rows"));
        assert!(provenance_details.contains(&"finding repeated message templates"));
        assert!(provenance_details.contains(&"loading inherited parent messages"));
        let classified = provenance_details
            .iter()
            .filter_map(|detail| {
                detail
                    .strip_prefix("classifying ")?
                    .strip_suffix(" messages")?
                    .split_once('/')?
                    .0
                    .parse::<usize>()
                    .ok()
            })
            .collect::<Vec<_>>();
        assert_eq!(classified, vec![0, 7]);
        assert!(classified.windows(2).all(|window| window[0] <= window[1]));

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
        let prior_hashes = report_refresh_prior_hashes(&store, &delta)
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
