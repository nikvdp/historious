use crate::analytics;
use crate::storage::Store;
use anyhow::Result;
use chrono::Utc;
use rusqlite::params;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub enum ReportSort {
    Tokens,
    Sessions,
    Messages,
    Duration,
}

#[derive(Debug, Clone)]
pub struct ReportOptions {
    pub since: Option<String>,
    pub project: Option<String>,
    pub sort: ReportSort,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageReport {
    pub schema: &'static str,
    pub generated_at: String,
    pub contains_raw_text: bool,
    pub filters: ReportFilters,
    pub warnings: Vec<String>,
    pub totals: ReportTotals,
    pub activity: Vec<ActivityPoint>,
    pub tokens_by_session_end_date: Vec<TokenPoint>,
    pub projects: Vec<ProjectRow>,
    pub provider_mix_by_month: Vec<MixPoint>,
    pub model_mix_by_month: Vec<MixPoint>,
    pub rhythms: Rhythms,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportFilters {
    pub since: Option<String>,
    pub project: Option<String>,
    pub timezone: &'static str,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReportTotals {
    pub sessions: u64,
    pub threads: u64,
    pub events: u64,
    pub human_messages: u64,
    pub agent_messages: u64,
    pub harness_messages: u64,
    pub first_activity_at: Option<String>,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActivityPoint {
    pub bucket: String,
    pub sessions: u64,
    pub human_messages: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenPoint {
    pub day: String,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectRow {
    pub workspace_path: String,
    pub sessions: u64,
    pub human_messages: u64,
    pub total_tokens: Option<i64>,
    pub duration_secs: i64,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MixPoint {
    pub month: String,
    pub name: String,
    pub sessions: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RhythmBucket {
    pub label: String,
    pub human_messages: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Rhythms {
    pub by_hour: Vec<RhythmBucket>,
    pub by_weekday: Vec<RhythmBucket>,
}

pub fn compute(store: &Store, options: &ReportOptions) -> Result<UsageReport> {
    let project_pattern = options.project.as_ref().map(|value| format!("%{value}%"));
    let totals = report_totals(store, options.since.as_deref(), project_pattern.as_deref())?;
    let activity = report_activity(store, options.since.as_deref(), project_pattern.as_deref())?;
    let tokens_by_session_end_date =
        report_tokens(store, options.since.as_deref(), project_pattern.as_deref())?;
    let mut projects =
        report_projects(store, options.since.as_deref(), project_pattern.as_deref())?;
    sort_projects(&mut projects, options.sort);
    let provider_mix_by_month = report_mix(
        store,
        "sf.source_kind",
        options.since.as_deref(),
        project_pattern.as_deref(),
    )?;
    let model_mix_by_month = report_mix(
        store,
        "sf.primary_model",
        options.since.as_deref(),
        project_pattern.as_deref(),
    )?;
    let rhythms = report_rhythms(store, options.since.as_deref(), project_pattern.as_deref())?;
    let mut warnings = analytics::freshness(store)?
        .into_iter()
        .filter(|status| status.stale)
        .map(|status| {
            format!(
                "{} is stale by {} event rows; run `histo lab rebuild`",
                status.name, status.new_event_rows
            )
        })
        .collect::<Vec<_>>();
    let missing_opencode: i64 = store.with_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM session_facts
             WHERE source_kind = 'opencode' AND input_tokens IS NULL",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
    })?;
    if missing_opencode > 0 {
        warnings.push(format!(
            "{missing_opencode} OpenCode sessions have no token data; run the OpenCode backfill, then `histo lab rebuild`"
        ));
    }

    Ok(UsageReport {
        schema: "historious.report.v1",
        generated_at: Utc::now().to_rfc3339(),
        contains_raw_text: false,
        filters: ReportFilters {
            since: options.since.clone(),
            project: options.project.clone(),
            timezone: "local",
        },
        warnings,
        totals,
        activity,
        tokens_by_session_end_date,
        projects,
        provider_mix_by_month,
        model_mix_by_month,
        rhythms,
    })
}

fn report_totals(
    store: &Store,
    since: Option<&str>,
    project: Option<&str>,
) -> Result<ReportTotals> {
    store.with_conn(|conn| {
        conn.query_row(
            "WITH provenance_counts AS (
               SELECT session_id,
                      SUM(authored_by = 'human') AS human_messages,
                      SUM(authored_by = 'agent') AS agent_messages,
                      SUM(authored_by = 'harness') AS harness_messages
               FROM message_provenance
               GROUP BY session_id
             )
             SELECT COUNT(*),
                    SUM(CASE WHEN session_class != 'subagent' THEN 1 ELSE 0 END),
                    COALESCE(SUM(event_count), 0),
                    MIN(first_event_at), MAX(last_event_at),
                    COALESCE(SUM(pc.human_messages), 0),
                    COALESCE(SUM(pc.agent_messages), 0),
                    COALESCE(SUM(pc.harness_messages), 0)
             FROM session_facts sf
             LEFT JOIN provenance_counts pc ON pc.session_id = sf.session_id
             WHERE (?1 IS NULL OR sf.last_event_at >= ?1)
               AND (?2 IS NULL OR sf.workspace_path LIKE ?2)",
            params![since, project],
            |row| {
                Ok(ReportTotals {
                    sessions: nonnegative(row.get(0)?),
                    threads: nonnegative(row.get::<_, Option<i64>>(1)?.unwrap_or(0)),
                    events: nonnegative(row.get(2)?),
                    first_activity_at: row.get(3)?,
                    last_activity_at: row.get(4)?,
                    human_messages: nonnegative(row.get(5)?),
                    agent_messages: nonnegative(row.get(6)?),
                    harness_messages: nonnegative(row.get(7)?),
                })
            },
        )
        .map_err(Into::into)
    })
}

fn report_activity(
    store: &Store,
    since: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<ActivityPoint>> {
    store.with_conn(|conn| {
        let mut values = BTreeMap::<String, (u64, u64)>::new();
        let bucket = "CASE WHEN first_event_at >= datetime('now', '-56 days')
                           THEN date(first_event_at, 'localtime')
                           ELSE strftime('%Y-W%W', first_event_at, 'localtime') END";
        let sql = format!(
            "SELECT {bucket}, COUNT(*) FROM session_facts sf
             WHERE first_event_at IS NOT NULL
               AND (?1 IS NULL OR last_event_at >= ?1)
               AND (?2 IS NULL OR workspace_path LIKE ?2)
             GROUP BY 1"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![since, project], |row| {
            Ok((row.get::<_, String>(0)?, nonnegative(row.get(1)?)))
        })?;
        for row in rows {
            let (label, sessions) = row?;
            values.entry(label).or_default().0 = sessions;
        }

        let mut stmt = conn.prepare(
            "SELECT CASE WHEN hi.occurred_at >= datetime('now', '-56 days')
                         THEN date(hi.occurred_at, 'localtime')
                         ELSE strftime('%Y-W%W', hi.occurred_at, 'localtime') END,
                    COUNT(*)
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human' AND hi.occurred_at IS NOT NULL
               AND (?1 IS NULL OR hi.occurred_at >= ?1)
               AND (?2 IS NULL OR sf.workspace_path LIKE ?2)
             GROUP BY 1",
        )?;
        let rows = stmt.query_map(params![since, project], |row| {
            Ok((row.get::<_, String>(0)?, nonnegative(row.get(1)?)))
        })?;
        for row in rows {
            let (label, messages) = row?;
            values.entry(label).or_default().1 = messages;
        }
        Ok(values
            .into_iter()
            .map(|(bucket, (sessions, human_messages))| ActivityPoint {
                bucket,
                sessions,
                human_messages,
            })
            .collect())
    })
}

fn report_tokens(
    store: &Store,
    since: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<TokenPoint>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT date(last_event_at, 'localtime'),
                    COALESCE(SUM(input_tokens), 0),
                    COALESCE(SUM(cached_input_tokens), 0),
                    COALESCE(SUM(output_tokens), 0)
             FROM session_facts sf
             WHERE last_event_at IS NOT NULL
               AND (?1 IS NULL OR last_event_at >= ?1)
               AND (?2 IS NULL OR workspace_path LIKE ?2)
             GROUP BY 1 ORDER BY 1",
        )?;
        let rows = stmt.query_map(params![since, project], |row| {
            Ok(TokenPoint {
                day: row.get(0)?,
                input_tokens: row.get::<_, i64>(1)?.max(0),
                cached_input_tokens: row.get::<_, i64>(2)?.max(0),
                output_tokens: row.get::<_, i64>(3)?.max(0),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn report_projects(
    store: &Store,
    since: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<ProjectRow>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "WITH human_counts AS (
               SELECT session_id, COUNT(*) AS human_messages
               FROM message_provenance
               WHERE authored_by = 'human'
               GROUP BY session_id
             )
             SELECT COALESCE(sf.workspace_path, 'unknown'), COUNT(*),
                    COALESCE(SUM(hc.human_messages), 0),
                    SUM(CASE WHEN input_tokens IS NOT NULL OR cached_input_tokens IS NOT NULL
                                  OR output_tokens IS NOT NULL
                             THEN COALESCE(input_tokens, 0) + COALESCE(cached_input_tokens, 0)
                                  + COALESCE(output_tokens, 0) END),
                    COALESCE(SUM(duration_secs), 0), MAX(last_event_at)
             FROM session_facts sf
             LEFT JOIN human_counts hc ON hc.session_id = sf.session_id
             WHERE (?1 IS NULL OR last_event_at >= ?1)
               AND (?2 IS NULL OR workspace_path LIKE ?2)
             GROUP BY sf.workspace_path",
        )?;
        let rows = stmt.query_map(params![since, project], |row| {
            Ok(ProjectRow {
                workspace_path: row.get(0)?,
                sessions: nonnegative(row.get(1)?),
                human_messages: nonnegative(row.get(2)?),
                total_tokens: row.get::<_, Option<i64>>(3)?.map(|value| value.max(0)),
                duration_secs: row.get::<_, i64>(4)?.max(0),
                last_activity_at: row.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn report_mix(
    store: &Store,
    dimension: &str,
    since: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<MixPoint>> {
    store.with_conn(|conn| {
        let sql = format!(
            "SELECT strftime('%Y-%m', sf.first_event_at, 'localtime'), {dimension}, COUNT(*)
             FROM session_facts sf
             WHERE sf.first_event_at IS NOT NULL AND {dimension} IS NOT NULL
               AND trim({dimension}) != ''
               AND (?1 IS NULL OR sf.last_event_at >= ?1)
               AND (?2 IS NULL OR sf.workspace_path LIKE ?2)
             GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![since, project], |row| {
            Ok(MixPoint {
                month: row.get(0)?,
                name: row.get(1)?,
                sessions: nonnegative(row.get(2)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn report_rhythms(store: &Store, since: Option<&str>, project: Option<&str>) -> Result<Rhythms> {
    store.with_conn(|conn| {
        let query = |format: &str| -> Result<Vec<RhythmBucket>> {
            let sql = format!(
                "SELECT strftime('{format}', hi.occurred_at, 'localtime'), COUNT(*)
                 FROM message_provenance p
                 JOIN history_items hi ON hi.id = p.item_id
                 JOIN session_facts sf ON sf.session_id = p.session_id
                 WHERE p.authored_by = 'human' AND hi.occurred_at IS NOT NULL
                   AND (?1 IS NULL OR hi.occurred_at >= ?1)
                   AND (?2 IS NULL OR sf.workspace_path LIKE ?2)
                 GROUP BY 1 ORDER BY 1"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![since, project], |row| {
                Ok(RhythmBucket {
                    label: row.get(0)?,
                    human_messages: nonnegative(row.get(1)?),
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        };
        Ok(Rhythms {
            by_hour: query("%H")?,
            by_weekday: query("%w")?,
        })
    })
}

fn sort_projects(rows: &mut [ProjectRow], sort: ReportSort) {
    rows.sort_by(|left, right| match sort {
        ReportSort::Tokens => right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| right.sessions.cmp(&left.sessions)),
        ReportSort::Sessions => right.sessions.cmp(&left.sessions),
        ReportSort::Messages => right.human_messages.cmp(&left.human_messages),
        ReportSort::Duration => right.duration_secs.cmp(&left.duration_secs),
    });
}

fn nonnegative(value: i64) -> u64 {
    value.max(0) as u64
}

pub fn render_terminal(report: &UsageReport) -> String {
    let mut out = String::new();
    out.push_str("Historious usage report\n\n");
    out.push_str(&format!(
        "Sessions {} · Threads {} · Events {} · Human messages {} · Agent {} · Harness {}\n",
        report.totals.sessions,
        report.totals.threads,
        report.totals.events,
        report.totals.human_messages,
        report.totals.agent_messages,
        report.totals.harness_messages
    ));
    out.push_str(&format!(
        "Span {} → {} · local-time buckets\n",
        report
            .totals
            .first_activity_at
            .as_deref()
            .unwrap_or("unknown"),
        report
            .totals
            .last_activity_at
            .as_deref()
            .unwrap_or("unknown")
    ));
    for warning in &report.warnings {
        out.push_str(&format!("Note: {warning}\n"));
    }

    out.push_str("\nActivity (older weeks, recent days)\n");
    for point in report.activity.iter().rev().take(16).rev() {
        out.push_str(&format!(
            "  {:<10} sessions {:>5}  human messages {:>6}\n",
            point.bucket, point.sessions, point.human_messages
        ));
    }
    out.push_str("\nTokens by session end date\n");
    for point in report
        .tokens_by_session_end_date
        .iter()
        .rev()
        .take(14)
        .rev()
    {
        out.push_str(&format!(
            "  {}  input {:>12}  cached {:>12}  output {:>10}\n",
            point.day, point.input_tokens, point.cached_input_tokens, point.output_tokens
        ));
    }
    out.push_str(
        "  Session-granularity caveat: multi-day usage is attributed to the session end date.\n",
    );

    out.push_str("\nProjects\n");
    out.push_str("  sessions  messages       tokens   duration(h)  workspace\n");
    for row in report.projects.iter().take(20) {
        out.push_str(&format!(
            "  {:>8}  {:>8}  {:>11}  {:>11.1}  {}\n",
            row.sessions,
            row.human_messages,
            row.total_tokens
                .map(|value| value.to_string())
                .unwrap_or_else(|| "—".to_string()),
            row.duration_secs as f64 / 3600.0,
            row.workspace_path
        ));
    }

    out.push_str("\nProvider mix by month\n");
    for point in report.provider_mix_by_month.iter().rev().take(20).rev() {
        out.push_str(&format!(
            "  {}  {:<16} {}\n",
            point.month, point.name, point.sessions
        ));
    }
    out.push_str("\nModels by month\n");
    for point in report.model_mix_by_month.iter().rev().take(20).rev() {
        out.push_str(&format!(
            "  {}  {:<28} {}\n",
            point.month, point.name, point.sessions
        ));
    }
    out.push_str("\nRhythms (human messages)\n  hour  ");
    for bucket in &report.rhythms.by_hour {
        out.push_str(&format!("{}:{} ", bucket.label, bucket.human_messages));
    }
    out.push_str("\n  weekday  ");
    for bucket in &report.rhythms.by_weekday {
        out.push_str(&format!("{}:{} ", bucket.label, bucket.human_messages));
    }
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_computes_aggregate_sections_and_ignores_null_rhythm_times() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO session_facts
                      (session_id, source_kind, workspace_path, session_class, models_json,
                       primary_model, input_tokens, cached_input_tokens, output_tokens,
                       event_count, user_message_count, first_event_at, last_event_at, duration_secs)
                    VALUES
                      ('session_a', 'codex', '/repo/a', 'interactive', '["gpt-5.4"]',
                       'gpt-5.4', 100, 20, 30, 2, 2,
                       '2026-06-01T01:00:00Z', '2026-06-01T01:10:00Z', 600),
                      ('session_b', 'codex', '/repo/b', 'subagent', '["gpt-5.5"]',
                       'gpt-5.5', 50, 10, 15, 1, 1,
                       '2026-06-02T02:00:00Z', '2026-06-02T02:05:00Z', 300);

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('item_human', 'event_1', 'session_a', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'hello', 'hash_1', '2026-06-01T01:02:00Z',
                       1, 'required', '{}', 'item_hash_1'),
                      ('item_null', 'event_2', 'session_a', 'source', 'machine', 'codex', 1, 0,
                       'conversation', 'user', 'no timestamp', 'hash_2', NULL,
                       1, 'required', '{}', 'item_hash_2'),
                      ('item_agent', 'event_3', 'session_b', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'delegated', 'hash_3', '2026-06-02T02:01:00Z',
                       1, 'required', '{}', 'item_hash_3');

                    INSERT INTO message_provenance
                      (item_id, session_id, source_kind, authored_by, sentiment_usable, rule, occurred_at)
                    VALUES
                      ('item_human', 'session_a', 'codex', 'human', 'yes', 'default.human', '2026-06-01T01:02:00Z'),
                      ('item_null', 'session_a', 'codex', 'human', 'yes', 'default.human', NULL),
                      ('item_agent', 'session_b', 'codex', 'agent', 'no', 'session.subagent', '2026-06-02T02:01:00Z');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert report fixtures");

        let report = compute(
            &store,
            &ReportOptions {
                since: Some("2026-06-01T00:00:00+00:00".to_string()),
                project: None,
                sort: ReportSort::Tokens,
            },
        )
        .expect("compute report");

        assert!(!report.contains_raw_text);
        assert_eq!(report.totals.sessions, 2);
        assert_eq!(report.totals.threads, 1);
        assert_eq!(report.totals.events, 3);
        assert_eq!(report.totals.human_messages, 2);
        assert_eq!(report.totals.agent_messages, 1);
        assert_eq!(report.projects[0].workspace_path, "/repo/a");
        assert_eq!(report.projects[0].total_tokens, Some(150));
        assert_eq!(report.tokens_by_session_end_date.len(), 2);
        assert_eq!(
            report
                .rhythms
                .by_hour
                .iter()
                .map(|bucket| bucket.human_messages)
                .sum::<u64>(),
            1
        );
        assert_eq!(report.provider_mix_by_month[0].name, "codex");
        assert_eq!(report.model_mix_by_month.len(), 2);

        let filtered = compute(
            &store,
            &ReportOptions {
                since: None,
                project: Some("/repo/b".to_string()),
                sort: ReportSort::Messages,
            },
        )
        .expect("compute filtered report");
        assert_eq!(filtered.totals.sessions, 1);
        assert_eq!(filtered.totals.human_messages, 0);
    }
}
