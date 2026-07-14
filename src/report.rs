use crate::analytics;
use crate::cli::{styled_role, StyleRole};
use crate::provenance;
use crate::storage::Store;
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, Local, NaiveDate, Utc};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const PROJECT_INSIGHT_LIMIT: usize = 2;
const MIN_CHANGE_SAMPLE: u64 = 20;
const MIN_CHANGE_ABSOLUTE: u64 = 5;
const MIN_CHANGE_PERCENT: f64 = 20.0;
const MIN_DAYPART_MESSAGES: u64 = 50;
const MIN_DAYPART_LIFT: f64 = 1.25;
const MIN_DAYPART_SHARE_GAP: f64 = 5.0;

#[derive(Debug, Clone, Copy)]
pub enum ReportSort {
    Tokens,
    Sessions,
    Messages,
    Duration,
}

#[derive(Debug, Clone)]
pub struct ReportOptions {
    pub after: Option<String>,
    pub before: Option<String>,
    pub project: Option<String>,
    pub sort: ReportSort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageReport {
    pub schema: String,
    pub generated_at: String,
    pub contains_raw_text: bool,
    pub filters: ReportFilters,
    pub warnings: Vec<String>,
    pub totals: ReportTotals,
    pub activity: Vec<ActivityPoint>,
    #[serde(default)]
    pub comparisons: Vec<ComparisonWindow>,
    pub tokens_by_session_end_date: Vec<TokenPoint>,
    pub projects: Vec<ProjectRow>,
    pub provider_mix_by_month: Vec<MixPoint>,
    pub model_mix_by_month: Vec<MixPoint>,
    pub rhythms: Rhythms,
    #[serde(default)]
    pub dayparts: Vec<DaypartBucket>,
    #[serde(default)]
    pub daypart_insights: Vec<DaypartInsight>,
    pub frequencies: FrequencySection,
    pub topics: Option<TopicSection>,
    pub sentiment: Option<SentimentSection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportFilters {
    #[serde(alias = "since")]
    pub after: Option<String>,
    #[serde(default)]
    pub before: Option<String>,
    pub project: Option<String>,
    pub timezone: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReportTotals {
    pub sessions: u64,
    pub threads: u64,
    pub events: u64,
    #[serde(alias = "human_messages")]
    pub human_turns: u64,
    #[serde(default)]
    pub assistant_turns: u64,
    #[serde(alias = "agent_messages")]
    pub delegated_turns: u64,
    #[serde(alias = "harness_messages")]
    pub harness_turns: u64,
    pub first_activity_at: Option<String>,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityPoint {
    pub bucket: String,
    pub sessions: u64,
    #[serde(alias = "human_messages")]
    pub human_turns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeInsight {
    pub metric: String,
    pub subject: Option<String>,
    pub current: u64,
    pub previous: u64,
    pub change_percent: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonWindow {
    pub days: u16,
    pub current_start: String,
    pub current_end: String,
    pub previous_start: String,
    pub previous_end: String,
    pub metrics: Vec<ChangeInsight>,
    pub project_changes: Vec<ChangeInsight>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportWindow {
    Default,
    Seven,
    Fourteen,
    TwentyEight,
    All,
}

impl ReportWindow {
    fn includes(self, days: u16) -> bool {
        match self {
            Self::Default => days == 7 || days == 28,
            Self::Seven => days == 7,
            Self::Fourteen => days == 14,
            Self::TwentyEight => days == 28,
            Self::All => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPoint {
    pub day: String,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRow {
    pub project: String,
    #[serde(skip)]
    workspace_path: String,
    pub sessions: u64,
    pub human_messages: u64,
    pub total_tokens: Option<i64>,
    pub duration_secs: i64,
    pub last_activity_at: Option<String>,
    #[serde(default)]
    pub terms: Vec<TermCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MixPoint {
    pub month: String,
    pub name: String,
    pub sessions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RhythmBucket {
    pub label: String,
    pub human_messages: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Rhythms {
    pub by_hour: Vec<RhythmBucket>,
    pub by_weekday: Vec<RhythmBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaypartBucket {
    pub label: String,
    pub human_messages: u64,
    pub share_percent: f64,
    pub baseline_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaypartInsight {
    pub label: String,
    pub human_messages: u64,
    pub share_percent: f64,
    pub baseline_percent: f64,
    pub lift: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FrequencySection {
    pub unigrams: Vec<TermCount>,
    pub bigrams: Vec<TermCount>,
    pub trigrams: Vec<TermCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermCount {
    pub term: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSection {
    pub version: String,
    pub model_id: String,
    pub assigned_messages: u64,
    pub corpus_messages: u64,
    pub topics: Vec<TopicSummary>,
    pub by_month: Vec<TopicPeriod>,
    pub by_project: Vec<TopicProject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSummary {
    pub topic_id: i64,
    pub label: String,
    pub messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPeriod {
    pub month: String,
    pub topic_id: i64,
    pub label: String,
    pub messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicProject {
    pub project: String,
    pub topic_id: i64,
    pub label: String,
    pub messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentSection {
    pub annotator_version: String,
    pub model: String,
    pub annotated_messages: u64,
    pub corpus_messages: u64,
    pub by_week: Vec<SentimentPeriod>,
    pub by_project: Vec<SentimentProject>,
    pub by_hour: Vec<SentimentHour>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentPeriod {
    pub week: String,
    pub axis: String,
    pub average: f64,
    pub messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentProject {
    pub project: String,
    pub axis: String,
    pub average: f64,
    pub messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentHour {
    pub hour: String,
    pub axis: String,
    pub average: f64,
    pub messages: u64,
}

pub fn compute(store: &Store, options: &ReportOptions) -> Result<UsageReport> {
    if options.after.is_none() && options.before.is_none() && options.project.is_none() {
        let mut report = read_snapshot(store)?.context(
            "report snapshot is unavailable; run `histo lab rebuild` before `histo report`",
        )?;
        sort_projects(&mut report.projects, options.sort);
        let freshness = analytics::report_snapshot_freshness(store)?;
        if freshness.stale {
            report.warnings.push(format!(
                "report snapshot is {} old and stale by {} event rows; run `histo lab rebuild`",
                snapshot_age(&report.generated_at),
                freshness.new_event_rows
            ));
        }
        return Ok(report);
    }
    compute_live(store, options, true)
}

pub fn rebuild_snapshot(store: &Store) -> Result<()> {
    let report = compute_live(
        store,
        &ReportOptions {
            after: None,
            before: None,
            project: None,
            sort: ReportSort::Tokens,
        },
        false,
    )?;
    let report_json = serde_json::to_string(&report)?;
    store.with_conn(|conn| {
        conn.execute(
            "INSERT INTO report_snapshot (singleton, report_json, generated_at)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
               report_json = excluded.report_json,
               generated_at = excluded.generated_at",
            params![report_json, report.generated_at],
        )?;
        Ok(())
    })
}

fn read_snapshot(store: &Store) -> Result<Option<UsageReport>> {
    let report_json = store.with_conn(|conn| {
        conn.query_row(
            "SELECT report_json FROM report_snapshot WHERE singleton = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
    })?;
    report_json
        .map(|json| serde_json::from_str(&json).context("reading report snapshot JSON"))
        .transpose()
}

fn snapshot_age(generated_at: &str) -> String {
    let Ok(generated_at) = DateTime::parse_from_rfc3339(generated_at) else {
        return "an unknown amount of time".to_string();
    };
    let seconds = (Utc::now() - generated_at.with_timezone(&Utc))
        .num_seconds()
        .max(0);
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3_599 => format!("{}m", seconds / 60),
        3_600..=86_399 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

fn compute_live(
    store: &Store,
    options: &ReportOptions,
    include_projection_warnings: bool,
) -> Result<UsageReport> {
    let project_pattern = options.project.as_ref().map(|value| format!("%{value}%"));
    let totals = report_totals(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let activity = report_activity(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let comparisons = report_comparisons(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let tokens_by_session_end_date = report_tokens(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let mut projects = report_projects(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    sort_projects(&mut projects, options.sort);
    let provider_mix_by_month = report_mix(
        store,
        "sf.source_kind",
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let model_mix_by_month = report_mix(
        store,
        "sf.primary_model",
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let rhythms = report_rhythms(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let dayparts = report_dayparts(&rhythms);
    let daypart_insights = select_daypart_insights(&dayparts);
    let (frequencies, project_terms) = report_frequencies(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    for project in &mut projects {
        project.terms = project_terms
            .get(&project.workspace_path)
            .cloned()
            .unwrap_or_default();
    }
    let (topics, topic_warning) = report_topics(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let (sentiment, sentiment_warning) = report_sentiment(
        store,
        options.after.as_deref(),
        options.before.as_deref(),
        project_pattern.as_deref(),
    )?;
    let mut warnings = if include_projection_warnings {
        analytics::freshness(store)?
            .into_iter()
            .filter(|status| status.stale)
            .map(|status| {
                format!(
                    "{} is stale by {} event rows; run `histo lab rebuild`",
                    status.name, status.new_event_rows
                )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
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
    if let Some(warning) = topic_warning {
        warnings.push(warning);
    }
    if let Some(warning) = sentiment_warning {
        warnings.push(warning);
    }

    Ok(UsageReport {
        schema: "historious.report.v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        contains_raw_text: false,
        filters: ReportFilters {
            after: options.after.clone(),
            before: options.before.clone(),
            project: options.project.as_deref().map(project_label),
            timezone: "local".to_string(),
        },
        warnings,
        totals,
        activity,
        comparisons,
        tokens_by_session_end_date,
        projects,
        provider_mix_by_month,
        model_mix_by_month,
        rhythms,
        dayparts,
        daypart_insights,
        frequencies,
        topics,
        sentiment,
    })
}

fn sentiment_axes_sql() -> String {
    crate::annotate::SENTIMENT_AXES
        .iter()
        .map(|axis| format!("'{axis}'"))
        .collect::<Vec<_>>()
        .join(",")
}

fn report_sentiment(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<(Option<SentimentSection>, Option<String>)> {
    let corpus_messages: i64 = store.with_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM message_provenance
             WHERE authored_by = 'human' AND sentiment_usable IN ('yes', 'strip_wrapper')",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
    })?;
    let axes = sentiment_axes_sql();
    let complete = store.with_conn(|conn| {
        let sql = format!(
            "SELECT a.annotator_version, MIN(a.model), COUNT(DISTINCT a.item_id), MAX(a.annotated_at)
             FROM message_annotations a
             JOIN message_provenance p ON p.item_id = a.item_id
             WHERE p.authored_by = 'human' AND p.sentiment_usable IN ('yes', 'strip_wrapper')
               AND a.axis IN ({axes})
             GROUP BY a.annotator_version
             HAVING COUNT(*) = ?1 AND COUNT(DISTINCT a.item_id) = ?2
             ORDER BY MAX(a.annotated_at) DESC
             LIMIT 1"
        );
        conn.query_row(
            &sql,
            params![
                corpus_messages * crate::annotate::SENTIMENT_AXES.len() as i64,
                corpus_messages
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(Into::into)
    })?;
    let Some((version, model, annotated_messages)) = complete else {
        let latest = store.with_conn(|conn| {
            let sql = format!(
                "SELECT annotator_version, COUNT(DISTINCT item_id)
                 FROM message_annotations WHERE axis IN ({axes})
                 GROUP BY annotator_version ORDER BY MAX(annotated_at) DESC LIMIT 1"
            );
            conn.query_row(&sql, [], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()
            .map_err(Into::into)
        })?;
        let warning = latest.map_or_else(
            || "sentiment is unavailable; run `histo lab annotate`".to_string(),
            |(version, count)| {
                format!(
                    "sentiment version {version} has {count} of {corpus_messages} messages; resume `histo lab annotate`"
                )
            },
        );
        return Ok((None, Some(warning)));
    };
    let by_week = sentiment_periods(store, &version, after, before, project, "%Y-%W", "week")?
        .into_iter()
        .map(|(week, axis, average, messages)| SentimentPeriod {
            week,
            axis,
            average,
            messages,
        })
        .collect();
    let by_hour = sentiment_periods(store, &version, after, before, project, "%H", "hour")?
        .into_iter()
        .map(|(hour, axis, average, messages)| SentimentHour {
            hour,
            axis,
            average,
            messages,
        })
        .collect();
    let by_project = store.with_conn(|conn| {
        let sql = format!(
            "SELECT COALESCE(sf.workspace_path, 'unknown'), a.axis, AVG(a.score), COUNT(*)
             FROM message_annotations a
             JOIN history_items hi ON hi.id = a.item_id
             JOIN session_facts sf ON sf.session_id = hi.session_id
             WHERE a.annotator_version = ?1 AND a.axis IN ({})
               AND (?2 IS NULL OR hi.occurred_at >= ?2)
               AND (?3 IS NULL OR hi.occurred_at < ?3)
               AND (?4 IS NULL OR sf.workspace_path LIKE ?4)
             GROUP BY sf.workspace_path, a.axis
             ORDER BY COUNT(*) DESC, sf.workspace_path, a.axis",
            sentiment_axes_sql()
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![version, after, before, project], |row| {
            Ok(SentimentProject {
                project: project_label(&row.get::<_, String>(0)?),
                axis: row.get(1)?,
                average: row.get(2)?,
                messages: nonnegative(row.get(3)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    Ok((
        Some(SentimentSection {
            annotator_version: version,
            model,
            annotated_messages: nonnegative(annotated_messages),
            corpus_messages: nonnegative(corpus_messages),
            by_week,
            by_project,
            by_hour,
        }),
        None,
    ))
}

fn sentiment_periods(
    store: &Store,
    version: &str,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
    format: &str,
    label: &str,
) -> Result<Vec<(String, String, f64, u64)>> {
    store.with_conn(|conn| {
        let sql = format!(
            "SELECT strftime('{format}', hi.occurred_at, 'localtime'), a.axis,
                    AVG(a.score), COUNT(*)
             FROM message_annotations a
             JOIN history_items hi ON hi.id = a.item_id
             JOIN session_facts sf ON sf.session_id = hi.session_id
             WHERE a.annotator_version = ?1 AND a.axis IN ({})
               AND hi.occurred_at IS NOT NULL
               AND (?2 IS NULL OR hi.occurred_at >= ?2)
               AND (?3 IS NULL OR hi.occurred_at < ?3)
               AND (?4 IS NULL OR sf.workspace_path LIKE ?4)
             GROUP BY 1, a.axis ORDER BY 1, a.axis",
            sentiment_axes_sql()
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![version, after, before, project], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                nonnegative(row.get(3)?),
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|error| anyhow::anyhow!("reading sentiment by {label}: {error}"))
    })
}

fn report_topics(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<(Option<TopicSection>, Option<String>)> {
    let Some((version, model_id, corpus_messages, selected_k, silhouette)) = store.with_conn(|conn| {
        conn.query_row(
            "SELECT version, model_id, item_count, selected_k, silhouette_score
             FROM topic_runs
             WHERE status = 'completed'
             ORDER BY completed_at DESC
             LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, f64>(4)?,
                ))
            },
        )
        .optional()
        .map_err(Into::into)
    })?
    else {
        return Ok((None, Some("topics are unavailable; run `histo lab topics cluster` and `histo lab topics label`".to_string())));
    };
    if silhouette < crate::topics::MIN_TOPIC_SILHOUETTE {
        return Ok((None, None));
    }
    let labeled: i64 = store.with_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM topics WHERE version = ?1 AND label IS NOT NULL",
            params![version],
            |row| row.get(0),
        )
        .map_err(Into::into)
    })?;
    if labeled != selected_k {
        return Ok((
            None,
            Some(format!(
                "topic version {version} has {labeled} of {selected_k} labels; run `histo lab topics label`"
            )),
        ));
    }
    let topics = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT a.topic_id, t.label, COUNT(*)
             FROM topic_assignments a
             JOIN topics t ON t.version = a.version AND t.topic_id = a.topic_id
             JOIN history_items hi ON hi.id = a.item_id
             JOIN session_facts sf ON sf.session_id = hi.session_id
             WHERE a.version = ?1
               AND (?2 IS NULL OR hi.occurred_at >= ?2)
               AND (?3 IS NULL OR hi.occurred_at < ?3)
               AND (?4 IS NULL OR sf.workspace_path LIKE ?4)
             GROUP BY a.topic_id, t.label
             ORDER BY COUNT(*) DESC, a.topic_id",
        )?;
        let rows = stmt.query_map(params![version, after, before, project], |row| {
            Ok(TopicSummary {
                topic_id: row.get(0)?,
                label: row.get(1)?,
                messages: nonnegative(row.get(2)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    let by_month = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT strftime('%Y-%m', hi.occurred_at, 'localtime'), a.topic_id, t.label, COUNT(*)
             FROM topic_assignments a
             JOIN topics t ON t.version = a.version AND t.topic_id = a.topic_id
             JOIN history_items hi ON hi.id = a.item_id
             JOIN session_facts sf ON sf.session_id = hi.session_id
             WHERE a.version = ?1 AND hi.occurred_at IS NOT NULL
               AND (?2 IS NULL OR hi.occurred_at >= ?2)
               AND (?3 IS NULL OR hi.occurred_at < ?3)
               AND (?4 IS NULL OR sf.workspace_path LIKE ?4)
             GROUP BY 1, a.topic_id, t.label ORDER BY 1, COUNT(*) DESC",
        )?;
        let rows = stmt.query_map(params![version, after, before, project], |row| {
            Ok(TopicPeriod {
                month: row.get(0)?,
                topic_id: row.get(1)?,
                label: row.get(2)?,
                messages: nonnegative(row.get(3)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    let by_project = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT COALESCE(sf.workspace_path, 'unknown'), a.topic_id, t.label, COUNT(*)
             FROM topic_assignments a
             JOIN topics t ON t.version = a.version AND t.topic_id = a.topic_id
             JOIN history_items hi ON hi.id = a.item_id
             JOIN session_facts sf ON sf.session_id = hi.session_id
             WHERE a.version = ?1
               AND (?2 IS NULL OR hi.occurred_at >= ?2)
               AND (?3 IS NULL OR hi.occurred_at < ?3)
               AND (?4 IS NULL OR sf.workspace_path LIKE ?4)
             GROUP BY sf.workspace_path, a.topic_id, t.label
             ORDER BY COUNT(*) DESC, sf.workspace_path, a.topic_id",
        )?;
        let rows = stmt.query_map(params![version, after, before, project], |row| {
            Ok(TopicProject {
                project: project_label(&row.get::<_, String>(0)?),
                topic_id: row.get(1)?,
                label: row.get(2)?,
                messages: nonnegative(row.get(3)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    let assigned_messages = topics.iter().map(|topic| topic.messages).sum();
    Ok((
        Some(TopicSection {
            version,
            model_id,
            assigned_messages,
            corpus_messages: nonnegative(corpus_messages),
            topics,
            by_month,
            by_project,
        }),
        None,
    ))
}

fn report_frequencies(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<(FrequencySection, HashMap<String, Vec<TermCount>>)> {
    let messages = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT hi.text, p.sentiment_usable, p.rule,
                    COALESCE(sf.workspace_path, 'unknown')
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human'
               AND p.sentiment_usable IN ('yes', 'strip_wrapper')
               AND (?1 IS NULL OR hi.occurred_at >= ?1)
               AND (?2 IS NULL OR hi.occurred_at < ?2)
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)",
        )?;
        let rows = stmt.query_map(params![after, before, project], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;

    let stopwords = english_stopwords();
    let mut unigrams = HashMap::new();
    let mut bigrams = HashMap::new();
    let mut trigrams = HashMap::new();
    let project_noise = project_noise_words();
    let mut project_unigrams = HashMap::<String, HashMap<String, u64>>::new();
    for (text, usable, rule, workspace_path) in messages {
        let text = if usable == "strip_wrapper" {
            provenance::strip_human_wrapper(&text, &rule)
        } else {
            text
        };
        let tokens = tokenize_frequency_text(&text);
        let mut message_unigrams = HashMap::new();
        let mut project_message_unigrams = HashMap::new();
        let mut message_bigrams = HashMap::new();
        let mut message_trigrams = HashMap::new();
        for token in &tokens {
            if token.len() > 1 && !stopwords.contains(token.as_str()) {
                increment_capped(&mut unigrams, &mut message_unigrams, token.clone());
            }
            if token.len() > 2
                && !stopwords.contains(token.as_str())
                && !project_noise.contains(token.as_str())
            {
                increment_capped(
                    project_unigrams.entry(workspace_path.clone()).or_default(),
                    &mut project_message_unigrams,
                    token.clone(),
                );
            }
        }
        for window in tokens.windows(2) {
            if window
                .iter()
                .any(|token| !stopwords.contains(token.as_str()))
            {
                increment_capped(&mut bigrams, &mut message_bigrams, window.join(" "));
            }
        }
        for window in tokens.windows(3) {
            if window
                .iter()
                .any(|token| !stopwords.contains(token.as_str()))
            {
                increment_capped(&mut trigrams, &mut message_trigrams, window.join(" "));
            }
        }
    }
    Ok((
        FrequencySection {
            unigrams: top_terms(unigrams, 3, 20),
            bigrams: top_terms(bigrams, 3, 20),
            trigrams: top_terms(trigrams, 3, 20),
        },
        distinctive_project_terms(project_unigrams),
    ))
}

fn project_noise_words() -> HashSet<&'static str> {
    [
        "agent", "branch", "check", "code", "continue", "done", "fix", "get", "good",
        "issue", "just", "look", "make", "need", "okay", "please", "proceed", "sure",
        "thing", "things", "use", "want", "work", "yes",
    ]
    .into_iter()
    .collect()
}

fn distinctive_project_terms(
    counts: HashMap<String, HashMap<String, u64>>,
) -> HashMap<String, Vec<TermCount>> {
    let project_count = counts.len().max(1) as f64;
    let mut document_frequency = HashMap::<String, usize>::new();
    for terms in counts.values() {
        for term in terms.keys() {
            *document_frequency.entry(term.clone()).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .map(|(project, counts)| {
            let mut scored = counts
                .into_iter()
                .filter(|(_, count)| *count >= 3)
                .map(|(term, count)| {
                    let frequency = document_frequency[&term] as f64;
                    let score = count as f64
                        * ((1.0 + project_count) / (1.0 + frequency)).ln();
                    (term, count, score)
                })
                .collect::<Vec<_>>();
            scored.sort_by(|left, right| {
                right
                    .2
                    .total_cmp(&left.2)
                    .then_with(|| right.1.cmp(&left.1))
                    .then_with(|| left.0.cmp(&right.0))
            });
            scored.truncate(3);
            (
                project,
                scored
                    .into_iter()
                    .map(|(term, count, _)| TermCount { term, count })
                    .collect(),
            )
        })
        .collect()
}

pub(crate) fn tokenize_frequency_text(text: &str) -> Vec<String> {
    let mut outside_code = String::new();
    let mut in_fence = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if !in_fence {
            outside_code.push_str(line);
            outside_code.push(' ');
        }
    }
    let without_paths = outside_code
        .split_whitespace()
        .filter(|chunk| {
            let chunk = chunk.trim_matches(|character: char| {
                matches!(character, '(' | ')' | '[' | ']' | '<' | '>' | ',' | ';')
            });
            !chunk.starts_with("http://")
                && !chunk.starts_with("https://")
                && !chunk.starts_with('/')
                && !chunk.starts_with("~/")
                && !chunk.contains("://")
        })
        .collect::<Vec<_>>()
        .join(" ");
    without_paths
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || character == '\'' {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .filter(|token| !token.chars().any(|character| character.is_ascii_digit()))
        .map(ToOwned::to_owned)
        .collect()
}

fn increment_capped(
    totals: &mut HashMap<String, u64>,
    message_counts: &mut HashMap<String, u8>,
    term: String,
) {
    let count = message_counts.entry(term.clone()).or_default();
    if *count < 3 {
        *totals.entry(term).or_default() += 1;
        *count += 1;
    }
}

fn top_terms(counts: HashMap<String, u64>, minimum: u64, limit: usize) -> Vec<TermCount> {
    let mut terms = counts
        .into_iter()
        .filter(|(_, count)| *count >= minimum)
        .map(|(term, count)| TermCount { term, count })
        .collect::<Vec<_>>();
    terms.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.term.cmp(&right.term))
    });
    terms.truncate(limit);
    terms
}

pub(crate) fn english_stopwords() -> HashSet<&'static str> {
    [
        "a", "all", "also", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can",
        "could", "do", "for", "from", "had", "has", "have", "he", "her", "here", "him", "his",
        "how", "i", "if", "in", "into", "is", "it", "its", "just", "me", "my", "no", "not", "of",
        "on", "or", "our", "please", "she", "should", "so", "that", "the", "their", "them", "then",
        "there", "these", "they", "this", "to", "up", "us", "was", "we", "were", "what", "when",
        "where", "which", "who", "why", "will", "with", "would", "you", "your",
    ]
    .into_iter()
    .collect()
}

fn report_totals(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<ReportTotals> {
    store.with_conn(|conn| {
        conn.query_row(
            "WITH provenance_counts AS (
               SELECT session_id,
                      SUM(authored_by = 'human') AS human_messages,
                      SUM(authored_by = 'assistant') AS assistant_messages,
                      SUM(authored_by = 'agent') AS delegated_messages,
                      SUM(authored_by = 'harness') AS harness_messages
               FROM message_provenance
               WHERE (?1 IS NULL OR occurred_at >= ?1)
                 AND (?2 IS NULL OR occurred_at < ?2)
               GROUP BY session_id
             )
             SELECT COUNT(*),
                    SUM(CASE WHEN session_class != 'subagent' THEN 1 ELSE 0 END),
                    COALESCE(SUM(event_count), 0),
                    MIN(first_event_at), MAX(last_event_at),
                    COALESCE(SUM(pc.human_messages), 0),
                    COALESCE(SUM(pc.assistant_messages), 0),
                    COALESCE(SUM(pc.delegated_messages), 0),
                    COALESCE(SUM(pc.harness_messages), 0)
             FROM session_facts sf
             LEFT JOIN provenance_counts pc ON pc.session_id = sf.session_id
             WHERE (?1 IS NULL OR sf.last_event_at >= ?1)
               AND (?2 IS NULL OR sf.first_event_at < ?2)
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)",
            params![after, before, project],
            |row| {
                Ok(ReportTotals {
                    sessions: nonnegative(row.get(0)?),
                    threads: nonnegative(row.get::<_, Option<i64>>(1)?.unwrap_or(0)),
                    events: nonnegative(row.get(2)?),
                    first_activity_at: row.get(3)?,
                    last_activity_at: row.get(4)?,
                    human_turns: nonnegative(row.get(5)?),
                    assistant_turns: nonnegative(row.get(6)?),
                    delegated_turns: nonnegative(row.get(7)?),
                    harness_turns: nonnegative(row.get(8)?),
                })
            },
        )
        .map_err(Into::into)
    })
}

fn report_activity(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<ActivityPoint>> {
    store.with_conn(|conn| {
        let mut values = BTreeMap::<String, (u64, u64)>::new();
        let today = Local::now().date_naive();
        let start = today - ChronoDuration::days(364);
        for offset in 0..365 {
            values.insert((start + ChronoDuration::days(offset)).to_string(), (0, 0));
        }
        let mut stmt = conn.prepare(
            "SELECT date(first_event_at, 'localtime'), COUNT(*) FROM session_facts sf
             WHERE first_event_at IS NOT NULL
               AND (?1 IS NULL OR first_event_at >= ?1)
               AND (?2 IS NULL OR first_event_at < ?2)
               AND (?3 IS NULL OR workspace_path LIKE ?3)
               AND date(first_event_at, 'localtime') >= ?4
             GROUP BY 1"
        )?;
        let rows = stmt.query_map(params![after, before, project, start.to_string()], |row| {
            Ok((row.get::<_, String>(0)?, nonnegative(row.get(1)?)))
        })?;
        for row in rows {
            let (label, sessions) = row?;
            values.entry(label).or_default().0 = sessions;
        }

        let mut stmt = conn.prepare(
            "SELECT date(hi.occurred_at, 'localtime'), COUNT(*)
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human' AND hi.occurred_at IS NOT NULL
               AND (?1 IS NULL OR hi.occurred_at >= ?1)
               AND (?2 IS NULL OR hi.occurred_at < ?2)
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
               AND date(hi.occurred_at, 'localtime') >= ?4
             GROUP BY 1",
        )?;
        let rows = stmt.query_map(params![after, before, project, start.to_string()], |row| {
            Ok((row.get::<_, String>(0)?, nonnegative(row.get(1)?)))
        })?;
        for row in rows {
            let (label, messages) = row?;
            values.entry(label).or_default().1 = messages;
        }
        Ok(values
            .into_iter()
            .map(|(bucket, (sessions, human_turns))| ActivityPoint {
                bucket,
                sessions,
                human_turns,
            })
            .collect())
    })
}

fn report_comparisons(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<ComparisonWindow>> {
    if after.is_some() || before.is_some() {
        return Ok(Vec::new());
    }
    let today = Local::now().date_naive();
    let mut windows = Vec::new();
    for days in [7u16, 14, 28] {
        let current_start = today - ChronoDuration::days(i64::from(days - 1));
        let current_end = today;
        let previous_end = current_start - ChronoDuration::days(1);
        let previous_start = previous_end - ChronoDuration::days(i64::from(days - 1));
        let dates = (
            current_start.to_string(),
            current_end.to_string(),
            previous_start.to_string(),
            previous_end.to_string(),
        );
        let (current_sessions, previous_sessions, current_tokens, previous_tokens) =
            store.with_conn(|conn| {
            conn.query_row(
                "SELECT
                   COALESCE(SUM(date(last_event_at, 'localtime') BETWEEN ?1 AND ?2), 0),
                   COALESCE(SUM(date(last_event_at, 'localtime') BETWEEN ?3 AND ?4), 0),
                   COALESCE(SUM(CASE WHEN date(last_event_at, 'localtime') BETWEEN ?1 AND ?2
                                     THEN COALESCE(input_tokens, 0)
                                        + COALESCE(cached_input_tokens, 0)
                                        + COALESCE(output_tokens, 0) ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN date(last_event_at, 'localtime') BETWEEN ?3 AND ?4
                                     THEN COALESCE(input_tokens, 0)
                                        + COALESCE(cached_input_tokens, 0)
                                        + COALESCE(output_tokens, 0) ELSE 0 END), 0)
                 FROM session_facts
                 WHERE (?5 IS NULL OR workspace_path LIKE ?5)",
                params![dates.0, dates.1, dates.2, dates.3, project],
                |row| {
                    Ok((
                        nonnegative(row.get(0)?),
                        nonnegative(row.get(1)?),
                        nonnegative(row.get(2)?),
                        nonnegative(row.get(3)?),
                    ))
                },
            )
            .map_err(Into::into)
        })?;
        let (current_messages, previous_messages) = store.with_conn(|conn| {
            conn.query_row(
            "SELECT
               COALESCE(SUM(date(p.occurred_at, 'localtime') BETWEEN ?1 AND ?2), 0),
               COALESCE(SUM(date(p.occurred_at, 'localtime') BETWEEN ?3 AND ?4), 0)
             FROM message_provenance p
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human'
               AND (?5 IS NULL OR sf.workspace_path LIKE ?5)",
            params![dates.0, dates.1, dates.2, dates.3, project],
            |row| Ok((nonnegative(row.get(0)?), nonnegative(row.get(1)?))),
        )
        .map_err(Into::into)
    })?;
        let metrics = [
            ("sessions", current_sessions, previous_sessions),
            ("human turns", current_messages, previous_messages),
            ("tokens", current_tokens, previous_tokens),
        ]
        .into_iter()
        .map(|(metric, current, previous)| change_row(metric, None, current, previous))
        .collect();
        let projects = store.with_conn(|conn| {
            let mut stmt = conn.prepare(
            "SELECT COALESCE(sf.workspace_path, 'unknown'),
                    SUM(date(p.occurred_at, 'localtime') BETWEEN ?1 AND ?2),
                    SUM(date(p.occurred_at, 'localtime') BETWEEN ?3 AND ?4)
             FROM message_provenance p
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human'
               AND date(p.occurred_at, 'localtime') BETWEEN ?3 AND ?2
               AND (?5 IS NULL OR sf.workspace_path LIKE ?5)
             GROUP BY sf.workspace_path",
        )?;
            let rows = stmt.query_map(params![dates.0, dates.1, dates.2, dates.3, project], |row| {
            Ok((
                row.get::<_, String>(0)?,
                nonnegative(row.get(1)?),
                nonnegative(row.get(2)?),
            ))
        })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })?;
        let mut project_ranked = Vec::new();
        for (workspace_path, current, previous) in projects {
            push_change(
                &mut project_ranked,
                "human turns",
                Some(project_label(&workspace_path)),
                current,
                previous,
            );
        }
        project_ranked.sort_by(|left, right| right.0.total_cmp(&left.0));
        windows.push(ComparisonWindow {
            days,
            current_start: dates.0,
            current_end: dates.1,
            previous_start: dates.2,
            previous_end: dates.3,
            metrics,
            project_changes: project_ranked
                .into_iter()
                .take(PROJECT_INSIGHT_LIMIT)
                .map(|(_, insight)| insight)
                .collect(),
        });
    }
    Ok(windows)
}

fn change_row(metric: &str, subject: Option<String>, current: u64, previous: u64) -> ChangeInsight {
    let change_percent = if previous == 0 {
        None
    } else {
        Some((((current as f64 - previous as f64) * 100.0) / previous as f64).round() as i64)
    };
    ChangeInsight {
        metric: metric.to_string(),
        subject,
        current,
        previous,
        change_percent,
    }
}

fn push_change(
    ranked: &mut Vec<(f64, ChangeInsight)>,
    metric: &str,
    subject: Option<String>,
    current: u64,
    previous: u64,
) {
    if current + previous < MIN_CHANGE_SAMPLE
        || current.abs_diff(previous) < MIN_CHANGE_ABSOLUTE
    {
        return;
    }
    let change = if previous == 0 {
        None
    } else {
        Some((current as f64 - previous as f64) * 100.0 / previous as f64)
    };
    if change.is_some_and(|percent| percent.abs() < MIN_CHANGE_PERCENT) {
        return;
    }
    let score = change.map_or(100.0, f64::abs) * (current + previous) as f64;
    ranked.push((
        score,
        ChangeInsight {
            metric: metric.to_string(),
            subject,
            current,
            previous,
            change_percent: change.map(|percent| percent.round() as i64),
        },
    ));
}

fn report_tokens(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
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
               AND (?2 IS NULL OR last_event_at < ?2)
               AND (?3 IS NULL OR workspace_path LIKE ?3)
             GROUP BY 1 ORDER BY 1",
        )?;
        let rows = stmt.query_map(params![after, before, project], |row| {
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
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<ProjectRow>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "WITH human_counts AS (
               SELECT session_id, COUNT(*) AS human_messages
               FROM message_provenance
               WHERE authored_by = 'human'
                 AND (?1 IS NULL OR occurred_at >= ?1)
                 AND (?2 IS NULL OR occurred_at < ?2)
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
               AND (?2 IS NULL OR first_event_at < ?2)
               AND (?3 IS NULL OR workspace_path LIKE ?3)
             GROUP BY sf.workspace_path",
        )?;
        let rows = stmt.query_map(params![after, before, project], |row| {
            let workspace_path = row.get::<_, String>(0)?;
            Ok(ProjectRow {
                project: project_label(&workspace_path),
                workspace_path,
                sessions: nonnegative(row.get(1)?),
                human_messages: nonnegative(row.get(2)?),
                total_tokens: row.get::<_, Option<i64>>(3)?.map(|value| value.max(0)),
                duration_secs: row.get::<_, i64>(4)?.max(0),
                last_activity_at: row.get(5)?,
                terms: Vec::new(),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn project_label(workspace_path: &str) -> String {
    let trimmed = workspace_path.trim_end_matches(['/', '\\']);
    trimmed
        .rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

fn report_mix(
    store: &Store,
    dimension: &str,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<MixPoint>> {
    store.with_conn(|conn| {
        let sql = format!(
            "SELECT strftime('%Y-%m', sf.first_event_at, 'localtime'), {dimension}, COUNT(*)
             FROM session_facts sf
             WHERE sf.first_event_at IS NOT NULL AND {dimension} IS NOT NULL
               AND trim({dimension}) != ''
               AND (?1 IS NULL OR sf.first_event_at >= ?1)
               AND (?2 IS NULL OR sf.first_event_at < ?2)
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
             GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![after, before, project], |row| {
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

fn report_rhythms(
    store: &Store,
    after: Option<&str>,
    before: Option<&str>,
    project: Option<&str>,
) -> Result<Rhythms> {
    store.with_conn(|conn| {
        let query = |format: &str| -> Result<Vec<RhythmBucket>> {
            let sql = format!(
                "SELECT strftime('{format}', hi.occurred_at, 'localtime'), COUNT(*)
                 FROM message_provenance p
                 JOIN history_items hi ON hi.id = p.item_id
                 JOIN session_facts sf ON sf.session_id = p.session_id
                 WHERE p.authored_by = 'human' AND hi.occurred_at IS NOT NULL
                   AND (?1 IS NULL OR hi.occurred_at >= ?1)
                   AND (?2 IS NULL OR hi.occurred_at < ?2)
                   AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
                 GROUP BY 1 ORDER BY 1"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![after, before, project], |row| {
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

fn report_dayparts(rhythms: &Rhythms) -> Vec<DaypartBucket> {
    let definitions = [
        ("night", 8u64),
        ("early morning", 4),
        ("morning", 3),
        ("afternoon", 5),
        ("evening", 4),
    ];
    let mut counts = [0u64; 5];
    for bucket in &rhythms.by_hour {
        let Ok(hour) = bucket.label.parse::<u8>() else {
            continue;
        };
        let index = match hour {
            5..=8 => 1,
            9..=11 => 2,
            12..=16 => 3,
            17..=20 => 4,
            _ => 0,
        };
        counts[index] += bucket.human_messages;
    }
    let total = counts.iter().sum::<u64>().max(1) as f64;
    definitions
        .into_iter()
        .enumerate()
        .map(|(index, (label, hours))| DaypartBucket {
            label: label.to_string(),
            human_messages: counts[index],
            share_percent: counts[index] as f64 * 100.0 / total,
            baseline_percent: hours as f64 * 100.0 / 24.0,
        })
        .collect()
}

fn select_daypart_insights(dayparts: &[DaypartBucket]) -> Vec<DaypartInsight> {
    let mut insights = dayparts
        .iter()
        .filter_map(|bucket| {
            let lift = bucket.share_percent / bucket.baseline_percent;
            (bucket.human_messages >= MIN_DAYPART_MESSAGES
                && lift >= MIN_DAYPART_LIFT
                && bucket.share_percent - bucket.baseline_percent >= MIN_DAYPART_SHARE_GAP)
                .then(|| DaypartInsight {
                    label: bucket.label.clone(),
                    human_messages: bucket.human_messages,
                    share_percent: bucket.share_percent,
                    baseline_percent: bucket.baseline_percent,
                    lift,
                })
        })
        .collect::<Vec<_>>();
    insights.sort_by(|left, right| right.lift.total_cmp(&left.lift));
    insights.truncate(2);
    insights
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

pub(crate) fn horizontal_bar(value: f64, maximum: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let ratio = if maximum > 0.0 {
        (value / maximum).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let filled = (ratio * width as f64).round() as usize;
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

#[allow(dead_code)] // The enrichment ticket consumes this neutral sentiment primitive.
pub(crate) fn neutral_bar(value: f64, maximum: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let mut cells = vec![' '; width];
    let center = width / 2;
    cells[center] = '│';
    if maximum > 0.0 {
        let reach = center.min(width.saturating_sub(center + 1));
        let filled = ((value.abs() / maximum).clamp(0.0, 1.0) * reach as f64).round() as usize;
        if value < 0.0 {
            for cell in &mut cells[center.saturating_sub(filled)..center] {
                *cell = '█';
            }
        } else {
            for cell in &mut cells[center + 1..(center + 1 + filled).min(width)] {
                *cell = '█';
            }
        }
    }
    cells.into_iter().collect()
}

pub(crate) fn compact_heatmap(values: &[u64], width: usize) -> String {
    let values = sampled_values(values, width);
    let maximum = values.iter().copied().max().unwrap_or(0);
    let levels = ['·', '░', '▒', '▓', '█'];
    values
        .into_iter()
        .map(|value| {
            if maximum == 0 {
                levels[0]
            } else {
                levels[value as usize * (levels.len() - 1) / maximum as usize]
            }
        })
        .collect()
}

fn sampled_values(values: &[u64], width: usize) -> Vec<u64> {
    if values.is_empty() || width == 0 {
        return Vec::new();
    }
    let count = values.len().min(width);
    (0..count)
        .map(|index| values[index * values.len() / count])
        .collect()
}

#[cfg(test)]
pub fn render_terminal(report: &UsageReport) -> String {
    render_terminal_themed(report, 80, false)
}

#[cfg(test)]
pub fn render_terminal_themed(report: &UsageReport, width: usize, color: bool) -> String {
    render_terminal_window(report, width, color, ReportWindow::Default, false)
}

pub fn render_terminal_window(
    report: &UsageReport,
    width: usize,
    color: bool,
    window: ReportWindow,
    show_models: bool,
) -> String {
    let width = width.max(20);
    let mut out = String::new();
    out.push_str(&styled_role("Historious report", StyleRole::Header, color));
    out.push('\n');
    push_wrapped(
        &mut out,
        &format!(
            "Period {} → {}",
            report.totals.first_activity_at.as_deref().unwrap_or("unknown"),
            report.totals.last_activity_at.as_deref().unwrap_or("unknown")
        ),
        width,
        0,
        StyleRole::Time,
        color,
    );
    push_wrapped(
        &mut out,
        &format!("Snapshot {} · local time", report.generated_at),
        width,
        0,
        StyleRole::Muted,
        color,
    );
    if report.filters.after.is_some()
        || report.filters.before.is_some()
        || report.filters.project.is_some()
    {
        push_wrapped(
            &mut out,
            &format!(
                "Filters · after {} · before {} · project {}",
                report.filters.after.as_deref().unwrap_or("all time"),
                report.filters.before.as_deref().unwrap_or("now"),
                report.filters.project.as_deref().unwrap_or("all projects")
            ),
            width,
            0,
            StyleRole::Muted,
            color,
        );
    }
    for warning in &report.warnings {
        push_wrapped(
            &mut out,
            &format!("Note: {warning}"),
            width,
            0,
            StyleRole::Muted,
            color,
        );
    }

    out.push('\n');
    out.push_str(&styled_role("Activity overview", StyleRole::Section, color));
    out.push('\n');
    out.push_str(&format!(
        "  {} sessions · {} threads\n",
        styled_role(
            &compact_number(report.totals.sessions),
            StyleRole::Count,
            color
        ),
        styled_role(
            &compact_number(report.totals.threads),
            StyleRole::Count,
            color
        )
    ));
    push_wrapped(
        &mut out,
        &format!(
            "{} human turns · {} assistant turns",
            exact_number(report.totals.human_turns),
            exact_number(report.totals.assistant_turns)
        ),
        width,
        2,
        StyleRole::Count,
        color,
    );
    push_wrapped(
        &mut out,
        &format!(
            "{} delegated turns · {} harness/context turns",
            exact_number(report.totals.delegated_turns),
            exact_number(report.totals.harness_turns)
        ),
        width,
        2,
        StyleRole::Count,
        color,
    );
    render_activity_calendar(&mut out, &report.activity, width, color);

    out.push('\n');
    out.push_str(&styled_role("What changed", StyleRole::Section, color));
    out.push('\n');
    let selected = report
        .comparisons
        .iter()
        .filter(|comparison| window.includes(comparison.days))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        push_wrapped(
            &mut out,
            "No comparison data is available for this filtered report.",
            width,
            2,
            StyleRole::Muted,
            color,
        );
    } else {
        for (index, comparison) in selected.into_iter().enumerate() {
            if index > 0 {
                out.push('\n');
            }
            render_comparison_table(&mut out, comparison, width, color);
        }
    }

    if !report.daypart_insights.is_empty() {
        out.push('\n');
        out.push_str(&styled_role("Working rhythm", StyleRole::Section, color));
        out.push('\n');
        let hour_values = report
            .rhythms
            .by_hour
            .iter()
            .map(|bucket| bucket.human_messages)
            .collect::<Vec<_>>();
        out.push_str(&format!(
            "  {} {} {}\n",
            styled_role("00", StyleRole::Time, color),
            styled_role(
                &compact_heatmap(&hour_values, width.saturating_sub(12).min(24)),
                StyleRole::Count,
                color
            ),
            styled_role("23", StyleRole::Time, color)
        ));
        for insight in &report.daypart_insights {
            let bar_width = width.saturating_sub(42).clamp(8, 24);
            let bar = styled_role(
                &horizontal_bar(insight.share_percent, 100.0, bar_width),
                StyleRole::Count,
                color,
            );
            if width < 70 {
                out.push_str(&format!(
                    "  {} {:.0}% vs {:.0}% · {} msg\n    {}\n",
                    styled_role(&insight.label, StyleRole::Time, color),
                    insight.share_percent,
                    insight.baseline_percent,
                    compact_number(insight.human_messages),
                    bar
                ));
            } else {
                out.push_str(&format!(
                    "  {:<13} {} {:>3.0}% vs {:>3.0}% · {} msg\n",
                    styled_role(&insight.label, StyleRole::Time, color),
                    bar,
                    insight.share_percent,
                    insight.baseline_percent,
                    compact_number(insight.human_messages)
                ));
            }
        }
    }

    if show_models {
        render_model_usage(&mut out, &report.model_mix_by_month, width, color);
    }

    out.push('\n');
    out.push_str(&styled_role("Leading projects", StyleRole::Section, color));
    out.push('\n');
    for row in report.projects.iter().take(5) {
        let project = ellipsize_middle(&row.project, width.saturating_sub(2).max(10));
        out.push_str(&format!(
            "  {}\n    {} sess · {} msg · {}\n",
            styled_role(&project, StyleRole::Project, color),
            styled_role(&compact_number(row.sessions), StyleRole::Count, color),
            styled_role(&compact_number(row.human_messages), StyleRole::Count, color),
            styled_role(
                &format!("{}h", compact_number((row.duration_secs / 3_600).max(0) as u64)),
                StyleRole::Muted,
                color
            )
        ));
        if !row.terms.is_empty() {
            push_wrapped(
                &mut out,
                &format!(
                    "about {}",
                    row.terms
                        .iter()
                        .map(|term| term.term.as_str())
                        .collect::<Vec<_>>()
                        .join(" · ")
                ),
                width,
                4,
                StyleRole::Muted,
                color,
            );
        }
    }
    if report.topics.as_ref().is_some_and(|topics| {
        topics
            .topics
            .iter()
            .any(|topic| topic.label != "miscellaneous")
    }) {
        out.push_str("\nTopics\n  Coherent topic data is ready for ranked integration.\n");
    }
    out
}

#[derive(Debug)]
struct ModelSlice {
    name: String,
    sessions: u64,
    other: bool,
}

fn model_slices(points: &[MixPoint]) -> Vec<ModelSlice> {
    let mut totals = HashMap::<&str, u64>::new();
    for point in points.iter().filter(|point| point.sessions > 0) {
        *totals.entry(&point.name).or_default() += point.sessions;
    }
    let mut slices = totals
        .into_iter()
        .map(|(name, sessions)| ModelSlice {
            name: name.to_string(),
            sessions,
            other: false,
        })
        .collect::<Vec<_>>();
    slices.sort_by(|left, right| {
        right
            .sessions
            .cmp(&left.sessions)
            .then_with(|| left.name.cmp(&right.name))
    });
    if slices.len() > 5 {
        let sessions = slices.drain(4..).map(|slice| slice.sessions).sum();
        slices.push(ModelSlice {
            name: "other models".to_string(),
            sessions,
            other: true,
        });
    }
    slices
}

fn model_style(index: usize) -> (&'static str, StyleRole) {
    match index {
        0 => ("█", StyleRole::Header),
        1 => ("▓", StyleRole::Section),
        2 => ("▒", StyleRole::Project),
        3 => ("░", StyleRole::Count),
        _ => ("·", StyleRole::Muted),
    }
}

fn slice_at(slices: &[ModelSlice], position: u64) -> usize {
    let mut cumulative = 0;
    for (index, slice) in slices.iter().enumerate() {
        cumulative += slice.sessions;
        if position < cumulative {
            return index;
        }
    }
    slices.len().saturating_sub(1)
}

fn render_model_usage(
    out: &mut String,
    points: &[MixPoint],
    width: usize,
    color: bool,
) {
    out.push('\n');
    out.push_str(&styled_role("Model usage", StyleRole::Section, color));
    out.push('\n');
    let slices = model_slices(points);
    if slices.is_empty() {
        push_wrapped(
            out,
            "No primary-model data is available for this report.",
            width,
            2,
            StyleRole::Muted,
            color,
        );
        return;
    }

    push_wrapped(
        out,
        "Primary model by session",
        width,
        2,
        StyleRole::Muted,
        color,
    );
    render_model_donut(out, &slices, width, color);
    render_model_legend(out, &slices, width, color);
    render_monthly_model_mix(out, points, &slices, width, color);
}

fn render_model_donut(out: &mut String, slices: &[ModelSlice], width: usize, color: bool) {
    const CHART_WIDTH: usize = 19;
    const CHART_HEIGHT: usize = 9;
    let total = slices.iter().map(|slice| slice.sessions).sum::<u64>();
    let exact_total = exact_number(total);
    let total_label = if exact_total.chars().count() <= 9 {
        exact_total
    } else {
        compact_number(total)
    };
    let indent = usize::from(width > CHART_WIDTH);
    for y in 0..CHART_HEIGHT {
        let center_label = match y {
            4 => Some((total_label.as_str(), StyleRole::Count)),
            5 => Some(("sessions", StyleRole::Muted)),
            _ => None,
        };
        let label_start = center_label
            .map(|(label, _)| (CHART_WIDTH - label.chars().count()) / 2)
            .unwrap_or(CHART_WIDTH);
        let mut row = " ".repeat(indent);
        let mut x = 0;
        while x < CHART_WIDTH {
            if let Some((label, role)) = center_label {
                if x == label_start {
                    row.push_str(&styled_role(label, role, color));
                    x += label.chars().count();
                    continue;
                }
            }
            let dx = (x as f64 - 9.0) / 2.0;
            let dy = y as f64 - 4.0;
            let radius = dx.hypot(dy);
            if (2.25..=4.25).contains(&radius) {
                let angle = (dx.atan2(-dy) + std::f64::consts::TAU)
                    % std::f64::consts::TAU;
                let position = ((angle / std::f64::consts::TAU) * total as f64)
                    .floor()
                    .min((total - 1) as f64) as u64;
                let index = slice_at(slices, position);
                let (glyph, role) = model_style(index);
                row.push_str(&styled_role(glyph, role, color));
            } else {
                row.push(' ');
            }
            x += 1;
        }
        out.push_str(row.trim_end());
        out.push('\n');
    }
}

fn render_model_legend(out: &mut String, slices: &[ModelSlice], width: usize, color: bool) {
    let total = slices.iter().map(|slice| slice.sessions).sum::<u64>() as f64;
    for (index, slice) in slices.iter().enumerate() {
        let (glyph, role) = model_style(index);
        let detail = format!(
            "{} session{} · {:.1}%",
            exact_number(slice.sessions),
            if slice.sessions == 1 { "" } else { "s" },
            slice.sessions as f64 * 100.0 / total
        );
        if width < 60 {
            out.push_str("  ");
            out.push_str(&styled_role(glyph, role, color));
            out.push(' ');
            out.push_str(&styled_role(
                &ellipsize_middle(&slice.name, width.saturating_sub(4).max(1)),
                role,
                color,
            ));
            out.push('\n');
            push_wrapped(out, &detail, width, 4, StyleRole::Count, color);
        } else {
            let detail_width = detail.chars().count() + 2;
            let name_width = width.saturating_sub(detail_width + 4).max(1);
            out.push_str("  ");
            out.push_str(&styled_role(glyph, role, color));
            out.push(' ');
            out.push_str(&styled_role(
                &format!(
                    "{:<name_width$}",
                    ellipsize_middle(&slice.name, name_width)
                ),
                role,
                color,
            ));
            out.push_str("  ");
            out.push_str(&styled_role(&detail, StyleRole::Count, color));
            out.push('\n');
        }
    }
}

fn render_monthly_model_mix(
    out: &mut String,
    points: &[MixPoint],
    slices: &[ModelSlice],
    width: usize,
    color: bool,
) {
    let other = slices.iter().position(|slice| slice.other);
    let mut months = BTreeMap::<&str, Vec<u64>>::new();
    for point in points.iter().filter(|point| point.sessions > 0) {
        let index = slices
            .iter()
            .position(|slice| !slice.other && slice.name == point.name)
            .or(other);
        if let Some(index) = index {
            months
                .entry(&point.month)
                .or_insert_with(|| vec![0; slices.len()])[index] += point.sessions;
        }
    }
    let skipped = months.len().saturating_sub(12);
    push_wrapped(
        out,
        "Monthly composition",
        width,
        2,
        StyleRole::Title,
        color,
    );
    for (month, counts) in months.into_iter().skip(skipped) {
        let total = counts.iter().sum::<u64>();
        let total_label = format!("{} sess", compact_number(total));
        let bar_width = width
            .saturating_sub(11 + total_label.chars().count())
            .clamp(1, 48);
        out.push_str("  ");
        out.push_str(&styled_role(month, StyleRole::Time, color));
        out.push(' ');
        for cell in 0..bar_width {
            let position = (((cell as u128 * 2 + 1) * total as u128)
                / (bar_width as u128 * 2)) as u64;
            let mut cumulative = 0;
            let index = counts
                .iter()
                .position(|count| {
                    cumulative += count;
                    position < cumulative
                })
                .unwrap_or_else(|| counts.len().saturating_sub(1));
            let (glyph, role) = model_style(index);
            out.push_str(&styled_role(glyph, role, color));
        }
        out.push(' ');
        out.push_str(&styled_role(&total_label, StyleRole::Count, color));
        out.push('\n');
    }
    if skipped > 0 {
        push_wrapped(
            out,
            &format!("{skipped} earlier months remain in JSON output"),
            width,
            2,
            StyleRole::Muted,
            color,
        );
    }
}

fn render_activity_calendar(
    out: &mut String,
    activity: &[ActivityPoint],
    width: usize,
    color: bool,
) {
    let counts = activity
        .iter()
        .filter_map(|point| {
            NaiveDate::parse_from_str(&point.bucket, "%Y-%m-%d")
                .ok()
                .map(|day| (day, point.human_turns))
        })
        .collect::<BTreeMap<_, _>>();
    let Some(data_end) = counts.keys().next_back().copied() else {
        return;
    };
    let weeks = (width.saturating_sub(4) / 2).clamp(8, 52);
    let days_until_saturday = 6 - i64::from(data_end.weekday().num_days_from_sunday());
    let calendar_end = data_end + ChronoDuration::days(days_until_saturday);
    let calendar_start = calendar_end - ChronoDuration::days((weeks * 7 - 1) as i64);
    let positives = counts
        .iter()
        .filter(|(day, count)| **day >= calendar_start && **day <= data_end && **count > 0)
        .map(|(_, count)| *count)
        .collect::<Vec<_>>();
    let mut positives = positives;
    positives.sort_unstable();

    let mut months = vec![' '; weeks * 2];
    for week in 0..weeks {
        let sunday = calendar_start + ChronoDuration::days((week * 7) as i64);
        let marker = (0..7)
            .map(|offset| sunday + ChronoDuration::days(offset))
            .find(|day| day.day() == 1)
            .or_else(|| (week == 0).then_some(sunday));
        if let Some(day) = marker {
            let label = day.format("%b").to_string();
            for (offset, character) in label.chars().enumerate() {
                if let Some(slot) = months.get_mut(week * 2 + offset) {
                    *slot = character;
                }
            }
        }
    }
    out.push_str("    ");
    out.extend(months);
    out.push('\n');

    for (weekday, label) in ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        .into_iter()
        .enumerate()
    {
        out.push_str(label);
        out.push(' ');
        let mut cells = String::with_capacity(weeks * 2);
        for week in 0..weeks {
            let day = calendar_start + ChronoDuration::days((week * 7 + weekday) as i64);
            if day > data_end {
                cells.push_str("  ");
                continue;
            }
            let count = counts.get(&day).copied().unwrap_or(0);
            cells.push(calendar_glyph(count, &positives));
            cells.push(' ');
        }
        out.push_str(&styled_role(&cells, StyleRole::Count, color));
        out.push('\n');
    }
    if width < 50 {
        out.push_str("  Less ·░▒▓█ More\n");
    } else {
        out.push_str("  Less · ░ ▒ ▓ █ More · human turns/day\n");
    }
}

fn calendar_glyph(count: u64, positive_counts: &[u64]) -> char {
    if count == 0 || positive_counts.is_empty() {
        return '·';
    }
    let rank = positive_counts.partition_point(|value| *value <= count);
    match ((rank * 4 + positive_counts.len() - 1) / positive_counts.len()).clamp(1, 4) {
        1 => '░',
        2 => '▒',
        3 => '▓',
        _ => '█',
    }
}

fn ellipsize_middle(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let keep = width - 3;
    let left = keep / 2;
    let right = keep - left;
    let start = value.chars().take(left).collect::<String>();
    let end = value
        .chars()
        .rev()
        .take(right)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{start}...{end}")
}

fn render_comparison_table(
    out: &mut String,
    comparison: &ComparisonWindow,
    width: usize,
    color: bool,
) {
    out.push_str(&format!(
        "  {}\n",
        styled_role(
            &format!("{} days", comparison.days),
            StyleRole::Title,
            color
        )
    ));
    if width < 64 {
        out.push_str("  ");
        out.push_str(&styled_role(
            &format!("{}–{}", comparison.current_start, comparison.current_end),
            StyleRole::Time,
            color,
        ));
        out.push_str("\n  ");
        out.push_str(&styled_role(
            &format!(
                "vs {}–{}",
                comparison.previous_start, comparison.previous_end
            ),
            StyleRole::Muted,
            color,
        ));
        out.push('\n');
        for row in &comparison.metrics {
            render_comparison_row(out, row, width, color);
        }
        render_project_changes(out, &comparison.project_changes, width, color);
    } else {
        out.push_str("  ");
        out.push_str(&styled_role(
            &format!(
                "{}–{} vs {}–{}",
                comparison.current_start,
                comparison.current_end,
                comparison.previous_start,
                comparison.previous_end
            ),
            StyleRole::Time,
            color,
        ));
        out.push('\n');
        out.push_str("  ");
        out.push_str(&styled_role(
            "metric                        current   previous    change",
            StyleRole::Title,
            color,
        ));
        out.push('\n');
        for row in &comparison.metrics {
            render_comparison_row(out, row, width, color);
        }
        render_project_changes(out, &comparison.project_changes, width, color);
    }
}

fn render_project_changes(out: &mut String, rows: &[ChangeInsight], width: usize, color: bool) {
    if rows.is_empty() {
        return;
    }
    out.push('\n');
    out.push_str("  ");
    out.push_str(&styled_role(
        "project shifts · human turns",
        StyleRole::Muted,
        color,
    ));
    out.push('\n');
    for row in rows {
        render_comparison_row(out, row, width, color);
    }
}

fn render_comparison_row(out: &mut String, row: &ChangeInsight, width: usize, color: bool) {
    let label = comparison_row_label(
        row,
        if width < 64 {
            width.saturating_sub(2)
        } else {
            28
        },
    );
    let label_role = if row.subject.is_some() {
        StyleRole::Project
    } else {
        StyleRole::Title
    };
    if width < 64 {
        out.push_str("  ");
        out.push_str(&styled_role(&label, label_role, color));
        out.push_str("\n    ");
        out.push_str(&styled_role("cur", StyleRole::Muted, color));
        out.push(' ');
        out.push_str(&styled_role(
            &comparison_value(row),
            StyleRole::Count,
            color,
        ));
        out.push_str(" · ");
        out.push_str(&styled_role("prev", StyleRole::Muted, color));
        out.push(' ');
        out.push_str(&styled_role(&previous_value(row), StyleRole::Count, color));
        out.push_str(" · ");
        out.push_str(&styled_role(&change_value(row), StyleRole::Count, color));
        out.push('\n');
    } else {
        out.push_str("  ");
        out.push_str(&styled_role(&format!("{label:<28}"), label_role, color));
        out.push(' ');
        out.push_str(&styled_role(
            &format!("{:>9}", comparison_value(row)),
            StyleRole::Count,
            color,
        ));
        out.push_str("  ");
        out.push_str(&styled_role(
            &format!("{:>9}", previous_value(row)),
            StyleRole::Count,
            color,
        ));
        out.push_str("  ");
        out.push_str(&styled_role(
            &format!("{:>8}", change_value(row)),
            StyleRole::Count,
            color,
        ));
        out.push('\n');
    }
}

fn comparison_row_label(row: &ChangeInsight, width: usize) -> String {
    let label = row
        .subject
        .as_ref()
        .cloned()
        .unwrap_or_else(|| row.metric.clone());
    ellipsize_middle(&label, width)
}

fn comparison_value(row: &ChangeInsight) -> String {
    if row.metric == "tokens" {
        compact_number(row.current)
    } else {
        exact_number(row.current)
    }
}

fn previous_value(row: &ChangeInsight) -> String {
    if row.metric == "tokens" {
        compact_number(row.previous)
    } else {
        exact_number(row.previous)
    }
}

fn change_value(row: &ChangeInsight) -> String {
    match row.change_percent {
        Some(percent) if percent > 0 => format!("+{percent}%"),
        Some(percent) => format!("{percent}%"),
        None if row.current > 0 => "new".to_string(),
        None => "—".to_string(),
    }
}

fn compact_number(value: u64) -> String {
    for (threshold, suffix) in [(1_000_000_000, "B"), (1_000_000, "M"), (1_000, "k")] {
        if value >= threshold {
            let scaled = value as f64 / threshold as f64;
            return if scaled >= 10.0 {
                format!("{scaled:.0}{suffix}")
            } else {
                format!("{scaled:.1}{suffix}")
            };
        }
    }
    value.to_string()
}

fn exact_number(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn push_wrapped(
    out: &mut String,
    text: &str,
    width: usize,
    indent: usize,
    role: StyleRole,
    color: bool,
) {
    let prefix = " ".repeat(indent.min(width.saturating_sub(1)));
    let budget = width.saturating_sub(prefix.len()).max(1);
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && line.chars().count() + 1 + word.chars().count() > budget {
            out.push_str(&prefix);
            out.push_str(&styled_role(&line, role, color));
            out.push('\n');
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        if word.chars().count() > budget {
            line.push_str(&ellipsize_middle(word, budget));
        } else {
            line.push_str(word);
        }
    }
    if !line.is_empty() {
        out.push_str(&prefix);
        out.push_str(&styled_role(&line, role, color));
        out.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranked_changes_and_dayparts_suppress_weak_patterns() {
        let mut ranked = Vec::new();
        push_change(&mut ranked, "weak sample", None, 11, 10);
        push_change(&mut ranked, "weak effect", None, 110, 100);
        push_change(&mut ranked, "strong effect", None, 150, 100);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].1.metric, "strong effect");
        assert_eq!(ranked[0].1.change_percent, Some(50));

        let dayparts = report_dayparts(&Rhythms {
            by_hour: vec![
                RhythmBucket {
                    label: "00".to_string(),
                    human_messages: 10,
                },
                RhythmBucket {
                    label: "09".to_string(),
                    human_messages: 100,
                },
            ],
            by_weekday: Vec::new(),
        });
        assert_eq!(
            dayparts
                .iter()
                .map(|bucket| bucket.label.as_str())
                .collect::<Vec<_>>(),
            vec!["night", "early morning", "morning", "afternoon", "evening"]
        );
        let insights = select_daypart_insights(&dayparts);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].label, "morning");
        assert!(insights[0].share_percent > insights[0].baseline_percent);
    }

    #[test]
    fn terminal_primitives_bound_width_and_handle_empty_or_flat_values() {
        assert_eq!(horizontal_bar(5.0, 10.0, 6).chars().count(), 6);
        assert_eq!(horizontal_bar(5.0, 0.0, 4), "░░░░");
        assert_eq!(neutral_bar(-0.5, 1.0, 9).chars().count(), 9);
        assert!(neutral_bar(0.0, 1.0, 9).contains('│'));

        assert_eq!(compact_heatmap(&[], 8), "");
        assert!(compact_heatmap(&[0, 1, 2, 3, 4], 3).chars().count() <= 3);
    }

    #[test]
    fn model_usage_reconciles_pie_totals_and_monthly_composition() {
        let point = |month: &str, name: &str, sessions| MixPoint {
            month: month.to_string(),
            name: name.to_string(),
            sessions,
        };
        let points = vec![
            point("2026-01", "alpha", 50),
            point("2026-01", "beta", 40),
            point("2026-01", "gamma", 30),
            point("2026-01", "delta", 20),
            point("2026-01", "epsilon", 10),
            point("2026-01", "zeta", 5),
            point("2026-02", "alpha", 20),
            point("2026-02", "beta", 30),
            point("2026-02", "gamma", 10),
            point("2026-02", "delta", 10),
            point("2026-02", "epsilon", 5),
            point("2026-02", "zeta", 5),
        ];
        let slices = model_slices(&points);
        assert_eq!(
            slices
                .iter()
                .map(|slice| slice.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma", "delta", "other models"]
        );
        assert_eq!(
            slices
                .iter()
                .map(|slice| slice.sessions)
                .collect::<Vec<_>>(),
            vec![70, 70, 40, 30, 25]
        );
        assert_eq!(slices.iter().map(|slice| slice.sessions).sum::<u64>(), 235);

        let mut plain = String::new();
        render_model_usage(&mut plain, &points, 80, false);
        assert!(plain.contains("Model usage"));
        assert!(plain.contains("235"));
        assert_eq!(plain.matches("70 sessions · 29.8%").count(), 2);
        assert!(plain.contains("other models"));
        assert!(plain.contains("25 sessions · 10.6%"));
        assert!(plain.find("2026-01") < plain.find("2026-02"));
        assert!(!plain.contains('\x1b'));

        for width in [20, 40, 80] {
            let mut narrow = String::new();
            render_model_usage(&mut narrow, &points, width, false);
            let overlong = narrow
                .lines()
                .filter(|line| line.chars().count() > width)
                .collect::<Vec<_>>();
            assert!(overlong.is_empty(), "width {width}: {overlong:?}");
        }
        let mut colored = String::new();
        render_model_usage(&mut colored, &points, 80, true);
        assert!(colored.contains("\x1b["));

        let mut empty = String::new();
        render_model_usage(&mut empty, &[], 40, false);
        assert!(empty.contains("No primary-model data is available"));
    }

    #[test]
    fn comparison_tables_follow_themed_hierarchy() {
        let comparison = ComparisonWindow {
            days: 7,
            current_start: "2026-07-07".to_string(),
            current_end: "2026-07-13".to_string(),
            previous_start: "2026-06-30".to_string(),
            previous_end: "2026-07-06".to_string(),
            metrics: vec![change_row("sessions", None, 288, 206)],
            project_changes: vec![change_row(
                "human turns",
                Some("kittylitter".to_string()),
                212,
                12,
            )],
        };

        let mut plain = String::new();
        render_comparison_table(&mut plain, &comparison, 80, false);
        assert!(plain.contains("project shifts · human turns\n  kittylitter"));
        assert!(!plain.contains("kittylitter · human turns"));
        assert!(!plain.contains('\x1b'));

        let mut narrow = String::new();
        render_comparison_table(&mut narrow, &comparison, 40, false);
        assert!(narrow.lines().all(|line| line.chars().count() <= 40));

        let mut colored = String::new();
        render_comparison_table(&mut colored, &comparison, 80, true);
        assert!(colored.contains(&styled_role(
            "2026-07-07–2026-07-13 vs 2026-06-30–2026-07-06",
            StyleRole::Time,
            true,
        )));
        assert!(colored.contains(&styled_role(
            &format!("{:<28}", "sessions"),
            StyleRole::Title,
            true,
        )));
        assert!(colored.contains(&styled_role(
            &format!("{:<28}", "kittylitter"),
            StyleRole::Project,
            true,
        )));
        assert!(colored.contains(&styled_role(
            &format!("{:>8}", "+40%"),
            StyleRole::Count,
            true,
        )));
    }

    #[test]
    fn contribution_calendar_uses_quantiles_and_stays_width_bounded() {
        let positive = vec![1, 2, 3, 100];
        assert_eq!(calendar_glyph(0, &positive), '·');
        assert_eq!(calendar_glyph(1, &positive), '░');
        assert_eq!(calendar_glyph(2, &positive), '▒');
        assert_eq!(calendar_glyph(3, &positive), '▓');
        assert_eq!(calendar_glyph(100, &positive), '█');

        let start = NaiveDate::from_ymd_opt(2025, 7, 13).expect("start date");
        let activity = (0..365)
            .map(|offset| ActivityPoint {
                bucket: (start + ChronoDuration::days(offset)).to_string(),
                sessions: (offset % 5) as u64,
                human_turns: if offset % 11 == 0 { 100 } else { (offset % 7) as u64 },
            })
            .collect::<Vec<_>>();
        for width in [40, 80, 120, 200] {
            let mut plain = String::new();
            render_activity_calendar(&mut plain, &activity, width, false);
            assert!(plain.lines().all(|line| line.chars().count() <= width));
            for weekday in ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] {
                assert!(plain.lines().any(|line| line.starts_with(weekday)));
            }
            assert!(plain.contains("Less"));
            assert!(!plain.contains('\x1b'));
        }
        let mut colored = String::new();
        render_activity_calendar(&mut colored, &activity, 80, true);
        assert!(colored.contains("\x1b["));
    }

    #[test]
    fn shareable_labels_strip_paths_and_exact_counts_stay_distinct() {
        assert_eq!(project_label("/Users/example/Code/project-alpha"), "project-alpha");
        assert_eq!(project_label(r"C:\\Users\\example\\Code\\project-beta"), "project-beta");
        assert_eq!(project_label("/repo/trailing/"), "trailing");
        assert_eq!(exact_number(32_919), "32,919");
        assert_eq!(exact_number(33_245), "33,245");
        assert_ne!(exact_number(32_919), exact_number(33_245));
    }

    #[test]
    fn report_filters_read_legacy_since_snapshots() {
        let filters = serde_json::from_str::<ReportFilters>(
            r#"{"since":"2026-06-01T00:00:00Z","project":null,"timezone":"local"}"#,
        )
        .expect("legacy report filters");

        assert_eq!(filters.after.as_deref(), Some("2026-06-01T00:00:00Z"));
        assert!(filters.before.is_none());
    }

    #[test]
    fn frequency_tokenizer_removes_code_urls_paths_and_punctuation() {
        let tokens = tokenize_frequency_text(
            "Hello, friend!\n```rust\nsecret_code();\n```\nvisit https://example.com /tmp/file okay-done",
        );
        assert_eq!(tokens, vec!["hello", "friend", "visit", "okay", "done"]);
    }

    #[test]
    fn frequency_counts_apply_threshold_and_stable_order() {
        let terms = top_terms(
            HashMap::from([
                ("alpha".to_string(), 4),
                ("beta".to_string(), 4),
                ("rare".to_string(), 2),
            ]),
            3,
            10,
        );
        assert_eq!(
            terms
                .iter()
                .map(|term| (term.term.as_str(), term.count))
                .collect::<Vec<_>>(),
            vec![("alpha", 4), ("beta", 4)]
        );
        assert!(english_stopwords().contains("please"));
    }

    #[test]
    fn project_terms_prefer_distinctive_subjects_over_shared_workflow_words() {
        assert!(project_noise_words().contains("make"));
        let terms = distinctive_project_terms(HashMap::from([
            (
                "/repo/booking".to_string(),
                HashMap::from([
                    ("booking".to_string(), 10),
                    ("shared".to_string(), 8),
                ]),
            ),
            (
                "/repo/wallet".to_string(),
                HashMap::from([
                    ("wallet".to_string(), 9),
                    ("shared".to_string(), 8),
                ]),
            ),
        ]));
        assert_eq!(terms["/repo/booking"][0].term, "booking");
        assert_eq!(terms["/repo/wallet"][0].term, "wallet");
    }

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

        rebuild_snapshot(&store).expect("build report snapshot");
        let snapshot_generated_at = read_snapshot(&store)
            .expect("read report snapshot")
            .expect("stored report snapshot")
            .generated_at;
        let report = compute(
            &store,
            &ReportOptions {
                after: Some("2026-06-01T00:00:00+00:00".to_string()),
                before: None,
                project: None,
                sort: ReportSort::Tokens,
            },
        )
        .expect("compute report");

        assert!(!report.contains_raw_text);
        assert_ne!(report.generated_at, snapshot_generated_at);
        assert_eq!(report.totals.sessions, 2);
        assert_eq!(report.totals.threads, 1);
        assert_eq!(report.totals.events, 3);
        assert_eq!(report.totals.human_turns, 1);
        assert_eq!(report.totals.assistant_turns, 0);
        assert_eq!(report.totals.delegated_turns, 1);
        assert_eq!(report.totals.harness_turns, 0);
        assert_eq!(report.projects[0].project, "a");
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
        assert!(report.topics.is_none());
        assert!(report.sentiment.is_none());
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("sentiment is unavailable")));
        let rendered = render_terminal(&report);
        assert!(rendered.contains("What changed"));
        assert!(!rendered.contains("now vs"));
        assert!(rendered.contains("Leading projects"));
        assert!(!rendered.contains("Model usage"));
        assert!(!rendered.contains("Provider mix by month"));
        assert!(!rendered.contains("Sentiment by local hour"));
        assert!(!rendered.contains("/repo/"));
        let json = serde_json::to_string(&report).expect("serialize shareable report");
        assert!(!json.contains("/repo/"));
        assert!(json.contains("\"project\":\"a\""));
        let narrow = render_terminal_themed(&report, 40, false);
        assert!(narrow.lines().all(|line| line.chars().count() <= 40));
        assert!(!narrow.contains('\x1b'));
        assert!(render_terminal_themed(&report, 80, true).contains("\x1b["));
        let with_models =
            render_terminal_window(&report, 80, false, ReportWindow::Default, true);
        assert!(with_models.contains("Model usage"));
        assert!(with_models.contains("gpt-5.4"));
        assert!(with_models.contains("gpt-5.5"));
        assert_eq!(with_models.matches("1 session · 50.0%").count(), 2);
        let narrow_models =
            render_terminal_window(&report, 40, false, ReportWindow::Default, true);
        assert!(narrow_models
            .lines()
            .all(|line| line.chars().count() <= 40));
        assert!(!narrow_models.contains('\x1b'));

        let bounded = compute(
            &store,
            &ReportOptions {
                after: Some("2026-06-02T00:00:00+00:00".to_string()),
                before: Some("2026-06-03T00:00:00+00:00".to_string()),
                project: None,
                sort: ReportSort::Tokens,
            },
        )
        .expect("compute bounded report");
        assert_eq!(bounded.totals.sessions, 1);
        assert_eq!(bounded.totals.events, 1);
        assert_eq!(bounded.totals.human_turns, 0);
        assert_eq!(bounded.totals.delegated_turns, 1);
        assert_eq!(bounded.projects[0].project, "b");
        assert_eq!(bounded.tokens_by_session_end_date.len(), 1);
        assert_eq!(bounded.model_mix_by_month[0].name, "gpt-5.5");
        assert!(bounded.comparisons.is_empty());
        assert_eq!(
            bounded.filters.after.as_deref(),
            Some("2026-06-02T00:00:00+00:00")
        );
        assert_eq!(
            bounded.filters.before.as_deref(),
            Some("2026-06-03T00:00:00+00:00")
        );
        let bounded_terminal = render_terminal(&bounded);
        assert!(bounded_terminal.contains("Filters · after"));
        assert!(bounded_terminal.contains("2026-06-02T00:00:00+00:00"));
        assert!(bounded_terminal.contains("2026-06-03T00:00:00+00:00"));

        let filtered = compute(
            &store,
            &ReportOptions {
                after: None,
                before: None,
                project: Some("/repo/b".to_string()),
                sort: ReportSort::Messages,
            },
        )
        .expect("compute filtered report");
        assert_eq!(filtered.totals.sessions, 1);
        assert_eq!(filtered.totals.human_turns, 0);
        assert_eq!(filtered.filters.project.as_deref(), Some("b"));

        let unfiltered = compute(
            &store,
            &ReportOptions {
                after: None,
                before: None,
                project: None,
                sort: ReportSort::Tokens,
            },
        )
        .expect("compute unfiltered comparison report");
        assert_eq!(unfiltered.generated_at, snapshot_generated_at);
        assert!(unfiltered
            .warnings
            .iter()
            .any(|warning| warning.contains("report snapshot is") && warning.contains("stale")));
        assert_eq!(
            unfiltered
                .comparisons
                .iter()
                .map(|window| window.days)
                .collect::<Vec<_>>(),
            vec![7, 14, 28]
        );
        for window in &unfiltered.comparisons {
            let current_start = NaiveDate::parse_from_str(&window.current_start, "%Y-%m-%d")
                .expect("current start");
            let current_end = NaiveDate::parse_from_str(&window.current_end, "%Y-%m-%d")
                .expect("current end");
            let previous_start = NaiveDate::parse_from_str(&window.previous_start, "%Y-%m-%d")
                .expect("previous start");
            let previous_end = NaiveDate::parse_from_str(&window.previous_end, "%Y-%m-%d")
                .expect("previous end");
            assert_eq!((current_end - current_start).num_days(), i64::from(window.days - 1));
            assert_eq!((previous_end - previous_start).num_days(), i64::from(window.days - 1));
            assert_eq!(previous_end + ChronoDuration::days(1), current_start);
            assert_eq!(window.metrics.len(), 3);
        }
        let default_tables = render_terminal_themed(&unfiltered, 80, false);
        assert!(default_tables.contains("7 days"));
        assert!(!default_tables.contains("14 days"));
        assert!(default_tables.contains("28 days"));
        assert!(default_tables.contains("current"));
        assert!(default_tables.contains("previous"));
        assert!(default_tables.contains("\n\n  28 days"));
        let all_tables = render_terminal_window(&unfiltered, 80, false, ReportWindow::All, false);
        assert!(all_tables.contains("14 days"));
        let narrow_tables =
            render_terminal_window(&unfiltered, 40, false, ReportWindow::Seven, false);
        assert!(narrow_tables.lines().all(|line| line.chars().count() <= 40));
    }

    #[test]
    fn topic_report_uses_latest_completed_labeled_version_and_aggregate_dimensions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO session_facts (session_id, source_kind, workspace_path)
                    VALUES ('session_a', 'codex', '/repo/project-alpha'),
                           ('session_b', 'codex', '/repo/project-beta');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('item_a', 'event_a', 'session_a', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'a', 'hash_a', '2026-05-01T00:00:00Z',
                       1, 'required', '{}', 'item_hash_a'),
                      ('item_b', 'event_b', 'session_b', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'b', 'hash_b', '2026-06-01T00:00:00Z',
                       1, 'required', '{}', 'item_hash_b');

                    INSERT INTO topic_runs
                      (version, algorithm_version, model_id, input_hash, item_count, selected_k,
                       silhouette_score, status, started_at, completed_at)
                    VALUES
                      ('complete', 1, 'model', 'input', 2, 2, 0.8, 'completed',
                       '2026-07-01T00:00:00Z', '2026-07-01T00:01:00Z'),
                      ('building', 1, 'model', 'input2', 2, NULL, NULL, 'building',
                       '2026-07-02T00:00:00Z', NULL);

                    INSERT INTO topic_assignments (version, item_id, topic_id, distance)
                    VALUES ('complete', 'item_a', 0, 0.1), ('complete', 'item_b', 1, 0.2);
                    "#,
                )?;
                for (topic_id, label) in [(0, "Project alpha"), (1, "Project beta")] {
                    conn.execute(
                        "INSERT INTO topics (version, topic_id, size, centroid, label)
                         VALUES ('complete', ?1, 1, ?2, ?3)",
                        params![topic_id, vec![0u8], label],
                    )?;
                }
                Ok(())
            })
            .expect("insert topic report fixtures");

        let (topics, warning) = report_topics(&store, None, None, None).expect("topic report");
        let topics = topics.expect("completed topics");
        assert!(warning.is_none());
        assert_eq!(topics.version, "complete");
        assert_eq!(topics.assigned_messages, 2);
        assert_eq!(topics.by_month[0].month, "2026-05");
        assert_eq!(topics.by_project[0].project, "project-alpha");

        store
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO topic_runs
                     (version, algorithm_version, model_id, input_hash, item_count, selected_k,
                      silhouette_score, status, started_at, completed_at)
                     VALUES ('unlabeled', 1, 'model', 'input3', 2, 2, 0.9, 'completed',
                             '2026-07-03T00:00:00Z', '2026-07-03T00:01:00Z')",
                    [],
                )?;
                conn.execute(
                    "INSERT INTO topics (version, topic_id, size, centroid, label)
                     VALUES ('unlabeled', 0, 1, ?1, 'Ready'),
                            ('unlabeled', 1, 1, ?1, NULL)",
                    params![vec![0u8]],
                )?;
                Ok(())
            })
            .expect("insert incomplete labels");
        let (topics, warning) =
            report_topics(&store, None, None, None).expect("incomplete topics");
        assert!(topics.is_none());
        assert!(warning.expect("warning").contains("1 of 2 labels"));

        store
            .with_conn(|conn| {
                conn.execute_batch(
                    "INSERT INTO topic_runs
                       (version, algorithm_version, model_id, input_hash, item_count, selected_k,
                        silhouette_score, status, started_at, completed_at)
                     VALUES ('demoted', 2, 'model', 'input4', 2, 1, 0.07, 'completed',
                             '2026-07-04T00:00:00Z', '2026-07-04T00:01:00Z');
                     INSERT INTO topics (version, topic_id, size, centroid, label)
                     VALUES ('demoted', 0, 2, X'00', 'miscellaneous');",
                )?;
                Ok(())
            })
            .expect("insert demoted topics");
        let (topics, warning) =
            report_topics(&store, None, None, None).expect("demoted topics");
        assert!(topics.is_none());
        assert!(warning.is_none());
    }

    #[test]
    fn sentiment_report_uses_latest_complete_version_and_shared_dimensions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO session_facts (session_id, source_kind, workspace_path)
                    VALUES ('session_a', 'codex', '/repo/project-alpha'),
                           ('session_b', 'codex', '/repo/project-beta');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, occurred_at, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('item_a', 'event_a', 'session_a', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'a', 'hash_a', '2026-05-01T01:00:00Z',
                       1, 'required', '{}', 'item_hash_a'),
                      ('item_b', 'event_b', 'session_b', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'b', 'hash_b', '2026-05-08T13:00:00Z',
                       1, 'required', '{}', 'item_hash_b');

                    INSERT INTO message_provenance
                      (item_id, session_id, source_kind, authored_by, sentiment_usable, rule)
                    VALUES
                      ('item_a', 'session_a', 'codex', 'human', 'yes', 'default.human'),
                      ('item_b', 'session_b', 'codex', 'human', 'yes', 'default.human');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert sentiment report fixtures");
        let first_time = chrono::DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let second_time = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let complete = [(&"item_a", 2u8), (&"item_b", 4u8)]
            .into_iter()
            .flat_map(|(item_id, score)| {
                crate::annotate::SENTIMENT_AXES.map(|axis| crate::annotate::MessageAnnotation {
                    item_id: (*item_id).to_string(),
                    axis: axis.to_string(),
                    score,
                    model: "complete-model".to_string(),
                    annotator_version: "complete-v1".to_string(),
                    annotated_at: first_time,
                })
            })
            .collect::<Vec<_>>();
        crate::annotate::insert_annotations(&store, &complete).expect("insert complete scores");
        let incomplete =
            crate::annotate::SENTIMENT_AXES.map(|axis| crate::annotate::MessageAnnotation {
                item_id: "item_a".to_string(),
                axis: axis.to_string(),
                score: 5,
                model: "newer-model".to_string(),
                annotator_version: "incomplete-v2".to_string(),
                annotated_at: second_time,
            });
        crate::annotate::insert_annotations(&store, &incomplete).expect("insert incomplete scores");

        let (sentiment, warning) =
            report_sentiment(&store, None, None, None).expect("sentiment report");
        let sentiment = sentiment.expect("complete sentiment version");
        assert!(warning.is_none());
        assert_eq!(sentiment.annotator_version, "complete-v1");
        assert_eq!(sentiment.annotated_messages, 2);
        assert_eq!(sentiment.by_week.len(), 22);
        assert_eq!(sentiment.by_project.len(), 22);
        assert_eq!(sentiment.by_hour.len(), 22);
        assert!(sentiment
            .by_project
            .iter()
            .any(|row| row.project == "project-alpha" && row.average == 2.0));
        assert!(sentiment
            .by_hour
            .iter()
            .any(|row| row.average == 4.0 && row.messages == 1));

        let (filtered, _) = report_sentiment(&store, None, None, Some("%project-alpha%"))
            .expect("filtered sentiment report");
        let filtered = filtered.expect("filtered sentiment");
        assert_eq!(filtered.by_week.len(), 11);
        assert_eq!(filtered.by_project.len(), 11);
        assert!(filtered
            .by_project
            .iter()
            .all(|row| row.project == "project-alpha"));
    }
}
