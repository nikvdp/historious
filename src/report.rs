use crate::analytics;
use crate::cli::{styled_role, StyleRole};
use crate::provenance;
use crate::storage::Store;
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const INSIGHT_LIMIT: usize = 5;
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
    pub since: Option<String>,
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
    pub changes: Vec<ChangeInsight>,
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
    pub since: Option<String>,
    pub project: Option<String>,
    pub timezone: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityPoint {
    pub bucket: String,
    pub sessions: u64,
    pub human_messages: u64,
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
pub struct TokenPoint {
    pub day: String,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRow {
    pub workspace_path: String,
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
    pub workspace_path: String,
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
    pub workspace_path: String,
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
    if options.since.is_none() && options.project.is_none() {
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
            since: None,
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
    let totals = report_totals(store, options.since.as_deref(), project_pattern.as_deref())?;
    let activity = report_activity(store, options.since.as_deref(), project_pattern.as_deref())?;
    let changes = report_changes(store, options.since.as_deref(), project_pattern.as_deref())?;
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
    let dayparts = report_dayparts(&rhythms);
    let daypart_insights = select_daypart_insights(&dayparts);
    let (frequencies, project_terms) =
        report_frequencies(store, options.since.as_deref(), project_pattern.as_deref())?;
    for project in &mut projects {
        project.terms = project_terms
            .get(&project.workspace_path)
            .cloned()
            .unwrap_or_default();
    }
    let (topics, topic_warning) =
        report_topics(store, options.since.as_deref(), project_pattern.as_deref())?;
    let (sentiment, sentiment_warning) =
        report_sentiment(store, options.since.as_deref(), project_pattern.as_deref())?;
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
            since: options.since.clone(),
            project: options.project.clone(),
            timezone: "local".to_string(),
        },
        warnings,
        totals,
        activity,
        changes,
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
    since: Option<&str>,
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
    let by_week = sentiment_periods(store, &version, since, project, "%Y-%W", "week")?
        .into_iter()
        .map(|(week, axis, average, messages)| SentimentPeriod {
            week,
            axis,
            average,
            messages,
        })
        .collect();
    let by_hour = sentiment_periods(store, &version, since, project, "%H", "hour")?
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
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
             GROUP BY sf.workspace_path, a.axis
             ORDER BY COUNT(*) DESC, sf.workspace_path, a.axis",
            sentiment_axes_sql()
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![version, since, project], |row| {
            Ok(SentimentProject {
                workspace_path: row.get(0)?,
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
    since: Option<&str>,
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
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
             GROUP BY 1, a.axis ORDER BY 1, a.axis",
            sentiment_axes_sql()
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![version, since, project], |row| {
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
    since: Option<&str>,
    project: Option<&str>,
) -> Result<(Option<TopicSection>, Option<String>)> {
    let Some((version, model_id, corpus_messages, selected_k)) = store.with_conn(|conn| {
        conn.query_row(
            "SELECT version, model_id, item_count, selected_k
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
                ))
            },
        )
        .optional()
        .map_err(Into::into)
    })?
    else {
        return Ok((None, Some("topics are unavailable; run `histo lab topics cluster` and `histo lab topics label`".to_string())));
    };
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
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
             GROUP BY a.topic_id, t.label
             ORDER BY COUNT(*) DESC, a.topic_id",
        )?;
        let rows = stmt.query_map(params![version, since, project], |row| {
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
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
             GROUP BY 1, a.topic_id, t.label ORDER BY 1, COUNT(*) DESC",
        )?;
        let rows = stmt.query_map(params![version, since, project], |row| {
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
               AND (?3 IS NULL OR sf.workspace_path LIKE ?3)
             GROUP BY sf.workspace_path, a.topic_id, t.label
             ORDER BY COUNT(*) DESC, sf.workspace_path, a.topic_id",
        )?;
        let rows = stmt.query_map(params![version, since, project], |row| {
            Ok(TopicProject {
                workspace_path: row.get(0)?,
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
    since: Option<&str>,
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
               AND (?2 IS NULL OR sf.workspace_path LIKE ?2)",
        )?;
        let rows = stmt.query_map(params![since, project], |row| {
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
        let mut activity = values
            .into_iter()
            .map(|(bucket, (sessions, human_messages))| ActivityPoint {
                bucket,
                sessions,
                human_messages,
            })
            .collect::<Vec<_>>();
        activity.sort_by_key(|point| activity_bucket_order(&point.bucket));
        Ok(activity)
    })
}

fn activity_bucket_order(bucket: &str) -> i32 {
    if let Ok(day) = NaiveDate::parse_from_str(bucket, "%Y-%m-%d") {
        return day.year() * 400 + day.ordinal() as i32;
    }
    bucket
        .split_once("-W")
        .and_then(|(year, week)| year.parse::<i32>().ok().zip(week.parse::<i32>().ok()))
        .map(|(year, week)| year * 400 + week * 7)
        .unwrap_or(i32::MIN)
}

fn report_changes(
    store: &Store,
    since: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<ChangeInsight>> {
    if since.is_some() {
        return Ok(Vec::new());
    }
    let (current_sessions, previous_sessions, current_tokens, previous_tokens) =
        store.with_conn(|conn| {
            conn.query_row(
                "SELECT
                   COALESCE(SUM(julianday(last_event_at) >= julianday('now', '-28 days')), 0),
                   COALESCE(SUM(julianday(last_event_at) >= julianday('now', '-56 days')
                                AND julianday(last_event_at) < julianday('now', '-28 days')), 0),
                   COALESCE(SUM(CASE WHEN julianday(last_event_at) >= julianday('now', '-28 days')
                                     THEN COALESCE(input_tokens, 0)
                                        + COALESCE(cached_input_tokens, 0)
                                        + COALESCE(output_tokens, 0) ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN julianday(last_event_at) >= julianday('now', '-56 days')
                                          AND julianday(last_event_at) < julianday('now', '-28 days')
                                     THEN COALESCE(input_tokens, 0)
                                        + COALESCE(cached_input_tokens, 0)
                                        + COALESCE(output_tokens, 0) ELSE 0 END), 0)
                 FROM session_facts
                 WHERE (?1 IS NULL OR workspace_path LIKE ?1)",
                params![project],
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
               COALESCE(SUM(julianday(p.occurred_at) >= julianday('now', '-28 days')), 0),
               COALESCE(SUM(julianday(p.occurred_at) >= julianday('now', '-56 days')
                            AND julianday(p.occurred_at) < julianday('now', '-28 days')), 0)
             FROM message_provenance p
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human'
               AND (?1 IS NULL OR sf.workspace_path LIKE ?1)",
            params![project],
            |row| Ok((nonnegative(row.get(0)?), nonnegative(row.get(1)?))),
        )
        .map_err(Into::into)
    })?;

    let mut ranked = Vec::<(f64, ChangeInsight)>::new();
    for (metric, current, previous) in [
        ("sessions", current_sessions, previous_sessions),
        ("human messages", current_messages, previous_messages),
        ("tokens", current_tokens, previous_tokens),
    ] {
        push_change(&mut ranked, metric, None, current, previous);
    }

    let projects = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT COALESCE(sf.workspace_path, 'unknown'),
                    SUM(julianday(p.occurred_at) >= julianday('now', '-28 days')),
                    SUM(julianday(p.occurred_at) >= julianday('now', '-56 days')
                        AND julianday(p.occurred_at) < julianday('now', '-28 days'))
             FROM message_provenance p
             JOIN session_facts sf ON sf.session_id = p.session_id
             WHERE p.authored_by = 'human'
               AND julianday(p.occurred_at) >= julianday('now', '-56 days')
               AND (?1 IS NULL OR sf.workspace_path LIKE ?1)
             GROUP BY sf.workspace_path",
        )?;
        let rows = stmt.query_map(params![project], |row| {
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
            "project human messages",
            Some(workspace_path),
            current,
            previous,
        );
    }
    project_ranked.sort_by(|left, right| right.0.total_cmp(&left.0));
    ranked.extend(project_ranked.into_iter().take(PROJECT_INSIGHT_LIMIT));
    ranked.sort_by(|left, right| right.0.total_cmp(&left.0));
    Ok(ranked
        .into_iter()
        .take(INSIGHT_LIMIT)
        .map(|(_, insight)| insight)
        .collect())
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
                terms: Vec::new(),
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

pub(crate) fn sparkline(values: &[u64], width: usize) -> String {
    let values = sampled_values(values, width);
    let Some((&minimum, &maximum)) = values.iter().min().zip(values.iter().max()) else {
        return String::new();
    };
    let levels = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    values
        .into_iter()
        .map(|value| {
            if minimum == maximum {
                levels[3]
            } else {
                let index = (value - minimum) as usize * (levels.len() - 1)
                    / (maximum - minimum) as usize;
                levels[index]
            }
        })
        .collect()
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

pub fn render_terminal_themed(report: &UsageReport, width: usize, color: bool) -> String {
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
    if report.filters.since.is_some() || report.filters.project.is_some() {
        push_wrapped(
            &mut out,
            &format!(
                "Filters · since {} · project {}",
                report.filters.since.as_deref().unwrap_or("all time"),
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
        "  {} sessions · {} threads\n  {} human messages · {} agent messages\n",
        styled_role(
            &compact_number(report.totals.sessions),
            StyleRole::Count,
            color
        ),
        styled_role(
            &compact_number(report.totals.threads),
            StyleRole::Count,
            color
        ),
        styled_role(
            &compact_number(report.totals.human_messages),
            StyleRole::Count,
            color
        ),
        styled_role(
            &compact_number(report.totals.agent_messages),
            StyleRole::Count,
            color
        )
    ));
    for point in report.activity.iter().rev().take(6).rev() {
        out.push_str(&format!(
            "  {:<10} {:>5} sess · {:>6} msg\n",
            point.bucket,
            compact_number(point.sessions),
            compact_number(point.human_messages)
        ));
    }

    let activity_values = report
        .activity
        .iter()
        .rev()
        .take(16)
        .map(|point| point.human_messages)
        .collect::<Vec<_>>();
    let activity_values = activity_values.into_iter().rev().collect::<Vec<_>>();
    if !activity_values.is_empty() {
        let chart = sparkline(&activity_values, width.saturating_sub(12).min(48));
        out.push_str(&format!(
            "  {} {}\n",
            styled_role("trend", StyleRole::Muted, color),
            styled_role(&chart, StyleRole::Time, color)
        ));
    }

    out.push('\n');
    out.push_str(&styled_role("What changed", StyleRole::Section, color));
    out.push('\n');
    push_wrapped(
        &mut out,
        "trailing 4 weeks vs prior 4 weeks",
        width,
        2,
        StyleRole::Muted,
        color,
    );
    if report.changes.is_empty() {
        push_wrapped(
            &mut out,
            "No change cleared the sample and effect thresholds.",
            width,
            2,
            StyleRole::Muted,
            color,
        );
    } else {
        for insight in &report.changes {
            push_wrapped(
                &mut out,
                &render_change(insight, width),
                width,
                2,
                StyleRole::Title,
                color,
            );
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

    out.push('\n');
    out.push_str(&styled_role("Leading projects", StyleRole::Section, color));
    out.push('\n');
    for row in report.projects.iter().take(5) {
        let path = ellipsize_middle(&row.workspace_path, width.saturating_sub(2).max(10));
        out.push_str(&format!(
            "  {}\n    {} sess · {} msg · {}\n",
            styled_role(&path, StyleRole::Project, color),
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

fn render_change(insight: &ChangeInsight, width: usize) -> String {
    let label = insight
        .subject
        .as_ref()
        .map(|subject| {
            format!(
                "{} in {}",
                insight.metric,
                ellipsize_middle(subject, width.saturating_sub(28).max(12))
            )
        })
        .unwrap_or_else(|| insight.metric.clone());
    match insight.change_percent {
        Some(percent) if percent >= 0 => format!(
            "{label} rose {percent}% · {} now vs {} before",
            compact_number(insight.current),
            compact_number(insight.previous)
        ),
        Some(percent) => format!(
            "{label} fell {}% · {} now vs {} before",
            percent.unsigned_abs(),
            compact_number(insight.current),
            compact_number(insight.previous)
        ),
        None => format!(
            "{label} is new · {} now vs none before",
            compact_number(insight.current)
        ),
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
        assert!(activity_bucket_order("2026-W19") < activity_bucket_order("2026-07-12"));
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
        assert_eq!(sparkline(&[], 10), "");
        assert_eq!(sparkline(&[5, 5, 5], 10), "▄▄▄");
        assert!(sparkline(&[1, 2, 3, 4, 5], 3).chars().count() <= 3);

        assert_eq!(horizontal_bar(5.0, 10.0, 6).chars().count(), 6);
        assert_eq!(horizontal_bar(5.0, 0.0, 4), "░░░░");
        assert_eq!(neutral_bar(-0.5, 1.0, 9).chars().count(), 9);
        assert!(neutral_bar(0.0, 1.0, 9).contains('│'));

        assert_eq!(compact_heatmap(&[], 8), "");
        assert!(compact_heatmap(&[0, 1, 2, 3, 4], 3).chars().count() <= 3);
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
                since: Some("2026-06-01T00:00:00+00:00".to_string()),
                project: None,
                sort: ReportSort::Tokens,
            },
        )
        .expect("compute report");

        assert!(!report.contains_raw_text);
        assert_eq!(report.generated_at, snapshot_generated_at);
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
        assert!(report.topics.is_none());
        assert!(report.sentiment.is_none());
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("sentiment is unavailable")));
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("report snapshot is") && warning.contains("stale")));
        let rendered = render_terminal(&report);
        assert!(rendered.contains("What changed · trailing 4 weeks vs prior 4 weeks"));
        assert!(rendered.contains("Leading projects"));
        assert!(!rendered.contains("Provider mix by month"));
        assert!(!rendered.contains("Sentiment by local hour"));
        let narrow = render_terminal_themed(&report, 40, false);
        assert!(narrow.lines().all(|line| line.chars().count() <= 40));
        assert!(!narrow.contains('\x1b'));
        assert!(render_terminal_themed(&report, 80, true).contains("\x1b["));

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

        let (topics, warning) = report_topics(&store, None, None).expect("topic report");
        let topics = topics.expect("completed topics");
        assert!(warning.is_none());
        assert_eq!(topics.version, "complete");
        assert_eq!(topics.assigned_messages, 2);
        assert_eq!(topics.by_month[0].month, "2026-05");
        assert_eq!(topics.by_project[0].workspace_path, "/repo/gail");

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
        let (topics, warning) = report_topics(&store, None, None).expect("incomplete topics");
        assert!(topics.is_none());
        assert!(warning.expect("warning").contains("1 of 2 labels"));
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

        let (sentiment, warning) = report_sentiment(&store, None, None).expect("sentiment report");
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
            .any(|row| row.workspace_path == "/repo/gail" && row.average == 2.0));
        assert!(sentiment
            .by_hour
            .iter()
            .any(|row| row.average == 4.0 && row.messages == 1));

        let (filtered, _) =
            report_sentiment(&store, None, Some("%gail%")).expect("filtered sentiment report");
        let filtered = filtered.expect("filtered sentiment");
        assert_eq!(filtered.by_week.len(), 11);
        assert_eq!(filtered.by_project.len(), 11);
        assert!(filtered
            .by_project
            .iter()
            .all(|row| row.workspace_path == "/repo/gail"));
    }
}
