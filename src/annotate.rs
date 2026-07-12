use crate::config::{EnrichmentConfig, EnrichmentProvider};
use crate::storage::Store;
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Transaction, TransactionBehavior};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAnnotation {
    pub item_id: String,
    pub axis: String,
    pub score: u8,
    pub model: String,
    pub annotator_version: String,
    pub annotated_at: DateTime<Utc>,
}

pub const SENTIMENT_AXES: [&str; 11] = [
    "happiness",
    "excitement",
    "frustration",
    "satisfaction",
    "curiosity",
    "confidence",
    "urgency",
    "decisiveness",
    "warmth",
    "momentum",
    "playfulness",
];

pub const SENTIMENT_EXCERPT_CHARS: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentPreflight {
    pub provider: String,
    pub model: String,
    pub destination: String,
    pub data_category: String,
    pub record_count: usize,
    pub max_excerpt_chars: usize,
    pub estimated_tokens: usize,
    pub estimated_cost: String,
    pub logging_and_retention: String,
    pub resumability: String,
    pub deletion: String,
}

pub fn insert_annotations(store: &Store, annotations: &[MessageAnnotation]) -> Result<()> {
    for annotation in annotations {
        if !(1..=5).contains(&annotation.score) {
            bail!(
                "annotation score for {}:{} must be between 1 and 5",
                annotation.item_id,
                annotation.axis
            );
        }
    }

    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting annotation batch")?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO message_annotations
                 (item_id, axis, score, model, annotator_version, annotated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for annotation in annotations {
                stmt.execute(params![
                    annotation.item_id,
                    annotation.axis,
                    annotation.score,
                    annotation.model,
                    annotation.annotator_version,
                    annotation.annotated_at.to_rfc3339(),
                ])?;
            }
        }
        tx.commit().context("committing annotation batch")
    })
}

pub fn has_annotation(
    store: &Store,
    item_id: &str,
    axis: &str,
    annotator_version: &str,
) -> Result<bool> {
    store.with_conn(|conn| {
        Ok(conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM message_annotations
                 WHERE item_id = ?1 AND axis = ?2 AND annotator_version = ?3
             )",
            params![item_id, axis, annotator_version],
            |row| row.get(0),
        )?)
    })
}

pub fn latest_annotations(store: &Store) -> Result<Vec<MessageAnnotation>> {
    let rows = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT item_id, axis, score, model, annotator_version, annotated_at
             FROM message_annotations
             WHERE annotator_version = (
                 SELECT annotator_version
                 FROM message_annotations
                 ORDER BY annotated_at DESC, annotator_version DESC
                 LIMIT 1
             )
             ORDER BY item_id, axis",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u8>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;

    rows.into_iter()
        .map(
            |(item_id, axis, score, model, annotator_version, annotated_at)| {
                Ok(MessageAnnotation {
                    item_id,
                    axis,
                    score,
                    model,
                    annotator_version,
                    annotated_at: DateTime::parse_from_rfc3339(&annotated_at)
                        .with_context(|| format!("parsing annotation timestamp {annotated_at}"))?
                        .with_timezone(&Utc),
                })
            },
        )
        .collect()
}

pub trait JsonLlm: Sync {
    fn model(&self) -> &str;
    fn complete_json(&self, system: &str, prompt: &str) -> Result<String>;
}

pub struct ConfiguredJsonLlm {
    provider: EnrichmentProvider,
    url: String,
    api_key: String,
    model: String,
}

impl ConfiguredJsonLlm {
    pub fn from_config(config: &EnrichmentConfig) -> Result<Self> {
        if !config.configured() {
            bail!(
                "enrichment provider is not configured; run `histo config enrichment --provider <openai|anthropic> --base-url <url> --api-key <key> --model <model>`"
            );
        }
        let provider = config.provider.expect("checked provider");
        let base_url = config.base_url.as_ref().expect("checked URL").clone();
        let url = match provider {
            EnrichmentProvider::Openai => chat_completions_url(base_url),
            EnrichmentProvider::Anthropic => messages_url(base_url),
        };
        Ok(Self {
            provider,
            url,
            api_key: config.api_key.as_ref().expect("checked API key").clone(),
            model: config.model.as_ref().expect("checked model").clone(),
        })
    }

    pub fn provider(&self) -> &'static str {
        self.provider.as_str()
    }

    pub fn destination(&self) -> &str {
        &self.url
    }
}

impl JsonLlm for ConfiguredJsonLlm {
    fn model(&self) -> &str {
        &self.model
    }

    fn complete_json(&self, system: &str, prompt: &str) -> Result<String> {
        for attempt in 0..4 {
            let result = match self.provider {
                EnrichmentProvider::Openai => ureq::post(&self.url)
                    .set("Authorization", &format!("Bearer {}", self.api_key))
                    .set("Content-Type", "application/json")
                    .send_json(openai_request(&self.model, system, prompt)),
                EnrichmentProvider::Anthropic => ureq::post(&self.url)
                    .set("x-api-key", &self.api_key)
                    .set("anthropic-version", "2023-06-01")
                    .set("Content-Type", "application/json")
                    .send_json(anthropic_request(&self.model, system, prompt)),
            };
            match result {
                Ok(response) => {
                    let value: Value = response.into_json().context("parsing LLM response")?;
                    return match self.provider {
                        EnrichmentProvider::Openai => chat_content(&value),
                        EnrichmentProvider::Anthropic => anthropic_content(&value),
                    }
                    .map(ToOwned::to_owned);
                }
                Err(ureq::Error::Status(status, _)) if retryable_status(status) && attempt < 3 => {
                    std::thread::sleep(Duration::from_secs(1 << attempt));
                }
                Err(error) => return Err(error).context("calling enrichment endpoint"),
            }
        }
        unreachable!("retry loop returns on its final attempt")
    }
}

fn openai_request(model: &str, system: &str, prompt: &str) -> Value {
    serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt}
        ],
        "response_format": {"type": "json_object"}
    })
}

fn anthropic_request(model: &str, system: &str, prompt: &str) -> Value {
    serde_json::json!({
        "model": model,
        "max_tokens": 4096,
        "system": system,
        "messages": [{"role": "user", "content": prompt}]
    })
}

fn retryable_status(status: u16) -> bool {
    status == 429 || status >= 500
}

fn chat_completions_url(base: String) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/chat/completions") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{base}/chat/completions")
    } else {
        format!("{base}/v1/chat/completions")
    }
}

fn messages_url(base: String) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/messages") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{base}/messages")
    } else {
        format!("{base}/v1/messages")
    }
}

fn chat_content(value: &Value) -> Result<&str> {
    value
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .context("LLM response did not include choices[0].message.content")
}

fn anthropic_content(value: &Value) -> Result<&str> {
    value
        .pointer("/content/0/text")
        .and_then(Value::as_str)
        .context("Anthropic response did not include content[0].text")
}

#[derive(Debug, Clone)]
pub struct AnnotateOptions {
    pub limit: Option<usize>,
    pub batch_size: usize,
    pub concurrency: usize,
    pub annotator_version: String,
}

#[derive(Debug, Clone)]
pub struct AnnotateOutcome {
    pub model: String,
    pub annotator_version: String,
    pub annotated_messages: usize,
    pub skipped_messages: usize,
    pub scores_written: usize,
    pub pending_messages: usize,
}

pub fn sentiment_preflight(
    store: &Store,
    llm: &ConfiguredJsonLlm,
    options: &AnnotateOptions,
) -> Result<EnrichmentPreflight> {
    let inputs = missing_annotation_inputs(
        store,
        &options.annotator_version,
        options.limit.unwrap_or(usize::MAX),
    )?;
    let excerpt_chars = inputs
        .iter()
        .map(|input| input.text.chars().count().min(SENTIMENT_EXCERPT_CHARS))
        .sum::<usize>();
    Ok(EnrichmentPreflight {
        provider: llm.provider().to_string(),
        model: llm.model().to_string(),
        destination: llm.destination().to_string(),
        data_category: "human-authored coding-agent messages for sentiment scoring".to_string(),
        record_count: inputs.len(),
        max_excerpt_chars: SENTIMENT_EXCERPT_CHARS,
        estimated_tokens: excerpt_chars.saturating_add(inputs.len() * 300) / 4,
        estimated_cost: "unknown; check the configured provider's current pricing".to_string(),
        logging_and_retention: "unknown; governed by the configured provider account".to_string(),
        resumability: format!(
            "yes; completed records for version {} are skipped",
            options.annotator_version
        ),
        deletion: format!(
            "histo enrich delete --kind sentiment --version {}",
            options.annotator_version
        ),
    })
}

pub fn record_enrichment_run(
    store: &Store,
    kind: &str,
    version: &str,
    preflight: &EnrichmentPreflight,
) -> Result<()> {
    if kind.trim().is_empty() || version.trim().is_empty() {
        bail!("enrichment kind and version must not be empty");
    }
    store.with_conn(|conn| {
        conn.execute(
            "INSERT OR IGNORE INTO enrichment_runs
             (kind, version, provider, model, destination, data_scope, max_excerpt_chars, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                kind,
                version,
                preflight.provider,
                preflight.model,
                preflight.destination,
                preflight.data_category,
                preflight.max_excerpt_chars as i64,
                Utc::now().to_rfc3339(),
            ],
        )?;
        let stored = conn.query_row(
            "SELECT provider, model, destination, data_scope, max_excerpt_chars
             FROM enrichment_runs WHERE kind = ?1 AND version = ?2",
            params![kind, version],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?;
        let requested = (
            preflight.provider.clone(),
            preflight.model.clone(),
            preflight.destination.clone(),
            preflight.data_category.clone(),
            preflight.max_excerpt_chars as i64,
        );
        if stored != requested {
            bail!(
                "enrichment version {version} already belongs to different provider metadata; choose a new version"
            );
        }
        Ok(())
    })
}

struct AnnotationInput {
    item_id: String,
    text: String,
    rule: String,
    usable: String,
}

#[derive(Debug, Deserialize)]
struct AnnotationBatchResponse {
    items: Vec<AnnotationScores>,
}

#[derive(Debug, Deserialize)]
struct AnnotationScores {
    id: String,
    scores: Vec<u8>,
}

pub fn annotate_messages(
    store: &Store,
    llm: &dyn JsonLlm,
    options: &AnnotateOptions,
) -> Result<AnnotateOutcome> {
    if options.batch_size == 0 {
        bail!("annotation batch size must be greater than zero");
    }
    if options.concurrency == 0 {
        bail!("annotation concurrency must be greater than zero");
    }
    if options.annotator_version.trim().is_empty() {
        bail!("annotator version must not be empty");
    }
    let total = annotatable_message_count(store)?;
    let complete_before = complete_message_count(store, &options.annotator_version)?;
    let rows_before = annotation_row_count(store, &options.annotator_version)?;
    let target = options.limit.unwrap_or(usize::MAX);
    let mut annotated_messages = 0;
    while annotated_messages < target {
        let round_limit = options
            .batch_size
            .saturating_mul(options.concurrency)
            .min(target - annotated_messages);
        let inputs = missing_annotation_inputs(store, &options.annotator_version, round_limit)?;
        if inputs.is_empty() {
            break;
        }
        let batch_results = std::thread::scope(|scope| {
            let handles = inputs
                .chunks(options.batch_size)
                .map(|batch| scope.spawn(move || request_annotation_batch(llm, batch)))
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(anyhow!("sentiment annotation worker panicked")))
                })
                .collect::<Vec<_>>()
        });
        let annotated_at = Utc::now();
        let mut rows = Vec::with_capacity(inputs.len() * SENTIMENT_AXES.len());
        let mut completed_in_round = 0;
        let mut first_error = None;
        for (batch, result) in inputs.chunks(options.batch_size).zip(batch_results) {
            match result {
                Ok(scores) => {
                    for scores in scores {
                        let index = scores
                            .id
                            .parse::<usize>()
                            .context("annotation response item id was not an ordinal")?;
                        let input = batch
                            .get(index)
                            .context("annotation response item ordinal was out of range")?;
                        rows.extend(SENTIMENT_AXES.iter().enumerate().map(
                            |(score_index, axis)| MessageAnnotation {
                                item_id: input.item_id.clone(),
                                axis: (*axis).to_string(),
                                score: scores.scores[score_index],
                                model: llm.model().to_string(),
                                annotator_version: options.annotator_version.clone(),
                                annotated_at,
                            },
                        ));
                    }
                    completed_in_round += batch.len();
                }
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        insert_annotations(store, &rows)?;
        annotated_messages += completed_in_round;
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    let complete_after = complete_message_count(store, &options.annotator_version)?;
    let rows_after = annotation_row_count(store, &options.annotator_version)?;
    Ok(AnnotateOutcome {
        model: llm.model().to_string(),
        annotator_version: options.annotator_version.clone(),
        annotated_messages,
        skipped_messages: complete_before,
        scores_written: rows_after.saturating_sub(rows_before),
        pending_messages: total.saturating_sub(complete_after),
    })
}

fn request_annotation_batch(
    llm: &dyn JsonLlm,
    inputs: &[AnnotationInput],
) -> Result<Vec<AnnotationScores>> {
    let prompt_items = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let text = if input.usable == "strip_wrapper" {
                crate::provenance::strip_human_wrapper(&input.text, &input.rule)
            } else {
                input.text.clone()
            };
            serde_json::json!({
                "id": index.to_string(),
                "message": text.chars().take(SENTIMENT_EXCERPT_CHARS).collect::<String>()
            })
        })
        .collect::<Vec<_>>();
    let response = llm.complete_json(
        sentiment_system_prompt(),
        &Value::Array(prompt_items).to_string(),
    )?;
    let expected_ids = (0..inputs.len())
        .map(|index| index.to_string())
        .collect::<Vec<_>>();
    let expected_id_refs = expected_ids.iter().map(String::as_str).collect::<Vec<_>>();
    parse_annotation_response(&response, &expected_id_refs)
}

fn sentiment_system_prompt() -> &'static str {
    r#"Score each message independently from 1 to 5 on every axis. Use 3 for neutral, mixed, or unclear. Score array order: happiness (negative/unhappy to positive/happy); excitement (calm/flat to enthusiastic/energized); frustration (unbothered to strongly frustrated); satisfaction (dissatisfied with results to pleased); curiosity (purely directive to exploratory/inquisitive); confidence (uncertain/confused to confident); urgency (leisurely to time-pressured); decisiveness (tentative to firmly decided); warmth (cold/transactional to warm/appreciative); momentum (stuck/blocked to flowing/making progress); playfulness (serious/literal to playful/humorous). Do not infer traits or mental health. Return JSON only: {"items":[{"id":"exact input id","scores":[1,1,1,1,1,1,1,1,1,1,1]}]}."#
}

fn parse_annotation_response(
    response: &str,
    expected_ids: &[&str],
) -> Result<Vec<AnnotationScores>> {
    let parsed: AnnotationBatchResponse =
        serde_json::from_str(response).context("parsing sentiment annotation response")?;
    let expected = expected_ids.iter().copied().collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    for item in &parsed.items {
        if !expected.contains(item.id.as_str()) || !seen.insert(item.id.as_str()) {
            bail!("annotation response contained an unexpected or duplicate item id");
        }
        if item.scores.len() != SENTIMENT_AXES.len() {
            bail!("annotation response must contain all 11 approved axes");
        }
        for (axis, score) in SENTIMENT_AXES.iter().zip(&item.scores) {
            if !(1..=5).contains(score) {
                bail!("annotation score for {axis} must be between 1 and 5");
            }
        }
    }
    if seen.len() != expected.len() {
        bail!("annotation response omitted one or more input messages");
    }
    Ok(parsed.items)
}

fn axes_sql() -> String {
    SENTIMENT_AXES
        .iter()
        .map(|axis| format!("'{axis}'"))
        .collect::<Vec<_>>()
        .join(",")
}

fn annotatable_message_count(store: &Store) -> Result<usize> {
    store.with_conn(|conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM message_provenance
             WHERE authored_by = 'human' AND sentiment_usable IN ('yes', 'strip_wrapper')",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize)
    })
}

fn complete_message_count(store: &Store, version: &str) -> Result<usize> {
    store.with_conn(|conn| {
        let sql = format!(
            "SELECT COUNT(*) FROM message_provenance p
             WHERE p.authored_by = 'human' AND p.sentiment_usable IN ('yes', 'strip_wrapper')
               AND (SELECT COUNT(DISTINCT a.axis) FROM message_annotations a
                    WHERE a.item_id = p.item_id AND a.annotator_version = ?1
                      AND a.axis IN ({})) = {}",
            axes_sql(),
            SENTIMENT_AXES.len()
        );
        Ok(conn.query_row(&sql, params![version], |row| row.get::<_, i64>(0))? as usize)
    })
}

fn annotation_row_count(store: &Store, version: &str) -> Result<usize> {
    store.with_conn(|conn| {
        let sql = format!(
            "SELECT COUNT(*) FROM message_annotations
             WHERE annotator_version = ?1 AND axis IN ({})",
            axes_sql()
        );
        Ok(conn.query_row(&sql, params![version], |row| row.get::<_, i64>(0))? as usize)
    })
}

fn missing_annotation_inputs(
    store: &Store,
    version: &str,
    limit: usize,
) -> Result<Vec<AnnotationInput>> {
    store.with_conn(|conn| {
        let sql = format!(
            "SELECT hi.id, hi.text, p.rule, p.sentiment_usable
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             LEFT JOIN message_annotations a
               ON a.item_id = p.item_id AND a.annotator_version = ?1
              AND a.axis IN ({})
             WHERE p.authored_by = 'human' AND p.sentiment_usable IN ('yes', 'strip_wrapper')
             GROUP BY hi.id
             HAVING COUNT(DISTINCT a.axis) < {}
             ORDER BY hi.rowid
             LIMIT ?2",
            axes_sql(),
            SENTIMENT_AXES.len()
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![version, limit as i64], |row| {
            Ok(AnnotationInput {
                item_id: row.get(0)?,
                text: row.get(1)?,
                rule: row.get(2)?,
                usable: row.get(3)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn annotation(axis: &str, score: u8, version: &str, annotated_at: &str) -> MessageAnnotation {
        MessageAnnotation {
            item_id: "item_1".to_string(),
            axis: axis.to_string(),
            score,
            model: "test-model".to_string(),
            annotator_version: version.to_string(),
            annotated_at: DateTime::parse_from_rfc3339(annotated_at)
                .expect("timestamp")
                .with_timezone(&Utc),
        }
    }

    #[test]
    fn annotation_storage_is_versioned_resumable_and_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let first = vec![
            annotation("frustration", 2, "v1", "2026-07-11T00:00:00Z"),
            annotation("energy", 4, "v1", "2026-07-11T00:00:00Z"),
        ];

        insert_annotations(&store, &first).expect("insert first version");
        insert_annotations(&store, &first).expect("repeat first version");
        assert!(has_annotation(&store, "item_1", "energy", "v1").expect("check annotation"));
        assert!(!has_annotation(&store, "item_1", "energy", "v2").expect("check version"));

        let second = vec![
            annotation("energy", 5, "v2", "2026-07-12T00:00:00Z"),
            annotation("frustration", 3, "v2", "2026-07-12T00:00:00Z"),
        ];
        insert_annotations(&store, &second).expect("insert second version");

        let count = store
            .with_conn(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM message_annotations", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                )
            })
            .expect("count annotations");
        assert_eq!(count, 4);
        assert_eq!(
            latest_annotations(&store).expect("latest annotations"),
            second
        );
    }

    #[test]
    fn annotation_scores_must_be_between_one_and_five() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let invalid = annotation("energy", 0, "v1", "2026-07-12T00:00:00Z");

        let error = insert_annotations(&store, &[invalid]).expect_err("reject invalid score");
        assert!(error.to_string().contains("between 1 and 5"));
    }

    #[test]
    fn enrichment_run_provenance_is_versioned_and_immutable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let mut preflight = EnrichmentPreflight {
            provider: "openai".to_string(),
            model: "gpt-test".to_string(),
            destination: "https://example.test/v1/chat/completions".to_string(),
            data_category: "human messages".to_string(),
            record_count: 2,
            max_excerpt_chars: 2_000,
            estimated_tokens: 100,
            estimated_cost: "unknown".to_string(),
            logging_and_retention: "unknown".to_string(),
            resumability: "yes".to_string(),
            deletion: "delete command".to_string(),
        };

        record_enrichment_run(&store, "sentiment", "v1", &preflight)
            .expect("record provenance");
        record_enrichment_run(&store, "sentiment", "v1", &preflight)
            .expect("repeat same provenance");
        preflight.model = "different-model".to_string();
        let error = record_enrichment_run(&store, "sentiment", "v1", &preflight)
            .expect_err("version metadata must not change");
        assert!(error.to_string().contains("different provider metadata"));
    }

    #[test]
    fn openai_response_content_and_base_urls_parse() {
        let response = serde_json::json!({
            "choices": [{"message": {"content": "{\"label\":\"Databases\"}"}}]
        });
        assert_eq!(
            chat_content(&response).expect("chat content"),
            "{\"label\":\"Databases\"}"
        );
        assert_eq!(
            chat_completions_url("http://localhost:4000".to_string()),
            "http://localhost:4000/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_url("http://localhost:4000/v1/chat/completions".to_string()),
            "http://localhost:4000/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_url("http://localhost:4000/v1".to_string()),
            "http://localhost:4000/v1/chat/completions"
        );
        assert!(retryable_status(429));
        assert!(retryable_status(503));
        assert!(!retryable_status(400));
    }

    #[test]
    fn provider_wire_formats_normalize_json_content() {
        let openai = openai_request("gpt-test", "system", "prompt");
        assert_eq!(openai["model"], "gpt-test");
        assert_eq!(openai["messages"][0]["role"], "system");
        assert_eq!(openai["messages"][1]["content"], "prompt");
        assert_eq!(openai["response_format"]["type"], "json_object");

        let anthropic = anthropic_request("claude-test", "system", "prompt");
        assert_eq!(anthropic["model"], "claude-test");
        assert_eq!(anthropic["system"], "system");
        assert_eq!(anthropic["messages"][0]["role"], "user");
        assert_eq!(anthropic["messages"][0]["content"], "prompt");
        let response = serde_json::json!({
            "content": [{"type": "text", "text": "{\"label\":\"Databases\"}"}]
        });
        assert_eq!(
            anthropic_content(&response).expect("Anthropic content"),
            "{\"label\":\"Databases\"}"
        );
        assert_eq!(
            messages_url("https://api.anthropic.com".to_string()),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            messages_url("https://example.test/v1".to_string()),
            "https://example.test/v1/messages"
        );
    }

    #[test]
    fn configured_client_refuses_partial_or_implicit_provider_settings() {
        let error = match ConfiguredJsonLlm::from_config(&EnrichmentConfig::default()) {
            Ok(_) => panic!("missing provider config must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("histo config enrichment"));
    }

    struct MockSentimentLlm {
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        prompts: Mutex<Vec<String>>,
    }

    impl JsonLlm for MockSentimentLlm {
        fn model(&self) -> &str {
            "mock-sentiment"
        }

        fn complete_json(&self, _system: &str, prompt: &str) -> Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            self.prompts
                .lock()
                .expect("prompts")
                .push(prompt.to_string());
            let inputs: Value = serde_json::from_str(prompt)?;
            let items = inputs
                .as_array()
                .expect("prompt items")
                .iter()
                .map(|input| {
                    let scores = vec![3; SENTIMENT_AXES.len()];
                    serde_json::json!({"id": input["id"], "scores": scores})
                })
                .collect::<Vec<_>>();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(serde_json::json!({"items": items}).to_string())
        }
    }

    #[test]
    fn sentiment_pipeline_filters_strips_resumes_and_versions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('wrapped', 'event_1', 'session', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', '<image name="x"> useful caption', 'hash_1',
                       1, 'required', '{}', 'item_hash_1'),
                      ('plain', 'event_2', 'session', 'source', 'machine', 'codex', 1, 0,
                       'conversation', 'user', 'plain human message', 'hash_2',
                       1, 'required', '{}', 'item_hash_2'),
                      ('plain_2', 'event_4', 'session', 'source', 'machine', 'codex', 2, 0,
                       'conversation', 'user', 'another human message', 'hash_4',
                       1, 'required', '{}', 'item_hash_4'),
                      ('agent', 'event_3', 'session', 'source', 'machine', 'codex', 2, 0,
                       'conversation', 'user', 'agent text', 'hash_3',
                       1, 'required', '{}', 'item_hash_3');

                    INSERT INTO message_provenance
                      (item_id, session_id, source_kind, authored_by, sentiment_usable, rule)
                    VALUES
                      ('wrapped', 'session', 'codex', 'human', 'strip_wrapper', 'tag.image'),
                      ('plain', 'session', 'codex', 'human', 'yes', 'default.human'),
                      ('plain_2', 'session', 'codex', 'human', 'yes', 'default.human'),
                      ('agent', 'session', 'codex', 'agent', 'no', 'session.subagent');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert sentiment fixtures");
        let llm = MockSentimentLlm {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            prompts: Mutex::new(Vec::new()),
        };

        let first = annotate_messages(
            &store,
            &llm,
            &AnnotateOptions {
                limit: Some(1),
                batch_size: 10,
                concurrency: 1,
                annotator_version: "v1".to_string(),
            },
        )
        .expect("annotate trial");
        assert_eq!(
            (
                first.annotated_messages,
                first.scores_written,
                first.pending_messages
            ),
            (1, 11, 2)
        );
        let prompt = llm.prompts.lock().expect("prompts")[0].clone();
        assert!(prompt.contains("useful caption"));
        assert!(!prompt.contains("<image"));

        let second = annotate_messages(
            &store,
            &llm,
            &AnnotateOptions {
                limit: None,
                batch_size: 1,
                concurrency: 2,
                annotator_version: "v1".to_string(),
            },
        )
        .expect("resume annotation");
        assert_eq!(
            (
                second.annotated_messages,
                second.skipped_messages,
                second.pending_messages
            ),
            (2, 1, 0)
        );
        assert_eq!(llm.max_active.load(Ordering::SeqCst), 2);
        let repeated = annotate_messages(
            &store,
            &llm,
            &AnnotateOptions {
                limit: None,
                batch_size: 10,
                concurrency: 1,
                annotator_version: "v1".to_string(),
            },
        )
        .expect("repeat annotation");
        assert_eq!(repeated.annotated_messages, 0);
        assert_eq!(llm.calls.load(Ordering::SeqCst), 3);

        let next_version = annotate_messages(
            &store,
            &llm,
            &AnnotateOptions {
                limit: None,
                batch_size: 1,
                concurrency: 2,
                annotator_version: "v2".to_string(),
            },
        )
        .expect("annotate next version");
        assert_eq!(next_version.scores_written, 33);
        assert_eq!(llm.calls.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn sentiment_response_requires_every_approved_axis() {
        let scores = vec![3; SENTIMENT_AXES.len() - 1];
        let response = serde_json::json!({"items": [{"id": "item", "scores": scores}]});
        let error = parse_annotation_response(&response.to_string(), &["item"])
            .expect_err("missing axis should fail");
        assert!(error.to_string().contains("all 11 approved axes"));
    }
}
