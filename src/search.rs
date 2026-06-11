use crate::embed::{Embedder, EmbedderConfig};
use crate::memory::MemorySample;
use crate::storage::{
    HistoryItemEmbeddingCursor, HistoryItemForEmbedding, ImportDelta, SearchRow, Store,
    VectorSearchRow,
};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const RRF_K: f64 = 60.0;
const LEXICAL_RRF_WEIGHT: f64 = 1.0;
const NON_USER_LEXICAL_RRF_WEIGHT: f64 = 0.88;
const SEMANTIC_RRF_WEIGHT: f64 = 0.98;
const BACKEND_LIMIT_MULTIPLIER: usize = 50;
const BACKEND_MIN_LIMIT: usize = 200;
const SQLITE_VEC_MAX_K: usize = 4096;
const SEMANTIC_CANDIDATE_MIN_CHARS: usize = 80;
const SEMANTIC_CANDIDATE_MIN_TERMS: usize = 8;
const EMBEDDING_BATCH_START: usize = 64;
const EMBEDDING_BATCH_MAX: usize = 64;
const EMBEDDING_BATCH_MIN: usize = 1;
const EMBEDDING_TEXT_MAX_CHARS: usize = 8192;
const EMBEDDING_CRITICAL_AVAILABLE_BYTES: u64 = 768 * 1024 * 1024;
const EMBEDDING_LOW_AVAILABLE_BYTES: u64 = 1536 * 1024 * 1024;
const EMBEDDING_RSS_SPIKE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Default, Serialize)]
pub struct EmbeddingRefresh {
    pub embedded: usize,
    pub vectors_indexed: usize,
    pub degraded_reason: Option<String>,
    pub pending: usize,
    pub deferred_reason: Option<String>,
    pub batch_size_reductions: usize,
    pub final_batch_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum EmbeddingProgress {
    LoadingModel {
        model_id: String,
    },
    Batch {
        embedded: usize,
        pending: usize,
        batch_size: usize,
        reductions: usize,
        available_gib: Option<f64>,
    },
    Deferred {
        pending: usize,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResponse {
    pub degraded_reason: Option<String>,
    pub results: Vec<SearchResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    Relevance,
    Newest,
    Oldest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Hybrid,
    Lexical,
    Semantic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryTier {
    Conversation,
    Tool,
    Raw,
}

impl HistoryTier {
    pub fn as_str(self) -> &'static str {
        match self {
            HistoryTier::Conversation => "conversation",
            HistoryTier::Tool => "tool",
            HistoryTier::Raw => "raw",
        }
    }

    fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "conversation" | "conversations" | "default" => Ok(Self::Conversation),
            "tool" | "tools" => Ok(Self::Tool),
            "raw" | "full" => Ok(Self::Raw),
            other => bail!(
                "unknown search corpus tier '{other}'. Available tiers: conversation,tool,raw"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchCorpus {
    tiers: Vec<HistoryTier>,
}

impl Default for SearchCorpus {
    fn default() -> Self {
        Self {
            tiers: vec![HistoryTier::Conversation],
        }
    }
}

impl SearchCorpus {
    pub fn parse(input: &str) -> Result<Self> {
        let mut tiers = Vec::new();
        for raw in input.split(',') {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let tier = HistoryTier::parse(raw)?;
            if !tiers.contains(&tier) {
                tiers.push(tier);
            }
        }
        if tiers.is_empty() {
            bail!("search corpus cannot be empty");
        }
        Ok(Self { tiers })
    }

    pub fn conversation_with_tools() -> Self {
        Self {
            tiers: vec![HistoryTier::Conversation, HistoryTier::Tool],
        }
    }

    pub fn raw() -> Self {
        Self {
            tiers: vec![HistoryTier::Raw],
        }
    }

    pub fn tier_names(&self) -> Vec<&'static str> {
        self.tiers.iter().map(|tier| tier.as_str()).collect()
    }

    pub fn as_csv(&self) -> String {
        self.tiers
            .iter()
            .map(|tier| tier.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl Default for SearchMode {
    fn default() -> Self {
        Self::Hybrid
    }
}

impl SearchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchMode::Hybrid => "hybrid",
            SearchMode::Lexical => "lexical",
            SearchMode::Semantic => "semantic",
        }
    }

    fn includes_lexical(self) -> bool {
        matches!(self, SearchMode::Hybrid | SearchMode::Lexical)
    }

    fn includes_semantic(self) -> bool {
        matches!(self, SearchMode::Hybrid | SearchMode::Semantic)
    }
}

#[derive(Debug, Clone)]
pub struct SearchOptions {
    pub limit: usize,
    pub sort: SortMode,
    pub mode: SearchMode,
    pub recency_bias: f64,
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    pub machine_id: Option<String>,
    pub machine_id_prefix: Option<String>,
    pub workspace_scope: Option<String>,
    pub corpus: SearchCorpus,
    pub show_duplicates: bool,
}

impl SearchOptions {
    pub fn new(limit: usize, sort: SortMode, recency_bias: f64) -> Self {
        Self {
            limit,
            sort,
            mode: SearchMode::Hybrid,
            recency_bias: recency_bias.clamp(0.0, 1.0),
            after: None,
            before: None,
            machine_id: None,
            machine_id_prefix: None,
            workspace_scope: None,
            corpus: SearchCorpus::default(),
            show_duplicates: false,
        }
    }

    pub fn with_mode(mut self, mode: SearchMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_time_window(
        mut self,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Self {
        self.after = after;
        self.before = before;
        self
    }

    pub fn with_machine_filter(
        mut self,
        machine_id: Option<String>,
        hostname: Option<String>,
    ) -> Self {
        self.machine_id = machine_id.filter(|value| !value.trim().is_empty());
        self.machine_id_prefix = hostname
            .filter(|value| !value.trim().is_empty())
            .map(|value| machine_id_prefix_for_hostname(&value));
        self
    }

    pub fn with_workspace_scope(mut self, workspace_scope: Option<String>) -> Self {
        self.workspace_scope = workspace_scope.filter(|value| !value.trim().is_empty());
        self
    }

    pub fn with_corpus(mut self, corpus: SearchCorpus) -> Self {
        self.corpus = corpus;
        self
    }

    pub fn with_show_duplicates(mut self, show_duplicates: bool) -> Self {
        self.show_duplicates = show_duplicates;
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub history_item_id: Option<String>,
    pub match_type: MatchType,
    pub event_id: String,
    pub session_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub tier: Option<String>,
    pub kind: String,
    pub score: f64,
    pub lexical_rank: Option<usize>,
    pub semantic_rank: Option<usize>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub session_title: Option<String>,
    pub workspace_values: Vec<String>,
    pub snippet: String,
    pub duplicate_group: Vec<DuplicateSearchMember>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DuplicateSearchMember {
    pub history_item_id: Option<String>,
    pub match_type: MatchType,
    pub event_id: String,
    pub session_id: String,
    pub machine_id: String,
    pub source_kind: String,
    pub tier: Option<String>,
    pub kind: String,
    pub score: f64,
    pub lexical_rank: Option<usize>,
    pub semantic_rank: Option<usize>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub session_title: Option<String>,
    pub workspace_values: Vec<String>,
    pub snippet: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    Lexical,
    Semantic,
    Hybrid,
}

pub fn refresh(store: &Store) -> Result<usize> {
    let indexed = refresh_search_index(store)?;
    store.refresh_history_items()?;
    Ok(indexed)
}

pub fn refresh_search_index(store: &Store) -> Result<usize> {
    store.refresh_search_index(
        crate::embed::HashEmbedder::MODEL_ID,
        crate::embed::HashEmbedder::DIMS,
        crate::embed::hash_embed,
    )
}

pub fn refresh_incremental(store: &Store, delta: &ImportDelta) -> Result<usize> {
    let search_event_ids = delta.search_index_event_ids();
    let mut indexed = store.refresh_search_index_for_events(
        crate::embed::HashEmbedder::MODEL_ID,
        crate::embed::HashEmbedder::DIMS,
        &search_event_ids,
        crate::embed::hash_embed,
    )?;
    if store.search_index_needs_repair(crate::embed::HashEmbedder::MODEL_ID)? {
        indexed = refresh_search_index(store)?;
    }
    if store.history_items_projection_ready()? {
        store.refresh_history_items_for_events(&delta.touched_events)?;
    } else {
        store.refresh_history_items()?;
    }
    Ok(indexed)
}

pub fn refresh_embeddings(
    store: &Store,
    machine_id: &str,
    embedder: Option<&dyn Embedder>,
    degraded_reason: Option<String>,
) -> Result<EmbeddingRefresh> {
    let Some(embedder) = embedder else {
        return Ok(EmbeddingRefresh {
            vectors_indexed: store.refresh_vector_projection()?,
            degraded_reason,
            ..EmbeddingRefresh::default()
        });
    };
    if !embedder.is_semantic() {
        return Ok(EmbeddingRefresh {
            vectors_indexed: store.refresh_vector_projection()?,
            pending: store.history_items_missing_required_embedding_count(embedder.model_id())?,
            degraded_reason: Some(
                "embedder is not semantic; skipping durable embeddings".to_string(),
            ),
            ..EmbeddingRefresh::default()
        });
    }

    refresh_embeddings_loaded(
        store,
        machine_id,
        embedder,
        degraded_reason,
        EmbeddingScope::All,
        Vec::new(),
        |_| {},
    )
}

pub fn refresh_embeddings_repair_with_progress(
    store: &Store,
    machine_id: &str,
    embedder_config: &EmbedderConfig,
    mut progress: impl FnMut(&EmbeddingProgress),
) -> Result<EmbeddingRefresh> {
    let status = embedder_config.status_without_loading();
    let Some(model_id) = status.model_id.as_deref() else {
        return Ok(EmbeddingRefresh {
            vectors_indexed: store.refresh_vector_projection()?,
            degraded_reason: status.degraded_reason,
            ..EmbeddingRefresh::default()
        });
    };
    if !status.semantic {
        return Ok(EmbeddingRefresh {
            vectors_indexed: store.refresh_vector_projection()?,
            pending: store.history_items_missing_required_embedding_count(model_id)?,
            degraded_reason: status.degraded_reason.or_else(|| {
                Some("embedder is not semantic; skipping durable embeddings".to_string())
            }),
            ..EmbeddingRefresh::default()
        });
    }
    if let Some(reason) = memory_model_load_defer_reason(crate::memory::sample_memory()) {
        let pending = store.history_items_missing_required_embedding_count(model_id)?;
        progress(&EmbeddingProgress::Deferred {
            pending,
            reason: reason.clone(),
        });
        return Ok(EmbeddingRefresh {
            vectors_indexed: store.refresh_vector_projection()?,
            degraded_reason: status.degraded_reason,
            pending,
            deferred_reason: Some(reason),
            final_batch_size: Some(EMBEDDING_BATCH_START),
            ..EmbeddingRefresh::default()
        });
    }
    progress(&EmbeddingProgress::LoadingModel {
        model_id: model_id.to_string(),
    });
    let embedder = embedder_config.load()?;
    if !embedder.is_semantic() {
        return Ok(EmbeddingRefresh {
            vectors_indexed: store.refresh_vector_projection()?,
            pending: store.history_items_missing_required_embedding_count(embedder.model_id())?,
            degraded_reason: Some(
                "embedder is not semantic; skipping durable embeddings".to_string(),
            ),
            ..EmbeddingRefresh::default()
        });
    }
    refresh_embeddings_loaded(
        store,
        machine_id,
        embedder.as_ref(),
        status.degraded_reason,
        EmbeddingScope::All,
        Vec::new(),
        progress,
    )
}

pub fn refresh_embeddings_incremental(
    store: &Store,
    machine_id: &str,
    embedder_config: &EmbedderConfig,
    delta: &ImportDelta,
) -> Result<EmbeddingRefresh> {
    refresh_embeddings_incremental_with_progress(store, machine_id, embedder_config, delta, |_| {})
}

pub fn refresh_embeddings_incremental_with_progress(
    store: &Store,
    machine_id: &str,
    embedder_config: &EmbedderConfig,
    delta: &ImportDelta,
    mut progress: impl FnMut(&EmbeddingProgress),
) -> Result<EmbeddingRefresh> {
    let status = embedder_config.status_without_loading();
    let vector_embedding_ids = delta.inserted_embeddings.clone();
    let Some(model_id) = status.model_id.as_deref() else {
        return Ok(EmbeddingRefresh {
            vectors_indexed: refresh_vector_projection_incremental(store, &vector_embedding_ids)?,
            degraded_reason: status.degraded_reason,
            ..EmbeddingRefresh::default()
        });
    };

    if !status.semantic {
        return Ok(EmbeddingRefresh {
            vectors_indexed: refresh_vector_projection_incremental(store, &vector_embedding_ids)?,
            degraded_reason: status.degraded_reason.or_else(|| {
                Some("embedder is not semantic; skipping durable embeddings".to_string())
            }),
            ..EmbeddingRefresh::default()
        });
    }

    let mut first_units = store.history_items_missing_required_embedding_for_events(
        model_id,
        &delta.inserted_events,
        EMBEDDING_BATCH_START,
    )?;
    if first_units.is_empty() {
        if store.history_items_need_required_embedding(model_id)? {
            return refresh_embeddings_repair_with_progress(
                store,
                machine_id,
                embedder_config,
                progress,
            );
        }
        return Ok(EmbeddingRefresh {
            vectors_indexed: refresh_vector_projection_incremental(store, &vector_embedding_ids)?,
            degraded_reason: status.degraded_reason,
            ..EmbeddingRefresh::default()
        });
    }

    if let Some(reason) = memory_model_load_defer_reason(crate::memory::sample_memory()) {
        let pending = store.history_items_missing_required_embedding_count(model_id)?;
        progress(&EmbeddingProgress::Deferred {
            pending,
            reason: reason.clone(),
        });
        return Ok(EmbeddingRefresh {
            vectors_indexed: refresh_vector_projection_incremental(store, &vector_embedding_ids)?,
            degraded_reason: status.degraded_reason,
            pending,
            deferred_reason: Some(reason),
            final_batch_size: Some(EMBEDDING_BATCH_START),
            ..EmbeddingRefresh::default()
        });
    }
    progress(&EmbeddingProgress::LoadingModel {
        model_id: model_id.to_string(),
    });
    let embedder = embedder_config.load()?;
    if !embedder.is_semantic() {
        return Ok(EmbeddingRefresh {
            vectors_indexed: store
                .refresh_vector_projection_for_embeddings(&vector_embedding_ids)?,
            pending: store.history_items_missing_required_embedding_count(embedder.model_id())?,
            degraded_reason: Some(
                "embedder is not semantic; skipping durable embeddings".to_string(),
            ),
            ..EmbeddingRefresh::default()
        });
    }

    let refresh = refresh_embeddings_loaded(
        store,
        machine_id,
        embedder.as_ref(),
        status.degraded_reason.clone(),
        EmbeddingScope::Delta {
            event_ids: &delta.inserted_events,
            first_units: Some(std::mem::take(&mut first_units)),
        },
        vector_embedding_ids,
        &mut progress,
    )?;

    if refresh.pending > 0 && refresh.deferred_reason.is_none() {
        let repair = refresh_embeddings_loaded(
            store,
            machine_id,
            embedder.as_ref(),
            status.degraded_reason,
            EmbeddingScope::All,
            Vec::new(),
            &mut progress,
        )?;
        return Ok(EmbeddingRefresh {
            embedded: refresh.embedded + repair.embedded,
            vectors_indexed: repair.vectors_indexed,
            degraded_reason: repair.degraded_reason,
            pending: repair.pending,
            deferred_reason: repair.deferred_reason,
            batch_size_reductions: refresh.batch_size_reductions + repair.batch_size_reductions,
            final_batch_size: repair.final_batch_size,
        });
    }

    Ok(refresh)
}

fn refresh_vector_projection_incremental(store: &Store, embedding_ids: &[String]) -> Result<usize> {
    let indexed = store.refresh_vector_projection_for_embeddings(embedding_ids)?;
    if store.vector_projection_needs_repair()? {
        store.refresh_vector_projection()
    } else {
        Ok(indexed)
    }
}

enum EmbeddingScope<'a> {
    All,
    Delta {
        event_ids: &'a [String],
        first_units: Option<Vec<HistoryItemForEmbedding>>,
    },
}

fn refresh_embeddings_loaded(
    store: &Store,
    machine_id: &str,
    embedder: &dyn Embedder,
    degraded_reason: Option<String>,
    mut scope: EmbeddingScope<'_>,
    mut vector_embedding_ids: Vec<String>,
    mut progress: impl FnMut(&EmbeddingProgress),
) -> Result<EmbeddingRefresh> {
    let mut embedded = 0;
    let mut controller = AdaptiveEmbeddingBatch::new();
    let mut full_cursor: Option<HistoryItemEmbeddingCursor> = None;
    let mut pending_hint: Option<usize> = None;
    loop {
        if let Some(reason) = controller.defer_reason(crate::memory::sample_memory()) {
            let pending = match pending_hint {
                Some(pending) => pending,
                None => {
                    store.history_items_missing_required_embedding_count(embedder.model_id())?
                }
            };
            progress(&EmbeddingProgress::Deferred {
                pending,
                reason: reason.clone(),
            });
            return Ok(EmbeddingRefresh {
                embedded,
                vectors_indexed: refresh_vector_projection_incremental(
                    store,
                    &vector_embedding_ids,
                )?,
                degraded_reason,
                pending,
                deferred_reason: Some(reason),
                batch_size_reductions: controller.reductions,
                final_batch_size: Some(controller.batch_size),
            });
        }

        let units = match &mut scope {
            EmbeddingScope::All => match &full_cursor {
                Some(cursor) => store.history_items_missing_required_embedding_after(
                    embedder.model_id(),
                    cursor,
                    controller.batch_size,
                )?,
                None => store.history_items_missing_required_embedding(
                    embedder.model_id(),
                    controller.batch_size,
                )?,
            },
            EmbeddingScope::Delta {
                event_ids,
                first_units,
            } => first_units.take().map(Ok).unwrap_or_else(|| {
                store.history_items_missing_required_embedding_for_events(
                    embedder.model_id(),
                    event_ids,
                    controller.batch_size,
                )
            })?,
        };
        if units.is_empty() {
            break;
        }
        let pending = match &scope {
            EmbeddingScope::All => match pending_hint {
                Some(pending) => pending,
                None => {
                    let pending = store
                        .history_items_missing_required_embedding_count(embedder.model_id())?;
                    pending_hint = Some(pending);
                    pending
                }
            },
            EmbeddingScope::Delta { .. } => {
                store.history_items_missing_required_embedding_count(embedder.model_id())?
            }
        };
        progress(&EmbeddingProgress::Batch {
            embedded,
            pending,
            batch_size: controller.batch_size,
            reductions: controller.reductions,
            available_gib: crate::memory::sample_memory().and_then(MemorySample::available_gib),
        });
        let texts = units
            .iter()
            .map(|unit| embedding_input(&unit.text))
            .collect::<Vec<_>>();
        let before = crate::memory::sample_memory();
        let vectors = match embedder.embed_batch(&texts, controller.batch_size) {
            Ok(vectors) => vectors,
            Err(err) if controller.can_reduce() && is_memory_like_error(&err) => {
                controller.reduce();
                tracing::debug!(
                    "embedding batch failed with memory-like error; reducing batch size to {}: {err:#}",
                    controller.batch_size
                );
                continue;
            }
            Err(err) => return Err(err),
        };
        if vectors.len() != units.len() {
            bail!(
                "embedder returned {} vectors for {} history items",
                vectors.len(),
                units.len()
            );
        }

        let mut records = Vec::with_capacity(units.len());
        for (unit, vector) in units.iter().zip(vectors) {
            records.push(embedding_record(machine_id, embedder, unit, vector)?);
        }
        let stats = store.import_records(&records)?;
        embedded += stats.inserted;
        vector_embedding_ids.extend(stats.delta.inserted_embeddings);
        if let Some(pending) = pending_hint.as_mut() {
            *pending = pending.saturating_sub(stats.inserted);
        }
        if matches!(&scope, EmbeddingScope::All) {
            full_cursor = units.last().map(|unit| unit.cursor.clone());
        }
        controller.observe(before, crate::memory::sample_memory());
    }

    let pending = store.history_items_missing_required_embedding_count(embedder.model_id())?;
    Ok(EmbeddingRefresh {
        embedded,
        vectors_indexed: refresh_vector_projection_incremental(store, &vector_embedding_ids)?,
        degraded_reason,
        pending,
        batch_size_reductions: controller.reductions,
        final_batch_size: Some(controller.batch_size),
        ..EmbeddingRefresh::default()
    })
}

struct AdaptiveEmbeddingBatch {
    batch_size: usize,
    reductions: usize,
    stable_batches: usize,
}

impl AdaptiveEmbeddingBatch {
    fn new() -> Self {
        Self {
            batch_size: EMBEDDING_BATCH_START,
            reductions: 0,
            stable_batches: 0,
        }
    }

    fn can_reduce(&self) -> bool {
        self.batch_size > EMBEDDING_BATCH_MIN
    }

    fn reduce(&mut self) {
        let reduced = (self.batch_size / 2).max(EMBEDDING_BATCH_MIN);
        if reduced < self.batch_size {
            self.batch_size = reduced;
            self.reductions += 1;
            self.stable_batches = 0;
        }
    }

    fn observe(&mut self, before: Option<MemorySample>, after: Option<MemorySample>) {
        if memory_sample_is_low(after) || rss_spiked_under_pressure(before, after) {
            self.reduce();
            return;
        }
        self.stable_batches += 1;
        if self.stable_batches >= 6 && self.batch_size < EMBEDDING_BATCH_MAX {
            self.batch_size = (self.batch_size * 2).min(EMBEDDING_BATCH_MAX);
            self.stable_batches = 0;
        }
    }

    fn defer_reason(&mut self, sample: Option<MemorySample>) -> Option<String> {
        if memory_sample_is_critical(sample) {
            if self.can_reduce() {
                self.reduce();
                None
            } else {
                Some(memory_pressure_reason(sample))
            }
        } else {
            None
        }
    }
}

fn memory_sample_is_critical(sample: Option<MemorySample>) -> bool {
    sample
        .and_then(|sample| {
            sample.available_bytes.map(|available| {
                available < EMBEDDING_CRITICAL_AVAILABLE_BYTES
                    || sample
                        .total_bytes
                        .is_some_and(|total| available < total.saturating_div(20))
            })
        })
        .unwrap_or(false)
}

fn memory_sample_is_low(sample: Option<MemorySample>) -> bool {
    sample
        .and_then(|sample| {
            sample.available_bytes.map(|available| {
                available < EMBEDDING_LOW_AVAILABLE_BYTES
                    || sample
                        .total_bytes
                        .is_some_and(|total| available < total.saturating_div(10))
            })
        })
        .unwrap_or(false)
}

fn rss_spiked_under_pressure(before: Option<MemorySample>, after: Option<MemorySample>) -> bool {
    if !memory_sample_is_low(after) {
        return false;
    }
    let Some(before) = before.and_then(|sample| sample.process_rss_bytes) else {
        return false;
    };
    let Some(after) = after.and_then(|sample| sample.process_rss_bytes) else {
        return false;
    };
    after.saturating_sub(before) > EMBEDDING_RSS_SPIKE_BYTES
}

fn memory_model_load_defer_reason(sample: Option<MemorySample>) -> Option<String> {
    memory_sample_is_low(sample).then(|| memory_pressure_reason(sample))
}

fn memory_pressure_reason(sample: Option<MemorySample>) -> String {
    let Some(sample) = sample else {
        return "memory pressure detected".to_string();
    };
    if let Some(available) = sample.available_gib() {
        format!("memory pressure: only {available:.1} GiB appears available")
    } else {
        "memory pressure detected".to_string()
    }
}

fn is_memory_like_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}").to_ascii_lowercase();
    ["memory", "alloc", "oom", "resource exhausted"]
        .iter()
        .any(|needle| text.contains(needle))
}

pub fn machine_id_prefix_for_hostname(hostname: &str) -> String {
    format!("machine_{}_", sanitize_machine_hostname(hostname))
}

fn sanitize_machine_hostname(input: &str) -> String {
    input
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn embedding_input(text: &str) -> String {
    text.chars()
        .take(EMBEDDING_TEXT_MAX_CHARS)
        .collect::<String>()
}

fn embedding_record(
    machine_id: &str,
    embedder: &dyn Embedder,
    unit: &HistoryItemForEmbedding,
    vector: Vec<f32>,
) -> Result<crate::archive::ArchiveRecord> {
    if vector.len() != embedder.dims() {
        bail!(
            "embedder returned vector with {} dimensions, expected {}",
            vector.len(),
            embedder.dims()
        );
    }
    let vector_blob = crate::storage::f32_vector_to_blob(&vector);
    let vector_hash = crate::archive::blake3_hex(&vector_blob);
    let id =
        crate::archive::stable_id(&["embedding", &unit.id, &unit.text_hash, embedder.model_id()]);
    let hash = crate::archive::stable_hash(&(
        &id,
        &unit.id,
        &unit.text_hash,
        embedder.model_id(),
        embedder.dims(),
        &vector_hash,
    ))?;
    Ok(crate::archive::ArchiveRecord::Embedding(
        crate::archive::EmbeddingRecord {
            id,
            unit_id: unit.id.clone(),
            text_hash: unit.text_hash.clone(),
            model_id: embedder.model_id().to_string(),
            dims: embedder.dims() as u32,
            vector_hash,
            vector: vector_blob,
            producer_machine_id: machine_id.to_string(),
            embedded_at: Utc::now(),
            metadata: serde_json::json!({
                "embedded_text_max_chars": EMBEDDING_TEXT_MAX_CHARS,
                "provider": "local",
                "indexer": "semantic_embedding_v1"
            }),
            hash,
        },
    ))
}

pub fn search(
    store: &Store,
    query: &str,
    options: SearchOptions,
    query_embedder: Option<&dyn Embedder>,
    degraded_reason: Option<String>,
) -> Result<SearchResponse> {
    let backend_limit = options
        .limit
        .saturating_mul(BACKEND_LIMIT_MULTIPLIER)
        .max(BACKEND_MIN_LIMIT);
    let semantic_limit = backend_limit.min(SQLITE_VEC_MAX_K);
    let tier_names = options.corpus.tier_names();
    let lexical = if options.mode.includes_lexical() {
        store.search_fts(
            query,
            &tier_names,
            backend_limit,
            options.after,
            options.before,
            options.machine_id.as_deref(),
            options.machine_id_prefix.as_deref(),
            options.workspace_scope.as_deref(),
        )?
    } else {
        Vec::new()
    };
    let (semantic, degraded_reason) = if options.mode.includes_semantic() {
        semantic_search(
            store,
            query,
            query_embedder,
            degraded_reason,
            semantic_limit,
            options.after,
            options.before,
            options.machine_id.as_deref(),
            options.machine_id_prefix.as_deref(),
            options.workspace_scope.as_deref(),
            &tier_names,
        )?
    } else {
        (Vec::new(), None)
    };
    Ok(SearchResponse {
        degraded_reason,
        results: fuse(lexical, semantic, options),
    })
}

fn semantic_search(
    store: &Store,
    query: &str,
    query_embedder: Option<&dyn Embedder>,
    degraded_reason: Option<String>,
    limit: usize,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    machine_id: Option<&str>,
    machine_id_prefix: Option<&str>,
    workspace_scope: Option<&str>,
    selected_tiers: &[&str],
) -> Result<(Vec<SearchRow>, Option<String>)> {
    let Some(embedder) = query_embedder else {
        return Ok((Vec::new(), degraded_reason));
    };
    if !embedder.is_semantic() {
        return Ok((
            Vec::new(),
            Some("query embedder is not semantic; using lexical search only".to_string()),
        ));
    }
    if embedder.dims() != crate::embed::DEFAULT_SEMANTIC_DIMS {
        return Ok((
            Vec::new(),
            Some(format!(
                "query embedder dimensions {} are not supported by the local vector index",
                embedder.dims()
            )),
        ));
    }
    let query_concepts = semantic_query_concepts(query);
    let query_text = semantic_embedding_query_text(query, &query_concepts);
    let query_vector = embedder.embed_one(&query_text)?;
    let vector_rows = store.vector_search(
        embedder.model_id(),
        &query_vector,
        selected_tiers,
        limit,
        after,
        before,
        machine_id,
        machine_id_prefix,
        workspace_scope,
    )?;
    let rows = vector_rows
        .into_iter()
        .filter(|row| semantic_candidate_has_context(&row.content))
        .filter(|row| semantic_candidate_matches_query(&row.content, &query_concepts))
        .enumerate()
        .map(|(idx, mut row)| {
            row.rank = idx + 1;
            row
        })
        .map(search_row_from_vector)
        .collect();
    Ok((rows, degraded_reason))
}

fn semantic_candidate_has_context(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.chars().count() < SEMANTIC_CANDIDATE_MIN_CHARS {
        return false;
    }
    let informative_terms = trimmed
        .split_whitespace()
        .filter(|term| term.chars().any(char::is_alphanumeric))
        .count();
    informative_terms >= SEMANTIC_CANDIDATE_MIN_TERMS
}

fn semantic_query_concepts(query: &str) -> Vec<Vec<&'static str>> {
    let mut concepts = Vec::new();
    let lower = query.to_ascii_lowercase();
    for token in lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let concept = match token {
            "billing" | "bill" | "charge" | "charged" | "fee" | "fees" | "money" | "payment"
            | "payments" | "stripe" => Some(vec![
                "billing", "charge", "fee", "money", "payment", "stripe",
            ]),
            "crash" | "crashed" | "died" => Some(vec!["crash", "died"]),
            "database" | "databases" | "db" => Some(vec!["database", "db"]),
            "bug" | "bugs" | "fail" | "failed" | "failing" | "failure" | "failures" | "fix"
            | "fixes" | "fixed" | "issue" | "issues" | "error" | "errors" => {
                Some(vec!["bug", "error", "fail", "fix", "issue"])
            }
            "label" | "labels" | "name" | "names" | "summary" | "summaries" | "title"
            | "titles" => Some(vec!["label", "name", "summar", "title"]),
            "process" | "processed" | "processing" => Some(vec!["process"]),
            "remote" | "remotes" => Some(vec!["remote"]),
            "replicate" | "replicated" | "replication" | "sync" | "synced" | "syncing" => {
                Some(vec!["replic", "sync"])
            }
            "tap" | "tapped" | "tapping" => Some(vec!["tap"]),
            "thread" | "threads" | "conversation" | "conversations" => {
                Some(vec!["conversation", "thread"])
            }
            _ => None,
        };
        if let Some(concept) = concept {
            if !concepts.contains(&concept) {
                concepts.push(concept);
            }
        }
    }
    if lower.contains("did not go through")
        || lower.contains("didn't go through")
        || lower.contains("does not go through")
        || lower.contains("did not process")
        || lower.contains("didn't process")
        || lower.contains("does not process")
        || lower.contains("failed to process")
    {
        let concept = vec!["bug", "error", "fail", "fix", "issue", "process"];
        if !concepts.contains(&concept) {
            concepts.push(concept);
        }
    }
    concepts
}

fn semantic_embedding_query_text(query: &str, query_concepts: &[Vec<&'static str>]) -> String {
    if query_concepts.is_empty() {
        return query.to_string();
    }
    let mut terms = Vec::new();
    let mut seen = HashSet::new();
    for concept in query_concepts {
        for term in concept {
            if seen.insert(*term) {
                terms.push(*term);
            }
        }
    }
    format!("{query} {}", terms.join(" "))
}

fn semantic_candidate_matches_query(text: &str, query_concepts: &[Vec<&'static str>]) -> bool {
    if query_concepts.len() < 2 {
        return true;
    }
    let lower = text.to_ascii_lowercase();
    let matched = query_concepts
        .iter()
        .filter(|concept| concept.iter().any(|variant| lower.contains(variant)))
        .count();
    let required = if query_concepts
        .iter()
        .any(|concept| semantic_concept_is_metadata_label(concept))
    {
        1
    } else if query_concepts.len() <= 2 {
        query_concepts.len()
    } else {
        2
    };
    matched >= required
}

fn semantic_concept_is_metadata_label(concept: &[&'static str]) -> bool {
    concept
        .iter()
        .any(|variant| matches!(*variant, "label" | "name" | "summar" | "title"))
}

fn fuse(
    lexical: Vec<SearchRow>,
    semantic: Vec<SearchRow>,
    options: SearchOptions,
) -> Vec<SearchResult> {
    #[derive(Debug, Default)]
    struct Acc {
        row: Option<SearchRow>,
        score: f64,
        lexical_rank: Option<usize>,
        semantic_rank: Option<usize>,
    }

    let mut acc: HashMap<String, Acc> = HashMap::new();
    let mut seen_lexical = HashSet::new();
    for row in lexical {
        let key = fusion_key(&row);
        if !seen_lexical.insert(key.clone()) {
            continue;
        }
        let entry = acc.entry(key).or_default();
        entry.score += lexical_rrf_weight(&row) / (RRF_K + row.rank as f64);
        entry.lexical_rank = Some(row.rank);
        entry.row = Some(row);
    }
    for row in semantic {
        let key = fusion_key(&row);
        let entry = acc.entry(key).or_default();
        entry.score += SEMANTIC_RRF_WEIGHT / (RRF_K + row.rank as f64);
        entry.semantic_rank = Some(row.rank);
        if entry.row.is_none() {
            entry.row = Some(row);
        }
    }

    let mut results = acc
        .into_values()
        .filter_map(|entry| {
            let row = entry.row?;
            Some(SearchResult {
                history_item_id: row.history_item_id,
                match_type: match_type(entry.lexical_rank, entry.semantic_rank),
                event_id: row.event_id,
                session_id: row.session_id,
                machine_id: row.machine_id,
                source_kind: row.source_kind,
                tier: row.tier,
                kind: row.search_kind,
                score: entry.score,
                lexical_rank: entry.lexical_rank,
                semantic_rank: entry.semantic_rank,
                occurred_at: row.occurred_at,
                session_title: row.session_title,
                workspace_values: row.workspace_values,
                snippet: snippet(&row.content, 240),
                duplicate_group: Vec::new(),
            })
        })
        .collect::<Vec<_>>();
    apply_recency_bias(&mut results, options.recency_bias);
    sort_results(&mut results, options.sort);
    if !options.show_duplicates {
        collapse_results_by_snippet(&mut results);
    }
    results.truncate(options.limit);
    results
}

fn fusion_key(row: &SearchRow) -> String {
    row.history_item_id
        .clone()
        .unwrap_or_else(|| format!("event:{}", row.event_id))
}

fn lexical_rrf_weight(row: &SearchRow) -> f64 {
    if row.search_kind == "user" {
        LEXICAL_RRF_WEIGHT
    } else {
        NON_USER_LEXICAL_RRF_WEIGHT
    }
}

fn collapse_results_by_snippet(results: &mut Vec<SearchResult>) {
    let mut representatives = HashMap::new();
    let mut collapsed = Vec::with_capacity(results.len());
    for result in results.drain(..) {
        let key = normalized_result_key(&result.snippet);
        if let Some(idx) = representatives.get(&key).copied() {
            let representative: &mut SearchResult = &mut collapsed[idx];
            representative
                .duplicate_group
                .push(duplicate_member_from_result(result));
        } else {
            representatives.insert(key, collapsed.len());
            collapsed.push(result);
        }
    }
    *results = collapsed;
}

fn duplicate_member_from_result(result: SearchResult) -> DuplicateSearchMember {
    DuplicateSearchMember {
        history_item_id: result.history_item_id,
        match_type: result.match_type,
        event_id: result.event_id,
        session_id: result.session_id,
        machine_id: result.machine_id,
        source_kind: result.source_kind,
        tier: result.tier,
        kind: result.kind,
        score: result.score,
        lexical_rank: result.lexical_rank,
        semantic_rank: result.semantic_rank,
        occurred_at: result.occurred_at,
        session_title: result.session_title,
        workspace_values: result.workspace_values,
        snippet: result.snippet,
    }
}

fn normalized_result_key(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn search_row_from_vector(row: VectorSearchRow) -> SearchRow {
    let _distance = row.distance;
    SearchRow {
        history_item_id: Some(row.history_item_id),
        event_id: row.event_id,
        session_id: row.session_id,
        machine_id: row.machine_id,
        source_kind: row.source_kind,
        tier: Some(row.tier),
        search_kind: row.search_kind,
        content: row.content,
        occurred_at: row.occurred_at,
        session_title: row.session_title,
        workspace_values: row.workspace_values,
        rank: row.rank,
    }
}

fn match_type(lexical_rank: Option<usize>, semantic_rank: Option<usize>) -> MatchType {
    match (lexical_rank, semantic_rank) {
        (Some(_), Some(_)) => MatchType::Hybrid,
        (Some(_), None) => MatchType::Lexical,
        (None, Some(_)) => MatchType::Semantic,
        (None, None) => MatchType::Lexical,
    }
}

fn apply_recency_bias(results: &mut [SearchResult], recency_bias: f64) {
    if recency_bias <= 0.0 {
        return;
    }
    let now = Utc::now();
    for result in results {
        let Some(occurred_at) = result.occurred_at else {
            continue;
        };
        let age_days = (now - occurred_at).num_seconds().max(0) as f64 / 86_400.0;
        let recency = 1.0 / (1.0 + age_days / 30.0);
        result.score *= 1.0 + recency_bias * recency;
    }
}

fn sort_results(results: &mut [SearchResult], sort: SortMode) {
    match sort {
        SortMode::Relevance => results.sort_by(|left, right| {
            lexical_lane(left)
                .cmp(&lexical_lane(right))
                .then_with(|| lexical_rank_bucket(left).cmp(&lexical_rank_bucket(right)))
                .then_with(|| right.score.total_cmp(&left.score))
                .then_with(|| {
                    best_rank(left)
                        .cmp(&best_rank(right))
                        .then_with(|| left.event_id.cmp(&right.event_id))
                })
        }),
        SortMode::Newest => results.sort_by(|left, right| {
            right
                .occurred_at
                .cmp(&left.occurred_at)
                .then_with(|| right.score.total_cmp(&left.score))
        }),
        SortMode::Oldest => results.sort_by(|left, right| {
            left.occurred_at
                .cmp(&right.occurred_at)
                .then_with(|| right.score.total_cmp(&left.score))
        }),
    }
}

fn lexical_lane(result: &SearchResult) -> usize {
    if result.lexical_rank.is_some() {
        0
    } else {
        1
    }
}

fn lexical_rank_bucket(result: &SearchResult) -> usize {
    result
        .lexical_rank
        .map(|rank| rank.saturating_sub(1) / 3)
        .unwrap_or(usize::MAX)
}

fn best_rank(result: &SearchResult) -> usize {
    result
        .lexical_rank
        .into_iter()
        .chain(result.semantic_rank)
        .min()
        .unwrap_or(usize::MAX)
}

fn snippet(input: &str, max_chars: usize) -> String {
    let normalized = input.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = normalized.chars().take(max_chars).collect::<String>();
    if normalized.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{
        stable_hash, stable_id, ArchiveRecord, EmbeddingRecord, EventRecord, SearchUnitRecord,
        SessionRecord, SourceRecord,
    };
    use crate::embed::{EmbedderConfig, EmbedderProvider, FastEmbedModel};
    use crate::storage::{f32_vector_to_blob, HistoryItemRecord, ImportDelta, Store};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn vector_only_result_can_win_without_fts_overlap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_user_event_and_project(
            &store,
            "exact lexical marker with enough surrounding human context to stay eligible for semantic retrieval",
        );
        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &unit,
                unit_vector(3),
            )))
            .expect("embedding");
        store
            .refresh_vector_projection()
            .expect("vector projection");

        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(3),
        };
        let response = search(
            &store,
            "conceptual neighbor",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            Some(&embedder),
            None,
        )
        .expect("search");

        assert_eq!(response.degraded_reason, None);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, unit.event_id);
        assert_eq!(response.results[0].lexical_rank, None);
        assert_eq!(response.results[0].semantic_rank, Some(1));
    }

    #[test]
    fn high_user_limit_does_not_exceed_sqlite_vec_knn_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_user_event_and_project(
            &store,
            "high limit semantic target with enough surrounding context to pass semantic candidate filtering",
        );
        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &unit,
                unit_vector(5),
            )))
            .expect("embedding");
        store
            .refresh_vector_projection()
            .expect("vector projection");

        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(5),
        };
        let response = search(
            &store,
            "semantic neighbor",
            SearchOptions::new(1000, SortMode::Relevance, 0.0),
            Some(&embedder),
            None,
        )
        .expect("search");

        assert_eq!(response.degraded_reason, None);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, unit.event_id);
    }

    #[test]
    fn lexical_and_vector_results_are_fused_with_rrf() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let lexical_only = import_event_and_project(&store, "alpha token only");
        let hybrid = import_user_event_and_project(
            &store,
            "alpha token also semantic with enough surrounding words to remain a useful vector candidate",
        );
        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &hybrid,
                unit_vector(7),
            )))
            .expect("embedding");
        store
            .refresh_vector_projection()
            .expect("vector projection");

        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(7),
        };
        let response = search(
            &store,
            "alpha",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            Some(&embedder),
            None,
        )
        .expect("search");

        let first = response.results.first().expect("result");
        assert_eq!(first.event_id, hybrid.event_id);
        assert!(first.lexical_rank.is_some());
        assert_eq!(first.semantic_rank, Some(1));
        assert!(response.results.iter().any(
            |result| result.event_id == lexical_only.event_id && result.semantic_rank.is_none()
        ));
    }

    #[test]
    fn semantic_search_filters_short_low_information_candidates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let short = import_event_and_project_with_kind_at(&store, "fix", "user", None);
        let rich = import_user_event_and_project(
            &store,
            "payment workflow failed after the funding step and the logs explain the retry behavior clearly",
        );
        for unit in [&short, &rich] {
            store
                .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                    unit,
                    unit_vector(11),
                )))
                .expect("embedding");
        }
        store
            .refresh_vector_projection()
            .expect("vector projection");
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(11),
        };

        let response = search(
            &store,
            "payment failure",
            SearchOptions::new(5, SortMode::Relevance, 0.0).with_mode(SearchMode::Semantic),
            Some(&embedder),
            None,
        )
        .expect("search");

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, rich.event_id);
        assert_eq!(response.results[0].semantic_rank, Some(1));
    }

    #[test]
    fn semantic_search_filters_candidates_missing_multi_concept_intent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unrelated_failure = import_user_event_and_project(
            &store,
            "please check actual logs and figure out why the deploy keeps failing during startup retries",
        );
        let payment_failure = import_user_event_and_project(
            &store,
            "payment failed after the stripe charge step and the customer money did not process correctly",
        );
        for unit in [&unrelated_failure, &payment_failure] {
            store
                .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                    unit,
                    unit_vector(23),
                )))
                .expect("embedding");
        }
        store
            .refresh_vector_projection()
            .expect("vector projection");
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(23),
        };

        let response = search(
            &store,
            "payment failed",
            SearchOptions::new(5, SortMode::Relevance, 0.0).with_mode(SearchMode::Semantic),
            Some(&embedder),
            None,
        )
        .expect("search");

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, payment_failure.event_id);
    }

    #[test]
    fn semantic_query_gate_requires_multiple_intent_concepts_when_present() {
        let payment_failed = semantic_query_concepts("payment failed");
        assert!(semantic_candidate_matches_query(
            "the payment failed after the stripe charge step",
            &payment_failed
        ));
        assert!(!semantic_candidate_matches_query(
            "please check actual logs and figure out why startup keeps failing",
            &payment_failed
        ));

        let vague_payment = semantic_query_concepts("money did not go through");
        assert!(semantic_candidate_matches_query(
            "the customer payment failed to process after the charge step",
            &vague_payment
        ));
        assert!(!semantic_candidate_matches_query(
            "the background worker did not process the queue cleanly",
            &vague_payment
        ));

        let payment_process = semantic_query_concepts("payment did not process");
        assert!(semantic_candidate_matches_query(
            "the stripe billing integration had several critical issues",
            &payment_process
        ));

        let thread_title = semantic_query_concepts("thread summary title");
        assert!(semantic_candidate_matches_query(
            "give me the overall summary of what the feature is doing",
            &thread_title
        ));
        assert!(semantic_candidate_matches_query(
            "explain how the thread system stores conversation history",
            &thread_title
        ));

        let crash = semantic_query_concepts("app crash tap");
        assert!(semantic_candidate_matches_query(
            "the app crashed when the user tapped the billing button",
            &crash
        ));
    }

    #[test]
    fn disabled_semantic_search_reports_degraded_fts_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_event_and_project(&store, "plain lexical phrase");

        let response = search(
            &store,
            "plain lexical",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            None,
            Some("query embedder disabled".to_string()),
        )
        .expect("search");

        assert_eq!(
            response.degraded_reason.as_deref(),
            Some("query embedder disabled")
        );
        assert_eq!(response.results[0].event_id, unit.event_id);
        assert_eq!(response.results[0].semantic_rank, None);
    }

    #[test]
    fn lexical_mode_skips_semantic_degraded_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_event_and_project(&store, "lexical mode phrase");

        let response = search(
            &store,
            "lexical mode",
            SearchOptions::new(5, SortMode::Relevance, 0.0).with_mode(SearchMode::Lexical),
            None,
            Some("query embedder disabled".to_string()),
        )
        .expect("search");

        assert_eq!(response.degraded_reason, None);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, unit.event_id);
        assert_eq!(response.results[0].match_type, MatchType::Lexical);
    }

    #[test]
    fn semantic_mode_skips_lexical_only_results() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        import_event_and_project(&store, "semantic mode lexical phrase");

        let response = search(
            &store,
            "semantic mode lexical",
            SearchOptions::new(5, SortMode::Relevance, 0.0).with_mode(SearchMode::Semantic),
            None,
            Some("query embedder disabled".to_string()),
        )
        .expect("search");

        assert_eq!(
            response.degraded_reason.as_deref(),
            Some("query embedder disabled")
        );
        assert!(response.results.is_empty());
    }

    #[test]
    fn newest_sort_orders_by_event_time() {
        let old = Utc::now() - chrono::Duration::days(10);
        let new = Utc::now();
        let results = fuse(
            vec![
                ranked_row("old_event", 1, old),
                ranked_row("new_event", 2, new),
            ],
            Vec::new(),
            SearchOptions::new(10, SortMode::Newest, 0.0),
        );

        assert_eq!(results[0].event_id, "new_event");
        assert_eq!(results[1].event_id, "old_event");
    }

    #[test]
    fn rrf_keeps_exact_lexical_anchors_before_semantic_complements() {
        let now = Utc::now();
        let results = fuse(
            vec![
                ranked_row("lexical_one", 1, now),
                ranked_row("lexical_two", 2, now),
                ranked_row("lexical_three", 3, now),
            ],
            vec![ranked_row("semantic_one", 1, now)],
            SearchOptions::new(10, SortMode::Relevance, 0.0),
        );

        let ordered = results
            .iter()
            .map(|result| result.event_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                "lexical_one",
                "lexical_two",
                "lexical_three",
                "semantic_one"
            ]
        );
    }

    #[test]
    fn rrf_keeps_stronger_lexical_rank_before_semantic_boosted_later_hits() {
        let now = Utc::now();
        let results = fuse(
            vec![
                ranked_row("lexical_one", 1, now),
                ranked_row("lexical_two", 4, now),
            ],
            vec![ranked_row("lexical_two", 2, now)],
            SearchOptions::new(10, SortMode::Relevance, 0.0),
        );

        let ordered = results
            .iter()
            .map(|result| result.event_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["lexical_one", "lexical_two"]);
    }

    #[test]
    fn rrf_keeps_non_user_lexical_anchors_before_semantic_neighbors() {
        let now = Utc::now();
        let results = fuse(
            vec![
                ranked_row_with_kind("assistant_note", "assistant", 1, now),
                ranked_row_with_kind("user_phrase", "user", 2, now),
            ],
            vec![ranked_row("semantic_user", 1, now)],
            SearchOptions::new(10, SortMode::Relevance, 0.0),
        );

        let ordered = results
            .iter()
            .map(|result| result.event_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec!["user_phrase", "assistant_note", "semantic_user"]
        );
    }

    #[test]
    fn fuse_collapses_duplicate_snippets_and_refills_with_distinct_results() {
        let now = Utc::now();
        let duplicate_text = "same forked transcript result repeated across agent branches";
        let results = fuse(
            vec![
                ranked_row_with_content("lexical_one", "first exact lexical result", 1, now),
                ranked_row_with_content("lexical_dup", duplicate_text, 2, now),
            ],
            vec![
                ranked_row_with_content("semantic_dup", duplicate_text, 1, now),
                ranked_row_with_content("semantic_two", "second distinct semantic result", 2, now),
                ranked_row_with_content("semantic_three", "third distinct semantic result", 3, now),
            ],
            SearchOptions::new(4, SortMode::Relevance, 0.0),
        );

        let snippets = results
            .iter()
            .map(|result| result.snippet.as_str())
            .collect::<Vec<_>>();
        assert_eq!(snippets.len(), 4);
        assert_eq!(
            snippets
                .iter()
                .filter(|snippet| **snippet == duplicate_text)
                .count(),
            1
        );
        let duplicate = results
            .iter()
            .find(|result| result.snippet == duplicate_text)
            .expect("duplicate representative");
        assert_eq!(duplicate.duplicate_group.len(), 1);
        assert_eq!(duplicate.duplicate_group[0].event_id, "semantic_dup");
        assert!(snippets.contains(&"third distinct semantic result"));
    }

    #[test]
    fn fuse_can_show_duplicate_snippets_without_collapsing() {
        let now = Utc::now();
        let duplicate_text = "same visible result from two forked histories";
        let results = fuse(
            vec![ranked_row_with_content(
                "lexical_dup",
                duplicate_text,
                1,
                now,
            )],
            vec![ranked_row_with_content(
                "semantic_dup",
                duplicate_text,
                1,
                now,
            )],
            SearchOptions::new(10, SortMode::Relevance, 0.0).with_show_duplicates(true),
        );

        assert_eq!(
            results
                .iter()
                .filter(|result| result.snippet == duplicate_text)
                .count(),
            2
        );
        assert!(results
            .iter()
            .all(|result| result.duplicate_group.is_empty()));
    }

    #[test]
    fn rrf_fuses_by_history_item_id_not_event_id() {
        let now = Utc::now();
        let mut lexical = ranked_row_with_content("shared_event", "first item exact", 1, now);
        lexical.history_item_id = Some("history_item_first".to_string());
        let mut semantic = ranked_row_with_content("shared_event", "second item semantic", 1, now);
        semantic.history_item_id = Some("history_item_second".to_string());

        let distinct = fuse(
            vec![lexical.clone()],
            vec![semantic],
            SearchOptions::new(10, SortMode::Relevance, 0.0),
        );

        assert_eq!(distinct.len(), 2);
        assert!(distinct.iter().any(|result| {
            result.history_item_id.as_deref() == Some("history_item_first")
                && result.match_type == MatchType::Lexical
        }));
        assert!(distinct.iter().any(|result| {
            result.history_item_id.as_deref() == Some("history_item_second")
                && result.match_type == MatchType::Semantic
        }));

        let mut semantic_same_item =
            ranked_row_with_content("shared_event", "first item semantic", 1, now);
        semantic_same_item.history_item_id = Some("history_item_first".to_string());
        let hybrid = fuse(
            vec![lexical],
            vec![semantic_same_item],
            SearchOptions::new(10, SortMode::Relevance, 0.0),
        );

        assert_eq!(hybrid.len(), 1);
        assert_eq!(hybrid[0].match_type, MatchType::Hybrid);
        assert_eq!(
            hybrid[0].history_item_id.as_deref(),
            Some("history_item_first")
        );
    }

    #[test]
    fn recency_bias_can_promote_recent_matches() {
        let old = Utc::now() - chrono::Duration::days(365);
        let new = Utc::now();
        let unbiased = fuse(
            vec![
                ranked_row("old_event", 1, old),
                ranked_row("new_event", 2, new),
            ],
            Vec::new(),
            SearchOptions::new(10, SortMode::Relevance, 0.0),
        );
        let biased = fuse(
            vec![
                ranked_row("old_event", 1, old),
                ranked_row("new_event", 2, new),
            ],
            Vec::new(),
            SearchOptions::new(10, SortMode::Relevance, 1.0),
        );

        assert_eq!(unbiased[0].event_id, "old_event");
        assert_eq!(biased[0].event_id, "new_event");
    }

    #[test]
    fn refresh_embeddings_creates_durable_vectors_for_missing_history_items() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_user_event_and_project(
            &store,
            "semantic refresh target with enough user context to become a durable embedding in the local vector index",
        );
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(13),
        };

        let refresh = refresh_embeddings(&store, "machine_fixture", Some(&embedder), None)
            .expect("refresh embeddings");

        assert_eq!(refresh.embedded, 1);
        assert_eq!(refresh.vectors_indexed, 1);
        assert_eq!(refresh.degraded_reason, None);
        assert_eq!(store.stats().expect("stats").embeddings, 1);
        let hits = store
            .vector_search(
                "fixture-semantic-384",
                &unit_vector(13),
                &["conversation"],
                5,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("vector search");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].history_item_id,
            history_item_id_for_search_unit(&unit)
        );
    }

    #[test]
    fn refresh_embeddings_skips_hash_fallback_as_degraded_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        import_event_and_project(&store, "hash fallback should not become semantic");
        let hash = crate::embed::HashEmbedder;

        let refresh = refresh_embeddings(&store, "machine_fixture", Some(&hash), None)
            .expect("refresh embeddings");

        assert_eq!(refresh.embedded, 0);
        assert_eq!(refresh.vectors_indexed, 0);
        assert_eq!(
            refresh.degraded_reason.as_deref(),
            Some("embedder is not semantic; skipping durable embeddings")
        );
        assert_eq!(store.stats().expect("stats").embeddings, 0);
    }

    #[test]
    fn refresh_embeddings_incremental_skips_model_load_without_delta_work() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let config = EmbedderConfig {
            provider: EmbedderProvider::FastEmbed,
            semantic_model: FastEmbedModel::BgeSmallEnV15Q,
            model_cache: dir.path().join("models"),
            intra_threads: 1,
        };

        let refresh = refresh_embeddings_incremental(
            &store,
            "machine_fixture",
            &config,
            &ImportDelta::default(),
        )
        .expect("refresh embeddings");

        assert_eq!(refresh.embedded, 0);
        assert_eq!(refresh.vectors_indexed, 0);
    }

    #[test]
    fn refresh_embeddings_incremental_indexes_transferred_embedding_when_disabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_user_event_and_project(
            &store,
            "transferred vector target with enough user context to stay eligible for semantic retrieval",
        );
        let stats = store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &unit,
                unit_vector(17),
            )))
            .expect("embedding import");
        let config = EmbedderConfig {
            provider: EmbedderProvider::Disabled,
            semantic_model: FastEmbedModel::BgeSmallEnV15Q,
            model_cache: dir.path().join("models"),
            intra_threads: 1,
        };

        let refresh =
            refresh_embeddings_incremental(&store, "machine_fixture", &config, &stats.delta)
                .expect("refresh embeddings");
        let hits = store
            .vector_search(
                "fixture-semantic-384",
                &unit_vector(17),
                &["conversation"],
                5,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("vector search");

        assert_eq!(refresh.embedded, 0);
        assert_eq!(refresh.vectors_indexed, 1);
        assert_eq!(
            refresh.degraded_reason.as_deref(),
            Some("query embedder disabled")
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].history_item_id,
            history_item_id_for_search_unit(&unit)
        );
    }

    #[test]
    fn refresh_embeddings_incremental_repairs_vector_projection_after_empty_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_user_event_and_project(
            &store,
            "empty delta vector target with enough user context to stay eligible for semantic retrieval",
        );
        store
            .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                &unit,
                unit_vector(23),
            )))
            .expect("embedding import");
        let config = EmbedderConfig {
            provider: EmbedderProvider::Disabled,
            semantic_model: FastEmbedModel::BgeSmallEnV15Q,
            model_cache: dir.path().join("models"),
            intra_threads: 1,
        };

        let refresh = refresh_embeddings_incremental(
            &store,
            "machine_fixture",
            &config,
            &ImportDelta::default(),
        )
        .expect("refresh embeddings");
        let hits = store
            .vector_search(
                "fixture-semantic-384",
                &unit_vector(23),
                &["conversation"],
                5,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("vector search");

        assert_eq!(refresh.embedded, 0);
        assert_eq!(refresh.vectors_indexed, 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].history_item_id,
            history_item_id_for_search_unit(&unit)
        );
    }

    #[test]
    fn adaptive_embedding_batch_reduces_when_memory_is_low() {
        let mut batch = AdaptiveEmbeddingBatch::new();

        batch.observe(memory_sample(16, 8, 512), memory_sample(16, 1, 1024));

        assert_eq!(batch.batch_size, 32);
        assert_eq!(batch.reductions, 1);
    }

    #[test]
    fn adaptive_embedding_batch_defers_after_reaching_minimum_on_critical_memory() {
        let mut batch = AdaptiveEmbeddingBatch::new();
        let critical = memory_sample(16, 0, 512);

        while batch.can_reduce() {
            assert!(batch.defer_reason(critical).is_none());
        }
        let reason = batch.defer_reason(critical).expect("defer reason");

        assert_eq!(batch.batch_size, 1);
        assert_eq!(batch.reductions, 6);
        assert!(reason.contains("memory pressure"));
    }

    #[test]
    fn adaptive_embedding_batch_grows_after_stable_memory() {
        let mut batch = AdaptiveEmbeddingBatch::new();

        for _ in 0..6 {
            batch.observe(memory_sample(16, 12, 512), memory_sample(16, 12, 512));
        }

        assert_eq!(batch.batch_size, 64);
        assert_eq!(batch.reductions, 0);
    }

    #[test]
    fn model_load_defers_on_low_memory_before_fastembed_is_loaded() {
        assert!(memory_model_load_defer_reason(memory_sample(16, 1, 512)).is_some());
        assert!(memory_model_load_defer_reason(memory_sample(16, 8, 512)).is_none());
    }

    #[test]
    fn rss_spike_counts_as_batch_pressure_only_when_memory_is_low() {
        assert!(rss_spiked_under_pressure(
            memory_sample(16, 8, 512),
            memory_sample(16, 1, 1200)
        ));
        assert!(!rss_spiked_under_pressure(
            memory_sample(16, 8, 512),
            memory_sample(16, 8, 1200)
        ));
    }

    #[test]
    fn memory_like_embedding_errors_are_detected_case_insensitively() {
        assert!(is_memory_like_error(&anyhow::anyhow!(
            "Resource exhausted while allocating tensor"
        )));
        assert!(!is_memory_like_error(&anyhow::anyhow!(
            "network download failed"
        )));
    }

    #[test]
    fn refresh_incremental_indexes_inserted_event_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let (event_id, delta) = import_event_only(&store, "delta indexed phrase");

        let indexed = refresh_incremental(&store, &delta).expect("incremental refresh");
        let response = search(
            &store,
            "delta indexed",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            None,
            None,
        )
        .expect("search");

        assert_eq!(indexed, 1);
        assert_eq!(delta.inserted_events, vec![event_id.clone()]);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, event_id);
    }

    #[test]
    fn refresh_incremental_indexes_large_inserted_event_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let (event_ids, delta) = import_many_events(&store, 1200, "chunked indexed phrase");

        let indexed = refresh_incremental(&store, &delta).expect("incremental refresh");
        let response = search(
            &store,
            "chunked indexed phrase 1199",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            None,
            None,
        )
        .expect("search");

        assert_eq!(indexed, event_ids.len());
        assert_eq!(delta.inserted_events.len(), event_ids.len());
        assert!(response
            .results
            .iter()
            .any(|result| result.event_id == event_ids[1199]));
    }

    #[test]
    fn refresh_incremental_repairs_missing_index_rows_after_empty_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let (event_id, delta) = import_event_only(&store, "empty delta repair phrase");
        refresh_incremental(&store, &delta).expect("initial refresh");
        store
            .with_conn(|conn| {
                conn.execute("DELETE FROM events_fts", [])?;
                conn.execute("DELETE FROM event_embeddings", [])?;
                conn.execute("DELETE FROM search_units", [])?;
                Ok(())
            })
            .expect("damage derived rows");

        let indexed = refresh_incremental(&store, &ImportDelta::default())
            .expect("empty delta repair refresh");
        let response = search(
            &store,
            "empty delta repair",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            None,
            None,
        )
        .expect("search");

        assert_eq!(indexed, 1);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, event_id);
    }

    #[test]
    fn full_refresh_repairs_missing_search_index_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let (event_id, delta) = import_event_only(&store, "repairable indexed phrase");
        refresh_incremental(&store, &delta).expect("initial refresh");
        store
            .with_conn(|conn| {
                conn.execute("DELETE FROM events_fts", [])?;
                conn.execute("DELETE FROM event_embeddings", [])?;
                conn.execute("DELETE FROM search_units", [])?;
                Ok(())
            })
            .expect("damage derived rows");

        let indexed = refresh(&store).expect("repair refresh");
        let response = search(
            &store,
            "repairable indexed",
            SearchOptions::new(5, SortMode::Relevance, 0.0),
            None,
            None,
        )
        .expect("search");

        assert_eq!(indexed, 1);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, event_id);
    }

    #[test]
    fn lexical_search_honors_time_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let old_time = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("old time")
            .with_timezone(&Utc);
        let new_time = DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
            .expect("new time")
            .with_timezone(&Utc);
        let old_id = import_event_at(&store, "shared lexical old", Some(old_time)).0;
        let new_id = import_event_at(&store, "shared lexical new", Some(new_time)).0;
        refresh(&store).expect("refresh search");

        let response = search(
            &store,
            "shared lexical",
            SearchOptions::new(10, SortMode::Relevance, 0.0).with_time_window(
                Some(
                    DateTime::parse_from_rfc3339("2026-01-15T00:00:00Z")
                        .expect("after")
                        .with_timezone(&Utc),
                ),
                None,
            ),
            None,
            None,
        )
        .expect("search");

        assert!(response
            .results
            .iter()
            .any(|result| result.event_id == new_id));
        assert!(!response
            .results
            .iter()
            .any(|result| result.event_id == old_id));
    }

    #[test]
    fn lexical_search_filters_by_machine_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let target_id = import_event_at_machine(
            &store,
            "shared machine filter target",
            None,
            "machine_devbox_111",
        )
        .0;
        import_event_at_machine(
            &store,
            "shared machine filter other",
            None,
            "machine_laptop_222",
        );
        refresh(&store).expect("refresh search");

        let response = search(
            &store,
            "shared machine filter",
            SearchOptions::new(10, SortMode::Relevance, 0.0)
                .with_mode(SearchMode::Lexical)
                .with_machine_filter(Some("machine_devbox_111".to_string()), None),
            None,
            None,
        )
        .expect("search");

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, target_id);
        assert_eq!(response.results[0].machine_id, "machine_devbox_111");
    }

    #[test]
    fn lexical_search_uses_composable_history_item_tiers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let conversation =
            import_user_event_and_project(&store, "human visible corpus marker request");
        let tool = import_event_and_project_with_kind_at(
            &store,
            r#"{"payload":{"name":"shell","arguments":{"cmd":"printf toolmarker"}}}"#,
            "tool_call",
            None,
        );
        let raw = import_event_and_project_with_kind_at(
            &store,
            "# AGENTS.md instructions rawbootstrap marker",
            "user",
            None,
        );

        let default = search(
            &store,
            "toolmarker",
            SearchOptions::new(10, SortMode::Relevance, 0.0).with_mode(SearchMode::Lexical),
            None,
            None,
        )
        .expect("default search");
        assert!(default.results.is_empty());

        let tools = search(
            &store,
            "toolmarker",
            SearchOptions::new(10, SortMode::Relevance, 0.0)
                .with_mode(SearchMode::Lexical)
                .with_corpus(SearchCorpus::conversation_with_tools()),
            None,
            None,
        )
        .expect("tool search");
        assert_eq!(tools.results.len(), 1);
        assert_eq!(tools.results[0].event_id, tool.event_id);
        assert_eq!(tools.results[0].tier.as_deref(), Some("tool"));
        assert_eq!(tools.results[0].kind, "tool_call");
        assert!(tools.results[0].history_item_id.is_some());

        let conversation_hits = search(
            &store,
            "human visible corpus",
            SearchOptions::new(10, SortMode::Relevance, 0.0).with_mode(SearchMode::Lexical),
            None,
            None,
        )
        .expect("conversation search");
        assert_eq!(conversation_hits.results.len(), 1);
        assert_eq!(conversation_hits.results[0].event_id, conversation.event_id);
        assert_eq!(
            conversation_hits.results[0].tier.as_deref(),
            Some("conversation")
        );

        let raw_default = search(
            &store,
            "rawbootstrap",
            SearchOptions::new(10, SortMode::Relevance, 0.0).with_mode(SearchMode::Lexical),
            None,
            None,
        )
        .expect("default raw search");
        assert!(raw_default.results.is_empty());

        let raw_hits = search(
            &store,
            "rawbootstrap",
            SearchOptions::new(10, SortMode::Relevance, 0.0)
                .with_mode(SearchMode::Lexical)
                .with_corpus(SearchCorpus::raw()),
            None,
            None,
        )
        .expect("raw search");
        assert_eq!(raw_hits.results.len(), 1);
        assert_eq!(raw_hits.results[0].event_id, raw.event_id);
        assert_eq!(raw_hits.results[0].tier.as_deref(), Some("raw"));
    }

    #[test]
    fn semantic_search_respects_selected_history_item_tiers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let tool = import_event_and_project_with_kind_at(
            &store,
            r#"{"payload":{"name":"shell","arguments":"payment failed during the stripe charge and customer money did not process correctly with enough diagnostic context for semantic retrieval toolsemanticmarker"}}"#,
            "tool_call",
            None,
        );
        let tool_item = store
            .history_items_for_event(&tool.event_id)
            .expect("history items")
            .into_iter()
            .find(|item| item.tier == "tool")
            .expect("tool item");
        store
            .import_record(&ArchiveRecord::Embedding(
                fixture_embedding_for_history_item(&tool_item, unit_vector(41)),
            ))
            .expect("tool embedding");
        store
            .refresh_vector_projection()
            .expect("vector projection");
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(41),
        };

        let default = search(
            &store,
            "payment failed",
            SearchOptions::new(5, SortMode::Relevance, 0.0).with_mode(SearchMode::Semantic),
            Some(&embedder),
            None,
        )
        .expect("default semantic search");
        assert!(default.results.is_empty());

        let tool_semantic = search(
            &store,
            "payment failed",
            SearchOptions::new(5, SortMode::Relevance, 0.0)
                .with_mode(SearchMode::Semantic)
                .with_corpus(SearchCorpus::parse("tool").expect("tool corpus")),
            Some(&embedder),
            None,
        )
        .expect("tool semantic search");
        assert_eq!(tool_semantic.results.len(), 1);
        assert_eq!(
            tool_semantic.results[0].history_item_id.as_deref(),
            Some(tool_item.id.as_str())
        );
        assert_eq!(tool_semantic.results[0].tier.as_deref(), Some("tool"));
    }

    #[test]
    fn hybrid_tool_corpus_keeps_lexical_hits_without_required_semantic_degradation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let tool = import_event_and_project_with_kind_at(
            &store,
            r#"{"payload":{"name":"shell","arguments":"toolmissingmarker payment failed during stripe processing with enough diagnostic words to qualify for optional semantic embedding coverage"}}"#,
            "tool_call",
            None,
        );
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(43),
        };

        let response = search(
            &store,
            "toolmissingmarker",
            SearchOptions::new(5, SortMode::Relevance, 0.0)
                .with_corpus(SearchCorpus::conversation_with_tools()),
            Some(&embedder),
            None,
        )
        .expect("hybrid search");

        assert_eq!(response.degraded_reason, None);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, tool.event_id);
        assert_eq!(response.results[0].tier.as_deref(), Some("tool"));
        assert_eq!(response.results[0].lexical_rank, Some(1));
        assert_eq!(response.results[0].semantic_rank, None);
    }

    #[test]
    fn semantic_search_filters_by_hostname_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let target = import_event_and_project_with_kind_at_machine(
            &store,
            "shared semantic machine target with enough surrounding context for useful vector retrieval",
            "user",
            None,
            "machine_dev_box_111",
        );
        let other = import_event_and_project_with_kind_at_machine(
            &store,
            "shared semantic machine other with enough surrounding context for useful vector retrieval",
            "user",
            None,
            "machine_other_222",
        );
        for unit in [&target, &other] {
            store
                .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                    unit,
                    unit_vector(31),
                )))
                .expect("embedding");
        }
        store
            .refresh_vector_projection()
            .expect("vector projection");
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(31),
        };

        let response = search(
            &store,
            "conceptual neighbor",
            SearchOptions::new(10, SortMode::Relevance, 0.0)
                .with_mode(SearchMode::Semantic)
                .with_machine_filter(None, Some("Dev-Box".to_string())),
            Some(&embedder),
            None,
        )
        .expect("search");

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, target.event_id);
        assert_eq!(response.results[0].machine_id, "machine_dev_box_111");
    }

    #[test]
    fn semantic_search_honors_time_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let old_time = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("old time")
            .with_timezone(&Utc);
        let new_time = DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
            .expect("new time")
            .with_timezone(&Utc);
        let old_unit = import_event_and_project_with_kind_at(
            &store,
            "old semantic target with enough surrounding context for useful vector retrieval during regression testing",
            "user",
            Some(old_time),
        );
        let new_unit = import_event_and_project_with_kind_at(
            &store,
            "new semantic target with enough surrounding context for useful vector retrieval during regression testing",
            "user",
            Some(new_time),
        );
        for unit in [&old_unit, &new_unit] {
            store
                .import_record(&ArchiveRecord::Embedding(fixture_embedding(
                    unit,
                    unit_vector(19),
                )))
                .expect("embedding");
        }
        store
            .refresh_vector_projection()
            .expect("vector projection");
        let embedder = FixtureEmbedder {
            model_id: "fixture-semantic-384",
            vector: unit_vector(19),
        };

        let response = search(
            &store,
            "conceptual neighbor",
            SearchOptions::new(10, SortMode::Relevance, 0.0).with_time_window(
                Some(
                    DateTime::parse_from_rfc3339("2026-01-15T00:00:00Z")
                        .expect("after")
                        .with_timezone(&Utc),
                ),
                None,
            ),
            Some(&embedder),
            None,
        )
        .expect("search");

        assert!(response
            .results
            .iter()
            .any(|result| result.event_id == new_unit.event_id));
        assert!(!response
            .results
            .iter()
            .any(|result| result.event_id == old_unit.event_id));
    }

    fn import_event_and_project(store: &Store, search_text: &str) -> SearchUnitRecord {
        import_event_and_project_at(store, search_text, None)
    }

    fn import_user_event_and_project(store: &Store, search_text: &str) -> SearchUnitRecord {
        import_event_and_project_with_kind_at(store, search_text, "user", None)
    }

    fn import_event_and_project_at(
        store: &Store,
        search_text: &str,
        occurred_at: Option<DateTime<Utc>>,
    ) -> SearchUnitRecord {
        import_event_and_project_with_kind_at(store, search_text, "assistant", occurred_at)
    }

    fn import_event_and_project_with_kind_at(
        store: &Store,
        search_text: &str,
        search_kind: &str,
        occurred_at: Option<DateTime<Utc>>,
    ) -> SearchUnitRecord {
        import_event_and_project_with_kind_at_machine(
            store,
            search_text,
            search_kind,
            occurred_at,
            "machine_fixture",
        )
    }

    fn import_event_and_project_with_kind_at_machine(
        store: &Store,
        search_text: &str,
        search_kind: &str,
        occurred_at: Option<DateTime<Utc>>,
        machine_id: &str,
    ) -> SearchUnitRecord {
        let (event_id, delta) = import_event_with_kind_at_machine(
            store,
            search_text,
            search_kind,
            occurred_at,
            machine_id,
        );
        refresh_incremental(store, &delta).expect("refresh projection");

        let text_hash = crate::archive::blake3_hex(search_text.as_bytes());
        let unit_id = stable_id(&["search_unit", &event_id, &text_hash]);
        SearchUnitRecord {
            id: unit_id,
            event_id,
            session_id: stable_id(&["session", search_text]),
            source_id: stable_id(&["source", search_text]),
            machine_id: machine_id.to_string(),
            source_kind: "fixture".to_string(),
            role: Some(search_kind.to_string()),
            search_kind: search_kind.to_string(),
            text: search_text.to_string(),
            text_hash,
            occurred_at,
            metadata: json!({}),
            hash: "unused".to_string(),
        }
    }

    fn import_event_only(store: &Store, search_text: &str) -> (String, ImportDelta) {
        import_event_at(store, search_text, None)
    }

    fn import_many_events(store: &Store, count: usize, label: &str) -> (Vec<String>, ImportDelta) {
        let source = SourceRecord {
            id: stable_id(&["source", label]),
            kind: "fixture".to_string(),
            identity: label.to_string(),
            path: None,
            first_seen_at: Utc::now(),
            updated_at: Utc::now(),
            hash: stable_hash(&("source", label)).expect("source hash"),
        };
        let session = SessionRecord {
            id: stable_id(&["session", label]),
            source_id: source.id.clone(),
            machine_id: "machine_fixture".to_string(),
            source_kind: "fixture".to_string(),
            external_id: label.to_string(),
            title: None,
            status: "closed".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({}),
            hash: stable_hash(&("session", label)).expect("session hash"),
        };
        let mut event_ids = Vec::with_capacity(count);
        let mut records = vec![
            ArchiveRecord::Source(source.clone()),
            ArchiveRecord::Session(session.clone()),
        ];
        for idx in 0..count {
            let search_text = format!("{label} {idx}");
            let event_id = stable_id(&["event", label, &idx.to_string()]);
            let event_hash = stable_hash(&("event", label, idx)).expect("event hash");
            records.push(ArchiveRecord::Event(EventRecord {
                id: event_id.clone(),
                session_id: session.id.clone(),
                source_id: source.id.clone(),
                machine_id: "machine_fixture".to_string(),
                source_kind: "fixture".to_string(),
                ordinal: idx as i64,
                event_type: "assistant".to_string(),
                role: Some("assistant".to_string()),
                content: search_text.clone(),
                raw_artifact_hash: None,
                occurred_at: None,
                metadata: json!({
                    "search_indexable": true,
                    "search_kind": "assistant",
                    "search_text": search_text
                }),
                hash: event_hash,
            }));
            event_ids.push(event_id);
        }
        let stats = store
            .import_records(&records)
            .expect("import fixture events");
        (event_ids, stats.delta)
    }

    fn import_event_at(
        store: &Store,
        search_text: &str,
        occurred_at: Option<DateTime<Utc>>,
    ) -> (String, ImportDelta) {
        import_event_at_machine(store, search_text, occurred_at, "machine_fixture")
    }

    fn import_event_at_machine(
        store: &Store,
        search_text: &str,
        occurred_at: Option<DateTime<Utc>>,
        machine_id: &str,
    ) -> (String, ImportDelta) {
        import_event_with_kind_at_machine(store, search_text, "assistant", occurred_at, machine_id)
    }

    fn import_event_with_kind_at_machine(
        store: &Store,
        search_text: &str,
        search_kind: &str,
        occurred_at: Option<DateTime<Utc>>,
        machine_id: &str,
    ) -> (String, ImportDelta) {
        let source = SourceRecord {
            id: stable_id(&["source", search_text]),
            kind: "fixture".to_string(),
            identity: search_text.to_string(),
            path: None,
            first_seen_at: Utc::now(),
            updated_at: Utc::now(),
            hash: stable_hash(&("source", search_text)).expect("source hash"),
        };
        let session = SessionRecord {
            id: stable_id(&["session", search_text]),
            source_id: source.id.clone(),
            machine_id: machine_id.to_string(),
            source_kind: "fixture".to_string(),
            external_id: search_text.to_string(),
            title: None,
            status: "closed".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({}),
            hash: stable_hash(&("session", search_text)).expect("session hash"),
        };
        let event_id = stable_id(&["event", search_text]);
        let event_hash = stable_hash(&("event", search_text)).expect("event hash");
        let event = EventRecord {
            id: event_id.clone(),
            session_id: session.id.clone(),
            source_id: source.id.clone(),
            machine_id: machine_id.to_string(),
            source_kind: "fixture".to_string(),
            ordinal: 0,
            event_type: search_kind.to_string(),
            role: Some(search_kind.to_string()),
            content: search_text.to_string(),
            raw_artifact_hash: None,
            occurred_at,
            metadata: json!({
                "search_indexable": true,
                "search_kind": search_kind,
                "search_text": search_text
            }),
            hash: event_hash,
        };
        let stats = store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(event),
            ])
            .expect("import fixture event");
        (event_id, stats.delta)
    }

    fn fixture_embedding(unit: &SearchUnitRecord, vector: Vec<f32>) -> EmbeddingRecord {
        let vector_blob = f32_vector_to_blob(&vector);
        let vector_hash = crate::archive::blake3_hex(&vector_blob);
        let history_item_id = history_item_id_for_search_unit(unit);
        let id = stable_id(&[
            "embedding",
            &history_item_id,
            &unit.text_hash,
            "fixture-semantic-384",
        ]);
        EmbeddingRecord {
            id: id.clone(),
            unit_id: history_item_id.clone(),
            text_hash: unit.text_hash.clone(),
            model_id: "fixture-semantic-384".to_string(),
            dims: 384,
            vector_hash: vector_hash.clone(),
            vector: vector_blob,
            producer_machine_id: "machine_fixture".to_string(),
            embedded_at: Utc::now(),
            metadata: json!({}),
            hash: stable_hash(&(&id, &unit.id, &unit.text_hash, &vector_hash))
                .expect("embedding hash"),
        }
    }

    fn fixture_embedding_for_history_item(
        item: &HistoryItemRecord,
        vector: Vec<f32>,
    ) -> EmbeddingRecord {
        let vector_blob = f32_vector_to_blob(&vector);
        let vector_hash = crate::archive::blake3_hex(&vector_blob);
        let id = stable_id(&[
            "embedding",
            &item.id,
            &item.text_hash,
            "fixture-semantic-384",
        ]);
        EmbeddingRecord {
            id: id.clone(),
            unit_id: item.id.clone(),
            text_hash: item.text_hash.clone(),
            model_id: "fixture-semantic-384".to_string(),
            dims: 384,
            vector_hash: vector_hash.clone(),
            vector: vector_blob,
            producer_machine_id: "machine_fixture".to_string(),
            embedded_at: Utc::now(),
            metadata: json!({}),
            hash: stable_hash(&(&id, &item.id, &item.text_hash, &vector_hash))
                .expect("embedding hash"),
        }
    }

    fn history_item_id_for_search_unit(unit: &SearchUnitRecord) -> String {
        stable_id(&[
            "history_item",
            &unit.event_id,
            "0",
            "conversation",
            &unit.search_kind,
            &unit.text_hash,
        ])
    }

    struct FixtureEmbedder {
        model_id: &'static str,
        vector: Vec<f32>,
    }

    impl Embedder for FixtureEmbedder {
        fn model_id(&self) -> &str {
            self.model_id
        }

        fn dims(&self) -> usize {
            384
        }

        fn is_semantic(&self) -> bool {
            true
        }

        fn embed_batch(&self, texts: &[String], _batch_size: usize) -> Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| self.vector.clone()).collect())
        }
    }

    fn memory_sample(
        total_gib: u64,
        available_gib: u64,
        process_rss_mib: u64,
    ) -> Option<MemorySample> {
        Some(MemorySample {
            total_bytes: Some(total_gib * 1024 * 1024 * 1024),
            available_bytes: Some(available_gib * 1024 * 1024 * 1024),
            process_rss_bytes: Some(process_rss_mib * 1024 * 1024),
        })
    }

    fn unit_vector(index: usize) -> Vec<f32> {
        let mut vector = vec![0.0; 384];
        vector[index] = 1.0;
        vector
    }

    fn ranked_row(event_id: &str, rank: usize, occurred_at: chrono::DateTime<Utc>) -> SearchRow {
        ranked_row_with_content(event_id, event_id, rank, occurred_at)
    }

    fn ranked_row_with_kind(
        event_id: &str,
        search_kind: &str,
        rank: usize,
        occurred_at: chrono::DateTime<Utc>,
    ) -> SearchRow {
        ranked_row_with_content_and_kind(event_id, event_id, search_kind, rank, occurred_at)
    }

    fn ranked_row_with_content(
        event_id: &str,
        content: &str,
        rank: usize,
        occurred_at: chrono::DateTime<Utc>,
    ) -> SearchRow {
        ranked_row_with_content_and_kind(event_id, content, "user", rank, occurred_at)
    }

    fn ranked_row_with_content_and_kind(
        event_id: &str,
        content: &str,
        search_kind: &str,
        rank: usize,
        occurred_at: chrono::DateTime<Utc>,
    ) -> SearchRow {
        SearchRow {
            history_item_id: Some(format!("history_item_{event_id}")),
            event_id: event_id.to_string(),
            session_id: "session".to_string(),
            machine_id: "machine_fixture".to_string(),
            source_kind: "fixture".to_string(),
            tier: Some("conversation".to_string()),
            search_kind: search_kind.to_string(),
            content: content.to_string(),
            occurred_at: Some(occurred_at),
            session_title: Some("fixture session".to_string()),
            workspace_values: vec!["/tmp/fixture".to_string()],
            rank,
        }
    }
}
