#![cfg_attr(not(feature = "analytics-topics"), allow(dead_code))]

use crate::annotate::JsonLlm;
use crate::archive::ArchiveRecord;
#[cfg(feature = "semantic-fastembed")]
use crate::embed::EmbedderConfig;
use crate::embed::{Embedder, DEFAULT_SEMANTIC_DIMS};
use crate::provenance;
use crate::storage::{HistoryItemEmbeddingCursor, HistoryItemForEmbedding, Store};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
#[cfg(feature = "analytics-topics")]
use uuid::Uuid;

const BATCH_SIZE: usize = 64;
const TOPIC_CLUSTER_ALGORITHM_VERSION: u32 = 2;
const MIN_TOPIC_SILHOUETTE: f64 = 0.18;
const MISC_TOPIC_LABEL: &str = "miscellaneous";
const MISC_TOPIC_LABELER_VERSION: &str = "coherence-gate-v1";

#[derive(Debug, Clone)]
pub struct TopicEmbeddingOutcome {
    pub model_id: String,
    pub embedded: usize,
    pub reused: usize,
    pub pending: usize,
    pub vectors_indexed: usize,
    pub elapsed: Duration,
}

impl TopicEmbeddingOutcome {
    pub fn per_second(&self) -> f64 {
        if self.elapsed.is_zero() {
            return self.embedded as f64;
        }
        self.embedded as f64 / self.elapsed.as_secs_f64()
    }
}

#[cfg(feature = "semantic-fastembed")]
pub fn load_embedder(data_dir: &std::path::Path) -> Result<Box<dyn Embedder>> {
    let config = EmbedderConfig::from_config_and_env(data_dir, true);
    let embedder = config.load()?;
    validate_embedder(embedder.as_ref())?;
    Ok(embedder)
}

#[cfg(not(feature = "semantic-fastembed"))]
pub fn load_embedder(_data_dir: &std::path::Path) -> Result<Box<dyn Embedder>> {
    bail!("topic embeddings require the semantic-fastembed feature")
}

pub fn backfill(
    store: &Store,
    machine_id: &str,
    embedder: &dyn Embedder,
    limit: Option<usize>,
) -> Result<TopicEmbeddingOutcome> {
    validate_embedder(embedder)?;
    let started = Instant::now();
    let model_id = embedder.model_id();
    let total = human_topic_item_count(store)?;
    let missing_before = missing_topic_item_count(store, model_id)?;
    let reused = total.saturating_sub(missing_before);
    let mut vectors_indexed = repair_vector_projection(store, model_id)?;
    let mut embedded = 0;
    let target = limit.unwrap_or(usize::MAX);

    while embedded < target {
        let batch_limit = BATCH_SIZE.min(target - embedded);
        let items = missing_topic_items(store, model_id, batch_limit)?;
        if items.is_empty() {
            break;
        }
        let texts = items
            .iter()
            .map(|item| {
                crate::search::embedding_input(&provenance::strip_human_wrapper(
                    &item.unit.text,
                    &item.rule,
                ))
            })
            .collect::<Vec<_>>();
        let vectors = embedder.embed_batch(&texts, batch_limit)?;
        if vectors.len() != items.len() {
            bail!(
                "embedder returned {} vectors for {} topic items",
                vectors.len(),
                items.len()
            );
        }

        let records = items
            .iter()
            .zip(vectors)
            .map(|(item, vector)| {
                crate::search::embedding_record(machine_id, embedder, &item.unit, vector)
            })
            .collect::<Result<Vec<ArchiveRecord>>>()?;
        let stats = store.import_records(&records)?;
        if stats.inserted == 0 {
            bail!("topic embedding batch did not store any vectors");
        }
        embedded += stats.inserted;
        vectors_indexed +=
            store.refresh_vector_projection_for_embeddings(&stats.delta.inserted_embeddings)?;
    }

    Ok(TopicEmbeddingOutcome {
        model_id: model_id.to_string(),
        embedded,
        reused,
        pending: missing_topic_item_count(store, model_id)?,
        vectors_indexed,
        elapsed: started.elapsed(),
    })
}

fn validate_embedder(embedder: &dyn Embedder) -> Result<()> {
    if !embedder.is_semantic() || embedder.dims() != DEFAULT_SEMANTIC_DIMS {
        bail!("topic embeddings require a local 384-dimensional semantic embedder");
    }
    Ok(())
}

struct TopicItem {
    unit: HistoryItemForEmbedding,
    rule: String,
}

fn missing_topic_items(store: &Store, model_id: &str, limit: usize) -> Result<Vec<TopicItem>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT hi.id, hi.text, hi.text_hash, COALESCE(hi.occurred_at, ''),
                    hi.session_id, hi.ordinal, hi.subordinal, p.rule
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             LEFT JOIN embeddings e
               ON e.unit_id = hi.id
              AND e.text_hash = hi.text_hash
              AND e.model_id = ?1
             WHERE p.authored_by = 'human'
               AND COALESCE(p.sentiment_usable, 'no') != 'no'
               AND e.id IS NULL
             ORDER BY hi.rowid
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![model_id, limit as i64], |row| {
            Ok(TopicItem {
                unit: HistoryItemForEmbedding {
                    id: row.get(0)?,
                    text: row.get(1)?,
                    text_hash: row.get(2)?,
                    cursor: HistoryItemEmbeddingCursor {
                        occurred_at_key: row.get(3)?,
                        session_id: row.get(4)?,
                        ordinal: row.get(5)?,
                        subordinal: row.get(6)?,
                        id: row.get(0)?,
                    },
                },
                rule: row.get(7)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn human_topic_item_count(store: &Store) -> Result<usize> {
    store.with_conn(|conn| {
        let count = conn.query_row(
            "SELECT COUNT(*)
             FROM message_provenance
             WHERE authored_by = 'human'
               AND COALESCE(sentiment_usable, 'no') != 'no'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count.max(0) as usize)
    })
}

fn missing_topic_item_count(store: &Store, model_id: &str) -> Result<usize> {
    store.with_conn(|conn| {
        let count = conn.query_row(
            "SELECT COUNT(*)
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             LEFT JOIN embeddings e
               ON e.unit_id = hi.id
              AND e.text_hash = hi.text_hash
              AND e.model_id = ?1
             WHERE p.authored_by = 'human'
               AND COALESCE(p.sentiment_usable, 'no') != 'no'
               AND e.id IS NULL",
            params![model_id],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count.max(0) as usize)
    })
}

fn repair_vector_projection(store: &Store, model_id: &str) -> Result<usize> {
    let ids = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT e.id
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             JOIN embeddings e
               ON e.unit_id = hi.id
              AND e.text_hash = hi.text_hash
              AND e.model_id = ?1
              AND e.dims = 384
             LEFT JOIN vec_embeddings_384 v ON v.rowid = e.rowid
             WHERE p.authored_by = 'human'
               AND COALESCE(p.sentiment_usable, 'no') != 'no'
               AND v.rowid IS NULL",
        )?;
        let rows = stmt.query_map(params![model_id], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<String>>>()
            .map_err(Into::into)
    })?;
    store.refresh_vector_projection_for_embeddings(&ids)
}

#[derive(Debug, Clone)]
pub struct ClusterOptions {
    pub min_k: usize,
    pub max_k: usize,
    pub step: usize,
    pub sample_size: usize,
    pub rebuild: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterCandidate {
    pub k: usize,
    pub silhouette: f64,
}

#[derive(Debug, Clone)]
pub struct ClusterOutcome {
    pub version: String,
    pub item_count: usize,
    pub selected_k: usize,
    pub silhouette: f64,
    pub candidates: Vec<ClusterCandidate>,
    pub reused: bool,
    pub demoted: bool,
}

#[cfg(feature = "analytics-topics")]
pub fn cluster(
    store: &Store,
    options: &ClusterOptions,
    mut progress: impl FnMut(&ClusterCandidate),
) -> Result<ClusterOutcome> {
    validate_cluster_options(options)?;
    let dataset = load_topic_dataset(store)?;
    if !options.rebuild {
        if let Some(mut existing) =
            completed_cluster_for_input(store, &dataset.model_id, &dataset.input_hash)?
        {
            existing.reused = true;
            return Ok(existing);
        }
    }

    let sample = sample_points(&dataset.points, DEFAULT_SEMANTIC_DIMS, options.sample_size);
    let sample_count = sample.len() / DEFAULT_SEMANTIC_DIMS;
    let candidates = candidate_ks(options)
        .into_iter()
        .filter(|k| *k < sample_count)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        bail!("topic clustering needs more sampled messages than candidate clusters");
    }

    let mut scores = Vec::with_capacity(candidates.len());
    for k in candidates {
        let model = fit_kmeans(&sample, k, 30, 1)?;
        let assignments = model
            .predict(&sample)
            .context("assigning topic score sample")?;
        let candidate = ClusterCandidate {
            k,
            silhouette: centroid_silhouette(
                &sample,
                DEFAULT_SEMANTIC_DIMS,
                model.centroids(),
                k,
                &assignments,
            ),
        };
        progress(&candidate);
        scores.push(candidate);
    }
    let best = scores
        .iter()
        .max_by(|left, right| left.silhouette.total_cmp(&right.silhouette))
        .expect("non-empty candidates")
        .clone();
    let demoted = best.silhouette < MIN_TOPIC_SILHOUETTE;
    let (selected_k, centroids, assignments) = if demoted {
        (
            1,
            mean_centroid(&dataset.points, DEFAULT_SEMANTIC_DIMS),
            vec![0; dataset.item_ids.len()],
        )
    } else {
        let model = fit_kmeans_minibatch(&dataset.points, best.k)?;
        let assignments = model
            .predict(&dataset.points)
            .context("assigning topic corpus")?;
        (best.k, model.centroids().to_vec(), assignments)
    };
    let distances = assigned_distances(
        &dataset.points,
        DEFAULT_SEMANTIC_DIMS,
        &centroids,
        &assignments,
    );
    let version = format!("topics_{}", Uuid::new_v4().simple());
    start_cluster_run(store, &version, &dataset)?;
    persist_cluster_run(
        store,
        &version,
        &dataset,
        &centroids,
        &assignments,
        &distances,
        &scores,
        selected_k,
        best.silhouette,
    )?;
    prune_topic_versions(store)?;

    Ok(ClusterOutcome {
        version,
        item_count: dataset.item_ids.len(),
        selected_k,
        silhouette: best.silhouette,
        candidates: scores,
        reused: false,
        demoted,
    })
}

#[cfg(not(feature = "analytics-topics"))]
pub fn cluster(
    _store: &Store,
    _options: &ClusterOptions,
    _progress: impl FnMut(&ClusterCandidate),
) -> Result<ClusterOutcome> {
    bail!("topic clustering requires the analytics-topics feature")
}

fn validate_cluster_options(options: &ClusterOptions) -> Result<()> {
    if options.min_k < 2 {
        bail!("minimum topic count must be at least two");
    }
    if options.max_k < options.min_k {
        bail!("maximum topic count must not be smaller than the minimum");
    }
    if options.step == 0 {
        bail!("topic count step must be greater than zero");
    }
    if options.sample_size <= options.max_k {
        bail!("topic sample size must be greater than the maximum topic count");
    }
    Ok(())
}

fn candidate_ks(options: &ClusterOptions) -> Vec<usize> {
    (options.min_k..=options.max_k)
        .step_by(options.step)
        .collect()
}

struct TopicDataset {
    model_id: String,
    input_hash: String,
    item_ids: Vec<String>,
    points: Vec<f32>,
}

fn load_topic_dataset(store: &Store) -> Result<TopicDataset> {
    let total = human_topic_item_count(store)?;
    let model_id = store
        .with_conn(|conn| {
            conn.query_row(
                "SELECT e.model_id
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             JOIN embeddings e ON e.unit_id = hi.id AND e.text_hash = hi.text_hash
             WHERE p.authored_by = 'human'
               AND COALESCE(p.sentiment_usable, 'no') != 'no'
               AND e.dims = 384
             GROUP BY e.model_id
             ORDER BY COUNT(*) DESC, e.model_id
             LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })?
        .context("no human topic embeddings found; run `histo lab topics embed`")?;
    let rows = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT hi.id, e.vector
             FROM message_provenance p
             JOIN history_items hi ON hi.id = p.item_id
             JOIN embeddings e
               ON e.unit_id = hi.id
              AND e.text_hash = hi.text_hash
              AND e.model_id = ?1
              AND e.dims = 384
             WHERE p.authored_by = 'human'
               AND COALESCE(p.sentiment_usable, 'no') != 'no'
             ORDER BY hi.rowid",
        )?;
        let rows = stmt.query_map(params![model_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    if rows.len() != total {
        bail!(
            "topic embeddings are incomplete: {} of {} human messages ready; run `histo lab topics embed`",
            rows.len(),
            total
        );
    }
    let mut item_ids = Vec::with_capacity(rows.len());
    let mut points = Vec::with_capacity(rows.len() * DEFAULT_SEMANTIC_DIMS);
    for (item_id, vector) in rows {
        let vector = crate::storage::f32_vector_from_blob(&vector)?;
        if vector.len() != DEFAULT_SEMANTIC_DIMS {
            bail!("topic embedding {item_id} is not 384-dimensional");
        }
        item_ids.push(item_id);
        points.extend(vector);
    }
    let input_hash =
        crate::archive::stable_hash(&(TOPIC_CLUSTER_ALGORITHM_VERSION, &model_id, &item_ids))?;
    Ok(TopicDataset {
        model_id,
        input_hash,
        item_ids,
        points,
    })
}

fn sample_points(points: &[f32], dims: usize, limit: usize) -> Vec<f32> {
    let count = points.len() / dims;
    let sample_count = count.min(limit);
    if sample_count == count {
        return points.to_vec();
    }
    let mut sample = Vec::with_capacity(sample_count * dims);
    for index in 0..sample_count {
        let source = index * count / sample_count;
        sample.extend_from_slice(&points[source * dims..(source + 1) * dims]);
    }
    sample
}

#[cfg(feature = "analytics-topics")]
fn fit_kmeans(
    points: &[f32],
    k: usize,
    iterations: usize,
    attempts: usize,
) -> Result<kmeans_uni::KMeans<f32>> {
    kmeans_uni::KMeansBuilder::new(k)
        .iterations(iterations)
        .attempts(attempts)
        .seed(0x4849_5354_4f52_494f_u64.wrapping_add(k as u64))
        .cpu_simd()
        .euclidean()
        .parallel()
        .fit(points, DEFAULT_SEMANTIC_DIMS)
        .context("fitting topic k-means")
}

#[cfg(feature = "analytics-topics")]
fn fit_kmeans_minibatch(points: &[f32], k: usize) -> Result<kmeans_uni::KMeans<f32>> {
    let source = kmeans_uni::SlicePointSource::new(points, DEFAULT_SEMANTIC_DIMS)
        .context("reading topic vectors for k-means")?;
    kmeans_uni::KMeansBuilder::new(k)
        .iterations(100)
        .attempts(2)
        .seed(0x4849_5354_4f52_494f_u64.wrapping_add(k as u64))
        .cpu_simd()
        .euclidean()
        .parallel()
        .fit_mini_batch_from_source(&source, 1_024.min(points.len() / DEFAULT_SEMANTIC_DIMS))
        .context("fitting full topic k-means")
}

fn centroid_silhouette(
    points: &[f32],
    dims: usize,
    centroids: &[f32],
    k: usize,
    assignments: &[usize],
) -> f64 {
    let mut total = 0.0;
    for (point, own) in points.chunks_exact(dims).zip(assignments.iter().copied()) {
        let mut own_distance = 0.0;
        let mut nearest_other = f32::INFINITY;
        for (topic_id, centroid) in centroids.chunks_exact(dims).take(k).enumerate() {
            let distance = squared_distance(point, centroid).sqrt();
            if topic_id == own {
                own_distance = distance;
            } else {
                nearest_other = nearest_other.min(distance);
            }
        }
        let denominator = own_distance.max(nearest_other);
        if denominator > 0.0 && denominator.is_finite() {
            total += ((nearest_other - own_distance) / denominator) as f64;
        }
    }
    total / assignments.len().max(1) as f64
}

fn assigned_distances(
    points: &[f32],
    dims: usize,
    centroids: &[f32],
    assignments: &[usize],
) -> Vec<f32> {
    points
        .chunks_exact(dims)
        .zip(assignments.iter().copied())
        .map(|(point, topic_id)| {
            squared_distance(point, &centroids[topic_id * dims..(topic_id + 1) * dims]).sqrt()
        })
        .collect()
}

fn mean_centroid(points: &[f32], dims: usize) -> Vec<f32> {
    let mut centroid = vec![0.0; dims];
    let count = points.len() / dims;
    for point in points.chunks_exact(dims) {
        for (mean, value) in centroid.iter_mut().zip(point) {
            *mean += value;
        }
    }
    if count > 0 {
        for value in &mut centroid {
            *value /= count as f32;
        }
    }
    centroid
}

fn squared_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn start_cluster_run(store: &Store, version: &str, dataset: &TopicDataset) -> Result<()> {
    store.with_conn(|conn| {
        conn.execute(
            "INSERT INTO topic_runs
             (version, algorithm_version, model_id, input_hash, item_count, status, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'building', ?6)",
            params![
                version,
                TOPIC_CLUSTER_ALGORITHM_VERSION,
                dataset.model_id,
                dataset.input_hash,
                dataset.item_ids.len() as i64,
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
fn persist_cluster_run(
    store: &Store,
    version: &str,
    dataset: &TopicDataset,
    centroids: &[f32],
    assignments: &[usize],
    distances: &[f32],
    candidates: &[ClusterCandidate],
    selected_k: usize,
    silhouette: f64,
) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting topic cluster write")?;
        let mut sizes = vec![0usize; selected_k];
        for topic_id in assignments {
            sizes[*topic_id] += 1;
        }
        {
            let mut stmt = tx.prepare(
                "INSERT INTO topics (version, topic_id, size, centroid)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (topic_id, centroid) in centroids
                .chunks_exact(DEFAULT_SEMANTIC_DIMS)
                .take(selected_k)
                .enumerate()
            {
                stmt.execute(params![
                    version,
                    topic_id as i64,
                    sizes[topic_id] as i64,
                    crate::storage::f32_vector_to_blob(centroid),
                ])?;
            }
        }
        {
            let mut stmt = tx.prepare(
                "INSERT INTO topic_assignments (version, item_id, topic_id, distance)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for ((item_id, topic_id), distance) in
                dataset.item_ids.iter().zip(assignments).zip(distances)
            {
                stmt.execute(params![version, item_id, *topic_id as i64, distance])?;
            }
        }
        tx.execute(
            "UPDATE topic_runs
             SET selected_k = ?2, silhouette_score = ?3, candidates_json = ?4,
                 status = 'completed', completed_at = ?5
             WHERE version = ?1",
            params![
                version,
                selected_k as i64,
                silhouette,
                serde_json::to_string(candidates)?,
                Utc::now().to_rfc3339(),
            ],
        )?;
        if silhouette < MIN_TOPIC_SILHOUETTE {
            tx.execute(
                "UPDATE topics
                 SET label = ?2, label_model = 'local', labeler_version = ?3, labeled_at = ?4
                 WHERE version = ?1",
                params![
                    version,
                    MISC_TOPIC_LABEL,
                    MISC_TOPIC_LABELER_VERSION,
                    Utc::now().to_rfc3339(),
                ],
            )?;
        }
        tx.commit().context("committing topic cluster write")
    })
}

fn completed_cluster_for_input(
    store: &Store,
    model_id: &str,
    input_hash: &str,
) -> Result<Option<ClusterOutcome>> {
    let row = store.with_conn(|conn| {
        conn.query_row(
            "SELECT version, item_count, selected_k, silhouette_score, candidates_json
             FROM topic_runs
             WHERE status = 'completed'
               AND algorithm_version = ?1
               AND model_id = ?2
               AND input_hash = ?3
             ORDER BY completed_at DESC
             LIMIT 1",
            params![TOPIC_CLUSTER_ALGORITHM_VERSION, model_id, input_hash],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(Into::into)
    })?;
    row.map(
        |(version, item_count, selected_k, silhouette, candidates)| {
            Ok(ClusterOutcome {
                version,
                item_count: item_count.max(0) as usize,
                selected_k: selected_k.max(0) as usize,
                silhouette,
                candidates: serde_json::from_str(&candidates)?,
                reused: false,
                demoted: silhouette < MIN_TOPIC_SILHOUETTE,
            })
        },
    )
    .transpose()
}

fn prune_topic_versions(store: &Store) -> Result<()> {
    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting topic version pruning")?;
        for table in ["topic_assignments", "topics"] {
            tx.execute(
                &format!(
                    "DELETE FROM {table}
                     WHERE version NOT IN (
                         SELECT version FROM topic_runs
                         WHERE status = 'completed'
                         ORDER BY completed_at DESC
                         LIMIT 2
                     )"
                ),
                [],
            )?;
        }
        tx.execute(
            "DELETE FROM topic_runs
             WHERE status != 'completed'
                OR version NOT IN (
                    SELECT version FROM topic_runs
                    WHERE status = 'completed'
                    ORDER BY completed_at DESC
                    LIMIT 2
                )",
            [],
        )?;
        tx.commit().context("committing topic version pruning")
    })
}

#[derive(Debug, Clone)]
pub struct TopicLabelOutcome {
    pub version: String,
    pub model: String,
    pub labeled: usize,
    pub skipped: usize,
    pub pending: usize,
}

pub fn label_topics(
    store: &Store,
    llm: &dyn JsonLlm,
    labeler_version: &str,
    limit: Option<usize>,
) -> Result<TopicLabelOutcome> {
    let version = current_topic_version(store)?;
    let silhouette = topic_run_silhouette(store, &version)?;
    if silhouette < MIN_TOPIC_SILHOUETTE {
        bail!(
            "topic run scored {silhouette:.4}, below the {MIN_TOPIC_SILHOUETTE:.2} coherence bar; keeping its local miscellaneous label"
        );
    }
    let pending_before = unlabeled_topic_ids(store, &version, labeler_version)?;
    let total = topic_count(store, &version)?;
    let terms = distinctive_topic_terms(store, &version)?;
    let mut labeled = 0;
    for topic_id in pending_before
        .iter()
        .copied()
        .take(limit.unwrap_or(usize::MAX))
    {
        let representatives = topic_representatives(store, &version, topic_id, 5)?;
        let prompt = serde_json::json!({
            "topic_id": topic_id,
            "distinctive_terms": terms.get(&topic_id).cloned().unwrap_or_default(),
            "representative_messages": representatives,
        })
        .to_string();
        let response = llm.complete_json(
            "Name a cluster of the user's coding-agent requests. Return JSON only as {\"label\":\"2-6 concrete words\"}. Use \"misc\" when incoherent. Never infer psychology or personality.",
            &prompt,
        )?;
        let label = parse_topic_label(&response)?;
        store_topic_label(
            store,
            &version,
            topic_id,
            &label,
            llm.model(),
            labeler_version,
        )?;
        labeled += 1;
    }
    let pending = unlabeled_topic_ids(store, &version, labeler_version)?.len();
    Ok(TopicLabelOutcome {
        version,
        model: llm.model().to_string(),
        labeled,
        skipped: total.saturating_sub(pending_before.len()),
        pending,
    })
}

fn current_topic_version(store: &Store) -> Result<String> {
    store.with_conn(|conn| {
        conn.query_row(
            "SELECT version FROM topic_runs
             WHERE status = 'completed' AND algorithm_version = ?1
             ORDER BY completed_at DESC
             LIMIT 1",
            params![TOPIC_CLUSTER_ALGORITHM_VERSION],
            |row| row.get(0),
        )
        .optional()?
        .context("no current completed topic version; run `histo lab topics cluster`")
    })
}

fn topic_run_silhouette(store: &Store, version: &str) -> Result<f64> {
    store.with_conn(|conn| {
        conn.query_row(
            "SELECT silhouette_score FROM topic_runs WHERE version = ?1",
            params![version],
            |row| row.get(0),
        )
        .map_err(Into::into)
    })
}

fn topic_count(store: &Store, version: &str) -> Result<usize> {
    store.with_conn(|conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM topics WHERE version = ?1",
            params![version],
            |row| row.get::<_, i64>(0),
        )? as usize)
    })
}

fn unlabeled_topic_ids(store: &Store, version: &str, labeler_version: &str) -> Result<Vec<i64>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT topic_id FROM topics
             WHERE version = ?1
               AND (label IS NULL OR labeler_version IS NULL OR labeler_version != ?2)
             ORDER BY size DESC, topic_id",
        )?;
        let rows = stmt.query_map(params![version, labeler_version], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
}

fn topic_representatives(
    store: &Store,
    version: &str,
    topic_id: i64,
    limit: usize,
) -> Result<Vec<String>> {
    store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT hi.text, p.rule, p.sentiment_usable
             FROM topic_assignments a
             JOIN history_items hi ON hi.id = a.item_id
             JOIN message_provenance p ON p.item_id = a.item_id
             WHERE a.version = ?1 AND a.topic_id = ?2
             ORDER BY a.distance, a.item_id
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![version, topic_id, limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (text, rule, usable) = row?;
            let text = if usable == "strip_wrapper" {
                provenance::strip_human_wrapper(&text, &rule)
            } else {
                text
            };
            out.push(text.chars().take(800).collect());
        }
        Ok(out)
    })
}

fn distinctive_topic_terms(store: &Store, version: &str) -> Result<HashMap<i64, Vec<String>>> {
    let rows = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT a.topic_id, hi.text, p.rule, p.sentiment_usable
             FROM topic_assignments a
             JOIN history_items hi ON hi.id = a.item_id
             JOIN message_provenance p ON p.item_id = a.item_id
             WHERE a.version = ?1",
        )?;
        let rows = stmt.query_map(params![version], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;
    let stopwords = crate::report::english_stopwords();
    let mut counts = HashMap::<i64, HashMap<String, u64>>::new();
    let mut topic_terms = HashMap::<i64, HashSet<String>>::new();
    for (topic_id, text, rule, usable) in rows {
        let text = if usable == "strip_wrapper" {
            provenance::strip_human_wrapper(&text, &rule)
        } else {
            text
        };
        let unique = crate::report::tokenize_frequency_text(&text)
            .into_iter()
            .filter(|term| term.len() > 1 && !stopwords.contains(term.as_str()))
            .collect::<HashSet<_>>();
        for term in unique {
            *counts
                .entry(topic_id)
                .or_default()
                .entry(term.clone())
                .or_default() += 1;
            topic_terms.entry(topic_id).or_default().insert(term);
        }
    }
    let topic_total = counts.len().max(1) as f64;
    let mut document_frequency = HashMap::<String, usize>::new();
    for terms in topic_terms.values() {
        for term in terms {
            *document_frequency.entry(term.clone()).or_default() += 1;
        }
    }
    Ok(counts
        .into_iter()
        .map(|(topic_id, counts)| {
            let mut scored = counts
                .into_iter()
                .map(|(term, count)| {
                    let df = document_frequency[&term] as f64;
                    let score = count as f64 * ((1.0 + topic_total) / (1.0 + df)).ln();
                    (term, score)
                })
                .collect::<Vec<_>>();
            scored.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            scored.truncate(12);
            (topic_id, scored.into_iter().map(|(term, _)| term).collect())
        })
        .collect())
}

fn parse_topic_label(response: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(response).context("parsing topic label")?;
    let label = value
        .get("label")
        .and_then(serde_json::Value::as_str)
        .context("topic label response did not include a label")?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if label.is_empty() || label.chars().count() > 60 {
        bail!("topic label must contain between 1 and 60 characters");
    }
    Ok(label)
}

fn store_topic_label(
    store: &Store,
    version: &str,
    topic_id: i64,
    label: &str,
    model: &str,
    labeler_version: &str,
) -> Result<()> {
    store.with_conn(|conn| {
        conn.execute(
            "UPDATE topics
             SET label = ?3, label_model = ?4, labeler_version = ?5, labeled_at = ?6
             WHERE version = ?1 AND topic_id = ?2",
            params![
                version,
                topic_id,
                label,
                model,
                labeler_version,
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct FixtureEmbedder {
        inputs: Mutex<Vec<String>>,
    }

    impl Embedder for FixtureEmbedder {
        fn model_id(&self) -> &str {
            "fixture-topic-384"
        }

        fn dims(&self) -> usize {
            DEFAULT_SEMANTIC_DIMS
        }

        fn is_semantic(&self) -> bool {
            true
        }

        fn embed_batch(&self, texts: &[String], _batch_size: usize) -> Result<Vec<Vec<f32>>> {
            self.inputs.lock().expect("inputs").extend_from_slice(texts);
            Ok(texts
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let mut vector = vec![0.0; DEFAULT_SEMANTIC_DIMS];
                    vector[index] = 1.0;
                    vector
                })
                .collect())
        }
    }

    #[test]
    fn topic_embedding_backfill_resumes_and_skips_existing_vectors() {
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
                      ('agent', 'event_3', 'session', 'source', 'machine', 'codex', 2, 0,
                       'conversation', 'user', 'agent message', 'hash_3',
                       1, 'required', '{}', 'item_hash_3'),
                      ('unusable', 'event_4', 'session', 'source', 'machine', 'codex', 3, 0,
                       'conversation', 'user', 'unusable message', 'hash_4',
                       1, 'required', '{}', 'item_hash_4');

                    INSERT INTO message_provenance
                      (item_id, session_id, source_kind, authored_by, sentiment_usable, rule)
                    VALUES
                      ('wrapped', 'session', 'codex', 'human', 'strip_wrapper', 'tag.image'),
                      ('plain', 'session', 'codex', 'human', 'yes', 'default.human'),
                      ('agent', 'session', 'codex', 'agent', 'no', 'session.subagent'),
                      ('unusable', 'session', 'codex', 'human', 'no', 'tag.unknown');
                    "#,
                )?;
                Ok(())
            })
            .expect("insert topic fixtures");
        let embedder = FixtureEmbedder {
            inputs: Mutex::new(Vec::new()),
        };

        let first = backfill(&store, "machine", &embedder, Some(1)).expect("first backfill");
        assert_eq!(first.embedded, 1);
        assert_eq!(first.pending, 1);
        assert_eq!(embedder.inputs.lock().expect("inputs")[0], "useful caption");

        let second = backfill(&store, "machine", &embedder, None).expect("resume backfill");
        assert_eq!(second.embedded, 1);
        assert_eq!(second.reused, 1);
        assert_eq!(second.pending, 0);

        let third = backfill(&store, "machine", &embedder, None).expect("completed backfill");
        assert_eq!(third.embedded, 0);
        assert_eq!(third.reused, 2);
        assert_eq!(third.pending, 0);
        let counts = store
            .with_conn(|conn| {
                Ok((
                    conn.query_row("SELECT COUNT(*) FROM embeddings", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    conn.query_row("SELECT COUNT(*) FROM vec_embeddings_384", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                ))
            })
            .expect("count topic vectors");
        assert_eq!(counts, (2, 2));
    }

    #[cfg(feature = "analytics-topics")]
    #[test]
    fn synthetic_clusters_are_assigned_to_distinct_centroids() {
        let mut points = Vec::new();
        for index in 0..12 {
            let mut point = vec![0.0f32; DEFAULT_SEMANTIC_DIMS];
            point[0] = 1.0;
            point[2] = index as f32 * 0.001;
            points.extend(point);
        }
        for index in 0..12 {
            let mut point = vec![0.0f32; DEFAULT_SEMANTIC_DIMS];
            point[1] = 1.0;
            point[2] = index as f32 * 0.001;
            points.extend(point);
        }

        let model = fit_kmeans(&points, 2, 50, 2).expect("fit synthetic clusters");
        let assignments = model.predict(&points).expect("assign synthetic clusters");
        assert!(assignments[..12]
            .iter()
            .all(|topic| *topic == assignments[0]));
        assert!(assignments[12..]
            .iter()
            .all(|topic| *topic == assignments[12]));
        assert_ne!(assignments[0], assignments[12]);
        assert!(
            centroid_silhouette(
                &points,
                DEFAULT_SEMANTIC_DIMS,
                model.centroids(),
                2,
                &assignments,
            ) > 0.99
        );
    }

    #[test]
    fn completed_topic_versions_ignore_interruptions_and_retain_two_runs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let candidates = vec![ClusterCandidate {
            k: 2,
            silhouette: 0.8,
        }];
        let centroids = vec![0.0; DEFAULT_SEMANTIC_DIMS * 2];
        let assignments = vec![0, 0, 1];
        let distances = vec![0.1, 0.2, 0.3];

        let first = fixture_topic_dataset("input_1");
        start_cluster_run(&store, "version_1", &first).expect("start first run");
        persist_cluster_run(
            &store,
            "version_1",
            &first,
            &centroids,
            &assignments,
            &distances,
            &candidates,
            2,
            0.8,
        )
        .expect("complete first run");
        let interrupted = fixture_topic_dataset("input_interrupted");
        start_cluster_run(&store, "interrupted", &interrupted).expect("start interrupted run");
        assert_eq!(
            completed_cluster_for_input(&store, "fixture-topic-384", "input_1")
                .expect("read completed run")
                .expect("completed run")
                .version,
            "version_1"
        );

        for (version, input_hash) in [("version_2", "input_2"), ("version_3", "input_3")] {
            let dataset = fixture_topic_dataset(input_hash);
            start_cluster_run(&store, version, &dataset).expect("start retained run");
            persist_cluster_run(
                &store,
                version,
                &dataset,
                &centroids,
                &assignments,
                &distances,
                &candidates,
                2,
                0.8,
            )
            .expect("complete retained run");
            prune_topic_versions(&store).expect("prune topic versions");
        }

        let counts = store
            .with_conn(|conn| {
                Ok((
                    conn.query_row("SELECT COUNT(*) FROM topic_runs", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    conn.query_row("SELECT COUNT(*) FROM topics", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    conn.query_row("SELECT COUNT(*) FROM topic_assignments", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    conn.query_row(
                        "SELECT COUNT(*) FROM topic_runs WHERE version IN ('version_1', 'interrupted')",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?,
                ))
            })
            .expect("count retained topic versions");
        assert_eq!(counts, (2, 4, 6, 0));
    }

    fn fixture_topic_dataset(input_hash: &str) -> TopicDataset {
        TopicDataset {
            model_id: "fixture-topic-384".to_string(),
            input_hash: input_hash.to_string(),
            item_ids: vec![
                "item_1".to_string(),
                "item_2".to_string(),
                "item_3".to_string(),
            ],
            points: Vec::new(),
        }
    }

    struct MockLabeler {
        calls: AtomicUsize,
    }

    impl JsonLlm for MockLabeler {
        fn model(&self) -> &str {
            "mock-labeler"
        }

        fn complete_json(&self, _system: &str, _prompt: &str) -> Result<String> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(format!("{{\"label\":\"Topic {}\"}}", call + 1))
        }
    }

    #[test]
    fn topic_labeling_resumes_by_labeler_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    r#"
                    INSERT INTO topic_runs
                      (version, algorithm_version, model_id, input_hash, item_count, selected_k,
                       silhouette_score, status, started_at, completed_at)
                    VALUES
                      ('current', 1, 'fixture-topic-384', 'input', 2, 2, 0.8, 'completed',
                       '2026-07-12T00:00:00Z', '2026-07-12T00:01:00Z');

                    INSERT INTO history_items
                      (id, event_id, session_id, source_id, machine_id, source_kind, ordinal,
                       subordinal, tier, kind, text, text_hash, lexical_indexable,
                       semantic_policy, metadata_json, hash)
                    VALUES
                      ('item_a', 'event_a', 'session', 'source', 'machine', 'codex', 0, 0,
                       'conversation', 'user', 'database migration schema', 'hash_a',
                       1, 'required', '{}', 'item_hash_a'),
                      ('item_b', 'event_b', 'session', 'source', 'machine', 'codex', 1, 0,
                       'conversation', 'user', 'browser layout css', 'hash_b',
                       1, 'required', '{}', 'item_hash_b');

                    INSERT INTO message_provenance
                      (item_id, session_id, source_kind, authored_by, sentiment_usable, rule)
                    VALUES
                      ('item_a', 'session', 'codex', 'human', 'yes', 'default.human'),
                      ('item_b', 'session', 'codex', 'human', 'yes', 'default.human');

                    INSERT INTO topic_assignments (version, item_id, topic_id, distance)
                    VALUES ('current', 'item_a', 0, 0.1), ('current', 'item_b', 1, 0.1);
                    "#,
                )?;
                for topic_id in 0..2 {
                    conn.execute(
                        "INSERT INTO topics (version, topic_id, size, centroid)
                         VALUES ('current', ?1, 1, ?2)",
                        params![topic_id, crate::storage::f32_vector_to_blob(&[0.0; 384])],
                    )?;
                }
                Ok(())
            })
            .expect("insert label fixtures");
        let labeler = MockLabeler {
            calls: AtomicUsize::new(0),
        };

        let first = label_topics(&store, &labeler, "labels-v1", Some(1)).expect("label one");
        assert_eq!((first.labeled, first.skipped, first.pending), (1, 0, 1));
        let second = label_topics(&store, &labeler, "labels-v1", None).expect("resume labels");
        assert_eq!((second.labeled, second.skipped, second.pending), (1, 1, 0));
        let third = label_topics(&store, &labeler, "labels-v1", None).expect("skip labels");
        assert_eq!((third.labeled, third.skipped, third.pending), (0, 2, 0));
        assert_eq!(labeler.calls.load(Ordering::SeqCst), 2);

        let stored = store
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT label, label_model, labeler_version FROM topics ORDER BY topic_id",
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
            .expect("read stored labels");
        assert_eq!(stored[0].1, "mock-labeler");
        assert!(stored.iter().all(|row| row.2 == "labels-v1"));
    }

    #[cfg(not(feature = "semantic-fastembed"))]
    #[test]
    fn feature_disabled_build_returns_a_clear_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = match load_embedder(dir.path()) {
            Ok(_) => panic!("feature should be required"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("semantic-fastembed"));
    }

    #[cfg(not(feature = "analytics-topics"))]
    #[test]
    fn clustering_feature_disabled_build_returns_a_clear_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let options = ClusterOptions {
            min_k: 2,
            max_k: 2,
            step: 1,
            sample_size: 3,
            rebuild: false,
        };
        let error = cluster(&store, &options, |_| {}).expect_err("feature should be required");
        assert!(error.to_string().contains("analytics-topics"));
    }
}
