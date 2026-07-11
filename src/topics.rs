use crate::archive::ArchiveRecord;
#[cfg(feature = "semantic-fastembed")]
use crate::embed::EmbedderConfig;
use crate::embed::{Embedder, DEFAULT_SEMANTIC_DIMS};
use crate::provenance;
use crate::storage::{HistoryItemEmbeddingCursor, HistoryItemForEmbedding, Store};
use anyhow::{bail, Result};
use rusqlite::params;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 64;

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

#[cfg(test)]
mod tests {
    use super::*;
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
}
