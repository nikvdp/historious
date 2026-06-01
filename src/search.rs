use crate::embed::Embedder;
use crate::storage::{SearchRow, Store, VectorSearchRow};
use anyhow::Result;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

const RRF_K: f64 = 60.0;
const BACKEND_LIMIT_MULTIPLIER: usize = 50;
const BACKEND_MIN_LIMIT: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct SearchResponse {
    pub degraded_reason: Option<String>,
    pub results: Vec<SearchResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub event_id: String,
    pub session_id: String,
    pub source_kind: String,
    pub score: f64,
    pub lexical_rank: Option<usize>,
    pub semantic_rank: Option<usize>,
    pub snippet: String,
}

pub fn refresh(store: &Store) -> Result<usize> {
    store.refresh_search_projection(
        crate::embed::HashEmbedder::MODEL_ID,
        crate::embed::HashEmbedder::DIMS,
        crate::embed::hash_embed,
    )
}

pub fn search(
    store: &Store,
    query: &str,
    limit: usize,
    query_embedder: Option<&dyn Embedder>,
    degraded_reason: Option<String>,
) -> Result<SearchResponse> {
    let backend_limit = limit
        .saturating_mul(BACKEND_LIMIT_MULTIPLIER)
        .max(BACKEND_MIN_LIMIT);
    let lexical = store.search_fts(query, backend_limit)?;
    let (semantic, degraded_reason) =
        semantic_search(store, query, query_embedder, degraded_reason, backend_limit)?;
    Ok(SearchResponse {
        degraded_reason,
        results: fuse(lexical, semantic, limit),
    })
}

fn semantic_search(
    store: &Store,
    query: &str,
    query_embedder: Option<&dyn Embedder>,
    degraded_reason: Option<String>,
    limit: usize,
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
                "query embedder dimensions {} are not supported by local vector projection",
                embedder.dims()
            )),
        ));
    }
    let query_vector = embedder.embed_one(query)?;
    let rows = store
        .vector_search(embedder.model_id(), &query_vector, limit)?
        .into_iter()
        .map(search_row_from_vector)
        .collect();
    Ok((rows, degraded_reason))
}

fn fuse(lexical: Vec<SearchRow>, semantic: Vec<SearchRow>, limit: usize) -> Vec<SearchResult> {
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
        if !seen_lexical.insert(row.event_id.clone()) {
            continue;
        }
        let entry = acc.entry(row.event_id.clone()).or_default();
        entry.score += 1.0 / (RRF_K + row.rank as f64);
        entry.lexical_rank = Some(row.rank);
        entry.row = Some(row);
    }
    for row in semantic {
        let entry = acc.entry(row.event_id.clone()).or_default();
        entry.score += 1.0 / (RRF_K + row.rank as f64);
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
                event_id: row.event_id,
                session_id: row.session_id,
                source_kind: row.source_kind,
                score: entry.score,
                lexical_rank: entry.lexical_rank,
                semantic_rank: entry.semantic_rank,
                snippet: snippet(&row.content, 240),
            })
        })
        .collect::<Vec<_>>();
    results.sort_by(|left, right| {
        right.score.total_cmp(&left.score).then_with(|| {
            best_rank(left)
                .cmp(&best_rank(right))
                .then_with(|| left.event_id.cmp(&right.event_id))
        })
    });
    results.truncate(limit);
    results
}

fn search_row_from_vector(row: VectorSearchRow) -> SearchRow {
    let _unit_id = row.unit_id;
    let _distance = row.distance;
    SearchRow {
        event_id: row.event_id,
        session_id: row.session_id,
        source_kind: row.source_kind,
        content: row.content,
        rank: row.rank,
    }
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
    use crate::storage::{f32_vector_to_blob, Store};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn vector_only_result_can_win_without_fts_overlap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let unit = import_event_and_project(&store, "exact lexical marker");
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
        let response =
            search(&store, "conceptual neighbor", 5, Some(&embedder), None).expect("search");

        assert_eq!(response.degraded_reason, None);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].event_id, unit.event_id);
        assert_eq!(response.results[0].lexical_rank, None);
        assert_eq!(response.results[0].semantic_rank, Some(1));
    }

    #[test]
    fn lexical_and_vector_results_are_fused_with_rrf() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let lexical_only = import_event_and_project(&store, "alpha token only");
        let hybrid = import_event_and_project(&store, "alpha token also semantic");
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
        let response = search(&store, "alpha", 5, Some(&embedder), None).expect("search");

        let first = response.results.first().expect("result");
        assert_eq!(first.event_id, hybrid.event_id);
        assert!(first.lexical_rank.is_some());
        assert_eq!(first.semantic_rank, Some(1));
        assert!(response.results.iter().any(
            |result| result.event_id == lexical_only.event_id && result.semantic_rank.is_none()
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
            5,
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

    fn import_event_and_project(store: &Store, search_text: &str) -> SearchUnitRecord {
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
            machine_id: "machine_fixture".to_string(),
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
            machine_id: "machine_fixture".to_string(),
            source_kind: "fixture".to_string(),
            ordinal: 0,
            event_type: "assistant".to_string(),
            role: Some("assistant".to_string()),
            content: search_text.to_string(),
            raw_artifact_hash: None,
            occurred_at: None,
            metadata: json!({
                "search_indexable": true,
                "search_kind": "assistant",
                "search_text": search_text
            }),
            hash: event_hash,
        };
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(event),
            ])
            .expect("import fixture event");
        refresh(store).expect("refresh projection");

        let text_hash = crate::archive::blake3_hex(search_text.as_bytes());
        let unit_id = stable_id(&["search_unit", &event_id, &text_hash]);
        SearchUnitRecord {
            id: unit_id,
            event_id,
            session_id: stable_id(&["session", search_text]),
            source_id: stable_id(&["source", search_text]),
            machine_id: "machine_fixture".to_string(),
            source_kind: "fixture".to_string(),
            role: Some("assistant".to_string()),
            search_kind: "assistant".to_string(),
            text: search_text.to_string(),
            text_hash,
            occurred_at: None,
            metadata: json!({}),
            hash: "unused".to_string(),
        }
    }

    fn fixture_embedding(unit: &SearchUnitRecord, vector: Vec<f32>) -> EmbeddingRecord {
        let vector_blob = f32_vector_to_blob(&vector);
        let vector_hash = crate::archive::blake3_hex(&vector_blob);
        let id = stable_id(&[
            "embedding",
            &unit.id,
            &unit.text_hash,
            "fixture-semantic-384",
        ]);
        EmbeddingRecord {
            id: id.clone(),
            unit_id: unit.id.clone(),
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

        fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| self.vector.clone()).collect())
        }
    }

    fn unit_vector(index: usize) -> Vec<f32> {
        let mut vector = vec![0.0; 384];
        vector[index] = 1.0;
        vector
    }
}
