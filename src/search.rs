use crate::storage::{SearchRow, Store};
use anyhow::Result;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

const RRF_K: f64 = 60.0;

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

pub fn search(store: &Store, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
    let candidate_limit = limit.saturating_mul(50).max(200);
    let lexical = store.search_fts(query, candidate_limit)?;
    let candidate_ids = lexical
        .iter()
        .map(|row| row.event_id.clone())
        .collect::<Vec<_>>();
    let semantic = semantic_search(store, query, &candidate_ids, candidate_limit)?;
    Ok(fuse(lexical, semantic, limit))
}

fn semantic_search(
    store: &Store,
    query: &str,
    candidate_ids: &[String],
    limit: usize,
) -> Result<Vec<SearchRow>> {
    if candidate_ids.is_empty() {
        return Ok(Vec::new());
    }
    let query_vector = crate::embed::hash_embed(query);
    let mut scored = store
        .embeddings_for_ids(crate::embed::HashEmbedder::MODEL_ID, candidate_ids)?
        .into_iter()
        .map(|(id, vector)| (id, dot(&query_vector, &vector)))
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.1.total_cmp(&left.1));
    let ids = scored
        .into_iter()
        .take(limit)
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    let mut rows = store.load_projected_search_rows(&ids)?;
    let rank_by_id = ids
        .iter()
        .enumerate()
        .map(|(idx, id)| (id.clone(), idx + 1))
        .collect::<HashMap<_, _>>();
    rows.sort_by_key(|row| rank_by_id.get(&row.event_id).copied().unwrap_or(usize::MAX));
    for row in &mut rows {
        row.rank = rank_by_id.get(&row.event_id).copied().unwrap_or(usize::MAX);
        row.content = snippet(&row.content, 240);
    }
    Ok(rows)
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
    results.sort_by(|left, right| right.score.total_cmp(&left.score));
    results.truncate(limit);
    results
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right.iter()).map(|(a, b)| a * b).sum()
}

fn snippet(input: &str, max_chars: usize) -> String {
    let normalized = input.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = normalized.chars().take(max_chars).collect::<String>();
    if normalized.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}
