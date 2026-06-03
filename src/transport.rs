use crate::archive::{ArchiveEnvelope, ArchiveRecord, ARCHIVE_SCHEMA};
use crate::storage::{ArchiveExportFilter, ImportStats, Store};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Component, Path, PathBuf};

pub fn export_jsonl(store: &Store, mut writer: impl Write) -> Result<usize> {
    let records = store.export_records()?;
    write_jsonl_records(records, &mut writer)
}

pub fn export_jsonl_with_options(
    store: &Store,
    options: ExportOptions,
    mut writer: impl Write,
) -> Result<usize> {
    let records = filter_export_records(store.export_records()?, options);
    write_jsonl_records(records, &mut writer)
}

#[allow(dead_code)]
pub fn export_jsonl_filtered(
    store: &Store,
    filter: &ArchiveExportFilter,
    mut writer: impl Write,
) -> Result<usize> {
    if filter.is_empty() {
        return export_jsonl(store, writer);
    }
    let session_ids = store.session_ids_for_export_filter(filter)?;
    let records = store.export_records_for_session_ids(&session_ids)?;
    write_jsonl_records(records, &mut writer)
}

pub fn export_jsonl_filtered_with_options(
    store: &Store,
    filter: &ArchiveExportFilter,
    options: ExportOptions,
    mut writer: impl Write,
) -> Result<usize> {
    if filter.is_empty() {
        return export_jsonl_with_options(store, options, writer);
    }
    let session_ids = store.session_ids_for_export_filter(filter)?;
    let records =
        filter_export_records(store.export_records_for_session_ids(&session_ids)?, options);
    write_jsonl_records(records, &mut writer)
}

#[derive(Debug, Clone, Copy)]
pub struct ExportOptions {
    pub include_embeddings: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            include_embeddings: true,
        }
    }
}

fn filter_export_records(
    records: Vec<ArchiveRecord>,
    options: ExportOptions,
) -> Vec<ArchiveRecord> {
    if options.include_embeddings {
        return records;
    }
    records
        .into_iter()
        .filter(|record| !matches!(record, ArchiveRecord::Embedding(_)))
        .collect()
}

fn write_jsonl_records(records: Vec<ArchiveRecord>, writer: &mut impl Write) -> Result<usize> {
    let count = records.len();
    for record in records {
        let envelope = ArchiveEnvelope::new(record);
        serde_json::to_writer(&mut *writer, &envelope)?;
        writer.write_all(b"\n")?;
    }
    Ok(count)
}

pub fn normalize_workspace_arg(path: &Path) -> String {
    if let Ok(canonical) = path.canonicalize() {
        return canonical.to_string_lossy().to_string();
    }
    clean_path(path).to_string_lossy().to_string()
}

pub fn parse_since_arg(input: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    let Some(input) = input else {
        return Ok(None);
    };
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Ok(Some(dt.with_timezone(&Utc)));
    }
    let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .with_context(|| format!("parsing --since as RFC3339 or YYYY-MM-DD: {input}"))?;
    let at_midnight = date
        .and_hms_opt(0, 0, 0)
        .context("constructing midnight for --since date")?;
    Ok(Some(DateTime::from_naive_utc_and_offset(at_midnight, Utc)))
}

fn clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            _ => out.push(component.as_os_str()),
        }
    }
    out
}

pub fn import_jsonl_path(store: &Store, path: &str) -> Result<ImportStats> {
    if path == "-" {
        let stdin = io::stdin();
        import_jsonl_reader(store, stdin.lock())
    } else {
        let file = File::open(Path::new(path)).with_context(|| format!("opening {path}"))?;
        import_jsonl_reader(store, file)
    }
}

pub fn import_jsonl_reader(store: &Store, reader: impl io::Read) -> Result<ImportStats> {
    let mut stats = ImportStats::default();
    let reader = BufReader::new(reader);
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading JSONL line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let envelope: ArchiveEnvelope = serde_json::from_str(&line)
            .with_context(|| format!("parsing archive JSONL line {}", idx + 1))?;
        if envelope.schema != ARCHIVE_SCHEMA {
            bail!(
                "unsupported archive schema on line {}: {}",
                idx + 1,
                envelope.schema
            );
        }
        if envelope.id != envelope.record.id() || envelope.hash != envelope.record.hash() {
            bail!("envelope identity mismatch on line {}", idx + 1);
        }
        let delta = store.import_record(&envelope.record)?;
        stats.inserted += delta.inserted;
        stats.duplicates += delta.duplicates;
        stats.delta.merge(delta.delta);
    }
    stats.vectors_indexed =
        store.refresh_vector_projection_for_embeddings(&stats.delta.inserted_embeddings)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{
        stable_hash, stable_id, ArchiveRecord, EmbeddingRecord, EventRecord, RawArtifact,
        SearchUnitRecord, SessionRecord, SourceRecord,
    };
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn embedding_records_round_trip_through_jsonl() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit();
        let embedding = fixture_embedding(&unit);
        store
            .import_records(&[
                ArchiveRecord::SearchUnit(unit.clone()),
                ArchiveRecord::Embedding(embedding.clone()),
            ])
            .expect("import records");

        let mut body = Vec::new();
        export_jsonl(&store, &mut body).expect("export jsonl");

        let imported_dir = tempfile::tempdir().expect("import tempdir");
        let imported_store = Store::open(imported_dir.path()).expect("open imported store");
        let stats = import_jsonl_reader(&imported_store, body.as_slice()).expect("import jsonl");
        assert_eq!(stats.inserted, 2);
        assert_eq!(stats.duplicates, 0);

        let exported = imported_store.export_records().expect("export imported");
        assert!(exported.iter().any(|record| matches!(
            record,
            ArchiveRecord::SearchUnit(imported) if imported.id == unit.id
        )));
        assert!(exported.iter().any(|record| matches!(
            record,
            ArchiveRecord::Embedding(imported)
                if imported.id == embedding.id && imported.vector == embedding.vector
        )));
    }

    #[test]
    fn export_options_can_omit_embeddings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit();
        let embedding = fixture_embedding(&unit);
        store
            .import_records(&[
                ArchiveRecord::SearchUnit(unit.clone()),
                ArchiveRecord::Embedding(embedding),
            ])
            .expect("import records");

        let mut default_body = Vec::new();
        export_jsonl_with_options(&store, ExportOptions::default(), &mut default_body)
            .expect("default export");
        let mut lean_body = Vec::new();
        export_jsonl_with_options(
            &store,
            ExportOptions {
                include_embeddings: false,
            },
            &mut lean_body,
        )
        .expect("lean export");

        assert_jsonl_contains_kind(&default_body, "embedding");
        assert_jsonl_contains_kind(&default_body, "search_unit");
        assert!(!jsonl_contains_kind(&lean_body, "embedding"));
        assert_jsonl_contains_kind(&lean_body, "search_unit");
    }

    #[test]
    fn embedding_record_import_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit();
        let embedding = fixture_embedding(&unit);
        let records = [
            ArchiveRecord::SearchUnit(unit),
            ArchiveRecord::Embedding(embedding),
        ];

        let first = store.import_records(&records).expect("first import");
        let second = store.import_records(&records).expect("second import");

        assert_eq!(first.inserted, 2);
        assert_eq!(first.duplicates, 0);
        assert_eq!(second.inserted, 0);
        assert_eq!(second.duplicates, 2);
    }

    #[test]
    fn synced_embedding_import_rebuilds_vector_projection_and_degrades_without_query_embedder() {
        let machine_a_dir = tempfile::tempdir().expect("machine a tempdir");
        let machine_a = Store::open(machine_a_dir.path()).expect("open machine a");
        let unit = fixture_search_unit_384();
        machine_a
            .import_record(&ArchiveRecord::SearchUnit(unit.clone()))
            .expect("machine a unit");

        let mut a_export = Vec::new();
        export_jsonl(&machine_a, &mut a_export).expect("export a");

        let machine_b_dir = tempfile::tempdir().expect("machine b tempdir");
        let machine_b = Store::open(machine_b_dir.path()).expect("open machine b");
        import_jsonl_reader(&machine_b, a_export.as_slice()).expect("import a into b");
        machine_b
            .import_record(&ArchiveRecord::Embedding(fixture_embedding_384(&unit)))
            .expect("machine b embedding");

        let mut b_export = Vec::new();
        export_jsonl(&machine_b, &mut b_export).expect("export b");
        let sync_back = import_jsonl_reader(&machine_a, b_export.as_slice()).expect("sync back");

        assert_eq!(sync_back.vectors_indexed, 1);
        let vector_hits = machine_a
            .vector_search("fixture-semantic-384", &unit_vector_384(11), 5)
            .expect("vector search");
        assert_eq!(vector_hits.len(), 1);
        assert_eq!(vector_hits[0].unit_id, unit.id);

        let response = crate::search::search(
            &machine_a,
            "offline convergence",
            crate::search::SearchOptions::new(5, crate::search::SortMode::Relevance, 0.0),
            None,
            Some("query embedder disabled".to_string()),
        )
        .expect("degraded search");
        assert_eq!(
            response.degraded_reason.as_deref(),
            Some("query embedder disabled")
        );
    }

    #[test]
    fn filtered_export_round_trips_workspace_scope() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        store
            .import_records(&[
                ArchiveRecord::Source(fixture_source("source_selected")),
                ArchiveRecord::RawArtifact(fixture_raw("source_selected", "raw_selected")),
                ArchiveRecord::Session(fixture_session(
                    "session_selected",
                    "source_selected",
                    "/tmp/super-cass/repo",
                )),
                ArchiveRecord::Event(fixture_event(
                    "event_selected",
                    "session_selected",
                    "source_selected",
                    Some("raw_selected"),
                )),
                ArchiveRecord::Source(fixture_source("source_other")),
                ArchiveRecord::Session(fixture_session(
                    "session_other",
                    "source_other",
                    "/tmp/other",
                )),
            ])
            .expect("import records");

        let filter = ArchiveExportFilter {
            workspaces: vec!["/tmp/super-cass".to_string()],
            ..ArchiveExportFilter::default()
        };
        let mut body = Vec::new();
        export_jsonl_filtered(&store, &filter, &mut body).expect("filtered export");

        let imported_dir = tempfile::tempdir().expect("import tempdir");
        let imported = Store::open(imported_dir.path()).expect("open imported");
        let stats = import_jsonl_reader(&imported, body.as_slice()).expect("import filtered");
        assert_eq!(stats.inserted, 4);

        let records = imported.export_records().expect("export imported");
        assert!(records
            .iter()
            .any(|record| record.id() == "session_selected"));
        assert!(records.iter().any(|record| record.id() == "event_selected"));
        assert!(records.iter().any(|record| record.id() == "raw_selected"));
        assert!(!records.iter().any(|record| record.id() == "session_other"));
    }

    #[test]
    fn filtered_export_options_can_omit_embeddings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let unit = fixture_search_unit_384();
        let embedding = fixture_embedding_384(&unit);
        let session = fixture_session("session_sync", "source_sync", "/tmp/super-cass/repo");
        store
            .import_records(&[
                ArchiveRecord::Source(fixture_source("source_sync")),
                ArchiveRecord::Session(session),
                ArchiveRecord::SearchUnit(unit),
                ArchiveRecord::Embedding(embedding),
            ])
            .expect("import records");
        let filter = ArchiveExportFilter {
            workspaces: vec!["/tmp/super-cass/repo".to_string()],
            ..ArchiveExportFilter::default()
        };

        let mut body = Vec::new();
        export_jsonl_filtered_with_options(
            &store,
            &filter,
            ExportOptions {
                include_embeddings: false,
            },
            &mut body,
        )
        .expect("filtered lean export");

        assert_jsonl_contains_kind(&body, "search_unit");
        assert!(!jsonl_contains_kind(&body, "embedding"));
    }

    #[test]
    fn parse_since_accepts_rfc3339_and_date() {
        let rfc3339 = parse_since_arg(Some("2026-06-02T01:02:03Z"))
            .expect("parse rfc3339")
            .expect("rfc3339 value");
        let date = parse_since_arg(Some("2026-06-02"))
            .expect("parse date")
            .expect("date value");

        assert_eq!(rfc3339.to_rfc3339(), "2026-06-02T01:02:03+00:00");
        assert_eq!(date.to_rfc3339(), "2026-06-02T00:00:00+00:00");
    }

    fn fixture_search_unit() -> SearchUnitRecord {
        let id = stable_id(&["search_unit", "event_test", "hash_test"]);
        let text = "offline machines should converge after sync".to_string();
        let text_hash = crate::archive::blake3_hex(text.as_bytes());
        let hash = stable_hash(&(&id, "event_test", &text_hash, &text)).expect("unit hash");
        SearchUnitRecord {
            id,
            event_id: "event_test".to_string(),
            session_id: "session_test".to_string(),
            source_id: "source_test".to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            role: Some("user".to_string()),
            search_kind: "user".to_string(),
            text,
            text_hash,
            occurred_at: None,
            metadata: json!({"fixture": true}),
            hash,
        }
    }

    fn assert_jsonl_contains_kind(body: &[u8], kind: &str) {
        assert!(
            jsonl_contains_kind(body, kind),
            "expected JSONL to contain kind {kind}"
        );
    }

    fn jsonl_contains_kind(body: &[u8], kind: &str) -> bool {
        String::from_utf8_lossy(body)
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .any(|value| value.get("kind").and_then(|kind| kind.as_str()) == Some(kind))
    }

    fn fixture_embedding(unit: &SearchUnitRecord) -> EmbeddingRecord {
        let vector = vec![0, 0, 128, 63, 0, 0, 0, 0];
        let vector_hash = crate::archive::blake3_hex(&vector);
        let id = stable_id(&["embedding", &unit.id, &unit.text_hash, "fixture-model"]);
        let hash = stable_hash(&(
            &id,
            &unit.id,
            &unit.text_hash,
            "fixture-model",
            &vector_hash,
        ))
        .expect("embedding hash");
        EmbeddingRecord {
            id,
            unit_id: unit.id.clone(),
            text_hash: unit.text_hash.clone(),
            model_id: "fixture-model".to_string(),
            dims: 2,
            vector_hash,
            vector,
            producer_machine_id: "machine_b".to_string(),
            embedded_at: Utc::now(),
            metadata: json!({"fixture": true}),
            hash,
        }
    }

    fn fixture_search_unit_384() -> SearchUnitRecord {
        let id = stable_id(&["search_unit", "event_sync", "hash_sync"]);
        let text = "offline machines should converge after vector sync".to_string();
        let text_hash = crate::archive::blake3_hex(text.as_bytes());
        let hash = stable_hash(&(&id, "event_sync", &text_hash, &text)).expect("unit hash");
        SearchUnitRecord {
            id,
            event_id: "event_sync".to_string(),
            session_id: "session_sync".to_string(),
            source_id: "source_sync".to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            role: Some("user".to_string()),
            search_kind: "user".to_string(),
            text,
            text_hash,
            occurred_at: None,
            metadata: json!({"fixture": true}),
            hash,
        }
    }

    fn fixture_embedding_384(unit: &SearchUnitRecord) -> EmbeddingRecord {
        let vector = crate::storage::f32_vector_to_blob(&unit_vector_384(11));
        let vector_hash = crate::archive::blake3_hex(&vector);
        let id = stable_id(&[
            "embedding",
            &unit.id,
            &unit.text_hash,
            "fixture-semantic-384",
        ]);
        let hash = stable_hash(&(
            &id,
            &unit.id,
            &unit.text_hash,
            "fixture-semantic-384",
            &vector_hash,
        ))
        .expect("embedding hash");
        EmbeddingRecord {
            id,
            unit_id: unit.id.clone(),
            text_hash: unit.text_hash.clone(),
            model_id: "fixture-semantic-384".to_string(),
            dims: 384,
            vector_hash,
            vector,
            producer_machine_id: "machine_b".to_string(),
            embedded_at: Utc::now(),
            metadata: json!({"fixture": true}),
            hash,
        }
    }

    fn unit_vector_384(index: usize) -> Vec<f32> {
        let mut vector = vec![0.0; 384];
        vector[index] = 1.0;
        vector
    }

    fn fixture_source(id: &str) -> SourceRecord {
        SourceRecord {
            id: id.to_string(),
            kind: "codex".to_string(),
            identity: id.to_string(),
            path: Some(format!("/tmp/{id}.jsonl")),
            first_seen_at: Utc::now(),
            updated_at: Utc::now(),
            hash: stable_hash(&(id, "source")).expect("source hash"),
        }
    }

    fn fixture_raw(source_id: &str, hash: &str) -> RawArtifact {
        RawArtifact {
            hash: hash.to_string(),
            source_id: source_id.to_string(),
            path: format!("/tmp/{hash}.jsonl"),
            size: 7,
            mtime_ms: Some(1),
            media_type: "application/jsonl".to_string(),
            content: b"fixture".to_vec(),
            first_seen_at: Utc::now(),
        }
    }

    fn fixture_session(id: &str, source_id: &str, workspace_path: &str) -> SessionRecord {
        SessionRecord {
            id: id.to_string(),
            source_id: source_id.to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            external_id: id.to_string(),
            title: Some("fixture session".to_string()),
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({"workspace_path": workspace_path}),
            hash: stable_hash(&(id, source_id, "session")).expect("session hash"),
        }
    }

    fn fixture_event(
        id: &str,
        session_id: &str,
        source_id: &str,
        raw_artifact_hash: Option<&str>,
    ) -> EventRecord {
        EventRecord {
            id: id.to_string(),
            session_id: session_id.to_string(),
            source_id: source_id.to_string(),
            machine_id: "machine_a".to_string(),
            source_kind: "codex".to_string(),
            ordinal: 1,
            event_type: "message".to_string(),
            role: Some("user".to_string()),
            content: "fixture message".to_string(),
            raw_artifact_hash: raw_artifact_hash.map(ToOwned::to_owned),
            occurred_at: None,
            metadata: json!({"fixture": true}),
            hash: stable_hash(&(id, session_id, "event")).expect("event hash"),
        }
    }
}
