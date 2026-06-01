use crate::archive::{ArchiveEnvelope, ARCHIVE_SCHEMA};
use crate::storage::{ImportStats, Store};
use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

pub fn export_jsonl(store: &Store, mut writer: impl Write) -> Result<usize> {
    let records = store.export_records()?;
    let count = records.len();
    for record in records {
        let envelope = ArchiveEnvelope::new(record);
        serde_json::to_writer(&mut writer, &envelope)?;
        writer.write_all(b"\n")?;
    }
    Ok(count)
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
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{
        stable_hash, stable_id, ArchiveRecord, EmbeddingRecord, SearchUnitRecord,
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
}
