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
            bail!("unsupported archive schema on line {}: {}", idx + 1, envelope.schema);
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
