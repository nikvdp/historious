use assert_cmd::Command;
use base64::Engine;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn raw_blobs_compact_previews_and_applies_append_snapshot_cleanup() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");
    let archive_path = temp.path().join("archive.jsonl");
    let blobs_path = temp.path().join("blobs.jsonl");
    let raw_path = temp.path().join("session.jsonl");
    let old_bytes = b"{\"message\":\"first\"}\n";
    let new_bytes = b"{\"message\":\"first\"}\n{\"message\":\"second\"}\n";
    let old_hash = blake3_hex(old_bytes);
    let new_hash = blake3_hex(new_bytes);

    fs::write(
        &archive_path,
        archive_jsonl(
            &raw_path.to_string_lossy(),
            old_bytes,
            &old_hash,
            new_bytes,
            &new_hash,
        ),
    )
    .expect("write archive");
    fs::write(
        &blobs_path,
        format!(
            "{}\n{}\n",
            raw_blob_jsonl(&old_hash, old_bytes),
            raw_blob_jsonl(&new_hash, new_bytes)
        ),
    )
    .expect("write raw blobs");

    histo()
        .args([
            "--data-dir",
            data_dir.to_str().expect("data dir"),
            "--robot",
            "import",
            "--jsonl",
            archive_path.to_str().expect("archive path"),
        ])
        .assert()
        .success();
    histo()
        .args([
            "--data-dir",
            data_dir.to_str().expect("data dir"),
            "--robot",
            "raw-blobs",
            "import",
            blobs_path.to_str().expect("blob path"),
        ])
        .assert()
        .success();

    let preview = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "compact",
        "--dry-run",
    ]));
    assert_eq!(preview["data"]["dry_run"], true);
    assert_eq!(preview["data"]["compaction"]["raw_artifacts_compacted"], 1);
    assert_eq!(preview["data"]["compaction"]["events_repointed"], 1);
    assert_eq!(preview["data"]["compaction"]["raw_blobs_deleted"], 0);

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "compact",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["dry_run"], false);
    assert_eq!(applied["data"]["confirmed"], true);
    assert_eq!(applied["data"]["compaction"]["raw_artifacts_compacted"], 1);
    assert_eq!(applied["data"]["compaction"]["events_repointed"], 1);
    assert_eq!(applied["data"]["compaction"]["raw_blobs_deleted"], 1);

    let missing = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "missing",
    ]));
    assert_eq!(missing["data"]["count"], 0);

    let status = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));
    assert_eq!(status["data"]["stats"]["raw_artifacts"], 1);
    assert_eq!(status["data"]["stats"]["events"], 1);
}

#[test]
fn raw_blobs_migrate_objects_moves_loose_objects_into_sqlite() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");
    let object_bytes = br#"{"event":"old loose object"}"#;
    let object_hash = blake3_hex(object_bytes);

    command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));

    seed_empty_raw_object(&data_dir, &object_hash, object_bytes);
    write_loose_blob(&data_dir, &object_hash, object_bytes);

    let preview = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "migrate-objects",
        "--dry-run",
    ]));
    assert_eq!(preview["data"]["dry_run"], true);
    assert_eq!(preview["data"]["migration"]["raw_objects_inspected"], 1);
    assert_eq!(preview["data"]["migration"]["raw_objects_migrated"], 1);
    assert_eq!(preview["data"]["migration"]["raw_blobs_deleted"], 0);
    assert!(loose_blob_path(&data_dir, &object_hash).exists());
    assert_eq!(raw_object_content_len(&data_dir, &object_hash), 0);

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "migrate-objects",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["dry_run"], false);
    assert_eq!(applied["data"]["confirmed"], true);
    assert_eq!(applied["data"]["migration"]["raw_objects_migrated"], 1);
    assert_eq!(applied["data"]["migration"]["raw_blobs_deleted"], 1);
    assert_eq!(
        applied["data"]["migration"]["raw_blob_bytes_deleted"],
        object_bytes.len()
    );
    assert!(!loose_blob_path(&data_dir, &object_hash).exists());
    assert_eq!(
        raw_object_content_len(&data_dir, &object_hash),
        object_bytes.len()
    );
}

#[test]
fn raw_blobs_migrate_objects_skips_invalid_loose_objects() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");
    let object_bytes = br#"{"event":"valid object"}"#;
    let invalid_bytes = br#"{"event":"tampered object"}"#;
    let object_hash = blake3_hex(object_bytes);

    command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));

    seed_empty_raw_object(&data_dir, &object_hash, object_bytes);
    write_loose_blob(&data_dir, &object_hash, invalid_bytes);

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "migrate-objects",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["migration"]["raw_objects_migrated"], 0);
    assert_eq!(
        applied["data"]["migration"]["raw_objects_skipped_invalid_blob"],
        1
    );
    assert_eq!(applied["data"]["migration"]["raw_blobs_deleted"], 0);
    assert!(loose_blob_path(&data_dir, &object_hash).exists());
    assert_eq!(raw_object_content_len(&data_dir, &object_hash), 0);
}

#[test]
fn raw_blobs_clean_manifest_artifacts_deletes_verified_legacy_artifact() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");
    let raw_path = temp.path().join("manifest-session.jsonl");
    let first = b"{\"event\":\"first\"}\n";
    let second = b"{\"event\":\"second\"}\n";
    let full = [first.as_slice(), second.as_slice()].concat();
    let raw_hash = blake3_hex(&full);

    command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));
    seed_manifest_covered_raw_artifact(
        &data_dir,
        &raw_path.to_string_lossy(),
        &full,
        &[first, second],
    );

    let preview = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "clean-manifest-artifacts",
        "--dry-run",
    ]));
    assert_eq!(preview["data"]["cleanup"]["raw_artifacts_inspected"], 1);
    assert_eq!(preview["data"]["cleanup"]["raw_artifacts_verified"], 1);
    assert_eq!(preview["data"]["cleanup"]["raw_artifacts_deleted"], 0);
    assert_eq!(raw_artifact_count(&data_dir, &raw_hash), 1);
    assert!(loose_blob_path(&data_dir, &raw_hash).exists());

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "clean-manifest-artifacts",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["cleanup"]["raw_artifacts_verified"], 1);
    assert_eq!(applied["data"]["cleanup"]["raw_artifacts_deleted"], 1);
    assert_eq!(applied["data"]["cleanup"]["raw_blobs_deleted"], 1);
    assert_eq!(raw_artifact_count(&data_dir, &raw_hash), 0);
    assert!(!loose_blob_path(&data_dir, &raw_hash).exists());
}

#[test]
fn raw_blobs_clean_manifest_artifacts_keeps_mismatched_legacy_artifact() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");
    let raw_path = temp.path().join("manifest-mismatch.jsonl");
    let manifest_line = b"{\"event\":\"manifest\"}\n";
    let legacy_bytes = b"{\"event\":\"legacy\"}\n";
    let raw_hash = blake3_hex(legacy_bytes);

    command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));
    seed_manifest_covered_raw_artifact_with_legacy_bytes(
        &data_dir,
        &raw_path.to_string_lossy(),
        &[manifest_line],
        legacy_bytes,
    );

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "clean-manifest-artifacts",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["cleanup"]["raw_artifacts_verified"], 0);
    assert_eq!(applied["data"]["cleanup"]["raw_artifacts_deleted"], 0);
    assert_eq!(
        applied["data"]["cleanup"]["raw_artifacts_skipped_mismatch"],
        1
    );
    assert_eq!(raw_artifact_count(&data_dir, &raw_hash), 1);
    assert!(loose_blob_path(&data_dir, &raw_hash).exists());
}

#[test]
fn raw_blobs_clean_source_archives_removes_legacy_archives_only() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");
    let raw_path = temp.path().join("session.jsonl");
    let first = br#"{"message":"first"}"#;
    let second = br#"{"message":"second"}"#;
    let full = [first.as_slice(), b"\n", second.as_slice(), b"\n"].concat();
    let raw_hash = blake3_hex(&full);

    command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));
    seed_manifest_covered_raw_artifact(
        &data_dir,
        &raw_path.to_string_lossy(),
        &full,
        &[first, second],
    );
    seed_normalized_event_with_raw_refs(&data_dir, &raw_path.to_string_lossy(), &raw_hash);

    let preview = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "clean-source-archives",
        "--dry-run",
    ]));
    assert_eq!(preview["data"]["dry_run"], true);
    assert_eq!(preview["data"]["cleanup"]["raw_artifacts_deleted"], 1);
    assert_eq!(preview["data"]["cleanup"]["raw_manifests_deleted"], 1);
    assert_eq!(
        preview["data"]["cleanup"]["raw_manifest_entries_deleted"],
        2
    );
    assert_eq!(preview["data"]["cleanup"]["raw_objects_deleted"], 2);
    assert_eq!(preview["data"]["cleanup"]["events_unlinked"], 1);
    assert_eq!(preview["data"]["cleanup"]["raw_blobs_deleted"], 1);
    assert_eq!(raw_artifact_count(&data_dir, &raw_hash), 1);
    assert!(loose_blob_path(&data_dir, &raw_hash).exists());

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "raw-blobs",
        "clean-source-archives",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["dry_run"], false);
    assert_eq!(applied["data"]["confirmed"], true);
    assert_eq!(applied["data"]["cleanup"]["raw_artifacts_deleted"], 1);
    assert_eq!(applied["data"]["cleanup"]["raw_blobs_deleted"], 1);
    assert_eq!(raw_archive_table_count(&data_dir, "raw_artifacts"), 0);
    assert_eq!(raw_archive_table_count(&data_dir, "raw_manifests"), 0);
    assert_eq!(
        raw_archive_table_count(&data_dir, "raw_manifest_entries"),
        0
    );
    assert_eq!(raw_archive_table_count(&data_dir, "raw_objects"), 0);
    assert!(!loose_blob_path(&data_dir, &raw_hash).exists());
    assert_normalized_event_survived_source_archive_cleanup(&data_dir);
}

#[test]
fn maintenance_compact_previews_and_runs_sqlite_maintenance() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("histo-data");

    command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "status",
    ]));

    let preview = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "maintenance",
        "compact",
        "--dry-run",
    ]));
    assert_eq!(preview["data"]["dry_run"], true);
    assert_eq!(preview["data"]["confirmed"], false);
    assert_eq!(preview["data"]["maintenance"]["fts_optimized"], false);
    assert_eq!(preview["data"]["maintenance"]["vacuumed"], false);
    assert!(
        preview["data"]["maintenance"]["database_bytes_before"]
            .as_u64()
            .expect("database bytes")
            > 0
    );

    let applied = command_json(histo().args([
        "--data-dir",
        data_dir.to_str().expect("data dir"),
        "--robot",
        "maintenance",
        "compact",
        "--confirm",
    ]));
    assert_eq!(applied["data"]["dry_run"], false);
    assert_eq!(applied["data"]["confirmed"], true);
    assert_eq!(applied["data"]["maintenance"]["fts_optimized"], true);
    assert_eq!(applied["data"]["maintenance"]["vacuumed"], true);
    assert!(
        applied["data"]["maintenance"]["database_bytes_after"]
            .as_u64()
            .expect("database bytes")
            > 0
    );
}

fn histo() -> Command {
    Command::cargo_bin("histo").expect("histo binary")
}

fn command_json(command: &mut Command) -> Value {
    let output = command.assert().success().get_output().stdout.clone();
    serde_json::from_slice(&output).expect("json output")
}

fn seed_empty_raw_object(data_dir: &Path, hash: &str, bytes: &[u8]) {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    conn.execute(
        "INSERT INTO raw_objects (hash, media_type, size, content, first_seen_at)
         VALUES (?1, 'application/jsonl-record', ?2, ?3, '2026-06-24T00:00:00Z')",
        params![hash, bytes.len() as i64, Vec::<u8>::new()],
    )
    .expect("insert raw object");
}

fn write_loose_blob(data_dir: &Path, hash: &str, bytes: &[u8]) {
    let path = loose_blob_path(data_dir, hash);
    fs::create_dir_all(path.parent().expect("blob parent")).expect("blob dir");
    fs::write(path, bytes).expect("write loose blob");
}

fn loose_blob_path(data_dir: &Path, hash: &str) -> PathBuf {
    let clean = hash.strip_prefix("blake3:").unwrap_or(hash);
    let shard = clean.get(0..2).unwrap_or("xx");
    data_dir.join("blobs").join(shard).join(clean)
}

fn raw_object_content_len(data_dir: &Path, hash: &str) -> usize {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    conn.query_row(
        "SELECT length(content) FROM raw_objects WHERE hash = ?1",
        params![hash],
        |row| row.get::<_, i64>(0),
    )
    .expect("content length") as usize
}

fn seed_manifest_covered_raw_artifact(
    data_dir: &Path,
    raw_path: &str,
    manifest_bytes: &[u8],
    entries: &[&[u8]],
) {
    seed_manifest_covered_raw_artifact_with_legacy_bytes(
        data_dir,
        raw_path,
        entries,
        manifest_bytes,
    );
}

fn seed_manifest_covered_raw_artifact_with_legacy_bytes(
    data_dir: &Path,
    raw_path: &str,
    entries: &[&[u8]],
    legacy_bytes: &[u8],
) {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    let raw_hash = blake3_hex(legacy_bytes);
    conn.execute(
        "INSERT INTO sources (id, kind, identity, path, first_seen_at, updated_at, hash)
         VALUES ('source_manifest_cleanup', 'codex', ?1, ?1, '2026-06-24T00:00:00Z', '2026-06-24T00:00:00Z', 'source_hash')",
        params![raw_path],
    )
    .expect("insert source");
    conn.execute(
        "INSERT INTO raw_artifacts
         (hash, source_id, path, size, mtime_ms, media_type, content, first_seen_at)
         VALUES (?1, 'source_manifest_cleanup', ?2, ?3, 1, 'application/jsonl', ?4, '2026-06-24T00:00:01Z')",
        params![raw_hash.as_str(), raw_path, legacy_bytes.len() as i64, Vec::<u8>::new()],
    )
    .expect("insert raw artifact");
    write_loose_blob(data_dir, &raw_hash, legacy_bytes);

    let manifest_hash = format!("manifest:{}", blake3_hex(raw_path.as_bytes()));
    let full_size = entries.iter().map(|entry| entry.len()).sum::<usize>() as i64;
    conn.execute(
        "INSERT INTO raw_manifests
         (hash, source_id, source_kind, source_identity, path, external_session_id, full_size,
          mtime_ms, media_type, entry_count, created_at, metadata_json)
         VALUES (?1, 'source_manifest_cleanup', 'codex', ?2, ?2, 'session_manifest_cleanup',
                 ?3, 1, 'application/jsonl', ?4, '2026-06-24T00:00:02Z', '{}')",
        params![
            manifest_hash.as_str(),
            raw_path,
            full_size,
            entries.len() as i64
        ],
    )
    .expect("insert manifest");

    let mut byte_offset = 0i64;
    for (idx, entry) in entries.iter().enumerate() {
        let object_hash = blake3_hex(entry);
        conn.execute(
            "INSERT INTO raw_objects (hash, media_type, size, content, first_seen_at)
             VALUES (?1, 'application/jsonl-record', ?2, ?3, '2026-06-24T00:00:02Z')",
            params![object_hash.as_str(), entry.len() as i64, entry],
        )
        .expect("insert raw object");
        conn.execute(
            "INSERT INTO raw_manifest_entries
             (manifest_hash, ordinal, object_hash, byte_offset, byte_len, raw_line_hash,
              parsed_event_hash, event_id, external_event_id, metadata_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?3, NULL, NULL, NULL, '{}')",
            params![
                manifest_hash.as_str(),
                idx as i64,
                object_hash.as_str(),
                byte_offset,
                entry.len() as i64
            ],
        )
        .expect("insert manifest entry");
        byte_offset += entry.len() as i64;
    }
}

fn raw_artifact_count(data_dir: &Path, hash: &str) -> usize {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM raw_artifacts WHERE hash = ?1",
        params![hash],
        |row| row.get::<_, i64>(0),
    )
    .expect("raw artifact count") as usize
}

fn raw_archive_table_count(data_dir: &Path, table: &str) -> usize {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    let sql = format!("SELECT COUNT(*) FROM {table}");
    conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
        .expect("raw archive table count") as usize
}

fn seed_normalized_event_with_raw_refs(data_dir: &Path, raw_path: &str, raw_hash: &str) {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    conn.execute(
        "INSERT INTO sessions
         (id, source_id, machine_id, source_kind, external_id, title, status,
          started_at, updated_at, metadata_json, hash)
         VALUES ('session_source_archive_cleanup', 'source_manifest_cleanup', 'machine_fixture',
                 'codex', 'session_source_archive_cleanup', 'cleanup fixture', 'open',
                 '2026-06-24T00:00:03Z', '2026-06-24T00:00:03Z', ?1, 'session_hash_cleanup')",
        params![json!({"path": raw_path, "workspace_path": "/tmp/histo-cleanup"}).to_string()],
    )
    .expect("insert session");
    conn.execute(
        "INSERT INTO events
         (id, session_id, source_id, machine_id, source_kind, ordinal, event_type, role,
          content, raw_artifact_hash, occurred_at, metadata_json, hash)
         VALUES ('event_source_archive_cleanup', 'session_source_archive_cleanup',
                 'source_manifest_cleanup', 'machine_fixture', 'codex', 0, 'message', 'user',
                 'normalized user request survives cleanup', ?1, '2026-06-24T00:00:03Z',
                 ?2, 'event_hash_cleanup')",
        params![
            raw_hash,
            json!({
                "raw_artifact_hash": raw_hash,
                "raw_manifest_hash": "manifest-cleanup",
                "raw_object_hash": "object-cleanup",
                "raw_line_hash": "line-cleanup",
                "byte_offset": 0,
                "byte_len": 1
            })
            .to_string()
        ],
    )
    .expect("insert event");
}

fn assert_normalized_event_survived_source_archive_cleanup(data_dir: &Path) {
    let conn = Connection::open(data_dir.join("historious.db")).expect("open db");
    let (content, raw_artifact_hash, raw_ref_count): (String, Option<String>, i64) = conn
        .query_row(
            "SELECT content,
                    raw_artifact_hash,
                    (CASE WHEN json_extract(metadata_json, '$.raw_artifact_hash') IS NULL THEN 0 ELSE 1 END) +
                    (CASE WHEN json_extract(metadata_json, '$.raw_manifest_hash') IS NULL THEN 0 ELSE 1 END) +
                    (CASE WHEN json_extract(metadata_json, '$.raw_object_hash') IS NULL THEN 0 ELSE 1 END) +
                    (CASE WHEN json_extract(metadata_json, '$.raw_line_hash') IS NULL THEN 0 ELSE 1 END)
             FROM events
             WHERE id = 'event_source_archive_cleanup'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("event after cleanup");
    assert_eq!(content, "normalized user request survives cleanup");
    assert!(raw_artifact_hash.is_none());
    assert_eq!(raw_ref_count, 0);
}

fn archive_jsonl(
    raw_path: &str,
    old_bytes: &[u8],
    old_hash: &str,
    new_bytes: &[u8],
    new_hash: &str,
) -> String {
    let source = json!({
        "id": "source_fixture",
        "kind": "codex",
        "identity": raw_path,
        "path": raw_path,
        "first_seen_at": "2026-06-19T00:00:00Z",
        "updated_at": "2026-06-19T00:00:00Z",
        "hash": "source_hash"
    });
    let old_raw = raw_artifact(raw_path, old_bytes, old_hash, "2026-06-19T00:00:01Z");
    let new_raw = raw_artifact(raw_path, new_bytes, new_hash, "2026-06-19T00:00:02Z");
    let session = json!({
        "id": "session_fixture",
        "source_id": "source_fixture",
        "machine_id": "machine_fixture",
        "source_kind": "codex",
        "external_id": "session_fixture",
        "title": "fixture session",
        "status": "open",
        "started_at": null,
        "updated_at": null,
        "metadata": {"path": raw_path},
        "hash": "session_hash"
    });
    let event = json!({
        "id": "event_fixture",
        "session_id": "session_fixture",
        "source_id": "source_fixture",
        "machine_id": "machine_fixture",
        "source_kind": "codex",
        "ordinal": 0,
        "event_type": "message",
        "role": "user",
        "content": "first",
        "raw_artifact_hash": old_hash,
        "occurred_at": null,
        "metadata": {
            "raw_artifact_hash": old_hash,
            "byte_offset": 0,
            "byte_len": old_bytes.len(),
            "capture_fidelity": "exact_local_log"
        },
        "hash": "event_hash"
    });

    [
        envelope("source", "source_fixture", "source_hash", source),
        envelope("raw_artifact", old_hash, old_hash, old_raw),
        envelope("raw_artifact", new_hash, new_hash, new_raw),
        envelope("session", "session_fixture", "session_hash", session),
        envelope("event", "event_fixture", "event_hash", event),
    ]
    .into_iter()
    .map(|value| value.to_string())
    .collect::<Vec<_>>()
    .join("\n")
        + "\n"
}

fn raw_artifact(raw_path: &str, bytes: &[u8], hash: &str, first_seen_at: &str) -> Value {
    json!({
        "hash": hash,
        "source_id": "source_fixture",
        "path": raw_path,
        "size": bytes.len(),
        "mtime_ms": 1,
        "media_type": "application/jsonl",
        "content": "",
        "first_seen_at": first_seen_at
    })
}

fn envelope(kind: &str, id: &str, hash: &str, payload: Value) -> Value {
    json!({
        "schema": "historious.archive.v1",
        "id": id,
        "hash": hash,
        "producer": "historious-test",
        "produced_at": "2026-06-19T00:00:00Z",
        "kind": kind,
        "payload": payload
    })
}

fn raw_blob_jsonl(hash: &str, bytes: &[u8]) -> String {
    json!({
        "hash": hash,
        "size": bytes.len(),
        "content": base64::engine::general_purpose::STANDARD.encode(bytes)
    })
    .to_string()
}

fn blake3_hex(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}
