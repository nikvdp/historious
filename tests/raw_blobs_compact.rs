use assert_cmd::Command;
use base64::Engine;
use serde_json::{json, Value};
use std::fs;

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

fn histo() -> Command {
    Command::cargo_bin("histo").expect("histo binary")
}

fn command_json(command: &mut Command) -> Value {
    let output = command.assert().success().get_output().stdout.clone();
    serde_json::from_slice(&output).expect("json output")
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
