use assert_cmd::Command;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Output};

const MACHINE: &str = "00000000-0000-4000-8000-000000000001";

struct Fixture {
    _temp: tempfile::TempDir,
    repo: PathBuf,
    file: PathBuf,
    data: PathBuf,
    first_sha: String,
    second_sha: String,
}

fn git(repo: &Path, args: &[&str]) -> Output {
    let output = ProcessCommand::new("git")
        .current_dir(repo)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", repo.join("absent-global-config"))
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_EDITOR", "true")
        .env("GIT_SEQUENCE_EDITOR", "true")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn head(repo: &Path) -> String {
    String::from_utf8(git(repo, &["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_string()
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn envelope(kind: &str, mut payload: Value) -> String {
    let id = payload["id"].as_str().unwrap().to_string();
    payload["hash"] = json!(format!("hash-{id}"));
    json!({
        "schema":"historious.archive.v2", "id":id, "hash":payload["hash"],
        "producer":"synthetic-cli-test", "produced_at":"2026-01-01T00:00:00Z",
        "kind":kind, "payload":payload
    })
    .to_string()
}

fn call(id: &str, name: &str, arguments: Value) -> Value {
    json!({"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":id,"name":name,"arguments":arguments}]}})
}

fn result(id: &str, text: &str) -> Value {
    json!({"type":"message","message":{"role":"toolResult","toolCallId":id,"isError":false,"content":[{"type":"text","text":text}]}})
}

fn session_archive(id: &str, repo: &Path, values: Vec<Value>) -> Vec<String> {
    let source = format!("source-{id}");
    let mut rows = vec![
        envelope(
            "source",
            json!({"id":source,"kind":"omp","identity":source,"path":null,"first_seen_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}),
        ),
        envelope(
            "session",
            json!({"id":id,"source_id":source,"machine_id":MACHINE,"source_kind":"omp","external_id":id,"title":id,"status":"closed","started_at":null,"updated_at":null,"metadata":{"cwd":repo}}),
        ),
    ];
    for (ordinal, value) in values.into_iter().enumerate() {
        rows.push(envelope(
            "event",
            json!({
                "id":format!("{id}-event-{ordinal}"),"session_id":id,"source_id":source,
                "machine_id":MACHINE,"source_kind":"omp","ordinal":ordinal,"event_type":"message",
                "role":null,"content":value.to_string(),"raw_artifact_hash":null,
                "occurred_at":"2026-01-01T00:00:00Z","metadata":{}
            }),
        ));
    }
    rows
}

fn histo(data: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_histo"));
    command.arg("--data-dir").arg(data);
    command
}

fn json_output(command: &mut Command) -> Value {
    let output = command.assert().success().get_output().stdout.clone();
    serde_json::from_slice(&output).expect("one valid JSON envelope")
}

fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repository");
    fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    fs::write(repo.join("base.txt"), "base\n").unwrap();
    git(&repo, &["add", "--", "base.txt"]);
    git(&repo, &["commit", "-q", "-m", "Create synthetic base"]);
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    let file = repo.join("source file.txt");
    fs::write(&file, "first\nsecond\n").unwrap();
    let message = "Preserve source context\n\nKeep exact message-file evidence.\n";
    let message_file = temp.path().join("message's file.txt");
    fs::write(&message_file, message).unwrap();
    git(&repo, &["add", "--", "source file.txt"]);
    let first = git(&repo, &["commit", "-F", message_file.to_str().unwrap()]);
    let first_sha = head(&repo);
    let mut rows = session_archive(
        "session-first",
        &repo,
        vec![
            call(
                "message",
                "write",
                json!({"path":message_file,"content":message}),
            ),
            result("message", "Wrote message file"),
            call(
                "commit",
                "bash",
                json!({"command":format!("git commit -F {}", quote(message_file.to_str().unwrap())),"cwd":repo}),
            ),
            result("commit", &String::from_utf8(first.stdout).unwrap()),
        ],
    );
    fs::write(&file, "first\nsecond\nthird\n").unwrap();
    git(&repo, &["add", "--", "source file.txt"]);
    let second = git(&repo, &["commit", "-m", "Add the final line"]);
    let second_sha = head(&repo);
    rows.extend(session_archive(
        "session-second",
        &repo,
        vec![
            call(
                "commit",
                "bash",
                json!({"command":"git commit -m 'Add the final line'","cwd":repo}),
            ),
            result("commit", &String::from_utf8(second.stdout).unwrap()),
        ],
    ));
    let data = temp.path().join("history data");
    fs::create_dir(&data).unwrap();
    fs::write(
        data.join("config.toml"),
        format!("[machine]\nid = \"{MACHINE}\"\nname = \"Fixture\"\n"),
    )
    .unwrap();
    histo(&data)
        .args(["--robot", "import", "--jsonl", "-"])
        .write_stdin(rows.join("\n") + "\n")
        .assert()
        .success();
    Fixture {
        _temp: temp,
        repo,
        file,
        data,
        first_sha,
        second_sha,
    }
}

#[test]
fn blame_cli_file_range_json_parity_and_exact_citations_are_read_only() {
    let f = fixture();
    let db = Connection::open(f.data.join("historious.db")).unwrap();
    let before: (String, String) = db.query_row("SELECT input_high_watermark, updated_at FROM projection_status WHERE projection_name='commit_evidence_v1'", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    let config = fs::read(f.data.join("config.toml")).unwrap();
    let full = json_output(histo(&f.data).arg("--robot").arg("blame").arg(&f.file));
    assert_eq!(full["command"], "blame");
    assert_eq!(full["data"]["coverage"]["exact_sha_lines"], 3);
    assert_eq!(full["data"]["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(full["data"]["sessions"][0]["session_id"], "session-first");
    let normal_json = json_output(histo(&f.data).arg("blame").arg(&f.file).arg("--json"));
    assert_eq!(full["data"], normal_json["data"]);
    let ranged = json_output(
        histo(&f.data)
            .arg("--robot")
            .arg("blame")
            .arg(&f.file)
            .args(["--lines", "2:3"]),
    );
    assert_eq!(ranged["data"]["coverage"]["selected_lines"], 2);
    assert_eq!(ranged["data"]["coverage"]["exact_sha_lines"], 2);
    for session in full["data"]["sessions"].as_array().unwrap() {
        for citation in session["commits"][0]["citations"].as_array().unwrap() {
            for key in [
                "event_id",
                "result_event_id",
                "message_event_id",
                "message_result_event_id",
            ] {
                if let Some(id) = citation[key].as_str() {
                    let shown = json_output(histo(&f.data).args([
                        "--robot", "show", id, "--full", "--before", "0", "--after", "0",
                    ]));
                    assert_eq!(shown["data"]["target"]["event_id"], id);
                }
            }
        }
    }
    let first_citation = &full["data"]["sessions"][0]["commits"][0]["citations"][0];
    assert_eq!(first_citation["message_event_id"], "session-first-event-0");
    #[cfg(unix)]
    for hint in full["data"]["next_commands"].as_array().unwrap() {
        let hint = hint.as_str().unwrap();
        assert!(hint.contains("--data-dir"));
        let command = hint.replacen("histo", &quote(env!("CARGO_BIN_EXE_histo")), 1);
        let output = ProcessCommand::new("sh")
            .args(["-c", &command])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap()["success"],
            true
        );
    }
    let human = histo(&f.data)
        .arg("blame")
        .arg(&f.file)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let human = String::from_utf8(human).unwrap();
    assert!(human.contains("session-first") && human.contains("session-second"));
    assert!(human.contains(&f.first_sha[..12]) && human.contains(&f.second_sha[..12]));
    let after: (String, String) = db.query_row("SELECT input_high_watermark, updated_at FROM projection_status WHERE projection_name='commit_evidence_v1'", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(before, after);
    assert_eq!(config, fs::read(f.data.join("config.toml")).unwrap());
    assert!(git(&f.repo, &["status", "--porcelain"]).stdout.is_empty());
}

#[test]
fn blame_cli_rebase_fallback_and_stale_snapshot_remain_visible() {
    let f = fixture();
    git(&f.repo, &["checkout", "-q", "main"]);
    fs::write(f.repo.join("new-base.txt"), "new base\n").unwrap();
    git(&f.repo, &["add", "--", "new-base.txt"]);
    git(&f.repo, &["commit", "-q", "-m", "Extend the base"]);
    git(&f.repo, &["checkout", "-q", "feature"]);
    git(&f.repo, &["rebase", "-q", "main"]);
    assert_ne!(head(&f.repo), f.second_sha);
    let report = json_output(histo(&f.data).arg("--robot").arg("blame").arg(&f.file));
    assert_eq!(report["data"]["coverage"]["exact_sha_lines"], 0);
    assert_eq!(report["data"]["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(report["data"]["sessions"][0]["session_id"], "session-first");
    let db = Connection::open(f.data.join("historious.db")).unwrap();
    db.execute(
        "UPDATE events SET metadata_json=? WHERE session_id='session-first' AND ordinal=0",
        [json!({"dirty":true}).to_string()],
    )
    .unwrap();
    let stale = json_output(histo(&f.data).arg("--robot").arg("blame").arg(&f.file));
    assert_eq!(stale["data"]["projection"]["state"], "stale");
    assert_eq!(stale["data"]["sessions"], report["data"]["sessions"]);
    assert!(!stale["data"]["warnings"].as_array().unwrap().is_empty());
}

#[test]
fn blame_cli_invalid_ranges_and_missing_archive_do_not_initialize_storage() {
    let temp = tempfile::tempdir().unwrap();
    let absent = temp.path().join("absent");
    let missing = histo(&absent)
        .args(["--robot", "blame", "file.txt"])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let missing: Value = serde_json::from_slice(&missing).unwrap();
    assert_eq!(missing["command"], "blame");
    assert!(missing["error"]["message"]
        .as_str()
        .unwrap()
        .contains("histo update"));
    assert!(!absent.exists());
    let invalid = histo(&absent)
        .args(["--robot", "blame", "file.txt", "--lines", "0:2"])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let invalid: Value = serde_json::from_slice(&invalid).unwrap();
    assert_eq!(invalid["command"], "blame");
    assert!(invalid["error"]["message"]
        .as_str()
        .unwrap()
        .contains("--lines"));
    assert!(!absent.exists());
    histo(&absent)
        .args(["provenance", "file.txt"])
        .assert()
        .failure();
}

#[test]
fn blame_cli_empty_archive_and_empty_file_are_valid_results() {
    let f = fixture();
    let empty_data = f._temp.path().join("empty-history");
    histo(&empty_data)
        .args(["--robot", "import", "--jsonl", "-"])
        .write_stdin("")
        .assert()
        .success();
    let no_history = json_output(histo(&empty_data).arg("--robot").arg("blame").arg(&f.file));
    assert_eq!(no_history["data"]["projection"]["state"], "ready");
    assert_eq!(no_history["data"]["coverage"]["unresolved_lines"], 3);
    assert!(no_history["data"]["sessions"]
        .as_array()
        .unwrap()
        .is_empty());
    let empty_file = f.repo.join("empty.txt");
    fs::write(&empty_file, "").unwrap();
    git(&f.repo, &["add", "--", "empty.txt"]);
    git(&f.repo, &["commit", "-q", "-m", "Add empty file"]);
    let empty = json_output(histo(&f.data).arg("--robot").arg("blame").arg(&empty_file));
    assert_eq!(empty["data"]["coverage"]["selected_lines"], 0);
    assert!(empty["data"]["sessions"].as_array().unwrap().is_empty());
}
