#!/usr/bin/env python3
"""Measure forced report refresh SQL behavior at N and 2N synthetic scale."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator


ROOT = Path(__file__).resolve().parents[1]
PHASES = (
    "session_relationships",
    "message_provenance",
    "session_facts",
    "message_model_context",
    "report_snapshot",
)
PHASE_MARKERS = (
    ("starting session relationships", "session_relationships"),
    ("starting message provenance", "message_provenance"),
    ("starting session facts", "session_facts"),
    ("starting message model context", "message_model_context"),
    ("starting report snapshot", "report_snapshot"),
)
PROFILE_ENV = "HISTO_REPORT_SQL_PROFILE"
SCHEMA = "historious.archive.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Existing empty directory outside the repository and Historious data directory",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=500,
        help="Session count N; the second run uses 2N (default: 500)",
    )
    return parser.parse_args()


def resolved(path: Path) -> Path:
    return path.expanduser().resolve()


def default_historious_dirs() -> set[Path]:
    home = Path.home().resolve()
    candidates = {
        home / ".local" / "share" / "historious",
        home / "Library" / "Application Support" / "com.historious.historious",
        home / "Library" / "Application Support" / "com.historious.super-cass",
    }
    configured = os.environ.get("HISTO_DATA_DIR")
    if configured:
        candidates.add(resolved(Path(configured)))
    return {resolved(path) for path in candidates}


def validate_output_dir(path: Path) -> Path:
    if not path.exists():
        raise ValueError(f"output directory does not exist: {path}")
    output = resolved(path)
    if not output.is_dir():
        raise ValueError(f"output path is not a directory: {output}")
    if any(output.iterdir()):
        raise ValueError(f"output directory is not empty: {output}")
    home = Path.home().resolve()
    if output == home:
        raise ValueError("refusing to use the home directory")
    root = ROOT.resolve()
    if output == root or root in output.parents:
        raise ValueError("refusing to use a repository directory")
    if output in default_historious_dirs():
        raise ValueError("refusing to use a configured Historious data directory")
    return output


def digest(*parts: object) -> str:
    value = "\0".join(str(part) for part in parts).encode()
    return "blake3:" + hashlib.blake2s(value).hexdigest()


def envelope(kind: str, payload: dict[str, Any], produced_at: str) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "id": payload["id"],
        "hash": payload["hash"],
        "producer": "historious/profile-harness",
        "produced_at": produced_at,
        "kind": kind,
        "payload": payload,
    }


def synthetic_records(session_count: int) -> Iterator[dict[str, Any]]:
    source_id = "synthetic-source"
    origin = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    produced_at = origin.isoformat().replace("+00:00", "Z")
    source = {
        "id": source_id,
        "kind": "claude_code",
        "identity": "synthetic-profile-input",
        "path": None,
        "first_seen_at": produced_at,
        "updated_at": produced_at,
        "hash": digest("source", source_id),
    }
    yield envelope("source", source, produced_at)

    for index in range(session_count):
        session_id = f"synthetic-session-{index:08d}"
        started = origin + dt.timedelta(hours=index * 3)
        ended = started + dt.timedelta(minutes=20 + index % 40)
        started_at = started.isoformat().replace("+00:00", "Z")
        ended_at = ended.isoformat().replace("+00:00", "Z")
        session = {
            "id": session_id,
            "source_id": source_id,
            "machine_id": "synthetic-machine",
            "source_kind": "claude_code",
            "external_id": f"profile-{index:08d}",
            "title": f"Synthetic session {index:08d}",
            "status": "complete",
            "started_at": started_at,
            "updated_at": ended_at,
            "metadata": {"workspace_path": f"/synthetic/project-{index % 8}"},
            "hash": digest("session", index),
        }
        yield envelope("session", session, produced_at)

        model = f"synthetic-model-{index % 6}"
        for ordinal in range(6):
            event_id = f"synthetic-event-{index:08d}-{ordinal}"
            role = "user" if ordinal % 2 == 0 else "assistant"
            occurred = started + dt.timedelta(minutes=ordinal * 3)
            metadata: dict[str, Any] = {}
            if role == "assistant":
                metadata = {
                    "message": {
                        "model": model,
                        "usage": {
                            "input_tokens": 80 + ordinal,
                            "cache_read_input_tokens": 20 + ordinal,
                            "output_tokens": 30 + ordinal,
                        },
                    },
                    "claude_relationship": {
                        "uuid": event_id,
                        "parent_uuid": f"synthetic-event-{index:08d}-{ordinal - 1}",
                        "is_sidechain": False,
                        "task_tool_use": ordinal == 3 and index % 5 == 0,
                    },
                }
            content = (
                f"Synthetic human request {index:08d}-{ordinal}"
                if role == "user"
                else json.dumps({"message": metadata["message"]}, separators=(",", ":"))
            )
            if role == "user" and ordinal == 4 and index % 11 == 0:
                content += " includes wtf signal"
            search_text = (
                content
                if role == "user"
                else f"Synthetic assistant response {index:08d}-{ordinal}"
            )
            metadata.update(
                {
                    "search_indexable": True,
                    "search_kind": role,
                    "search_text": search_text,
                }
            )
            event = {
                "id": event_id,
                "session_id": session_id,
                "source_id": source_id,
                "machine_id": "synthetic-machine",
                "source_kind": "claude_code",
                "ordinal": ordinal,
                "event_type": "message",
                "role": role,
                "content": content,
                "raw_artifact_hash": None,
                "occurred_at": occurred.isoformat().replace("+00:00", "Z"),
                "metadata": metadata,
                "hash": digest("event", index, ordinal),
            }
            yield envelope("event", event, produced_at)


def write_archive(path: Path, session_count: int) -> None:
    with path.open("w", encoding="utf-8") as stream:
        for record in synthetic_records(session_count):
            json.dump(record, stream, separators=(",", ":"), sort_keys=True)
            stream.write("\n")


def run_checked(args: list[str], env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(
        args,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with exit {completed.returncode}: {' '.join(args)}\n"
            f"{completed.stderr}"
        )


def run_report(
    binary: Path,
    store: Path,
    profile_path: Path | None,
) -> dict[str, Any]:
    env = os.environ.copy()
    env.pop(PROFILE_ENV, None)
    if profile_path is not None:
        env[PROFILE_ENV] = str(profile_path)
    command = [
        str(binary),
        "--data-dir",
        str(store),
        "report",
        "--update",
        "--plain",
    ]
    started = time.monotonic_ns()
    proc = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=1,
    )
    assert proc.stderr is not None
    phase_started: dict[str, int] = {}
    phase_finished: dict[str, int] = {}
    current: str | None = None
    stderr_lines: list[str] = []
    for line in proc.stderr:
        now = time.monotonic_ns()
        stderr_lines.append(line)
        for marker, phase in PHASE_MARKERS:
            if marker in line:
                if current is not None and current not in phase_finished:
                    phase_finished[current] = now
                current = phase
                phase_started.setdefault(phase, now)
                break
        if "report refreshed" in line and current is not None:
            phase_finished[current] = now
            current = None
    returncode = proc.wait()
    finished = time.monotonic_ns()
    if returncode != 0:
        raise RuntimeError(
            f"forced report failed with exit {returncode}:\n{''.join(stderr_lines)}"
        )
    if current is not None:
        phase_finished[current] = finished
    missing = [phase for phase in PHASES if phase not in phase_started or phase not in phase_finished]
    if missing:
        raise RuntimeError(f"forced report omitted phase transitions: {', '.join(missing)}")
    return {
        "total_duration_ns": finished - started,
        "phase_duration_ns": {
            phase: phase_finished[phase] - phase_started[phase] for phase in PHASES
        },
    }


def load_profile(path: Path) -> dict[str, Any]:
    phases: dict[str, dict[str, Any]] = {}
    statements: dict[tuple[str, str], dict[str, Any]] = {}
    status: dict[str, Any] | None = None
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            row_type = row.get("type")
            if row_type == "phase":
                phases[row["phase"]] = row
            elif row_type == "statement":
                statements[(row["phase"], row["fingerprint"])] = row
            elif row_type == "run":
                status = row
    if status is None or status.get("status") != "success":
        raise RuntimeError(f"profile did not finish successfully: {path}")
    missing = [phase for phase in PHASES if phase not in phases]
    if missing:
        raise RuntimeError(f"profile omitted phases: {', '.join(missing)}")
    for phase in PHASES:
        phase_rows = [row for (name, _), row in statements.items() if name == phase]
        if not phase_rows or not any(row["calls"] > 0 and row["vm_steps"] > 0 for row in phase_rows):
            raise RuntimeError(f"profile phase has no measured work: {phase}")
    return {"status": status, "phases": phases, "statements": statements}


def table_counts(store: Path) -> dict[str, int]:
    tables = (
        "sessions",
        "events",
        "session_relationships",
        "message_provenance",
        "session_facts",
        "message_model_context",
        "report_snapshot",
    )
    with sqlite3.connect(store / "historious.db") as conn:
        return {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tables}


def ratio(left: int | float, right: int | float) -> float | None:
    return None if left == 0 else right / left


def classify_candidate(
    n_row: dict[str, Any] | None,
    two_n_row: dict[str, Any],
    sessions_2n: int,
) -> tuple[str, str] | None:
    if n_row is None:
        return "shape_only_at_2n", "inconclusive"
    call_ratio = ratio(n_row["calls"], two_n_row["calls"]) or 0.0
    scan_ratio = ratio(n_row["full_scan_steps"], two_n_row["full_scan_steps"]) or 0.0
    vm_ratio = ratio(n_row["vm_steps"], two_n_row["vm_steps"]) or 0.0
    sql = two_n_row["sql"].lstrip().upper()
    repeated = two_n_row["calls"] >= sessions_2n and call_ratio >= 1.8
    if repeated and sql.startswith("SELECT") and " WHERE " in sql:
        return "repeated_point_query", "confirmed"
    if repeated:
        return "repeated_statement", "inconclusive"
    if two_n_row["full_scan_steps"] > 0:
        disposition = "inconclusive" if scan_ratio >= 1.7 else "ruled_out"
        return "scan_growth", disposition
    if vm_ratio >= 3.0:
        return "superlinear_vm_work", "inconclusive"
    if any(marker in sql for marker in ("CREATE TEMP", " ORDER BY ", " GROUP BY ")):
        return "temp_or_sort_shape", "ruled_out"
    return None


def rank_candidates(
    n_profile: dict[str, Any],
    two_n_profile: dict[str, Any],
    sessions_2n: int,
) -> list[dict[str, Any]]:
    n_rows = n_profile["statements"]
    rows = []
    for key, two_n_row in two_n_profile["statements"].items():
        n_row = n_rows.get(key)
        classification = classify_candidate(n_row, two_n_row, sessions_2n)
        if classification is None:
            continue
        candidate_type, disposition = classification
        score = (
            two_n_row["total_duration_ns"]
            + two_n_row["vm_steps"] * 100
            + two_n_row["full_scan_steps"] * 1_000
        )
        rows.append(
            {
                "rank_score": score,
                "phase": key[0],
                "fingerprint": key[1],
                "sql": two_n_row["sql"],
                "candidate_type": candidate_type,
                "disposition": disposition,
                "n": {
                    field: n_row[field] if n_row else None
                    for field in ("calls", "total_duration_ns", "max_duration_ns", "full_scan_steps", "vm_steps")
                },
                "2n": {
                    field: two_n_row[field]
                    for field in ("calls", "total_duration_ns", "max_duration_ns", "full_scan_steps", "vm_steps")
                },
                "ratios": {
                    field: ratio(n_row[field], two_n_row[field]) if n_row else None
                    for field in ("calls", "total_duration_ns", "full_scan_steps", "vm_steps")
                },
            }
        )
    rows.sort(key=lambda row: (-row["rank_score"], row["phase"], row["fingerprint"]))
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    return rows


def build_summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    by_scale = {run["sessions"]: run for run in runs}
    scales = sorted(by_scale)
    n_run, two_n_run = by_scale[scales[0]], by_scale[scales[1]]
    phase_scaling = {}
    for phase in PHASES:
        phase_scaling[phase] = {
            "baseline_n_to_2n": ratio(
                n_run["baseline"]["phase_duration_ns"][phase],
                two_n_run["baseline"]["phase_duration_ns"][phase],
            ),
            "instrumented_n_to_2n": ratio(
                n_run["instrumented"]["phase_duration_ns"][phase],
                two_n_run["instrumented"]["phase_duration_ns"][phase],
            ),
        }
    candidates = rank_candidates(n_run["profile"], two_n_run["profile"], scales[1])
    return {
        "schema": "historious.report_sql_scale.v1",
        "sessions": {"n": scales[0], "2n": scales[1]},
        "runs": [
            {
                "sessions": run["sessions"],
                "rows": run["rows"],
                "baseline": run["baseline"],
                "instrumented": run["instrumented"],
                "profiling_overhead_ratio": ratio(
                    run["baseline"]["total_duration_ns"],
                    run["instrumented"]["total_duration_ns"],
                ),
                "profile_status": run["profile"]["status"],
                "profile_phase_totals": run["profile"]["phases"],
            }
            for run in runs
        ],
        "scaling": {
            "baseline_total_n_to_2n": ratio(
                n_run["baseline"]["total_duration_ns"],
                two_n_run["baseline"]["total_duration_ns"],
            ),
            "instrumented_total_n_to_2n": ratio(
                n_run["instrumented"]["total_duration_ns"],
                two_n_run["instrumented"]["total_duration_ns"],
            ),
            "phases": phase_scaling,
        },
        "candidates": candidates,
        "candidate_counts": dict(
            sorted(
                {
                    disposition: sum(row["disposition"] == disposition for row in candidates)
                    for disposition in ("confirmed", "inconclusive", "ruled_out")
                }.items()
            )
        ),
    }


def milliseconds(nanoseconds: int | None) -> str:
    return "-" if nanoseconds is None else f"{nanoseconds / 1_000_000:.1f}"


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Forced report SQL scaling",
        "",
        "## Run timing",
        "",
        "| Sessions | Mode | Total ms | Relationships | Provenance | Facts | Model context | Snapshot |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for run in summary["runs"]:
        for mode in ("baseline", "instrumented"):
            timing = run[mode]
            phase = timing["phase_duration_ns"]
            lines.append(
                f"| {run['sessions']} | {mode} | {milliseconds(timing['total_duration_ns'])} "
                f"| {milliseconds(phase['session_relationships'])} "
                f"| {milliseconds(phase['message_provenance'])} "
                f"| {milliseconds(phase['session_facts'])} "
                f"| {milliseconds(phase['message_model_context'])} "
                f"| {milliseconds(phase['report_snapshot'])} |"
            )
    lines.extend(
        [
            "",
            "## Generated row counts",
            "",
            "| Sessions | Sessions | Events | Relationships | Provenance | Facts | Model context | Snapshot |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for run in summary["runs"]:
        rows = run["rows"]
        lines.append(
            f"| {run['sessions']} | {rows['sessions']} | {rows['events']} "
            f"| {rows['session_relationships']} | {rows['message_provenance']} "
            f"| {rows['session_facts']} | {rows['message_model_context']} "
            f"| {rows['report_snapshot']} |"
        )
    lines.extend(
        [
            "",
            "## Scaling and profiling overhead",
            "",
            "| Work | Baseline N→2N | Instrumented N→2N |",
            "|---|---:|---:|",
            f"| Total | {summary['scaling']['baseline_total_n_to_2n']:.2f}× "
            f"| {summary['scaling']['instrumented_total_n_to_2n']:.2f}× |",
        ]
    )
    for phase in PHASES:
        scaling = summary["scaling"]["phases"][phase]
        lines.append(
            f"| {phase} | {scaling['baseline_n_to_2n']:.2f}× "
            f"| {scaling['instrumented_n_to_2n']:.2f}× |"
        )
    lines.extend(
        [
            "",
            "| Sessions | Instrumented ÷ baseline |",
            "|---:|---:|",
        ]
    )
    for run in summary["runs"]:
        lines.append(f"| {run['sessions']} | {run['profiling_overhead_ratio']:.2f}× |")
    lines.extend(
        [
            "",
            "## Ranked SQL candidates",
            "",
            "| Rank | Phase | Fingerprint | Candidate | Disposition | Calls N→2N | Total ms N→2N | Max ms N→2N | VM N→2N | Scan N→2N | SQL shape |",
            "|---:|---|---|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in summary["candidates"]:
        sql = row["sql"].replace("|", "\\|").replace("\n", " ")
        if len(sql) > 180:
            sql = sql[:177] + "..."
        lines.append(
            f"| {row['rank']} | {row['phase']} | `{row['fingerprint']}` "
            f"| {row['candidate_type']} | {row['disposition']} "
            f"| {row['n']['calls']}→{row['2n']['calls']} "
            f"| {milliseconds(row['n']['total_duration_ns'])}→{milliseconds(row['2n']['total_duration_ns'])} "
            f"| {milliseconds(row['n']['max_duration_ns'])}→{milliseconds(row['2n']['max_duration_ns'])} "
            f"| {row['n']['vm_steps']}→{row['2n']['vm_steps']} "
            f"| {row['n']['full_scan_steps']}→{row['2n']['full_scan_steps']} "
            f"| `{sql}` |"
        )
    lines.extend(
        [
            "",
            "Dispositions are evidence-bounded: repeated per-session point reads are confirmed; "
            "scan, write, or VM growth without plan proof remains inconclusive; low-growth candidates are ruled out.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    if args.sessions < 2:
        raise ValueError("--sessions must be at least 2")
    output = validate_output_dir(args.output_dir)
    binary = ROOT / "target" / "release" / "histo"

    print("build 0/1: compiling release binary", flush=True)
    run_checked(["cargo", "build", "--release"])
    print("build 1/1: release binary ready", flush=True)

    runs: list[dict[str, Any]] = []
    for scale_index, session_count in enumerate((args.sessions, args.sessions * 2), 1):
        label = "n" if scale_index == 1 else "2n"
        scale_dir = output / label
        scale_dir.mkdir()
        archive = scale_dir / "input.jsonl"
        store = scale_dir / "store"
        profile = scale_dir / "profile.jsonl"

        print(f"dataset {scale_index}/2: generating {session_count} sessions", flush=True)
        write_archive(archive, session_count)
        import_env = os.environ.copy()
        import_env.pop(PROFILE_ENV, None)
        run_checked(
            [
                str(binary),
                "--data-dir",
                str(store),
                "import",
                "--jsonl",
                "--json",
                "--no-embeddings",
                str(archive),
            ],
            env=import_env,
        )

        print(f"baseline {scale_index}/2: rebuilding {session_count} sessions", flush=True)
        baseline = run_report(binary, store, None)
        if profile.exists():
            raise RuntimeError("profiling output appeared during the baseline run")

        print(f"profile {scale_index}/2: rebuilding {session_count} sessions", flush=True)
        instrumented = run_report(binary, store, profile)
        profile_data = load_profile(profile)
        rows = table_counts(store)
        for table, count in rows.items():
            if count <= 0:
                raise RuntimeError(f"{label} projection table is empty: {table}")
        runs.append(
            {
                "sessions": session_count,
                "rows": rows,
                "baseline": baseline,
                "instrumented": instrumented,
                "profile": profile_data,
            }
        )
        print(f"profile {scale_index}/2: all five projections measured", flush=True)

    summary = build_summary(runs)
    json_path = output / "summary.json"
    markdown_path = output / "summary.md"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    markdown_path.write_text(render_markdown(summary))
    counts = summary["candidate_counts"]
    print(
        "summary: "
        f"{counts['confirmed']} confirmed, {counts['inconclusive']} inconclusive, "
        f"{counts['ruled_out']} ruled out",
        flush=True,
    )
    print(f"wrote {json_path}", flush=True)
    print(f"wrote {markdown_path}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2)
