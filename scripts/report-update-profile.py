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
import shutil
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
        "--histo",
        required=True,
        type=Path,
        help="Release histo binary to measure",
    )
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
        started = origin + dt.timedelta(hours=(index % 480) * 12)
        ended = started + dt.timedelta(minutes=20 + index % 40)
        started_at = started.isoformat().replace("+00:00", "Z")
        ended_at = ended.isoformat().replace("+00:00", "Z")
        root_external_id = f"00000000-0000-4000-8000-{index:012x}"
        session_metadata = {"workspace_path": f"/synthetic/project-{index % 8}"}
        if index % 5 == 1:
            parent_external_id = f"00000000-0000-4000-8000-{index - 1:012x}"
            external_id = f"agent-{index:08d}"
            session_metadata["path"] = (
                f"/synthetic/{parent_external_id}/subagents/{external_id}.jsonl"
            )
        else:
            external_id = root_external_id
            session_metadata["path"] = f"/synthetic/{external_id}.jsonl"
        session = {
            "id": session_id,
            "source_id": source_id,
            "machine_id": "synthetic-machine",
            "source_kind": "claude_code",
            "external_id": external_id,
            "title": f"Synthetic session {index:08d}",
            "status": "complete",
            "started_at": started_at,
            "updated_at": ended_at,
            "metadata": session_metadata,
            "hash": digest("session", index),
        }
        yield envelope("session", session, produced_at)

        for ordinal in range(6):
            event_id = f"synthetic-event-{index:08d}-{ordinal}"
            role = "user" if ordinal % 2 == 0 else "assistant"
            occurred = started + dt.timedelta(minutes=ordinal * 3)
            model = f"synthetic-model-{(index + ordinal // 2) % 6}"
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
    proc = subprocess.Popen(
        args,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=1,
    )
    assert proc.stderr is not None
    stderr_lines = []
    last_emit = 0.0
    last_phase: str | None = None
    for line in proc.stderr:
        stderr_lines.append(line)
        now = time.monotonic()
        emit = True
        try:
            payload = json.loads(line)
            phase = payload.get("phase")
            status = payload.get("data", {}).get("status")
            emit = phase != last_phase or now - last_emit >= 1.0 or status == "finished"
            last_phase = phase
        except json.JSONDecodeError:
            pass
        if emit:
            print(f"  {line}", end="", flush=True)
            last_emit = now
    returncode = proc.wait()
    if returncode != 0:
        raise RuntimeError(
            f"command failed with exit {returncode}: {' '.join(args)}\n"
            f"{''.join(stderr_lines)}"
        )


def run_report(
    binary: Path,
    store: Path,
    profile_path: Path | None,
    stdout_path: Path,
    stderr_path: Path,
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
    observations = [started]
    transitions: list[dict[str, int | str]] = []
    phase_started: dict[str, int] = {}
    phase_finished: dict[str, int] = {}
    current: str | None = None
    stderr_lines: list[str] = []
    with (
        stdout_path.open("w", encoding="utf-8") as stdout_stream,
        stderr_path.open("w", encoding="utf-8") as stderr_stream,
    ):
        proc = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            text=True,
            stdout=stdout_stream,
            stderr=subprocess.PIPE,
            bufsize=1,
        )
        assert proc.stderr is not None
        for line in proc.stderr:
            now = time.monotonic_ns()
            observations.append(now)
            stderr_lines.append(line)
            stderr_stream.write(line)
            stderr_stream.flush()
            print(f"  {line}", end="", flush=True)
            for marker, phase in PHASE_MARKERS:
                if marker in line:
                    if current is not None and current not in phase_finished:
                        phase_finished[current] = now
                    current = phase
                    phase_started.setdefault(phase, now)
                    transitions.append(
                        {"phase": phase, "elapsed_ns": now - started}
                    )
                    break
            if "report refreshed" in line and current is not None:
                phase_finished[current] = now
                current = None
        returncode = proc.wait()
    finished = time.monotonic_ns()
    observations.append(finished)
    if returncode != 0:
        raise RuntimeError(
            f"forced report failed with exit {returncode}:\n{''.join(stderr_lines)}"
        )
    if current is not None:
        phase_finished[current] = finished
    missing = [
        phase
        for phase in PHASES
        if phase not in phase_started or phase not in phase_finished
    ]
    if missing:
        raise RuntimeError(f"forced report omitted phase transitions: {', '.join(missing)}")
    return {
        "total_duration_ns": finished - started,
        "time_to_first_progress_ns": observations[1] - started,
        "longest_silent_interval_ns": max(
            right - left for left, right in zip(observations, observations[1:])
        ),
        "phase_duration_ns": {
            phase: phase_finished[phase] - phase_started[phase] for phase in PHASES
        },
        "progress_transitions": transitions,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
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
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
        counts["delegated_relationships"] = conn.execute(
            "SELECT COUNT(*) FROM session_relationships WHERE relationship = 'subagent'"
        ).fetchone()[0]
        ready = conn.execute(
            "SELECT status FROM projection_status WHERE projection_name = 'report_snapshot'"
        ).fetchone()
        counts["report_snapshot_ready"] = int(ready is not None and ready[0] == "ready")
        return counts


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
    n_run: dict[str, Any],
    two_n_run: dict[str, Any],
) -> list[dict[str, Any]]:
    n_rows = n_run["profile"]["statements"]
    two_n_rows = two_n_run["profile"]["statements"]
    sessions_2n = two_n_run["sessions"]
    rows = []
    for key, two_n_row in two_n_rows.items():
        if key[0] not in PHASES:
            continue
        n_row = n_rows.get(key)
        classification = classify_candidate(n_row, two_n_row, sessions_2n)
        if classification is None:
            continue
        candidate_type, disposition = classification
        if candidate_type == "repeated_point_query":
            candidate_class = "N+1"
        elif candidate_type == "scan_growth" and two_n_row["calls"] > 1:
            candidate_class = "repeated scan"
        elif candidate_type == "scan_growth" and key[0] == "report_snapshot":
            candidate_class = "necessary aggregation"
        else:
            candidate_class = "inconclusive"
        n_phase_duration = n_run["baseline"]["phase_duration_ns"][key[0]]
        rows.append(
            {
                "phase": key[0],
                "fingerprint": key[1],
                "sql": two_n_row["sql"],
                "candidate_type": candidate_type,
                "candidate_class": candidate_class,
                "disposition": disposition,
                "baseline_phase_duration_ns": n_phase_duration,
                "baseline_phase_share": ratio(
                    n_run["baseline"]["total_duration_ns"], n_phase_duration
                ),
                "n": {
                    **{
                        field: n_row[field] if n_row else None
                        for field in (
                            "calls",
                            "total_duration_ns",
                            "max_duration_ns",
                            "full_scan_steps",
                            "vm_steps",
                        )
                    },
                    "calls_per_session": (
                        n_row["calls"] / n_run["rows"]["sessions"] if n_row else None
                    ),
                    "calls_per_message": (
                        n_row["calls"] / n_run["rows"]["message_provenance"]
                        if n_row
                        else None
                    ),
                },
                "2n": {
                    **{
                        field: two_n_row[field]
                        for field in (
                            "calls",
                            "total_duration_ns",
                            "max_duration_ns",
                            "full_scan_steps",
                            "vm_steps",
                        )
                    },
                    "calls_per_session": (
                        two_n_row["calls"] / two_n_run["rows"]["sessions"]
                    ),
                    "calls_per_message": (
                        two_n_row["calls"]
                        / two_n_run["rows"]["message_provenance"]
                    ),
                },
                "ratios": {
                    field: ratio(n_row[field], two_n_row[field]) if n_row else None
                    for field in (
                        "calls",
                        "total_duration_ns",
                        "full_scan_steps",
                        "vm_steps",
                    )
                },
            }
        )
    rows.sort(
        key=lambda row: (
            -row["baseline_phase_duration_ns"],
            -(row["n"]["total_duration_ns"] or 0),
            row["phase"],
            row["fingerprint"],
        )
    )
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    return rows


def build_summary(
    runs: list[dict[str, Any]], metadata: dict[str, Any]
) -> dict[str, Any]:
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
    candidates = rank_candidates(n_run, two_n_run)
    return {
        "schema": "historious.report_sql_scale.v1",
        "measurement": metadata,
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
    measurement = summary["measurement"]
    lines = [
        "# Forced report SQL scaling",
        "",
        f"- Binary: `{measurement['binary']}`",
        f"- SHA-256: `{measurement['binary_sha256']}`",
        f"- Harness arguments: `{measurement['arguments']}`",
        "",
        "## Run timing",
        "",
        "| Sessions | Mode | Total ms | First progress ms | Longest silence ms | Durable | Relationships | Provenance | Facts | Model context | Snapshot |",
        "|---:|---|---:|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    for run in summary["runs"]:
        for mode in ("baseline", "instrumented"):
            timing = run[mode]
            phase = timing["phase_duration_ns"]
            lines.append(
                f"| {run['sessions']} | {mode} | {milliseconds(timing['total_duration_ns'])} "
                f"| {milliseconds(timing['time_to_first_progress_ns'])} "
                f"| {milliseconds(timing['longest_silent_interval_ns'])} "
                f"| {'yes' if timing['durable_completion'] else 'no'} "
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
            "| Sessions | Sessions | Events | Relationships | Delegated | Provenance | Facts | Model context | Snapshot |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for run in summary["runs"]:
        rows = run["rows"]
        lines.append(
            f"| {run['sessions']} | {rows['sessions']} | {rows['events']} "
            f"| {rows['session_relationships']} | {rows['delegated_relationships']} "
            f"| {rows['message_provenance']} | {rows['session_facts']} "
            f"| {rows['message_model_context']} | {rows['report_snapshot']} |"
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
            "| Rank | Phase | Phase share | Fingerprint | Class | Evidence | Disposition | Calls N→2N | Total ms N→2N | Max ms N→2N | VM N→2N | Scan N→2N | SQL shape |",
            "|---:|---|---:|---|---|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in summary["candidates"]:
        sql = row["sql"].replace("|", "\\|").replace("\n", " ")
        if len(sql) > 180:
            sql = sql[:177] + "..."
        lines.append(
            f"| {row['rank']} | {row['phase']} | {row['baseline_phase_share']:.1%} "
            f"| `{row['fingerprint']}` | {row['candidate_class']} "
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
            "Dispositions confirm observed scaling patterns, not inefficiency findings. "
            "Point-read growth is reproducible; scan, write, and VM growth require plan proof.",
            "",
        ]
    )
    return "\n".join(lines)

def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_saved_runs(output: Path, session_counts: tuple[int, int]) -> list[dict[str, Any]]:
    runs = []
    for label, session_count in zip(("n", "2n"), session_counts):
        scale_dir = output / label
        runs.append(
            {
                "sessions": session_count,
                "rows": json.loads((scale_dir / "rows.json").read_text()),
                "baseline": json.loads(
                    (scale_dir / "baseline-timing.json").read_text()
                ),
                "instrumented": json.loads(
                    (scale_dir / "instrumented-timing.json").read_text()
                ),
                "profile": load_profile(scale_dir / "profile.jsonl"),
            }
        )
    return runs




def main() -> int:
    args = parse_args()
    if args.sessions < 2:
        raise ValueError("--sessions must be at least 2")
    output = validate_output_dir(args.output_dir)
    binary = args.histo.expanduser()
    if not binary.is_absolute():
        binary = ROOT / binary
    binary = binary.resolve()

    print("build 0/1: compiling release binary", flush=True)
    run_checked(["cargo", "build", "--release"])
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f"histo binary is not executable after release build: {binary}")
    print("build 1/1: release binary ready", flush=True)

    session_counts = (args.sessions, args.sessions * 2)
    metadata = {
        "binary": str(binary),
        "binary_sha256": sha256_file(binary),
        "arguments": {
            "histo": str(args.histo),
            "sessions": args.sessions,
            "output_dir": str(output),
        },
    }
    runs: list[dict[str, Any]] = []
    for scale_index, session_count in enumerate(session_counts, 1):
        label = "n" if scale_index == 1 else "2n"
        scale_dir = output / label
        scale_dir.mkdir()
        archive = scale_dir / "input.jsonl"
        pristine_store = scale_dir / "pristine-store"
        baseline_store = scale_dir / "baseline-store"
        instrumented_store = scale_dir / "instrumented-store"
        profile = scale_dir / "profile.jsonl"

        print(f"dataset {scale_index}/2: generating {session_count} sessions", flush=True)
        write_archive(archive, session_count)
        import_env = os.environ.copy()
        import_env.pop(PROFILE_ENV, None)
        run_checked(
            [
                str(binary),
                "--data-dir",
                str(pristine_store),
                "import",
                "--jsonl",
                "--json",
                "--no-embeddings",
                str(archive),
            ],
            env=import_env,
        )

        shutil.copytree(pristine_store, baseline_store)
        print(f"baseline {scale_index}/2: rebuilding {session_count} sessions", flush=True)
        baseline = run_report(
            binary,
            baseline_store,
            None,
            scale_dir / "baseline.stdout",
            scale_dir / "baseline.stderr",
        )
        if profile.exists():
            raise RuntimeError("profiling output appeared during the baseline run")
        baseline_rows = table_counts(baseline_store)
        baseline["durable_completion"] = bool(
            baseline_rows["report_snapshot"] and baseline_rows["report_snapshot_ready"]
        )

        shutil.copytree(pristine_store, instrumented_store)
        print(f"profile {scale_index}/2: rebuilding {session_count} sessions", flush=True)
        instrumented = run_report(
            binary,
            instrumented_store,
            profile,
            scale_dir / "instrumented.stdout",
            scale_dir / "instrumented.stderr",
        )
        profile_data = load_profile(profile)
        rows = table_counts(instrumented_store)
        instrumented["durable_completion"] = bool(
            rows["report_snapshot"] and rows["report_snapshot_ready"]
        )
        if rows != baseline_rows:
            raise RuntimeError(f"{label} baseline and instrumented row counts differ")
        for table, count in rows.items():
            if count <= 0:
                raise RuntimeError(f"{label} projection evidence is empty: {table}")
        write_json(scale_dir / "baseline-timing.json", baseline)
        write_json(scale_dir / "instrumented-timing.json", instrumented)
        write_json(scale_dir / "rows.json", rows)
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

    summary = build_summary(runs, metadata)
    replayed = build_summary(load_saved_runs(output, session_counts), metadata)
    if replayed != summary:
        raise RuntimeError("saved-artifact summarizer replay changed the candidate table")
    summary["summarizer_replay_match"] = True
    json_path = output / "summary.json"
    markdown_path = output / "summary.md"
    write_json(json_path, summary)
    markdown_path.write_text(render_markdown(summary))
    counts = summary["candidate_counts"]
    print(
        "summary: "
        f"{counts['confirmed']} reproducible point-read patterns, "
        f"{counts['inconclusive']} inconclusive, {counts['ruled_out']} ruled out",
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
