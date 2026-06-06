#!/usr/bin/env python3
"""Validate search speed and ordering against the local real corpus.

This is intentionally local-only. It uses the operator's configured super-cass
data directory and should be run before closing search/indexing work.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRIMARY_QUERY = "thread title summary"
DEFAULT_SEMANTIC_QUERY = "fee cache"


def env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        print(f"{name} must be a number, got {value!r}", file=sys.stderr)
        sys.exit(2)
    if parsed <= 0:
        print(f"{name} must be positive, got {value!r}", file=sys.stderr)
        sys.exit(2)
    return parsed


def default_bin() -> Path:
    release = ROOT / "target" / "release" / "super-cass"
    debug = ROOT / "target" / "debug" / "super-cass"
    if release.exists() and os.access(release, os.X_OK):
        return release
    if debug.exists() and os.access(debug, os.X_OK):
        return debug
    return release


def elapsed_ms(start: float) -> float:
    return (time.monotonic() - start) * 1000.0


def run_command(args: list[str], timeout_seconds: float) -> tuple[float, str]:
    start = time.monotonic()
    proc = subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
    )
    duration_ms = elapsed_ms(start)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(args)} failed with exit {proc.returncode}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return duration_ms, proc.stdout


def run_search_json(
    binary: Path,
    query: str,
    mode: str,
    limit: int,
    timeout_seconds: float,
) -> tuple[float, dict]:
    duration_ms, stdout = run_command(
        [
            str(binary),
            "search",
            query,
            "--mode",
            mode,
            "--limit",
            str(limit),
            "--json",
        ],
        timeout_seconds,
    )
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"search {mode} did not emit valid JSON: {exc}") from exc
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        raise RuntimeError(f"search {mode} JSON has unexpected shape")
    return duration_ms, data


def assert_under(name: str, value_ms: float, threshold_ms: float, failures: list[str]) -> None:
    status = "ok" if value_ms <= threshold_ms else "slow"
    print(f"{name}: {value_ms:.0f}ms <= {threshold_ms:.0f}ms [{status}]")
    if value_ms > threshold_ms:
        failures.append(f"{name} took {value_ms:.0f}ms, threshold {threshold_ms:.0f}ms")


def assert_results(name: str, data: dict, failures: list[str]) -> list[dict]:
    results = data.get("results")
    if not isinstance(results, list):
        failures.append(f"{name} JSON has no results array")
        return []
    if not results:
        failures.append(f"{name} returned no results")
    return results


def validate_primary_order(results: list[dict], failures: list[str]) -> None:
    if not results:
        return
    first = results[0]
    lexical_rank = first.get("lexical_rank")
    match_type = first.get("match_type")
    print(
        "primary hybrid first result: "
        f"ref={first.get('ref')} match={match_type} lexical_rank={lexical_rank} "
        f"semantic_rank={first.get('semantic_rank')}"
    )
    if lexical_rank != 1:
        failures.append(
            "primary hybrid did not keep lexical rank 1 first "
            f"(got lexical_rank={lexical_rank}, match_type={match_type})"
        )


def validate_semantic_value(results: list[dict], failures: list[str]) -> None:
    if not results:
        return
    has_semantic = any(
        row.get("match_type") in {"semantic", "hybrid"} or row.get("semantic_rank") is not None
        for row in results
    )
    print(f"semantic-query semantic/hybrid evidence: {'yes' if has_semantic else 'no'}")
    if not has_semantic:
        failures.append("semantic query returned no semantic or hybrid evidence")


class StartedServer:
    def __init__(self, proc: subprocess.Popen[str], log_path: Path) -> None:
        self.proc = proc
        self.log_path = log_path

    def stop(self) -> None:
        if self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)


def urlopen_text(url: str, timeout_seconds: float) -> str:
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8")


def start_server(binary: Path, port: int, startup_timeout_seconds: float) -> StartedServer:
    bind = f"127.0.0.1:{port}"
    log_file = tempfile.NamedTemporaryFile(
        prefix="super-cass-live-search-guardrail.",
        suffix=".log",
        delete=False,
    )
    log_path = Path(log_file.name)
    proc = subprocess.Popen(
        [str(binary), "serve", "--bind", bind],
        cwd=ROOT,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    log_file.close()
    server = StartedServer(proc, log_path)
    health_url = f"http://{bind}/health"
    start = time.monotonic()
    while elapsed_ms(start) < startup_timeout_seconds * 1000.0:
        if proc.poll() is not None:
            log = log_path.read_text(errors="replace") if log_path.exists() else ""
            raise RuntimeError(f"server exited before health check passed\n{log}")
        try:
            urlopen_text(health_url, 1.0)
            print(f"server startup: {elapsed_ms(start):.0f}ms")
            return server
        except Exception:
            time.sleep(0.1)
    server.stop()
    log = log_path.read_text(errors="replace") if log_path.exists() else ""
    raise RuntimeError(f"server did not become healthy at {health_url}\n{log}")


def server_search_fzf(
    port: int,
    query: str,
    mode: str,
    limit: int,
    timeout_seconds: float,
) -> tuple[float, str]:
    params = urllib.parse.urlencode(
        {
            "q": query,
            "limit": str(limit),
            "sort": "relevance",
            "mode": mode,
            "corpus": "conversation",
            "recency_bias": "0",
            "format": "fzf",
        }
    )
    url = f"http://127.0.0.1:{port}/search?{params}"
    start = time.monotonic()
    body = urlopen_text(url, timeout_seconds)
    return elapsed_ms(start), body


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(
        description="Run live real-corpus search speed and ordering guardrails."
    )
    parser.add_argument(
        "--bin",
        default=os.environ.get("SUPER_CASS_BIN", str(default_bin())),
        help="super-cass binary to test; defaults to target/release then target/debug",
    )
    parser.add_argument(
        "--primary-query",
        default=os.environ.get("SUPER_CASS_GUARDRAIL_PRIMARY_QUERY", DEFAULT_PRIMARY_QUERY),
        help="exact/anchor query used for lexical and hybrid ordering checks",
    )
    parser.add_argument(
        "--semantic-query",
        default=os.environ.get("SUPER_CASS_GUARDRAIL_SEMANTIC_QUERY", DEFAULT_SEMANTIC_QUERY),
        help="synonym/concept query used to confirm semantic search still adds value",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("SUPER_CASS_GUARDRAIL_PORT", "7396")),
        help="local server port for warmed HTTP/fzf validation",
    )
    parser.add_argument(
        "--skip-server",
        action="store_true",
        help="skip warmed server/fzf validation",
    )
    args = parser.parse_args()

    binary = Path(args.bin)
    if not binary.exists() or not os.access(binary, os.X_OK):
        print(
            f"super-cass binary not found or not executable: {binary}\n"
            "Run cargo build --release or set SUPER_CASS_BIN.",
            file=sys.stderr,
        )
        return 2

    thresholds = {
        "cli_lexical": env_float("SUPER_CASS_GUARDRAIL_CLI_LEXICAL_MS", 1500.0),
        "cli_semantic": env_float("SUPER_CASS_GUARDRAIL_CLI_SEMANTIC_MS", 5000.0),
        "cli_hybrid": env_float("SUPER_CASS_GUARDRAIL_CLI_HYBRID_MS", 5000.0),
        "semantic_query": env_float("SUPER_CASS_GUARDRAIL_SEMANTIC_QUERY_MS", 5000.0),
        "server_fzf": env_float("SUPER_CASS_GUARDRAIL_SERVER_FZF_MS", 2000.0),
        "server_startup": env_float("SUPER_CASS_GUARDRAIL_SERVER_STARTUP_MS", 30000.0),
    }
    command_timeout = max(thresholds["cli_semantic"], thresholds["cli_hybrid"], 5000.0) / 1000.0
    command_timeout += 10.0
    failures: list[str] = []

    print(f"binary: {binary}")
    print(f"primary query: {args.primary_query!r}")
    print(f"semantic query: {args.semantic_query!r}")

    lexical_ms, lexical = run_search_json(
        binary, args.primary_query, "lexical", 3, command_timeout
    )
    assert_under("cli lexical primary", lexical_ms, thresholds["cli_lexical"], failures)
    assert_results("cli lexical primary", lexical, failures)

    semantic_ms, semantic = run_search_json(
        binary, args.primary_query, "semantic", 3, command_timeout
    )
    assert_under("cli semantic primary", semantic_ms, thresholds["cli_semantic"], failures)
    assert_results("cli semantic primary", semantic, failures)

    hybrid_ms, hybrid = run_search_json(binary, args.primary_query, "hybrid", 3, command_timeout)
    assert_under("cli hybrid primary", hybrid_ms, thresholds["cli_hybrid"], failures)
    hybrid_results = assert_results("cli hybrid primary", hybrid, failures)
    validate_primary_order(hybrid_results, failures)

    semantic_query_ms, semantic_query = run_search_json(
        binary, args.semantic_query, "hybrid", 5, command_timeout
    )
    assert_under(
        "cli hybrid semantic-query",
        semantic_query_ms,
        thresholds["semantic_query"],
        failures,
    )
    semantic_query_results = assert_results(
        "cli hybrid semantic-query", semantic_query, failures
    )
    validate_semantic_value(semantic_query_results, failures)

    if not args.skip_server:
        server = start_server(binary, args.port, thresholds["server_startup"] / 1000.0)
        try:
            server_ms, rows = server_search_fzf(
                args.port,
                args.primary_query,
                "hybrid",
                3,
                thresholds["server_fzf"] / 1000.0 + 5.0,
            )
            assert_under("server fzf hybrid primary", server_ms, thresholds["server_fzf"], failures)
            first_row = rows.splitlines()[0] if rows.splitlines() else ""
            if not first_row:
                failures.append("server fzf hybrid primary returned no rows")
            else:
                fields = first_row.split("\t")
                match_type = fields[2] if len(fields) > 2 else ""
                print(f"server fzf first row: ref={fields[0] if fields else ''} match={match_type}")
                if match_type == "semantic":
                    failures.append("server fzf first row was semantic-only for primary query")
        finally:
            server.stop()

    if failures:
        print("\nSearch guardrail failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print("\nSearch guardrail passed.")
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    raise SystemExit(main())
