#!/usr/bin/env bash
set -euo pipefail

port="${SUPER_CASS_SMOKE_PORT:-7395}"
bind="127.0.0.1:${port}"
server="http://${bind}"
query="${SUPER_CASS_SMOKE_QUERY:-remote tui smoke}"
tui_query="${SUPER_CASS_TUI_SMOKE_QUERY:-http backed preview}"
bin="${SUPER_CASS_BIN:-target/debug/super-cass}"
tmpdir=""

if [[ ! -x "$bin" ]]; then
  echo "Smoke binary not found at $bin; run cargo build first or set SUPER_CASS_BIN." >&2
  exit 1
fi

pid=""
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  if [[ -n "$tmpdir" ]]; then
    rm -rf "$tmpdir"
  fi
}
trap cleanup EXIT

tmpdir="$(mktemp -d)"
remote_dir="${tmpdir}/remote"
client_dir="${tmpdir}/client"
archive="${tmpdir}/remote-fixture.jsonl"
mkdir -p "$remote_dir" "$client_dir"

cat >"$archive" <<'JSONL'
{"schema":"super-cass.archive.v1","id":"source_remote_smoke","hash":"hash_source_remote_smoke","producer":"super-cass/smoke","produced_at":"2026-06-11T00:00:00Z","kind":"source","payload":{"id":"source_remote_smoke","kind":"codex","identity":"source_remote_smoke","path":"/tmp/remote-smoke.jsonl","first_seen_at":"2026-06-11T00:00:00Z","updated_at":"2026-06-11T00:00:00Z","hash":"hash_source_remote_smoke"}}
{"schema":"super-cass.archive.v1","id":"session_remote_smoke","hash":"hash_session_remote_smoke","producer":"super-cass/smoke","produced_at":"2026-06-11T00:00:00Z","kind":"session","payload":{"id":"session_remote_smoke","source_id":"source_remote_smoke","machine_id":"machine_remote_smoke","source_kind":"codex","external_id":"agent_session_remote_smoke","title":"Remote TUI smoke","status":"open","started_at":"2026-06-11T00:00:00Z","updated_at":"2026-06-11T00:01:00Z","metadata":{"workspace_path":"/tmp/remote-smoke-workspace"},"hash":"hash_session_remote_smoke"}}
{"schema":"super-cass.archive.v1","id":"event_remote_smoke_1","hash":"hash_event_remote_smoke_1","producer":"super-cass/smoke","produced_at":"2026-06-11T00:00:00Z","kind":"event","payload":{"id":"event_remote_smoke_1","session_id":"session_remote_smoke","source_id":"source_remote_smoke","machine_id":"machine_remote_smoke","source_kind":"codex","ordinal":1,"event_type":"message","role":"user","content":"remote tui smoke setup before the target","raw_artifact_hash":null,"occurred_at":"2026-06-11T00:00:10Z","metadata":{"search_indexable":true,"search_kind":"user","search_text":"remote tui smoke setup before the target"},"hash":"hash_event_remote_smoke_1"}}
{"schema":"super-cass.archive.v1","id":"event_remote_smoke_2","hash":"hash_event_remote_smoke_2","producer":"super-cass/smoke","produced_at":"2026-06-11T00:00:00Z","kind":"event","payload":{"id":"event_remote_smoke_2","session_id":"session_remote_smoke","source_id":"source_remote_smoke","machine_id":"machine_remote_smoke","source_kind":"codex","ordinal":2,"event_type":"message","role":"assistant","content":"remote tui smoke target verifies http backed preview and enter","raw_artifact_hash":null,"occurred_at":"2026-06-11T00:00:20Z","metadata":{"search_indexable":true,"search_kind":"assistant","search_text":"remote tui smoke target verifies http backed preview and enter"},"hash":"hash_event_remote_smoke_2"}}
{"schema":"super-cass.archive.v1","id":"event_remote_smoke_3","hash":"hash_event_remote_smoke_3","producer":"super-cass/smoke","produced_at":"2026-06-11T00:00:00Z","kind":"event","payload":{"id":"event_remote_smoke_3","session_id":"session_remote_smoke","source_id":"source_remote_smoke","machine_id":"machine_remote_smoke","source_kind":"codex","ordinal":3,"event_type":"message","role":"assistant","content":"remote tui smoke follow up after the target","raw_artifact_hash":null,"occurred_at":"2026-06-11T00:00:30Z","metadata":{"search_indexable":true,"search_kind":"assistant","search_text":"remote tui smoke follow up after the target"},"hash":"hash_event_remote_smoke_3"}}
JSONL

"$bin" --data-dir "$remote_dir" import --jsonl --json "$archive" >/dev/null

"$bin" --data-dir "$remote_dir" serve --bind "$bind" >/tmp/super-cass-tui-smoke.log 2>&1 &
pid="$!"

for _ in {1..40}; do
  if curl -fsS --max-time 1 "${server}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Server exited before becoming ready:" >&2
    cat /tmp/super-cass-tui-smoke.log >&2
    exit 1
  fi
  sleep 0.1
done

curl -fsS --max-time 2 "${server}/health" >/dev/null
rows="$(
  curl -fsSG --max-time 10 "${server}/search" \
    --data-urlencode "q=${query}" \
    --data-urlencode "limit=5" \
    --data-urlencode "sort=relevance" \
    --data-urlencode "recency_bias=0" \
    --data-urlencode "format=fzf"
)"

if [[ -z "$rows" ]]; then
  echo "TUI search smoke returned no fzf rows for query: $query" >&2
  exit 1
fi

show="$(
  curl -fsSG --max-time 10 "${server}/show" \
    --data-urlencode "event=event_remote_smoke_2" \
    --data-urlencode "before=1" \
    --data-urlencode "after=1"
)"

if [[ "$show" != *"remote tui smoke target verifies http backed preview and enter"* ]]; then
  echo "Remote show endpoint did not return the expected target content." >&2
  exit 1
fi

tui_out="${tmpdir}/tui.out"
FZF_DEFAULT_OPTS="--sync --select-1 --exit-0" \
  "$bin" --data-dir "$client_dir" tui --remote "$server" "$tui_query" --limit 5 --no-color >"$tui_out"

if ! grep -q "remote tui smoke target verifies http backed preview and enter" "$tui_out"; then
  echo "Remote TUI smoke did not print the selected remote transcript." >&2
  cat "$tui_out" >&2
  exit 1
fi

printf '%s\n' "$rows" | head -n 5
cat "$tui_out"
