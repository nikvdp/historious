#!/usr/bin/env bash
set -euo pipefail

port="${SUPER_CASS_SMOKE_PORT:-7395}"
bind="127.0.0.1:${port}"
server="http://${bind}"
query="${SUPER_CASS_SMOKE_QUERY:-thread}"
bin="${SUPER_CASS_BIN:-target/debug/super-cass}"

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
}
trap cleanup EXIT

"$bin" serve --bind "$bind" >/tmp/super-cass-tui-smoke.log 2>&1 &
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

printf '%s\n' "$rows" | head -n 5
