# Historious

Historious indexes local coding-agent history and exposes it through the `histo`
CLI, robot-friendly JSON, and an optional local TUI/server.

## Basic Usage

```bash
# Refresh the local archive from agent log files.
histo update

# Search indexed history.
histo search "distinctive query terms"

# Use stable JSON envelopes for automation.
histo --robot search "distinctive query terms" --limit 20

# Inspect a search result by returned ref.
histo --robot show <ref> --before 5 --after 8
histo --robot transcript <session_id> --at <ref>
```

## Embedding Mode

Historious can run with embeddings enabled or disabled. Embeddings are enabled
by default. To persistently turn them off for this data directory:

```bash
histo config embeddings off
```

Turn them back on with:

```bash
histo config embeddings on
```

Inspect the current setting and the exact config file path with:

```bash
histo config show
```

Use `--no-embeddings` on commands such as `update`, `import`, `search`, `tui`,
`daemon`, or `serve` when you want a one-off lexical-only run without changing
`config.toml`.

## Sync Between Machines

The canonical way to exchange history between machines is JSONL over
stdin/stdout. Both machines need `histo` installed and available on `PATH` for
the SSH command.

Pull history from a remote machine into the local machine:

```bash
ssh <remote> 'histo export --jsonl' \
  | histo import --jsonl -
```

Push local history to a remote machine:

```bash
histo export --jsonl \
  | ssh <remote> 'histo import --jsonl -'
```

Run both commands for a reciprocal exchange:

```bash
ssh <remote> 'histo export --jsonl' \
  | histo import --jsonl -

histo export --jsonl \
  | ssh <remote> 'histo import --jsonl -'
```

Useful export filters:

```bash
histo export --jsonl --source codex
histo export --jsonl --workspace /absolute/repo/path
histo export --jsonl --session <session_id>
histo export --jsonl --since 2026-06-01
```

When embeddings are enabled, exports include existing embedding records by
default. Imports store those embeddings and refresh the local vector index, so
compatible transferred embeddings do not need to be recreated.

Use `--embeddings omit` when bandwidth or storage matters:

```bash
histo export --jsonl --embeddings omit \
  | ssh <remote> 'histo import --jsonl -'
```

To use another machine for embedding work, keep embeddings enabled on that
machine, pipe history to it, run any embedding maintenance there, then export
back from it:

```bash
histo export --jsonl \
  | ssh <embedding-host> 'histo import --jsonl -'

ssh <embedding-host> 'histo export --jsonl' \
  | histo import --jsonl -
```

Do not add `histo update` to these exchange flows. `update` scans local agent log
files; export/import exchanges archive records that already exist in Historious.

## Remote TUI

Local TUI starts and uses the default local server automatically:

```bash
histo tui
```

To start the server yourself, keep it running in one terminal:

```bash
histo serve
```

Then connect the TUI from another terminal:

```bash
histo tui --server-url http://127.0.0.1:7391
```

For another machine, keep the server bound to loopback on the remote host and
reach it through an SSH tunnel. In one terminal:

```bash
ssh -L 7391:127.0.0.1:7391 <remote> 'histo serve'
```

Then connect the local TUI through the tunnel:

```bash
histo tui --server-url http://127.0.0.1:7391
```

Direct LAN exposure is explicit because the HTTP server is unauthenticated. In
one terminal:

```bash
ssh <remote> 'histo serve --bind 0.0.0.0:7391 --allow-network-bind'
```

Then connect to the remote address:

```bash
histo tui --server-url http://<remote-ip>:7391
```

Do not expose the unauthenticated HTTP server directly on a public interface.
