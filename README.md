# Historious

Your coding agents have a past. Historious makes it searchable.

It indexes local agent transcripts from tools like Codex, Claude Code, pi,
OpenClaw, Hermes, OpenCode, and optional Treechat sources, then gives you one
fast `histo` command for finding old decisions, fixes, commands, failures, and
half-remembered threads. Instead of asking "where did we solve this already?",
you can search the actual history and hand the useful parts back to your next
agent.

Install it if you use coding agents across more than one repo, machine, model,
or session. Historious helps with agent memory: recovering old threads, avoiding
repeated mistakes, reusing working commands, syncing history between devices,
and giving agents a reliable way to learn from prior work without you becoming
the human clipboard.

In a hurry? Point your agent at this README and say:

```text
Install Historious from nikvdp/historious, put `histo` on PATH, run
`histo update`, then run `histo onboard --agents-md` or install the packaged
Historious skill for this agent. Use `histo --robot` for searches.
```

## Quick Start

Install a release binary:

```bash
mkdir -p ~/.local/bin
asset=histo-macos-aarch64 # choose from the table below
curl -L "https://github.com/nikvdp/historious/releases/latest/download/$asset" \
  -o ~/.local/bin/histo
chmod +x ~/.local/bin/histo
```

Use the asset that matches your machine:

| Platform | Asset |
| --- | --- |
| macOS Apple Silicon | `histo-macos-aarch64` |
| macOS Intel | `histo-macos-x86_64` |
| Linux x86_64 static | `histo-linux-x86_64-musl` |
| Linux ARM64 static | `histo-linux-aarch64-musl` |
| Windows x86_64 | `histo-windows-x86_64.exe` |

Or build from source with Rust:

```bash
cargo install --git https://github.com/nikvdp/historious historious --locked
```

Then index your local history and search it:

```bash
histo update
histo search "that weird auth retry bug"
histo show <ref> --before 5 --after 8
histo transcript <session_id> --at <ref>
```

For agents and scripts, use robot mode:

```bash
histo --robot status
histo --robot search "distinctive query terms" --limit 20
histo --robot show <ref> --before 5 --after 8
```

## What You Get

- One local archive for many agent histories.
- Search by words, paths, errors, branch names, commands, or fuzzy concepts.
- Timeline discovery with `threads` when you remember when work happened.
- Exact transcript recovery with stable refs for follow-up commands.
- Machine-friendly JSON envelopes for agents.
- A local TUI with `histo tui`.
- JSONL import/export so machines can exchange history over SSH.
- Packaged agent instructions and skills so your agent knows how to use it.

Historious is local-first. It scans local logs into a SQLite-backed archive in
your Historious data directory, and it does not need a hosted service for normal
use.

## Teach An Agent To Use Historious

The simplest path is to let Historious print the instructions:

```bash
histo onboard
histo onboard --agents-md
```

For agents with skill systems, use the packaged skill:

```bash
histo skill list
histo skill emit search-agent-history-historious
histo skill install search-agent-history-historious --codex
histo skill install search-agent-history-historious --claude
histo skill install search-agent-history-historious --pi
```

Agent rule of thumb: prefer `histo --robot`, search broadly first, group hits by
`session_id`, then inspect promising refs with `show` or `transcript`. Use full
transcripts when exact commands, file paths, or final decisions matter.

## Daily Commands

Refresh the archive:

```bash
histo update
```

Check health:

```bash
histo status
histo --robot status
```

Find recent threads:

```bash
histo threads --all --today
histo threads --all --after "3 days ago"
histo threads --project /absolute/repo/path
```

Search with useful filters:

```bash
histo search "migration rollback sqlite" --project /absolute/repo/path
histo search "rate limit 429" --all --after 2026-06-01
histo search "cargo zigbuild release" --include-tools
histo search "exact_function_name" --mode lexical
histo search "why did the sync loop repeat" --mode semantic
```

Inspect results:

```bash
histo show <ref>
histo show <ref> --before 10 --after 10
histo transcript <session_id> --at <ref>
histo tail <session_id>
```

Open the terminal UI:

```bash
histo tui
```

## Sync Between Machines

Historious sync is plain JSONL over stdin/stdout. Both machines need `histo` on
`PATH`.

Pull remote history into the local machine:

```bash
ssh <remote> 'histo export --jsonl' \
  | histo import --jsonl -
```

Push local history to a remote machine:

```bash
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

Do not add `histo update` to these exchange flows. `update` scans local agent log
files; export/import exchanges records already stored in Historious.

## Embeddings

Historious can use embeddings for semantic search. Embeddings are enabled by
default when the binary was built with FastEmbed support.

Persistently turn embeddings off for this data directory:

```bash
histo config embeddings off
```

Turn them back on:

```bash
histo config embeddings on
```

Inspect the current setting and config path:

```bash
histo config show
```

Use `--no-embeddings` on commands such as `update`, `import`, `search`, `tui`,
`daemon`, or `serve` when you want a one-off lexical-only run.

Release note: Linux musl release binaries are built without FastEmbed so they
stay static and portable. Build from source with Cargo when you want Linux
semantic embedding support in the binary itself.

## Remote TUI

Local TUI starts and uses the default local server automatically:

```bash
histo tui
```

To run the server yourself:

```bash
histo serve
histo tui --server-url http://127.0.0.1:7391
```

For another machine, keep the server bound to loopback and reach it through SSH:

```bash
ssh -L 7391:127.0.0.1:7391 <remote> 'histo serve'
histo tui --server-url http://127.0.0.1:7391
```

Direct LAN exposure is explicit because the HTTP server is unauthenticated:

```bash
ssh <remote> 'histo serve --bind 0.0.0.0:7391 --allow-network-bind'
histo tui --server-url http://<remote-ip>:7391
```

Do not expose the unauthenticated HTTP server directly on a public interface.

## Release Flow

Maintainers can prepare a release locally:

```bash
make release-dry-run
make release VERSION=0.1.1
```

Without `VERSION`, the release task bumps the patch version. It updates
`Cargo.toml` and `Cargo.lock`, creates a release commit, and creates an
annotated `vX.Y.Z` tag. Pushing the tag to `nikvdp/historious` triggers GitHub
Actions to build release artifacts and create or update the GitHub release:

```bash
git push origin HEAD
git push origin v0.1.1
```
