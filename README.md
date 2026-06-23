# Historious

Historious is a local search engine for old coding-agent conversations.

If you use Codex, Claude Code, OpenCode, pi, OpenClaw, Hermes, or similar tools,
you already have a pile of useful work sitting in transcript files: commands
that worked, fixes that failed, decisions you made, error messages you chased,
and half-finished ideas that are annoying to find later. Historious indexes that
history and gives you one command, `histo`, for searching it again.

The main use case is agent memory. You can search across projects, machines, and
sessions; recover the exact thread where something happened; and give your next
agent enough context to avoid solving the same problem from scratch. It is not a
hosted memory service. It is a local archive your agents can query.

Short version for the impatient:

```text
Install Historious from nikvdp/historious, put `histo` on PATH, run
`histo update`, then run `histo onboard --agents-md` or install the packaged
Historious skill for this agent. Use `histo --robot` for agent searches.
```

## Install

Download a release binary:

```bash
mkdir -p ~/.local/bin
asset=histo-macos-aarch64 # choose from the table below
curl -L "https://github.com/nikvdp/historious/releases/latest/download/$asset" \
  -o ~/.local/bin/histo
chmod +x ~/.local/bin/histo
```

Pick the asset for your machine:

| Platform | Asset |
| --- | --- |
| macOS Apple Silicon | `histo-macos-aarch64` |
| macOS Intel | `histo-macos-x86_64` |
| Linux x86_64 static | `histo-linux-x86_64-musl` |
| Linux ARM64 static | `histo-linux-aarch64-musl` |
| Windows x86_64 | `histo-windows-x86_64.exe` |

Or build from source:

```bash
cargo install --git https://github.com/nikvdp/historious historious --locked
```

Make sure `~/.local/bin` is on your `PATH` if you used the binary install.

## First Run

Index your local history:

```bash
histo update
```

Check what Historious found:

```bash
histo status
```

Search for something you half remember:

```bash
histo search "that weird auth retry bug"
histo show <ref> --before 5 --after 8
histo transcript <session_id> --at <ref>
```

For scripts and agents, use `--robot` so output is stable JSON:

```bash
histo --robot status
histo --robot search "distinctive query terms" --limit 20
histo --robot show <ref> --before 5 --after 8
```

## Agent Setup

Historious can print instructions that you can paste into `AGENTS.md`,
`CLAUDE.md`, or the equivalent file for your agent:

```bash
histo onboard
histo onboard --agents-md
```

It also ships a packaged skill:

```bash
histo skill list
histo skill emit search-agent-history-historious
histo skill install search-agent-history-historious --codex
histo skill install search-agent-history-historious --claude
histo skill install search-agent-history-historious --pi
```

Good agent behavior is simple: start broad, use `--robot`, group hits by
`session_id`, then inspect promising refs with `show` or `transcript`. Use full
transcripts when exact commands, file paths, or decisions matter.

## Useful Searches

Find recent threads:

```bash
histo threads --all --today
histo threads --all --after "3 days ago"
histo threads --project /absolute/repo/path
```

Search with filters:

```bash
histo search "migration rollback sqlite" --project /absolute/repo/path
histo search "rate limit 429" --all --after 2026-06-01
histo search "cargo zigbuild release" --include-tools
histo search "exact_function_name" --mode lexical
```

After enabling embeddings, semantic search is available too:

```bash
histo search "why did the sync loop repeat" --mode semantic
```

Inspect results:

```bash
histo show <ref>
histo show <ref> --before 10 --after 10
histo transcript <session_id> --at <ref>
histo tail <session_id>
```

## Local TUI

`histo tui` is a local terminal UI built on `fzf`. Install `fzf` first:

```bash
brew install fzf
# or, on Debian/Ubuntu:
sudo apt install fzf
```

Then run:

```bash
histo tui
```

The TUI starts a local Historious server for itself when needed. You usually do
not need to run `histo serve` by hand.

For a fixed result set instead of live search, use:

```bash
histo search "query terms" --fzf
```

## Sync Machines

Sync is plain JSONL over stdin/stdout. Both machines need `histo` on `PATH`.

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
files; export/import moves records already stored in Historious.

## Embeddings

Embeddings are off by default. That keeps first indexing quick and avoids model
downloads unless you ask for them.

Turn embeddings on for this data directory:

```bash
histo config embeddings on
```

Turn them back off:

```bash
histo config embeddings off
```

Check the current setting and config path:

```bash
histo config show
```

Use `--no-embeddings` on commands such as `update`, `import`, `search`, `tui`,
`daemon`, or `serve` for a one-off lexical-only run.

Linux musl release binaries are built without FastEmbed so they stay static and
portable. If you want Linux semantic embeddings in the binary itself, build from
source with Cargo and then run `histo config embeddings on`.

## Serve Mode

Most people can ignore this section.

Historious has a small unauthenticated HTTP server because the TUI talks to the
search engine through that API. `histo tui` starts a local server automatically,
but you can run one yourself:

```bash
histo serve
histo tui --server-url http://127.0.0.1:7391
```

Because the TUI accepts `--server-url`, you can also query a Historious server
running on another machine. Prefer an SSH tunnel:

```bash
ssh -L 7391:127.0.0.1:7391 <remote> 'histo serve'
histo tui --server-url http://127.0.0.1:7391
```

Direct LAN exposure is explicit because the server is unauthenticated:

```bash
ssh <remote> 'histo serve --bind 0.0.0.0:7391 --allow-network-bind'
histo tui --server-url http://<remote-ip>:7391
```

Do not expose the Historious HTTP server directly on a public interface.

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
