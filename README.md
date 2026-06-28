# Historious

Your coding agents have a past. Historious makes it searchable.

If you use Codex, Claude Code, OpenCode, pi, OpenClaw, Hermes, or similar tools,
your machine already has a quiet archive of useful work: commands that worked,
fixes that failed, decisions you made, error messages you chased, and threads you
would absolutely reuse if you could find them. Historious indexes those local
transcripts and gives you one command, `histo`, for searching them again.

The point is agent memory without a hosted memory service. Search across
projects, machines, and sessions; recover the exact thread where something
happened; then hand that context to your next agent so it does not have to learn
the same lesson twice.

Short version for the impatient:

> Install Historious from `nikvdp/historious`, put `histo` on `PATH`, run
> `histo update`, then run `histo onboard --agents-md` or install the packaged
> Historious skill for this agent. Use `histo --robot` for agent searches.

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
| Linux x86_64, modern glibc with embeddings | `histo-linux-x86_64-gnu` |
| Linux ARM64, modern glibc with embeddings | `histo-linux-aarch64-gnu` |
| Linux x86_64, portable fallback | `histo-linux-x86_64-musl` |
| Linux ARM64, portable fallback | `histo-linux-aarch64-musl` |
| Windows x86_64 | `histo-windows-x86_64.exe` |

Use the `gnu` Linux builds on modern glibc distros such as Ubuntu 24.04 or
newer; those builds include FastEmbed support. Use the `musl` builds on older
Linux distros or Alpine; those portable builds do not include FastEmbed yet.

Or build from source:

```bash
cargo install --git https://github.com/nikvdp/historious historious --locked
```

Make sure `~/.local/bin` is on your `PATH` if you used the binary install.

Update an installed release binary later with:

```bash
histo self-update
```

Use `histo self-update --check` to only check for a newer GitHub release.

## First Run

Index your local history:

```bash
histo update
```

The first run can take a little while, especially if you have a lot of old agent
chat sessions. Later updates only need to catch up with new history.

Check what Historious found:

```bash
histo status
```

Search for a concrete clue you remember:

```bash
histo search "429 reqwest retry"
histo show <ref> --before 5 --after 8
histo transcript <session_id> --at <ref>
```

For scripts and agents, use `--robot` so output is stable JSON:

```bash
histo --robot status
histo --robot search "Cargo.lock toml_edit" --limit 20
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

## Browse Recent Threads

Use `threads` when you remember roughly when work happened, or which repo it
was in, but not the exact words from the conversation:

```bash
histo threads --all --today
histo threads --all --after "3 days ago"
histo threads --project /absolute/repo/path
```

## Search History

By default, `search` is mainly lexical. Use words and symbols that actually
appeared in the transcript: error codes, command names, file paths, function
names, branch names, package names, ports, hosts, or log text.

```bash
histo search "migration rollback sqlite" --project /absolute/repo/path
histo search dog parade
histo search --match or dog parade
histo search "rate limit 429" --all --after 2026-06-01
histo search "cargo zigbuild release" --include-tools
histo search "exact_function_name" --mode lexical
```

Multiple unquoted query terms match with AND behavior by default. Use
`--match or` when any term may match. The older `--match all` and `--match any`
spellings still work as aliases.

Inspect results:

```bash
histo show <ref>
histo show <ref> --before 10 --after 10
histo transcript <session_id> --at <ref>
histo tail <session_id>
```

## Local TUI

`histo tui` is a local terminal UI built on `fzf`. If you already have `fzf` on
your `PATH`, you are set. If not, install it first:

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
  | histo import --jsonl --json -
```

Push local history to a remote machine:

```bash
histo export --jsonl \
  | ssh <remote> 'histo import --jsonl --json -'
```

Omit embeddings when bandwidth or storage is constrained:

```bash
histo export --jsonl --embeddings omit \
  | ssh <remote> 'histo import --jsonl --json -'
```

Control raw artifact transfer with `--raw-artifacts inline|metadata|omit`
(or the alias `--no-raw-artifacts`):

```bash
histo export --jsonl --raw-artifacts metadata
histo export --jsonl --no-raw-artifacts
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

Then run `histo update` so Historious can index embedding vectors into its
database. After that, semantic search can help with fuzzy, concept-shaped
queries:

```bash
histo update
histo search "why did the sync loop repeat" --mode semantic
```

Turn them back off:

```bash
histo config embeddings off
```

Check the current setting and config path:

```bash
histo config show
```

Use `--embeddings` or `-e` on commands such as `update`, `import`, `search`,
`tui`, `daemon`, or `serve` to force embeddings on for a single run even when
config has them off. Use `--no-embeddings` or `-E` for a one-off lexical-only
run.

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
