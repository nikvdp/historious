# Historious

Your coding agents have a past. Historious makes it searchable.

If you use Codex, Claude Code, OpenCode, pi, Oh My Pi (OMP), OpenClaw, Hermes, or
similar tools, your machine already has a quiet archive of useful work: commands
that worked, fixes that failed, decisions you made, error messages you chased,
and threads you would absolutely reuse if you could find them. Historious indexes
those local transcripts and gives you one command, `histo`, for searching them
again.

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
| Linux x86_64, modern glibc, FastEmbed-capable | `histo-linux-x86_64-gnu` |
| Linux ARM64, modern glibc, FastEmbed-capable | `histo-linux-aarch64-gnu` |
| Linux x86_64, portable fallback | `histo-linux-x86_64-musl` |
| Linux ARM64, portable fallback | `histo-linux-aarch64-musl` |
| Windows x86_64 | `histo-windows-x86_64.exe` |

Use the `gnu` Linux builds on modern glibc distros such as Ubuntu 24.04 or
newer; those builds include FastEmbed support. Runtime embeddings remain off
until you enable them. The portable `musl` builds do not include FastEmbed yet.

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

### Persistent maintenance

Install user-level scheduled maintenance after the first update:

```bash
histo service install
histo service status
```

This installs an hourly `histo update` and a daily `histo report --update` at
03:00 local time. Historious uses LaunchAgents on macOS and systemd user timers
on Linux; neither requires root access. Remove both jobs with:

```bash
histo service uninstall
```

Search for a concrete clue you remember:

```bash
histo search 429 reqwest
histo show <ref> --before 5 --after 8
histo transcript <session_id> --at <ref>
```

Human `show` and `transcript` output is readable Markdown by default. You
can redirect it directly to a `.md` file:

```bash
histo transcript <session_id> > conversation.md
histo show <ref> --only > answer.md
```

For scripts and agents, use `--robot` so output is stable JSON:

```bash
histo --robot status
histo --robot search Cargo.lock toml_edit --mode lexical --limit 20
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

Good agent behavior is simple: start with one distinctive literal, use
`--robot`, and add project, date, or machine filters before another keyword.
Group hits by `session_id`, then inspect promising refs with `show` or `transcript`.

## Browse Recent Threads

Use `threads` when you remember roughly when work happened, or which repo it
was in, but not the exact words from the conversation:

```bash
histo threads --all --today
histo threads --all --after "3 days ago"
histo threads --project /absolute/repo/path
```

## Search History

By default, `search` is lexical. Use words and symbols that actually appeared
in the transcript: error codes, command names, file paths, function names,
branch names, package names, ports, hosts, or log text.

```bash
histo search migration rollback --project /absolute/repo/path
histo search timeout --all --after 2026-06-01
histo search --match or rollback revert
histo search 429 reqwest --include-tools
histo search exact_function_name --mode lexical
```

### Filter by machine

Historious stores two independent machine fields:

- A machine ID is a stable UUID for one Historious installation.
- A machine name is a human-readable label, usually discovered from the host.

Changing a machine name does not change its ID. Two installations with the
same name still have different IDs.

Use `--hostname` (or `--host`) to match the machine name. Use `--machine` to
match the exact UUID:

```bash
histo threads --all --hostname <machine_name>
histo search sqlite --all --machine <machine_uuid>
```

Run `histo --robot status` on a machine to read its `machine_name` and
`machine_id`.

Multiple keywords use AND matching by default. Use `--match or` when any
keyword may match. Shell quotes only group arguments; they do not request
phrase matching. The older `--match all` and `--match any` spellings remain aliases.

Inspect results:

```bash
histo show <ref>
histo show <ref> --before 10 --after 10
histo show <ref> --only              # print only the selected clean item
histo transcript <session_id> --at <ref>
histo transcript <session_id> --last          # print the last clean item
histo transcript <session_id> --last-answer  # print the last assistant answer
histo transcript <session_id> --no-timestamps
histo tail <session_id>
```

Human output is Markdown: headings for each message, bullet-point metadata,
and fenced code blocks for raw JSON. Use `--only`, `--last`, or
`--last-answer` to print a single item to stdout — these bypass the pager and
are useful for exporting one reply to a file. `--last-assistant` is an alias
for `--last-answer`. Use `--no-timestamps` to omit timestamps from Markdown
headings for compact exports. Use `--full` to see raw event payloads instead
of clean conversation items.

## Find sessions behind code

Use `blame` to find indexed agent sessions connected to the current lines of a
tracked Git file. Build or refresh the commit-evidence index with `update`, then
select a file or an inclusive, 1-based line range:

```bash
histo update
histo blame src/main.rs
histo --robot blame src/main.rs --lines 1:20
```

Results group matching commits by session and include current line ranges,
original filenames, evidence strength, and exact transcript citations.
`--robot` and `--json` return the same result data. Follow-up commands preserve
the selected data directory and use `--full` to retrieve the actual tool events.

Exact recorded commit hashes take precedence. Complete messages from `-m` or
recoverable `-F` message-file writes support matching after hashes change.
Message similarity, subject-only matches, and unavailable historical directories
remain weaker candidates. Ambiguous matches, unmatched lines, and uncommitted
lines are reported separately.

This identifies recorded committing sessions, not every contributor who edited
the code. Message equality does not prove a rebase or squash relationship.
Opaque scripts, missing tool records, and missing message-file contents can
limit coverage. Matching is scoped to this installation's machine identity and
the selected repository or worktrees; identical paths on other machines are not
treated as repository proof.

`blame` is read-only and does not use embeddings or model APIs. It never scans
native logs or rebuilds an index during lookup. Missing or incompatible evidence
snapshots require an explicit `histo update`; stale compatible snapshots remain
readable with a warning. The first update backfills stored history, and later
updates maintain changed sessions.

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
histo search 429 reqwest --fzf
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

Current exports preserve both the source machine UUID and its name. Import
keeps that identity instead of assigning the receiving machine's identity.
Reimporting the same session with corrected machine metadata updates the
existing session and its derived records.

### Repair machine identity from an older import

An archive imported by an older Historious version might contain
`machine_unknown_host` or another unresolved legacy ID. The receiving machine
cannot safely determine which source owns those sessions.

Repair the identity from each source machine:

1. Upgrade Historious on the source machine.
2. Run `histo update` on the source machine to assign its stable UUID to its
   local sessions.
3. Export from the source and import the corrected archive into the receiving
   machine:

   ```bash
   ssh <remote> 'histo update'
   ssh <remote> 'histo export --jsonl' \
     | histo import --jsonl --json -
   ```

The corrected reimport repairs matching sessions in place. You don't need to
delete the receiving database.

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
histo export --jsonl --source omp
histo export --jsonl --workspace /absolute/repo/path
histo export --jsonl --session <session_id>
histo export --jsonl --since 2026-06-01
```

Do not run `histo update` on the receiving machine to repair imported identity.
`update` scans local agent log files; it cannot infer ownership for records
that came from another machine.

## Optional Semantic Search

Embeddings are off by default, and normal search remains lexical. This keeps
first indexing quick and avoids model downloads unless you ask for them.

Turn embeddings on for this data directory:

```bash
histo config embeddings on
```

Then run `histo update` so Historious can index embedding vectors into its
database. Select semantic mode for natural-language intent, or hybrid mode to
combine vector and lexical results:

```bash
histo update
histo search "why did the sync loop repeat" --mode semantic
histo search retry timeout --mode hybrid
```

Turn them back off:

```bash
histo config embeddings off
```

Check the current setting and config path:

```bash
histo config show
```

Use `--embeddings` or `-e` to enable embeddings for one command when config has
them off. Pair it with `--mode hybrid` or `--mode semantic` for vector results.
Use `--no-embeddings --mode lexical` for a search that neither loads nor uses
embeddings.

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
