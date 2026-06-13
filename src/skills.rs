use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub struct PackagedSkill {
    pub name: &'static str,
    pub description: &'static str,
    pub skill_md: &'static str,
}

const SEARCH_AGENT_HISTORY_HISTORIOUS: &str = r#"---
name: search-agent-history-historious
description: Search coding-agent conversation history with Historious across Codex, Claude Code, OpenCode, pi, OpenClaw, Hermes, and other indexed local agent logs.
---

# Search Agent History With Historious

Use this skill when the user asks what happened in a prior coding-agent session, wants to find a session ID, remembers a partial implementation detail, or asks to search across coding agents.

## Core Rules

- Prefer `histo --robot` for agent usage. It emits stable JSON envelopes and disables interactive behavior.
- Start with broad search, then inspect the best refs with `show` or `transcript`.
- Group candidates by `session_id`; do not treat every matching event as a separate conversation.
- Use transcript JSON for exact content. Do not parse human transcript markers when JSON is available.
- For exact commands, secrets, paths, branch names, or final workflow recovery, verify against exact transcript content before answering.
- Redact secrets found in transcripts.

## Health Check

```bash
histo --robot status
```

Useful fields:

- `.success`
- `.data.stats.sessions`
- `.data.stats.events`
- `.data.stats.search_units`
- `.data.stats.embeddings`
- `.data.query_embedder.semantic`
- `.data.query_embedder.degraded_reason`

If the archive looks stale or empty, run:

```bash
histo --robot update
```

## Search

```bash
histo --robot search "distinctive query terms" --limit 20
```

Each result includes:

- `ref`: short recent-result ref for follow-up commands
- `event_id`: full canonical event id
- `session_id`: full canonical session id
- `source_kind`: agent/source such as `codex` or `claude_code`
- `match_type`: `lexical`, `semantic`, or `hybrid`
- `snippet`: indexed text near the hit
- ranks and score fields for debugging result quality

Prefer refs for interactive follow-up:

```bash
histo --robot show <ref>
histo --robot transcript <session_id> --at <ref>
```

## Inspect A Result

Use `show` for nearby context:

```bash
histo --robot show <ref> --before 5 --after 8
```

The JSON payload has:

- `.data.before[]`
- `.data.target`
- `.data.after[]`

Use `transcript` for the full conversation:

```bash
histo --robot transcript <session_id> --at <ref>
```

The JSON payload has ordered `.data.events[]` with exact content, roles, event ids, metadata, and target index.

## History Exchange

Use JSONL export/import over stdin/stdout to exchange history with another machine. This does not require daemon setup or remote query support.

Pull remote history into the local machine:

```bash
ssh <remote> 'histo export --jsonl [filters]' \
  | histo import --jsonl --json -
```

Push local history to a remote machine:

```bash
histo export --jsonl [filters] \
  | ssh <remote> 'histo import --jsonl --json -'
```

Reciprocal exchange is just both directions:

```bash
ssh <remote> 'histo export --jsonl [filters]' \
  | histo import --jsonl --json -

histo export --jsonl [filters] \
  | ssh <remote> 'histo import --jsonl --json -'
```

Useful filters:

```bash
histo export --jsonl --source codex
histo export --jsonl --workspace /absolute/repo/path
histo export --jsonl --session <session_id>
histo export --jsonl --since 2026-06-01
```

## Remote TUI

Use `--server-url <url>` when an already-running Historious server should back
the interactive TUI end to end. Keep the server running in one terminal:

```bash
histo serve
```

Then connect from another terminal:

```bash
histo tui --server-url http://127.0.0.1:7391
```

For another machine, prefer an SSH tunnel instead of exposing the unauthenticated
HTTP server directly. Keep this running in one terminal:

```bash
ssh -L 7391:127.0.0.1:7391 <remote> 'histo serve'
```

Then connect the local TUI through the tunnel:

```bash
histo tui --server-url http://127.0.0.1:7391
```

Direct LAN exposure is explicit because the HTTP server is unauthenticated. Keep
this running on the remote:

```bash
ssh <remote> 'histo serve --bind 0.0.0.0:7391 --allow-network-bind'
```

Then connect to the remote address:

```bash
histo tui --server-url http://<remote-ip>:7391
```

### Embedding Mode

Historious can run with embeddings enabled or disabled. Embeddings are enabled by default.

Persistently disable embeddings for this data directory:

```bash
histo config embeddings off
```

Re-enable them:

```bash
histo config embeddings on
```

Inspect the current setting and config file path:

```bash
histo config show
```

Use `--no-embeddings` on commands such as `update`, `import`, `search`, `tui`, `daemon`, or `serve` for a one-off lexical-only run without changing `config.toml`. This can speed up maintenance or sync work when semantic search is not needed right away; a later embedding-enabled `histo update` can backfill skipped embeddings.

### Embedding Transfer

When embeddings are enabled, exports include existing embedding records by default. Imports store those embeddings and refresh the local vector index, so the receiving machine can use compatible transferred embeddings without recreating them.

Round-trip through an embedding-capable host:

```bash
histo export --jsonl [filters] \
  | ssh <embedding-host> 'histo import --jsonl --json -'

ssh <embedding-host> 'histo export --jsonl [filters]' \
  | histo import --jsonl --json -
```

Use `--embeddings omit` when bandwidth or storage matters:

```bash
histo export --jsonl --embeddings omit [filters] \
  | ssh <remote> 'histo import --jsonl --json -'
```

Do not add `histo update` to these exchange flows. `update` scans local agent log files and is a separate maintenance concern.

## Search Strategy

- Start with 3-8 strong terms from the user's memory.
- Include exact anchors when known: file paths, function names, branch names, ticket IDs, commands, errors, ports, hosts, or model names.
- If results are noisy, add one anchor at a time.
- If results are sparse, remove exact path fragments and search for concept words.
- For timeline-style questions, search terms from the topic and then group by `session_id` and timestamps.

## Answer Shape

For session-finding requests, return:

```text
Best match: <session_id>
Source: <source_kind>
Time: <occurred_at or session timestamp>
Why: <1-2 matching anchors>
Useful refs: <ref list>
```

For workflow recovery, summarize the recovered recipe and cite the session id/ref used. Do not dump full transcripts unless the user explicitly asks.
"#;

const AGENTS_MD: &str = r#"## Historious Agent History Search

Use `histo` for cross-agent coding-history search.

Preferred agent pattern:

```bash
histo --robot status
histo --robot search "distinctive query terms" --limit 20
histo --robot show <ref> --before 5 --after 8
histo --robot transcript <session_id> --at <ref>
```

Rules:

- Use `--robot` for machine-friendly JSON and structured errors.
- Group search hits by `session_id`.
- Use returned `ref` values for `show` and `transcript` follow-ups.
- Use `transcript` JSON when exact wording, commands, or file paths matter.
- Redact secrets found in transcripts.
- Do not add a separate search command; the canonical entry point is `histo search`.

History exchange:

```bash
# Pull remote history into local. Existing embeddings transfer when enabled.
ssh <remote> 'histo export --jsonl [filters]' \
  | histo import --jsonl --json -

# Push local history to remote. Existing embeddings transfer when enabled.
histo export --jsonl [filters] \
  | ssh <remote> 'histo import --jsonl --json -'

# Omit embeddings when bandwidth or storage is constrained.
histo export --jsonl --embeddings omit [filters] \
  | ssh <remote> 'histo import --jsonl --json -'

# Round-trip through an embedding-capable host.
histo export --jsonl [filters] \
  | ssh <embedding-host> 'histo import --jsonl --json -'

ssh <embedding-host> 'histo export --jsonl [filters]' \
  | histo import --jsonl --json -
```

Remote TUI:

```bash
# Terminal 1: keep the server bound to loopback on the remote host and forward it locally.
ssh -L 7391:127.0.0.1:7391 <remote> 'histo serve'

# Terminal 2: use the forwarded server for search, preview, and Enter.
histo tui --server-url http://127.0.0.1:7391

# Direct LAN exposure is explicit because the HTTP server is unauthenticated.
# Terminal 1:
ssh <remote> 'histo serve --bind 0.0.0.0:7391 --allow-network-bind'

# Terminal 2:
histo tui --server-url http://<remote-ip>:7391
```

Exchange rules:

- Export includes existing embeddings by default when embeddings are enabled.
- Import stores transferred embeddings and refreshes the local vector index when embeddings are enabled.
- Use `--embeddings omit` for bandwidth or storage constrained exchanges.
- Use an embedding-capable machine by piping history to it, then exporting back from it.
- Do not include `histo update` in exchange flows; local log scanning is separate.

Packaged skill:

```bash
histo skill emit search-agent-history-historious
histo skill install search-agent-history-historious --codex
```
"#;

const SKILLS: &[PackagedSkill] = &[PackagedSkill {
    name: "search-agent-history-historious",
    description: "Search coding-agent conversation history through Historious robot JSON.",
    skill_md: SEARCH_AGENT_HISTORY_HISTORIOUS,
}];

pub fn list_skills() -> &'static [PackagedSkill] {
    SKILLS
}

pub fn get_skill(name: &str) -> Option<&'static PackagedSkill> {
    SKILLS.iter().find(|skill| skill.name == name)
}

pub fn onboard_agents_md() -> &'static str {
    AGENTS_MD
}

pub fn onboard_wrapper() -> String {
    format!(
        "Add this to AGENTS.md, CLAUDE.md, or the equivalent agent instruction file:\n\n{AGENTS_MD}"
    )
}

pub fn install_skill(skill: &PackagedSkill, root: &Path) -> Result<PathBuf> {
    let dir = root.join(skill.name);
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = dir.join("SKILL.md");
    fs::write(&path, skill.skill_md).with_context(|| format!("writing {}", path.display()))?;
    Ok(dir)
}

pub fn skill_names_for_arg(name: &str) -> Result<Vec<&'static str>> {
    if name == "all" {
        return Ok(SKILLS.iter().map(|skill| skill.name).collect());
    }
    if get_skill(name).is_none() {
        bail!("unknown packaged skill: {name}");
    }
    Ok(vec![get_skill(name).expect("checked").name])
}
