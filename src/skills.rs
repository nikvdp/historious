use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub struct PackagedSkill {
    pub name: &'static str,
    pub description: &'static str,
    pub skill_md: &'static str,
}

const SEARCH_AGENT_HISTORY_SUPER_CASS: &str = r#"---
name: search-agent-history-super-cass
description: Search coding-agent conversation history with super-cass across Codex, Claude Code, OpenCode, pi, OpenClaw, Hermes, and other indexed local agent logs.
---

# Search Agent History With super-cass

Use this skill when the user asks what happened in a prior coding-agent session, wants to find a session ID, remembers a partial implementation detail, or asks to search across coding agents.

## Core Rules

- Prefer `super-cass --robot` for agent usage. It emits stable JSON envelopes and disables interactive behavior.
- Start with broad search, then inspect the best refs with `show` or `transcript`.
- Group candidates by `session_id`; do not treat every matching event as a separate conversation.
- Use transcript JSON for exact content. Do not parse human transcript markers when JSON is available.
- For exact commands, secrets, paths, branch names, or final workflow recovery, verify against exact transcript content before answering.
- Redact secrets found in transcripts.

## Health Check

```bash
super-cass --robot status
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
super-cass --robot update
```

## Search

```bash
super-cass --robot search "distinctive query terms" --limit 20
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
super-cass --robot show <ref>
super-cass --robot transcript <session_id> --at <ref>
```

## Inspect A Result

Use `show` for nearby context:

```bash
super-cass --robot show <ref> --before 5 --after 8
```

The JSON payload has:

- `.data.before[]`
- `.data.target`
- `.data.after[]`

Use `transcript` for the full conversation:

```bash
super-cass --robot transcript <session_id> --at <ref>
```

The JSON payload has ordered `.data.events[]` with exact content, roles, event ids, metadata, and target index.

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

const AGENTS_MD: &str = r#"## super-cass Agent History Search

Use `super-cass` for cross-agent coding-history search.

Preferred agent pattern:

```bash
super-cass --robot status
super-cass --robot search "distinctive query terms" --limit 20
super-cass --robot show <ref> --before 5 --after 8
super-cass --robot transcript <session_id> --at <ref>
```

Rules:

- Use `--robot` for machine-friendly JSON and structured errors.
- Group search hits by `session_id`.
- Use returned `ref` values for `show` and `transcript` follow-ups.
- Use `transcript` JSON when exact wording, commands, or file paths matter.
- Redact secrets found in transcripts.
- Do not add a separate search command; the canonical entry point is `super-cass search`.

Packaged skill:

```bash
super-cass skill emit search-agent-history-super-cass
super-cass skill install search-agent-history-super-cass --codex
```
"#;

const SKILLS: &[PackagedSkill] = &[PackagedSkill {
    name: "search-agent-history-super-cass",
    description: "Search coding-agent conversation history through super-cass robot JSON.",
    skill_md: SEARCH_AGENT_HISTORY_SUPER_CASS,
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
