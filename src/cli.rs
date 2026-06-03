use crate::config::AppConfig;
use crate::ingest;
use crate::search;
use crate::server;
use crate::storage::{RecentResultRefInput, Store};
use crate::transport;
use anyhow::{bail, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
#[command(name = "super-cass")]
#[command(about = "Search and sync local coding-agent transcripts")]
pub struct Cli {
    #[arg(
        long,
        env = "SUPER_CASS_DATA_DIR",
        help = "Use a custom super-cass data directory"
    )]
    pub data_dir: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Use machine-friendly JSON output and noninteractive behavior for supported commands"
    )]
    pub robot: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Scan local agent logs and update the search index.
    Update {
        #[arg(long, help = "Scan at most this many newest files")]
        max_files: Option<usize>,
        #[arg(long, help = "Scan only one source kind, such as codex or claude_code")]
        source: Option<String>,
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
    },
    /// Search indexed transcripts.
    Search {
        #[arg(help = "Words, paths, errors, or other details to search for")]
        query: String,
        #[arg(
            short,
            long,
            default_value_t = 10,
            help = "Maximum number of results to show"
        )]
        limit: usize,
        #[arg(long, help = "Print structured JSON with refs for follow-up commands")]
        json: bool,
        #[arg(long, help = "Show scores and full ids in table output")]
        verbose: bool,
        #[arg(long, help = "Show exactly these columns, comma-separated")]
        cols: Option<String>,
        #[arg(
            long,
            help = "Add these columns to the default output, comma-separated"
        )]
        include: Option<String>,
        #[arg(
            long,
            help = "Remove these columns from the default output, comma-separated"
        )]
        exclude: Option<String>,
        #[arg(long, value_enum, default_value_t = SearchSort::Relevance, help = "Sort results by relevance or time")]
        sort: SearchSort,
        #[arg(
            long,
            default_value_t = 0.0,
            help = "Favor newer results, from 0.0 to 1.0"
        )]
        recency_bias: f64,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Browse results interactively with fzf")]
        fzf: bool,
    },
    /// Show nearby transcript context for a search result.
    Show {
        #[arg(
            value_name = "REF_OR_EVENT_ID",
            help = "Recent search ref or full event id"
        )]
        target: Option<String>,
        #[arg(
            long,
            conflicts_with = "search_unit",
            help = "Full event id or recent ref"
        )]
        event: Option<String>,
        #[arg(
            long = "search-unit",
            conflicts_with = "event",
            help = "Search unit id"
        )]
        search_unit: Option<String>,
        #[arg(long, default_value_t = 3, help = "Number of earlier events to show")]
        before: usize,
        #[arg(long, default_value_t = 5, help = "Number of later events to show")]
        after: usize,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
        #[arg(long, help = "Print structured JSON with exact event content")]
        json: bool,
    },
    /// Deprecated alias for `show`.
    #[command(hide = true)]
    Expand {
        #[arg(
            value_name = "REF_OR_EVENT_ID",
            help = "Recent search ref or full event id"
        )]
        target: Option<String>,
        #[arg(
            long,
            conflicts_with = "search_unit",
            help = "Full event id or recent ref"
        )]
        event: Option<String>,
        #[arg(
            long = "search-unit",
            conflicts_with = "event",
            help = "Search unit id"
        )]
        search_unit: Option<String>,
        #[arg(long, default_value_t = 3, help = "Number of earlier events to show")]
        before: usize,
        #[arg(long, default_value_t = 5, help = "Number of later events to show")]
        after: usize,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
        #[arg(long, help = "Print structured JSON with exact event content")]
        json: bool,
    },
    /// Show a full conversation transcript.
    Transcript {
        #[arg(
            value_name = "SESSION_OR_REF",
            help = "Session id, recent search ref, or full event id"
        )]
        target: String,
        #[arg(
            long,
            conflicts_with = "search_unit",
            help = "Recent search ref or full event id to jump to"
        )]
        at: Option<String>,
        #[arg(
            long = "search-unit",
            conflicts_with = "at",
            help = "Search unit id to jump to"
        )]
        search_unit: Option<String>,
        #[arg(long, help = "Print directly instead of opening a pager")]
        no_pager: bool,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
        #[arg(long, help = "Print structured JSON with exact event content")]
        json: bool,
    },
    /// Write history records to JSONL for backup or transfer.
    Export {
        #[arg(long, help = "Write newline-delimited JSON records")]
        jsonl: bool,
        #[arg(long, help = "Omit embedding records from the JSONL history stream")]
        no_embeddings: bool,
        #[arg(
            long,
            help = "Export only one source kind, such as codex or claude_code"
        )]
        source: Vec<String>,
        #[arg(long, help = "Export sessions from this workspace path")]
        workspace: Vec<std::path::PathBuf>,
        #[arg(long, help = "Export this session id")]
        session: Vec<String>,
        #[arg(
            long,
            help = "Export sessions since an RFC3339 timestamp or YYYY-MM-DD"
        )]
        since: Option<String>,
    },
    /// Read history records from JSONL.
    Import {
        #[arg(long, help = "Read newline-delimited JSON records")]
        jsonl: bool,
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(default_value = "-", help = "Input file, or '-' for stdin")]
        input: String,
    },
    /// Keep local history and search up to date.
    Daemon {
        #[arg(long, default_value_t = 30, help = "Seconds between scans")]
        interval_secs: u64,
        #[arg(long, help = "Scan at most this many newest files each pass")]
        max_files: Option<usize>,
        #[arg(long, help = "Scan only one source kind, such as codex or claude_code")]
        source: Option<String>,
    },
    /// Serve local history over HTTP and keep it up to date.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7391", help = "Address to listen on")]
        bind: String,
        #[arg(long, default_value_t = 30, help = "Seconds between scans")]
        interval_secs: u64,
        #[arg(long, help = "Scan at most this many newest files each pass")]
        max_files: Option<usize>,
        #[arg(long, help = "Scan only one source kind, such as codex or claude_code")]
        source: Option<String>,
    },
    /// Show local history and search health.
    Status {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
    },
    /// Output agent instructions for super-cass.
    Onboard {
        #[arg(long, help = "Emit only the AGENTS.md-ready block")]
        agents_md: bool,
    },
    /// List, emit, and install packaged super-cass skills.
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum SkillCommand {
    /// List packaged skills embedded in super-cass.
    List,
    /// Print packaged skill content.
    Emit {
        #[arg(help = "Skill name or 'all'")]
        name: String,
    },
    /// Install packaged skill content to a skills directory.
    Install {
        #[arg(default_value = "all", help = "Skill name or 'all'")]
        name: String,
        #[arg(long, help = "Skills directory root containing skill folders")]
        dir: Option<PathBuf>,
        #[arg(long, help = "Install to ~/.claude/skills")]
        claude: bool,
        #[arg(long, help = "Install to ~/.codex/skills")]
        codex: bool,
        #[arg(long, help = "Install to ~/.pi/agent/skills")]
        pi: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SearchSort {
    Relevance,
    Newest,
    Oldest,
}

impl From<SearchSort> for search::SortMode {
    fn from(value: SearchSort) -> Self {
        match value {
            SearchSort::Relevance => search::SortMode::Relevance,
            SearchSort::Newest => search::SortMode::Newest,
            SearchSort::Oldest => search::SortMode::Oldest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    Ref,
    Source,
    Match,
    When,
    Title,
    Preview,
    Score,
    Lex,
    Sem,
    Event,
    Session,
}

impl Cli {
    pub fn command_name(&self) -> &'static str {
        self.command.name()
    }

    pub fn wants_structured_errors(&self) -> bool {
        self.robot || self.command.wants_structured_errors()
    }

    pub async fn run(self) -> Result<()> {
        let robot = self.robot;
        let config = AppConfig::load(self.data_dir)?;
        let store = Store::open(&config.data_dir)?;
        match self.command {
            Command::Update {
                max_files,
                source,
                json,
            } => {
                if json || robot {
                    let output = run_update_once(&store, &config, max_files, source)?;
                    crate::output::write_success("update", output, Default::default())?;
                } else {
                    let output = run_update_once_human(&store, &config, max_files, source)?;
                    print_update_output(&output, std::io::stdout().is_terminal());
                }
            }
            Command::Search {
                query,
                limit,
                json,
                verbose,
                cols,
                include,
                exclude,
                sort,
                recency_bias,
                no_color,
                fzf,
            } => {
                if robot && fzf {
                    bail!("--robot cannot be combined with --fzf");
                }
                let (embedder, degraded_reason) = load_embedder(&config);
                let options = search::SearchOptions::new(limit, sort.into(), recency_bias);
                let response = search::search(
                    &store,
                    &query,
                    options,
                    embedder.as_deref(),
                    degraded_reason,
                )?;
                if json || robot {
                    let refs =
                        store.record_recent_result_refs(&recent_ref_inputs(&response.results))?;
                    let output = search_output(&query, limit, sort, recency_bias, &response, &refs);
                    crate::output::write_success(
                        "search",
                        output,
                        crate::output::EnvelopeOptions {
                            degraded_reason: response.degraded_reason.clone(),
                            hints: search_hints(&response.results, &refs),
                            ..Default::default()
                        },
                    )?;
                } else if fzf {
                    if let Some(reason) = &response.degraded_reason {
                        eprintln!("search degraded: {reason}");
                    }
                    let refs =
                        store.record_recent_result_refs(&recent_ref_inputs(&response.results))?;
                    let color = !no_color;
                    run_fzf_search(&config, &query, &response.results, &refs, color)?;
                } else {
                    if let Some(reason) = &response.degraded_reason {
                        eprintln!("search degraded: {reason}");
                    }
                    let refs =
                        store.record_recent_result_refs(&recent_ref_inputs(&response.results))?;
                    let columns = resolve_columns(verbose, cols, include, exclude)?;
                    let color = !no_color && !robot && std::io::stdout().is_terminal();
                    print_search_results(&query, &response.results, &refs, &columns, color);
                }
            }
            Command::Show {
                target,
                event,
                search_unit,
                before,
                after,
                no_color,
                verbose,
                json,
            }
            | Command::Expand {
                target,
                event,
                search_unit,
                before,
                after,
                no_color,
                verbose,
                json,
            } => {
                let event_id = resolve_context_event_id(&store, target, event, search_unit)?;
                let context = store
                    .events_around_event(&event_id, before, after)?
                    .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                if json || robot {
                    crate::output::write_success(
                        "show",
                        show_output(&store, &context)?,
                        crate::output::EnvelopeOptions {
                            hints: vec![format!(
                                "super-cass transcript {} --at {} --json",
                                context.session.id, context.target_event.id
                            )],
                            ..Default::default()
                        },
                    )?;
                } else {
                    let metadata = view_metadata_for_event(&store, &context.target_event, verbose)?;
                    let color = !no_color && !robot && std::io::stdout().is_terminal();
                    write_stdout(&crate::transcript::render_context(
                        &context, &metadata, color,
                    ))?;
                }
            }
            Command::Transcript {
                target,
                at,
                search_unit,
                no_pager,
                no_color,
                verbose,
                json,
            } => {
                let (session, target_event_id) =
                    resolve_transcript_target(&store, &target, at, search_unit)?;
                let session_record = store
                    .session_by_id(&session)?
                    .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?;
                let events = store.events_for_session(&session)?;
                let target_event = target_event_id
                    .as_deref()
                    .map(|event_id| {
                        store
                            .event_by_id(event_id)?
                            .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))
                    })
                    .transpose()?;
                if json || robot {
                    crate::output::write_success(
                        "transcript",
                        transcript_output(
                            &store,
                            &session_record,
                            &events,
                            target_event_id.as_deref(),
                        )?,
                        Default::default(),
                    )?;
                } else {
                    let metadata = view_metadata_for_session(
                        &store,
                        &session_record,
                        target_event.as_ref(),
                        verbose,
                    )?;
                    let color = !no_color && !robot && std::io::stdout().is_terminal();
                    let rendered = crate::transcript::render_session(
                        &session_record,
                        &events,
                        target_event_id.as_deref(),
                        &metadata,
                        color,
                    );
                    page_or_print(&rendered, target_event_id.as_deref(), no_pager || robot)?;
                }
            }
            Command::Export {
                jsonl,
                no_embeddings,
                source,
                workspace,
                session,
                since,
            } => {
                if jsonl {
                    let stdout = std::io::stdout();
                    let filter = crate::storage::ArchiveExportFilter {
                        sources: source,
                        workspaces: workspace
                            .iter()
                            .map(|path| transport::normalize_workspace_arg(path))
                            .collect(),
                        sessions: session,
                        since: transport::parse_since_arg(since.as_deref())?,
                    };
                    transport::export_jsonl_filtered_with_options(
                        &store,
                        &filter,
                        transport::ExportOptions {
                            include_embeddings: !no_embeddings,
                        },
                        stdout.lock(),
                    )?;
                } else {
                    anyhow::bail!("only --jsonl export is supported in v0");
                }
            }
            Command::Import { jsonl, json, input } => {
                if jsonl {
                    if json || robot {
                        let output = run_import_once(&store, &config, &input)?;
                        crate::output::write_success("import", output, Default::default())?;
                    } else {
                        let output = run_import_once_human(&store, &config, &input)?;
                        print_import_output(&output, std::io::stdout().is_terminal());
                    }
                } else {
                    anyhow::bail!("only --jsonl import is supported in v0");
                }
            }
            Command::Daemon {
                interval_secs,
                max_files,
                source,
            } => {
                run_daemon(
                    &store,
                    &config.machine_id,
                    config.embedder.clone(),
                    interval_secs,
                    max_files,
                    source,
                )
                .await?;
            }
            Command::Serve {
                bind,
                interval_secs,
                max_files,
                source,
            } => {
                let addr = bind.parse()?;
                let server_store = store.clone();
                let server_machine_id = config.machine_id.clone();
                let server_embedder = config.embedder.clone();
                let server_task = tokio::spawn(async move {
                    server::serve(server_store, addr, server_machine_id, server_embedder).await
                });
                run_daemon(
                    &store,
                    &config.machine_id,
                    config.embedder.clone(),
                    interval_secs,
                    max_files,
                    source,
                )
                .await?;
                server_task.abort();
            }
            Command::Status { json } => {
                let output = status_output(&store, &config)?;
                if json || robot {
                    crate::output::write_success("status", output, Default::default())?;
                } else {
                    print_status_output(&output);
                }
            }
            Command::Onboard { agents_md } => {
                if agents_md {
                    write_stdout(crate::skills::onboard_agents_md())?;
                } else {
                    write_stdout(&crate::skills::onboard_wrapper())?;
                }
            }
            Command::Skill { command } => run_skill_command(command)?,
        }
        Ok(())
    }
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Command::Update { .. } => "update",
            Command::Search { .. } => "search",
            Command::Show { .. } => "show",
            Command::Expand { .. } => "expand",
            Command::Transcript { .. } => "transcript",
            Command::Export { .. } => "export",
            Command::Import { .. } => "import",
            Command::Daemon { .. } => "daemon",
            Command::Serve { .. } => "serve",
            Command::Status { .. } => "status",
            Command::Onboard { .. } => "onboard",
            Command::Skill { .. } => "skill",
        }
    }

    fn wants_structured_errors(&self) -> bool {
        matches!(
            self,
            Command::Update { json: true, .. }
                | Command::Search { json: true, .. }
                | Command::Show { json: true, .. }
                | Command::Expand { json: true, .. }
                | Command::Transcript { json: true, .. }
                | Command::Import { json: true, .. }
                | Command::Status { json: true, .. }
        )
    }
}

fn run_skill_command(command: SkillCommand) -> Result<()> {
    match command {
        SkillCommand::List => {
            println!("Packaged skills:");
            for skill in crate::skills::list_skills() {
                println!("- {}: {}", skill.name, skill.description);
            }
            println!();
            println!("Install targets:");
            println!("- --claude -> ~/.claude/skills");
            println!("- --codex  -> ~/.codex/skills");
            println!("- --pi     -> ~/.pi/agent/skills");
        }
        SkillCommand::Emit { name } => {
            let names = crate::skills::skill_names_for_arg(&name)?;
            for (idx, skill_name) in names.iter().enumerate() {
                let skill = crate::skills::get_skill(skill_name).expect("known skill");
                if names.len() > 1 {
                    if idx > 0 {
                        println!();
                    }
                    println!("### {}", skill.name);
                }
                write_stdout(skill.skill_md)?;
            }
        }
        SkillCommand::Install {
            name,
            dir,
            claude,
            codex,
            pi,
        } => {
            let roots = install_roots(dir, claude, codex, pi)?;
            let names = crate::skills::skill_names_for_arg(&name)?;
            for root in roots {
                println!("Installing to {}", root.display());
                for skill_name in &names {
                    let skill = crate::skills::get_skill(skill_name).expect("known skill");
                    let installed = crate::skills::install_skill(skill, &root)?;
                    println!("Installed {} -> {}", skill.name, installed.display());
                }
            }
        }
    }
    Ok(())
}

fn install_roots(
    dir: Option<PathBuf>,
    claude: bool,
    codex: bool,
    pi: bool,
) -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    if let Some(dir) = dir {
        roots.push(dir);
    }
    if claude {
        roots.push(home_path(".claude/skills")?);
    }
    if codex {
        roots.push(home_path(".codex/skills")?);
    }
    if pi {
        roots.push(home_path(".pi/agent/skills")?);
    }
    roots.sort();
    roots.dedup();
    if roots.is_empty() {
        bail!("specify at least one install target: --dir, --claude, --codex, or --pi");
    }
    Ok(roots)
}

fn home_path(relative: &str) -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(relative))
}

#[derive(Debug, Serialize)]
struct UpdateOutput {
    ingest: ingest::UpdateStats,
    search_index: SearchIndexOutput,
    embeddings: search::EmbeddingRefresh,
}

#[derive(Debug, Serialize)]
struct ImportOutput {
    import: crate::storage::ImportStats,
    search_index: SearchIndexOutput,
    embeddings: search::EmbeddingRefresh,
}

#[derive(Debug, Serialize)]
struct SearchIndexOutput {
    indexed_events: usize,
}

#[derive(Debug, Serialize)]
struct SearchOutput {
    query: String,
    options: SearchOptionsOutput,
    degraded_reason: Option<String>,
    results: Vec<SearchResultOutput>,
    next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SearchOptionsOutput {
    limit: usize,
    sort: &'static str,
    recency_bias: f64,
}

#[derive(Debug, Serialize)]
struct SearchResultOutput {
    #[serde(rename = "ref")]
    ref_id: String,
    match_type: search::MatchType,
    event_id: String,
    session_id: String,
    source_kind: String,
    score: f64,
    lexical_rank: Option<usize>,
    semantic_rank: Option<usize>,
    occurred_at: Option<chrono::DateTime<chrono::Utc>>,
    session_title: Option<String>,
    snippet: String,
}

#[derive(Debug, Serialize)]
struct ShowOutput {
    session: crate::archive::SessionRecord,
    target_index: usize,
    target_ref: Option<String>,
    before: Vec<EventOutput>,
    target: EventOutput,
    after: Vec<EventOutput>,
}

#[derive(Debug, Serialize)]
struct TranscriptOutput {
    session: crate::archive::SessionRecord,
    target_event_id: Option<String>,
    target_ref: Option<String>,
    target_index: Option<usize>,
    events: Vec<EventOutput>,
}

#[derive(Debug, Serialize)]
struct EventOutput {
    event_id: String,
    #[serde(rename = "ref")]
    ref_id: Option<String>,
    session_id: String,
    source_id: String,
    machine_id: String,
    source_kind: String,
    ordinal: i64,
    event_type: String,
    role: Option<String>,
    content: String,
    raw_artifact_hash: Option<String>,
    occurred_at: Option<chrono::DateTime<chrono::Utc>>,
    metadata: serde_json::Value,
    hash: String,
}

#[derive(Debug, Serialize)]
struct StatusOutput {
    data_dir: String,
    db_path: String,
    stats: crate::storage::ArchiveStats,
    query_embedder: crate::embed::EmbedderStatus,
    query_embedder_probe: Option<EmbedderProbeOutput>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct EmbedderProbeOutput {
    status: EmbedderProbeStatus,
    model_id: Option<String>,
    dims: Option<usize>,
    semantic: Option<bool>,
    sample_dims: Option<usize>,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum EmbedderProbeStatus {
    Ready,
    Degraded,
}

fn run_update_once(
    store: &Store,
    config: &AppConfig,
    max_files: Option<usize>,
    source: Option<String>,
) -> Result<UpdateOutput> {
    let ingest = ingest::update_local(
        store,
        &config.machine_id,
        ingest::UpdateOptions { max_files, source },
    )?;
    let projected = search::refresh(store)?;
    let (embedder, degraded_reason) = load_embedder(config);
    let embeddings = search::refresh_embeddings(
        store,
        &config.machine_id,
        embedder.as_deref(),
        degraded_reason,
    )?;
    Ok(UpdateOutput {
        ingest,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn run_update_once_human(
    store: &Store,
    config: &AppConfig,
    max_files: Option<usize>,
    source: Option<String>,
) -> Result<UpdateOutput> {
    let progress = ProgressUi::new();
    let scan = progress.phase("Scanning local agent logs");
    let ingest = ingest::update_local(
        store,
        &config.machine_id,
        ingest::UpdateOptions { max_files, source },
    )?;
    scan.finish(format!(
        "{} files, {} new events",
        format_count(ingest.files_seen),
        format_count(ingest.inserted)
    ));

    let index = progress.phase("Updating search index");
    let projected = search::refresh(store)?;
    index.finish(format!("{} events indexed", format_count(projected)));

    let embed = progress.phase("Updating embeddings");
    let (embedder, degraded_reason) = load_embedder(config);
    let embeddings = search::refresh_embeddings(
        store,
        &config.machine_id,
        embedder.as_deref(),
        degraded_reason,
    )?;
    embed.finish(embedding_phase_detail(&embeddings));

    Ok(UpdateOutput {
        ingest,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn run_import_once(store: &Store, config: &AppConfig, input: &str) -> Result<ImportOutput> {
    let stats = transport::import_jsonl_path(store, input)?;
    let projected = search::refresh(store)?;
    let (embedder, degraded_reason) = load_embedder(config);
    let embeddings = search::refresh_embeddings(
        store,
        &config.machine_id,
        embedder.as_deref(),
        degraded_reason,
    )?;
    Ok(ImportOutput {
        import: stats,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn run_import_once_human(store: &Store, config: &AppConfig, input: &str) -> Result<ImportOutput> {
    let progress = ProgressUi::new();
    let import = progress.phase("Importing history stream");
    let stats = transport::import_jsonl_path(store, input)?;
    import.finish(format!(
        "{} new records, {} duplicates",
        format_count(stats.inserted),
        format_count(stats.duplicates)
    ));

    let index = progress.phase("Updating search index");
    let projected = search::refresh(store)?;
    index.finish(format!("{} events indexed", format_count(projected)));

    let embed = progress.phase("Updating embeddings");
    let (embedder, degraded_reason) = load_embedder(config);
    let embeddings = search::refresh_embeddings(
        store,
        &config.machine_id,
        embedder.as_deref(),
        degraded_reason,
    )?;
    embed.finish(embedding_phase_detail(&embeddings));

    Ok(ImportOutput {
        import: stats,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn status_output(store: &Store, config: &AppConfig) -> Result<StatusOutput> {
    Ok(StatusOutput {
        data_dir: config.data_dir.display().to_string(),
        db_path: store.db_path().display().to_string(),
        stats: store.stats()?,
        query_embedder: config.embedder.status_without_loading(),
        query_embedder_probe: embedder_probe_output(config),
    })
}

fn embedder_probe_output(config: &AppConfig) -> Option<EmbedderProbeOutput> {
    if std::env::var("SUPER_CASS_PROBE_EMBEDDER").as_deref() != Ok("1") {
        return None;
    }
    Some(match config.embedder.load() {
        Ok(loaded) => match loaded.embed_one("super cass query embedder probe") {
            Ok(vector) => EmbedderProbeOutput {
                status: EmbedderProbeStatus::Ready,
                model_id: Some(loaded.model_id().to_string()),
                dims: Some(loaded.dims()),
                semantic: Some(loaded.is_semantic()),
                sample_dims: Some(vector.len()),
                reason: None,
            },
            Err(err) => degraded_probe(err),
        },
        Err(err) => degraded_probe(err),
    })
}

fn degraded_probe(err: impl std::fmt::Display) -> EmbedderProbeOutput {
    EmbedderProbeOutput {
        status: EmbedderProbeStatus::Degraded,
        model_id: None,
        dims: None,
        semantic: None,
        sample_dims: None,
        reason: Some(format!("{err:#}")),
    }
}

fn print_update_output(output: &UpdateOutput, color: bool) {
    println!();
    println!("{}", styled("Update complete", "1;32", color));
    print_section(
        "Files",
        &[
            ("Seen", format_count(output.ingest.files_seen)),
            ("Unchanged", format_count(output.ingest.skipped_unchanged)),
            ("New events", format_count(output.ingest.inserted)),
            ("Duplicates", format_count(output.ingest.duplicates)),
            ("Errors", format_count(output.ingest.errors)),
        ],
        color,
    );
    print_search_summary(output.search_index.indexed_events, color);
    print_embedding_summary(&output.embeddings, color);
}

fn print_import_output(output: &ImportOutput, color: bool) {
    println!();
    println!("{}", styled("Import complete", "1;32", color));
    print_section(
        "Records",
        &[
            ("New records", format_count(output.import.inserted)),
            ("Duplicates", format_count(output.import.duplicates)),
            (
                "Imported vectors",
                format_count(output.import.vectors_indexed),
            ),
        ],
        color,
    );
    print_search_summary(output.search_index.indexed_events, color);
    print_embedding_summary(&output.embeddings, color);
}

fn print_search_summary(indexed_events: usize, color: bool) {
    print_section(
        "Search",
        &[("Indexed events", format_count(indexed_events))],
        color,
    );
}

fn print_embedding_summary(embeddings: &search::EmbeddingRefresh, color: bool) {
    let mode = embeddings
        .degraded_reason
        .as_deref()
        .map(|reason| format!("degraded ({reason})"))
        .unwrap_or_else(|| "ready".to_string());
    print_section(
        "Embeddings",
        &[
            ("New embeddings", format_count(embeddings.embedded)),
            ("Indexed vectors", format_count(embeddings.vectors_indexed)),
            ("Mode", mode),
        ],
        color,
    );
}

fn print_section(title: &str, rows: &[(&str, String)], color: bool) {
    println!();
    println!("  {}", styled(title, "1;36", color));
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in rows {
        println!("    {label:<width$}  {value}");
    }
}

fn embedding_phase_detail(embeddings: &search::EmbeddingRefresh) -> String {
    if let Some(reason) = &embeddings.degraded_reason {
        format!("degraded: {reason}")
    } else {
        format!(
            "{} new embeddings, {} vectors indexed",
            format_count(embeddings.embedded),
            format_count(embeddings.vectors_indexed)
        )
    }
}

fn format_count(value: usize) -> String {
    let text = value.to_string();
    let mut out = String::with_capacity(text.len() + text.len() / 3);
    for (idx, ch) in text.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn styled(text: &str, code: &str, color: bool) -> String {
    if color {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

struct ProgressUi {
    interactive: bool,
}

impl ProgressUi {
    fn new() -> Self {
        Self {
            interactive: std::io::stderr().is_terminal(),
        }
    }

    fn phase(&self, label: &str) -> ProgressPhase {
        ProgressPhase::start(label, self.interactive)
    }
}

struct ProgressPhase {
    label: String,
    interactive: bool,
    started: Instant,
    stop: Option<mpsc::Sender<()>>,
    handle: Option<thread::JoinHandle<()>>,
    finished: bool,
}

impl ProgressPhase {
    fn start(label: &str, interactive: bool) -> Self {
        let started = Instant::now();
        if interactive {
            let label_for_thread = label.to_string();
            let (tx, rx) = mpsc::channel();
            let handle = thread::spawn(move || {
                let frames = ["-", "\\", "|", "/"];
                let mut idx = 0usize;
                loop {
                    eprint!(
                        "\r\x1b[36m{}\x1b[0m {}",
                        frames[idx % frames.len()],
                        label_for_thread
                    );
                    let _ = std::io::stderr().flush();
                    idx = idx.wrapping_add(1);
                    if rx.recv_timeout(Duration::from_millis(90)).is_ok() {
                        break;
                    }
                }
            });
            Self {
                label: label.to_string(),
                interactive,
                started,
                stop: Some(tx),
                handle: Some(handle),
                finished: false,
            }
        } else {
            eprintln!("{label}...");
            Self {
                label: label.to_string(),
                interactive,
                started,
                stop: None,
                handle: None,
                finished: false,
            }
        }
    }

    fn finish(mut self, detail: String) {
        self.stop_spinner();
        let elapsed = format_elapsed(self.started.elapsed());
        if self.interactive {
            eprintln!(
                "\r\x1b[2K\x1b[32mdone\x1b[0m {} \x1b[2m{}; {}\x1b[0m",
                self.label, detail, elapsed
            );
        } else {
            eprintln!("{} done: {} ({})", self.label, detail, elapsed);
        }
        self.finished = true;
    }

    fn stop_spinner(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ProgressPhase {
    fn drop(&mut self) {
        if !self.finished {
            self.stop_spinner();
            if self.interactive {
                eprint!("\r\x1b[2K");
                let _ = std::io::stderr().flush();
            }
        }
    }
}

fn format_elapsed(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis < 1_000 {
        format!("{millis}ms")
    } else {
        format!("{:.1}s", duration.as_secs_f64())
    }
}

fn print_status_output(output: &StatusOutput) {
    println!("data_dir={}", output.data_dir);
    println!("db_path={}", output.db_path);
    println!("sources={}", output.stats.sources);
    println!("raw_artifacts={}", output.stats.raw_artifacts);
    println!("sessions={}", output.stats.sessions);
    println!("events={}", output.stats.events);
    println!("search_units={}", output.stats.search_units);
    println!("embeddings={}", output.stats.embeddings);
    println!(
        "query_embedder={} semantic={} available={} degraded_reason={}",
        output.query_embedder.provider,
        output.query_embedder.semantic,
        output.query_embedder.available,
        output
            .query_embedder
            .degraded_reason
            .clone()
            .unwrap_or_else(|| "none".to_string())
    );
    if let Some(probe) = &output.query_embedder_probe {
        match probe.status {
            EmbedderProbeStatus::Ready => println!(
                "query_embedder_probe=ready model_id={} dims={} semantic={} sample_dims={}",
                probe.model_id.as_deref().unwrap_or("unknown"),
                probe.dims.unwrap_or(0),
                probe.semantic.unwrap_or(false),
                probe.sample_dims.unwrap_or(0)
            ),
            EmbedderProbeStatus::Degraded => println!(
                "query_embedder_probe=degraded reason={}",
                probe.reason.as_deref().unwrap_or("unknown")
            ),
        }
    }
}

fn show_output(store: &Store, context: &crate::storage::TranscriptContext) -> Result<ShowOutput> {
    let before = context
        .events
        .iter()
        .take(context.target_index)
        .map(|event| event_output(store, event))
        .collect::<Result<Vec<_>>>()?;
    let after = context
        .events
        .iter()
        .skip(context.target_index + 1)
        .map(|event| event_output(store, event))
        .collect::<Result<Vec<_>>>()?;
    Ok(ShowOutput {
        session: context.session.clone(),
        target_index: context.target_index,
        target_ref: store.recent_ref_for_event_id(&context.target_event.id)?,
        before,
        target: event_output(store, &context.target_event)?,
        after,
    })
}

fn transcript_output(
    store: &Store,
    session: &crate::archive::SessionRecord,
    events: &[crate::archive::EventRecord],
    target_event_id: Option<&str>,
) -> Result<TranscriptOutput> {
    let target_index =
        target_event_id.and_then(|event_id| events.iter().position(|event| event.id == event_id));
    let target_ref = target_event_id
        .map(|event_id| store.recent_ref_for_event_id(event_id))
        .transpose()?
        .flatten();
    let events = events
        .iter()
        .map(|event| event_output(store, event))
        .collect::<Result<Vec<_>>>()?;
    Ok(TranscriptOutput {
        session: session.clone(),
        target_event_id: target_event_id.map(str::to_string),
        target_ref,
        target_index,
        events,
    })
}

fn event_output(store: &Store, event: &crate::archive::EventRecord) -> Result<EventOutput> {
    Ok(EventOutput {
        event_id: event.id.clone(),
        ref_id: store.recent_ref_for_event_id(&event.id)?,
        session_id: event.session_id.clone(),
        source_id: event.source_id.clone(),
        machine_id: event.machine_id.clone(),
        source_kind: event.source_kind.clone(),
        ordinal: event.ordinal,
        event_type: event.event_type.clone(),
        role: event.role.clone(),
        content: event.content.clone(),
        raw_artifact_hash: event.raw_artifact_hash.clone(),
        occurred_at: event.occurred_at,
        metadata: event.metadata.clone(),
        hash: event.hash.clone(),
    })
}

fn search_output(
    query: &str,
    limit: usize,
    sort: SearchSort,
    recency_bias: f64,
    response: &search::SearchResponse,
    refs: &[String],
) -> SearchOutput {
    SearchOutput {
        query: query.to_string(),
        options: SearchOptionsOutput {
            limit,
            sort: sort.as_str(),
            recency_bias,
        },
        degraded_reason: response.degraded_reason.clone(),
        results: response
            .results
            .iter()
            .enumerate()
            .map(|(idx, result)| SearchResultOutput {
                ref_id: refs.get(idx).cloned().unwrap_or_else(|| "-".to_string()),
                match_type: result.match_type,
                event_id: result.event_id.clone(),
                session_id: result.session_id.clone(),
                source_kind: result.source_kind.clone(),
                score: result.score,
                lexical_rank: result.lexical_rank,
                semantic_rank: result.semantic_rank,
                occurred_at: result.occurred_at,
                session_title: result.session_title.clone(),
                snippet: result.snippet.clone(),
            })
            .collect(),
        next_commands: search_hints(&response.results, refs),
    }
}

fn search_hints(results: &[search::SearchResult], refs: &[String]) -> Vec<String> {
    let Some(result) = results.first() else {
        return vec!["super-cass update --json".to_string()];
    };
    let Some(ref_id) = refs.first() else {
        return Vec::new();
    };
    vec![
        format!("super-cass show {ref_id} --json"),
        format!(
            "super-cass transcript {} --at {ref_id} --json",
            result.session_id
        ),
    ]
}

impl SearchSort {
    fn as_str(self) -> &'static str {
        match self {
            SearchSort::Relevance => "relevance",
            SearchSort::Newest => "newest",
            SearchSort::Oldest => "oldest",
        }
    }
}

fn resolve_columns(
    verbose: bool,
    cols: Option<String>,
    include: Option<String>,
    exclude: Option<String>,
) -> Result<Vec<Column>> {
    if let Some(cols) = cols {
        return parse_columns(&cols);
    }
    let mut columns = if verbose {
        vec![
            Column::Ref,
            Column::Source,
            Column::Match,
            Column::Score,
            Column::Lex,
            Column::Sem,
            Column::When,
            Column::Title,
            Column::Preview,
            Column::Session,
            Column::Event,
        ]
    } else {
        vec![
            Column::Ref,
            Column::Source,
            Column::Match,
            Column::When,
            Column::Preview,
        ]
    };
    for column in parse_columns_opt(include)? {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    for column in parse_columns_opt(exclude)? {
        columns.retain(|candidate| *candidate != column);
    }
    Ok(columns)
}

fn resolve_context_event_id(
    store: &Store,
    target: Option<String>,
    event: Option<String>,
    search_unit: Option<String>,
) -> Result<String> {
    match (target, event, search_unit) {
        (Some(value), None, None) => resolve_event_ref_or_id(store, &value),
        (None, Some(event_id), None) => resolve_event_ref_or_id(store, &event_id),
        (None, None, Some(unit_id)) => store
            .search_unit_by_id(&unit_id)?
            .map(|unit| unit.event_id)
            .ok_or_else(|| anyhow::anyhow!("search unit not found: {unit_id}")),
        (None, None, None) => bail!("show requires a ref, --event <id>, or --search-unit <id>"),
        _ => bail!("show accepts one target: positional ref, --event, or --search-unit"),
    }
}

fn resolve_transcript_target(
    store: &Store,
    target: &str,
    at: Option<String>,
    search_unit: Option<String>,
) -> Result<(String, Option<String>)> {
    if let Some(session) = store.session_by_id(target)? {
        let target_event_id = resolve_optional_transcript_at(store, at, search_unit)?;
        if let Some(event_id) = &target_event_id {
            let event = store
                .event_by_id(event_id)?
                .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
            if event.session_id != session.id {
                bail!(
                    "event {event_id} belongs to session {}, not {}",
                    event.session_id,
                    session.id
                );
            }
        }
        return Ok((session.id, target_event_id));
    }

    if at.is_some() || search_unit.is_some() {
        bail!("transcript accepts --at only when the target is a session id");
    }
    let event_id = resolve_event_ref_or_id(store, target)?;
    let event = store
        .event_by_id(&event_id)?
        .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
    Ok((event.session_id, Some(event.id)))
}

fn resolve_optional_transcript_at(
    store: &Store,
    at: Option<String>,
    search_unit: Option<String>,
) -> Result<Option<String>> {
    match (at, search_unit) {
        (Some(event_id), None) => Ok(Some(resolve_event_ref_or_id(store, &event_id)?)),
        (None, Some(unit_id)) => store
            .search_unit_by_id(&unit_id)?
            .map(|unit| Some(unit.event_id))
            .ok_or_else(|| anyhow::anyhow!("search unit not found: {unit_id}")),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => bail!("transcript accepts either --at or --search-unit, not both"),
    }
}

fn resolve_event_ref_or_id(store: &Store, value: &str) -> Result<String> {
    if store.event_by_id(value)?.is_some() {
        return Ok(value.to_string());
    }
    if let Some(event_id) = store.event_id_for_recent_ref(value)? {
        return Ok(event_id);
    }
    bail!("event/ref not found: {value}")
}

fn view_metadata_for_event(
    store: &Store,
    event: &crate::archive::EventRecord,
    verbose: bool,
) -> Result<crate::transcript::ViewMetadata> {
    let source = if verbose {
        store.source_by_id(&event.source_id)?
    } else {
        None
    };
    let raw_artifact = if verbose {
        event
            .raw_artifact_hash
            .as_deref()
            .map(|hash| store.raw_artifact_summary_by_hash(hash))
            .transpose()?
            .flatten()
    } else {
        None
    };
    Ok(crate::transcript::ViewMetadata {
        ref_id: store.recent_ref_for_event_id(&event.id)?,
        source,
        raw_artifact,
        verbose,
    })
}

fn view_metadata_for_session(
    store: &Store,
    session: &crate::archive::SessionRecord,
    target_event: Option<&crate::archive::EventRecord>,
    verbose: bool,
) -> Result<crate::transcript::ViewMetadata> {
    if let Some(event) = target_event {
        return view_metadata_for_event(store, event, verbose);
    }
    let source = if verbose {
        store.source_by_id(&session.source_id)?
    } else {
        None
    };
    Ok(crate::transcript::ViewMetadata {
        ref_id: None,
        source,
        raw_artifact: None,
        verbose,
    })
}

fn page_or_print(output: &str, target_event_id: Option<&str>, no_pager: bool) -> Result<()> {
    if no_pager || !std::io::stdout().is_terminal() {
        return write_stdout(output);
    }
    let Some(mut pager) = pager_command(target_event_id) else {
        return write_stdout(output);
    };
    match pager.stdin(Stdio::piped()).spawn() {
        Ok(mut child) => {
            if let Some(stdin) = child.stdin.as_mut() {
                stdin.write_all(output.as_bytes())?;
            }
            let _ = child.wait();
        }
        Err(_) => {
            write_stdout(output)?;
        }
    }
    Ok(())
}

fn recent_ref_inputs(results: &[search::SearchResult]) -> Vec<RecentResultRefInput> {
    results
        .iter()
        .map(|result| RecentResultRefInput {
            event_id: result.event_id.clone(),
            session_id: result.session_id.clone(),
            source_kind: result.source_kind.clone(),
            occurred_at: result.occurred_at,
            preview: result.snippet.clone(),
        })
        .collect()
}

fn run_fzf_search(
    config: &AppConfig,
    query: &str,
    results: &[search::SearchResult],
    refs: &[String],
    color: bool,
) -> Result<()> {
    if results.is_empty() {
        println!("No results for: \"{query}\"");
        return Ok(());
    }
    if !command_exists("fzf") {
        bail!(
            "fzf is not installed. Use `super-cass search {}` and then `super-cass show <ref>` or `super-cass transcript <ref>`.",
            shell_quote(query)
        );
    }
    let current_exe = std::env::current_exe()?;
    let exe = shell_quote(&current_exe.to_string_lossy());
    let data_dir = shell_quote(&config.data_dir.to_string_lossy());
    let preview = format!("{exe} --data-dir {data_dir} show {{7}} --before 3 --after 5");
    let open = format!("{exe} --data-dir {data_dir} transcript {{7}}");
    let mut child = ProcessCommand::new("fzf")
        .arg("--ansi")
        .arg("--delimiter")
        .arg("\t")
        .arg("--with-nth")
        .arg("1,2,3,4,5")
        .arg("--header")
        .arg("Ref\tSource\tMatch\tWhen\tPreview")
        .arg("--preview")
        .arg(preview)
        .arg("--bind")
        .arg(format!("enter:execute({open})+abort"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(fzf_rows(results, refs, color).as_bytes())?;
    }
    let status = child.wait()?;
    if !status.success() {
        return Ok(());
    }
    Ok(())
}

fn fzf_rows(results: &[search::SearchResult], refs: &[String], color: bool) -> String {
    let mut rows = String::new();
    for (idx, result) in results.iter().enumerate() {
        rows.push_str(&fzf_row(result, refs.get(idx).map(String::as_str), color));
        rows.push('\n');
    }
    rows
}

fn fzf_row(result: &search::SearchResult, ref_id: Option<&str>, color: bool) -> String {
    [
        clean_fzf_field(ref_id.unwrap_or("-")),
        clean_fzf_field(&result.source_kind),
        clean_fzf_field(&color_match(
            match result.match_type {
                search::MatchType::Lexical => "lexical",
                search::MatchType::Semantic => "semantic",
                search::MatchType::Hybrid => "hybrid",
            },
            color,
        )),
        clean_fzf_field(
            &result
                .occurred_at
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        clean_fzf_field(&result.snippet),
        clean_fzf_field(&result.session_id),
        clean_fzf_field(&result.event_id),
    ]
    .join("\t")
}

fn clean_fzf_field(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn command_exists(command: &str) -> bool {
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths).find(|path| {
                let candidate = path.join(command);
                candidate.is_file()
            })
        })
        .is_some()
}

fn write_stdout(output: &str) -> Result<()> {
    let mut stdout = io::stdout().lock();
    match stdout.write_all(output.as_bytes()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn shell_quote(input: &str) -> String {
    if input.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", input.replace('\'', "'\\''"))
}

fn pager_command(target_event_id: Option<&str>) -> Option<ProcessCommand> {
    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
    let mut parts = pager.split_whitespace();
    let program = parts.next()?;
    let mut command = ProcessCommand::new(program);
    command.args(parts);
    if program.ends_with("less") {
        command.arg("-R");
        if target_event_id.is_some() {
            command.arg("+/^=>");
        }
    }
    Some(command)
}

fn parse_columns_opt(input: Option<String>) -> Result<Vec<Column>> {
    input
        .map(|text| parse_columns(&text))
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn parse_columns(input: &str) -> Result<Vec<Column>> {
    let mut columns = Vec::new();
    for raw in input.split(',') {
        let name = raw.trim();
        if name.is_empty() {
            continue;
        }
        if name.eq_ignore_ascii_case("ids") {
            columns.push(Column::Session);
            columns.push(Column::Event);
            continue;
        }
        columns.push(parse_column(name)?);
    }
    if columns.is_empty() {
        bail!("no columns selected");
    }
    Ok(columns)
}

fn parse_column(name: &str) -> Result<Column> {
    match name.to_ascii_lowercase().as_str() {
        "ref" | "refs" => Ok(Column::Ref),
        "source" => Ok(Column::Source),
        "match" => Ok(Column::Match),
        "when" | "time" | "date" => Ok(Column::When),
        "title" => Ok(Column::Title),
        "preview" | "snippet" => Ok(Column::Preview),
        "score" => Ok(Column::Score),
        "lex" | "lexical" => Ok(Column::Lex),
        "sem" | "semantic" => Ok(Column::Sem),
        "event" | "event_id" => Ok(Column::Event),
        "session" | "session_id" => Ok(Column::Session),
        _ => bail!(
            "unknown column '{name}'. Available columns: ref,source,match,when,title,preview,score,lex,sem,event,session,ids"
        ),
    }
}

fn print_search_results(
    query: &str,
    results: &[search::SearchResult],
    refs: &[String],
    columns: &[Column],
    color: bool,
) {
    if results.is_empty() {
        println!("No results for: \"{query}\"");
        return;
    }
    let rows = results
        .iter()
        .enumerate()
        .map(|(idx, result)| {
            columns
                .iter()
                .map(|column| cell_value(*column, result, refs.get(idx).map(String::as_str)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let widths = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            rows.iter()
                .map(|row| row[idx].chars().count())
                .max()
                .unwrap_or(0)
                .max(column_header(*column).chars().count())
        })
        .collect::<Vec<_>>();

    for (idx, column) in columns.iter().enumerate() {
        let header = column_header(*column);
        print_cell(
            header,
            widths[idx],
            idx + 1 == columns.len(),
            color,
            Style::Header,
        );
    }
    println!();

    let terms = query_terms(query);
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            let is_last = idx + 1 == columns.len();
            let rendered = match columns[idx] {
                Column::Match => color_match(value, color),
                Column::Preview => highlight_terms(value, &terms, color),
                _ => value.to_string(),
            };
            print_rendered_cell(&rendered, value.chars().count(), widths[idx], is_last);
        }
        println!();
    }
}

fn cell_value(column: Column, result: &search::SearchResult, ref_id: Option<&str>) -> String {
    match column {
        Column::Ref => ref_id.unwrap_or("-").to_string(),
        Column::Source => result.source_kind.clone(),
        Column::Match => match result.match_type {
            search::MatchType::Lexical => "lexical".to_string(),
            search::MatchType::Semantic => "semantic".to_string(),
            search::MatchType::Hybrid => "hybrid".to_string(),
        },
        Column::When => result
            .occurred_at
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "-".to_string()),
        Column::Title => truncate_cell(
            &result
                .session_title
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            72,
        ),
        Column::Preview => result.snippet.clone(),
        Column::Score => format!("{:.6}", result.score),
        Column::Lex => rank_cell(result.lexical_rank),
        Column::Sem => rank_cell(result.semantic_rank),
        Column::Event => result.event_id.clone(),
        Column::Session => result.session_id.clone(),
    }
}

fn rank_cell(rank: Option<usize>) -> String {
    rank.map(|rank| rank.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn truncate_cell(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let mut out = input
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

fn column_header(column: Column) -> &'static str {
    match column {
        Column::Ref => "Ref",
        Column::Source => "Source",
        Column::Match => "Match",
        Column::When => "When",
        Column::Title => "Title",
        Column::Preview => "Preview",
        Column::Score => "Score",
        Column::Lex => "Lex",
        Column::Sem => "Sem",
        Column::Event => "Event",
        Column::Session => "Session",
    }
}

#[derive(Clone, Copy)]
enum Style {
    Header,
}

fn print_cell(text: &str, width: usize, is_last: bool, color: bool, style: Style) {
    let rendered = match style {
        Style::Header if color => format!("\x1b[2m{text}\x1b[0m"),
        _ => text.to_string(),
    };
    print_rendered_cell(&rendered, text.chars().count(), width, is_last);
}

fn print_rendered_cell(rendered: &str, visible_width: usize, width: usize, is_last: bool) {
    if is_last {
        print!("{rendered}");
    } else {
        print!(
            "{rendered}{}",
            " ".repeat(width.saturating_sub(visible_width) + 2)
        );
    }
}

fn color_match(value: &str, color: bool) -> String {
    if !color {
        return value.to_string();
    }
    let code = match value {
        "hybrid" => "32",
        "lexical" => "34",
        "semantic" => "35",
        _ => "0",
    };
    format!("\x1b[{code}m{value}\x1b[0m")
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
        .filter(|part| part.chars().count() >= 2)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn highlight_terms(input: &str, terms: &[String], color: bool) -> String {
    if !color || terms.is_empty() {
        return input.to_string();
    }
    input
        .split_whitespace()
        .map(|word| {
            let normalized = word
                .trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
                .to_ascii_lowercase();
            if terms.iter().any(|term| normalized.contains(term)) {
                format!("\x1b[1;33m{word}\x1b[0m")
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

async fn run_daemon(
    store: &Store,
    machine_id: &str,
    embedder_config: crate::embed::EmbedderConfig,
    interval_secs: u64,
    max_files: Option<usize>,
    source: Option<String>,
) -> Result<()> {
    let interval = std::time::Duration::from_secs(interval_secs.max(1));
    let progress = ProgressUi::new();
    loop {
        let scan = progress.phase("Scanning local agent logs");
        let stats = ingest::update_local(
            store,
            machine_id,
            ingest::UpdateOptions {
                max_files,
                source: source.clone(),
            },
        )?;
        scan.finish(format!(
            "{} files, {} new events",
            format_count(stats.files_seen),
            format_count(stats.inserted)
        ));

        let index = progress.phase("Updating search index");
        let projected = search::refresh(store)?;
        index.finish(format!("{} events indexed", format_count(projected)));

        let embed = progress.phase("Updating embeddings");
        let (embedder, degraded_reason) = load_embedder_config(&embedder_config);
        let embeddings =
            search::refresh_embeddings(store, machine_id, embedder.as_deref(), degraded_reason)?;
        embed.finish(embedding_phase_detail(&embeddings));

        print_update_output(
            &UpdateOutput {
                ingest: stats,
                search_index: SearchIndexOutput {
                    indexed_events: projected,
                },
                embeddings,
            },
            std::io::stdout().is_terminal(),
        );
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }
    Ok(())
}

fn load_embedder(config: &AppConfig) -> (Option<Box<dyn crate::embed::Embedder>>, Option<String>) {
    load_embedder_config(&config.embedder)
}

fn load_embedder_config(
    config: &crate::embed::EmbedderConfig,
) -> (Option<Box<dyn crate::embed::Embedder>>, Option<String>) {
    match config.load() {
        Ok(embedder) => (Some(embedder), None),
        Err(err) => (None, Some(err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{stable_hash, ArchiveRecord, EventRecord, SessionRecord, SourceRecord};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn exact_cols_controls_order() {
        let columns = resolve_columns(
            false,
            Some("source,score,preview".to_string()),
            Some("ids".to_string()),
            None,
        )
        .expect("columns");

        assert_eq!(
            columns,
            vec![Column::Source, Column::Score, Column::Preview]
        );
    }

    #[test]
    fn include_and_exclude_modify_default_columns() {
        let columns = resolve_columns(
            false,
            None,
            Some("score,ids".to_string()),
            Some("when".to_string()),
        )
        .expect("columns");

        assert_eq!(
            columns,
            vec![
                Column::Ref,
                Column::Source,
                Column::Match,
                Column::Preview,
                Column::Score,
                Column::Session,
                Column::Event
            ]
        );
    }

    #[test]
    fn fzf_row_keeps_stable_ids_hidden_after_visible_fields() {
        let result = search::SearchResult {
            match_type: search::MatchType::Hybrid,
            event_id: "event_1".to_string(),
            session_id: "session_1".to_string(),
            source_kind: "codex".to_string(),
            score: 0.5,
            lexical_rank: Some(1),
            semantic_rank: Some(2),
            occurred_at: None,
            session_title: None,
            snippet: "preview with\nnew line".to_string(),
        };

        let row = fzf_row(&result, Some("ab3f"), false);
        let fields = row.split('\t').collect::<Vec<_>>();

        assert_eq!(fields.len(), 7);
        assert_eq!(fields[0], "ab3f");
        assert_eq!(fields[1], "codex");
        assert_eq!(fields[2], "hybrid");
        assert_eq!(fields[4], "preview with new line");
        assert_eq!(fields[5], "session_1");
        assert_eq!(fields[6], "event_1");
    }

    #[test]
    fn search_json_output_includes_refs_and_next_commands() {
        let result = search::SearchResult {
            match_type: search::MatchType::Hybrid,
            event_id: "event_1".to_string(),
            session_id: "session_1".to_string(),
            source_kind: "codex".to_string(),
            score: 0.5,
            lexical_rank: Some(1),
            semantic_rank: Some(2),
            occurred_at: None,
            session_title: Some("Session title".to_string()),
            snippet: "useful snippet".to_string(),
        };
        let response = search::SearchResponse {
            degraded_reason: None,
            results: vec![result],
        };

        let output = search_output(
            "workspace metadata",
            20,
            SearchSort::Relevance,
            0.25,
            &response,
            &["ab3f".to_string()],
        );
        let value = serde_json::to_value(output).expect("serialize search output");

        assert_eq!(value["query"], "workspace metadata");
        assert_eq!(value["options"]["limit"], 20);
        assert_eq!(value["options"]["sort"], "relevance");
        assert_eq!(value["results"][0]["ref"], "ab3f");
        assert_eq!(value["results"][0]["event_id"], "event_1");
        assert_eq!(value["next_commands"][0], "super-cass show ab3f --json");
        assert_eq!(
            value["next_commands"][1],
            "super-cass transcript session_1 --at ab3f --json"
        );
    }

    #[test]
    fn shell_quote_handles_spaces_and_single_quotes() {
        assert_eq!(shell_quote("/tmp/a path/it's"), "'/tmp/a path/it'\\''s'");
    }

    #[test]
    fn robot_mode_is_global_and_requests_structured_errors() {
        let cli = Cli::try_parse_from(["super-cass", "--robot", "search", "needle"])
            .expect("parse robot search");

        assert!(cli.robot);
        assert_eq!(cli.command_name(), "search");
        assert!(cli.wants_structured_errors());
    }

    #[test]
    fn transcript_target_resolves_recent_ref_to_containing_session() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let ref_id = store
            .recent_ref_for_event_id("event_view")
            .expect("ref lookup")
            .expect("ref exists");

        let (session_id, event_id) =
            resolve_transcript_target(&store, &ref_id, None, None).expect("target");

        assert_eq!(session_id, "session_view");
        assert_eq!(event_id.as_deref(), Some("event_view"));
    }

    #[test]
    fn transcript_session_accepts_at_recent_ref() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let ref_id = store
            .recent_ref_for_event_id("event_view")
            .expect("ref lookup")
            .expect("ref exists");

        let (session_id, event_id) =
            resolve_transcript_target(&store, "session_view", Some(ref_id), None).expect("target");

        assert_eq!(session_id, "session_view");
        assert_eq!(event_id.as_deref(), Some("event_view"));
    }

    fn fixture_store_with_viewer_ref() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let source = SourceRecord {
            id: "source_view".to_string(),
            kind: "codex".to_string(),
            identity: "source_view".to_string(),
            path: Some("/tmp/source.jsonl".to_string()),
            first_seen_at: Utc::now(),
            updated_at: Utc::now(),
            hash: stable_hash(&("source_view", "source")).expect("source hash"),
        };
        let session = SessionRecord {
            id: "session_view".to_string(),
            source_id: source.id.clone(),
            machine_id: "machine_view".to_string(),
            source_kind: "codex".to_string(),
            external_id: "agent_session_view".to_string(),
            title: Some("Viewer fixture".to_string()),
            status: "open".to_string(),
            started_at: None,
            updated_at: None,
            metadata: json!({}),
            hash: stable_hash(&("session_view", "session")).expect("session hash"),
        };
        let event = EventRecord {
            id: "event_view".to_string(),
            session_id: session.id.clone(),
            source_id: source.id.clone(),
            machine_id: "machine_view".to_string(),
            source_kind: "codex".to_string(),
            ordinal: 7,
            event_type: "message".to_string(),
            role: Some("assistant".to_string()),
            content: "viewer test content".to_string(),
            raw_artifact_hash: None,
            occurred_at: None,
            metadata: json!({}),
            hash: stable_hash(&("event_view", "event")).expect("event hash"),
        };
        store
            .import_records(&[
                ArchiveRecord::Source(source),
                ArchiveRecord::Session(session),
                ArchiveRecord::Event(event),
            ])
            .expect("import records");
        store
            .record_recent_result_refs(&[RecentResultRefInput {
                event_id: "event_view".to_string(),
                session_id: "session_view".to_string(),
                source_kind: "codex".to_string(),
                occurred_at: None,
                preview: "viewer test content".to_string(),
            }])
            .expect("record recent ref");
        (dir, store)
    }
}
