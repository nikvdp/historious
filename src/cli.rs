use crate::config::AppConfig;
use crate::ingest;
use crate::search;
use crate::server;
use crate::storage::{RecentResultRefInput, Store, ThreadListOptions, ThreadSortMode};
use crate::transport;
use anyhow::{bail, Result};
use chrono::{DateTime, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Utc};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_SEARCH_LIMIT: usize = 10;
const DEFAULT_THREAD_LIMIT: usize = 10;
const DEFAULT_FZF_LIMIT: usize = 25;
const DEFAULT_LIVE_SEARCH_LIMIT: usize = 50;
const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:7391";
const LIVE_SEARCH_RELOAD_DELAY_SECS: f32 = 0.35;

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
        #[arg(long, help = "Fully reconcile derived search and vector indexes")]
        repair: bool,
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
    },
    /// Search indexed transcripts.
    Search {
        #[arg(help = "Words, paths, errors, or other details to search for")]
        query: Option<String>,
        #[arg(
            short,
            long,
            help = "Maximum number of results to show; defaults to 10, or 25 for static --fzf"
        )]
        limit: Option<usize>,
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
        #[arg(long, value_enum, help = "Search mode: hybrid, lexical, or semantic")]
        mode: Option<SearchModeArg>,
        #[arg(
            long,
            help = "Search these history item tiers, comma-separated: conversation,tool,raw"
        )]
        corpus: Option<String>,
        #[arg(long, help = "Search readable conversation plus tool calls and output")]
        include_tools: bool,
        #[arg(long, help = "Search raw event payload text only")]
        raw: bool,
        #[arg(
            long,
            default_value_t = 0.0,
            help = "Favor newer results, from 0.0 to 1.0"
        )]
        recency_bias: f64,
        #[arg(
            long,
            help = "Only show results at or after a date or time, like 2026-04-20 or \"3 days ago\""
        )]
        after: Option<String>,
        #[arg(
            long,
            help = "Only show results before a date or time, like 2026-04-20 or \"3 days ago\""
        )]
        before: Option<String>,
        #[arg(long, help = "Only show results from this exact machine id")]
        machine: Option<String>,
        #[arg(
            long,
            alias = "host",
            help = "Only show results from machine ids generated for this hostname"
        )]
        hostname: Option<String>,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(
            long,
            help = "Browse this query's fixed result set with fzf; use tui for the interactive terminal UI"
        )]
        fzf: bool,
        #[arg(long, hide = true)]
        fzf_rows: bool,
    },
    /// Open the interactive terminal search UI.
    Tui {
        #[arg(help = "Initial query for the picker")]
        query: Option<String>,
        #[arg(
            short,
            long,
            help = "Maximum number of results to show",
            default_value_t = DEFAULT_LIVE_SEARCH_LIMIT
        )]
        limit: usize,
        #[arg(
            long,
            default_value = DEFAULT_SERVER_URL,
            help = "Base URL for the local search server; tui starts it when needed"
        )]
        server: String,
        #[arg(long, value_enum, default_value_t = SearchSort::Relevance, help = "Sort results by relevance or time")]
        sort: SearchSort,
        #[arg(long, value_enum, help = "Search mode: hybrid, lexical, or semantic")]
        mode: Option<SearchModeArg>,
        #[arg(
            long,
            help = "Search these history item tiers, comma-separated: conversation,tool,raw"
        )]
        corpus: Option<String>,
        #[arg(long, help = "Search readable conversation plus tool calls and output")]
        include_tools: bool,
        #[arg(long, help = "Search raw event payload text only")]
        raw: bool,
        #[arg(
            long,
            default_value_t = 0.0,
            help = "Favor newer results, from 0.0 to 1.0"
        )]
        recency_bias: f64,
        #[arg(
            long,
            help = "Only show results at or after a date or time, like 2026-04-20 or \"3 days ago\""
        )]
        after: Option<String>,
        #[arg(
            long,
            help = "Only show results before a date or time, like 2026-04-20 or \"3 days ago\""
        )]
        before: Option<String>,
        #[arg(long, help = "Only show results from this exact machine id")]
        machine: Option<String>,
        #[arg(
            long,
            alias = "host",
            help = "Only show results from machine ids generated for this hostname"
        )]
        hostname: Option<String>,
        #[arg(long, help = "Disable colored preview output")]
        no_color: bool,
    },
    /// List recent conversation threads chronologically.
    Threads {
        #[arg(
            short,
            long,
            default_value_t = DEFAULT_THREAD_LIMIT,
            help = "Maximum number of threads to show"
        )]
        limit: usize,
        #[arg(
            long,
            value_enum,
            default_value_t = ThreadSort::Newest,
            help = "Sort threads by newest or oldest activity"
        )]
        sort: ThreadSort,
        #[arg(
            long,
            help = "Only show threads active at or after a date or time, like 2026-04-20, today, or \"3 days ago\""
        )]
        after: Option<String>,
        #[arg(
            long,
            help = "Only show threads active before a date or time, like 2026-04-20, today, or \"3 days ago\""
        )]
        before: Option<String>,
        #[arg(
            long,
            conflicts_with_all = ["after", "before"],
            help = "Only show threads active today"
        )]
        today: bool,
        #[arg(
            long,
            conflicts_with = "all",
            help = "Show threads for this folder scope instead of cwd"
        )]
        project: Option<PathBuf>,
        #[arg(
            long,
            conflicts_with = "project",
            help = "Show threads across every project"
        )]
        all: bool,
        #[arg(long, help = "Print structured JSON")]
        json: bool,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
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
        #[arg(long, value_enum, help = "When to use colored output")]
        color: Option<ColorArg>,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
        #[arg(
            long,
            help = "Show raw event payloads instead of clean conversation history"
        )]
        full: bool,
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
        #[arg(long, value_enum, help = "When to use colored output")]
        color: Option<ColorArg>,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
        #[arg(
            long,
            help = "Show raw event payloads instead of clean conversation history"
        )]
        full: bool,
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
        #[arg(long, value_enum, help = "When to use colored output")]
        color: Option<ColorArg>,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
        #[arg(
            long,
            help = "Show raw event payloads instead of clean conversation history"
        )]
        full: bool,
        #[arg(long, help = "Print structured JSON with exact event content")]
        json: bool,
    },
    /// Write history records to JSONL for backup or transfer.
    Export {
        #[arg(long, help = "Write newline-delimited JSON records")]
        jsonl: bool,
        #[arg(
            long,
            value_enum,
            default_value_t = EmbeddingExportMode::Include,
            help = "Whether to include embedding records in JSONL exports"
        )]
        embeddings: EmbeddingExportMode,
        #[arg(long, help = "Alias for --embeddings omit")]
        no_embeddings: bool,
        #[arg(
            long,
            value_enum,
            default_value_t = RawArtifactExportMode::Inline,
            help = "How to include raw artifacts in JSONL exports: inline content, metadata only, or omit for search-only/incomplete sync"
        )]
        raw_artifacts: RawArtifactExportMode,
        #[arg(
            long,
            help = "Alias for --raw-artifacts omit; export is search-only/incomplete"
        )]
        no_raw_artifacts: bool,
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
    /// List, export, and import raw artifact blobs by content hash.
    RawBlobs {
        #[command(subcommand)]
        command: RawBlobCommand,
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
    /// Serve already-indexed local history over HTTP.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7391", help = "Address to listen on")]
        bind: String,
        #[arg(
            long,
            help = "Also keep local history up to date by scanning periodically"
        )]
        watch: bool,
        #[arg(
            long,
            default_value_t = 30,
            requires = "watch",
            help = "Seconds between scans when --watch is enabled"
        )]
        interval_secs: u64,
        #[arg(
            long,
            requires = "watch",
            help = "Scan at most this many newest files each pass"
        )]
        max_files: Option<usize>,
        #[arg(
            long,
            requires = "watch",
            help = "Scan only one source kind, such as codex or claude_code"
        )]
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
    /// Generate a shell completion script.
    Completion {
        #[arg(value_enum, help = "Shell to generate completions for")]
        shell: Shell,
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

#[derive(Debug, Subcommand)]
pub enum RawBlobCommand {
    /// List raw artifact blob hashes missing from local blob storage.
    Missing {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            help = "Only include sessions from this source kind, such as codex or claude_code"
        )]
        source: Vec<String>,
        #[arg(long, help = "Only include sessions from this workspace path")]
        workspace: Vec<std::path::PathBuf>,
        #[arg(long, help = "Only include this session id")]
        session: Vec<String>,
        #[arg(
            long,
            help = "Only include sessions since an RFC3339 timestamp or YYYY-MM-DD"
        )]
        since: Option<String>,
    },
    /// Write raw artifact blob records for hashes passed as args or stdin.
    Export {
        #[arg(help = "Raw artifact hashes; read newline-delimited hashes from stdin when omitted")]
        hash: Vec<String>,
    },
    /// Read raw artifact blob records from JSONL.
    Import {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(default_value = "-", help = "Input file, or '-' for stdin")]
        input: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SearchSort {
    Relevance,
    Newest,
    Oldest,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SearchModeArg {
    Hybrid,
    Lexical,
    Semantic,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ColorArg {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RawArtifactExportMode {
    Inline,
    Metadata,
    Omit,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum EmbeddingExportMode {
    Include,
    Omit,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ThreadSort {
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

impl From<SearchModeArg> for search::SearchMode {
    fn from(value: SearchModeArg) -> Self {
        match value {
            SearchModeArg::Hybrid => search::SearchMode::Hybrid,
            SearchModeArg::Lexical => search::SearchMode::Lexical,
            SearchModeArg::Semantic => search::SearchMode::Semantic,
        }
    }
}

impl From<ThreadSort> for ThreadSortMode {
    fn from(value: ThreadSort) -> Self {
        match value {
            ThreadSort::Newest => ThreadSortMode::Newest,
            ThreadSort::Oldest => ThreadSortMode::Oldest,
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
    Machine,
    Event,
    Session,
    Item,
    Tier,
    Kind,
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
        let command = self.command;
        if let Command::Completion { shell } = command {
            print_completion(shell);
            return Ok(());
        }

        let config = AppConfig::load(self.data_dir)?;
        let store = Store::open(&config.data_dir)?;
        match command {
            Command::Update {
                max_files,
                source,
                repair,
                json,
            } => {
                if json || robot {
                    let output = run_update_once(&store, &config, max_files, source, repair)?;
                    crate::output::write_success("update", output, Default::default())?;
                } else {
                    let output = run_update_once_human(&store, &config, max_files, source, repair)?;
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
                mode,
                corpus,
                include_tools,
                raw,
                recency_bias,
                after,
                before,
                machine,
                hostname,
                no_color,
                fzf,
                fzf_rows,
            } => {
                if robot && fzf {
                    bail!("--robot cannot be combined with --fzf");
                }
                let limit = search_limit(limit, fzf);
                let query = query.unwrap_or_default();
                if query.trim().is_empty() && fzf {
                    bail!("search --fzf requires a query; use `super-cass tui` for live interactive search");
                }
                if query.trim().is_empty() && !fzf_rows {
                    bail!("search requires a query");
                }
                if fzf_rows && query.trim().is_empty() {
                    return Ok(());
                }
                let after_bound =
                    parse_optional_search_time(after.as_deref(), TimeFilterBound::After)?;
                let before_bound =
                    parse_optional_search_time(before.as_deref(), TimeFilterBound::Before)?;
                let mode = mode
                    .map(search::SearchMode::from)
                    .unwrap_or(config.default_search_mode);
                let corpus = resolve_search_corpus(corpus, include_tools, raw)?;
                let (embedder, degraded_reason) = load_embedder(&config);
                let options = search::SearchOptions::new(limit, sort.into(), recency_bias)
                    .with_mode(mode)
                    .with_corpus(corpus.clone())
                    .with_time_window(after_bound, before_bound)
                    .with_machine_filter(machine.clone(), hostname.clone());
                let response = search::search(
                    &store,
                    &query,
                    options,
                    embedder.as_deref(),
                    degraded_reason,
                )?;
                if fzf {
                    if let Some(reason) = &response.degraded_reason {
                        eprintln!("search degraded: {reason}");
                    }
                    let refs =
                        store.record_recent_result_refs(&recent_ref_inputs(&response.results))?;
                    let color = !no_color;
                    let rows = fzf_rows_output(&response.results, &refs, color);
                    run_static_fzf_search(&config, &query, &rows, color)?;
                } else if json || robot {
                    let refs =
                        store.record_recent_result_refs(&recent_ref_inputs(&response.results))?;
                    let output = search_output(
                        &query,
                        limit,
                        sort,
                        mode,
                        recency_bias,
                        &corpus,
                        after_bound,
                        before_bound,
                        machine.clone(),
                        hostname.clone(),
                        &response,
                        &refs,
                    );
                    crate::output::write_success(
                        "search",
                        output,
                        crate::output::EnvelopeOptions {
                            degraded_reason: response.degraded_reason.clone(),
                            hints: search_hints(&response.results, &refs),
                            ..Default::default()
                        },
                    )?;
                } else if fzf_rows {
                    let refs =
                        store.record_recent_result_refs(&recent_ref_inputs(&response.results))?;
                    let color = !no_color;
                    print!("{}", fzf_rows_output(&response.results, &refs, color));
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
            Command::Tui {
                query,
                limit,
                server,
                sort,
                mode,
                corpus,
                include_tools,
                raw,
                recency_bias,
                after,
                before,
                machine,
                hostname,
                no_color,
            } => {
                if robot {
                    bail!("--robot cannot be combined with tui");
                }
                let after_bound =
                    parse_optional_search_time(after.as_deref(), TimeFilterBound::After)?;
                let before_bound =
                    parse_optional_search_time(before.as_deref(), TimeFilterBound::Before)?;
                let mode = mode
                    .map(search::SearchMode::from)
                    .unwrap_or(config.default_search_mode);
                let corpus = resolve_search_corpus(corpus, include_tools, raw)?;
                run_tui_search(
                    &config,
                    query.as_deref().unwrap_or_default(),
                    &server,
                    limit,
                    sort,
                    mode,
                    corpus,
                    recency_bias,
                    after_bound,
                    before_bound,
                    machine,
                    hostname,
                    !no_color,
                )?;
            }
            Command::Threads {
                limit,
                sort,
                after,
                before,
                today,
                project,
                all,
                json,
                no_color,
            } => {
                let (after_bound, before_bound) = if today {
                    (
                        Some(parse_search_time("today", TimeFilterBound::After)?),
                        Some(parse_search_time("today", TimeFilterBound::Before)?),
                    )
                } else {
                    (
                        parse_optional_search_time(after.as_deref(), TimeFilterBound::After)?,
                        parse_optional_search_time(before.as_deref(), TimeFilterBound::Before)?,
                    )
                };
                let scope = resolve_thread_scope(project.as_deref(), all)?;
                if scope.inferred {
                    if let Some(path) = &scope.path {
                        eprintln!(
                            "Warning: no --project supplied; focusing on cwd: {path}. Use --all for every project."
                        );
                    }
                }
                let options = ThreadListOptions {
                    limit,
                    sort: sort.into(),
                    after: after_bound,
                    before: before_bound,
                    workspace_scope: scope.path.clone(),
                };
                let threads = store.list_threads(&options)?;
                if json || robot {
                    crate::output::write_success(
                        "threads",
                        threads_output(limit, sort, after_bound, before_bound, &scope, &threads),
                        Default::default(),
                    )?;
                } else {
                    let color = !no_color && !robot && std::io::stdout().is_terminal();
                    print_threads_output(&scope, &threads, color);
                }
            }
            Command::Show {
                target,
                event,
                search_unit,
                before,
                after,
                color,
                no_color,
                verbose,
                full,
                json,
            }
            | Command::Expand {
                target,
                event,
                search_unit,
                before,
                after,
                color,
                no_color,
                verbose,
                full,
                json,
            } => {
                let event_id = resolve_context_event_id(&store, target, event, search_unit)?;
                if json || robot {
                    let context = store
                        .events_around_event(&event_id, before, after)?
                        .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
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
                } else if full {
                    let context = store
                        .events_around_event(&event_id, before, after)?
                        .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                    let metadata = view_metadata_for_event(&store, &context.target_event, verbose)?;
                    let color = should_color(no_color, color, robot);
                    write_stdout(&crate::transcript::render_context(
                        &context, &metadata, color,
                    ))?;
                } else {
                    let context = store
                        .history_items_around_event(&event_id, before, after)?
                        .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                    let metadata = if let Some(event) = &context.target_event {
                        view_metadata_for_event(&store, event, verbose)?
                    } else {
                        view_metadata_for_session(&store, &context.session, None, verbose)?
                    };
                    let color = should_color(no_color, color, robot);
                    write_stdout(&crate::transcript::render_history_context(
                        &context, &metadata, color,
                    ))?;
                }
            }
            Command::Transcript {
                target,
                at,
                search_unit,
                no_pager,
                color,
                no_color,
                verbose,
                full,
                json,
            } => {
                let (session, target_event_id) =
                    resolve_transcript_target(&store, &target, at, search_unit)?;
                let session_record = store
                    .session_by_id(&session)?
                    .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?;
                let target_event = target_event_id
                    .as_deref()
                    .map(|event_id| {
                        store
                            .event_by_id(event_id)?
                            .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))
                    })
                    .transpose()?;
                if json || robot {
                    let events = store.events_for_session(&session)?;
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
                } else if full {
                    let events = store.events_for_session(&session)?;
                    let metadata = view_metadata_for_session(
                        &store,
                        &session_record,
                        target_event.as_ref(),
                        verbose,
                    )?;
                    let color = should_color(no_color, color, robot);
                    let rendered = crate::transcript::render_session(
                        &session_record,
                        &events,
                        target_event_id.as_deref(),
                        &metadata,
                        color,
                    );
                    page_or_print(&rendered, target_event_id.as_deref(), no_pager || robot)?;
                } else {
                    let metadata = view_metadata_for_session(
                        &store,
                        &session_record,
                        target_event.as_ref(),
                        verbose,
                    )?;
                    let color = should_color(no_color, color, robot);
                    let context = if let Some(event_id) = target_event_id.as_deref() {
                        store
                            .history_items_around_event(event_id, usize::MAX / 4, usize::MAX / 4)?
                            .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?
                    } else {
                        store
                            .history_items_for_transcript_session(&session)?
                            .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?
                    };
                    let rendered =
                        crate::transcript::render_history_session(&context, &metadata, color);
                    page_or_print(&rendered, target_event_id.as_deref(), no_pager || robot)?;
                }
            }
            Command::Export {
                jsonl,
                embeddings,
                no_embeddings,
                raw_artifacts,
                no_raw_artifacts,
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
                    let options = transport::ExportOptions {
                        include_embeddings: include_embedding_records(embeddings, no_embeddings),
                        include_raw_artifact_records: include_raw_artifact_records(
                            raw_artifacts,
                            no_raw_artifacts,
                        ),
                        include_raw_artifact_content: matches!(
                            raw_artifacts,
                            RawArtifactExportMode::Inline
                        ) && !no_raw_artifacts,
                    };
                    if robot {
                        transport::export_jsonl_filtered_with_options(
                            &store,
                            &filter,
                            options,
                            stdout.lock(),
                        )?;
                    } else {
                        let progress = ProgressUi::new();
                        let mut export = progress.phase("Exporting history stream");
                        let mut last = transport::JsonlProgress::default();
                        let count = transport::export_jsonl_filtered_with_options_and_progress(
                            &store,
                            &filter,
                            options,
                            stdout.lock(),
                            |event| {
                                last = event;
                                export.update(jsonl_progress_detail(event));
                            },
                        )?;
                        export.finish(format!(
                            "{} records exported, {}",
                            format_count(count),
                            format_bytes(last.bytes)
                        ));
                    }
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
            Command::RawBlobs { command } => match command {
                RawBlobCommand::Missing {
                    json,
                    source,
                    workspace,
                    session,
                    since,
                } => {
                    let filter = crate::storage::ArchiveExportFilter {
                        sources: source,
                        workspaces: workspace
                            .iter()
                            .map(|path| transport::normalize_workspace_arg(path))
                            .collect(),
                        sessions: session,
                        since: transport::parse_since_arg(since.as_deref())?,
                    };
                    let hashes = store.missing_raw_artifact_blob_hashes(&filter)?;
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs missing",
                            RawBlobMissingOutput {
                                count: hashes.len(),
                                hashes,
                            },
                            Default::default(),
                        )?;
                    } else {
                        for hash in hashes {
                            println!("{hash}");
                        }
                    }
                }
                RawBlobCommand::Export { hash } => {
                    let hashes = if hash.is_empty() {
                        transport::read_hashes_from_stdin()?
                    } else {
                        hash
                    };
                    let stdout = std::io::stdout();
                    transport::export_raw_blobs(&store, &hashes, stdout.lock())?;
                }
                RawBlobCommand::Import { json, input } => {
                    let output = transport::import_raw_blobs_path(&store, &input)?;
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs import",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        println!(
                            "Imported {} raw blobs, skipped {} already present",
                            format_count(output.imported),
                            format_count(output.duplicates)
                        );
                    }
                }
            },
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
                watch,
                interval_secs,
                max_files,
                source,
            } => {
                let addr = bind.parse()?;
                if watch {
                    let server_store = store.clone();
                    let server_machine_id = config.machine_id.clone();
                    let default_search_mode = config.default_search_mode;
                    let server_embedder = config.embedder.clone();
                    let server_task = tokio::spawn(async move {
                        server::serve(
                            server_store,
                            addr,
                            server_machine_id,
                            default_search_mode,
                            server_embedder,
                        )
                        .await
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
                } else {
                    server::serve(
                        store,
                        addr,
                        config.machine_id,
                        config.default_search_mode,
                        config.embedder,
                    )
                    .await?;
                }
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
            Command::Completion { .. } => unreachable!("completion returns before storage setup"),
        }
        Ok(())
    }
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Command::Update { .. } => "update",
            Command::Search { .. } => "search",
            Command::Tui { .. } => "tui",
            Command::Threads { .. } => "threads",
            Command::Show { .. } => "show",
            Command::Expand { .. } => "expand",
            Command::Transcript { .. } => "transcript",
            Command::Export { .. } => "export",
            Command::Import { .. } => "import",
            Command::RawBlobs { .. } => "raw-blobs",
            Command::Daemon { .. } => "daemon",
            Command::Serve { .. } => "serve",
            Command::Status { .. } => "status",
            Command::Onboard { .. } => "onboard",
            Command::Skill { .. } => "skill",
            Command::Completion { .. } => "completion",
        }
    }

    fn wants_structured_errors(&self) -> bool {
        matches!(
            self,
            Command::Update { json: true, .. }
                | Command::Search { json: true, .. }
                | Command::Threads { json: true, .. }
                | Command::Show { json: true, .. }
                | Command::Expand { json: true, .. }
                | Command::Transcript { json: true, .. }
                | Command::Import { json: true, .. }
                | Command::RawBlobs {
                    command: RawBlobCommand::Missing { json: true, .. }
                        | RawBlobCommand::Import { json: true, .. },
                }
                | Command::Status { json: true, .. }
        )
    }
}

fn print_completion(shell: Shell) {
    let mut command = Cli::command();
    clap_complete::generate(shell, &mut command, "super-cass", &mut io::stdout());
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
struct RawBlobMissingOutput {
    count: usize,
    hashes: Vec<String>,
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
    mode: &'static str,
    corpus: String,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    machine: Option<String>,
    hostname: Option<String>,
}

#[derive(Debug, Serialize)]
struct SearchResultOutput {
    #[serde(rename = "ref")]
    ref_id: String,
    history_item_id: Option<String>,
    match_type: search::MatchType,
    event_id: String,
    session_id: String,
    machine_id: String,
    source_kind: String,
    tier: Option<String>,
    kind: String,
    score: f64,
    lexical_rank: Option<usize>,
    semantic_rank: Option<usize>,
    occurred_at: Option<chrono::DateTime<chrono::Utc>>,
    session_title: Option<String>,
    workspace_values: Vec<String>,
    snippet: String,
}

#[derive(Debug, Serialize)]
struct ThreadsOutput {
    options: ThreadsOptionsOutput,
    results: Vec<ThreadOutput>,
    next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ThreadsOptionsOutput {
    limit: usize,
    sort: &'static str,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    scope: ThreadScopeOutput,
}

#[derive(Debug, Serialize)]
struct ThreadScopeOutput {
    mode: &'static str,
    path: Option<String>,
    inferred: bool,
}

#[derive(Debug, Serialize)]
struct ThreadOutput {
    session_id: String,
    source_kind: String,
    title: Option<String>,
    started_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    first_event_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    last_activity_at: Option<DateTime<Utc>>,
    event_count: u64,
    workspace_path: Option<String>,
    workspace_values: Vec<String>,
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
    repair: bool,
) -> Result<UpdateOutput> {
    let ingest = ingest::update_local(
        store,
        &config.machine_id,
        ingest::UpdateOptions { max_files, source },
    )?;
    let projected = refresh_search_after_update(store, &ingest.delta, repair)?;
    let embeddings = refresh_embeddings_after_update(store, config, &ingest.delta, repair)?;
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
    repair: bool,
) -> Result<UpdateOutput> {
    let progress = ProgressUi::new();
    let mut scan = progress.phase("Scanning local agent logs");
    let ingest = ingest::update_local_with_progress(
        store,
        &config.machine_id,
        ingest::UpdateOptions { max_files, source },
        |event| scan.update(update_progress_detail(event)),
    )?;
    scan.finish(format!(
        "{} files, {} new events, {} unchanged, {} errors",
        format_count(ingest.files_seen),
        format_count(ingest.inserted),
        format_count(ingest.skipped_unchanged),
        format_count(ingest.errors)
    ));

    let mut index = progress.phase(if repair {
        "Repairing search index"
    } else {
        "Updating search index"
    });
    let projected =
        refresh_search_after_update_with_progress(store, &ingest.delta, repair, |detail| {
            index.update(detail);
        })?;
    index.finish(format!("{} events indexed", format_count(projected)));

    let mut embed = progress.phase(if repair {
        "Repairing embeddings"
    } else {
        "Updating embeddings"
    });
    let embeddings = refresh_embeddings_after_update_with_progress(
        store,
        config,
        &ingest.delta,
        repair,
        |event| {
            embed.update(embedding_progress_detail(event));
        },
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

fn refresh_search_after_update(
    store: &Store,
    delta: &crate::storage::ImportDelta,
    repair: bool,
) -> Result<usize> {
    refresh_search_after_update_with_progress(store, delta, repair, |_| {})
}

fn refresh_search_after_update_with_progress(
    store: &Store,
    delta: &crate::storage::ImportDelta,
    repair: bool,
    mut progress: impl FnMut(String),
) -> Result<usize> {
    if repair {
        progress("repairing all indexable events".to_string());
        let indexed = search::refresh(store)?;
        Ok(indexed)
    } else {
        progress(format!(
            "indexing {} new events",
            format_count(delta.inserted_events.len())
        ));
        let mut last_index_progress = Instant::now();
        let mut last_index_report: Option<(usize, usize)> = None;
        let indexed = store.refresh_search_index_for_events_with_progress(
            crate::embed::HashEmbedder::MODEL_ID,
            crate::embed::HashEmbedder::DIMS,
            &delta.inserted_events,
            crate::embed::hash_embed,
            |processed, total| {
                if processed == 1
                    || processed == total
                    || last_index_progress.elapsed() >= Duration::from_secs(1)
                {
                    let report = (processed, total);
                    if last_index_report != Some(report) {
                        progress(format!(
                            "indexed {}/{} new events",
                            format_count(processed),
                            format_count(total)
                        ));
                        last_index_progress = Instant::now();
                        last_index_report = Some(report);
                    }
                }
            },
        )?;
        progress("checking index health".to_string());
        let indexed = if store.search_index_needs_repair(crate::embed::HashEmbedder::MODEL_ID)? {
            progress("repairing missing search index rows".to_string());
            search::refresh(store)
        } else {
            Ok(indexed)
        }?;
        if store.history_items_projection_ready()? {
            progress("projecting changed history items".to_string());
            store.refresh_history_items_for_events(&delta.touched_events)?;
        } else {
            progress("projecting history items".to_string());
            store.refresh_history_items()?;
        }
        Ok(indexed)
    }
}

fn refresh_embeddings_after_update(
    store: &Store,
    config: &AppConfig,
    delta: &crate::storage::ImportDelta,
    repair: bool,
) -> Result<search::EmbeddingRefresh> {
    refresh_embeddings_after_update_with_progress(store, config, delta, repair, |_| {})
}

fn refresh_embeddings_after_update_with_progress(
    store: &Store,
    config: &AppConfig,
    delta: &crate::storage::ImportDelta,
    repair: bool,
    progress: impl FnMut(&search::EmbeddingProgress),
) -> Result<search::EmbeddingRefresh> {
    if repair {
        search::refresh_embeddings_repair_with_progress(
            store,
            &config.machine_id,
            &config.embedder,
            progress,
        )
    } else {
        search::refresh_embeddings_incremental_with_progress(
            store,
            &config.machine_id,
            &config.embedder,
            delta,
            progress,
        )
    }
}

fn run_import_once(store: &Store, config: &AppConfig, input: &str) -> Result<ImportOutput> {
    let stats = transport::import_jsonl_path(store, input)?;
    let projected = search::refresh_incremental(store, &stats.delta)?;
    let embeddings = search::refresh_embeddings_incremental(
        store,
        &config.machine_id,
        &config.embedder,
        &stats.delta,
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
    let mut import = progress.phase("Importing history stream");
    let mut last = transport::JsonlProgress::default();
    let stats = transport::import_jsonl_path_with_progress(store, input, |event| {
        last = event;
        import.update(jsonl_progress_detail(event));
    })?;
    import.finish(format!(
        "{} new records, {} duplicates, read {} records, {}",
        format_count(stats.inserted),
        format_count(stats.duplicates),
        format_count(last.records),
        format_bytes(last.bytes)
    ));

    let index = progress.phase("Updating search index");
    let projected = search::refresh_incremental(store, &stats.delta)?;
    index.finish(format!("{} events indexed", format_count(projected)));

    let embed = progress.phase("Updating embeddings");
    let embeddings = search::refresh_embeddings_incremental(
        store,
        &config.machine_id,
        &config.embedder,
        &stats.delta,
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
    let mode = if let Some(reason) = embeddings.deferred_reason.as_deref() {
        format!("deferred ({reason})")
    } else {
        embeddings
            .degraded_reason
            .as_deref()
            .map(|reason| format!("degraded ({reason})"))
            .unwrap_or_else(|| "ready".to_string())
    };
    let reductions = if embeddings.batch_size_reductions == 0 {
        "none".to_string()
    } else {
        format_count(embeddings.batch_size_reductions)
    };
    print_section(
        "Embeddings",
        &[
            ("New embeddings", format_count(embeddings.embedded)),
            ("Indexed vectors", format_count(embeddings.vectors_indexed)),
            ("Pending", format_count(embeddings.pending)),
            ("Batch reductions", reductions),
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
    if let Some(reason) = &embeddings.deferred_reason {
        format!(
            "deferred: {reason}; {} pending, {} new embeddings",
            format_count(embeddings.pending),
            format_count(embeddings.embedded)
        )
    } else if let Some(reason) = &embeddings.degraded_reason {
        if embeddings.pending > 0 {
            format!(
                "degraded: {reason}; {} pending",
                format_count(embeddings.pending)
            )
        } else {
            format!("degraded: {reason}")
        }
    } else {
        let detail = format!(
            "{} new embeddings, {} vectors indexed",
            format_count(embeddings.embedded),
            format_count(embeddings.vectors_indexed)
        );
        if embeddings.batch_size_reductions > 0 {
            format!(
                "{detail}, batch reduced {} times",
                format_count(embeddings.batch_size_reductions)
            )
        } else {
            detail
        }
    }
}

fn embedding_progress_detail(event: &search::EmbeddingProgress) -> String {
    match event {
        search::EmbeddingProgress::LoadingModel { model_id } => {
            format!("loading model {model_id}")
        }
        search::EmbeddingProgress::Batch {
            embedded,
            pending,
            batch_size,
            reductions,
            available_gib,
        } => {
            let memory = available_gib
                .map(|value| format!(", {value:.1} GiB available"))
                .unwrap_or_default();
            format!(
                "{} embedded, {} pending, batch {}{}{}",
                format_count(*embedded),
                format_count(*pending),
                format_count(*batch_size),
                if *reductions > 0 { ", reduced" } else { "" },
                memory
            )
        }
        search::EmbeddingProgress::Deferred { pending, reason } => format!(
            "deferred {pending} pending embeddings: {reason}",
            pending = format_count(*pending)
        ),
    }
}

fn update_progress_detail(event: &ingest::UpdateProgress) -> String {
    match event {
        ingest::UpdateProgress::Discovered {
            sources,
            selected_files,
        } => {
            let source_text = sources
                .iter()
                .filter(|source| source.found_files > 0)
                .map(|source| {
                    if source.selected_files == source.found_files {
                        format!("{} {}", source.kind, format_count(source.found_files))
                    } else {
                        format!(
                            "{} {}/{}",
                            source.kind,
                            format_count(source.selected_files),
                            format_count(source.found_files)
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            if source_text.is_empty() {
                "no local agent logs found".to_string()
            } else {
                format!(
                    "found {} files across {}",
                    format_count(*selected_files),
                    source_text
                )
            }
        }
        ingest::UpdateProgress::Processing {
            kind,
            path,
            file_index,
            total_files,
            source_file_index,
            source_file_count,
            stats,
        } => format!(
            "{} {}/{} file {}/{} {}; {} new, {} unchanged, {} errors",
            kind,
            format_count(*source_file_index),
            format_count(*source_file_count),
            format_count(*file_index),
            format_count(*total_files),
            compact_path(path),
            format_count(stats.inserted),
            format_count(stats.skipped_unchanged),
            format_count(stats.errors)
        ),
        ingest::UpdateProgress::CompletedFile {
            kind,
            path,
            file_index,
            total_files,
            source_file_index,
            source_file_count,
            stats,
        } => format!(
            "{} {}/{} file {}/{} done {}; {} new, {} unchanged, {} errors",
            kind,
            format_count(*source_file_index),
            format_count(*source_file_count),
            format_count(*file_index),
            format_count(*total_files),
            compact_path(path),
            format_count(stats.inserted),
            format_count(stats.skipped_unchanged),
            format_count(stats.errors)
        ),
    }
}

fn compact_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    let home = std::env::var("HOME").ok();
    let text = home
        .as_deref()
        .and_then(|home| text.strip_prefix(home).map(|rest| format!("~{rest}")))
        .unwrap_or_else(|| text.to_string());
    const MAX_CHARS: usize = 72;
    let char_count = text.chars().count();
    if char_count <= MAX_CHARS {
        text
    } else {
        let tail = text
            .chars()
            .skip(char_count.saturating_sub(MAX_CHARS - 3))
            .collect::<String>();
        format!("...{tail}")
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

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn jsonl_progress_detail(progress: transport::JsonlProgress) -> String {
    format!(
        "streamed {} records, {}",
        format_count(progress.records),
        format_bytes(progress.bytes)
    )
}

fn styled(text: &str, code: &str, color: bool) -> String {
    if color {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn should_color(no_color: bool, color: Option<ColorArg>, robot: bool) -> bool {
    if no_color || robot {
        return false;
    }
    match color.unwrap_or(ColorArg::Auto) {
        ColorArg::Auto => std::io::stdout().is_terminal(),
        ColorArg::Always => true,
        ColorArg::Never => false,
    }
}

fn include_embedding_records(mode: EmbeddingExportMode, no_embeddings: bool) -> bool {
    !no_embeddings && matches!(mode, EmbeddingExportMode::Include)
}

fn include_raw_artifact_records(mode: RawArtifactExportMode, no_raw_artifacts: bool) -> bool {
    !no_raw_artifacts && !matches!(mode, RawArtifactExportMode::Omit)
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
    detail: Option<Arc<Mutex<String>>>,
    last_update_emit: Instant,
    emitted_update: bool,
    stop: Option<mpsc::Sender<()>>,
    handle: Option<thread::JoinHandle<()>>,
    finished: bool,
}

impl ProgressPhase {
    fn start(label: &str, interactive: bool) -> Self {
        let started = Instant::now();
        if interactive {
            let label_for_thread = label.to_string();
            let detail = Arc::new(Mutex::new(String::new()));
            let detail_for_thread = Arc::clone(&detail);
            let (tx, rx) = mpsc::channel();
            let handle = thread::spawn(move || {
                let frames = ["-", "\\", "|", "/"];
                let mut idx = 0usize;
                loop {
                    let detail = detail_for_thread
                        .lock()
                        .ok()
                        .map(|detail| detail.clone())
                        .unwrap_or_default();
                    let detail =
                        fit_progress_detail(&label_for_thread, &detail, terminal_columns());
                    let suffix = if detail.is_empty() {
                        String::new()
                    } else {
                        format!(" {detail}")
                    };
                    eprint!(
                        "\r\x1b[2K\x1b[36m{}\x1b[0m {}...{} ",
                        frames[idx % frames.len()],
                        label_for_thread,
                        suffix
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
                detail: Some(detail),
                last_update_emit: started,
                emitted_update: false,
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
                detail: None,
                last_update_emit: started,
                emitted_update: false,
                stop: None,
                handle: None,
                finished: false,
            }
        }
    }

    fn update(&mut self, detail: String) {
        if let Some(shared) = &self.detail {
            if let Ok(mut current) = shared.lock() {
                *current = detail.clone();
            }
        }
        if !self.interactive
            && (!self.emitted_update || self.last_update_emit.elapsed() >= Duration::from_secs(2))
        {
            eprintln!("{}: {}", self.label, detail);
            self.last_update_emit = Instant::now();
            self.emitted_update = true;
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

fn fit_progress_detail(label: &str, detail: &str, columns: usize) -> String {
    if detail.is_empty() {
        return String::new();
    }

    let frame_width = 1usize;
    let separators_width = 1usize + 3usize + 1usize;
    let trailing_space_width = 1usize;
    let base_width = frame_width + separators_width + label.chars().count() + trailing_space_width;
    let budget = columns.saturating_sub(base_width).saturating_sub(1);
    ellipsize_middle(detail, budget)
}

fn ellipsize_middle(value: &str, max_chars: usize) -> String {
    let total = value.chars().count();
    if total <= max_chars {
        return value.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }

    let keep = max_chars - 3;
    let left = keep / 2;
    let right = keep - left;
    let start = value.chars().take(left).collect::<String>();
    let end = value
        .chars()
        .rev()
        .take(right)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{start}...{end}")
}

fn terminal_columns() -> usize {
    terminal_columns_from_stderr()
        .or_else(|| {
            std::env::var("COLUMNS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
        })
        .filter(|columns| *columns > 0)
        .unwrap_or(80)
}

#[cfg(unix)]
fn terminal_columns_from_stderr() -> Option<usize> {
    use std::os::fd::AsRawFd;

    let mut size = libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let result = unsafe { libc::ioctl(std::io::stderr().as_raw_fd(), libc::TIOCGWINSZ, &mut size) };
    if result == 0 && size.ws_col > 0 {
        Some(size.ws_col as usize)
    } else {
        None
    }
}

#[cfg(not(unix))]
fn terminal_columns_from_stderr() -> Option<usize> {
    None
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
    println!("history_items={}", output.stats.history_items);
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

fn print_threads_output(scope: &ThreadScope, threads: &[crate::storage::ThreadRow], color: bool) {
    println!();
    println!("{}", styled("Threads", "1;32", color));
    match scope.mode {
        ThreadScopeMode::All => println!("scope: all projects"),
        ThreadScopeMode::Path => {
            println!("scope: {}", scope.path.as_deref().unwrap_or("unknown"));
        }
    }
    if threads.is_empty() {
        println!();
        println!("No threads found.");
        return;
    }
    println!();
    for thread in threads {
        let when = thread
            .last_activity_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{}  {}  {}",
            styled(&when, "1;36", color),
            thread.session.source_kind,
            thread
                .session
                .title
                .as_deref()
                .unwrap_or("(untitled thread)")
        );
        println!("  session: {}", thread.session.id);
        println!("  events: {}", thread.event_count);
        if let Some(workspace) = &thread.workspace_path {
            println!("  project: {workspace}");
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

fn threads_output(
    limit: usize,
    sort: ThreadSort,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    scope: &ThreadScope,
    threads: &[crate::storage::ThreadRow],
) -> ThreadsOutput {
    let results = threads.iter().map(thread_output).collect::<Vec<_>>();
    ThreadsOutput {
        options: ThreadsOptionsOutput {
            limit,
            sort: sort.as_str(),
            after,
            before,
            scope: ThreadScopeOutput {
                mode: scope.mode.as_str(),
                path: scope.path.clone(),
                inferred: scope.inferred,
            },
        },
        next_commands: results
            .first()
            .map(|thread| {
                vec![format!(
                    "super-cass transcript {} --json",
                    thread.session_id
                )]
            })
            .unwrap_or_else(|| vec!["super-cass update --json".to_string()]),
        results,
    }
}

fn thread_output(thread: &crate::storage::ThreadRow) -> ThreadOutput {
    ThreadOutput {
        session_id: thread.session.id.clone(),
        source_kind: thread.session.source_kind.clone(),
        title: thread.session.title.clone(),
        started_at: thread.session.started_at,
        updated_at: thread.session.updated_at,
        first_event_at: thread.first_event_at,
        last_event_at: thread.last_event_at,
        last_activity_at: thread.last_activity_at,
        event_count: thread.event_count,
        workspace_path: thread.workspace_path.clone(),
        workspace_values: thread.workspace_values.clone(),
    }
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
    mode: search::SearchMode,
    recency_bias: f64,
    corpus: &search::SearchCorpus,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    machine: Option<String>,
    hostname: Option<String>,
    response: &search::SearchResponse,
    refs: &[String],
) -> SearchOutput {
    SearchOutput {
        query: query.to_string(),
        options: SearchOptionsOutput {
            limit,
            sort: sort.as_str(),
            mode: mode.as_str(),
            corpus: corpus.as_csv(),
            recency_bias,
            after,
            before,
            machine,
            hostname,
        },
        degraded_reason: response.degraded_reason.clone(),
        results: response
            .results
            .iter()
            .enumerate()
            .map(|(idx, result)| SearchResultOutput {
                ref_id: refs.get(idx).cloned().unwrap_or_else(|| "-".to_string()),
                history_item_id: result.history_item_id.clone(),
                match_type: result.match_type,
                event_id: result.event_id.clone(),
                session_id: result.session_id.clone(),
                machine_id: result.machine_id.clone(),
                source_kind: result.source_kind.clone(),
                tier: result.tier.clone(),
                kind: result.kind.clone(),
                score: result.score,
                lexical_rank: result.lexical_rank,
                semantic_rank: result.semantic_rank,
                occurred_at: result.occurred_at,
                session_title: result.session_title.clone(),
                workspace_values: result.workspace_values.clone(),
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

impl ThreadSort {
    fn as_str(self) -> &'static str {
        match self {
            ThreadSort::Newest => "newest",
            ThreadSort::Oldest => "oldest",
        }
    }
}

#[derive(Debug, Clone)]
struct ThreadScope {
    mode: ThreadScopeMode,
    path: Option<String>,
    inferred: bool,
}

#[derive(Debug, Clone, Copy)]
enum ThreadScopeMode {
    All,
    Path,
}

impl ThreadScopeMode {
    fn as_str(self) -> &'static str {
        match self {
            ThreadScopeMode::All => "all",
            ThreadScopeMode::Path => "path",
        }
    }
}

fn resolve_thread_scope(project: Option<&std::path::Path>, all: bool) -> Result<ThreadScope> {
    if all {
        return Ok(ThreadScope {
            mode: ThreadScopeMode::All,
            path: None,
            inferred: false,
        });
    }
    if let Some(project) = project {
        return Ok(ThreadScope {
            mode: ThreadScopeMode::Path,
            path: Some(transport::normalize_workspace_arg(project)),
            inferred: false,
        });
    }
    let cwd = std::env::current_dir()?;
    Ok(ThreadScope {
        mode: ThreadScopeMode::Path,
        path: Some(transport::normalize_workspace_arg(&cwd)),
        inferred: true,
    })
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
            Column::Tier,
            Column::Kind,
            Column::When,
            Column::Title,
            Column::Preview,
            Column::Machine,
            Column::Session,
            Column::Event,
            Column::Item,
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

fn resolve_search_corpus(
    corpus: Option<String>,
    include_tools: bool,
    raw: bool,
) -> Result<search::SearchCorpus> {
    if corpus.is_some() && (include_tools || raw) {
        bail!("use either --corpus or a corpus shortcut, not both");
    }
    if include_tools && raw {
        bail!("use either --include-tools or --raw, not both");
    }
    if let Some(corpus) = corpus {
        return search::SearchCorpus::parse(&corpus);
    }
    if include_tools {
        return Ok(search::SearchCorpus::conversation_with_tools());
    }
    if raw {
        return Ok(search::SearchCorpus::raw());
    }
    Ok(search::SearchCorpus::default())
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

fn search_limit(limit: Option<usize>, fzf: bool) -> usize {
    limit.unwrap_or(if fzf {
        DEFAULT_FZF_LIMIT
    } else {
        DEFAULT_SEARCH_LIMIT
    })
}

#[derive(Debug, Clone, Copy)]
enum TimeFilterBound {
    After,
    Before,
}

fn parse_optional_search_time(
    value: Option<&str>,
    bound: TimeFilterBound,
) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| parse_search_time(value, bound))
        .transpose()
}

fn parse_search_time(value: &str, bound: TimeFilterBound) -> Result<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("time filter cannot be empty");
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(dt.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, format) {
            return local_naive_to_utc(dt);
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return local_date_bound_to_utc(date, bound);
    }

    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "now" => return Ok(Local::now().with_timezone(&Utc)),
        "today" => return local_date_bound_to_utc(Local::now().date_naive(), bound),
        "yesterday" => {
            return local_date_bound_to_utc(
                Local::now().date_naive() - chrono::Duration::days(1),
                bound,
            );
        }
        _ => {}
    }
    if let Some(dt) = parse_relative_ago(&lower)? {
        return Ok(dt);
    }
    bail!("could not parse time filter: {trimmed}");
}

fn parse_relative_ago(value: &str) -> Result<Option<DateTime<Utc>>> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 || parts[2] != "ago" {
        return Ok(None);
    }
    let amount = parts[0]
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("could not parse relative time amount: {}", parts[0]))?;
    if amount < 0 {
        bail!("relative time amount must be positive");
    }
    let duration = match parts[1].trim_end_matches('s') {
        "minute" => chrono::Duration::minutes(amount),
        "hour" => chrono::Duration::hours(amount),
        "day" => chrono::Duration::days(amount),
        "week" => chrono::Duration::weeks(amount),
        unit => bail!("unsupported relative time unit: {unit}"),
    };
    Ok(Some((Local::now() - duration).with_timezone(&Utc)))
}

fn local_date_bound_to_utc(date: NaiveDate, bound: TimeFilterBound) -> Result<DateTime<Utc>> {
    let date = match bound {
        TimeFilterBound::After => date,
        TimeFilterBound::Before => date
            .succ_opt()
            .ok_or_else(|| anyhow::anyhow!("date is too large for --before"))?,
    };
    let naive = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow::anyhow!("could not build local day boundary"))?;
    local_naive_to_utc(naive)
}

fn local_naive_to_utc(value: NaiveDateTime) -> Result<DateTime<Utc>> {
    match Local.from_local_datetime(&value) {
        LocalResult::Single(dt) => Ok(dt.with_timezone(&Utc)),
        LocalResult::Ambiguous(earliest, _) => Ok(earliest.with_timezone(&Utc)),
        LocalResult::None => bail!("local time does not exist: {value}"),
    }
}

fn run_static_fzf_search(config: &AppConfig, query: &str, rows: &str, color: bool) -> Result<()> {
    ensure_fzf_available()?;
    let preview = fzf_preview_command(config, color);
    let open = fzf_open_command(config, color);
    let mut child = base_fzf_command()
        .arg("--query")
        .arg(query)
        .arg("--header")
        .arg("Ref\tSource\tMatch\tWhen\tPreview")
        .arg("--preview")
        .arg(preview)
        .arg("--bind")
        .arg(format!("enter:execute({open})+abort"))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-up", "preview-up", 10))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-down", "preview-down", 10))
        .arg("--bind")
        .arg("ctrl-u:preview-half-page-up")
        .arg("--bind")
        .arg("ctrl-d:preview-half-page-down")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(rows.as_bytes())?;
    }
    let status = child.wait()?;
    if !status.success() {
        return Ok(());
    }
    Ok(())
}

fn run_tui_search(
    config: &AppConfig,
    query: &str,
    server: &str,
    limit: usize,
    sort: SearchSort,
    mode: search::SearchMode,
    corpus: search::SearchCorpus,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    machine: Option<String>,
    hostname: Option<String>,
    color: bool,
) -> Result<()> {
    ensure_fzf_available()?;
    ensure_curl_available()?;
    let _server = ensure_server_available(config, server)?;
    let reload = tui_reload_command(
        server,
        limit,
        sort,
        mode,
        &corpus,
        recency_bias,
        after,
        before,
        machine.as_deref(),
        hostname.as_deref(),
    )?;
    let preview = fzf_preview_command(config, color);
    let open = fzf_open_command(config, color);
    let mut child = base_fzf_command()
        .arg("--query")
        .arg(query)
        .arg("--header")
        .arg("Ref\tSource\tMatch\tWhen\tPreview")
        .arg("--preview")
        .arg(preview)
        .arg("--bind")
        .arg(format!("start:reload({reload})"))
        .arg("--bind")
        .arg(format!(
            "change:reload(sleep {LIVE_SEARCH_RELOAD_DELAY_SECS}; {reload})"
        ))
        .arg("--bind")
        .arg(format!("enter:execute({open})+abort"))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-up", "preview-up", 10))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-down", "preview-down", 10))
        .arg("--bind")
        .arg("ctrl-u:preview-half-page-up")
        .arg("--bind")
        .arg("ctrl-d:preview-half-page-down")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()?;
    let status = child.wait()?;
    if !status.success() {
        return Ok(());
    }
    Ok(())
}

fn base_fzf_command() -> ProcessCommand {
    let mut command = ProcessCommand::new("fzf");
    command
        .arg("--ansi")
        .arg("--no-hscroll")
        .arg("--delimiter")
        .arg("\t")
        .arg("--nth")
        .arg("1,2,3,4,5,8,9,10")
        .arg("--with-nth")
        .arg("1,2,3,4,5");
    command
}

fn fzf_preview_command(config: &AppConfig, color: bool) -> String {
    let current_exe = std::env::current_exe().ok();
    let exe = current_exe
        .as_ref()
        .map(|path| shell_quote(&path.to_string_lossy()))
        .unwrap_or_else(|| "super-cass".to_string());
    let data_dir = shell_quote(&config.data_dir.to_string_lossy());
    let color_flag = if color {
        " --color always"
    } else {
        " --no-color"
    };
    format!("{exe} --data-dir {data_dir} show {{7}} {{12}} --before 3 --after 5{color_flag}")
}

fn fzf_open_command(config: &AppConfig, color: bool) -> String {
    let current_exe = std::env::current_exe().ok();
    let exe = current_exe
        .as_ref()
        .map(|path| shell_quote(&path.to_string_lossy()))
        .unwrap_or_else(|| "super-cass".to_string());
    let data_dir = shell_quote(&config.data_dir.to_string_lossy());
    let color_flag = if color {
        " --color auto"
    } else {
        " --no-color"
    };
    format!("{exe} --data-dir {data_dir} transcript {{6}} --at {{7}} {{12}}{color_flag}")
}

fn tui_reload_command(
    server: &str,
    limit: usize,
    sort: SearchSort,
    mode: search::SearchMode,
    corpus: &search::SearchCorpus,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    machine: Option<&str>,
    hostname: Option<&str>,
) -> Result<String> {
    let search_url = shell_quote(&server_url(server, "search")?);
    let after_arg = after
        .map(|dt| {
            format!(
                " --data-urlencode {}",
                shell_quote(&format!("after={}", dt.to_rfc3339()))
            )
        })
        .unwrap_or_default();
    let before_arg = before
        .map(|dt| {
            format!(
                " --data-urlencode {}",
                shell_quote(&format!("before={}", dt.to_rfc3339()))
            )
        })
        .unwrap_or_default();
    let machine_arg = machine
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            format!(
                " --data-urlencode {}",
                shell_quote(&format!("machine={value}"))
            )
        })
        .unwrap_or_default();
    let hostname_arg = hostname
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            format!(
                " --data-urlencode {}",
                shell_quote(&format!("hostname={value}"))
            )
        })
        .unwrap_or_default();
    Ok(format!(
        "if [ -z {{q}} ]; then :; else curl -fsSG {search_url} --data-urlencode q={{q}} --data-urlencode limit={limit} --data-urlencode sort={} --data-urlencode mode={} --data-urlencode corpus={} --data-urlencode recency_bias={recency_bias} --data-urlencode format=fzf{after_arg}{before_arg}{machine_arg}{hostname_arg}; fi",
        sort.as_str(),
        mode.as_str(),
        shell_quote(&corpus.as_csv())
    ))
}

fn ensure_fzf_available() -> Result<()> {
    if !command_exists("fzf") {
        bail!(
            "fzf is not installed. Use `super-cass search <query>` and then `super-cass show <ref>` or `super-cass transcript <ref>`."
        );
    }
    Ok(())
}

fn ensure_curl_available() -> Result<()> {
    if !command_exists("curl") {
        bail!("curl is not installed; tui uses curl to query the local search server");
    }
    Ok(())
}

struct StartedServer {
    child: Child,
}

impl Drop for StartedServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn ensure_server_available(config: &AppConfig, server: &str) -> Result<Option<StartedServer>> {
    if server_health_available(server)? {
        return Ok(None);
    }
    let Some(bind) = local_server_bind_addr(server) else {
        let health_url = server_url(server, "health")?;
        bail!("could not reach super-cass server at {health_url}");
    };
    let mut started = start_local_server(config, &bind)?;
    wait_for_started_server(server, &mut started)?;
    Ok(Some(started))
}

fn server_health_available(server: &str) -> Result<bool> {
    let health_url = server_url(server, "health")?;
    let status = ProcessCommand::new("curl")
        .arg("-fsS")
        .arg("--max-time")
        .arg("2")
        .arg(&health_url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    Ok(status.success())
}

fn start_local_server(config: &AppConfig, bind: &str) -> Result<StartedServer> {
    eprintln!("Starting local super-cass server at {bind}...");
    let exe = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("super-cass"));
    let child = ProcessCommand::new(exe)
        .arg("--data-dir")
        .arg(&config.data_dir)
        .arg("serve")
        .arg("--bind")
        .arg(bind)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(StartedServer { child })
}

fn wait_for_started_server(server: &str, started: &mut StartedServer) -> Result<()> {
    for _ in 0..40 {
        if server_health_available(server)? {
            return Ok(());
        }
        if let Some(status) = started.child.try_wait()? {
            bail!("local super-cass server exited before it became ready: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let health_url = server_url(server, "health")?;
    bail!("local super-cass server did not become ready at {health_url}")
}

fn server_url(server: &str, path: &str) -> Result<String> {
    let server = server.trim();
    if server.is_empty() {
        bail!("server URL cannot be empty");
    }
    Ok(format!("{}/{}", server.trim_end_matches('/'), path))
}

fn local_server_bind_addr(server: &str) -> Option<String> {
    let server = server.trim().trim_end_matches('/');
    let without_scheme = server
        .strip_prefix("http://")
        .or_else(|| server.strip_prefix("https://"))?;
    if without_scheme.contains('/') {
        return None;
    }
    if let Some(rest) = without_scheme.strip_prefix("127.0.0.1:") {
        return Some(format!("127.0.0.1:{rest}"));
    }
    if let Some(rest) = without_scheme.strip_prefix("localhost:") {
        return Some(format!("127.0.0.1:{rest}"));
    }
    if let Some(rest) = without_scheme.strip_prefix("[::1]:") {
        return Some(format!("[::1]:{rest}"));
    }
    None
}

fn preview_scroll_bind(key: &str, action: &str, count: usize) -> String {
    format!("{key}:{}", vec![action; count].join("+"))
}

fn fzf_rows_output(results: &[search::SearchResult], refs: &[String], color: bool) -> String {
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
        clean_fzf_field(result.session_title.as_deref().unwrap_or("")),
        clean_fzf_field(&result.workspace_values.join(" ")),
        clean_fzf_field(&result.machine_id),
        clean_fzf_field(result.history_item_id.as_deref().unwrap_or("")),
        clean_fzf_field(fzf_open_mode_flag(result)),
    ]
    .join("\t")
}

fn fzf_open_mode_flag(result: &search::SearchResult) -> &'static str {
    match result.tier.as_deref() {
        Some("conversation") => "",
        _ => "--full",
    }
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
            columns.push(Column::Item);
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
        "machine" | "machine_id" => Ok(Column::Machine),
        "event" | "event_id" => Ok(Column::Event),
        "session" | "session_id" => Ok(Column::Session),
        "item" | "history_item" | "history_item_id" => Ok(Column::Item),
        "tier" | "corpus" => Ok(Column::Tier),
        "kind" | "search_kind" => Ok(Column::Kind),
        _ => bail!(
            "unknown column '{name}'. Available columns: ref,source,match,when,title,preview,score,lex,sem,machine,event,session,item,tier,kind,ids"
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
        Column::Machine => result.machine_id.clone(),
        Column::Event => result.event_id.clone(),
        Column::Session => result.session_id.clone(),
        Column::Item => result
            .history_item_id
            .clone()
            .unwrap_or_else(|| "-".to_string()),
        Column::Tier => result.tier.clone().unwrap_or_else(|| "-".to_string()),
        Column::Kind => result.kind.clone(),
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
        Column::Machine => "Machine",
        Column::Event => "Event",
        Column::Session => "Session",
        Column::Item => "Item",
        Column::Tier => "Tier",
        Column::Kind => "Kind",
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
        let mut scan = progress.phase("Scanning local agent logs");
        let stats = ingest::update_local_with_progress(
            store,
            machine_id,
            ingest::UpdateOptions {
                max_files,
                source: source.clone(),
            },
            |event| scan.update(update_progress_detail(event)),
        )?;
        scan.finish(format!(
            "{} files, {} new events, {} unchanged, {} errors",
            format_count(stats.files_seen),
            format_count(stats.inserted),
            format_count(stats.skipped_unchanged),
            format_count(stats.errors)
        ));

        let index = progress.phase("Updating search index");
        let projected = search::refresh_incremental(store, &stats.delta)?;
        index.finish(format!("{} events indexed", format_count(projected)));

        let mut embed = progress.phase("Updating embeddings");
        let embeddings = search::refresh_embeddings_incremental_with_progress(
            store,
            machine_id,
            &embedder_config,
            &stats.delta,
            |event| embed.update(embedding_progress_detail(event)),
        )?;
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
                Column::Event,
                Column::Item
            ]
        );
    }

    #[test]
    fn corpus_shortcuts_resolve_to_clear_tier_sets() {
        assert_eq!(
            resolve_search_corpus(Some("tool,raw".to_string()), false, false)
                .expect("explicit corpus")
                .as_csv(),
            "tool,raw"
        );
        assert_eq!(
            resolve_search_corpus(None, true, false)
                .expect("tools shortcut")
                .as_csv(),
            "conversation,tool"
        );
        assert_eq!(
            resolve_search_corpus(None, false, true)
                .expect("raw shortcut")
                .as_csv(),
            "raw"
        );
        assert!(resolve_search_corpus(Some("conversation".to_string()), true, false).is_err());
        assert!(resolve_search_corpus(None, true, true).is_err());
    }

    #[test]
    fn embedding_progress_detail_reports_pending_batch_and_memory() {
        let detail = embedding_progress_detail(&search::EmbeddingProgress::Batch {
            embedded: 1200,
            pending: 3400,
            batch_size: 8,
            reductions: 2,
            available_gib: Some(1.2),
        });

        assert_eq!(
            detail,
            "1,200 embedded, 3,400 pending, batch 8, reduced, 1.2 GiB available"
        );
    }

    #[test]
    fn embedding_phase_detail_reports_deferred_pending_work() {
        let detail = embedding_phase_detail(&search::EmbeddingRefresh {
            embedded: 16,
            pending: 42,
            deferred_reason: Some("memory pressure: only 0.6 GiB appears available".to_string()),
            ..search::EmbeddingRefresh::default()
        });

        assert_eq!(
            detail,
            "deferred: memory pressure: only 0.6 GiB appears available; 42 pending, 16 new embeddings"
        );
    }

    #[test]
    fn progress_detail_is_capped_to_available_terminal_columns() {
        let detail = "codex 1,677/1,893 file 1,980/2,196 /home/example/.codex/sessions/rollout-2026-01-21T17-51-26-019be02e-4296-7b30-8260-9a97f387618f.jsonl; 44,364 new";
        let fitted = fit_progress_detail("Scanning local agent logs", detail, 80);

        assert!(fitted.chars().count() <= 47);
        assert!(fitted.starts_with("codex 1,677"));
        assert!(fitted.contains("..."));
        assert!(fitted.ends_with("364 new"));
    }

    #[test]
    fn fzf_row_keeps_stable_ids_hidden_after_visible_fields() {
        let result = search::SearchResult {
            history_item_id: Some("hi_1".to_string()),
            match_type: search::MatchType::Hybrid,
            event_id: "event_1".to_string(),
            session_id: "session_1".to_string(),
            machine_id: "machine_devbox_123".to_string(),
            source_kind: "codex".to_string(),
            tier: Some("conversation".to_string()),
            kind: "user".to_string(),
            score: 0.5,
            lexical_rank: Some(1),
            semantic_rank: Some(2),
            occurred_at: None,
            session_title: Some("Planning Session".to_string()),
            workspace_values: vec!["/home/example/projects/super-cass".to_string()],
            snippet: "preview with\nnew line".to_string(),
        };

        let row = fzf_row(&result, Some("ab3f"), false);
        let fields = row.split('\t').collect::<Vec<_>>();

        assert_eq!(fields.len(), 12);
        assert_eq!(fields[0], "ab3f");
        assert_eq!(fields[1], "codex");
        assert_eq!(fields[2], "hybrid");
        assert_eq!(fields[4], "preview with new line");
        assert_eq!(fields[5], "session_1");
        assert_eq!(fields[6], "event_1");
        assert_eq!(fields[7], "Planning Session");
        assert_eq!(fields[8], "/home/example/projects/super-cass");
        assert_eq!(fields[9], "machine_devbox_123");
        assert_eq!(fields[10], "hi_1");
        assert_eq!(fields[11], "");

        let mut raw_result = result;
        raw_result.tier = Some("raw".to_string());
        let raw_row = fzf_row(&raw_result, Some("ab3f"), false);
        let raw_fields = raw_row.split('\t').collect::<Vec<_>>();
        assert_eq!(raw_fields[11], "--full");
    }

    #[test]
    fn fzf_filters_visible_and_hidden_metadata_fields() {
        let command = base_fzf_command();
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();

        assert!(args
            .windows(2)
            .any(|pair| pair == ["--nth", "1,2,3,4,5,8,9,10"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--with-nth", "1,2,3,4,5"]));
        assert!(args.iter().any(|arg| arg == "--no-hscroll"));
    }

    #[test]
    fn search_json_output_includes_refs_and_next_commands() {
        let result = search::SearchResult {
            history_item_id: Some("hi_1".to_string()),
            match_type: search::MatchType::Hybrid,
            event_id: "event_1".to_string(),
            session_id: "session_1".to_string(),
            machine_id: "machine_devbox_123".to_string(),
            source_kind: "codex".to_string(),
            tier: Some("conversation".to_string()),
            kind: "user".to_string(),
            score: 0.5,
            lexical_rank: Some(1),
            semantic_rank: Some(2),
            occurred_at: None,
            session_title: Some("Session title".to_string()),
            workspace_values: vec!["/tmp/workspace".to_string()],
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
            search::SearchMode::Semantic,
            0.25,
            &search::SearchCorpus::conversation_with_tools(),
            None,
            None,
            Some("machine_devbox_123".to_string()),
            Some("devbox".to_string()),
            &response,
            &["ab3f".to_string()],
        );
        let value = serde_json::to_value(output).expect("serialize search output");

        assert_eq!(value["query"], "workspace metadata");
        assert_eq!(value["options"]["limit"], 20);
        assert_eq!(value["options"]["sort"], "relevance");
        assert_eq!(value["options"]["mode"], "semantic");
        assert_eq!(value["options"]["corpus"], "conversation,tool");
        assert_eq!(value["options"]["after"], serde_json::Value::Null);
        assert_eq!(value["options"]["before"], serde_json::Value::Null);
        assert_eq!(value["options"]["machine"], "machine_devbox_123");
        assert_eq!(value["options"]["hostname"], "devbox");
        assert_eq!(value["results"][0]["ref"], "ab3f");
        assert_eq!(value["results"][0]["history_item_id"], "hi_1");
        assert_eq!(value["results"][0]["event_id"], "event_1");
        assert_eq!(value["results"][0]["machine_id"], "machine_devbox_123");
        assert_eq!(value["results"][0]["tier"], "conversation");
        assert_eq!(value["results"][0]["kind"], "user");
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
    fn tui_accepts_missing_starting_query() {
        let cli = Cli::try_parse_from(["super-cass", "tui"]).expect("parse tui search");

        match cli.command {
            Command::Tui { query, limit, .. } => {
                assert_eq!(query, None);
                assert_eq!(limit, DEFAULT_LIVE_SEARCH_LIMIT);
            }
            _ => panic!("expected tui command"),
        }
    }

    #[test]
    fn fzf_uses_wider_default_limit_than_plain_search() {
        assert_eq!(search_limit(None, false), DEFAULT_SEARCH_LIMIT);
        assert_eq!(search_limit(None, true), DEFAULT_FZF_LIMIT);
        assert_eq!(search_limit(Some(25), true), 25);
    }

    #[test]
    fn preview_scroll_bind_repeats_standard_fzf_actions() {
        assert_eq!(
            preview_scroll_bind("shift-down", "preview-down", 3),
            "shift-down:preview-down+preview-down+preview-down"
        );
    }

    #[test]
    fn color_always_overrides_non_tty_output() {
        assert!(should_color(false, Some(ColorArg::Always), false));
        assert!(!should_color(false, Some(ColorArg::Never), false));
        assert!(!should_color(true, Some(ColorArg::Always), false));
        assert!(!should_color(false, Some(ColorArg::Always), true));
    }

    #[test]
    fn embedding_export_mode_defaults_include_and_can_omit() {
        assert!(include_embedding_records(
            EmbeddingExportMode::Include,
            false
        ));
        assert!(!include_embedding_records(EmbeddingExportMode::Omit, false));
        assert!(!include_embedding_records(
            EmbeddingExportMode::Include,
            true
        ));
    }

    #[test]
    fn raw_artifact_export_mode_defaults_include_and_can_omit() {
        assert!(include_raw_artifact_records(
            RawArtifactExportMode::Inline,
            false
        ));
        assert!(include_raw_artifact_records(
            RawArtifactExportMode::Metadata,
            false
        ));
        assert!(!include_raw_artifact_records(
            RawArtifactExportMode::Omit,
            false
        ));
        assert!(!include_raw_artifact_records(
            RawArtifactExportMode::Inline,
            true
        ));
    }

    #[test]
    fn jsonl_progress_detail_formats_records_and_bytes() {
        assert_eq!(format_bytes(999), "999 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(
            jsonl_progress_detail(transport::JsonlProgress {
                records: 1234,
                bytes: 5 * 1024 * 1024,
            }),
            "streamed 1,234 records, 5.0 MiB"
        );
    }

    #[test]
    fn fzf_preview_forces_color_when_enabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        let with_color = fzf_preview_command(&config, true);
        let without_color = fzf_preview_command(&config, false);
        let open = fzf_open_command(&config, true);

        assert!(with_color.contains("show {7} {12}"));
        assert!(open.contains("transcript {6} --at {7} {12}"));
        assert!(with_color.contains("--color always"));
        assert!(!with_color.contains("--no-color"));
        assert!(without_color.contains("--no-color"));
    }

    #[test]
    fn date_only_time_filters_use_local_day_bounds() {
        let after = parse_search_time("2026-04-20", TimeFilterBound::After).expect("after");
        let before = parse_search_time("2026-04-20", TimeFilterBound::Before).expect("before");

        assert!(before > after);
        assert_eq!(before - after, chrono::Duration::days(1));
    }

    #[test]
    fn relative_ago_time_filters_parse() {
        let parsed = parse_search_time("3 days ago", TimeFilterBound::After).expect("relative");
        let now = Utc::now();

        assert!(parsed < now);
        assert!(parsed > now - chrono::Duration::days(4));
    }

    #[test]
    fn live_reload_command_passes_normalized_time_values() {
        let after = DateTime::parse_from_rfc3339("2026-04-20T00:00:00Z")
            .expect("after")
            .with_timezone(&Utc);
        let command = tui_reload_command(
            DEFAULT_SERVER_URL,
            25,
            SearchSort::Newest,
            search::SearchMode::Lexical,
            &search::SearchCorpus::conversation_with_tools(),
            0.25,
            Some(after),
            None,
            Some("machine_devbox_123"),
            Some("devbox"),
        )
        .expect("reload command");

        assert!(command.contains("curl -fsSG"));
        assert!(command.contains("sort=newest"));
        assert!(command.contains("mode=lexical"));
        assert!(command.contains("corpus='conversation,tool'"));
        assert!(command.contains("recency_bias=0.25"));
        assert!(command.contains("after=2026-04-20T00:00:00+00:00"));
        assert!(command.contains("machine=machine_devbox_123"));
        assert!(command.contains("hostname=devbox"));
    }

    #[test]
    fn tui_auto_start_only_targets_local_servers() {
        assert_eq!(
            local_server_bind_addr("http://127.0.0.1:7391").as_deref(),
            Some("127.0.0.1:7391")
        );
        assert_eq!(
            local_server_bind_addr("http://localhost:7391").as_deref(),
            Some("127.0.0.1:7391")
        );
        assert_eq!(
            local_server_bind_addr("http://[::1]:7391").as_deref(),
            Some("[::1]:7391")
        );
        assert_eq!(local_server_bind_addr("https://example.com:7391"), None);
        assert_eq!(local_server_bind_addr("http://127.0.0.1:7391/path"), None);
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
