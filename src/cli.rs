use crate::config::AppConfig;
use crate::ingest;
use crate::search;
use crate::server;
use crate::storage::{RecentResultRefInput, Store, ThreadListOptions, ThreadSortMode};
use crate::transport;
use anyhow::{bail, Result};
use chrono::{DateTime, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Utc};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::{self, IsTerminal, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_SEARCH_LIMIT: usize = 10;
const DEFAULT_THREAD_LIMIT: usize = 10;
const DEFAULT_FZF_LIMIT: usize = 25;
const DEFAULT_LIVE_SEARCH_LIMIT: usize = 50;
const DEFAULT_TAIL_LINES: usize = 20;
const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:7391";
const DEFAULT_SERVER_BIND: &str = "127.0.0.1:7391";
const LIVE_SEARCH_RELOAD_DELAY_SECS: f32 = 0.35;
static TAIL_CANCELLED: AtomicBool = AtomicBool::new(false);
static TAIL_LOCK_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Parser)]
#[command(name = "histo", version)]
#[command(about = "Search and sync local coding-agent transcripts")]
pub struct Cli {
    #[arg(
        long,
        env = "HISTO_DATA_DIR",
        help = "Use a custom Historious data directory"
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
        #[arg(
            long,
            value_delimiter = ',',
            help = "Scan these source kinds; may be repeated or comma-separated"
        )]
        source: Vec<String>,
        #[arg(long, help = "Fully reconcile derived search and vector indexes")]
        repair: bool,
        #[arg(
            short = 'e',
            long,
            conflicts_with = "no_embeddings",
            help = "Use embedding-backed semantic indexing for this run"
        )]
        embeddings: bool,
        #[arg(
            short = 'E',
            long,
            conflicts_with = "embeddings",
            help = "Skip embedding work for this run"
        )]
        no_embeddings: bool,
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
    },
    /// Search indexed transcripts.
    Search {
        #[arg(
            value_name = "QUERY",
            help = "Words, paths, errors, or other details to search for"
        )]
        query: Vec<String>,
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
            long = "match",
            value_enum,
            help = "How to combine multiple query terms: 'and' or 'or'"
        )]
        match_mode: Option<SearchMatchArg>,
        #[arg(
            short = 'e',
            long,
            conflicts_with = "no_embeddings",
            help = "Use embedding-backed semantic search for this run"
        )]
        embeddings: bool,
        #[arg(
            short = 'E',
            long,
            conflicts_with = "embeddings",
            help = "Skip embedding-backed semantic search for this run"
        )]
        no_embeddings: bool,
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
            alias = "no-collapse",
            help = "Show duplicate/forked matches instead of collapsing them"
        )]
        show_duplicates: bool,
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
        #[arg(
            long,
            conflicts_with_all = ["after", "before"],
            help = "Only show results from today"
        )]
        today: bool,
        #[arg(
            long,
            conflicts_with = "all",
            help = "Only show results from this folder scope"
        )]
        project: Option<PathBuf>,
        #[arg(long, conflicts_with = "project", help = "Search across every project")]
        all: bool,
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
            long = "server-url",
            alias = "server",
            alias = "remote",
            value_name = "URL",
            help = "HTTP server URL for TUI requests, such as http://127.0.0.1:7391"
        )]
        server_url: Option<String>,
        #[arg(long, value_enum, default_value_t = SearchSort::Relevance, help = "Sort results by relevance or time")]
        sort: SearchSort,
        #[arg(long, value_enum, help = "Search mode: hybrid, lexical, or semantic")]
        mode: Option<SearchModeArg>,
        #[arg(
            short = 'e',
            long,
            conflicts_with = "no_embeddings",
            help = "Use embedding-backed semantic search for this run"
        )]
        embeddings: bool,
        #[arg(
            short = 'E',
            long,
            conflicts_with = "embeddings",
            help = "Skip embedding-backed semantic search for this run"
        )]
        no_embeddings: bool,
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
            alias = "no-collapse",
            help = "Show duplicate/forked matches instead of collapsing them"
        )]
        show_duplicates: bool,
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
        #[arg(
            long,
            conflicts_with_all = ["after", "before"],
            help = "Only show results from today"
        )]
        today: bool,
        #[arg(
            long,
            conflicts_with = "all",
            help = "Only show results from this folder scope"
        )]
        project: Option<PathBuf>,
        #[arg(long, conflicts_with = "project", help = "Search across every project")]
        all: bool,
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
        #[command(flatten)]
        filters: SessionFilterArgs,
        #[arg(long, help = "Print structured JSON")]
        json: bool,
        #[arg(long, help = "Refresh local inputs before listing threads")]
        update: bool,
        #[arg(
            long,
            hide = true,
            help = "Deprecated; threads reads stored data unless --update is passed"
        )]
        no_update: bool,
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
        #[arg(long, help = "Print structured JSON for the selected view")]
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
        #[arg(long, help = "Print structured JSON for the selected view")]
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
        #[arg(
            long,
            value_name = "TEXT",
            help = "Only show transcript items matching text plus context"
        )]
        grep: Option<String>,
        #[arg(
            short = 'A',
            long = "after-context",
            value_name = "COUNT",
            help = "Number of transcript items to show after each --grep match"
        )]
        after_context: Option<usize>,
        #[arg(
            short = 'B',
            long = "before-context",
            value_name = "COUNT",
            help = "Number of transcript items to show before each --grep match"
        )]
        before_context: Option<usize>,
        #[arg(
            short = 'C',
            long = "context",
            value_name = "COUNT",
            help = "Number of transcript items to show before and after each --grep match"
        )]
        context: Option<usize>,
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
        #[arg(long, help = "Print structured JSON for the selected view")]
        json: bool,
    },
    /// Follow a conversation transcript and append new clean messages to stdout.
    Tail {
        #[arg(
            value_name = "SESSION_OR_REF",
            help = "Session id, recent search ref, or full event id"
        )]
        target: String,
        #[arg(
            long,
            default_value_t = 1.0,
            value_name = "SECONDS",
            help = "Seconds to wait between local input scans"
        )]
        interval: f64,
        #[arg(
            short = 'n',
            long,
            default_value_t = DEFAULT_TAIL_LINES,
            value_name = "COUNT",
            help = "Number of existing clean transcript items to print before following"
        )]
        lines: usize,
        #[arg(long, value_enum, help = "When to use colored output")]
        color: Option<ColorArg>,
        #[arg(long, help = "Disable colored output")]
        no_color: bool,
        #[arg(long, help = "Show source file details and internal ids")]
        verbose: bool,
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
            default_value_t = RawArtifactExportMode::Omit,
            help = "How to include legacy raw artifacts in JSONL exports: omit by default, metadata only, or inline content"
        )]
        raw_artifacts: RawArtifactExportMode,
        #[arg(long, help = "Alias for --raw-artifacts omit")]
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
        #[arg(
            short = 'e',
            long,
            conflicts_with = "no_embeddings",
            help = "Import embedding records for this run"
        )]
        embeddings: bool,
        #[arg(
            short = 'E',
            long,
            conflicts_with = "embeddings",
            help = "Skip importing embedding records for this run"
        )]
        no_embeddings: bool,
        #[arg(default_value = "-", help = "Input file, or '-' for stdin")]
        input: String,
    },
    /// Preview or remove indexed history sessions.
    Prune {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(long, conflicts_with = "dry_run", help = "Remove matching sessions")]
        confirm: bool,
        #[arg(long, help = "Preview matching sessions without deleting")]
        dry_run: bool,
        #[arg(
            long,
            requires = "confirm",
            help = "Compact the SQLite database after deleting rows"
        )]
        vacuum: bool,
        #[arg(long, help = "Match this session id")]
        session: Vec<String>,
        #[arg(
            long,
            help = "Match sessions active at or after a date or time, like 2026-04-20 or \"3 days ago\""
        )]
        after: Option<String>,
        #[arg(
            long,
            help = "Match sessions active before a date or time, like 2026-04-20 or \"3 days ago\""
        )]
        before: Option<String>,
        #[arg(
            long,
            conflicts_with_all = ["after", "before"],
            help = "Match sessions active today"
        )]
        today: bool,
        #[command(flatten)]
        filters: SessionFilterArgs,
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
        #[arg(
            long,
            value_delimiter = ',',
            help = "Scan these source kinds each pass; may be repeated or comma-separated"
        )]
        source: Vec<String>,
        #[arg(
            short = 'e',
            long,
            conflicts_with = "no_embeddings",
            help = "Use embedding-backed semantic indexing while this daemon runs"
        )]
        embeddings: bool,
        #[arg(
            short = 'E',
            long,
            conflicts_with = "embeddings",
            help = "Skip embedding work while this daemon runs"
        )]
        no_embeddings: bool,
    },
    /// Serve already-indexed local history over HTTP.
    Serve {
        #[arg(
            long,
            default_value = DEFAULT_SERVER_BIND,
            help = "Address to listen on; non-loopback addresses require --allow-network-bind"
        )]
        bind: String,
        #[arg(
            long,
            help = "Allow serving unauthenticated HTTP on a non-loopback address"
        )]
        allow_network_bind: bool,
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
            value_delimiter = ',',
            help = "Scan these source kinds when watching; may be repeated or comma-separated"
        )]
        source: Vec<String>,
        #[arg(
            short = 'e',
            long,
            conflicts_with = "no_embeddings",
            help = "Use embedding-backed semantic indexing and search for this server process"
        )]
        embeddings: bool,
        #[arg(
            short = 'E',
            long,
            conflicts_with = "embeddings",
            help = "Skip embedding work for this server process"
        )]
        no_embeddings: bool,
    },
    /// Show local history and search health.
    Status {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
    },
    /// Read and update persistent Historious configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Run explicit cold-path maintenance tasks.
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
    },
    /// Output agent instructions for Historious.
    Onboard {
        #[arg(long, help = "Emit only the AGENTS.md-ready block")]
        agents_md: bool,
    },
    /// List, emit, and install packaged Historious skills.
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
    /// List packaged skills embedded in Historious.
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
pub enum ConfigCommand {
    /// Show the config file path and effective settings.
    Show,
    /// Show or change persistent embedding behavior.
    Embeddings {
        #[arg(value_enum, default_value_t = ConfigEmbeddingState::Status)]
        state: ConfigEmbeddingState,
    },
    /// Show or change Treechat ingestion opt-in behavior.
    Treechat {
        #[arg(value_enum, default_value_t = ConfigSourceState::Status)]
        state: ConfigSourceState,
    },
}

#[derive(Debug, Subcommand)]
pub enum MaintenanceCommand {
    /// Optimize FTS indexes and vacuum the SQLite database.
    Compact {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            conflicts_with = "confirm",
            help = "Preview database size without running maintenance"
        )]
        dry_run: bool,
        #[arg(
            long,
            conflicts_with = "dry_run",
            help = "Run FTS optimize and SQLite VACUUM"
        )]
        confirm: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConfigEmbeddingState {
    On,
    Off,
    Status,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConfigSourceState {
    On,
    Off,
    Status,
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
    /// Preview or remove superseded append-only raw artifact snapshots.
    Compact {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            conflicts_with = "confirm",
            help = "Preview compactable append snapshots without deleting anything"
        )]
        dry_run: bool,
        #[arg(
            long,
            conflicts_with = "dry_run",
            help = "Repoint covered raw artifact references and delete superseded blobs"
        )]
        confirm: bool,
    },
    /// Move legacy loose raw object blobs into SQLite storage.
    MigrateObjects {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            conflicts_with = "confirm",
            help = "Preview loose raw objects that can move into SQLite"
        )]
        dry_run: bool,
        #[arg(
            long,
            conflicts_with = "dry_run",
            help = "Store loose raw objects in SQLite and remove migrated loose blobs"
        )]
        confirm: bool,
    },
    /// Remove legacy raw artifacts that are exactly covered by manifests.
    CleanManifestArtifacts {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            conflicts_with = "confirm",
            help = "Preview manifest-covered raw artifacts without deleting anything"
        )]
        dry_run: bool,
        #[arg(
            long,
            conflicts_with = "dry_run",
            help = "Delete raw artifacts only after byte-for-byte manifest verification"
        )]
        confirm: bool,
    },
    /// Remove legacy source-native raw archives after normalized records exist.
    CleanSourceArchives {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            conflicts_with = "confirm",
            help = "Preview legacy source archive cleanup without deleting anything"
        )]
        dry_run: bool,
        #[arg(
            long,
            conflicts_with = "dry_run",
            help = "Delete legacy source archive rows and loose blobs"
        )]
        confirm: bool,
    },
    /// Remove unreferenced legacy loose raw blob files.
    CleanOrphans {
        #[arg(long, help = "Print a structured JSON result")]
        json: bool,
        #[arg(
            long,
            conflicts_with = "confirm",
            help = "Preview unreferenced loose raw blob cleanup without deleting anything"
        )]
        dry_run: bool,
        #[arg(
            long,
            conflicts_with = "dry_run",
            help = "Delete unreferenced loose raw blob files"
        )]
        confirm: bool,
    },
}

#[derive(Debug, Clone, Args, Default)]
pub struct SessionFilterArgs {
    #[arg(
        long,
        value_delimiter = ',',
        help = "Only include sessions from these source kinds; may be repeated or comma-separated"
    )]
    source: Vec<String>,
    #[command(flatten)]
    workspace: WorkspaceFilterArgs,
    #[command(flatten)]
    machine: MachineFilterArgs,
}

#[derive(Debug, Clone, Args, Default)]
struct WorkspaceFilterArgs {
    #[arg(
        long,
        visible_alias = "dir",
        conflicts_with = "all",
        help = "Only include sessions from this folder scope"
    )]
    project: Option<PathBuf>,
    #[arg(
        long,
        help = "Only include sessions whose project folder basename matches this name"
    )]
    basename: Option<String>,
    #[arg(long, conflicts_with = "project", help = "Include every project scope")]
    all: bool,
}

#[derive(Debug, Clone, Args, Default)]
struct MachineFilterArgs {
    #[arg(long, help = "Only include sessions from this exact machine id")]
    machine: Option<String>,
    #[arg(
        long,
        alias = "host",
        help = "Only include sessions from machine ids generated for this hostname"
    )]
    hostname: Option<String>,
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
pub enum SearchMatchArg {
    #[value(alias = "all")]
    And,
    #[value(alias = "any")]
    Or,
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

impl From<SearchMatchArg> for search::SearchTermMatch {
    fn from(value: SearchMatchArg) -> Self {
        match value {
            SearchMatchArg::And => search::SearchTermMatch::All,
            SearchMatchArg::Or => search::SearchTermMatch::Any,
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
    Similar,
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
        let data_dir = self.data_dir;
        let command = self.command;
        if let Command::Completion { shell } = command {
            print_completion(shell);
            return Ok(());
        }
        if let Command::Config { command } = command {
            run_config_command(data_dir, command, robot)?;
            return Ok(());
        }

        let mut config = AppConfig::load(data_dir)?;
        let store = Store::open(&config.data_dir)?;
        match command {
            Command::Update {
                max_files,
                source,
                repair,
                embeddings,
                no_embeddings,
                json,
            } => {
                apply_embeddings_override(&mut config, embeddings, no_embeddings);
                if json || robot {
                    let output =
                        run_update_once_machine(&store, &config, max_files, source, repair)?;
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
                match_mode,
                embeddings,
                no_embeddings,
                corpus,
                include_tools,
                raw,
                show_duplicates,
                recency_bias,
                after,
                before,
                today,
                project,
                all,
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
                let resolved_query = resolve_search_query(query, match_mode)?;
                let query = resolved_query.query;
                if query.trim().is_empty() && fzf {
                    bail!("search --fzf requires a query; use `histo tui` for live interactive search");
                }
                if query.trim().is_empty() && !fzf_rows {
                    bail!("search requires a query");
                }
                if fzf_rows && query.trim().is_empty() {
                    return Ok(());
                }
                apply_embeddings_override(&mut config, embeddings, no_embeddings);
                let (after_bound, before_bound) =
                    search_time_bounds(today, after.as_deref(), before.as_deref())?;
                let workspace_scope = search_workspace_scope(project.as_deref(), all);
                let mode = mode
                    .map(search::SearchMode::from)
                    .unwrap_or(config.default_search_mode);
                let corpus = resolve_search_corpus(corpus, include_tools, raw)?;
                let (embedder, degraded_reason) = load_embedder(&config);
                let options = search::SearchOptions::new(limit, sort.into(), recency_bias)
                    .with_mode(mode)
                    .with_corpus(corpus.clone())
                    .with_show_duplicates(show_duplicates)
                    .with_time_window(after_bound, before_bound)
                    .with_machine_filter(machine.clone(), hostname.clone())
                    .with_workspace_scope(workspace_scope.clone())
                    .with_term_match(resolved_query.term_match, resolved_query.terms.clone());
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
                    run_static_fzf_search(&store, &config, &query, &rows, color)?;
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
                        show_duplicates,
                        after_bound,
                        before_bound,
                        workspace_scope.clone(),
                        machine.clone(),
                        hostname.clone(),
                        resolved_query.term_match,
                        &resolved_query.terms,
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
                server_url,
                sort,
                mode,
                embeddings,
                no_embeddings,
                corpus,
                include_tools,
                raw,
                show_duplicates,
                recency_bias,
                after,
                before,
                today,
                project,
                all,
                machine,
                hostname,
                no_color,
            } => {
                if robot {
                    bail!("--robot cannot be combined with tui");
                }
                apply_embeddings_override(&mut config, embeddings, no_embeddings);
                let (after_bound, before_bound) =
                    search_time_bounds(today, after.as_deref(), before.as_deref())?;
                let workspace_scope = search_workspace_scope(project.as_deref(), all);
                let mode = mode
                    .map(search::SearchMode::from)
                    .unwrap_or(config.default_search_mode);
                let corpus = resolve_search_corpus(corpus, include_tools, raw)?;
                let backend = resolve_tui_backend(server_url)?;
                run_tui_search(
                    &store,
                    &config,
                    query.as_deref().unwrap_or_default(),
                    &backend,
                    limit,
                    sort,
                    mode,
                    corpus,
                    show_duplicates,
                    recency_bias,
                    after_bound,
                    before_bound,
                    workspace_scope,
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
                filters,
                json,
                update,
                no_update,
                no_color,
            } => {
                let implicit_update = update && !no_update;
                if implicit_update {
                    refresh_threads_inputs(&store, &config, json || robot)?;
                }
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
                let resolved_filters = resolve_session_filter(&filters, true)?;
                let scope = thread_scope_from_filter(&resolved_filters);
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
                    filter: resolved_filters.filter.clone(),
                };
                let threads = store.list_threads(&options)?;
                if json || robot {
                    crate::output::write_success(
                        "threads",
                        threads_output(
                            limit,
                            sort,
                            after_bound,
                            before_bound,
                            &scope,
                            &resolved_filters,
                            implicit_update,
                            &threads,
                        ),
                        Default::default(),
                    )?;
                } else {
                    let color = !no_color && !robot && std::io::stdout().is_terminal();
                    print_threads_output(&scope, &resolved_filters, &threads, color);
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
                    if full {
                        let context = store
                            .events_around_event(&event_id, before, after)?
                            .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                        crate::output::write_success(
                            "show",
                            show_output(&store, &context)?,
                            crate::output::EnvelopeOptions {
                                hints: vec![format!(
                                    "histo transcript {} --at {} --full --json",
                                    context.session.id, context.target_event.id
                                )],
                                ..Default::default()
                            },
                        )?;
                    } else {
                        let context = store
                            .history_items_around_event(&event_id, before, after)?
                            .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                        let target_hint = context
                            .target_event
                            .as_ref()
                            .map(|event| event.id.as_str())
                            .unwrap_or(event_id.as_str());
                        crate::output::write_success(
                            "show",
                            history_show_output(&store, &context)?,
                            crate::output::EnvelopeOptions {
                                hints: vec![format!(
                                    "histo transcript {} --at {} --json",
                                    context.session.id, target_hint
                                )],
                                ..Default::default()
                            },
                        )?;
                    }
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
                grep,
                after_context,
                before_context,
                context,
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
                let grep = resolve_transcript_grep(grep, before_context, after_context, context)?;
                if json || robot {
                    if full {
                        let events = store.events_for_session(&session)?;
                        let events = if let Some(grep) = &grep {
                            grep_events(&events, grep)
                        } else {
                            events
                        };
                        crate::output::write_success(
                            "transcript",
                            transcript_output(
                                &store,
                                &session_record,
                                &events,
                                target_event_id.as_deref(),
                                grep.as_ref(),
                            )?,
                            Default::default(),
                        )?;
                    } else {
                        let mut context = if let Some(event_id) = target_event_id.as_deref() {
                            store
                                .history_items_around_event(
                                    event_id,
                                    usize::MAX / 4,
                                    usize::MAX / 4,
                                )?
                                .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?
                        } else {
                            store
                                .history_items_for_transcript_session(&session)?
                                .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?
                        };
                        if let Some(grep) = &grep {
                            context = grep_history_context(context, grep);
                        }
                        crate::output::write_success(
                            "transcript",
                            history_transcript_output(&store, &context, grep.as_ref())?,
                            Default::default(),
                        )?;
                    }
                } else if full {
                    let events = store.events_for_session(&session)?;
                    let events = if let Some(grep) = &grep {
                        grep_events(&events, grep)
                    } else {
                        events
                    };
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
                    let mut context = if let Some(event_id) = target_event_id.as_deref() {
                        store
                            .history_items_around_event(event_id, usize::MAX / 4, usize::MAX / 4)?
                            .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?
                    } else {
                        store
                            .history_items_for_transcript_session(&session)?
                            .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?
                    };
                    if let Some(grep) = &grep {
                        context = grep_history_context(context, grep);
                    }
                    let rendered =
                        crate::transcript::render_history_session(&context, &metadata, color);
                    page_or_print(&rendered, target_event_id.as_deref(), no_pager || robot)?;
                }
            }
            Command::Tail {
                target,
                interval,
                lines,
                color,
                no_color,
                verbose,
            } => {
                if robot {
                    bail!("tail streams text output and cannot be combined with --robot");
                }
                let color = should_color(no_color, color, robot);
                run_transcript_tail(&store, &config, &target, interval, lines, color, verbose)
                    .await?;
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
                        sessions: resolve_session_filter_targets(&store, session)?,
                        since: transport::parse_since_arg(since.as_deref())?,
                    };
                    let options = transport::ExportOptions {
                        include_embeddings: include_embedding_records_for_config(
                            embeddings,
                            no_embeddings,
                            &config,
                        ),
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
            Command::Import {
                jsonl,
                json,
                embeddings,
                no_embeddings,
                input,
            } => {
                apply_embeddings_override(&mut config, embeddings, no_embeddings);
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
            Command::Prune {
                json,
                confirm,
                dry_run,
                vacuum,
                session,
                after,
                before,
                today,
                filters,
            } => {
                let (after_bound, before_bound) =
                    search_time_bounds(today, after.as_deref(), before.as_deref())?;
                let filter = prune_filter(
                    filters,
                    resolve_session_filter_targets(&store, session)?,
                    after_bound,
                    before_bound,
                )?;
                let dry_run = dry_run || !confirm;
                let output = if dry_run {
                    let plan = store.prune_plan(&filter)?;
                    PruneOutput {
                        dry_run: true,
                        confirmed: false,
                        vacuumed: false,
                        plan,
                        deleted: None,
                    }
                } else {
                    let outcome = store.prune(&filter)?;
                    let vacuumed = if vacuum && outcome.plan.sessions > 0 {
                        store.vacuum()?;
                        true
                    } else {
                        false
                    };
                    PruneOutput {
                        dry_run: false,
                        confirmed: true,
                        vacuumed,
                        plan: outcome.plan.clone(),
                        deleted: Some(PruneDeletedOutput {
                            raw_blobs_deleted: outcome.raw_blobs_deleted,
                            raw_blob_bytes_deleted: outcome.raw_blob_bytes_deleted,
                        }),
                    }
                };
                if json || robot {
                    crate::output::write_success("prune", output, Default::default())?;
                } else {
                    print_prune_output(&output);
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
                        sessions: resolve_session_filter_targets(&store, session)?,
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
                RawBlobCommand::Compact {
                    json,
                    dry_run: _,
                    confirm,
                } => {
                    let compaction = if confirm {
                        store.compact_append_raw_artifacts()?
                    } else {
                        store.preview_append_raw_artifact_compaction()?
                    };
                    let output = RawBlobCompactOutput {
                        dry_run: !confirm,
                        confirmed: confirm,
                        compaction,
                    };
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs compact",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        print_raw_blob_compact_output(&output);
                    }
                }
                RawBlobCommand::MigrateObjects {
                    json,
                    dry_run: _,
                    confirm,
                } => {
                    let migration = if confirm {
                        store.migrate_loose_raw_objects_to_sqlite()?
                    } else {
                        store.preview_loose_raw_object_migration()?
                    };
                    let output = RawObjectMigrationOutput {
                        dry_run: !confirm,
                        confirmed: confirm,
                        migration,
                    };
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs migrate-objects",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        print_raw_object_migration_output(&output);
                    }
                }
                RawBlobCommand::CleanManifestArtifacts {
                    json,
                    dry_run: _,
                    confirm,
                } => {
                    let cleanup = if confirm {
                        store.cleanup_manifest_raw_artifacts()?
                    } else {
                        store.preview_manifest_raw_artifact_cleanup()?
                    };
                    let output = ManifestRawArtifactCleanupOutput {
                        dry_run: !confirm,
                        confirmed: confirm,
                        cleanup,
                    };
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs clean-manifest-artifacts",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        print_manifest_raw_artifact_cleanup_output(&output);
                    }
                }
                RawBlobCommand::CleanSourceArchives {
                    json,
                    dry_run: _,
                    confirm,
                } => {
                    let cleanup = if confirm {
                        store.cleanup_source_archives()?
                    } else {
                        store.preview_source_archive_cleanup()?
                    };
                    let maintenance = if confirm && source_archive_cleanup_removed_data(&cleanup) {
                        Some(store.compact_sqlite()?)
                    } else {
                        None
                    };
                    let output = SourceArchiveCleanupOutput {
                        dry_run: !confirm,
                        confirmed: confirm,
                        cleanup,
                        maintenance,
                    };
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs clean-source-archives",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        print_source_archive_cleanup_output(&output);
                    }
                }
                RawBlobCommand::CleanOrphans {
                    json,
                    dry_run: _,
                    confirm,
                } => {
                    let cleanup = if confirm {
                        store.cleanup_orphan_raw_blobs()?
                    } else {
                        store.preview_orphan_raw_blob_cleanup()?
                    };
                    let output = OrphanRawBlobCleanupOutput {
                        dry_run: !confirm,
                        confirmed: confirm,
                        cleanup,
                    };
                    if json || robot {
                        crate::output::write_success(
                            "raw-blobs clean-orphans",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        print_orphan_raw_blob_cleanup_output(&output);
                    }
                }
            },
            Command::Maintenance { command } => match command {
                MaintenanceCommand::Compact {
                    json,
                    dry_run: _,
                    confirm,
                } => {
                    let maintenance = if confirm {
                        store.compact_sqlite()?
                    } else {
                        store.preview_sqlite_compaction()?
                    };
                    let output = MaintenanceCompactOutput {
                        dry_run: !confirm,
                        confirmed: confirm,
                        maintenance,
                    };
                    if json || robot {
                        crate::output::write_success(
                            "maintenance compact",
                            output,
                            Default::default(),
                        )?;
                    } else {
                        print_maintenance_compact_output(&output);
                    }
                }
            },
            Command::Daemon {
                interval_secs,
                max_files,
                source,
                embeddings,
                no_embeddings,
            } => {
                apply_embeddings_override(&mut config, embeddings, no_embeddings);
                run_daemon(
                    &store,
                    &config.machine_id,
                    config.embedder.clone(),
                    config.sources.clone(),
                    interval_secs,
                    max_files,
                    source,
                )
                .await?;
            }
            Command::Serve {
                bind,
                allow_network_bind,
                watch,
                interval_secs,
                max_files,
                source,
                embeddings,
                no_embeddings,
            } => {
                apply_embeddings_override(&mut config, embeddings, no_embeddings);
                let addr = parse_server_bind_addr(&bind, allow_network_bind)?;
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
                        config.sources.clone(),
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
                if json || robot {
                    let output = status_output(&store, &config)?;
                    crate::output::write_success("status", output, Default::default())?;
                } else if std::io::stdout().is_terminal() {
                    print_status_output_live(&store, &config)?;
                } else {
                    let output = status_output(&store, &config)?;
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
            Command::Config { .. } => unreachable!("config returns before storage setup"),
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
            Command::Tail { .. } => "tail",
            Command::Export { .. } => "export",
            Command::Import { .. } => "import",
            Command::Prune { .. } => "prune",
            Command::RawBlobs { .. } => "raw-blobs",
            Command::Daemon { .. } => "daemon",
            Command::Serve { .. } => "serve",
            Command::Status { .. } => "status",
            Command::Config { .. } => "config",
            Command::Maintenance { .. } => "maintenance",
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
                | Command::Prune { json: true, .. }
                | Command::RawBlobs {
                    command: RawBlobCommand::Missing { json: true, .. }
                        | RawBlobCommand::Import { json: true, .. }
                        | RawBlobCommand::Compact { json: true, .. }
                        | RawBlobCommand::MigrateObjects { json: true, .. }
                        | RawBlobCommand::CleanManifestArtifacts { json: true, .. }
                        | RawBlobCommand::CleanSourceArchives { json: true, .. }
                        | RawBlobCommand::CleanOrphans { json: true, .. },
                }
                | Command::Maintenance {
                    command: MaintenanceCommand::Compact { json: true, .. },
                }
                | Command::Status { json: true, .. }
        )
    }
}

fn print_completion(shell: Shell) {
    let mut command = Cli::command();
    clap_complete::generate(shell, &mut command, "histo", &mut io::stdout());
}

fn apply_embeddings_override(config: &mut AppConfig, embeddings: bool, no_embeddings: bool) {
    if embeddings {
        config.embedder = crate::embed::EmbedderConfig::from_config_and_env(&config.data_dir, true);
    }
    if no_embeddings {
        config.embedder.disable();
    }
}

#[derive(Debug, Serialize)]
struct ConfigOutput {
    config_path: String,
    embeddings_enabled: bool,
    treechat_enabled: bool,
}

fn run_config_command(
    data_dir: Option<PathBuf>,
    command: ConfigCommand,
    robot: bool,
) -> Result<()> {
    let data_dir = crate::config::resolve_data_dir(data_dir)?;
    let path = crate::config::config_path(&data_dir);
    match command {
        ConfigCommand::Show => {
            let output = ConfigOutput {
                config_path: path.display().to_string(),
                embeddings_enabled: crate::config::load_embeddings_enabled(&data_dir)?,
                treechat_enabled: crate::config::load_treechat_enabled(&data_dir)?,
            };
            if robot {
                crate::output::write_success("config show", output, Default::default())?;
            } else {
                print_config_output(&output);
            }
        }
        ConfigCommand::Embeddings { state } => {
            let path = match state {
                ConfigEmbeddingState::On => crate::config::set_embeddings_enabled(&data_dir, true)?,
                ConfigEmbeddingState::Off => {
                    crate::config::set_embeddings_enabled(&data_dir, false)?
                }
                ConfigEmbeddingState::Status => path,
            };
            let output = ConfigOutput {
                config_path: path.display().to_string(),
                embeddings_enabled: crate::config::load_embeddings_enabled(&data_dir)?,
                treechat_enabled: crate::config::load_treechat_enabled(&data_dir)?,
            };
            if robot {
                crate::output::write_success("config embeddings", output, Default::default())?;
            } else {
                print_config_output(&output);
            }
        }
        ConfigCommand::Treechat { state } => {
            let path = match state {
                ConfigSourceState::On => crate::config::set_treechat_enabled(&data_dir, true)?,
                ConfigSourceState::Off => crate::config::set_treechat_enabled(&data_dir, false)?,
                ConfigSourceState::Status => path,
            };
            let output = ConfigOutput {
                config_path: path.display().to_string(),
                embeddings_enabled: crate::config::load_embeddings_enabled(&data_dir)?,
                treechat_enabled: crate::config::load_treechat_enabled(&data_dir)?,
            };
            if robot {
                crate::output::write_success("config treechat", output, Default::default())?;
            } else {
                print_config_output(&output);
            }
        }
    }
    Ok(())
}

fn print_config_output(output: &ConfigOutput) {
    println!("config_path={}", output.config_path);
    println!("embeddings.enabled={}", output.embeddings_enabled);
    println!("sources.treechat.enabled={}", output.treechat_enabled);
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
struct PruneOutput {
    dry_run: bool,
    confirmed: bool,
    vacuumed: bool,
    plan: crate::storage::PrunePlan,
    deleted: Option<PruneDeletedOutput>,
}

#[derive(Debug, Serialize)]
struct PruneDeletedOutput {
    raw_blobs_deleted: usize,
    raw_blob_bytes_deleted: u64,
}

#[derive(Debug, Serialize)]
struct RawBlobMissingOutput {
    count: usize,
    hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RawBlobCompactOutput {
    dry_run: bool,
    confirmed: bool,
    compaction: crate::storage::RawArtifactCompactionOutcome,
}

#[derive(Debug, Serialize)]
struct RawObjectMigrationOutput {
    dry_run: bool,
    confirmed: bool,
    migration: crate::storage::RawObjectMigrationOutcome,
}

#[derive(Debug, Serialize)]
struct ManifestRawArtifactCleanupOutput {
    dry_run: bool,
    confirmed: bool,
    cleanup: crate::storage::ManifestRawArtifactCleanupOutcome,
}

#[derive(Debug, Serialize)]
struct SourceArchiveCleanupOutput {
    dry_run: bool,
    confirmed: bool,
    cleanup: crate::storage::SourceArchiveCleanupOutcome,
    maintenance: Option<crate::storage::SqliteMaintenanceOutcome>,
}

#[derive(Debug, Serialize)]
struct OrphanRawBlobCleanupOutput {
    dry_run: bool,
    confirmed: bool,
    cleanup: crate::storage::OrphanRawBlobCleanupOutcome,
}

#[derive(Debug, Serialize)]
struct MaintenanceCompactOutput {
    dry_run: bool,
    confirmed: bool,
    maintenance: crate::storage::SqliteMaintenanceOutcome,
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
    match_mode: Option<&'static str>,
    match_terms: Vec<String>,
    corpus: String,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    project: Option<String>,
    machine: Option<String>,
    hostname: Option<String>,
    show_duplicates: bool,
}

#[derive(Debug)]
struct ResolvedSearchQuery {
    query: String,
    term_match: Option<search::SearchTermMatch>,
    terms: Vec<String>,
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
    duplicate_group: Vec<search::DuplicateSearchMember>,
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
    filters: SessionFilterOutput,
    implicit_update: bool,
}

#[derive(Debug, Serialize)]
struct ThreadScopeOutput {
    mode: &'static str,
    path: Option<String>,
    inferred: bool,
}

#[derive(Debug, Serialize)]
struct SessionFilterOutput {
    source: Vec<String>,
    machine: Option<String>,
    machine_prefix: Option<String>,
    project_basename: Option<String>,
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
struct HistoryShowOutput {
    session: crate::archive::SessionRecord,
    target_event_id: Option<String>,
    target_ref: Option<String>,
    target_index: Option<usize>,
    omitted_target: bool,
    before: Vec<HistoryItemOutput>,
    target: Option<HistoryItemOutput>,
    after: Vec<HistoryItemOutput>,
}

#[derive(Debug, Serialize)]
struct TranscriptOutput {
    session: crate::archive::SessionRecord,
    target_event_id: Option<String>,
    target_ref: Option<String>,
    target_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    grep: Option<TranscriptGrepOutput>,
    events: Vec<EventOutput>,
}

#[derive(Debug, Serialize)]
struct HistoryTranscriptOutput {
    session: crate::archive::SessionRecord,
    target_event_id: Option<String>,
    target_ref: Option<String>,
    target_index: Option<usize>,
    omitted_target: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    grep: Option<TranscriptGrepOutput>,
    items: Vec<HistoryItemOutput>,
}

#[derive(Debug, Clone)]
struct TranscriptGrep {
    pattern: String,
    before_context: usize,
    after_context: usize,
}

#[derive(Debug, Serialize)]
struct TranscriptGrepOutput {
    pattern: String,
    before_context: usize,
    after_context: usize,
    match_count: usize,
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
struct HistoryItemOutput {
    history_item_id: String,
    event_id: String,
    #[serde(rename = "ref")]
    ref_id: Option<String>,
    session_id: String,
    source_id: String,
    machine_id: String,
    source_kind: String,
    ordinal: i64,
    subordinal: i64,
    tier: String,
    kind: String,
    text: String,
    text_hash: String,
    occurred_at: Option<chrono::DateTime<chrono::Utc>>,
    lexical_indexable: bool,
    semantic_policy: String,
    metadata: serde_json::Value,
    hash: String,
}

#[derive(Debug, Serialize)]
struct StatusOutput {
    data_dir: String,
    db_path: String,
    config: StatusConfigOutput,
    disk_usage: StatusDiskUsageOutput,
    stats: crate::storage::ArchiveStats,
    query_embedder: crate::embed::EmbedderStatus,
    query_embedder_probe: Option<EmbedderProbeOutput>,
}

#[derive(Debug, Serialize)]
struct StatusConfigOutput {
    machine_id: String,
    default_search_mode: search::SearchMode,
    embeddings_enabled: bool,
    treechat_enabled: bool,
}

#[derive(Debug, Serialize)]
struct StatusDiskUsageOutput {
    total_bytes: u64,
    database_bytes: u64,
    raw_blobs_bytes: u64,
    models_bytes: u64,
    other_bytes: u64,
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

fn run_update_once_machine(
    store: &Store,
    config: &AppConfig,
    max_files: Option<usize>,
    source: Vec<String>,
    repair: bool,
) -> Result<UpdateOutput> {
    let source_selection = ingest::SourceSelection::parse(source)?;
    let ingest = ingest::update_local_with_progress(
        store,
        &config.machine_id,
        ingest::UpdateOptions {
            max_files,
            source_selection,
            sources: config.sources.clone(),
        },
        |event| {
            write_update_progress(
                "scan",
                update_progress_detail(event),
                update_progress_payload(event),
            );
        },
    )?;
    write_update_progress(
        "scan",
        format!(
            "{} files, {} new events, {} unchanged, {} errors",
            format_count(ingest.files_seen),
            format_count(ingest.inserted),
            format_count(ingest.skipped_unchanged),
            format_count(ingest.errors)
        ),
        serde_json::json!({
            "status": "finished",
            "files_seen": ingest.files_seen,
            "inserted": ingest.inserted,
            "skipped_unchanged": ingest.skipped_unchanged,
            "errors": ingest.errors,
        }),
    );

    let projected = refresh_search_after_update_with_progress(
        store,
        &ingest.delta,
        repair,
        config.embedder.is_disabled(),
        |detail| {
            write_update_progress(
                "search_index",
                detail.clone(),
                serde_json::json!({ "detail": detail }),
            );
        },
    )?;
    write_update_progress(
        "search_index",
        format!("{} events indexed", format_count(projected)),
        serde_json::json!({
            "status": "finished",
            "indexed_events": projected,
        }),
    );

    let embeddings = if config.embedder.is_disabled() {
        search::EmbeddingRefresh::disabled()
    } else {
        let embeddings = refresh_embeddings_after_update_with_progress(
            store,
            config,
            &ingest.delta,
            repair,
            |event| {
                write_update_progress(
                    "embeddings",
                    embedding_progress_detail(event),
                    embedding_progress_payload(event),
                );
            },
        )?;
        write_update_progress(
            "embeddings",
            embedding_phase_detail(&embeddings),
            serde_json::json!({
                "status": "finished",
                "embedded": embeddings.embedded,
                "pending": embeddings.pending,
                "vectors_indexed": embeddings.vectors_indexed,
                "disabled": embeddings.disabled,
                "degraded_reason": embeddings.degraded_reason,
                "deferred_reason": embeddings.deferred_reason,
                "batch_size_reductions": embeddings.batch_size_reductions,
                "final_batch_size": embeddings.final_batch_size,
            }),
        );
        embeddings
    };

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
    source: Vec<String>,
    repair: bool,
) -> Result<UpdateOutput> {
    let source_selection = ingest::SourceSelection::parse(source)?;
    let mut progress = UpdateProgressView::new();
    let ingest = ingest::update_local_with_progress(
        store,
        &config.machine_id,
        ingest::UpdateOptions {
            max_files,
            source_selection,
            sources: config.sources.clone(),
        },
        |event| progress.ingest_event(event),
    )?;
    progress.finish_ingest();

    progress.start_search_data(repair, config.embedder.is_disabled());
    let projected = refresh_search_after_update_with_progress(
        store,
        &ingest.delta,
        repair,
        config.embedder.is_disabled(),
        |detail| {
            progress.search_detail(detail);
        },
    )?;
    progress.finish_search_index(projected);

    let embeddings = if config.embedder.is_disabled() {
        let embeddings = search::EmbeddingRefresh::disabled();
        progress.finish_embeddings(&embeddings);
        embeddings
    } else {
        let embeddings = refresh_embeddings_after_update_with_progress(
            store,
            config,
            &ingest.delta,
            repair,
            |event| {
                progress.embedding_event(event);
            },
        )?;
        progress.finish_embeddings(&embeddings);
        embeddings
    };
    progress.finish_all();

    Ok(UpdateOutput {
        ingest,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn refresh_search_after_update_with_progress(
    store: &Store,
    delta: &crate::storage::ImportDelta,
    repair: bool,
    embeddings_disabled: bool,
    mut progress: impl FnMut(String),
) -> Result<usize> {
    if repair {
        progress("repairing all indexable events".to_string());
        let indexed = if embeddings_disabled {
            refresh_search_text_index_repair_with_progress(store, &mut progress)?
        } else {
            refresh_search_index_repair_with_progress(store, &mut progress)?
        };
        progress("projecting history items".to_string());
        store.refresh_history_items_with_progress(|processed, total| {
            progress(format!(
                "projected {}/{} events into history items",
                format_count(processed),
                format_count(total)
            ));
        })?;
        Ok(indexed)
    } else {
        let search_event_ids = delta.search_index_event_ids();
        progress(format!(
            "indexing {} new events",
            format_count(search_event_ids.len())
        ));
        let mut last_index_progress = Instant::now();
        let mut last_index_report: Option<(usize, usize)> = None;
        let indexed = if embeddings_disabled {
            store.refresh_search_text_index_for_events_with_progress(
                &search_event_ids,
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
            )
        } else {
            store.refresh_search_index_for_events_with_progress(
                crate::embed::HashEmbedder::MODEL_ID,
                crate::embed::HashEmbedder::DIMS,
                &search_event_ids,
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
            )
        }?;
        progress("checking index health".to_string());
        let needs_repair = if embeddings_disabled {
            store.search_text_index_needs_repair()?
        } else {
            store.search_index_needs_repair(crate::embed::HashEmbedder::MODEL_ID)?
        };
        let indexed = if needs_repair {
            progress("repairing missing search index rows".to_string());
            if embeddings_disabled {
                refresh_search_text_index_repair_with_progress(store, &mut progress)
            } else {
                refresh_search_index_repair_with_progress(store, &mut progress)
            }
        } else {
            Ok(indexed)
        }?;
        if store.history_items_projection_ready()? {
            progress(format!(
                "projecting changed history items for {} events",
                format_count(delta.touched_events.len())
            ));
            store.refresh_history_items_for_events_with_progress(
                &delta.touched_events,
                |processed, total| {
                    progress(format!(
                        "projected {}/{} changed events into history items",
                        format_count(processed),
                        format_count(total)
                    ));
                },
            )?;
        } else {
            progress("projecting history items".to_string());
            store.refresh_history_items_with_progress(|processed, total| {
                progress(format!(
                    "projected {}/{} events into history items",
                    format_count(processed),
                    format_count(total)
                ));
            })?;
        }
        Ok(indexed)
    }
}

fn refresh_search_index_repair_with_progress(
    store: &Store,
    mut progress: impl FnMut(String),
) -> Result<usize> {
    let mut last_progress = Instant::now();
    let mut last_report: Option<(&'static str, usize, usize)> = None;
    store.refresh_search_index_with_progress(
        crate::embed::HashEmbedder::MODEL_ID,
        crate::embed::HashEmbedder::DIMS,
        crate::embed::hash_embed,
        |phase, processed, total| {
            let report = (phase, processed, total);
            if processed == 0
                || processed == total
                || last_progress.elapsed() >= Duration::from_secs(1)
            {
                if last_report != Some(report) {
                    let label = match phase {
                        "search_rows" => "search index rows",
                        "search_units" => "search units",
                        _ => "search rows",
                    };
                    progress(format!(
                        "repaired {}/{} {}",
                        format_count(processed),
                        format_count(total),
                        label
                    ));
                    last_progress = Instant::now();
                    last_report = Some(report);
                }
            }
        },
    )
}

fn refresh_search_text_index_repair_with_progress(
    store: &Store,
    mut progress: impl FnMut(String),
) -> Result<usize> {
    let mut last_progress = Instant::now();
    let mut last_report: Option<(&'static str, usize, usize)> = None;
    store.refresh_search_text_index_with_progress(|phase, processed, total| {
        let report = (phase, processed, total);
        if processed == 0 || processed == total || last_progress.elapsed() >= Duration::from_secs(1)
        {
            if last_report != Some(report) {
                let label = match phase {
                    "search_rows" => "search index rows",
                    "search_units" => "search units",
                    _ => "search rows",
                };
                progress(format!(
                    "repaired {}/{} {}",
                    format_count(processed),
                    format_count(total),
                    label
                ));
                last_progress = Instant::now();
                last_report = Some(report);
            }
        }
    })
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

fn refresh_import_search_index_with_progress(
    store: &Store,
    delta: &crate::storage::ImportDelta,
    embeddings_disabled: bool,
    mut progress: impl FnMut(String),
) -> Result<usize> {
    let search_event_ids = delta.search_index_event_ids();
    progress(format!(
        "indexing {} imported events",
        format_count(search_event_ids.len())
    ));
    let mut last_index_progress = Instant::now();
    let mut last_index_report: Option<(usize, usize)> = None;
    let indexed = if embeddings_disabled {
        store.refresh_search_text_index_for_events_with_progress(
            &search_event_ids,
            |processed, total| {
                if processed == 1
                    || processed == total
                    || last_index_progress.elapsed() >= Duration::from_secs(1)
                {
                    let report = (processed, total);
                    if last_index_report != Some(report) {
                        progress(format!(
                            "indexed {}/{} imported events",
                            format_count(processed),
                            format_count(total)
                        ));
                        last_index_progress = Instant::now();
                        last_index_report = Some(report);
                    }
                }
            },
        )
    } else {
        store.refresh_search_index_for_events_with_progress(
            crate::embed::HashEmbedder::MODEL_ID,
            crate::embed::HashEmbedder::DIMS,
            &search_event_ids,
            crate::embed::hash_embed,
            |processed, total| {
                if processed == 1
                    || processed == total
                    || last_index_progress.elapsed() >= Duration::from_secs(1)
                {
                    let report = (processed, total);
                    if last_index_report != Some(report) {
                        progress(format!(
                            "indexed {}/{} imported events",
                            format_count(processed),
                            format_count(total)
                        ));
                        last_index_progress = Instant::now();
                        last_index_report = Some(report);
                    }
                }
            },
        )
    }?;
    Ok(indexed)
}

fn import_progress_event(
    event: transport::ImportProgress,
) -> (&'static str, String, serde_json::Value) {
    match event {
        transport::ImportProgress::Stream(state) => (
            "stream",
            jsonl_progress_detail(state),
            serde_json::json!({
                "status": "streaming",
                "records": state.records,
                "bytes": state.bytes,
            }),
        ),
        transport::ImportProgress::HistoryItems { processed, total } => (
            "history_items",
            format!(
                "projected {}/{} events into history items",
                format_count(processed),
                format_count(total)
            ),
            serde_json::json!({
                "status": "projecting",
                "processed": processed,
                "total": total,
            }),
        ),
        transport::ImportProgress::VectorProjectionStarted { embeddings } => (
            "vectors",
            format!(
                "refreshing vector projection for {} embeddings",
                format_count(embeddings)
            ),
            serde_json::json!({
                "status": "refreshing",
                "embeddings": embeddings,
            }),
        ),
        transport::ImportProgress::VectorProjectionFinished { vectors_indexed } => (
            "vectors",
            format!("{} vectors indexed", format_count(vectors_indexed)),
            serde_json::json!({
                "status": "finished",
                "vectors_indexed": vectors_indexed,
            }),
        ),
    }
}

fn run_import_once(store: &Store, _config: &AppConfig, input: &str) -> Result<ImportOutput> {
    let import_options = import_options_for_config(_config);
    let stats = transport::import_jsonl_path_with_options_and_import_progress(
        store,
        input,
        import_options,
        |event| {
            let (phase, detail, data) = import_progress_event(event);
            write_machine_progress("import", phase, detail, data);
        },
    )?;
    let projected = refresh_import_search_index_with_progress(
        store,
        &stats.delta,
        _config.embedder.is_disabled(),
        |detail| {
            write_machine_progress(
                "import",
                "search_index",
                detail.clone(),
                serde_json::json!({ "detail": detail }),
            );
        },
    )?;
    let embeddings = embedding_refresh_from_import_stats(_config, stats.vectors_indexed);
    Ok(ImportOutput {
        import: stats,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn run_import_once_human(store: &Store, _config: &AppConfig, input: &str) -> Result<ImportOutput> {
    let progress = ProgressUi::new();
    let mut import = progress.phase("Importing history stream");
    let mut last = transport::JsonlProgress::default();
    let import_options = import_options_for_config(_config);
    let stats = transport::import_jsonl_path_with_options_and_import_progress(
        store,
        input,
        import_options,
        |event| match event {
            transport::ImportProgress::Stream(state) => {
                last = state;
                import.update(jsonl_progress_detail(state));
            }
            transport::ImportProgress::HistoryItems { processed, total } => {
                import.update(format!(
                    "projected {}/{} events into history items",
                    format_count(processed),
                    format_count(total)
                ));
            }
            transport::ImportProgress::VectorProjectionStarted { embeddings } => {
                import.update(format!(
                    "refreshing vector projection for {} embeddings",
                    format_count(embeddings)
                ));
            }
            transport::ImportProgress::VectorProjectionFinished { vectors_indexed } => {
                import.update(format!("{} vectors indexed", format_count(vectors_indexed)));
            }
        },
    )?;
    import.finish(format!(
        "{} new records, {} duplicates, read {} records, {}",
        format_count(stats.inserted),
        format_count(stats.duplicates),
        format_count(last.records),
        format_bytes(last.bytes)
    ));

    let mut index = progress.phase("Updating search index");
    let projected = refresh_import_search_index_with_progress(
        store,
        &stats.delta,
        _config.embedder.is_disabled(),
        |detail| {
            index.update(detail);
        },
    )?;
    index.finish(format!("{} events indexed", format_count(projected)));

    let embeddings = embedding_refresh_from_import_stats(_config, stats.vectors_indexed);
    if !embeddings.disabled {
        let embed = progress.phase("Updating embeddings");
        embed.finish(embedding_phase_detail(&embeddings));
    }

    Ok(ImportOutput {
        import: stats,
        search_index: SearchIndexOutput {
            indexed_events: projected,
        },
        embeddings,
    })
}

fn import_options_for_config(config: &AppConfig) -> transport::ImportOptions {
    let enabled = !config.embedder.is_disabled();
    transport::ImportOptions {
        include_embeddings: enabled,
        refresh_vector_projection: enabled,
    }
}

fn embedding_refresh_from_import_stats(
    config: &AppConfig,
    vectors_indexed: usize,
) -> search::EmbeddingRefresh {
    if config.embedder.is_disabled() {
        search::EmbeddingRefresh::disabled()
    } else {
        search::EmbeddingRefresh {
            vectors_indexed,
            ..search::EmbeddingRefresh::default()
        }
    }
}

fn status_output(store: &Store, config: &AppConfig) -> Result<StatusOutput> {
    let disk_usage = status_disk_usage(&config.data_dir, store.db_path());
    Ok(StatusOutput {
        data_dir: config.data_dir.display().to_string(),
        db_path: store.db_path().display().to_string(),
        config: status_config_output(config),
        disk_usage,
        stats: store.stats()?,
        query_embedder: config.embedder.status_without_loading(),
        query_embedder_probe: embedder_probe_output(config),
    })
}

fn status_config_output(config: &AppConfig) -> StatusConfigOutput {
    StatusConfigOutput {
        machine_id: config.machine_id.clone(),
        default_search_mode: config.default_search_mode,
        embeddings_enabled: !config.embedder.is_disabled(),
        treechat_enabled: config.sources.treechat.enabled,
    }
}

fn embedder_probe_output(config: &AppConfig) -> Option<EmbedderProbeOutput> {
    if std::env::var("HISTO_PROBE_EMBEDDER").as_deref() != Ok("1") {
        return None;
    }
    if config.embedder.is_disabled() {
        return None;
    }
    Some(match config.embedder.load() {
        Ok(loaded) => match loaded.embed_one("historious query embedder probe") {
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

fn status_disk_usage(data_dir: &Path, db_path: &Path) -> StatusDiskUsageOutput {
    let database_bytes = database_file_bytes(db_path);
    let raw_blobs_bytes = path_bytes(&data_dir.join("blobs"));
    let models_bytes = path_bytes(&data_dir.join("models"));
    let total_bytes = path_bytes(data_dir);
    let other_bytes = total_bytes.saturating_sub(database_bytes + raw_blobs_bytes + models_bytes);
    StatusDiskUsageOutput {
        total_bytes,
        database_bytes,
        raw_blobs_bytes,
        models_bytes,
        other_bytes,
    }
}

fn database_file_bytes(db_path: &Path) -> u64 {
    let mut bytes = path_bytes(db_path);
    for suffix in ["-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{suffix}", db_path.to_string_lossy()));
        bytes += path_bytes(&path);
    }
    bytes
}

fn path_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(std::result::Result::ok)
        .map(|entry| path_bytes(&entry.path()))
        .sum()
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

fn print_prune_output(output: &PruneOutput) {
    println!();
    let title = if output.dry_run {
        "Prune preview"
    } else {
        "Prune complete"
    };
    println!("{title}");
    print_section(
        "Matched",
        &[
            ("Sessions", format_count(output.plan.sessions as usize)),
            ("Events", format_count(output.plan.events as usize)),
            (
                "History items",
                format_count(output.plan.history_items as usize),
            ),
            (
                "Search units",
                format_count(output.plan.search_units as usize),
            ),
            ("Embeddings", format_count(output.plan.embeddings as usize)),
            (
                "Raw artifacts",
                format_count(output.plan.raw_artifacts as usize),
            ),
            ("Sources", format_count(output.plan.sources as usize)),
            ("Raw blob bytes", format_bytes(output.plan.raw_blob_bytes)),
        ],
        std::io::stdout().is_terminal(),
    );
    if let Some(deleted) = &output.deleted {
        print_section(
            "Removed",
            &[
                ("Raw blobs", format_count(deleted.raw_blobs_deleted)),
                (
                    "Raw blob bytes",
                    format_bytes(deleted.raw_blob_bytes_deleted),
                ),
                ("Vacuumed", output.vacuumed.to_string()),
            ],
            std::io::stdout().is_terminal(),
        );
    } else if output.plan.sessions > 0 {
        println!("Run again with --confirm to remove these sessions.");
    } else {
        println!("No matching sessions.");
    }
}

fn print_raw_blob_compact_output(output: &RawBlobCompactOutput) {
    println!();
    let title = if output.dry_run {
        "Raw blob compaction preview"
    } else {
        "Raw blob compaction complete"
    };
    println!("{title}");
    print_section(
        "Append snapshots",
        &[
            (
                "Paths inspected",
                format_count(output.compaction.paths_inspected),
            ),
            (
                "Raw artifacts compactable",
                format_count(output.compaction.raw_artifacts_compacted),
            ),
            (
                "Raw blob bytes compactable",
                format_bytes(output.compaction.raw_blob_bytes_compacted),
            ),
            (
                "Events repointed",
                format_count(output.compaction.events_repointed),
            ),
            (
                "Raw artifacts skipped",
                format_count(output.compaction.raw_artifacts_skipped),
            ),
            (
                "Raw blobs deleted",
                format_count(output.compaction.raw_blobs_deleted),
            ),
            (
                "Raw blob bytes deleted",
                format_bytes(output.compaction.raw_blob_bytes_deleted),
            ),
        ],
        std::io::stdout().is_terminal(),
    );
    if output.dry_run && output.compaction.raw_artifacts_compacted > 0 {
        println!("Run again with --confirm to apply this compaction.");
    } else if output.compaction.raw_artifacts_compacted == 0 {
        println!("No append-covered raw artifacts found.");
    }
}

fn print_raw_object_migration_output(output: &RawObjectMigrationOutput) {
    println!();
    let title = if output.dry_run {
        "Raw object migration preview"
    } else {
        "Raw object migration complete"
    };
    println!("{title}");
    print_section(
        "Loose raw objects",
        &[
            (
                "Objects inspected",
                format_count(output.migration.raw_objects_inspected),
            ),
            (
                "Objects migratable",
                format_count(output.migration.raw_objects_migrated),
            ),
            (
                "Object bytes migratable",
                format_bytes(output.migration.raw_object_bytes_migrated),
            ),
            (
                "Missing loose blobs",
                format_count(output.migration.raw_objects_skipped_missing_blob),
            ),
            (
                "Invalid loose blobs",
                format_count(output.migration.raw_objects_skipped_invalid_blob),
            ),
            (
                "Loose blobs deleted",
                format_count(output.migration.raw_blobs_deleted),
            ),
            (
                "Loose blob bytes deleted",
                format_bytes(output.migration.raw_blob_bytes_deleted),
            ),
            (
                "Loose blobs retained for raw artifacts",
                format_count(output.migration.raw_blobs_retained_for_raw_artifacts),
            ),
        ],
        std::io::stdout().is_terminal(),
    );
    if output.dry_run && output.migration.raw_objects_migrated > 0 {
        println!("Run again with --confirm to migrate these raw objects.");
    } else if output.migration.raw_objects_migrated == 0 {
        println!("No loose raw objects found for SQLite migration.");
    }
}

fn print_manifest_raw_artifact_cleanup_output(output: &ManifestRawArtifactCleanupOutput) {
    println!();
    let title = if output.dry_run {
        "Manifest raw artifact cleanup preview"
    } else {
        "Manifest raw artifact cleanup complete"
    };
    println!("{title}");
    print_section(
        "Manifest-covered raw artifacts",
        &[
            (
                "Raw artifacts inspected",
                format_count(output.cleanup.raw_artifacts_inspected),
            ),
            (
                "Raw artifacts verified",
                format_count(output.cleanup.raw_artifacts_verified),
            ),
            (
                "Raw artifacts deleted",
                format_count(output.cleanup.raw_artifacts_deleted),
            ),
            (
                "Raw artifact bytes verified",
                format_bytes(output.cleanup.raw_artifact_bytes_verified),
            ),
            (
                "Raw artifact bytes deleted",
                format_bytes(output.cleanup.raw_artifact_bytes_deleted),
            ),
            (
                "Missing legacy blobs",
                format_count(output.cleanup.raw_artifacts_skipped_missing_blob),
            ),
            (
                "Mismatches",
                format_count(output.cleanup.raw_artifacts_skipped_mismatch),
            ),
            (
                "Reconstruction failures",
                format_count(output.cleanup.raw_artifacts_skipped_reconstruction_failed),
            ),
            (
                "Raw blobs deleted",
                format_count(output.cleanup.raw_blobs_deleted),
            ),
            (
                "Raw blob bytes deleted",
                format_bytes(output.cleanup.raw_blob_bytes_deleted),
            ),
            (
                "Raw blobs retained for raw objects",
                format_count(output.cleanup.raw_blobs_retained_for_raw_objects),
            ),
        ],
        std::io::stdout().is_terminal(),
    );
    if output.dry_run && output.cleanup.raw_artifacts_verified > 0 {
        println!("Run again with --confirm to delete verified raw artifacts.");
    } else if output.cleanup.raw_artifacts_verified == 0 {
        println!("No manifest-covered raw artifacts verified for cleanup.");
    }
}

fn print_source_archive_cleanup_output(output: &SourceArchiveCleanupOutput) {
    println!();
    let title = if output.dry_run {
        "Source archive cleanup preview"
    } else {
        "Source archive cleanup complete"
    };
    println!("{title}");
    print_section(
        "Legacy source archives",
        &[
            (
                "Raw artifacts",
                format_count(output.cleanup.raw_artifacts_deleted),
            ),
            (
                "Raw artifact bytes",
                format_bytes(output.cleanup.raw_artifact_bytes_deleted),
            ),
            (
                "Raw manifests",
                format_count(output.cleanup.raw_manifests_deleted),
            ),
            (
                "Raw manifest entries",
                format_count(output.cleanup.raw_manifest_entries_deleted),
            ),
            (
                "Raw objects",
                format_count(output.cleanup.raw_objects_deleted),
            ),
            (
                "Raw object bytes",
                format_bytes(output.cleanup.raw_object_bytes_deleted),
            ),
            (
                "Events unlinked",
                format_count(output.cleanup.events_unlinked),
            ),
            (
                "Loose blobs",
                format_count(output.cleanup.raw_blobs_deleted),
            ),
            (
                "Loose blob bytes",
                format_bytes(output.cleanup.raw_blob_bytes_deleted),
            ),
        ],
        std::io::stdout().is_terminal(),
    );
    if let Some(maintenance) = &output.maintenance {
        print_section(
            "SQLite maintenance",
            &[
                ("Before", format_bytes(maintenance.database_bytes_before)),
                ("After", format_bytes(maintenance.database_bytes_after)),
                (
                    "Reclaimed",
                    format_bytes(maintenance.database_bytes_reclaimed),
                ),
                ("FTS optimized", maintenance.fts_optimized.to_string()),
                ("Vacuumed", maintenance.vacuumed.to_string()),
            ],
            std::io::stdout().is_terminal(),
        );
    }
    if output.dry_run
        && (output.cleanup.raw_artifacts_deleted > 0
            || output.cleanup.raw_manifests_deleted > 0
            || output.cleanup.raw_objects_deleted > 0
            || output.cleanup.raw_blobs_deleted > 0)
    {
        println!(
            "Run again with --confirm to delete these legacy source archives and compact SQLite."
        );
    } else if output.cleanup.raw_artifacts_deleted == 0
        && output.cleanup.raw_manifests_deleted == 0
        && output.cleanup.raw_objects_deleted == 0
        && output.cleanup.raw_blobs_deleted == 0
    {
        println!("No legacy source archives found.");
    }
}

fn source_archive_cleanup_removed_data(
    cleanup: &crate::storage::SourceArchiveCleanupOutcome,
) -> bool {
    cleanup.raw_artifacts_deleted > 0
        || cleanup.raw_manifests_deleted > 0
        || cleanup.raw_manifest_entries_deleted > 0
        || cleanup.raw_objects_deleted > 0
        || cleanup.events_unlinked > 0
        || cleanup.raw_blobs_deleted > 0
}

fn print_orphan_raw_blob_cleanup_output(output: &OrphanRawBlobCleanupOutput) {
    println!();
    let title = if output.dry_run {
        "Orphan raw blob cleanup preview"
    } else {
        "Orphan raw blob cleanup complete"
    };
    println!("{title}");
    print_section(
        "Loose raw blobs",
        &[
            (
                "Blobs inspected",
                format_count(output.cleanup.raw_blobs_inspected),
            ),
            (
                "Blobs retained",
                format_count(output.cleanup.raw_blobs_retained),
            ),
            (
                "Invalid paths skipped",
                format_count(output.cleanup.raw_blobs_skipped_invalid_path),
            ),
            (
                "Orphan blobs",
                format_count(output.cleanup.raw_blobs_deleted),
            ),
            (
                "Orphan blob bytes",
                format_bytes(output.cleanup.raw_blob_bytes_deleted),
            ),
        ],
        std::io::stdout().is_terminal(),
    );
    if output.dry_run && output.cleanup.raw_blobs_deleted > 0 {
        println!("Run again with --confirm to delete these unreferenced loose raw blobs.");
    } else if output.cleanup.raw_blobs_deleted == 0 {
        println!("No unreferenced loose raw blobs found.");
    }
}

fn print_maintenance_compact_output(output: &MaintenanceCompactOutput) {
    println!();
    let title = if output.dry_run {
        "SQLite maintenance preview"
    } else {
        "SQLite maintenance complete"
    };
    println!("{title}");
    print_section(
        "Database",
        &[
            (
                "Before",
                format_bytes(output.maintenance.database_bytes_before),
            ),
            (
                "After",
                format_bytes(output.maintenance.database_bytes_after),
            ),
            (
                "Reclaimed",
                format_bytes(output.maintenance.database_bytes_reclaimed),
            ),
            (
                "FTS optimized",
                output.maintenance.fts_optimized.to_string(),
            ),
            ("Vacuumed", output.maintenance.vacuumed.to_string()),
        ],
        std::io::stdout().is_terminal(),
    );
    if output.dry_run {
        println!("Run again with --confirm to optimize FTS indexes and vacuum the database.");
    }
}

fn print_search_summary(indexed_events: usize, color: bool) {
    print_section(
        "Search",
        &[("Indexed events", format_count(indexed_events))],
        color,
    );
}

fn print_embedding_summary(embeddings: &search::EmbeddingRefresh, color: bool) {
    let mode = if embeddings.disabled {
        "disabled".to_string()
    } else if let Some(reason) = embeddings.deferred_reason.as_deref() {
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
    if embeddings.disabled {
        "disabled".to_string()
    } else if let Some(reason) = &embeddings.deferred_reason {
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

#[derive(Debug, Serialize)]
struct MachineProgressEvent {
    #[serde(rename = "type")]
    event_type: &'static str,
    command: &'static str,
    phase: &'static str,
    detail: String,
    data: serde_json::Value,
}

fn write_update_progress(phase: &'static str, detail: String, data: serde_json::Value) {
    write_machine_progress("update", phase, detail, data);
}

fn write_machine_progress(
    command: &'static str,
    phase: &'static str,
    detail: String,
    data: serde_json::Value,
) {
    let event = MachineProgressEvent {
        event_type: "progress",
        command,
        phase,
        detail,
        data,
    };
    let stderr = io::stderr();
    let mut handle = stderr.lock();
    if serde_json::to_writer(&mut handle, &event).is_ok() {
        let _ = writeln!(handle);
        let _ = handle.flush();
    }
}

fn embedding_progress_payload(event: &search::EmbeddingProgress) -> serde_json::Value {
    match event {
        search::EmbeddingProgress::LoadingModel { model_id } => serde_json::json!({
            "status": "loading_model",
            "model_id": model_id,
        }),
        search::EmbeddingProgress::Batch {
            embedded,
            pending,
            batch_size,
            reductions,
            available_gib,
        } => serde_json::json!({
            "status": "batch",
            "embedded": embedded,
            "pending": pending,
            "batch_size": batch_size,
            "reductions": reductions,
            "available_gib": available_gib,
        }),
        search::EmbeddingProgress::Deferred { pending, reason } => serde_json::json!({
            "status": "deferred",
            "pending": pending,
            "reason": reason,
        }),
    }
}

fn update_progress_detail(event: &ingest::UpdateProgress) -> String {
    match event {
        ingest::UpdateProgress::Discovering { sources } => {
            if sources.is_empty() {
                "looking for enabled sources".to_string()
            } else {
                format!("discovering sources {}", sources.join(", "))
            }
        }
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
            adapter_kind: _,
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
        ingest::UpdateProgress::PreparingImports {
            changed_files,
            sources: _,
            stats,
        } => format!(
            "preparing {} changed files; {} new, {} unchanged, {} errors",
            format_count(*changed_files),
            format_count(stats.inserted),
            format_count(stats.skipped_unchanged),
            format_count(stats.errors)
        ),
        ingest::UpdateProgress::ImportingFile {
            adapter_kind: _,
            kind,
            path,
            changed_file_index,
            changed_file_count,
            stats,
        } => format!(
            "importing changed {} {}/{} {}; {} new, {} unchanged, {} errors",
            kind,
            format_count(*changed_file_index),
            format_count(*changed_file_count),
            compact_path(path),
            format_count(stats.inserted),
            format_count(stats.skipped_unchanged),
            format_count(stats.errors)
        ),
        ingest::UpdateProgress::ImportedFile {
            adapter_kind: _,
            kind,
            path,
            changed_file_index,
            changed_file_count,
            stats,
        } => format!(
            "imported changed {} {}/{} {}; {} new, {} unchanged, {} errors",
            kind,
            format_count(*changed_file_index),
            format_count(*changed_file_count),
            compact_path(path),
            format_count(stats.inserted),
            format_count(stats.skipped_unchanged),
            format_count(stats.errors)
        ),
        ingest::UpdateProgress::CompletedFile {
            adapter_kind: _,
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

fn update_progress_payload(event: &ingest::UpdateProgress) -> serde_json::Value {
    match event {
        ingest::UpdateProgress::Discovering { sources } => serde_json::json!({
            "status": "discovering",
            "sources": sources,
        }),
        ingest::UpdateProgress::Discovered {
            sources,
            selected_files,
        } => serde_json::json!({
            "status": "discovered",
            "selected_files": selected_files,
            "sources": sources.iter().map(|source| {
                serde_json::json!({
                    "kind": source.kind,
                    "found_files": source.found_files,
                    "selected_files": source.selected_files,
                })
            }).collect::<Vec<_>>(),
        }),
        ingest::UpdateProgress::Processing {
            adapter_kind,
            kind,
            path,
            file_index,
            total_files,
            source_file_index,
            source_file_count,
            stats,
        } => serde_json::json!({
            "status": "processing",
            "adapter_kind": adapter_kind,
            "kind": kind,
            "path": path.display().to_string(),
            "file_index": file_index,
            "total_files": total_files,
            "source_file_index": source_file_index,
            "source_file_count": source_file_count,
            "stats": stats,
        }),
        ingest::UpdateProgress::PreparingImports {
            changed_files,
            sources,
            stats,
        } => serde_json::json!({
            "status": "preparing_imports",
            "changed_files": changed_files,
            "sources": sources.iter().map(|source| {
                serde_json::json!({
                    "kind": source.kind,
                    "changed_files": source.changed_files,
                })
            }).collect::<Vec<_>>(),
            "stats": stats,
        }),
        ingest::UpdateProgress::ImportingFile {
            adapter_kind,
            kind,
            path,
            changed_file_index,
            changed_file_count,
            stats,
        } => serde_json::json!({
            "status": "importing_file",
            "adapter_kind": adapter_kind,
            "kind": kind,
            "path": path.display().to_string(),
            "changed_file_index": changed_file_index,
            "changed_file_count": changed_file_count,
            "stats": stats,
        }),
        ingest::UpdateProgress::ImportedFile {
            adapter_kind,
            kind,
            path,
            changed_file_index,
            changed_file_count,
            stats,
        } => serde_json::json!({
            "status": "imported_file",
            "adapter_kind": adapter_kind,
            "kind": kind,
            "path": path.display().to_string(),
            "changed_file_index": changed_file_index,
            "changed_file_count": changed_file_count,
            "stats": stats,
        }),
        ingest::UpdateProgress::CompletedFile {
            adapter_kind,
            kind,
            path,
            file_index,
            total_files,
            source_file_index,
            source_file_count,
            stats,
        } => serde_json::json!({
            "status": "completed_file",
            "adapter_kind": adapter_kind,
            "kind": kind,
            "path": path.display().to_string(),
            "file_index": file_index,
            "total_files": total_files,
            "source_file_index": source_file_index,
            "source_file_count": source_file_count,
            "stats": stats,
        }),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpdateDisplayPhase {
    LocalLogs,
    ChangedLogs,
    SearchData,
}

#[derive(Debug, Default)]
struct UpdateSourceProgress {
    total_files: usize,
    checked_files: usize,
    changed_files: usize,
    read_files: usize,
    state: &'static str,
    current_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
struct UpdateDataProgress {
    state: &'static str,
    current: Option<usize>,
    total: Option<usize>,
    detail: String,
}

struct UpdateProgressView {
    interactive: bool,
    phase: UpdateDisplayPhase,
    sources: BTreeMap<String, UpdateSourceProgress>,
    data_rows: BTreeMap<String, UpdateDataProgress>,
    drawn_rows: usize,
    last_emit: Instant,
}

impl UpdateProgressView {
    fn new() -> Self {
        Self {
            interactive: std::io::stderr().is_terminal(),
            phase: UpdateDisplayPhase::LocalLogs,
            sources: BTreeMap::new(),
            data_rows: BTreeMap::new(),
            drawn_rows: 0,
            last_emit: Instant::now(),
        }
    }

    fn ingest_event(&mut self, event: &ingest::UpdateProgress) {
        match event {
            ingest::UpdateProgress::Discovering { sources } => {
                self.phase = UpdateDisplayPhase::LocalLogs;
                for source in sources {
                    let row = self.sources.entry(source.clone()).or_default();
                    row.state = "discovering";
                }
            }
            ingest::UpdateProgress::Discovered { sources, .. } => {
                self.phase = UpdateDisplayPhase::LocalLogs;
                for source in sources {
                    if source.selected_files == 0 {
                        continue;
                    }
                    let row = self.sources.entry(source.kind.clone()).or_default();
                    row.total_files = source.selected_files;
                    row.state = "checking";
                }
            }
            ingest::UpdateProgress::Processing {
                kind,
                path,
                source_file_index,
                source_file_count,
                ..
            } => {
                self.phase = UpdateDisplayPhase::LocalLogs;
                let row = self.sources.entry(kind.clone()).or_default();
                row.total_files = *source_file_count;
                row.checked_files = source_file_index.saturating_sub(1);
                row.state = "checking";
                row.current_path = Some(path.clone());
            }
            ingest::UpdateProgress::CompletedFile {
                kind,
                source_file_index,
                source_file_count,
                ..
            } => {
                self.phase = UpdateDisplayPhase::LocalLogs;
                let row = self.sources.entry(kind.clone()).or_default();
                row.total_files = *source_file_count;
                row.checked_files = *source_file_index;
                row.state = if row.checked_files >= row.total_files {
                    "checked"
                } else {
                    "checking"
                };
                row.current_path = None;
            }
            ingest::UpdateProgress::PreparingImports { sources, .. } => {
                for source in sources {
                    let row = self.sources.entry(source.kind.clone()).or_default();
                    row.changed_files = source.changed_files;
                    row.read_files = 0;
                    row.state = "preparing";
                    row.current_path = None;
                }
                for row in self.sources.values_mut() {
                    if row.checked_files >= row.total_files {
                        row.state = "checked";
                    }
                }
                self.phase = UpdateDisplayPhase::ChangedLogs;
            }
            ingest::UpdateProgress::ImportingFile { kind, path, .. } => {
                self.phase = UpdateDisplayPhase::ChangedLogs;
                let row = self.sources.entry(kind.clone()).or_default();
                row.state = "reading";
                row.current_path = Some(path.clone());
            }
            ingest::UpdateProgress::ImportedFile { kind, .. } => {
                self.phase = UpdateDisplayPhase::ChangedLogs;
                let row = self.sources.entry(kind.clone()).or_default();
                row.read_files = row
                    .read_files
                    .saturating_add(1)
                    .min(row.changed_files.max(row.read_files.saturating_add(1)));
                row.state = if row.changed_files > 0 && row.read_files >= row.changed_files {
                    "read"
                } else {
                    "reading"
                };
                row.current_path = None;
            }
        }
        self.render(false);
    }

    fn finish_ingest(&mut self) {
        if self.sources.values().all(|row| row.changed_files == 0) {
            self.phase = UpdateDisplayPhase::LocalLogs;
            for row in self.sources.values_mut() {
                if row.checked_files >= row.total_files {
                    row.state = "checked";
                }
            }
        } else {
            self.phase = UpdateDisplayPhase::ChangedLogs;
            for row in self.sources.values_mut() {
                if row.changed_files > 0 && row.read_files >= row.changed_files {
                    row.state = "read";
                }
            }
        }
        self.render(true);
    }

    fn start_search_data(&mut self, repair: bool, embeddings_disabled: bool) {
        self.phase = UpdateDisplayPhase::SearchData;
        self.data_rows.clear();
        self.data_rows.insert(
            "search".to_string(),
            UpdateDataProgress {
                state: if repair { "repairing" } else { "waiting" },
                ..Default::default()
            },
        );
        self.data_rows.insert(
            "history".to_string(),
            UpdateDataProgress {
                state: "waiting",
                ..Default::default()
            },
        );
        self.data_rows.insert(
            "vectors".to_string(),
            UpdateDataProgress {
                state: if embeddings_disabled {
                    "skipped"
                } else {
                    "waiting"
                },
                detail: if embeddings_disabled {
                    "embeddings disabled".to_string()
                } else {
                    String::new()
                },
                ..Default::default()
            },
        );
        self.render(true);
    }

    fn search_detail(&mut self, detail: String) {
        self.phase = UpdateDisplayPhase::SearchData;
        if detail.contains("project") {
            let (current, total) = parse_progress_fraction(&detail).unwrap_or((0, 0));
            let row = self.data_rows.entry("history".to_string()).or_default();
            row.state = if detail.contains("projected") {
                "projecting"
            } else {
                "projecting"
            };
            if total > 0 {
                row.current = Some(current);
                row.total = Some(total);
            }
            row.detail = detail;
        } else {
            let (current, total) = parse_progress_fraction(&detail).unwrap_or((0, 0));
            let row = self.data_rows.entry("search".to_string()).or_default();
            row.state = if detail.contains("repair") {
                "repairing"
            } else if detail.contains("checking") {
                "checking"
            } else {
                "indexing"
            };
            if total > 0 {
                row.current = Some(current);
                row.total = Some(total);
            }
            row.detail = detail;
        }
        self.render(false);
    }

    fn finish_search_index(&mut self, projected: usize) {
        let row = self.data_rows.entry("search".to_string()).or_default();
        row.state = "indexed";
        row.current = Some(projected);
        row.total = Some(projected);
        row.detail = format!("{} events indexed", format_count(projected));
        self.render(true);
    }

    fn embedding_event(&mut self, event: &search::EmbeddingProgress) {
        self.phase = UpdateDisplayPhase::SearchData;
        let row = self.data_rows.entry("vectors".to_string()).or_default();
        match event {
            search::EmbeddingProgress::LoadingModel { model_id } => {
                row.state = "loading";
                row.detail = model_id.clone();
            }
            search::EmbeddingProgress::Batch {
                embedded, pending, ..
            } => {
                row.state = "embedding";
                row.current = Some(*embedded);
                row.total = Some(embedded.saturating_add(*pending));
                row.detail = format!(
                    "{} embedded, {} pending",
                    format_count(*embedded),
                    format_count(*pending)
                );
            }
            search::EmbeddingProgress::Deferred { pending, reason } => {
                row.state = "deferred";
                row.detail = format!("{} pending: {reason}", format_count(*pending));
            }
        }
        self.render(false);
    }

    fn finish_embeddings(&mut self, embeddings: &search::EmbeddingRefresh) {
        let row = self.data_rows.entry("vectors".to_string()).or_default();
        if embeddings.disabled {
            row.state = "skipped";
            row.detail = "embeddings disabled".to_string();
        } else if let Some(reason) = &embeddings.deferred_reason {
            row.state = "deferred";
            row.detail = reason.clone();
        } else {
            row.state = "embedded";
            row.current = Some(embeddings.embedded);
            row.total = Some(embeddings.embedded);
            row.detail = format!(
                "{} new embeddings, {} vectors indexed",
                format_count(embeddings.embedded),
                format_count(embeddings.vectors_indexed)
            );
        }
        self.render(true);
    }

    fn finish_all(&mut self) {
        self.settle_rendering();
    }

    fn settle_rendering(&mut self) {
        if self.interactive && self.drawn_rows > 0 {
            self.drawn_rows = 0;
        }
    }

    fn render(&mut self, force: bool) {
        if !self.interactive && !force && self.last_emit.elapsed() < Duration::from_secs(2) {
            return;
        }
        if self.interactive {
            let columns = terminal_columns();
            let lines = self.lines_for_terminal(columns);
            self.clear_rendered_block();
            for line in &lines {
                eprint!("\r\x1b[2K{line}");
                eprintln!();
            }
            self.drawn_rows = terminal_rows_for_lines(&lines, columns);
        } else {
            for line in self.lines() {
                eprintln!("{line}");
            }
            eprintln!();
        }
        let _ = std::io::stderr().flush();
        self.last_emit = Instant::now();
    }

    fn clear_rendered_block(&mut self) {
        if self.drawn_rows == 0 {
            return;
        }
        eprint!("\x1b[{}F", self.drawn_rows);
        for idx in 0..self.drawn_rows {
            eprint!("\r\x1b[2K");
            if idx + 1 < self.drawn_rows {
                eprint!("\x1b[1E");
            }
        }
        if self.drawn_rows > 1 {
            eprint!("\x1b[{}F", self.drawn_rows - 1);
        }
    }

    fn lines(&self) -> Vec<String> {
        match self.phase {
            UpdateDisplayPhase::LocalLogs => self.source_lines("local logs: scanning", true),
            UpdateDisplayPhase::ChangedLogs => self.source_lines("changed logs: reading", false),
            UpdateDisplayPhase::SearchData => self.data_lines("search data: updating"),
        }
    }

    fn lines_for_terminal(&self, columns: usize) -> Vec<String> {
        self.lines()
            .into_iter()
            .map(|line| fit_terminal_line(&line, columns))
            .collect()
    }

    fn source_lines(&self, heading: &str, checking: bool) -> Vec<String> {
        let mut lines = vec![heading.to_string()];
        let label_width = self.source_label_width();
        for (kind, row) in &self.sources {
            if !checking && row.changed_files == 0 {
                continue;
            }
            let (current, total, suffix) = if checking {
                (
                    row.checked_files,
                    row.total_files,
                    format!(
                        "{}/{} files, {} changed",
                        format_count(row.checked_files),
                        format_count(row.total_files),
                        format_count(row.changed_files)
                    ),
                )
            } else {
                (
                    row.read_files,
                    row.changed_files,
                    format!(
                        "{}/{} files",
                        format_count(row.read_files),
                        format_count(row.changed_files)
                    ),
                )
            };
            lines.push(format!(
                "  {kind:<label_width$}  {:<10} {}  {suffix}",
                row.state,
                progress_meter(current, total, 20)
            ));
            if !checking {
                if let Some(path) = &row.current_path {
                    lines.push(format!(
                        "  {:<label_width$}  {:<10} {}  current {}",
                        "",
                        "",
                        " ".repeat(20),
                        compact_path(path)
                    ));
                }
            }
        }
        lines
    }

    fn data_lines(&self, heading: &str) -> Vec<String> {
        let mut lines = vec![heading.to_string()];
        let label_width = self.data_label_width();
        let keys: &[&str] = &["search", "history", "vectors"];
        for key in keys {
            if let Some(row) = self.data_rows.get(*key) {
                let meter = match (row.current, row.total) {
                    (Some(current), Some(total)) => progress_meter(current, total, 20),
                    _ => " ".repeat(20),
                };
                lines.push(format!(
                    "  {key:<label_width$}  {:<10} {meter}  {}",
                    row.state, row.detail
                ));
            }
        }
        lines
    }

    fn source_label_width(&self) -> usize {
        self.sources
            .keys()
            .map(|key| key.chars().count())
            .max()
            .unwrap_or(7)
            .max(7)
    }

    fn data_label_width(&self) -> usize {
        self.data_rows
            .keys()
            .map(|key| key.chars().count())
            .max()
            .unwrap_or(7)
            .max(7)
    }
}

fn progress_meter(current: usize, total: usize, width: usize) -> String {
    if total == 0 || width == 0 {
        return " ".repeat(width);
    }
    if current >= total {
        return "█".repeat(width);
    }
    let units = current.saturating_mul(width).saturating_mul(8) / total;
    let full = units / 8;
    let partial = units % 8;
    let partials = ["", "▁", "▂", "▃", "▄", "▅", "▆", "▇"];
    let mut meter = "█".repeat(full.min(width));
    if full < width && partial > 0 {
        meter.push_str(partials[partial]);
    }
    let meter_width = meter.chars().count();
    if meter_width < width {
        meter.push_str(&" ".repeat(width - meter_width));
    }
    meter
}

fn parse_progress_fraction(detail: &str) -> Option<(usize, usize)> {
    let slash = detail.find('/')?;
    let left = detail[..slash]
        .chars()
        .rev()
        .take_while(|ch| ch.is_ascii_digit() || *ch == ',')
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>()
        .replace(',', "");
    let right = detail[slash + 1..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == ',')
        .collect::<String>()
        .replace(',', "");
    Some((left.parse().ok()?, right.parse().ok()?))
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

fn fit_terminal_line(line: &str, columns: usize) -> String {
    let budget = terminal_line_budget(columns);
    ellipsize_middle(line, budget)
}

fn terminal_rows_for_lines(lines: &[String], columns: usize) -> usize {
    let columns = terminal_line_budget(columns);
    lines
        .iter()
        .map(|line| line.chars().count().max(1).div_ceil(columns))
        .sum()
}

fn terminal_line_budget(columns: usize) -> usize {
    columns.saturating_sub(1).max(1)
}

fn format_count(value: usize) -> String {
    format_number_text(&value.to_string())
}

fn format_count_u64(value: u64) -> String {
    let text = value.to_string();
    format_number_text(&text)
}

fn format_number_text(text: &str) -> String {
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

fn include_embedding_records_for_config(
    mode: EmbeddingExportMode,
    no_embeddings: bool,
    config: &AppConfig,
) -> bool {
    !config.embedder.is_disabled() && include_embedding_records(mode, no_embeddings)
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
    let color = std::io::stdout().is_terminal();
    println!();
    println!("{}", styled("Historious status", "1;32", color));
    println!("  {}", status_summary(&output.stats));
    print_status_search(&output.config, &output.query_embedder, color);
    print_status_attention(&output.query_embedder, color);
    print_status_probe(output.query_embedder_probe.as_ref(), color);
    print_status_indexed_history(&output.stats, color);
    print_status_disk_usage(&output.disk_usage, color);
    print_status_config(&output.config, color);
    print_status_storage(&output.data_dir, &output.db_path, color);
}

fn print_status_output_live(store: &Store, config: &AppConfig) -> Result<()> {
    let color = true;
    let data_dir = config.data_dir.display().to_string();
    let db_path = store.db_path().display().to_string();
    let status_config = status_config_output(config);
    let query_embedder = config.embedder.status_without_loading();
    let spinner = std::io::stderr().is_terminal();

    println!();
    println!("{}", styled("Historious status", "1;32", color));
    println!("  Loading indexed history and disk usage...");
    print_status_search(&status_config, &query_embedder, color);
    print_status_attention(&query_embedder, color);
    print_status_config(&status_config, color);
    print_status_storage(&data_dir, &db_path, color);
    flush_stdout()?;

    let stats = status_phase("Reading indexed counts", spinner, || store.stats())?;
    println!();
    println!("  {}", status_summary(&stats));
    print_status_indexed_history(&stats, color);
    flush_stdout()?;

    let disk_usage = status_phase("Measuring disk usage", spinner, || {
        Ok(status_disk_usage(&config.data_dir, store.db_path()))
    })?;
    print_status_disk_usage(&disk_usage, color);
    flush_stdout()?;

    if should_probe_embedder(config) {
        let probe = status_phase("Checking query embedder", spinner, || {
            Ok(embedder_probe_output(config))
        })?;
        print_status_probe(probe.as_ref(), color);
        flush_stdout()?;
    }

    Ok(())
}

fn status_phase<T>(label: &str, spinner: bool, action: impl FnOnce() -> Result<T>) -> Result<T> {
    if spinner {
        let phase = ProgressPhase::start(label, true);
        let value = action()?;
        phase.finish("ready".to_string());
        Ok(value)
    } else {
        action()
    }
}

fn should_probe_embedder(config: &AppConfig) -> bool {
    std::env::var("HISTO_PROBE_EMBEDDER").as_deref() == Ok("1") && !config.embedder.is_disabled()
}

fn print_status_search(
    config: &StatusConfigOutput,
    query_embedder: &crate::embed::EmbedderStatus,
    color: bool,
) {
    print_section(
        "Search",
        &[
            (
                "Default mode",
                config.default_search_mode.as_str().to_string(),
            ),
            ("Semantic search", semantic_status(config, query_embedder)),
            ("Query embedder", embedder_status(query_embedder)),
            ("Embedder threads", embedder_threads(query_embedder)),
        ],
        color,
    );
}

fn print_status_attention(query_embedder: &crate::embed::EmbedderStatus, color: bool) {
    if let Some(reason) = query_embedder.degraded_reason.as_deref() {
        print_section(
            "Attention",
            &[("Semantic fallback", reason.to_string())],
            color,
        );
    }
}

fn print_status_probe(probe: Option<&EmbedderProbeOutput>, color: bool) {
    let Some(probe) = probe else {
        return;
    };
    match probe.status {
        EmbedderProbeStatus::Ready => print_section(
            "Embedder probe",
            &[
                ("Status", "ready".to_string()),
                (
                    "Model",
                    probe.model_id.as_deref().unwrap_or("unknown").to_string(),
                ),
                ("Dimensions", probe.dims.unwrap_or(0).to_string()),
                ("Sample vector", probe.sample_dims.unwrap_or(0).to_string()),
            ],
            color,
        ),
        EmbedderProbeStatus::Degraded => print_section(
            "Embedder probe",
            &[
                ("Status", "degraded".to_string()),
                (
                    "Reason",
                    probe.reason.as_deref().unwrap_or("unknown").to_string(),
                ),
            ],
            color,
        ),
    }
}

fn print_status_indexed_history(stats: &crate::storage::ArchiveStats, color: bool) {
    print_section(
        "Indexed history",
        &[
            ("Sessions", format_count_u64(stats.sessions)),
            ("History items", format_count_u64(stats.history_items)),
            ("Events", format_count_u64(stats.events)),
            ("Sources", format_count_u64(stats.sources)),
            (
                "Legacy raw artifacts",
                format_count_u64(stats.raw_artifacts),
            ),
            ("Search units", format_count_u64(stats.search_units)),
            ("Embeddings", format_count_u64(stats.embeddings)),
        ],
        color,
    );
}

fn print_status_disk_usage(disk_usage: &StatusDiskUsageOutput, color: bool) {
    print_section(
        "Disk usage",
        &[
            ("Total", format_bytes(disk_usage.total_bytes)),
            (
                "Database + indexes",
                format_bytes(disk_usage.database_bytes),
            ),
            ("Legacy raw blobs", format_bytes(disk_usage.raw_blobs_bytes)),
            ("Models", format_bytes(disk_usage.models_bytes)),
            ("Other app files", format_bytes(disk_usage.other_bytes)),
        ],
        color,
    );
}

fn print_status_config(config: &StatusConfigOutput, color: bool) {
    print_section(
        "Config",
        &[
            (
                "Embeddings",
                enabled_label(config.embeddings_enabled).to_string(),
            ),
            (
                "Treechat",
                enabled_label(config.treechat_enabled).to_string(),
            ),
            ("Machine", config.machine_id.clone()),
        ],
        color,
    );
}

fn print_status_storage(data_dir: &str, db_path: &str, color: bool) {
    print_section(
        "Storage",
        &[
            ("Data directory", data_dir.to_string()),
            ("Database", db_path.to_string()),
        ],
        color,
    );
}

fn status_summary(stats: &crate::storage::ArchiveStats) -> String {
    if stats.history_items > 0 {
        format!(
            "{} sessions indexed into {} searchable history items.",
            format_count_u64(stats.sessions),
            format_count_u64(stats.history_items)
        )
    } else if stats.events > 0 {
        format!(
            "{} events indexed; history-item projection has not been built yet.",
            format_count_u64(stats.events)
        )
    } else {
        "No indexed history yet. Run `histo update` when you are ready to ingest local sessions."
            .to_string()
    }
}

fn semantic_status(
    config: &StatusConfigOutput,
    query_embedder: &crate::embed::EmbedderStatus,
) -> String {
    if query_embedder.semantic && query_embedder.available {
        "available".to_string()
    } else if config.default_search_mode == search::SearchMode::Semantic {
        "unavailable for semantic-only searches".to_string()
    } else {
        "not available; lexical search is available".to_string()
    }
}

fn embedder_status(query_embedder: &crate::embed::EmbedderStatus) -> String {
    let mut label = query_embedder.provider.clone();
    if let Some(model_id) = query_embedder.model_id.as_deref() {
        label.push_str(" / ");
        label.push_str(model_id);
    }
    if let Some(dims) = query_embedder.dims {
        label.push_str(&format!(" ({dims} dims)"));
    }
    label
}

fn embedder_threads(query_embedder: &crate::embed::EmbedderStatus) -> String {
    query_embedder
        .intra_threads
        .map(|threads| threads.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn enabled_label(enabled: bool) -> &'static str {
    if enabled {
        "enabled"
    } else {
        "disabled"
    }
}

fn print_threads_output(
    scope: &ThreadScope,
    filters: &ResolvedSessionFilter,
    threads: &[crate::storage::ThreadRow],
    color: bool,
) {
    println!();
    println!("{}", styled("Threads", "1;32", color));
    match scope.mode {
        ThreadScopeMode::All => println!("scope: all projects"),
        ThreadScopeMode::Path => {
            println!("scope: {}", scope.path.as_deref().unwrap_or("unknown"));
        }
    }
    print_thread_filter_summary(filters);
    if threads.is_empty() {
        println!();
        println!("No threads found.");
        return;
    }
    println!();
    for thread in threads {
        let when = format_thread_time(thread.last_activity_at);
        let title = display_thread_title(thread);
        println!(
            "{}  {}",
            styled(&when, "1;36", color),
            styled(&title, "1", color)
        );
        println!(
            "  updated: {}  last event: {}  source: {}  events: {}",
            format_thread_time(thread.session.updated_at),
            format_thread_time(thread.last_event_at),
            thread.session.source_kind,
            format_count(thread.event_count as usize)
        );
        println!("  session: {}", thread.session.id);
        if let Some(workspace) = &thread.workspace_path {
            println!("  project: {workspace}");
        }
    }
}

fn print_thread_filter_summary(filters: &ResolvedSessionFilter) {
    let filter = &filters.filter;
    if !filter.sources.is_empty() {
        println!("source: {}", filter.sources.join(", "));
    }
    if let Some(machine) = &filter.machine_id {
        println!("machine: {machine}");
    }
    if let Some(machine_prefix) = &filter.machine_id_prefix {
        println!("machine prefix: {machine_prefix}");
    }
    if let Some(basename) = &filter.workspace_basename {
        println!("project basename: {basename}");
    }
}

fn display_thread_title(thread: &crate::storage::ThreadRow) -> String {
    thread
        .session
        .title
        .as_deref()
        .filter(|title| !title.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("(untitled) {}", thread.session.external_id))
}

fn format_thread_time(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn refresh_threads_inputs(store: &Store, config: &AppConfig, quiet: bool) -> Result<()> {
    let stats = ingest::update_local_with_progress(
        store,
        &config.machine_id,
        ingest::UpdateOptions {
            max_files: None,
            source_selection: ingest::SourceSelection::default(),
            sources: config.sources.clone(),
        },
        |_| {},
    )?;
    if stats.errors > 0 && !quiet {
        eprintln!(
            "Warning: refreshed local inputs with {} ingest errors; some threads may be missing or stale.",
            format_count(stats.errors)
        );
    }
    Ok(())
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

fn history_show_output(
    store: &Store,
    context: &crate::storage::HistoryTranscriptContext,
) -> Result<HistoryShowOutput> {
    let target_event_id = context.target_event.as_ref().map(|event| event.id.clone());
    let target_ref = target_event_id
        .as_deref()
        .map(|event_id| store.recent_ref_for_event_id(event_id))
        .transpose()?
        .flatten();
    let target_index = context.target_index;
    let before = context
        .items
        .iter()
        .take(target_index.unwrap_or(context.items.len()))
        .map(|item| history_item_output(store, item))
        .collect::<Result<Vec<_>>>()?;
    let target = target_index
        .and_then(|idx| context.items.get(idx))
        .map(|item| history_item_output(store, item))
        .transpose()?;
    let after = target_index
        .map(|idx| {
            context
                .items
                .iter()
                .skip(idx + 1)
                .map(|item| history_item_output(store, item))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(HistoryShowOutput {
        session: context.session.clone(),
        target_event_id,
        target_ref,
        target_index,
        omitted_target: context.omitted_target,
        before,
        target,
        after,
    })
}

fn threads_output(
    limit: usize,
    sort: ThreadSort,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    scope: &ThreadScope,
    filters: &ResolvedSessionFilter,
    implicit_update: bool,
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
            filters: session_filter_output(filters),
            implicit_update,
        },
        next_commands: results
            .first()
            .map(|thread| vec![format!("histo transcript {} --json", thread.session_id)])
            .unwrap_or_else(|| vec!["histo update --json".to_string()]),
        results,
    }
}

fn session_filter_output(filters: &ResolvedSessionFilter) -> SessionFilterOutput {
    SessionFilterOutput {
        source: filters.filter.sources.clone(),
        machine: filters.filter.machine_id.clone(),
        machine_prefix: filters.filter.machine_id_prefix.clone(),
        project_basename: filters.filter.workspace_basename.clone(),
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
    grep: Option<&TranscriptGrep>,
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
        grep: grep.map(|grep| TranscriptGrepOutput {
            pattern: grep.pattern.clone(),
            before_context: grep.before_context,
            after_context: grep.after_context,
            match_count: transcript_match_count(
                events.iter().map(|event| event.content.as_str()),
                grep,
            ),
        }),
        events,
    })
}

fn history_transcript_output(
    store: &Store,
    context: &crate::storage::HistoryTranscriptContext,
    grep: Option<&TranscriptGrep>,
) -> Result<HistoryTranscriptOutput> {
    let target_event_id = context.target_event.as_ref().map(|event| event.id.clone());
    let target_ref = target_event_id
        .as_deref()
        .map(|event_id| store.recent_ref_for_event_id(event_id))
        .transpose()?
        .flatten();
    let items = context
        .items
        .iter()
        .map(|item| history_item_output(store, item))
        .collect::<Result<Vec<_>>>()?;
    Ok(HistoryTranscriptOutput {
        session: context.session.clone(),
        target_event_id,
        target_ref,
        target_index: context.target_index,
        omitted_target: context.omitted_target,
        grep: grep.map(|grep| TranscriptGrepOutput {
            pattern: grep.pattern.clone(),
            before_context: grep.before_context,
            after_context: grep.after_context,
            match_count: transcript_match_count(
                context.items.iter().map(|item| item.text.as_str()),
                grep,
            ),
        }),
        items,
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

fn history_item_output(
    store: &Store,
    item: &crate::storage::HistoryItemRecord,
) -> Result<HistoryItemOutput> {
    Ok(HistoryItemOutput {
        history_item_id: item.id.clone(),
        event_id: item.event_id.clone(),
        ref_id: store.recent_ref_for_event_id(&item.event_id)?,
        session_id: item.session_id.clone(),
        source_id: item.source_id.clone(),
        machine_id: item.machine_id.clone(),
        source_kind: item.source_kind.clone(),
        ordinal: item.ordinal,
        subordinal: item.subordinal,
        tier: item.tier.clone(),
        kind: item.kind.clone(),
        text: item.text.clone(),
        text_hash: item.text_hash.clone(),
        occurred_at: item.occurred_at,
        lexical_indexable: item.lexical_indexable,
        semantic_policy: item.semantic_policy.clone(),
        metadata: item.metadata.clone(),
        hash: item.hash.clone(),
    })
}

fn search_output(
    query: &str,
    limit: usize,
    sort: SearchSort,
    mode: search::SearchMode,
    recency_bias: f64,
    corpus: &search::SearchCorpus,
    show_duplicates: bool,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    project: Option<String>,
    machine: Option<String>,
    hostname: Option<String>,
    term_match: Option<search::SearchTermMatch>,
    match_terms: &[String],
    response: &search::SearchResponse,
    refs: &[String],
) -> SearchOutput {
    SearchOutput {
        query: query.to_string(),
        options: SearchOptionsOutput {
            limit,
            sort: sort.as_str(),
            mode: mode.as_str(),
            match_mode: term_match.map(search::SearchTermMatch::as_str),
            match_terms: match_terms.to_vec(),
            corpus: corpus.as_csv(),
            recency_bias,
            after,
            before,
            project,
            machine,
            hostname,
            show_duplicates,
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
                duplicate_group: result.duplicate_group.clone(),
            })
            .collect(),
        next_commands: search_hints(&response.results, refs),
    }
}

fn search_hints(results: &[search::SearchResult], refs: &[String]) -> Vec<String> {
    let Some(result) = results.first() else {
        return vec!["histo update --json".to_string()];
    };
    let Some(ref_id) = refs.first() else {
        return Vec::new();
    };
    vec![
        format!("histo show {ref_id} --json"),
        format!(
            "histo transcript {} --at {ref_id} --json",
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

#[derive(Debug, Clone)]
struct ResolvedSessionFilter {
    filter: crate::storage::SessionFilter,
    workspace_inferred: bool,
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

fn resolve_session_filter(
    args: &SessionFilterArgs,
    infer_cwd_scope: bool,
) -> Result<ResolvedSessionFilter> {
    let (workspace_scope, workspace_inferred) = if args.workspace.all {
        (None, false)
    } else if let Some(project) = args.workspace.project.as_deref() {
        (Some(transport::normalize_workspace_arg(project)), false)
    } else if infer_cwd_scope {
        let cwd = std::env::current_dir()?;
        (Some(transport::normalize_workspace_arg(&cwd)), true)
    } else {
        (None, false)
    };
    let workspace_basename = args
        .workspace
        .basename
        .as_deref()
        .map(normalize_basename_arg)
        .transpose()?;
    Ok(ResolvedSessionFilter {
        filter: crate::storage::SessionFilter {
            sources: args.source.clone(),
            workspace_scope,
            workspace_basename,
            machine_id: args
                .machine
                .machine
                .clone()
                .filter(|value| !value.trim().is_empty()),
            machine_id_prefix: args
                .machine
                .hostname
                .clone()
                .filter(|value| !value.trim().is_empty())
                .map(|value| search::machine_id_prefix_for_hostname(&value)),
        },
        workspace_inferred,
    })
}

fn normalize_basename_arg(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("--basename cannot be empty");
    }
    let basename = Path::new(value)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(value)
        .trim();
    if basename.is_empty() {
        bail!("--basename must include a folder name");
    }
    Ok(basename.to_string())
}

fn thread_scope_from_filter(filter: &ResolvedSessionFilter) -> ThreadScope {
    if let Some(path) = &filter.filter.workspace_scope {
        ThreadScope {
            mode: ThreadScopeMode::Path,
            path: Some(path.clone()),
            inferred: filter.workspace_inferred,
        }
    } else {
        ThreadScope {
            mode: ThreadScopeMode::All,
            path: None,
            inferred: false,
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
            Column::Tier,
            Column::Kind,
            Column::When,
            Column::Title,
            Column::Preview,
            Column::Similar,
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
            Column::Similar,
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

fn resolve_search_query(
    query_terms: Vec<String>,
    match_mode: Option<SearchMatchArg>,
) -> Result<ResolvedSearchQuery> {
    let terms = query_terms
        .into_iter()
        .map(|term| term.trim().to_string())
        .filter(|term| !term.is_empty())
        .collect::<Vec<_>>();
    let term_match = match match_mode {
        Some(match_mode) => Some(search::SearchTermMatch::from(match_mode)),
        None if terms.len() > 1 => Some(search::SearchTermMatch::All),
        None => None,
    };
    let query = terms.join(" ");
    Ok(ResolvedSearchQuery {
        query,
        term_match,
        terms,
    })
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
    if let Some(session) = resolve_session_target(store, target)? {
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

fn resolve_transcript_grep(
    pattern: Option<String>,
    before_context: Option<usize>,
    after_context: Option<usize>,
    context: Option<usize>,
) -> Result<Option<TranscriptGrep>> {
    if pattern.is_none()
        && (before_context.is_some() || after_context.is_some() || context.is_some())
    {
        bail!("transcript context flags require --grep");
    }

    let Some(pattern) = pattern.map(|value| value.trim().to_string()) else {
        return Ok(None);
    };
    if pattern.is_empty() {
        bail!("transcript --grep requires non-empty text");
    }

    let shared_context = context.unwrap_or(0);
    Ok(Some(TranscriptGrep {
        pattern,
        before_context: before_context.unwrap_or(shared_context),
        after_context: after_context.unwrap_or(shared_context),
    }))
}

fn grep_history_context(
    mut context: crate::storage::HistoryTranscriptContext,
    grep: &TranscriptGrep,
) -> crate::storage::HistoryTranscriptContext {
    let selected = grep_window_indices(
        context.items.len(),
        |idx| transcript_text_matches(&context.items[idx].text, grep),
        grep.before_context,
        grep.after_context,
    );
    let original_items = context.items;
    let target_event_id = context.target_event.as_ref().map(|event| event.id.as_str());
    let mut target_index = None;
    context.items = selected
        .into_iter()
        .enumerate()
        .map(|(new_idx, old_idx)| {
            let item = original_items[old_idx].clone();
            if target_event_id == Some(item.event_id.as_str()) {
                target_index = Some(new_idx);
            }
            item
        })
        .collect();
    context.target_index = target_index;
    context.omitted_target = target_event_id.is_some() && target_index.is_none();
    context
}

fn grep_events(
    events: &[crate::archive::EventRecord],
    grep: &TranscriptGrep,
) -> Vec<crate::archive::EventRecord> {
    grep_window_indices(
        events.len(),
        |idx| transcript_text_matches(&events[idx].content, grep),
        grep.before_context,
        grep.after_context,
    )
    .into_iter()
    .map(|idx| events[idx].clone())
    .collect()
}

fn grep_window_indices(
    len: usize,
    mut is_match: impl FnMut(usize) -> bool,
    before_context: usize,
    after_context: usize,
) -> Vec<usize> {
    let mut selected = Vec::new();
    let mut next_allowed = 0usize;

    for idx in 0..len {
        if !is_match(idx) {
            continue;
        }
        let start = idx.saturating_sub(before_context).max(next_allowed);
        let end = idx.saturating_add(after_context).saturating_add(1).min(len);
        selected.extend(start..end);
        next_allowed = end;
    }

    selected
}

fn transcript_text_matches(text: &str, grep: &TranscriptGrep) -> bool {
    text.to_ascii_lowercase()
        .contains(&grep.pattern.to_ascii_lowercase())
}

fn transcript_match_count<'a>(
    texts: impl IntoIterator<Item = &'a str>,
    grep: &TranscriptGrep,
) -> usize {
    texts
        .into_iter()
        .filter(|text| transcript_text_matches(text, grep))
        .count()
}

fn resolve_session_filter_targets(store: &Store, targets: Vec<String>) -> Result<Vec<String>> {
    targets
        .into_iter()
        .map(|target| {
            resolve_session_target(store, &target)?
                .map(|session| session.id)
                .ok_or_else(|| anyhow::anyhow!("session/native id not found: {target}"))
        })
        .collect()
}

fn resolve_session_target(
    store: &Store,
    target: &str,
) -> Result<Option<crate::archive::SessionRecord>> {
    if let Some(session) = store.session_by_id(target)? {
        return Ok(Some(session));
    }
    let sessions = store.sessions_by_external_id(target)?;
    match sessions.len() {
        0 => Ok(None),
        1 => Ok(sessions.into_iter().next()),
        _ => bail!(
            "ambiguous native session id {target}; matched {} sessions: {}",
            sessions.len(),
            format_ambiguous_sessions(&sessions)
        ),
    }
}

fn format_ambiguous_sessions(sessions: &[crate::archive::SessionRecord]) -> String {
    sessions
        .iter()
        .take(5)
        .map(|session| {
            let title = session
                .title
                .as_deref()
                .filter(|title| !title.trim().is_empty())
                .unwrap_or("untitled");
            format!("{}:{} ({title})", session.source_kind, session.id)
        })
        .collect::<Vec<_>>()
        .join(", ")
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

async fn run_transcript_tail(
    store: &Store,
    config: &AppConfig,
    target: &str,
    interval_secs: f64,
    initial_lines: usize,
    color: bool,
    verbose: bool,
) -> Result<()> {
    if interval_secs <= 0.0 {
        bail!("--interval must be greater than 0");
    }
    install_tail_sigint_handler();
    let interval = Duration::from_secs_f64(interval_secs);
    let (session, _) = resolve_transcript_target(store, target, None, None)?;
    let session_record = store
        .session_by_id(&session)?
        .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?;
    let tail_source_path = store
        .source_by_id(&session_record.source_id)?
        .and_then(|source| source.path)
        .map(PathBuf::from);
    let metadata = view_metadata_for_session(store, &session_record, None, verbose)?;

    ensure_history_items_ready(store)?;
    let context = store
        .history_items_for_transcript_session(&session)?
        .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?;
    let initial_context = tail_initial_context(context.clone(), initial_lines);
    write_stdout(&crate::transcript::render_history_session(
        &initial_context,
        &metadata,
        color,
    ))?;
    flush_stdout()?;
    let mut last_cursor = context.items.last().map(history_item_cursor);

    if refresh_tail_inputs(
        store,
        &config.machine_id,
        &session_record.source_kind,
        tail_source_path.as_deref(),
        &config.sources,
    )? && !append_tail_updates(store, &session, &mut last_cursor, color, verbose)?
    {
        return Ok(());
    }

    loop {
        if tail_cancelled() {
            break;
        }
        sleep_tail_interval(interval).await;
        if tail_cancelled() {
            break;
        }

        if !refresh_tail_inputs(
            store,
            &config.machine_id,
            &session_record.source_kind,
            tail_source_path.as_deref(),
            &config.sources,
        )? {
            break;
        }
        append_tail_updates(store, &session, &mut last_cursor, color, verbose)?;
    }
    Ok(())
}

fn tail_initial_context(
    mut context: crate::storage::HistoryTranscriptContext,
    initial_lines: usize,
) -> crate::storage::HistoryTranscriptContext {
    if context.items.len() > initial_lines {
        let start = context.items.len() - initial_lines;
        context.items = context.items.split_off(start);
    }
    context.target_event = None;
    context.target_index = None;
    context.omitted_target = false;
    context
}

fn append_tail_updates(
    store: &Store,
    session: &str,
    last_cursor: &mut Option<(i64, i64, String)>,
    color: bool,
    verbose: bool,
) -> Result<bool> {
    if tail_cancelled() {
        return Ok(false);
    }
    let context = store
        .history_items_for_transcript_session(session)?
        .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?;
    let new_items: Vec<_> = context
        .items
        .into_iter()
        .filter(|item| match last_cursor.as_ref() {
            Some(cursor) => history_item_is_after(item, cursor),
            None => true,
        })
        .collect();
    if new_items.is_empty() {
        return Ok(true);
    }
    *last_cursor = new_items.last().map(history_item_cursor);
    write_stdout(&crate::transcript::render_history_items(
        &new_items, color, verbose,
    ))?;
    flush_stdout()?;
    Ok(!tail_cancelled())
}

fn refresh_tail_inputs(
    store: &Store,
    machine_id: &str,
    source_kind: &str,
    source_path: Option<&Path>,
    sources: &crate::config::SourceConfigs,
) -> Result<bool> {
    let stats = match source_path {
        Some(path) => ingest::update_source_path_with_progress_and_cancel(
            store,
            machine_id,
            source_kind,
            path,
            |_| {},
            tail_cancelled,
        ),
        None => ingest::update_local_with_progress_and_cancel(
            store,
            machine_id,
            ingest::UpdateOptions {
                max_files: None,
                source_selection: ingest::SourceSelection::single(source_kind)?,
                sources: sources.clone(),
            },
            |_| {},
            tail_cancelled,
        ),
    };
    let stats = match stats {
        Ok(stats) => stats,
        Err(_) if tail_cancelled() => return Ok(false),
        Err(err) if is_database_locked_error(&err) => {
            warn_tail_database_locked_once();
            return Ok(true);
        }
        Err(err) => return Err(err),
    };
    if tail_cancelled() {
        return Ok(false);
    }
    if stats.delta.touched_events.is_empty() {
        return Ok(true);
    }
    if let Err(err) = refresh_tail_history_items(store, &stats.delta.touched_events) {
        if tail_cancelled() {
            return Ok(false);
        }
        if is_database_locked_error(&err) {
            warn_tail_database_locked_once();
            return Ok(true);
        }
        return Err(err);
    }
    Ok(!tail_cancelled())
}

async fn sleep_tail_interval(interval: Duration) {
    let step = Duration::from_millis(100);
    let started = Instant::now();
    while started.elapsed() < interval && !tail_cancelled() {
        let remaining = interval.saturating_sub(started.elapsed());
        tokio::time::sleep(remaining.min(step)).await;
    }
}

fn install_tail_sigint_handler() {
    TAIL_CANCELLED.store(false, Ordering::SeqCst);
    TAIL_LOCK_WARNED.store(false, Ordering::SeqCst);
    unsafe {
        libc::signal(
            libc::SIGINT,
            handle_tail_sigint as *const () as libc::sighandler_t,
        );
    }
}

extern "C" fn handle_tail_sigint(_: libc::c_int) {
    if TAIL_CANCELLED.swap(true, Ordering::SeqCst) {
        unsafe {
            libc::_exit(130);
        }
    }
}

fn tail_cancelled() -> bool {
    TAIL_CANCELLED.load(Ordering::SeqCst)
}

fn is_database_locked_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .and_then(|err| match err {
                rusqlite::Error::SqliteFailure(error, _) => Some(error.code),
                _ => None,
            })
            .is_some_and(|code| {
                matches!(
                    code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                )
            })
    })
}

fn warn_tail_database_locked_once() {
    if !TAIL_LOCK_WARNED.swap(true, Ordering::SeqCst) && !tail_cancelled() {
        eprintln!("tail: database is busy; showing stored transcript and retrying");
    }
}

fn ensure_history_items_ready(store: &Store) -> Result<()> {
    if store.history_items_projection_ready()? {
        return Ok(());
    }
    store.refresh_history_items()?;
    Ok(())
}

fn refresh_tail_history_items(store: &Store, event_ids: &[String]) -> Result<()> {
    if store.history_items_projection_ready()? {
        store.refresh_history_items_for_events(event_ids)?;
    } else {
        store.refresh_history_items()?;
    }
    Ok(())
}

fn history_item_cursor(item: &crate::storage::HistoryItemRecord) -> (i64, i64, String) {
    (item.ordinal, item.subordinal, item.id.clone())
}

fn history_item_is_after(
    item: &crate::storage::HistoryItemRecord,
    cursor: &(i64, i64, String),
) -> bool {
    (item.ordinal, item.subordinal, item.id.as_str()) > (cursor.0, cursor.1, cursor.2.as_str())
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

fn search_time_bounds(
    today: bool,
    after: Option<&str>,
    before: Option<&str>,
) -> Result<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    if today {
        return Ok((
            Some(parse_search_time("today", TimeFilterBound::After)?),
            Some(parse_search_time("today", TimeFilterBound::Before)?),
        ));
    }
    Ok((
        parse_optional_search_time(after, TimeFilterBound::After)?,
        parse_optional_search_time(before, TimeFilterBound::Before)?,
    ))
}

fn search_workspace_scope(project: Option<&std::path::Path>, all: bool) -> Option<String> {
    if all {
        return None;
    }
    project.map(transport::normalize_workspace_arg)
}

fn prune_filter(
    filter_args: SessionFilterArgs,
    sessions: Vec<String>,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
) -> Result<crate::storage::PruneFilter> {
    let resolved = resolve_session_filter(&filter_args, false)?;
    let filter = crate::storage::PruneFilter {
        session_filter: resolved.filter,
        sessions,
        after,
        before,
    };
    if !filter.has_selector() {
        bail!(
            "prune requires at least one filter, such as --before, --today, --project, --basename, --machine, --hostname, --source, or --session"
        );
    }
    Ok(filter)
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

fn run_static_fzf_search(
    store: &Store,
    config: &AppConfig,
    query: &str,
    rows: &str,
    color: bool,
) -> Result<()> {
    ensure_fzf_available()?;
    let preview = local_fzf_preview_command(config, color);
    let copy = fzf_copy_session_command();
    let mut child = base_fzf_command()
        .arg("--query")
        .arg(query)
        .arg("--header")
        .arg(fzf_header())
        .arg("--preview")
        .arg(preview)
        .arg("--bind")
        .arg(format!("ctrl-y:execute-silent[{copy}]"))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-up", "preview-up", 10))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-down", "preview-down", 10))
        .arg("--bind")
        .arg("ctrl-u:preview-half-page-up")
        .arg("--bind")
        .arg("ctrl-d:preview-half-page-down")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(rows.as_bytes())?;
    }
    let output = child.wait_with_output()?;
    if let Some(selection) = fzf_selection_from_output(&output)? {
        handle_local_fzf_selection(store, selection, color)?;
    }
    Ok(())
}

fn run_tui_search(
    store: &Store,
    config: &AppConfig,
    query: &str,
    backend: &TuiBackend,
    limit: usize,
    sort: SearchSort,
    mode: search::SearchMode,
    corpus: search::SearchCorpus,
    show_duplicates: bool,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    project: Option<String>,
    machine: Option<String>,
    hostname: Option<String>,
    color: bool,
) -> Result<()> {
    ensure_fzf_available()?;
    ensure_curl_available()?;
    let _server = ensure_tui_backend_available(config, backend)?;
    let server = backend.base_url();
    let reload = tui_reload_command(
        &server,
        limit,
        sort,
        mode,
        &corpus,
        show_duplicates,
        recency_bias,
        after,
        before,
        project.as_deref(),
        machine.as_deref(),
        hostname.as_deref(),
    )?;
    let preview = tui_preview_command(config, backend, color)?;
    let copy = fzf_copy_session_command();
    let child = base_fzf_command()
        .arg("--query")
        .arg(query)
        .arg("--header")
        .arg(fzf_header())
        .arg("--preview")
        .arg(preview)
        .arg("--bind")
        .arg(format!("start:reload({reload})"))
        .arg("--bind")
        .arg(format!(
            "change:reload(sleep {LIVE_SEARCH_RELOAD_DELAY_SECS}; {reload})"
        ))
        .arg("--bind")
        .arg(format!("ctrl-y:execute-silent[{copy}]"))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-up", "preview-up", 10))
        .arg("--bind")
        .arg(preview_scroll_bind("shift-down", "preview-down", 10))
        .arg("--bind")
        .arg("ctrl-u:preview-half-page-up")
        .arg("--bind")
        .arg("ctrl-d:preview-half-page-down")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()?;
    let output = child.wait_with_output()?;
    if let Some(selection) = fzf_selection_from_output(&output)? {
        handle_tui_fzf_selection(store, backend, selection, color)?;
    }
    Ok(())
}

fn base_fzf_command() -> ProcessCommand {
    let mut command = ProcessCommand::new("fzf");
    command
        .arg("--ansi")
        .arg("--no-hscroll")
        .arg("--prompt")
        .arg("Search> ")
        .arg("--delimiter")
        .arg("\t")
        .arg("--nth")
        .arg("1,2,3,4,5,8,9,10")
        .arg("--with-nth")
        .arg("1,2,3,4,5");
    command
}

fn fzf_header() -> &'static str {
    "Enter: open transcript | Ctrl-Y: copy session id\nRef\tSource\tMatch\tWhen\tPreview"
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FzfSelection {
    row: FzfSelectedRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FzfSelectedRow {
    source_kind: String,
    session_id: String,
    event_id: String,
    workspace: Option<String>,
    machine_id: Option<String>,
    full: bool,
}

fn fzf_selection_from_output(output: &std::process::Output) -> Result<Option<FzfSelection>> {
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_fzf_selection_output(&stdout)
}

fn parse_fzf_selection_output(output: &str) -> Result<Option<FzfSelection>> {
    let mut lines = output.lines();
    let Some(first) = lines.next() else {
        return Ok(None);
    };
    let row = match first {
        "enter" | "" => lines.next(),
        row => Some(row),
    };
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(FzfSelection {
        row: parse_fzf_selected_row(row)?,
    }))
}

fn parse_fzf_selected_row(row: &str) -> Result<FzfSelectedRow> {
    let fields = row.split('\t').collect::<Vec<_>>();
    let source_kind = fields.get(1).copied().unwrap_or("-").to_string();
    let session_id = fields
        .get(5)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("fzf selection is missing a session id"))?;
    let event_id = fields
        .get(6)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("fzf selection is missing an event id"))?;
    Ok(FzfSelectedRow {
        source_kind,
        session_id: (*session_id).to_string(),
        event_id: (*event_id).to_string(),
        workspace: fields
            .get(8)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        machine_id: fields
            .get(9)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        full: fields
            .get(11)
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false),
    })
}

fn handle_local_fzf_selection(store: &Store, selection: FzfSelection, color: bool) -> Result<()> {
    open_fzf_selection_in_pager(store, &selection.row, color)?;
    print_fzf_selection_details(store, &selection.row)?;
    Ok(())
}

fn handle_tui_fzf_selection(
    store: &Store,
    backend: &TuiBackend,
    selection: FzfSelection,
    color: bool,
) -> Result<()> {
    match backend {
        TuiBackend::LocalAuto { .. } => handle_local_fzf_selection(store, selection, color),
        TuiBackend::ServerUrl { base_url } => {
            open_remote_fzf_selection_in_pager(base_url, &selection.row, color)?;
            print_remote_fzf_selection_details(&selection.row)
        }
    }
}

fn open_fzf_selection_in_pager(store: &Store, row: &FzfSelectedRow, color: bool) -> Result<()> {
    let session = store
        .session_by_id(&row.session_id)?
        .ok_or_else(|| anyhow::anyhow!("session not found: {}", row.session_id))?;
    let target_event = store
        .event_by_id(&row.event_id)?
        .ok_or_else(|| anyhow::anyhow!("event not found: {}", row.event_id))?;
    if target_event.session_id != session.id {
        bail!(
            "event {} belongs to session {}, not {}",
            row.event_id,
            target_event.session_id,
            session.id
        );
    }
    let metadata = view_metadata_for_session(store, &session, Some(&target_event), false)?;
    if row.full {
        let events = store.events_for_session(&session.id)?;
        let rendered = crate::transcript::render_session(
            &session,
            &events,
            Some(&row.event_id),
            &metadata,
            color,
        );
        page_or_print(&rendered, Some(&row.event_id), false)?;
    } else {
        let context = store
            .history_items_around_event(&row.event_id, usize::MAX / 4, usize::MAX / 4)?
            .ok_or_else(|| anyhow::anyhow!("event not found: {}", row.event_id))?;
        let rendered = crate::transcript::render_history_session(&context, &metadata, color);
        page_or_print(&rendered, Some(&row.event_id), false)?;
    }
    Ok(())
}

fn open_remote_fzf_selection_in_pager(
    server: &str,
    row: &FzfSelectedRow,
    color: bool,
) -> Result<()> {
    let rendered = fetch_remote_fzf_selection(server, row, color)?;
    page_or_print(&rendered, Some(&row.event_id), false)
}

fn fetch_remote_fzf_selection(server: &str, row: &FzfSelectedRow, color: bool) -> Result<String> {
    let transcript_url = server_url(server, "transcript")?;
    let mut command = ProcessCommand::new("curl");
    command
        .arg("-fsSG")
        .arg("--connect-timeout")
        .arg("2")
        .arg("--max-time")
        .arg("10")
        .arg(&transcript_url)
        .arg("--data-urlencode")
        .arg(format!("session={}", row.session_id))
        .arg("--data-urlencode")
        .arg(format!("at={}", row.event_id))
        .arg("--data-urlencode")
        .arg(format!("color={}", if color { "always" } else { "never" }));
    if row.full {
        command.arg("--data-urlencode").arg("full=true");
    }
    let output = command.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let detail = if stderr.is_empty() {
            format!("status {}", output.status)
        } else {
            stderr
        };
        bail!("remote transcript request failed for {transcript_url}: {detail}");
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn print_fzf_selection_details(store: &Store, row: &FzfSelectedRow) -> Result<()> {
    let source_path =
        fzf_selection_source_path(store, &row.session_id)?.unwrap_or_else(|| "-".to_string());
    write_stdout(&format!(
        "session_id: {}\nsource_path: {}\n",
        row.session_id, source_path
    ))
}

fn print_remote_fzf_selection_details(row: &FzfSelectedRow) -> Result<()> {
    let workspace = row.workspace.as_deref().unwrap_or("-");
    let machine = row.machine_id.as_deref().unwrap_or("-");
    write_stdout(&format!(
        "session_id: {}\nevent_id: {}\nsource: {}\nworkspace: {}\nmachine_id: {}\n",
        row.session_id, row.event_id, row.source_kind, workspace, machine
    ))
}

fn fzf_selection_source_path(store: &Store, session_id: &str) -> Result<Option<String>> {
    let Some(session) = store.session_by_id(session_id)? else {
        return Ok(None);
    };
    Ok(store
        .source_by_id(&session.source_id)?
        .and_then(|source| source.path)
        .filter(|path| !path.trim().is_empty()))
}

fn fzf_copy_session_command() -> &'static str {
    "value={6}; (command -v pbcopy >/dev/null 2>&1 && printf %s \"$value\" | pbcopy) || (command -v wl-copy >/dev/null 2>&1 && printf %s \"$value\" | wl-copy) || (command -v xclip >/dev/null 2>&1 && printf %s \"$value\" | xclip -selection clipboard) || (command -v xsel >/dev/null 2>&1 && printf %s \"$value\" | xsel --clipboard --input) || (command -v clip.exe >/dev/null 2>&1 && printf %s \"$value\" | clip.exe) || true"
}

fn local_fzf_preview_command(config: &AppConfig, color: bool) -> String {
    let current_exe = std::env::current_exe().ok();
    let exe = current_exe
        .as_ref()
        .map(|path| shell_quote(&path.to_string_lossy()))
        .unwrap_or_else(|| "histo".to_string());
    let data_dir = shell_quote(&config.data_dir.to_string_lossy());
    let color_flag = if color {
        " --color always"
    } else {
        " --no-color"
    };
    format!(
        "mode={{12}}; if [ -n \"$mode\" ]; then {exe} --data-dir {data_dir} show {{7}} \"$mode\" --before 3 --after 5{color_flag}; else {exe} --data-dir {data_dir} show {{7}} --before 3 --after 5{color_flag}; fi"
    )
}

fn tui_preview_command(config: &AppConfig, backend: &TuiBackend, color: bool) -> Result<String> {
    match backend {
        TuiBackend::LocalAuto { .. } => Ok(local_fzf_preview_command(config, color)),
        TuiBackend::ServerUrl { base_url } => remote_fzf_preview_command(base_url, color),
    }
}

fn remote_fzf_preview_command(server: &str, color: bool) -> Result<String> {
    let show_url = shell_quote(&server_url(server, "show")?);
    let color_value = if color { "always" } else { "never" };
    Ok(format!(
        "mode={{12}}; full_arg=; if [ -n \"$mode\" ]; then full_arg=' --data-urlencode full=true'; fi; curl -fsSG --connect-timeout 2 --max-time 10 {show_url} --data-urlencode event={{7}} --data-urlencode before=3 --data-urlencode after=5 --data-urlencode color={color_value}$full_arg 2>/dev/null || :"
    ))
}

fn tui_reload_command(
    server: &str,
    limit: usize,
    sort: SearchSort,
    mode: search::SearchMode,
    corpus: &search::SearchCorpus,
    show_duplicates: bool,
    recency_bias: f64,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    project: Option<&str>,
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
    let project_arg = project
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            format!(
                " --data-urlencode {}",
                shell_quote(&format!("project={value}"))
            )
        })
        .unwrap_or_default();
    let duplicate_arg = if show_duplicates {
        " --data-urlencode show_duplicates=true"
    } else {
        ""
    };
    Ok(format!(
        "if [ -z {{q}} ]; then :; else curl -fsSG --connect-timeout 2 --max-time 10 {search_url} --data-urlencode q={{q}} --data-urlencode limit={limit} --data-urlencode sort={} --data-urlencode mode={} --data-urlencode corpus={} --data-urlencode recency_bias={recency_bias} --data-urlencode format=fzf{after_arg}{before_arg}{project_arg}{machine_arg}{hostname_arg}{duplicate_arg} 2>/dev/null || :; fi",
        sort.as_str(),
        mode.as_str(),
        shell_quote(&corpus.as_csv())
    ))
}

fn ensure_fzf_available() -> Result<()> {
    if !command_exists("fzf") {
        bail!(
            "fzf is required for `histo tui` and `histo search --fzf`, but it was not found on PATH.\nInstall it with `brew install fzf` on macOS or `sudo apt install fzf` on Debian/Ubuntu.\nWithout fzf, use `histo search <query>` and then `histo show <ref>` or `histo transcript <session_id> --at <ref>`."
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum TuiBackend {
    LocalAuto { base_url: String },
    ServerUrl { base_url: String },
}

impl TuiBackend {
    fn base_url(&self) -> String {
        match self {
            Self::LocalAuto { base_url } | Self::ServerUrl { base_url } => base_url.clone(),
        }
    }
}

fn resolve_tui_backend(server_url: Option<String>) -> Result<TuiBackend> {
    if let Some(server_url) = server_url {
        let base_url = normalize_non_empty_server_url(&server_url, "server URL")?;
        return Ok(TuiBackend::ServerUrl { base_url });
    }
    let base_url = normalize_non_empty_server_url(DEFAULT_SERVER_URL, "server URL")?;
    Ok(TuiBackend::LocalAuto { base_url })
}

fn normalize_non_empty_server_url(value: &str, name: &str) -> Result<String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("{name} URL cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn ensure_tui_backend_available(
    config: &AppConfig,
    backend: &TuiBackend,
) -> Result<Option<StartedServer>> {
    match backend {
        TuiBackend::LocalAuto { base_url } => ensure_server_available(config, base_url),
        TuiBackend::ServerUrl { base_url } => {
            if server_health_available(base_url)? {
                Ok(None)
            } else {
                let health_url = server_url(base_url, "health")?;
                bail!(
                    "could not reach Historious server at {health_url}. Start one with `histo serve` or check the URL."
                );
            }
        }
    }
}

fn ensure_server_available(config: &AppConfig, server: &str) -> Result<Option<StartedServer>> {
    if server_health_available(server)? {
        return Ok(None);
    }
    let Some(bind) = local_server_bind_addr(server) else {
        let health_url = server_url(server, "health")?;
        bail!("could not reach Historious server at {health_url}");
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
    eprintln!("Starting local Historious server at {bind}...");
    let exe = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("histo"));
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
            bail!("local Historious server exited before it became ready: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let health_url = server_url(server, "health")?;
    bail!("local Historious server did not become ready at {health_url}")
}

fn parse_server_bind_addr(bind: &str, allow_network_bind: bool) -> Result<SocketAddr> {
    let addr: SocketAddr = bind.parse()?;
    if !allow_network_bind && !addr.ip().is_loopback() {
        bail!(
            "refusing to bind unauthenticated Historious HTTP server to {addr}. Use `--allow-network-bind` only on a trusted network."
        );
    }
    Ok(addr)
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

fn flush_stdout() -> Result<()> {
    let mut stdout = io::stdout().lock();
    match stdout.flush() {
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
        "similar" | "duplicates" | "duplicate_group" => Ok(Column::Similar),
        _ => bail!(
            "unknown column '{name}'. Available columns: ref,source,match,when,title,preview,similar,score,lex,sem,machine,event,session,item,tier,kind,ids"
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
        Column::Similar => similar_cell(result.duplicate_group.len()),
    }
}

fn similar_cell(count: usize) -> String {
    if count == 0 {
        "-".to_string()
    } else {
        format!("+{count} similar")
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
        Column::Similar => "Similar",
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
    source_configs: crate::config::SourceConfigs,
    interval_secs: u64,
    max_files: Option<usize>,
    source: Vec<String>,
) -> Result<()> {
    let interval = std::time::Duration::from_secs(interval_secs.max(1));
    let source_selection = ingest::SourceSelection::parse(source)?;
    let progress = ProgressUi::new();
    loop {
        let mut scan = progress.phase("Scanning local agent logs");
        let stats = ingest::update_local_with_progress(
            store,
            machine_id,
            ingest::UpdateOptions {
                max_files,
                source_selection: source_selection.clone(),
                sources: source_configs.clone(),
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

        let mut index = progress.phase("Updating search index");
        let projected = refresh_search_after_update_with_progress(
            store,
            &stats.delta,
            false,
            embedder_config.is_disabled(),
            |detail| {
                index.update(detail);
            },
        )?;
        index.finish(format!("{} events indexed", format_count(projected)));

        let embeddings = if embedder_config.is_disabled() {
            search::EmbeddingRefresh::disabled()
        } else {
            let mut embed = progress.phase("Updating embeddings");
            let embeddings = search::refresh_embeddings_incremental_with_progress(
                store,
                machine_id,
                &embedder_config,
                &stats.delta,
                |event| embed.update(embedding_progress_detail(event)),
            )?;
            embed.finish(embedding_phase_detail(&embeddings));
            embeddings
        };

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
    if config.is_disabled() {
        return (None, None);
    }
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
                Column::Similar,
                Column::Score,
                Column::Session,
                Column::Event,
                Column::Item
            ]
        );
    }

    #[test]
    fn similar_cell_marks_collapsed_duplicate_count() {
        assert_eq!(similar_cell(0), "-");
        assert_eq!(similar_cell(3), "+3 similar");
    }

    #[test]
    fn machine_progress_event_has_jsonl_safe_shape() {
        let event = MachineProgressEvent {
            event_type: "progress",
            command: "update",
            phase: "embeddings",
            detail: "64 embedded, 128 pending, batch 64".to_string(),
            data: json!({
                "status": "batch",
                "embedded": 64,
                "pending": 128,
                "batch_size": 64,
            }),
        };
        let value = serde_json::to_value(event).expect("serialize progress event");

        assert_eq!(value["type"], "progress");
        assert_eq!(value["command"], "update");
        assert_eq!(value["phase"], "embeddings");
        assert_eq!(value["detail"], "64 embedded, 128 pending, batch 64");
        assert_eq!(value["data"]["status"], "batch");
        assert_eq!(value["data"]["pending"], 128);
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
            workspace_values: vec!["/home/example/projects/historious".to_string()],
            snippet: "preview with\nnew line".to_string(),
            duplicate_group: Vec::new(),
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
        assert_eq!(fields[8], "/home/example/projects/historious");
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
    fn fzf_selection_output_parses_enter_selected_rows() {
        let row = [
            "ab3f",
            "codex",
            "hybrid",
            "-",
            "preview",
            "session_1",
            "event_1",
            "",
            "/tmp/workspace",
            "machine_1",
            "hi_1",
            "--full",
        ]
        .join("\t");

        let enter = parse_fzf_selection_output(&format!("{row}\n"))
            .expect("parse enter")
            .expect("selection");
        assert_eq!(enter.row.source_kind, "codex");
        assert_eq!(enter.row.session_id, "session_1");
        assert_eq!(enter.row.event_id, "event_1");
        assert_eq!(enter.row.workspace.as_deref(), Some("/tmp/workspace"));
        assert_eq!(enter.row.machine_id.as_deref(), Some("machine_1"));
        assert!(enter.row.full);

        let explicit_enter = parse_fzf_selection_output(&format!("\n{row}\n"))
            .expect("parse explicit enter")
            .expect("selection");
        assert_eq!(explicit_enter.row.session_id, "session_1");

        assert!(parse_fzf_selection_output("")
            .expect("missing row")
            .is_none());
    }

    #[test]
    fn fzf_copy_session_command_checks_common_clipboards() {
        let command = fzf_copy_session_command();

        assert!(command.contains("value={6}"));
        assert!(command.contains("pbcopy"));
        assert!(command.contains("wl-copy"));
        assert!(command.contains("xclip -selection clipboard"));
        assert!(command.contains("xsel --clipboard --input"));
        assert!(command.contains("clip.exe"));
    }

    #[test]
    fn fzf_selection_source_path_uses_session_source_record() {
        let (_dir, store) = fixture_store_with_viewer_ref();

        let path = fzf_selection_source_path(&store, "session_view").expect("source path");

        assert_eq!(path.as_deref(), Some("/tmp/source.jsonl"));
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
        assert!(args.windows(2).any(|pair| pair == ["--prompt", "Search> "]));
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
            duplicate_group: vec![search::DuplicateSearchMember {
                history_item_id: Some("hi_2".to_string()),
                match_type: search::MatchType::Lexical,
                event_id: "event_2".to_string(),
                session_id: "session_2".to_string(),
                machine_id: "machine_devbox_123".to_string(),
                source_kind: "codex".to_string(),
                tier: Some("conversation".to_string()),
                kind: "user".to_string(),
                score: 0.25,
                lexical_rank: Some(3),
                semantic_rank: None,
                occurred_at: None,
                session_title: Some("Forked session".to_string()),
                workspace_values: vec!["/tmp/workspace".to_string()],
                snippet: "useful snippet".to_string(),
            }],
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
            false,
            None,
            None,
            None,
            Some("machine_devbox_123".to_string()),
            Some("devbox".to_string()),
            None,
            &[],
            &response,
            &["ab3f".to_string()],
        );
        let value = serde_json::to_value(output).expect("serialize search output");

        assert_eq!(value["query"], "workspace metadata");
        assert_eq!(value["options"]["limit"], 20);
        assert_eq!(value["options"]["sort"], "relevance");
        assert_eq!(value["options"]["mode"], "semantic");
        assert_eq!(value["options"]["match_mode"], serde_json::Value::Null);
        assert_eq!(value["options"]["match_terms"], serde_json::json!([]));
        assert_eq!(value["options"]["corpus"], "conversation,tool");
        assert_eq!(value["options"]["show_duplicates"], false);
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
        assert_eq!(
            value["results"][0]["duplicate_group"][0]["event_id"],
            "event_2"
        );
        assert_eq!(
            value["results"][0]["duplicate_group"][0]["history_item_id"],
            "hi_2"
        );
        assert_eq!(value["next_commands"][0], "histo show ab3f --json");
        assert_eq!(
            value["next_commands"][1],
            "histo transcript session_1 --at ab3f --json"
        );
    }

    #[test]
    fn search_query_resolver_defaults_multiple_terms_to_and() {
        let resolved = resolve_search_query(vec!["needle".to_string(), "red".to_string()], None)
            .expect("query");

        assert_eq!(resolved.query, "needle red");
        assert_eq!(resolved.term_match, Some(search::SearchTermMatch::All));
        assert_eq!(resolved.terms, vec!["needle", "red"]);
    }

    #[test]
    fn search_query_resolver_keeps_single_query_compatible() {
        let resolved = resolve_search_query(vec!["needle red".to_string()], None).expect("query");

        assert_eq!(resolved.query, "needle red");
        assert_eq!(resolved.term_match, None);
        assert_eq!(resolved.terms, vec!["needle red"]);
    }

    #[test]
    fn search_query_resolver_accepts_multiple_terms_with_match() {
        let resolved = resolve_search_query(
            vec!["needle".to_string(), "red".to_string()],
            Some(SearchMatchArg::Or),
        )
        .expect("query");

        assert_eq!(resolved.query, "needle red");
        assert_eq!(resolved.term_match, Some(search::SearchTermMatch::Any));
        assert_eq!(resolved.terms, vec!["needle", "red"]);
    }

    #[test]
    fn search_match_mode_accepts_and_or_with_all_any_aliases() {
        for value in ["and", "all"] {
            let cli = Cli::try_parse_from(["histo", "search", "--match", value, "needle"])
                .expect("parse and match mode");
            match cli.command {
                Command::Search { match_mode, .. } => {
                    assert!(matches!(match_mode, Some(SearchMatchArg::And)));
                }
                _ => panic!("expected search command"),
            }
        }

        for value in ["or", "any"] {
            let cli = Cli::try_parse_from(["histo", "search", "--match", value, "needle"])
                .expect("parse or match mode");
            match cli.command {
                Command::Search { match_mode, .. } => {
                    assert!(matches!(match_mode, Some(SearchMatchArg::Or)));
                }
                _ => panic!("expected search command"),
            }
        }
    }

    #[test]
    fn shell_quote_handles_spaces_and_single_quotes() {
        assert_eq!(shell_quote("/tmp/a path/it's"), "'/tmp/a path/it'\\''s'");
    }

    #[test]
    fn robot_mode_is_global_and_requests_structured_errors() {
        let cli = Cli::try_parse_from(["histo", "--robot", "search", "needle"])
            .expect("parse robot search");

        assert!(cli.robot);
        assert_eq!(cli.command_name(), "search");
        assert!(cli.wants_structured_errors());
    }

    #[test]
    fn tui_accepts_missing_starting_query() {
        let cli = Cli::try_parse_from(["histo", "tui"]).expect("parse tui search");

        match cli.command {
            Command::Tui {
                query,
                limit,
                server_url,
                ..
            } => {
                assert_eq!(query, None);
                assert_eq!(limit, DEFAULT_LIVE_SEARCH_LIMIT);
                assert_eq!(server_url, None);
            }
            _ => panic!("expected tui command"),
        }
    }

    #[test]
    fn tui_accepts_explicit_server_url_backend() {
        let cli = Cli::try_parse_from([
            "histo",
            "tui",
            "--server-url",
            "http://example.com:7391/",
            "query",
        ])
        .expect("parse server-url tui search");

        match cli.command {
            Command::Tui {
                query, server_url, ..
            } => {
                assert_eq!(query.as_deref(), Some("query"));
                assert_eq!(server_url.as_deref(), Some("http://example.com:7391/"));
            }
            _ => panic!("expected tui command"),
        }
    }

    #[test]
    fn tui_keeps_legacy_remote_alias_for_server_url() {
        let cli = Cli::try_parse_from(["histo", "tui", "--remote", "http://example.com:7391"])
            .expect("parse legacy remote alias");

        match cli.command {
            Command::Tui { server_url, .. } => {
                assert_eq!(server_url.as_deref(), Some("http://example.com:7391"));
            }
            _ => panic!("expected tui command"),
        }
    }

    #[test]
    fn tui_backend_resolution_distinguishes_default_and_explicit_url() {
        assert_eq!(
            resolve_tui_backend(None).expect("default backend"),
            TuiBackend::LocalAuto {
                base_url: DEFAULT_SERVER_URL.to_string()
            }
        );
        assert_eq!(
            resolve_tui_backend(Some(" http://localhost:7000/ ".to_string()))
                .expect("server URL backend"),
            TuiBackend::ServerUrl {
                base_url: "http://localhost:7000".to_string()
            }
        );
        assert_eq!(
            resolve_tui_backend(Some(" http://remote:7391/ ".to_string()))
                .expect("server URL backend"),
            TuiBackend::ServerUrl {
                base_url: "http://remote:7391".to_string()
            }
        );
        assert!(resolve_tui_backend(Some("   ".to_string())).is_err());
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
    fn embedding_override_short_flags_parse_for_search() {
        let cli = Cli::try_parse_from(["histo", "search", "-e", "needle"])
            .expect("parse embeddings override");
        match cli.command {
            Command::Search {
                embeddings,
                no_embeddings,
                ..
            } => {
                assert!(embeddings);
                assert!(!no_embeddings);
            }
            _ => panic!("expected search command"),
        }

        let cli = Cli::try_parse_from(["histo", "search", "-E", "needle"])
            .expect("parse no embeddings override");
        match cli.command {
            Command::Search {
                embeddings,
                no_embeddings,
                ..
            } => {
                assert!(!embeddings);
                assert!(no_embeddings);
            }
            _ => panic!("expected search command"),
        }
    }

    #[test]
    fn embedding_override_flags_conflict_for_search() {
        let error = Cli::try_parse_from([
            "histo",
            "search",
            "--embeddings",
            "--no-embeddings",
            "needle",
        ])
        .expect_err("embedding override flags should conflict");

        assert!(error.to_string().contains("cannot be used with"));
    }

    #[test]
    fn embedding_override_can_force_config_disabled_embedder_on() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert!(config.embedder.is_disabled());
        apply_embeddings_override(&mut config, true, false);
        assert!(!config.embedder.is_disabled());
        apply_embeddings_override(&mut config, false, true);
        assert!(config.embedder.is_disabled());
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

        let with_color = local_fzf_preview_command(&config, true);
        let without_color = local_fzf_preview_command(&config, false);

        assert!(with_color.contains("mode={12}; if [ -n \"$mode\" ]"));
        assert!(with_color.contains("show {7} \"$mode\""));
        assert!(with_color.contains("show {7} --before 3 --after 5"));
        assert!(with_color.contains("--color always"));
        assert!(!with_color.contains("--no-color"));
        assert!(without_color.contains("--no-color"));
    }

    #[test]
    fn remote_fzf_preview_queries_server_show_endpoint() {
        let command =
            remote_fzf_preview_command("http://remote.example:7391/", true).expect("preview");

        assert!(command.contains("curl -fsSG --connect-timeout 2 --max-time 10"));
        assert!(command.contains("'http://remote.example:7391/show'"));
        assert!(command.contains("--data-urlencode event={7}"));
        assert!(command.contains("--data-urlencode before=3"));
        assert!(command.contains("--data-urlencode after=5"));
        assert!(command.contains("--data-urlencode color=always"));
        assert!(command.contains("full_arg=' --data-urlencode full=true'"));
        assert!(command.contains(" 2>/dev/null || :"));
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
            true,
            0.25,
            Some(after),
            None,
            None,
            Some("machine_devbox_123"),
            Some("devbox"),
        )
        .expect("reload command");

        assert!(command.contains("curl -fsSG --connect-timeout 2 --max-time 10"));
        assert!(command.contains("sort=newest"));
        assert!(command.contains("mode=lexical"));
        assert!(command.contains("corpus='conversation,tool'"));
        assert!(command.contains("recency_bias=0.25"));
        assert!(command.contains("after=2026-04-20T00:00:00+00:00"));
        assert!(command.contains("machine=machine_devbox_123"));
        assert!(command.contains("hostname=devbox"));
        assert!(command.contains("show_duplicates=true"));
        assert!(command.contains(" 2>/dev/null"));
        assert!(command.contains(" || :; fi"));
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
    fn serve_bind_rejects_network_addresses_without_opt_in() {
        assert!(parse_server_bind_addr("127.0.0.1:7391", false).is_ok());
        assert!(parse_server_bind_addr("[::1]:7391", false).is_ok());
        assert!(parse_server_bind_addr("0.0.0.0:7391", false).is_err());
        assert!(parse_server_bind_addr("0.0.0.0:7391", true).is_ok());
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

    #[test]
    fn clean_transcript_json_uses_history_items() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let session = store
            .session_by_id("session_view")
            .expect("session lookup")
            .expect("session exists");
        let event = store
            .event_by_id("event_view")
            .expect("event lookup")
            .expect("event exists");
        let context = crate::storage::HistoryTranscriptContext {
            session,
            target_event: Some(event),
            items: vec![fixture_history_item(
                "history_view",
                "event_view",
                7,
                "assistant",
                "clean assistant text",
            )],
            target_index: Some(0),
            omitted_target: false,
        };

        let output = history_transcript_output(&store, &context, None).expect("history output");
        let value = serde_json::to_value(output).expect("serialize");
        let ref_id = store
            .recent_ref_for_event_id("event_view")
            .expect("ref lookup")
            .expect("ref exists");

        assert_eq!(value["target_ref"], ref_id);
        assert_eq!(value["target_index"], 0);
        assert!(value.get("events").is_none());
        assert_eq!(value["items"][0]["history_item_id"], "history_view");
        assert_eq!(value["items"][0]["kind"], "assistant");
        assert_eq!(value["items"][0]["text"], "clean assistant text");
    }

    #[test]
    fn clean_show_json_keeps_before_target_after_history_shape() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let session = store
            .session_by_id("session_view")
            .expect("session lookup")
            .expect("session exists");
        let event = store
            .event_by_id("event_view")
            .expect("event lookup")
            .expect("event exists");
        let context = crate::storage::HistoryTranscriptContext {
            session,
            target_event: Some(event),
            items: vec![
                fixture_history_item("history_before", "event_before", 6, "user", "before text"),
                fixture_history_item("history_view", "event_view", 7, "assistant", "target text"),
                fixture_history_item("history_after", "event_after", 8, "assistant", "after text"),
            ],
            target_index: Some(1),
            omitted_target: false,
        };

        let output = history_show_output(&store, &context).expect("history show output");
        let value = serde_json::to_value(output).expect("serialize");
        let ref_id = store
            .recent_ref_for_event_id("event_view")
            .expect("ref lookup")
            .expect("ref exists");

        assert_eq!(value["target_ref"], ref_id);
        assert_eq!(value["before"][0]["text"], "before text");
        assert_eq!(value["target"]["text"], "target text");
        assert_eq!(value["after"][0]["text"], "after text");
        assert!(value.get("events").is_none());
    }

    #[test]
    fn full_transcript_json_preserves_raw_events() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let session = store
            .session_by_id("session_view")
            .expect("session lookup")
            .expect("session exists");
        let events = store
            .events_for_session("session_view")
            .expect("events lookup");

        let output = transcript_output(&store, &session, &events, Some("event_view"), None)
            .expect("raw output");
        let value = serde_json::to_value(output).expect("serialize");
        let ref_id = store
            .recent_ref_for_event_id("event_view")
            .expect("ref lookup")
            .expect("ref exists");

        assert_eq!(value["target_ref"], ref_id);
        assert_eq!(value["events"][0]["content"], "viewer test content");
        assert!(value.get("items").is_none());
    }

    #[test]
    fn transcript_grep_context_flags_require_grep() {
        assert!(resolve_transcript_grep(None, Some(1), None, None).is_err());
        assert!(resolve_transcript_grep(Some("   ".to_string()), None, None, None).is_err());

        let grep = resolve_transcript_grep(Some(" Basis ".to_string()), None, Some(3), Some(2))
            .expect("resolve grep")
            .expect("grep");

        assert_eq!(grep.pattern, "Basis");
        assert_eq!(grep.before_context, 2);
        assert_eq!(grep.after_context, 3);
    }

    #[test]
    fn transcript_grep_windows_collapse_overlapping_context() {
        let selected = grep_window_indices(6, |idx| idx == 1 || idx == 3, 1, 1);

        assert_eq!(selected, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn transcript_grep_filters_clean_context_and_preserves_target_index() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let session = store
            .session_by_id("session_view")
            .expect("session lookup")
            .expect("session exists");
        let event = store
            .event_by_id("event_view")
            .expect("event lookup")
            .expect("event exists");
        let context = crate::storage::HistoryTranscriptContext {
            session,
            target_event: Some(event),
            items: vec![
                fixture_history_item("history_before", "event_before", 6, "user", "before text"),
                fixture_history_item(
                    "history_view",
                    "event_view",
                    7,
                    "assistant",
                    "Basis article text",
                ),
                fixture_history_item("history_after", "event_after", 8, "assistant", "after text"),
            ],
            target_index: Some(1),
            omitted_target: false,
        };
        let grep = TranscriptGrep {
            pattern: "basis".to_string(),
            before_context: 1,
            after_context: 1,
        };

        let filtered = grep_history_context(context, &grep);
        let output = history_transcript_output(&store, &filtered, Some(&grep))
            .expect("history transcript output");
        let value = serde_json::to_value(output).expect("serialize");

        assert_eq!(value["target_index"], 1);
        assert_eq!(value["omitted_target"], false);
        assert_eq!(value["grep"]["pattern"], "basis");
        assert_eq!(value["grep"]["before_context"], 1);
        assert_eq!(value["grep"]["after_context"], 1);
        assert_eq!(value["grep"]["match_count"], 1);
        assert_eq!(value["items"][0]["history_item_id"], "history_before");
        assert_eq!(value["items"][1]["history_item_id"], "history_view");
        assert_eq!(value["items"][2]["history_item_id"], "history_after");
    }

    #[test]
    fn transcript_grep_marks_target_omitted_when_slice_excludes_it() {
        let (_dir, store) = fixture_store_with_viewer_ref();
        let session = store
            .session_by_id("session_view")
            .expect("session lookup")
            .expect("session exists");
        let event = store
            .event_by_id("event_view")
            .expect("event lookup")
            .expect("event exists");
        let context = crate::storage::HistoryTranscriptContext {
            session,
            target_event: Some(event),
            items: vec![
                fixture_history_item("history_view", "event_view", 7, "assistant", "target text"),
                fixture_history_item(
                    "history_after",
                    "event_after",
                    8,
                    "assistant",
                    "Basis article text",
                ),
            ],
            target_index: Some(0),
            omitted_target: false,
        };
        let grep = TranscriptGrep {
            pattern: "basis".to_string(),
            before_context: 0,
            after_context: 0,
        };

        let filtered = grep_history_context(context, &grep);

        assert_eq!(filtered.target_index, None);
        assert!(filtered.omitted_target);
        assert_eq!(filtered.items.len(), 1);
        assert_eq!(filtered.items[0].id, "history_after");
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

    fn fixture_history_item(
        id: &str,
        event_id: &str,
        ordinal: i64,
        kind: &str,
        text: &str,
    ) -> crate::storage::HistoryItemRecord {
        crate::storage::HistoryItemRecord {
            id: id.to_string(),
            event_id: event_id.to_string(),
            session_id: "session_view".to_string(),
            source_id: "source_view".to_string(),
            machine_id: "machine_view".to_string(),
            source_kind: "codex".to_string(),
            ordinal,
            subordinal: 0,
            tier: "conversation".to_string(),
            kind: kind.to_string(),
            text: text.to_string(),
            text_hash: stable_hash(&(id, text)).expect("text hash"),
            occurred_at: None,
            lexical_indexable: true,
            semantic_policy: "required".to_string(),
            metadata: json!({}),
            hash: stable_hash(&(id, event_id, text)).expect("history item hash"),
        }
    }
}
