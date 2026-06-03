use crate::config::AppConfig;
use crate::ingest;
use crate::search;
use crate::server;
use crate::storage::{RecentResultRefInput, Store};
use crate::transport;
use anyhow::{bail, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::io::{self, IsTerminal, Write};
use std::process::{Command as ProcessCommand, Stdio};

#[derive(Debug, Parser)]
#[command(name = "super-cass")]
#[command(about = "Local-first coding-agent transcript archive and search")]
pub struct Cli {
    #[arg(long, env = "SUPER_CASS_DATA_DIR")]
    pub data_dir: Option<std::path::PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Scan configured local sources once and refresh local projections.
    Update {
        #[arg(long)]
        max_files: Option<usize>,
        #[arg(long)]
        source: Option<String>,
    },
    /// Search indexed transcripts.
    Search {
        query: String,
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        verbose: bool,
        #[arg(long)]
        cols: Option<String>,
        #[arg(long)]
        include: Option<String>,
        #[arg(long)]
        exclude: Option<String>,
        #[arg(long, value_enum, default_value_t = SearchSort::Relevance)]
        sort: SearchSort,
        #[arg(long, default_value_t = 0.0)]
        recency_bias: f64,
        #[arg(long)]
        no_color: bool,
        #[arg(long)]
        fzf: bool,
    },
    /// Print surrounding transcript context for an event or search unit.
    Expand {
        target: Option<String>,
        #[arg(long, conflicts_with = "search_unit")]
        event: Option<String>,
        #[arg(long = "search-unit", conflicts_with = "event")]
        search_unit: Option<String>,
        #[arg(long, default_value_t = 3)]
        before: usize,
        #[arg(long, default_value_t = 5)]
        after: usize,
        #[arg(long)]
        no_color: bool,
    },
    /// Render a full session transcript.
    Show {
        session: String,
        #[arg(long, conflicts_with = "search_unit")]
        event: Option<String>,
        #[arg(long = "search-unit", conflicts_with = "event")]
        search_unit: Option<String>,
        #[arg(long)]
        no_pager: bool,
        #[arg(long)]
        no_color: bool,
    },
    /// Export canonical archive records as JSONL.
    Export {
        #[arg(long)]
        jsonl: bool,
        #[arg(long)]
        source: Vec<String>,
        #[arg(long)]
        workspace: Vec<std::path::PathBuf>,
        #[arg(long)]
        session: Vec<String>,
        #[arg(long)]
        since: Option<String>,
    },
    /// Import canonical archive records from JSONL.
    Import {
        #[arg(long)]
        jsonl: bool,
        #[arg(default_value = "-")]
        input: String,
    },
    /// Run the local updater continuously. No network listener is started.
    Daemon {
        #[arg(long, default_value_t = 30)]
        interval_secs: u64,
        #[arg(long)]
        max_files: Option<usize>,
        #[arg(long)]
        source: Option<String>,
    },
    /// Opt-in HTTP server for local/peer archive exchange.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7391")]
        bind: String,
        #[arg(long, default_value_t = 30)]
        interval_secs: u64,
        #[arg(long)]
        max_files: Option<usize>,
        #[arg(long)]
        source: Option<String>,
    },
    /// Show local archive health and projection freshness.
    Status,
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
    pub async fn run(self) -> Result<()> {
        let config = AppConfig::load(self.data_dir)?;
        let store = Store::open(&config.data_dir)?;
        match self.command {
            Command::Update { max_files, source } => {
                let stats = ingest::update_local(
                    &store,
                    &config.machine_id,
                    ingest::UpdateOptions { max_files, source },
                )?;
                println!(
                    "files_seen={} skipped_unchanged={} inserted={} duplicates={} errors={}",
                    stats.files_seen,
                    stats.skipped_unchanged,
                    stats.inserted,
                    stats.duplicates,
                    stats.errors
                );
                let projected = search::refresh(&store)?;
                println!("projection=search_rrf_v1 projected_events={projected}");
                let (embedder, degraded_reason) = load_embedder(&config);
                let embeddings = search::refresh_embeddings(
                    &store,
                    &config.machine_id,
                    embedder.as_deref(),
                    degraded_reason,
                )?;
                println!(
                    "projection=semantic_embeddings embedded={} inserted_vectors={} degraded_reason={}",
                    embeddings.embedded,
                    embeddings.vectors_projected,
                    embeddings.degraded_reason.unwrap_or_else(|| "none".to_string())
                );
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
                let (embedder, degraded_reason) = load_embedder(&config);
                let response = search::search(
                    &store,
                    &query,
                    search::SearchOptions::new(limit, sort.into(), recency_bias),
                    embedder.as_deref(),
                    degraded_reason,
                )?;
                if json {
                    serde_json::to_writer_pretty(std::io::stdout(), &response)?;
                    println!();
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
                    let color = !no_color && std::io::stdout().is_terminal();
                    print_search_results(&query, &response.results, &refs, &columns, color);
                }
            }
            Command::Expand {
                target,
                event,
                search_unit,
                before,
                after,
                no_color,
            } => {
                let event_id = resolve_expand_event_id(&store, target, event, search_unit)?;
                let context = store
                    .events_around_event(&event_id, before, after)?
                    .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                let color = !no_color && std::io::stdout().is_terminal();
                write_stdout(&crate::transcript::render_context(&context, color))?;
            }
            Command::Show {
                session,
                event,
                search_unit,
                no_pager,
                no_color,
            } => {
                let target_event_id = resolve_optional_event_id(&store, event, search_unit)?;
                if let Some(event_id) = &target_event_id {
                    let event = store
                        .event_by_id(event_id)?
                        .ok_or_else(|| anyhow::anyhow!("event not found: {event_id}"))?;
                    if event.session_id != session {
                        bail!(
                            "event {event_id} belongs to session {}, not {session}",
                            event.session_id
                        );
                    }
                }
                let session_record = store
                    .session_by_id(&session)?
                    .ok_or_else(|| anyhow::anyhow!("session not found: {session}"))?;
                let events = store.events_for_session(&session)?;
                let color = !no_color && std::io::stdout().is_terminal();
                let rendered = crate::transcript::render_session(
                    &session_record,
                    &events,
                    target_event_id.as_deref(),
                    color,
                );
                page_or_print(&rendered, target_event_id.as_deref(), no_pager)?;
            }
            Command::Export {
                jsonl,
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
                    transport::export_jsonl_filtered(&store, &filter, stdout.lock())?;
                } else {
                    anyhow::bail!("only --jsonl export is supported in v0");
                }
            }
            Command::Import { jsonl, input } => {
                if jsonl {
                    let stats = transport::import_jsonl_path(&store, &input)?;
                    println!(
                        "imported={} duplicates={} inserted_vectors={}",
                        stats.inserted, stats.duplicates, stats.vectors_projected
                    );
                    let projected = search::refresh(&store)?;
                    println!("projection=search_rrf_v1 projected_events={projected}");
                    let (embedder, degraded_reason) = load_embedder(&config);
                    let embeddings = search::refresh_embeddings(
                        &store,
                        &config.machine_id,
                        embedder.as_deref(),
                        degraded_reason,
                    )?;
                    println!(
                        "projection=semantic_embeddings embedded={} inserted_vectors={} degraded_reason={}",
                        embeddings.embedded,
                        embeddings.vectors_projected,
                        embeddings.degraded_reason.unwrap_or_else(|| "none".to_string())
                    );
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
            Command::Status => {
                let stats = store.stats()?;
                let embedder = config.embedder.status_without_loading();
                println!("data_dir={}", config.data_dir.display());
                println!("db_path={}", store.db_path().display());
                println!("sources={}", stats.sources);
                println!("raw_artifacts={}", stats.raw_artifacts);
                println!("sessions={}", stats.sessions);
                println!("events={}", stats.events);
                println!("search_units={}", stats.search_units);
                println!("embeddings={}", stats.embeddings);
                println!(
                    "query_embedder={} semantic={} available={} degraded_reason={}",
                    embedder.provider,
                    embedder.semantic,
                    embedder.available,
                    embedder
                        .degraded_reason
                        .unwrap_or_else(|| "none".to_string())
                );
                if std::env::var("SUPER_CASS_PROBE_EMBEDDER").as_deref() == Ok("1") {
                    match config.embedder.load() {
                        Ok(loaded) => match loaded.embed_one("super cass query embedder probe") {
                            Ok(vector) => println!(
                                "query_embedder_probe=ready model_id={} dims={} semantic={} sample_dims={}",
                                loaded.model_id(),
                                loaded.dims(),
                                loaded.is_semantic(),
                                vector.len()
                            ),
                            Err(err) => println!("query_embedder_probe=degraded reason={err:#}"),
                        },
                        Err(err) => println!("query_embedder_probe=degraded reason={err:#}"),
                    }
                }
            }
        }
        Ok(())
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

fn resolve_expand_event_id(
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
        (None, None, None) => bail!("expand requires a ref, --event <id>, or --search-unit <id>"),
        _ => bail!("expand accepts one target: positional ref, --event, or --search-unit"),
    }
}

fn resolve_optional_event_id(
    store: &Store,
    event: Option<String>,
    search_unit: Option<String>,
) -> Result<Option<String>> {
    match (event, search_unit) {
        (Some(event_id), None) => Ok(Some(resolve_event_ref_or_id(store, &event_id)?)),
        (None, Some(unit_id)) => store
            .search_unit_by_id(&unit_id)?
            .map(|unit| Some(unit.event_id))
            .ok_or_else(|| anyhow::anyhow!("search unit not found: {unit_id}")),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => bail!("show accepts either --event or --search-unit, not both"),
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
            "fzf is not installed. Use `super-cass search {}` and then `super-cass expand --event <id>` or `super-cass show <session-id> --event <event-id>`.",
            shell_quote(query)
        );
    }
    let current_exe = std::env::current_exe()?;
    let exe = shell_quote(&current_exe.to_string_lossy());
    let data_dir = shell_quote(&config.data_dir.to_string_lossy());
    let preview = format!("{exe} --data-dir {data_dir} expand --event {{7}} --before 3 --after 5");
    let open = format!("{exe} --data-dir {data_dir} show {{6}} --event {{7}}");
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
        if let Some(event_id) = target_event_id {
            command.arg(format!("+/event:{event_id}"));
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
    loop {
        let stats = ingest::update_local(
            store,
            machine_id,
            ingest::UpdateOptions {
                max_files,
                source: source.clone(),
            },
        )?;
        let projected = search::refresh(store)?;
        let (embedder, degraded_reason) = load_embedder_config(&embedder_config);
        let embeddings =
            search::refresh_embeddings(store, machine_id, embedder.as_deref(), degraded_reason)?;
        println!(
            "files_seen={} skipped_unchanged={} inserted={} duplicates={} errors={} projected_events={} embedded={} inserted_vectors={} degraded_reason={}",
            stats.files_seen,
            stats.skipped_unchanged,
            stats.inserted,
            stats.duplicates,
            stats.errors,
            projected,
            embeddings.embedded,
            embeddings.vectors_projected,
            embeddings.degraded_reason.unwrap_or_else(|| "none".to_string())
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
    fn shell_quote_handles_spaces_and_single_quotes() {
        assert_eq!(shell_quote("/tmp/a path/it's"), "'/tmp/a path/it'\\''s'");
    }
}
