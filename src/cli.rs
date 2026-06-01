use crate::config::AppConfig;
use crate::ingest;
use crate::search;
use crate::server;
use crate::storage::Store;
use crate::transport;
use anyhow::Result;
use clap::{Parser, Subcommand};

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
    },
    /// Export canonical archive records as JSONL.
    Export {
        #[arg(long)]
        jsonl: bool,
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
            Command::Search { query, limit, json } => {
                let (embedder, degraded_reason) = load_embedder(&config);
                let response =
                    search::search(&store, &query, limit, embedder.as_deref(), degraded_reason)?;
                if json {
                    serde_json::to_writer_pretty(std::io::stdout(), &response)?;
                    println!();
                } else {
                    if let Some(reason) = &response.degraded_reason {
                        eprintln!("search degraded: {reason}");
                    }
                    for result in response.results {
                        println!(
                            "{:.6}\t{}\t{}\t{}\t{}",
                            result.score,
                            result.source_kind,
                            result.session_id,
                            result.event_id,
                            result.snippet
                        );
                    }
                }
            }
            Command::Export { jsonl } => {
                if jsonl {
                    let stdout = std::io::stdout();
                    transport::export_jsonl(&store, stdout.lock())?;
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
