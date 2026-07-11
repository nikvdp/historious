mod analytics;
mod archive;
mod cli;
mod config;
mod embed;
mod ingest;
mod memory;
mod output;
mod provenance;
mod report;
mod search;
mod self_update;
mod server;
mod skills;
mod source;
mod storage;
mod transcript;
mod transport;
mod treechat;

use anyhow::Result;
use clap::Parser;
use std::time::Instant;

#[tokio::main]
async fn main() {
    if let Err(err) = run_main().await {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}

async fn run_main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "historious=info".into()),
        )
        .with_target(false)
        .init();

    let started_at = Instant::now();
    let cli = cli::Cli::parse();
    let command = cli.command_name();
    let structured_errors = cli.wants_structured_errors();
    if let Err(err) = cli.run().await {
        if structured_errors {
            output::write_error(command, &err, None, None, Some(started_at))?;
            std::process::exit(1);
        }
        return Err(err);
    }
    Ok(())
}
