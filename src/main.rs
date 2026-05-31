mod archive;
mod cli;
mod config;
mod ingest;
mod search;
mod server;
mod storage;
mod transport;

use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "super_cass=info".into()),
        )
        .with_target(false)
        .init();

    cli::Cli::parse().run().await
}
