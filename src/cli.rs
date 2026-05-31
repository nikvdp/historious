use crate::config::AppConfig;
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
    Update,
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
    Daemon,
    /// Opt-in HTTP server for local/peer archive exchange.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7391")]
        bind: String,
    },
    /// Show local archive health and projection freshness.
    Status,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let config = AppConfig::load(self.data_dir)?;
        match self.command {
            Command::Update => {
                println!("data_dir={}", config.data_dir.display());
                println!("update: not implemented yet");
            }
            Command::Search { query, limit, json } => {
                let _ = (query, limit, json);
                println!("search: not implemented yet");
            }
            Command::Export { jsonl } => {
                let _ = jsonl;
                println!("export: not implemented yet");
            }
            Command::Import { jsonl, input } => {
                let _ = (jsonl, input);
                println!("import: not implemented yet");
            }
            Command::Daemon => {
                println!("daemon: not implemented yet");
            }
            Command::Serve { bind } => {
                println!("serve: not implemented yet on {bind}");
            }
            Command::Status => {
                println!("data_dir={}", config.data_dir.display());
                println!("status: not implemented yet");
            }
        }
        Ok(())
    }
}

