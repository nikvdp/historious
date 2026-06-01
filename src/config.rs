use crate::embed::EmbedderConfig;
use anyhow::{Context, Result};
use directories::ProjectDirs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub machine_id: String,
    pub embedder: EmbedderConfig,
}

impl AppConfig {
    pub fn load(data_dir: Option<PathBuf>) -> Result<Self> {
        let data_dir = match data_dir {
            Some(path) => expand_home(path),
            None => default_data_dir()?,
        };
        std::fs::create_dir_all(&data_dir)
            .with_context(|| format!("creating data dir {}", data_dir.display()))?;
        let machine_id = load_machine_id(&data_dir)?;
        let embedder = EmbedderConfig::from_env(&data_dir);
        Ok(Self {
            data_dir,
            machine_id,
            embedder,
        })
    }
}

fn default_data_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("com", "example", "super-cass")
        .context("could not resolve platform data directory")?;
    Ok(dirs.data_dir().to_path_buf())
}

fn expand_home(path: PathBuf) -> PathBuf {
    let Some(text) = path.to_str() else {
        return path;
    };
    if text == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
    }
    if let Some(rest) = text.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    path
}

fn load_machine_id(data_dir: &PathBuf) -> Result<String> {
    let path = data_dir.join("machine-id");
    if path.exists() {
        return std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))
            .map(|text| text.trim().to_string());
    }
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string());
    let id = format!("machine_{}_{}", sanitize(&host), uuid::Uuid::new_v4().simple());
    std::fs::write(&path, format!("{id}\n"))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(id)
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '_' })
        .collect()
}
