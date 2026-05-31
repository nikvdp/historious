use anyhow::{Context, Result};
use directories::ProjectDirs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
}

impl AppConfig {
    pub fn load(data_dir: Option<PathBuf>) -> Result<Self> {
        let data_dir = match data_dir {
            Some(path) => expand_home(path),
            None => default_data_dir()?,
        };
        std::fs::create_dir_all(&data_dir)
            .with_context(|| format!("creating data dir {}", data_dir.display()))?;
        Ok(Self { data_dir })
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

