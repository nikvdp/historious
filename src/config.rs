use crate::embed::EmbedderConfig;
use crate::search::SearchMode;
use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub machine_id: String,
    pub embedder: EmbedderConfig,
    pub default_search_mode: SearchMode,
}

impl AppConfig {
    pub fn load(data_dir: Option<PathBuf>) -> Result<Self> {
        let data_dir = match data_dir {
            Some(path) => expand_home(path),
            None => default_data_dir_with_legacy_migration()?,
        };
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("creating data dir {}", data_dir.display()))?;
        let file_config = load_file_config(&data_dir)?;
        let machine_id = load_machine_id(&data_dir)?;
        let embedder = EmbedderConfig::from_env(&data_dir);
        Ok(Self {
            data_dir,
            machine_id,
            embedder,
            default_search_mode: file_config.search.default_mode,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FileConfig {
    #[serde(default)]
    search: SearchConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct SearchConfig {
    #[serde(default)]
    default_mode: SearchMode,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            default_mode: SearchMode::Hybrid,
        }
    }
}

fn load_file_config(data_dir: &PathBuf) -> Result<FileConfig> {
    let path = data_dir.join("config.toml");
    if !path.exists() {
        return Ok(FileConfig {
            search: SearchConfig::default(),
        });
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
}

fn default_data_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("com", "example", "historious")
        .context("could not resolve platform data directory")?;
    Ok(dirs.data_dir().to_path_buf())
}

fn legacy_data_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("com", "example", "super-cass")
        .context("could not resolve legacy platform data directory")?;
    Ok(dirs.data_dir().to_path_buf())
}

fn default_data_dir_with_legacy_migration() -> Result<PathBuf> {
    let data_dir = default_data_dir()?;
    migrate_legacy_data_dir(&data_dir, &legacy_data_dir()?)?;
    Ok(data_dir)
}

fn migrate_legacy_data_dir(data_dir: &PathBuf, legacy_dir: &PathBuf) -> Result<()> {
    if !legacy_dir.exists() {
        return Ok(());
    }
    if data_dir.exists() && !is_empty_dir(data_dir)? {
        return Ok(());
    }
    if data_dir.exists() {
        fs::remove_dir(data_dir)
            .with_context(|| format!("removing empty data dir {}", data_dir.display()))?;
    }
    fs::rename(legacy_dir, data_dir).with_context(|| {
        format!(
            "migrating legacy data dir {} to {}",
            legacy_dir.display(),
            data_dir.display()
        )
    })?;
    rename_legacy_db_files(data_dir)
}

fn is_empty_dir(path: &PathBuf) -> Result<bool> {
    let mut entries =
        fs::read_dir(path).with_context(|| format!("reading data dir {}", path.display()))?;
    Ok(entries.next().is_none())
}

fn rename_legacy_db_files(data_dir: &PathBuf) -> Result<()> {
    for suffix in ["", "-wal", "-shm"] {
        let legacy = data_dir.join(format!("super-cass.db{suffix}"));
        let current = data_dir.join(format!("historious.db{suffix}"));
        if legacy.exists() && !current.exists() {
            fs::rename(&legacy, &current).with_context(|| {
                format!(
                    "renaming legacy database file {} to {}",
                    legacy.display(),
                    current.display()
                )
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_config_defaults_search_mode_to_hybrid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert_eq!(config.default_search_mode, SearchMode::Hybrid);
    }

    #[test]
    fn config_file_can_set_default_search_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("config.toml"),
            "[search]\ndefault_mode = \"lexical\"\n",
        )
        .expect("write config");

        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert_eq!(config.default_search_mode, SearchMode::Lexical);
    }

    #[test]
    fn invalid_search_mode_in_config_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("config.toml"),
            "[search]\ndefault_mode = \"spicy\"\n",
        )
        .expect("write config");

        let err =
            AppConfig::load(Some(dir.path().to_path_buf())).expect_err("invalid mode should fail");

        assert!(err.to_string().contains("parsing config"));
    }

    #[test]
    fn legacy_data_dir_migrates_to_empty_default_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let legacy = root.path().join("com.historious.super-cass");
        let current = root.path().join("com.historious.historious");
        fs::create_dir_all(&legacy).expect("legacy dir");
        fs::write(legacy.join("machine-id"), "machine_old\n").expect("machine id");
        fs::write(legacy.join("super-cass.db"), "db").expect("legacy db");
        fs::write(legacy.join("super-cass.db-wal"), "wal").expect("legacy wal");
        fs::write(legacy.join("super-cass.db-shm"), "shm").expect("legacy shm");

        migrate_legacy_data_dir(&current, &legacy).expect("migrate");

        assert!(!legacy.exists());
        assert_eq!(
            fs::read_to_string(current.join("machine-id")).expect("machine id"),
            "machine_old\n"
        );
        assert_eq!(
            fs::read_to_string(current.join("historious.db")).expect("current db"),
            "db"
        );
        assert_eq!(
            fs::read_to_string(current.join("historious.db-wal")).expect("current wal"),
            "wal"
        );
        assert_eq!(
            fs::read_to_string(current.join("historious.db-shm")).expect("current shm"),
            "shm"
        );
    }

    #[test]
    fn legacy_data_dir_does_not_overwrite_non_empty_current_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let legacy = root.path().join("com.historious.super-cass");
        let current = root.path().join("com.historious.historious");
        fs::create_dir_all(&legacy).expect("legacy dir");
        fs::create_dir_all(&current).expect("current dir");
        fs::write(legacy.join("super-cass.db"), "old").expect("legacy db");
        fs::write(current.join("historious.db"), "new").expect("current db");

        migrate_legacy_data_dir(&current, &legacy).expect("skip migrate");

        assert!(legacy.exists());
        assert_eq!(
            fs::read_to_string(current.join("historious.db")).expect("current db"),
            "new"
        );
    }
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
    let id = format!(
        "machine_{}_{}",
        sanitize(&host),
        uuid::Uuid::new_v4().simple()
    );
    std::fs::write(&path, format!("{id}\n"))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(id)
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}
