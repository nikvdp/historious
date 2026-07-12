use crate::embed::EmbedderConfig;
use crate::search::SearchMode;
use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub machine_name: String,
    pub machine_id: String,
    pub embedder: EmbedderConfig,
    pub default_search_mode: SearchMode,
    pub sources: SourceConfigs,
    pub enrichment: EnrichmentConfig,
}

impl AppConfig {
    pub fn load(data_dir: Option<PathBuf>) -> Result<Self> {
        let data_dir = resolve_data_dir(data_dir)?;
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("creating data dir {}", data_dir.display()))?;
        let file_config = load_file_config(&data_dir)?;
        let machine_name = ensure_machine_name(&data_dir, file_config.machine.name)?;
        let machine_id = machine_id_for_name(&machine_name);
        let embedder =
            EmbedderConfig::from_config_and_env(&data_dir, file_config.embeddings.enabled);
        Ok(Self {
            data_dir,
            machine_name,
            machine_id,
            embedder,
            default_search_mode: file_config.search.default_mode,
            sources: file_config.sources,
            enrichment: file_config.enrichment,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FileConfig {
    #[serde(default)]
    machine: MachineConfig,
    #[serde(default)]
    search: SearchConfig,
    #[serde(default)]
    embeddings: EmbeddingsConfig,
    #[serde(default)]
    sources: SourceConfigs,
    #[serde(default)]
    enrichment: EnrichmentConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnrichmentProvider {
    Openai,
    Anthropic,
}

impl EnrichmentProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Anthropic => "anthropic",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct EnrichmentConfig {
    pub provider: Option<EnrichmentProvider>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub model: Option<String>,
}

impl EnrichmentConfig {
    pub fn configured(&self) -> bool {
        self.provider.is_some()
            && self.base_url.as_ref().is_some_and(|value| !value.trim().is_empty())
            && self.api_key.as_ref().is_some_and(|value| !value.trim().is_empty())
            && self.model.as_ref().is_some_and(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MachineConfig {
    name: Option<String>,
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

#[derive(Debug, Clone, Deserialize)]
struct EmbeddingsConfig {
    #[serde(default)]
    enabled: bool,
}

impl Default for EmbeddingsConfig {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SourceConfigs {
    #[serde(default)]
    pub treechat: TreechatSourceConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TreechatSourceConfig {
    #[serde(default)]
    pub enabled: bool,
    pub profile: Option<String>,
    pub backend_url: Option<String>,
    pub app_host: Option<String>,
    pub access_token: Option<String>,
    pub client: Option<String>,
    pub uid: Option<String>,
    pub page_limit: Option<usize>,
    pub thread_limit: Option<usize>,
    pub content_scope: Option<String>,
}

pub fn resolve_data_dir(data_dir: Option<PathBuf>) -> Result<PathBuf> {
    match data_dir {
        Some(path) => Ok(expand_home(path)),
        None => default_data_dir_with_legacy_migration(),
    }
}

pub fn config_path(data_dir: &Path) -> PathBuf {
    data_dir.join("config.toml")
}

pub fn set_machine_name(data_dir: &Path, name: &str) -> Result<PathBuf> {
    let name = normalize_machine_name(name)?;
    set_config_string(data_dir, &["machine", "name"], &name)
}

pub fn set_embeddings_enabled(data_dir: &Path, enabled: bool) -> Result<PathBuf> {
    set_config_bool(data_dir, &["embeddings", "enabled"], enabled)
}

pub fn set_treechat_enabled(data_dir: &Path, enabled: bool) -> Result<PathBuf> {
    set_config_bool(data_dir, &["sources", "treechat", "enabled"], enabled)
}

pub fn set_enrichment_config(
    data_dir: &Path,
    provider: EnrichmentProvider,
    base_url: &str,
    api_key: &str,
    model: &str,
) -> Result<PathBuf> {
    for (name, value) in [("base URL", base_url), ("API key", api_key), ("model", model)] {
        if value.trim().is_empty() {
            anyhow::bail!("enrichment {name} must not be empty");
        }
    }
    let values = [
        ("provider", provider.as_str()),
        ("base_url", base_url.trim()),
        ("api_key", api_key.trim()),
        ("model", model.trim()),
    ];
    let mut path = config_path(data_dir);
    for (key, value) in values {
        path = set_config_string(data_dir, &["enrichment", key], value)?;
    }
    restrict_config_permissions(&path)?;
    Ok(path)
}

#[cfg(unix)]
fn restrict_config_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_config_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn set_config_bool(data_dir: &Path, path_parts: &[&str], enabled: bool) -> Result<PathBuf> {
    set_config_value(data_dir, path_parts, toml::Value::Boolean(enabled))
}

fn set_config_string(data_dir: &Path, path_parts: &[&str], text: &str) -> Result<PathBuf> {
    set_config_value(data_dir, path_parts, toml::Value::String(text.to_string()))
}

fn set_config_value(
    data_dir: &Path,
    path_parts: &[&str],
    replacement: toml::Value,
) -> Result<PathBuf> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;
    let path = config_path(data_dir);
    let mut value = if path.exists() {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading config {}", path.display()))?;
        text.parse::<toml::Value>()
            .with_context(|| format!("parsing config {}", path.display()))?
    } else {
        toml::Value::Table(toml::map::Map::new())
    };
    let root = value
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("config root must be a TOML table"))?;
    let Some((last, parents)) = path_parts.split_last() else {
        anyhow::bail!("config path must not be empty");
    };
    let mut cursor = root;
    for part in parents {
        let value = cursor
            .entry((*part).to_string())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
        if !value.is_table() {
            *value = toml::Value::Table(toml::map::Map::new());
        }
        cursor = value.as_table_mut().expect("config table");
    }
    cursor.insert((*last).to_string(), replacement);
    fs::write(
        &path,
        toml::to_string_pretty(&value).context("serializing config")?,
    )
    .with_context(|| format!("writing config {}", path.display()))?;
    Ok(path)
}

fn load_file_config(data_dir: &Path) -> Result<FileConfig> {
    let path = config_path(data_dir);
    if !path.exists() {
        return Ok(FileConfig {
            machine: MachineConfig::default(),
            search: SearchConfig::default(),
            embeddings: EmbeddingsConfig::default(),
            sources: SourceConfigs::default(),
            enrichment: EnrichmentConfig::default(),
        });
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
}

fn default_data_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("com", "historious", "historious")
        .context("could not resolve platform data directory")?;
    Ok(dirs.data_dir().to_path_buf())
}

fn legacy_data_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("com", "historious", "super-cass")
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
    fn config_file_can_persist_disabled_embeddings() {
        let dir = tempfile::tempdir().expect("tempdir");

        let path = set_embeddings_enabled(dir.path(), false).expect("write embeddings config");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert_eq!(path, dir.path().join("config.toml"));
        assert!(config.embedder.is_disabled());
    }

    #[test]
    fn config_file_can_persist_treechat_enabled() {
        let dir = tempfile::tempdir().expect("tempdir");

        let path = set_treechat_enabled(dir.path(), true).expect("write treechat config");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert_eq!(path, dir.path().join("config.toml"));
        assert!(config.sources.treechat.enabled);
    }

    #[test]
    fn enrichment_config_is_explicit_complete_and_private() {
        let dir = tempfile::tempdir().expect("tempdir");

        let path = set_enrichment_config(
            dir.path(),
            EnrichmentProvider::Anthropic,
            "https://api.anthropic.com",
            "secret-test-key",
            "claude-test",
        )
        .expect("write enrichment config");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert!(config.enrichment.configured());
        assert_eq!(config.enrichment.provider, Some(EnrichmentProvider::Anthropic));
        assert_eq!(config.enrichment.base_url.as_deref(), Some("https://api.anthropic.com"));
        assert_eq!(config.enrichment.api_key.as_deref(), Some("secret-test-key"));
        assert_eq!(config.enrichment.model.as_deref(), Some("claude-test"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(path).expect("metadata").permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn enrichment_config_never_uses_environment_fallbacks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert!(!config.enrichment.configured());
        assert!(config.enrichment.provider.is_none());
        assert!(config.enrichment.api_key.is_none());
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

    #[test]
    fn missing_machine_name_is_initialized_in_config() {
        let dir = tempfile::tempdir().expect("tempdir");

        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");
        let text = fs::read_to_string(dir.path().join("config.toml")).expect("config text");

        assert!(!config.machine_name.trim().is_empty());
        assert_eq!(config.machine_id, machine_id_for_name(&config.machine_name));
        assert!(text.contains("[machine]"));
        assert!(text.contains("name = "));
    }

    #[test]
    fn config_file_can_set_machine_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("config.toml"),
            "[machine]\nname = \"Dev Box\"\n",
        )
        .expect("write config");

        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert_eq!(config.machine_name, "Dev Box");
        assert_eq!(config.machine_id, "machine_dev_box");
    }

    #[test]
    fn set_machine_name_updates_the_configured_source_of_truth() {
        let dir = tempfile::tempdir().expect("tempdir");

        set_machine_name(dir.path(), "Laptop 2").expect("set machine name");
        let config = AppConfig::load(Some(dir.path().to_path_buf())).expect("config");

        assert_eq!(config.machine_name, "Laptop 2");
        assert_eq!(config.machine_id, "machine_laptop_2");
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

fn ensure_machine_name(data_dir: &Path, configured: Option<String>) -> Result<String> {
    if let Some(name) = configured {
        return normalize_machine_name(&name);
    }
    let name = default_machine_name();
    set_machine_name(data_dir, &name)?;
    Ok(name)
}

fn default_machine_name() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

fn normalize_machine_name(input: &str) -> Result<String> {
    let name = input.trim();
    if name.is_empty() {
        anyhow::bail!("machine name must not be empty");
    }
    Ok(name.to_string())
}

pub fn machine_id_for_name(name: &str) -> String {
    format!("machine_{}", sanitize_machine_name(name))
}

pub fn machine_id_prefix_for_name(name: &str) -> String {
    machine_id_for_name(name)
}

fn sanitize_machine_name(input: &str) -> String {
    input
        .trim()
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
