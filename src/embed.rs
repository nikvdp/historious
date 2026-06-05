use anyhow::{Context, Result};
#[cfg(feature = "semantic-fastembed")]
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use serde::Serialize;
use std::path::{Path, PathBuf};
#[cfg(feature = "semantic-fastembed")]
use std::sync::Mutex;

pub const DEFAULT_SEMANTIC_MODEL_ID: &str = "fastembed:snowflake/snowflake-arctic-embed-xs-q";
pub const DEFAULT_SEMANTIC_DIMS: usize = 384;
const DEFAULT_FASTEMBED_MODEL: FastEmbedModel = FastEmbedModel::SnowflakeArcticEmbedXSQ;

pub trait Embedder: Send + Sync {
    fn model_id(&self) -> &str;
    fn dims(&self) -> usize;
    fn is_semantic(&self) -> bool;
    fn embed_batch(&self, texts: &[String], batch_size: usize) -> Result<Vec<Vec<f32>>>;

    fn embed_one(&self, text: &str) -> Result<Vec<f32>> {
        let embeddings = self.embed_batch(&[text.to_string()], 1)?;
        embeddings
            .into_iter()
            .next()
            .context("embedder returned no query embedding")
    }
}

#[derive(Debug, Clone)]
pub struct EmbedderConfig {
    pub provider: EmbedderProvider,
    pub semantic_model: FastEmbedModel,
    pub model_cache: PathBuf,
    pub intra_threads: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedderProvider {
    FastEmbed,
    HashFallback,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastEmbedModel {
    AllMiniLML12V2Q,
    BgeSmallEnV15Q,
    SnowflakeArcticEmbedSQ,
    SnowflakeArcticEmbedXSQ,
}

impl FastEmbedModel {
    fn from_name(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "bge-small-en-v1.5-q" | "bge-small-en-v15-q" | "bge-small-q" | "bge" => {
                Some(Self::BgeSmallEnV15Q)
            }
            "minilm-l12-q" | "all-minilm-l12-v2-q" | "all-minilm-l12-q" | "minilm" => {
                Some(Self::AllMiniLML12V2Q)
            }
            "snowflake-s-q" | "snowflake-arctic-embed-s-q" | "snowflake-small-q" => {
                Some(Self::SnowflakeArcticEmbedSQ)
            }
            "snowflake-xs-q" | "snowflake-arctic-embed-xs-q" | "snowflake" => {
                Some(Self::SnowflakeArcticEmbedXSQ)
            }
            _ => None,
        }
    }

    fn model_id(self) -> &'static str {
        match self {
            Self::AllMiniLML12V2Q => "fastembed:all-minilm-l12-v2-q",
            Self::BgeSmallEnV15Q => "fastembed:bge-small-en-v1.5-q",
            Self::SnowflakeArcticEmbedSQ => "fastembed:snowflake/snowflake-arctic-embed-s-q",
            Self::SnowflakeArcticEmbedXSQ => DEFAULT_SEMANTIC_MODEL_ID,
        }
    }

    #[cfg(feature = "semantic-fastembed")]
    fn embedding_model(self) -> EmbeddingModel {
        match self {
            Self::AllMiniLML12V2Q => EmbeddingModel::AllMiniLML12V2Q,
            Self::BgeSmallEnV15Q => EmbeddingModel::BGESmallENV15Q,
            Self::SnowflakeArcticEmbedSQ => EmbeddingModel::SnowflakeArcticEmbedSQ,
            Self::SnowflakeArcticEmbedXSQ => EmbeddingModel::SnowflakeArcticEmbedXSQ,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EmbedderStatus {
    pub provider: String,
    pub model_id: Option<String>,
    pub dims: Option<usize>,
    pub semantic: bool,
    pub available: bool,
    pub degraded_reason: Option<String>,
}

impl EmbedderConfig {
    pub fn from_env(data_dir: &Path) -> Self {
        let provider = match std::env::var("SUPER_CASS_EMBEDDER")
            .unwrap_or_else(|_| "fastembed".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "off" | "none" | "disabled" => EmbedderProvider::Disabled,
            "hash" | "hash-fallback" => EmbedderProvider::HashFallback,
            _ => EmbedderProvider::FastEmbed,
        };
        let model_cache = std::env::var_os("SUPER_CASS_MODEL_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("models").join("fastembed"));
        let semantic_model = std::env::var("SUPER_CASS_FASTEMBED_MODEL")
            .ok()
            .and_then(|value| FastEmbedModel::from_name(&value))
            .unwrap_or(DEFAULT_FASTEMBED_MODEL);
        let intra_threads = std::env::var("SUPER_CASS_EMBEDDER_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(default_intra_threads);
        Self {
            provider,
            semantic_model,
            model_cache,
            intra_threads,
        }
    }

    pub fn status_without_loading(&self) -> EmbedderStatus {
        match self.provider {
            EmbedderProvider::FastEmbed => EmbedderStatus {
                provider: "fastembed".to_string(),
                model_id: Some(self.semantic_model.model_id().to_string()),
                dims: Some(DEFAULT_SEMANTIC_DIMS),
                semantic: cfg!(feature = "semantic-fastembed"),
                available: cfg!(feature = "semantic-fastembed"),
                degraded_reason: fastembed_unavailable_reason(),
            },
            EmbedderProvider::HashFallback => EmbedderStatus {
                provider: "hash".to_string(),
                model_id: Some(HashEmbedder::MODEL_ID.to_string()),
                dims: Some(HashEmbedder::DIMS),
                semantic: false,
                available: true,
                degraded_reason: Some("hash fallback is lexical overlap, not semantic".to_string()),
            },
            EmbedderProvider::Disabled => EmbedderStatus {
                provider: "disabled".to_string(),
                model_id: None,
                dims: None,
                semantic: false,
                available: false,
                degraded_reason: Some("query embedder disabled".to_string()),
            },
        }
    }

    pub fn load(&self) -> Result<Box<dyn Embedder>> {
        match self.provider {
            EmbedderProvider::FastEmbed => {
                load_fastembed(&self.model_cache, self.intra_threads, self.semantic_model)
            }
            EmbedderProvider::HashFallback => Ok(Box::new(HashEmbedder)),
            EmbedderProvider::Disabled => anyhow::bail!("query embedder disabled"),
        }
    }
}

fn default_intra_threads() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, 2)
}

#[cfg(feature = "semantic-fastembed")]
fn fastembed_unavailable_reason() -> Option<String> {
    None
}

#[cfg(not(feature = "semantic-fastembed"))]
fn fastembed_unavailable_reason() -> Option<String> {
    Some(
        "fastembed support was not compiled; rebuild with the semantic-fastembed feature"
            .to_string(),
    )
}

#[cfg(feature = "semantic-fastembed")]
fn load_fastembed(
    model_cache: &Path,
    intra_threads: usize,
    semantic_model: FastEmbedModel,
) -> Result<Box<dyn Embedder>> {
    Ok(Box::new(FastEmbedder::new(
        model_cache,
        intra_threads,
        semantic_model,
    )?))
}

#[cfg(not(feature = "semantic-fastembed"))]
fn load_fastembed(
    _model_cache: &Path,
    _intra_threads: usize,
    _semantic_model: FastEmbedModel,
) -> Result<Box<dyn Embedder>> {
    anyhow::bail!("fastembed support was not compiled; rebuild with the semantic-fastembed feature")
}

#[cfg(feature = "semantic-fastembed")]
pub struct FastEmbedder {
    model: Mutex<TextEmbedding>,
    model_id: &'static str,
}

#[cfg(feature = "semantic-fastembed")]
impl FastEmbedder {
    pub fn new(
        model_cache: &Path,
        intra_threads: usize,
        semantic_model: FastEmbedModel,
    ) -> Result<Self> {
        std::fs::create_dir_all(model_cache)
            .with_context(|| format!("creating model cache {}", model_cache.display()))?;
        let options = InitOptions::new(semantic_model.embedding_model())
            .with_cache_dir(model_cache.to_path_buf())
            .with_intra_threads(intra_threads.max(1))
            .with_show_download_progress(false);
        let model = TextEmbedding::try_new(options).context("loading fastembed model")?;
        Ok(Self {
            model: Mutex::new(model),
            model_id: semantic_model.model_id(),
        })
    }
}

#[cfg(feature = "semantic-fastembed")]
impl Embedder for FastEmbedder {
    fn model_id(&self) -> &str {
        self.model_id
    }

    fn dims(&self) -> usize {
        DEFAULT_SEMANTIC_DIMS
    }

    fn is_semantic(&self) -> bool {
        true
    }

    fn embed_batch(&self, texts: &[String], batch_size: usize) -> Result<Vec<Vec<f32>>> {
        let mut model = self
            .model
            .lock()
            .map_err(|_| anyhow::anyhow!("fastembed model lock poisoned"))?;
        model
            .embed(texts, Some(batch_size.max(1)))
            .context("embedding text with fastembed")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HashEmbedder;

impl HashEmbedder {
    pub const MODEL_ID: &'static str = "hash-embed-v1";
    pub const DIMS: usize = 256;
}

impl Embedder for HashEmbedder {
    fn model_id(&self) -> &str {
        Self::MODEL_ID
    }

    fn dims(&self) -> usize {
        Self::DIMS
    }

    fn is_semantic(&self) -> bool {
        false
    }

    fn embed_batch(&self, texts: &[String], _batch_size: usize) -> Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|text| hash_embed(text)).collect())
    }
}

pub fn hash_embed(text: &str) -> Vec<f32> {
    let mut vector = vec![0.0f32; HashEmbedder::DIMS];
    for token in tokens(text) {
        let hash = blake3::hash(token.as_bytes());
        let bytes = hash.as_bytes();
        let idx = u16::from_le_bytes([bytes[0], bytes[1]]) as usize % HashEmbedder::DIMS;
        let sign = if bytes[2] & 1 == 0 { 1.0 } else { -1.0 };
        vector[idx] += sign;
    }
    normalize(&mut vector);
    vector
}

fn tokens(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
        .filter(|part| !part.is_empty())
        .map(|part| part.to_ascii_lowercase())
}

fn normalize(vector: &mut [f32]) {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in vector {
            *value /= norm;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_embedder_is_explicitly_not_semantic() {
        let embedder = HashEmbedder;
        assert_eq!(embedder.model_id(), "hash-embed-v1");
        assert_eq!(embedder.dims(), 256);
        assert!(!embedder.is_semantic());
    }

    #[test]
    fn disabled_provider_reports_degraded_status() {
        let config = EmbedderConfig {
            provider: EmbedderProvider::Disabled,
            semantic_model: DEFAULT_FASTEMBED_MODEL,
            model_cache: PathBuf::from("unused"),
            intra_threads: 1,
        };
        let status = config.status_without_loading();
        assert!(!status.available);
        assert!(!status.semantic);
        assert_eq!(
            status.degraded_reason.as_deref(),
            Some("query embedder disabled")
        );
    }

    #[test]
    fn default_fastembed_model_stays_fast_snowflake_xs_quantized() {
        let config = EmbedderConfig {
            provider: EmbedderProvider::FastEmbed,
            semantic_model: DEFAULT_FASTEMBED_MODEL,
            model_cache: PathBuf::from("unused"),
            intra_threads: 1,
        };
        let status = config.status_without_loading();

        assert_eq!(status.model_id.as_deref(), Some(DEFAULT_SEMANTIC_MODEL_ID));
        assert_eq!(status.dims, Some(DEFAULT_SEMANTIC_DIMS));
    }

    #[test]
    fn fastembed_model_names_parse_supported_384_dim_choices() {
        assert_eq!(
            FastEmbedModel::from_name("bge-small-en-v1.5-q"),
            Some(FastEmbedModel::BgeSmallEnV15Q)
        );
        assert_eq!(
            FastEmbedModel::from_name("snowflake-xs-q"),
            Some(FastEmbedModel::SnowflakeArcticEmbedXSQ)
        );
        assert_eq!(
            FastEmbedModel::from_name("snowflake-s-q"),
            Some(FastEmbedModel::SnowflakeArcticEmbedSQ)
        );
        assert_eq!(
            FastEmbedModel::from_name("minilm-l12-q"),
            Some(FastEmbedModel::AllMiniLML12V2Q)
        );
        assert_eq!(FastEmbedModel::from_name("large-model"), None);
    }

    #[test]
    fn hash_fallback_is_deterministic() {
        let embedder = HashEmbedder;
        let first = embedder.embed_one("offline convergence").expect("first");
        let second = embedder.embed_one("offline convergence").expect("second");
        assert_eq!(first, second);
    }
}
