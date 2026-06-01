use anyhow::{Context, Result};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const DEFAULT_SEMANTIC_MODEL_ID: &str = "fastembed:snowflake/snowflake-arctic-embed-xs-q";
pub const DEFAULT_SEMANTIC_DIMS: usize = 384;

pub trait Embedder: Send + Sync {
    fn model_id(&self) -> &str;
    fn dims(&self) -> usize;
    fn is_semantic(&self) -> bool;
    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    fn embed_one(&self, text: &str) -> Result<Vec<f32>> {
        let embeddings = self.embed_batch(&[text.to_string()])?;
        embeddings
            .into_iter()
            .next()
            .context("embedder returned no query embedding")
    }
}

#[derive(Debug, Clone)]
pub struct EmbedderConfig {
    pub provider: EmbedderProvider,
    pub model_cache: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedderProvider {
    FastEmbed,
    HashFallback,
    Disabled,
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
        Self {
            provider,
            model_cache,
        }
    }

    pub fn status_without_loading(&self) -> EmbedderStatus {
        match self.provider {
            EmbedderProvider::FastEmbed => EmbedderStatus {
                provider: "fastembed".to_string(),
                model_id: Some(DEFAULT_SEMANTIC_MODEL_ID.to_string()),
                dims: Some(DEFAULT_SEMANTIC_DIMS),
                semantic: true,
                available: true,
                degraded_reason: None,
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
            EmbedderProvider::FastEmbed => Ok(Box::new(FastEmbedder::new(&self.model_cache)?)),
            EmbedderProvider::HashFallback => Ok(Box::new(HashEmbedder)),
            EmbedderProvider::Disabled => anyhow::bail!("query embedder disabled"),
        }
    }
}

pub struct FastEmbedder {
    model: Mutex<TextEmbedding>,
}

impl FastEmbedder {
    pub fn new(model_cache: &Path) -> Result<Self> {
        std::fs::create_dir_all(model_cache)
            .with_context(|| format!("creating model cache {}", model_cache.display()))?;
        let options = InitOptions::new(EmbeddingModel::SnowflakeArcticEmbedXSQ)
            .with_cache_dir(model_cache.to_path_buf())
            .with_show_download_progress(false);
        let model = TextEmbedding::try_new(options).context("loading fastembed model")?;
        Ok(Self {
            model: Mutex::new(model),
        })
    }
}

impl Embedder for FastEmbedder {
    fn model_id(&self) -> &str {
        DEFAULT_SEMANTIC_MODEL_ID
    }

    fn dims(&self) -> usize {
        DEFAULT_SEMANTIC_DIMS
    }

    fn is_semantic(&self) -> bool {
        true
    }

    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut model = self
            .model
            .lock()
            .map_err(|_| anyhow::anyhow!("fastembed model lock poisoned"))?;
        model
            .embed(texts, None)
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

    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
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
            model_cache: PathBuf::from("unused"),
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
    fn hash_fallback_is_deterministic() {
        let embedder = HashEmbedder;
        let first = embedder.embed_one("offline convergence").expect("first");
        let second = embedder.embed_one("offline convergence").expect("second");
        assert_eq!(first, second);
    }
}
