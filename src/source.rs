use crate::storage::{ImportStats, Store};
use anyhow::{bail, Result};
use serde_json::{Map, Value};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SourceCandidate {
    pub adapter_kind: &'static str,
    pub kind: String,
    pub identity: String,
    pub path: Option<PathBuf>,
    pub modified: i128,
    pub size: Option<u64>,
    pub mtime_ms: Option<i64>,
}

impl SourceCandidate {
    pub fn progress_path(&self) -> PathBuf {
        self.path
            .clone()
            .unwrap_or_else(|| PathBuf::from(&self.identity))
    }
}

pub struct SourceSyncContext<'a> {
    pub store: &'a Store,
    pub machine_id: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticPolicy {
    Required,
    #[allow(dead_code)]
    Opportunistic,
    Never,
}

impl SemanticPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Opportunistic => "opportunistic",
            Self::Never => "never",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchSegment {
    pub tier: String,
    pub kind: String,
    pub text: String,
    pub lexical_indexable: bool,
    pub semantic_policy: SemanticPolicy,
    pub provenance: Option<String>,
    pub stable_parts: Vec<String>,
    pub metadata: Map<String, Value>,
    pub skip_reason: Option<String>,
}

impl SearchSegment {
    pub fn indexed(
        tier: impl Into<String>,
        kind: impl Into<String>,
        text: impl Into<String>,
        semantic_policy: SemanticPolicy,
    ) -> Self {
        Self {
            tier: tier.into(),
            kind: kind.into(),
            text: text.into(),
            lexical_indexable: true,
            semantic_policy,
            provenance: None,
            stable_parts: Vec::new(),
            metadata: Map::new(),
            skip_reason: None,
        }
    }

    pub fn skipped(kind: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            tier: "raw".to_string(),
            kind: kind.into(),
            text: String::new(),
            lexical_indexable: false,
            semantic_policy: SemanticPolicy::Never,
            provenance: None,
            stable_parts: Vec::new(),
            metadata: Map::new(),
            skip_reason: Some(reason.into()),
        }
    }

    pub fn with_provenance(mut self, provenance: impl Into<String>) -> Self {
        self.provenance = Some(provenance.into());
        self
    }

    pub fn with_lexical_indexable(mut self, lexical_indexable: bool) -> Self {
        self.lexical_indexable = lexical_indexable;
        self
    }

    pub fn with_stable_part(mut self, part: impl Into<String>) -> Self {
        self.stable_parts.push(part.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    pub fn is_searchable(&self) -> bool {
        self.lexical_indexable && !self.text.trim().is_empty()
    }

    pub fn apply_compat_metadata(&self, metadata: &mut Map<String, Value>) {
        metadata.insert(
            "search_indexable".to_string(),
            Value::Bool(self.is_searchable()),
        );
        metadata.insert("search_kind".to_string(), Value::String(self.kind.clone()));
        metadata.insert("search_text".to_string(), Value::String(self.text.clone()));
        metadata.insert(
            "search_semantic_policy".to_string(),
            Value::String(self.semantic_policy.as_str().to_string()),
        );
        metadata.insert("search_tier".to_string(), Value::String(self.tier.clone()));
        metadata.insert(
            "search_skip_reason".to_string(),
            self.skip_reason
                .as_ref()
                .map(|reason| Value::String(reason.clone()))
                .unwrap_or(Value::Null),
        );
        metadata.insert(
            "search_provenance".to_string(),
            self.provenance
                .as_ref()
                .map(|provenance| Value::String(provenance.clone()))
                .unwrap_or(Value::Null),
        );
        if !self.stable_parts.is_empty() {
            metadata.insert(
                "search_stable_parts".to_string(),
                Value::Array(
                    self.stable_parts
                        .iter()
                        .map(|part| Value::String(part.clone()))
                        .collect(),
                ),
            );
        }
        if !self.metadata.is_empty() {
            metadata.insert(
                "search_segment_metadata".to_string(),
                Value::Object(self.metadata.clone()),
            );
        }
    }

    #[allow(dead_code)]
    pub fn compat_metadata(&self) -> Value {
        let mut metadata = Map::new();
        self.apply_compat_metadata(&mut metadata);
        Value::Object(metadata)
    }
}

pub trait SourceAdapter {
    fn kind(&self) -> &'static str;
    fn discover(&self) -> Result<Vec<SourceCandidate>>;
    fn is_current(
        &self,
        context: &SourceSyncContext<'_>,
        candidate: &SourceCandidate,
    ) -> Result<bool>;
    fn import(
        &self,
        context: &SourceSyncContext<'_>,
        candidate: &SourceCandidate,
    ) -> Result<ImportStats>;
}

#[derive(Default)]
pub struct SourceAdapterRegistry {
    adapters: Vec<Box<dyn SourceAdapter>>,
}

impl SourceAdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<A>(mut self, adapter: A) -> Result<Self>
    where
        A: SourceAdapter + 'static,
    {
        if self
            .adapters
            .iter()
            .any(|existing| existing.kind() == adapter.kind())
        {
            bail!("source adapter '{}' is already registered", adapter.kind());
        }
        self.adapters.push(Box::new(adapter));
        Ok(self)
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn SourceAdapter> {
        self.adapters.iter().map(Box::as_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    struct FakeAdapter {
        kind: &'static str,
    }

    impl SourceAdapter for FakeAdapter {
        fn kind(&self) -> &'static str {
            self.kind
        }

        fn discover(&self) -> Result<Vec<SourceCandidate>> {
            Ok(Vec::new())
        }

        fn is_current(
            &self,
            _context: &SourceSyncContext<'_>,
            _candidate: &SourceCandidate,
        ) -> Result<bool> {
            Ok(false)
        }

        fn import(
            &self,
            _context: &SourceSyncContext<'_>,
            _candidate: &SourceCandidate,
        ) -> Result<ImportStats> {
            Ok(ImportStats::default())
        }
    }

    #[test]
    fn registry_iterates_registered_adapters() {
        let registry = SourceAdapterRegistry::new()
            .register(FakeAdapter { kind: "fake" })
            .expect("register fake")
            .register(FakeAdapter { kind: "other" })
            .expect("register other");
        let kinds = registry
            .iter()
            .map(|adapter| adapter.kind())
            .collect::<Vec<_>>();

        assert_eq!(kinds, vec!["fake", "other"]);
    }

    #[test]
    fn registry_rejects_duplicate_adapter_kinds() {
        let registry = SourceAdapterRegistry::new()
            .register(FakeAdapter { kind: "fake" })
            .expect("register fake");
        let err = match registry.register(FakeAdapter { kind: "fake" }) {
            Ok(_) => panic!("duplicate adapter kind should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("already registered"));
    }

    #[test]
    fn source_context_carries_store_and_machine_id() {
        let dir = tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("store");
        let context = SourceSyncContext {
            store: &store,
            machine_id: "machine-test",
        };

        assert_eq!(context.machine_id, "machine-test");
    }

    #[test]
    fn search_segment_exposes_compatibility_metadata() {
        let segment = SearchSegment::indexed(
            "conversation",
            "user",
            "find adapter notes",
            SemanticPolicy::Required,
        )
        .with_provenance("message_text")
        .with_stable_part("message-1")
        .with_metadata("source_field", serde_json::json!("content"));

        let metadata = segment.compat_metadata();

        assert_eq!(metadata["search_indexable"], true);
        assert_eq!(metadata["search_kind"], "user");
        assert_eq!(metadata["search_text"], "find adapter notes");
        assert_eq!(metadata["search_semantic_policy"], "required");
        assert_eq!(metadata["search_tier"], "conversation");
        assert_eq!(metadata["search_provenance"], "message_text");
        assert_eq!(metadata["search_stable_parts"][0], "message-1");
        assert_eq!(
            metadata["search_segment_metadata"]["source_field"],
            "content"
        );
    }

    #[test]
    fn skipped_search_segment_preserves_skip_reason_without_searching() {
        let segment = SearchSegment::skipped("tool", "tool event");
        let metadata = segment.compat_metadata();

        assert!(!segment.is_searchable());
        assert_eq!(metadata["search_indexable"], false);
        assert_eq!(metadata["search_kind"], "tool");
        assert_eq!(metadata["search_text"], "");
        assert_eq!(metadata["search_semantic_policy"], "never");
        assert_eq!(metadata["search_skip_reason"], "tool event");
    }

    #[test]
    fn semantic_policy_renders_storage_values() {
        assert_eq!(SemanticPolicy::Required.as_str(), "required");
        assert_eq!(SemanticPolicy::Opportunistic.as_str(), "opportunistic");
        assert_eq!(SemanticPolicy::Never.as_str(), "never");
    }
}
