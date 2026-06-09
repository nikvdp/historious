use crate::storage::{ImportStats, Store};
use anyhow::{bail, Result};
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
}
