use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, RawArtifact, SessionRecord,
};
use crate::config::TreechatSourceConfig;
use crate::source::{
    AdapterConcurrency, PreparedImport, SearchSegment, SemanticPolicy, SourceAdapter,
    SourceCandidate, SourceCheckpointUpsert, SourceSyncContext, SourceUpsert,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use directories::BaseDirs;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::time::Duration;

const TREECHAT_SOURCE_KIND: &str = "treechat";
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_SEARCH_PART_CHARS: usize = 24_000;

pub struct TreechatAdapter {
    config: ResolvedTreechatConfig,
    agent: ureq::Agent,
}

impl TreechatAdapter {
    pub fn from_config(
        config: &TreechatSourceConfig,
        selected_source: Option<&str>,
    ) -> Result<Option<Self>> {
        let selected = selected_source == Some(TREECHAT_SOURCE_KIND);
        if !config.enabled && !selected {
            return Ok(None);
        }
        let resolved = ResolvedTreechatConfig::resolve(config)?;
        if !resolved.has_credentials() {
            bail!(
                "missing Treechat credentials; run treectl login or set [sources.treechat] credentials"
            );
        }
        Ok(Some(Self {
            config: resolved,
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(20))
                .build(),
        }))
    }

    fn list_clip_threads(&self) -> Result<Vec<TreechatQuest>> {
        let mut quests = Vec::new();
        let mut seen = HashSet::new();
        let page_limit = self.config.page_limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(1);
        for page in 1..=page_limit {
            let page_text = page.to_string();
            let (value, _) = self.get_json(
                "/api/v1/quests",
                &[("clip", "true"), ("order", "time"), ("page", &page_text)],
            )?;
            let response: TreechatQuestListResponse = serde_json::from_value(value)
                .with_context(|| format!("parsing Treechat clip list page {page}"))?;
            if response.quests.is_empty() {
                break;
            }
            for quest in response.quests {
                if seen.insert(quest.id.clone()) {
                    quests.push(quest);
                    if self
                        .config
                        .thread_limit
                        .is_some_and(|limit| quests.len() >= limit)
                    {
                        return Ok(quests);
                    }
                }
            }
        }
        Ok(quests)
    }

    fn fetch_thread(&self, quest_id: &str) -> Result<(TreechatQuest, Vec<u8>)> {
        let (value, raw) = self.get_json(&format!("/api/v1/quests/{quest_id}"), &[])?;
        let response: TreechatThreadResponse = serde_json::from_value(value)
            .with_context(|| format!("parsing Treechat thread {quest_id}"))?;
        Ok((response.quest, raw))
    }

    fn get_json(&self, path: &str, query: &[(&str, &str)]) -> Result<(Value, Vec<u8>)> {
        let url = format!("{}{}", self.config.backend_url, path);
        let mut request = self
            .agent
            .get(&url)
            .set("accept", "application/json")
            .set("access-token", &self.config.access_token)
            .set("client", &self.config.client)
            .set("uid", &self.config.uid);
        for (key, value) in query {
            request = request.query(key, value);
        }
        let response = request
            .call()
            .map_err(|err| anyhow::anyhow!("Treechat request failed for {url}: {err}"))?;
        let body = response
            .into_string()
            .with_context(|| format!("reading Treechat response {url}"))?;
        let value = serde_json::from_str(&body)
            .with_context(|| format!("parsing Treechat response {url}"))?;
        Ok((value, body.into_bytes()))
    }
}

impl SourceAdapter for TreechatAdapter {
    fn kind(&self) -> &'static str {
        TREECHAT_SOURCE_KIND
    }

    fn concurrency(&self) -> AdapterConcurrency {
        AdapterConcurrency::new(1, 2)
    }

    fn discover(&self) -> Result<Vec<SourceCandidate>> {
        self.list_clip_threads().map(|quests| {
            quests
                .into_iter()
                .map(|quest| {
                    let modified = quest
                        .updated_time()
                        .map(|time| time.timestamp_millis() as i128)
                        .unwrap_or(0);
                    SourceCandidate {
                        adapter_kind: self.kind(),
                        kind: TREECHAT_SOURCE_KIND.to_string(),
                        identity: quest.id.clone(),
                        path: quest.quest_url.or(quest.path).map(Into::into),
                        modified,
                        size: None,
                        mtime_ms: None,
                    }
                })
                .collect()
        })
    }

    fn is_current(
        &self,
        context: &SourceSyncContext<'_>,
        candidate: &SourceCandidate,
    ) -> Result<bool> {
        if candidate.modified <= 0 {
            return Ok(false);
        }
        let checkpoint = context
            .store
            .source_checkpoint(TREECHAT_SOURCE_KIND, &candidate.identity)?;
        Ok(checkpoint
            .and_then(|checkpoint| checkpoint.cursor)
            .is_some_and(|cursor| cursor == candidate.modified.to_string()))
    }

    fn prepare_import(
        &self,
        _context: &SourceSyncContext<'_>,
        machine_id: &str,
        candidate: &SourceCandidate,
    ) -> Result<PreparedImport> {
        let (quest, raw) = self.fetch_thread(&candidate.identity)?;
        let raw_hash = blake3_hex(&raw);
        let source_identity = format!("{}:{}", self.config.profile, quest.id);
        let source_id = stable_id(&["source", TREECHAT_SOURCE_KIND, &source_identity]);
        let source_path = quest.quest_url.as_deref().or(quest.path.as_deref());
        let source_upsert = SourceUpsert {
            id: source_id.clone(),
            kind: TREECHAT_SOURCE_KIND.to_string(),
            identity: source_identity,
            path: source_path.map(ToOwned::to_owned),
        };

        let session_id = stable_id(&["session", TREECHAT_SOURCE_KIND, &quest.id]);
        let answers = quest.searchable_answers();
        let title = treechat_thread_title(&quest, &answers);
        let started_at = answers
            .iter()
            .filter_map(|answer| answer.created_time())
            .min();
        let updated_at = answers
            .iter()
            .filter_map(|answer| answer.updated_time().or_else(|| answer.created_time()))
            .max()
            .or_else(|| quest.updated_time());
        let mut records = vec![
            ArchiveRecord::RawArtifact(RawArtifact {
                hash: raw_hash.clone(),
                source_id: source_id.clone(),
                path: format!("{}/api/v1/quests/{}", self.config.backend_url, quest.id),
                size: raw.len() as u64,
                mtime_ms: candidate
                    .modified
                    .try_into()
                    .ok()
                    .filter(|modified: &i64| *modified > 0),
                media_type: "application/json".to_string(),
                content: raw,
                first_seen_at: Utc::now(),
            }),
            ArchiveRecord::Session(SessionRecord {
                id: session_id.clone(),
                source_id: source_id.clone(),
                machine_id: machine_id.to_string(),
                source_kind: TREECHAT_SOURCE_KIND.to_string(),
                external_id: quest.id.clone(),
                title,
                status: "open".to_string(),
                started_at,
                updated_at,
                metadata: json!({
                    "source_type": "treechat_thread",
                    "scope": "clips",
                    "profile": self.config.profile,
                    "backend_url": self.config.backend_url,
                    "app_host": self.config.app_host,
                    "quest_id": quest.id,
                    "quest_url": quest.quest_url,
                    "path": quest.path,
                    "is_clip": quest.is_clip,
                    "search_content_scope": self.config.content_scope.as_str()
                }),
                hash: stable_hash(&(
                    TREECHAT_SOURCE_KIND,
                    &source_id,
                    &quest.id,
                    &updated_at.map(|time| time.to_rfc3339()),
                ))?,
            }),
        ];

        let mut ordinal = 0i64;
        for answer in &answers {
            let answer_id = answer.id.as_deref().unwrap_or("unknown");
            let role = if answer.is_system.unwrap_or(false) {
                "system"
            } else {
                "user"
            };
            for part in treechat_answer_search_parts(answer, self.config.content_scope) {
                let chunks = chunk_search_text(&part.text);
                let chunk_count = chunks.len();
                for (chunk_index, search_text) in chunks.into_iter().enumerate() {
                    let search = part
                        .search_segment(role, search_text.clone(), self.config.content_scope)
                        .with_lexical_indexable(role == "user")
                        .with_stable_part(quest.id.clone())
                        .with_stable_part(answer_id.to_string())
                        .with_stable_part(chunk_index.to_string());
                    let event_id = stable_id(&[
                        "event",
                        TREECHAT_SOURCE_KIND,
                        &session_id,
                        &ordinal.to_string(),
                        answer_id,
                        part.provenance,
                        &chunk_index.to_string(),
                    ]);
                    let mut metadata = json!({
                        "raw_artifact_hash": raw_hash,
                        "capture_fidelity": "treechat_api_thread",
                        "parser": "treechat_api_v1",
                        "treechat_quest_id": quest.id,
                        "treechat_answer_id": answer.id,
                        "treechat_answer_path": answer.path,
                        "treechat_user_id": answer.user_id,
                        "treechat_user_name": answer.user.as_ref().and_then(|user| user.name.clone()),
                        "treechat_message_type": answer.message_type,
                        "treechat_is_clip": answer.is_clip,
                        "treechat_url": answer.url.as_ref().map(|url| json!({
                            "id": url.id,
                            "address": url.address,
                            "title": url.title,
                            "full_text_attachment_id": url.full_text_attachment_id,
                        })),
                        "treechat_url_hash": answer.url_address().map(|address| blake3_hex(address.as_bytes())),
                        "search_content_scope": self.config.content_scope.as_str(),
                        "search_chunk_index": chunk_index,
                        "search_chunk_count": chunk_count,
                    })
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                    search.apply_compat_metadata(&mut metadata);
                    records.push(ArchiveRecord::Event(EventRecord {
                        id: event_id,
                        session_id: session_id.clone(),
                        source_id: source_id.clone(),
                        machine_id: machine_id.to_string(),
                        source_kind: TREECHAT_SOURCE_KIND.to_string(),
                        ordinal,
                        event_type: part.event_type.to_string(),
                        role: Some(role.to_string()),
                        content: search_text.clone(),
                        raw_artifact_hash: Some(raw_hash.clone()),
                        occurred_at: answer.created_time(),
                        metadata: Value::Object(metadata),
                        hash: stable_hash(&(
                            TREECHAT_SOURCE_KIND,
                            &quest.id,
                            answer_id,
                            part.provenance,
                            chunk_index,
                            &answer.updated_at,
                            &search_text,
                        ))?,
                    }));
                    ordinal += 1;
                }
            }
        }

        let mut prepared = PreparedImport::full(vec![source_upsert], records);
        if candidate.modified > 0 {
            prepared = prepared.with_checkpoint(SourceCheckpointUpsert {
                source_kind: TREECHAT_SOURCE_KIND.to_string(),
                source_identity: candidate.identity.clone(),
                cursor: Some(candidate.modified.to_string()),
                metadata: json!({
                    "profile": self.config.profile,
                    "scope": "clips",
                    "quest_id": candidate.identity,
                    "updated_at": updated_at.map(|time| time.to_rfc3339())
                }),
            });
        }
        Ok(prepared)
    }
}

#[derive(Debug, Clone)]
struct ResolvedTreechatConfig {
    profile: String,
    backend_url: String,
    app_host: Option<String>,
    access_token: String,
    client: String,
    uid: String,
    page_limit: Option<usize>,
    thread_limit: Option<usize>,
    content_scope: TreechatContentScope,
}

impl ResolvedTreechatConfig {
    fn resolve(config: &TreechatSourceConfig) -> Result<Self> {
        let treectl = TreectlConfigFile::load();
        let profile = config
            .profile
            .as_deref()
            .map(normalize_profile)
            .or_else(|| {
                std::env::var("TREECTL_PROFILE")
                    .ok()
                    .map(|value| normalize_profile(&value))
            })
            .or_else(|| {
                treectl
                    .active_profile
                    .clone()
                    .map(|value| normalize_profile(&value))
            })
            .unwrap_or_else(|| "dev".to_string());
        let mut resolved = built_in_profile(&profile);
        if let Some(stored) = treectl.profiles.get(&profile) {
            resolved.merge(stored);
        }
        resolved.apply_env();
        resolved.apply_historious_config(config);
        resolved.profile = profile;
        resolved.backend_url = normalize_base_url(&resolved.backend_url);
        resolved.app_host = resolved.app_host.as_deref().map(normalize_base_url);
        resolved.page_limit = config.page_limit;
        resolved.thread_limit = config.thread_limit;
        resolved.content_scope = TreechatContentScope::parse(config.content_scope.as_deref())?;
        Ok(resolved)
    }

    fn has_credentials(&self) -> bool {
        !self.access_token.is_empty() && !self.client.is_empty() && !self.uid.is_empty()
    }

    fn merge(&mut self, other: &TreectlProfile) {
        merge_string(&mut self.backend_url, other.backend_url.as_deref());
        merge_option(&mut self.app_host, other.app_host.as_deref());
        merge_string(&mut self.access_token, other.access_token.as_deref());
        merge_string(&mut self.client, other.client.as_deref());
        merge_string(&mut self.uid, other.uid.as_deref());
    }

    fn apply_env(&mut self) {
        merge_string(
            &mut self.backend_url,
            std::env::var("TREECTL_BACKEND_URL").ok().as_deref(),
        );
        merge_option(
            &mut self.app_host,
            std::env::var("TREECTL_APP_HOST").ok().as_deref(),
        );
    }

    fn apply_historious_config(&mut self, config: &TreechatSourceConfig) {
        merge_string(&mut self.backend_url, config.backend_url.as_deref());
        merge_option(&mut self.app_host, config.app_host.as_deref());
        merge_string(&mut self.access_token, config.access_token.as_deref());
        merge_string(&mut self.client, config.client.as_deref());
        merge_string(&mut self.uid, config.uid.as_deref());
    }
}

#[derive(Debug, Default, Deserialize)]
struct TreectlConfigFile {
    active_profile: Option<String>,
    #[serde(default)]
    profiles: HashMap<String, TreectlProfile>,
}

impl TreectlConfigFile {
    fn load() -> Self {
        let Some(base_dirs) = BaseDirs::new() else {
            return Self::default();
        };
        let path = base_dirs.config_dir().join("treectl/config.toml");
        let Ok(text) = fs::read_to_string(path) else {
            return Self::default();
        };
        toml::from_str(&text).unwrap_or_default()
    }
}

#[derive(Debug, Default, Deserialize)]
struct TreectlProfile {
    backend_url: Option<String>,
    app_host: Option<String>,
    access_token: Option<String>,
    client: Option<String>,
    uid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TreechatQuestListResponse {
    #[serde(default)]
    quests: Vec<TreechatQuest>,
}

#[derive(Debug, Deserialize)]
struct TreechatThreadResponse {
    quest: TreechatQuest,
}

#[derive(Debug, Clone, Deserialize)]
struct TreechatQuest {
    id: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    quest_url: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    updated_at_iso: Option<String>,
    #[serde(default)]
    is_clip: Option<bool>,
    #[serde(default)]
    parent: Option<TreechatAnswer>,
    #[serde(default)]
    sorted_answers: Vec<TreechatAnswer>,
}

impl TreechatQuest {
    fn updated_time(&self) -> Option<DateTime<Utc>> {
        parse_treechat_time(
            self.updated_at_iso
                .as_deref()
                .or(self.updated_at.as_deref()),
        )
    }

    fn searchable_answers(&self) -> Vec<TreechatAnswer> {
        let mut answers = Vec::new();
        let mut seen = HashSet::new();
        if let Some(parent) = &self.parent {
            if let Some(id) = &parent.id {
                seen.insert(id.clone());
            }
            answers.push(parent.clone());
        }
        for answer in &self.sorted_answers {
            if answer
                .id
                .as_ref()
                .is_some_and(|id| !seen.insert(id.clone()))
            {
                continue;
            }
            answers.push(answer.clone());
        }
        answers
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TreechatAnswer {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    display_content: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    message_type: Option<String>,
    #[serde(default)]
    is_clip: Option<bool>,
    #[serde(default)]
    is_system: Option<bool>,
    #[serde(default)]
    user: Option<TreechatUser>,
    #[serde(default)]
    url: Option<TreechatUrl>,
    #[serde(default)]
    url_title: Option<String>,
    #[serde(default)]
    url_address: Option<String>,
    #[serde(default)]
    url_full_text: Option<String>,
}

impl TreechatAnswer {
    fn created_time(&self) -> Option<DateTime<Utc>> {
        parse_treechat_time(self.created_at.as_deref())
    }

    fn updated_time(&self) -> Option<DateTime<Utc>> {
        parse_treechat_time(self.updated_at.as_deref())
    }

    fn url_title(&self) -> Option<&str> {
        self.url_title
            .as_deref()
            .or_else(|| self.url.as_ref().and_then(|url| url.title.as_deref()))
    }

    fn url_address(&self) -> Option<&str> {
        self.url_address
            .as_deref()
            .or_else(|| self.url.as_ref().and_then(|url| url.address.as_deref()))
    }

    fn url_full_text(&self) -> Option<&str> {
        self.url_full_text
            .as_deref()
            .or_else(|| self.url.as_ref().and_then(|url| url.full_text.as_deref()))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TreechatUser {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TreechatUrl {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    full_text: Option<String>,
    #[serde(default)]
    full_text_attachment_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum TreechatContentScope {
    TextOnly,
    UrlMetadata,
    FullText,
}

impl TreechatContentScope {
    fn parse(value: Option<&str>) -> Result<Self> {
        match value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("full_text")
        {
            "text" | "text_only" | "treechat_text" => Ok(Self::TextOnly),
            "url" | "url_metadata" | "metadata" => Ok(Self::UrlMetadata),
            "full" | "full_text" | "linked_full_text" => Ok(Self::FullText),
            other => bail!(
                "unknown Treechat content_scope '{other}'; use text_only, url_metadata, or full_text"
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::TextOnly => "text_only",
            Self::UrlMetadata => "url_metadata",
            Self::FullText => "full_text",
        }
    }
}

struct SearchPart {
    provenance: &'static str,
    event_type: &'static str,
    text: String,
}

impl SearchPart {
    fn search_segment(
        &self,
        role: &str,
        text: String,
        scope: TreechatContentScope,
    ) -> SearchSegment {
        SearchSegment::indexed("conversation", role, text, SemanticPolicy::Required)
            .with_provenance(self.provenance)
            .with_metadata("event_type", json!(self.event_type))
            .with_metadata("content_scope", json!(scope.as_str()))
    }
}

fn built_in_profile(profile: &str) -> ResolvedTreechatConfig {
    let (backend_url, app_host) = match profile {
        // Staging and prod profiles default to localhost; set real URLs via
        // config or the TREECHAT_BACKEND_URL / TREECHAT_APP_HOST env vars.
        "staging" | "prod" => ("http://localhost:5001", Some("http://localhost:5173")),
        _ => ("http://localhost:5001", Some("http://localhost:5173")),
    };
    ResolvedTreechatConfig {
        profile: profile.to_string(),
        backend_url: backend_url.to_string(),
        app_host: app_host.map(ToOwned::to_owned),
        access_token: String::new(),
        client: String::new(),
        uid: String::new(),
        page_limit: None,
        thread_limit: None,
        content_scope: TreechatContentScope::FullText,
    }
}

fn treechat_thread_title(quest: &TreechatQuest, answers: &[TreechatAnswer]) -> Option<String> {
    clean_text(quest.content.as_deref())
        .or_else(|| {
            answers
                .iter()
                .find_map(|answer| clean_text(answer.content.as_deref()))
        })
        .map(|text| truncate_title(&text))
}

fn treechat_answer_search_parts(
    answer: &TreechatAnswer,
    scope: TreechatContentScope,
) -> Vec<SearchPart> {
    let mut parts = Vec::new();
    if let Some(text) = treechat_answer_text(answer) {
        parts.push(SearchPart {
            provenance: "treechat_text",
            event_type: "treechat_answer",
            text,
        });
    }
    if matches!(
        scope,
        TreechatContentScope::UrlMetadata | TreechatContentScope::FullText
    ) {
        if let Some(text) = treechat_url_metadata_text(answer) {
            parts.push(SearchPart {
                provenance: "url_metadata",
                event_type: "treechat_url_metadata",
                text,
            });
        }
    }
    if matches!(scope, TreechatContentScope::FullText) {
        if let Some(text) = clean_text(answer.url_full_text()) {
            parts.push(SearchPart {
                provenance: "url_full_text",
                event_type: "treechat_url_full_text",
                text,
            });
        }
    }
    parts
}

fn treechat_answer_text(answer: &TreechatAnswer) -> Option<String> {
    clean_text(
        answer
            .display_content
            .as_deref()
            .or(answer.content.as_deref()),
    )
}

fn treechat_url_metadata_text(answer: &TreechatAnswer) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(title) = clean_text(answer.url_title()) {
        parts.push(title);
    }
    if let Some(address) = clean_text(answer.url_address()) {
        parts.push(address);
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn chunk_search_text(text: &str) -> Vec<String> {
    let chars = text.chars().collect::<Vec<_>>();
    if chars.len() <= MAX_SEARCH_PART_CHARS {
        return vec![text.to_string()];
    }
    chars
        .chunks(MAX_SEARCH_PART_CHARS)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect()
}

fn clean_text(value: Option<&str>) -> Option<String> {
    let raw = value?.trim();
    if raw.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(raw.len());
    let mut in_tag = false;
    for ch in raw.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                out.push(' ');
            }
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    let text = out.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn truncate_title(text: &str) -> String {
    const MAX_CHARS: usize = 120;
    let mut title = text.chars().take(MAX_CHARS).collect::<String>();
    if text.chars().count() > MAX_CHARS {
        title.push_str("...");
    }
    title
}

fn parse_treechat_time(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|time| time.with_timezone(&Utc))
}

fn normalize_profile(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_base_url(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
}

fn merge_string(target: &mut String, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        *target = value.to_string();
    }
}

fn merge_option(target: &mut Option<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        *target = Some(value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answer_search_parts_follow_configured_content_scope() {
        let answer = TreechatAnswer {
            id: Some("answer".to_string()),
            user_id: None,
            content: Some("<p>saved this</p>".to_string()),
            display_content: None,
            path: None,
            created_at: None,
            updated_at: None,
            message_type: None,
            is_clip: Some(true),
            is_system: None,
            user: None,
            url: Some(TreechatUrl {
                id: Some("url".to_string()),
                address: Some("https://example.com/article".to_string()),
                title: Some("Example Article".to_string()),
                full_text: Some("Long page text".to_string()),
                full_text_attachment_id: Some("attachment".to_string()),
            }),
            url_title: None,
            url_address: None,
            url_full_text: None,
        };

        let text_only = treechat_answer_search_parts(&answer, TreechatContentScope::TextOnly);
        let full_text = treechat_answer_search_parts(&answer, TreechatContentScope::FullText);
        let joined = full_text
            .iter()
            .map(|part| part.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert_eq!(text_only.len(), 1);
        assert!(joined.contains("saved this"));
        assert!(joined.contains("Example Article"));
        assert!(joined.contains("https://example.com/article"));
        assert!(joined.contains("Long page text"));
        assert!(full_text
            .iter()
            .any(|part| part.provenance == "url_full_text"));
    }

    #[test]
    fn search_part_builds_explicit_segment_metadata() {
        let part = SearchPart {
            provenance: "url_metadata",
            event_type: "treechat_url_metadata",
            text: "Example Article\nhttps://example.com/article".to_string(),
        };

        let segment = part
            .search_segment("user", part.text.clone(), TreechatContentScope::UrlMetadata)
            .with_lexical_indexable(true);
        let metadata = segment.compat_metadata();

        assert_eq!(metadata["search_indexable"], true);
        assert_eq!(metadata["search_kind"], "user");
        assert_eq!(metadata["search_text"], part.text);
        assert_eq!(metadata["search_provenance"], "url_metadata");
        assert_eq!(
            metadata["search_segment_metadata"]["event_type"],
            "treechat_url_metadata"
        );
        assert_eq!(
            metadata["search_segment_metadata"]["content_scope"],
            "url_metadata"
        );
    }
}
