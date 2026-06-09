use crate::archive::{
    blake3_hex, stable_hash, stable_id, ArchiveRecord, EventRecord, RawArtifact, SessionRecord,
};
use crate::config::TreechatSourceConfig;
use crate::source::{SourceAdapter, SourceCandidate, SourceSyncContext};
use crate::storage::ImportStats;
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

    fn import(
        &self,
        context: &SourceSyncContext<'_>,
        candidate: &SourceCandidate,
    ) -> Result<ImportStats> {
        let (quest, raw) = self.fetch_thread(&candidate.identity)?;
        let raw_hash = blake3_hex(&raw);
        let source_identity = format!("{}:{}", self.config.profile, quest.id);
        let source_id = stable_id(&["source", TREECHAT_SOURCE_KIND, &source_identity]);
        let source_path = quest.quest_url.as_deref().or(quest.path.as_deref());
        context.store.upsert_source(
            &source_id,
            TREECHAT_SOURCE_KIND,
            &source_identity,
            source_path,
        )?;

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
                machine_id: context.machine_id.to_string(),
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
                    "search_content_scope": "treechat_text_with_url_metadata"
                }),
                hash: stable_hash(&(
                    TREECHAT_SOURCE_KIND,
                    &source_id,
                    &quest.id,
                    &updated_at.map(|time| time.to_rfc3339()),
                ))?,
            }),
        ];

        for (ordinal, answer) in answers.iter().enumerate() {
            let Some(search_text) = treechat_answer_search_text(answer) else {
                continue;
            };
            let answer_id = answer.id.as_deref().unwrap_or("unknown");
            let event_id = stable_id(&[
                "event",
                TREECHAT_SOURCE_KIND,
                &session_id,
                &ordinal.to_string(),
                answer_id,
            ]);
            let role = if answer.is_system.unwrap_or(false) {
                "system"
            } else {
                "user"
            };
            records.push(ArchiveRecord::Event(EventRecord {
                id: event_id,
                session_id: session_id.clone(),
                source_id: source_id.clone(),
                machine_id: context.machine_id.to_string(),
                source_kind: TREECHAT_SOURCE_KIND.to_string(),
                ordinal: ordinal as i64,
                event_type: "treechat_answer".to_string(),
                role: Some(role.to_string()),
                content: search_text.clone(),
                raw_artifact_hash: Some(raw_hash.clone()),
                occurred_at: answer.created_time(),
                metadata: json!({
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
                        "address": url.address,
                        "title": url.title,
                    })),
                    "search_indexable": role == "user",
                    "search_kind": role,
                    "search_text": search_text,
                    "search_content_scope": "treechat_text_with_url_metadata",
                    "search_skip_reason": null
                }),
                hash: stable_hash(&(
                    TREECHAT_SOURCE_KIND,
                    &quest.id,
                    answer_id,
                    &answer.updated_at,
                    &search_text,
                ))?,
            }));
        }

        let stats = context.store.import_records(&records)?;
        if candidate.modified > 0 {
            context.store.upsert_source_checkpoint(
                TREECHAT_SOURCE_KIND,
                &candidate.identity,
                Some(&candidate.modified.to_string()),
                &json!({
                    "profile": self.config.profile,
                    "scope": "clips",
                    "quest_id": candidate.identity,
                    "updated_at": updated_at.map(|time| time.to_rfc3339())
                }),
            )?;
        }
        Ok(stats)
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
}

impl TreechatAnswer {
    fn created_time(&self) -> Option<DateTime<Utc>> {
        parse_treechat_time(self.created_at.as_deref())
    }

    fn updated_time(&self) -> Option<DateTime<Utc>> {
        parse_treechat_time(self.updated_at.as_deref())
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
    address: Option<String>,
    #[serde(default)]
    title: Option<String>,
}

fn built_in_profile(profile: &str) -> ResolvedTreechatConfig {
    let (backend_url, app_host) = match profile {
        "staging" => (
            "https://knov-staging-jajw.onrender.com",
            Some("https://staging-frontend-vi5w.onrender.com"),
        ),
        "prod" => (
            "https://knov-prod.onrender.com",
            Some("https://prod-frontend-kitu.onrender.com"),
        ),
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

fn treechat_answer_search_text(answer: &TreechatAnswer) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(text) = clean_text(
        answer
            .display_content
            .as_deref()
            .or(answer.content.as_deref()),
    ) {
        parts.push(text);
    }
    if let Some(url) = &answer.url {
        if let Some(title) = clean_text(url.title.as_deref()) {
            parts.push(title);
        }
        if let Some(address) = clean_text(url.address.as_deref()) {
            parts.push(address);
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
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
    fn answer_search_text_includes_message_and_url_metadata() {
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
                address: Some("https://example.com/article".to_string()),
                title: Some("Example Article".to_string()),
            }),
        };

        let text = treechat_answer_search_text(&answer).expect("search text");

        assert!(text.contains("saved this"));
        assert!(text.contains("Example Article"));
        assert!(text.contains("https://example.com/article"));
    }
}
