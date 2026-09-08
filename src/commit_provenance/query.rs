use super::{store, CommitEvidence};
use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod git;

const CANDIDATE_LIMIT: usize = 64;
const MAX_MESSAGE_TOKENS: usize = 64;

/// The evidence class used to rank a current blamed commit.
///
/// The scoped variants are deliberately weaker than their repository-verified
/// counterparts. They are retained only when the archived cwd cannot be
/// inspected anymore, while the machine and lexical repository scope still
/// provide a conservative bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EvidenceStrength {
    ExactSha,
    ExactMessage,
    WhitespaceNormalizedMessage,
    FuzzyMessage,
    ScopedSha,
    ScopedExactMessage,
    ScopedWhitespaceNormalizedMessage,
}

impl EvidenceStrength {
    fn rank(self) -> u8 {
        match self {
            Self::ExactSha => 7,
            Self::ScopedSha => 6,
            Self::ExactMessage => 5,
            Self::WhitespaceNormalizedMessage => 4,
            Self::ScopedExactMessage => 3,
            Self::ScopedWhitespaceNormalizedMessage => 2,
            Self::FuzzyMessage => 1,
        }
    }

    fn verified(self) -> bool {
        matches!(
            self,
            Self::ExactSha
                | Self::ExactMessage
                | Self::WhitespaceNormalizedMessage
                | Self::FuzzyMessage
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LineRange {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EvidenceCitation {
    pub(crate) evidence_id: String,
    pub(crate) session_id: String,
    pub(crate) source_kind: String,
    pub(crate) event_id: String,
    pub(crate) result_event_id: String,
    pub(crate) call_id: String,
    pub(crate) message_event_id: Option<String>,
    pub(crate) message_result_event_id: Option<String>,
    pub(crate) observed_sha: String,
    pub(crate) cwd: Option<String>,
    pub(crate) occurred_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CommitMatch {
    pub(crate) current_sha: String,
    pub(crate) observed_sha: String,
    pub(crate) observed_shas: Vec<String>,
    pub(crate) resolved_observed_sha: Option<String>,
    pub(crate) resolved_observed_shas: Vec<String>,
    pub(crate) current_message: String,
    pub(crate) current_subject: String,
    pub(crate) observed_message: Option<String>,
    pub(crate) ranges: Vec<LineRange>,
    pub(crate) original_paths: Vec<String>,
    pub(crate) strength: EvidenceStrength,
    pub(crate) repository_verified: bool,
    pub(crate) reason: String,
    pub(crate) shared_words: Vec<String>,
    pub(crate) citations: Vec<EvidenceCitation>,
    pub(crate) author_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionMatch {
    pub(crate) session_id: String,
    pub(crate) strongest: EvidenceStrength,
    pub(crate) covered_lines: usize,
    pub(crate) ambiguous_lines: usize,
    pub(crate) ranges: Vec<LineRange>,
    pub(crate) ambiguous_ranges: Vec<LineRange>,
    pub(crate) commits: Vec<CommitMatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct CoverageSummary {
    pub(crate) selected_lines: usize,
    pub(crate) exact_sha_lines: usize,
    pub(crate) exact_message_lines: usize,
    pub(crate) whitespace_normalized_message_lines: usize,
    pub(crate) fuzzy_message_lines: usize,
    pub(crate) scoped_sha_lines: usize,
    pub(crate) scoped_exact_message_lines: usize,
    pub(crate) scoped_whitespace_normalized_message_lines: usize,
    pub(crate) ambiguous_lines: usize,
    pub(crate) unresolved_lines: usize,
    pub(crate) uncommitted_lines: usize,
}

impl CoverageSummary {
    fn add(&mut self, coverage: &LineClassification) {
        match coverage {
            LineClassification::Uncommitted => self.uncommitted_lines += 1,
            LineClassification::Unresolved { .. } => self.unresolved_lines += 1,
            LineClassification::Matched { strength, .. } => match strength {
                EvidenceStrength::ExactSha => self.exact_sha_lines += 1,
                EvidenceStrength::ExactMessage => self.exact_message_lines += 1,
                EvidenceStrength::WhitespaceNormalizedMessage => {
                    self.whitespace_normalized_message_lines += 1
                }
                EvidenceStrength::FuzzyMessage => self.fuzzy_message_lines += 1,
                EvidenceStrength::ScopedSha => self.scoped_sha_lines += 1,
                EvidenceStrength::ScopedExactMessage => self.scoped_exact_message_lines += 1,
                EvidenceStrength::ScopedWhitespaceNormalizedMessage => {
                    self.scoped_whitespace_normalized_message_lines += 1
                }
            },
            LineClassification::Ambiguous { .. } => self.ambiguous_lines += 1,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AmbiguousRange {
    pub(crate) range: LineRange,
    pub(crate) current_sha: String,
    pub(crate) original_paths: Vec<String>,
    pub(crate) session_ids: Vec<String>,
    pub(crate) strength: EvidenceStrength,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UnresolvedKind {
    Uncommitted,
    NoEvidence,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct UnresolvedRange {
    pub(crate) range: LineRange,
    pub(crate) current_sha: Option<String>,
    pub(crate) original_paths: Vec<String>,
    pub(crate) kind: UnresolvedKind,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BlameReport {
    pub(crate) repository: String,
    pub(crate) file: String,
    pub(crate) head: String,
    pub(crate) selected_start: usize,
    pub(crate) selected_end: usize,
    pub(crate) total_lines: usize,
    pub(crate) projection: store::ProjectionStatus,
    pub(crate) coverage: CoverageSummary,
    pub(crate) sessions: Vec<SessionMatch>,
    pub(crate) ambiguous: Vec<AmbiguousRange>,
    pub(crate) unresolved: Vec<UnresolvedRange>,
    pub(crate) warnings: Vec<String>,
    pub(crate) candidates_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepositoryScope {
    Verified,
    Unavailable,
    Rejected,
}

#[derive(Debug, Clone)]
struct ClassifiedCandidate {
    record: CommitEvidence,
    strength: EvidenceStrength,
    reason: String,
    shared_words: Vec<String>,
    resolved_sha: Option<String>,
}

#[derive(Debug, Clone)]
struct CommitAccumulator {
    current_sha: String,
    current_message: String,
    current_subject: String,
    author_time: Option<i64>,
    ranges: Vec<LineRange>,
    original_paths: Vec<String>,
    strength: EvidenceStrength,
    repository_verified: bool,
    observed_shas: BTreeSet<String>,
    resolved_observed_shas: BTreeSet<String>,
    observed_messages: BTreeSet<String>,
    reasons: BTreeSet<String>,
    shared_words: BTreeSet<String>,
    citations: BTreeMap<String, EvidenceCitation>,
}

#[derive(Debug, Clone, Default)]
struct SessionAccumulator {
    commits: BTreeMap<String, CommitAccumulator>,
    lines: BTreeSet<usize>,
}

#[derive(Debug, Clone)]
enum LineClassification {
    Uncommitted,
    Unresolved {
        current_sha: String,
        reason: String,
    },
    Matched {
        strength: EvidenceStrength,
    },
    Ambiguous {
        current_sha: String,
        session_ids: Vec<String>,
        strength: EvidenceStrength,
    },
}

/// Resolve selected file lines to archived commit evidence.
///
/// The database is opened read-only and status plus candidate lookups happen
/// in one deferred transaction, so a concurrent writer cannot make the report
/// mix projection generations.
pub(crate) fn query(
    db_path: &Path,
    machine_id: &str,
    file: &Path,
    range: Option<(usize, usize)>,
    progress: impl FnMut(store::Progress),
) -> Result<BlameReport> {
    let mut progress = progress;
    progress(store::Progress {
        phase: "starting",
        current: 0,
        total: 0,
        evidence: 0,
    });

    progress(store::Progress {
        phase: "snapshot",
        current: 0,
        total: 1,
        evidence: 0,
    });
    if !db_path.exists() {
        bail!(
            "commit provenance snapshot is unavailable: database {} is missing; run `histo update` first",
            db_path.display()
        );
    }
    let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| {
            format!(
                "opening provenance database {} read-only",
                db_path.display()
            )
        })?;
    conn.busy_timeout(Duration::from_secs(4))?;
    let tx = conn.unchecked_transaction()?;
    let status = store::status(&tx)?;
    if !status.has_snapshot || status.state == store::ProjectionState::Missing {
        bail!("commit provenance snapshot is unavailable; run `histo update` first to build it");
    }
    let mut git_file = git::inspect(file, range, &mut progress)?;
    let line_map = unique_lines(std::mem::take(&mut git_file.lines));
    let selected_lines = line_map.len();
    let blamed = blamed_shas(&line_map);
    let total_commits = blamed.len();

    let mut warnings = Vec::new();
    if status.state == store::ProjectionState::Stale {
        warnings.push(
            "commit provenance snapshot is stale; results use the last committed snapshot; run `histo update` to refresh"
                .to_string(),
        );
    }
    progress(store::Progress {
        phase: "snapshot",
        current: 1,
        total: 1,
        evidence: status.evidence_count,
    });

    let mut cwd_cache = HashMap::<String, Option<PathBuf>>::new();
    let mut resolved_sha_cache = HashMap::<String, Option<String>>::new();
    let mut matches_by_sha = BTreeMap::<String, Vec<ClassifiedCandidate>>::new();
    let mut candidates_truncated = false;
    let roots = repository_roots(&git_file);
    let mut accepted_evidence = 0usize;

    progress(store::Progress {
        phase: "matching",
        current: 0,
        total: total_commits,
        evidence: 0,
    });
    for (commit_index, current_sha) in blamed.iter().enumerate() {
        let current_commit = find_commit(&git_file.commits, current_sha);
        let message = current_commit
            .map(|commit| commit.message.as_str())
            .unwrap_or_default();
        let page = store::candidates(
            &tx,
            machine_id,
            &roots,
            current_sha,
            message,
            CANDIDATE_LIMIT,
        )?;
        candidates_truncated |= page.truncated;
        if page.truncated {
            warnings.push(format!(
                "candidate evidence for blamed commit {current_sha} was truncated at {CANDIDATE_LIMIT} records"
            ));
        }

        let mut accepted = Vec::new();
        if let Some(current_commit) = current_commit {
            for record in page.records {
                let scope =
                    repository_scope(record.cwd.as_deref(), &git_file.common_dir, &mut cwd_cache);
                if let Some(classified) = classify_candidate(
                    record,
                    current_sha,
                    current_commit,
                    scope,
                    &git_file.root,
                    &mut resolved_sha_cache,
                ) {
                    accepted.push(classified);
                }
            }
        }
        if let Some(strongest) = accepted
            .iter()
            .map(|candidate| candidate.strength)
            .max_by_key(|strength| strength.rank())
        {
            accepted.retain(|candidate| candidate.strength == strongest);
        }
        accepted_evidence += accepted.len();
        matches_by_sha.insert((*current_sha).to_string(), accepted);
        progress(store::Progress {
            phase: "matching",
            current: commit_index + 1,
            total: total_commits,
            evidence: accepted_evidence,
        });
    }
    tx.commit()?;

    let classifications = classify_lines(&line_map, &matches_by_sha);
    let mut coverage = CoverageSummary {
        selected_lines,
        exact_sha_lines: 0,
        exact_message_lines: 0,
        whitespace_normalized_message_lines: 0,
        fuzzy_message_lines: 0,
        scoped_sha_lines: 0,
        scoped_exact_message_lines: 0,
        scoped_whitespace_normalized_message_lines: 0,
        ambiguous_lines: 0,
        unresolved_lines: 0,
        uncommitted_lines: 0,
    };
    // Keep the total and every category disjoint: ambiguous lines are counted
    // once in `ambiguous_lines`, never once for each competing session.
    progress(store::Progress {
        phase: "grouping",
        current: 0,
        total: selected_lines,
        evidence: accepted_evidence,
    });
    for (index, classification) in classifications.values().enumerate() {
        coverage.add(classification);
        progress(store::Progress {
            phase: "grouping",
            current: index + 1,
            total: selected_lines,
            evidence: accepted_evidence,
        });
    }

    let mut sessions = build_sessions(
        &line_map,
        &git_file.commits,
        &matches_by_sha,
        &classifications,
    );
    let ambiguous = build_ambiguous_ranges(&line_map, &classifications);
    let unresolved = build_unresolved_ranges(&line_map, &classifications);
    sessions.sort_by(|left, right| {
        right
            .strongest
            .rank()
            .cmp(&left.strongest.rank())
            .then_with(|| right.covered_lines.cmp(&left.covered_lines))
            .then_with(|| left.session_id.cmp(&right.session_id))
    });

    progress(store::Progress {
        phase: "complete",
        current: selected_lines,
        total: selected_lines,
        evidence: accepted_evidence,
    });

    Ok(BlameReport {
        repository: git_file.root.to_string_lossy().into_owned(),
        file: git_file.file.to_string_lossy().into_owned(),
        head: git_file.head,
        selected_start: git_file.selected_start,
        selected_end: git_file.selected_end,
        total_lines: git_file.line_count,
        projection: status,
        coverage,
        sessions,
        ambiguous,
        unresolved,
        warnings,
        candidates_truncated,
    })
}

fn repository_roots(git_file: &git::GitFile) -> Vec<String> {
    if !git_file.roots.is_empty() {
        return git_file.roots.clone();
    }
    vec![git_file.root.to_string_lossy().into_owned()]
}

fn unique_lines(lines: Vec<git::GitLine>) -> BTreeMap<usize, git::GitLine> {
    lines.into_iter().map(|line| (line.line, line)).collect()
}

fn blamed_shas(lines: &BTreeMap<usize, git::GitLine>) -> BTreeSet<&str> {
    lines
        .values()
        .filter_map(|line| line.sha.as_deref())
        .collect()
}

fn blamed_lines(lines: &BTreeMap<usize, git::GitLine>) -> BTreeMap<String, Vec<git::GitLine>> {
    let mut groups = BTreeMap::<String, Vec<git::GitLine>>::new();
    for line in lines.values() {
        if let Some(sha) = &line.sha {
            groups.entry(sha.clone()).or_default().push(line.clone());
        }
    }
    groups
}

fn find_commit<'a>(
    commits: &'a BTreeMap<String, git::GitCommit>,
    sha: &str,
) -> Option<&'a git::GitCommit> {
    commits.get(sha).or_else(|| {
        commits
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(sha))
            .map(|(_, commit)| commit)
    })
}

fn repository_scope(
    cwd: Option<&str>,
    target_common_dir: &Path,
    cache: &mut HashMap<String, Option<PathBuf>>,
) -> RepositoryScope {
    let Some(cwd) = cwd.map(str::trim).filter(|cwd| !cwd.is_empty()) else {
        return RepositoryScope::Rejected;
    };
    let cached = cache
        .entry(cwd.to_string())
        .or_insert_with(|| git::common_dir(Path::new(cwd)).ok());
    match cached.as_deref() {
        Some(common_dir) if common_dir == target_common_dir => RepositoryScope::Verified,
        Some(_) => RepositoryScope::Rejected,
        None => RepositoryScope::Unavailable,
    }
}

fn classify_candidate(
    record: CommitEvidence,
    current_sha: &str,
    current_commit: &git::GitCommit,
    scope: RepositoryScope,
    root: &Path,
    resolved_sha_cache: &mut HashMap<String, Option<String>>,
) -> Option<ClassifiedCandidate> {
    if scope == RepositoryScope::Rejected {
        return None;
    }
    let observed_sha = record.sha.trim();
    let resolved_sha = resolve_observed_sha(observed_sha, root, resolved_sha_cache);
    let sha_match = resolved_sha
        .as_deref()
        .is_some_and(|resolved| resolved.eq_ignore_ascii_case(current_sha));
    let candidate_message = record.message.as_deref();
    let current_message = current_commit.message.as_str();
    let exact_message = !current_message.is_empty()
        && candidate_message
            .filter(|message| !message.is_empty())
            .is_some_and(|message| message == current_message);
    let normalized_message = !exact_message
        && !current_message.is_empty()
        && candidate_message
            .filter(|message| !message.is_empty())
            .is_some_and(|message| {
                normalize_message(message) == normalize_message(current_message)
            });
    let shared_words = if !sha_match && !exact_message && !normalized_message {
        candidate_message
            .or(record.subject.as_deref())
            .filter(|message| !message.is_empty())
            .map(|message| shared_message_words(current_message, message))
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let fuzzy_message = !shared_words.is_empty();

    let (strength, reason) = match scope {
        RepositoryScope::Verified => {
            if sha_match {
                (
                    EvidenceStrength::ExactSha,
                    "recorded SHA/prefix resolves unambiguously to the current blamed commit"
                        .to_string(),
                )
            } else if exact_message {
                (
                    EvidenceStrength::ExactMessage,
                    "full commit message matches exactly; a differing SHA may reflect a rewrite, but does not prove lineage"
                        .to_string(),
                )
            } else if normalized_message {
                (
                    EvidenceStrength::WhitespaceNormalizedMessage,
                    "full commit message matches after whitespace normalization; a differing SHA may reflect a rewrite, but does not prove lineage"
                        .to_string(),
                )
            } else if fuzzy_message {
                (
                    EvidenceStrength::FuzzyMessage,
                    format!(
                        "{} shares words: {}",
                        if candidate_message.is_some() {
                            "repository-verified full-message candidate"
                        } else {
                            "repository-verified subject-only candidate; full recorded message unavailable"
                        },
                        shared_words.join(", ")
                    ),
                )
            } else {
                return None;
            }
        }
        RepositoryScope::Unavailable => {
            // A deleted/unavailable cwd remains useful only with a SHA or full
            // message signal. It is never reported as repository-verified.
            if sha_match {
                (
                    EvidenceStrength::ScopedSha,
                    "repository directory unavailable; identity not revalidated; machine/root-scoped SHA resolves to the current blamed commit"
                        .to_string(),
                )
            } else if exact_message {
                (
                    EvidenceStrength::ScopedExactMessage,
                    "repository directory unavailable; identity not revalidated; machine/root-scoped full message matches exactly"
                        .to_string(),
                )
            } else if normalized_message {
                (
                    EvidenceStrength::ScopedWhitespaceNormalizedMessage,
                    "repository directory unavailable; identity not revalidated; machine/root-scoped full message matches after whitespace normalization"
                        .to_string(),
                )
            } else {
                return None;
            }
        }
        RepositoryScope::Rejected => return None,
    };

    Some(ClassifiedCandidate {
        record,
        strength,
        reason,
        shared_words,
        resolved_sha,
    })
}

fn resolve_observed_sha(
    observed_sha: &str,
    root: &Path,
    cache: &mut HashMap<String, Option<String>>,
) -> Option<String> {
    let key = observed_sha.to_ascii_lowercase();
    if key.is_empty() {
        return None;
    }
    if !cache.contains_key(&key) {
        let resolved = git::resolve_commit(root, observed_sha)
            .ok()
            .flatten()
            .map(|sha| sha.to_ascii_lowercase());
        cache.insert(key.clone(), resolved);
    }
    cache.get(&key).cloned().flatten()
}

fn normalize_message(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn message_tokens(message: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();
    for character in message.chars() {
        if character.is_ascii_alphanumeric() {
            current.push(character.to_ascii_lowercase());
        } else if !current.is_empty() {
            tokens.insert(std::mem::take(&mut current));
            if tokens.len() == MAX_MESSAGE_TOKENS {
                return tokens;
            }
        }
    }
    if !current.is_empty() && tokens.len() < MAX_MESSAGE_TOKENS {
        tokens.insert(current);
    }
    tokens
}

fn shared_message_words(current: &str, candidate: &str) -> Vec<String> {
    let current_tokens = message_tokens(current);
    let candidate_tokens = message_tokens(candidate);
    if current_tokens.is_empty() || candidate_tokens.is_empty() {
        return Vec::new();
    }
    let shared = current_tokens
        .intersection(&candidate_tokens)
        .cloned()
        .collect::<Vec<_>>();
    let minimum = current_tokens.len().min(candidate_tokens.len());
    let distinctive = shared.iter().any(|word| word.len() >= 4);
    let enough = if minimum <= 1 {
        shared.len() == 1 && distinctive
    } else {
        shared.len() >= 2 && shared.len() * 2 >= minimum && distinctive
    };
    enough.then_some(shared).unwrap_or_default()
}

fn classify_lines(
    lines: &BTreeMap<usize, git::GitLine>,
    matches_by_sha: &BTreeMap<String, Vec<ClassifiedCandidate>>,
) -> BTreeMap<usize, LineClassification> {
    let mut result = BTreeMap::new();
    for (line_number, line) in lines {
        let Some(raw_sha) = line
            .sha
            .as_deref()
            .map(str::trim)
            .filter(|sha| !sha.is_empty())
        else {
            result.insert(*line_number, LineClassification::Uncommitted);
            continue;
        };
        let current_sha = raw_sha.to_ascii_lowercase();
        let Some(matches) = matches_by_sha
            .get(&current_sha)
            .filter(|matches| !matches.is_empty())
        else {
            result.insert(
                *line_number,
                LineClassification::Unresolved {
                    current_sha,
                    reason:
                        "no repository-scoped commit evidence matched the current blamed commit"
                            .to_string(),
                },
            );
            continue;
        };
        let session_ids = matches
            .iter()
            .map(|candidate| candidate.record.session_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let strength = matches[0].strength;
        let classification = if session_ids.len() > 1 {
            LineClassification::Ambiguous {
                current_sha,
                session_ids,
                strength,
            }
        } else {
            LineClassification::Matched { strength }
        };
        result.insert(*line_number, classification);
    }
    result
}

fn build_sessions(
    lines: &BTreeMap<usize, git::GitLine>,
    commits: &BTreeMap<String, git::GitCommit>,
    matches_by_sha: &BTreeMap<String, Vec<ClassifiedCandidate>>,
    classifications: &BTreeMap<usize, LineClassification>,
) -> Vec<SessionMatch> {
    let mut accumulators = BTreeMap::<String, SessionAccumulator>::new();
    for (current_sha, current_lines) in blamed_lines(lines) {
        let Some(matches) = matches_by_sha
            .get(&current_sha)
            .filter(|matches| !matches.is_empty())
        else {
            continue;
        };
        let Some(current_commit) = find_commit(commits, &current_sha) else {
            continue;
        };
        let mut by_session = BTreeMap::<String, Vec<&ClassifiedCandidate>>::new();
        for candidate in matches {
            by_session
                .entry(candidate.record.session_id.clone())
                .or_default()
                .push(candidate);
        }
        for (session_id, candidates) in by_session {
            let session = accumulators.entry(session_id).or_default();
            session
                .lines
                .extend(current_lines.iter().map(|line| line.line));
            let first = candidates[0];
            let commit = session
                .commits
                .entry(current_sha.clone())
                .or_insert_with(|| CommitAccumulator {
                    current_sha: current_sha.clone(),
                    current_message: current_commit.message.clone(),
                    current_subject: current_commit.subject.clone(),
                    author_time: current_commit.author_time,
                    ranges: ranges_for_lines(&current_lines),
                    original_paths: original_paths(&current_lines),
                    strength: first.strength,
                    repository_verified: first.strength.verified(),
                    observed_shas: BTreeSet::new(),
                    resolved_observed_shas: BTreeSet::new(),
                    observed_messages: BTreeSet::new(),
                    reasons: BTreeSet::new(),
                    shared_words: BTreeSet::new(),
                    citations: BTreeMap::new(),
                });
            for candidate in candidates {
                commit.observed_shas.insert(candidate.record.sha.clone());
                if let Some(resolved_sha) = &candidate.resolved_sha {
                    commit.resolved_observed_shas.insert(resolved_sha.clone());
                }
                if let Some(message) = &candidate.record.message {
                    commit.observed_messages.insert(message.clone());
                }
                commit.reasons.insert(candidate.reason.clone());
                commit
                    .shared_words
                    .extend(candidate.shared_words.iter().cloned());
                commit.repository_verified &= candidate.strength.verified();
                commit
                    .citations
                    .insert(candidate.record.id.clone(), citation(&candidate.record));
            }
        }
    }

    accumulators
        .into_iter()
        .map(|(session_id, accumulator)| {
            let lines = accumulator
                .lines
                .iter()
                .filter_map(|line| lines.get(line).cloned())
                .collect::<Vec<_>>();
            let ambiguous_lines = accumulator
                .lines
                .iter()
                .filter(|line| {
                    matches!(
                        classifications.get(line),
                        Some(LineClassification::Ambiguous { session_ids, .. })
                            if session_ids.contains(&session_id)
                    )
                })
                .copied()
                .collect::<BTreeSet<_>>();
            let commits = accumulator
                .commits
                .into_values()
                .map(commit_match)
                .collect::<Vec<_>>();
            let strongest = commits
                .iter()
                .map(|commit| commit.strength)
                .max_by_key(|strength| strength.rank())
                .expect("session has at least one accepted commit");
            SessionMatch {
                session_id,
                strongest,
                covered_lines: accumulator.lines.len(),
                ambiguous_lines: ambiguous_lines.len(),
                ranges: ranges_for_lines(&lines),
                ambiguous_ranges: ranges_for_line_numbers(&ambiguous_lines),
                commits,
            }
        })
        .collect()
}

fn citation(record: &CommitEvidence) -> EvidenceCitation {
    EvidenceCitation {
        evidence_id: record.id.clone(),
        session_id: record.session_id.clone(),
        source_kind: record.source_kind.clone(),
        event_id: record.event_id.clone(),
        result_event_id: record.result_event_id.clone(),
        call_id: record.call_id.clone(),
        message_event_id: record.message_event_id.clone(),
        message_result_event_id: record.message_result_event_id.clone(),
        observed_sha: record.sha.clone(),
        cwd: record.cwd.clone(),
        occurred_at: record.occurred_at,
    }
}

fn commit_match(accumulator: CommitAccumulator) -> CommitMatch {
    let observed_shas = accumulator.observed_shas.into_iter().collect::<Vec<_>>();
    let resolved_observed_shas = accumulator
        .resolved_observed_shas
        .into_iter()
        .collect::<Vec<_>>();
    let observed_sha = observed_shas.first().cloned().unwrap_or_default();
    CommitMatch {
        current_sha: accumulator.current_sha,
        observed_sha,
        observed_shas,
        resolved_observed_sha: resolved_observed_shas.first().cloned(),
        resolved_observed_shas,
        current_message: accumulator.current_message,
        current_subject: accumulator.current_subject,
        observed_message: accumulator.observed_messages.into_iter().next(),
        ranges: accumulator.ranges,
        original_paths: accumulator.original_paths,
        strength: accumulator.strength,
        repository_verified: accumulator.repository_verified,
        reason: accumulator
            .reasons
            .into_iter()
            .collect::<Vec<_>>()
            .join("; "),
        shared_words: accumulator.shared_words.into_iter().collect(),
        citations: accumulator.citations.into_values().collect(),
        author_time: accumulator.author_time,
    }
}

fn build_ambiguous_ranges(
    lines: &BTreeMap<usize, git::GitLine>,
    classifications: &BTreeMap<usize, LineClassification>,
) -> Vec<AmbiguousRange> {
    let mut result: Vec<AmbiguousRange> = Vec::new();
    for (line_number, classification) in classifications {
        let LineClassification::Ambiguous {
            current_sha,
            session_ids,
            strength,
        } = classification
        else {
            continue;
        };
        let paths = lines
            .get(line_number)
            .map(|line| vec![line.original_path.clone()])
            .unwrap_or_default();
        if let Some(previous) = result.last_mut() {
            if previous.range.end + 1 == *line_number
                && previous.current_sha == *current_sha
                && previous.session_ids == *session_ids
                && previous.strength == *strength
            {
                previous.range.end = *line_number;
                extend_unique(&mut previous.original_paths, paths);
                continue;
            }
        }
        result.push(AmbiguousRange {
            range: LineRange {
                start: *line_number,
                end: *line_number,
            },
            current_sha: current_sha.clone(),
            original_paths: paths,
            session_ids: session_ids.clone(),
            strength: *strength,
        });
    }
    result
}

fn build_unresolved_ranges(
    lines: &BTreeMap<usize, git::GitLine>,
    classifications: &BTreeMap<usize, LineClassification>,
) -> Vec<UnresolvedRange> {
    let mut result: Vec<UnresolvedRange> = Vec::new();
    for (line_number, classification) in classifications {
        let (current_sha, kind, reason) = match classification {
            LineClassification::Uncommitted => (
                None,
                UnresolvedKind::Uncommitted,
                "line has no committed blame; working-tree changes are unresolved".to_string(),
            ),
            LineClassification::Unresolved {
                current_sha,
                reason,
            } => (
                Some(current_sha.clone()),
                UnresolvedKind::NoEvidence,
                reason.clone(),
            ),
            _ => continue,
        };
        let paths = lines
            .get(line_number)
            .map(|line| vec![line.original_path.clone()])
            .unwrap_or_default();
        if let Some(previous) = result.last_mut() {
            if previous.range.end + 1 == *line_number
                && previous.current_sha == current_sha
                && previous.kind == kind
                && previous.reason == reason
            {
                previous.range.end = *line_number;
                extend_unique(&mut previous.original_paths, paths);
                continue;
            }
        }
        result.push(UnresolvedRange {
            range: LineRange {
                start: *line_number,
                end: *line_number,
            },
            current_sha,
            original_paths: paths,
            kind,
            reason,
        });
    }
    result
}

fn ranges_for_lines(lines: &[git::GitLine]) -> Vec<LineRange> {
    let numbers = lines.iter().map(|line| line.line).collect::<BTreeSet<_>>();
    ranges_for_line_numbers(&numbers)
}

fn ranges_for_line_numbers(numbers: &BTreeSet<usize>) -> Vec<LineRange> {
    let mut ranges: Vec<LineRange> = Vec::new();
    for number in numbers {
        if let Some(previous) = ranges.last_mut() {
            if previous.end + 1 == *number {
                previous.end = *number;
                continue;
            }
        }
        ranges.push(LineRange {
            start: *number,
            end: *number,
        });
    }
    ranges
}

fn original_paths(lines: &[git::GitLine]) -> Vec<String> {
    let mut paths = lines
        .iter()
        .map(|line| line.original_path.clone())
        .collect::<BTreeSet<_>>();
    paths.retain(|path| !path.is_empty());
    paths.into_iter().collect()
}

fn extend_unique(target: &mut Vec<String>, values: Vec<String>) {
    for value in values {
        if !value.is_empty() && !target.contains(&value) {
            target.push(value);
        }
    }
    target.sort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{ArchiveRecord, EventRecord, SessionRecord, SourceRecord};
    use crate::storage::Store;
    use chrono::Utc;
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tempfile::TempDir;

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("git output is UTF-8")
            .trim()
            .to_string()
    }

    fn init_repo() -> (TempDir, PathBuf) {
        let temp = tempfile::tempdir().expect("tempdir");
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).expect("repo directory");
        git(&repo, &["init", "-q"]);
        git(&repo, &["config", "user.email", "fixture@example.test"]);
        git(&repo, &["config", "user.name", "Fixture"]);
        (temp, repo)
    }

    fn commit(repo: &Path, message: &str, content: &str) -> String {
        fs::write(repo.join("src.txt"), content).expect("write fixture");
        git(repo, &["add", "--", "src.txt"]);
        git(repo, &["commit", "-q", "-m", message]);
        git(repo, &["rev-parse", "HEAD"])
    }

    fn seed_session(store: &Store, repo: &Path, session_id: &str, commits: &[(&str, &str)]) {
        let source_id = format!("source-{session_id}");
        let cwd = repo.to_string_lossy().into_owned();
        let now = Utc::now();
        let source = SourceRecord {
            id: source_id.clone(),
            kind: "omp".to_string(),
            identity: source_id.clone(),
            path: None,
            first_seen_at: now,
            updated_at: now,
            hash: format!("hash-{source_id}"),
        };
        let session = SessionRecord {
            id: session_id.to_string(),
            source_id: source_id.clone(),
            machine_id: "fixture-machine".to_string(),
            source_kind: "omp".to_string(),
            external_id: session_id.to_string(),
            title: None,
            status: "closed".to_string(),
            started_at: Some(now),
            updated_at: Some(now),
            metadata: json!({"cwd": cwd.as_str()}),
            hash: format!("hash-{session_id}"),
        };
        let mut records = vec![
            ArchiveRecord::Source(source),
            ArchiveRecord::Session(session),
        ];
        for (index, (sha, message)) in commits.iter().enumerate() {
            let call_id = format!("{session_id}-call-{index}");
            let call_event_id = format!("{session_id}-call-event-{index}");
            let result_event_id = format!("{session_id}-result-event-{index}");
            let call = json!({
                "type": "toolCall",
                "id": call_id.as_str(),
                "name": "bash",
                "arguments": {
                    "command": format!("git commit -m '{message}'"),
                    "cwd": cwd.as_str(),
                }
            });
            let result = json!({
                "role": "toolResult",
                "toolCallId": call_id,
                "isError": false,
                "content": [{
                    "type": "text",
                    "text": format!("[main {sha}] {message}\n 1 file changed")
                }]
            });
            records.push(ArchiveRecord::Event(EventRecord {
                id: call_event_id.clone(),
                session_id: session_id.to_string(),
                source_id: source_id.clone(),
                machine_id: "fixture-machine".to_string(),
                source_kind: "omp".to_string(),
                ordinal: (index * 2) as i64,
                event_type: "message".to_string(),
                role: None,
                content: call.to_string(),
                raw_artifact_hash: None,
                occurred_at: Some(now),
                metadata: json!({}),
                hash: format!("hash-{call_event_id}"),
            }));
            records.push(ArchiveRecord::Event(EventRecord {
                id: result_event_id.clone(),
                session_id: session_id.to_string(),
                source_id: source_id.clone(),
                machine_id: "fixture-machine".to_string(),
                source_kind: "omp".to_string(),
                ordinal: (index * 2 + 1) as i64,
                event_type: "message".to_string(),
                role: None,
                content: result.to_string(),
                raw_artifact_hash: None,
                occurred_at: Some(now),
                metadata: json!({}),
                hash: format!("hash-{result_event_id}"),
            }));
        }
        store
            .import_records(&records)
            .expect("import fixture events");
    }

    fn refresh(store: &Store) {
        store::maintain(store, |_| {}, || false).expect("refresh commit evidence");
    }

    #[test]
    fn file_provenance_groups_commits_and_counts_ambiguous_uncommitted_lines() {
        let (temp, repo) = init_repo();
        let first_sha = commit(&repo, "Add alpha lines", "alpha\nbeta\n");
        let second_sha = commit(&repo, "Add gamma line", "alpha\nbeta\ngamma\n");
        let store = Store::open(&temp.path().join("data")).expect("open fixture store");
        seed_session(
            &store,
            &repo,
            "session-a",
            &[
                (&first_sha, "Add alpha lines"),
                (&second_sha, "Add gamma line"),
            ],
        );
        refresh(&store);

        let file = repo.join("src.txt");
        let full = query(store.db_path(), "fixture-machine", &file, None, |_| {})
            .expect("query full file");
        assert_eq!(full.coverage.selected_lines, 3);
        assert_eq!(full.coverage.exact_sha_lines, 3);
        assert_eq!(full.sessions.len(), 1);
        assert_eq!(full.sessions[0].covered_lines, 3);
        assert_eq!(full.sessions[0].commits.len(), 2);
        assert_eq!(full.sessions[0].commits[0].citations.len(), 1);

        let ranged = query(
            store.db_path(),
            "fixture-machine",
            &file,
            Some((2, 3)),
            |_| {},
        )
        .expect("query selected range");
        assert_eq!(ranged.coverage.selected_lines, 2);
        assert_eq!(ranged.sessions[0].covered_lines, 2);
        assert_eq!(
            ranged.sessions[0].ranges,
            vec![LineRange { start: 2, end: 3 }]
        );

        fs::write(&file, "alpha\nbeta\ngamma\nworking\n").expect("write uncommitted line");
        let uncommitted = query(store.db_path(), "fixture-machine", &file, None, |_| {})
            .expect("query uncommitted file");
        assert_eq!(uncommitted.coverage.uncommitted_lines, 1);
        assert!(uncommitted
            .unresolved
            .iter()
            .any(|range| range.kind == UnresolvedKind::Uncommitted));
        seed_session(
            &store,
            &repo,
            "session-b",
            &[(&first_sha, "Add alpha lines")],
        );
        refresh(&store);
        fs::write(&file, "alpha\nbeta\ngamma\nunrecorded\n").expect("write unknown commit");
        git(&repo, &["add", "--", "src.txt"]);
        git(&repo, &["commit", "-q", "-m", "Unrecorded change"]);
        let partial = query(store.db_path(), "fixture-machine", &file, None, |_| {})
            .expect("query partial file");
        assert_eq!(partial.coverage.selected_lines, 4);
        assert_eq!(partial.coverage.ambiguous_lines, 2);
        assert_eq!(partial.coverage.exact_sha_lines, 1);
        assert_eq!(partial.coverage.unresolved_lines, 1);
        assert_eq!(
            partial.coverage.ambiguous_lines
                + partial.coverage.exact_sha_lines
                + partial.coverage.unresolved_lines,
            partial.coverage.selected_lines
        );
        assert_eq!(partial.sessions.len(), 2);
        assert_eq!(partial.sessions[0].session_id, "session-a");
        assert_eq!(partial.sessions[0].covered_lines, 3);
        assert_eq!(partial.ambiguous.len(), 1);
        assert_eq!(
            partial.ambiguous[0].session_ids,
            vec!["session-a".to_string(), "session-b".to_string()]
        );
        assert!(partial
            .unresolved
            .iter()
            .any(|range| range.kind == UnresolvedKind::NoEvidence));
    }

    #[test]
    fn file_provenance_actual_rebase_uses_message_fallback() {
        let (temp, repo) = init_repo();
        let base_sha = commit(&repo, "Base file", "base\n");
        let target_sha = commit(&repo, "Implement parser", "parser\n");
        let store = Store::open(&temp.path().join("data")).expect("open fixture store");
        seed_session(
            &store,
            &repo,
            "session-rebase",
            &[(&target_sha, "Implement parser")],
        );
        refresh(&store);
        let file = repo.join("src.txt");
        let before = query(store.db_path(), "fixture-machine", &file, None, |_| {})
            .expect("query before rebase");
        assert_eq!(
            before.sessions[0].commits[0].strength,
            EvidenceStrength::ExactSha
        );

        git(
            &repo,
            &["checkout", "-q", "-b", "rewritten-base", &base_sha],
        );
        fs::write(repo.join("unrelated.txt"), "new base\n").expect("write rebased base");
        git(&repo, &["add", "--", "unrelated.txt"]);
        git(&repo, &["commit", "-q", "-m", "New base"]);
        let new_base_sha = git(&repo, &["rev-parse", "HEAD"]);
        git(&repo, &["checkout", "-q", "-b", "rebased", &target_sha]);
        git(&repo, &["rebase", "-q", "--onto", &new_base_sha, &base_sha]);
        let rebased_sha = git(&repo, &["rev-parse", "HEAD"]);
        assert_ne!(rebased_sha, target_sha);

        let after = query(store.db_path(), "fixture-machine", &file, None, |_| {})
            .expect("query after rebase");
        let commit_match = &after.sessions[0].commits[0];
        assert_eq!(commit_match.current_sha, rebased_sha);
        assert_eq!(commit_match.observed_sha, target_sha);
        assert_eq!(
            commit_match.strength,
            EvidenceStrength::WhitespaceNormalizedMessage
        );
        assert!(commit_match.repository_verified);
        assert!(commit_match.reason.contains("whitespace normalization"));
        assert_eq!(after.coverage.whitespace_normalized_message_lines, 1);
        store
            .with_conn(|conn| {
                let text: String = conn.query_row(
                "SELECT content FROM events WHERE session_id = 'session-rebase' AND ordinal = 0",
                [], |row| row.get(0),
            )?;
                let mut call: serde_json::Value = serde_json::from_str(&text)?;
                call["arguments"]["command"] = json!("git commit -F /tmp/not-retained");
                conn.execute(
                "UPDATE events SET content = ? WHERE session_id = 'session-rebase' AND ordinal = 0",
                [call.to_string()],
            )?;
                Ok(())
            })
            .expect("remove full message evidence");
        refresh(&store);
        let partial = query(store.db_path(), "fixture-machine", &file, None, |_| {}).unwrap();
        assert_eq!(partial.coverage.fuzzy_message_lines, 1);
        assert_eq!(
            partial.sessions[0].commits[0].strength,
            EvidenceStrength::FuzzyMessage
        );
        assert!(partial.sessions[0].commits[0].observed_message.is_none());
    }

    #[test]
    fn file_provenance_missing_snapshot_guidance_and_stale_warning() {
        let (temp, repo) = init_repo();
        let sha = commit(&repo, "Snapshot fixture", "line\n");
        let file = repo.join("src.txt");
        let missing_db = temp.path().join("missing").join("historious.db");
        let error = query(&missing_db, "fixture-machine", &file, None, |_| {})
            .expect_err("missing snapshot should fail");
        assert!(error.to_string().contains("histo update"));

        let store = Store::open(&temp.path().join("data")).expect("open fixture store");
        seed_session(
            &store,
            &repo,
            "session-ready",
            &[(&sha, "Snapshot fixture")],
        );
        refresh(&store);
        seed_session(
            &store,
            &repo,
            "session-stale-only",
            &[(&sha, "Snapshot fixture")],
        );
        let report = query(store.db_path(), "fixture-machine", &file, None, |_| {})
            .expect("stale snapshot should still serve");
        assert_eq!(report.sessions.len(), 1);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("snapshot is stale")));
    }

    #[test]
    fn file_provenance_rename_keeps_git_original_path() {
        let (temp, repo) = init_repo();
        fs::write(repo.join("old.txt"), "original\n").expect("write original file");
        git(&repo, &["add", "--", "old.txt"]);
        git(&repo, &["commit", "-q", "-m", "Add original file"]);
        let original_sha = git(&repo, &["rev-parse", "HEAD"]);
        git(&repo, &["mv", "old.txt", "new.txt"]);
        git(&repo, &["commit", "-q", "-m", "Rename file"]);
        let store = Store::open(&temp.path().join("data")).expect("open fixture store");
        seed_session(
            &store,
            &repo,
            "session-rename",
            &[(&original_sha, "Add original file")],
        );
        refresh(&store);
        let report = query(
            store.db_path(),
            "fixture-machine",
            &repo.join("new.txt"),
            Some((1, 1)),
            |_| {},
        )
        .expect("query renamed file");
        assert_eq!(report.sessions.len(), 1);
        assert_eq!(
            report.sessions[0].commits[0].original_paths,
            vec!["old.txt".to_string()]
        );
    }

    #[test]
    fn file_provenance_historical_sha_outranks_verified_message_similarity() {
        let (temp, repo) = init_repo();
        let sha = commit(&repo, "Preserve indexed context", "line\n");
        git(
            &repo,
            &[
                "commit",
                "--allow-empty",
                "-q",
                "-m",
                "Preserve indexed history",
            ],
        );
        let distractor = git(&repo, &["rev-parse", "HEAD"]);
        let store = Store::open(&temp.path().join("data")).unwrap();
        seed_session(
            &store,
            &repo.join("deleted-worktree"),
            "historical",
            &[(&sha, "Preserve indexed context")],
        );
        seed_session(
            &store,
            &repo,
            "similar",
            &[(&distractor, "Preserve indexed history")],
        );
        refresh(&store);
        let report = query(
            store.db_path(),
            "fixture-machine",
            &repo.join("src.txt"),
            None,
            |_| {},
        )
        .unwrap();
        assert_eq!(report.sessions.len(), 1);
        assert_eq!(report.sessions[0].session_id, "historical");
        assert_eq!(report.coverage.scoped_sha_lines, 1);
        assert!(!report.sessions[0].commits[0].repository_verified);
    }
}
