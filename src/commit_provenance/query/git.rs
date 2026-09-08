use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};

#[derive(Debug, Clone)]
pub(super) struct GitFile {
    pub(super) root: PathBuf,
    pub(super) common_dir: PathBuf,
    pub(super) head: String,
    pub(super) file: PathBuf,
    pub(super) line_count: usize,
    pub(super) selected_start: usize,
    pub(super) selected_end: usize,
    pub(super) lines: Vec<GitLine>,
    pub(super) commits: BTreeMap<String, GitCommit>,
    pub(super) roots: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct GitLine {
    pub(super) line: usize,
    pub(super) original_line: usize,
    pub(super) original_path: String,
    pub(super) sha: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct GitCommit {
    pub(super) message: String,
    pub(super) subject: String,
    pub(super) author_time: Option<i64>,
}

#[derive(Debug, Clone)]
struct Repository {
    root: PathBuf,
    root_lexical: PathBuf,
    common_dir: PathBuf,
}

const REPOSITORY_ENV: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_QUARANTINE_PATH",
    "GIT_NAMESPACE",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_SYSTEM",
    "GIT_CONFIG_GLOBAL",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    "GIT_PREFIX",
    "GIT_TRACE",
    "GIT_TRACE_PERFORMANCE",
    "GIT_TRACE_SETUP",
    "GIT_TRACE2",
    "GIT_TRACE2_EVENT",
    "GIT_TRACE2_PERF",
];

/// Inspect the current worktree version of a tracked text file.
///
/// The blame output is parsed as bytes until pathname fields are decoded. This
/// keeps file contents and quoted Git paths separate, and makes malformed
/// pathname escapes an explicit failure instead of a lossy conversion.
pub(super) fn inspect(
    file: &Path,
    range: Option<(usize, usize)>,
    progress: &mut dyn FnMut(super::super::store::Progress),
) -> Result<GitFile> {
    progress(super::super::store::Progress {
        phase: "git-resolve",
        current: 0,
        total: 0,
        evidence: 0,
    });

    let current_dir = std::env::current_dir().context("reading the current directory")?;
    let current_dir_canonical = current_dir
        .canonicalize()
        .context("canonicalizing the current directory")?;
    let file_lexical = absolute_lexical(file, &current_dir);
    let lexical_metadata = fs::symlink_metadata(&file_lexical).with_context(|| {
        format!(
            "reading Git provenance input file {}",
            file_lexical.display()
        )
    })?;
    if lexical_metadata.file_type().is_symlink() {
        bail!(
            "Git provenance input is not a regular file: {}",
            file.display()
        );
    }
    let file_metadata = fs::metadata(&file_lexical).with_context(|| {
        format!(
            "reading Git provenance input file {}",
            file_lexical.display()
        )
    })?;
    if !file_metadata.is_file() {
        bail!(
            "Git provenance input is not a regular file: {}",
            file.display()
        );
    }
    let file_canonical = file_lexical
        .canonicalize()
        .with_context(|| format!("canonicalizing input file {}", file_lexical.display()))?;
    let invocation_dir = file_lexical
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(&current_dir)
        .to_path_buf();
    let repository = repository(&invocation_dir)?;
    if !file_canonical.starts_with(&repository.root) {
        bail!(
            "Git provenance input is outside the repository: {}",
            file.display()
        );
    }

    let relative = file_canonical
        .strip_prefix(&repository.root)
        .map_err(|_| anyhow::anyhow!("Git provenance input is outside the repository"))?;
    let relative = if relative.as_os_str().is_empty() {
        bail!(
            "Git provenance input is not a regular file: {}",
            file.display()
        );
    } else {
        relative.to_path_buf()
    };
    let relative_text = relative.to_str().ok_or_else(|| {
        anyhow::anyhow!(
            "Git provenance input path is not valid UTF-8: {}",
            file.display()
        )
    })?;
    progress(super::super::store::Progress {
        phase: "git-file",
        current: 0,
        total: 0,
        evidence: 0,
    });
    let tracked_path = tracked_path(&repository.root, relative_text)?;
    let file_bytes = fs::read(&file_lexical)
        .with_context(|| format!("reading Git provenance input {}", file_lexical.display()))?;
    if file_bytes.contains(&0) {
        bail!("Git provenance input is binary: {}", file.display());
    }
    let line_count = line_count(&file_bytes);
    let (selected_start, selected_end) = selected_range(range, line_count)?;
    progress(super::super::store::Progress {
        phase: "git-read",
        current: line_count,
        total: line_count,
        evidence: 0,
    });

    let head = head_commit(&repository.root)?;
    progress(super::super::store::Progress {
        phase: "git-roots",
        current: 0,
        total: 0,
        evidence: 0,
    });
    let roots = collect_roots(
        &file_lexical,
        &current_dir,
        &current_dir_canonical,
        &repository,
    )?;
    progress(super::super::store::Progress {
        phase: "git-roots",
        current: roots.len(),
        total: roots.len(),
        evidence: 0,
    });

    let selected_count = if selected_start == 0 {
        0
    } else {
        selected_end - selected_start + 1
    };
    let mut lines = Vec::new();
    let mut sha_set = BTreeSet::new();
    if line_count == 0 {
        progress(super::super::store::Progress {
            phase: "git-blame",
            current: 0,
            total: 0,
            evidence: 0,
        });
    } else {
        progress(super::super::store::Progress {
            phase: "git-blame",
            current: 0,
            total: selected_count,
            evidence: 0,
        });
        let blame = blame(
            &repository.root,
            &tracked_path,
            selected_start,
            selected_end,
        )?;
        let (parsed_lines, parsed_shas) =
            parse_blame(&blame, line_count, selected_start, selected_end)?;
        lines = parsed_lines;
        sha_set = parsed_shas;
        progress(super::super::store::Progress {
            phase: "git-blame",
            current: lines.len(),
            total: selected_count,
            evidence: 0,
        });
    }

    let commits = gather_commits(&repository.root, &sha_set, progress)?;
    let final_head = head_commit(&repository.root)?;
    if final_head != head {
        bail!("Git HEAD changed while inspecting {}", file.display());
    }
    progress(super::super::store::Progress {
        phase: "git-ready",
        current: lines.len(),
        total: selected_count,
        evidence: commits.len(),
    });

    Ok(GitFile {
        root: repository.root,
        common_dir: repository.common_dir,
        head,
        file: file_canonical,
        line_count,
        selected_start,
        selected_end,
        lines,
        commits,
        roots,
    })
}

/// Return the canonical shared Git directory for the repository containing cwd.
pub(super) fn common_dir(cwd: &Path) -> Result<PathBuf> {
    Ok(repository(cwd)?.common_dir)
}

/// Resolve a hexadecimal object prefix to one unambiguous commit.
pub(super) fn resolve_commit(root: &Path, prefix: &str) -> Result<Option<String>> {
    if !(4..=64).contains(&prefix.len()) || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    let prefix = prefix.to_ascii_lowercase();
    let output = run_git(root, ["rev-parse", &format!("--disambiguate={prefix}")])?;
    if !output.status.success() {
        bail!(
            "Git object lookup failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let text = std::str::from_utf8(&output.stdout).context("decoding Git object identities")?;
    let mut hashes = text.lines().filter(|line| !line.is_empty());
    let Some(hash) = hashes.next() else {
        return Ok(None);
    };
    if hashes.next().is_some() || !is_full_hash(hash) || !hash.starts_with(&prefix) {
        return Ok(None);
    }
    let kind = run_git(root, ["cat-file", "-t", hash])?;
    Ok((kind.status.success() && kind.stdout == b"commit\n").then(|| hash.to_string()))
}

fn repository(cwd: &Path) -> Result<Repository> {
    if !cwd.is_dir() {
        bail!(
            "Git repository lookup directory is not a directory: {}",
            cwd.display()
        );
    }
    let root_output = run_git(cwd, ["rev-parse", "--show-toplevel"])?;
    if !root_output.status.success() {
        bail!("not a Git repository: {}", cwd.display());
    }
    let root_raw = one_line(&root_output.stdout, "Git repository root")?;
    if root_raw.is_empty() {
        bail!("Git returned an empty repository root");
    }
    let root_lexical = absolute_lexical(Path::new(root_raw), cwd);
    let root = root_lexical
        .canonicalize()
        .with_context(|| format!("canonicalizing Git repository root {root_raw}"))?;
    if !root.is_dir() {
        bail!("Git repository root is not a directory: {}", root.display());
    }

    let common_output = run_git(
        cwd,
        ["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    let common_raw = if common_output.status.success() {
        one_line(&common_output.stdout, "Git common directory")?.to_owned()
    } else {
        let fallback = run_git(cwd, ["rev-parse", "--git-common-dir"])?;
        if !fallback.status.success() {
            bail!("unable to resolve Git common directory: {}", cwd.display());
        }
        one_line(&fallback.stdout, "Git common directory")?.to_owned()
    };
    let common = resolve_git_path(&common_raw, cwd, &root_lexical, "Git common directory")?;
    if !common.is_dir() {
        bail!(
            "Git common directory is not a directory: {}",
            common.display()
        );
    }

    Ok(Repository {
        root,
        root_lexical,
        common_dir: common,
    })
}

fn tracked_path(root: &Path, relative: &str) -> Result<String> {
    let pathspec = format!(":(literal){relative}");
    let output = run_git(
        root,
        [
            "ls-files",
            "--stage",
            "--full-name",
            "-z",
            "--",
            pathspec.as_str(),
        ],
    )?;
    if !output.status.success() {
        bail!(
            "unable to inspect the Git index for {}: {}",
            relative,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let mut found: Option<(String, u32, u32)> = None;
    for record in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or_else(|| anyhow::anyhow!("Git returned a malformed index record"))?;
        let fields = record[..tab]
            .split(|byte| *byte == b' ')
            .collect::<Vec<_>>();
        if fields.len() != 3 {
            bail!("Git returned a malformed index record");
        }
        let mode = std::str::from_utf8(fields[0])
            .ok()
            .and_then(|value| u32::from_str_radix(value, 8).ok())
            .ok_or_else(|| anyhow::anyhow!("Git returned an invalid index mode"))?;
        let stage = std::str::from_utf8(fields[2])
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| anyhow::anyhow!("Git returned an invalid index stage"))?;
        let path = String::from_utf8(record[tab + 1..].to_vec())
            .map_err(|_| anyhow::anyhow!("Git returned a non-UTF-8 tracked pathname"))?;
        if path == relative {
            if found.is_some() {
                bail!("Git index has multiple entries for {relative}");
            }
            found = Some((path, mode, stage));
        }
    }
    let Some((path, mode, stage)) = found else {
        bail!("Git provenance input is untracked: {relative}");
    };
    if stage != 0 {
        bail!("Git provenance input has unmerged index entries: {relative}");
    }
    if mode & 0o170000 != 0o100000 {
        bail!("Git provenance input is not a tracked regular file: {relative}");
    }
    Ok(path)
}

fn selected_range(range: Option<(usize, usize)>, line_count: usize) -> Result<(usize, usize)> {
    match range {
        None if line_count == 0 => Ok((0, 0)),
        None => Ok((1, line_count)),
        Some((start, end)) if start == 0 || end == 0 || start > end || end > line_count => {
            bail!("invalid Git provenance line range {start}..={end} for {line_count} lines")
        }
        Some((start, end)) => Ok((start, end)),
    }
}

fn line_count(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        0
    } else {
        bytes.iter().filter(|byte| **byte == b'\n').count()
            + if bytes.ends_with(b"\n") { 0 } else { 1 }
    }
}

fn head_commit(root: &Path) -> Result<String> {
    let output = run_git(
        root,
        [
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            "HEAD",
        ],
    )?;
    if !output.status.success() {
        bail!("Git repository has no readable HEAD: {}", root.display());
    }
    let hash = one_line(&output.stdout, "Git HEAD")?;
    if !is_full_hash(hash) {
        bail!("Git returned an invalid HEAD hash");
    }
    Ok(hash.to_ascii_lowercase())
}

fn blame(root: &Path, tracked_path: &str, start: usize, end: usize) -> Result<Vec<u8>> {
    let range = format!("{start},{end}");
    let output = run_git(
        root,
        [
            "blame",
            "--line-porcelain",
            "-L",
            range.as_str(),
            "--no-textconv",
            "--",
            tracked_path,
        ],
    )?;
    if !output.status.success() {
        bail!(
            "Git blame failed for {tracked_path}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

fn parse_blame(
    bytes: &[u8],
    line_count: usize,
    selected_start: usize,
    selected_end: usize,
) -> Result<(Vec<GitLine>, BTreeSet<String>)> {
    let mut cursor = 0usize;
    let mut expected_line = selected_start;
    let mut lines = Vec::new();
    let mut shas = BTreeSet::new();
    while let Some(header) = next_line(bytes, &mut cursor) {
        if header.is_empty() {
            bail!("Git blame returned an empty line where a header was expected");
        }
        let fields = header
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|field| !field.is_empty())
            .collect::<Vec<_>>();
        if fields.len() < 3 || fields.len() > 4 {
            bail!("Git blame returned a malformed line header");
        }
        let (sha, zero_sha) = parse_blame_hash(fields[0])?;
        let original_line = parse_usize(fields[1], "original line")?;
        let final_line = parse_usize(fields[2], "final line")?;
        let group_lines = if let Some(field) = fields.get(3) {
            parse_usize(field, "blame group length")?
        } else {
            1
        };
        if group_lines == 0 || final_line != expected_line {
            bail!("Git blame returned non-contiguous final line numbers");
        }
        let mut original_path = None;
        let first_content = loop {
            let metadata = next_line(bytes, &mut cursor)
                .ok_or_else(|| anyhow::anyhow!("Git blame ended before line content"))?;
            if metadata.first() == Some(&b'\t') {
                break metadata;
            }
            if let Some(path) = metadata.strip_prefix(b"filename ") {
                original_path = Some(decode_git_path(path)?);
            }
        };
        let original_path = original_path
            .ok_or_else(|| anyhow::anyhow!("Git blame omitted the original pathname"))?;
        if first_content.first() != Some(&b'\t') {
            bail!("Git blame returned malformed line content");
        }
        if final_line > line_count || final_line > selected_end {
            bail!("Git blame returned lines outside the selected range");
        }
        lines.push(GitLine {
            line: final_line,
            original_line,
            original_path,
            sha: (!zero_sha).then(|| sha.clone()),
        });
        if !zero_sha {
            shas.insert(sha);
        }
        expected_line = final_line
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Git blame line number overflow"))?;
    }
    if expected_line != selected_end.saturating_add(1) {
        bail!("Git blame did not cover every selected line");
    }
    Ok((lines, shas))
}

fn parse_blame_hash(field: &[u8]) -> Result<(String, bool)> {
    let field = field.strip_prefix(b"^").unwrap_or(field);
    if (field.len() != 40 && field.len() != 64) || !field.iter().all(u8::is_ascii_hexdigit) {
        bail!("Git blame returned an invalid object ID");
    }
    let hash = std::str::from_utf8(field)
        .map_err(|_| anyhow::anyhow!("Git blame returned a non-UTF-8 object ID"))?
        .to_ascii_lowercase();
    Ok((hash.clone(), field.iter().all(|byte| *byte == b'0')))
}

fn parse_usize(bytes: &[u8], name: &str) -> Result<usize> {
    let text = std::str::from_utf8(bytes).map_err(|_| anyhow::anyhow!("invalid Git {name}"))?;
    text.parse::<usize>()
        .map_err(|_| anyhow::anyhow!("invalid Git {name}: {text}"))
}

fn gather_commits(
    root: &Path,
    shas: &BTreeSet<String>,
    progress: &mut dyn FnMut(super::super::store::Progress),
) -> Result<BTreeMap<String, GitCommit>> {
    let total = shas.len();
    progress(super::super::store::Progress {
        phase: "git-commits",
        current: 0,
        total,
        evidence: 0,
    });
    if shas.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut command = git_command(root);
    command
        .arg("cat-file")
        .arg("--batch")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = spawn_git(command, "starting Git commit metadata lookup")?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("Git did not provide a commit metadata input"))?;
    let requested = shas.iter().cloned().collect::<Vec<_>>();
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        for sha in requested {
            stdin.write_all(sha.as_bytes())?;
            stdin.write_all(b"\n")?;
        }
        Ok(())
    });
    let output = child
        .wait_with_output()
        .context("waiting for Git commit metadata lookup")?;
    let writer_result = writer
        .join()
        .map_err(|_| anyhow::anyhow!("Git commit metadata writer panicked"))?;
    if !output.status.success() {
        bail!(
            "Git commit metadata lookup failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    writer_result.context("writing Git commit metadata requests")?;
    let commits = parse_batch_commits(&output.stdout, shas)?;
    progress(super::super::store::Progress {
        phase: "git-commits",
        current: commits.len(),
        total,
        evidence: commits.len(),
    });
    Ok(commits)
}

fn parse_batch_commits(
    bytes: &[u8],
    requested: &BTreeSet<String>,
) -> Result<BTreeMap<String, GitCommit>> {
    let mut cursor = 0usize;
    let mut commits = BTreeMap::new();
    for requested_sha in requested {
        let header = next_line(bytes, &mut cursor)
            .ok_or_else(|| anyhow::anyhow!("Git commit metadata output ended early"))?;
        let fields = header
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .collect::<Vec<_>>();
        if fields.len() == 2 && fields[1] == b"missing" {
            bail!("Git commit metadata is missing for {requested_sha}");
        }
        if fields.len() != 3 || fields[1] != b"commit" {
            bail!("Git returned a non-commit object for {requested_sha}");
        }
        let returned_sha = std::str::from_utf8(fields[0])
            .map_err(|_| anyhow::anyhow!("Git returned a non-UTF-8 commit hash"))?;
        if !is_full_hash(returned_sha) || !returned_sha.eq_ignore_ascii_case(requested_sha) {
            bail!("Git returned an unexpected commit hash for {requested_sha}");
        }
        let size_text = std::str::from_utf8(fields[2])
            .map_err(|_| anyhow::anyhow!("Git returned an invalid commit size"))?;
        let size = size_text
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("Git returned an invalid commit size"))?;
        let end = cursor
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("Git commit size overflow"))?;
        if end > bytes.len() {
            bail!("Git commit metadata output ended inside an object");
        }
        let object = &bytes[cursor..end];
        cursor = end;
        if !matches!(next_line(bytes, &mut cursor), Some(line) if line.is_empty()) {
            bail!("Git commit metadata output is malformed");
        }
        let (message, subject, author_time) = parse_commit_object(object)?;
        let sha = returned_sha.to_ascii_lowercase();
        commits.insert(
            sha,
            GitCommit {
                message,
                subject,
                author_time,
            },
        );
    }
    if cursor != bytes.len() {
        bail!("Git returned trailing commit metadata");
    }
    Ok(commits)
}

fn parse_commit_object(object: &[u8]) -> Result<(String, String, Option<i64>)> {
    let separator = object
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| anyhow::anyhow!("Git commit object has no message separator"))?;
    let headers = &object[..separator];
    let message = String::from_utf8(object[separator + 2..].to_vec())
        .map_err(|_| anyhow::anyhow!("Git commit message is not valid UTF-8"))?;
    let mut author_time = None;
    for header in headers.split(|byte| *byte == b'\n') {
        if let Some(author) = header.strip_prefix(b"author ") {
            let mut fields = author.rsplit(|byte| *byte == b' ');
            let _timezone = fields.next();
            if let Some(timestamp) = fields.next() {
                author_time = std::str::from_utf8(timestamp)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok());
            }
            break;
        }
    }
    let subject = message
        .split('\n')
        .next()
        .unwrap_or_default()
        .trim_end_matches('\r')
        .to_owned();
    Ok((message, subject, author_time))
}

fn collect_roots(
    file_lexical: &Path,
    current_dir: &Path,
    current_dir_canonical: &Path,
    repo: &Repository,
) -> Result<Vec<String>> {
    let mut roots = BTreeSet::new();
    add_root_alias(&mut roots, &repo.root, &repo.root)?;
    add_root_alias(&mut roots, &repo.root_lexical, &repo.root)?;
    let mut ancestor = file_lexical.parent();
    while let Some(path) = ancestor {
        if path.canonicalize().ok().as_deref() == Some(repo.root.as_path()) {
            add_root_alias(&mut roots, path, &repo.root)?;
        }
        if path.parent() == Some(path) {
            break;
        }
        ancestor = path.parent();
    }

    if let Some(pwd) = std::env::var_os("PWD") {
        let pwd = absolute_lexical(Path::new(&pwd), current_dir);
        if pwd.canonicalize().ok().as_deref() == Some(current_dir_canonical) {
            if let Ok(pwd_repository) = repository(&pwd) {
                if pwd_repository.common_dir == repo.common_dir && pwd_repository.root == repo.root
                {
                    add_root_alias(&mut roots, &pwd, &repo.root)?;
                    add_root_alias(&mut roots, &pwd_repository.root_lexical, &repo.root)?;
                    let mut pwd_ancestor = Some(pwd.as_path());
                    while let Some(path) = pwd_ancestor {
                        if path.canonicalize().ok().as_deref() == Some(repo.root.as_path()) {
                            add_root_alias(&mut roots, path, &repo.root)?;
                        }
                        if path.parent() == Some(path) {
                            break;
                        }
                        pwd_ancestor = path.parent();
                    }
                }
            }
        }
    }

    let worktrees = run_git(repo.root.as_path(), ["worktree", "list", "--porcelain"])?;
    if !worktrees.status.success() {
        bail!(
            "unable to list Git worktrees: {}",
            String::from_utf8_lossy(&worktrees.stderr).trim()
        );
    }
    for line in worktrees.stdout.split(|byte| *byte == b'\n') {
        let Some(raw_path) = line.strip_prefix(b"worktree ") else {
            continue;
        };
        if raw_path.is_empty() {
            bail!("Git returned an empty worktree root");
        }
        let path_text = decode_git_path(raw_path)?;
        let path = absolute_lexical(Path::new(&path_text), &repo.root);
        let Ok(canonical) = path.canonicalize() else {
            continue;
        };
        if !canonical.is_dir() {
            continue;
        }
        let Ok(worktree_repository) = repository(&path) else {
            continue;
        };
        if worktree_repository.common_dir == repo.common_dir
            && worktree_repository.root == canonical
        {
            add_root_alias(&mut roots, &path, &worktree_repository.root)?;
            add_root_alias(
                &mut roots,
                &worktree_repository.root_lexical,
                &worktree_repository.root,
            )?;
        }
    }
    Ok(roots.into_iter().collect())
}

fn add_root_alias(roots: &mut BTreeSet<String>, path: &Path, expected: &Path) -> Result<()> {
    if path.canonicalize().ok().as_deref() != Some(expected) {
        return Ok(());
    }
    let text = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Git root alias is not valid UTF-8"))?;
    roots.insert(text.to_owned());
    Ok(())
}

fn resolve_git_path(raw: &str, cwd: &Path, root: &Path, label: &str) -> Result<PathBuf> {
    let raw_path = Path::new(raw);
    let candidates = if raw_path.is_absolute() {
        vec![raw_path.to_path_buf()]
    } else {
        vec![cwd.join(raw_path), root.join(raw_path)]
    };
    let mut canonical = BTreeSet::new();
    for candidate in candidates {
        if let Ok(path) = candidate.canonicalize() {
            canonical.insert(path);
        }
    }
    match canonical.len() {
        1 => Ok(canonical.into_iter().next().expect("one canonical path")),
        0 => bail!("Git returned an invalid {label}: {raw}"),
        _ => bail!("Git returned an ambiguous {label}: {raw}"),
    }
}

fn decode_git_path(raw: &[u8]) -> Result<String> {
    if raw.first() != Some(&b'"') {
        return String::from_utf8(raw.to_vec())
            .map_err(|_| anyhow::anyhow!("Git returned a non-UTF-8 pathname"));
    }
    if raw.len() < 2 || raw.last() != Some(&b'"') {
        bail!("Git returned a malformed quoted pathname");
    }
    let mut decoded = Vec::with_capacity(raw.len().saturating_sub(2));
    let mut index = 1usize;
    while index + 1 < raw.len() {
        let byte = raw[index];
        index += 1;
        if byte != b'\\' {
            decoded.push(byte);
            continue;
        }
        let escaped = *raw
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("Git returned a truncated pathname escape"))?;
        index += 1;
        match escaped {
            b'a' => decoded.push(0x07),
            b'b' => decoded.push(0x08),
            b't' => decoded.push(b'\t'),
            b'n' => decoded.push(b'\n'),
            b'v' => decoded.push(0x0b),
            b'f' => decoded.push(0x0c),
            b'r' => decoded.push(b'\r'),
            b'\\' => decoded.push(b'\\'),
            b'"' => decoded.push(b'"'),
            b'x' => {
                let high = *raw
                    .get(index)
                    .filter(|byte| byte.is_ascii_hexdigit())
                    .ok_or_else(|| {
                        anyhow::anyhow!("Git returned an invalid hexadecimal pathname escape")
                    })?;
                let low = *raw
                    .get(index + 1)
                    .filter(|byte| byte.is_ascii_hexdigit())
                    .ok_or_else(|| {
                        anyhow::anyhow!("Git returned an invalid hexadecimal pathname escape")
                    })?;
                decoded.push((hex_value(high) << 4) | hex_value(low));
                index += 2;
            }
            b'0'..=b'7' => {
                let mut value = (escaped - b'0') as u8;
                let mut digits = 1;
                while digits < 3 {
                    let Some(next @ b'0'..=b'7') = raw.get(index).copied() else {
                        break;
                    };
                    value = value * 8 + (next - b'0');
                    index += 1;
                    digits += 1;
                }
                decoded.push(value);
            }
            _ => bail!("Git returned an unsupported pathname escape"),
        }
    }
    String::from_utf8(decoded).map_err(|_| anyhow::anyhow!("Git pathname is not valid UTF-8"))
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => 0,
    }
}

fn next_line<'a>(bytes: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    if *cursor >= bytes.len() {
        return None;
    }
    let start = *cursor;
    while *cursor < bytes.len() && bytes[*cursor] != b'\n' {
        *cursor += 1;
    }
    let end = *cursor;
    if *cursor < bytes.len() {
        *cursor += 1;
    }
    Some(&bytes[start..end])
}

fn one_line<'a>(bytes: &'a [u8], label: &str) -> Result<&'a str> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| anyhow::anyhow!("Git returned invalid {label}"))?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    let text = text.strip_suffix('\r').unwrap_or(text);
    if text.contains('\n') || text.contains('\r') {
        bail!("Git returned multiple lines for {label}");
    }
    Ok(text)
}

fn is_full_hash(hash: &str) -> bool {
    (hash.len() == 40 || hash.len() == 64) && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn absolute_lexical(path: &Path, cwd: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(std::path::MAIN_SEPARATOR.to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
}

fn run_git<I, S>(cwd: &Path, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = git_command(cwd);
    command.args(args);
    command.output().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            anyhow::anyhow!("Git is unavailable: {error}")
        } else {
            anyhow::anyhow!("unable to execute Git: {error}")
        }
    })
}

fn git_command(cwd: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(cwd)
        .arg("-c")
        .arg("core.fsmonitor=false")
        .arg("-c")
        .arg("core.pager=cat")
        .arg("-c")
        .arg("diff.external=")
        .arg("-c")
        .arg("interactive.diffFilter=")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_PAGER", "cat")
        .env("GIT_PAGER_IN_USE", "0")
        .env("GIT_EXTERNAL_DIFF", "")
        .env("GIT_DIFF_OPTS", "")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_COUNT", "0");
    for variable in REPOSITORY_ENV {
        command.env_remove(variable);
    }
    command
}

fn spawn_git(mut command: Command, context: &str) -> Result<Child> {
    command.spawn().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            anyhow::anyhow!("Git is unavailable while {context}: {error}")
        } else {
            anyhow::anyhow!("unable to execute Git while {context}: {error}")
        }
    })
}

#[cfg(test)]
mod file_provenance_git_tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .expect("git available");
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("git output utf8")
    }

    fn repo() -> TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        git(dir.path(), &["init", "-q"]);
        git(dir.path(), &["config", "user.name", "Test User"]);
        git(dir.path(), &["config", "user.email", "test@example.com"]);
        dir
    }

    fn commit(dir: &Path, message: &str) {
        git(dir, &["add", "--all"]);
        git(dir, &["commit", "-q", "-m", message]);
    }

    #[test]
    fn file_provenance_git_range_whole_file_and_uncommitted_lines() {
        let dir = repo();
        let path = dir.path().join("tracked file.txt");
        fs::write(&path, "one\ntwo\nthree\n").expect("write");
        commit(dir.path(), "initial");
        fs::write(&path, "one changed\ntwo\nthree\n").expect("write");
        commit(dir.path(), "second");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open")
            .write_all(b"uncommitted\n")
            .expect("append");

        let mut progress = Vec::new();
        let report = inspect(&path, None, &mut |value| progress.push(value)).expect("inspect");
        assert_eq!(report.line_count, 4);
        assert_eq!((report.selected_start, report.selected_end), (1, 4));
        assert_eq!(report.lines.len(), 4);
        assert!(report.lines.iter().any(|line| line.sha.is_none()));
        assert!(report.commits.len() >= 2);

        let mut no_progress = |_| {};
        let ranged = inspect(&path, Some((2, 3)), &mut no_progress).expect("range");
        assert_eq!(
            ranged
                .lines
                .iter()
                .map(|line| line.line)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!((ranged.selected_start, ranged.selected_end), (2, 3));
    }

    #[test]
    fn file_provenance_git_follows_rename_and_decodes_quoted_paths() {
        let dir = repo();
        let old = dir.path().join("old name \"quoted\".txt");
        let new = dir.path().join("new name \"quoted\".txt");
        fs::write(&old, "rename me\n").expect("write");
        commit(dir.path(), "before rename");
        git(
            dir.path(),
            &["mv", "old name \"quoted\".txt", "new name \"quoted\".txt"],
        );
        commit(dir.path(), "after rename");

        let mut progress = |_| {};
        let report = inspect(&new, None, &mut progress).expect("inspect rename");
        assert_eq!(report.lines.len(), 1);
        assert_eq!(report.lines[0].original_path, "old name \"quoted\".txt");
        assert_eq!(report.lines[0].original_line, 1);
    }

    #[test]
    fn file_provenance_git_rejects_invalid_ranges_untracked_and_binary_files() {
        let dir = repo();
        let tracked = dir.path().join("tracked.txt");
        fs::write(&tracked, "text\n").expect("write");
        commit(dir.path(), "tracked");
        let binary = dir.path().join("binary.dat");
        fs::write(&binary, b"text\0bytes").expect("write");
        git(dir.path(), &["add", "--", "binary.dat"]);
        git(dir.path(), &["commit", "-q", "-m", "binary"]);
        let untracked = dir.path().join("untracked.txt");
        fs::write(&untracked, "text\n").expect("write");

        let mut progress = |_| {};
        let error = inspect(&tracked, Some((0, 1)), &mut progress).expect_err("invalid range");
        assert!(error
            .to_string()
            .contains("invalid Git provenance line range"));
        let error = inspect(&untracked, None, &mut progress).expect_err("untracked");
        assert!(error.to_string().contains("untracked"));
        let error = inspect(&binary, None, &mut progress).expect_err("binary");
        assert!(error.to_string().contains("binary"));
    }

    #[test]
    fn file_provenance_git_resolve_commit_rejects_non_hex_and_missing_prefixes() {
        let dir = repo();
        let path = dir.path().join("file.txt");
        fs::write(&path, "text\n").expect("write");
        commit(dir.path(), "commit");
        assert_eq!(
            resolve_commit(dir.path(), "not-hex").expect("invalid prefix"),
            None
        );
        assert_eq!(
            resolve_commit(dir.path(), "deadbeef").expect("missing prefix"),
            None
        );
        let head = git(dir.path(), &["rev-parse", "HEAD"]);
        let head = head.trim();
        assert_eq!(
            resolve_commit(dir.path(), head)
                .expect("full prefix")
                .as_deref(),
            Some(head)
        );
        assert_eq!(
            resolve_commit(dir.path(), &head[..7])
                .expect("short prefix")
                .as_deref(),
            Some(head)
        );
        git(dir.path(), &["branch", "deadbeef", "HEAD"]);
        assert_eq!(
            resolve_commit(dir.path(), "deadbeef").expect("hex branch is not an object"),
            None
        );
    }
}
