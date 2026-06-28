use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DEFAULT_UPDATE_API_ROOT: &str = "https://api.github.com";
const DEFAULT_UPDATE_REPO: &str = "nikvdp/historious";
const DEFAULT_RELEASES_PAGE: &str = "https://github.com/nikvdp/historious/releases";

#[derive(Debug, Deserialize)]
struct ReleaseResponse {
    tag_name: String,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone)]
pub struct SelfUpdateOptions {
    pub check: bool,
    pub force: bool,
    pub tag: Option<String>,
    pub path: Option<PathBuf>,
    pub portable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelfUpdateResult {
    pub local_version: String,
    pub remote_version: String,
    pub already_updated: bool,
    pub binary_path: PathBuf,
    pub release_url: String,
    pub asset_name: String,
    pub updated_path: Option<PathBuf>,
}

pub fn run_self_update(
    options: SelfUpdateOptions,
    mut status: impl FnMut(&str),
) -> Result<SelfUpdateResult> {
    status("Checking for updates...");
    let api_root =
        env::var("HISTO_UPDATE_API_ROOT").unwrap_or_else(|_| DEFAULT_UPDATE_API_ROOT.to_string());
    let repo = env::var("HISTO_UPDATE_REPO").unwrap_or_else(|_| DEFAULT_UPDATE_REPO.to_string());
    let release = fetch_release(&api_root, &repo, options.tag.as_deref())?;
    let remote_version = normalize_tag(&release.tag_name);
    let local_version = normalize_tag(env!("CARGO_PKG_VERSION"));
    let asset_name = release_asset_name(options.portable)?.to_string();

    status("Locating installed binary...");
    let explicit_path = options.path.is_some();
    let binary_path = resolve_binary_path(options.path)?;
    let release_url = release_page_url(&repo, &remote_version);

    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == asset_name)
        .ok_or_else(|| {
            anyhow!("release {remote_version} does not contain the expected asset {asset_name:?}")
        })?;

    let already_updated = remote_version == local_version;
    if options.check || (already_updated && !options.force) {
        return Ok(SelfUpdateResult {
            local_version,
            remote_version: remote_version.clone(),
            already_updated,
            binary_path,
            release_url,
            asset_name,
            updated_path: None,
        });
    }

    let checksum_asset = release
        .assets
        .iter()
        .find(|asset| asset.name == "checksums.txt" || asset.name == "SHA256SUMS");
    let checksums = if let Some(checksum_asset) = checksum_asset {
        status("Downloading checksums...");
        Some(download_text(&checksum_asset.browser_download_url)?)
    } else {
        None
    };

    status("Downloading update...");
    let bytes = download_bytes(&asset.browser_download_url)?;

    if let Some(checksums) = checksums {
        status("Verifying checksum...");
        verify_checksum(&checksums, &asset_name, &bytes)?;
    }

    status("Installing update...");
    let staged_binary = write_staged_binary(&asset_name, &bytes)?;
    let install_result = install_staged_binary(&staged_binary, &binary_path, explicit_path);
    let _ = fs::remove_file(&staged_binary);
    install_result?;

    Ok(SelfUpdateResult {
        local_version,
        remote_version,
        already_updated: false,
        release_url,
        asset_name,
        updated_path: Some(binary_path.clone()),
        binary_path,
    })
}

fn fetch_release(api_root: &str, repo: &str, tag: Option<&str>) -> Result<ReleaseResponse> {
    let tag = tag.map(normalize_tag);
    let url = match tag {
        Some(tag) => format!("{api_root}/repos/{repo}/releases/tags/{tag}"),
        None => format!("{api_root}/repos/{repo}/releases/latest"),
    };
    let response = github_request(&url).call()?;
    response
        .into_json()
        .with_context(|| format!("could not parse GitHub release response from {url}"))
}

fn download_text(url: &str) -> Result<String> {
    let response = github_request(url).call()?;
    response
        .into_string()
        .with_context(|| format!("could not read text response from {url}"))
}

fn download_bytes(url: &str) -> Result<Vec<u8>> {
    let response = github_request(url).call()?;
    let mut reader = response.into_reader();
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("could not read release asset from {url}"))?;
    Ok(bytes)
}

fn github_request(url: &str) -> ureq::Request {
    let request = ureq::get(url)
        .set("Accept", "application/vnd.github+json")
        .set("User-Agent", concat!("histo/", env!("CARGO_PKG_VERSION")));

    match env::var("GITHUB_TOKEN").or_else(|_| env::var("GH_TOKEN")) {
        Ok(token) if !token.trim().is_empty() => {
            request.set("Authorization", &format!("Bearer {token}"))
        }
        _ => request,
    }
}

fn write_staged_binary(asset_name: &str, bytes: &[u8]) -> Result<PathBuf> {
    let mut temp = tempfile::Builder::new()
        .prefix(&format!("{asset_name}-"))
        .tempfile()?;
    temp.write_all(bytes)?;
    temp.flush()?;

    #[cfg(unix)]
    {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755))?;
    }

    let path = temp.into_temp_path();
    path.keep()
        .context("could not persist the downloaded update")
}

fn install_staged_binary(
    staged_binary: &PathBuf,
    binary_path: &PathBuf,
    explicit_path: bool,
) -> Result<()> {
    if !explicit_path {
        return self_replace::self_replace(staged_binary)
            .with_context(|| format!("could not replace {}", binary_path.display()));
    }

    fs::copy(staged_binary, binary_path)
        .with_context(|| format!("could not replace {}", binary_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(binary_path, fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

fn resolve_binary_path(path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = path {
        let path = if path.is_absolute() {
            path
        } else {
            env::current_dir()?.join(path)
        };
        if !path.is_file() {
            bail!("target binary is not a regular file: {}", path.display());
        }
        return Ok(path);
    }

    env::current_exe().context("could not resolve the current histo executable path")
}

fn release_asset_name(portable: bool) -> Result<&'static str> {
    match (env::consts::OS, env::consts::ARCH) {
        ("linux", "x86_64") if portable || cfg!(target_env = "musl") => {
            Ok("histo-linux-x86_64-musl")
        }
        ("linux", "x86_64") => Ok("histo-linux-x86_64-gnu"),
        ("linux", "aarch64") if portable || cfg!(target_env = "musl") => {
            Ok("histo-linux-aarch64-musl")
        }
        ("linux", "aarch64") => Ok("histo-linux-aarch64-gnu"),
        ("macos", "aarch64") => Ok("histo-macos-aarch64"),
        ("windows", "x86_64") => Ok("histo-windows-x86_64.exe"),
        (os, arch) => bail!("self-update is not configured for {os}/{arch}"),
    }
}

fn verify_checksum(checksums: &str, asset_name: &str, bytes: &[u8]) -> Result<()> {
    let Some(expected) = parse_checksum_file(checksums, asset_name) else {
        bail!("release checksums are missing an entry for {asset_name}");
    };
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected {
        bail!("checksum mismatch for {asset_name}");
    }
    Ok(())
}

fn parse_checksum_file(checksums: &str, asset_name: &str) -> Option<String> {
    checksums.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() {
            return None;
        }
        let mut parts = line.split_whitespace();
        let hash = parts.next()?.trim();
        let name = parts.next()?.trim_start_matches('*');
        if hash.len() == 64 && name == asset_name {
            Some(hash.to_ascii_lowercase())
        } else {
            None
        }
    })
}

fn normalize_tag(tag: &str) -> String {
    let tag = tag.trim();
    if tag.starts_with('v') {
        tag.to_string()
    } else {
        format!("v{tag}")
    }
}

fn release_page_url(repo: &str, tag: &str) -> String {
    if repo == DEFAULT_UPDATE_REPO {
        format!("{DEFAULT_RELEASES_PAGE}/tag/{tag}")
    } else {
        format!("https://github.com/{repo}/releases/tag/{tag}")
    }
}
