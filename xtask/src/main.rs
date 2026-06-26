use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use toml_edit::{value, ArrayOfTables, DocumentMut};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return Err("missing xtask command".into());
    };

    match command.as_str() {
        "release" => release(args.collect()),
        "--help" | "-h" | "help" => {
            print_usage();
            Ok(())
        }
        other => Err(format!("unknown xtask command: {other}").into()),
    }
}

fn release(args: Vec<String>) -> Result<()> {
    let mut dry_run = false;
    let mut requested_version: Option<String> = None;

    for arg in args {
        match arg.as_str() {
            "--dry-run" => dry_run = true,
            "--help" | "-h" => {
                print_release_usage();
                return Ok(());
            }
            _ if requested_version.is_none() => requested_version = Some(arg),
            _ => {
                return Err(format!(
                    "unexpected argument {arg:?}; expected only an optional version and --dry-run"
                )
                .into());
            }
        }
    }

    let repo_root = repo_root()?;
    let cargo_toml_path = repo_root.join("Cargo.toml");
    let cargo_lock_path = repo_root.join("Cargo.lock");

    let cargo_toml = fs::read_to_string(&cargo_toml_path)?;
    let current_version = root_package_version(&cargo_toml)?;
    let next_version = match requested_version {
        Some(version) => validate_release_version(&version)?,
        None => next_patch_version(&current_version)?,
    };
    let tag = format!("v{next_version}");

    if git_tag_exists(&repo_root, &tag)? {
        return Err(format!("git tag {tag} already exists").into());
    }

    let updated_cargo_toml = update_root_package_version(&cargo_toml, &next_version)?;
    let updated_cargo_lock = if cargo_lock_path.exists() {
        let cargo_lock = fs::read_to_string(&cargo_lock_path)?;
        Some(update_lockfile_version(
            &cargo_lock,
            "historious",
            &next_version,
        )?)
    } else {
        None
    };

    if dry_run {
        println!("Current version: {current_version}");
        println!("Next version:    {next_version}");
        println!("Tag:             {tag}");
        println!();
        println!("Would update:");
        println!("  {}", cargo_toml_path.display());
        if cargo_lock_path.exists() {
            println!("  {}", cargo_lock_path.display());
        }
        println!();
        println!("Would run:");
        println!("  git add Cargo.toml Cargo.lock");
        println!("  git commit -F <release-message>");
        println!("  git tag -a {tag} -m \"Release {tag}\"");
        println!();
        println!("Push with:");
        println!("  git push origin HEAD");
        println!("  git push origin {tag}");
        return Ok(());
    }

    ensure_clean_tree(&repo_root)?;

    fs::write(&cargo_toml_path, updated_cargo_toml)?;
    if let Some(updated_cargo_lock) = updated_cargo_lock {
        fs::write(&cargo_lock_path, updated_cargo_lock)?;
    }

    let message_path = repo_root.join("target").join("release-message.txt");
    fs::create_dir_all(message_path.parent().expect("message path has parent"))?;
    fs::write(&message_path, release_commit_message(&tag))?;

    run_git(&repo_root, &["add", "Cargo.toml", "Cargo.lock"])?;
    run_git(
        &repo_root,
        &[
            "commit",
            "-F",
            path_str(&message_path, "release message path")?,
        ],
    )?;
    run_git(
        &repo_root,
        &["tag", "-a", &tag, "-m", &format!("Release {tag}")],
    )?;

    println!("Created release {tag}");
    println!();
    println!("Push with:");
    println!("  git push origin HEAD");
    println!("  git push origin {tag}");

    Ok(())
}

fn repo_root() -> Result<PathBuf> {
    if let Some(explicit_root) = env::var_os("HISTORIOUS_REPO_ROOT") {
        return Ok(PathBuf::from(explicit_root));
    }

    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(env::current_dir()?)
        .output()?;
    if !output.status.success() {
        return Err("could not resolve the repository root from git".into());
    }

    let root = String::from_utf8(output.stdout)?;
    let root = root.trim();
    if root.is_empty() {
        return Err("git returned an empty repository root".into());
    }
    Ok(PathBuf::from(root))
}

fn ensure_clean_tree(repo_root: &Path) -> Result<()> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(repo_root)
        .output()?;
    if !output.status.success() {
        return Err("could not inspect git status before releasing".into());
    }
    if !String::from_utf8_lossy(&output.stdout).trim().is_empty() {
        return Err(
            "git has tracked changes; commit or stash them before running the release script"
                .into(),
        );
    }
    Ok(())
}

fn root_package_version(cargo_toml: &str) -> Result<String> {
    let document = cargo_toml.parse::<DocumentMut>()?;
    document["package"]["version"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| "Cargo.toml package.version is missing or not a string".into())
}

fn validate_release_version(version: &str) -> Result<String> {
    let version = version.strip_prefix('v').unwrap_or(version);
    let parts = parse_release_version(version)?;
    Ok(format!("{}.{}.{}", parts.0, parts.1, parts.2))
}

fn next_patch_version(current_version: &str) -> Result<String> {
    let (major, minor, patch) = parse_release_version(current_version)?;
    Ok(format!("{}.{}.{}", major, minor, patch + 1))
}

fn parse_release_version(version: &str) -> Result<(u64, u64, u64)> {
    let mut pieces = version.split('.');
    let major = pieces
        .next()
        .ok_or_else(|| format!("invalid version {version:?}"))?
        .parse::<u64>()?;
    let minor = pieces
        .next()
        .ok_or_else(|| format!("invalid version {version:?}"))?
        .parse::<u64>()?;
    let patch = pieces
        .next()
        .ok_or_else(|| format!("invalid version {version:?}"))?
        .parse::<u64>()?;

    if pieces.next().is_some() {
        return Err(format!("invalid version {version:?}").into());
    }

    Ok((major, minor, patch))
}

fn git_tag_exists(repo_root: &Path, tag: &str) -> Result<bool> {
    let status = Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", tag])
        .current_dir(repo_root)
        .status()?;
    Ok(status.success())
}

fn update_root_package_version(cargo_toml: &str, next_version: &str) -> Result<String> {
    let mut document = cargo_toml.parse::<DocumentMut>()?;
    document["package"]["version"] = value(next_version);
    Ok(document.to_string())
}

fn update_lockfile_version(
    cargo_lock: &str,
    package_name: &str,
    next_version: &str,
) -> Result<String> {
    let mut document = cargo_lock.parse::<DocumentMut>()?;
    let packages = document["package"]
        .as_array_of_tables_mut()
        .ok_or("Cargo.lock package table is missing")?;
    update_package_version(packages, package_name, next_version)?;
    Ok(document.to_string())
}

fn update_package_version(
    packages: &mut ArrayOfTables,
    package_name: &str,
    next_version: &str,
) -> Result<()> {
    for package in packages.iter_mut() {
        if package["name"].as_str() == Some(package_name) {
            package["version"] = value(next_version);
            return Ok(());
        }
    }
    Err(format!("Cargo.lock does not contain a package entry for {package_name}").into())
}

fn release_commit_message(tag: &str) -> String {
    format!(
        "Release {tag} so GitHub can publish binaries\n\n\
- Bump Cargo package metadata for {tag}\n\
- Refresh the lockfile package entry for the released version\n\
- Leave an annotated git tag for the GitHub release workflow\n\n\
This prepares the local release commit and tag; pushing the tag publishes the GitHub release artifacts.\n"
    )
}

fn run_git(repo_root: &Path, args: &[&str]) -> Result<()> {
    let status = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("git command failed: git {}", args.join(" ")).into())
    }
}

fn path_str<'a>(path: &'a Path, label: &str) -> Result<&'a str> {
    path.to_str()
        .ok_or_else(|| format!("{label} is not valid UTF-8: {}", path.display()).into())
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  cargo run -p xtask -- release [VERSION] [--dry-run]");
}

fn print_release_usage() {
    eprintln!("Usage:");
    eprintln!("  cargo run -p xtask -- release [VERSION] [--dry-run]");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  make release-dry-run");
    eprintln!("  make release VERSION=0.2.0");
    eprintln!("  scripts/release.sh 0.2.0 --dry-run");
}
