//! The checkers the checks shell out to, at pinned versions.
//!
//! `cargo xtask ci tools` installs them under `target/tools/bin`, and every
//! check runs with that directory first on `PATH`. One definition, so a laptop
//! needs no packages and a runner needs no setup step: the versions live here.
//! An absent checker is a failure and never a skip — the same rule the lint's
//! own `deps` check follows, because a check that passes when its checker is
//! missing is a check nobody would notice had died.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "this module's output is its product: status on stdout, findings on stderr"
)]

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};

/// A checker, where it comes from, and the version it has to report.
struct Tool {
    /// The binary's name, as `PATH` and the checks both spell it.
    name: &'static str,
    /// The exact version every run is checked against.
    version: &'static str,
    /// One sentence: what the checks use it for.
    what: &'static str,
    /// How it gets onto `PATH`.
    how: Install,
}

/// How a checker gets onto `PATH`.
enum Install {
    /// A binary in a GitHub release, tagged `v` + the pinned version.
    Release {
        repo: &'static str,
        /// The asset for this machine, or `None` where none is published.
        asset: fn(&str) -> Option<String>,
        unpack: Unpack,
    },
    /// A crate, built from source by `cargo install` at the pinned version.
    Cargo,
}

/// What a downloaded asset needs before it is a binary.
#[derive(Clone, Copy)]
enum Unpack {
    /// The asset is the binary.
    Bare,
    /// The asset is a tarball holding it, one `tar -xf` away.
    TarGz,
}

/// Every checker a check may name.
const TOOLS: &[Tool] = &[
    Tool {
        name: "buf",
        version: "1.74.0",
        what: "lints the wire contract under `proto/`",
        how: Install::Release {
            repo: "bufbuild/buf",
            asset: buf_asset,
            unpack: Unpack::Bare,
        },
    },
    Tool {
        name: "cargo-machete",
        version: "0.9.2",
        what: "finds a dependency no source file names",
        how: Install::Cargo,
    },
    Tool {
        name: "shellcheck",
        version: "0.11.0",
        what: "reads `install.sh`, which reaches users through `curl | sh`",
        how: Install::Release {
            repo: "koalaman/shellcheck",
            asset: shellcheck_asset,
            unpack: Unpack::TarGz,
        },
    },
];

/// buf's asset for this machine, or `None` where buf publishes none.
fn buf_asset(_version: &str) -> Option<String> {
    let asset = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "buf-Linux-x86_64",
        ("linux", "aarch64") => "buf-Linux-aarch64",
        ("macos", "aarch64") => "buf-Darwin-arm64",
        ("macos", "x86_64") => "buf-Darwin-x86_64",
        _ => return None,
    };
    Some(asset.to_string())
}

/// `ShellCheck`'s asset for this machine, or `None` where none is published.
fn shellcheck_asset(version: &str) -> Option<String> {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "linux.x86_64",
        ("linux", "aarch64") => "linux.aarch64",
        ("macos", "aarch64") => "darwin.aarch64",
        ("macos", "x86_64") => "darwin.x86_64",
        _ => return None,
    };
    Some(format!("shellcheck-v{version}.{platform}.tar.gz"))
}

/// Where the checkers are installed: inside `target`, so it is disposable.
pub(crate) fn dir(root: &Path) -> PathBuf {
    root.join("target").join("tools")
}

/// The `PATH` every check runs with: the checkers first, then the caller's.
pub(crate) fn search_path(root: &Path) -> OsString {
    let mut path = dir(root).join("bin").into_os_string();
    if let Some(existing) = std::env::var_os("PATH") {
        path.push(separator());
        path.push(existing);
    }
    path
}

/// The list separator, which Windows spells differently.
fn separator() -> &'static str {
    if cfg!(windows) { ";" } else { ":" }
}

/// A cache key naming every pinned version, so bumping one busts the cache.
pub(crate) fn cache_key() -> String {
    let pins: Vec<String> = TOOLS
        .iter()
        .map(|tool| format!("{}-{}", tool.name, tool.version))
        .collect();
    format!("checkers-{}-{}", pins.join("_"), std::env::consts::ARCH)
}

/// The named checkers that are not installed at the version pinned here.
pub(crate) fn absent(root: &Path, names: &[&str]) -> Vec<String> {
    let mut missing: Vec<String> = names
        .iter()
        .filter(|name| !TOOLS.iter().any(|tool| tool.name == **name))
        .map(|name| format!("{name} (`cargo xtask ci tools` pins no such checker)"))
        .collect();
    missing.extend(names.iter().filter_map(|name| {
        let tool = TOOLS.iter().find(|tool| tool.name == *name)?;
        (!installed(root, tool)).then(|| format!("{} {}", tool.name, tool.version))
    }));
    missing
}

/// True when the checker answers with the version pinned here.
///
/// The local copy is preferred over `PATH`, because a check does the same: an
/// outdated system-wide binary must not read as the pinned one.
fn installed(root: &Path, tool: &Tool) -> bool {
    let local = dir(root).join("bin").join(tool.name);
    let program = if local.exists() {
        local.as_os_str()
    } else {
        OsStr::new(tool.name)
    };
    version_of(program).is_some_and(|found| found.contains(tool.version))
}

/// The version a checker reports, if it runs at all.
fn version_of(program: &OsStr) -> Option<String> {
    let output = Command::new(program).arg("--version").output().ok()?;
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    output.status.success().then_some(text)
}

/// Install every absent checker, or with `check_only` only report them.
pub(crate) fn run(root: &Path, check_only: bool) -> Result<()> {
    let mut missing = Vec::new();

    for tool in TOOLS {
        if installed(root, tool) {
            println!("ok      {} {}", tool.name, tool.version);
            continue;
        }
        if check_only {
            report_missing(tool);
            missing.push(tool.name);
            continue;
        }

        println!("install {} {}", tool.name, tool.version);
        match install(root, tool).and_then(|()| verified(root, tool)) {
            Ok(()) => println!("ok      {} {}", tool.name, tool.version),
            Err(err) => {
                eprintln!("failed  {} {}: {err:#}", tool.name, tool.version);
                report_missing(tool);
                missing.push(tool.name);
            }
        }
    }

    if missing.is_empty() {
        println!("checkers: ok");
        return Ok(());
    }
    bail!(
        "{} checker(s) missing: {}",
        missing.len(),
        missing.join(", ")
    )
}

/// The refusal to continue with a checker that is not the pinned one.
fn verified(root: &Path, tool: &Tool) -> Result<()> {
    if installed(root, tool) {
        return Ok(());
    }
    Err(anyhow!(
        "no {} {} under {}",
        tool.name,
        tool.version,
        dir(root).display()
    ))
}

/// Say what a missing checker is and how to get it.
fn report_missing(tool: &Tool) {
    eprintln!(
        "missing {} {}\n         {} — run `cargo xtask ci tools`",
        tool.name, tool.version, tool.what
    );
}

/// Put one checker under `target/tools/bin` at the pinned version.
fn install(root: &Path, tool: &Tool) -> Result<()> {
    let bin = dir(root).join("bin");
    std::fs::create_dir_all(&bin).with_context(|| format!("creating {}", bin.display()))?;

    match &tool.how {
        Install::Release {
            repo,
            asset,
            unpack,
        } => {
            let Some(asset) = asset(tool.version) else {
                bail!(
                    "no {} release asset for {} {}",
                    tool.name,
                    std::env::consts::OS,
                    std::env::consts::ARCH
                );
            };
            let url = format!(
                "https://github.com/{repo}/releases/download/v{}/{asset}",
                tool.version
            );
            fetch(root, tool, &url, *unpack, &bin)
        }
        Install::Cargo => {
            let status = Command::new("cargo")
                .args(["install", "--locked", "--version", tool.version, "--root"])
                .arg(dir(root))
                .arg(tool.name)
                .status()
                .context("running `cargo install`")?;
            if !status.success() {
                bail!("`cargo install {}` failed", tool.name);
            }
            Ok(())
        }
    }
}

/// Download one release asset and leave its binary at `bin`/`tool.name`.
fn fetch(root: &Path, tool: &Tool, url: &str, unpack: Unpack, bin: &Path) -> Result<()> {
    let scratch = dir(root).join(tool.name);
    std::fs::create_dir_all(&scratch).with_context(|| format!("creating {}", scratch.display()))?;
    let download = scratch.join("download");

    let status = Command::new("curl")
        .args([
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            "--output",
        ])
        .arg(&download)
        .arg(url)
        .status()
        .context("running `curl`, which is how a release binary arrives")?;
    if !status.success() {
        bail!("curl {url} failed");
    }

    let source = match unpack {
        Unpack::Bare => download,
        Unpack::TarGz => {
            let status = Command::new("tar")
                .arg("-xf")
                .arg(&download)
                .arg("-C")
                .arg(&scratch)
                .status()
                .context("running `tar`")?;
            if !status.success() {
                bail!("tar -xf {} failed", download.display());
            }
            find_file(&scratch, tool.name)
                .with_context(|| format!("no `{}` inside {}", tool.name, download.display()))?
        }
    };

    let target = bin.join(tool.name);
    std::fs::rename(&source, &target)
        .or_else(|_| {
            std::fs::copy(&source, &target)?;
            std::fs::remove_file(&source)
        })
        .with_context(|| format!("placing {}", target.display()))?;
    make_executable(&target)?;
    // The download is a copy; leaving it would put two of the binary in the
    // artifact every job uploads and downloads.
    drop(std::fs::remove_dir_all(&scratch));
    Ok(())
}

/// The first file named `name` at or under `dir`.
fn find_file(dir: &Path, name: &str) -> Option<PathBuf> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name() == Some(OsStr::new(name)) {
                return Some(path);
            }
        }
    }
    None
}

/// Make a downloaded binary runnable.
#[cfg(unix)]
fn make_executable(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
}

/// Make a downloaded binary runnable, which on these platforms it already is.
#[cfg(not(unix))]
fn make_executable(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key that ignored a version would serve a stale checker from the cache
    /// after a bump, so the run would exercise a version the pin does not name.
    #[test]
    fn the_cache_key_names_every_pinned_version() {
        let key = cache_key();
        for tool in TOOLS {
            assert!(key.contains(tool.name), "{key}");
            assert!(key.contains(tool.version), "{key}");
        }
    }

    /// `ShellCheck` ships one binary inside a versioned directory, which is
    /// what the download has to walk to; `find_file` is the whole of that walk.
    #[test]
    fn a_binary_inside_an_archive_is_found() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("shellcheck-v0.11.0").join("docs");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("shellcheck.txt"), "not a binary").unwrap();
        std::fs::write(root.path().join("shellcheck-v0.11.0/shellcheck"), "").unwrap();

        assert_eq!(
            find_file(root.path(), "shellcheck"),
            Some(root.path().join("shellcheck-v0.11.0/shellcheck"))
        );
        assert_eq!(find_file(root.path(), "buf"), None);
    }

    /// A checker nothing pins would otherwise be reported as installed, since
    /// there is no version to disagree with.
    #[test]
    fn an_unpinned_checker_is_reported_as_missing() {
        let missing = absent(Path::new("/nonexistent"), &["not-a-checker"]);
        assert_eq!(missing.len(), 1);
        assert!(
            missing
                .first()
                .is_some_and(|message| message.contains("pins no such checker")),
            "{missing:?}"
        );
    }
}
