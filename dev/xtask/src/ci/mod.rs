//! The pipeline as a program: every check CI runs, runnable here.
//!
//! `cargo xtask ci` runs them all, `cargo xtask ci fmt clippy` runs some, and
//! the workflow files name the same checks instead of repeating their commands,
//! so a laptop and a runner cannot disagree about what passing means. That
//! disagreement was not hypothetical: `cargo fmt --all --check` failed on
//! `main` for every pull request that landed, because nothing local ran it.
//! [`verify_workflow`] closes the other half — a check the workflow stops
//! naming is itself a finding, so no check can be dropped in silence.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the pipeline's output is its product: status on stdout, findings on stderr"
)]

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use yaml_rust2::{Yaml, YamlLoader};

use crate::root;

// The lint rules run the same checkers, from the same pinned copies.
pub(crate) mod tools;

/// The workflow [`verify_workflow`] reads when it is given no path.
const WORKFLOW: &str = ".github/workflows/ci.yml";

/// Words `cargo xtask ci` takes that are not checks: a workflow naming one is
/// naming the tool, not a check.
const SUBCOMMANDS: [&str; 2] = ["list", "tools"];

/// One check: the name it goes by, what a green run means, and how it runs.
#[derive(Debug)]
struct Check {
    /// The name `cargo xtask ci` and the workflows both spell.
    name: &'static str,
    /// One sentence: what a green run of it means.
    proves: &'static str,
    /// The checkers it needs, by the names [`tools`] pins.
    tools: &'static [&'static str],
    /// Variables the commands read, which no command line can carry.
    env: &'static [(&'static str, &'static str)],
    /// The commands it runs, in order; the first non-zero exit ends it.
    commands: &'static [&'static [&'static str]],
}

/// Every check, the cheapest signal first, so a break shows up early.
const CHECKS: &[Check] = &[
    Check {
        name: "fmt",
        proves: "Formatting is rustfmt's, so no diff carries a style argument.",
        tools: &[],
        env: &[],
        commands: &[&["cargo", "fmt", "--all", "--check"]],
    },
    Check {
        name: "clippy",
        proves: "No lint fires, with every lint a warning and every warning fatal.",
        tools: &[],
        env: &[],
        commands: &[&[
            "cargo",
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ]],
    },
    Check {
        name: "lint",
        proves: "The repository's own rules hold: imports, comments, layering, the contract, the skill.",
        tools: &["buf", "cargo-machete"],
        env: &[],
        commands: &[&["cargo", "xtask", "lint"]],
    },
    Check {
        name: "unit",
        proves: "Every unit test in the workspace passes.",
        tools: &[],
        env: &[],
        commands: &[&[
            "cargo",
            "test",
            "--workspace",
            "--lib",
            "--bins",
            "--locked",
            "--no-fail-fast",
        ]],
    },
    Check {
        name: "doc",
        proves: "Every example in a doc comment runs, and every doc link resolves.",
        tools: &[],
        // A doc link rustdoc cannot follow is a claim nothing backs.
        env: &[("RUSTDOCFLAGS", "-D warnings")],
        commands: &[
            &["cargo", "test", "--workspace", "--doc", "--locked"],
            &["cargo", "doc", "--workspace", "--no-deps", "--locked"],
        ],
    },
    Check {
        name: "e2e",
        proves: "The end-to-end suite passes against a node, real git and real traffic.",
        tools: &[],
        env: &[],
        commands: &[&[
            "cargo",
            "test",
            "-p",
            "enroute",
            "--tests",
            "--locked",
            "--no-fail-fast",
        ]],
    },
    Check {
        name: "shell",
        proves: "`install.sh` parses, shellcheck is silent on it, and its version gate keeps its contract.",
        tools: &["shellcheck"],
        env: &[],
        commands: &[
            &["sh", "-n", "install.sh"],
            &["shellcheck", "install.sh"],
            &["sh", "dev/install-version-test.sh"],
        ],
    },
];

/// Run the named checks, or every one of them when none is named.
pub(crate) fn run(selected: &[String]) -> ExitCode {
    let root = root::workspace();
    let chosen = match chosen(selected) {
        Ok(chosen) => chosen,
        Err(err) => {
            eprintln!("{err:#}");
            return ExitCode::FAILURE;
        }
    };

    let path = tools::search_path(&root);
    let outcomes: Vec<Outcome> = chosen
        .iter()
        .map(|check| one(&root, check, &path))
        .collect();
    report(&outcomes)
}

/// One check's verdict, and how long it took to reach.
struct Outcome {
    name: &'static str,
    passed: bool,
    took: Duration,
}

/// Run one check, saying what it is doing before it does it.
fn one(root: &Path, check: &Check, path: &OsStr) -> Outcome {
    println!("\n── {} ── {}", check.name, check.proves);
    let started = Instant::now();

    let missing = tools::absent(root, check.tools);
    let passed = if missing.is_empty() {
        commands(check, root, path)
    } else {
        eprintln!(
            "{} needs {}: run `cargo xtask ci tools`",
            check.name,
            missing.join(", ")
        );
        false
    };

    let took = started.elapsed();
    println!(
        "── {} {} in {}",
        check.name,
        if passed { "ok" } else { "FAILED" },
        human(took)
    );
    Outcome {
        name: check.name,
        passed,
        took,
    }
}

/// Run a check's commands in order, stopping at the first that fails.
fn commands(check: &Check, root: &Path, path: &OsStr) -> bool {
    for argv in check.commands {
        let Some((program, args)) = argv.split_first() else {
            continue;
        };
        println!("$ {}", argv.join(" "));
        let status = Command::new(program)
            .args(args)
            .current_dir(root)
            // The pinned checkers, then whatever the machine has.
            .env("PATH", path)
            .envs(check.env.iter().copied())
            .status();
        match status {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("`{}` exited with {status}", argv.join(" "));
                return false;
            }
            Err(err) => {
                eprintln!("could not run `{}`: {err}", argv.join(" "));
                return false;
            }
        }
    }
    true
}

/// The summary: one line per check, then the verdict for the run.
fn report(outcomes: &[Outcome]) -> ExitCode {
    let failed = outcomes.iter().filter(|outcome| !outcome.passed).count();
    println!("\n── pipeline ──");
    for outcome in outcomes {
        println!(
            "{:>7}  {}  {}",
            if outcome.passed { "ok" } else { "FAILED" },
            human(outcome.took),
            outcome.name
        );
    }
    if failed == 0 {
        println!("pipeline: {} check(s) ok", outcomes.len());
        return ExitCode::SUCCESS;
    }
    eprintln!("pipeline: {failed} of {} check(s) failed", outcomes.len());
    ExitCode::FAILURE
}

/// The checks named, all of them, or the refusal naming what exists.
fn chosen(selected: &[String]) -> Result<Vec<&'static Check>> {
    if selected.is_empty() {
        return Ok(CHECKS.iter().collect());
    }
    selected
        .iter()
        .map(|name| {
            CHECKS
                .iter()
                .find(|check| check.name == name)
                .ok_or_else(|| anyhow!("no check named `{name}`; there is {}", names()))
        })
        .collect()
}

/// Every check's name, for an error message.
fn names() -> String {
    CHECKS
        .iter()
        .map(|check| check.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Print the registry: what the pipeline runs, and what each check proves.
pub(crate) fn list(markdown: bool) -> ExitCode {
    if markdown {
        // The table a CI run publishes to its own step summary, so a green run
        // says what it proved rather than only that it finished.
        println!("| check | what a green run means | needs |");
        println!("| --- | --- | --- |");
        for check in CHECKS {
            println!(
                "| `{}` | {} | {} |",
                check.name,
                check.proves,
                if check.tools.is_empty() {
                    "—".to_string()
                } else {
                    check.tools.join(", ")
                }
            );
        }
        return ExitCode::SUCCESS;
    }

    for check in CHECKS {
        println!("{:<6} {}", check.name, check.proves);
        for argv in check.commands {
            println!("         $ {}", argv.join(" "));
        }
    }
    println!(
        "\nthe checkers: run `cargo xtask ci tools`; every version is pinned in dev/xtask/src/ci/tools.rs"
    );
    ExitCode::SUCCESS
}

/// Install or verify the pinned checkers, or print the key they cache under.
pub(crate) fn checkers(check_only: bool, cache_key: bool) -> ExitCode {
    if cache_key {
        println!("{}", tools::cache_key());
        return ExitCode::SUCCESS;
    }
    match tools::run(&root::workspace(), check_only) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

/// Fail when a workflow stops naming a check, or names one that is not here.
pub(crate) fn verify_workflow(path: Option<&Path>) -> ExitCode {
    let file = root::workspace().join(path.unwrap_or(Path::new(WORKFLOW)));
    let source = match std::fs::read_to_string(&file) {
        Ok(source) => source,
        Err(err) => {
            eprintln!("{}: {err}", file.display());
            return ExitCode::FAILURE;
        }
    };

    let named = match named_checks(&source) {
        Ok(named) => named,
        Err(err) => {
            eprintln!("{}: {err:#}", file.display());
            return ExitCode::FAILURE;
        }
    };

    let unrun: Vec<&str> = CHECKS
        .iter()
        .map(|check| check.name)
        .filter(|name| !named.contains(*name))
        .collect();
    let unknown: Vec<&str> = named
        .iter()
        .filter(|name| !CHECKS.iter().any(|check| check.name == name.as_str()))
        .map(String::as_str)
        .collect();

    if unrun.is_empty() && unknown.is_empty() {
        println!(
            "pipeline: {} check(s), every one named by {}",
            CHECKS.len(),
            file.display()
        );
        return ExitCode::SUCCESS;
    }

    for name in &unrun {
        eprintln!(
            "{}: `{name}` is a check, and no job runs it — a check nobody runs is a check nobody has",
            file.display()
        );
    }
    for name in &unknown {
        eprintln!(
            "{}: `{name}` is not a check; `cargo xtask ci list` has them",
            file.display()
        );
    }
    ExitCode::FAILURE
}

/// Every check name the workflow's `cargo xtask ci ...` steps spell out.
fn named_checks(source: &str) -> Result<BTreeSet<String>> {
    let documents = YamlLoader::load_from_str(source).context("parsing the workflow")?;
    let mut named = BTreeSet::new();
    for document in &documents {
        collect(document, &mut named);
    }
    Ok(named)
}

/// Collect the check names from every string in a parsed document.
fn collect(node: &Yaml, named: &mut BTreeSet<String>) {
    match node {
        Yaml::String(text) => named.extend(names_in(text)),
        Yaml::Array(items) => items.iter().for_each(|item| collect(item, named)),
        Yaml::Hash(map) => map.iter().for_each(|(key, value)| {
            collect(key, named);
            collect(value, named);
        }),
        _ => {}
    }
}

/// The check names after `cargo xtask ci` on one line.
///
/// Only the run of bare words right after the command counts, so a shell
/// operator or another program later on the line cannot be read as a check.
fn names_in(text: &str) -> Vec<String> {
    let mut named = Vec::new();
    let mut rest = text;
    while let Some((_, tail)) = rest.split_once("xtask ci") {
        for word in tail.split_whitespace() {
            let a_name = !word.starts_with('-')
                && !SUBCOMMANDS.contains(&word)
                && word.chars().all(|c| c.is_ascii_alphanumeric());
            if !a_name {
                break;
            }
            named.push(word.to_string());
        }
        rest = tail;
    }
    named
}

/// A duration a person reads at a glance.
fn human(took: Duration) -> String {
    if took.as_secs() < 1 {
        format!("{}ms", took.as_millis())
    } else if took.as_secs() < 60 {
        format!("{:.1}s", took.as_secs_f64())
    } else {
        format!("{}m{}s", took.as_secs() / 60, took.as_secs() % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_named_check_is_found_and_a_typo_is_refused() {
        let picked = chosen(&["fmt".to_string(), "e2e".to_string()]).unwrap();
        let names: Vec<&str> = picked.iter().map(|check| check.name).collect();
        assert_eq!(names, ["fmt", "e2e"]);

        let err = chosen(&["test".to_string()]).unwrap_err();
        assert!(err.to_string().contains("no check named `test`"), "{err}");
        assert!(err.to_string().contains("unit"), "{err}");
    }

    #[test]
    fn no_names_means_every_check() {
        assert_eq!(chosen(&[]).unwrap().len(), CHECKS.len());
    }

    /// The line a shell continues on is the case that read a `&&` and the next
    /// program as check names, which would then be reported as unknown.
    #[test]
    fn only_the_run_of_names_after_the_command_counts() {
        assert_eq!(
            names_in("cargo xtask ci fmt clippy lint"),
            ["fmt", "clippy", "lint"]
        );
        assert_eq!(names_in("cargo xtask ci shell && echo done"), ["shell"]);
        assert_eq!(names_in("cargo xtask ci tools"), Vec::<String>::new());
        assert_eq!(
            names_in("cargo xtask ci list --markdown"),
            Vec::<String>::new()
        );
        assert_eq!(
            names_in("cargo xtask ci verify-workflow ci.yml"),
            Vec::<String>::new()
        );
        assert_eq!(names_in("cargo xtask ci"), Vec::<String>::new());
        assert_eq!(names_in("nothing here"), Vec::<String>::new());
    }

    /// A workflow that parses but names no check would otherwise verify
    /// vacuously, the failure this whole check exists to catch.
    #[test]
    fn a_workflow_naming_nothing_names_nothing() {
        let source = "jobs:\n  test:\n    steps:\n      - run: cargo test\n";
        assert!(named_checks(source).unwrap().is_empty());

        let named =
            named_checks("jobs:\n  test:\n    steps:\n      - run: cargo xtask ci unit\n").unwrap();
        assert!(named.contains("unit"));
    }

    /// Every check names a command, and the names are the ones the workflow
    /// files and this module's own errors spell.
    #[test]
    fn every_check_is_runnable_and_uniquely_named() {
        let mut seen = BTreeSet::new();
        for check in CHECKS {
            assert!(
                check.commands.iter().all(|argv| !argv.is_empty()),
                "{}",
                check.name
            );
            assert!(seen.insert(check.name), "{} is named twice", check.name);
        }
    }
}
