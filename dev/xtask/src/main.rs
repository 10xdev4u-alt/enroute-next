//! Repo-wide checks complementing `cargo fmt`/clippy but not fitting either.
//!
//! Run with `cargo xtask lint`, or `cargo xtask ci` for the pipeline CI runs.

mod ci;
mod lint;
mod root;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the repository-wide lint rules.
    Lint,
    /// Run the pipeline CI runs, here and identically.
    #[command(args_conflicts_with_subcommands = true)]
    Ci {
        #[command(subcommand)]
        command: Option<Ci>,
        /// Checks to run, by name; every one of them when none is named.
        checks: Vec<String>,
    },
}

#[derive(Subcommand)]
enum Ci {
    /// Print every check, what it proves and the commands it runs.
    List {
        /// Format it as the table a CI run publishes to its step summary.
        #[arg(long)]
        markdown: bool,
    },
    /// Install or verify the checkers the checks shell out to.
    Tools {
        /// Only report what is missing, and install nothing.
        #[arg(long)]
        check: bool,
        /// Print the key a CI run caches the installed checkers under.
        #[arg(long)]
        cache_key: bool,
    },
    /// Fail when a workflow stops naming a check, or names one that is gone.
    VerifyWorkflow {
        /// The workflow to read; `.github/workflows/ci.yml` by default.
        path: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Lint => lint::run(),
        Command::Ci { command, checks } => match command {
            None => ci::run(&checks),
            Some(Ci::List { markdown }) => ci::list(markdown),
            Some(Ci::Tools { check, cache_key }) => ci::checkers(check, cache_key),
            Some(Ci::VerifyWorkflow { path }) => ci::verify_workflow(path.as_deref()),
        },
    }
}
