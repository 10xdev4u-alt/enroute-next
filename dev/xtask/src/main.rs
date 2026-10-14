//! Repo-wide checks complementing `cargo fmt`/clippy but not fitting either.
//!
//! Run with `cargo xtask lint`.

mod lint;
mod root;

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
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Lint => lint::run(),
    }
}
