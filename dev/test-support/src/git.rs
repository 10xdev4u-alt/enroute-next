//! The system `git` client, run the same way by every harness.

use std::process::Command;

/// A `git` command that reads no configuration outside the repository.
///
/// Fixed identity and dates, so a commit hashes the same on every machine.
#[must_use]
pub fn isolated() -> Command {
    let mut cmd = Command::new("git");
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        // A prompt has nobody to answer it here, and it hangs the run instead.
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000");
    // Lets `GIT_TRACE_PACKET=1 cargo test` show the wire.
    if let Some(trace) = std::env::var_os("GIT_TRACE_PACKET") {
        cmd.env("GIT_TRACE_PACKET", trace);
    }
    cmd
}
