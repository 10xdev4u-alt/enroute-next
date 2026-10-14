//! The commands besides `serve` and `members`: a local node, the data
//! commands over the contract, and a deployment check.

pub(crate) mod data;
pub(crate) mod dev;
pub(crate) mod diagnose;
pub(crate) mod dotenv;

use std::io::Write as _;

/// Write `text` to stdout.
///
/// # Errors
///
/// Returns an error if stdout is closed.
pub(crate) fn print(text: &str) -> anyhow::Result<()> {
    let mut out = std::io::stdout().lock();
    out.write_all(text.as_bytes())?;
    if !text.ends_with('\n') {
        out.write_all(b"\n")?;
    }
    Ok(())
}

/// Write `value` to stdout as pretty JSON.
///
/// # Errors
///
/// Returns an error if stdout is closed.
pub(crate) fn print_json(value: &serde_json::Value) -> anyhow::Result<()> {
    print(&serde_json::to_string_pretty(value)?)
}
