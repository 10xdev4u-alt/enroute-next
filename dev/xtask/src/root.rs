//! Where the workspace is.

use std::path::{Path, PathBuf};

/// The workspace root.
pub(crate) fn workspace() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or(root)
}
