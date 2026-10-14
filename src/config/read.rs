//! One capped read of one object.
//!
//! The deployment file arrives this way: a store, a key, and a cap that
//! refuses a URI pointing at the wrong object before its bytes are held.

use anyhow::{Context as _, Result, bail};

use crate::config::ObjectUri;

/// The largest object this reads.
///
/// A URI pointing at the wrong object should not be read into a process's
/// memory to find that out.
const MAX_BYTES: u64 = 1 << 20;

/// The text of the object `uri` names, refusing one larger than a
/// configuration is.
///
/// A store per read: this runs once, before anything a store could be shared
/// with.
///
/// # Errors
///
/// Returns an error if the object cannot be read, is over the cap, or is not
/// UTF-8.
pub(crate) async fn read_capped(uri: &ObjectUri) -> Result<String> {
    let (store, path) = uri.build()?;
    let read = store
        .get_opts(&path, object_store::GetOptions::default())
        .await
        .with_context(|| format!("reading {uri}"))?;

    // Off the metadata rather than after reading: the point is not to hold it.
    if read.meta.size > MAX_BYTES {
        bail!(
            "{uri}: {} bytes, over the {MAX_BYTES} this reads",
            read.meta.size
        );
    }
    let bytes = read
        .bytes()
        .await
        .with_context(|| format!("reading {uri}"))?;
    String::from_utf8(bytes.into()).with_context(|| format!("{uri}: not UTF-8"))
}
