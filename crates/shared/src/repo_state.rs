//! A repository's state in the object store: the ledger writes it
//! (creating a repository, checkpoints; see `enroute-ledger`); the layer
//! sweep and readers of the object store read it.
//!
//! ```text
//! repos/<repo>/state        the latest state: incarnation E, a log index,
//!                           its voters, and the state machine (refs,
//!                           manifest, inline layers),
//!                           as a protobuf `Record`;
//!                           replaced by compare-and-swap
//! repos/<repo>/epochs/<E>   the state incarnation E started from (what a
//!                           node joining it hosts)
//! trash/<repo>/<E>          incarnation E was deleted and its layer files
//!                           may remain (see [`Trash`])
//! ```

use anyhow::Result;
use prost::Message;

pub use crate::proto::Record;
use crate::state::RepoMeta;
use crate::store::{ObjectVersion, Objects};

impl Record {
    /// The state machine (empty if absent).
    #[must_use]
    pub fn meta(&self) -> RepoMeta {
        self.meta.clone().unwrap_or_default()
    }

    /// As stored in the object store (protobuf).
    #[must_use]
    pub fn to_bytes(&self) -> bytes::Bytes {
        self.encode_to_vec().into()
    }

    /// Decode a record as stored in the object store.
    ///
    /// # Errors
    ///
    /// Returns an error if `data` is not a valid record.
    pub fn from_bytes(data: &[u8]) -> Result<Record> {
        Ok(Record::decode(data)?)
    }
}

/// The object of incarnation `epoch` of `repo`.
#[must_use]
pub fn epoch_path(repo: &str, epoch: u64) -> String {
    format!("repos/{repo}/epochs/{epoch:016x}")
}

/// The object of the latest state of `repo`.
#[must_use]
pub fn state_path(repo: &str) -> String {
    format!("repos/{repo}/state")
}

/// The prefix of the trash objects.
pub const TRASH_PREFIX: &str = "trash/";

/// The trash object of incarnation `epoch` of `repo`.
#[must_use]
pub fn trash_path(repo: &str, epoch: u64) -> String {
    format!("{TRASH_PREFIX}{repo}/{epoch:016x}")
}

/// The repository and incarnation of a trash path.
#[must_use]
pub fn parse_trash_path(path: &str) -> Option<(&str, u64)> {
    let (repo, epoch) = path.strip_prefix(TRASH_PREFIX)?.rsplit_once('/')?;
    Some((repo, u64::from_str_radix(epoch, 16).ok()?))
}

/// A deleted incarnation's layer files to reclaim, as JSON at its trash
/// path. The ledger writes it before it marks the state object deleted, so
/// no deletion leaves files nobody will reclaim; the compute removes it once
/// they are gone. Every file of the incarnation (or an older one) has an
/// upload generation of at most `upload_gen`, and no later incarnation
/// takes such a file (it starts above `upload_gen + UPLOAD_GEN_WINDOW`).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Trash {
    /// The highest upload generation of the files to reclaim.
    pub upload_gen: u64,
    /// The reclaim's progress, as the layer sweep records it.
    #[serde(default)]
    pub unnamed: Unnamed,
}

/// Layer files seen unnamed, each with since when by the object store's
/// clock (milliseconds since the Unix epoch), once known.
pub type Unnamed = std::collections::BTreeMap<String, Option<u64>>;

/// The state of `repo` and its version, if the repository exists.
///
/// # Errors
///
/// Returns an error if the state cannot be read or decoded.
pub async fn latest(objects: &Objects, repo: &str) -> Result<Option<(Record, ObjectVersion)>> {
    let Some((data, version)) = objects.get_versioned(&state_path(repo)).await? else {
        return Ok(None);
    };
    Ok(Some((Record::from_bytes(&data)?, version)))
}
