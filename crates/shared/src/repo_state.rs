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

#[cfg(test)]
mod tests {
    use super::*;

    /// The sweep reads a trash path back to find the layer files it names, so
    /// the two have to agree. An epoch is padded to sixteen hex digits so that
    /// the object store lists incarnations in numeric order.
    #[test]
    fn a_trash_path_round_trips() {
        for (repo, epoch) in [
            ("acme", 0_u64),
            ("acme/widgets", 1),
            ("3f1b9a4e-0000-4000-8000-000000000000", u64::MAX),
        ] {
            let path = trash_path(repo, epoch);
            assert_eq!(parse_trash_path(&path), Some((repo, epoch)), "{path}");
        }
    }

    /// A key may contain `/`, so the repository is everything before the last
    /// segment and the epoch is the last one. Splitting on the first `/` would
    /// read `acme` as the repository and `widgets/...` as the epoch.
    #[test]
    fn a_namespaced_key_survives_the_split() {
        assert_eq!(
            trash_path("acme/widgets", 7),
            "trash/acme/widgets/0000000000000007"
        );
        assert_eq!(
            parse_trash_path("trash/acme/widgets/0000000000000007"),
            Some(("acme/widgets", 7))
        );
    }

    #[test]
    fn an_epoch_is_sixteen_hex_digits() {
        assert_eq!(epoch_path("acme", 1), "repos/acme/epochs/0000000000000001");
        assert_eq!(
            epoch_path("acme", u64::MAX),
            "repos/acme/epochs/ffffffffffffffff"
        );
        assert_eq!(state_path("acme/widgets"), "repos/acme/widgets/state");
    }

    #[test]
    fn a_path_that_is_not_a_trash_path_is_refused() {
        for bad in [
            "repos/acme/state",
            "trash/acme",
            "trash/acme/zz",
            "trash/acme/",
            "trash/",
            "",
        ] {
            assert_eq!(parse_trash_path(bad), None, "{bad:?}");
        }
    }
}
