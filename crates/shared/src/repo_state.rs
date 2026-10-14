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
    use crate::oid::Oid;
    use crate::state::{LayerMeta, layer_id};

    use super::*;

    /// A record is written by one incarnation and read by a node that has
    /// never seen it, so the pair has to survive being the only two things
    /// between them.
    #[test]
    fn a_record_round_trips_through_its_stored_form() {
        let mut record = Record {
            repo: "acme/widgets".to_string(),
            epoch: 3,
            index: 17,
            voters: vec![1, 2, 5],
            learners: vec![7],
            meta: None,
        };
        assert_eq!(Record::from_bytes(&record.to_bytes()).unwrap(), record);
        assert!(
            record.meta().refs.is_empty(),
            "an absent state machine reads as empty"
        );

        // With a state machine, which is the case the object store holds: refs,
        // a manifest carrying a stored layer and an inline one.
        let mut meta = RepoMeta {
            version: 4,
            next_id: 2,
            upload_gen: 1,
            epoch: 3,
            ..Default::default()
        };
        meta.refs.insert(
            "refs/heads/main".to_string(),
            Oid::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap(),
        );
        meta.layers = vec![
            LayerMeta {
                id: layer_id(9, 1),
                objects: 2,
                bytes: 40,
                first_id: 0,
                id_span: 2,
                ..Default::default()
            },
            LayerMeta {
                id: layer_id(10, 1),
                objects: 1,
                bytes: 5,
                data: Some(bytes::Bytes::from_static(b"layer")),
                first_id: 2,
                id_span: 1,
                ..Default::default()
            },
        ];
        record.meta = Some(meta.clone());

        let decoded = Record::from_bytes(&record.to_bytes()).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(decoded.meta(), meta);
        assert!(
            decoded
                .meta()
                .layers
                .first()
                .is_some_and(LayerMeta::is_stored),
            "a layer with no bytes is the stored one"
        );
        assert!(
            decoded
                .meta()
                .layers
                .get(1)
                .is_some_and(|inline| inline.end_id() == 3),
            "an inline layer keeps its id range"
        );
    }

    /// A corrupt record is refused, not guessed at: a node that read one as
    /// empty would host a repository with no refs, and answer a clone with
    /// an empty repository rather than an error.
    #[test]
    fn a_corrupt_record_is_refused() {
        let record = Record {
            repo: "acme".to_string(),
            epoch: 1,
            index: 2,
            ..Default::default()
        };
        let stored = record.to_bytes();

        // Protobuf's rule, worth pinning because it is the one input that is
        // not an error: an empty message is every field's default.
        assert_eq!(Record::from_bytes(&[]).unwrap(), Record::default());

        // Refused for being cut short, which is what a partial write leaves,
        // and reported against the record rather than as a bare failure.
        for cut in [1, 2, stored.len() - 1] {
            let err = Record::from_bytes(&stored[..cut]).unwrap_err();
            assert!(
                err.to_string().contains("Record"),
                "{cut} of {} bytes: {err}",
                stored.len()
            );
        }

        // And refused for a tag whose wire type is one no decoder accepts,
        // which is what a byte written over one leaves.
        let mut invented = stored.to_vec();
        invented[0] = 0xff;
        let err = Record::from_bytes(&invented).unwrap_err();
        assert!(err.to_string().contains("wire type"), "{err}");
    }

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
