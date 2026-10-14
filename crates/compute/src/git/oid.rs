//! Object ids, object kinds and object hashing. [`Oid`] is shared with the
//! ledger, which never hashes an object, so kinds and hashing live here.

pub(crate) use enroute_shared::oid::{OID_LEN, Oid};

/// Object kinds. The discriminants match git's pack type codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
}

impl Kind {
    pub const ALL: [Kind; 4] = [Kind::Commit, Kind::Tree, Kind::Blob, Kind::Tag];

    pub fn name(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Tree => "tree",
            Kind::Blob => "blob",
            Kind::Tag => "tag",
        }
    }

    pub(crate) fn from_code(c: u8) -> Option<Kind> {
        Some(match c {
            1 => Kind::Commit,
            2 => Kind::Tree,
            3 => Kind::Blob,
            4 => Kind::Tag,
            _ => return None,
        })
    }

    /// The pack type code.
    pub(crate) fn code(self) -> u8 {
        match self {
            Kind::Commit => 1,
            Kind::Tree => 2,
            Kind::Blob => 3,
            Kind::Tag => 4,
        }
    }

    /// Index into per-kind arrays.
    pub fn idx(self) -> usize {
        match self {
            Kind::Commit => 0,
            Kind::Tree => 1,
            Kind::Blob => 2,
            Kind::Tag => 3,
        }
    }
}

/// Hash an object the way git does (`"<kind> <len>\0<data>"`), with
/// SHA-1 collision detection (sha1dc). Content addressing is only sound
/// if two different objects can't share an id, so objects that carry a
/// known collision attack are rejected, as git does.
pub(crate) fn try_hash_object(kind: Kind, data: &[u8]) -> anyhow::Result<Oid> {
    let mut h = sha1dc::Hasher::new();
    h.update(kind.name().as_bytes());
    h.update(b" ");
    h.update(data.len().to_string().as_bytes());
    h.update(&[0u8]);
    h.update(data);
    let d = h.finalize().map_err(|e| {
        anyhow::anyhow!(
            "SHA-1 collision attack detected in {} object: {e}",
            kind.name()
        )
    })?;
    Ok(Oid(d.into()))
}

/// [`try_hash_object`], for tests: they hash only objects that they make.
#[cfg(test)]
pub(crate) fn hash_object(kind: Kind, data: &[u8]) -> Oid {
    try_hash_object(kind, data).expect("collision attack in locally generated object")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// The four codes are git's pack type codes. Pinned as literals, because a
    /// reordering of the enum would keep a round trip passing.
    #[test]
    fn the_pack_codes_are_gits() {
        assert_eq!(Kind::Commit.code(), 1);
        assert_eq!(Kind::Tree.code(), 2);
        assert_eq!(Kind::Blob.code(), 3);
        assert_eq!(Kind::Tag.code(), 4);
    }

    #[test]
    fn the_pack_code_round_trips() {
        for kind in Kind::ALL {
            assert_eq!(Kind::from_code(kind.code()), Some(kind), "{kind:?}");
        }
    }

    #[test]
    fn a_code_that_is_no_kind_is_refused() {
        for code in [0_u8, 5, 6, 7, 255] {
            assert_eq!(Kind::from_code(code), None, "{code}");
        }
    }

    #[test]
    fn every_kind_has_its_own_index() {
        for (i, kind) in Kind::ALL.iter().enumerate() {
            assert_eq!(kind.idx(), i, "{kind:?}");
        }
    }

    #[test]
    fn every_kind_has_its_own_name() {
        let names: BTreeSet<&str> = Kind::ALL.iter().map(|kind| kind.name()).collect();
        assert_eq!(names.len(), Kind::ALL.len());
        assert!(names.contains("commit"));
        assert!(names.contains("tag"));
    }

    /// The header is git's, `<kind> <len>\0<data>`, so the id is the one `git
    /// hash-object` prints for the same content. The literal catches a wrong
    /// kind name, a wrong length and a missing NUL in one assertion.
    #[test]
    fn hashing_matches_gits_object_header() {
        // printf 'hello, world\n' | git hash-object --stdin
        assert_eq!(
            hash_object(Kind::Blob, b"hello, world\n").to_hex(),
            "4b5fa63702dd96796042e92787f464e28f09f17d"
        );
    }
}
