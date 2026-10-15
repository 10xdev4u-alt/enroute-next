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

/// Hash an object the way git does (`"<kind> <len>\0<data>"`), refusing a
/// message whose blocks reproduce a known SHA-1 attack from its first block.
///
/// Git's header is in front of the data, so this covers an attack built for
/// the message hashed, not a replayed publication; see #78.
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

    /// The two 640-byte messages of a published SHA-1 collision (the
    /// `SHAmbles` attack), as hex: the fixture the `sha1dc` crate tests its
    /// detection against. Each byte is two digits.
    const SHAMBLES: [&str; 2] = [
        concat!(
            "99040d047fe81780012000ff4b65792069732070617274206f66206120636f6c",
            "6c6973696f6e212049742773206120747261702179c61af0afcc054515d9274e",
            "7307624b1dc7fb23988bb8de8b575dba7b9eab31c1674b6d974378a827732ff5",
            "851c76a2e60772b5a47ce1eac40bb993c12d8c70e24a4f8d5fcdedc1b32c9cf1",
            "9e31af2429759d42e4dfdb31719f587623ee552939b6dcdc459fca53553b70f8",
            "7ede30a247ea3af6c759a2f20b320d760db64ff479084fd3ccb3cdd48362d96a",
            "9c430617caff6c36c637e53fde28417f626fec54ed7943a46e5f5730f2bb38fb",
            "1df6e0090010d00e24ad78bf92641993608e8d158a789f34c46fe1e6027f35a4",
            "cbfb827076c50eca0e8b7cca69bb2c2b790259f9bf9570dd8d4437a3115faff7",
            "c3cac09ad25266055c27104755178eaeff825a2caa2acfb5de64ce7641dc59a5",
            "41a9fc9c756756e2e23dc713c8c24c9790aa6b0e38a7f55f14452a1ca2850ddd",
            "9562fd9a18ad42496aa97008f74672f68ef461eb88b09933d626b4f918749cc0",
            "27fddd6c425fc4216835d0134d15285bab2cb784a4f7cbb4fb514d4bf0f6237c",
            "f00a9e9f132b9a066e6fd17f6c42987478586ff651af96747fb426b9872b9a88",
            "e4063f59bb334cc00650f83a80c42751b71974d300fc2819a2e8f1e32c1b51cb",
            "18e6bfc4db9baef675d4aaf5b1574a047f8f6dd2ec153a93412293974d928f88",
            "ced9363cfef97ce2e742bf34c96b8ef3875676fea5cca8e5f7dea0bab2413d4d",
            "e00ee71ee01f162bdb6d1eafd925e6aebaae6a354ef17cf205a404fbdb12fc45",
            "4d41fdd95cf2459664a2ad032d1da60a73264075d7f1e0d6c1403ae7a0d861df",
            "3fe5707188dd5e07d1589b9f8b6630553f8fc352b3e0c27da80bddba4c64020d"
        ),
        concat!(
            "99030d047fe81780011800ff50726163746963616c205348412d312063686f73",
            "656e2d70726566697820636f6c6c6973696f6e211d276c6ba661e1040e1f7d76",
            "7f076249ddc7fb332c8bb8c2b7575dbec79eab2be1674b7db34378b4cb732fe1",
            "891c76a0260772a5107ce1f6e80bb9977d2d8c68524a4f9d5fcdedcd0b2c9ce1",
            "9231af26e9759d5250dfdb2d4d9f58729fee553319b6dccc619fca4fb93b70ec",
            "72de30a087ea3ae67359a2ee27320d72b1b64fecc9084fc3ccb3cdd83b62d97a",
            "904306150aff6c267237e523e228417bde6fec4ecd7943b44a5f572c1ebb38ef",
            "11f6e00bc010d01e90ad78a3be641997dc8e8d0d3a789f24c46fe1eaba7f35b4",
            "c7fb8272b6c50edaba8b7cd655bb2c2fc50259e39f9570cda94437bffd5fafe3",
            "cfcac09812526615e827105b79178eaa43825a341a2acfa5de64ce7af9dc59b5",
            "4da9fc9eb56756f2563dc70ff4c24c932caa6b1418a7f54f30452a004e850dc9",
            "9962fd98d8ad4259dea97014db4672f232f461f338b09923d626b4f5a0749cd0",
            "2bfddd6e825fc431dc35d00f7115285f172cb79e84f7cba4df514d571cf62368",
            "fc0a9e9dd32b9a16da6fd16340429870c4586feee1af96647fb426b53f2b9a98",
            "e8063f5b7b334cd0b250f826bcc427550b1974c920fc280986e8f1ffc01b51df",
            "14e6bfc61b9baee6c1d4aae99d574a00c38f6dca5c153a834122939bf5928f98",
            "c2d9363e3ef97cf25342bf28f56b8ef73b5676e485cca8f5d3dea0a65e413d59",
            "ec0ee71c201f163b6f6d1eb3f525e6aa06ae6a2dfef17ce205a404f76312fc55",
            "4141fddb9cf24586d0a2ad1f111da60ecf26406ff7f1e0c6e5403afb4cd861cb",
            "33e5707348dd5e1765589b83a7663051838fc34a03e0c26da80bddb6f464021d"
        ),
    ];

    /// The shared SHA-1 of the pair, which `sha1dc` reports for both.
    const SHAMBLES_DIGEST: &str = "8ac60ba76f1999a1ab70223f225aefdc78d4ddc0";

    fn bytes(hex: &str) -> Vec<u8> {
        hex.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("ascii hex");
                u8::from_str_radix(pair, 16).expect("hex")
            })
            .collect()
    }

    fn sha1dc_hasher(data: &[u8]) -> sha1dc::Hasher {
        let mut h = sha1dc::Hasher::new();
        h.update(data);
        h
    }

    /// The fixture is the collision it claims to be: two different messages,
    /// one SHA-1. Whatever the refusal below covers rests on that.
    #[test]
    fn the_fixture_is_a_collision_pair() {
        use sha1dc::Digest;

        let (one, two) = (bytes(SHAMBLES[0]), bytes(SHAMBLES[1]));
        assert_ne!(one, two);
        assert_eq!((one.len(), two.len()), (640, 640));
        let digest = |data: &[u8]| {
            sha1dc_hasher(data)
                .finalize()
                .expect_err("the attack")
                .digest()
        };
        assert_eq!(digest(&one), digest(&two), "the messages do not collide");
        let expected = Oid::from_hex(SHAMBLES_DIGEST).expect("hex");
        assert_eq!(digest(&one), expected.0);
        assert_eq!(digest(&two), expected.0);
        assert_eq!(Oid::from_slice(&expected.0), expected);
        // A different message is not flagged, so a refusal means something.
        assert_ne!(digest(&one), Digest::from([0u8; 20]));
    }

    /// The detection recognizes the attack's blocks where they open the
    /// message hashed. One byte in front of them is enough to lose it: this
    /// pair was measured with prefixes of 1 through 320 bytes, and truncated
    /// at 64 through 576, and only the whole, unshifted message is refused.
    #[test]
    fn a_known_attack_is_refused_only_from_its_first_block() {
        for hex in SHAMBLES {
            assert!(
                sha1dc_hasher(&bytes(hex)).finalize().is_err(),
                "the attack went unnoticed"
            );
            let mut shifted = vec![b'x'];
            shifted.extend_from_slice(&bytes(hex));
            assert!(
                sha1dc_hasher(&shifted).finalize().is_ok(),
                "a shifted attack was still recognized"
            );
        }
    }

    /// A git object is hashed as `<kind> <len>\0<data>`, and that header is
    /// nine bytes in front of the data: it moves a published attack's blocks
    /// out of the position the detection recognizes. So the pair is not
    /// refused as objects, and, being built for the standard state and this
    /// length, the two do not collide as objects either — which is what
    /// keeps the pair harmless here. The refusal covers an attack built for
    /// the message it hashes, not a replayed publication; see #78.
    #[test]
    fn a_git_header_moves_a_publication_out_of_the_refusals_reach() {
        let one = try_hash_object(Kind::Blob, &bytes(SHAMBLES[0])).expect("hashed");
        let two = try_hash_object(Kind::Blob, &bytes(SHAMBLES[1])).expect("hashed");
        assert_ne!(one, two, "the pair collides as git objects after all");
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
