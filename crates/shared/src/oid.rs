//! Object identifiers. Enroute currently only speaks SHA-1, but everything that
//! touches hashes goes through [`Oid`] so SHA-256 can be added later. Hashing
//! objects is the compute's job.

use std::fmt;

/// The length of an object id, in bytes (SHA-1).
pub const OID_LEN: usize = 20;
/// `OID_LEN` as a protobuf varint, which takes a `u64`.
const OID_WIRE_LEN: u64 = 20;

/// A SHA-1 object id.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::Unaligned,
)]
#[repr(transparent)]
pub struct Oid(pub [u8; OID_LEN]);

impl Oid {
    /// The all-zero id, which names no object.
    pub const ZERO: Oid = Oid([0; OID_LEN]);

    /// The id in `b`, which must be `OID_LEN` bytes long.
    ///
    /// Bytes past `OID_LEN` are ignored rather than refused: this reads a
    /// slice whose length the caller has already established, and a truncated
    /// copy would name a different object than the one it came from.
    ///
    /// # Panics
    ///
    /// Panics if `b` is shorter than `OID_LEN` bytes.
    #[must_use]
    pub fn from_slice(b: &[u8]) -> Oid {
        let mut o = [0u8; OID_LEN];
        #[expect(
            clippy::indexing_slicing,
            reason = "the documented contract: the caller passes OID_LEN bytes"
        )]
        o.copy_from_slice(&b[..OID_LEN]);
        Oid(o)
    }

    /// Parse an id from 40 hex digits. Surrounding whitespace is ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if `s` is not 40 hex digits.
    pub fn from_hex(s: &str) -> anyhow::Result<Oid> {
        let s = s.trim();
        anyhow::ensure!(s.len() == OID_LEN * 2, "invalid object id length: {s:?}");
        let mut o = [0u8; OID_LEN];
        hex::decode_to_slice(s, &mut o)
            .map_err(|e| anyhow::anyhow!("invalid object id {s:?}: {e}"))?;
        Ok(Oid(o))
    }

    /// The id as lowercase hex.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Whether this is the all-zero id.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.0 == [0; OID_LEN]
    }

    /// The raw bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; OID_LEN] {
        &self.0
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// The protocol's `Oid` message (`message Oid { bytes id = 1; }`), so the
/// generated types hold our type rather than raw bytes.
impl prost::Message for Oid {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        use prost::encoding::{WireType, encode_key, encode_varint};
        encode_key(1, WireType::LengthDelimited, buf);
        encode_varint(OID_WIRE_LEN, buf);
        buf.put_slice(&self.0);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        if tag != 1 {
            return prost::encoding::skip_field(wire_type, tag, buf, ctx);
        }
        let mut v: Vec<u8> = Vec::new();
        prost::encoding::bytes::merge(wire_type, &mut v, buf, ctx)?;
        if v.len() != OID_LEN {
            // prost has no other way to make a decode error.
            #[expect(deprecated, reason = "prost has no other way to make a decode error")]
            return Err(prost::DecodeError::new("object id is not 20 bytes"));
        }
        self.0.copy_from_slice(&v);
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::key_len(1) + 1 + OID_LEN
    }

    fn clear(&mut self) {
        *self = Oid::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_length_is_the_id_length() {
        assert_eq!(usize::try_from(OID_WIRE_LEN).unwrap(), OID_LEN);
    }

    /// `Oid` is `message Oid { bytes id = 1; }` in the contract, so the
    /// hand-written codec must put exactly those bytes on the wire: key 1,
    /// length-delimited, `OID_LEN` bytes. A wrong tag or length would stay
    /// invisible until another language decoded it.
    #[test]
    fn the_wire_form_is_the_generated_message() {
        use prost::Message as _;

        let oid = Oid::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap();
        let encoded = oid.encode_to_vec();
        assert_eq!(oid.encoded_len(), encoded.len());

        let mut expected = vec![0x0a, u8::try_from(OID_LEN).unwrap()];
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(encoded, expected);
    }

    #[test]
    fn the_wire_form_round_trips() {
        use prost::Message as _;

        for hex in [
            "0000000000000000000000000000000000000000",
            "0123456789abcdef0123456789abcdef01234567",
            "ffffffffffffffffffffffffffffffffffffffff",
        ] {
            let oid = Oid::from_hex(hex).unwrap();
            let decoded = Oid::decode(&oid.encode_to_vec()[..]).unwrap();
            assert_eq!(decoded, oid, "{hex}");
        }
    }

    /// The field carries `OID_LEN` bytes or it names nothing. Anything else
    /// must fail the decode rather than leave a zero-padded id.
    #[test]
    fn a_wire_id_of_the_wrong_length_is_refused() {
        use prost::Message as _;

        for len in [0u8, 19, 21] {
            let mut buf = vec![0x0a, len];
            buf.extend(std::iter::repeat_n(0xab, usize::from(len)));
            assert!(Oid::decode(&buf[..]).is_err(), "length {len}");
        }
    }

    #[test]
    fn clearing_yields_the_zero_id() {
        use prost::Message as _;

        let mut oid = Oid::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap();
        oid.clear();
        assert!(oid.is_zero());
        assert_eq!(oid, Oid::ZERO);
    }

    #[test]
    fn hex_round_trips_and_refuses_anything_else() {
        let hex = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(Oid::from_hex(hex).unwrap().to_hex(), hex);
        // Surrounding whitespace is ignored, because the value arrives from a
        // command line or a file as often as from the wire.
        assert_eq!(Oid::from_hex(&format!("  {hex}\n")).unwrap().to_hex(), hex);
        for bad in ["", "abc", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"] {
            assert!(Oid::from_hex(bad).is_err(), "{bad:?}");
        }
        for bad in [hex[..39].to_string(), format!("{hex}0")] {
            assert!(Oid::from_hex(&bad).is_err(), "{bad:?}");
        }
    }

    /// The documented contract, pinned: the first `OID_LEN` bytes are the id,
    /// and the rest is ignored.
    #[test]
    fn from_slice_uses_the_first_id_length_bytes() {
        let mut bytes = vec![0x5au8; OID_LEN];
        bytes.extend_from_slice(&[0xff; 8]);
        assert_eq!(Oid::from_slice(&bytes).as_bytes(), &[0x5a; OID_LEN]);
    }

    /// A peer may add a field to `message Oid`. Decoding a message that carries
    /// one must still yield the id: refusing it would make this node unable to
    /// read a newer writer's state.
    #[test]
    fn an_unknown_field_is_skipped() {
        use prost::Message as _;

        let oid = Oid::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap();
        let mut buf = oid.encode_to_vec();
        // Field 2, length-delimited: key 0x12, length 3, "abc".
        buf.extend_from_slice(&[0x12, 0x03, b'a', b'b', b'c']);
        assert_eq!(Oid::decode(&buf[..]).unwrap(), oid);

        // And before the id, where a decoder that stopped at the first
        // unexpected key would lose it.
        let mut before = vec![0x12, 0x03, b'a', b'b', b'c'];
        before.extend_from_slice(&oid.encode_to_vec());
        assert_eq!(Oid::decode(&before[..]).unwrap(), oid);
    }

    /// `from_slice` documents a panic for a slice shorter than `OID_LEN`,
    /// because truncating or zero-padding would name a different object than
    /// the caller passed. A change to that contract has to be deliberate.
    #[test]
    #[should_panic]
    fn from_slice_panics_on_a_short_slice() {
        let _ = Oid::from_slice(&[0u8; OID_LEN - 1]);
    }
}
