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
}
