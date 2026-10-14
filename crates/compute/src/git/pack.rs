//! Packfile codecs: entry headers, a sequential pack reader, stored entries,
//! and helpers for writing packs. No I/O: packs are read in order through
//! [`Incoming`].

use anyhow::{Context, Result, bail, ensure};

use super::cast::to_u64;
use super::io::Incoming;
use super::oid::{Kind, OID_LEN, Oid};
use super::zlib;

pub(crate) const OFS_DELTA: u8 = 6;
pub(crate) const REF_DELTA: u8 = 7;

/// `v & mask` as a byte.
fn masked(v: u64, mask: u8) -> u8 {
    u8::try_from(v & u64::from(mask)).unwrap_or(mask)
}

pub(crate) fn encode_entry_header(type_code: u8, mut size: u64, out: &mut Vec<u8>) {
    let mut b = (type_code << 4) | masked(size, 0x0f);
    size >>= 4;
    while size > 0 {
        out.push(b | 0x80);
        b = masked(size, 0x7f);
        size >>= 7;
    }
    out.push(b);
}

/// Returns (type code, size, header length).
pub(crate) fn decode_entry_header(data: &[u8]) -> Result<(u8, u64, usize)> {
    let (&first, rest) = data.split_first().context("truncated pack entry")?;
    let mut c = first;
    let ty = (c >> 4) & 7;
    let mut size = u64::from(c & 0x0f);
    let mut shift = 4;
    let mut i = 1;
    let mut rest = rest.iter();
    while c & 0x80 != 0 {
        c = *rest.next().context("truncated pack entry header")?;
        ensure!(shift < 64, "pack entry size overflow");
        size |= u64::from(c & 0x7f) << shift;
        shift += 7;
        i += 1;
    }
    Ok((ty, size, i))
}

fn decode_ofs(data: &[u8]) -> Result<(u64, usize)> {
    let (&first, rest) = data.split_first().context("truncated ofs-delta")?;
    let mut c = first;
    let mut ofs = u64::from(c & 0x7f);
    let mut i = 1;
    let mut rest = rest.iter();
    while c & 0x80 != 0 {
        ensure!(ofs < (1 << 56), "bad ofs-delta");
        c = *rest.next().context("bad ofs-delta")?;
        ofs = ((ofs + 1) << 7) | u64::from(c & 0x7f);
        i += 1;
    }
    Ok((ofs, i))
}

/// Encode an `OFS_DELTA` base distance (inverse of `decode_ofs`).
pub(crate) fn encode_ofs(mut ofs: u64, out: &mut Vec<u8>) {
    // Bytes come out least significant first; the format wants them reversed.
    let start = out.len();
    out.push(masked(ofs, 0x7f));
    ofs >>= 7;
    while ofs > 0 {
        ofs -= 1;
        out.push(0x80 | masked(ofs, 0x7f));
        ofs >>= 7;
    }
    if let Some(tail) = out.get_mut(start..) {
        tail.reverse();
    }
}

/// A pack entry of `data`, after the type code and `base` bytes.
fn entry(type_code: u8, base: &[u8], data: &[u8]) -> Vec<u8> {
    // The header, the base and the zlib data at about 2:1, so that it
    // rarely grows.
    let mut out = Vec::with_capacity(10 + base.len() + data.len() / 2 + 16);
    encode_entry_header(type_code, to_u64(data.len()), &mut out);
    out.extend_from_slice(base);
    zlib::compress(data, &mut out);
    out
}

/// A full (non-delta) pack entry.
pub(crate) fn full_entry(kind: Kind, content: &[u8]) -> Vec<u8> {
    entry(kind.code(), &[], content)
}

/// A `REF_DELTA` pack entry.
pub(crate) fn ref_delta_entry(base: &Oid, delta: &[u8]) -> Vec<u8> {
    entry(REF_DELTA, base.as_bytes(), delta)
}

/// A pack entry as stored by Enroute (always full or `REF_DELTA`).
#[derive(Debug)]
pub(crate) struct StoredEntry<'a> {
    pub delta: bool,
    /// Inflated size of `zdata`.
    pub size: u64,
    pub zdata: &'a [u8],
}

impl StoredEntry<'_> {
    pub(crate) fn parse(bytes: &[u8]) -> Result<StoredEntry<'_>> {
        let (ty, size, hl) = decode_entry_header(bytes)?;
        let delta = ty == REF_DELTA;
        let skip = if delta {
            OID_LEN
        } else {
            Kind::from_code(ty).context("unsupported stored entry type")?;
            0
        };
        let zdata = bytes.get(hl + skip..).context("truncated pack entry")?;
        Ok(StoredEntry { delta, size, zdata })
    }

    /// The object, with `base` the content of a delta's base.
    pub(crate) fn decode(&self, base: Option<&[u8]>) -> Result<Vec<u8>> {
        let data = zlib::decompress(
            self.zdata,
            usize::try_from(self.size).context("pack entry too large")?,
        )?;
        if !self.delta {
            return Ok(data);
        }
        super::delta::apply(base.context("delta without a base")?, &data)
    }
}

/// Delta base of a pack entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryBase {
    None,
    /// `OFS_DELTA`: the base entry's offset in the pack.
    Ofs(u64),
    /// `REF_DELTA`: the base object's id.
    Ref(Oid),
}

/// One entry of a pack, as read in pack order.
pub(crate) struct PackEntry {
    pub offset: u64,
    pub type_code: u8,
    /// Inflated size of the entry data (for deltas: of the delta).
    pub size: u64,
    pub base: EntryBase,
    /// Where the compressed data lies in the pack.
    pub data_start: u64,
    pub data_len: u64,
    /// The inflated data (an object, or a delta).
    pub content: Vec<u8>,
}

/// Reads a pack's entries in order, as the bytes arrive. Entry boundaries
/// are only known after inflating each entry, so this is the one sequential
/// pass over a pack. The trailer checksum is checked at the end.
pub(crate) struct PackReader<'a> {
    inc: &'a mut Incoming,
    hash: PackHasher,
    pos: u64,
    count: u32,
    read: u32,
}

impl<'a> PackReader<'a> {
    /// Read the header.
    pub(crate) fn new(inc: &'a mut Incoming) -> Result<PackReader<'a>> {
        let head = inc.take(12).context("pack too short")?;
        let (magic, rest) = head.split_first_chunk::<4>().context("not a pack")?;
        ensure!(magic == b"PACK", "not a pack");
        let (version, rest) = rest.split_first_chunk::<4>().context("not a pack")?;
        let version = u32::from_be_bytes(*version);
        ensure!(
            version == 2 || version == 3,
            "unsupported pack version {version}"
        );
        let (count, _) = rest.split_first_chunk::<4>().context("not a pack")?;
        let count = u32::from_be_bytes(*count);
        let mut hash = PackHasher::default();
        hash.update(head);
        Ok(PackReader {
            inc,
            hash,
            pos: 12,
            count,
            read: 0,
        })
    }

    /// Number of entries the header announces.
    pub(crate) fn count(&self) -> u32 {
        self.count
    }

    /// The next entry; after the last, checks the trailer.
    pub(crate) fn next_entry(&mut self) -> Result<Option<PackEntry>> {
        if self.read == self.count {
            let trailer = self.inc.take(OID_LEN).context("pack truncated")?;
            ensure!(
                trailer == std::mem::take(&mut self.hash).finish(),
                "pack checksum mismatch"
            );
            ensure!(self.inc.fill(1)?.is_empty(), "trailing garbage in pack");
            return Ok(None);
        }
        let offset = self.pos;
        // Entry header, ofs or ref base: at most 10 + 10 + 20 bytes.
        let w = self.inc.fill(64)?;
        ensure!(!w.is_empty(), "pack truncated at entry {}", self.read);
        let (ty, size, hl) = decode_entry_header(w)?;
        let (base, bl) = match ty {
            OFS_DELTA => {
                let (ofs, n) = decode_ofs(w.get(hl..).context("truncated ofs-delta")?)?;
                (
                    EntryBase::Ofs(
                        offset
                            .checked_sub(ofs)
                            .context("ofs-delta before pack start")?,
                    ),
                    n,
                )
            }
            REF_DELTA => {
                let id = w.get(hl..hl + OID_LEN).context("truncated ref-delta")?;
                (EntryBase::Ref(Oid::from_slice(id)), OID_LEN)
            }
            1..=4 => (EntryBase::None, 0),
            _ => bail!("invalid pack entry type {ty}"),
        };
        let head = hl + bl;
        let (content, used) = self.inc.inflate(head, usize::try_from(size)?)?;
        self.hash.update(self.inc.take(head + used)?);
        self.pos += to_u64(head + used);
        self.read += 1;
        Ok(Some(PackEntry {
            offset,
            type_code: ty,
            size,
            base,
            data_start: offset + to_u64(head),
            data_len: to_u64(used),
            content,
        }))
    }
}

/// A pack of `entries` (header, entries, trailer).
pub(crate) fn pack_of(entries: &[Vec<u8>]) -> Vec<u8> {
    let (mut h, mut pack) = PackHasher::header(
        // A pack with 2^32 entries does not fit in memory.
        u32::try_from(entries.len()).unwrap_or(u32::MAX),
    );
    for e in entries {
        h.update(e);
        pack.extend_from_slice(e);
    }
    pack.extend_from_slice(&h.finish());
    pack
}

/// The hash of a pack's trailer. It is an integrity checksum, not an
/// address, so it is plain SHA-1, without collision detection.
#[derive(Default)]
pub(crate) struct PackHasher(sha1::Sha1);

impl PackHasher {
    /// The header of a pack of `count` entries, and a hasher that has it.
    pub(crate) fn header(count: u32) -> (Self, Vec<u8>) {
        let mut h = Vec::with_capacity(12);
        h.extend_from_slice(b"PACK");
        h.extend_from_slice(&2u32.to_be_bytes());
        h.extend_from_slice(&count.to_be_bytes());
        let mut hasher = PackHasher::default();
        hasher.update(&h);
        (hasher, h)
    }

    pub(crate) fn update(&mut self, b: &[u8]) {
        sha1::Digest::update(&mut self.0, b);
    }

    pub(crate) fn finish(self) -> [u8; OID_LEN] {
        sha1::Digest::finalize(self.0).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ofs_roundtrip() {
        for ofs in [
            1u64,
            127,
            128,
            129,
            16383,
            16384,
            16511,
            16512,
            1 << 30,
            (1 << 40) + 12345,
        ] {
            let mut b = Vec::new();
            encode_ofs(ofs, &mut b);
            assert_eq!(decode_ofs(&b).unwrap(), (ofs, b.len()), "ofs {ofs}");
        }
    }

    #[test]
    fn header_roundtrip() {
        for &(t, s) in &[
            (1u8, 0u64),
            (3, 15),
            (3, 16),
            (7, 1 << 40),
            (2, 123_456_789),
        ] {
            let mut v = Vec::new();
            encode_entry_header(t, s, &mut v);
            let (t2, s2, n) = decode_entry_header(&v).unwrap();
            assert_eq!((t, s, v.len()), (t2, s2, n));
        }
    }
}
