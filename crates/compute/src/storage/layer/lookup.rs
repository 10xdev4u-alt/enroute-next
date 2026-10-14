//! Object lookup in a layer's oid index.
//!
//! Entries of one kind are sorted by oid, and the layer keeps every
//! `SAMPLE`th oid in memory. Each probe of the index is a pool access and
//! a cache miss, so the search reads as few of them as it can: SHA-1 oids
//! are uniform, so interpolation between two samples finds the right page,
//! and a short scan from the guess finds the entry and its local index.

use std::ops::Range;

use anyhow::Result;

use zerocopy::FromBytes;

use crate::git::oid::{Kind, Oid};
use crate::storage::layer::{Layer, OID_ENTRY, OidEntry, PAGE_ENTRIES, SAMPLE, widen};
use crate::storage::pool::{Access, must};

/// The local index of `oid` among objects of `kind` in `layer`.
pub(super) fn find(layer: &Layer, kind: Kind, oid: &Oid) -> Result<Option<u32>> {
    let range = layer.index_range(kind);
    let samples = &layer.samples[kind];
    // The bucket of SAMPLE entries whose first oid is <= oid.
    let Some(b) = samples.partition_point(|s| s <= oid).checked_sub(1) else {
        return Ok(None);
    };
    // At most one sample per SAMPLE objects, so the bucket fits in u32.
    let start = range.start + u32::try_from(b)? * SAMPLE;
    let mut s = Span {
        lo: start,
        hi: (start + SAMPLE).min(range.end),
        lo_key: oid_key(must(samples.get(b), "bucket sample")),
        hi_key: samples.get(b + 1).map_or(u64::MAX, oid_key),
    };
    let key = oid_key(oid);
    while s.lo < s.hi {
        match search_page(layer, s, s.guess(key), oid)? {
            Step::Found(local) => return Ok(Some(local)),
            Step::Absent => return Ok(None),
            Step::Narrow(next) => s = next,
        }
    }
    Ok(None)
}

/// The outcome of searching one page.
#[expect(
    variant_size_differences,
    reason = "returned once per page, never stored"
)]
enum Step {
    /// The local index of `oid`, or, from [`Span::step`], its entry.
    Found(u32),
    Absent,
    /// `oid` is in the span, if anywhere, but off the page.
    Narrow(Span),
}

/// Search for `oid` in entries `s` on the index page that holds entry
/// `guess`.
fn search_page(layer: &Layer, s: Span, guess: u32, oid: &Oid) -> Result<Step> {
    let page = guess / PAGE_ENTRIES;
    let first = page * PAGE_ENTRIES;
    let (a, b) = (first.max(s.lo), (first + PAGE_ENTRIES).min(s.hi));
    layer.oids.with_page(page, Access::Hot, |buf| {
        let entry = |i: u32| {
            let off = widen(i - first) * OID_ENTRY;
            let e = buf
                .get(off..off + OID_ENTRY)
                .and_then(|b| OidEntry::ref_from_bytes(b).ok());
            must(e, "entry on its page")
        };
        match s.step(a..b, guess, oid, |i| entry(i).oid) {
            Step::Found(i) => Step::Found(entry(i).local.get()),
            step => step,
        }
    })
}

/// Entries `lo..hi` of one kind, whose oids have keys in `lo_key..=hi_key`.
#[derive(Clone, Copy)]
struct Span {
    lo: u32,
    hi: u32,
    lo_key: u64,
    hi_key: u64,
}

impl Span {
    /// Where `key` would be if the keys were spread evenly.
    fn guess(&self, key: u64) -> u32 {
        let span = u128::from(self.hi_key - self.lo_key) + 1;
        let ahead = u128::from(key - self.lo_key) * u128::from(self.hi - self.lo) / span;
        self.lo + must(u32::try_from(ahead).ok(), "guess inside the span")
    }

    /// Search entries `r`, part of the span, which `at` reads, starting
    /// from `g`. Entries are adjacent, so a short scan reads few cache
    /// lines, and in order.
    fn step(self, r: Range<u32>, g: u32, oid: &Oid, at: impl Fn(u32) -> Oid) -> Step {
        let mut i = g;
        if at(g) < *oid {
            i += 1;
            while i < r.end && at(i) < *oid {
                i += 1;
            }
        } else {
            while i > r.start && at(i - 1) >= *oid {
                i -= 1;
            }
        }
        // `i` is where `oid` is, or would be.
        if i < r.end && at(i) == *oid {
            Step::Found(i)
        } else if i == r.start && i > self.lo {
            Step::Narrow(Span {
                hi: i,
                hi_key: oid_key(&at(i)),
                ..self
            })
        } else if i == r.end && i < self.hi {
            Step::Narrow(Span {
                lo: i,
                lo_key: oid_key(&at(i - 1)),
                ..self
            })
        } else {
            Step::Absent
        }
    }
}

/// The leading bytes of `oid`, in the same order as the oids.
fn oid_key(oid: &Oid) -> u64 {
    u64::from_be_bytes(*must(oid.0.first_chunk(), "oid prefix"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::collection::vec;
    use proptest::prelude::any;

    use super::*;
    use crate::storage::layer::tests::blob;
    use crate::storage::layer::{INDEX_PAGE, LayerBuilder, NewObject};
    use crate::storage::spill::Workspace;

    /// A layer with `oids`, every `i`th of kind `kinds[i % kinds.len()]`.
    fn layer_of(oids: &[Oid], kinds: &[Kind], pool_bytes: usize) -> Layer {
        let dir = tempfile::tempdir().unwrap();
        let mut b = LayerBuilder::new(&Workspace::at(dir.path()));
        for (o, &kind) in oids.iter().zip(kinds.iter().cycle()) {
            b.add(NewObject {
                kind,
                ..blob(*o, &[0], None)
            })
            .unwrap();
        }
        let mut file = Vec::new();
        b.write_to(&mut file).unwrap();
        let pool = crate::storage::pool::BufferPool::new(pool_bytes);
        let f = crate::storage::pool::Memory(file.into());
        Layer::from_file("x".into(), pool.open(Arc::new(f))).unwrap()
    }

    proptest::proptest! {
        /// Lookups find every object and nothing else, also when oids are
        /// far from uniform.
        #[test]
        fn find_matches_a_scan(
            // Skew: every oid starts with one of four 4-byte values.
            skewed in vec((0u8..4, any::<[u8; 20]>()), 1..1500),
            absent in vec(any::<[u8; 20]>(), 200),
            pool_pages in 1usize..64,
        ) {
            let present: std::collections::BTreeSet<Oid> = skewed
                .into_iter()
                .map(|(p, mut o)| {
                    o[..4].fill(p * 85);
                    Oid(o)
                })
                .collect();
            let present: Vec<Oid> = present.into_iter().collect();
            let absent: Vec<Oid> = absent.into_iter().map(Oid).collect();
            let kinds = [Kind::Blob, Kind::Tree, Kind::Blob, Kind::Commit, Kind::Tag];
            let l = layer_of(&present, &kinds, pool_pages * INDEX_PAGE);
            let mut want = std::collections::HashMap::new();
            l.for_each_oid(|local, o| {
                want.insert(*o, local);
            })
            .unwrap();
            for o in &present {
                let local = want[o];
                proptest::prop_assert_eq!(l.find(o), Some(local));
                proptest::prop_assert_eq!(l.find_kind(l.kind_of(local), o), Some(local));
            }
            for o in absent.iter().chain([Oid::ZERO, Oid([0xff; 20])].iter()) {
                if !want.contains_key(o) {
                    proptest::prop_assert_eq!(l.find(o), None);
                }
            }
        }
    }
}
