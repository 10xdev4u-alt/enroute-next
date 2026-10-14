//! Encoding: each new object whole or as a delta against its assigned
//! base, added to the layer in member order.

use anyhow::{Context, Result};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::delta;
use crate::git::oid::{Kind, Oid};
use crate::git::pack::{full_entry, ref_delta_entry};
use crate::repo::view::{DenseId, must};
use crate::storage::{BitSet, LayerBuilder, NewObject, Sealed, TempTable, Workspace};
use crate::util::{ix, to_u64, to_usize};

use super::segments::TreeDeltas;
use super::{Assign, Ctx};

const MAX_DEPTH: u8 = 50;
/// Trees get longer chains. Every version of a path deltas against the
/// previous one, so a large, often-changed tree (git.git's `t/`, ~60 KiB) is
/// stored whole once per chain; at depth 50 those copies were half of all
/// tree bytes. Tree deltas are small and cheap to apply.
const MAX_TREE_DEPTH: u8 = 100;

fn max_depth(kind: Kind) -> u8 {
    if kind == Kind::Tree {
        MAX_TREE_DEPTH
    } else {
        MAX_DEPTH
    }
}
const MAX_DELTA_SOURCE: usize = 32 << 20;

/// Whether an object of `size` bytes may be stored as a delta.
pub(super) fn delta_size(size: usize) -> bool {
    (64..=MAX_DELTA_SOURCE).contains(&size)
}

/// No delta base assigned, in the result of [`base_entries`].
const NO_BASE: u32 = u32::MAX;
/// The delta base is not a new pack entry.
const OLD_BASE: u32 = u32::MAX - 1;

/// An assigned delta base and the entry it was assigned to.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct BaseRef {
    base: Oid,
    entry: u32,
}

/// The pack entry of each entry's delta base (or [`NO_BASE`], [`OLD_BASE`]),
/// and the snapshot objects used as bases. Done in oid order: random lookups
/// read from disk once the oid index is larger than the buffer pool.
pub(super) fn base_entries(
    cx: &Ctx,
    assign: &TempTable<Assign>,
    ws: &Workspace,
) -> Result<(Vec<u32>, Vec<DenseId>)> {
    let mut refs = TempTable::new(ws);
    for (i, a) in assign.iter().enumerate() {
        if !a.base.is_zero() {
            let entry = u32::try_from(i).context("pack has too many objects")?;
            refs.push(BaseRef {
                base: a.base,
                entry,
            });
        }
    }
    refs.sort_by_key(|r| r.base);
    let mut entries = vec![NO_BASE; cx.idx.len()];
    let mut old = Vec::new();
    for r in refs.iter() {
        let b = match cx.idx.lookup(&r.base) {
            Some(b) if cx.new.contains(b) => {
                u32::try_from(b).context("pack has too many objects")?
            }
            Some(_) => OLD_BASE,
            // Encode reads no tree bases (see `Encoder::encode_one`).
            None if cx.idx.kind(ix(r.entry)) == Kind::Tree => OLD_BASE,
            None => {
                old.extend(cx.snap.lookup(&r.base));
                OLD_BASE
            }
        };
        *must(entries.get_mut(ix(r.entry)), "pack entry in range") = b;
    }
    old.sort_unstable();
    old.dedup();
    Ok((entries, old))
}

/// An encoded object: kind, size, stored entry and, for a delta, its base
/// and base context.
type Encoded = (Kind, u64, Vec<u8>, Option<(Oid, Oid)>);

/// Objects per block of encoding work. Object costs are very uneven: small
/// blocks with work stealing.
const ENCODE_BLOCK: usize = 128;
/// Encode window by window and hand each window to the builder (which spills
/// to disk), so encoded entries never all sit in memory at once.
const ENCODE_WINDOW: usize = 64 * ENCODE_BLOCK;

/// Encodes pack entries as stored entries: whole or as a delta against the
/// base chosen for them.
struct Encoder<'a> {
    cx: &'a Ctx,
    assign: &'a TempTable<Assign>,
    tree_deltas: &'a TreeDeltas,
    base_of: &'a [u32],
    /// Entries planned as deltas.
    plan_delta: BitSet,
}

impl Encoder<'_> {
    fn deltifiable(&self, i: usize, size: usize) -> bool {
        self.plan_delta.contains(i) && delta_size(size)
    }

    fn try_delta(&self, i: usize, kind: Kind, content: &[u8]) -> Result<Option<Encoded>> {
        let Assign { base, ctx } = self.assign.get(i);
        let cx = self.cx;
        let base_content = match self.base_of.get(i) {
            Some(&b) if b < OLD_BASE => cx.idx.content(ix(b), &|o| cx.external(o), &cx.cache)?,
            _ => cx.content(&base)?,
        };
        if base_content.len() > MAX_DELTA_SOURCE {
            return Ok(None);
        }
        Ok(
            delta::encode(&base_content, content, content.len() / 2).map(|d| {
                (
                    kind,
                    to_u64(content.len()),
                    ref_delta_entry(&base, &d),
                    Some((base, ctx)),
                )
            }),
        )
    }

    // Client deltas are compressed again: they are small, so that costs
    // little.
    fn encode_one(&self, i: usize) -> Result<Encoded> {
        let (cx, idx) = (self.cx, &self.cx.idx);
        let external = |oid: &Oid| cx.external(oid);
        let kind = idx.kind(i);
        // A tree's delta comes from its diff: a search would only repeat it.
        if kind == Kind::Tree
            && self.plan_delta.contains(i)
            && let Some(e) = self.tree_deltas.with(i, |d| -> Result<Encoded> {
                let Assign { base, ctx } = self.assign.get(i);
                let size = to_u64(delta::target_size(d)?);
                Ok((kind, size, ref_delta_entry(&base, d), Some((base, ctx))))
            })
        {
            return e;
        }
        let search = |size| kind != Kind::Tree && self.deltifiable(i, size);
        if let Some(size) = idx.whole_size(i) {
            if search(to_usize(size))
                && let Some(e) = self.try_delta(i, kind, &idx.content(i, &external, &cx.cache)?)?
            {
                return Ok(e);
            }
            return Ok((kind, size, idx.whole_entry(i)?, None));
        }
        let content = idx.content(i, &external, &cx.cache)?;
        if search(content.len())
            && let Some(e) = self.try_delta(i, kind, &content)?
        {
            return Ok(e);
        }
        Ok((
            kind,
            to_u64(content.len()),
            full_entry(kind, &content),
            None,
        ))
    }
}

/// The entries of `member_order` to encode as deltas, and all its entries.
/// Planned depths assume every delta is accepted (an overestimate), which
/// makes objects independent: they encode in parallel, then actual depths
/// are fixed up sequentially.
fn plan_deltas(
    cx: &Ctx,
    member_order: &TempTable<u32>,
    assign: &TempTable<Assign>,
    base_of: &[u32],
) -> (BitSet, BitSet) {
    let idx = &cx.idx;
    let mut planned = vec![0u8; idx.len()];
    let mut is_planned = BitSet::new(idx.len());
    let mut plan_delta = BitSet::new(idx.len());
    for i in member_order.iter() {
        let i = ix(i);
        let b = base_of.get(i).copied().unwrap_or(NO_BASE);
        let bd = match b {
            NO_BASE => None,
            OLD_BASE => old_depth(cx, &assign.get(i).base),
            b => is_planned
                .contains(ix(b))
                .then(|| planned.get(ix(b)).copied())
                .flatten(),
        };
        let max = max_depth(idx.kind(i));
        let p = bd.filter(|&d| d < max).map(|d| d + 1);
        *must(planned.get_mut(i), "pack entry in range") = p.unwrap_or(0);
        is_planned.insert(i);
        if p.is_some() {
            plan_delta.insert(i);
        }
    }
    (plan_delta, is_planned)
}

/// Depth of `base` in the snapshot.
fn old_depth(cx: &Ctx, base: &Oid) -> Option<u8> {
    cx.snap.lookup(base).map(|d| cx.snap.delta_depth(d))
}

/// Actual delta depths of the entries added to the layer so far.
struct Depths {
    depths: Vec<u8>,
    known: BitSet,
}

impl Depths {
    fn new(n: usize) -> Depths {
        Depths {
            depths: vec![0; n],
            known: BitSet::new(n),
        }
    }

    /// The depth of an entry that is a delta against `base`, at entry `b` of
    /// [`base_entries`]. A base that is not known yet counts as the deepest.
    fn of_delta(&self, cx: &Ctx, b: u32, base: &Oid) -> u8 {
        let bd = match b {
            NO_BASE | OLD_BASE => old_depth(cx, base),
            b if self.known.contains(ix(b)) => self.depths.get(ix(b)).copied(),
            _ => None,
        };
        bd.unwrap_or(MAX_DEPTH) + 1
    }

    fn set(&mut self, i: usize, depth: u8) {
        *must(self.depths.get_mut(i), "pack entry in range") = depth;
        self.known.insert(i);
    }
}

/// Add the encoded entry `i` to `builder`, with its actual depth.
fn add_encoded(
    cx: &Ctx,
    builder: &mut LayerBuilder,
    depths: &mut Depths,
    (i, b): (usize, u32),
    (kind, size, entry, base): EncodedRef<'_>,
) -> Result<()> {
    let depth = base.map_or(0, |(base, _)| depths.of_delta(cx, b, &base));
    depths.set(i, depth);
    builder.add(NewObject {
        oid: cx.idx.oid(i),
        kind,
        size,
        entry,
        base: base.map(|x| x.0),
        base_ctx: base.map(|x| x.1),
        depth,
        id: None,
    })
}

/// [`Encoded`], with the entry borrowed.
type EncodedRef<'a> = (Kind, u64, &'a [u8], Option<(Oid, Oid)>);

/// An encoded entry held until its turn in member order: where its bytes
/// are in the entry log, and the rest of [`Encoded`]. A zero `base` means
/// a whole entry.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct EncodedRec {
    at: u64,
    size: u64,
    len: u32,
    kind: u8,
    _pad: [u8; 3],
    base: Oid,
    ctx: Oid,
}

/// Encoded entries, appended in the order they are encoded to temp tables
/// that spill to disk.
struct Spilled {
    /// Position in `recs` of each pack entry's record.
    slot: TempTable<u32>,
    recs: TempTable<EncodedRec>,
    log: TempTable<u8>,
}

impl Spilled {
    fn new(n: usize, ws: &Workspace) -> Spilled {
        Spilled {
            slot: TempTable::filled(ws, n, 0),
            recs: TempTable::new(ws),
            log: TempTable::new(ws),
        }
    }

    fn put(&mut self, i: usize, (kind, size, entry, base): &Encoded) -> Result<()> {
        let (base, ctx) = base.unwrap_or((Oid::ZERO, Oid::ZERO));
        self.slot.set(
            i,
            u32::try_from(self.recs.len()).context("pack has too many objects")?,
        );
        self.recs.push(EncodedRec {
            at: to_u64(self.log.len()),
            size: *size,
            len: u32::try_from(entry.len()).context("entry too large")?,
            kind: kind.code(),
            _pad: [0; 3],
            base,
            ctx,
        });
        self.log.extend_from_slice(entry);
        Ok(())
    }

    /// Call `f` with the encoded entry `i`.
    fn with<R>(&self, i: usize, f: impl FnOnce(EncodedRef<'_>) -> R) -> Result<R> {
        let r = self.recs.get(ix(self.slot.get(i)));
        let at = to_usize(r.at);
        let kind = Kind::from_code(r.kind).context("bad object type")?;
        let base = (!r.base.is_zero()).then_some((r.base, r.ctx));
        Ok(self
            .log
            .with_slice(at..at + ix(r.len), |entry| f((kind, r.size, entry, base))))
    }
}

/// Planned delta depths, then the encoded objects added to `builder` in
/// `member_order`.
pub(super) fn encode(
    cx: &Ctx,
    member_order: &TempTable<u32>,
    assign: &TempTable<Assign>,
    tree_deltas: &TreeDeltas,
    base_of: &[u32],
    mut builder: LayerBuilder,
    ws: &Workspace,
) -> Result<(Sealed, u32)> {
    let (plan_delta, members) = plan_deltas(cx, member_order, assign, base_of);
    let enc = Encoder {
        cx,
        assign,
        tree_deltas,
        base_of,
        plan_delta,
    };
    // Encode in walk order, so that reading each entry rebuilds its delta
    // chain once, then add the entries in member order.
    let order: Vec<u32> = cx
        .idx
        .walk_order()
        .filter(|&i| members.contains(ix(i)))
        .collect();
    let mut done = Spilled::new(cx.idx.len(), ws);
    for window in order.chunks(ENCODE_WINDOW) {
        let encoded = crate::util::par_blocks(
            window.len().div_ceil(ENCODE_BLOCK),
            |b| -> Result<Vec<Encoded>> {
                window
                    .chunks(ENCODE_BLOCK)
                    .nth(b)
                    .unwrap_or_default()
                    .iter()
                    .map(|&i| enc.encode_one(ix(i)))
                    .collect()
            },
        );
        let mut entries = window.iter();
        for part in encoded {
            for e in part? {
                done.put(ix(*must(entries.next(), "an entry per encoded object")), &e)?;
            }
        }
    }
    let mut depths = Depths::new(cx.idx.len());
    for i in member_order.iter() {
        done.with(ix(i), |e| {
            let b = base_of.get(ix(i)).copied().unwrap_or(NO_BASE);
            add_encoded(cx, &mut builder, &mut depths, (ix(i), b), e)
        })??;
    }
    let count = u32::try_from(builder.object_count()).context("too many objects")?;
    Ok((builder.finish()?, count))
}
