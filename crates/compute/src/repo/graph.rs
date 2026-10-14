//! Walks over a snapshot's commit graph: ancestry, merge bases and
//! first-parent history.

use std::collections::BinaryHeap;

use anyhow::{Context, Result};
use foldhash::{HashMap, HashSet, HashSetExt};

use super::view::{CommitNode, DenseId, Snapshot};

/// The graph node of a commit the snapshot holds.
fn node(snap: &Snapshot, d: DenseId) -> Result<CommitNode<'_>> {
    snap.commit(d)
        .with_context(|| format!("commit {} has no graph node", snap.oid(d)))
}

/// Whether `target` is one of `from` or an ancestor of one.
pub(crate) fn reaches(
    snap: &Snapshot,
    from: impl IntoIterator<Item = DenseId>,
    target: DenseId,
) -> Result<bool> {
    let mut seen: HashSet<DenseId> = HashSet::new();
    let mut stack: Vec<DenseId> = from.into_iter().filter(|&d| seen.insert(d)).collect();
    if stack.contains(&target) {
        return Ok(true);
    }
    // Generations fall strictly along parent edges, so nothing at or below
    // the target's generation (other than the target) leads to it.
    let floor = node(snap, target)?.generation;
    while let Some(c) = stack.pop() {
        for &p in node(snap, c)?.parents {
            if p == target {
                return Ok(true);
            }
            if seen.insert(p) && node(snap, p)?.generation > floor {
                stack.push(p);
            }
        }
    }
    Ok(false)
}

const FROM_A: u8 = 1;
const FROM_B: u8 = 2;
const STALE: u8 = 4;

/// Git's `paint_down_to_common`, highest generation first: every commit is
/// taken after all of its descendants, so its marks are final when it is.
///
/// The result therefore needs no reduction: a common commit below another
/// one is stale when it is taken.
pub(crate) fn paint_down_to_common(
    snap: &Snapshot,
    a: DenseId,
    b: DenseId,
    budget: usize,
) -> Result<(Vec<DenseId>, bool)> {
    if a == b {
        return Ok((vec![a], false));
    }
    let mut marks: HashMap<DenseId, u8> = HashMap::from_iter([(a, FROM_A), (b, FROM_B)]);
    let mut queue: BinaryHeap<(u32, DenseId)> = BinaryHeap::new();
    let mut queued: HashSet<DenseId> = HashSet::from_iter([a, b]);
    queue.push((node(snap, a)?.generation, a));
    queue.push((node(snap, b)?.generation, b));
    // Queued commits not yet marked stale: the walk ends when none are left.
    let mut live = 2usize;
    let mut found = Vec::new();
    let mut taken = 0usize;
    while live > 0 {
        let Some((_, c)) = queue.pop() else { break };
        queued.remove(&c);
        let mut m = marks.get(&c).copied().unwrap_or_default();
        if m & STALE == 0 {
            live -= 1;
        }
        taken += 1;
        if taken > budget {
            return Ok((found, true));
        }
        if m == FROM_A | FROM_B {
            found.push(c);
            m |= STALE;
        }
        for &p in node(snap, c)?.parents {
            let pm = marks.entry(p).or_default();
            if *pm & m == m {
                continue;
            }
            let was_stale = *pm & STALE != 0;
            *pm |= m;
            let stale = *pm & STALE != 0;
            if queued.insert(p) {
                queue.push((node(snap, p)?.generation, p));
                live += usize::from(!stale);
            } else if stale && !was_stale {
                live -= 1;
            }
        }
    }
    Ok((found, false))
}

/// `tip`, then its first parent, and so on: at most `limit` commits.
pub(crate) fn first_parents(snap: &Snapshot, tip: DenseId, limit: usize) -> Result<Vec<DenseId>> {
    let mut page = Vec::new();
    let mut at = Some(tip);
    while let Some(c) = at {
        if page.len() >= limit {
            break;
        }
        page.push(c);
        at = node(snap, c)?.parents.first().copied();
    }
    Ok(page)
}
