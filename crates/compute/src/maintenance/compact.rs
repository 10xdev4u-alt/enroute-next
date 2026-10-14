//! Background maintenance of layer manifests (LSM-style), run by the compute
//! holding the repository's maintenance lease.
//!
//! - **Flush.** Inline layers (the memtable: their files are in the log)
//!   move to the object store. A contiguous run of them is merged into one
//!   layer first, so a busy repository costs one PUT per flush rather than
//!   one per push (at ~1000 pushes/s, ~86M PUTs a day) and the store does
//!   not fill with tiny objects; a run of one is uploaded as is. A run is
//!   flushed once its oldest layer has been inline for `flush_interval`, or
//!   once it holds [`FLUSH_BYTES`] or `flush_max_layers`.
//! - **Compaction.** Stored layers accumulate as pushes land. Runs of
//!   layers of about one size are merged into bigger ones, so layer sizes
//!   grow geometrically (see [`pick`]).
//!   Merging copies stored pack entries verbatim (no recompression), drops
//!   duplicate objects, and rewrites the commit graph and segments with
//!   fresh layer-local refs.
//!
//! Either way the run is swapped for its replacement with one
//! compare-and-swap on the manifest, so concurrent pushes are never blocked
//! and a lost race is harmless: outputs are new files (layer ids are
//! never reused). Replaced layers, and outputs of lost races, are left to
//! the layer sweep ([`super::sweep`]), which deletes files nothing names.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use enroute_shared::state::{Command, Commit, LayerMeta, RepoMeta};

use crate::compute::ComputeConfig;
use crate::repo::merge::{Merged, WINDOW, merge_layers};
use crate::repo::view::LayerView;
use crate::storage::{Sealed, new_layer_id, stored_meta};

use super::Maintainer;

/// Flushes of one repository in flight on a node at most. Under heavy
/// writes inline layers arrive faster than one flush at a time (merge,
/// upload, swap) moves them, and every layer left inline makes lookups and
/// view builds slower; flushes of disjoint runs swap independently.
const MAX_FLUSHES: usize = 4;

/// A run of inline layers this big is flushed whatever its age.
pub(crate) const FLUSH_BYTES: u64 = 32 << 20;

/// Compactions of one repository in flight on a node at most. A big merge
/// can take minutes; small layers keep merging next to it meanwhile.
const MAX_COMPACTIONS: usize = 2;

/// Compactions in flight on a node at most, over all repositories: they
/// share its local disk and network. Many repositories are due at once
/// after a change of policy, or of `merge_factor`.
const MAX_NODE_COMPACTIONS: usize = 4;

/// Layers below this size are in the smallest size class: merging tiny
/// flushes by exact size buys nothing. Small, because a layer of this class
/// is rewritten each time new layers of the class merge into it.
const MIN_CLASS_BYTES: u64 = 64 << 10;

/// This node's maintenance: what it has in flight per repository, and a
/// wake-up for the next tick.
#[derive(Default)]
pub(super) struct Maintenance {
    repos: parking_lot::Mutex<HashMap<String, InFlight>>,
    /// Wakes maintenance before its next tick (a flush finished, and more
    /// may be due).
    wake: tokio::sync::Notify,
}

#[derive(Default)]
struct InFlight {
    /// When this node first saw each inline layer, by first id (flush age;
    /// on every replica, so a new maintainer knows them too).
    seen: BTreeMap<u32, Instant>,
    /// The id ranges of inline runs being flushed (several may be, on
    /// disjoint layers).
    flushes: Vec<Range<u64>>,
    /// The id ranges of stored runs being compacted. They are disjoint from
    /// each other and from flushes, so no swap invalidates another.
    compactions: Vec<Range<u64>>,
}

impl InFlight {
    fn is_idle(&self) -> bool {
        self.flushes.is_empty() && self.compactions.is_empty()
    }
}

/// A flush or a compaction of the layers in an id range, in flight; its
/// layers are free again once it is dropped.
struct Job {
    maintenance: Arc<Maintenance>,
    repo: String,
    ids: Range<u64>,
}

impl Drop for Job {
    fn drop(&mut self) {
        let mut repos = self.maintenance.repos.lock();
        if let Some(w) = repos.get_mut(&self.repo) {
            // Flushes and compactions hold disjoint ranges.
            w.flushes.retain(|r| *r != self.ids);
            w.compactions.retain(|r| *r != self.ids);
        }
    }
}

pub(super) fn spawn(node: &Arc<Maintainer>) {
    super::every(
        node,
        |n| {
            let m = n.in_flight.clone();
            async move {
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(100)) => {}
                    () = m.wake.notified() => {}
                }
            }
        },
        |n| async move { tick(&n) },
    );
}

/// Start the work due for each repository this node follows, if this node
/// holds (or can take) the repository's maintenance lease: a flush if one
/// is due, and compactions if some are. They run independently (a flush
/// never waits for a compaction, which can take minutes), since each takes
/// layers no other job holds. So several can be in flight (up to
/// [`MAX_FLUSHES`] and [`MAX_COMPACTIONS`]).
fn tick(node: &Arc<Maintainer>) {
    let now = Instant::now();
    let names: HashSet<String> = node.states.names().into_iter().collect();
    let mut due: Vec<(Arc<RepoMeta>, Job, Range<usize>)> = Vec::new();
    {
        let mut repos = node.in_flight.repos.lock();
        let mut compactions: usize = repos.values().map(|w| w.compactions.len()).sum();
        for repo in &names {
            let Some(meta) = node.states.get(repo) else {
                continue;
            };
            let w = repos.entry(repo.clone()).or_default();
            let inline = meta.layers.iter().filter(|l| !l.is_stored());
            w.seen = inline
                .map(|l| (l.first_id, w.seen.get(&l.first_id).copied().unwrap_or(now)))
                .collect();
            let job = |ids| Job {
                maintenance: node.in_flight.clone(),
                repo: repo.clone(),
                ids,
            };
            if let Some((run, ids)) = next_flush(w, &meta, now, &node.config) {
                w.flushes.push(ids.clone());
                due.push((meta.clone(), job(ids.clone()), run));
            }
            let room = MAX_NODE_COMPACTIONS.saturating_sub(compactions);
            for (run, ids) in next_compactions(w, &meta, room, &node.config) {
                w.compactions.push(ids.clone());
                compactions += 1;
                due.push((meta.clone(), job(ids), run));
            }
        }
        repos.retain(|r, w| names.contains(r) || !w.is_idle());
    }
    for (meta, job, run) in due {
        tokio::spawn(run_job(node.clone(), meta, job, run));
    }
}

/// Run one flush or compaction of `meta`, then wake maintenance if a flush
/// freed inline layers.
async fn run_job(node: Arc<Maintainer>, meta: Arc<RepoMeta>, job: Job, run: Range<usize>) {
    let repo = job.repo.clone();
    match node.leases.hold(&repo).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            tracing::warn!(repo, "taking the maintenance lease: {e:#}");
            return;
        }
    }
    match replace_run(&node, &repo, &meta, run).await {
        Err(e) => tracing::warn!(repo, "layer maintenance failed (will retry): {e:#}"),
        Ok(Some(version)) => {
            // Keep its layers marked until this node's state shows the
            // swap: picked again before that, they would be merged and
            // uploaded again only for the ledger to reject the swap (under
            // load, hundreds of times a minute).
            drop(node.repos.handle(&repo).wait_version(version).await);
            // More may be due already: look again now, with this job's
            // layers free.
            drop(job);
            node.in_flight.wake.notify_one();
        }
        Ok(None) => {}
    }
}

/// The disjoint runs of stored layers, of `sizes`, that are due to merge,
/// oldest first: none if compaction is off.
fn compaction(sizes: &[u64], config: &ComputeConfig) -> Vec<Range<usize>> {
    // A merge of one layer would only rewrite it, again and again.
    if config.merge_factor < 2 {
        return Vec::new();
    }
    pick(sizes, config.merge_factor, MIN_CLASS_BYTES)
}

/// Whether maintenance has nothing left to do on `meta`: every layer is
/// stored, and no run is due to merge.
#[must_use]
pub fn settled(meta: &RepoMeta, config: &ComputeConfig) -> bool {
    let sizes = stored_sizes(&meta.layers);
    sizes.len() == meta.layers.len() && compaction(&sizes, config).is_empty()
}

/// The runs of stored layers of `meta` due to merge that no compaction in
/// flight holds, with their id ranges: at most `room`, and only as many as
/// [`MAX_COMPACTIONS`] allows.
fn next_compactions(
    w: &InFlight,
    meta: &RepoMeta,
    room: usize,
    config: &ComputeConfig,
) -> Vec<(Range<usize>, Range<u64>)> {
    let room = room.min(MAX_COMPACTIONS.saturating_sub(w.compactions.len()));
    if room == 0 {
        return Vec::new();
    }
    let mut runs = compaction(&stored_sizes(&meta.layers), config);
    // The newest run goes first: big old merges must not take every slot
    // while small layers pile up.
    if let Some(newest) = runs.pop() {
        runs.insert(0, newest);
    }
    runs.into_iter()
        .map(|run| {
            let ids = id_range(meta, &run);
            (run, ids)
        })
        .filter(|(_, ids)| !w.compactions.iter().any(|r| overlaps(r, ids)))
        .take(room)
        .collect()
}

/// The id range of layers `run` of `meta`.
#[expect(
    clippy::indexing_slicing,
    reason = "runs are non-empty ranges of meta.layers"
)]
fn id_range(meta: &RepoMeta, run: &Range<usize>) -> Range<u64> {
    let layers = &meta.layers[run.clone()];
    u64::from(layers[0].first_id)..layers[layers.len() - 1].end_id()
}

fn overlaps(a: &Range<u64>, b: &Range<u64>) -> bool {
    a.start < b.end && b.start < a.end
}

/// The next run of inline layers of `meta` due to flush, with its id range,
/// if this node may flush more of the repository now.
fn next_flush(
    w: &InFlight,
    meta: &RepoMeta,
    now: Instant,
    config: &ComputeConfig,
) -> Option<(Range<usize>, Range<u64>)> {
    if w.flushes.len() >= MAX_FLUSHES {
        return None;
    }
    let moving = |l: &LayerMeta| w.flushes.iter().any(|r| r.contains(&u64::from(l.first_id)));
    let runs = inline_runs(meta.layers.iter().map(|l| (l, moving(l))));
    due_run(meta, &w.seen, runs, now, config)
}

/// The first of `runs` due to flush at `now`, with its layers' id range.
fn due_run(
    meta: &RepoMeta,
    seen: &BTreeMap<u32, Instant>,
    runs: Vec<Range<usize>>,
    now: Instant,
    config: &ComputeConfig,
) -> Option<(Range<usize>, Range<u64>)> {
    runs.into_iter().find_map(|run| {
        #[expect(clippy::indexing_slicing, reason = "runs are ranges of meta.layers")]
        let layers = &meta.layers[run.clone()];
        let ids = id_range(meta, &run);
        let oldest = layers
            .iter()
            .filter_map(|l| seen.get(&l.first_id))
            .min()
            .copied()
            .unwrap_or(now);
        let due = now.duration_since(oldest) >= config.flush_interval
            || layers.iter().map(|l| l.bytes).sum::<u64>() >= FLUSH_BYTES
            || run.len() >= config.flush_max_layers;
        due.then_some((run, ids))
    })
}

/// Maximal contiguous runs of inline layers, of those not excluded (being
/// flushed already).
fn inline_runs<'a>(layers: impl IntoIterator<Item = (&'a LayerMeta, bool)>) -> Vec<Range<usize>> {
    let mut runs: Vec<Range<usize>> = Vec::new();
    for (i, (l, excluded)) in layers.into_iter().enumerate() {
        if l.is_stored() || excluded {
            continue;
        }
        match runs.last_mut() {
            Some(r) if r.end == i => r.end = i + 1,
            _ => runs.push(i..i + 1),
        }
    }
    runs
}

/// Choose which layers to merge: disjoint runs of at least `factor`
/// adjacent layers of about one size, oldest first.
///
/// This is a log-structured merge policy over a list that must stay in id
/// order, as Lucene's `LogMergePolicy` is for segments in document order.
/// From the oldest layer on, a stretch reaches to the last layer within
/// `factor` of the largest one left (sizes under `floor` count as `floor`),
/// and its layers merge `factor` at a time, or more, up to `factor` times
/// the largest one, when a backlog has built up. Smaller layers between them
/// join their merges, so none is left behind. A merge makes its output about
/// `factor` times bigger than its inputs. So each byte is rewritten about
/// once per size class (log base `factor` of the repository size over
/// `floor`), and a repository has fewer than `factor` stored layers per
/// size class. `factor` is at least 2.
#[expect(clippy::indexing_slicing, reason = "indexes are within `sizes`")]
fn pick(sizes: &[u64], factor: usize, floor: u64) -> Vec<Range<usize>> {
    let wide = u64::try_from(factor).unwrap_or(u64::MAX);
    let size = |i: usize| sizes[i].max(floor);
    let n = sizes.len();
    let mut runs = Vec::new();
    let mut start = 0;
    while start < n {
        let largest = (start..n).map(size).max().unwrap_or(floor);
        let end = (start..n)
            .rev()
            .find(|&i| size(i).saturating_mul(wide) >= largest)
            .map_or(n, |i| i + 1);
        // A backlog merges in wide runs: one merge per `factor` layers would
        // drain only `factor - 1` layers per merge. The byte cap keeps each
        // merge short.
        let cap = largest.saturating_mul(wide);
        let mut i = start;
        while i < end {
            let (mut j, mut bytes) = (i, 0u64);
            while j < end && (j - i < factor || bytes.saturating_add(sizes[j]) <= cap) {
                bytes = bytes.saturating_add(sizes[j]);
                j += 1;
            }
            if j - i >= factor {
                runs.push(i..j);
            }
            i = j;
        }
        start = end;
    }
    runs
}

/// Sizes of the stored prefix of a manifest: what compaction may merge
/// (inline layers are flushed first).
fn stored_sizes(layers: &[LayerMeta]) -> Vec<u64> {
    layers
        .iter()
        .take_while(|l| l.is_stored())
        .map(|l| l.bytes)
        .collect()
}

/// Publish merged layer `m` of `name`, under a new id of upload generation
/// `generation`.
async fn publish_merged(
    node: &Maintainer,
    name: &str,
    generation: u64,
    m: Merged,
) -> Result<LayerMeta> {
    let id = new_layer_id(generation);
    let written = node.storage.publish_built(name, &id, m.builder).await?;
    Ok(LayerMeta {
        first_id: m.ids.start,
        id_span: m.ids.end - m.ids.start,
        ..stored_meta(id, &written, m.objects)
    })
}

/// Replace layers `run` of `meta` with stored layers: a single inline layer
/// is uploaded as is (as a new file, under a new id), anything else merged
/// first. New files are of `meta`'s upload generation. Returns the
/// repository's version with the swap, unless it did not happen (the
/// manifest moved on).
async fn replace_run(
    node: &Maintainer,
    name: &str,
    meta: &RepoMeta,
    run: Range<usize>,
) -> Result<Option<u64>> {
    #[expect(
        clippy::indexing_slicing,
        reason = "the run is of this manifest's layers"
    )]
    let replaced = &meta.layers[run.clone()];
    let t0 = Instant::now();
    let added = match replaced {
        [l] if !l.is_stored() => {
            let sealed = Sealed::of(l.data.clone().unwrap_or_default())?;
            let id = new_layer_id(meta.upload_gen);
            node.storage.publish_layer(name, &id, sealed).await?;
            vec![LayerMeta {
                id,
                data: None,
                ..l.clone()
            }]
        }
        _ => {
            let snap = node.repos.handle(name).wait_version(meta.version).await?;
            #[expect(clippy::indexing_slicing, reason = "a run has at least one layer")]
            let start = snap
                .layers
                .partition_point(|lv| lv.base < replaced[0].first_id);
            let same =
                |w: &[Arc<LayerView>]| w.iter().zip(replaced).all(|(lv, m)| lv.layer.id == m.id);
            if !snap
                .layers
                .get(start..start + replaced.len())
                .is_some_and(same)
            {
                return Ok(None); // the manifest moved on; next tick decides again
            }
            let out = merge_layers(
                &snap,
                start..start + replaced.len(),
                node.storage.workspace(),
                WINDOW,
            )
            .await?;
            // A run of nothing but copies of older objects goes away.
            match out {
                Some(m) => vec![publish_merged(node, name, meta.upload_gen, m).await?],
                None => Vec::new(),
            }
        }
    };
    let (objects, bytes) = added.iter().fold((0u64, 0u64), |(o, b), l| {
        (o + u64::from(l.objects), b + l.bytes)
    });
    let res = node
        .propose(
            Command::commit(
                name,
                Commit {
                    add_layers: added.clone(),
                    remove_layers: replaced.iter().map(Into::into).collect(),
                    updates: vec![],
                    atomic: true,
                    ..Default::default()
                },
            )
            .at(meta.epoch),
        )
        .await?;
    if let Some(e) = res.error {
        // The outputs are left to the layer sweep. (A slow merge whose
        // upload generation expired lands here too, and is done again.)
        tracing::debug!(
            repo = name,
            "another writer changed the layers first: {e:#}"
        );
        return Ok(None);
    }
    tracing::debug!(
        repo = name,
        replaced = replaced.len(),
        outputs = added.len(),
        objects,
        bytes,
        ms = crate::util::millis(t0.elapsed()),
        "replaced layers"
    );
    Ok(Some(res.repo_version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_classes_merge_factor_at_a_time() {
        // Too few of a size class: nothing.
        assert_eq!(pick(&[100, 1, 1, 1], 4, 1), Vec::<Range<usize>>::new());
        // Four small layers on top of a big one: only they merge.
        assert_eq!(pick(&[1000, 10, 10, 10, 10], 4, 1), vec![1..5]);
        // Under the floor, sizes do not matter, and a backlog merges at
        // once up to `factor` times the class size.
        assert_eq!(pick(&[5, 1, 9, 2, 1, 1, 7, 3], 4, 10), vec![0..8]);
        assert_eq!(pick(&[9; 10], 4, 10), vec![0..4, 4..8]);
        assert_eq!(pick(&[1; 100], 4, 10), vec![0..40, 40..80, 80..100]);
        // A smaller layer between two of a class joins their merge.
        assert_eq!(pick(&[100, 1, 100, 100, 100, 10], 4, 1), vec![0..4]);
        // Classes merge independently, oldest first.
        assert_eq!(
            pick(&[1000, 1000, 1000, 1000, 10, 10, 10, 10], 4, 1),
            vec![0..4, 4..8]
        );
    }

    /// Merge `sizes` until nothing is due; returns the bytes written.
    fn compact(sizes: &mut Vec<u64>, factor: usize, floor: u64) -> u64 {
        let mut written = 0;
        let mut runs = pick(sizes, factor, floor);
        while !runs.is_empty() {
            for r in runs.into_iter().rev() {
                let merged: u64 = sizes[r.clone()].iter().sum();
                written += merged;
                sizes.splice(r, [merged]);
            }
            runs = pick(sizes, factor, floor);
        }
        written
    }

    #[test]
    fn write_amplification_and_layer_count_are_logarithmic() {
        // A hot repository: flushes of 10 bytes, up to 10 million bytes.
        let (factor, floor) = (4, 10);
        let mut sizes: Vec<u64> = Vec::new();
        let (mut written, mut flushed) = (0u64, 0u64);
        for _ in 0..1_000_000u64 {
            sizes.push(10);
            flushed += 10;
            written += compact(&mut sizes, factor, floor);
            // log4(10M / 10) = 10 size classes, fewer than 4 layers each.
            assert!(sizes.len() <= 3 * 11, "{} layers", sizes.len());
        }
        assert!(
            written <= 11 * flushed,
            "write amplification {written}/{flushed}"
        );
    }
}
