use std::sync::Arc;

pub(crate) mod coalesce;

pub(crate) use crate::git::cast::{ix, to_u64, to_usize};

/// Run `f` on the blocking pool.
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(f).await?
}

/// The items `rx` delivers, as a stream.
pub(crate) fn received<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
) -> impl futures::Stream<Item = T> + Send + 'static {
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|x| (x, rx)) })
}

/// A pack sink that sends each chunk, as `frame` makes it, into `tx`. The
/// pack writer runs on blocking threads, so the sink sends synchronously.
pub(crate) fn channel_sink<E: Send + 'static>(
    tx: tokio::sync::mpsc::Sender<Result<bytes::Bytes, E>>,
    mut frame: impl FnMut(&[u8]) -> bytes::Bytes + Send + 'static,
) -> crate::repo::serve::Sink {
    Box::new(move |chunk| {
        tx.blocking_send(Ok(frame(chunk)))
            .map_err(|_gone| anyhow::anyhow!("the pack's reader went away"))
    })
}

/// A cache of shared byte buffers (decoded objects, pack entries) bounded
/// by their total size: `quick_cache`'s sharded, size-weighted cache.
pub(crate) type ByteCache<K> = quick_cache::sync::Cache<K, Arc<Vec<u8>>, ByteWeight>;

/// Make a [`ByteCache`] of at most `bytes`.
pub(crate) fn byte_cache<K: std::hash::Hash + Eq>(bytes: usize) -> ByteCache<K> {
    // Sized for objects of a few KiB (trees, source files).
    let weight = u64::try_from(bytes).unwrap_or(u64::MAX);
    quick_cache::sync::Cache::with_weighter((bytes / 4096).max(64), weight, ByteWeight)
}

/// A cached buffer's weight: its bytes plus a fixed per-entry overhead
/// (allocation headers and rounding, the `Arc`, the cache's slot), which
/// dominates for the small objects typical of trees and source files.
#[derive(Clone, Debug)]
pub(crate) struct ByteWeight;

impl<K> quick_cache::Weighter<K, Arc<Vec<u8>>> for ByteWeight {
    fn weight(&self, _: &K, v: &Arc<Vec<u8>>) -> u64 {
        u64::try_from(v.len())
            .unwrap_or(u64::MAX)
            .saturating_add(128)
    }
}

/// A duration in whole microseconds, saturated at `u64::MAX`.
pub(crate) fn micros(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// A duration in whole milliseconds, saturated at `u64::MAX`.
pub(crate) fn millis(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// `t` in Unix milliseconds (0 before 1970).
pub(crate) fn unix_ms(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH).map_or(0, millis)
}

/// Now, in Unix milliseconds.
pub(crate) fn now_ms() -> u64 {
    unix_ms(std::time::SystemTime::now())
}

/// Start the process-wide pool for CPU-bound work (encoding, compression,
/// replay): `threads` workers (0: one per core). Every parallel step runs
/// on it, so concurrent requests share the cores instead of each spawning
/// its own threads. Work on the pool must not block on I/O from other
/// requests or on clients. Later calls (e.g. several nodes in one test
/// process) keep the first pool.
pub fn init_cpu_pool(threads: usize) {
    // Fails when a pool already exists; the first pool stays.
    drop(
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("enroute-cpu-{i}"))
            .build_global(),
    );
}

/// Run `work(0..blocks)` on the CPU pool; results in block order.
pub(crate) fn par_blocks<T: Send>(blocks: usize, work: impl Fn(usize) -> T + Sync) -> Vec<T> {
    use rayon::prelude::*;
    if blocks <= 1 {
        return (0..blocks).map(work).collect();
    }
    (0..blocks).into_par_iter().map(&work).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quick_cache::Weighter as _;

    use super::*;

    /// A cached buffer costs its bytes plus a fixed overhead. The overhead is
    /// what makes the entry count in `byte_cache` a count and not an accident:
    /// without it, a cache of many empty buffers would weigh nothing and never
    /// evict.
    #[test]
    fn a_buffer_weighs_its_bytes_plus_a_fixed_overhead() {
        let weight = ByteWeight;
        assert_eq!(weight.weight(&0_u64, &Arc::new(Vec::new())), 128);
        assert_eq!(
            weight.weight(&0_u64, &Arc::new(vec![0_u8; 4096])),
            4096 + 128
        );
        assert_eq!(weight.weight(&7_u64, &Arc::new(vec![0_u8; 1])), 1 + 128);
    }

    /// The weight is a fact about the value, not about the key.
    #[test]
    fn the_key_does_not_change_the_weight() {
        let weight = ByteWeight;
        let value = Arc::new(vec![0_u8; 32]);
        assert_eq!(weight.weight(&1_u64, &value), weight.weight(&2_u64, &value));
    }

    /// A budget of zero bytes admits nothing, because every entry weighs its
    /// bytes plus the per-entry overhead, and 128 is more than zero.
    ///
    /// Asserted rather than changed. It is worth knowing that a zero here
    /// silently turns caching off, where `node.disk_cache` means "no limit" by
    /// the same value. That is a configuration decision, not a test's.
    #[test]
    fn a_cache_of_no_bytes_admits_nothing() {
        let cache: ByteCache<u64> = byte_cache(0);
        cache.insert(1, Arc::new(vec![7_u8]));
        assert!(cache.get(&1).is_none());
    }

    /// The two conversions the engine reads timings through, at the point where
    /// the unit changes.
    #[test]
    fn durations_convert_to_each_unit() {
        assert_eq!(micros(std::time::Duration::from_micros(1_500)), 1_500);
        assert_eq!(millis(std::time::Duration::from_micros(1_500)), 1);
        assert_eq!(millis(std::time::Duration::from_millis(2)), 2);
        assert_eq!(unix_ms(std::time::UNIX_EPOCH), 0);
    }
}
