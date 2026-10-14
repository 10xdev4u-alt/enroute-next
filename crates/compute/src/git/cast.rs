//! Saturating integer casts, here because every module may name `git`.

/// `n` as an index. Saturates on a target where `usize` is narrower: an
/// index that large is out of range, so checked access fails instead of
/// reaching a wrong element.
pub(crate) fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// A `u32` count or index as a `usize`, as [`to_usize`] does.
pub(crate) fn ix(n: u32) -> usize {
    to_usize(u64::from(n))
}

/// `n` as a `u64`. Saturates; `usize` is never wider than `u64` on
/// supported targets.
pub(crate) fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}
