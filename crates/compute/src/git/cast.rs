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

#[cfg(test)]
mod tests {
    use super::*;

    /// A value the target holds crosses unchanged, in both directions.
    #[test]
    fn a_representable_value_is_unchanged() {
        for n in [0, 1, 4096, u64::from(u32::MAX), 1 << 40] {
            match usize::try_from(n) {
                Ok(small) => assert_eq!((to_usize(n), to_u64(small)), (small, n), "{n}"),
                Err(_) => assert_eq!(to_usize(n), usize::MAX, "{n} did not saturate"),
            }
        }
    }

    /// What the names promise: a value too large for the target saturates.
    /// A wrapped one lands back inside the range, where the checked access
    /// this exists for would read a wrong element instead of failing.
    #[test]
    fn a_value_too_large_saturates_rather_than_wrapping() {
        assert_eq!(to_usize(u64::MAX), usize::MAX);
        assert_eq!(
            to_usize(u64::from(u32::MAX) + 1),
            usize::try_from(u64::from(u32::MAX) + 1).unwrap_or(usize::MAX)
        );
        // Non-decreasing throughout: only the clamp can flatten it.
        let mut last = 0;
        for n in [0, 1, 1 << 20, u64::from(u32::MAX), u64::MAX] {
            let at = to_usize(n);
            assert!(at >= last, "to_usize({n}) went backwards");
            last = at;
        }
    }

    /// The width a `u32` index has on the targets this runs on.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn a_u32_index_is_widened_exactly() {
        assert_eq!(ix(u32::MAX), 4_294_967_295);
        assert_eq!(ix(0), 0);
        assert_eq!(ix(1 << 31), 2_147_483_648);
    }
}
