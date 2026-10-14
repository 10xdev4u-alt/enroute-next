//! Statistics the loop and the apply thread log now and then
//! (`enroute::ledger_timing`).

use std::time::{Duration, Instant};

/// How often the statistics are logged.
const EVERY: Duration = Duration::from_secs(5);

/// Statistics `T` gathered over a window of [`EVERY`].
pub(super) struct Window<T> {
    since: Instant,
    stats: T,
}

impl<T: Default> Window<T> {
    pub(super) fn new() -> Self {
        Window {
            since: Instant::now(),
            stats: T::default(),
        }
    }

    /// Once the window has lasted long enough: its length in seconds and
    /// its statistics (and the next window starts).
    pub(super) fn due(&mut self) -> Option<(f64, T)> {
        let span = self.since.elapsed();
        if span < EVERY {
            return None;
        }
        self.since = Instant::now();
        Some((span.as_secs_f64(), std::mem::take(&mut self.stats)))
    }
}

impl<T> std::ops::Deref for Window<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.stats
    }
}

impl<T> std::ops::DerefMut for Window<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.stats
    }
}

/// `x` per `n` (0 if there were none).
pub(super) fn per(x: u128, n: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    u64::try_from(x / u128::from(n)).unwrap_or(u64::MAX)
}

/// `n` per second over a window of `span` seconds.
pub(super) fn rate(n: u64, span: f64) -> u64 {
    #[expect(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        reason = "log output only; precision loss is harmless"
    )]
    let n = n as f64;
    trunc(n / span)
}

/// The percentage of a window of `span` seconds that `busy` covers.
pub(super) fn pct(busy: Duration, span: f64) -> u64 {
    trunc(100.0 * busy.as_secs_f64() / span)
}

fn trunc(x: f64) -> u64 {
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "float-to-int `as` saturates; wanted for log output"
    )]
    let n = x as u64;
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in for the loop's counters: the window is generic, and this is
    /// about when it reports, not about what it carries.
    #[derive(Default)]
    struct Counters {
        calls: u64,
    }

    /// A window reports every five seconds, not on every call. The numbers it
    /// carries are rates over a window, so a report per call would divide by a
    /// span of microseconds and read as an idle node.
    #[test]
    fn a_window_reports_only_once_it_has_lasted() {
        let mut window = Window::<Counters>::new();
        window.calls = 3;
        assert_eq!(
            window.calls, 3,
            "the counters are written through the window"
        );
        assert!(
            window.due().is_none(),
            "a window that has just opened is not due"
        );

        std::thread::sleep(EVERY + Duration::from_millis(50));

        let (span, stats) = window.due().expect("five seconds have passed");
        assert!(span >= EVERY.as_secs_f64(), "{span}");
        assert_eq!(stats.calls, 3, "the report carries what was counted");
        assert_eq!(window.calls, 0, "the next window starts empty");
        assert!(
            window.due().is_none(),
            "and it is not due again immediately"
        );
    }

    /// `per` divides, truncates, and answers zero rather than dividing by a
    /// window nothing happened in — the state most fresh nodes are in.
    #[test]
    fn per_divides_and_never_divides_by_nothing() {
        assert_eq!(per(0, 0), 0);
        assert_eq!(per(7, 0), 0);
        assert_eq!(per(10, 4), 2, "whole numbers only");
        assert_eq!(per(u128::MAX, 1), u64::MAX, "saturating, not wrapping");
    }

    /// Rates are per second and percentages are of the window. Both truncate:
    /// these are log lines, and a rounding rule nobody reads is noise in them.
    #[test]
    fn rates_and_percentages_truncate() {
        assert_eq!(rate(300, 5.0), 60);
        assert_eq!(rate(1, 2.0), 0);
        assert_eq!(rate(0, 5.0), 0);
        assert_eq!(pct(Duration::from_secs(1), 5.0), 20);
        assert_eq!(pct(Duration::from_millis(1), 5.0), 0);
        assert_eq!(pct(Duration::from_secs(5), 5.0), 100);
    }
}
