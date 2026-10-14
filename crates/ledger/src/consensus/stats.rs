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
