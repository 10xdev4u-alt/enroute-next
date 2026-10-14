//! Where a call's time goes between a compute and the ledger node answering
//! it (for benchmarks): the compute stamps a request with its wall clock
//! (`x-enroute-sent-us`); the answer carries how late the request reached the
//! handler by this node's clock (`x-enroute-to-us`), how long the handler took
//! (`x-enroute-handler-us`), and when it answered (`x-enroute-done-us`). Clocks
//! of nodes in one region agree to well under a millisecond. Only while
//! `enroute::ledger_call` debug logging is on: otherwise calls carry nothing.

use tonic::{Request, Response};

/// The wall clock, in microseconds since the Unix epoch.
#[must_use]
pub(crate) fn unix_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

fn get(m: &tonic::metadata::MetadataMap, key: &str) -> Option<i64> {
    m.get(key)?.to_str().ok()?.parse().ok()
}

/// Stamp a request with the time it is sent (if calls are timed).
pub fn send<T>(mut req: Request<T>) -> Request<T> {
    if tracing::enabled!(target: "enroute::ledger_call", tracing::Level::DEBUG)
        && let Ok(v) = unix_us().to_string().parse()
    {
        req.metadata_mut().insert("x-enroute-sent-us", v);
    }
    req
}

/// When a stamped request was sent.
pub fn sent<T>(req: &Request<T>) -> Option<i64> {
    get(req.metadata(), "x-enroute-sent-us")
}

/// Stamp an answer to a request sent at `sent` (if it was stamped),
/// handled from `start`.
pub fn stamp<T>(mut res: Response<T>, sent: Option<i64>, start: std::time::Instant) -> Response<T> {
    let Some(sent) = sent else { return res };
    let handler = i64::try_from(start.elapsed().as_micros()).unwrap_or(i64::MAX);
    let now = unix_us();
    let m = res.metadata_mut();
    for (k, v) in [
        ("x-enroute-to-us", now - handler - sent),
        ("x-enroute-handler-us", handler),
        ("x-enroute-done-us", now),
    ] {
        if let Ok(v) = v.to_string().parse() {
            m.insert(k, v);
        }
    }
    res
}

/// Log where the time of a call sent at `t0` went (`enroute::ledger_call`,
/// debug): to the ledger's handler, in it, and back.
pub fn log<T>(call: &str, res: &Result<Response<T>, tonic::Status>, t0: std::time::Instant) {
    let Ok(r) = res else { return };
    let m = r.metadata();
    if let (Some(to), Some(handler), Some(done)) = (
        get(m, "x-enroute-to-us"),
        get(m, "x-enroute-handler-us"),
        get(m, "x-enroute-done-us"),
    ) {
        let total = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
        let back = unix_us() - done;
        tracing::debug!(target: "enroute::ledger_call", call, total_us = total, to_us = to, handler_us = handler, back_us = back, "ledger call timing");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SENT: &str = "x-enroute-sent-us";

    #[test]
    fn a_request_without_the_header_has_no_send_time() {
        assert_eq!(sent(&Request::new(())), None);
    }

    #[test]
    fn a_stamped_request_reports_its_send_time() {
        let mut req = Request::new(());
        if let Ok(value) = "1700000000000000".parse() {
            req.metadata_mut().insert(SENT, value);
        }
        assert_eq!(sent(&req), Some(1_700_000_000_000_000));
    }

    /// Without a send time there is no leg to attribute, so the answer must
    /// gain nothing. A response that carried half the stamps would be worse
    /// than one that carried none, because the log reads them as a set.
    #[test]
    fn stamping_an_unstamped_call_adds_no_metadata() {
        let res = stamp(Response::new(()), None, std::time::Instant::now());
        assert!(res.metadata().is_empty());
        assert_eq!(res.into_inner(), ());
    }

    #[test]
    fn a_stamped_call_reports_its_three_times() {
        let res = stamp(
            Response::new(()),
            Some(unix_us()),
            std::time::Instant::now(),
        );
        for key in [
            "x-enroute-to-us",
            "x-enroute-handler-us",
            "x-enroute-done-us",
        ] {
            assert!(res.metadata().contains_key(key), "{key}");
        }
    }

    /// The wall clock is microseconds since the epoch, so it is positive for
    /// any time this test runs.
    #[test]
    fn the_wall_clock_is_after_the_epoch() {
        assert!(unix_us() > 0);
    }
}
