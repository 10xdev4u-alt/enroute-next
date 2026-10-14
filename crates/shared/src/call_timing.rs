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
