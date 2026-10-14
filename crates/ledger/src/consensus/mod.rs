//! Consensus: one replicated log per repository.
//!
//! Every repository has its own log whose state machine is that
//! repository's refs and layer manifest, hosted by its voters for as long
//! as it exists. Which repositories exist, and a checkpoint of each one's
//! state, are in the object store (see [`crate::Ledger`]). All logs on a
//! node share one log store (`logdb`) and one event loop (see
//! `engine`), so many logs cost no more fsyncs or RPCs than one. Idle
//! logs go quiet (no ticks, no heartbeats).
//!
//! The consensus core is raft-rs (`TiKV`'s port of etcd/raft). A node's Raft
//! id is its store id (`logdb::store_id`), and nodes find each
//! other through the node registry ([`enroute_shared::registry`]). Every
//! repository's voters are the ledger's members: a log's leader changes
//! them to match when the members change (see `engine`).

mod apply;
mod codec;
mod engine;
mod logstore;
mod stats;
mod transport;

pub use engine::{Engine, Hosted, Stats};

/// The id of incarnation `epoch` of `repo`'s log: a hash, so nodes agree on
/// it without coordination (never 0).
pub(crate) fn log_id(repo: &str, epoch: u64) -> u64 {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(repo.as_bytes());
    h.update([0]);
    h.update(epoch.to_le_bytes());
    let d = h.finalize();
    // Little-endian first 8 bytes of the digest.
    d.iter()
        .take(8)
        .rev()
        .fold(0u64, |acc, b| acc << 8 | u64::from(*b))
        | 1
}
