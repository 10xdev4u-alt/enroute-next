//! One replicated log's storage, implementing raft-rs `Storage`.
//!
//! Entries live in the node's log store ([`crate::logdb`]); their terms,
//! the hard state and where the log starts (the latest snapshot's
//! metadata) are also kept in memory ([`LogMeta`]). The engine writes every
//! change to the log store and only then applies it here, so this view is
//! always of durable state; entries not yet durable are in raft-rs's
//! unstable log, not here.
//!
//! The state machine is in the node's state store, so a snapshot (for a
//! follower behind the log's start) is made when raft asks for one: the
//! state as of the last entry applied.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use prost::Message as _;
use raft::prelude::*;
use raft::{GetEntriesContext, RaftState, Storage, StorageError};

use enroute_shared::state::{Command, RepoMeta};

use crate::logdb::LogDb;
use crate::state_machine::count;
use crate::state_store::{Root, StateStore};
use crate::stored;

use super::codec::{Entries, Prost};

/// Saturating: indexes and lengths never reach the limits.
fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

#[derive(Default)]
pub(super) struct LogMeta {
    pub hard_state: HardState,
    /// Where the log starts: the latest snapshot's metadata (its index and
    /// term), without data.
    pub snapshot: Snapshot,
    /// The log's membership as of the last entry applied (handed to the
    /// apply thread): the state store keeps it with its applied index.
    pub conf: ConfState,
    /// Terms of the entries after the snapshot index.
    terms: VecDeque<u64>,
}

impl LogMeta {
    pub(super) fn first_index(&self) -> u64 {
        self.snapshot.get_metadata().index + 1
    }

    pub(super) fn last_index(&self) -> u64 {
        self.snapshot.get_metadata().index + count(self.terms.len())
    }

    /// A snapshot's metadata (no data) at `index` of `term`, with the
    /// log's membership.
    pub(super) fn snapshot_at(&self, index: u64, term: u64) -> Snapshot {
        let mut snap = Snapshot::default();
        let md = snap.mut_metadata();
        (md.index, md.term) = (index, term);
        md.set_conf_state(self.conf.clone());
        snap
    }

    /// Entries `(index, term)` were appended, truncating any conflicting
    /// suffix.
    pub(super) fn append(&mut self, ents: impl IntoIterator<Item = (u64, u64)>) {
        for (index, term) in ents {
            let first = self.first_index();
            if index < first {
                continue; // covered by the snapshot
            }
            self.terms.truncate(to_usize(index - first));
            self.terms.push_back(term);
        }
    }

    /// Install a snapshot from the leader: every entry is dropped (as the
    /// engine does in the log store).
    pub(super) fn apply_snapshot(&mut self, snap: Snapshot) {
        let meta = snap.get_metadata();
        if meta.index <= self.snapshot.get_metadata().index && !self.snapshot.is_empty() {
            return;
        }
        if self.hard_state.commit < meta.index {
            self.hard_state.commit = meta.index;
        }
        if self.hard_state.term < meta.term {
            self.hard_state.term = meta.term;
        }
        self.terms.clear();
        self.conf = meta.get_conf_state().clone();
        self.snapshot = snap;
    }

    /// Replace the snapshot with a newer one taken locally (log compaction):
    /// entries up to its index are dropped.
    pub(super) fn compact(&mut self, snap: Snapshot) {
        let index = snap.get_metadata().index;
        let old = self.snapshot.get_metadata().index;
        if index <= old {
            return;
        }
        let n = to_usize(index - old).min(self.terms.len());
        self.terms.drain(..n);
        self.snapshot = snap;
    }

    pub(super) fn term(&self, idx: u64) -> Option<u64> {
        let meta = self.snapshot.get_metadata();
        if idx == meta.index {
            return Some(meta.term);
        }
        if idx < self.first_index() || idx > self.last_index() {
            return None;
        }
        self.terms.get(to_usize(idx - self.first_index())).copied()
    }
}

#[derive(Clone)]
pub(super) struct LogStore {
    pub id: u64,
    pub db: LogDb,
    pub state: Arc<StateStore>,
    pub meta: Arc<RwLock<LogMeta>>,
}

impl LogStore {
    pub(super) fn new(id: u64, db: LogDb, state: Arc<StateStore>, meta: LogMeta) -> LogStore {
        LogStore {
            id,
            db,
            state,
            meta: Arc::new(RwLock::new(meta)),
        }
    }

    /// Entries `low..high` from the log store.
    pub(super) fn read(
        &self,
        low: u64,
        high: u64,
        max_size: Option<u64>,
    ) -> raft::Result<Vec<Entry>> {
        let id = self.id;
        self.db
            .read(move |db| fetch(db, id, low, high, max_size.map(to_usize)))
            .map_err(|e| raft::Error::Store(StorageError::Other(Box::new(e))))
    }
}

/// Log `id`'s entries `low..high` (at most `max_size` bytes, but at least
/// one), read on the store's reader thread (see [`LogDb::read`]).
pub(super) fn fetch(
    db: &raft_engine::Engine,
    id: u64,
    low: u64,
    high: u64,
    max_size: Option<usize>,
) -> raft_engine::Result<Vec<Entry>> {
    let mut entries = Vec::with_capacity(to_usize(high.saturating_sub(low).min(1024)));
    db.fetch_entries_to_with::<Entries, Prost>(id, low, high, max_size, &mut entries)?;
    Ok(entries)
}

impl Storage for LogStore {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let m = self.meta.read();
        Ok(RaftState {
            hard_state: m.hard_state.clone(),
            conf_state: m.conf.clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        {
            let m = self.meta.read();
            if low < m.first_index() {
                return Err(raft::Error::Store(StorageError::Compacted));
            }
            if high > m.last_index() + 1 {
                return Err(raft::Error::Store(StorageError::Unavailable));
            }
        }
        if low == high {
            return Ok(Vec::new());
        }
        // The store returns at least one entry, as raft requires.
        self.read(low, high, max_size.into())
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let m = self.meta.read();
        if idx < m.snapshot.get_metadata().index {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        m.term(idx)
            .ok_or(raft::Error::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.meta.read().first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.meta.read().last_index())
    }

    /// The state machine as of the last entry applied, with its identity.
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let other = |e: anyhow::Error| raft::Error::Store(StorageError::Other(e.into()));
        let Some((root, meta)) = load(&self.state, &self.db, self.id).map_err(other)? else {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        };
        let index = root.applied_index;
        if index < request_index {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        // With the membership as of its index.
        let mut snap = self.meta.read().snapshot_at(index, root.applied_term);
        snap.mut_metadata().set_conf_state(root.conf());
        snap.data = root.record(meta).encode_to_vec();
        Ok(snap)
    }
}

/// An entry's command, if it is one (of log `log`, for the error).
pub(super) fn decode(log: u64, e: &Entry) -> Option<Command> {
    if e.get_entry_type() != EntryType::EntryNormal || e.data.is_empty() {
        return None;
    }
    match Command::decode(e.data.as_slice()) {
        Ok(c) => Some(c),
        Err(err) => {
            tracing::error!(log, index = e.index, "undecodable entry: {err}");
            None
        }
    }
}

/// The command at log index `index`.
fn command_at(db: &LogDb, log: u64, index: u64) -> Result<Command> {
    let entries = db.read(move |db| fetch(db, log, index, index + 1, None))?;
    let e = entries.first().context("no such entry")?;
    Ok(Command::decode(e.data.as_slice())?)
}

/// `log`'s root and whole state at the last entry applied, inline layers'
/// bytes read from the log. None if it has no state.
pub(super) fn load(store: &StateStore, db: &LogDb, log: u64) -> Result<Option<(Root, RepoMeta)>> {
    stored::load(store, log, |index| command_at(db, log, index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_truncates_conflicts_and_compacts() {
        let mut m = LogMeta::default();
        m.append([(1, 1), (2, 1), (3, 1)]);
        assert_eq!(m.last_index(), 3);
        m.append([(2, 2)]);
        assert_eq!(m.last_index(), 2);
        assert_eq!(m.term(2), Some(2));
        let mut s = Snapshot::default();
        s.mut_metadata().index = 2;
        s.mut_metadata().term = 2;
        m.compact(s);
        assert_eq!(m.first_index(), 3);
        assert_eq!(m.term(2), Some(2));
        assert_eq!(m.term(1), None);
        m.append([(3, 2), (4, 2)]);
        assert_eq!(m.last_index(), 4);
    }
}
