//! How the ledger's logs are stored in the node's log store
//! ([`crate::logdb`]): raft-rs's own messages, with its prost codec.

use raft::prelude::Entry;
use raft_engine::{MessageExt, ValueCodec};

/// Values encoded with prost (raft-rs's codec).
pub(super) struct Prost;

impl<T: raft_prost::Message + Default> ValueCodec<T> for Prost {
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> raft_engine::Result<()> {
        v.encode(buf)
            .map_err(|e| raft_engine::Error::InvalidArgument(e.to_string()))
    }

    fn decode(bytes: &[u8]) -> raft_engine::Result<T> {
        T::decode(bytes).map_err(|e| raft_engine::Error::InvalidArgument(e.to_string()))
    }
}

/// Raft entries, as stored.
pub(super) struct Entries;

impl MessageExt<Prost> for Entries {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}
