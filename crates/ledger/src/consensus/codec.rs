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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> Entry {
        Entry {
            term: 7,
            index: 42,
            data: b"payload".to_vec(),
            ..Default::default()
        }
    }

    fn encoded(e: &Entry) -> Vec<u8> {
        let mut buf = Vec::new();
        <Prost as ValueCodec<Entry>>::encode_to(e, &mut buf).expect("encoding an entry");
        buf
    }

    fn decoded(bytes: &[u8]) -> raft_engine::Result<Entry> {
        <Prost as ValueCodec<Entry>>::decode(bytes)
    }

    /// What the store wrote is what it reads back: an entry comes out of a
    /// restart with its term, index and payload.
    #[test]
    fn an_entry_round_trips() {
        assert_eq!(decoded(&encoded(&entry())).unwrap(), entry());
        // An entry the engine has nothing to add to: prost writes no field
        // of it at all, and the empty message reads back as such.
        let empty = Entry::default();
        assert!(encoded(&empty).is_empty());
        assert_eq!(decoded(&encoded(&empty)).unwrap(), empty);
    }

    /// A truncated entry is an error: recovery replays, so half an entry
    /// must not read as a whole one with a short payload.
    #[test]
    fn a_truncated_entry_is_refused() {
        let buf = encoded(&entry());
        let refused = decoded(&buf[..buf.len() - 1]).unwrap_err();
        assert!(matches!(refused, raft_engine::Error::InvalidArgument(_)));
    }

    /// A length prefix longer than what is left is refused too, rather than
    /// read as far as it goes.
    #[test]
    fn a_length_past_the_end_is_refused() {
        // Field 4 (data), length-delimited, saying 127 bytes follow.
        let refused = decoded(&[0x22, 0x7f]).unwrap_err();
        assert!(matches!(refused, raft_engine::Error::InvalidArgument(_)));
    }

    /// The engine indexes entries by this, so it is the entry's own index
    /// and not a count of what was written before it.
    #[test]
    fn the_index_is_the_entrys() {
        assert_eq!(<Entries as MessageExt<Prost>>::index(&entry()), 42);
        assert_eq!(<Entries as MessageExt<Prost>>::index(&Entry::default()), 0);
    }
}
