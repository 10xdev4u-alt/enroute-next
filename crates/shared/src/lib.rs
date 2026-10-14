//! What every Enroute node shares, whichever roles it runs (the ledger, the
//! compute): the object store, the node registry, a repository's state and the
//! commands that change it, the ledger's protocol, and the few types those
//! carry (object ids).

#![deny(unsafe_code)]

pub mod call_timing;
pub mod oid;
pub mod proto;
pub mod registry;
pub mod repo_state;
pub mod roles;
pub mod state;
pub mod store;
