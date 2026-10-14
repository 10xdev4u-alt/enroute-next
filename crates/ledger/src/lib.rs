//! The ledger: the nodes that hold the replicated logs. Every repository
//! has its own log ([`consensus`]); the ledger creates it, decides who
//! votes in it, and checkpoints it to the object store ([`Ledger`]).

#![deny(unsafe_code)]

pub mod consensus;
mod ledger;
mod logdb;
mod proto;
mod state_machine;
mod state_store;
mod stored;

pub use ledger::{Ledger, LedgerConfig};
