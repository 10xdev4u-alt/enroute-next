//! What a deployment configures.
//!
//! Two things live here, and they are the two a process needs before it can do
//! anything: where a store's bytes are, as one string, and the file those
//! strings are written in. The engine's defaults stay in the engine, so a key
//! that a file leaves out takes the value the engine would take.

mod expand;
mod file;
mod read;
mod secret;
mod store_uri;

pub use enroute_compute::{Hook, PreReceiveMode};
pub use file::{Config, Git, Hooks, Layers, Ledger, Listen, Node, Telemetry};
pub use secret::Secret;
pub use store_uri::{Bucket, ObjectUri, StoreUri};
