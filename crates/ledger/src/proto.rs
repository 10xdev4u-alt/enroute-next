//! The ledger's own protocol types, generated from `proto/` at build time
//! (see `build.rs`): the Raft service between ledger nodes.

// Generated code: prost's output trips lints that hand-written code must pass.
#![expect(
    unused_qualifications,
    unreachable_pub,
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::large_enum_variant,
    clippy::as_conversions,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    reason = "prost/tonic codegen"
)]

include!(concat!(env!("OUT_DIR"), "/enroute.internal.raft.v1.rs"));
