//! What the gRPC contract and the hook contract share: the repository key and
//! the conversions of their common messages.
//!
//! Below both [`crate::api`] and [`crate::hooks`], because the API depends on
//! the compute, which depends on the hooks.

pub(crate) mod repo_key;
pub(crate) mod wire;
