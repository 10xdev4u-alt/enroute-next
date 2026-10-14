//! Shared setup for the dev-only harnesses: the end-to-end suite (`e2e`) and
//! `tests/`.
//!
//! [`collect`], which reads a run's spans back, [`git`], the client both
//! drive, and [`hooks`], the application Enroute asks.
#![allow(
    clippy::expect_used,
    reason = "dev harness support code, never compiled into the production build; \
              a stub that cannot start is a harness that cannot run"
)]

pub mod collect;
pub mod git;
pub mod hooks;
