//! The interface between the roles is the `Ledger` gRPC service
//! (`proto/ledger.proto`): the ledger implements its generated trait
//! ([`ledger_server::Ledger`]); in a node running both roles the compute
//! calls it in-process, and a compute-only node through gRPC (an
//! implementation of the same trait that forwards to ledger nodes). This
//! module has the trait object the compute holds, and plain-typed calls on it.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;

use crate::proto::{
    ActivateRequest, DeleteRequest, DescribeRequest, FollowRequest, ListRequest, ListResponse,
    ReadBarrierRequest, RepoSummary, Update, ledger_server,
};
use crate::state::{Command, Outcome};

/// A subscription's stream, as the service returns it.
pub type FollowStream = BoxStream<'static, Result<Update, tonic::Status>>;

/// A ledger, in-process or remote.
pub type DynLedger = dyn ledger_server::Ledger<FollowStream = FollowStream>;

/// A subscription's updates (see [`follow`]).
pub type Updates = BoxStream<'static, anyhow::Result<Update>>;

/// The error of a request for a repository whose log the ledger node asked
/// does not host, or for whose incarnation it is not a voter: another node
/// may serve it.
#[derive(Debug, Clone, Copy)]
pub struct NotHosted;

impl std::fmt::Display for NotHosted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("repository is not active on this node")
    }
}

impl std::error::Error for NotHosted {}

/// The status code of [`NotHosted`], so a caller across gRPC can tell it.
const NOT_HOSTED: tonic::Code = tonic::Code::FailedPrecondition;

/// An error as a status (its message, with causes).
#[must_use]
#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a `map_err` function, which passes its error by value"
)]
pub fn status(e: anyhow::Error) -> tonic::Status {
    let code = if not_hosted(&e) {
        NOT_HOSTED
    } else {
        tonic::Code::Internal
    };
    tonic::Status::new(code, format!("{e:#}"))
}

/// Whether the status says the node asked does not host the repository.
#[must_use]
pub fn not_hosted_status(s: &tonic::Status) -> bool {
    s.code() == NOT_HOSTED
}

/// Whether `e` says the ledger node asked does not host the repository,
/// in this process ([`NotHosted`]) or across gRPC (its status).
#[must_use]
pub fn not_hosted(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.is::<NotHosted>()
            || c.downcast_ref::<tonic::Status>()
                .is_some_and(not_hosted_status)
    })
}

/// A status as an error: its message, keeping the status (for callers that
/// look at its code).
#[must_use]
pub fn error(s: tonic::Status) -> anyhow::Error {
    let message = s.message().to_string();
    anyhow::Error::new(s).context(message)
}

/// Make `repo`'s log live, waking it from the object store or creating it
/// (`create`). False: it does not exist.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn activate(ledger: &DynLedger, repo: &str, create: bool) -> anyhow::Result<bool> {
    let req = ActivateRequest {
        repo: repo.to_string(),
        create,
        head: None,
    };
    Ok(ledger
        .activate(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner()
        .exists)
}

/// Create `repo` with HEAD `head` unless it exists (then it is left as it
/// is).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn create(ledger: &DynLedger, repo: &str, head: &str) -> anyhow::Result<()> {
    let req = ActivateRequest {
        repo: repo.to_string(),
        create: true,
        head: Some(head.to_string()),
    };
    ledger
        .activate(tonic::Request::new(req))
        .await
        .map_err(error)?;
    Ok(())
}

/// Delete `repo` (see `Delete` in `ledger.proto`).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn delete(ledger: &DynLedger, repo: &str) -> anyhow::Result<()> {
    let req = DeleteRequest {
        repo: repo.to_string(),
    };
    ledger
        .delete(tonic::Request::new(req))
        .await
        .map_err(error)?;
    Ok(())
}

/// `repo` as of everything committed before the call (None: it does not
/// exist).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn describe(ledger: &DynLedger, repo: &str) -> anyhow::Result<Option<RepoSummary>> {
    let req = DescribeRequest {
        repo: repo.to_string(),
    };
    Ok(ledger
        .describe(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner()
        .repo)
}

/// Repositories named with `prefix`, after `after`, at most `limit` (see
/// `List` in `ledger.proto`).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn list(
    ledger: &DynLedger,
    prefix: &str,
    after: &str,
    limit: u32,
) -> anyhow::Result<ListResponse> {
    let req = ListRequest {
        prefix: prefix.to_string(),
        after: after.to_string(),
        limit,
    };
    Ok(ledger
        .list(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner())
}

/// Order and apply `cmd` in its repository's log; resolves with the outcome
/// once applied.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn propose(ledger: &DynLedger, cmd: Command) -> anyhow::Result<Outcome> {
    Ok(ledger
        .propose(tonic::Request::new(cmd))
        .await
        .map_err(error)?
        .into_inner())
}

/// Wait until everything committed to `repo`'s log before the call has been
/// applied on the ledger; returns the repository's version then, which a
/// follower waits for its own state to reach.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn read_barrier(ledger: &DynLedger, repo: &str) -> anyhow::Result<u64> {
    let req = ReadBarrierRequest {
        repo: repo.to_string(),
    };
    Ok(ledger
        .read_barrier(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner()
        .version)
}

/// Follow `repo` (its log must be live): its state, then every change to it.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn follow(ledger: &DynLedger, repo: &str) -> anyhow::Result<Updates> {
    let req = FollowRequest {
        repo: repo.to_string(),
    };
    let stream = ledger
        .follow(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner();
    Ok(stream.map(|u| u.map_err(error)).boxed())
}

/// A shared ledger.
pub type LedgerRef = Arc<DynLedger>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_hosted_keeps_its_type_across_grpc() {
        let e = anyhow::Error::new(NotHosted).context("not a voter of incarnation 3");
        assert!(not_hosted(&e));
        let s = status(e);
        assert!(not_hosted_status(&s));
        assert!(s.message().contains("not a voter"), "{s:?}");
        assert!(not_hosted(&error(s)));
        let other = status(anyhow::anyhow!("disk full"));
        assert_eq!(other.code(), tonic::Code::Internal);
        assert!(!not_hosted(&error(other)));
    }
}
