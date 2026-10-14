//! How a compute reaches the ledger in a cluster: the `Ledger` service
//! (`proto/ledger.proto`) on the ledger nodes it finds in the registry,
//! as an implementation of the same service ([`RemoteLedger`]), so the
//! compute calls it as it would the ledger in its own process. A node running
//! both roles asks its own ledger first: it is a voter of only some
//! repositories' incarnations.

use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use enroute_shared::proto::{
    ActivateRequest, ActivateResponse, DeleteRequest, DeleteResponse, DescribeRequest,
    DescribeResponse, FollowRequest, ListRequest, ListResponse, ReadBarrierRequest,
    ReadBarrierResponse, RepoSummary, ledger_client, ledger_server,
};
use enroute_shared::roles::{DynLedger, FollowStream, LedgerRef};
use enroute_shared::state::{Command, Outcome};

type Client = ledger_client::LedgerClient<Channel>;

/// A client for the ledger node at `url` (its internal address),
/// connecting lazily (and again after failures).
///
/// # Errors
///
/// Returns an error if `url` is not a valid URI.
pub fn client(url: &str) -> Result<Client> {
    let channel = Channel::from_shared(url.to_string())?
        .connect_timeout(std::time::Duration::from_secs(2))
        .connect_lazy();
    // Both ways: a subscription starts with the repository's whole state,
    // inline layers included (well over tonic's 4 MiB default under load).
    Ok(Client::new(channel)
        .max_encoding_message_size(enroute_shared::proto::MAX_MESSAGE)
        .max_decoding_message_size(enroute_shared::proto::MAX_MESSAGE))
}

/// The `Ledger` service of the ledger nodes alive in the registry. Each
/// request goes to them in the repository's [rank](enroute_shared::registry::rank)
/// (so a repository's requests and subscription share a connection),
/// moving on while one cannot serve it: unreachable, or not hosting the
/// repository's log. A proposal is retried elsewhere only then,
/// since otherwise it may already be in the log.
pub struct RemoteLedger {
    registry: Arc<enroute_shared::registry::Registry>,
    /// The ledger in this process, asked first, and its node id.
    local: Option<(u64, LedgerRef)>,
    /// Connections to each ledger node, used in turn: one HTTP/2
    /// connection is driven by one task, which every call to that node
    /// (thousands a second, each proposal carrying its inline layer) then
    /// waits on.
    clients: parking_lot::Mutex<std::collections::HashMap<String, Vec<Client>>>,
    next: std::sync::atomic::AtomicUsize,
}

/// Connections per ledger node: one per core, since each connection's
/// encoding and decoding runs on one task.
fn connections() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::thread::available_parallelism().map_or(8, std::num::NonZero::get))
}

/// The connection for one call: the `turn`-th of `cs`, in turn.
#[expect(clippy::indexing_slicing, reason = "connections() is at least one")]
fn pick(cs: &[Client], turn: usize) -> Client {
    cs[turn % cs.len()].clone()
}

impl std::fmt::Debug for RemoteLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLedger")
            .field("local", &self.local.as_ref().map(|(id, _)| id))
            .finish_non_exhaustive()
    }
}

impl RemoteLedger {
    /// A ledger that calls the ledger nodes alive in `registry`. A node
    /// running both roles gives its own ledger and node id as `local`: it
    /// is asked first.
    pub fn new(
        registry: Arc<enroute_shared::registry::Registry>,
        local: Option<(u64, LedgerRef)>,
    ) -> RemoteLedger {
        RemoteLedger {
            registry,
            local,
            clients: parking_lot::Mutex::default(),
            next: std::sync::atomic::AtomicUsize::default(),
        }
    }

    /// The local ledger, if any.
    fn local(&self) -> Option<&DynLedger> {
        self.local.as_ref().map(|(_, l)| &**l)
    }

    /// Clients for the other live ledger nodes, in `repo`'s rank.
    fn clients(&self, repo: &str) -> Result<Vec<Client>, Status> {
        let me = self.local.as_ref().map(|(id, _)| *id);
        let alive: Vec<u64> = self
            .registry
            .alive()
            .into_iter()
            .filter(|id| Some(*id) != me)
            .collect();
        let urls: Vec<String> = enroute_shared::registry::rank(repo, &alive)
            .into_iter()
            .filter_map(|id| self.registry.address(id))
            .collect();
        let turn = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut clients = self.clients.lock();
        urls.iter()
            .map(|url| {
                if let Some(cs) = clients.get(url) {
                    return Ok(pick(cs, turn));
                }
                let cs = (0..connections())
                    .map(|_| client(url))
                    .collect::<Result<Vec<_>>>()
                    .map_err(|e| {
                        Status::invalid_argument(format!("ledger address {url}: {e:#}"))
                    })?;
                let c = pick(&cs, turn);
                clients.insert(url.clone(), cs);
                Ok(c)
            })
            .collect()
    }

    /// Ask the ledger nodes in turn until one can answer (see the type),
    /// after the local ledger's answer `local` if there is one.
    async fn each<T, F, Fut>(
        &self,
        repo: &str,
        local: Option<Result<T, Status>>,
        f: F,
    ) -> Result<T, Status>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut last = match local {
            Some(Err(e)) if elsewhere(&e) => Some(e),
            Some(answer) => return answer,
            None => None,
        };
        for c in self.clients(repo)? {
            match f(c).await {
                Ok(v) => return Ok(v),
                Err(e) if elsewhere(&e) => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| Status::unavailable("no ledger node is alive")))
    }
}

/// Whether another ledger node may serve a request this status answered:
/// the node was unreachable, or does not host the repository's log.
fn elsewhere(s: &Status) -> bool {
    s.code() == tonic::Code::Unavailable || enroute_shared::roles::not_hosted_status(s)
}

/// Answer `req` with the local ledger, then each ledger node (see
/// [`RemoteLedger::each`]), logging the remote call's timing as `$call`.
macro_rules! forward {
    ($self:ident, $req:ident, $method:ident, $call:literal) => {{
        let r = $req.into_inner();
        let local = match $self.local() {
            Some(l) => Some(l.$method(Request::new(r.clone())).await),
            None => None,
        };
        let t0 = std::time::Instant::now();
        let res = $self
            .each(&r.repo.clone(), local, |mut c| {
                let r = r.clone();
                async move {
                    c.$method(enroute_shared::call_timing::send(Request::new(r)))
                        .await
                }
            })
            .await;
        enroute_shared::call_timing::log($call, &res, t0);
        res
    }};
}

#[tonic::async_trait]
impl ledger_server::Ledger for RemoteLedger {
    async fn activate(
        &self,
        req: Request<ActivateRequest>,
    ) -> Result<Response<ActivateResponse>, Status> {
        forward!(self, req, activate, "activate")
    }

    async fn propose(&self, req: Request<Command>) -> Result<Response<Outcome>, Status> {
        forward!(self, req, propose, "propose")
    }

    async fn read_barrier(
        &self,
        req: Request<ReadBarrierRequest>,
    ) -> Result<Response<ReadBarrierResponse>, Status> {
        forward!(self, req, read_barrier, "read barrier")
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        forward!(self, req, delete, "delete")
    }

    async fn describe(
        &self,
        req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        forward!(self, req, describe, "describe")
    }

    /// Every live ledger node's list, merged: one that lags a create or a
    /// deletion is outvoted by one that has it (the node that answered the
    /// create or the deletion, at least).
    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let r = req.into_inner();
        let mut answers = Vec::new();
        if let Some(l) = self.local() {
            answers.push(l.list(Request::new(r.clone())).await?.into_inner());
        }
        let asked = self.clients("")?.into_iter().map(|mut c| {
            let r = r.clone();
            async move { c.list(r).await }
        });
        let replies = futures::future::join_all(asked).await;
        for a in replies {
            match a {
                Ok(a) => answers.push(a.into_inner()),
                Err(e) if e.code() == tonic::Code::Unavailable => {}
                Err(e) => return Err(e),
            }
        }
        if answers.is_empty() {
            return Err(Status::unavailable("no ledger node is alive"));
        }
        Ok(Response::new(merge_lists(answers)))
    }

    type FollowStream = FollowStream;

    async fn follow(&self, req: Request<FollowRequest>) -> Result<Response<FollowStream>, Status> {
        let r = req.into_inner();
        if let Some(l) = self.local() {
            match l.follow(Request::new(r.clone())).await {
                Err(e) if elsewhere(&e) => {}
                answer => return answer,
            }
        }
        let stream = self
            .each(&r.repo.clone(), None, |mut c| {
                let r = r.clone();
                async move { c.follow(r).await }
            })
            .await?;
        Ok(Response::new(stream.into_inner().boxed()))
    }
}

/// The lists of several ledger nodes as one. A node may lag a create or a
/// deletion, so for each name the most advanced answer wins: the newest
/// incarnation, deleted over live, then the newest version. A node that
/// stopped at its limit knows nothing past its last name, so the merged
/// list stops there too.
fn merge_lists(answers: impl IntoIterator<Item = ListResponse>) -> ListResponse {
    let mut repos = std::collections::BTreeMap::<String, RepoSummary>::new();
    let mut bound: Option<String> = None;
    for a in answers {
        if a.more
            && let Some(last) = a.repos.last()
            && bound.as_ref().is_none_or(|b| last.name < *b)
        {
            bound = Some(last.name.clone());
        }
        for r in a.repos {
            let rank = |r: &RepoSummary| (r.epoch, r.deleted, r.version);
            match repos.get(&r.name) {
                Some(have) if rank(have) >= rank(&r) => {}
                _ => drop(repos.insert(r.name.clone(), r)),
            }
        }
    }
    let repos = repos
        .into_values()
        .filter(|r| bound.as_ref().is_none_or(|b| r.name <= *b))
        .collect();
    ListResponse {
        repos,
        more: bound.is_some(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(name: &str, epoch: u64, deleted: bool) -> RepoSummary {
        RepoSummary {
            name: name.into(),
            epoch,
            deleted,
            ..Default::default()
        }
    }

    /// The most advanced answer wins, and a node that hit its limit bounds
    /// the page.
    #[test]
    fn lists_merge() {
        let a = ListResponse {
            repos: vec![summary("a", 1, true), summary("b", 1, false)],
            more: true,
        };
        let b = ListResponse {
            repos: vec![
                summary("a", 1, false),
                summary("b", 2, false),
                summary("c", 1, false),
            ],
            more: false,
        };
        let m = merge_lists([a, b]);
        assert!(m.more);
        assert_eq!(m.repos, vec![summary("a", 1, true), summary("b", 2, false)]);
        let m = merge_lists([ListResponse {
            repos: vec![summary("x", 1, false)],
            more: false,
        }]);
        assert!(!m.more && m.repos.len() == 1);
    }
}
