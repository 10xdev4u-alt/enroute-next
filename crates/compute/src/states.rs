//! The repositories this node follows: the one owner of each one's
//! lifecycle. Activation on the ledger, the subscription to its log
//! ([`roles::follow`]), its latest state, the view built from it, and
//! teardown. A repository is followed on demand ([`States::open`]) and until
//! the compute stops needing it ([`States::evict`], [`States::unfollow`]).

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::watch;

use enroute_shared::proto::update;
use enroute_shared::roles::{self, LedgerRef};
use enroute_shared::state::RepoMeta;

use crate::repo::{Repo, Repos};
use crate::util::coalesce::Coalesce;
use crate::util::micros;

/// How fresh a view [`States::open`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consistency {
    /// The latest state this node knows (after it follows the repository,
    /// if it did not).
    Known,
    /// Every write committed before the call.
    Latest,
}

/// How long to wait for a subscription's first state.
const FIRST_STATE: Duration = Duration::from_secs(30);
/// Updates applied (and published) together.
const BATCH: usize = 64;

pub(crate) struct States {
    pub repos: Arc<Repos>,
    ledger: LedgerRef,
    followed: Mutex<HashMap<String, Followed>>,
    next: std::sync::atomic::AtomicU64,
}

struct Followed {
    /// Tells this subscription from a later one for the same repository.
    id: u64,
    /// The latest state (None until the first arrives).
    state: watch::Sender<Option<Arc<RepoMeta>>>,
    task: tokio::task::AbortHandle,
    /// Read barriers on the log: concurrent linearizable reads share one.
    barrier: Arc<Coalesce<u64>>,
}

impl Drop for Followed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl States {
    pub(crate) fn new(repos: Arc<Repos>, ledger: LedgerRef) -> Arc<States> {
        Arc::new(States {
            repos,
            ledger,
            followed: Mutex::default(),
            next: std::sync::atomic::AtomicU64::default(),
        })
    }

    /// The latest state of `repo`, if this node follows it (and it is not
    /// deleted).
    pub(crate) fn get(&self, repo: &str) -> Option<Arc<RepoMeta>> {
        self.followed
            .lock()
            .get(repo)
            .and_then(|f| f.state.borrow().clone())
            .filter(|m| !m.deleted)
    }

    /// Wait until the followed state of `repo` reaches `version` (or says
    /// the repository was deleted), and return it.
    pub(crate) async fn wait_version(&self, repo: &str, version: u64) -> Result<Arc<RepoMeta>> {
        let mut rx = self
            .followed
            .lock()
            .get(repo)
            .context("stopped following the repository")?
            .state
            .subscribe();
        let reached = |s: &Option<Arc<RepoMeta>>| {
            s.as_ref()
                .is_some_and(|m| m.version >= version || m.deleted)
        };
        let state = tokio::time::timeout(FIRST_STATE, rx.wait_for(reached))
            .await
            .context("timed out waiting for the repository's state")?
            .context("stopped following the repository")?;
        #[expect(
            clippy::expect_used,
            reason = "wait_for returns only once a state is set"
        )]
        let state = state.clone().expect("waited for a state");
        Ok(state)
    }

    /// The repositories this node follows (with a state, not deleted).
    pub(crate) fn names(&self) -> Vec<String> {
        self.followed
            .lock()
            .iter()
            .filter(|(_, f)| f.state.borrow().as_ref().is_some_and(|m| !m.deleted))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Follow `repo` (its log must be live), if this node does not already,
    /// and return its state once there is one (maybe deleted: the
    /// subscription then ends).
    pub(crate) async fn follow(self: &Arc<Self>, repo: &str) -> Result<Arc<RepoMeta>> {
        let mut rx = {
            let mut followed = self.followed.lock();
            if let Some(f) = followed.get(repo) {
                f.state.subscribe()
            } else {
                let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let (tx, rx) = watch::channel(None);
                let task =
                    tokio::spawn(run(Arc::downgrade(self), repo.to_string(), id, tx.clone()))
                        .abort_handle();
                followed.insert(
                    repo.to_string(),
                    Followed {
                        id,
                        state: tx,
                        task,
                        barrier: Coalesce::new(),
                    },
                );
                rx
            }
        };
        let state = tokio::time::timeout(FIRST_STATE, rx.wait_for(Option::is_some))
            .await
            .context("timed out following the repository")?
            .context("could not follow the repository")?;
        #[expect(
            clippy::expect_used,
            reason = "wait_for returns only once a state is set"
        )]
        let state = state.clone().expect("waited for a state");
        Ok(state)
    }

    /// Whether this node follows `repo`'s log.
    pub(crate) fn following(&self, repo: &str) -> bool {
        self.get(repo).is_some()
    }

    /// The view of `repo` as it is, if this node follows it.
    pub(crate) fn current(&self, repo: &str) -> Option<Arc<Repo>> {
        self.repos.get(repo).filter(|_| self.following(repo))
    }

    /// Follow `repo`, making its log live on the ledger first if this node
    /// does not follow it yet. None: it does not exist.
    pub(crate) async fn follow_active(
        self: &Arc<Self>,
        repo: &str,
    ) -> Result<Option<Arc<RepoMeta>>> {
        if !self.following(repo) && !roles::activate(&*self.ledger, repo, false).await? {
            return Ok(None);
        }
        Ok(Some(self.follow(repo).await?))
    }

    /// Make `repo`'s log live (creating the repository with `create`), and
    /// open it as of everything committed before the call. None: it does
    /// not exist.
    pub(crate) async fn ensure(
        self: &Arc<Self>,
        repo: &str,
        create: bool,
    ) -> Result<Option<Arc<Repo>>> {
        if !roles::activate(&*self.ledger, repo, create).await? {
            return Ok(None);
        }
        self.follow(repo).await?;
        self.open(repo, Consistency::Latest).await
    }

    /// The view of `name`, at least as fresh as `consistency` asks. None:
    /// it does not exist.
    pub(crate) async fn open(
        self: &Arc<Self>,
        name: &str,
        consistency: Consistency,
    ) -> Result<Option<Arc<Repo>>> {
        let meta = match consistency {
            Consistency::Known => match self.get(name) {
                Some(meta) => Some(meta),
                None => self.follow_active(name).await?,
            },
            Consistency::Latest => self.latest(name, self.following(name)).await?,
        };
        let Some(meta) = meta.filter(|m| !m.deleted) else {
            return Ok(None);
        };
        let repo = self.repos.handle(name);
        // The view may have gone after the subscription published this state
        // (eviction, a new incarnation): ask for it again.
        if repo.snapshot().version() < meta.version {
            self.repos.on_apply(name, &meta);
        }
        repo.wait_version(meta.version).await?;
        repo.touch();
        Ok(Some(repo))
    }

    /// The version of `name` as of everything committed to its log before
    /// the call. Concurrent calls share one request to the ledger, as in
    /// etcd's linearizable read loop ([`Coalesce`]).
    async fn read_barrier(&self, name: &str) -> Result<u64> {
        let barrier = self.followed.lock().get(name).map(|f| f.barrier.clone());
        let Some(barrier) = barrier else {
            return roles::read_barrier(&*self.ledger, name).await;
        };
        let ask = || {
            let (ledger, repo) = (self.ledger.clone(), name.to_string());
            async move { roles::read_barrier(&*ledger, &repo).await }
        };
        Ok(barrier.call(ask).await?)
    }

    /// `name`'s state as of everything committed before the call;
    /// `followed`: this node follows it, so its log is live. None: it does
    /// not exist.
    #[expect(
        clippy::unwrap_used,
        reason = "the instants are taken in order, so the differences cannot be negative"
    )]
    async fn latest(self: &Arc<Self>, name: &str, followed: bool) -> Result<Option<Arc<RepoMeta>>> {
        let t0 = std::time::Instant::now();
        // A live subscription means an active log: the read needs only the
        // barrier (a round trip saved on every push's ref advertisement).
        let Some(mut meta) = self.follow_active(name).await? else {
            return Ok(None);
        };
        if meta.deleted {
            return Ok(None);
        }
        let t_follow = t0.elapsed();
        // Everything committed so far: wait for this node's subscription to
        // reach it.
        let version = match self.read_barrier(name).await {
            Ok(v) => v,
            // The ledger node no longer hosts it (it restarted, or the
            // incarnation was superseded): activate it again.
            Err(e) if followed && roles::not_hosted(&e) => {
                if !roles::activate(&*self.ledger, name, false).await? {
                    return Ok(None);
                }
                meta = self.follow(name).await?;
                self.read_barrier(name).await?
            }
            Err(e) => return Err(e),
        };
        let t_barrier = t0.elapsed();
        if meta.version < version {
            meta = self.wait_version(name, version).await?;
        }
        tracing::debug!(
            target: "enroute::read_timing",
            follow_us = micros(t_follow),
            barrier_us = micros(t_barrier.checked_sub(t_follow).unwrap()),
            catch_up_us = micros(t0.elapsed().checked_sub(t_barrier).unwrap()),
            "linearizable read timing"
        );
        Ok(Some(meta))
    }

    /// Stop following `repo`, and drop its view.
    pub(crate) fn unfollow(&self, repo: &str) {
        self.followed.lock().remove(repo);
        self.repos.remove(repo);
    }

    /// Drop the views of repositories not used for `idle`, and stop
    /// following them; but not while inline layers wait for a flush
    /// (maintenance needs the subscription). Returns how many went.
    pub(crate) fn evict(&self, idle: Duration) -> usize {
        let unflushed = |name: &str| {
            self.get(name)
                .is_some_and(|m| m.layers.iter().any(|l| !l.is_stored()))
        };
        let evicted = self.repos.evict(idle, unflushed);
        let mut followed = self.followed.lock();
        for name in &evicted {
            followed.remove(name);
        }
        evicted.len()
    }

    /// The subscription `id` of `repo` is over (dropping its last sender).
    fn ended(&self, repo: &str, id: u64) {
        let mut followed = self.followed.lock();
        if followed.get(repo).is_some_and(|f| f.id == id) {
            followed.remove(repo);
        }
    }
}

/// Apply a batch of updates to `cur`. False if the stream broke (skipping
/// an update would diverge from the log): the caller follows again, from a
/// new state.
fn apply_batch(
    cur: &mut Option<Arc<RepoMeta>>,
    batch: Vec<Result<enroute_shared::proto::Update>>,
    repo: &str,
) -> bool {
    for u in batch {
        let u = match u {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(repo, "following the repository: {e:#}");
                return false;
            }
        };
        match u.kind {
            Some(update::Kind::State(s)) => *cur = Some(Arc::new(s)),
            Some(update::Kind::Change(c)) => {
                if let Some(m) = cur.as_mut() {
                    // Copies only if a reader still holds the last published
                    // state (at most once per batch).
                    Arc::make_mut(m).apply_change(&c);
                }
            }
            None => {}
        }
    }
    true
}

/// Publish `cur` to the subscribers and the view. False if it is a
/// deletion, which ends the subscription.
fn publish(
    tx: &watch::Sender<Option<Arc<RepoMeta>>>,
    repos: &Arc<Repos>,
    repo: &str,
    cur: Option<&Arc<RepoMeta>>,
    failures: &mut u32,
) -> bool {
    let Some(m) = cur else { return true };
    let epoch = tx.borrow().as_ref().map(|p| p.epoch);
    if m.deleted || epoch.is_some_and(|e| e != m.epoch) {
        repos.remove(repo);
    }
    tx.send_replace(Some(m.clone()));
    if m.deleted {
        return false;
    }
    repos.on_apply(repo, m);
    *failures = 0;
    true
}

/// Follow `repo` until this node stops following it (which aborts this
/// task), publishing every state to `tx` and to the repository's view. A
/// stream that ends (the subscriber fell behind, a ledger node went away,
/// the incarnation moved) is followed again. A deletion ends it, and the
/// view with it; so does a new incarnation, whose view starts afresh.
async fn run(
    states: Weak<States>,
    repo: String,
    id: u64,
    tx: watch::Sender<Option<Arc<RepoMeta>>>,
) {
    let mut failures: u32 = 0;
    'follow: loop {
        let Some(s) = states.upgrade() else { return };
        let updates = match roles::follow(&*s.ledger, &repo).await {
            Ok(u) => u,
            Err(e) => {
                failures += 1;
                if failures > 5 {
                    tracing::warn!(repo, "following the repository: {e:#}");
                    break;
                }
                // Not active on that ledger node: (re)activate it.
                if let Ok(false) = roles::activate(&*s.ledger, &repo, false).await {
                    break; // deleted
                }
                drop(s);
                tokio::time::sleep(Duration::from_millis(100 << failures)).await;
                continue;
            }
        };
        let repos = s.repos.clone();
        drop(s);
        let mut cur: Option<Arc<RepoMeta>> = None;
        let mut batches = updates.ready_chunks(BATCH);
        let mut broken = false;
        while let Some(batch) = batches.next().await {
            if !apply_batch(&mut cur, batch, &repo) {
                broken = true;
            }
            if !publish(&tx, &repos, &repo, cur.as_ref(), &mut failures) {
                break 'follow;
            }
            if broken {
                break;
            }
        }
    }
    if let Some(s) = states.upgrade() {
        s.ended(&repo, id);
    }
}
