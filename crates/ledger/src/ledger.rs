//! The ledger: repositories' logs, who votes in them, and their state in
//! the object store. It creates a repository (its log's first and only
//! incarnation, with the ledger's members as its voters) and checkpoints
//! its state to the object store. The compute
//! reaches it through the `Ledger` service (`enroute_shared::roles`), which it
//! implements.
//!
//! The objects it keeps per repository are described in
//! [`enroute_shared::repo_state`].
//!
//! The state object is the only one that changes, always by
//! compare-and-swap on its version, so of concurrent writers exactly one
//! succeeds. **Creating** a repository is a conditional create of it;
//! **checkpointing** is a swap from the state its writer last saw, so a
//! leader whose incarnation was superseded (by an operator's recovery)
//! finds out at its next checkpoint, and stops hosting it.
//!
//! **Checkpoints** follow what changed, not the number of repositories:
//! the engine reports repositories whose state changed or whose log this
//! node started leading (`Events`), and each is
//! checkpointed by its leader within `checkpoint_age` (at a random point
//! in it, so a burst of them, after a restart or a change of leaders, is
//! spread out), at once after [`CHECKPOINT_ENTRIES`] entries, or as soon as
//! its log goes quiet, so an idle repository's latest state is in the
//! object store. An idle repository costs nothing here.
//!
//! **Activation.** A request for a repository this node does not host
//! reads its state. If there is none and the request creates it, this node
//! claims incarnation 1 (after a deletion, the next one), with the
//! ledger's members as its voters
//! ([`enroute_shared::registry`]; as they change, each log's leader changes its
//! voters): it records its initial state, hosts it durably, swaps it into
//! the state object, then announces it and campaigns. Any other member
//! joins from the initial state (Raft catches it up). A live incarnation is
//! never taken over: its voters resume it, and if a majority of them lost
//! their logs, recovering it from its last checkpoint is an operator's
//! decision.
//!
//! **Deletion** commits a tombstone to the repository's log, then marks
//! the state object deleted at that incarnation (after a trash marker for
//! its layer files, which the compute reclaims). The tombstoned log stays
//! hosted, quiet, until a new incarnation supersedes it.
//!
//! A repository is never dropped from its voters while it exists: idle
//! logs go quiet and cost no ticks or messages. Taking a whole ledger down
//! and back up (on other machines, say) is done by stopping its nodes and
//! moving their disks: a node's identity is its log store (see
//! "Freezing the ledger" in ARCHITECTURE.md).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use enroute_shared::proto::{
    ActivateRequest, ActivateResponse, DeleteRequest, DeleteResponse, DescribeRequest,
    DescribeResponse, FollowRequest, ListRequest, ListResponse, ReadBarrierRequest,
    ReadBarrierResponse, RepoSummary, ledger_server,
};
use enroute_shared::registry::Registry;
use enroute_shared::repo_state::{Record, Trash, epoch_path, state_path, trash_path};
use enroute_shared::roles::{FollowStream, NotHosted, not_hosted, status};
use enroute_shared::state::{Command, Outcome, Refusal, RepoMeta, UPLOAD_GEN_WINDOW};
use enroute_shared::store::{ObjectVersion, Objects};

use crate::consensus::Engine;
use crate::logdb;
use crate::state_store::StateStore;

/// Settings for a ledger node.
#[derive(Clone, Debug)]
pub struct LedgerConfig {
    /// This node's address for its peers; None: a standalone node, the
    /// only voter of its logs (no registry, no transport).
    pub advertise: Option<String>,
    /// Snapshot each log (dropping older entries) every N applied entries.
    pub snapshot_every: u64,
    /// The state store's memory: its block cache and memtables.
    pub state_memory_bytes: usize,
    /// Checkpoint a repository's state at most this long after it changed
    /// (or after [`CHECKPOINT_ENTRIES`] entries, or once its log is quiet).
    pub checkpoint_age: Duration,
    /// Checkpoints written at once.
    pub checkpoint_concurrency: usize,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        LedgerConfig {
            advertise: None,
            snapshot_every: 1000,
            state_memory_bytes: 512 << 20,
            checkpoint_age: Duration::from_secs(60),
            checkpoint_concurrency: 8,
        }
    }
}

/// A ledger node: its repositories' logs and their checkpoints.
pub struct Ledger {
    /// Checkpoint tuning.
    pub config: LedgerConfig,
    /// The consensus engine that hosts the logs.
    pub engine: Arc<Engine>,
    /// The object store that holds the checkpoints.
    pub objects: Arc<Objects>,
    /// The node registry (None for a standalone node).
    pub registry: Option<Arc<Registry>>,
    /// Activations in progress, one at a time per repository.
    activating: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per repository: what this node last wrote to its state object.
    written: parking_lot::Mutex<HashMap<String, Written>>,
    /// Repositories whose state changed since their last checkpoint, and
    /// when the next one is due.
    due: parking_lot::Mutex<HashMap<String, Instant>>,
}

impl std::fmt::Debug for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ledger")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A state this node wrote: for which incarnation, the object's version
/// then (checkpoints swap from it), and its log index.
struct Written {
    epoch: u64,
    version: ObjectVersion,
    index: u64,
}

/// A checkpoint that failed is tried again after this long.
const CHECKPOINT_RETRY: Duration = Duration::from_secs(5);

/// Checkpoint at least this often (entries), and when a state worth one
/// has not been checkpointed for [`LedgerConfig::checkpoint_age`].
const CHECKPOINT_ENTRIES: u64 = 1000;

impl Ledger {
    /// Open the ledger on `data_dir` and start it. Its log store's id is the
    /// node's Raft id. In a cluster (`advertise`), it registers under
    /// that id and finds its peers in the registry.
    ///
    /// Then it drops incarnations superseded while this node was away, runs
    /// lifecycle rounds (checkpoints) every second (or every half
    /// `checkpoint_age`, if shorter), and joins the incarnations that peers
    /// announce.
    ///
    /// # Errors
    ///
    /// Returns an error if the log store, the state store, the registry or
    /// the engine fails to start.
    pub async fn open(
        data_dir: &Path,
        config: LedgerConfig,
        objects: Arc<Objects>,
    ) -> Result<Arc<Ledger>> {
        let db = logdb::open(&data_dir.join("log"))?;
        let id = logdb::store_id(&db)?;
        let registry = match &config.advertise {
            Some(url) => Some(Registry::join(objects.clone(), id, url.clone()).await?),
            None => None,
        };
        let addresses = registry.as_ref().map(|r| r.addresses.clone());
        let state = Arc::new(StateStore::open(
            &data_dir.join("state"),
            config.state_memory_bytes,
        )?);
        let (announced, joins) = mpsc::unbounded_channel();
        let engine = Engine::start(db, state, config.snapshot_every, id, addresses, announced)?;
        let every =
            (config.checkpoint_age / 2).clamp(Duration::from_millis(50), Duration::from_secs(1));
        let ledger = Arc::new(Ledger {
            config,
            engine,
            objects,
            registry,
            activating: parking_lot::Mutex::default(),
            written: parking_lot::Mutex::default(),
            due: parking_lot::Mutex::default(),
        });
        tokio::spawn(lifecycle_loop(Arc::downgrade(&ledger), every));
        // Weak: the ledger owns the engine, which feeds this task.
        tokio::spawn(join_loop(Arc::downgrade(&ledger), joins));
        Ok(ledger)
    }

    /// Whether this node hosts `repo`'s log, and has not applied its
    /// deletion.
    fn live_here(&self, repo: &str) -> bool {
        self.engine.hosted(repo).is_some_and(|h| !h.deleted)
    }

    /// Make `repo` active on this node (hosting its log): join it if this
    /// node is one of its voters, or create it (`create`, with HEAD `head`
    /// if given; see `ActivateRequest`). Returns whether it is active
    /// (false: it does not exist).
    async fn activate(&self, repo: &str, create: bool, head: Option<&str>) -> Result<bool> {
        if head.is_none() && self.live_here(repo) {
            return Ok(true);
        }
        let lock = self
            .activating
            .lock()
            .entry(repo.to_string())
            .or_default()
            .clone();
        let activated = {
            let _g = lock.lock().await;
            self.activate_now(repo, create, head).await
        };
        let mut activating = self.activating.lock();
        // Ours and the map's: nobody else waits on it.
        if Arc::strong_count(&lock) == 2 {
            activating.remove(repo);
        }
        activated
    }

    async fn activate_now(&self, repo: &str, create: bool, head: Option<&str>) -> Result<bool> {
        for attempt in 0..100 {
            if head.is_none() && self.live_here(repo) {
                return Ok(true);
            }
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let found = enroute_shared::repo_state::latest(&self.objects, repo).await?;
            let rec = match found {
                Some((rec, _)) if !is_deleted(&rec) => rec,
                prior => match self.claim_next(repo, create, head, prior).await? {
                    Some(active) => return Ok(active),
                    None => continue,
                },
            };
            match self.engine.hosted(repo) {
                // The state object is behind a deletion applied here.
                Some(h) if h.epoch == rec.epoch && h.deleted => self.mark_deleted(repo).await?,
                Some(h) if h.epoch >= rec.epoch && !h.deleted => return Ok(true),
                _ => self.join_incarnation(repo, rec.epoch).await?,
            }
        }
        bail!("could not activate {repo}")
    }

    /// Claims the first incarnation of `repo`, or the one after the deleted
    /// `prior`, if `create`. `Some` is the final answer of the activation;
    /// `None`: another node won the claim, so try again.
    async fn claim_next(
        &self,
        repo: &str,
        create: bool,
        head: Option<&str>,
        prior: Option<(Record, ObjectVersion)>,
    ) -> Result<Option<bool>> {
        if !create {
            return Ok(Some(false));
        }
        // Incarnation 1, empty, for a new repository.
        let epoch = prior.as_ref().map_or(1, |(rec, _)| rec.epoch + 1);
        let meta = initial(epoch, head, prior.as_ref().map(|(rec, _)| rec));
        let claimed = self.claim(repo, epoch, meta, prior.map(|(_, v)| v)).await?;
        Ok(claimed.then_some(true))
    }

    /// Delete `repo`: commit its tombstone, then mark the state object
    /// deleted (see `Delete` in `ledger.proto`).
    async fn delete(&self, repo: &str) -> Result<()> {
        for _ in 0..10 {
            let Some((rec, _)) = enroute_shared::repo_state::latest(&self.objects, repo).await?
            else {
                return Ok(());
            };
            if is_deleted(&rec) {
                return Ok(());
            }
            self.join_incarnation(repo, rec.epoch).await?;
            // Answered once applied here, so the tombstone is in the state
            // this node marks.
            let out = self
                .engine
                .propose(Command::delete(repo).at(rec.epoch))
                .await?;
            match out.refused() {
                None | Some(Refusal::Deleted) => return self.mark_deleted(repo).await,
                // The state object was behind a newer incarnation.
                Some(Refusal::OtherIncarnation) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Some(Refusal::Invalid | Refusal::StaleUpload | Refusal::Unspecified) => {
                    bail!("deleting {repo}: {}", out.error.unwrap_or_default())
                }
            }
        }
        bail!("could not delete {repo}")
    }

    /// Swap the deleted state this node applied into the state object (a
    /// checkpoint, which writes the trash marker first).
    async fn mark_deleted(&self, repo: &str) -> Result<()> {
        let name = repo.to_string();
        let rec = self
            .blocking(move |e| e.record(&name))
            .await?
            .ok_or(NotHosted)?;
        anyhow::ensure!(is_deleted(&rec), "{repo} is not deleted here");
        self.checkpoint(repo, &rec).await
    }

    /// `repo` as of everything committed before the call, if it exists.
    async fn describe(&self, repo: &str) -> Result<Option<RepoSummary>> {
        for _ in 0..3 {
            if !self.activate(repo, false, None).await? {
                return Ok(None);
            }
            match self.engine.read_barrier(repo).await {
                Ok(()) => {}
                // No longer hosted here (superseded): activate again.
                Err(e) if not_hosted(&e) => continue,
                Err(e) => return Err(e),
            }
            let name = repo.to_string();
            // (None: dropped meanwhile; activate again.)
            if let Some(s) = self.blocking(move |e| e.summary(&name)).await? {
                return Ok((!s.deleted).then_some(s));
            }
        }
        bail!("could not describe {repo}")
    }

    /// Run `f` on the engine off the async runtime: it reads the stores.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Engine) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || f(&engine)).await?
    }

    /// Claim incarnation `epoch` of `repo` starting from `meta`: record it
    /// as the incarnation's initial state, host it durably, create the
    /// state object, then announce it and campaign. False if another node
    /// created the state first.
    ///
    /// The initial state of `epoch` is the same whoever claims it (`meta`,
    /// and the voters of the first claimant to record it), so it is
    /// written before the claim, and a node that lost the race for the
    /// same incarnation keeps hosting it as a replica.
    ///
    /// After a deletion, `expected` is the version of the deleted state the
    /// claim swaps out.
    async fn claim(
        &self,
        repo: &str,
        epoch: u64,
        meta: RepoMeta,
        mut expected: Option<ObjectVersion>,
    ) -> Result<bool> {
        let voters = self.voters_for_claim().await?;
        let mut rec = Record {
            repo: repo.to_string(),
            epoch,
            voters,
            meta: Some(meta),
            ..Default::default()
        };
        let mut data = rec.to_bytes();
        // Each claimant sees its own set of live voters: the first to record
        // the initial state decides them, and the others adopt it.
        if self
            .objects
            .put_if(&epoch_path(repo, epoch), data.clone(), None)
            .await?
            .is_none()
        {
            data = self
                .objects
                .get_object(&epoch_path(repo, epoch))
                .await?
                .context("initial state vanished")?;
            rec = Record::from_bytes(&data)?;
        }
        if !rec.voters.contains(&self.engine.id) {
            return Err(anyhow::Error::new(NotHosted).context(format!(
                "this node is not a voter of incarnation {epoch} of {repo}"
            )));
        }
        self.engine
            .host(repo, epoch, rec.meta.unwrap_or_default(), rec.voters)
            .await?;
        let version = loop {
            if let Some(v) = self
                .objects
                .put_if(&state_path(repo), data.clone(), expected.as_ref())
                .await?
            {
                break v;
            }
            match enroute_shared::repo_state::latest(&self.objects, repo).await? {
                // Another claimant won it: this node is a replica.
                Some((r, _)) if r.epoch == epoch && !is_deleted(&r) => return Ok(true),
                // The deleted state was checkpointed again.
                Some((r, v)) if r.epoch < epoch && is_deleted(&r) && expected.is_some() => {
                    expected = Some(v);
                }
                // Deleted already: a later activation supersedes it.
                Some((r, _)) if r.epoch == epoch => return Ok(false),
                _ => {
                    self.engine.unhost(repo, epoch);
                    return Ok(false);
                }
            }
        };
        self.written.lock().insert(
            repo.to_string(),
            Written {
                epoch,
                version,
                index: 0,
            },
        );
        self.engine.lead(repo, epoch);
        tracing::debug!(repo, epoch, "repository active");
        Ok(true)
    }

    /// Host incarnation `epoch` of `repo` (live on a peer) from its initial
    /// state, if this node is a member. It never acknowledged anything in it
    /// (a node that lost its log has a new id, and a removed node never
    /// comes back), so Raft's guarantees hold.
    async fn join_incarnation(&self, repo: &str, epoch: u64) -> Result<()> {
        if self.engine.hosted(repo).is_some_and(|h| h.epoch >= epoch) {
            return Ok(());
        }
        anyhow::ensure!(self.is_member(), "this node is not a member of the ledger");
        let data = self
            .objects
            .get_object(&epoch_path(repo, epoch))
            .await?
            .context("incarnation without an initial state")?;
        let rec = Record::from_bytes(&data)?;
        self.engine
            .host(repo, epoch, rec.meta.unwrap_or_default(), rec.voters)
            .await
    }

    /// Join incarnation `epoch` of `repo`, which a peer announced, unless it
    /// ended. An ended incarnation is never joined: this node would vote in
    /// it from a fresh state, though it may have voted in it before.
    async fn join_announced(&self, repo: &str, epoch: u64) -> Result<()> {
        if let Some((rec, _)) = enroute_shared::repo_state::latest(&self.objects, repo).await?
            && (rec.epoch > epoch || rec.epoch == epoch && is_deleted(&rec))
        {
            return Ok(());
        }
        self.join_incarnation(repo, epoch).await
    }

    /// The voters of a new incarnation: the members, read now (the claimant
    /// among them, so a claimed incarnation always has a host).
    async fn voters_for_claim(&self) -> Result<Vec<u64>> {
        let Some(registry) = &self.registry else {
            return Ok(vec![self.engine.id]);
        };
        let members = enroute_shared::registry::members(&self.objects)
            .await?
            .members;
        anyhow::ensure!(
            members.contains(&self.engine.id),
            "this node is not a member of the ledger"
        );
        // Its announcement must reach them all.
        if members.iter().any(|&m| registry.address(m).is_none()) {
            registry.round().await?;
        }
        Ok(members)
    }

    fn is_member(&self) -> bool {
        self.registry
            .as_ref()
            .is_none_or(|r| r.members().members.contains(&self.engine.id))
    }

    /// One round of lifecycle work, on what happened since the last (see
    /// `Events`): write the checkpoints that are due,
    /// and pass the members on to the engine.
    async fn lifecycle_round(&self) {
        if let Some(r) = &self.registry {
            let m = r.members();
            if m.removed.contains(&self.engine.id) {
                self.retire();
            }
            self.engine.set_members(m.members, m.removed);
        }
        let events = self.engine.take_events();
        let due = self.take_due(events.changed, &events.quiet, Instant::now());
        futures::stream::iter(due)
            .for_each_concurrent(
                self.config.checkpoint_concurrency.max(1),
                |repo| async move {
                    if let Err(e) = self.checkpoint_changed(&repo).await {
                        tracing::warn!(repo, "checkpoint: {e:#}");
                        self.due
                            .lock()
                            .insert(repo, Instant::now() + CHECKPOINT_RETRY);
                    }
                },
            )
            .await;
    }

    /// Schedule the `changed` repositories, and remove (and return) those
    /// whose checkpoint is due at `now`. A `quiet` repository is due at once.
    fn take_due(
        &self,
        changed: HashSet<String>,
        quiet: &HashSet<String>,
        now: Instant,
    ) -> Vec<String> {
        let mut due = self.due.lock();
        for repo in changed {
            let jitter = self
                .config
                .checkpoint_age
                .mul_f64(rand::random_range(0.5..1.0));
            due.entry(repo).or_insert(now + jitter);
        }
        // A quiet log's latest state goes to the object store now.
        for repo in quiet {
            if let Some(at) = due.get_mut(repo) {
                *at = now;
            }
        }
        let written = self.written.lock();
        let lagging = |repo: &str| {
            let index = written.get(repo).map_or(0, |w| w.index);
            self.engine
                .hosted(repo)
                .is_some_and(|h| h.applied >= index + CHECKPOINT_ENTRIES)
        };
        let ready: Vec<String> = due
            .iter()
            .filter(|(repo, at)| **at <= now || lagging(repo))
            .map(|(r, _)| r.clone())
            .collect();
        for r in &ready {
            due.remove(r);
        }
        ready
    }

    /// Checkpoint `repo` if this node leads it and its state moved past the
    /// last checkpoint written here. (A follower does not: if it becomes
    /// leader, the engine reports the repository again.)
    async fn checkpoint_changed(&self, repo: &str) -> Result<()> {
        let Some(h) = self.engine.hosted(repo) else {
            return Ok(());
        };
        if !h.leading {
            return Ok(());
        }
        let index = self
            .written
            .lock()
            .get(repo)
            .filter(|w| w.epoch == h.epoch)
            .map_or(0, |w| w.index);
        if h.applied <= index {
            return Ok(());
        }
        // Read from the state store (and the log, for inline layers).
        let name = repo.to_string();
        let Some(state) = self.blocking(move |e| e.record(&name)).await? else {
            return Ok(());
        };
        if state.epoch != h.epoch {
            return Ok(());
        }
        self.checkpoint(repo, &state).await
    }

    /// Swap `state` (of `repo`, at a log index of an incarnation) into the
    /// state object. Fails if the incarnation was superseded (and stops
    /// hosting it).
    async fn checkpoint(&self, repo: &str, state: &Record) -> Result<()> {
        let (epoch, index) = (state.epoch, state.index);
        if let Some(m) = state.meta.as_ref().filter(|m| m.deleted) {
            // Before the state object says deleted: no deletion leaves
            // files that nobody reclaims.
            let trash = Trash {
                upload_gen: m.upload_gen,
                ..Default::default()
            };
            let data = serde_json::to_vec(&trash)?.into();
            self.objects
                .put_if(&trash_path(repo, epoch), data, None)
                .await?;
        }
        let data = state.to_bytes();
        let mut from = self
            .written
            .lock()
            .get(repo)
            .filter(|w| w.epoch == epoch)
            .map(|w| w.version.clone());
        for _ in 0..3 {
            let version = if let Some(v) = from.take() {
                Some(v)
            } else {
                self.current_version(repo, epoch, index).await?
            };
            let Some(version) = version else {
                return Ok(()); // a newer leader got further
            };
            if let Some(version) = self
                .objects
                .put_if(&state_path(repo), data.clone(), Some(&version))
                .await?
            {
                self.written.lock().insert(
                    repo.to_string(),
                    Written {
                        epoch,
                        version,
                        index,
                    },
                );
                return Ok(());
            }
        }
        bail!("checkpointing {repo}: the state kept changing")
    }

    /// The version of `repo`'s state object, to swap from. `None`: it is
    /// already at or past log index `index`. Fails if incarnation `epoch`
    /// was superseded (and stops hosting it).
    async fn current_version(
        &self,
        repo: &str,
        epoch: u64,
        index: u64,
    ) -> Result<Option<ObjectVersion>> {
        let (cur, v) = enroute_shared::repo_state::latest(&self.objects, repo)
            .await?
            .context("repository state vanished")?;
        if cur.epoch != epoch {
            tracing::warn!(
                repo,
                epoch,
                current = cur.epoch,
                "incarnation superseded; no longer hosting it"
            );
            self.engine.unhost(repo, epoch);
            self.forget(repo);
            bail!(
                "incarnation {epoch} of {repo} was superseded by {}",
                cur.epoch
            );
        }
        Ok((cur.index < index).then_some(v))
    }

    /// Drop what this node keeps about `repo` for its lifecycle (its log
    /// is no longer hosted here).
    fn forget(&self, repo: &str) {
        self.written.lock().remove(repo);
        self.due.lock().remove(repo);
    }

    /// This node was removed from the ledger, for good: drop every log it
    /// still hosts (one whose leader removed it may never hear so).
    fn retire(&self) {
        let repos = self.engine.repos();
        if !repos.is_empty() {
            tracing::warn!(
                logs = repos.len(),
                "this node was removed from the ledger: dropping its logs"
            );
        }
        for repo in repos {
            if let Some(h) = self.engine.hosted(&repo) {
                self.engine.unhost(&repo, h.epoch);
                self.forget(&repo);
            }
        }
    }

    /// After a restart: drop local incarnations a newer one superseded, and
    /// those deleted elsewhere while this node was away. The state object
    /// is marked deleted only once the tombstone is committed, so every
    /// later leader has it; and an ended incarnation is never joined again.
    async fn drop_superseded(&self) {
        for repo in self.engine.repos() {
            let Some(h) = self.engine.hosted(&repo) else {
                continue;
            };
            let epoch = h.epoch;
            match enroute_shared::repo_state::latest(&self.objects, &repo).await {
                Ok(Some((rec, _)))
                    if rec.epoch > epoch
                        || rec.epoch == epoch && is_deleted(&rec) && !h.deleted =>
                {
                    tracing::info!(
                        repo,
                        epoch,
                        current = rec.epoch,
                        "dropping a superseded incarnation"
                    );
                    self.engine.unhost(&repo, epoch);
                    self.forget(&repo);
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(repo, "checking incarnation: {e:#}"),
            }
        }
    }
}

/// Runs the lifecycle rounds every `every` until the ledger is gone.
async fn lifecycle_loop(weak: std::sync::Weak<Ledger>, every: Duration) {
    if let Some(v) = weak.upgrade() {
        v.drop_superseded().await;
    }
    loop {
        tokio::time::sleep(every).await;
        let Some(v) = weak.upgrade() else { return };
        v.lifecycle_round().await;
    }
}

/// Joins each announced incarnation until the ledger is gone.
async fn join_loop(
    weak: std::sync::Weak<Ledger>,
    mut joins: mpsc::UnboundedReceiver<(String, u64)>,
) {
    while let Some((repo, epoch)) = joins.recv().await {
        let Some(ledger) = weak.upgrade() else { return };
        tokio::spawn(async move {
            if let Err(e) = ledger.join_announced(&repo, epoch).await {
                tracing::warn!(repo, epoch, "joining an incarnation: {e:#}");
            }
        });
    }
}

/// Whether a state is a deleted repository's.
fn is_deleted(rec: &Record) -> bool {
    rec.meta.as_ref().is_some_and(|m| m.deleted)
}

/// The initial state of incarnation `epoch`, with HEAD `head` if given
/// (kept, see `keep_head`). After the deleted incarnation `prev`, versions go
/// on increasing, and upload generations start past the window of every
/// file an older incarnation could name, so none is ever committed here.
fn initial(epoch: u64, head: Option<&str>, prev: Option<&Record>) -> RepoMeta {
    let mut m = RepoMeta::new();
    m.epoch = epoch;
    if let Some(head) = head {
        (m.head, m.keep_head) = (head.to_string(), true);
    }
    if let Some(p) = prev.and_then(|p| p.meta.as_ref()) {
        m.version = p.version + 1;
        m.upload_gen = p.upload_gen + UPLOAD_GEN_WINDOW + 1;
    }
    m
}

/// The `Ledger` service (`proto/ledger.proto`): what the compute calls, in
/// this process or over gRPC.
#[tonic::async_trait]
impl ledger_server::Ledger for Ledger {
    async fn activate(
        &self,
        req: Request<ActivateRequest>,
    ) -> Result<Response<ActivateResponse>, Status> {
        let r = req.into_inner();
        let exists = Ledger::activate(self, &r.repo, r.create, r.head.as_deref())
            .await
            .map_err(status)?;
        Ok(Response::new(ActivateResponse { exists }))
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        Ledger::delete(self, &req.into_inner().repo)
            .await
            .map_err(status)?;
        Ok(Response::new(DeleteResponse {}))
    }

    async fn describe(
        &self,
        req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        let repo = Ledger::describe(self, &req.into_inner().repo)
            .await
            .map_err(status)?;
        Ok(Response::new(DescribeResponse { repo }))
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let r = req.into_inner();
        // Saturating is safe: the limit is clamped.
        let limit = usize::try_from(r.limit)
            .unwrap_or(usize::MAX)
            .clamp(1, 1000);
        let (repos, more) = self
            .blocking(move |e| e.list(&r.prefix, &r.after, limit))
            .await
            .map_err(status)?;
        Ok(Response::new(ListResponse { repos, more }))
    }

    async fn propose(&self, req: Request<Command>) -> Result<Response<Outcome>, Status> {
        let t0 = Instant::now();
        let sent = enroute_shared::call_timing::sent(&req);
        let out = self
            .engine
            .propose(req.into_inner())
            .await
            .map_err(status)?;
        Ok(enroute_shared::call_timing::stamp(
            Response::new(out),
            sent,
            t0,
        ))
    }

    async fn read_barrier(
        &self,
        req: Request<ReadBarrierRequest>,
    ) -> Result<Response<ReadBarrierResponse>, Status> {
        let t0 = Instant::now();
        let sent = enroute_shared::call_timing::sent(&req);
        let repo = req.into_inner().repo;
        self.engine.read_barrier(&repo).await.map_err(status)?;
        let version = self.engine.hosted(&repo).map_or(0, |h| h.version);
        Ok(enroute_shared::call_timing::stamp(
            Response::new(ReadBarrierResponse { version }),
            sent,
            t0,
        ))
    }

    type FollowStream = FollowStream;

    async fn follow(&self, req: Request<FollowRequest>) -> Result<Response<FollowStream>, Status> {
        use futures::StreamExt;
        let rx = self
            .engine
            .follow(&req.into_inner().repo)
            .await
            .map_err(status)?;
        let updates =
            futures::stream::unfold(rx, |mut rx| async move { Some((Ok(rx.recv().await?), rx)) });
        Ok(Response::new(updates.boxed()))
    }
}
