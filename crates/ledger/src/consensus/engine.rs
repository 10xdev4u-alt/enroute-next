//! The consensus engine: one replicated log per repository, all driven by
//! one event loop on top of the node's log store ([`crate::logdb`]).
//!
//! **Incarnations.** Incarnation `epoch` of a repository's log is hosted
//! from an initial state in the object store (see the ledger,
//! `enroute-ledger`): an empty one when the repository is created (epoch 1).
//! Its log id is derived from (repo, epoch), and its identity is part of
//! its snapshot data, so a recovering node knows which log is which. Peers
//! learn of a new incarnation from a `Host` frame. A newer incarnation
//! supersedes an older one hosted here, which is dropped.
//!
//! Each loop iteration takes whatever inputs are pending (proposals, peer
//! messages, read requests, ticks), collects the `Ready` of every log that
//! was touched, makes all of their changes durable with one log store
//! write (group commit), sends messages batched per peer, applies committed
//! entries, and answers proposers. Inputs that arrive during the fsync are
//! batched into the next iteration. Each log is a raft group in the log
//! store: its entries, plus its hard state and where it starts (its latest
//! snapshot's metadata) as key-value pairs.
//!
//! **State machines on disk.** Every log's state machine is in the node's
//! state store ([`crate::state_store`], laid out by [`crate::stored`]):
//! committed entries are applied in atomic batches that also record them
//! as applied, unsynced, on the apply thread ([`super::apply`]), which
//! also answers proposers. Nothing of a repository's state stays in
//! memory, so an idle or huge repository costs the engine no more than a
//! small one. Three rules keep the two stores in step:
//! - a log is truncated (compacted) only up to an entry the state store has
//!   durably applied, as the apply thread reports after each sync;
//! - nor past an inline layer's entry until the layer is flushed (its bytes
//!   are only there; see `stored`), as of that same durable point;
//! - on restart, each log replays its committed entries after the state's
//!   applied index. A snapshot installed from a leader (or a new log's
//!   initial state) is durable in the state store before the log is reset
//!   to it (or created): a crash in between leaves the state ahead of the
//!   log, which recovery resets (or a state without a log, dropped).
//!
//! A round that needs the latter waits, before its log write, for the
//! apply thread to have applied and synced everything handed to it.
//!
//! Raft snapshots are made when needed, from the state store. Whole states
//! (a new subscriber's, a checkpoint's) are read from it too.
//!
//! **Membership.** A log's leader moves its voters toward the ledger's
//! members ([`Engine::set_members`]), one change at a time (see
//! `reconcile`). The apply thread records each change with the state.
//!
//! **Quiet logs.** Only recently active logs are ticked. Real traffic
//! (proposals, appends, votes) keeps a log active; heartbeats do not. A
//! leader keeps ticking (heartbeating) for longer after activity than
//! followers keep their election timers running, so an idle log goes quiet
//! without spurious elections, and pre-vote protects against stragglers. A
//! node with work for a log whose leader it does not know sends a *wake*
//! frame so the quiet leader resumes and reveals itself.
//!
//! **Proposals** are tagged with a node-unique id in the entry context.
//! Followers forward them to the leader (raft-rs does this), and since every
//! node applies every entry, the node that accepted the request answers it
//! when it applies the entry.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use parking_lot::{Mutex, RwLock};
use prost::Message as _;
use raft::prelude::*;
use raft::{Config as RaftCfg, RawNode, StateRole};
use raft_engine::LogBatch;
use tokio::sync::{mpsc, oneshot};

use enroute_shared::proto::{Record, RepoSummary, Update};
use enroute_shared::registry::Addresses;
use enroute_shared::roles::NotHosted;
use enroute_shared::state::{Command, Outcome, RepoMeta};

use crate::logdb::{LogDb, WeakLogDb, is_log};
use crate::state_machine::{self, count};
use crate::state_store::StateStore;
use crate::stored;

use super::apply::{Applier, Job, Progress};
use super::codec::{Entries, Prost};
use super::log_id;
use super::logstore::{LogMeta, LogStore, decode, fetch, load};
use super::stats::{Window, pct, per, rate};
use super::transport::{Frame, Transport};

const TICK: Duration = Duration::from_millis(100);
const ELECTION_TICKS: usize = 10;
const HEARTBEAT_TICKS: usize = 2;
/// How long a follower keeps ticking after real activity.
const FOLLOWER_ACTIVE: Duration = Duration::from_secs(3);
/// How long a leader keeps ticking (heartbeating) after real activity.
const LEADER_ACTIVE: Duration = Duration::from_secs(6);
const PROPOSAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Re-issue an unanswered read index request after this long.
const READ_RETRY: Duration = Duration::from_millis(500);
/// Keys of a log's state in the log store.
const SNAPSHOT_KEY: &[u8] = b"snapshot";
const HARD_STATE_KEY: &[u8] = b"hard_state";
/// How often old log store files are purged.
const PURGE_EVERY: Duration = Duration::from_secs(10);
/// Updates a subscriber may fall behind before it is dropped.
const FOLLOW_QUEUE: usize = 256;
/// Leaders re-announce their active logs every this many ticks.
const ANNOUNCE_TICKS: u64 = 20;
/// Logs the membership sweep visits per tick.
const RECONCILE_PER_TICK: usize = 100;
/// A learner this close to the commit index is caught up.
const CATCH_UP: u64 = 100;

pub(super) enum Input {
    Propose(Command, oneshot::Sender<Result<Outcome>>),
    ReadIndex(u64, oneshot::Sender<Result<u64>>),
    Frames(Vec<Frame>),
    Unreachable(u64, Vec<u64>),
    /// The ledger's members, and the nodes removed from it.
    Members(Vec<u64>, Vec<u64>),
    Host {
        repo: String,
        epoch: u64,
        meta: RepoMeta,
        voters: Vec<u64>,
        resp: oneshot::Sender<Result<()>>,
    },
    Start(String, u64),
    Unhost(String, u64),
    /// Subscribe to a hosted repository's log (see [`Engine::follow`]).
    Follow(String, mpsc::Sender<Update>, oneshot::Sender<Result<()>>),
    /// Purging old log store files is held back by these logs: compact them
    /// (or drop them, if not hosted).
    Purge(Vec<u64>),
    /// From the apply thread: these logs applied entries (see [`Progress`]).
    Applied(Vec<u64>),
    /// From the apply thread, after a sync: logs durably applied up to
    /// these indexes, and their oldest pinned entry then.
    Durable(Vec<(u64, u64, Option<u64>)>),
    Stop,
}

/// What happened to this node's logs since the ledger last looked (see
/// [`Engine::take_events`]): what its lifecycle work (checkpoints) acts
/// on, instead of visiting every hosted repository.
#[derive(Default, Debug)]
pub(crate) struct Events {
    /// Repositories whose state changed, or whose log this node started
    /// leading.
    pub(crate) changed: HashSet<String>,
    /// Repositories whose log went quiet (stopped ticking: no traffic for a
    /// few seconds).
    pub(crate) quiet: HashSet<String>,
}

/// A hosted repository, as readers outside the loop see it (see
/// [`Engine::hosted`]). Its state is in the state store.
#[derive(Clone, Debug)]
pub struct Hosted {
    /// Its log's id.
    pub log: u64,
    /// The incarnation of the repository this log serves.
    pub epoch: u64,
    /// Its state's version.
    pub version: u64,
    /// The log index its state is at.
    pub applied: u64,
    /// Whether this node leads its log.
    pub leading: bool,
    /// The leader this node knows of (0: none), and its term. A paused or
    /// cut-off leader still names itself until it hears of a newer term.
    pub leader: u64,
    /// The term of that leader.
    pub term: u64,
    /// Whether its state is a tombstone (the repository was deleted).
    pub deleted: bool,
}

/// A hosted log's published entry. Only the loop writes the map, `leading`,
/// `leader` and `term`, and only the apply thread writes `progress`, so
/// neither checks that a log is still its repository's.
struct Published {
    log: u64,
    epoch: u64,
    leading: bool,
    leader: u64,
    term: u64,
    progress: Arc<Mutex<Progress>>,
}

impl Published {
    fn hosted(&self) -> Hosted {
        let p = *self.progress.lock();
        Hosted {
            log: self.log,
            epoch: self.epoch,
            version: p.version,
            applied: p.applied,
            leading: self.leading,
            leader: self.leader,
            term: self.term,
            deleted: p.deleted,
        }
    }
}

/// Counters of the consensus engine, for the status endpoint.
#[derive(Default, Clone, Debug, serde::Serialize)]
pub struct Stats {
    /// Hosted logs.
    pub logs: usize,
    /// Logs that are ticking (recent traffic).
    pub active_logs: usize,
    /// Raft messages sent to peers.
    pub messages_sent: u64,
    /// Writes to the log store.
    pub log_writes: u64,
    /// Bytes written to the log store.
    pub log_bytes: u64,
    /// Snapshots installed from a leader (a follower too far behind).
    pub snapshots_installed: u64,
    /// Log truncations (compactions).
    pub compactions: u64,
}

/// This node's consensus engine: one Raft group per hosted repository.
pub struct Engine {
    tx: mpsc::UnboundedSender<Input>,
    /// The stores, owned by the loop (so they close once it has stopped).
    state: std::sync::Weak<StateStore>,
    db: WeakLogDb,
    /// True once the loop and the apply thread have stopped, closing the
    /// stores.
    stopped: tokio::sync::watch::Receiver<bool>,
    hosted: Arc<RwLock<BTreeMap<String, Published>>>,
    events: Arc<Mutex<Events>>,
    stats: Arc<RwLock<Stats>>,
    /// This node's Raft id (its store id).
    pub id: u64,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

struct Log {
    raw: RawNode<LogStore>,
    repo: String,
    epoch: u64,
    /// As the apply thread publishes it (also in [`Published`]).
    progress: Arc<Mutex<Progress>>,
    /// The last entry durably applied, and the oldest entry pinned by an
    /// inline layer then: how far the log may be truncated.
    durable: (u64, Option<u64>),
    /// Ticked until then (see `Loop::ticking`).
    active_until: Instant,
    /// The term last published to `hosted`. A hard state comes with most
    /// commits, but its term changes seldom.
    published_term: u64,
}

impl Log {
    /// The log's other voters and learners: whom to send to.
    fn peers(&self, me: u64) -> Vec<u64> {
        let conf = self.raw.raft.prs().conf().to_conf_state();
        conf.voters
            .into_iter()
            .chain(conf.learners)
            .filter(|&p| p != me)
            .collect()
    }

    /// Whether this node is the log's only voter (it can lead alone).
    fn solo(&self) -> bool {
        let voters = self.raw.raft.prs().conf().voters().ids();
        voters.len() == 1 && voters.contains(self.raw.raft.id)
    }
}

/// A proposal, or a read barrier, waiting for its log to know a leader.
struct Waiting {
    log: u64,
    deadline: Instant,
    kind: Wait,
}

enum Wait {
    Propose {
        pid: u64,
        data: Vec<u8>,
        resp: oneshot::Sender<Result<Outcome>>,
    },
    Read(oneshot::Sender<Result<u64>>),
}

struct Pending {
    resp: oneshot::Sender<Result<Outcome>>,
    deadline: Instant,
}

/// A read index request issued to raft.
struct Read {
    log: u64,
    resp: oneshot::Sender<Result<u64>>,
    issued: Instant,
    deadline: Instant,
}

struct Loop {
    node_id: u64,
    db: LogDb,
    state: Arc<StateStore>,
    snapshot_every: u64,
    /// Writes for the next round beyond its `Ready`s (compactions, dropped
    /// logs).
    batch: LogBatch,
    /// The apply thread's queue.
    applier: std::sync::mpsc::Sender<Job>,
    /// Whether `batch` may only be written once the apply thread has
    /// applied and synced everything handed to it: it starts logs (hosted,
    /// or reset to a snapshot) whose state was just handed over.
    barrier: bool,
    transport: Option<Transport>,
    logs: HashMap<u64, Log>,
    /// What readers outside the loop see; also the hosted logs by repository.
    hosted: Arc<RwLock<BTreeMap<String, Published>>>,
    /// For the ledger's lifecycle work.
    events: Arc<Mutex<Events>>,
    /// Logs being ticked (their `active_until` not passed when last
    /// ticked): the only ones a tick visits. A quiet log costs nothing per
    /// tick.
    ticking: HashSet<u64>,
    /// Incarnations a peer announced without their state: the ledger loads
    /// it from the object store, then hosts them.
    announced: mpsc::UnboundedSender<(String, u64)>,
    /// Logs hosted since the last write, whose identity (initial snapshot)
    /// must be written with their first entries.
    fresh: Vec<u64>,
    /// The ledger's members, the nodes removed from it, and the logs the
    /// sweep has yet to visit.
    members: Vec<u64>,
    removed: Vec<u64>,
    to_reconcile: std::collections::VecDeque<u64>,
    /// Callers of `host` to answer once the hosted log is durable.
    hosted_replies: Vec<oneshot::Sender<Result<()>>>,
    pending: HashMap<u64, Pending>,
    waiting: Vec<Waiting>,
    /// Read index requests in flight.
    reads: HashMap<u64, Read>,
    /// Read barriers with their read index, until applied here.
    read_applied: Vec<(u64, u64, oneshot::Sender<Result<u64>>)>,
    next_id: u64,
    dirty: HashSet<u64>,
    ticks: u64,
    logger: slog::Logger,
    stats: Stats,
    shared_stats: Arc<RwLock<Stats>>,
    /// Round statistics since they were last logged (`enroute::ledger_timing`).
    rounds: Window<Rounds>,
}

/// What the loop's rounds did over a window: how many, how long they took
/// (and their durable write), how many inputs and logs each saw.
#[derive(Default)]
struct Rounds {
    count: u64,
    busy: Duration,
    write: Duration,
    inputs: u64,
    logs: u64,
}

fn raft_config(node_id: u64, applied: u64) -> RaftCfg {
    RaftCfg {
        id: node_id,
        election_tick: ELECTION_TICKS,
        heartbeat_tick: HEARTBEAT_TICKS,
        applied,
        pre_vote: true,
        check_quorum: true,
        max_size_per_msg: 1 << 20,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

fn is_real_traffic(m: &Message) -> bool {
    !matches!(
        m.get_msg_type(),
        MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse
    )
}

fn stopped<T>(_: T) -> anyhow::Error {
    anyhow::anyhow!("consensus engine stopped")
}

/// The logs recovered at start.
struct RecoveredLogs {
    logs: HashMap<u64, Log>,
    hosted: BTreeMap<String, Published>,
    batch: LogBatch,
    unapplied: HashSet<u64>,
}

impl RecoveredLogs {
    fn drop_log(&mut self, state: &StateStore, log: u64) -> Result<()> {
        self.batch.add_command(log, raft_engine::Command::Clean);
        state.delete(log)
    }

    /// Recover log `log` from what was `read` of it, or drop it.
    fn add(
        &mut self,
        db: &LogDb,
        state: &Arc<StateStore>,
        node: u64,
        logger: &slog::Logger,
        log: u64,
        read: (Option<Snapshot>, Vec<Entry>),
    ) -> Result<()> {
        let (snapshot, entries) = read;
        let Some(r) = recover(db, state, log, node, snapshot, entries, &mut self.batch)
            .with_context(|| format!("recovering log {log:x}"))?
        else {
            return self.drop_log(state, log);
        };
        if self.hosted.get(&r.repo).is_some_and(|p| p.epoch > r.epoch) {
            return self.drop_log(state, log);
        }
        if r.applied < r.meta.hard_state.commit.min(r.meta.last_index()) {
            self.unapplied.insert(log);
        }
        let progress = Arc::new(Mutex::new(Progress {
            version: state.root(log).map_or(0, |root| root.version),
            applied: r.applied,
            deleted: state.round().counters(log)?.deleted,
        }));
        let published = Published {
            log,
            epoch: r.epoch,
            leading: false,
            leader: 0,
            term: 0,
            progress: progress.clone(),
        };
        if let Some(old) = self.hosted.insert(r.repo.clone(), published) {
            self.logs.remove(&old.log);
            self.drop_log(state, old.log)?;
        }
        let store = LogStore::new(log, db.clone(), state.clone(), r.meta);
        self.logs.insert(
            log,
            Log {
                raw: RawNode::new(&raft_config(node, r.applied), store, logger)?,
                repo: r.repo,
                epoch: r.epoch,
                progress,
                // Nothing replayed counts until the apply thread's first sync.
                durable: (0, None),
                active_until: Instant::now(),
                published_term: 0,
            },
        );
        Ok(())
    }
}

/// Recover every log in the log store, applying its committed entries
/// synchronously (raft hands over those from a membership change on). Logs
/// whose incarnation ended (or that are not repository logs) are not
/// hosted again, and dropped.
fn recover_all(
    db: &LogDb,
    state: &Arc<StateStore>,
    node: u64,
    logger: &slog::Logger,
) -> Result<RecoveredLogs> {
    let mut rec = RecoveredLogs {
        logs: HashMap::new(),
        hosted: BTreeMap::new(),
        batch: LogBatch::default(),
        unapplied: HashSet::new(),
    };
    let groups: Vec<u64> = db
        .raft_groups()
        .into_iter()
        .filter(|&l| is_log(l))
        .collect();
    // Entries are read on the store's reader thread, many logs at a time.
    for chunk in groups.chunks(RECOVER_CHUNK) {
        let ids = chunk.to_vec();
        let mut read: HashMap<u64, (Option<Snapshot>, Vec<Entry>)> = db.read(move |db| {
            ids.into_iter()
                .map(|log| Ok((log, read_log(db, log)?)))
                .collect::<raft_engine::Result<_>>()
        })?;
        for &log in chunk {
            let entries = read.remove(&log).unwrap_or_default();
            rec.add(db, state, node, logger, log, entries)?;
        }
    }
    // States without a log: hosting or unhosting was interrupted.
    for log in state.logs() {
        if !rec.logs.contains_key(&log) {
            state.delete(log)?;
        }
    }
    // What recovery wrote, durable before any log write relies on it.
    state.persist()?;
    Ok(rec)
}

/// Purge old log store files; logs holding them back get compacted.
fn spawn_purge(db: WeakLogDb, tx: mpsc::UnboundedSender<Input>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(PURGE_EVERY).await;
            if !purge_pass(&db, &tx).await {
                return;
            }
        }
    });
}

/// One purge pass; false once the engine has stopped.
async fn purge_pass(db: &WeakLogDb, tx: &mpsc::UnboundedSender<Input>) -> bool {
    // The loop owns the store: once it has stopped, so do we.
    let Some(db) = db.upgrade() else { return false };
    match tokio::task::spawn_blocking(move || db.read(raft_engine::Engine::purge_expired_files))
        .await
    {
        Ok(Ok(ids)) if !ids.is_empty() => tx.send(Input::Purge(ids)).is_ok(),
        Ok(Ok(_)) => true,
        Ok(Err(e)) => {
            tracing::warn!("purging the log store: {e:#}");
            true
        }
        Err(_) => false,
    }
}

impl Engine {
    /// Start the engine on `db` (the log store) and `state` (the state
    /// store) as node `id`, snapshotting each log every `snapshot_every`
    /// applied entries; `addresses`: peers' addresses (None for a
    /// standalone node, the only voter of its logs, with no transport).
    /// Announced incarnations to join go to `announced`.
    pub(crate) fn start(
        db: LogDb,
        state: Arc<StateStore>,
        snapshot_every: u64,
        id: u64,
        addresses: Option<Addresses>,
        announced: mpsc::UnboundedSender<(String, u64)>,
    ) -> Result<Arc<Engine>> {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let RecoveredLogs {
            logs,
            hosted,
            batch,
            unapplied,
        } = recover_all(&db, &state, id, &logger)?;
        let hosted = Arc::new(RwLock::new(hosted));
        let events: Arc<Mutex<Events>> = Arc::default();
        let counters: Arc<RwLock<Stats>> = Arc::default();

        let (tx, rx) = mpsc::unbounded_channel();
        let transport = addresses.map(|a| Transport::new(a, tx.clone()));
        let purge_db = db.downgrade();
        let (weak_state, weak_db) = (Arc::downgrade(&state), db.downgrade());
        let (jobs, queue) = std::sync::mpsc::channel();
        let applier = Applier::new(
            state.clone(),
            db.clone(),
            logs.iter().map(|(&id, l)| (id, l.progress.clone())),
            tx.clone(),
        );
        let apply_thread = std::thread::Builder::new()
            .name("ledger-apply".into())
            .spawn(move || applier.run(&queue))?;
        let lp = Loop {
            next_id: id << 48,
            node_id: id,
            db,
            state,
            snapshot_every,
            batch,
            applier: jobs,
            barrier: false,
            transport,
            logs,
            hosted: hosted.clone(),
            events: events.clone(),
            ticking: HashSet::new(),
            announced,
            fresh: Vec::new(),
            members: Vec::new(),
            removed: Vec::new(),
            to_reconcile: std::collections::VecDeque::default(),
            hosted_replies: Vec::new(),
            pending: HashMap::new(),
            waiting: Vec::new(),
            reads: HashMap::new(),
            read_applied: Vec::new(),
            dirty: unapplied,
            ticks: 0,
            logger,
            stats: Stats::default(),
            shared_stats: counters.clone(),
            rounds: Window::new(),
        };
        let (done, stopped) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            // The loop, and with it the stores, are dropped when it returns;
            // the apply thread then finishes its queue and drops its own.
            lp.run(rx).await;
            drop(tokio::task::spawn_blocking(move || apply_thread.join()).await);
            if done.send(true).is_err() {
                tracing::debug!("nobody waits for the engine to stop");
            }
        });
        spawn_purge(purge_db, tx.clone());
        Ok(Arc::new(Engine {
            tx,
            state: weak_state,
            db: weak_db,
            stopped,
            hosted,
            events,
            stats: counters,
            id,
        }))
    }

    /// Order and apply a command; resolves once applied on this node.
    pub(crate) async fn propose(&self, cmd: Command) -> Result<Outcome> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Input::Propose(cmd, tx)).map_err(stopped)?;
        rx.await.map_err(stopped)?
    }

    /// Linearizable read barrier: returns once this node has applied
    /// everything committed to `repo`'s log before the call (the repository
    /// must be active here).
    pub(crate) async fn read_barrier(&self, repo: &str) -> Result<()> {
        let log = log_of(&self.hosted, repo).ok_or(NotHosted)?;
        let (tx, rx) = oneshot::channel();
        self.tx.send(Input::ReadIndex(log, tx)).map_err(stopped)?;
        tokio::time::timeout(PROPOSAL_TIMEOUT, rx)
            .await
            .context("read barrier timed out")?
            .map_err(stopped)??;
        Ok(())
    }

    /// Follow `repo`'s log (it must be hosted here): its state, then every
    /// change after it. The stream ends when the log is dropped, or when the
    /// subscriber falls `FOLLOW_QUEUE` updates behind.
    pub(crate) async fn follow(&self, repo: &str) -> Result<mpsc::Receiver<Update>> {
        let (tx, rx) = mpsc::channel(FOLLOW_QUEUE);
        let (resp, done) = oneshot::channel();
        self.tx
            .send(Input::Follow(repo.to_string(), tx, resp))
            .map_err(stopped)?;
        done.await.map_err(stopped)??;
        Ok(rx)
    }

    /// The ledger's members, and the nodes removed from it (see
    /// [`enroute_shared::registry`]).
    pub(crate) fn set_members(&self, members: Vec<u64>, removed: Vec<u64>) {
        drop(self.tx.send(Input::Members(members, removed)));
    }

    /// Deliver frames received from a peer.
    pub(crate) fn receive(&self, frames: Vec<Frame>) {
        drop(self.tx.send(Input::Frames(frames)));
    }

    /// Host incarnation `epoch` of `repo`'s log, starting from `meta` (as
    /// every replica does, from the same state in the object store), with
    /// `voters`. Resolves once the log is durable in the log store, quiet:
    /// see [`lead`](Self::lead).
    pub(crate) async fn host(
        &self,
        repo: &str,
        epoch: u64,
        meta: RepoMeta,
        voters: Vec<u64>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let input = Input::Host {
            repo: repo.to_string(),
            epoch,
            meta,
            voters,
            resp: tx,
        };
        self.tx.send(input).map_err(stopped)?;
        rx.await.map_err(stopped)?
    }

    /// Lead a hosted incarnation: announce it to peers (with its state, so
    /// they host it before they hear from it) and campaign for it.
    pub(crate) fn lead(&self, repo: &str, epoch: u64) {
        drop(self.tx.send(Input::Start(repo.to_string(), epoch)));
    }

    /// What happened to this node's logs since the last call.
    pub(crate) fn take_events(&self) -> Events {
        std::mem::take(&mut *self.events.lock())
    }

    /// Stop hosting incarnation `epoch` of `repo` (it ended, or a newer one
    /// exists), and drop its log.
    pub(crate) fn unhost(&self, repo: &str, epoch: u64) {
        drop(self.tx.send(Input::Unhost(repo.to_string(), epoch)));
    }

    /// The hosted incarnation of `repo`: its log and epoch, how far its
    /// state is applied, and whether this node leads it.
    pub fn hosted(&self, repo: &str) -> Option<Hosted> {
        self.hosted.read().get(repo).map(Published::hosted)
    }

    /// The identity and whole state of the hosted incarnation of `repo`,
    /// at the last entry applied, read from the state store (and the log:
    /// inline layers' bytes). Blocking. None if not hosted here.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine has stopped, or the state cannot be
    /// read.
    pub fn record(&self, repo: &str) -> Result<Option<Record>> {
        let Some(log) = log_of(&self.hosted, repo) else {
            return Ok(None);
        };
        let (Some(state), Some(db)) = (self.state.upgrade(), self.db.upgrade()) else {
            return Err(stopped(()));
        };
        Ok(load(&state, &db, log)?.map(|(root, meta)| root.record(meta)))
    }

    /// The repository's current state on this node, read from the state
    /// store. Blocking.
    pub fn repo_meta(&self, repo: &str) -> Option<RepoMeta> {
        match self.record(repo) {
            Ok(r) => r.and_then(|r| r.meta),
            Err(e) => {
                tracing::warn!(repo, "reading the state: {e:#}");
                None
            }
        }
    }

    /// Repositories active (hosted) on this node.
    #[must_use]
    pub fn repos(&self) -> Vec<String> {
        self.hosted.read().keys().cloned().collect()
    }

    /// The summaries of up to `limit` hosted repositories (deleted ones
    /// too) named with `prefix` and after `after`, in name order; and
    /// whether more follow. Blocking.
    pub(crate) fn list(
        &self,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<(Vec<RepoSummary>, bool)> {
        use std::ops::Bound;
        let from = if after >= prefix {
            Bound::Excluded(after)
        } else {
            Bound::Included(prefix)
        };
        let (page, more) = {
            let hosted = self.hosted.read();
            let mut names = hosted
                .range::<str, _>((from, Bound::Unbounded))
                .take_while(|(n, _)| n.starts_with(prefix));
            let page: Vec<_> = names
                .by_ref()
                .take(limit)
                .map(|(n, p)| (n.clone(), p.log, p.epoch))
                .collect();
            (page, names.next().is_some())
        };
        let state = self.state.upgrade().ok_or_else(|| stopped(()))?;
        let mut round = state.round();
        let page = page
            .into_iter()
            // (Not if dropped meanwhile.)
            .filter(|(_, log, _)| state.contains(*log))
            .map(|(name, log, epoch)| stored::summary(&mut round, log, &name, epoch))
            .collect::<Result<_>>()?;
        Ok((page, more))
    }

    /// The summary of the hosted incarnation of `repo`: its identity and
    /// what describes it (no refs or layers). Blocking.
    pub(crate) fn summary(&self, repo: &str) -> Result<Option<RepoSummary>> {
        let Some(h) = self.hosted(repo) else {
            return Ok(None);
        };
        let state = self.state.upgrade().ok_or_else(|| stopped(()))?;
        Ok(Some(stored::summary(
            &mut state.round(),
            h.log,
            repo,
            h.epoch,
        )?))
    }

    /// A snapshot of the engine's counters.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats.read().clone()
    }

    /// The `Raft` gRPC service for this engine (the transport between
    /// ledger nodes), to mount on the node's server.
    #[must_use]
    pub fn raft_server(
        self: &Arc<Self>,
    ) -> crate::proto::raft_server::RaftServer<super::transport::RaftService> {
        super::transport::server(Arc::downgrade(self))
    }

    /// What this node knows of `repo`'s log, or `None` if it does not host
    /// it. Each node answers from its own view, so ask a majority to find
    /// the leader.
    #[must_use]
    pub fn repo_status(&self, repo: &str) -> Option<serde_json::Value> {
        let h = self.hosted(repo)?;
        Some(serde_json::json!({
            "node": self.id,
            "repo": repo,
            "epoch": h.epoch,
            "term": h.term,
            "leader": (h.leader != 0).then_some(h.leader),
            "leading": h.leading,
            "version": h.version,
            "applied": h.applied,
        }))
    }

    /// This node's view of its hosted repositories, for the status endpoint.
    #[must_use]
    pub fn status(&self) -> serde_json::Value {
        let hosted = self.hosted.read();
        serde_json::json!({
            "node": self.id,
            "repos": hosted.len(),
            "leading": hosted.values().filter(|h| h.leading).count(),
            "engine": self.stats(),
        })
    }

    /// Ask the loop to stop.
    pub fn shutdown(&self) {
        drop(self.tx.send(Input::Stop));
    }

    /// Wait until the engine has stopped (see [`shutdown`](Self::shutdown))
    /// and closed its stores, so their files may be copied.
    pub async fn stopped(&self) {
        drop(self.stopped.clone().wait_for(|&s| s).await);
    }
}

/// The log hosted for `repo`.
fn log_of(hosted: &RwLock<BTreeMap<String, Published>>, repo: &str) -> Option<u64> {
    hosted.read().get(repo).map(|p| p.log)
}

/// A log recovered at startup.
struct Recovered {
    meta: LogMeta,
    repo: String,
    epoch: u64,
    applied: u64,
}

/// Logs whose entries recovery reads in one go.
const RECOVER_CHUNK: usize = 1000;

/// A log as stored: where it starts (its snapshot), and its entries after
/// that.
fn read_log(
    db: &raft_engine::Engine,
    id: u64,
) -> raft_engine::Result<(Option<Snapshot>, Vec<Entry>)> {
    let snapshot = db.get_value::<Snapshot, Prost>(id, SNAPSHOT_KEY)?;
    let start = snapshot.as_ref().map_or(0, |s| s.get_metadata().index);
    let entries = match db.last_index(id).filter(|&l| l > start) {
        Some(last) => fetch(db, id, start + 1, last + 1, None)?,
        None => Vec::new(),
    };
    Ok((snapshot, entries))
}

/// Load log `id` (where it starts, and `entries` after that) and bring its
/// state machine (in the state store) up to its commit index. None if the
/// log is not one of ours (no state, or no snapshot: torn). Repairs to the
/// log go in `batch`.
fn recover(
    db: &LogDb,
    state: &StateStore,
    id: u64,
    me: u64,
    snapshot: Option<Snapshot>,
    mut entries: Vec<Entry>,
    batch: &mut LogBatch,
) -> Result<Option<Recovered>> {
    let (Some(snapshot), Some(root)) = (snapshot, state.root(id)) else {
        return Ok(None);
    };
    let hard_state = db
        .get_value::<HardState, Prost>(id, HARD_STATE_KEY)?
        .unwrap_or_default();
    let mut meta = LogMeta::default();
    (meta.hard_state, meta.snapshot) = (hard_state, snapshot);
    meta.conf = root.conf();
    let start = meta.snapshot.get_metadata().index;
    let applied = root.applied_index;
    // The log was truncated past what the state store kept: the entries in
    // between are gone (the sync before truncating failed us).
    anyhow::ensure!(
        applied >= start,
        "its state is at entry {applied}, before the log's start ({start})"
    );
    meta.append(entries.iter().map(|e| (e.index, e.term)));
    if applied > meta.last_index() {
        // A snapshot from the leader was installed in the state store, but
        // the log was not reset to it: reset it now.
        let snap = meta.snapshot_at(applied, root.applied_term);
        put_snapshot(batch, id, &snap);
        batch.add_command(id, raft_engine::Command::Compact { index: applied + 1 });
        meta.apply_snapshot(snap);
        batch.put_value::<Prost, _>(id, HARD_STATE_KEY.to_vec(), &meta.hard_state)?;
        entries.clear();
    }
    if meta.conf.voters == [me] {
        // With a single voter, every durable entry is committed.
        meta.hard_state.commit = meta.hard_state.commit.max(meta.last_index());
    }
    // Entries applied were committed, whether or not the hard state saying
    // so was written.
    meta.hard_state.commit = meta.hard_state.commit.max(applied);
    let commit = meta.hard_state.commit.min(meta.last_index());
    // Up to the first membership change (raft must apply it).
    let mut round = state.round();
    let mut replayed = applied;
    for e in entries
        .iter()
        .filter(|e| e.index > applied && e.index <= commit)
    {
        if conf_change(e).is_some() {
            break;
        }
        stored::apply(&mut round, id, e.index, e.term, decode(id, e).as_ref())?;
        replayed = e.index;
    }
    round.commit()?;
    Ok(Some(Recovered {
        meta,
        repo: root.repo,
        epoch: root.epoch,
        applied: replayed,
    }))
}

/// A membership change entry's change (only `ConfChangeV2`s are proposed).
pub(super) fn conf_change(e: &Entry) -> Option<ConfChangeV2> {
    use raft_prost::Message as _;
    match e.get_entry_type() {
        EntryType::EntryConfChangeV2 => ConfChangeV2::decode(e.data.as_slice()).ok(),
        _ => None,
    }
}

/// What a round changes in one log's storage view once it is durable: an
/// installed snapshot, appended entries as (index, term), the hard state.
type Persisted = (u64, Option<Snapshot>, Vec<(u64, u64)>, Option<HardState>);

/// What a round collects from the logs' `Ready`s, for after its write.
#[derive(Default)]
struct RoundWork {
    /// Rounds handed off this pass (log, `Ready` number).
    rounds: Vec<(u64, u64)>,
    /// What each changes in the logs' storage view once durable.
    persisted_state: Vec<Persisted>,
    /// Messages that may only be sent once durable.
    held: Vec<(u64, Vec<Message>)>,
    /// Logs this node was removed from.
    removed: Vec<u64>,
}

/// Hand a snapshot from the leader to the apply thread.
fn hand_snapshot(
    applier: &std::sync::mpsc::Sender<Job>,
    l: &Log,
    id: u64,
    snap: &mut Snapshot,
) -> Result<()> {
    let rec = Record::decode(snap.data.as_slice())?;
    snap.data.clear();
    let md = snap.get_metadata();
    let root = stored::root(&l.repo, l.epoch, md.get_conf_state(), md.index, md.term);
    hand(
        applier,
        Job::Install {
            log: id,
            root,
            meta: rec.meta.unwrap_or_default(),
        },
    )
}

/// Queue the log's reset to a snapshot from the leader in `batch`.
fn reset_to_snapshot(batch: &mut LogBatch, l: &Log, id: u64, snap: &Snapshot) {
    put_snapshot(batch, id, snap);
    let last = l.raw.store().meta.read().last_index();
    let index = snap.get_metadata().index.max(last) + 1;
    batch.add_command(id, raft_engine::Command::Compact { index });
}

/// Publish a log's leader and term to `hosted` when they changed.
fn publish_role(
    hosted: &RwLock<BTreeMap<String, Published>>,
    events: &Mutex<Events>,
    l: &mut Log,
    soft_state: bool,
) {
    // A new leader comes in the soft state. A new term alone (a
    // follower that missed an election) comes only in the hard state.
    if !soft_state && l.raw.raft.term == l.published_term {
        return;
    }
    let mut hosted = hosted.write();
    let Some(h) = hosted.get_mut(&l.repo) else {
        return;
    };
    l.published_term = l.raw.raft.term;
    let leading = l.raw.raft.state == StateRole::Leader;
    // A new leader checkpoints what earlier ones may not have.
    if leading && !h.leading {
        events.lock().changed.insert(l.repo.clone());
    }
    h.leading = leading;
    h.leader = l.raw.raft.leader_id;
    h.term = l.raw.raft.term;
}

/// Hand the membership changes among `committed` to raft, as the apply
/// thread will see them: the membership after each (by entry index). Pushes
/// `id` to `removed` if one removes this node.
fn apply_confs(
    l: &mut Log,
    node_id: u64,
    id: u64,
    committed: &[Entry],
    removed: &mut Vec<u64>,
) -> HashMap<u64, ConfState> {
    // Membership changes take effect in raft as they are handed
    // over (a refused one is refused on every node).
    let mut confs = HashMap::new();
    for (e, cc) in committed.iter().filter_map(|e| Some((e, conf_change(e)?))) {
        let Ok(cs) = l.raw.apply_conf_change(&cc) else {
            continue;
        };
        tracing::info!(repo = l.repo, voters = ?cs.voters, learners = ?cs.learners, "membership changed");
        // (Not just absent: a joining node replays changes from before it.)
        if cc
            .changes
            .iter()
            .any(|c| c.node_id == node_id && c.change_type() == ConfChangeType::RemoveNode)
        {
            removed.push(id);
        }
        l.raw.store().meta.write().conf = cs.clone();
        confs.insert(e.index, cs);
    }
    confs
}

/// A log known to be hosted.
#[expect(
    clippy::expect_used,
    reason = "invariant: the caller checked the log is hosted"
)]
fn hosted_log(logs: &HashMap<u64, Log>, id: u64) -> &Log {
    logs.get(&id).expect("hosted")
}

#[expect(
    clippy::expect_used,
    reason = "invariant: the caller checked the log is hosted"
)]
fn hosted_log_mut(logs: &mut HashMap<u64, Log>, id: u64) -> &mut Log {
    logs.get_mut(&id).expect("hosted")
}

/// Fail a waiting proposal or read barrier with `e`.
fn reject(kind: Wait, e: anyhow::Error) {
    match kind {
        Wait::Propose { resp, .. } => drop(resp.send(Err(e))),
        Wait::Read(resp) => drop(resp.send(Err(e))),
    }
}

/// Hand the apply thread a job (it only stops with the loop, or failing).
fn hand(applier: &std::sync::mpsc::Sender<Job>, job: Job) -> Result<()> {
    applier
        .send(job)
        .map_err(|e| anyhow::anyhow!("the apply thread stopped: {e}"))
}

/// Whether a log needs no membership change: every member a voter, and no
/// removed node in it.
fn settled(conf: &ConfState, members: &[u64], removed: &[u64]) -> bool {
    members.iter().all(|n| conf.voters.contains(n))
        && !conf
            .voters
            .iter()
            .chain(&conf.learners)
            .any(|n| removed.contains(n))
}

/// Add a log's snapshot to a write.
#[expect(
    clippy::expect_used,
    reason = "invariant: the snapshot key is not reserved"
)]
fn put_snapshot(batch: &mut LogBatch, id: u64, snap: &Snapshot) {
    batch
        .put_value::<Prost, _>(id, SNAPSHOT_KEY.to_vec(), snap)
        .expect("snapshot key is not reserved");
}

impl Loop {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Input>) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                input = rx.recv() => if !self.handle(input.unwrap_or(Input::Stop)) {
                    return;
                },
                _ = ticker.tick() => self.tick(),
            }
            // Batch everything else that is pending.
            if !self.drain(&mut rx) {
                return;
            }
            self.retry_waiting();
            if !self.settle(&mut rx).await {
                return;
            }
        }
    }

    /// Process rounds while logs have work (e.g. entries that just became
    /// durable are now committed), taking new inputs in between; false if
    /// the loop must stop.
    #[expect(
        clippy::panic,
        reason = "a node that cannot persist its state must stop: fail-stop is what Raft safety assumes"
    )]
    async fn settle(&mut self, rx: &mut mpsc::UnboundedReceiver<Input>) -> bool {
        for _ in 0..16 {
            if let Err(e) = self.process().await {
                // Durability failed: we must not continue.
                panic!("consensus engine: {e:#}");
            }
            if self.dirty.is_empty() {
                break;
            }
            if !self.drain(rx) {
                return false;
            }
            // Issue reads and proposals that arrived meanwhile, or they
            // would wait for the next tick.
            self.retry_waiting();
        }
        true
    }

    /// Handle the inputs pending; false once told to stop.
    fn drain(&mut self, rx: &mut mpsc::UnboundedReceiver<Input>) -> bool {
        for _ in 0..10_000 {
            let Ok(i) = rx.try_recv() else { break };
            if !self.handle(i) {
                return false;
            }
        }
        true
    }

    fn touch(&mut self, log: u64, real: bool) {
        self.dirty.insert(log);
        if real && let Some(l) = self.logs.get_mut(&log) {
            let leader = l.raw.raft.state == StateRole::Leader;
            l.active_until = Instant::now()
                + if leader {
                    LEADER_ACTIVE
                } else {
                    FOLLOWER_ACTIVE
                };
            self.ticking.insert(log);
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        let mut active = 0;
        let mut quiet = Vec::new();
        for &id in &self.ticking {
            match self.logs.get_mut(&id) {
                Some(l) if l.active_until > now => {
                    l.raw.tick();
                    self.dirty.insert(id);
                    active += 1;
                }
                l => quiet.push((id, l.map(|l| l.repo.clone()))),
            }
        }
        if !quiet.is_empty() {
            let mut events = self.events.lock();
            for (id, repo) in quiet {
                self.ticking.remove(&id);
                events.quiet.extend(repo);
            }
        }
        // Re-issue read index requests that got no answer: a leader drops
        // them (silently) while it hasn't committed in its term, and
        // forwarded ones can be lost with a leadership change.
        let stale: Vec<u64> = self
            .reads
            .iter()
            .filter(|(_, r)| now.duration_since(r.issued) > READ_RETRY)
            .map(|(r, _)| *r)
            .collect();
        for rid in stale {
            if let Some(r) = self.reads.remove(&rid) {
                self.waiting.push(Waiting {
                    log: r.log,
                    deadline: r.deadline,
                    kind: Wait::Read(r.resp),
                });
            }
        }
        // Expire proposals that never committed.
        let expired: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline < now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(p) = self.pending.remove(&id) {
                drop(p.resp.send(Err(anyhow::anyhow!("proposal timed out"))));
            }
        }
        self.sweep();
        // Leaders of active logs re-announce them now and then, so a peer
        // that missed the first announcement (down, or restarted without
        // its disk) joins.
        self.ticks += 1;
        if self.ticks.is_multiple_of(ANNOUNCE_TICKS) {
            self.reannounce();
        }
        self.stats.logs = self.logs.len();
        self.stats.active_logs = active;
        *self.shared_stats.write() = self.stats.clone();
    }

    /// Announce the active logs this node leads to their peers.
    fn reannounce(&self) {
        let Some(t) = &self.transport else { return };
        let leading = self
            .ticking
            .iter()
            .filter_map(|id| self.logs.get(id))
            .filter(|l| l.raw.raft.state == StateRole::Leader);
        for l in leading {
            for p in l.peers(self.node_id) {
                t.send(p, Frame::Host(l.repo.clone(), l.epoch, None));
            }
        }
    }

    /// Handle one input; false if it says to stop.
    fn handle(&mut self, input: Input) -> bool {
        self.rounds.inputs += 1;
        match input {
            Input::Propose(cmd, resp) => self.on_propose(&cmd, resp),
            Input::Follow(repo, tx, resp) => self.on_follow(&repo, tx, resp),
            Input::ReadIndex(log, resp) => self.on_read_index(log, resp),
            Input::Frames(frames) => {
                for f in frames {
                    self.on_frame(f);
                }
            }
            Input::Host {
                repo,
                epoch,
                meta,
                voters,
                resp,
            } => match self.host(repo, epoch, meta, voters) {
                Ok(()) => self.hosted_replies.push(resp),
                Err(e) => {
                    drop(resp.send(Err(e)));
                }
            },
            Input::Unhost(repo, epoch) => {
                self.unhost(log_id(&repo, epoch));
            }
            Input::Start(repo, epoch) => self.on_start(&repo, epoch),
            Input::Purge(ids) => self.on_purge(ids),
            Input::Applied(logs) => {
                let repos = logs.iter().filter_map(|id| self.logs.get(id));
                (self.events.lock().changed).extend(repos.map(|l| l.repo.clone()));
                self.answer_reads();
            }
            Input::Durable(durable) => self.on_durable(durable),
            Input::Members(members, removed) => {
                if (&members, &removed) != (&self.members, &self.removed) {
                    (self.members, self.removed) = (members, removed);
                    self.to_reconcile = self.logs.keys().copied().collect();
                }
            }
            Input::Unreachable(peer, logs) => self.on_unreachable(peer, logs),
            Input::Stop => return false,
        }
        true
    }

    fn on_propose(&mut self, cmd: &Command, resp: oneshot::Sender<Result<Outcome>>) {
        let Some(log) = log_of(&self.hosted, &cmd.repo) else {
            drop(resp.send(Err(NotHosted.into())));
            return;
        };
        let progress = *hosted_log(&self.logs, log).progress.lock();
        // Refused anyway once applied: keep it out of the log.
        if progress.deleted {
            drop(resp.send(Ok(state_machine::on_deleted(cmd, progress.version))));
            return;
        }
        let data = cmd.encode_to_vec();
        self.next_id += 1;
        let pid = self.next_id;
        self.waiting.push(Waiting {
            log,
            deadline: Instant::now() + PROPOSAL_TIMEOUT,
            kind: Wait::Propose { pid, data, resp },
        });
        self.touch(log, true);
    }

    #[expect(
        clippy::expect_used,
        reason = "a node that cannot persist its state must stop: fail-stop is what Raft safety assumes"
    )]
    fn on_follow(
        &mut self,
        repo: &str,
        tx: mpsc::Sender<Update>,
        resp: oneshot::Sender<Result<()>>,
    ) {
        let Some(log) = log_of(&self.hosted, repo) else {
            drop(resp.send(Err(NotHosted.into())));
            return;
        };
        hand(&self.applier, Job::Follow { log, tx, resp }).expect("apply thread");
    }

    fn on_read_index(&mut self, log: u64, resp: oneshot::Sender<Result<u64>>) {
        if !self.logs.contains_key(&log) {
            drop(resp.send(Err(NotHosted.into())));
            return;
        }
        self.waiting.push(Waiting {
            log,
            deadline: Instant::now() + PROPOSAL_TIMEOUT,
            kind: Wait::Read(resp),
        });
        self.touch(log, true);
    }

    fn on_frame(&mut self, f: Frame) {
        match f {
            // For a node that had this address before.
            Frame::Raft(_, m) if m.to != self.node_id => {}
            Frame::Raft(log, m) => {
                let real = is_real_traffic(&m);
                if let Some(l) = self.logs.get_mut(&log) {
                    drop(l.raw.step(*m));
                    self.touch(log, real);
                }
                // Messages for logs not hosted here (yet) are
                // dropped; raft retransmits.
            }
            Frame::Wake(log) => self.touch(log, true),
            Frame::Host(repo, epoch, state) => self.on_announced(repo, epoch, state),
        }
    }

    /// A peer announced incarnation `epoch` of `repo`.
    fn on_announced(&mut self, repo: String, epoch: u64, state: Option<Box<Record>>) {
        if self.logs.contains_key(&log_id(&repo, epoch)) {
            return;
        }
        // A new incarnation, with its initial state: host it
        // now (its leader's vote requests follow); else load
        // that state from the object store.
        match state {
            Some(s) if s.repo == repo && s.epoch == epoch => {
                if let Err(e) = self.host(repo, epoch, s.meta.unwrap_or_default(), s.voters) {
                    tracing::debug!("hosting announced log: {e:#}");
                }
            }
            _ => drop(self.announced.send((repo, epoch))),
        }
    }

    fn on_start(&mut self, repo: &str, epoch: u64) {
        let id = log_id(repo, epoch);
        if !self.logs.contains_key(&id) {
            return;
        }
        self.announce(repo, epoch, id);
        drop(hosted_log_mut(&mut self.logs, id).raw.campaign());
        self.touch(id, true);
    }

    /// Send log `id`'s state to its peers, so they host it.
    fn announce(&self, repo: &str, epoch: u64, id: u64) {
        let Some(t) = &self.transport else { return };
        if let Ok(Some((root, meta))) = load(&self.state, &self.db, id) {
            let state = root.record(meta);
            for p in hosted_log(&self.logs, id).peers(self.node_id) {
                t.send(
                    p,
                    Frame::Host(repo.to_string(), epoch, Some(Box::new(state.clone()))),
                );
            }
        } else {
            // Peers load it from the object store instead.
            tracing::warn!(repo, epoch, "no state to announce");
        }
    }

    /// Compact the logs `ids` hold back, or drop them if not hosted.
    #[expect(
        clippy::expect_used,
        reason = "a node that cannot persist its state must stop: fail-stop is what Raft safety assumes"
    )]
    fn on_purge(&mut self, ids: Vec<u64>) {
        for id in ids {
            if self.logs.contains_key(&id) {
                self.compact(id);
                continue;
            }
            if !is_log(id) {
                continue;
            }
            hand(&self.applier, Job::Drop(id)).expect("apply thread");
            self.batch.add_command(id, raft_engine::Command::Clean);
        }
    }

    fn on_unreachable(&mut self, peer: u64, logs: Vec<u64>) {
        for log in logs {
            if let Some(l) = self.logs.get_mut(&log) {
                l.raw.report_unreachable(peer);
            }
        }
    }

    fn on_durable(&mut self, durable: Vec<(u64, u64, Option<u64>)>) {
        for (id, index, pin) in durable {
            if let Some(l) = self.logs.get_mut(&id) {
                l.durable = (index, pin);
                self.maybe_snapshot(id);
            }
        }
    }

    /// Propose waiting commands and issue waiting read barriers whose log
    /// knows a leader; wake the others.
    fn retry_waiting(&mut self) {
        let now = Instant::now();
        for w in std::mem::take(&mut self.waiting) {
            self.retry_one(w, now);
        }
    }

    fn retry_one(&mut self, w: Waiting, now: Instant) {
        let Some(l) = self.logs.get_mut(&w.log) else {
            reject(w.kind, NotHosted.into());
            return;
        };
        let no_leader = l.raw.raft.leader_id == 0;
        if w.deadline < now {
            reject(w.kind, anyhow::anyhow!("no leader for the log (timed out)"));
            return;
        }
        if no_leader {
            if l.solo() {
                drop(l.raw.campaign());
            } else {
                self.wake(w.log);
            }
            self.dirty.insert(w.log);
            self.waiting.push(w);
            return;
        }
        match w.kind {
            Wait::Read(resp) => {
                // raft-rs silently drops read index requests on a leader
                // that has not committed an entry in its current term yet.
                if l.raw.raft.state == StateRole::Leader && !l.raw.raft.commit_to_current_term() {
                    self.dirty.insert(w.log);
                    self.waiting.push(Waiting {
                        kind: Wait::Read(resp),
                        ..w
                    });
                    return;
                }
                self.next_id += 1;
                let rid = self.next_id;
                l.raw.read_index(rid.to_le_bytes().to_vec());
                self.reads.insert(
                    rid,
                    Read {
                        log: w.log,
                        resp,
                        issued: now,
                        deadline: w.deadline,
                    },
                );
            }
            Wait::Propose { pid, data, resp } => {
                if let Err(e) = l.raw.propose(pid.to_le_bytes().to_vec(), data) {
                    drop(resp.send(Err(anyhow::anyhow!("propose failed: {e}"))));
                    return;
                }
                self.pending.insert(
                    pid,
                    Pending {
                        resp,
                        deadline: w.deadline,
                    },
                );
            }
        }
        self.touch(w.log, true);
    }

    fn wake(&mut self, log: u64) {
        let Some(l) = self.logs.get_mut(&log) else {
            return;
        };
        if let Some(t) = &self.transport {
            for p in l.peers(self.node_id) {
                t.send(p, Frame::Wake(log));
            }
        }
        // Tick ourselves too, so an election happens if the leader is gone.
        l.active_until = Instant::now() + FOLLOWER_ACTIVE;
        self.ticking.insert(log);
    }

    fn send(&mut self, log: u64, msgs: Vec<Message>) {
        let Some(t) = &self.transport else { return };
        self.stats.messages_sent += count(msgs.len());
        for m in msgs {
            t.send(m.to, Frame::Raft(log, Box::new(m)));
        }
    }

    /// One round: collect every touched log's `Ready`, send the messages
    /// that don't need durability, apply committed entries, then make the
    /// round's state durable with one log store write (group commit across
    /// all logs). Only then is the logs' storage view updated, the rounds
    /// reported persisted to raft (`on_persist_ready`) and the messages that
    /// require durability (append acks, votes) sent. Committed entries are
    /// only ever up to the persisted index (raft-rs's default
    /// `max_apply_unpersisted_log_limit`).
    ///
    /// Synchronous: overlapping the fsync with the next round gains nothing
    /// once the loop is not CPU-bound.
    async fn process(&mut self) -> Result<()> {
        let started = Instant::now();
        let dirty: Vec<u64> = std::mem::take(&mut self.dirty).into_iter().collect();
        self.rounds.logs += count(dirty.len());
        let mut batch = std::mem::take(&mut self.batch);
        let hosted_replies = std::mem::take(&mut self.hosted_replies);
        for id in std::mem::take(&mut self.fresh) {
            if let Some(l) = self.logs.get(&id) {
                put_snapshot(&mut batch, id, &l.raw.store().meta.read().snapshot);
            }
        }
        let mut work = RoundWork::default();
        for id in dirty {
            self.reconcile(id);
            self.collect(id, &mut batch, &mut work)?;
        }
        // Read barriers whose index is applied already (the others are
        // answered as the apply thread reports).
        self.answer_reads();
        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        for resp in hosted_replies {
            drop(resp.send(Ok(())));
        }
        self.release(work);
        *self.shared_stats.write() = self.stats.clone();
        self.end_round(started);
        Ok(())
    }

    /// Collect log `id`'s `Ready` into the round: its writes into `batch`,
    /// what follows once they are durable into `work`.
    fn collect(&mut self, id: u64, batch: &mut LogBatch, work: &mut RoundWork) -> Result<()> {
        let Some(l) = self.logs.get_mut(&id) else {
            return Ok(());
        };
        if !l.raw.has_ready() {
            return Ok(());
        }
        let mut rd = l.raw.ready();
        let msgs = rd.take_messages();
        let mut snapshot = (!rd.snapshot().is_empty()).then(|| rd.snapshot().clone());
        // Installed from the leader: the state store takes its state
        // (durable before the log is reset to it: see the module doc),
        // and the log keeps where it starts.
        if let Some(snap) = snapshot.as_mut() {
            hand_snapshot(&self.applier, l, id, snap)?;
            self.barrier = true;
            self.stats.snapshots_installed += 1;
            reset_to_snapshot(batch, l, id, snap);
        }
        batch.add_entries_with::<Entries, Prost>(id, rd.entries())?;
        let terms = rd.entries().iter().map(|e| (e.index, e.term)).collect();
        let hs = rd.hs().cloned();
        if let Some(hs) = &hs {
            batch.put_value::<Prost, _>(id, HARD_STATE_KEY.to_vec(), hs)?;
        }
        work.persisted_state.push((id, snapshot.clone(), terms, hs));
        // Read indexes don't depend on local persistence.
        for rs in rd.take_read_states() {
            let rid = u64::from_le_bytes(rs.request_ctx.as_slice().try_into().unwrap_or([0; 8]));
            if let Some(Read { log, resp, .. }) = self.reads.remove(&rid) {
                self.read_applied.push((log, rs.index, resp));
            }
        }
        publish_role(&self.hosted, &self.events, l, rd.ss().is_some());
        let committed = rd.take_committed_entries();
        let persisted = rd.take_persisted_messages();
        let number = rd.number();
        l.raw.advance_append_async(rd);
        if let Some(last) = committed.last().map(|e| e.index) {
            let confs = apply_confs(l, self.node_id, id, &committed, &mut work.removed);
            // Raft counts them applied once handed to the apply thread.
            l.raw.advance_apply_to(last);
            self.apply(id, committed, confs)?;
        }
        self.send(id, msgs);
        work.rounds.push((id, number));
        if !persisted.is_empty() {
            work.held.push((id, persisted));
        }
        if hosted_log(&self.logs, id).raw.has_ready() {
            self.dirty.insert(id);
        }
        Ok(())
    }

    /// Write the round's `batch` to the log store (group commit).
    async fn write_batch(&mut self, mut batch: LogBatch) -> Result<()> {
        let db = self.db.clone();
        let w = Instant::now();
        if std::mem::take(&mut self.barrier) {
            // States this write starts logs from are durable first.
            let (done, synced) = oneshot::channel();
            hand(&self.applier, Job::Sync(done))?;
            synced
                .await
                .map_err(|e| anyhow::anyhow!("the apply thread stopped: {e}"))?;
        }
        let n = tokio::task::spawn_blocking(move || db.write(&mut batch, true)).await??;
        self.rounds.write += w.elapsed();
        self.stats.log_writes += 1;
        self.stats.log_bytes += count(n);
        Ok(())
    }

    /// Everything from this round is durable: update the logs' storage
    /// view, tell raft, and release the messages that required it
    /// (append acks, votes).
    fn release(&mut self, work: RoundWork) {
        for (id, snap, terms, hs) in work.persisted_state {
            let Some(l) = self.logs.get(&id) else {
                continue;
            };
            let mut m = l.raw.store().meta.write();
            if let Some(snap) = snap {
                m.apply_snapshot(snap);
            }
            m.append(terms);
            if let Some(hs) = hs {
                m.hard_state = hs;
            }
        }
        for (id, number) in work.rounds {
            if let Some(l) = self.logs.get_mut(&id) {
                l.raw.on_persist_ready(number);
                self.dirty.insert(id);
            }
        }
        for (id, msgs) in work.held {
            self.send(id, msgs);
        }
        for id in work.removed {
            self.unhost(id);
        }
    }

    /// Count a round, and log the statistics once a window is due.
    fn end_round(&mut self, started: Instant) {
        self.rounds.count += 1;
        self.rounds.busy += started.elapsed();
        let Some((span, r)) = self.rounds.due() else {
            return;
        };
        tracing::debug!(
            target: "enroute::ledger_timing",
            rounds_per_s = rate(r.count, span),
            busy_pct = pct(r.busy, span),
            round_us = per(r.busy.as_micros(), r.count),
            write_us = per(r.write.as_micros(), r.count),
            inputs = per(u128::from(r.inputs), r.count),
            logs = per(u128::from(r.logs), r.count),
            "engine rounds"
        );
    }

    /// Answer read barriers whose read index this node has applied.
    fn answer_reads(&mut self) {
        let mut still = Vec::new();
        for (log, index, resp) in std::mem::take(&mut self.read_applied) {
            if self
                .logs
                .get(&log)
                .is_some_and(|l| l.progress.lock().applied >= index)
            {
                drop(resp.send(Ok(index)));
            } else {
                still.push((log, index, resp));
            }
        }
        self.read_applied = still;
    }

    /// Hand committed entries of one log to the apply thread, with the
    /// proposers it answers and the membership after each change.
    fn apply(
        &mut self,
        id: u64,
        entries: Vec<Entry>,
        confs: HashMap<u64, ConfState>,
    ) -> Result<()> {
        let mut replies = HashMap::new();
        for e in &entries {
            let pid = u64::from_le_bytes(e.context.as_slice().try_into().unwrap_or([0; 8]));
            if let Some(p) = self.pending.remove(&pid) {
                replies.insert(e.index, p.resp);
            }
        }
        hand(
            &self.applier,
            Job::Apply {
                log: id,
                entries,
                replies,
                confs,
            },
        )
    }

    /// Host a log (see [`Engine::host`]).
    fn host(&mut self, repo: String, epoch: u64, meta: RepoMeta, voters: Vec<u64>) -> Result<()> {
        let id = log_id(&repo, epoch);
        if let Some(cur) = log_of(&self.hosted, &repo) {
            if cur == id {
                return Ok(());
            }
            anyhow::ensure!(
                hosted_log(&self.logs, cur).epoch < epoch,
                "a newer incarnation of {repo} is hosted"
            );
            // Superseded (it ended elsewhere while this node was away).
            self.unhost(cur);
        }
        // (Maybe not as a voter yet: its leader adds members.)
        let conf = ConfState {
            voters,
            ..Default::default()
        };
        // Its initial state, durable before (with the round's write) its log.
        let root = stored::root(&repo, epoch, &conf, 0, 0);
        let progress = Arc::new(Mutex::new(Progress {
            version: meta.version,
            applied: 0,
            deleted: meta.deleted,
        }));
        hand(
            &self.applier,
            Job::Host {
                log: id,
                root,
                meta,
                progress: progress.clone(),
            },
        )?;
        self.barrier = true;
        let mut mem = LogMeta::default();
        mem.snapshot.mut_metadata().set_conf_state(conf.clone());
        mem.conf = conf;
        let raw = RawNode::new(
            &raft_config(self.node_id, 0),
            LogStore::new(id, self.db.clone(), self.state.clone(), mem),
            &self.logger,
        )?;
        self.logs.insert(
            id,
            Log {
                raw,
                repo: repo.clone(),
                epoch,
                progress: progress.clone(),
                durable: (0, None),
                active_until: Instant::now() + FOLLOWER_ACTIVE,
                published_term: 0,
            },
        );
        self.ticking.insert(id);
        self.hosted.write().insert(
            repo,
            Published {
                log: id,
                epoch,
                leading: false,
                leader: 0,
                term: 0,
                progress,
            },
        );
        self.fresh.push(id);
        self.dirty.insert(id);
        Ok(())
    }

    /// Stop hosting a log (its incarnation ended, and its subscriptions with
    /// it), and drop it from the log store.
    #[expect(
        clippy::expect_used,
        reason = "a node that cannot persist its state must stop: fail-stop is what Raft safety assumes"
    )]
    fn unhost(&mut self, id: u64) {
        let Some(l) = self.logs.remove(&id) else {
            return;
        };
        self.ticking.remove(&id);
        // A state without a log is dropped at restart, as is a log without
        // a state: the two deletions need no order.
        hand(&self.applier, Job::Drop(id)).expect("apply thread");
        self.batch.add_command(id, raft_engine::Command::Clean);
        // A hosted log is always its repository's entry.
        self.hosted.write().remove(&l.repo);
    }

    /// Visit the next logs of the sweep, waking those not settled.
    fn sweep(&mut self) {
        for _ in 0..RECONCILE_PER_TICK {
            let Some(id) = self.to_reconcile.pop_front() else {
                return;
            };
            let Some(l) = self.logs.get(&id) else {
                continue;
            };
            if settled(
                &l.raw.store().meta.read().conf,
                &self.members,
                &self.removed,
            ) {
                continue;
            }
            match l.raw.raft.leader_id {
                // (A removed node drops its logs: it leads none.)
                leader if leader == 0 || self.removed.contains(&leader) => self.wake(id),
                leader if leader == self.node_id => {}
                _ => continue, // its leader's own sweep visits it
            }
            self.touch(id, true);
        }
    }

    /// If this node leads log `id` and its membership is not settled, take
    /// the next step: add a member as a learner (the leader's announcements
    /// tell it to host the log), make a caught-up learner a voter, or remove
    /// a removed node (the leader too: it then drops the log, and the others
    /// elect a new leader).
    fn reconcile(&mut self, id: u64) {
        let Some(l) = self.logs.get_mut(&id) else {
            return;
        };
        let raft = &l.raw.raft;
        // One change at a time, after an entry in this term (the fix to
        // single-server changes).
        if raft.state != StateRole::Leader
            || raft.has_pending_conf()
            || !raft.commit_to_current_term()
        {
            return;
        }
        if settled(
            &l.raw.store().meta.read().conf,
            &self.members,
            &self.removed,
        ) {
            return;
        }
        let conf = l.raw.store().meta.read().conf.clone();
        let (want, removed) = (&self.members, &self.removed);
        let floor = raft
            .raft_log
            .committed
            .saturating_sub(CATCH_UP)
            .max(raft.pending_conf_index);
        let caught_up = |n: u64| raft.prs().get(n).is_some_and(|p| p.matched >= floor);
        let unknown = |n: &&u64| !conf.voters.contains(n) && !conf.learners.contains(n);
        let change = if let Some(&n) = want.iter().find(unknown) {
            Some((ConfChangeType::AddLearnerNode, n))
        } else if let Some(&n) = conf
            .learners
            .iter()
            .find(|&&n| want.contains(&n) && caught_up(n))
        {
            Some((ConfChangeType::AddNode, n))
        } else {
            conf.learners
                .iter()
                .chain(&conf.voters)
                .find(|&&n| removed.contains(&n))
                .map(|&n| (ConfChangeType::RemoveNode, n))
        };
        let Some((ty, node)) = change else { return };
        let single = ConfChangeSingle {
            node_id: node,
            change_type: ty.into(),
        };
        let cc = ConfChangeV2 {
            changes: vec![single],
            ..Default::default()
        };
        tracing::info!(repo = l.repo, node, change = ?ty, "changing membership");
        if let Err(e) = l.raw.propose_conf_change(Vec::new(), cc) {
            tracing::warn!(repo = l.repo, "proposing a membership change: {e:#}");
            return;
        }
        // A new learner hosts the log only from the leader's announcements,
        // which stop once the log goes quiet.
        self.touch(id, true);
    }

    /// Compact a log every `snapshot_every` durably applied entries.
    fn maybe_snapshot(&mut self, id: u64) {
        let Some(l) = self.logs.get(&id) else { return };
        let snap_index = l.raw.store().meta.read().snapshot.get_metadata().index;
        if l.durable.0 >= snap_index + self.snapshot_every {
            self.compact(id);
        }
    }

    /// Log compaction: drop the entries up to the index the state store has
    /// durably applied (in the round's batch; and at once in memory: those entries
    /// are applied, so nothing reads them), but not the entry of the oldest
    /// inline layer not flushed then, nor any after it.
    fn compact(&mut self, id: u64) {
        let Some(l) = self.logs.get(&id) else { return };
        let (durable, pin) = l.durable;
        let index = pin.map_or(durable, |p| durable.min(p - 1));
        let store = l.raw.store();
        let snap = {
            let m = store.meta.read();
            if index <= m.snapshot.get_metadata().index {
                return;
            }
            let Some(term) = m.term(index) else { return };
            m.snapshot_at(index, term)
        };
        put_snapshot(&mut self.batch, id, &snap);
        self.batch
            .add_command(id, raft_engine::Command::Compact { index: index + 1 });
        store.meta.write().compact(snap);
        self.stats.compactions += 1;
    }
}

#[cfg(test)]
mod tests;
