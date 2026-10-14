//! The apply thread: every log's committed entries are applied to the
//! state store here, off the event loop, so a round's consensus work and
//! its fsync never wait for the state machine (as `TiKV` splits its raft and
//! apply pools).
//!
//! The loop hands over jobs in order through one queue, so a log's entries,
//! a snapshot that replaces its state, and its deletion apply in log order.
//! Everything that depends on the applied state happens here: answering
//! proposers, notifying subscribers (a new one is registered here, between
//! two jobs, so it sees the state and then every change after it), and
//! publishing each log's [`Progress`] to readers. The loop hears back which
//! logs applied entries ([`Input::Applied`], for read barriers and the
//! ledger's events) and what is durable ([`Input::Durable`], for log
//! truncation).
//!
//! **Durability.** Writes are not durable as they are applied: the store
//! has no write-ahead log, and a crash loses what it did not flush (the
//! Raft log replays it). Every [`SYNC_EVERY`] the thread lets the store
//! flush if it holds enough, and on a [`Job::Sync`] (the loop asks before a
//! round's log write that relies on a state just written: a new log's, or
//! one installed from a snapshot) it flushes now. The store writes a flush
//! on a thread of its own, so this thread waits for one only on a
//! [`Job::Sync`]. Once a flush is durable, what it covers is reported: for
//! every log written since the previous flush, the index the store has
//! durably applied and its oldest pinned inline layer *as of the flush*: a
//! log may be truncated up to the index, but not past that pin. (Pins as of
//! the store's current state would not do: a flush applied but not yet
//! durable has dropped a pin that a crash would bring back.)
//!
//! **Read pipelining.** Before applying a batch of jobs, the thread reads
//! every ref and counter its commands will read in one go, from several
//! threads ([`crate::state_store::Round::prefetch`]), so the entries then
//! apply against the block cache instead of waiting on the disk one read at
//! a time.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use raft::prelude::{ConfState, Entry};
use tokio::sync::{mpsc, oneshot};

use enroute_shared::proto::{Update, update};
use enroute_shared::roles::NotHosted;
use enroute_shared::state::{Command, Outcome, RepoMeta, command};

use crate::logdb::LogDb;
use crate::state_machine::count;
use crate::state_store::{Root, Round, StateStore};
use crate::stored;

use super::engine::Input;
use super::logstore::{decode, load};
use super::stats::{Window, pct, per, rate};

/// How often the store is synced (when it was written), and what is
/// durable reported: how far behind a log's truncation may lag.
const SYNC_EVERY: Duration = Duration::from_millis(100);

/// Jobs taken in one batch, at most.
const BATCH: usize = 4096;

/// How far a log's state is applied. The apply thread publishes it before
/// it answers proposers, so a proposer sees its own change.
#[derive(Clone, Copy, Default)]
pub(super) struct Progress {
    pub(super) version: u64,
    pub(super) applied: u64,
    pub(super) deleted: bool,
}

/// What the loop asks of the apply thread, in log order.
pub(super) enum Job {
    /// Apply committed entries; answer the proposers of those whose index
    /// is in `replies`; record the membership after each change in `confs`.
    Apply {
        log: u64,
        entries: Vec<Entry>,
        replies: HashMap<u64, oneshot::Sender<Result<Outcome>>>,
        confs: HashMap<u64, ConfState>,
    },
    /// A new log, from its initial state; its progress is published in
    /// `progress`.
    Host {
        log: u64,
        root: Root,
        meta: RepoMeta,
        progress: Arc<Mutex<Progress>>,
    },
    /// Replace a hosted log's state with one installed from the leader,
    /// which subscribers then start from.
    Install {
        log: u64,
        root: Root,
        meta: RepoMeta,
    },
    /// The log is no longer hosted: delete its state, end its subscriptions.
    Drop(u64),
    /// Subscribe to a log: its state, then every change.
    Follow {
        log: u64,
        tx: mpsc::Sender<Update>,
        resp: oneshot::Sender<Result<()>>,
    },
    /// Sync the store (everything queued before), then answer.
    Sync(oneshot::Sender<()>),
}

/// What a batch's applied entries leave to publish.
#[derive(Default)]
struct Done {
    deleted: HashSet<u64>,
    replies: Vec<(oneshot::Sender<Result<Outcome>>, Outcome)>,
}

/// What the thread knows of a hosted log.
struct Log {
    /// Only this thread writes it.
    progress: Arc<Mutex<Progress>>,
    subscribers: Vec<mpsc::Sender<Update>>,
}

impl Log {
    /// Send an update to the subscribers. One that cannot keep up (its
    /// queue is full) is dropped: its stream ends, and it follows again
    /// from a new state.
    fn notify(&mut self, update: impl Fn() -> Update) {
        self.subscribers.retain(|tx| tx.try_send(update()).is_ok());
    }
}

pub(super) struct Applier {
    state: Arc<StateStore>,
    db: LogDb,
    logs: HashMap<u64, Log>,
    /// The loop's input, for reports.
    report: mpsc::UnboundedSender<Input>,
    /// Logs written since the last sync.
    unsynced: HashSet<u64>,
    last_sync: Instant,
    /// Batches since they were last logged (`enroute::ledger_timing`).
    stats: Window<Batches>,
}

/// What the apply thread did over a window.
#[derive(Default)]
struct Batches {
    count: u64,
    jobs: u64,
    entries: u64,
    busy: Duration,
    prefetch: Duration,
    sync: Duration,
    syncs: u64,
}

/// Log the apply thread's statistics, as the loop does its rounds'.
fn log_batches(stats: &mut Window<Batches>) {
    let Some((span, b)) = stats.due() else { return };
    tracing::debug!(
        target: "enroute::ledger_timing",
        batches_per_s = rate(b.count, span),
        busy_pct = pct(b.busy, span),
        batch_us = per(b.busy.as_micros(), b.count),
        prefetch_us = per(b.prefetch.as_micros(), b.count),
        jobs = per(u128::from(b.jobs), b.count),
        entries = per(u128::from(b.entries), b.count),
        syncs = b.syncs,
        sync_us = per(b.sync.as_micros(), b.syncs),
        "apply batches"
    );
}

impl Applier {
    /// The thread for `logs` (log id → its published progress) as
    /// recovered.
    #[expect(
        clippy::unwrap_used,
        reason = "invariant: the monotonic clock is older than `SYNC_EVERY`"
    )]
    pub(super) fn new(
        state: Arc<StateStore>,
        db: LogDb,
        logs: impl IntoIterator<Item = (u64, Arc<Mutex<Progress>>)>,
        report: mpsc::UnboundedSender<Input>,
    ) -> Applier {
        let logs: HashMap<u64, Log> = logs
            .into_iter()
            .map(|(id, progress)| {
                (
                    id,
                    Log {
                        progress,
                        subscribers: Vec::new(),
                    },
                )
            })
            .collect();
        // Recovery replayed entries without syncing: the first sync reports
        // every log.
        let unsynced = logs.keys().copied().collect();
        Applier {
            state,
            db,
            logs,
            report,
            unsynced,
            last_sync: Instant::now().checked_sub(SYNC_EVERY).unwrap(),
            stats: Window::new(),
        }
    }

    /// Run until the loop drops its end of the queue. A failure to write the
    /// store stops the node, as a failed log write does.
    ///
    /// # Panics
    ///
    /// Panics if the store cannot be written or synced.
    #[expect(
        clippy::panic,
        reason = "a node that cannot persist its state must stop: fail-stop is what Raft safety assumes"
    )]
    pub(super) fn run(mut self, jobs: &Receiver<Job>) {
        if let Err(e) = self.serve(jobs) {
            panic!("apply thread: {e:#}");
        }
    }

    fn serve(&mut self, jobs: &Receiver<Job>) -> Result<()> {
        self.sync(true).context("syncing the state store")?;
        loop {
            let wait = SYNC_EVERY.saturating_sub(self.last_sync.elapsed());
            let mut batch = Vec::new();
            match jobs.recv_timeout(if self.unsynced.is_empty() {
                Duration::from_secs(3600)
            } else {
                wait
            }) {
                Ok(j) => batch.push(j),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            while batch.len() < BATCH
                && let Ok(j) = jobs.try_recv()
            {
                batch.push(j);
            }
            let t = Instant::now();
            if !batch.is_empty() {
                self.stats.count += 1;
                self.stats.jobs += count(batch.len());
            }
            self.batch(batch)?;
            self.stats.busy += t.elapsed();
            if !self.unsynced.is_empty() && self.last_sync.elapsed() >= SYNC_EVERY {
                self.sync(false).context("syncing the state store")?;
            }
            log_batches(&mut self.stats);
        }
        // The store flushes its memtables as it closes (no WAL to replay
        // at the next open).
        Ok(())
    }

    /// Run a batch of jobs: apply, publish, answer, report. Each run of
    /// consecutive `Apply` jobs is one round of the store (one write), read
    /// ahead first; any other job is handled between rounds.
    fn batch(&mut self, batch: Vec<Job>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        // Decoded once, for the read-ahead and to apply.
        let decoded: Vec<Decoded> = batch
            .iter()
            .map(|j| match j {
                Job::Apply { log, entries, .. } => {
                    Some((*log, entries.iter().map(|e| decode(*log, e)).collect()))
                }
                _ => None,
            })
            .collect();
        self.stats.entries += count(decoded.iter().flatten().map(|(_, c)| c.len()).sum());
        let state = self.state.clone();
        let mut round: Option<Round<'_>> = None;
        let mut touched = HashSet::new();
        let mut done = Done::default();
        for (i, (job, cmds)) in batch.into_iter().zip(&decoded).enumerate() {
            let Job::Apply {
                log,
                entries,
                replies: mut waiting,
                confs,
            } = job
            else {
                round.take().map(Round::commit).transpose()?;
                self.other(job, &mut touched)?;
                continue;
            };
            let r = if let Some(r) = &mut round {
                r
            } else {
                // A new run: read ahead what all of it will read.
                let run = decoded.get(i..).unwrap_or_default();
                let end = run.iter().position(Option::is_none).unwrap_or(run.len());
                let t = Instant::now();
                let mut r = state.round();
                prefetch(&state, &mut r, run.get(..end).unwrap_or_default())?;
                self.stats.prefetch += t.elapsed();
                round.insert(r)
            };
            if !state.contains(log) {
                continue; // dropped meanwhile
            }
            self.apply_entries(
                r,
                log,
                entries.iter().zip(cmds.iter().flat_map(|(_, c)| c)),
                &confs,
                &mut waiting,
                &mut done,
            )?;
            touched.insert(log);
            self.unsynced.insert(log);
        }
        if let Some(r) = round.take() {
            r.commit()?;
        }
        // Publish, then answer proposers, then tell the loop.
        let mut applied = Vec::with_capacity(touched.len());
        for log in touched {
            let (Some(root), Some(l)) = (state.root(log), self.logs.get(&log)) else {
                continue;
            };
            let mut p = l.progress.lock();
            (p.version, p.applied) = (root.version, root.applied_index);
            p.deleted |= done.deleted.contains(&log);
            applied.push(log);
        }
        for (resp, out) in done.replies {
            // A proposer that gave up needs no answer.
            drop(resp.send(Ok(out)));
        }
        if !applied.is_empty() {
            drop(self.report.send(Input::Applied(applied)));
        }
        Ok(())
    }

    /// Apply one log's entries in `r`, collecting the replies and deletions.
    fn apply_entries<'a>(
        &mut self,
        r: &mut Round<'_>,
        log: u64,
        entries: impl Iterator<Item = (&'a Entry, &'a Option<Command>)>,
        confs: &HashMap<u64, ConfState>,
        waiting: &mut HashMap<u64, oneshot::Sender<Result<Outcome>>>,
        done: &mut Done,
    ) -> Result<()> {
        for (e, cmd) in entries {
            if let Some(conf) = confs.get(&e.index) {
                stored::set_conf(r, log, conf)?;
            }
            let Some((out, change)) = stored::apply(r, log, e.index, e.term, cmd.as_ref())? else {
                continue;
            };
            if change.as_ref().is_some_and(|c| c.deleted) {
                done.deleted.insert(log);
            }
            if let Some(c) = change
                && let Some(l) = self.logs.get_mut(&log)
            {
                l.notify(|| Update {
                    kind: Some(update::Kind::Change(c.clone())),
                });
            }
            if let Some(resp) = waiting.remove(&e.index) {
                done.replies.push((resp, out));
            }
        }
        Ok(())
    }

    /// A job other than `Apply` (between rounds).
    fn other(&mut self, job: Job, touched: &mut HashSet<u64>) -> Result<()> {
        match job {
            Job::Apply { .. } => bail!("an Apply job reached the other-job path"),
            Job::Host {
                log,
                root,
                meta,
                progress,
            } => {
                self.state.install(log, root, stored::entries(&meta)?)?;
                let subscribers = Vec::new();
                self.logs.insert(
                    log,
                    Log {
                        progress,
                        subscribers,
                    },
                );
                touched.insert(log);
                self.unsynced.insert(log);
            }
            Job::Install { log, root, meta } => {
                self.state.install(log, root, stored::entries(&meta)?)?;
                // The loop hands a snapshot only for a log it hosts.
                let l = self
                    .logs
                    .get_mut(&log)
                    .context("a snapshot for a log that is not hosted")?;
                l.progress.lock().deleted = meta.deleted;
                l.notify(|| Update {
                    kind: Some(update::Kind::State(meta.clone())),
                });
                touched.insert(log);
                self.unsynced.insert(log);
            }
            Job::Drop(log) => {
                self.state.delete(log)?;
                self.logs.remove(&log);
                touched.remove(&log);
                self.unsynced.remove(&log);
            }
            Job::Follow { log, tx, resp } => self.follow(log, tx, resp),
            Job::Sync(done) => {
                self.sync(true)?;
                if done.send(()).is_err() {
                    tracing::debug!("nobody waits for the sync");
                }
            }
        }
        Ok(())
    }

    /// Subscribe `tx` to `log`, and answer `resp`.
    fn follow(&mut self, log: u64, tx: mpsc::Sender<Update>, resp: oneshot::Sender<Result<()>>) {
        // A requester that gave up needs no answer (so each `send` is dropped).
        let Some(l) = self.logs.get_mut(&log) else {
            drop(resp.send(Err(NotHosted.into())));
            return;
        };
        // The state first, then (registered in the same step, so nothing is
        // missed) every change after it.
        match load(&self.state, &self.db, log) {
            Ok(Some((_, meta))) => {
                if tx
                    .try_send(Update {
                        kind: Some(update::Kind::State(meta)),
                    })
                    .is_ok()
                {
                    l.subscribers.push(tx);
                }
                drop(resp.send(Ok(())));
            }
            Ok(None) => drop(resp.send(Err(NotHosted.into()))),
            Err(e) => drop(resp.send(Err(e.context("reading the state")))),
        }
    }

    /// Flush the store (if it holds enough, unless `now`), and once the
    /// flush is durable report, for every log written since the last one,
    /// its applied index and oldest pin as of the flush (the only writer is
    /// this thread). Unless `now`, the flush runs without this thread
    /// waiting for it.
    fn sync(&mut self, now: bool) -> Result<()> {
        let t = Instant::now();
        self.last_sync = t;
        if !now && !self.state.due() {
            return Ok(());
        }
        let mut durable = Vec::with_capacity(self.unsynced.len());
        for log in self.unsynced.drain() {
            let Some(root) = self.state.root(log) else {
                continue;
            };
            durable.push((log, root.applied_index, self.state.oldest_pin(log)?));
        }
        let report = self.report.clone();
        let flushed = self.state.flush(move || {
            if !durable.is_empty() {
                drop(report.send(Input::Durable(durable)));
            }
        })?;
        if now {
            flushed.wait()?;
        }
        (self.stats.sync, self.stats.syncs) = (self.stats.sync + t.elapsed(), self.stats.syncs + 1);
        Ok(())
    }
}

/// An `Apply` job's log and decoded commands, or `None` for another job.
type Decoded = Option<(u64, Vec<Option<Command>>)>;

/// Read into `round` every ref and counter a run of `Apply` jobs will read,
/// in one go.
fn prefetch(state: &StateStore, round: &mut Round<'_>, run: &[Decoded]) -> Result<()> {
    let mut keys: Vec<(u64, &[u8])> = Vec::new();
    for (log, commands) in run.iter().flatten() {
        if !state.contains(*log) {
            continue;
        }
        for cmd in commands.iter().flatten() {
            if let Some(command::Kind::Commit(c)) = &cmd.kind {
                keys.extend(c.updates.iter().map(|u| (*log, u.name.as_bytes())));
            }
        }
    }
    round.prefetch(&keys)
}
