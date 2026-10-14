//! A git push (a `receive-pack` request): ingest the pack, check each
//! command, ask the hooks, commit what is left in one log entry, and report.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use enroute_shared::state::{Commit, RefUpdate};

use crate::compute::Compute;
use crate::git::io::{Incoming, ReadAt, Suffix};
use crate::git::oid::Oid;
use crate::git::refname::{self, BadTarget, Mode};
use crate::hooks::{HookCommand, PreReceiveMode, PushContext, PushHooks, apply_judgements};
use crate::protocol::receive_pack::{self, PushRequest};
use crate::repo::Repo;
use crate::repo::ingest::{Ingested, Pass1, ingest, ingest_stored, kind_of, pass1};
use crate::repo::view::Snapshot;
use crate::storage::Sealed;
use crate::util::micros;
use crate::util::now_ms;

/// Where one command of a push stands.
#[derive(Debug, Clone)]
enum Status {
    /// Refused, with what the client sees.
    Refused(String),
    /// Not refused so far.
    Pending,
    /// Its ref moved.
    Landed,
}

/// A push's pack and its sequential pass, until the first try takes it.
struct Received {
    pack: Arc<dyn ReadAt>,
    scanned: Option<Result<Pass1>>,
}

/// A push ingested against a view: the objects, each command's check, and
/// that view.
type Checked = (Ingested, Vec<Option<BadCommand>>, Arc<Snapshot>);

impl Compute {
    /// Handle a receive-pack request body from the pusher `ctx`. Returns the
    /// report-status response.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository cannot be found or created, or if
    /// a commit or a hook's storage fails. A bad pack is not an error: its
    /// response says so.
    pub async fn push<S, E>(
        &self,
        name: &str,
        body: S,
        gzip: bool,
        ctx: &PushContext,
    ) -> Result<Vec<u8>>
    where
        S: futures::Stream<Item = std::result::Result<bytes::Bytes, E>> + Unpin,
        E: Into<anyhow::Error>,
    {
        let (mut body, req) = self.receive(body, gzip).await?;
        if req.commands.is_empty() {
            return Ok(Vec::new());
        }
        let mut clock = Stopwatch::start();
        let atomic = req.has("atomic");
        let hooks = self.hooks.push.as_deref();
        let pre = hooks.and_then(|h| Some((h, h.pre_receive_mode()?)));
        let followed = self.states.current(name);
        let fresh = followed.is_none();
        let repo = match followed {
            Some(r) => r,
            None => self.ensure_repo(name).await?,
        };
        repo.touch();
        clock.repo = clock.elapsed();
        // Only a pre-receive hook reads the force flags.
        let checked = self
            .ingest_latest(name, repo, fresh, &mut body, &req, pre.is_some())
            .await?;
        let (ingested, checks, snap) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                tracing::info!(repo = name, "push rejected: {e}");
                let status = vec![Status::Refused("unpacker error".into()); req.commands.len()];
                return Ok(report(&req, Err(e), status, &[]));
            }
        };
        clock.ingest = clock.elapsed();
        let epoch = snap.meta.epoch;
        let Ingested {
            mut layer, force, ..
        } = ingested;
        #[expect(clippy::indexing_slicing, reason = "force has one flag per command")]
        let forced = |i: usize| force.as_ref().is_some_and(|f| f[i]);
        let mut status: Vec<Status> = checks
            .into_iter()
            .map(|b| match b {
                Some(b) => Status::Refused(b.text().to_string()),
                None => Status::Pending,
            })
            .collect();
        // An atomic push with a refusal already lands nothing: no hook is asked.
        settle_atomic(atomic, &mut status);
        let asked = pending(&status);
        if let Some((hooks, mode)) = pre
            && !asked.is_empty()
        {
            let commands = hook_commands(&req, &asked, &forced);
            let verdicts = match self.readable(name, epoch, mode, &mut layer).await? {
                None => judge(hooks, ctx, name, &commands).await,
                Some(refused) => Err(refused),
            };
            let verdicts = verdicts.unwrap_or_else(|e| vec![Some(e); asked.len()]);
            apply(&mut status, &asked, verdicts, &Status::Pending);
            settle_atomic(atomic, &mut status);
        }

        let sent = pending(&status);
        clock.decided = clock.elapsed();
        // Objects no ref will name are not worth an entry.
        if !sent.is_empty() {
            let updates = ref_updates(&req, &sent);
            let c = self
                .commit(
                    name,
                    epoch,
                    layer,
                    Commit {
                        updates,
                        atomic,
                        pushed_at_ms: now_ms(),
                        ..Default::default()
                    },
                )
                .await?;
            let results = match &c.outcome.error {
                Some(e) => vec![Some(e.clone()); sent.len()],
                None => c.outcome.ref_errors().collect(),
            };
            apply(&mut status, &sent, results, &Status::Landed);
            clock.published = c.published.duration_since(clock.start);
            clock.answered = clock.elapsed();
        }
        clock.log();

        let mut messages = Vec::new();
        if let Some(hooks) = hooks {
            let landed = landed(&status);
            if !landed.is_empty() {
                let commands = hook_commands(&req, &landed, &forced);
                messages = post_receive(hooks, ctx, name, &commands).await;
            }
        }
        Ok(report(&req, Ok(()), status, &messages))
    }

    /// Get the push ready for its pre-receive hook. In `Objects` mode the
    /// pack is committed first (taking `layer`), with no ref moved, so the
    /// hook can read its objects by oid. Returns the commit's refusal.
    // The hook reads through the API, which takes a read barrier, so on any
    // node. A node-local overlay can replace this commit later.
    async fn readable(
        &self,
        name: &str,
        epoch: u64,
        mode: PreReceiveMode,
        layer: &mut Option<(Sealed, u32)>,
    ) -> Result<Option<String>> {
        if mode == PreReceiveMode::Objects
            && let Some(objects) = layer.take()
        {
            let c = self
                .commit(name, epoch, Some(objects), Commit::default())
                .await?;
            return Ok(c.outcome.error);
        }
        Ok(None)
    }

    /// Receive a push's body (small ones in memory, large ones in a scratch
    /// file, which ingest reads at random), parse its commands, and run its
    /// pack's sequential pass as the body arrives, as git's index-pack
    /// does.
    async fn receive<S, E>(&self, body: S, gzip: bool) -> Result<(Option<Received>, PushRequest)>
    where
        S: futures::Stream<Item = std::result::Result<bytes::Bytes, E>> + Unpin,
        E: Into<anyhow::Error>,
    {
        let t0 = Instant::now();
        let (tap, mut rx) = tokio::sync::mpsc::channel(256);
        let ws = self.storage.workspace();
        let scan = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut inc = Incoming::new(move || Ok(rx.blocking_recv()));
            let req = parse_commands(&mut inc)?;
            inc.take(req.pack_offset)?;
            let scanned = (!inc.fill(1)?.is_empty()).then(|| pass1(&mut inc, &ws));
            Ok((req, scanned))
        });
        let body = self.storage.spool(body, gzip, MEMORY_PUSH, tap).await?;
        tracing::debug!(target: "enroute::request_timing", spool_us = micros(t0.elapsed()), "spool timing");
        let (req, scanned) = scan.await??;
        let received = match scanned {
            Some(scanned) => Some(Received {
                pack: Arc::new(Suffix::new(body, u64::try_from(req.pack_offset)?)?),
                scanned: Some(scanned),
            }),
            None => None,
        };
        Ok((received, req))
    }

    /// Ingest the push against `repo`, this node's view, and check its
    /// commands. Validate against that view without waiting for it to catch
    /// up with every concurrent push: the ref CAS guards consistency, and a
    /// slightly older view can at worst store duplicate objects (removed by
    /// compaction). Only if something the push needs is missing (and the view
    /// is not `fresh`) wait for the latest state and try again. The inner
    /// `Err` says why the pack is bad.
    async fn ingest_latest(
        &self,
        name: &str,
        mut repo: Arc<Repo>,
        mut fresh: bool,
        body: &mut Option<Received>,
        req: &PushRequest,
        want_force: bool,
    ) -> Result<std::result::Result<Checked, String>> {
        loop {
            // The incarnation the push is checked against, the only one it
            // may land in.
            let snap = repo.snapshot();
            let pack = match body {
                Some(b) => match b.scanned.take().transpose() {
                    Ok(scanned) => Some((b.pack.clone(), scanned)),
                    // A bad pack is bad against every view.
                    Err(e) => return Ok(Err(format!("{e:#}"))),
                },
                None => None,
            };
            match self.ingest_push(&snap, pack, req, want_force).await {
                Ok((ing, checks))
                    if fresh || !checks.iter().flatten().any(|b| b.lacks_objects()) =>
                {
                    return Ok(Ok((ing, checks, snap)));
                }
                Err(e) if fresh => return Ok(Err(format!("{e:#}"))),
                _ => {
                    fresh = true;
                    repo = self.ensure_repo(name).await?;
                }
            }
        }
    }

    /// Ingest a push's pack (if any, with its sequential pass unless an
    /// earlier try used it) against `snap` and check its commands. With
    /// `want_force`, it also finds their force flags.
    async fn ingest_push(
        &self,
        snap: &Arc<Snapshot>,
        pack: Option<(Arc<dyn ReadAt>, Option<Pass1>)>,
        req: &PushRequest,
        want_force: bool,
    ) -> Result<(Ingested, Vec<Option<BadCommand>>)> {
        let targets: Vec<Oid> = req.commands.iter().map(|c| c.new).collect();
        let olds: Option<Vec<Oid>> =
            want_force.then(|| req.commands.iter().map(|c| c.old).collect());
        let ingested = match pack {
            Some((pack, Some(scanned))) => {
                let ws = self.storage.workspace();
                ingest(snap.clone(), pack, ws, targets, olds, scanned).await?
            }
            Some((pack, None)) => {
                let ws = self.storage.workspace();
                ingest_stored(snap.clone(), pack, ws, targets, olds).await?
            }
            None => Ingested::unpacked(snap, &targets, olds.as_deref()),
        };
        let checks = check_commands(snap, &ingested, req);
        Ok((ingested, checks))
    }
}

/// The commands not refused so far.
fn pending(status: &[Status]) -> Vec<usize> {
    status
        .iter()
        .enumerate()
        .filter(|(_, s)| matches!(s, Status::Pending))
        .map(|(i, _)| i)
        .collect()
}

/// The commands that landed.
fn landed(status: &[Status]) -> Vec<usize> {
    status
        .iter()
        .enumerate()
        .filter(|(_, s)| matches!(s, Status::Landed))
        .map(|(i, _)| i)
        .collect()
}

/// The ref updates of the commands `which`.
#[expect(clippy::indexing_slicing, reason = "which indexes req.commands")]
fn ref_updates(req: &PushRequest, which: &[usize]) -> Vec<RefUpdate> {
    which
        .iter()
        .map(|&i| &req.commands[i])
        .map(|c| RefUpdate {
            name: c.name.clone(),
            old: nonzero(c.old),
            new: nonzero(c.new),
        })
        .collect()
}

/// Set each of the `which` commands from its verdict: refused, or `ok`.
#[expect(clippy::indexing_slicing, reason = "which indexes status")]
fn apply(status: &mut [Status], which: &[usize], verdicts: Vec<Option<String>>, ok: &Status) {
    for (&i, why) in which.iter().zip(verdicts) {
        status[i] = match why {
            Some(why) => Status::Refused(why),
            None => ok.clone(),
        };
    }
}

/// An atomic push lands all of its commands or none: once one is refused,
/// refuse the others still pending.
fn settle_atomic(atomic: bool, status: &mut [Status]) {
    if !atomic || !status.iter().any(|s| matches!(s, Status::Refused(_))) {
        return;
    }
    for s in status.iter_mut().filter(|s| matches!(s, Status::Pending)) {
        *s = Status::Refused("atomic push failure".to_string());
    }
}

/// Request bodies up to this size are kept in memory.
const MEMORY_PUSH: usize = 16 << 20;

/// Parse the commands that precede the pack, reading only as much of the
/// body as they need (16 MiB of commands is far beyond any push).
fn parse_commands(inc: &mut Incoming) -> Result<PushRequest> {
    let mut size = 64 << 10;
    loop {
        let head = inc.fill(size)?;
        match receive_pack::parse_request(head) {
            Ok(r) => return Ok(r),
            Err(e) if head.len() >= size && size < 16 << 20 => {
                tracing::trace!("command section exceeds {size} bytes ({e:#}); reading more");
                size *= 16;
            }
            Err(e) => return Err(e),
        }
    }
}

/// A ref value on the wire: the zero id means none.
fn nonzero(o: Oid) -> Option<Oid> {
    (!o.is_zero()).then_some(o)
}

/// The commands `which` as hooks see them.
#[expect(clippy::indexing_slicing, reason = "which indexes req.commands")]
fn hook_commands(
    req: &PushRequest,
    which: &[usize],
    forced: &impl Fn(usize) -> bool,
) -> Vec<HookCommand> {
    which
        .iter()
        .map(|&i| {
            let c = &req.commands[i];
            HookCommand {
                refname: c.name.clone(),
                old: nonzero(c.old),
                new: nonzero(c.new),
                force: forced(i),
            }
        })
        .collect()
}

/// Run the post-receive hook; its messages, or none if it fails.
async fn post_receive(
    hooks: &dyn PushHooks,
    ctx: &PushContext,
    name: &str,
    commands: &[HookCommand],
) -> Vec<String> {
    match hooks.post_receive(ctx, name, commands).await {
        Ok(messages) => messages,
        Err(e) => {
            tracing::warn!(repo = name, "post-receive hook failed: {e:#}");
            Vec::new()
        }
    }
}

/// Each command's refusal, if any, from `pre_receive`. `Err` fails them all.
async fn judge(
    hooks: &dyn PushHooks,
    ctx: &PushContext,
    repo: &str,
    commands: &[HookCommand],
) -> std::result::Result<Vec<Option<String>>, String> {
    match hooks.pre_receive(ctx, repo, commands).await {
        Ok(judgements) => apply_judgements(commands, judgements),
        Err(e) => {
            // The client sees only that it failed: the cause is the host's.
            tracing::warn!(repo, "pre-receive hook failed: {e:#}");
            Err("pre-receive hook failed".to_string())
        }
    }
}

/// The report-status response to `req`: whether the pack unpacked, and each
/// command's refusal.
fn report(
    req: &PushRequest,
    unpack: Result<(), String>,
    status: Vec<Status>,
    messages: &[String],
) -> Vec<u8> {
    let named: Vec<_> = req
        .commands
        .iter()
        .map(|c| c.name.clone())
        .zip(status.into_iter().map(|s| match s {
            Status::Refused(why) => Some(why),
            // Pending only if the ledger answered for fewer refs than it
            // was sent, which it does not.
            Status::Landed | Status::Pending => None,
        }))
        .collect();
    receive_pack::report(unpack, &named, req.has("side-band-64k"), messages)
}

/// Why a push command is refused before it reaches the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BadCommand {
    Name,
    Target(BadTarget),
}

impl BadCommand {
    /// What git's `receive-pack` reports.
    fn text(self) -> &'static str {
        match self {
            BadCommand::Name => "funny refname",
            BadCommand::Target(BadTarget::Missing) => "missing necessary objects",
            BadCommand::Target(BadTarget::NotACommit) => "branches must point to commits",
        }
    }

    /// Whether a newer view may have what the command needs.
    fn lacks_objects(self) -> bool {
        match self {
            BadCommand::Target(BadTarget::Missing) => true,
            BadCommand::Name | BadCommand::Target(BadTarget::NotACommit) => false,
        }
    }
}

fn check_commands(snap: &Snapshot, ing: &Ingested, req: &PushRequest) -> Vec<Option<BadCommand>> {
    req.commands
        .iter()
        .map(|c| {
            if c.new.is_zero() {
                return (!refname::valid(&c.name, Mode::Delete)).then_some(BadCommand::Name);
            }
            if !refname::valid(&c.name, Mode::Set) {
                return Some(BadCommand::Name);
            }
            refname::check_target(&c.name, kind_of(snap, ing, &c.new))
                .err()
                .map(BadCommand::Target)
        })
        .collect()
}

/// When the stages of one push ended, from its start, for
/// `enroute::push_timing`.
struct Stopwatch {
    start: Instant,
    /// The repository was found.
    repo: Duration,
    /// The pack was ingested and the commands checked.
    ingest: Duration,
    /// The hooks were asked, and what to commit is known.
    decided: Duration,
    /// The layer was published, and the ledger answered (zero: nothing to
    /// commit).
    published: Duration,
    answered: Duration,
}

impl Stopwatch {
    fn start() -> Stopwatch {
        Stopwatch {
            start: Instant::now(),
            repo: Duration::ZERO,
            ingest: Duration::ZERO,
            decided: Duration::ZERO,
            published: Duration::ZERO,
            answered: Duration::ZERO,
        }
    }

    /// The time since the start.
    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    #[expect(
        clippy::unwrap_used,
        reason = "the stages are timed in order, so the differences cannot be negative"
    )]
    fn log(&self) {
        // A stage that did not run took no time.
        let published = self.published.max(self.decided);
        let answered = self.answered.max(published);
        tracing::debug!(
            target: "enroute::push_timing",
            repo_us = micros(self.repo),
            ingest_us = micros(self.ingest.checked_sub(self.repo).unwrap()),
            publish_us = micros(published.checked_sub(self.ingest).unwrap()),
            propose_us = micros(answered.checked_sub(published).unwrap()),
            "push timing"
        );
    }
}
