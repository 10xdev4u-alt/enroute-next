//! A repository's state in the node's state store ([`crate::state_store`]):
//! how a [`RepoMeta`] maps onto its keys, the view the state machine
//! applies commands to ([`Stored`]), and whole states in and out (hosting a
//! repository, Raft snapshots, checkpoints, new subscribers).
//!
//! ```text
//! HEAD          the branch HEAD names
//! ref           an object id, then when it was set (Unix ms, 8 B; none: unknown)
//! layer         by first id: kind (1 B), log index (8 B), LayerMeta without its bytes
//! counters      version, upload_gen, next_id, the number of layers, last push,
//!               epoch, deleted, keep_head
//! ```
//!
//! **Inline layers stay in the log.** A layer whose bytes came in a log
//! entry is *pinned* there: its kind says so, with the entry's index, and
//! the store pins that entry so the log keeps it until the layer is
//! flushed (replaced by a stored one). A layer whose bytes came in a whole
//! state (a snapshot from the leader, an initial state) has them *kept* in
//! the store instead, as no local log entry has them.

use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use prost::Message;
use prost::bytes::Bytes;
use raft::prelude::ConfState;

use enroute_shared::oid::Oid;
use enroute_shared::proto::{Record, RepoSummary};
use enroute_shared::state::{Change, Command, DEFAULT_HEAD, LayerMeta, Outcome, RepoMeta, command};

use crate::state_machine::{State, count};
use crate::state_store::{Counters, Entry, LogId, Root, Round, StateStore};

/// A layer in the object store.
const STORED: u8 = 0;
/// An inline layer whose bytes are in the log entry at its index.
const PINNED: u8 = 1;
/// An inline layer whose bytes are kept in the store.
const KEPT: u8 = 2;

fn encode_layer(l: &LayerMeta, kind: u8, index: u64) -> Vec<u8> {
    let meta = LayerMeta {
        data: None,
        ..l.clone()
    }
    .encode_to_vec();
    let mut v = Vec::with_capacity(9 + meta.len());
    v.push(kind);
    v.extend_from_slice(&index.to_le_bytes());
    v.extend_from_slice(&meta);
    v
}

/// A layer as stored: its metadata (an inline layer's bytes empty, as they
/// are elsewhere), kind and log index.
fn decode_layer(v: &[u8]) -> Result<(LayerMeta, u8, u64)> {
    let (&kind, rest) = v.split_first().context("short layer")?;
    let (index, meta) = rest.split_first_chunk::<8>().context("short layer")?;
    let mut l = LayerMeta::decode(meta)?;
    if kind != STORED {
        l.data = Some(Bytes::default());
    }
    Ok((l, kind, u64::from_le_bytes(*index)))
}

/// The root of incarnation `epoch` of `repo` (with membership `conf`) at
/// log index `index` of `term`.
pub(crate) fn root(repo: &str, epoch: u64, conf: &ConfState, index: u64, term: u64) -> Root {
    Root {
        applied_index: index,
        applied_term: term,
        repo: repo.to_string(),
        epoch,
        voters: conf.voters.clone(),
        learners: conf.learners.clone(),
        ..Default::default()
    }
}

impl Root {
    /// Its membership.
    pub(crate) fn conf(&self) -> ConfState {
        ConfState {
            voters: self.voters.clone(),
            learners: self.learners.clone(),
            ..Default::default()
        }
    }

    /// Its identity with whole state `meta`, as of its applied index.
    pub(crate) fn record(self, meta: RepoMeta) -> Record {
        Record {
            repo: self.repo,
            epoch: self.epoch,
            index: self.applied_index,
            voters: self.voters,
            learners: self.learners,
            meta: Some(meta),
        }
    }
}

impl Counters {
    /// The counters of a whole state.
    pub(crate) fn of(meta: &RepoMeta) -> Counters {
        Counters {
            version: meta.version,
            upload_gen: meta.upload_gen,
            next_id: meta.next_id,
            layers: count(meta.layers.len()),
            last_push_ms: meta.last_push_ms,
            epoch: meta.epoch,
            deleted: meta.deleted,
            keep_head: meta.keep_head,
        }
    }

    /// Set a whole state's counters (its layers are its own).
    pub(crate) fn apply_to(&self, meta: &mut RepoMeta) {
        (meta.version, meta.upload_gen, meta.next_id) =
            (self.version, self.upload_gen, self.next_id);
        (meta.last_push_ms, meta.epoch) = (self.last_push_ms, self.epoch);
        (meta.deleted, meta.keep_head) = (self.deleted, self.keep_head);
    }
}

/// The branch HEAD of `log` names.
fn head(round: &mut Round<'_>, log: LogId) -> Result<String> {
    Ok(round
        .head(log)?
        .map(String::from_utf8)
        .transpose()?
        .unwrap_or_else(|| DEFAULT_HEAD.to_string()))
}

/// The summary of `log`, incarnation `epoch` of `name`: what describes it
/// (no refs or layers).
pub(crate) fn summary(
    round: &mut Round<'_>,
    log: LogId,
    name: &str,
    epoch: u64,
) -> Result<RepoSummary> {
    let c = round.counters(log)?;
    let mut s = RepoSummary {
        name: name.to_string(),
        epoch,
        deleted: c.deleted,
        version: c.version,
        ..Default::default()
    };
    if !c.deleted {
        (s.head, s.last_push_ms) = (head(round, log)?, c.last_push_ms);
    }
    Ok(s)
}

/// `log`'s membership is now `conf`.
pub(crate) fn set_conf(round: &mut Round<'_>, log: LogId, conf: &ConfState) -> Result<()> {
    let root = round.root_mut(log)?;
    (root.voters, root.learners) = (conf.voters.clone(), conf.learners.clone());
    Ok(())
}

/// Apply log entry `index` (of `term`) to `log`'s state in `round`: its
/// command (None: not a command), and the entry as applied. The command's
/// outcome and change.
pub(crate) fn apply(
    round: &mut Round<'_>,
    log: LogId,
    index: u64,
    term: u64,
    cmd: Option<&Command>,
) -> Result<Option<(Outcome, Option<Change>)>> {
    let done = match cmd {
        Some(cmd) => Some(crate::state_machine::apply(
            &mut Stored::new(round, log, index),
            index + 1,
            cmd,
        )?),
        None => None,
    };
    let root = round.root_mut(log)?;
    (root.applied_index, root.applied_term) = (index, term);
    Ok(done)
}

/// One repository's state in a round of the store, as the state machine
/// sees it, applying the entry at log index `index`.
pub(crate) struct Stored<'r, 's> {
    round: &'r mut Round<'s>,
    log: LogId,
    index: u64,
}

impl<'r, 's> Stored<'r, 's> {
    pub(crate) fn new(round: &'r mut Round<'s>, log: LogId, index: u64) -> Self {
        Stored { round, log, index }
    }
}

/// A ref's stored value: its object id, and when it was set (0: unknown).
fn ref_value(oid: Oid, updated_ms: u64) -> Vec<u8> {
    let mut v = oid.0.to_vec();
    if updated_ms > 0 {
        v.extend_from_slice(&updated_ms.to_le_bytes());
    }
    v
}

/// The object id and update time in a ref's stored value.
fn parse_ref(name: &str, v: &[u8]) -> Result<(Oid, u64)> {
    let bad = || anyhow::anyhow!("ref {name} is not an object id");
    let (oid, time) = v.split_at_checked(20).ok_or_else(bad)?;
    let updated_ms = match <[u8; 8]>::try_from(time) {
        Ok(t) => u64::from_le_bytes(t),
        Err(_) if time.is_empty() => 0,
        Err(_) => return Err(bad()),
    };
    Ok((Oid::from_slice(oid), updated_ms))
}

impl State for Stored<'_, '_> {
    fn get_ref(&mut self, name: &str) -> Result<Option<Oid>> {
        self.round
            .get_ref(self.log, name.as_bytes())?
            .map(|v| parse_ref(name, &v).map(|(oid, _)| oid))
            .transpose()
    }

    fn set_ref(&mut self, name: &str, value: Option<Oid>, updated_ms: u64) -> Result<()> {
        let value = value.map(|oid| ref_value(oid, updated_ms));
        self.round
            .set_ref(self.log, name.as_bytes(), value.as_deref());
        Ok(())
    }

    fn first_ref(&mut self, prefix: &str) -> Result<Option<String>> {
        Ok(self
            .round
            .first_ref(self.log, prefix.as_bytes())?
            .map(String::from_utf8)
            .transpose()?)
    }

    fn head(&mut self) -> Result<String> {
        head(self.round, self.log)
    }

    fn set_head(&mut self, head: &str) -> Result<()> {
        self.round.set_head(self.log, head.as_bytes());
        Ok(())
    }

    fn counters(&mut self) -> Result<Counters> {
        self.round.counters(self.log)
    }

    fn set_counters(&mut self, c: Counters) -> Result<()> {
        self.round.set_counters(self.log, c);
        Ok(())
    }

    fn layer(&mut self, first_id: u32) -> Result<Option<LayerMeta>> {
        self.round
            .layer(self.log, first_id)?
            .map(|v| decode_layer(&v).map(|(l, _, _)| l))
            .transpose()
    }

    fn next_layer(&mut self, first_id: u32) -> Result<Option<LayerMeta>> {
        self.round
            .next_layer(self.log, first_id)?
            .map(|v| decode_layer(&v).map(|(l, _, _)| l))
            .transpose()
    }

    fn add_layer(&mut self, l: &LayerMeta) -> Result<()> {
        let value = if l.is_stored() {
            encode_layer(l, STORED, 0)
        } else {
            self.round.pin_inline(self.log, self.index, l.id.as_bytes());
            encode_layer(l, PINNED, self.index)
        };
        self.round.put_layer(self.log, l.first_id, &value);
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        self.round.clear_state(self.log)
    }

    fn remove_layer(&mut self, l: &LayerMeta) -> Result<()> {
        let v = self
            .round
            .layer(self.log, l.first_id)?
            .with_context(|| format!("layer {} is not in the manifest", l.id))?;
        let (_, kind, index) = decode_layer(&v)?;
        if kind != STORED {
            self.round.drop_inline(self.log, index, l.id.as_bytes());
        }
        self.round.delete_layer(self.log, l.first_id);
        Ok(())
    }
}

/// A whole state as store entries (to install): inline layers' bytes are
/// kept in the store.
pub(crate) fn entries(meta: &RepoMeta) -> Result<Vec<Entry>> {
    let mut out = Vec::with_capacity(meta.refs.len() + meta.layers.len() + 2);
    out.push(Entry::Head(meta.head.as_bytes().to_vec()));
    out.extend(meta.refs.iter().map(|(name, oid)| {
        let updated_ms = meta.ref_updated_ms.get(name).copied().unwrap_or(0);
        Entry::Ref {
            name: name.as_bytes().to_vec(),
            value: ref_value(*oid, updated_ms),
        }
    }));
    let mut last = None;
    for l in &meta.layers {
        ensure!(
            last.is_none_or(|f| f < l.first_id),
            "layer {} is out of id order (or shares its first id)",
            l.id
        );
        last = Some(l.first_id);
        let kind = if l.data.is_some() { KEPT } else { STORED };
        out.push(Entry::Layer {
            first_id: l.first_id,
            value: encode_layer(l, kind, 0),
        });
        if let Some(bytes) = &l.data {
            out.push(Entry::Inline {
                id: l.id.as_bytes().to_vec(),
                value: bytes.to_vec(),
            });
        }
    }
    out.push(Entry::Counters(Counters::of(meta)));
    Ok(out)
}

/// `log`'s whole state at one point: its root and state, inline layers'
/// bytes included (those pinned in the log read with `entry`, the command
/// at a log index). None if it has no state.
pub(crate) fn load(
    store: &StateStore,
    log: LogId,
    mut entry: impl FnMut(u64) -> Result<Command>,
) -> Result<Option<(Root, RepoMeta)>> {
    let mut meta = RepoMeta {
        head: DEFAULT_HEAD.to_string(),
        ..Default::default()
    };
    // Inline layers, by position: pinned (at a log index), and kept.
    let (mut pinned, mut kept_at, mut kept) = (Vec::new(), Vec::new(), HashMap::new());
    let Some(root) = store.export(log, |e| {
        match e {
            Entry::Head(value) => meta.head = String::from_utf8(value)?,
            Entry::Ref { name, value } => {
                let name = String::from_utf8(name)?;
                let (oid, updated_ms) = parse_ref(&name, &value)?;
                meta.set_ref_at(&name, Some(oid), updated_ms);
            }
            Entry::Layer { value, .. } => {
                let (l, kind, index) = decode_layer(&value)?;
                match kind {
                    PINNED => pinned.push((meta.layers.len(), index)),
                    KEPT => kept_at.push(meta.layers.len()),
                    _ => {}
                }
                meta.layers.push(l);
            }
            Entry::Counters(c) => c.apply_to(&mut meta),
            Entry::Inline { id, value } => drop(kept.insert(String::from_utf8(id)?, value)),
        }
        Ok(())
    })?
    else {
        return Ok(None);
    };
    // Each entry read once, however many inline layers it carries.
    let mut commands: HashMap<u64, Command> = HashMap::new();
    for (at, index) in pinned {
        let l = meta
            .layers
            .get_mut(at)
            .context("inline layer index out of range")?;
        let cmd = match commands.entry(index) {
            std::collections::hash_map::Entry::Occupied(c) => c.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(entry(index).with_context(|| {
                    format!("reading log entry {index} (inline layer {})", l.id)
                })?)
            }
        };
        let bytes = match &cmd.kind {
            Some(command::Kind::Commit(c)) => c
                .add_layers
                .iter()
                .find(|a| a.id == l.id)
                .and_then(|a| a.data.clone()),
            _ => None,
        };
        l.data =
            Some(bytes.with_context(|| {
                format!("log entry {index} does not carry inline layer {}", l.id)
            })?);
    }
    for at in kept_at {
        let l = meta
            .layers
            .get_mut(at)
            .context("inline layer index out of range")?;
        l.data = Some(
            kept.remove(&l.id)
                .with_context(|| format!("inline layer {} has no bytes", l.id))?
                .into(),
        );
    }
    Ok(Some((root, meta)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ref stored without a time reads as set at an unknown time.
    #[test]
    fn ref_values_carry_their_time() {
        let oid = Oid([7; 20]);
        assert_eq!(parse_ref("r", &ref_value(oid, 1234)).unwrap(), (oid, 1234));
        assert_eq!(ref_value(oid, 0), oid.0.to_vec());
        assert_eq!(parse_ref("r", &oid.0).unwrap(), (oid, 0));
        parse_ref("r", &[0; 21]).unwrap_err();
        parse_ref("r", &[0; 19]).unwrap_err();
    }
}
