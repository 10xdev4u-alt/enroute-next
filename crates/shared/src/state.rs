//! A repository's replicated state (refs and layer manifest), the commands
//! that change it and the changes they make: the protocol's types
//! (generated from `proto/state.proto`), with what both roles do with them.
//! Applying commands (the state machine) is the ledger's; a follower applies
//! the changes it is sent ([`RepoMeta::apply_change`]).

pub use crate::proto::{
    AdvanceUploadGen, Change, Command, Commit, Delete, LayerEdit, LayerMeta, Outcome, RefChange,
    RefResult, RefUpdate, Refusal, RemovedLayer, RepoMeta, command,
};

/// How many upload generations before the current one a stored layer may
/// still be added from (see `RepoMeta.upload_gen`): a publish has about
/// this many layer sweeps' time between its upload and its commit.
pub const UPLOAD_GEN_WINDOW: u64 = 2;

/// A layer id: `random` and the upload `generation`, in hex. The
/// generation is last, so ids (and object-store keys) spread evenly.
#[must_use]
pub fn layer_id(random: u128, generation: u64) -> String {
    format!("{random:032x}-{generation:016x}")
}

/// The upload generation in a layer id (None if it has none).
#[must_use]
pub fn layer_gen(id: &str) -> Option<u64> {
    let (_, generation) = id.rsplit_once('-')?;
    if generation.len() != 16 || !generation.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(generation, 16).ok()
}

impl LayerMeta {
    /// In the object store (not inline).
    pub fn is_stored(&self) -> bool {
        self.data.is_none()
    }

    /// The end of the layer's id range.
    pub fn end_id(&self) -> u64 {
        u64::from(self.first_id) + u64::from(self.id_span)
    }
}

impl From<&LayerMeta> for RemovedLayer {
    fn from(l: &LayerMeta) -> Self {
        RemovedLayer {
            id: l.id.clone(),
            first_id: l.first_id,
        }
    }
}

impl Command {
    /// A command that commits `commit` to `repo`'s log.
    #[must_use]
    pub fn commit(repo: &str, commit: Commit) -> Command {
        Command {
            repo: repo.to_string(),
            kind: Some(command::Kind::Commit(commit)),
            ..Default::default()
        }
    }

    /// A command that advances `repo`'s upload generation.
    #[must_use]
    pub fn advance_upload_gen(repo: &str) -> Command {
        Command {
            repo: repo.to_string(),
            kind: Some(command::Kind::AdvanceUploadGen(AdvanceUploadGen {})),
            ..Default::default()
        }
    }

    /// A command that deletes `repo`.
    #[must_use]
    pub fn delete(repo: &str) -> Command {
        Command {
            repo: repo.to_string(),
            kind: Some(command::Kind::Delete(Delete {})),
            ..Default::default()
        }
    }

    /// For incarnation `epoch` only (see `Command.epoch`).
    #[must_use]
    pub fn at(self, epoch: u64) -> Command {
        Command { epoch, ..self }
    }
}

impl Outcome {
    /// Per ref update: None if applied, else why not.
    pub fn ref_errors(&self) -> impl Iterator<Item = Option<String>> + '_ {
        self.ref_results.iter().map(|r| r.error.clone())
    }

    /// Refuse the whole command for `why`.
    pub fn refuse(&mut self, why: Refusal) {
        self.error = Some(why.text().to_string());
        self.set_refusal(why);
    }

    /// Refuse the whole command as invalid, showing `text`.
    pub fn refuse_invalid(&mut self, text: impl Into<String>) {
        self.error = Some(text.into());
        self.set_refusal(Refusal::Invalid);
    }

    /// Why the whole command was refused (None: it was not).
    #[must_use]
    pub fn refused(&self) -> Option<Refusal> {
        self.error.as_ref()?;
        Some(match self.refusal() {
            Refusal::Unspecified => Refusal::Invalid,
            why => why,
        })
    }
}

impl Refusal {
    /// What a refusal shows when it has no text of its own.
    #[must_use]
    pub fn text(self) -> &'static str {
        match self {
            Refusal::Unspecified | Refusal::Invalid => "command refused",
            Refusal::Deleted => "repository deleted",
            Refusal::OtherIncarnation => "repository deleted (and maybe created again) since",
            Refusal::StaleUpload => "layer uploaded in an expired generation; publish it again",
        }
    }
}

/// The HEAD of a new repository.
pub const DEFAULT_HEAD: &str = "refs/heads/main";

impl RepoMeta {
    /// A freshly created repository.
    #[must_use]
    pub fn new() -> RepoMeta {
        RepoMeta {
            head: DEFAULT_HEAD.to_string(),
            version: 1,
            ..Default::default()
        }
    }

    /// Set (or, with None, delete) a ref, set at `updated_ms` (0: unknown).
    pub fn set_ref_at(&mut self, name: &str, value: Option<crate::oid::Oid>, updated_ms: u64) {
        match value {
            Some(v) => self.refs.insert(name.to_string(), v),
            None => self.refs.remove(name),
        };
        if value.is_some() && updated_ms > 0 {
            self.ref_updated_ms.insert(name.to_string(), updated_ms);
        } else {
            self.ref_updated_ms.remove(name);
        }
    }

    /// Apply a change (as made by the ledger applying a command; see
    /// `Follow` in `ledger.proto`).
    pub fn apply_change(&mut self, c: &Change) {
        for r in &c.refs {
            self.set_ref_at(&r.name, r.value, r.updated_at_ms);
        }
        if let Some(head) = &c.head {
            self.head.clone_from(head);
        }
        if let Some(e) = &c.layers {
            let remove: std::collections::HashSet<&str> =
                e.remove.iter().map(String::as_str).collect();
            let at = self
                .layers
                .iter()
                .position(|l| remove.contains(l.id.as_str()))
                .unwrap_or(self.layers.len());
            self.layers.retain(|l| !remove.contains(l.id.as_str()));
            self.layers.splice(at..at, e.insert.iter().cloned());
        }
        if let Some(n) = c.next_id {
            self.next_id = n;
        }
        if let Some(g) = c.upload_gen {
            self.upload_gen = g;
        }
        if let Some(t) = c.last_push_ms {
            self.last_push_ms = t;
        }
        if c.deleted {
            self.refs.clear();
            self.ref_updated_ms.clear();
            self.layers.clear();
            self.deleted = true;
        }
        self.version = c.version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oid::Oid;

    #[test]
    fn states_roundtrip_through_protobuf() {
        use prost::Message;
        let mut m = RepoMeta::new();
        let layer = LayerMeta {
            id: "x".into(),
            objects: 2,
            bytes: 3,
            data: Some(bytes::Bytes::from_static(b"inline")),
            first_id: 5,
            id_span: 2,
            root: bytes::Bytes::from_static(&[9; 32]),
            meta_bytes: 4,
        };
        m.layers.push(layer);
        m.refs.insert("refs/heads/main".into(), Oid([7; 20]));
        let back = RepoMeta::decode(m.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.refs["refs/heads/main"], Oid([7; 20]));
    }

    #[test]
    fn a_refusal_without_a_reason_is_invalid() {
        let mut out = Outcome::default();
        assert_eq!(out.refused(), None);
        out.error = Some("something".into());
        assert_eq!(out.refused(), Some(Refusal::Invalid));
        out.refuse(Refusal::StaleUpload);
        assert_eq!(out.refused(), Some(Refusal::StaleUpload));
    }

    #[test]
    fn layer_ids_carry_their_generation() {
        let id = layer_id(u128::MAX - 5, 0x2a);
        assert_eq!(id.len(), 32 + 1 + 16);
        assert_eq!(layer_gen(&id), Some(0x2a));
        assert_eq!(layer_gen(&layer_id(0, u64::MAX)), Some(u64::MAX));
        for bad in [
            "",
            "abc",
            "x-2a",
            "a-+00000000000002a",
            "0123456789abcdef0123456789abcdef",
            "a-000000000000002g",
        ] {
            assert_eq!(layer_gen(bad), None, "{bad}");
        }
    }
}
