//! The refs that one caller sees, as every ref listing shows them.

use std::sync::Arc;

use anyhow::{Context, Result};
use foldhash::{HashMap, HashMapExt};

use enroute_shared::state::RepoMeta;

use crate::access::Hidden;
use crate::git::oid::{Kind, Oid};
use crate::repo::machine::peel;
use crate::repo::view::Snapshot;

/// The refs of a repository that one caller sees, under the prefixes it
/// asked for. Advertisements and listings only render it.
#[derive(Debug, Default)]
pub(crate) struct RefView<'a> {
    /// The branch `HEAD` names, and its tip if the branch exists: `None`
    /// when the caller does not see `HEAD` or no branch is named.
    pub head: Option<(&'a str, Option<Oid>)>,
    /// Sorted by name, byte for byte.
    pub refs: Vec<(&'a str, Oid)>,
    /// What the annotated tags among `refs` point at, after
    /// [`RefView::peel`].
    pub peeled: HashMap<Oid, Oid>,
}

impl<'a> RefView<'a> {
    /// The refs of `meta` that are not `hidden`, under any of `prefixes` (all
    /// of them when there are none). `HEAD` is filtered as a ref named `HEAD`.
    pub(crate) fn new(meta: &'a RepoMeta, hidden: &Hidden, prefixes: &[impl AsRef<str>]) -> Self {
        let sees = |name: &str| {
            (prefixes.is_empty() || prefixes.iter().any(|p| name.starts_with(p.as_ref())))
                && !hidden.hides(name)
        };
        RefView {
            head: (sees("HEAD") && !meta.head.is_empty())
                .then(|| (meta.head.as_str(), meta.refs.get(&meta.head).copied())),
            refs: meta
                .refs
                .iter()
                .filter(|(name, _)| sees(name))
                .map(|(name, oid)| (name.as_str(), *oid))
                .collect(),
            peeled: HashMap::new(),
        }
    }

    /// Fill [`RefView::peeled`], loading tags that are not local without
    /// waiting on them.
    ///
    /// # Errors
    ///
    /// A read failed, or a tag's target is missing.
    pub(crate) async fn peel(&mut self, snap: &Arc<Snapshot>) -> Result<()> {
        let tags: Vec<_> = self
            .refs
            .iter()
            .filter_map(|(_, oid)| snap.lookup_kind(oid, Kind::Tag))
            .collect();
        for p in peel(snap, tags).await? {
            self.peeled.insert(
                snap.oid(p.start),
                snap.oid(p.end.context("tag target missing")?),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A repository holding `refs`, each with the tip its byte names.
    fn meta(refs: &[(&str, u8)]) -> RepoMeta {
        let mut m = RepoMeta::new();
        for (name, byte) in refs {
            m.set_ref_at(name, Some(Oid([*byte; 20])), 0);
        }
        m
    }

    fn view<'a>(m: &'a RepoMeta, hidden: &Hidden, prefixes: &[&str]) -> RefView<'a> {
        RefView::new(m, hidden, prefixes)
    }

    fn listed<'a>(v: &'a RefView<'a>) -> Vec<(&'a str, u8)> {
        v.refs.iter().map(|(name, oid)| (*name, oid.0[0])).collect()
    }

    fn names<'a>(v: &'a RefView<'a>) -> Vec<&'a str> {
        v.refs.iter().map(|(name, _)| *name).collect()
    }

    /// A listing is ordered by name, whatever order the state holds the refs
    /// in: a client diffing two advertisements depends on it.
    #[test]
    fn refs_are_listed_in_name_order() {
        let m = meta(&[
            ("refs/tags/v2", 2),
            ("refs/heads/main", 1),
            ("refs/tags/v1", 3),
            ("refs/heads/dev", 4),
        ]);
        let v = view(&m, &Hidden::default(), &[]);
        assert_eq!(
            listed(&v),
            [
                ("refs/heads/dev", 4),
                ("refs/heads/main", 1),
                ("refs/tags/v1", 3),
                ("refs/tags/v2", 2),
            ]
        );
    }

    /// The prefixes are a filter, not a group: each names a prefix, several
    /// are a union, and none is everything.
    #[test]
    fn prefixes_filter_the_listing() {
        let m = meta(&[
            ("refs/heads/main", 1),
            ("refs/heads/dev", 2),
            ("refs/tags/v1", 3),
        ]);
        assert_eq!(
            names(&view(&m, &Hidden::default(), &["refs/tags/"])),
            ["refs/tags/v1"]
        );
        assert_eq!(
            names(&view(
                &m,
                &Hidden::default(),
                &["refs/heads/", "refs/tags/"]
            )),
            ["refs/heads/dev", "refs/heads/main", "refs/tags/v1"]
        );
        assert_eq!(
            names(&view(&m, &Hidden::default(), &["refs/heads/main"])),
            ["refs/heads/main"],
            "a prefix is matched, not required to end at a separator"
        );
        assert!(
            view(&m, &Hidden::default(), &["refs/notes/"])
                .refs
                .is_empty()
        );
    }

    /// `HEAD` is filtered as a ref named `HEAD`, and reported with the tip of
    /// the branch it names: an unborn branch has none.
    #[test]
    fn head_is_reported_with_the_tip_it_names() {
        let m = meta(&[("refs/heads/main", 1), ("refs/heads/dev", 2)]);
        assert_eq!(
            view(&m, &Hidden::default(), &[]).head,
            Some(("refs/heads/main", Some(Oid([1; 20]))))
        );
        // The branch exists but holds nothing yet.
        let mut unborn = m.clone();
        unborn.refs.remove("refs/heads/main");
        assert_eq!(
            view(&unborn, &Hidden::default(), &[]).head,
            Some(("refs/heads/main", None))
        );
        // No branch named at all.
        let mut none = m.clone();
        none.head.clear();
        assert_eq!(view(&none, &Hidden::default(), &[]).head, None);
        // A listing that asks for no branch of it.
        assert_eq!(view(&m, &Hidden::default(), &["refs/tags/"]).head, None);
    }

    /// What the caller may not see is not listed, and hiding the branch
    /// `HEAD` names hides `HEAD` with it.
    #[test]
    fn hidden_refs_are_not_listed() {
        let m = meta(&[("refs/heads/main", 1), ("refs/heads/dev", 2)]);
        let dev = Hidden::of(&["refs/heads/dev"]);
        assert_eq!(listed(&view(&m, &dev, &[])), [("refs/heads/main", 1)]);
        assert_eq!(
            view(&m, &dev, &[]).head.map(|(name, _)| name),
            Some("refs/heads/main")
        );
        let main = Hidden::of(&["HEAD", "refs/heads/main"]);
        assert_eq!(listed(&view(&m, &main, &[])), [("refs/heads/dev", 2)]);
        assert_eq!(view(&m, &main, &[]).head, None);
    }
}
