//! Which ref names and ref targets an update may use, as git's
//! `receive-pack` decides. A push and the API check the same rules, so a ref
//! that one path can set, the other can update and delete.

use crate::git::oid::Kind;

/// What an update does to its ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Create or move the ref.
    Set,
    /// Delete the ref.
    Delete,
}

/// Whether `receive-pack` takes `name` for an update in `mode`:
/// `check_refname_format`, plus its rule against one-level names such as
/// `refs/x`, which a delete may still use (to remove one made before).
pub(crate) fn valid(name: &str, mode: Mode) -> bool {
    let Some(rest) = name.strip_prefix("refs/") else {
        return false;
    };
    if mode == Mode::Set && !rest.contains('/') {
        return false;
    }
    gix_validate::reference::name(gix_object::bstr::BStr::new(name)).is_ok()
}

/// Whether `name` is a branch.
fn is_branch(name: &str) -> bool {
    name.starts_with("refs/heads/")
}

/// Whether `name` is a branch that an update may set.
pub(crate) fn valid_branch(name: &str) -> bool {
    is_branch(name) && valid(name, Mode::Set)
}

/// Why a ref may not point at an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BadTarget {
    /// The repository does not have the object.
    Missing,
    /// A branch names an object that is not a commit.
    NotACommit,
}

/// Whether ref `name` may point at an object of `kind` (`None`: the
/// repository does not have it).
pub(crate) fn check_target(name: &str, kind: Option<Kind>) -> Result<(), BadTarget> {
    match kind {
        None => Err(BadTarget::Missing),
        Some(k) if is_branch(name) && k != Kind::Commit => Err(BadTarget::NotACommit),
        Some(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{BadTarget, Kind, Mode, check_target, valid};

    #[test]
    fn refnames_follow_git() {
        for (name, mode, ok) in [
            ("HEAD", Mode::Set, false),
            ("refs/foo", Mode::Set, false),
            ("refs/foo", Mode::Delete, true),
            ("refs/", Mode::Delete, false),
            ("refs/heads/../evil", Mode::Set, false),
            ("refs/heads/ok-branch", Mode::Set, true),
            ("refs/tags/v1.0", Mode::Set, true),
            ("refs/heads/a.lock/b", Mode::Set, false),
            ("refs/heads/a.lock/b", Mode::Delete, false),
            ("refs/heads/a.lock", Mode::Set, false),
            ("refs/heads/a.", Mode::Set, false),
            ("refs/heads/a./b", Mode::Set, true),
            ("refs/heads/.a", Mode::Set, false),
            ("refs/heads/a@{b", Mode::Set, false),
            ("refs/heads/a//b", Mode::Set, false),
            ("refs/heads/a b", Mode::Set, false),
            ("refs/heads/a\x7fb", Mode::Set, false),
            ("refs/heads/é", Mode::Set, true),
            ("refs/heads/@", Mode::Set, true),
        ] {
            assert_eq!(valid(name, mode), ok, "{name} {mode:?}");
        }
    }

    #[test]
    fn only_a_branch_must_name_a_commit() {
        assert_eq!(check_target("refs/heads/a", None), Err(BadTarget::Missing));
        assert_eq!(
            check_target("refs/heads/a", Some(Kind::Tree)),
            Err(BadTarget::NotACommit)
        );
        assert_eq!(check_target("refs/tags/a", Some(Kind::Tree)), Ok(()));
        assert_eq!(check_target("refs/heads/a", Some(Kind::Commit)), Ok(()));
    }
}
