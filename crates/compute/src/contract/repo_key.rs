//! What an application calls a repository, and the name every call between an
//! application and Enroute spells.
//!
//! Chosen by the application rather than minted here, so an application
//! addresses a repository by the id it already has and keeps no table mapping
//! ours onto its own. Unique across the install, and flat: a key names a
//! repository and says nothing else about it. Opaque, too — Enroute stores a
//! key, compares it byte for byte, and never parses it or derives anything
//! from it. The shape is narrow on purpose: comparison is byte-exact, so
//! anything that lets two keys look alike while differing makes two
//! repositories a person cannot tell apart — outside ASCII that is homoglyphs
//! and normalization forms, and inside it whitespace. What is left needs no
//! escaping wherever a key is written. A `/` may join segments (`acme/widgets`),
//! so with no `authorize` hook the git URL path is the key. An application
//! that renames or transfers repositories still keys by an id that never
//! moves — see docs/patterns/namespacing.md.

use std::fmt;
use std::str::FromStr;

/// The most an application may hand us.
///
/// Bounded because it is recorded on spans and stored on every repository, so
/// the length is ours to cap rather than a caller's to choose.
pub(crate) const MAX_REPO_KEY_BYTES: usize = 256;

/// What a key, or the start of one, is made of. The `/` matters only to a
/// prefix: a key is split on it first.
const fn key_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/')
}

/// What an application calls one of its repositories.
///
/// A string that met the rules: Enroute takes any name, and this is where what
/// a key may be is answered.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RepoKey(String);

impl RepoKey {
    /// Whether `key` meets the rules: the one statement of them, so a git
    /// path, the contract and the ledger agree.
    #[must_use]
    pub(crate) fn is_valid(key: &str) -> bool {
        let ends_well = |b: Option<&u8>| b.is_some_and(u8::is_ascii_alphanumeric);
        key.len() <= MAX_REPO_KEY_BYTES
            && key.split('/').all(|segment| {
                ends_well(segment.as_bytes().first())
                    && ends_well(segment.as_bytes().last())
                    && segment.bytes().all(key_byte)
            })
    }

    /// The key as the ledger and a span want it.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The string does not meet the rules a key has.
///
/// One refusal and not one per rule: the rules are short, the caller picked the
/// string, and saying all of them is what lets it pick another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a repository key is 1 to {MAX_REPO_KEY_BYTES} bytes of '/'-separated segments; each \
     segment is ASCII letters, digits, '-', '_' and '.', starting and ending with a letter or digit"
)]
pub(crate) struct BadRepoKey;

impl fmt::Display for RepoKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for RepoKey {
    type Err = BadRepoKey;

    fn from_str(key: &str) -> Result<Self, Self::Err> {
        Self::is_valid(key)
            .then(|| Self(key.to_owned()))
            .ok_or(BadRepoKey)
    }
}

/// Refuses a listing prefix that starts no key.
///
/// A prefix is not a key: it ends wherever the caller stopped and may be
/// empty, which narrows nothing. What it holds is what a key holds.
///
/// # Errors
///
/// Returns an error if `prefix` holds what no key holds.
pub(crate) fn check_prefix(prefix: &str) -> Result<(), BadRepoKey> {
    let ok = prefix.len() <= MAX_REPO_KEY_BYTES && prefix.bytes().all(key_byte);
    ok.then_some(()).ok_or(BadRepoKey)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A prefix ends wherever the caller stopped, which is not where a key may.
    #[test]
    fn a_prefix_need_not_be_a_key() {
        for prefix in ["", "acme.", "acme-", "-", "_", "a", "acme/", "acme/back"] {
            check_prefix(prefix).unwrap_or_else(|bad| panic!("{prefix:?} refused: {bad}"));
        }
        // But it holds what a key holds: anything else starts no key.
        for bad in ["a b", "ünï", "a%2F"] {
            assert_eq!(check_prefix(bad), Err(BadRepoKey), "{bad:?}");
        }
        assert_eq!(
            check_prefix(&"k".repeat(MAX_REPO_KEY_BYTES + 1)),
            Err(BadRepoKey)
        );
    }

    #[test]
    fn a_key_round_trips_unchanged() {
        let key: RepoKey = "repo-1a2b3c".parse().unwrap();
        assert_eq!(key.to_string(), "repo-1a2b3c");
    }

    #[test]
    fn the_shapes_an_application_already_has_are_taken() {
        // A UUID, a ULID, a row id, a slug and a dotted name: whatever the
        // application calls the repository, it should not have to invent
        // something new to say it.
        for key in [
            "3f1b9a4e-0000-4000-8000-000000000000",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "42",
            "acme-backend",
            "repo_1.2",
        ] {
            key.parse::<RepoKey>()
                .unwrap_or_else(|bad| panic!("{key:?} refused: {bad}"));
        }
    }

    #[test]
    fn case_is_kept_and_never_folded() {
        // Two keys, because folding them together would make one repository
        // out of two an application means to keep apart.
        let upper: RepoKey = "Repo".parse().unwrap();
        let lower: RepoKey = "repo".parse().unwrap();
        assert_ne!(upper, lower);
    }

    #[test]
    fn a_key_is_bounded_at_both_ends() {
        assert_eq!("".parse::<RepoKey>(), Err(BadRepoKey));
        "k".repeat(MAX_REPO_KEY_BYTES).parse::<RepoKey>().unwrap();
        let over = "k".repeat(MAX_REPO_KEY_BYTES + 1);
        assert_eq!(over.parse::<RepoKey>(), Err(BadRepoKey));
    }

    #[test]
    fn nothing_outside_ascii_is_taken() {
        // The whole reason for the set: these render as `acme` or close to it
        // while differing byte for byte, so admitting them would make two
        // repositories nobody can tell apart.
        for key in ["ünïcode", "\u{0430}cme", "acme\u{0301}", "🔑"] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn whitespace_and_what_would_need_escaping_are_refused() {
        // A key reaches a URL, a log line and a metrics label, and none of
        // them should have to quote it.
        for key in [
            "a b", " acme", "acme ", "a\tb", "a\nb", "a%2Fb", "a?b", "a#b", "a:b", "a&b", "a\\b",
            "a\"b",
        ] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn a_key_starts_and_ends_with_a_letter_or_digit() {
        // Which also settles `.` and `..`, neither of which is a key.
        for key in ["-acme", "acme-", ".acme", "acme.", "_a_", ".", ".."] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn a_key_may_have_segments() {
        for key in ["acme/backend", "a/b/c", "acme/w.x"] {
            key.parse::<RepoKey>()
                .unwrap_or_else(|bad| panic!("{key:?} refused: {bad}"));
        }
        // Each segment follows the rules of a whole key.
        for key in [
            "a//b", "a/../b", "a/./b", "/acme", "acme/", "acme/-x", "acme/x.", "a/%2F", "../a",
            ".git", "a/.b", "a/_b", "a-/b",
        ] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
        // The bound is on the whole key, not on each segment.
        let long = format!("{}/k", "k".repeat(MAX_REPO_KEY_BYTES - 1));
        assert_eq!(long.parse::<RepoKey>(), Err(BadRepoKey));
    }

    #[test]
    fn a_refusal_states_the_rules() {
        // The application picked the key, so the one refusal has to be enough
        // to fix it without reading the proto.
        let said = BadRepoKey.to_string();
        assert!(said.contains("letters"), "{said}");
        assert!(said.contains(&MAX_REPO_KEY_BYTES.to_string()), "{said}");
    }
}
