//! The git object formats Enroute reads and writes: commits, trees and tag
//! targets, over gix-object.

use anyhow::{Context, Result, bail};
use gix_object::WriteTo as _;

use super::oid::{Kind, OID_LEN, Oid};

/// A commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Commit<'a> {
    pub tree: Oid,
    /// In the order the commit gives them.
    pub parents: Vec<Oid>,
    pub author: Signature<'a>,
    pub committer: Signature<'a>,
    /// Everything after the blank line that ends the headers.
    pub message: &'a [u8],
}

/// Someone a commit names: `Name <email> 1700000000 +0100`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Signature<'a> {
    /// Raw bytes: git does not require UTF-8.
    pub name: &'a [u8],
    pub email: &'a [u8],
    /// Seconds since the epoch.
    pub seconds: i64,
    /// The time zone, in seconds east of UTC.
    pub offset_seconds: i32,
}

/// The largest time zone offset git writes, `±9959`, in seconds.
const MAX_OFFSET_SECONDS: u32 = 99 * 3600 + 59 * 60;

impl Signature<'_> {
    /// What keeps git from reading the signature back as [`write_commit`]
    /// writes it, if anything does.
    pub(crate) fn unwritable(&self) -> Option<&'static str> {
        let plain = |field: &[u8]| {
            !field.is_empty() && !field.iter().any(|b| matches!(b, b'<' | b'>' | b'\n' | 0))
        };
        if !plain(self.name) {
            Some("name is empty or holds `<`, `>`, a newline or NUL")
        } else if !plain(self.email) {
            Some("email is empty or holds `<`, `>`, a newline or NUL")
        } else if self.seconds < 0 {
            Some("time is before 1970")
        } else if self.offset_seconds % 60 != 0
            || self.offset_seconds.unsigned_abs() > MAX_OFFSET_SECONDS
        {
            Some("UTC offset is not whole minutes within ±99:59")
        } else {
            None
        }
    }
}

/// Parse a commit: the headers start with `tree`, the `parent`s, `author`
/// and `committer`, in the order `git fsck` checks.
///
/// # Errors
///
/// A header is missing or out of order, a signature does not parse, or no
/// blank line ends the headers.
pub(crate) fn parse_commit(data: &[u8]) -> Result<Commit<'_>> {
    use gix_object::commit::ref_iter::Token;

    let (mut tree, mut parents, mut author, mut committer) = (None, Vec::new(), None, None);
    // The iterator, not `CommitRef`, which also collects every extra header.
    for token in gix_object::CommitRefIter::from_bytes(data, HASH) {
        match token? {
            Token::Tree { id } => tree = Some(oid(&id)),
            Token::Parent { id } => parents.push(oid(&id)),
            Token::Author { signature } => author = Some(signature.into()),
            Token::Committer { signature } => committer = Some(signature.into()),
            Token::Message(message) => {
                return Ok(Commit {
                    tree: tree.context("commit without a tree")?,
                    parents,
                    author: author.context("commit without an author")?,
                    committer: committer.context("commit without a committer")?,
                    message,
                });
            }
            Token::Encoding(_) | Token::ExtraHeader(_) => {}
        }
    }
    bail!("commit without a message")
}

impl<'a> From<gix_actor::SignatureRef<'a>> for Signature<'a> {
    fn from(who: gix_actor::SignatureRef<'a>) -> Self {
        let who = who.trim();
        let time = who.time().unwrap_or_default();
        Signature {
            name: who.name,
            email: who.email,
            seconds: time.seconds,
            offset_seconds: time.offset,
        }
    }
}

/// A commit as git writes it, with a final newline after the message.
///
/// # Errors
///
/// A signature is not one git can read back; see [`Signature::unwritable`].
pub(crate) fn write_commit(
    tree: Oid,
    parents: &[Oid],
    author: &Signature<'_>,
    committer: &Signature<'_>,
    message: &[u8],
) -> Result<Vec<u8>> {
    let signature = |who: &Signature<'_>| gix_actor::Signature {
        name: who.name.into(),
        email: who.email.into(),
        time: gix_date::Time::new(who.seconds, who.offset_seconds),
    };
    let mut message = message.to_vec();
    if !message.ends_with(b"\n") {
        message.push(b'\n');
    }
    let commit = gix_object::Commit {
        tree: id(tree),
        parents: parents.iter().copied().map(id).collect(),
        author: signature(author),
        committer: signature(committer),
        encoding: None,
        message: message.into(),
        extra_headers: Vec::new(),
    };
    let mut out = Vec::new();
    commit.write_to(&mut out)?;
    Ok(out)
}

/// The mode of a tree entry, which says what the entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMode(pub u32);

impl EntryMode {
    pub(crate) const FILE: EntryMode = EntryMode(0o100_644);
    pub(crate) const EXECUTABLE: EntryMode = EntryMode(0o100_755);
    /// A symbolic link, whose blob is its target.
    pub(crate) const SYMLINK: EntryMode = EntryMode(0o120_000);
    /// A directory.
    pub(crate) const TREE: EntryMode = EntryMode(0o040_000);
    /// A commit in another repository.
    pub(crate) const GITLINK: EntryMode = EntryMode(0o160_000);
    const TYPE_BITS: u32 = 0o170_000;

    pub(crate) fn is_tree(self) -> bool {
        self.0 & Self::TYPE_BITS == Self::TREE.0
    }

    pub(crate) fn is_gitlink(self) -> bool {
        self.0 & Self::TYPE_BITS == Self::GITLINK.0
    }

    /// What an entry of this mode names in this repository; `None` for a
    /// gitlink.
    pub(crate) fn kind(self) -> Option<Kind> {
        match self.0 & Self::TYPE_BITS {
            bits if bits == Self::TREE.0 => Some(Kind::Tree),
            bits if bits == Self::GITLINK.0 => None,
            _ => Some(Kind::Blob),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeEntry<'a> {
    pub mode: EntryMode,
    pub name: &'a [u8],
    pub oid: Oid,
}

impl TreeEntry<'_> {
    /// What the entry names in this repository; `None` for a gitlink.
    pub(crate) fn kind(&self) -> Option<Kind> {
        self.mode.kind()
    }
}

/// The entries of a tree, in the order it stores them.
pub(crate) fn tree_entries(data: &[u8]) -> impl Iterator<Item = Result<TreeEntry<'_>>> {
    gix_object::TreeRefIter::from_bytes(data, HASH).map(|entry| {
        let entry = entry?;
        Ok(TreeEntry {
            mode: EntryMode(u32::from(entry.mode.value())),
            name: entry.filename,
            oid: oid(entry.oid),
        })
    })
}

/// The entries of a tree with the bytes each takes, in the order the tree
/// stores them.
pub(crate) fn tree_entries_at(
    data: &[u8],
) -> impl Iterator<Item = Result<(TreeEntry<'_>, std::ops::Range<usize>)>> {
    let mut start = 0;
    tree_entries(data).map(move |e| {
        let e = e?;
        // The name borrows from `data`; its id follows its NUL.
        let end = e.name.as_ptr().addr() - data.as_ptr().addr() + e.name.len() + 1 + OID_LEN;
        let at = start..end;
        start = end;
        Ok((e, at))
    })
}

/// The order of two tree entries in git's tree order.
pub(crate) fn tree_order(a: &TreeEntry<'_>, b: &TreeEntry<'_>) -> std::cmp::Ordering {
    gix_object::tree::name_order(a.name, a.mode.is_tree(), b.name, b.mode.is_tree())
}

/// A tree in git's order, where a directory sorts as `name/`.
///
/// # Errors
///
/// An entry has a mode git does not know, or a name with a NUL.
pub(crate) fn write_tree<'a>(entries: impl IntoIterator<Item = TreeEntry<'a>>) -> Result<Vec<u8>> {
    let mut tree = gix_object::Tree {
        entries: entries
            .into_iter()
            .map(|entry| {
                Ok(gix_object::tree::Entry {
                    mode: entry
                        .mode
                        .0
                        .try_into()
                        .map_err(|mode| anyhow::anyhow!("tree entry mode {mode:o}"))?,
                    filename: entry.name.into(),
                    oid: id(entry.oid),
                })
            })
            .collect::<Result<_>>()?,
    };
    tree.entries.sort();
    let mut out = Vec::new();
    tree.write_to(&mut out)?;
    Ok(out)
}

#[derive(Debug, Clone)]
pub(crate) struct TagInfo {
    pub target: Oid,
    pub target_kind: Kind,
}

pub(crate) fn parse_tag(data: &[u8]) -> Result<TagInfo> {
    let tag = gix_object::TagRef::from_bytes(data, HASH)?;
    Ok(TagInfo {
        target: oid(&tag.target()),
        target_kind: match tag.target_kind {
            gix_object::Kind::Commit => Kind::Commit,
            gix_object::Kind::Tree => Kind::Tree,
            gix_object::Kind::Blob => Kind::Blob,
            gix_object::Kind::Tag => Kind::Tag,
        },
    })
}

/// Enroute's ids are SHA-1.
const HASH: gix_hash::Kind = gix_hash::Kind::Sha1;

fn oid(id: &gix_hash::oid) -> Oid {
    Oid::from_slice(id.as_bytes())
}

fn id(oid: Oid) -> gix_hash::ObjectId {
    gix_hash::ObjectId::Sha1(*oid.as_bytes())
}

/// Objects an object refers to (kind and id).
pub(crate) type References = Vec<(Kind, Oid)>;

/// Objects directly referenced by an object (fsck-lite + connectivity),
/// and, for a commit, what it says.
pub(crate) fn references(kind: Kind, data: &[u8]) -> Result<(References, Option<Commit<'_>>)> {
    Ok((
        match kind {
            Kind::Commit => {
                let c = parse_commit(data)?;
                let mut v = vec![(Kind::Tree, c.tree)];
                v.extend(c.parents.iter().map(|&p| (Kind::Commit, p)));
                return Ok((v, Some(c)));
            }
            Kind::Tree => {
                let mut v = Vec::new();
                for e in tree_entries(data) {
                    let e = e?;
                    if e.name.is_empty() || e.name.contains(&b'/') {
                        bail!("invalid tree entry name");
                    }
                    if let Some(k) = e.kind() {
                        v.push((k, e.oid));
                    }
                }
                v
            }
            Kind::Tag => {
                let t = parse_tag(data)?;
                vec![(t.target_kind, t.target)]
            }
            Kind::Blob => Vec::new(),
        },
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::oid::hash_object;

    const TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbe4904d";
    const PARENT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn a_commit_reads_back_as_written() {
        let ada = Signature {
            name: b"Ada L",
            email: b"ada@example.com",
            seconds: 1_700_000_000,
            offset_seconds: -9000,
        };
        let tree = Oid::from_hex(TREE).unwrap();
        let parent = Oid::from_hex(PARENT).unwrap();
        let data = write_commit(tree, &[parent], &ada, &ada, b"subject\n\nbody").unwrap();
        assert!(
            data.ends_with(
                b"committer Ada L <ada@example.com> 1700000000 -0230\n\nsubject\n\nbody\n"
            )
        );
        let commit = parse_commit(&data).unwrap();
        assert_eq!((commit.tree, commit.parents), (tree, vec![parent]));
        assert_eq!((commit.author, commit.committer), (ada, ada));
        assert_eq!(commit.message, b"subject\n\nbody\n");
    }

    #[test]
    fn commit_headers_come_in_the_order_git_fsck_checks() {
        let author = "author A <a> 1 +0000\n";
        let committer = "committer C <c> 5 +0130\n";
        let ok = format!("tree {TREE}\nparent {PARENT}\n{author}{committer}gpgsig x\n y\n\nm\n");
        let commit = parse_commit(ok.as_bytes()).unwrap();
        assert_eq!(commit.committer.seconds, 5);
        assert_eq!(commit.committer.offset_seconds, 5400);
        assert_eq!(commit.message, b"m\n");
        for bad in [
            format!("{author}tree {TREE}\n{committer}\nm\n"),
            format!("tree {TREE}\n{committer}{author}\nm\n"),
            format!("tree {TREE}\n{author}parent {PARENT}\n{committer}\nm\n"),
            format!("tree {TREE}\n{committer}\nm\n"),
            format!("tree {TREE}\n{author}{committer}"),
            format!("tree {TREE}\n{author}committer C <c> soon\n\nm\n"),
        ] {
            parse_commit(bad.as_bytes()).unwrap_err();
        }
    }

    #[test]
    fn a_tree_sorts_a_directory_as_if_it_ended_in_a_slash() {
        let oid = hash_object(Kind::Blob, b"");
        let file = TreeEntry {
            mode: EntryMode::FILE,
            name: b"a.txt",
            oid,
        };
        let dir = TreeEntry {
            mode: EntryMode::TREE,
            name: b"a",
            oid,
        };
        let encoded = write_tree([dir, file]).unwrap();
        let read: Vec<TreeEntry<'_>> = tree_entries(&encoded).map(Result::unwrap).collect();
        assert_eq!(read, [file, dir]);
    }

    #[test]
    fn modes_say_what_an_entry_is() {
        assert!(EntryMode::TREE.is_tree() && !EntryMode::TREE.is_gitlink());
        assert!(EntryMode::GITLINK.is_gitlink() && !EntryMode::GITLINK.is_tree());
        assert!(!EntryMode::FILE.is_tree() && !EntryMode::SYMLINK.is_gitlink());
        assert_eq!(EntryMode::TREE.kind(), Some(Kind::Tree));
        assert_eq!(EntryMode::GITLINK.kind(), None);
        assert_eq!(EntryMode::EXECUTABLE.kind(), Some(Kind::Blob));
    }
}
