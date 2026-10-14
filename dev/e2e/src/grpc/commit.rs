//! Making a commit on the server from file changes, with no git client.
//!
//! The commit has to be one real git takes: a clone of it passes
//! `git fsck --strict`.

use enroute_api::api::v1alpha1::expected_old_commit::{Absent, Expectation};
use enroute_api::api::v1alpha1::{
    CreateCommitRequest, DeleteFile, ExpectedOldCommit, FileMode, Identity, PathChange, PutFile,
    path_change,
};

use crate::contract::{hex, oid, repo_key};
use crate::support::{Stack, git};

use super::support::{absent_oid, contract_only, contract_with_repo, status_of};

const MAIN: &str = "refs/heads/main";

fn ada() -> Identity {
    Identity {
        name: "Ada Lovelace".into(),
        email: "ada@example.com".into(),
        timestamp: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        utc_offset_seconds: 3600,
    }
}

fn put_as(path: &str, content: &[u8], mode: FileMode) -> PathChange {
    PathChange {
        path: path.into(),
        change: Some(path_change::Change::Put(PutFile {
            content: content.to_vec().into(),
            mode: mode.into(),
        })),
    }
}

fn put(path: &str, content: &[u8]) -> PathChange {
    put_as(path, content, FileMode::Unspecified)
}

fn delete(path: &str) -> PathChange {
    PathChange {
        path: path.into(),
        change: Some(path_change::Change::Delete(DeleteFile {})),
    }
}

/// A commit of `changes` on `main`, waiving the check.
fn commit(repo: &str, message: &str, changes: Vec<PathChange>) -> CreateCommitRequest {
    CreateCommitRequest {
        repo: Some(repo_key(repo)),
        branch: MAIN.into(),
        expected_old_commit: None,
        message: message.into(),
        author: Some(ada()),
        committer: None,
        changes,
    }
}

fn at(commit_id: &str) -> ExpectedOldCommit {
    ExpectedOldCommit {
        expectation: Some(Expectation::CommitId(oid(commit_id).expect("a commit id"))),
    }
}

fn absent() -> ExpectedOldCommit {
    ExpectedOldCommit {
        expectation: Some(Expectation::Absent(Absent {})),
    }
}

/// Where `main` points, if anywhere.
async fn main_tip(client: &crate::contract::Client, repo: &str) -> Option<String> {
    let refs = client.list_refs(repo).await.unwrap();
    refs.refs
        .iter()
        .find(|r| r.name == MAIN)
        .map(|r| hex(r.object_id.as_ref()))
}

/// The first commit starts the branch, and the next read sees it.
#[tokio::test]
async fn a_commit_starts_a_branch_and_a_read_sees_it() {
    let (client, repo) = contract_with_repo().await;

    let created = client
        .create_commit(commit(
            &repo,
            "first\n\nThe body.",
            vec![put("README.md", b"hi\n")],
        ))
        .await
        .unwrap();

    assert!(created.parent_commit_id.is_none());
    let id = hex(created.commit_id.as_ref());
    assert_eq!(main_tip(&client, &repo).await.as_deref(), Some(id.as_str()));
    let (commits, _) = client.list_commits(&repo, &id, 0, "").await.unwrap();
    let only = commits.first().expect("the commit");
    assert_eq!(hex(only.tree_id.as_ref()), hex(created.tree_id.as_ref()));
    assert_eq!(only.summary, "first");
    assert_eq!(only.body, "The body.\n");
    let author = only.author.clone().expect("an author");
    assert_eq!(
        (author.name.as_str(), author.utc_offset_seconds),
        ("Ada Lovelace", 3600)
    );
    // The committer is the author when none is named.
    assert_eq!(only.committer, Some(author));
}

/// `expected_old_commit` is a compare-and-swap on the branch.
#[tokio::test]
async fn the_branch_moves_only_from_where_the_caller_expected() {
    let (client, repo) = contract_with_repo().await;

    let first = client
        .create_commit(CreateCommitRequest {
            expected_old_commit: Some(absent()),
            ..commit(&repo, "one", vec![put("f", b"1\n")])
        })
        .await
        .unwrap();
    let first = hex(first.commit_id.as_ref());

    let second = client
        .create_commit(CreateCommitRequest {
            expected_old_commit: Some(at(&first)),
            ..commit(&repo, "two", vec![put("f", b"2\n")])
        })
        .await
        .unwrap();
    assert_eq!(hex(second.parent_commit_id.as_ref()), first);
    let second = hex(second.commit_id.as_ref());

    for (expected, why) in [
        (at(&first), "a stale commit"),
        (absent(), "a branch that exists"),
        (at(&absent_oid()), "a commit the branch never held"),
    ] {
        let refused = client
            .create_commit(CreateCommitRequest {
                expected_old_commit: Some(expected),
                ..commit(&repo, "three", vec![put("f", b"3\n")])
            })
            .await
            .unwrap_err();
        assert_eq!(
            status_of(&refused).code(),
            tonic::Code::FailedPrecondition,
            "{why}: {refused:#}"
        );
    }
    assert_eq!(
        main_tip(&client, &repo).await.as_deref(),
        Some(second.as_str())
    );

    // A branch that does not exist matches no commit.
    let refused = client
        .create_commit(CreateCommitRequest {
            branch: "refs/heads/other".into(),
            expected_old_commit: Some(at(&second)),
            ..commit(&repo, "elsewhere", vec![put("f", b"4\n")])
        })
        .await
        .unwrap_err();
    assert_eq!(status_of(&refused).code(), tonic::Code::FailedPrecondition);
}

/// A delete takes the file, and the directories it leaves empty.
#[tokio::test]
async fn a_delete_removes_the_file_and_what_it_leaves_empty() {
    let (client, repo) = contract_with_repo().await;
    client
        .create_commit(commit(
            &repo,
            "files",
            vec![put("a/b/c.txt", b"c\n"), put("keep.txt", b"k\n")],
        ))
        .await
        .unwrap();

    let deleted = client
        .create_commit(commit(
            &repo,
            "delete",
            vec![delete("a/b/c.txt"), delete("never/there.txt")],
        ))
        .await
        .unwrap();

    let (entries, _) = client
        .list_tree(&repo, &hex(deleted.commit_id.as_ref()))
        .await
        .unwrap();
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, ["keep.txt"]);
    // A directory is not a file to delete.
    client
        .create_commit(commit(&repo, "dir", vec![put("d/x", b"x\n")]))
        .await
        .unwrap();
    let refused = client
        .create_commit(commit(&repo, "rm -r", vec![delete("d")]))
        .await
        .unwrap_err();
    assert_eq!(status_of(&refused).code(), tonic::Code::InvalidArgument);
}

/// Every mode a file can have, cloned by real git and checked strictly.
#[tokio::test]
async fn the_commit_clones_with_git_and_passes_fsck() {
    let stack = Stack::start().await;
    let client = stack.client().await;
    let repo = stack.create("committed").await;

    let created = client
        .create_commit(commit(
            &repo,
            "every mode",
            vec![
                put_as("bin/run.sh", b"#!/bin/sh\necho hi\n", FileMode::Executable),
                put_as("link", b"docs/target.txt", FileMode::Symlink),
                put_as("docs/target.txt", b"target\n", FileMode::Regular),
                put("plain.txt", b"plain\n"),
            ],
        ))
        .await
        .unwrap();
    // A file over the inline limit, so its layer goes to the object store.
    let mut x = 0x9e37_79b9_7f4a_7c15_u64;
    let large: Vec<u8> = std::iter::repeat_with(|| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x.to_le_bytes()
    })
    .take(5 << 17)
    .flatten()
    .collect();
    client
        .create_commit(CreateCommitRequest {
            expected_old_commit: Some(at(&hex(created.commit_id.as_ref()))),
            ..commit(&repo, "large", vec![put("large.bin", &large)])
        })
        .await
        .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("work");
    git(
        &[
            "clone",
            "-q",
            &stack.url("committed"),
            work.to_str().unwrap(),
        ],
        None,
    )
    .await;
    git(&["fsck", "--strict"], Some(&work)).await;
    let tree = git(&["ls-tree", "-r", "HEAD"], Some(&work)).await;
    assert!(tree.contains("100755 blob"), "{tree}");
    assert!(tree.contains("120000 blob"), "{tree}");
    assert_eq!(
        std::fs::read_link(work.join("link")).unwrap(),
        std::path::Path::new("docs/target.txt")
    );
    assert_eq!(std::fs::read(work.join("large.bin")).unwrap(), large);
    let log = git(
        &["log", "--format=%an <%ae> %at %ad|%s", "--date=format:%z"],
        Some(&work),
    )
    .await;
    assert!(
        log.contains("Ada Lovelace <ada@example.com> 1700000000 +0100|large"),
        "{log}"
    );
}

#[tokio::test]
async fn an_unknown_repository_is_not_found() {
    let client = contract_only().await;
    let refused = client
        .create_commit(commit("no-such-repo", "x", vec![put("f", b"x")]))
        .await
        .unwrap_err();
    assert_eq!(status_of(&refused).code(), tonic::Code::NotFound);
}

/// A path git would not check out, or a commit git would not parse, is the
/// caller's to fix, and nothing lands.
#[tokio::test]
async fn what_git_would_refuse_is_an_invalid_argument() {
    let (client, repo) = contract_with_repo().await;
    client
        .create_commit(commit(&repo, "base", vec![put("dir/f", b"f\n")]))
        .await
        .unwrap();
    let tip = main_tip(&client, &repo).await;

    let mut refused = Vec::new();
    for path in [
        "",
        "..",
        "../a",
        "a/../b",
        "/etc/passwd",
        "a/",
        "a//b",
        "./a",
        ".git/config",
        "a/.GIT/b",
        "a\\b",
    ] {
        refused.push((
            path.to_string(),
            commit(&repo, "bad", vec![put(path, b"x")]),
        ));
    }
    let named = |what: &str, request| (what.to_string(), request);
    refused.extend([
        named("no changes", commit(&repo, "bad", Vec::new())),
        named(
            "a short branch name",
            CreateCommitRequest {
                branch: "main".into(),
                ..commit(&repo, "bad", vec![put("f", b"x")])
            },
        ),
        named(
            "no author",
            CreateCommitRequest {
                author: None,
                ..commit(&repo, "bad", vec![put("f", b"x")])
            },
        ),
        named(
            "an author with a newline",
            CreateCommitRequest {
                author: Some(Identity {
                    name: "Ada\n".into(),
                    ..ada()
                }),
                ..commit(&repo, "bad", vec![put("f", b"x")])
            },
        ),
        named(
            "a file over a directory",
            commit(&repo, "bad", vec![put("dir", b"x")]),
        ),
        named(
            "a path through a file",
            commit(&repo, "bad", vec![put("dir/f/g", b"x")]),
        ),
        named(
            "one path twice",
            commit(&repo, "bad", vec![put("f", b"x"), delete("f")]),
        ),
        named(
            "a change of neither kind",
            commit(
                &repo,
                "bad",
                vec![PathChange {
                    path: "f".into(),
                    change: None,
                }],
            ),
        ),
    ]);

    for (what, request) in refused {
        let error = client.create_commit(request).await.unwrap_err();
        assert_eq!(
            status_of(&error).code(),
            tonic::Code::InvalidArgument,
            "{what:?}: {error:#}"
        );
    }
    assert_eq!(main_tip(&client, &repo).await, tip, "something landed");
}

/// The cap is checked before the request is read.
#[tokio::test]
async fn a_request_over_the_cap_is_refused() {
    let (client, repo) = contract_with_repo().await;
    let over = vec![0u8; 33 << 20];
    let refused = client
        .create_commit(commit(&repo, "big", vec![put("big.bin", &over)]))
        .await
        .unwrap_err();
    assert_eq!(status_of(&refused).code(), tonic::Code::OutOfRange);
    assert_eq!(main_tip(&client, &repo).await, None);
}
