//! Repository lifecycle: creating and destroying the thing every other
//! primitive operates on, with no name involved anywhere.
//!
//! A repository here is made by the contract and reached only by its id.

use crate::support::{Stack, git, git_allowing_failure};

use super::support::{PATH, contract_only, contract_with_repo, status_of};

/// A repository created through the contract is a working repository:
/// pushed into, cloned back, and never once named.
#[tokio::test]
async fn a_created_repository_takes_a_push() {
    let stack = Stack::start().await;
    let client = stack.client().await;

    let repo = client.create_repository("").await.unwrap();
    assert_eq!(repo.default_branch, "refs/heads/main");
    stack.repos.name(PATH, repo.repo.unwrap().key);

    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("work");
    std::fs::create_dir_all(&local).unwrap();
    let url = stack.url(PATH);
    git(&["init", "-b", "main"], Some(&local)).await;
    git(&["remote", "add", "origin", &url], Some(&local)).await;
    std::fs::write(local.join("a.txt"), "hello\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "first"], Some(&local)).await;
    git(&["push", "origin", "main"], Some(&local)).await;

    let back = tmp.path().join("back");
    git(&["clone", &url, back.to_str().unwrap()], Some(tmp.path())).await;
    assert_eq!(
        std::fs::read_to_string(back.join("a.txt")).unwrap(),
        "hello\n"
    );
}

/// The default branch is where `HEAD` points, so a repository created
/// with one has to advertise it before a single ref exists.
#[tokio::test]
async fn a_created_repository_keeps_its_default_branch() {
    let stack = Stack::start().await;
    let client = stack.client().await;

    let repo = client.create_repository("refs/heads/trunk").await.unwrap();
    assert_eq!(repo.default_branch, "refs/heads/trunk");
    stack.repos.name(PATH, repo.repo.unwrap().key);

    // Pushed to first, because a repository with no refs advertises none
    // — including `HEAD`, which is a symbolic ref to one of them.
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("work");
    std::fs::create_dir_all(&local).unwrap();
    let url = stack.url(PATH);
    git(&["init", "-b", "trunk"], Some(&local)).await;
    git(&["remote", "add", "origin", &url], Some(&local)).await;
    std::fs::write(local.join("a.txt"), "hello\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "first"], Some(&local)).await;
    git(&["push", "origin", "trunk"], Some(&local)).await;

    let advertised = git(&["ls-remote", "--symref", &url, "HEAD"], Some(&local)).await;
    assert!(
        advertised.contains("refs/heads/trunk"),
        "HEAD does not point at the requested default branch:\n{advertised}"
    );

    // What the default branch is actually for: the branch a fresh clone
    // lands on without being told.
    let back = tmp.path().join("back");
    git(&["clone", &url, back.to_str().unwrap()], Some(tmp.path())).await;
    let checked_out = git(&["rev-parse", "--abbrev-ref", "HEAD"], Some(&back)).await;
    assert_eq!(checked_out.trim(), "trunk");
}

/// A `default_branch` that is not a branch is refused rather than stored:
/// it would produce an advertisement no client could resolve.
#[tokio::test]
async fn a_default_branch_outside_refs_heads_is_refused() {
    let client = contract_only().await;

    client
        .create_repository("trunk")
        .await
        .expect_err("a bare branch name is not a refname");
}

/// Delete stops the id resolving, and a second delete of the same id is
/// not an error — a caller retrying wants to hear the repository is gone.
#[tokio::test]
async fn a_deleted_repository_stops_answering() {
    let client = contract_only().await;

    let repo = client.create_repository("").await.unwrap();
    let id = repo.repo.clone().unwrap().key;
    client.get_repository(&id).await.unwrap();

    client.delete_repository(&id).await.unwrap();

    let error = client
        .get_repository(&id)
        .await
        .expect_err("a deleted repository must not resolve");
    assert_eq!(status_of(&error).code(), tonic::Code::NotFound, "{error:?}");
    client
        .delete_repository(&id)
        .await
        .expect("deleting twice is not an error");
}

/// What a deleted repository looks like to a git client: not an empty
/// repository, which would invite a push into something already gone.
///
/// The stub application still names the key, so the refusal is the
/// deployment's own.
#[tokio::test]
async fn a_deleted_repository_refuses_a_client() {
    let stack = Stack::start().await;
    let client = stack.client().await;
    let repo = client.create_repository("").await.unwrap();
    let id = repo.repo.clone().unwrap().key;
    stack.repos.name(PATH, &id);
    let url = stack.url(PATH);
    let tmp = tempfile::tempdir().unwrap();

    client.delete_repository(&id).await.unwrap();

    let (ok, output) = git_allowing_failure(&["ls-remote", &url], tmp.path()).await;
    assert!(!ok, "a deleted repository still answered:\n{output}");
}

/// `last_push_unix_seconds` is absent until something is pushed, and set
/// afterwards — the field a repository listing is built from.
#[tokio::test]
async fn a_repository_reports_its_last_push() {
    let stack = Stack::start().await;
    let client = stack.client().await;
    let created = client.create_repository("").await.unwrap();
    let id = created.repo.clone().unwrap().key;
    assert_eq!(
        client.get_repository(&id).await.unwrap().last_push,
        None,
        "an empty repository has never been pushed to"
    );
    stack.repos.name(PATH, &id);

    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("work");
    std::fs::create_dir_all(&local).unwrap();
    let url = stack.url(PATH);
    git(&["init", "-b", "main"], Some(&local)).await;
    git(&["remote", "add", "origin", &url], Some(&local)).await;
    std::fs::write(local.join("a.txt"), "body\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "first"], Some(&local)).await;
    git(&["push", "origin", "main"], Some(&local)).await;

    assert!(
        client
            .get_repository(&id)
            .await
            .unwrap()
            .last_push
            .is_some(),
        "a pushed repository still reports no push"
    );
}

/// An id no repository has is a `NotFound`, not an error about the id.
///
/// A caller holding a stale id asked a well-formed question, and the answer
/// to it is simply no.
#[tokio::test]
async fn an_unknown_repository_is_not_found() {
    let client = contract_only().await;

    let error = client
        .get_repository("999999999")
        .await
        .expect_err("no repository has that id");
    assert_eq!(status_of(&error).code(), tonic::Code::NotFound);
}

/// A key nobody holds is `NotFound`, whatever it looks like.
///
/// A key an application picked is not ours to judge the shape of, so anything
/// the rules allow gets the same answer as a stale one: no.
#[tokio::test]
async fn a_key_nobody_holds_is_not_found() {
    let client = contract_only().await;

    for key in ["not-mine", "12x", "1", "Mine", "repo-none"] {
        let error = client
            .get_repository(key)
            .await
            .expect_err("a key nobody holds names no repository");
        assert_eq!(
            status_of(&error).code(),
            tonic::Code::NotFound,
            "wrong status for the key {key:?}"
        );
    }
}

/// And deleting one that never existed answers alike.
///
/// Refusing would say the id is somebody's, which is exactly what a caller
/// probing for ids wants to learn.
#[tokio::test]
async fn deleting_an_unknown_repository_is_not_an_error() {
    let client = contract_only().await;

    client
        .delete_repository("999999999")
        .await
        .expect("an id nobody holds deletes nothing and says nothing");
}

/// A repository is created with `refs/heads/main` when nothing is asked for.
#[tokio::test]
async fn a_default_branch_defaults_to_main() {
    let (client, id) = contract_with_repo().await;

    let repo = client.get_repository(&id).await.unwrap();
    assert_eq!(repo.default_branch, "refs/heads/main");
    assert_eq!(
        repo.repo.expect("a repository has a key").key,
        id,
        "get answered about a different repository than it was asked about"
    );
}

/// A listing walks every repository the caller has, a page at a time, and
/// reports each one once.
///
/// Created out of key order, since the order a listing promises is the keys'
/// and not the order they were made in.
#[tokio::test]
async fn a_listing_pages_over_every_repository() {
    let client = contract_only().await;
    for key in ["repo-3", "repo-5", "repo-1", "repo-4", "repo-2"] {
        client.create_repository_as(key, "").await.unwrap();
    }

    let mut walked = Vec::new();
    let mut token = String::new();
    let mut pages = 0;
    loop {
        let (page, next) = client.list_repositories(2, &token).await.unwrap();
        pages += 1;
        walked.extend(page.iter().map(|one| one.repo.clone().unwrap().key));
        if next.is_empty() {
            break;
        }
        token = next;
        assert!(pages < 10, "a walk that will not end");
    }

    assert_eq!(
        walked,
        ["repo-1", "repo-2", "repo-3", "repo-4", "repo-5"],
        "key order, each exactly once"
    );
    assert_eq!(pages, 3, "five over pages of two");
}

/// A token the server never minted is refused, rather than read as whatever
/// position it happens to parse as.
#[tokio::test]
async fn a_page_token_that_is_not_one_is_refused() {
    let client = contract_only().await;

    let refused = client
        .list_repositories(0, "not a token")
        .await
        .expect_err("a token nothing minted");
    assert_eq!(status_of(&refused).code(), tonic::Code::InvalidArgument);
}

/// A listing carries the same last-push time a `GetRepository` reports, so a
/// caller building a listing needs no call per repository.
#[tokio::test]
async fn a_listing_reports_the_last_push() {
    let (client, key) = contract_with_repo().await;

    let (listed, _) = client.list_repositories(0, "").await.unwrap();
    let one = listed.first().expect("the repository just created");
    assert_eq!(one.repo.clone().unwrap().key, key);
    assert!(one.last_push.is_none(), "nothing has been pushed to it yet");
    assert_eq!(one.default_branch, "refs/heads/main");
}

/// Two creates are two repositories, not one shared by whoever asked first.
#[tokio::test]
async fn every_create_is_its_own_repository() {
    let client = contract_only().await;

    let one = client.create_repository("").await.unwrap();
    let two = client.create_repository("").await.unwrap();
    assert_ne!(
        one.repo.expect("a key").key,
        two.repo.expect("a key").key,
        "two creates handed back one repository"
    );
}
