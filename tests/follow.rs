//! Subscriptions to repositories' logs (`Follow`): a follower that applies
//! the stream ends up with the ledger's state.

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::sync::Arc;

use common::{Git, Server, cluster, local_store, push_commits, wait_until};
use enroute_shared::proto::update;
use enroute_shared::roles::{self, Updates};
use enroute_shared::state::RepoMeta;
use futures::StreamExt;
use parking_lot::Mutex;

/// Apply a subscription to a state in the background.
fn apply(mut updates: Updates) -> Arc<Mutex<RepoMeta>> {
    let meta = Arc::new(Mutex::new(RepoMeta::default()));
    let m = meta.clone();
    tokio::spawn(async move {
        while let Some(u) = updates.next().await {
            match u.unwrap().kind.unwrap() {
                update::Kind::State(s) => *m.lock() = s,
                update::Kind::Change(c) => m.lock().apply_change(&c),
            }
        }
    });
    meta
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_tracks_the_log() {
    let server = Server::start().await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("r");
    push_commits(&git, &src, &url, 0, 2).await;

    let ledger = server.node.ledger().unwrap().clone();
    let meta = apply(roles::follow(&*ledger, "r").await.unwrap());
    push_commits(&git, &src, &url, 2, 3).await;
    let ours = || server.node.consensus().unwrap().repo_meta("r").unwrap();
    assert!(
        wait_until(|| *meta.lock() == ours(), 10).await,
        "{:?} vs {:?}",
        meta.lock().version,
        ours().version
    );
    assert_eq!(meta.lock().refs, ours().refs);

    // A read barrier reports the version a follower must reach.
    assert_eq!(
        roles::read_barrier(&*ledger, "r").await.unwrap(),
        ours().version
    );
}

/// Any ledger node hosting the log serves subscriptions, leader or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn followers_of_any_ledger_node_agree() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, _| {}).await;
    let git = Git::new();
    let src = git.init("src").await;
    push_commits(&git, &src, &servers[0].repo_url("r"), 0, 2).await;
    assert!(
        wait_until(
            || servers
                .iter()
                .all(|s| s.node.consensus().unwrap().repo_meta("r").is_some()),
            20
        )
        .await
    );

    let follows: Vec<_> = futures::future::join_all(
        servers
            .iter()
            .map(|s| roles::follow(&**s.node.ledger().unwrap(), "r")),
    )
    .await;
    let states: Vec<_> = follows.into_iter().map(|f| apply(f.unwrap())).collect();
    push_commits(&git, &src, &servers[1].repo_url("r"), 2, 3).await;
    let leader = servers
        .iter()
        .find(|s| {
            s.node
                .consensus()
                .unwrap()
                .hosted("r")
                .is_some_and(|h| h.leading)
        })
        .unwrap();
    // The leader's current state (a flush may still move it on).
    let target = || leader.node.consensus().unwrap().repo_meta("r").unwrap();
    assert!(wait_until(|| states.iter().all(|m| *m.lock() == target()), 10).await);
}
