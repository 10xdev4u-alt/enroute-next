//! A ledger node's lifetime: moving one by its disk (freezing and
//! restoring the ledger), and a node losing its disk.

mod common;

use std::time::Duration;

use common::{
    Git, Server, clone_matches, cluster, copy_dir, local_store, push_commits, wait_until,
};
use enroute::node::Config;

fn config(data: &std::path::Path, store: &enroute::node::Store) -> Config {
    let mut c = Config::local(data.to_path_buf(), store.clone());
    c.compute.flush_interval = Duration::from_millis(200);
    c
}

/// A stopped node's data directory, copied elsewhere, is the same node.
///
/// Its identity is its log store. Every repository comes back with its
/// history, on the same voters, and takes writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_moves_with_its_disk() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let git = Git::new();
    let mut srcs = Vec::new();
    let id = {
        let a = Server::start_config(config(&root.path().join("a"), &store)).await;
        for r in 0..3 {
            let src = git.init(&format!("src{r}")).await;
            push_commits(&git, &src, &a.repo_url(&format!("r{r}")), r * 10, 3).await;
            srcs.push(src);
        }
        let id = a.node.consensus().unwrap().id;
        a.node.shutdown().await;
        id
    };
    copy_dir(&root.path().join("a"), &root.path().join("b"));

    let b = Server::start_config(config(&root.path().join("b"), &store)).await;
    assert_eq!(
        b.node.consensus().unwrap().id,
        id,
        "the copy is the same node"
    );
    for (r, src) in srcs.iter().enumerate() {
        clone_matches(&git, src, &b.repo_url(&format!("r{r}")), &format!("b{r}")).await;
    }
    push_commits(&git, &srcs[1], &b.repo_url("r1"), 100, 2).await;
    clone_matches(&git, &srcs[1], &b.repo_url("r1"), "b1-after").await;
    assert_eq!(
        b.node.consensus().unwrap().hosted("r1").map(|h| h.epoch),
        Some(1),
        "the same incarnation"
    );
}

/// A node whose disk is lost comes back as a new node, never as the old one.
///
/// It joins the repository's log as a new member; once the operator removes
/// the old node, the log has three voters again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_node_that_lost_its_disk_is_replaced() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let mut servers = cluster(root.path(), &store, 3, |_, c| {
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    push_commits(&git, &src, &servers[0].repo_url("r"), 0, 3).await;
    assert!(
        wait_until(
            || servers
                .iter()
                .all(|s| s.node.consensus().unwrap().hosted("r").is_some()),
            20
        )
        .await,
        "replicas joined"
    );
    let old = servers[2].node.consensus().unwrap().id;

    // Node 2 loses its disk and comes back at the same address.
    let addr = servers.pop().unwrap().internal;
    let mut c = config(&root.path().join("n2-new"), &store);
    c.ledger.advertise = Some(format!("http://{addr}"));
    servers.push(Server::start_on(c, common::rebind(addr).await).await);
    let fresh = servers[2].node.consensus().unwrap().id;
    assert_ne!(fresh, old, "a new store is a new node");
    let voters = |s: &Server| common::voters(s, "r");
    let mut four = vec![
        servers[0].node.consensus().unwrap().id,
        servers[1].node.consensus().unwrap().id,
        old,
        fresh,
    ];
    four.sort_unstable();
    assert!(
        wait_until(|| voters(&servers[0]) == four, 20).await,
        "the new node is a voter: {:?}",
        voters(&servers[0])
    );

    // The operator removes the old node.
    let objects = &servers[0].node.ledger().unwrap().objects;
    enroute_shared::registry::check_removal(objects, old)
        .await
        .unwrap();
    enroute_shared::registry::remove_member(objects, old)
        .await
        .unwrap();
    let three: Vec<u64> = four.into_iter().filter(|&id| id != old).collect();
    assert!(
        wait_until(|| servers.iter().all(|s| voters(s) == three), 20).await,
        "the old node is not a voter"
    );

    push_commits(&git, &src, &servers[1].repo_url("r"), 3, 2).await;
    clone_matches(&git, &src, &servers[2].repo_url("r"), "a").await;
}

/// Checkpoints follow changes, not time.
///
/// A change reaches the state object once its log goes quiet, long before
/// `checkpoint_age`; an idle repository's state object is not written again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_are_checkpointed_when_the_log_goes_quiet() {
    let root = tempfile::tempdir().unwrap();
    let mut c = config(&root.path().join("a"), &local_store(root.path()));
    c.ledger.checkpoint_age = Duration::from_secs(3600);
    let server = Server::start_config(c).await;
    let mut api = server.api_client();
    api.create_repo("r").await.unwrap();
    api.create_commit(common::commit(
        "r",
        "main",
        "c0",
        vec![common::put("f", "x")],
    ))
    .await
    .unwrap();
    let path = root.path().join("store/repos/r/state");
    let checkpointed = || {
        enroute_shared::repo_state::Record::from_bytes(&std::fs::read(&path).unwrap())
            .unwrap()
            .meta()
            .version
    };
    let current = || {
        server
            .node
            .consensus()
            .unwrap()
            .repo_meta("r")
            .unwrap()
            .version
    };
    assert!(
        checkpointed() < current(),
        "checkpointed before going quiet"
    );
    // (The compute flushes the commit's inline layer meanwhile: a change too.)
    assert!(
        wait_until(|| checkpointed() == current(), 20).await,
        "not checkpointed once quiet: {} < {}",
        checkpointed(),
        current()
    );
    let written = std::fs::metadata(&path).unwrap().modified().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        written,
        "an idle repository was checkpointed again"
    );
}
