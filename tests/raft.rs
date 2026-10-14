//! Replication: failover, catch-up by snapshot, quiet idle logs and
//! membership.

mod common;

use std::time::Duration;

use common::{
    Git, Server, clone_matches, cluster, commit, local_store, put, repo_status, text, wait_until,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replicated_push_and_failover() {
    let root = tempfile::tempdir().unwrap();
    let mut servers: Vec<Option<Server>> =
        cluster(root.path(), &local_store(root.path()), 3, |_, _| {})
            .await
            .into_iter()
            .map(Some)
            .collect();
    let git = Git::new();
    let src = git.init("src").await;
    for i in 0..4 {
        git.commit_file(&src, &format!("f{i}"), &text(i, 50), &format!("c{i}"))
            .await;
    }
    // Push via node 1, clone from nodes 2 and 3.
    let url1 = servers[0].as_ref().unwrap().repo_url("rep");
    git.run(&src, &["push", "-q", &url1, "main"]).await;
    for (k, server) in servers.iter().enumerate().skip(1) {
        clone_matches(
            &git,
            &src,
            &server.as_ref().unwrap().repo_url("rep"),
            &format!("dst{k}"),
        )
        .await;
    }

    // Kill the leader of the repo's group; the rest elect a new one and keep
    // accepting pushes.
    let leader = servers
        .iter()
        .position(|s| {
            s.as_ref().is_some_and(|s| {
                s.node
                    .consensus()
                    .unwrap()
                    .hosted("rep")
                    .is_some_and(|h| h.leading)
            })
        })
        .unwrap();
    let dead = servers[leader].take().unwrap();
    dead.node.shutdown().await;
    drop(dead);
    let alive: Vec<usize> = (0..3).filter(|i| servers[*i].is_some()).collect();
    let new_leader = || {
        alive.iter().any(|i| {
            servers[*i]
                .as_ref()
                .unwrap()
                .node
                .consensus()
                .unwrap()
                .hosted("rep")
                .is_some_and(|h| h.leading)
        })
    };
    assert!(
        wait_until(new_leader, 10).await,
        "no new leader after failover"
    );

    git.commit_file(&src, "after", "failover\n", "after failover")
        .await;
    let url_a = servers[alive[0]].as_ref().unwrap().repo_url("rep");
    git.run(&src, &["push", "-q", &url_a, "main"]).await;
    clone_matches(
        &git,
        &src,
        &servers[alive[1]].as_ref().unwrap().repo_url("rep"),
        "after",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lagging_follower_catches_up_via_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let configure = |_: usize, c: &mut enroute::node::Config| {
        c.compute.merge_factor = 0;
        c.ledger.snapshot_every = 5;
    };
    let mut servers: Vec<Option<Server>> =
        cluster(root.path(), &local_store(root.path()), 3, configure)
            .await
            .into_iter()
            .map(Some)
            .collect();

    // Take down a node other than the one that will create the repository.
    let follower = 2;
    let down = servers[follower].take().unwrap();
    let addr = down.internal;
    down.node.shutdown().await;
    drop(down);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Many writes while it is down: the log gets snapshotted and truncated.
    let git = Git::new();
    let src = git.init("src").await;
    let live = (0..3).find(|i| servers[*i].is_some()).unwrap();
    let url = servers[live].as_ref().unwrap().repo_url("lag");
    for i in 0..12 {
        git.commit_file(&src, "f", &text(i, 20), &format!("c{i}"))
            .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
    }
    // Truncated once the pushes' inline layers are flushed and that is
    // durable (the apply thread reports it every SYNC_EVERY), on both live
    // nodes (either may lead).
    common::wait_stored(servers[live].as_ref().unwrap(), "lag").await;
    let truncated = || {
        servers
            .iter()
            .flatten()
            .all(|s| s.node.consensus().unwrap().stats().compactions > 0)
    };
    assert!(wait_until(truncated, 20).await, "the log was not truncated");

    // Restart the follower on its old data directory (same port).
    let mut config = enroute::node::Config::local(
        root.path().join(format!("n{follower}")),
        local_store(root.path()),
    );
    config.ledger.advertise = Some(format!("http://{addr}"));
    configure(follower, &mut config);
    servers[follower] = Some(Server::start_on(config, common::rebind(addr).await).await);
    let s = servers[follower].as_ref().unwrap();
    let want = git.rev(&src, "main").await;
    // It joins the live incarnation when asked for the repository, and
    // catches up (via a snapshot: the log was truncated meanwhile).
    git.run(&src, &["ls-remote", &s.repo_url("lag")]).await;
    let got = || {
        s.node.consensus().unwrap().repo_meta("lag").and_then(|m| {
            m.refs
                .get("refs/heads/main")
                .map(enroute_shared::oid::Oid::to_hex)
        })
    };
    assert!(
        wait_until(|| got().as_ref() == Some(&want), 10).await,
        "follower did not catch up"
    );
    // (Its statistics are published at the end of the round that installed it.)
    assert!(
        wait_until(
            || s.node.consensus().unwrap().stats().snapshots_installed > 0,
            5
        )
        .await,
        "it caught up from a snapshot"
    );
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &s.repo_url("lag"), dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&dst, "HEAD").await, want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn idle_repos_go_quiet() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, _| {}).await;
    let s0 = &servers[0];
    // Many repositories, each with its own log, created and written once.
    let n = 200;
    let mut client = s0.api_client();
    for i in 0..n {
        client.create_repo(&format!("idle{i}")).await.unwrap();
        client
            .create_commit(commit(
                &format!("idle{i}"),
                "main",
                "init",
                vec![put("f", "x")],
            ))
            .await
            .unwrap();
    }
    let sent = || -> u64 {
        servers
            .iter()
            .map(|s| s.node.consensus().unwrap().stats().messages_sent)
            .sum()
    };
    let active = || -> usize {
        servers
            .iter()
            .map(|s| s.node.consensus().unwrap().stats().active_logs)
            .sum()
    };
    // Let everything go quiet (leaders stop heartbeating after their
    // activity window).
    assert!(wait_until(|| active() == 0, 20).await, "logs still ticking");
    let before = sent();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let idle_traffic = sent() - before;
    assert_eq!(
        idle_traffic, 0,
        "{n} idle repos sent {idle_traffic} messages in 3s"
    );

    // A quiet repo still works on demand, through any node.
    let s2 = &servers[2];
    s2.api_client()
        .create_commit(commit("idle7", "main", "wake", vec![put("f", "y")]))
        .await
        .unwrap();
    let git = Git::new();
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            &servers[1].repo_url("idle7"),
            dst.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(std::fs::read_to_string(dst.join("f")).unwrap(), "y");
}

/// Every member votes in every repository's log, as members join and leave.
///
/// A node joins each log as a learner, then a voter; a removed member leaves
/// each log. The repositories take writes throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn members_are_every_logs_voters() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let mut servers = cluster(root.path(), &store, 3, |_, _| {}).await;
    let n = 10;
    for i in 0..n {
        let mut api = servers[i % servers.len()].api_client();
        api.create_repo(&format!("r{i}")).await.unwrap();
        api.create_commit(commit(
            &format!("r{i}"),
            "main",
            "init",
            vec![put("f", "x")],
        ))
        .await
        .unwrap();
    }
    let ids = |servers: &[Server]| -> Vec<u64> {
        let mut ids: Vec<u64> = servers
            .iter()
            .map(|s| s.node.consensus().unwrap().id)
            .collect();
        ids.sort_unstable();
        ids
    };
    // Every node hosts every log, with every member as a voter.
    let voters_are = |servers: &[Server], want: &[u64]| {
        (0..n).all(|i| {
            servers
                .iter()
                .all(|s| common::voters(s, &format!("r{i}")) == want)
        })
    };
    let want = ids(&servers);
    assert!(
        wait_until(|| voters_are(&servers, &want), 10).await,
        "every member votes"
    );

    // A fourth node joins: every log adds it.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut c = enroute::node::Config::local(root.path().join("n3"), store.clone());
    c.ledger.advertise = Some(format!("http://{}", listener.local_addr().unwrap()));
    servers.push(Server::start_on(c, listener).await);
    let want = ids(&servers);
    assert!(
        wait_until(|| voters_are(&servers, &want), 30).await,
        "the new member was not added everywhere"
    );

    // Node 0 is removed: every log drops it.
    let gone = servers[0].node.consensus().unwrap().id;
    enroute_shared::registry::remove_member(&servers[1].node.ledger().unwrap().objects, gone)
        .await
        .unwrap();
    let rest = &servers[1..];
    let want: Vec<u64> = want.into_iter().filter(|&id| id != gone).collect();
    assert!(
        wait_until(|| voters_are(rest, &want), 30).await,
        "the removed member is still a voter"
    );
    // It drops its logs, even those whose removal it never saw.
    assert!(
        wait_until(
            || servers[0].node.consensus().unwrap().repos().is_empty(),
            10
        )
        .await,
        "the removed node kept logs"
    );

    // Writes through the remaining nodes, read through the new one.
    servers[1]
        .api_client()
        .create_commit(commit("r0", "main", "after", vec![put("f", "y")]))
        .await
        .unwrap();
    let git = Git::new();
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            &servers[3].repo_url("r0"),
            dst.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(std::fs::read_to_string(dst.join("f")).unwrap(), "y");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_node_names_the_leader() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, _| {}).await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "x\n", "c").await;
    git.run(&src, &["push", "-q", &servers[0].repo_url("rep"), "main"])
        .await;

    let leader = servers
        .iter()
        .find(|s| {
            s.node
                .consensus()
                .unwrap()
                .hosted("rep")
                .is_some_and(|h| h.leading)
        })
        .map(|s| s.node.consensus().unwrap().status()["node"].clone())
        .unwrap();
    // Followers learn the leader from its first message.
    let mut agree = false;
    for _ in 0..100 {
        let mut named = Vec::new();
        for s in &servers {
            named.push(
                repo_status(s, "rep")
                    .await
                    .map(|v| (v["leader"].clone(), v["term"].clone())),
            );
        }
        if named
            .iter()
            .all(|n| n.as_ref().is_some_and(|(l, _)| *l == leader))
            && named.windows(2).all(|w| w[0] == w[1])
        {
            agree = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(agree, "the nodes do not all name {leader} at one term");
    assert_eq!(repo_status(&servers[0], "missing").await, None);
}
