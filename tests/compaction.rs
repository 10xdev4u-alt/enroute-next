//! Compaction: merged layers stay correct, and a replaced layer outlives
//! the checkpoint that names it.

#![expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::time::Duration;

use common::{Git, Server, text};
use enroute_compute::testing::settled;

/// Compaction merges flushed layers, and every kind of fetch stays correct
/// on them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_layers_stay_correct() {
    let server = Server::start_with(|c| {
        c.compute.merge_factor = 2;
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("part");
    for i in 0..30 {
        git.commit_file(
            &src,
            &format!("d{}/f{}.txt", i % 4, i % 7),
            &text(i, 40 + i as usize),
            &format!("c{i}"),
        )
        .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
        if i % 10 == 4 {
            git.run(&src, &["checkout", "-q", "-b", &format!("b{i}")])
                .await;
            git.commit_file(&src, "branch.txt", &text(1000 + i, 20), "branch")
                .await;
            git.run(&src, &["push", "-q", &url, &format!("b{i}")]).await;
            git.run(&src, &["checkout", "-q", "main"]).await;
        }
        if i % 10 == 9 {
            git.run(&src, &["tag", "-a", &format!("t{i}"), "-m", "t"])
                .await;
            git.run(&src, &["push", "-q", &url, &format!("t{i}")]).await;
        }
    }
    // Wait until everything is flushed and compaction has settled.
    // The layers are all in the smallest size class, so they merge into
    // fewer than `merge_factor`.
    let done = || {
        let meta = server.node.consensus().unwrap().repo_meta("part").unwrap();
        settled(&meta, server.node.compute().unwrap().config()) && meta.layers.len() == 1
    };
    assert!(
        common::wait_until(done, 20).await,
        "expected the layers merged into one"
    );

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", "--mirror", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "main").await);
    assert_eq!(git.rev(&src, "t29").await, git.rev(&dst, "t29").await);
    for i in [4, 14, 24] {
        assert_eq!(
            git.rev(&src, &format!("b{i}")).await,
            git.rev(&dst, &format!("b{i}")).await
        );
    }
    // Incremental fetch on top of partitioned storage (thin pack bases).
    let work = git.dir("work");
    git.run(
        git.home.path(),
        &["clone", "-q", "-b", "main", &url, work.to_str().unwrap()],
    )
    .await;
    git.commit_file(&src, "d0/f0.txt", &text(99, 60), "after")
        .await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    git.run(&work, &["pull", "-q", "--ff-only"]).await;
    git.run(&work, &["fsck", "--strict"]).await;
    // Shallow and blobless clones too.
    let sh = git.dir("sh");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=1", &url, sh.to_str().unwrap()],
    )
    .await;
    git.run(&sh, &["fsck"]).await;
    let pc = git.dir("pc");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            "--filter=blob:none",
            &url,
            pc.to_str().unwrap(),
        ],
    )
    .await;
    git.run(&pc, &["fsck"]).await;
}

/// The stored layers `repo`'s state object names.
fn checkpointed_layers(root: &std::path::Path, repo: &str) -> Vec<String> {
    let data = std::fs::read(root.join("store").join("repos").join(repo).join("state")).unwrap();
    let state = enroute_shared::repo_state::Record::from_bytes(&data).unwrap();
    state
        .meta()
        .layers
        .into_iter()
        .filter(enroute_shared::proto::LayerMeta::is_stored)
        .map(|l| l.id)
        .collect()
}

fn layer_exists(root: &std::path::Path, repo: &str, id: &str) -> bool {
    root.join("store")
        .join("repos")
        .join(repo)
        .join("layers")
        .join(format!("{id}.layer"))
        .exists()
}

/// A replaced layer stays while the last checkpoint still names it.
///
/// Here checkpoints stall, so no sweep deletes it; once they resume, the
/// sweep does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaced_layers_outlive_the_checkpoint_naming_them() {
    let root = tempfile::tempdir().unwrap();
    let mut config =
        enroute::node::Config::local(root.path().join("data"), common::local_store(root.path()));
    let outage = common::Outage::wrap(&mut config);
    config.ledger.checkpoint_age = Duration::from_millis(200);
    config.compute.sweep_interval = Duration::from_millis(100);
    config.compute.sweep_retention = Duration::from_millis(300);
    config.compute.merge_factor = 2;
    config.compute.inline_layer_bytes = 0; // each push uploads a layer
    let server = Server::start_config(config).await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("ckpt");
    let push = async |i: u64| {
        git.commit_file(
            &src,
            &format!("f{}.txt", i % 3),
            &text(i, 20),
            &format!("c{i}"),
        )
        .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
    };
    for i in 0..3 {
        push(i).await;
    }
    // A checkpoint names stored layers; then checkpoints stall.
    let live = || {
        server
            .node
            .consensus()
            .unwrap()
            .repo_meta("ckpt")
            .unwrap()
            .layers
            .iter()
            .map(|l| l.id.clone())
            .collect::<Vec<_>>()
    };
    assert!(
        common::wait_until(
            || {
                let c = checkpointed_layers(root.path(), "ckpt");
                !c.is_empty() && c == live()
            },
            20
        )
        .await
    );
    outage.set_states_down(true);
    let named = checkpointed_layers(root.path(), "ckpt");

    // Compaction replaces every one of them while the checkpoint still names them.
    for i in 3..8 {
        push(i).await;
    }
    assert!(
        common::wait_until(|| live().iter().all(|id| !named.contains(id)), 20).await,
        "compaction did not replace the checkpointed layers"
    );
    tokio::time::sleep(Duration::from_secs(2)).await; // many sweeps, well past the retention
    assert_eq!(
        checkpointed_layers(root.path(), "ckpt"),
        named,
        "a checkpoint was written while stalled"
    );
    for id in &named {
        assert!(
            layer_exists(root.path(), "ckpt", id),
            "layer {id} was deleted while the checkpoint names it"
        );
    }

    // Checkpoints resume: the next one no longer names them, and they go.
    outage.set_states_down(false);
    assert!(
        common::wait_until(
            || named
                .iter()
                .all(|id| !layer_exists(root.path(), "ckpt", id)),
            30
        )
        .await,
        "replaced layers were not deleted once the checkpoint moved on"
    );
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "main").await);
}
