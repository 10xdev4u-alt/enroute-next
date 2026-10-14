//! Shallow clones that rebuild boundary objects by replay.

#![expect(
    clippy::excessive_nesting,
    clippy::format_push_string,
    clippy::multiple_unsafe_ops_per_block,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, text};

/// Shallow clones whose boundary trees are rebuilt by replaying delta chains.
///
/// Replay is forced for every re-encode set here, across chains that span
/// several pushed layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[expect(
    unsafe_code,
    reason = "the engine reads its test-only tuning from the environment"
)]
async fn shallow_clones_by_replay() {
    // SAFETY: read once, on first use, and set before the server starts.
    unsafe {
        std::env::set_var("ENROUTE_REPLAY_MIN", "1");
        // Tiny groups: many data passes, and targets rebuilt again as chain
        // members of other groups (each must still be sent once).
        std::env::set_var("ENROUTE_REPLAY_GROUP_BYTES", "2000");
    }
    let server = Server::start().await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("replay");
    // Several pushes, each editing the same files: delta chains that cross
    // layers, plus files that never change (full entries).
    for push in 0..4u64 {
        for i in 0..6u64 {
            let mut t = text(1000 + i % 3, 120);
            t.push_str(&format!("push {push} edit {i}\n"));
            git.commit_file(
                &src,
                &format!("d{}/f{}.txt", i % 2, i % 3),
                &t,
                &format!("p{push} e{i}"),
            )
            .await;
        }
        git.commit_file(
            &src,
            &format!("static{push}.txt"),
            &text(push, 50),
            "static",
        )
        .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
    }
    // A tip file whose content is an old version of another path: that blob
    // is both a target and a member of the other path's chain.
    let mut old = text(1000, 120);
    old.push_str("push 1 edit 0\n");
    git.commit_file(&src, "copy.txt", &old, "copy an old version")
        .await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    for depth in ["1", "2", "5"] {
        let dst = git.dir(&format!("d{depth}"));
        git.run(
            git.home.path(),
            &["clone", "-q", "--depth", depth, &url, dst.to_str().unwrap()],
        )
        .await;
        git.run(&dst, &["fsck", "--strict"]).await;
        assert_eq!(git.rev(&src, "HEAD").await, git.rev(&dst, "HEAD").await);
        // fsck verified every object's hash; check the trees are complete.
        let commits = if depth == "1" {
            vec!["HEAD"]
        } else {
            vec!["HEAD", "HEAD~1"]
        };
        for c in commits {
            let want = git.run(&src, &["ls-tree", "-r", c]).await;
            assert_eq!(want, git.run(&dst, &["ls-tree", "-r", c]).await);
            for line in want.lines() {
                let oid = line.split_whitespace().nth(2).unwrap();
                git.run(&dst, &["cat-file", "-e", oid]).await;
            }
        }
    }
}
