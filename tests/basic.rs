//! Push, clone and fetch through one node, and a stale push refused.

#![expect(
    clippy::format_push_string,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, text};
use enroute_compute::testing::pktline;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_clone_fetch_roundtrip() {
    let server = Server::start().await;
    let git = Git::new();
    let src = git.init("src").await;
    for i in 0..5 {
        git.commit_file(
            &src,
            &format!("dir{}/file{i}.txt", i % 2),
            &text(i, 200),
            &format!("c{i}"),
        )
        .await;
    }
    // Modify a file across commits so deltas get exercised.
    for i in 0..5 {
        let mut t = text(100, 300);
        t.push_str(&format!("edit {i}\n"));
        git.commit_file(&src, "big.txt", &t, &format!("edit {i}"))
            .await;
    }
    let url = server.repo_url("demo");
    git.run(&src, &["push", "-q", &url, "main"]).await;

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "HEAD").await, git.rev(&dst, "HEAD").await);

    // Incremental push + fetch.
    git.commit_file(&src, "big.txt", &text(100, 310), "more")
        .await;
    git.run(&src, &["checkout", "-q", "-b", "feature"]).await;
    git.commit_file(&src, "feature.txt", "feature\n", "feature")
        .await;
    git.run(&src, &["tag", "-a", "v1", "-m", "release"]).await;
    git.run(&src, &["push", "-q", &url, "main", "feature", "v1"])
        .await;

    git.run(&dst, &["fetch", "-q", "origin", "--tags"]).await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(
        git.rev(&src, "main").await,
        git.rev(&dst, "origin/main").await
    );
    assert_eq!(
        git.rev(&src, "feature").await,
        git.rev(&dst, "origin/feature").await
    );
    assert_eq!(git.rev(&src, "v1").await, git.rev(&dst, "v1").await);

    // ls-remote sees everything.
    let ls = git.run(&dst, &["ls-remote", &url]).await;
    assert!(
        ls.contains("refs/heads/feature") && ls.contains("refs/tags/v1") && ls.contains("HEAD"),
        "{ls}"
    );

    // Delete a branch.
    git.run(&src, &["push", "-q", &url, ":feature"]).await;
    let ls = git.run(&dst, &["ls-remote", &url]).await;
    assert!(!ls.contains("refs/heads/feature"), "{ls}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_stale_push() {
    let server = Server::start().await;
    let git = Git::new();
    let url = server.repo_url("r");
    let a = git.init("a").await;
    git.commit_file(&a, "f", "1\n", "1").await;
    git.run(&a, &["push", "-q", &url, "main"]).await;
    let b = git.dir("b");
    git.run(git.home.path(), &["clone", "-q", &url, b.to_str().unwrap()])
        .await;
    git.commit_file(&a, "f", "2\n", "2").await;
    git.run(&a, &["push", "-q", &url, "main"]).await;
    git.commit_file(&b, "f", "3\n", "3").await;
    let (ok, _, err) = git.try_run(&b, &["push", &url, "main"]).await;
    assert!(
        !ok,
        "non-fast-forward push from stale clone should fail: {err}"
    );
    // Force push works (non-ff allowed, CAS on the advertised value).
    git.run(&b, &["push", "-q", "--force", &url, "main"]).await;
    assert_eq!(
        git.rev(&b, "HEAD").await,
        git.run(&b, &["ls-remote", &url, "main"])
            .await
            .split('\t')
            .next()
            .unwrap()
    );
}

/// A raw receive-pack request of `commands` (`old new name`), with no pack.
async fn receive_pack(server: &Server, repo: &str, commands: &[String]) -> String {
    let mut body = Vec::new();
    for (i, c) in commands.iter().enumerate() {
        let caps = if i == 0 { "\0report-status" } else { "" };
        pktline::line(&mut body, &format!("{c}{caps}\n"));
    }
    pktline::flush(&mut body);
    reqwest::Client::new()
        .post(format!("{}/git-receive-pack", server.repo_url(repo)))
        .header("content-type", "application/x-git-receive-pack-request")
        .body(body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// A push takes the ref names the API takes, so the API can update and
/// delete every ref a push made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_takes_the_ref_names_the_api_takes() {
    let server = Server::start().await;
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let tip = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let zero = "0".repeat(40);
    let report = receive_pack(
        &server,
        "r",
        &[
            format!("{zero} {tip} refs/x"),
            format!("{zero} {tip} refs/heads/a.lock/b"),
            format!("{zero} {tip} refs/heads/ok"),
        ],
    )
    .await;
    assert!(report.contains("ng refs/x funny refname"), "{report}");
    assert!(
        report.contains("ng refs/heads/a.lock/b funny refname"),
        "{report}"
    );
    assert!(report.contains("ok refs/heads/ok"), "{report}");
    // A delete may name a one-level ref, as git's receive-pack allows.
    let report = receive_pack(&server, "r", &[format!("{tip} {zero} refs/x")]).await;
    assert!(!report.contains("funny refname"), "{report}");
}
