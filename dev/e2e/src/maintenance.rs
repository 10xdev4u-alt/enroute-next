//! Maintenance against a repository that was really pushed to.
//!
//! Upkeep can rewrite what a fetch reads, so the check is the clone afterwards:
//! every commit, every blob, and a packfile git verifies itself.

#[cfg(test)]
mod tests {
    use crate::support::{Stack, git};

    /// Enough pushes for compaction to merge several times.
    const PUSHES: usize = 9;

    /// Clone `url` into a new directory, and check it holds every push.
    async fn assert_clones(url: &str, head: &str) {
        let tmp = tempfile::tempdir().unwrap();
        let into = tmp.path().join("cloned");
        git(&["clone", url, into.to_str().unwrap()], Some(tmp.path())).await;

        assert_eq!(
            git(&["rev-parse", "HEAD"], Some(&into)).await.trim(),
            head,
            "the clone landed on a different commit"
        );
        let log = git(&["log", "--oneline"], Some(&into)).await;
        assert_eq!(log.lines().count(), PUSHES, "history is short:\n{log}");
        for n in 0..PUSHES {
            assert_eq!(
                std::fs::read_to_string(into.join(format!("file-{n}.txt"))).unwrap(),
                format!("contents {n}\n"),
                "a blob came back wrong after maintenance"
            );
        }
        let status = git(&["status", "--porcelain"], Some(&into)).await;
        assert!(status.trim().is_empty(), "working tree is dirty:\n{status}");
    }

    /// A clone after every push was flushed and the layers were merged.
    #[tokio::test]
    async fn a_repository_still_clones_after_flushes_and_compactions() {
        let stack = Stack::start().await;
        let name = format!("repo-{}", uuid::Uuid::new_v4());
        let key = stack.create(&name).await;
        let url = stack.url(&name);

        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;

        // One commit for each push, and a wait for its flush, so each push
        // becomes a stored layer of its own for compaction to merge.
        for n in 0..PUSHES {
            std::fs::write(
                local.join(format!("file-{n}.txt")),
                format!("contents {n}\n"),
            )
            .unwrap();
            git(&["add", "."], Some(&local)).await;
            git(&["commit", "-m", &format!("commit {n}")], Some(&local)).await;
            git(&["push", "origin", "main"], Some(&local)).await;
            stack.maintain(&key).await;
        }
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
        assert_clones(&url, head.trim()).await;
    }
}
