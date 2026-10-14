//! Pushes through the front door, past the stub application.
//!
//! Plain clone, fetch and push through one node are in `tests/basic.rs` and
//! `tests/features.rs`. These need what the application answers for a URL.

#[cfg(test)]
mod push {
    use crate::support::{Stack, git, git_allowing_failure};

    /// The path the stub application resolves to a test's one repository.
    const PATH: &str = "anything";

    /// An empty repository with nothing seeded into it.
    ///
    /// The key comes back so a test can name the repository to the stub
    /// under another path.
    async fn front_door_only() -> (tempfile::TempDir, Stack, String) {
        let stack = Stack::start().await;
        let key = stack.create(PATH).await;
        (tempfile::tempdir().unwrap(), stack, key)
    }

    /// `git push --atomic` lands every ref or none.
    ///
    /// No hooks here, so the refusal is the server's own: a tree on a branch.
    #[tokio::test]
    async fn an_atomic_push_lands_whole_or_not_at_all() {
        let (tmp, stack, _key) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = stack.url(PATH);

        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["branch", "dev"], Some(&local)).await;

        let (ok, out) = git_allowing_failure(
            &[
                "push",
                "--atomic",
                "origin",
                "HEAD^{tree}:refs/heads/tree",
                "main",
            ],
            &local,
        )
        .await;
        assert!(!ok, "a tree on a branch must fail the push:\n{out}");
        assert!(out.contains("atomic push failure"), "{out}");
        let refs = git(&["ls-remote", "--heads", &url], Some(&local)).await;
        assert!(refs.trim().is_empty(), "something landed:\n{refs}");

        git(&["push", "--atomic", "origin", "main", "dev"], Some(&local)).await;
        let refs = git(&["ls-remote", "--heads", &url], Some(&local)).await;
        assert!(refs.contains("refs/heads/main"), "{refs}");
        assert!(refs.contains("refs/heads/dev"), "{refs}");
    }

    /// A clone URL that holds a namespace, which is the shape a forge wants
    /// and the one a single path segment refused before anyone was asked.
    #[tokio::test]
    async fn a_nested_path_pushes_and_clones_back() {
        let (tmp, stack, key) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        stack.repos.name("enroute-sh/enroute", &key);
        let url = stack.url("enroute-sh/enroute");

        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;

        let back = tmp.path().join("clone");
        git(&["clone", &url, back.to_str().unwrap()], None).await;
        assert_eq!(
            git(&["rev-parse", "HEAD"], Some(&local)).await,
            git(&["rev-parse", "HEAD"], Some(&back)).await,
        );
    }

    /// Git appends no suffix, so both spellings are URLs a client may send
    /// and both must reach the same repository behind one answer.
    #[tokio::test]
    async fn a_path_with_no_dot_git_is_served_too() {
        let (tmp, stack, key) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        stack.repos.name("team/group/repo", &key);
        let url = stack.url("team/group/repo");
        let url = url.strip_suffix(".git").expect("a URL with the suffix");

        git(&["init", "-b", "main"], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["push", url, "main"], Some(&local)).await;

        let refs = git(&["ls-remote", url], Some(&local)).await;
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
        assert!(
            refs.contains(head.trim()),
            "pushed commit is not advertised:\n{refs}"
        );
    }
}
