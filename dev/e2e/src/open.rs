//! A deployment with hooks off, and one with only some of them on.
//!
//! Every hook is opt-in, so these are the defaults an application gets for
//! each hook it does not enable.

#[cfg(test)]
mod tests {
    use crate::contract::Client;
    use crate::engine::Enroute;
    use crate::support::{Scratch, Stack, git, git_allowing_failure};

    /// A deployment that calls no hook.
    async fn open() -> (Enroute, Client) {
        let engine = Enroute::start(None).await.expect("an open deployment");
        let client = Client::connect(engine.grpc_url().to_string())
            .await
            .unwrap();
        (engine, client)
    }

    #[tokio::test]
    async fn a_push_creates_the_repository_at_its_path() {
        let (engine, client) = open().await;
        let scratch = Scratch::new().await;
        let tip = scratch.commit(b"hello", None, "first").await;
        let url = format!("{}/acme/widgets.git", engine.git_url());

        scratch.push(&url, "refs/heads/main", &tip).await;

        let repo = client.get_repository("acme/widgets").await.unwrap();
        assert_eq!(repo.repo.unwrap().key, "acme/widgets");
        let into = tempfile::tempdir().unwrap();
        let at = into.path().join("clone");
        git(&["clone", &url, at.to_str().unwrap()], None).await;
        let head = git(&["rev-parse", "HEAD"], Some(&at)).await;
        assert_eq!(head.trim(), tip.oid);
        assert_eq!(std::fs::read(at.join("hello.txt")).unwrap(), b"hello");

        // Without the suffix, the path names the same repository.
        let bare = format!("{}/acme/widgets", engine.git_url());
        let listed = git(&["ls-remote", &bare], None).await;
        assert!(listed.contains(&tip.oid), "{listed}");
    }

    #[tokio::test]
    async fn a_missing_repository_is_not_found_to_a_reader() {
        let (engine, client) = open().await;
        let scratch = Scratch::new().await;
        let url = format!("{}/nobody/here.git", engine.git_url());

        let (ok, said) = git_allowing_failure(&["ls-remote", &url], scratch.path()).await;
        assert!(!ok, "{said}");
        assert!(said.contains("not found"), "{said}");
        let at = scratch.path().join("clone");
        let (ok, said) =
            git_allowing_failure(&["clone", &url, at.to_str().unwrap()], scratch.path()).await;
        assert!(!ok, "{said}");
        // A read creates nothing.
        client.get_repository("nobody/here").await.unwrap_err();
    }

    #[tokio::test]
    async fn a_path_that_is_no_key_is_refused() {
        let (engine, _client) = open().await;
        let scratch = Scratch::new().await;
        let tip = scratch.commit(b"x", None, "x").await;
        let url = format!("{}/acme/-bad.git", engine.git_url());
        let refspec = format!("{}:refs/heads/main", tip.oid);

        let (ok, said) = git_allowing_failure(&["push", &url, &refspec], scratch.path()).await;
        assert!(!ok, "{said}");
    }

    /// Authorize off, pre-receive on: the path is the key, and the
    /// application still judges each push.
    #[tokio::test]
    async fn pre_receive_alone_still_judges_pushes() {
        let stack: Stack = Stack::start_hooks(&["pre_receive"]).await;
        let scratch = Scratch::new().await;
        let tip = scratch.commit(b"one", None, "one").await;
        let url = format!("{}/team/app.git", stack.deployment.git_url());

        scratch.push(&url, "refs/heads/main", &tip).await;
        let refspec = format!("{}:{}", tip.oid, crate::hooks::PROTECTED);
        let (ok, said) = git_allowing_failure(&["push", &url, &refspec], scratch.path()).await;

        assert!(!ok, "a protected ref landed:\n{said}");
        assert!(said.contains("protected by the stub"), "{said}");
        let asked = stack.landed.asked();
        assert_eq!(asked.len(), 2, "{asked:?}");
        // Post-receive is off, so the application hears of nothing landing.
        assert!(stack.landed.seen().is_empty());
        let refs = stack.client().await.list_refs("team/app").await.unwrap();
        let names: Vec<&str> = refs.refs.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["refs/heads/main"]);
    }
}
