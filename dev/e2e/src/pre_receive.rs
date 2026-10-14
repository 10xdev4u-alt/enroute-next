//! Wire-level smoke tests for the `pre-receive` hook: a rejection reaches a
//! real `git push`, and does not reach the application's own calls.
//!
//! Both doors, because they are not the same door. The hook is how an
//! application decides what a *git client* may land; the contract is the
//! application itself calling, and asking it to authorize its own call would
//! only catch its own bugs.

#[cfg(test)]
mod tests {
    use crate::support::{git, git_allowing_failure, push_refused_commit, spawn_smoke_repo};

    #[tokio::test]
    async fn a_hook_rejection_reaches_the_pusher() {
        let (_tmp, local, _stack) = spawn_smoke_repo().await;
        let local = local.as_path();

        // A branch the stub application has no opinion about lands, so the
        // failure below is the hook's doing and not a broken push path.
        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["push", "origin", "main"], Some(local)).await;

        // The one refname the stub refuses. A real application decides this
        // from what it knows; the stub decides by name, which is all a harness
        // needs to prove the answer travels.
        let protected = crate::hooks::PROTECTED
            .strip_prefix("refs/heads/")
            .expect("the stub protects a branch");
        git(&["checkout", "-b", protected], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "second"], Some(local)).await;

        let (ok, out) = git_allowing_failure(&["push", "origin", protected], local).await;
        assert!(!ok, "a push the hook refused must fail:\n{out}");
        assert!(
            out.contains("is protected by the stub application"),
            "the push output is missing the hook's reason:\n{out}"
        );
    }

    /// By default, a push whose every command is refused stores nothing: its
    /// objects are not readable afterwards.
    #[tokio::test]
    async fn a_refused_push_stores_no_objects() {
        let stack = crate::support::Stack::start().await;
        let (_tmp, local, key) = crate::support::smoke_repo_on(&stack).await;
        let commit = push_refused_commit(&local).await;

        let client = stack.client().await;
        assert!(
            client.get_object(&key, &commit).await.is_err(),
            "the refused commit was stored"
        );
    }

    /// In mode `objects`, a refusal reaches the pusher too.
    ///
    /// The objects were stored before the hook was asked, so they stay.
    #[tokio::test]
    async fn a_hook_reading_objects_refuses_too() {
        let stack = crate::support::Stack::start_reading_objects().await;
        let (_tmp, local, key) = crate::support::smoke_repo_on(&stack).await;
        let commit = push_refused_commit(&local).await;

        let client = stack.client().await;
        client.get_object(&key, &commit).await.unwrap();
    }

    /// A command the application judged neither way fails the push.
    ///
    /// An application that answers for part of a push has decided nothing
    /// about the rest, and silence read as consent lands what its policy missed.
    #[tokio::test]
    async fn a_command_the_application_left_unjudged_fails_the_push() {
        let (_tmp, local, _stack) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;

        let unjudged = crate::hooks::UNJUDGED
            .strip_prefix("refs/heads/")
            .expect("the stub leaves a branch unjudged");
        git(&["checkout", "-b", unjudged], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "second"], Some(local)).await;

        let (ok, out) = git_allowing_failure(&["push", "origin", unjudged], local).await;
        assert!(!ok, "a push nobody judged must not land:\n{out}");
        // Legibility to whoever has to fix the endpoint is the whole reason
        // for failing the push rather than guessing at it.
        assert!(
            out.contains("judged"),
            "the push output does not say what was left unjudged:\n{out}"
        );
    }

    /// What `post-receive` says reaches the person who pushed.
    ///
    /// A `remote:` line, and the only way an application can reach them at all:
    /// it holds no connection, and by here nothing is left to refuse.
    #[tokio::test]
    async fn what_post_receive_says_reaches_the_pusher() {
        let (_tmp, local, _stack) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        let out = git(&["push", "origin", "main"], Some(local)).await;

        assert!(
            out.contains(&format!("{} refs/heads/main", crate::hooks::LANDED_PREFIX)),
            "the push output is missing what the application said:\n{out}"
        );
    }

    /// What `authorize` said about the pusher reaches the hooks unchanged.
    ///
    /// The context is opaque to Enroute by contract, so the only thing there is
    /// to assert is that the bytes arrive as the application wrote them.
    #[tokio::test]
    async fn the_actor_and_its_context_are_played_back_to_the_application() {
        let (_tmp, local, stack) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["push", "origin", "main"], Some(local)).await;

        let seen = stack.landed.seen();
        let push = seen.first().expect("the application was told what landed");
        assert_eq!(push.actor, crate::hooks::ACTOR);
        assert_eq!(push.context, crate::hooks::CONTEXT);
    }

    /// The refused branch alone fails.
    ///
    /// Git reports per ref: an application objecting to one has not objected to
    /// the rest.
    #[tokio::test]
    async fn a_rejection_does_not_take_the_rest_of_the_push_with_it() {
        let (_tmp, local, _stack) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        let protected = crate::hooks::PROTECTED
            .strip_prefix("refs/heads/")
            .expect("the stub protects a branch");
        git(&["branch", protected], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        let (ok, out) = git_allowing_failure(&["push", "origin", protected, "fine"], local).await;
        assert!(!ok, "the refused branch still fails the command:\n{out}");

        // What landed is the question, and the remote is what answers it.
        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(refs.contains("refs/heads/fine"), "{refs}");
        assert!(
            !refs.contains(crate::hooks::PROTECTED),
            "the refused branch must not be there:\n{refs}"
        );
    }

    /// What git prints when a server offers no `atomic` capability.
    const NO_ATOMIC: &str = "does not support --atomic";

    /// The branch the stub refuses, as a client names it.
    fn protected() -> &'static str {
        crate::hooks::PROTECTED
            .strip_prefix("refs/heads/")
            .expect("the stub protects a branch")
    }

    /// With `--atomic`, one refusal fails the whole push: the refused ref
    /// carries the hook's reason, the rest carry git's own.
    #[tokio::test]
    async fn an_atomic_push_with_one_refusal_lands_nothing() {
        let (_tmp, local, stack) = spawn_smoke_repo().await;
        let landed = &stack.landed;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["branch", protected()], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        let (ok, out) =
            git_allowing_failure(&["push", "--atomic", "origin", protected(), "fine"], local).await;
        assert!(!ok, "an atomic push with a refusal must fail:\n{out}");
        assert!(!out.contains(NO_ATOMIC), "{out}");
        assert!(
            out.contains("is protected by the stub application"),
            "the push output is missing the hook's reason:\n{out}"
        );
        assert!(
            out.contains("atomic push failure"),
            "the allowed branch does not say why it failed:\n{out}"
        );

        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(refs.trim().is_empty(), "something landed:\n{refs}");
        assert!(landed.seen().is_empty(), "post-receive heard of a push");
    }

    /// With `--atomic`, a non-fast-forward fails the whole push before any
    /// hook is asked.
    ///
    /// Git checks a fast-forward before it sends, so the next test sends a
    /// refusal that only the server can find.
    #[tokio::test]
    async fn an_atomic_push_with_a_non_fast_forward_lands_nothing() {
        let (_tmp, local, stack) = spawn_smoke_repo().await;
        let landed = &stack.landed;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "second"], Some(local)).await;
        git(&["push", "origin", "main"], Some(local)).await;
        let landed_tip = git(&["rev-parse", "HEAD"], Some(local)).await;
        let asked_before = landed.asked().len();

        git(&["reset", "--hard", "HEAD~1"], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "diverged"], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        let (ok, out) =
            git_allowing_failure(&["push", "--atomic", "origin", "main", "fine"], local).await;
        assert!(!ok, "a non-fast-forward must fail the push:\n{out}");
        assert!(!out.contains(NO_ATOMIC), "{out}");

        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(refs.contains(landed_tip.trim()), "main moved:\n{refs}");
        assert!(!refs.contains("refs/heads/fine"), "fine landed:\n{refs}");
        assert_eq!(landed.asked().len(), asked_before, "the hook was asked");
    }

    /// With `--atomic`, a ref the server refuses before the hook fails the
    /// whole push, and the hook is never asked: no answer could land it.
    #[tokio::test]
    async fn an_atomic_push_refused_before_the_hook_does_not_ask_it() {
        let (_tmp, local, stack) = spawn_smoke_repo().await;
        let landed = &stack.landed;
        let local = local.as_path();

        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(local)).await;
        git(&["commit", "-m", "first"], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        // A tree on a branch: git sends it, and only the server refuses it.
        let (ok, out) = git_allowing_failure(
            &[
                "push",
                "--atomic",
                "origin",
                "HEAD^{tree}:refs/heads/tree",
                "fine",
            ],
            local,
        )
        .await;
        assert!(!ok, "a non-commit on a branch must fail the push:\n{out}");
        assert!(!out.contains(NO_ATOMIC), "{out}");
        // The contract does not fix the reason's words, only that the ref is
        // refused for itself.
        assert!(
            out.contains("[remote rejected] HEAD^{tree} -> tree"),
            "{out}"
        );
        assert!(!out.contains("tree (atomic push failure)"), "{out}");
        assert!(out.contains("fine (atomic push failure)"), "{out}");

        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(refs.trim().is_empty(), "something landed:\n{refs}");
        assert!(landed.asked().is_empty(), "the hook was asked");
        assert!(landed.seen().is_empty(), "post-receive heard of a push");
    }

    /// With `--atomic`, a ref the ref store refuses after the hook allowed it
    /// takes back the refs that would have landed with it.
    ///
    /// The hidden branch is not advertised, so git sends a create for a ref
    /// that is there, and only the ref store's guard can refuse it.
    #[tokio::test]
    async fn an_atomic_push_rolls_back_what_the_ref_store_refuses() {
        let (_tmp, local, stack) = spawn_smoke_repo().await;
        let landed = &stack.landed;
        let local = local.as_path();
        let hidden = crate::hooks::HIDDEN
            .strip_prefix("refs/heads/")
            .expect("the stub hides a branch");

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["push", "origin", &format!("main:{hidden}")], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "second"], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        let (ok, out) = git_allowing_failure(
            &[
                "push",
                "--atomic",
                "origin",
                &format!("main:{hidden}"),
                "fine",
            ],
            local,
        )
        .await;
        assert!(!ok, "a create of a ref that exists must fail:\n{out}");
        assert!(out.contains("already exists"), "{out}");
        assert!(out.contains("atomic push failure"), "{out}");

        let asked = landed.asked();
        assert!(
            asked
                .last()
                .is_some_and(|refs| refs.contains(&"refs/heads/fine".to_owned())),
            "the hook was not asked about the push: {asked:?}"
        );
        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(!refs.contains("refs/heads/fine"), "fine landed:\n{refs}");
        assert_eq!(landed.seen().len(), 1, "post-receive heard of the push");
    }

    /// With `--atomic` and nothing refused, every ref lands, and
    /// `post-receive` is told of all of them at once.
    #[tokio::test]
    async fn an_atomic_push_nothing_refuses_lands_whole() {
        let (_tmp, local, stack) = spawn_smoke_repo().await;
        let landed = &stack.landed;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        let out = git(&["push", "--atomic", "origin", "main", "fine"], Some(local)).await;
        assert!(!out.contains(NO_ATOMIC), "{out}");

        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(refs.contains("refs/heads/main"), "{refs}");
        assert!(refs.contains("refs/heads/fine"), "{refs}");

        let seen = landed.seen();
        let [push] = seen.as_slice() else {
            panic!("post-receive was not told once: {seen:?}");
        };
        let mut refnames: Vec<&str> = push.commands.iter().map(|(r, _)| r.as_str()).collect();
        refnames.sort_unstable();
        assert_eq!(refnames, ["refs/heads/fine", "refs/heads/main"]);
    }
}

/// The same refusal, over the contract rather than over git.
///
/// The contract's `UpdateRefs` runs the same hook through the same
/// `apply_ref_updates`; only the answer's spelling differs.
#[cfg(test)]
mod contract_tests {
    use enroute_api::api::v1alpha1::RefUpdate;

    use crate::contract::oid;
    use crate::support::{Scratch, Stack};

    /// The contract does not run the hook, and the same refname proves it.
    ///
    /// `PROTECTED` is the name the stub refuses, and the test above pushes it
    /// and fails. Here it lands, because the caller wrote that rule.
    #[tokio::test]
    async fn the_contract_is_not_held_to_the_git_door_s_hook() {
        let stack = Stack::start().await;
        let client = stack.client().await;

        let created = client.create_repository("refs/heads/main").await.unwrap();
        let repo = created.repo.expect("a created repository has a key").key;

        let scratch = Scratch::new().await;
        let commit = scratch.commit(b"hello\n", None, "first").await;

        // The objects have to be stored before a ref can name them: a push is
        // still the only way one arrives, which is what the contract enforces
        // in place of a hook.
        stack
            .seed(&repo, "refs/heads/seed", &scratch, &commit)
            .await;

        let updates = vec![RefUpdate {
            refname: crate::hooks::PROTECTED.to_string(),
            old_object_id: None,
            new_object_id: oid(commit.oid.clone()),
        }];

        let outcomes = client.update_refs(&repo, updates).await.unwrap();
        let protected = outcomes
            .iter()
            .find(|outcome| outcome.refname == crate::hooks::PROTECTED)
            .expect("an outcome for every requested update");
        assert_eq!(protected.rejection, None, "{outcomes:?}");

        let refs = client.list_refs(&repo).await.unwrap();
        let names: Vec<&str> = refs.refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&crate::hooks::PROTECTED), "{names:?}");
    }

    /// An object nobody pushed cannot be named, however the contract is called.
    ///
    /// The one rule the contract keeps, and the reason it needs no hook to
    /// keep it: git stays the only way an object arrives.
    #[tokio::test]
    async fn the_contract_cannot_name_an_object_that_never_arrived() {
        let stack = Stack::start().await;
        let client = stack.client().await;

        let created = client.create_repository("refs/heads/main").await.unwrap();
        let repo = created.repo.expect("a created repository has a key").key;

        let updates = vec![RefUpdate {
            refname: "refs/heads/invented".to_string(),
            old_object_id: None,
            new_object_id: oid("1".repeat(40)),
        }];

        let outcomes = client.update_refs(&repo, updates).await.unwrap();
        assert!(
            outcomes[0].rejection.is_some(),
            "a ref pointing at nothing must not land: {outcomes:?}"
        );
    }
}
