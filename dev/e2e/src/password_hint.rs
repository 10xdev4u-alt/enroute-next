//! Proves the 401 hint reaches a real `git` client.
//!
//! Git only prints an error body (as `remote:` lines) when it is
//! `text/plain`, so this asserts the user-visible outcome, not the shape.

#[cfg(test)]
mod tests {
    use crate::support::{ALICE, Stack, isolated_git, with_host_override};

    #[tokio::test]
    async fn a_password_gets_a_usable_explanation_over_the_wire() {
        let stack = Stack::start().await;
        let repo = format!("repo-{}", uuid::Uuid::new_v4());
        stack.create(&repo).await;
        // The stub's own token, replaced below by a password it does not know.
        let url = stack
            .url(&repo)
            .replacen(test_support::hooks::TOKEN, "hunter2", 1);

        let tmp = tempfile::tempdir().unwrap();
        let mut cmd = tokio::process::Command::from(isolated_git());
        cmd.arg("clone").arg(url).arg(tmp.path().join("clone"));
        with_host_override(&mut cmd, ALICE);
        let out = cmd.output().await.unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

        assert!(
            stderr.contains("remote: this server authenticates git with an access token"),
            "git did not surface the hint:\n{stderr}"
        );
    }
}
