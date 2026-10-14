# Contributing

## Read AGENTS.md first

[AGENTS.md](AGENTS.md) holds the rules a change here follows. The short
version: read the least local context the task needs, keep the change scoped,
and write comments and documentation in the voice of the
[Google Developer Documentation Style Guide](https://developers.google.com/style),
with Simplified Technical English preferred over the register around it.

## Pull requests are disabled

This repository does not accept pull requests. A coding agent makes it too
easy to send a large, low-context change that costs the maintainers more time
than it saves.

Send a `git format-patch` attachment to
[patches@enroute.sh](mailto:patches@enroute.sh) instead. By sending a patch
you certify that you have the right to submit it and assign to OpenCanopy
GmbH all rights in it that you can assign. The README states the full terms.

Before you send a patch, understand the code it changes, keep it focused, and
respect the review time you are asking for.

## Build and test

```sh
cargo build
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask lint
cargo test --workspace
```

`cargo xtask lint` runs this repository's own rules, which fmt and clippy do
not cover: import shape, comment shape, dead code across crates, layering,
manifest inheritance, the contract and the copy of it inside the skill. A
change that fails it fails review.

`cargo test --workspace` starts real nodes with a local bucket and needs `git`
on the path. Those tests are the ones that prove a change to the push or fetch
path.

## Commit messages

Write the reason, not the diff. A commit subject names the area and what
changed, and the body says why it needed to. The history is squashed per
release, so nothing depends on a commit number.
