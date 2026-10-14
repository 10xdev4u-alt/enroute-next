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
cargo xtask ci tools   # once: the checkers, at pinned versions
cargo xtask ci         # everything CI runs, here
```

The pipeline is a program (`dev/xtask/src/ci`), not a list in a workflow file:
the workflow jobs name checks, and both call the same commands. `cargo xtask ci`
runs all of them and prints a table of what passed and how long it took;
`cargo xtask ci fmt clippy` runs some; `cargo xtask ci list` says what each one
proves. A single check is worth running on its own — `cargo xtask ci unit` is
seconds, `cargo xtask ci e2e` is minutes.

Run it before you send a patch. The class of failure this prevents is the one
you cannot see locally: `cargo fmt --all --check` is not something a person
types, and for twenty-three consecutive pull requests the only thing that ever
ran it was a runner after the push.

Among them: `lint` runs this repository's own rules, which fmt and clippy do not
cover (import shape, comment shape, dead code across crates, layering, manifest
inheritance, and the copy of the wire contract inside the skill). `e2e` starts
real nodes against a local bucket and needs `git` on the path; those tests are
the ones that prove a change to the push or fetch path.

`.github/workflows/ci.yml` runs the same checks in four jobs. A check that no
job names fails the run: `cargo xtask ci verify-workflow` is the guard, so a
check cannot be dropped from the pipeline by editing the workflow alone.

## Commit messages

Write the reason, not the diff. A commit subject names the area and what
changed, and the body says why it needed to. The history is squashed per
release, so nothing depends on a commit number.
