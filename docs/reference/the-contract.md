# API contract

Your application calls Enroute over gRPC on `listen.api` (`:50051` by default).
Enroute serves plain HTTP/2; terminate TLS before it.

## Authentication

Enroute authenticates nothing on this listener. Your application is its only
caller, and any client that can reach the port can act as your application.
Keep it unreachable from anywhere else. See [Security](../operate/security.md).

## Services

| Service | RPCs |
| --- | --- |
| `RepositoryService` | `CreateRepository`, `GetRepository`, `ListRepositories`, `DeleteRepository` |
| `RefService` | `ListRefs`, `UpdateRefs`, `IsAncestor` |
| `ObjectService` | `GetObject`, `ListTree`, `ListCommits`, `DiffCommit`, `FindMergeBases` |
| `SyncService` | `PushToRemote` |
| `CommitService` | `CreateCommit` |

The proto files define all fields. See [Protocol Buffers](proto.md).

## Repository keys

Repository keys are application-owned, opaque identifiers. A key is at most
256 bytes, in one or more segments separated by `/`, such as `acme/widgets`.
Each segment holds ASCII letters, digits, `-`, `_`, or `.`, and starts and ends
with a letter or digit. A key holds no empty, `.`, or `..` segment. Keys are
case-sensitive and compared byte for byte. A key is unique across the
deployment.

If your application renames or moves repositories, use a stable ID such as a
UUID, ULID, or database ID as the key. The `authorize` hook maps Git URLs to
keys. Without `authorize`, the key is the URL path without `.git`.

A push to a repository that does not exist creates it, under the key that
`authorize` granted, or under the path key when `authorize` is not enabled.
A fetch, clone, or `ls-remote` of a missing repository is still not found.
Set `git.create_on_push = false` to create repositories only with
`CreateRepository`. Call `CreateRepository` before the first push to set a
different default branch.
`CreateRepository` is idempotent. `default_branch` is used only when creating
the repository and defaults to `refs/heads/main`. `ListRepositories` takes an
optional `prefix` and reports only the keys starting with it, byte for byte; a
page token belongs to the prefix that minted it. `ListRepositories` returns
keys in byte order and paginates at 100 items; follow `next_page_token` until empty.
Do not treat a short page as the last page. `GetRepository` includes the
repository key, default branch, and the time of the most recent push; the
timestamp is unset before the first push.
`DeleteRepository` is idempotent and releases the key immediately. Enroute
reclaims the stored data in the background.

## Refs and objects

`ListRefs` returns refs, their object IDs, and update times. It does not
return `HEAD` as a ref. It reports the branch `HEAD` points at in
`default_branch`. An update time comes from the clock of the node that
proposed the change, so times can go back when node clocks differ.

`UpdateRefs` is not all-or-nothing. Each update succeeds or fails on its own,
and the outcomes come back in request order. A rejected update is an outcome,
not an RPC failure. Unset `old_object_id` on an update creates a ref. If the
ref exists, the update fails with `ALREADY_EXISTS`. Unset `new_object_id`
deletes the ref. On a delete, an unset `old_object_id` waives the check, and
the ref goes whatever it holds. If other writers keep moving the refs, the
call fails with `ABORTED`.

Updates can only target objects already in the repository, from a Git push or
from `CreateCommit`. A ref under `refs/heads/` must point at a commit. Other refs can point at any stored
object. `UpdateRefs` does not run `pre_receive`; the gRPC caller is your
application. `IsAncestor` returns `false` if either commit is unknown.

All object reads stream. `ListTree` is breadth-first, `ListCommits` follows
first parents newest-first, `DiffCommit` compares against a first parent or
specified base, and `FindMergeBases` returns the merge bases as a set, in no
defined order.

| RPC | Limit | Completion signal |
| --- | --- | --- |
| `ListCommits` | 100 commits per page | `next_page_token` |
| `ListTree` | 50,000 entries | `truncated` |
| `DiffCommit` | 50,000 changes or 10,000 tree reads | `truncated` |
| `FindMergeBases` | Walk budget | `exhausted` |

`truncated` and `exhausted` results are partial. Check the signal before
presenting a tree or diff as complete.

## Commits

`CreateCommit` makes one commit on a branch from file changes, and moves the
branch to it. Enroute makes the blobs, the trees, and the commit. Your
application does not need a checkout or a Git client.

- **No hooks.** `CreateCommit` does not run `pre_receive` or `post_receive`.
  Your application is the caller, as with `UpdateRefs`. Apply your policy
  before the call.
- **The branch.** `branch` is a full refname under `refs/heads/`, such as
  `refs/heads/main`. The parent of the new commit is the commit that the branch
  points to. If the branch does not exist, the call creates it, and the commit
  has no parent.
- **Compare-and-swap.** `expected_old_commit` sets what the branch must hold.
  Set `commit_id` to require that commit, or `absent` to require that the
  branch does not exist. If the branch does not match, the call fails with
  `FAILED_PRECONDITION` and nothing changes. If you do not set
  `expected_old_commit`, there is no check. If another writer moves the branch
  during the call, Enroute makes the commit again on the new tip. If this
  continues, the call fails with `ABORTED`.
- **Changes.** Each change puts a file or deletes a file. A put writes
  `content` with a mode: regular (the default), executable, or symbolic link,
  where `content` is the link target. A put creates the directories in its
  path. A delete of a path that holds no file does nothing. A directory that a
  delete leaves empty is removed. Each path can occur only once in a call.
- **Paths.** A path is relative and `/`-separated, such as `src/lib.rs`. It
  has no empty, `.`, or `..` component, no component that Git reads as `.git`
  in any case, and no `\` or NUL. A put cannot replace a directory or go
  through a file, and a delete cannot remove a directory.
- **Identities.** `author` is required. Its name and email must not be empty
  and must not hold `<`, `>`, a newline, or NUL. If `timestamp` is unset,
  Enroute uses the time that the call arrives. `utc_offset_seconds` must be
  whole minutes, at most 99:59 in each direction. If `committer` is unset, it
  is the author.
- **Message.** Enroute stores the message as given and adds a final newline if
  there is none. The message cannot hold NUL.
- **Size.** A request is at most 32 MiB, encoded. Enroute refuses a larger
  request with `OUT_OF_RANGE` before it reads it. To add more data, push it
  over Git.
- **Read your writes.** When the call returns, a following read, such as
  `ListRefs`, sees the new commit.

| Status | Cause |
| --- | --- |
| `NOT_FOUND` | The repository does not exist. |
| `INVALID_ARGUMENT` | A field breaks a rule above. Nothing changes. |
| `FAILED_PRECONDITION` | The branch does not match `expected_old_commit`. |
| `ABORTED` | Other writers kept moving a branch that had no `expected_old_commit`. |
| `OUT_OF_RANGE` | The request is larger than 32 MiB. |

## Synchronization

`PushToRemote` sends refs to another Git server. See [Push to a remote](push-to-a-remote.md).
