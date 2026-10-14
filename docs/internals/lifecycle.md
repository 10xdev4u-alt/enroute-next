# Repository lifecycle

This page describes how a repository is created and deleted, how a deleted name
is reused safely, and how Enroute reclaims the files of a deleted repository.

The API creates a repository with a default branch (`CreateRepository`), gets
it with its last push time (`GetRepository`), lists (`ListRepositories`) and
deletes it (`DeleteRepository`, and the name is free at once). Between the
roles, the ledger service carries these as calls to activate with a HEAD,
describe, list and delete.

## Creation

`CreateRepository` activates the repository with the create option and a HEAD.
If the repository does not exist (or is deleted), the node claims the next
incarnation, as for an implicit create. HEAD names the default branch, and the
state marks HEAD as kept.

A kept HEAD is the fixed default branch of the repository. HEAD stays on its
branch while that branch is unborn, as Git does (`ls-refs unborn`; an empty
clone takes it). It also stays on its branch after the branch is deleted. A
repository that a push created is different: Enroute repairs its HEAD to an
existing branch (main, then master, then the first) when its branch is gone.

Creating an existing repository changes nothing and answers with what it has. An
explicit create reads the state object even when the node hosts the repository,
so a replica that missed a deletion cannot answer with the deleted repository.
Names are at most 256 bytes, so every Enroute key is one.

## Deletion

`DeleteRepository` asks a ledger node to do two things.

1. Host the live incarnation E (from the state object) and propose a delete for
   E. The state machine drops the refs and the manifest, and the pins of inline
   layers so the log is not held back. It marks the state deleted. Every later
   command in E is refused as deleted, on the node before it reaches the log,
   and by the state machine for any that did. Deleting a missing or deleted
   repository succeeds.
2. After the tombstone is applied there (the proposer is answered after
   applying), write the trash marker `trash/<repo>/<E>` with the last upload
   generation of E. Then swap the deleted state into the state object at E by
   compare-and-swap. Every checkpoint of a deleted state does the same. A crash
   between the two steps is finished by the next checkpoint, or by the next
   activation of a node that applied the tombstone.

When the call returns, the state object says deleted. Activation then answers
not found unless it creates. A create claims E + 1 by compare-and-swap from the
deleted state, through the same claim as incarnation 1.

- The initial state of the new incarnation is empty.
- Its version is one more than the last version of E, so views and subscribers
  never mistake it for an older state.
- Its upload generation is the last one of E plus the upload window, plus 1.
- A node that hosts E drops it when it hosts E + 1 (as a superseded
  incarnation).

### Why a lagging voter can never write again

The tombstone is committed. A majority of the voters of E have it, so any leader
of E has it (the election restriction of Raft), and a leader refuses every
command. The node marks the state object only after that commit. Whoever reads
"deleted" therefore knows that any later leader of E has the tombstone. A voter
that was down keeps E untombstoned, but it cannot lead without a majority. At
startup, it drops E once the object store says that E is deleted or superseded.

A node never joins an ended incarnation again: it checks the state object
first. A node that dropped its log would vote in E again from a fresh state,
although it may have voted in E before.

Reads without a barrier may see the old state of E on a lagging node, as any
view can lag the log ([Read path](read-path.md#fetch)). An API commit, a
describe call, the ref reads of a fetch and the receive-pack advertisement use a
read barrier, which brings the node up to the tombstone. A push can check
against the old state, and the next section explains why it cannot land.

### Commands prepared against a deleted incarnation

A push is checked against a snapshot of E. Suppose E is deleted and the name is
created again. The push could then land in E + 1, with references to objects
that only E had. So every compute command names the incarnation that its
snapshot came from. The state machine refuses it in any other incarnation, and
the compute answers not found. Stored layers of E cannot be committed into E + 1
either, because their upload generation is below its window.

### Compute nodes

A deletion reaches the subscription of a compute as a change that marks the
repository deleted. The compute drops the view and stops following. Later
requests activate the repository again and find nothing, or the new incarnation.
A subscription that ends without that change (E dropped on its ledger node) is
followed again. A state of another incarnation replaces the view instead of
updating it. The compute that asked for the deletion drops its view at once.

## Reclaiming layer files

The files of a deleted incarnation stay under `repos/<repo>/layers/`. Enroute
reclaims them after `layers.sweep_retention`, so that readers of older snapshots
can finish.

- **Created again:** the layer sweep of the new incarnation finds the files
  unnamed and out of its window. Their generation is at most the last one of E,
  which is below the start of the new window. The sweep deletes them like any
  other file.
- **Deleted, or not followed:** the compute reclaims trash markers every
  `layers.sweep_interval`, under the maintenance lease of the repository. Once
  the state object shows the deletion, nothing names a file whose generation is
  at most the one in the marker, and nothing ever will. The compute deletes the
  file after the retention, timed in the marker as a sweep record does. The
  marker goes once no such file is left.

`repos/<repo>/maintainer` and `repos/<repo>/sweep` carry over between
incarnations. A lease names a compute, not an incarnation. A sweep record only
delays deletions: it lists files seen unnamed, which the sweep of the new
incarnation checks again.

## Listing

Every ledger member hosts the log of every repository. The list of hosted
repositories on each node (in name order, in memory) is therefore the source,
with no object-store requests. The list of a node can lag a create (the claimant
hosts it first, peers when the announcement arrives) or a deletion (peers apply
the tombstone after the commit).

The compute asks every live ledger node and merges the answers. For each name,
the most advanced answer wins: the newest incarnation, deleted over live, then
the newest version. The node that answered a create hosts it, and the node that
answered a deletion applied it. A listing after the call therefore agrees with
it while that node is up.

Each node returns at most one page of names, deleted ones too, and says if more
follow. The merged page stops at the smallest last name among nodes that had
more, so pages never skip a name. A page can be short, because deleted names
take places. Listing the object store instead would mean a LIST over every layer
key of every repository, and a GET per repository to tell deleted ones.

## Last push

The compute stamps the commit entry of a Git push with its clock. The state
machine records it as the last push time if a ref moved, and never moves it
back. A refused push leaves it. API commits and maintenance do not stamp it. A
describe call returns it with HEAD.

## Ref update times

Each ref has the time it was last set (created or moved, also to the same
value), as `ListRefs` of Enroute reports it. The compute stamps every commit
entry that updates refs with its clock, so applying stays deterministic. The
state machine gives that time to each ref that the commit sets, and drops it
with the ref. A refused update leaves it.

The repository state keeps the time, and each ref change carries it to
followers. The state store keeps it after the object id in the
value of the ref (8 bytes), so snapshots, checkpoints and the state object carry
it with no new keys. Times come from the clocks of proposers, so they can go back when clocks differ.

## Limits

A deleted repository's log stays hosted until the name is created again.
