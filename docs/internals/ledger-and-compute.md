# Ledger and compute

Enroute splits a node into two roles: the ledger and the compute. This page
describes the roles, how they talk, and how the ledger keeps one owner for each
repository. Enroute does not place compute nodes or add them with load.

**Goal:** the compute scales with load, in both directions, without touching any
repository's log. At most one incarnation of a repository can acknowledge writes
at any time.

## Roles

Nodes run one binary with a role (`node.role`). A small deployment gives every
node both roles. The ledger keeps the record of what happened and in what order.
The compute (the usual word for Git hosting) does the Git work.

- **Ledger**: a small, stable set of nodes with durable disks, like TiKV's
  stores or Neon's safekeepers. Every member is a Raft voter of every
  repository's log. The members hold the logs and the state machines (refs and
  manifest), and little else.
- **Compute**: everything else, as many nodes as the load needs. A compute node
  that serves a repository **subscribes** to its log. A ledger node streams it
  the state of the repository, then every committed change, inline layers
  included, as Neon's pageservers stream WAL from its safekeepers. Compute nodes
  are not members of any log. They come and go without a single entry being
  written.

Compute nodes are subscribers, not Raft learners, for three reasons. A learner
join or leave would be a membership change in the log of every repository the
node serves. Learners must persist entries before they acknowledge them, so
every compute node would need a durable log. The leader alone would track and
feed every learner. A subscription is a stream from any ledger node that hosts
the log (followers apply committed entries too). The fan-out spreads over the
ledger, and a compute node keeps no consensus state.

### What runs where

| | Ledger | Compute |
|---|---|---|
| Disk | the log store (entries, inline layer bytes until flushed: ≤ 64 MiB backlog per repository) and the state store (each repository's refs, manifest and counters) | local copies of layers, under the disk budget |
| CPU | applying entries (ref and manifest compare-and-swaps), Raft itself | receiving and validating packs, building and uploading layers, serving fetches and clones, flushes and compaction |
| Memory | the state store's budget (block cache and memtables), and a Raft group per repository | views, the buffer pool |
| Network | inline bytes in from the compute and to the other ledger nodes, out to every subscriber | layers from and to the object store; pack data to and from clients |

The number of repositories, their refs (on disk) and the inline backlog set the
size of the ledger. Data does not. The state store's budget sets its memory,
which should hold the refs that are written. The fsync latency of the ledger is
the commit latency, so ledger nodes want fast disks.

## Protocol between nodes

Nodes talk gRPC on their own port. This is `node.internal_listen`, the address
that peers find in the registry. It also serves `GET /status`. It is separate
from the smart HTTP port (`listen.git`) and the API port (`listen.api`, also
gRPC), so each can be exposed or firewalled on its own. Expose the internal port
only to cluster members.

Protobuf messages are the contract between the roles. They are the types of
the state machine too. Commands, repository state and outcomes are protobuf
messages in the log, in the stream to subscribers, in the RPCs and in the state
objects in the object store. Inline layer bytes travel as bytes, not base64.

Two services run between nodes:

- **The ledger service**, which the compute uses:
  - Activate makes the log of a repository live. It creates the log if asked,
    with a HEAD for an explicit create.
  - Delete, and describe (the HEAD and last push of a repository, after a read
    barrier).
  - List returns the repositories that a ledger node hosts, deleted ones
    included ([Lifecycle](lifecycle.md#listing)).
  - Propose sends a command and answers with its outcome once applied.
  - Read barrier waits for everything committed so far.
  - Follow streams the state of a repository, then every change to it: refs
    set or deleted, HEAD, a layer edit, the version, the end of the incarnation.

  Subscribers patch their state with the changes. They need neither the
  commands, nor log indices, nor the rules of the state machine
  (compare-and-swaps, rejections). These stay with the ledger. A node that runs
  both roles calls its own ledger in-process through the same interface. A
  compute keeps one connection per core to each ledger node, because one task
  drives one HTTP/2 connection, and every call to that node would wait on it.
  Messages can be as large as a state with its inline layers, in both
  directions. A subscription that breaks is followed again from a new state. The
  same applies when the ledger drops a subscription because the compute fell 256
  updates behind.
- **The Raft service**, between ledger nodes: batches of Raft messages for many
  logs per request (each Raft message travels as opaque bytes),
  wake-ups and incarnation announcements
  ([Consensus](consensus.md#batched-transport)).

## What the compute does with it

- **Pushes** are received, validated and turned into a layer on a compute node.
  The connectivity check needs the view of the compute node, which follows the
  log. The node takes a read barrier for the ref advertisement and proposes the
  commit entry ([Write path](write-path.md)).
- **Maintenance** (flushes and compaction) runs on one compute node per
  repository ([Maintenance](maintenance.md)).
- **Checkpoints** need only the applied state. The state compare-and-swap
  protects them. They stay on the leader, which is a ledger node. There is no
  log upload.

### Maintenance leases

Maintenance is safe from any node, because each swap is a manifest
compare-and-swap in the log. Two maintainers at once only waste a merge. A
lease, not leadership, decides which node runs it.

A compute node that has maintenance due for a repository it follows takes
`repos/<repo>/maintainer` in the object store by compare-and-swap. The holder renews
the lease while it has work, and other nodes ask again only after it could have
expired. Repositories without work cost no requests. A compute node keeps following a repository whose
inline layers are not flushed yet, so eviction cannot strand maintenance.

## Node registry

The node registry holds the ledger's members
(see [Membership](#membership)). Each ledger node writes `nodes/<id>` in the
object store when it starts, with its store id and `node.advertise_url`, and
rewrites it every 2 s. The modification time of the object is its heartbeat. A
node that was not seen for 10 s is not alive. Every node lists the registry as
often. Compute-only nodes only read it, because nobody needs to find them. A
subscription is a connection that the compute opens, and a maintenance lease
names its holder without a lookup.

The registry does three things:

- **Addresses.** Raft ids are random store ids, so they cannot be configured.
  The transport resolves the destination id of a message to a URL through the
  registry.
- **Members.** `ledger/members` lists the ledger's members and the nodes removed
  from it. Only the place where compute nodes send requests depends on liveness.
- **Finding the ledger.** A compute sends the requests of each repository to the
  live ledger nodes in the rank of the repository. It moves on while a node is
  unreachable or does not host the log. A node that runs both roles asks its own
  ledger first.

Safety never depends on the registry. The voters of a log change only through
the log. A stale copy of the members can delay a change, but it can never remove
a member. If the object store is unreachable, running incarnations are
unaffected, because addresses already learned are kept. Waking repositories
needs the object store anyway.

One discovery mechanism serves both roles. A static list of ledger URLs would
need a change of every node's configuration whenever a ledger node joins or
leaves.

## Membership

Every ledger member is a voter of every repository's log. There is no voter
placement. The members are a list in the object store (`ledger/members`, changed
by compare-and-swap) with
the nodes removed from it, as etcd keeps its member list. The list is in the
object store instead of a log of its own.

- **Adding a node** is starting it. A ledger node adds itself to the list when
  it starts.
- **Removing a node** is a decision of the operator (`enroute members --remove
  <id>`), because a node that is down looks the same as a node that is gone. The
  command is refused if the members left would not have a live majority, unless
  forced. Removal is for good. The removed id never rejoins, so a node that lost
  its disk, or one being retired, comes back only as a new node with a wiped
  data directory. A removed node that is still running drops every log it hosts.
  The leader of a log stops replicating to a node once its removal commits, so
  the node may never apply it. The logs that it led elect another leader.
- **Each log follows the list** through its leader, one Raft membership change
  at a time, and only after the leader committed an entry in its term (the fix
  for single-server changes). A member that is not in the log is added as a
  learner. The announcements of the leader tell it to host the log. It becomes a
  voter once it is within 100 entries of the commit index. A removed node is
  taken out, the leader too (it then drops the log, and the others elect a new
  one). Logs lose only nodes marked removed, so a node that acts on an old copy
  of the list can be late to add a member but never removes one.
- **After the list changes**, every node wakes the logs whose membership does
  not match it, 100 per tick (a second), so that idle repositories follow too.
  Each change keeps the log active, because a new learner hosts the log only
  when the leader announces it, and leaders announce only active logs.

To replace a node that lost its disk, start the new one (it joins every log as a
learner, then a voter), then remove the old one. The compute never touches the
membership of a log.

### A standalone node becomes the first member

A standalone node becomes the first member when it restarts on its data
directory with `node.advertise_url`. Its Raft id is its
store id, with or without a registry, and it created every log with itself as
the only voter. It registers and adds itself to the empty list. Every log
already matches the list, so the restart changes no voters, terms or votes of
any log. Every entry it committed alone is on its disk. Ledger nodes that start
later join each log one change at a time.

Two limits apply. The bucket must hold no other ledger. The move is one way:
restarted without `node.advertise_url`, a node whose logs have other voters
cannot reach them, and it commits nothing.

The ledger scales up, not out: every member holds every repository.

## One owner at a time

Raft rules out two leaders within an incarnation. Between incarnations:

- **Claims** are a compare-and-swap on the state object. Of concurrent claimants
  exactly one wins, and only the winner hosts.
- **Incarnations end only by deletion**, through their own log (a tombstone),
  never by timeouts. A voter that missed the end can never write again, because
  any leader has the tombstone ([Lifecycle](lifecycle.md)). The next incarnation
  is claimed only after the state object says that the old one was deleted.
  Otherwise a new incarnation would come only from the recovery of an operator.
- **An incarnation that did not end** is never taken over automatically. Its
  voters resume it, and Raft tolerates a minority of them being down.
- **Losing a majority of the ledger** is a disaster, as in any Raft deployment.
  The operator decides the recovery: take over from the last checkpoint in the
  object store, and lose what came after it (up to a minute or 1000 entries).
  Recovery is a manual step.
- **Freezing the ledger** is stopping its nodes and moving their disks (see
  below).

Other systems end old logs the same way. Raft groups that idle without ticking:
TiKV's hibernated regions, CockroachDB's quiesced ranges. Logs replaced by new
ones through a metadata store: BookKeeper ledgers (Pulsar), the virtual log of
Delos, the log generations of FoundationDB, and in theory Vertical Paxos. All of
these end the old log through a quorum of its own members (fence, seal, lock),
never by timeouts.

## Freezing the ledger

Every repository stays on its voters while it exists. A log that nobody writes
goes quiet within seconds ([Quiet logs](consensus.md#quiet-logs)). A wake frame
resumes it at once.

Enroute does not move idle logs to the object store. Only a majority of the
voters sees a log end, so a voter that fell behind could keep the ended
incarnation and serve it as live. Every wake would also go through the object
store, and the resident cost it saves is small.

**Freezing a ledger**, to move it to other machines or to stop it for a while,
is done with nodes, not repositories:

1. Stop the ledger nodes.
2. Copy the data directory of each one (its log store and state store, together:
   a volume snapshot, or an archive in the object store).
3. Later, start a node on each copy.

The identity of a node is its log store. Nodes find each other
through the [node registry](#node-registry), so the copies are the same nodes at
new addresses. Every repository resumes on the same voters with its Raft state
intact.

Two rules apply. A store must never run twice, because two copies could vote
twice in one election. The registry refuses a node whose store id another live
node registered from another address. This is a check by heartbeat age that
catches mistakes. It is not a guarantee. A node must also never be restored from
an older copy than it last ran with. It would forget votes and acknowledgements,
and its manifests could name layers that the [layer
sweep](maintenance.md#the-layer-sweep) has since deleted. Compute nodes hold
nothing that needs this. They leave and come back whenever they like.

### Layout

Per repository, the object store holds:

```text
repos/<repo>/state       the latest state: incarnation E, a log index, its voters, the state
                         machine (refs, manifest, inline layers); replaced by compare-and-swap
repos/<repo>/epochs/<E>  the state incarnation E started from (what a joining member hosts);
                         the same whoever claims E, written before the claim
repos/<repo>/maintainer  the compute node maintaining it and until when (a lease; the compute's)
repos/<repo>/sweep       the layer sweep's record (see Maintenance)
trash/<repo>/<E>         incarnation E was deleted; its layer files may remain (see Lifecycle)
```

The state object changes only by compare-and-swap on its version (`If-Match` on
the ETag; a conditional create for a new repository). Of concurrent writers
exactly one succeeds. A checkpoint is a swap from the version its writer last
saw. A leader whose incarnation was superseded (by a recovery of an operator)
learns it at its next checkpoint and stops hosting it. The local filesystem
store adds compare-and-swap with a lock file. S3 and the in-memory store
have it natively. Which repositories exist is what the object store says.

### Activation

A write, or a linearizable read, activates a repository that a node does not
host. The node reads the state object (one GET). Then:

- **No state object, and the request creates it:** the node claims incarnation
  1, with the ledger's members (read from the object store at the claim) as its
  voters. It writes the initial state (empty, and the voters: the first claimant
  to write it decides them, and later ones adopt it) and hosts it durably. It
  creates the state object, announces it (with the state, so peers host it
  before the vote requests) and campaigns. A lost race for the same incarnation
  leaves this node hosting it as a replica.
- **This node is a member:** it joins from the initial state. Raft catches the
  node up, and the leader of the log adds it as a voter if it is not one yet.
  This is safe whether or not anyone else hosts the log. A member that does not
  host the incarnation never acknowledged anything in it. A member that lost its
  log has a new id, and a removed node never comes back. A majority that elects
  a leader therefore always includes every committed entry.
- **This node is not a member:** the node refuses, and a compute node then asks
  another ledger node. A live incarnation is never taken over: its voters resume
  it, and Raft tolerates a minority of them being down. If a majority of the
  voters lost their logs, acknowledged writes newer than the last checkpoint are
  gone. Recovery from that checkpoint, as a new incarnation, is the decision of
  an operator.

### Checkpoints

The leader of a repository swaps its state into the state object when the state
changed. A swap happens:

- once the log goes quiet (a few seconds after the last write);
- after 1000 entries;
- at most `ledger.checkpoint_age` (a minute) after the change.

The swap time is a random point in that period, so a burst of checkpoints (after
a restart, or a change of leaders) spreads out. Up to
`ledger.checkpoint_concurrency` (8) run at once. The engine reports what
changed: repositories whose state changed or whose log this node started
leading, and logs that went quiet. The engine ticks only logs that are not
quiet, so an idle repository costs no work at all.

Checkpoints bound what the recovery of an operator would lose. They let a leader
whose incarnation was superseded find out. The layer sweep and the readers of
the object store go by them.

### Costs and limits

- Creating a repository needs the object store. Writes to an existing one do not.
- Every ledger node holds every repository. Adding ledger nodes adds fault
  tolerance, not capacity.
- A deleted repository's log stays on the ledger, quiet, until the repository is
  created again ([Lifecycle](lifecycle.md)).
