# Consensus

Each active repository has its own replicated log. This page describes the log,
its storage, and the engine that drives all logs on a node. For the roles that
host the logs, see [Ledger and compute](ledger-and-compute.md).

## One log per repository

Every repository has its own replicated log. The state machine of the log is the
refs and the layer manifest of the repository. The voters of the log host it
for as long as the repository exists. The object store records which
repositories exist, and a checkpoint of each state ([Freezing the
ledger](ledger-and-compute.md#freezing-the-ledger)).

A log is an *incarnation* of its repository. Its log id comes from the
repository and an epoch. Incarnation 1 starts at creation. The next one starts
when the repository is created again after a deletion
([Lifecycle](lifecycle.md)), or after an operator's recovery. A node keeps the
identity of a log with its state, so a recovering node knows which log is which.
The voters are the ledger's members, kept in step as nodes join and leave
([Membership](ledger-and-compute.md#membership)).

One log for each repository has these properties:

1. **One physical log per node, many logical logs.** All logs on a node share
   one log store. One group-committed fsync covers every repository written in a
   batch. Without this, many small logs would destroy group commit, and
   throughput rests on group commit.
2. **Zero idle cost.** A repository that nobody writes has no timers, heartbeats
   or leases. Its log goes *quiet* within seconds: the engine stops ticking it,
   as TiKV hibernates regions.
3. **Leadership on demand.** A log gets a leader when a write arrives. Writes
   stay one round trip while the same leader keeps writing.
4. **Cheap per-repository state.** The state machine lives in the state store on
   disk, so a node can host millions of mostly idle repositories.
5. **Linearizable reads without a log write**, through ReadIndex, per
   repository, on demand.
6. **No coupling between repositories.** A hot repository does not slow others,
   because it shares no group with them.

Replication and flushing are node-level. A node does one log store write per
fsync. It sends one RPC per peer, which carries entries and acknowledgements for
many repositories, as TiKV batches its Raft messages.

Ref updates to *different* refs never contend. They share only log throughput,
which group commit amortizes. Updates to the *same* ref serialize, as Git
semantics require.

### Why Raft

Enroute runs Raft, in the etcd/raft design, with quiet logs. The protocol core
leaves log storage, transport and ticking to Enroute, and these are what
requirement 1 needs. The design is proven at TiKV scale. The alternatives each
cost more:

- Megastore-style Multi-Paxos has no standing leader, but Enroute would own the
  protocol's correctness.
- A CASPaxos register per repository ships the whole state with every update. It
  fails for repositories with many refs.
- A Raft design that keeps its own storage and transport per group has too much
  per-group overhead for millions of logs, and no cross-group batching.

## Commands

- **Commit.** One entry atomically adds layers, removes layers, and applies ref
  compare-and-swaps, with an optional atomic flag. Removing layers makes it a
  manifest compare-and-swap, which flushes and compaction use. A Git push stamps
  its time, which becomes the last push time if a ref moves. This is the
  proposer's clock, so applying stays deterministic, and the time never goes
  back. Every commit with ref updates stamps the update time of each ref it
  sets ([Ref update times](lifecycle.md#ref-update-times)).
- **Advance the upload generation.** The layer sweep's next upload generation
  ([Maintenance](maintenance.md#the-layer-sweep)).
- **Delete.** The tombstone ([Lifecycle](lifecycle.md)).

Every command may name the incarnation it was prepared against. The state
machine refuses a command for another incarnation.

Versions increase across incarnations. A change sets the version to its log
index + 1, or to one more than the previous version if that is larger. A new
incarnation's log starts again from 1.

## The engine

The consensus core is a Raft implementation in the etcd/raft design. Enroute
owns the storage, transport and scheduling around it.

### Log store

Each node has one log store, built for many Raft groups, as TiKV's is.

- Each log is a group under its log id. The group holds its entries, its hard
  state, and where it starts (the metadata of its latest snapshot). The hard
  state and the start are key-value pairs.
- The engine keeps the terms, the hard state and that metadata in memory.
  Entries are read from the store.
- Values are protobuf messages.
- The store group-commits concurrent writes. On macOS its syncs flush the drive
  cache with `F_FULLFSYNC`, like the other durable writes of Enroute. It recovers after a crash and purges old files in
  the background.
- A log that holds old files back is compacted on request, as far as its durably
  applied state and pinned inline layers allow. A log that is no longer hosted
  is dropped.

### Event loop

One event loop drives all logs. Each round it:

1. takes the pending inputs (proposals, batched peer messages, read barriers,
   ticks);
2. collects the ready work of every touched log;
3. sends the messages that do not need durability (a leader's appends go out
   before its own fsync, as Raft allows);
4. hands committed entries to the apply thread;
5. persists the round with one log store write.

Only then does the loop update each log's storage view. The view shows only
durable state. The rest is in the unstable log of the Raft core. The loop then
reports the persisted rounds to the core and sends the messages that require durability
(append acknowledgements, votes). Inputs that arrive during the fsync go into
the next round. The loop does not overlap the fsync with the next round.

### State store and apply thread

Every log's state machine is in the node's *state store*. It is one LSM
tree next to the log store, as
TiKV keeps the data of its regions. It has no write-ahead log: the Raft log
already holds every applied entry, so recovery replays what the tree did not
flush.

- **Keys.** Each ref, the HEAD, each layer of the manifest and the counters are
  keys under the log. A layer's key is its first id, which is unique because
  every layer has at least one object id. A root key holds the identity of the
  log, its applied index and its version.
- **Batches.** The apply thread applies committed entries in atomic batches. A
  batch also records the entries as applied, unsynced. A command reads only the
  refs it compare-and-swaps, and the manifest for replacements. Its cost does
  not grow with the repository, and nothing of a repository's state is resident.
- **Apply thread.** Applying runs on a thread of its own, as TiKV splits its
  raft and apply pools. The loop hands it each log's committed entries in order.
  It also hands over snapshots, new logs' states, deletions and new subscribers.
  The loop then goes on with consensus. The apply thread answers the proposers
  of the entries and notifies subscribers.
- **Run of entries.** Each run is one write to the store. Before the write, the
  thread reads every ref and counter that the run touches in one go, from
  several threads. Entries apply against what was read, not against one disk
  read at a time.
- **Sync.** The writes collect in the WAL buffer of the store, which is written
  out and synced every 100 ms. After each sync the thread reports what is
  durable. Log truncation goes by this report only. The store does not write
  each batch out to the OS on its own, because on macOS a write waits behind the
  full fsync of the log store on another thread.
- **Snapshots.** Raft snapshots are made from the store when a follower needs
  one. Whole states (a new subscriber's, a checkpoint's) are read from it.
- **Versions.** The apply thread publishes each repository's version once per
  batch. It answers proposers only after it publishes.

Three rules keep the two stores in step:

- A log is truncated only up to an entry that the state store has durably
  applied. It is not truncated past an inline layer that is not flushed at that
  point. The apply thread reports both after a sync.
- A snapshot from a leader, or the state of a new log, is durable in the state
  store before the log is reset to it. The loop waits for the apply thread
  before that write.
- A restart replays each log's committed entries after the applied index of its
  state.

### Inline layers stay in the log

An inline layer's bytes travel in the log entry of its push. The state store
only *pins* that entry. The log is not truncated past the oldest pinned entry
until the layer is flushed. Copying the bytes into the state store would make
them most of its writes and make it stall, so the bytes are written once.

While the object store is down, pins hold logs back. The inline cap bounds this.
The log store then rewrites the held entries out of old files itself, instead of
keeping the files. A snapshot carries the bytes, because the follower has no
such entry. The follower keeps them in its store until the flush.

### Node identity and discovery

The Raft id of a node is its **store id**. The node creates it at random with
its log store and keeps it there. A node that loses its disk
comes back as a new node. It can never vote in a log after it forgot what it
voted for and acknowledged. Raft safety rests on that state surviving.

Nodes find each other through the [node
registry](ledger-and-compute.md#node-registry) in the object store. A node
without `node.advertise_url` is standalone: it is the only voter of its logs,
and it has no registry.

### Membership changes

Membership changes go through each log, one at a time
([Membership](ledger-and-compute.md#membership)). A change takes effect in
the Raft core when the loop hands it to the apply thread. The thread records the
voters and learners of the log with its state, in the identity of the root. A
restart finds them as of the applied index.

### Batched transport

All messages for all logs that go to one peer travel in one call per batch. The
call is on the Raft gRPC service, on the internal port, over one HTTP/2
connection per peer.

The transport addresses peers by id and resolves them through the registry. Each
log sends only to its own voters and learners. The transport drops a message for
another id, such as a node that had the address before and lost its disk.

### Quiet logs

The engine ticks only recently active logs. Real traffic (proposals, appends,
votes) keeps a log active. Heartbeats do not. Leaders keep heartbeating longer
after activity than followers keep their election timers running. An idle log
therefore goes quiet without spurious elections. Pre-vote is a further
safeguard. A node with work for a log whose leader it does not know sends a
*wake* frame.

On a 3-node cluster, 200 idle repositories exchange **zero messages**.

A quiet log still holds a Raft group and its root. Its state is in the state
store, not in memory, so it costs no CPU. Memory is the budget of the state
store (`ledger.state_memory`, 512 MiB by default: block cache and memtables).
It is not a cost per ref. Every system surveyed that runs many consensus groups
(TiKV, CockroachDB, YugabyteDB, DynamoDB, Neon's safekeepers) keeps idle groups
resident and silent in the same way.

### Proposals

Proposals carry a node-unique id in the entry context. Followers forward them to
the leader. Every replica applies every entry, so the node that
accepted a request answers it when it applies the entry. There is no separate
forwarding RPC. The node that activates a repository campaigns for its log at
once, so the first push does not wait for an election timeout.

### Incarnation announcements

A *host* frame announces an incarnation. It carries the initial
state. The node sends it to the other voters of the incarnation before the vote
requests, and the per-peer queue keeps the order. The peers host the log before
they are asked to vote. If peers learned of new logs from the object store
instead, creating a repository would be slow.

Leaders of active logs announce them again to their voters and learners every 2
s. The re-announcement has no state, which the node then reads from the object
store. A member that missed the first announcement, and a new learner, join this
way. A member can also join on demand, when asked for the repository.

### Read barriers

Read barriers use ReadIndex of raft. It is a read confirmed by a quorum, not a
lease: a quiet leader cannot detect that it was deposed. The engine answers the
read as soon as the read index is applied, without waiting for the fsync of the
round.

The Raft core drops, without an answer, a read-index request on a leader that
has not committed in its current term. So the engine holds reads until that
commit. It issues
again any read that is unanswered after 500 ms.

### Recovery

On start, the engine reads every log in the log store: where it starts, its hard
state, and its entries after that. It also reads the applied index of the state.
It then:

1. applies the committed entries after the applied index, synchronously, so the
   [materializer](read-path.md#materialization) sees current states. It stops at
   the next membership change, which the Raft core must apply itself. The engine hands
   that entry and the rest over in the first round of the log;
2. resumes normal operation.

A state ahead of its log (a crash between installing a snapshot and resetting
the log) resets the log. A state behind the start of the log would mean that
entries were lost, and the node refuses to start. The node drops incarnations
that were superseded elsewhere meanwhile, once it has checked the object store.
In a one-node cluster, every durable entry counts as committed.

Checkpoints run per repository on the ledger node that leads its log
([Checkpoints](ledger-and-compute.md#checkpoints)). Layer maintenance runs on
the compute node that holds the maintenance lease of the repository
([Maintenance leases](ledger-and-compute.md#maintenance-leases)).
