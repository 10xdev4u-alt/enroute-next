# Storage

Enroute embeds a storage engine that is built like a modern database. Each
repository is a set of immutable layers in an object store. Each repository also has its own replicated log for
its refs and its layer list. There is no external database.

## Nodes

An Enroute process is a node. A node runs one of three roles:

- **Ledger.** A ledger node holds the replicated logs. It needs a durable local
  disk. Every ledger node is a voter in the log of every repository.
- **Compute.** A compute node does the Git work. It serves Git and the gRPC API. It
  keeps only caches that it can rebuild, so it needs no durable disk.
- **Both.** The default. One process runs the ledger and the compute.

A compute node reads the current state of a repository from the ledger. Compute
nodes find ledger nodes through the object store. See
[Hosting](../operate/hosting.md).

## Layers

A push or an API change adds a layer to the repository. A layer holds Git
objects, a commit graph, and an index. Layers never change after Enroute writes
them. In the background, Enroute merges small layers into larger ones, and it
removes layers that nothing refers to.

The layer list and the refs are the state of the repository. The ledger keeps
that state. Enroute writes a checkpoint of it to the object store within
a minute of a change.

## Durability

A small push is acknowledged when its layer is in the replicated log on a
majority of ledger nodes. Enroute then flushes the layer to the object store in
the background. A large push uploads its layer to the object store first.

Until the flush, the data is as durable as the logs on the ledger nodes' disks.
If a majority of ledger nodes lose their disks, recovery is manual, from the
last checkpoint, and up to about a minute of writes can be lost. See
[Maintenance](../operate/maintenance.md).

## Reads

Reads are linearizable: they see every write that finished before the read
began, through any compute node. This is true for the gRPC API reads, for the
refs of a fetch (`ls-refs`, or the ref advertisement of protocol v0), and for
the ref advertisement of a push.

## Repositories

`CreateRepository` creates a repository. A push to a key that does not exist
also creates one, unless `git.create_on_push` is `false`. A fetch never creates
one. `DeleteRepository` frees the key at once. Enroute reclaims the stored layers in
the background after a retention delay. A new repository can use the key
immediately.

## Retention

Enroute does not garbage-collect objects made unreachable by branch deletion.
Delete the repository to remove its data. Configure encryption at rest in the
object store and on the ledger disks; Enroute does not provide it. See
[Storage backends](../operate/storage-backends.md).
