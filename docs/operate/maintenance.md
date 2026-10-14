# Maintenance

Enroute has no scheduler to set up. Each node runs its upkeep in the
background.

## Automatic work

For each repository, one compute node runs this work:

1. Flushes small layers from the replicated log to the object store.
2. Merges layers of a similar size into a larger one. A large layer merges
   again only after the repository grows by about its size.
3. Removes layer files that no repository state names, once they have been
   unused for six hours. The delay lets slow readers finish.

Ledger nodes write a checkpoint of each changed repository to the object store
within a minute.

When you delete a repository, the key is free at once. The sweep reclaims the
layer files in the background after the same delay.

Merging layers does not repack objects or reclaim objects from deleted
branches. Delete the repository to reclaim that data.

The keys in `[layers]` and `[ledger]` tune this work. The defaults suit most
deployments. See [Configuration keys](../reference/configuration-keys.md).

## Add ledger nodes

Start a new ledger node with the same `bucket` and an `advertise_url`. It
joins the ledger and becomes a voter in every repository's log. Adding a node
adds fault tolerance, not capacity.

## Remove a ledger node

A node that lost its disk comes back as a new node. The old node stays a
member until you remove it. A dead member counts against the majority, so
remove it.

Use `enroute members`. Give it the same configuration as the nodes. It reads
only the `[bucket]` table, but the whole file must load, so the variables
that the file names must be set.

```sh
enroute members --config file:///etc/enroute/enroute.toml
enroute members --config file:///etc/enroute/enroute.toml --remove 2
```

Add `--json` to print the members and the removed nodes as JSON.

In a container, run it with `docker exec`:

```sh
docker exec <container> enroute members --config file:///etc/enroute/enroute.toml
```

Without `--remove`, the command lists the members and the removed nodes. With
`--remove <id>`, it removes one node for good, and the logs then drop it. The
command refuses to remove a node if the members that remain would not have a
live majority. `--force` removes it anyway.

## Recover from losing a majority

If a majority of ledger nodes lose their disks, the repositories on them cannot
accept writes. Recovery is manual, from the last checkpoint in the object
store. Up to about a minute of writes can be lost. There is no tool for this
yet.

Prevent it. Keep ledger disks durable and spread the nodes over failure
domains.

## Monitoring

Background work has no metrics. Enroute logs its failures. Alert on `ERROR`
logs from each node.
