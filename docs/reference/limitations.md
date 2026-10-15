# Limitations

Enroute is a `0.x` release with the following current limits.

## Compatibility

- There is no backwards compatibility between `0.x` releases.
- The `v1alpha1` contract is unstable until Enroute 1.0. Messages, fields, and
  services can change in any release.
- The object store format and the configuration keys can change in any
  release.
- Release `0.3.0` replaced the storage engine and has no migration. Storage
  from `0.2.0` is not readable, so start clean and push again. A later format
  change needs the same.
- There is no supported downgrade.

## Transport and synchronization

- Git smart HTTP is supported; SSH and `git://` are not.
- Enroute does not terminate TLS or authenticate API callers.
- Fetches that ask for `thin-pack` and `PushToRemote` send thin packs. The
  receiving Git must be version 2.50 or later. An earlier version can reject
  the pack with `REF_DELTA at offset N already resolved`.
- `PushToRemote` supports push-only `https` remotes with basic or bearer
  credentials. It does not fetch, use SSH, persist remotes, or follow redirects.
- Remote pushes are synchronous: one hour total and one minute without remote
  output. Progress is not reported.

## Data model and storage

- Production requires a durable disk for each ledger node, and an S3-compatible
  object store or a `file://` bucket. A `file://` bucket serves only nodes on
  one machine.
- There is no procedure yet to move a deployment from a `file://` bucket to an
  object store.
- Enroute does not encrypt data at rest or collect unreachable branch objects.
- SHA-1 is the only object format. Enroute creates no merge commits.
- Collision detection recognizes an attack only where its blocks open the message being
  hashed. An object carries git's header in front of its data, so a published attack that is
  replayed as an object is not refused; the pair that is published does not collide as an
  object either. See [#78](https://github.com/10xdev4u-alt/enroute-next/issues/78).
- `visible_refs` hides ref names, not objects.
- Reads are bounded: 100 repositories or commits per page and 50,000 tree or
  diff entries.
- `ListRepositories` compares a `prefix` byte for byte and returns keys in byte
  order.
- `updated_at` in `ListRefs` comes from the clock of the node that proposed the
  change. It can go back when node clocks differ.

## Operations

- There are no quotas, rate limits, per-caller concurrency limits, or
  metrics for background work.
- Ledger nodes scale up, not out. Each one holds every repository, so more
  ledger nodes add fault tolerance and not capacity. Compute nodes scale out.
- A push is acknowledged when a small layer is in the replicated log on a
  majority of ledger nodes. The log is the only copy until Enroute flushes the
  layer to the object store.
- Losing a majority of ledger nodes needs manual recovery from the last
  checkpoint. Up to about a minute of writes can be lost. There is no tool for
  this yet.
- Removing a ledger node is a manual step, `enroute members --remove`. See
  [Maintenance](../operate/maintenance.md).
- The `latest` image tag moves and can be rebuilt. A version tag names one
  image and is not rebuilt.
