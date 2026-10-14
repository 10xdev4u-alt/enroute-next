# Hosting

Enroute is currently self-hosted. Managed hosting is planned.

## Image

Images are published at `ghcr.io/enroute-sh/enroute` for `linux/amd64` and
`linux/arm64`. Releases use `0.x` version tags.

| Tag | Meaning |
| --- | --- |
| `latest` | Current release; moves with each one |
| `<version>` | One release; not rebuilt |
| `sha-<commit>` | A specific source commit; never reused |

The `latest` tag moves and may be rebuilt. A version tag names one image, so
use `<version>` or `sha-<commit>` when you need an immutable image. The image
contains the server and the proto files in `/usr/share/enroute/proto`. It
declares `/var/lib/enroute` as a volume for the node's data. The entrypoint is
the server.

A minimal start reads the configuration from a mounted directory:

```sh
docker run -d -p 8080:8080 -p 50051:50051 \
  -v /srv/enroute/config:/etc/enroute:ro -v enroute-data:/var/lib/enroute \
  ghcr.io/enroute-sh/enroute:<version> --config file:///etc/enroute/enroute.toml
```

Set `node.data_dir` under `/var/lib/enroute`, so that the node's data is on
the volume. Mount the directory, not the file. An editor that saves by renaming replaces
the inode, and a single-file bind mount keeps the old one. To run a local
node for development instead, see [Run the image](../quickstart.md#run-the-image).

## Upgrades

There is no backwards compatibility between `0.x` releases. Read the release
commit before you move, and plan an upgrade as a deployment change rather than
a restart.

- A release that changes the storage format reads nothing that the previous
  release wrote, and has no migration. Start with an empty bucket and empty
  node disks, then push your repositories again.
- The `v1alpha1` contract can change. Generate your client again from the
  proto files the new image carries.
- Configuration keys can change. Start the new image against your
  configuration before you send traffic to it.

## Deployment model

A node runs one of three roles, set with `node.role`:

| Role | Runs | Needs |
| --- | --- | --- |
| `both` | The ledger and the compute. This is the default | A durable disk, and the object store |
| `ledger` | The replicated logs only | A durable disk, `node.advertise_url`, and the object store |
| `compute` | Git and the gRPC API only | The object store, and a disk for caches |

The ledger holds each repository's refs and layer list in a replicated log.
Every ledger node votes in every log. The compute does the Git work and keeps
only caches that Enroute can rebuild.

One `both` node is the simplest deployment. To replicate, run three ledger
nodes or more. Set `node.internal_listen` and `node.advertise_url` on each one,
and point them all at one `bucket`. A repository stays writable while a
majority of its ledger nodes is up. Nodes find each other through the bucket.

Keep the internal listener (`7400` by default) reachable only inside the
cluster. Every node serves `GET /status` there.

Add compute nodes behind a load balancer. Other nodes never dial a compute node.
It finds the ledger nodes through the bucket.

[Deployment profiles](deployment-profiles.md) gives the three supported
models, with a configuration for each node, what each model survives, and how
to move a live deployment from one model to the next.

## Scaling

- **Ledger nodes scale up, not out.** Each ledger node holds every repository.
  More ledger nodes add fault tolerance, not capacity. To add capacity, give
  the nodes more CPU, memory, and disk.
- **Compute nodes scale out.** Add compute nodes for more Git and API traffic.

Configure [storage](storage-backends.md), [security](security.md),
[maintenance](maintenance.md), and [observability](observability.md) before
deploying.

## Operator responsibilities

- Terminate TLS for the Git listener.
- Keep the API listener reachable only by your application.
- Protect and rotate the hook signing key.
- Keep the disk of each ledger node durable.
- Apply quotas, rate limits, and concurrency limits.

Review [Limitations](../reference/limitations.md) before production use.
