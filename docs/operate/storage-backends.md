# Storage backends

Enroute keeps data in two places: an object store and the local disk of each
ledger node.

## Object store

Enroute uses object-store URIs such as `s3://`, `gs://`, `az://`, `https://`,
`file://`, and `memory://`.

```toml
[bucket]
uri = "s3://enroute-objects/prefix"

[bucket.credentials]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
```

Prefer workload credentials, such as an instance role or IRSA, over explicit
credentials. Put backend options in the URI query. Credentials in a URI query
are rejected. A `file://` bucket takes no query.

The bucket holds repository layers, checkpoints of repository state, and the
registry that nodes use to find each other. All nodes of a deployment must
share one bucket.

On S3, add a lifecycle rule that aborts incomplete multipart uploads
(`AbortIncompleteMultipartUpload`, after one day). A large upload that a crash
interrupts leaves parts that no listing shows. Nothing else removes them.

## Local directory

A `file://` bucket keeps the objects in a directory on the node. Use it to run
Enroute on one machine, such as a small server or a VPS, with no object store.

```toml
[bucket]
uri = "file:///var/lib/enroute/objects"

[node]
data_dir = "/var/lib/enroute/data"
```

- All nodes that use the bucket must run on the same machine. Do not put the
  directory on a network filesystem, such as NFS.
- Enroute syncs each object to disk before a write completes.
- If the bucket and `data_dir` are on the same filesystem, a local copy of a
  layer is a hard link to its file in the bucket. The layers then use disk
  space one time.
- The bucket and `data_dir` are the only copies of your data. If you lose the
  disk, you lose every repository. Make backups of the disk, for example with
  volume snapshots.

## Node disk

`node.data_dir` holds the replicated logs and the state of a ledger node. Put
it on a disk that persists, such as a cloud volume. In the image, put it under
`/var/lib/enroute`, which is a volume.

A node that loses this directory starts again as a new node. It does not
recover its old identity. Do not run two nodes from copies of one directory.

Compute nodes keep only caches in `data_dir`. Enroute rebuilds them when they are
lost.

## Production requirements

Production deployments need a persistent disk for every ledger node, and one of
these buckets:

- An S3-compatible object store.
- A [local directory](#local-directory) on a persistent disk, for a deployment
  on one machine.

Use `memory://` only for development.
Configure encryption at rest and access control in the storage providers;
Enroute does not provide them.
