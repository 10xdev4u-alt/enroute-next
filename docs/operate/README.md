# Operate Enroute

Use these pages to deploy, configure, and maintain Enroute.

An Enroute deployment needs an object store and a durable local disk for each
ledger node. It needs no external database. Expose the Git listener behind
TLS. Keep the API listener reachable only by your application.

| Page | Use it for |
| --- | --- |
| [Hosting](hosting.md) | Node roles, scaling, image tags, and operator responsibilities |
| [Deployment profiles](deployment-profiles.md) | The three deployment models, what each survives, and how to move between them |
| [Configuration](configuration.md) | Main configuration and secrets |
| [Storage backends](storage-backends.md) | The object store and the node disk |
| [Security](security.md) | Listener exposure, hook signatures, and what Enroute does not isolate |
| [Maintenance](maintenance.md) | Automatic upkeep, ledger members, and recovery |
| [Observability](observability.md) | Traces and logs |

For a production deployment, read Hosting, Deployment profiles, Storage
backends, and Security before Configuration. Then set up observability. Review
[Limitations](../reference/limitations.md) before deploying.
