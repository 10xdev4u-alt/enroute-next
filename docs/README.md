# Enroute documentation

Enroute is a fast, programmable Git server that is easy to operate. It is
built like a modern database: compute is separate from storage, and
repositories are in an object store. Enroute handles Git over HTTP, Git data,
and a gRPC API. Your application owns repository names, users, permissions,
and policy.

## Choose a starting point

| If you need to… | Read… |
| --- | --- |
| Run Enroute locally | [Quickstart](quickstart.md) |
| Build a code-hosting application | [Guide](build/README.md) |
| Understand responsibilities and boundaries | [Concepts](#concepts) |
| Assess the current limitations | [Limitations](reference/limitations.md) |

## Guides

[Build a code-hosting application](build/README.md) is a seven-part TypeScript
and Next.js tutorial. It creates an application that can create repositories,
serve Git traffic, and display files, history, and diffs. The same API and hook
contract work with any stack that supports gRPC and HTTP.

After the tutorial, use [Patterns](patterns/README.md) for common product
features such as branch protection, merge requests, CI, and mirroring.

## Concepts

See [Concepts](concepts/README.md) for the data model, interfaces, hooks, and
storage design.

## Operating Enroute

See [Operate Enroute](operate/README.md) for deployment, configuration,
security, storage, cluster care, and observability. To choose between one
node, one ledger with compute nodes, and a replicated ledger, see
[Deployment profiles](operate/deployment-profiles.md).

## Reference

See [Reference](reference/README.md) for API, hook, protocol, configuration,
and limitation details.

## Architecture

See [Architecture](internals/README.md) for how Enroute works inside. The design pages are the [overview](internals/overview.md),
[consensus](internals/consensus.md),
[ledger and compute](internals/ledger-and-compute.md),
[storage](internals/storage.md), [write path](internals/write-path.md),
[read path](internals/read-path.md), [serving](internals/serving.md),
[lifecycle](internals/lifecycle.md), and [maintenance](internals/maintenance.md).
[Jepsen tests](internals/jepsen.md) describes how the suite tests consistency
under faults.
