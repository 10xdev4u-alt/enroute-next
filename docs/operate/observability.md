# Observability

Enroute exports OpenTelemetry traces over OTLP when `[telemetry]` provides an
endpoint. Without that table, spans are written only to the console.

```toml
[telemetry]
endpoint = "https://otlp.example.com/v1/traces"
sample_ratio = 1.0

[telemetry.headers]
authorization = "Bearer ${OTEL_TOKEN}"
"x-dataset" = "enroute"
```

| Key | Default | Meaning |
| --- | --- | --- |
| `telemetry.endpoint` | none | Full OTLP traces URL |
| `telemetry.headers` | none | Collector headers; values are secret |
| `telemetry.sample_ratio` | `1.0` | Head-sampling ratio from `0.0` to `1.0` |

Invalid ratios or header names stop startup. `RUST_LOG` controls console logs;
exported spans retain an `INFO` minimum level.

The `actor` from `authorize` is attached to the span, so use an internal
identifier rather than a credential or email address.

Background work, such as flushes, compaction, and the layer sweep, has no
metrics. Enroute logs its failures. Alert on `ERROR` logs from each node.

## Timing logs

To see where a request spends its time, turn on debug logs for these targets.
Each line gives the times in microseconds (`*_us`). The fields can change in
any release.

| Target | What it times |
| --- | --- |
| `enroute::request_timing` | Each Git request, and the spool of a push body |
| `enroute::push_timing` | The stages of a push |
| `enroute::read_timing` | Linearizable reads: the follow, the read barrier and the catch-up |
| `enroute::ledger_call` | Each call from a compute to the ledger, split into network and handler time |
| `enroute::ledger_timing` | Rounds and apply batches on a ledger node |

```sh
RUST_LOG=info,enroute::push_timing=debug
```

With `enroute::ledger_call` on, calls to the ledger carry `x-enroute-*-us`
metadata so that the two nodes can split the time.

## Node status

Each node serves `GET /status` on its internal listener, `node.internal_listen`
(`0.0.0.0:7400` by default). The response is JSON diagnostics of each role
that the node runs: the compute's repository views and storage, and the
consensus engine under `consensus`. The fields are for people and can change
in any release. Do not parse them in automation.

```sh
curl http://127.0.0.1:7400/status
```

A ledger node also serves `GET /status/repos/<repo>`: what the node knows of
the repository's log. The response has the node ID (`node`), the leader that
the node knows of (`leader`, or `null`), the Raft `term`, whether the node
leads the log (`leading`), and the `version` of the state that the node has.
A node that does not host the log answers `404`.

```sh
curl http://127.0.0.1:7400/status/repos/acme/widgets
```

Each node answers from its own view. A paused or partitioned leader still
says that it leads. To find the leader, ask each ledger node, and take the
`leader` that a majority names at the highest `term`.

The Git and API listeners do not serve `/status`. The internal listener also
carries the gRPC between ledger nodes, so keep it reachable only inside the
cluster. For a liveness check from outside, use `GET /healthz` on the Git
listener.
