# wasmpoc

Proof of principle: the CHASM framework, the activity library, and a minimal workflow archetype
(activities and timers only), compiled to a `GOOS=wasip1` module that a Rust host drives through
the worker and client RPCs.

The packages here are copies of server packages with dependencies cut. Each cut corresponds to a
refactoring of the real server, listed below. Nothing outside `wasmpoc/` is changed.

## Build and run

```sh
go test ./wasmpoc/localserver/
./wasmpoc/build-module /tmp
```

The Rust host is `local-server-host/` on branch `local-workflow-progress` of
[dandavison/temporalio-sdk-core](https://github.com/dandavison/temporalio-sdk-core/tree/local-workflow-progress/local-server-host):

```sh
(cd local-server-host && cargo run --release -- /tmp/local-server.cwasm)
```

The Go test and the host both play the worker with hand-built requests and check histories I
wrote by hand. They check the module and its ABI, not that the histories satisfy a real SDK
worker.

`build-module` produces `local-server.wasm` (a reactor module, see `cmd/chasmwasm`) and
`local-server.cwasm` (precompiled by wasmtime 48 with the feature set of the host's runtime-only
wasmtime build). The host runs one workflow (an activity and a timer) and prints the history,
per-call latency and peak RSS.

## Measurements

darwin/arm64, Go 1.27.1, wasmtime 48.0.2, stripped (`-ldflags="-s -w"`).

| | wasm | wasm gzip | cwasm | cwasm gzip | host RSS |
|---|---|---|---|---|---|
| unmodified `chasm` + `chasm/lib/activity` | 139 MB | 21.6 MB | 267 MB | 54.5 MB | ~250 MB |
| this module (chasm, activity, workflow, local server) | 25.6 MB | 4.2 MB | 51.0 MB | 9.6 MB | 45.8 MB |

The first row is from an earlier spike (session `claude:406de489`), not rebuilt here.

The host binary, stripped, including the wasmtime runtime (no Cranelift), WASI and prost types, is
4.1 MB. Loading the cwasm and initializing takes 8–45 ms; warm RPCs take 0.1–0.4 ms.

Of the 25.6 MB, about 21 MB is protobuf generated code and descriptors, measured by building
modules that only import packages:

| imports | wasm |
|---|---|
| `api/history/v1`, `api/command/v1` | 13.8 MB |
| + `workflowservice/v1` messages | 18.2 MB |
| + server `api/persistence/v1` | 21.2 MB |
| + everything else | 25.6 MB |

## The refactorings the cuts correspond to

1. `chasm` defines its own physical task types; `service/history/tasks` wraps them, instead of
   `chasm` importing the history service's task package.
2. `chasm.Library` loses `RegisterServices` and the Nexus service methods; registering gRPC and
   Nexus services is server wiring.
3. The legacy workflow bridge (`MSPointer` and its `NodeBackend` methods) moves out of `chasm`.
4. `chasm` encodes its data and task blobs as proto3 itself rather than importing
   `common/persistence/serialization`; the codec's JSON support stays in the persistence layer.
5. The `common` root package stops being imported by low-level packages (`chasm`,
   `transitionhistory`, `history/consts`, `namespace`): `CloneProto` and constants move to leaf
   packages. The root package imports the admin, history and matching service stubs.
6. `common/log` without the Go SDK logger adapters; `common/metrics` without its OpenTelemetry,
   Prometheus, tally, statsd, gRPC and fx implementations; `common/namespace` without the registry.
7. `common/payload` and `searchattribute/sadefs` encode JSON payloads themselves instead of via the
   Go SDK's data converter, whose payload visitor imports every API service (16 MB of wasm).
8. Log tags hold a key and value; the zap logger builds the `zap.Field` (3.5 MB of wasm).
9. CHASM libraries do not import `common/resource`, `dynamicconfig` or fx: the activity library
   takes a `Config` of plain funcs and a one-method `MatchingClient`.
10. CHASM library handlers take library-owned request types instead of `historyservice` and
    `matchingservice` RPC messages, whose descriptors include every history, matching and admin
    service message (8 MB of wasm).
11. Generated gRPC stubs (and grpc-gateway) live in a different Go package from request/response
    messages. `wasmpoc/api/workflowservice` is a messages-only copy of the API package.

## Known differences from the server

- Workflow task retries write WorkflowTaskFailed/Scheduled/Started events; the server uses
  transient workflow tasks.
- No workflow task timeouts, sticky queues, activity cancellation command, heartbeats over the
  module ABI, signals, updates, queries, child workflows or Nexus.
- Polls return an empty response when there is no task; the host is responsible for waiting.
- Standalone activity features (completion callbacks, links, describe, and the pause, reset and
  update-options operator commands) are removed from the activity copy.
