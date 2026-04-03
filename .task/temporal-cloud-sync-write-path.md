# Temporal Cloud: Architecture of the Sync Write Path

This document is a comprehensive reference for AI agents (and humans) working on the Temporal Cloud
codebase. It covers the full request lifecycle for "accepting work" -- the synchronous write path
from client request through to durable persistence -- across all execution models: traditional
Workflows (with Activities and Child Workflows), CHASM Standalone Activities, and Nexus Operations.

The focus is on the **saas-temporal**-defined application, i.e., Temporal as deployed in Temporal
Cloud, where the open-source persistence layer is replaced by CDS (Cloud Data Service) with WAL
(Write-Ahead Log) technology backed by Apache BookKeeper (BOSS).

## Table of Contents

1. [Overview: Request Flow](#1-overview-request-flow)
2. [Frontend Service](#2-frontend-service)
3. [History Service: Shard Routing](#3-history-service-shard-routing)
4. [History Service: Execution Cache](#4-history-service-execution-cache)
5. [History Service: The Sync Write (OSS)](#5-history-service-the-sync-write-oss)
6. [CDS: The WAL-Based Sync Write (Cloud)](#6-cds-the-wal-based-sync-write-cloud)
7. [CDS: Flushing and Recovery](#7-cds-flushing-and-recovery)
8. [Post-Write: Task Dispatch to Matching](#8-post-write-task-dispatch-to-matching)
9. [Execution Models](#9-execution-models)
10. [Key File Index](#10-key-file-index)

---

## 1. Overview: Request Flow

The end-to-end path for accepting work (e.g., `StartWorkflowExecution`, `RespondWorkflowTaskCompleted`):

```
Client SDK
  |
  v
Envoy / Load Balancer  (external infrastructure; not in application repos)
  |
  v
Frontend Service  (gRPC server; interceptor chain; namespace validation; rate limiting)
  |  historyClient.StartWorkflowExecution(...)
  v
History Service   (shard-routed; the specific instance owning the target shard)
  |  workflow context lock; load/create mutable state from cache or DB
  |  apply mutations; generate history events & internal tasks
  |
  v
Persistence Write (the sync write)
  |  OSS: direct DB write (Cassandra/SQL)
  |  Cloud: WAL write to BOSS (BookKeeper); DB write is async via flusher
  |
  v
Post-Write Notifications
  |  notify queue processors of new transfer/timer/visibility/replication tasks
  |  transfer task processor pushes tasks to Matching service
  v
Matching Service  (task queues; sync matching to waiting pollers; or spool to DB)
  |
  v
Worker SDK  (polls and receives the task)
```

---

## 2. Frontend Service

### 2.1 Interceptor Chain

The Frontend gRPC server applies a deep interceptor chain before reaching the handler. In order
from outermost to innermost (see `service/frontend/fx.go:202-317`):

1. **MaskInternalErrorDetailsInterceptor** -- sanitizes internal error details from responses
2. **ServiceErrorInterceptor** -- converts internal errors to gRPC status codes
3. **NamespaceValidatorInterceptor** -- validates namespace exists and is in acceptable state
4. **NamespaceLogInterceptor** -- adds namespace to log context
5. **AuthorizationInterceptor** -- checks caller authorization
6. **NamespaceHandoverInterceptor** -- handles namespace region transitions
7. **BusinessIDInterceptor** -- extracts routing key (workflow ID, task queue, etc.) from request
8. **RedirectionInterceptor** -- routes to active cluster for multi-cluster namespaces
9. **TelemetryInterceptor** -- records RPC metrics
10. **HealthInterceptor** -- health check handling
11. **ConcurrentRequestLimitInterceptor** -- per-namespace concurrency limits
12. **NamespaceRateLimitInterceptor** -- per-namespace rate limiting
13. **RateLimitInterceptor** -- global rate limiting
14. **SDKVersionInterceptor** -- tracks SDK versions
15. **CallerInfoInterceptor** -- records caller metadata
16. **RetryableInterceptor** -- retries transient failures (innermost)

### 2.2 saas-temporal Additions

- **MetadataContextInterceptor** (`saas-temporal/service/frontend/fx.go`) -- must be first; injects
  cloud-specific metadata
- **MCN Business ID Extractor** -- extracts workflow IDs from MCN (Multi-Cluster Networking)
  requests for cross-shard parent-child communication

### 2.3 Handler Implementation

The `WorkflowHandler` (`service/frontend/workflow_handler.go`) validates requests and delegates to
the History service via a gRPC client:

```go
// StartWorkflowExecution: lines 384-423
resp, err := wh.historyClient.StartWorkflowExecution(ctx,
    &historyservice.StartWorkflowExecutionRequest{
        NamespaceId:  namespaceID.String(),
        StartRequest: request,
    })

// RespondWorkflowTaskCompleted: lines 1008-1054
response, err := wh.historyClient.RespondWorkflowTaskCompleted(ctx,
    &historyservice.RespondWorkflowTaskCompletedRequest{
        NamespaceId:     namespaceID.String(),
        CompleteRequest: request,
    })
```

For Nexus, the Frontend also serves as an HTTP gateway
(`service/frontend/nexus_http_handler.go`), resolving Nexus endpoints to handler
namespace + task queue and dispatching via Matching.

---

## 3. History Service: Shard Routing

### 3.1 Shard ID Computation

Every workflow execution maps to a shard via a deterministic hash:

```go
// common/util.go:389-397
func WorkflowIDToHistoryShard(namespaceID, workflowID string, numberOfShards int32) int32 {
    idBytes := []byte(namespaceID + "_" + workflowID)
    hash := farm.Fingerprint32(idBytes)
    return int32(hash%uint32(numberOfShards)) + 1
}
```

FarmHash Fingerprint32 of `"{namespaceID}_{workflowID}"`, mod number of shards, plus 1 (shards are
1-indexed).

### 3.2 Frontend's History Client

The Frontend's history client (`client/history/client.go:45-78`) routes RPCs to the History
instance owning the target shard:

1. **Compute shard ID** from namespaceID + workflowID
2. **Lookup host** via `membership.ServiceResolver.Lookup(shardID)` -- a ring-based membership
   protocol determines which History instance owns each shard
3. **Get/create gRPC connection** from connection pool (`client/history/connections.go`)
4. **Send RPC** to the owning History instance

Two redirector strategies exist:
- **BasicRedirector**: calls `resolver.Lookup()` on every operation
- **CachingRedirector**: maintains in-memory shard-to-host cache, invalidated on
  `ShardOwnershipLost` errors or membership changes

### 3.3 saas-temporal MCN Wrapper

In Cloud, cross-shard parent-child workflow communication is wrapped by
`MCNCrossShardClientWrapper` (`saas-temporal/client/history/mcn_client_wrapper.go`), which routes
through the MCN proxy service instead of direct History-to-History calls. This enables cross-region
communication.

### 3.4 Shard Controller

On the History service side, the `ShardController` (`service/history/shard/controller_impl.go`)
manages shard ownership:
- `GetShardByNamespaceWorkflow(namespaceID, workflowID)` computes shard ID and returns the shard
  context
- `getOrCreateShardContext(shardID)` verifies ownership via membership and creates/caches shard
  contexts
- Shard lifecycle states: `Initialized -> Acquiring -> Acquired -> Stopping -> Stopped`

---

## 4. History Service: Execution Cache

### 4.1 Cache Architecture

The workflow execution cache (`service/history/workflow/cache/cache.go`) is a per-host LRU cache
that sits in front of the persistence layer.

**Cache key** (`cache.go:73-79`):
```go
Key struct {
    WorkflowKey definition.WorkflowKey  // {NamespaceID, WorkflowID, RunID}
    ArchetypeID chasm.ArchetypeID       // distinguishes workflows vs standalone activities
    ShardUUID   string                  // isolates entries per shard owner
}
```

**Cached value**: the full `WorkflowContext` (`workflow/context.go:35-46`), which contains:
- `MutableState` -- the complete in-memory execution state
- `updateRegistry` -- for Workflow Update protocol
- `lock` -- a `PrioritySemaphore` for exclusive access (single-holder, with high/low priority)

### 4.2 Eviction Policy

The underlying LRU implementation (`common/cache/lru.go`) supports:
- **LRU ordering** via doubly-linked list
- **Reference counting (pinning)**: entries with `refCount > 0` cannot be evicted; the refcount is
  incremented on `Get` and decremented on `Release`
- **TTL**: entries expire after a configurable duration (only when unpinned)
- **Size modes**: either count-based (each entry = 1) or byte-size-based (using
  `MutableState.GetApproximatePersistedSize()`)
- **Background eviction**: optional goroutine periodically scans for expired entries

### 4.3 Loading from Persistence (Cache Miss)

On a cache miss, `LoadMutableState` (`workflow/context.go:137-227`) performs a single persistence
read:

```go
response, err := getWorkflowExecution(ctx, shardContext, &persistence.GetWorkflowExecutionRequest{
    ShardID:     shardContext.GetShardID(),
    NamespaceID: c.workflowKey.NamespaceID,
    WorkflowID:  c.workflowKey.WorkflowID,
    RunID:       c.workflowKey.RunID,
    ArchetypeID: c.archetypeID,
})
```

This returns the complete `WorkflowMutableState` protobuf from the database, containing:
- `ExecutionInfo` and `ExecutionState` (core metadata)
- `ActivityInfos`, `TimerInfos`, `ChildExecutionInfos`, `RequestCancelInfos`, `SignalInfos`
- `ChasmNodes` (CHASM tree serialization for standalone activities)
- `BufferedEvents` (events waiting to be applied)

**Mutable state is stored directly in the database -- there is no history replay on load.** History
events are a separate append-only log used for replay-based reconstruction in SDKs, not in the
server's mutable state loading path.

### 4.4 Cache Invalidation

- **On error**: the workflow context is cleared (mutable state set to nil)
- **On dirty state**: if mutable state is unexpectedly dirty after release, context is cleared and a
  panic is raised
- **On shard movement**: a `Finalizer` registered per cached entry acquires the lock, clears the
  context, and releases -- ensuring clean evacuation of all cached state when a shard moves

### 4.5 CHASM Executions

Regular workflows and CHASM standalone activities share the same LRU cache. They are distinguished
by `ArchetypeID` in the cache key. `GetOrCreateWorkflowExecution` delegates to
`GetOrCreateChasmExecution` with `chasm.WorkflowArchetypeID`.

---

## 5. History Service: The Sync Write (OSS)

### 5.1 UpdateWorkflowExecution

The shard context's `UpdateWorkflowExecution` (`service/history/shard/context_impl.go:628-688`) is
the critical write coordinator:

1. **Acquire IO semaphore** -- bounds concurrent persistence operations per shard
2. **Acquire shard write lock** -- protects shard-level mutable state
3. **Validate** shard state and namespace state
4. **Generate task keys** -- `taskKeyManager.setAndTrackTaskKeys(taskMaps...)` assigns
   monotonically increasing task IDs to all internal tasks (transfer, timer, visibility,
   replication)
5. **Set close task IDs** on `ExecutionInfo` (for workflow termination)
6. **Assign RangeID** -- the shard's lease identifier for optimistic concurrency
7. **Release shard write lock** -- lock is released BEFORE the persistence call
8. **Persistence write** -- `executionManager.UpdateWorkflowExecution(ctx, request)`
9. **Handle errors** -- `handleWriteError` may trigger RangeID renewal on `ShardOwnershipLostError`

### 5.2 What Gets Written

The `UpdateWorkflowExecutionRequest` (`common/persistence/data_interfaces.go:234-240`) contains:

- **WorkflowMutation**: upserts/deletes for activities, timers, child workflows, request cancels,
  signals, CHASM nodes; plus the `Tasks` map
- **WorkflowEvents**: history events to append (written to a separate history store/branch)
- **NewWorkflowSnapshot** (optional): for workflow continuations (continue-as-new, child workflows)

The `Tasks` map (`map[tasks.Category][]tasks.Task`) contains:
- `CategoryTransfer`: activity scheduling, child workflow starts, cancel requests, close execution
- `CategoryTimer`: user timers, activity timeouts, workflow timeouts
- `CategoryVisibility`: search attribute updates, execution open/close visibility records
- `CategoryReplication`: replication tasks for multi-cluster
- `CategoryOutbound`: outbound Nexus operation dispatch

### 5.3 History Events

Events are appended via `PersistWorkflowEvents` (`workflow/transaction_impl.go:245-343`):
- First events (new execution): `AppendHistoryNodes` with `IsNewBranch = true`
- Subsequent events: `AppendHistoryNodes` with `IsNewBranch = false`
- Each append carries `BranchToken`, `TransactionID`, `PrevTransactionID` for history versioning

### 5.4 Post-Write Notifications

After a successful write, `NotifyOnExecutionMutation` (`workflow/transaction_impl.go:584-617`)
notifies queue processors:

```go
engine.NotifyNewTasks(workflowMutation.Tasks)
engine.NotifyChasmExecution(...)  // for CHASM state machine updates
```

This wakes the transfer, timer, visibility, and replication queue processors to process the newly
created tasks.

---

## 6. CDS: The WAL-Based Sync Write (Cloud)

In Temporal Cloud, the OSS persistence layer is replaced by CDS (Cloud Data Service). The key
architectural difference: **the synchronous write path writes to WAL (backed by Apache BookKeeper /
BOSS), not to the database.** Database writes happen asynchronously via the flusher.

### 6.1 Architecture Overview

```
OSS Shard Context
  |  calls executionManager.UpdateWorkflowExecution(request)
  v
ExecutionStoreWrapper (saas-temporal/cds/export/cds/execution_store.go)
  |  implements persistence.ExecutionStore
  |  converts OSS request into WAL records
  v
runOperation (saas-temporal/cds/export/cds/execution_runner.go:98-249)
  |  orchestrates the multi-WAL write sequence
  v
WAL Writes (via BOSS / Apache BookKeeper)
  |  LPWAL -> HEWAL -> MSWAL (in order)
  v
In-Memory Apply
  |  update Element's in-memory state
  |  mark Element dirty on flush list
  v
Return to caller (sync path complete)
```

### 6.2 ExecutionStoreWrapper

`ExecutionStoreWrapper` (`saas-temporal/cds/export/cds/execution_store.go:32-59`) implements both
`persistence.ExecutionStore` and `persistence.ShardStore`. It embeds `WedgeShardController` and
bridges OSS persistence calls to CDS operations:

- Converts OSS `UpdateWorkflowExecutionRequest` into `WALLogMSData` (mutable state) and
  `WALLogHEData` (history events) protobuf records
- Dispatches via `runOperation()`

### 6.3 The Three WALs

CDS uses three Write-Ahead Logs, all backed by Apache BookKeeper (BOSS):

| WAL | Content | Purpose |
|-----|---------|---------|
| **MSWAL** (Mutable State) | Workflow mutations, snapshots, task additions, deletions | All workflow state changes |
| **HEWAL** (History Events) | History event appends, forks, deletions | History event log changes |
| **LPWAL** (Large Payload) | Oversized payloads extracted from MS/HE records | Prevents large records from bloating MS/HE WALs |

The protobuf definitions are in `saas-temporal/cds/idl/src/cds/cds.proto`.

**MSWAL record types** (`WALLogMSData`, lines 75-88):
- `CreateWorkflowRecord` -- new workflow with snapshot
- `UpdateWorkflowRecord` -- mutation to existing workflow
- `ConflictResolveWorkflowRecord` -- conflict resolution
- `SetWorkflowRecord` -- set operation
- `AddTasksRecord` -- add transfer/timer/replication/visibility tasks
- `DeleteCurrentWorkflowRecord`, `DeleteWorkflowRecord`
- `HistoryOnlyRecord`
- Plus `HeRecordWatermark` linking to the corresponding HEWAL record

**HEWAL record types** (`WALLogHEData`, lines 90-103):
- `CreateWorkflowExecutionRecord`, `UpdateWorkflowExecutionRecord`
- `AppendHistoryNodesRecord`, `DeleteHistoryNodesRecord`
- `ForkHistoryBranchRecord`, `DeleteHistoryBranchRecord`

### 6.4 runOperation: The Sync Write Sequence

`runOperation` (`execution_runner.go:98-249`) orchestrates the complete sync write:

```
1. beginOperation()
   - Lookup Element for (namespace, workflow) on the shard
   - Acquire Element's IO lock
   - Record WAL high watermark at operation start (for persisted watermark calculation)

2. operation.Prepare(data, handle)
   - Validate request against current in-memory state
   - Convert OSS request into WAL records (MSWALRecord, HEWALRecord)

3. LPWAL Write (if enabled and payloads exceed threshold)
   - Extract large payloads from MS and HE records
   - Write to LPWAL
   - Replace extracted payloads with LPWAL pointers in MS/HE records

4. HEWAL Write (if HE record present)
   - Write history events to HEWAL via shardInstance.HEWALWrite()
   - Optional: HESyncWrite() for oversized HE payloads written directly to DB
   - HEApply(): apply HE changes to Element's in-memory HistoryUpdater
   - Element marked HE-dirty

5. MSWAL Write (if MS record present)
   - Embed HE watermark into MS record (HeRecordWatermark field)
   - Write mutable state to MSWAL via shardInstance.MSWALWrite()
   - MSApply(): apply MS changes to Element's in-memory WorkflowData
   - Element marked MS-dirty

6. data.EndOperation(handle)
   - Release IO lock
   - Add Element to flush list if dirty (via AddIfUnflushed)
```

**Critical invariant**: WAL writes precede in-memory state updates. Both HEWAL and MSWAL writes
must succeed before `Apply()` is called. This ensures durability before in-memory visibility.

**If MSApply panics**, it is intentional -- the WAL record has already been durably written, so the
change will be recovered on the next shard owner. Panicking is safer than leaving in-memory state
inconsistent.

### 6.5 WAL Write Mechanics

Each WAL write (`shard/shard.go:811-839`):
1. Get `ReliableWriter` from shard's WAL manager
2. Serialize WAL record with request ID
3. Call `writer.Write(NewWriteRequest(walLog, uuid))` -- returns a `WriteFuture`
4. Wait on future with configurable timeout (`config.WALWriteTimeout()`)
5. On success: returns watermark (the logID assigned to this record)

The WAL itself is a BOSS (BookKeeper-based Object Storage Service) stream. Streams are identified
by ledger IDs with a base of 1,000,000 and support ledger rotation.

### 6.6 WedgeShardController

`WedgeShardController` (`saas-temporal/cds/shard/wedge_shard_controller.go:115-258`) is the
long-lived controller managing CDS shard lifecycles within a "wedge" (deployment unit):

- Manages 3 WAL pools: `mswalPool`, `hewalPool`, `lpwalPool`
- Creates per-shard `Shard` objects with references to WAL managers, stores, flusher
- Handles shard recovery before accepting operations

---

## 7. CDS: Flushing and Recovery

(Based on `saas-temporal/cds/doc/flushing-and-recovery.md` and `cds/shard/flusher_v2.go`)

### 7.1 Watermark Concepts

- **logID (watermark)**: monotonically increasing identifier for each WAL record
- **Persisted watermark (low watermark)**: recovery starts reading from this point; all records
  below this are known to be in the database
- **High watermark**: the next logID to be written; all records below this are known to be in the
  WAL
- **Dirty watermark**: the logID that transitioned an Element from clean to dirty

If you could replay all WAL records from logID 0 to N, you would reconstruct the complete shard
state as of logID N.

### 7.2 Elements

An `Element` is the primary in-memory data structure per workflow (keyed by namespace + workflowID).
The flusher maintains a list of dirty Elements ordered by dirty watermark (lowest first).

### 7.3 Flushing Algorithm

The flusher runs an event loop goroutine, triggered by:
- **Threshold breach**: too much dirty data or too many unrecovered WAL records
- **Periodic interval**: default 20 seconds

Processing:
1. Iterate flush list from head (oldest dirty mark)
2. For each Element: create a snapshot (set of DB row updates) if not already present
3. Attempt to write snapshot to database
4. On complete success: Element is clean up to snapshot's last WAL record; remove from or
   re-order in flush list
5. On partial/no success: retain snapshot, move to next Element

**Head Element special handling**: longer timeouts; keep flushing until snapshot fully succeeds
(since flushing head is what advances the persisted watermark).

**Snapshot aborts**: if a snapshot fails repeatedly, delete it and create fresh (defense against
poison pills like oversized Cassandra columns).

**No-progress backoff**: if no snapshots make progress in a pass, ignore wakeup signals until next
scheduled interval (avoids hammering an overloaded database).

### 7.4 Persisted Watermark Advancement

After each flush pass, the flusher calculates a new persisted watermark as the minimum of:

1. WAL's current high watermark (for quiescent shards with no dirty data)
2. Oldest in-progress operation's high-watermark-at-start (bounds for concurrent operations)
3. Head Element's dirty watermark (oldest unflushed data)

For MSWAL, the task controller's low watermark is also a floor.

### 7.5 Recovery

When a new shard owner starts:
1. Read watermark record from DB (contains MS, HE, and metering watermarks)
2. Read MS and HE WALs concurrently from their respective persisted watermarks
3. **MS records**: buffered in-memory to `BufferedLogRecoverer` (not immediately applied; replayed
   on first Element access -- "element recovery")
4. **HE records**: applied to Elements immediately as WAL is read

This design makes recovery fast: the shard becomes available for operations quickly, and individual
Element recovery is deferred until needed.

---

## 8. Post-Write: Task Dispatch to Matching

### 8.1 Transfer Task Processing

After the sync write, the History service's transfer queue processor
(`service/history/transfer_queue_active_task_executor.go`) picks up new tasks:

**Activity task** (`processActivityTask`, lines 204-257):
1. Load workflow execution context
2. Retrieve activity info from mutable state; validate version and stamp
3. Extract schedule-to-start timeout and version directive
4. Release workflow lock
5. Call `pushActivity()` -- sends `AddActivityTask` RPC to Matching

**Workflow task** (`processWorkflowTask`, lines 259-344):
1. Load workflow and workflow task from mutable state
2. Validate stamp and version
3. Create version directive
4. Release workflow lock
5. Call `pushWorkflowTask()` -- sends `AddWorkflowTask` RPC to Matching

### 8.2 Matching Service

The Matching service receives task additions and handles worker polling:

**AddTask flow** (`service/matching/task_queue_partition_manager.go:324-402`):
1. Determine physical queues (spoolQueue for persistence, syncMatchQueue for matching)
2. Attempt **sync match** (`TrySyncMatch`) -- if a poller is waiting, deliver task immediately
3. If no poller available, **spool to database** via `spoolQueue.SpoolTask()`

**Poll flow** (`service/matching/matching_engine.go:645-796`):
1. Worker calls `PollWorkflowTaskQueue` or `PollActivityTaskQueue`
2. Matching either delivers a sync-matched task or blocks until one arrives
3. On task delivery, Matching calls back to History: `RecordWorkflowTaskStarted` or
   `RecordActivityTaskStarted`
4. Returns task to worker

**Nexus dispatch** (`matching_engine.go:2480-2554`):
- `DispatchNexusTask` creates a result channel, dispatches to the handler namespace's Nexus task
  queue, and blocks until a worker responds
- Workers poll via `PollNexusTaskQueue` and respond via `RespondNexusTaskCompleted`

---

## 9. Execution Models

### 9.1 Traditional Workflows

The original execution model. A workflow is a sequence of history events, with mutable state
tracking pending activities, timers, child workflows, etc.

**Commands** returned by workers in `RespondWorkflowTaskCompleted`:
- `ScheduleActivityTask`, `StartChildWorkflowExecution`, `StartTimer`, `CancelTimer`
- `SignalExternalWorkflowExecution`, `RequestCancelExternalWorkflowExecution`
- `CompleteWorkflowExecution`, `FailWorkflowExecution`, `CancelWorkflowExecution`
- `ContinueAsNewWorkflowExecution`
- `ScheduleNexusOperation`

Each command generates corresponding history events (e.g., `ActivityTaskScheduled`,
`ChildWorkflowExecutionStarted`) and internal tasks (transfer tasks to push to Matching, timer
tasks for timeouts).

**History events** are the immutable ledger (~46+ event types), defined in
`service/history/historybuilder/event_factory.go`. Workers replay history to reconstruct workflow
state.

### 9.2 CHASM Standalone Activities

CHASM (Component-based History As State Machine) is a new framework that models executions as
component trees without history events. Standalone Activities are the first CHASM archetype.

**Key differences from workflow-embedded activities:**
- No history events; state transitions are directly persisted
- Uses `versioned_transition` (clock + sequence) instead of event sequence numbers
- State stored in `ChasmNodes` map within `WorkflowMutableState`
- Registered as a CHASM Library with components, tasks, and gRPC services

**State machine** (`chasm/lib/activity/statemachine.go`):
```
UNSPECIFIED -> SCHEDULED -> STARTED -> {COMPLETED | FAILED | TIMED_OUT | CANCELED | TERMINATED}
                  |                          ^
                  +--- RESCHEDULED ----------+  (retry)
```

**Task types** (`chasm/lib/activity/activity_tasks.go`):
1. `ActivityDispatchTask` (side-effect): pushes to Matching via `AddActivityTask`
2. `ScheduleToStartTimeoutTask` (pure): fires if not started by deadline
3. `ScheduleToCloseTimeoutTask` (pure): absolute execution deadline
4. `StartToCloseTimeoutTask` (pure): fires after activity starts
5. `HeartbeatTimeoutTask` (pure): fires if heartbeat not received

**Frontend API** (`chasm/lib/activity/frontend.go`):
- `StartActivityExecution` creates a new CHASM execution via `chasm.StartExecution()`
- The activity component is created and `TransitionScheduled` is applied
- This generates the dispatch and timeout tasks

**Caching**: CHASM executions share the same LRU cache as workflows, distinguished by
`ArchetypeID`.

### 9.3 Nexus Operations

Nexus provides cross-namespace async RPC. The key distinction is **caller namespace** vs **handler
namespace**.

**Current architecture (workflow-embedded):**
1. Workflow in caller namespace issues `ScheduleNexusOperation` command
2. This creates `NexusOperationScheduled` history event and transfer tasks
3. Transfer task processor dispatches via Frontend's Nexus HTTP handler
4. Frontend resolves Nexus endpoint to handler namespace + task queue
5. Frontend dispatches to Matching in the **handler namespace** via `DispatchNexusTask`
6. Worker in handler namespace polls, executes, responds
7. For async operations: handler calls completion callback, routed back to History

**Nexus endpoints** have two target types:
- **Worker target**: `{namespace_id, task_queue}` -- routes to a Matching task queue
- **External target**: `{url}` -- routes to an external HTTP endpoint

**CHASM-based Nexus operations** (`chasm/lib/nexusoperation/`):

A standalone Nexus operation state machine is being developed:
```
UNSPECIFIED -> SCHEDULED -> {STARTED | BACKING_OFF} -> {SUCCEEDED | FAILED | CANCELED | TIMED_OUT}
```

Task types: `InvocationTask`, `InvocationBackoffTask`, `InvocationTimeoutTask`,
`CancellationTask`, `CancellationBackoffTask`.

The `NexusOperationProcessor` framework (`chasm/nexus_operation_processor.go`) provides routing:
- `NexusOperationRoutingKeyExecution`: routes to the execution's shard
- `NexusOperationRoutingKeyRandom`: random shard selection

---

## 10. Key File Index

### Frontend Service
| File | Purpose |
|------|---------|
| `service/frontend/fx.go:202-317` | Interceptor chain setup |
| `service/frontend/workflow_handler.go` | Public gRPC handler; delegates to History |
| `service/frontend/nexus_http_handler.go` | Nexus HTTP gateway |
| `service/frontend/nexus_handler.go` | Nexus operation dispatch |
| `common/rpc/interceptor/business_id_interceptor.go` | Routing key extraction |
| `common/rpc/interceptor/redirection.go` | Multi-cluster redirection |

### History Service -- Routing & Sharding
| File | Purpose |
|------|---------|
| `common/util.go:389-397` | `WorkflowIDToHistoryShard` hash function |
| `client/history/client.go` | Frontend's History gRPC client with shard routing |
| `client/history/redirector.go` | Shard-to-host lookup via membership |
| `client/history/caching_redirector.go` | Cached shard-to-host mapping |
| `service/history/shard/controller_impl.go` | Shard controller; ownership management |
| `service/history/shard/context_impl.go` | Shard context; IO semaphore; write coordination |
| `common/membership/interfaces.go` | `ServiceResolver` interface for host lookup |

### History Service -- Execution Cache & State
| File | Purpose |
|------|---------|
| `service/history/workflow/cache/cache.go` | LRU execution cache |
| `service/history/workflow/context.go` | Workflow context; lock; mutable state loading |
| `service/history/workflow/mutable_state_impl.go` | Mutable state; `NewMutableStateFromDB` |
| `common/cache/lru.go` | LRU cache with pinning, TTL, background eviction |
| `common/finalizer/finalizer.go` | Shard-movement cache cleanup |

### History Service -- Write Path & Events
| File | Purpose |
|------|---------|
| `service/history/shard/context_impl.go:628-688` | `UpdateWorkflowExecution` write coordinator |
| `service/history/workflow/transaction_impl.go` | Event persistence; post-write notifications |
| `service/history/historybuilder/event_factory.go` | History event creation (~46 types) |
| `service/history/workflow/command_handler.go` | Command handler registry |
| `service/history/api/respondworkflowtaskcompleted/` | WFT completed processing |
| `common/persistence/data_interfaces.go` | `WorkflowMutation`, `WorkflowSnapshot` types |

### History Service -- Task Processing
| File | Purpose |
|------|---------|
| `service/history/transfer_queue_active_task_executor.go` | Transfer task dispatch |
| `service/history/transfer_queue_task_executor_base.go` | `pushActivity`, `pushWorkflowTask` |
| `service/history/tasks/` | Task type definitions |

### Matching Service
| File | Purpose |
|------|---------|
| `service/matching/matching_engine.go` | Core engine; poll loops; Nexus dispatch |
| `service/matching/task_queue_partition_manager.go` | AddTask; sync match; spool |
| `service/matching/handler.go` | gRPC handlers |
| `service/matching/matcher.go` | Sync matching (legacy) |
| `service/matching/pri_matcher.go` | Priority-based sync matching (new) |
| `service/matching/forwarder.go` | Partition forwarding |

### CHASM Framework
| File | Purpose |
|------|---------|
| `chasm/component.go` | Component interface; lifecycle states |
| `chasm/engine.go` | `StartExecution`, `UpdateComponent`, `ReadComponent` |
| `chasm/registry.go` | Type registration and lookup |
| `chasm/lib/activity/activity.go` | Standalone Activity component |
| `chasm/lib/activity/statemachine.go` | Activity state transitions |
| `chasm/lib/activity/activity_tasks.go` | Activity task handlers |
| `chasm/lib/activity/frontend.go` | `StartActivityExecution` API |
| `chasm/lib/activity/library.go` | Activity library registration |
| `chasm/lib/nexusoperation/` | CHASM Nexus operation (in development) |
| `service/history/chasm_task_util.go` | CHASM task execution in History |

### Nexus
| File | Purpose |
|------|---------|
| `components/nexusoperations/statemachine.go` | Workflow-level Nexus state machine (HSM) |
| `components/nexusoperations/workflow/commands.go` | `ScheduleNexusOperation` command |
| `components/nexusoperations/executors.go` | Nexus task executors |
| `components/nexusoperations/frontend/handler.go` | Async completion callback handler |
| `common/nexus/endpoint_registry.go` | Endpoint management |
| `service/matching/nexus_endpoint_client.go` | Nexus endpoint persistence in Matching |

### CDS / saas-temporal -- WAL
| File | Purpose |
|------|---------|
| `cds/export/cds/execution_store.go` | `ExecutionStoreWrapper`; bridges OSS to CDS |
| `cds/export/cds/execution_runner.go` | `runOperation`; multi-WAL write orchestration |
| `cds/shard/shard.go:741-839` | WAL write methods (`MSWALWrite`, `HEWALWrite`, `LPWALWrite`) |
| `cds/shard/wedge_shard_controller.go` | WAL pool management; shard lifecycle |
| `cds/shard/flusher_v2.go` | Flushing dirty data to database |
| `cds/idl/src/cds/cds.proto` | WAL record protobuf definitions |
| `cds/doc/flushing-and-recovery.md` | Authoritative flushing & recovery design doc |
| `cds/stream/stream.go` | BOSS/BookKeeper stream abstractions |
| `cds/config/lpwal_config.go` | LPWAL configuration |
| `cds/config/hewal_config.go` | HEWAL configuration |
| `cds/convert/proto.go` | OSS <-> CDS protobuf conversions |

### CDS / saas-temporal -- Other
| File | Purpose |
|------|---------|
| `cds/export/cds/fx.go` | Service DI; interceptor setup |
| `cds/persistence/execution_manager.go` | Custom execution manager with tiered storage |
| `cds/persistence/tieredstorage/` | S3/GCS/Azure tiered storage |
| `client/history/mcn_client_wrapper.go` | MCN cross-shard routing |
| `secrets/astra.go` | Astra/Cassandra credential injection |
