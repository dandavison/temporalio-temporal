# CHASM Component Lifecycle, Activity Statuses, and Run States

This guide explains the relationship between CHASM component lifecycle states, activity execution statuses, and run states. It covers how these concepts relate to each other and when/how they are updated.

## Conceptual Layers

There are four conceptual layers of state tracking for standalone activities:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Public API (go.temporal.io/api)                                    │
│  ├── ActivityExecutionStatus (6 values)                             │
│  └── PendingActivityState (3 running substates)                     │
├─────────────────────────────────────────────────────────────────────┤
│  Internal Activity Status (chasm/lib/activity/proto/v1)             │
│  └── ActivityExecutionStatus (8 values - finer-grained)             │
├─────────────────────────────────────────────────────────────────────┤
│  CHASM Component Lifecycle (chasm/component.go)                     │
│  └── LifecycleState (3 values: Running, Completed, Failed)          │
├─────────────────────────────────────────────────────────────────────┤
│  Workflow Execution State/Status (mutable_state)                    │
│  └── WorkflowExecutionState + WorkflowExecutionStatus               │
└─────────────────────────────────────────────────────────────────────┘
```

## 1. Internal Activity Status

The internal activity status is the primary source of truth for an activity's state. It is defined in the server's internal protobuf:

[chasm/lib/activity/proto/v1/activity_state.proto (`ActivityExecutionStatus`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/proto/v1/activity_state.proto#L15-L44)

```protobuf
enum ActivityExecutionStatus {
    ACTIVITY_EXECUTION_STATUS_UNSPECIFIED = 0;
    ACTIVITY_EXECUTION_STATUS_SCHEDULED = 1;
    ACTIVITY_EXECUTION_STATUS_STARTED = 2;
    ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED = 3;
    ACTIVITY_EXECUTION_STATUS_COMPLETED = 4;
    ACTIVITY_EXECUTION_STATUS_FAILED = 5;
    ACTIVITY_EXECUTION_STATUS_CANCELED = 6;
    ACTIVITY_EXECUTION_STATUS_TERMINATED = 7;
    ACTIVITY_EXECUTION_STATUS_TIMED_OUT = 8;
}
```

**Non-terminal statuses:** `SCHEDULED`, `STARTED`, `CANCEL_REQUESTED`
**Terminal statuses:** `COMPLETED`, `FAILED`, `CANCELED`, `TERMINATED`, `TIMED_OUT`

## 2. CHASM Component Lifecycle

The CHASM framework defines a generic `LifecycleState` interface that all components must implement. This is a coarse-grained abstraction over component-specific statuses:

[chasm/component.go (`LifecycleState`)](https://github.com/temporalio/temporal/blob/main/chasm/component.go#L42-L62)

```go
type LifecycleState int

const (
    LifecycleStateRunning   LifecycleState = 2 << iota
    LifecycleStateCompleted
    LifecycleStateFailed
)

func (s LifecycleState) IsClosed() bool {
    return s >= LifecycleStateCompleted
}
```

**Key concept:** `IsClosed()` returns true for both `LifecycleStateCompleted` and `LifecycleStateFailed`.

## 3. How Activity Status Maps to Lifecycle State

The `Activity` component implements the `LifecycleState()` method to derive CHASM lifecycle state from internal activity status:

[chasm/lib/activity/activity.go (`Activity.LifecycleState`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L124-L136)

```go
func (a *Activity) LifecycleState(_ chasm.Context) chasm.LifecycleState {
    switch a.Status {
    case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
        return chasm.LifecycleStateCompleted
    case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
        activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
        activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
        activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
        return chasm.LifecycleStateFailed
    default:
        return chasm.LifecycleStateRunning
    }
}
```

**Mapping summary:**

| Internal Activity Status | CHASM Lifecycle State |
|-------------------------|----------------------|
| `SCHEDULED`             | `Running`            |
| `STARTED`               | `Running`            |
| `CANCEL_REQUESTED`      | `Running`            |
| `COMPLETED`             | `Completed`          |
| `FAILED`                | `Failed`             |
| `CANCELED`              | `Failed`             |
| `TERMINATED`            | `Failed`             |
| `TIMED_OUT`             | `Failed`             |

**Note:** Only `COMPLETED` maps to `LifecycleStateCompleted`. All other terminal statuses map to `LifecycleStateFailed`.

## 4. How Lifecycle State Maps to Workflow State/Status

For standalone activities (non-workflow root components), the CHASM tree translates the root component's lifecycle state into workflow execution state/status at transaction close time:

[chasm/tree.go (`closeTransactionHandleRootLifecycleChange`)](https://github.com/temporalio/temporal/blob/main/chasm/tree.go#L1438-L1481)

```go
func (n *Node) closeTransactionHandleRootLifecycleChange() (bool, error) {
    if n.backend.IsWorkflow() {
        // Workflow manages its lifecycle directly in mutable state.
        return false, nil
    }
    // ... (for standalone activities)

    lifecycleState := rootComponent.LifecycleState(chasmContext)

    var newState enumsspb.WorkflowExecutionState
    var newStatus enumspb.WorkflowExecutionStatus
    switch lifecycleState {
    case LifecycleStateRunning:
        newState = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
        newStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
    case LifecycleStateCompleted:
        newState = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
        newStatus = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
    case LifecycleStateFailed:
        newState = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
        newStatus = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
    }

    return n.backend.UpdateWorkflowStateStatus(newState, newStatus)
}
```

**Mapping summary:**

| CHASM Lifecycle State | WorkflowExecutionState | WorkflowExecutionStatus |
|----------------------|------------------------|------------------------|
| `Running`            | `RUNNING`              | `RUNNING`              |
| `Completed`          | `COMPLETED`            | `COMPLETED`            |
| `Failed`             | `COMPLETED`            | `FAILED`               |

**Important:** A terminated activity becomes `WORKFLOW_EXECUTION_STATUS_FAILED` (via `LifecycleStateFailed`), not `WORKFLOW_EXECUTION_STATUS_TERMINATED`. Explicit termination via the `Terminate` API triggers special handling:

```go
if n.terminated {
    return n.backend.UpdateWorkflowStateStatus(
        enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
        enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}
```

## 5. Public API Status Mapping

The public API exposes a simplified view of the activity status through two enums:
- `ActivityExecutionStatus` - terminal vs. running
- `PendingActivityState` - substates while running

[chasm/lib/activity/activity.go (`InternalStatusToAPIStatus`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L571-L592)

```go
func InternalStatusToAPIStatus(status activitypb.ActivityExecutionStatus) enumspb.ActivityExecutionStatus {
    switch status {
    case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
        activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
        activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
        return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
    case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
        return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
    case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
        return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
    case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
        return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
    case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
        return enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
    case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
        return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
    // ...
    }
}
```

[chasm/lib/activity/activity.go (`internalStatusToRunState`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L594-L613)

```go
func internalStatusToRunState(status activitypb.ActivityExecutionStatus) enumspb.PendingActivityState {
    switch status {
    case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
        return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
    case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
        return enumspb.PENDING_ACTIVITY_STATE_STARTED
    case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
        return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
    case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
        activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
        // ... (all terminal statuses)
        return enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
    }
}
```

## 6. State Transition Mechanics

Activity status changes are governed by explicit state machine transitions defined in:

[chasm/lib/activity/statemachine.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/statemachine.go#L1-L368)

### Transition Definitions

Each transition specifies:
1. **Source states** - valid starting states
2. **Destination state** - the target state
3. **Apply function** - logic to execute during transition (schedule tasks, record metadata, etc.)

```go
var TransitionStarted = chasm.NewTransition(
    []activitypb.ActivityExecutionStatus{
        activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
    },
    activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
    func(a *Activity, ctx chasm.MutableContext, request *...) error {
        // Record start time, schedule timeout tasks, etc.
    },
)
```

### State Diagram

```
                    ┌────────────────┐
                    │  UNSPECIFIED   │
                    └───────┬────────┘
                            │ TransitionScheduled
                            ▼
                    ┌────────────────┐
         ┌─────────►│   SCHEDULED    │◄──────────────┐
         │          └───────┬────────┘               │
         │                  │ TransitionStarted      │ TransitionRescheduled
         │                  ▼                        │ (retry)
         │          ┌────────────────┐               │
         │          │    STARTED     │───────────────┘
         │          └───────┬────────┘
         │                  │ TransitionCancelRequested
         │                  ▼
         │          ┌────────────────┐
         │          │CANCEL_REQUESTED│
         │          └───────┬────────┘
         │                  │
    ─────┴──────────────────┴─────────────────────────
    │                       │                        │
    │ TransitionTimedOut    │ TransitionCanceled     │ TransitionCompleted
    │ TransitionTerminated  │                        │ TransitionFailed
    ▼                       ▼                        ▼
┌──────────┐         ┌──────────┐              ┌──────────┐
│TIMED_OUT │         │ CANCELED │              │COMPLETED │
│TERMINATED│         └──────────┘              │ FAILED   │
└──────────┘                                   └──────────┘
```

### Valid Transitions Table

| Transition | From States | To State |
|------------|-------------|----------|
| `TransitionScheduled` | `UNSPECIFIED` | `SCHEDULED` |
| `TransitionStarted` | `SCHEDULED` | `STARTED` |
| `TransitionRescheduled` | `STARTED` | `SCHEDULED` |
| `TransitionCancelRequested` | `SCHEDULED`, `STARTED`, `CANCEL_REQUESTED` | `CANCEL_REQUESTED` |
| `TransitionCompleted` | `STARTED`, `CANCEL_REQUESTED` | `COMPLETED` |
| `TransitionFailed` | `STARTED`, `CANCEL_REQUESTED` | `FAILED` |
| `TransitionCanceled` | `CANCEL_REQUESTED` | `CANCELED` |
| `TransitionTerminated` | `SCHEDULED`, `STARTED`, `CANCEL_REQUESTED` | `TERMINATED` |
| `TransitionTimedOut` | `SCHEDULED`, `STARTED`, `CANCEL_REQUESTED` | `TIMED_OUT` |

## 7. When Status Updates Occur

### CHASM Transaction Model

Status changes happen within CHASM transactions. The general flow:

1. **API Request** arrives (e.g., `RespondActivityTaskCompleted`)
2. **Handler** retrieves the activity component and applies a transition
3. **Transition** validates the current state and updates to the new state
4. **Transaction Close** (`CloseTransaction()`) triggers:
   - Serialization of updated component state
   - Lifecycle state calculation from component
   - Workflow state/status update via backend
   - Visibility updates
   - Task generation

[chasm/tree.go (`CloseTransaction`)](https://github.com/temporalio/temporal/blob/main/chasm/tree.go#L1330-L1376)

```go
func (n *Node) CloseTransaction() (NodesMutation, error) {
    defer n.cleanupTransaction()

    if err := n.executeImmediatePureTasks(); err != nil { /* ... */ }
    if err := n.syncSubComponents(); err != nil { /* ... */ }

    rootLifecycleChanged, err := n.closeTransactionHandleRootLifecycleChange()
    // ... updates workflow state/status based on root component's lifecycle

    if n.isActiveStateDirty {
        if err := n.closeTransactionForceUpdateVisibility(rootLifecycleChanged); err != nil { /* ... */ }
    }
    // ...
}
```

### Backend Persistence

The `NodeBackend` interface connects CHASM to the underlying mutable state persistence:

```go
type NodeBackend interface {
    UpdateWorkflowStateStatus(
        state enumsspb.WorkflowExecutionState,
        status enumspb.WorkflowExecutionStatus,
    ) (bool, error)
    // ...
}
```

For standalone activities, the mutable state implementation (`MutableStateImpl`) validates and persists the state/status change:

[service/history/workflow/mutable_state_impl.go (`UpdateWorkflowStateStatus`)](https://github.com/temporalio/temporal/blob/main/service/history/workflow/mutable_state_impl.go#L6559-L6572)

## 8. Complete Mapping Table

| Internal Status | Public API Status | PendingActivityState | Lifecycle State | WF State | WF Status |
|----------------|-------------------|----------------------|-----------------|----------|-----------|
| `SCHEDULED` | `RUNNING` | `SCHEDULED` | `Running` | `RUNNING` | `RUNNING` |
| `STARTED` | `RUNNING` | `STARTED` | `Running` | `RUNNING` | `RUNNING` |
| `CANCEL_REQUESTED` | `RUNNING` | `CANCEL_REQUESTED` | `Running` | `RUNNING` | `RUNNING` |
| `COMPLETED` | `COMPLETED` | `UNSPECIFIED` | `Completed` | `COMPLETED` | `COMPLETED` |
| `FAILED` | `FAILED` | `UNSPECIFIED` | `Failed` | `COMPLETED` | `FAILED` |
| `CANCELED` | `CANCELED` | `UNSPECIFIED` | `Failed` | `COMPLETED` | `FAILED` |
| `TERMINATED` | `TERMINATED` | `UNSPECIFIED` | `Failed` | `COMPLETED` | `TERMINATED`* |
| `TIMED_OUT` | `TIMED_OUT` | `UNSPECIFIED` | `Failed` | `COMPLETED` | `FAILED` |

\* `TERMINATED` status at workflow level is set directly when `n.terminated` is true, bypassing the lifecycle-to-status mapping.

## 9. Key Invariants

1. **Internal status is authoritative** - All other statuses are derived from it.
2. **Lifecycle state determines openness** - `IsClosed()` governs whether the component accepts further mutations.
3. **Transitions are validated** - Invalid state transitions return `ErrInvalidTransition`.
4. **Status changes are atomic** - They occur within a single CHASM transaction.
5. **Root lifecycle drives WF state** - For non-workflow root components, the root's lifecycle state determines workflow execution state/status.

