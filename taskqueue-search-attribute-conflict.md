# Why `TaskQueue` as a Search Attribute Alias Doesn't Work for Standalone Activities

## Two Independent Problems

### Problem 1: System Field Bypass

`TaskQueue` is a **system search attribute**:

[common/searchattribute/sadefs/constants.go (`system`)](https://github.com/temporalio/temporal/blob/main/common/searchattribute/sadefs/constants.go#L110-L127)
```go
system = map[string]enumspb.IndexedValueType{
    ...
    TaskQueue: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
    ...
}
```

`IsMappable("TaskQueue")` returns **false** for system fields:

[common/searchattribute/sadefs/constants.go (`IsMappable`)](https://github.com/temporalio/temporal/blob/main/common/searchattribute/sadefs/constants.go#L263-L270)
```go
func IsMappable(name string) bool {
    if _, ok := system[name]; ok {
        return false  // ← TaskQueue hits this
    }
    ...
}
```

In query resolution, the CHASM mapper is only consulted inside the `IsMappable` block:

[common/persistence/visibility/store/query/resolve.go (`ResolveSearchAttributeAlias`)](https://github.com/temporalio/temporal/blob/main/common/persistence/visibility/store/query/resolve.go#L19-L58)
```go
if sadefs.IsMappable(name) {
    // ... CHASM mapper lookup happens HERE ...
    fieldName, fieldType = tryChasmMapper(name, chasmMapper)
}
// For system fields like TaskQueue, skips directly to:
fieldName, fieldType, found := tryDirectAndPrefixedLookup(name, saTypeMap)
```

**Result:** `TaskQueue` queries bypass CHASM mapping entirely and resolve directly to the system `task_queue` column.

### Problem 2: System `task_queue` Column is Empty for Standalone Activities

When standalone activities are created:

[service/history/chasm_engine.go (`ChasmEngine.createNewExecution`)](https://github.com/temporalio/temporal/blob/main/service/history/chasm_engine.go#L433-L441)
```go
mutableState := workflow.NewMutableState(
    shardContext,
    shardContext.GetEventsCache(),
    shardContext.GetLogger(),
    nsEntry,
    executionKey.BusinessID,
    executionKey.RunID,
    shardContext.GetTimeSource().Now(),
)
```

`NewMutableState` does **not** set `executionInfo.TaskQueue` — that field is only populated when processing a `WorkflowExecutionStarted` event:

[service/history/workflow/mutable_state_impl.go (`MutableStateImpl.ApplyWorkflowExecutionStartedEvent`)](https://github.com/temporalio/temporal/blob/main/service/history/workflow/mutable_state_impl.go#L2729)
```go
ms.executionInfo.TaskQueue = event.TaskQueue.GetName()
```

When visibility records are written:

[service/history/visibility_queue_task_executor.go (`visibilityQueueTaskExecutor.getVisibilityRequestBase`)](https://github.com/temporalio/temporal/blob/main/service/history/visibility_queue_task_executor.go#L521)
```go
TaskQueue: executionInfo.TaskQueue,  // Empty for standalone activities!
```

**Result:** The system `task_queue` column is **empty** for all standalone activity visibility records.

## Combined Effect

1. Query: `TaskQueue = 'my-queue'`
2. Resolution: Bypasses CHASM mapper → resolves to system `task_queue` column
3. Lookup: System column is empty → **no match**

Even if we fixed Problem 1 (made CHASM mapper consulted), we'd still need a CHASM search attribute because the system column isn't populated.

## Solution

Use `ActivityTaskQueue` as the alias:

[chasm/lib/activity/activity.go (`ActivityTaskQueueSAAlias`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L31)
```go
ActivityTaskQueueSAAlias = "ActivityTaskQueue"  // Not "TaskQueue"
```

This:
- Is not a system field, so CHASM mapper IS consulted
- Maps to `TemporalChasmSearchAttributeKeyword02` which we populate
- Is consistent with `ActivityId`, `ActivityType`, `ActivityStatus` naming
