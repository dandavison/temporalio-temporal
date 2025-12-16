# Heartbeat Behavior Comparison: Standalone vs Workflow Activities

This guide compares heartbeat behavior between standalone activities (CHASM) and non-CHASM workflow activities across different activity states.

## Summary

Both standalone activities and workflow activities follow the same fundamental rule: **heartbeats are only valid while an activity attempt is in progress**. Once the activity reaches a terminal state or the workflow completes, heartbeats fail with "not found".

| Activity State | Standalone Activity | Workflow Activity | Heartbeat Result |
|----------------|---------------------|-------------------|------------------|
| STARTED | ✅ Valid | ✅ Valid | Success, `cancel_requested=false` |
| CANCEL_REQUESTED | ✅ Valid | ✅ Valid | Success, `cancel_requested=true` |
| COMPLETED | ❌ Invalid | ❌ Invalid | NotFound |
| FAILED | ❌ Invalid | ❌ Invalid | NotFound |
| CANCELED | ❌ Invalid | ❌ Invalid | NotFound |
| TERMINATED | ❌ Invalid | N/A | NotFound |
| TIMED_OUT | ❌ Invalid | ❌ Invalid | NotFound |
| Workflow Completed | N/A | ❌ Invalid | ErrWorkflowCompleted |

---

## Standalone Activity Heartbeat Validation

For standalone activities, heartbeat validation is performed in `validateActivityTaskToken`:

[chasm/lib/activity/activity.go (`Activity.validateActivityTaskToken`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L738-L752)
```go
func (a *Activity) validateActivityTaskToken(
	ctx chasm.Context,
	token *tokenspb.Task,
) error {
	if a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return serviceerror.NewNotFound("activity task not found")
	}
	if token.Attempt != a.LastAttempt.Get(ctx).GetCount() {
		return serviceerror.NewNotFound("activity task not found")
	}
	return nil
}
```

The `cancel_requested` flag is returned based on the activity status:

[chasm/lib/activity/activity.go (`Activity.RecordHeartbeat`)](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L543-L570)
```go
func (a *Activity) RecordHeartbeat(
	ctx chasm.MutableContext,
	input WithToken[*historyservice.RecordActivityTaskHeartbeatRequest],
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	// ... validation and heartbeat recording ...
	return &historyservice.RecordActivityTaskHeartbeatResponse{
		CancelRequested: a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	}, nil
}
```

**Test coverage:**

[tests/standalone_activity_test.go (`TestHeartbeat/HeartbeatWhileInState`)](https://github.com/temporalio/temporal/blob/main/tests/standalone_activity_test.go#L2286-L2456)
```go
t.Run("HeartbeatWhileInState", func(t *testing.T) {
	testCases := []struct {
		name            string
		setupFn         func(ctx context.Context, taskToken []byte, activityID, runID string) error
		expectSuccess   bool
		cancelRequested bool
	}{
		{name: "STARTED", setupFn: nil, expectSuccess: true, cancelRequested: false},
		{name: "CANCEL_REQUESTED", /* ... */, expectSuccess: true, cancelRequested: true},
		{name: "COMPLETED", /* ... */, expectSuccess: false},
		{name: "FAILED", /* ... */, expectSuccess: false},
		{name: "CANCELED", /* ... */, expectSuccess: false},
		{name: "TERMINATED", /* ... */, expectSuccess: false},
		{name: "TIMED_OUT", /* ... */, expectSuccess: false},
	}
	// ... test body verifies heartbeat succeeds or fails with NotFound ...
}
```

---

## Workflow Activity Heartbeat Validation

For non-CHASM workflow activities, heartbeat validation has two stages:

### Stage 1: Workflow Must Be Running

[service/history/api/recordactivitytaskheartbeat/api.go (`Invoke`)](https://github.com/temporalio/temporal/blob/main/service/history/api/recordactivitytaskheartbeat/api.go#L65-L69)
```go
func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
	mutableState := workflowLease.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
```

### Stage 2: Activity Must Be Pending and Match Token

[service/history/api/recordactivitytaskheartbeat/api.go (`Invoke` - activity lookup)](https://github.com/temporalio/temporal/blob/main/service/history/api/recordactivitytaskheartbeat/api.go#L78-L91)
```go
ai, isRunning := mutableState.GetActivityInfo(scheduledEventID)

// First check to see if cache needs to be refreshed...
if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
	// ... metrics ...
	return nil, consts.ErrStaleState
}

if !isRunning || api.IsActivityTaskNotFoundForToken(token, ai, nil) {
	return nil, consts.ErrActivityTaskNotFound
}
```

### Activity Lookup

`GetActivityInfo` returns the activity from `pendingActivityInfoIDs`. Once an activity is completed, failed, canceled, or timed out, it is removed from this map:

[service/history/workflow/mutable_state_impl.go (`MutableStateImpl.GetActivityInfo`)](https://github.com/temporalio/temporal/blob/main/service/history/workflow/mutable_state_impl.go#L1456-L1462)
```go
func (ms *MutableStateImpl) GetActivityInfo(
	scheduledEventID int64,
) (*persistencespb.ActivityInfo, bool) {
	ai, ok := ms.pendingActivityInfoIDs[scheduledEventID]
	return ai, ok
}
```

### Activity Removal on Completion

When an activity completes, fails, times out, or is canceled, `DeleteActivity` removes it from `pendingActivityInfoIDs`:

[service/history/workflow/mutable_state_impl.go (`MutableStateImpl.DeleteActivity`)](https://github.com/temporalio/temporal/blob/main/service/history/workflow/mutable_state_impl.go#L2034-L2050)
```go
func (ms *MutableStateImpl) DeleteActivity(
	scheduledEventID int64,
) error {
	if activityInfo, ok := ms.pendingActivityInfoIDs[scheduledEventID]; ok {
		delete(ms.pendingActivityInfoIDs, scheduledEventID)
		delete(ms.pendingActivityTimerHeartbeats, scheduledEventID)
		// ...
	}
	return nil
}
```

This is called from:
- `ReplicateActivityTaskCompletedEvent` (line 4049)
- `ReplicateActivityTaskFailedEvent` (line 4099)
- `ReplicateActivityTaskTimedOutEvent` (line 4151)
- `ReplicateActivityTaskCanceledEvent` (line 4284)

### Token Validation

[service/history/api/activity_util.go (`IsActivityTaskNotFoundForToken`)](https://github.com/temporalio/temporal/blob/main/service/history/api/activity_util.go#L58-L77)
```go
func IsActivityTaskNotFoundForToken(
	token *tokenspb.Task,
	ai *persistencespb.ActivityInfo,
	isCompletedByID *bool,
) bool {
	if isCompletedByID == nil || !*isCompletedByID {
		if ai.StartedEventId == common.EmptyEventID {
			return true  // Activity not started yet
		}
	}
	if token.GetScheduledEventId() != common.EmptyEventID && token.Attempt != ai.Attempt {
		return true  // Stale attempt
	}
	// ... version checks ...
}
```

### Cancel Requested Flag

When `RequestCancelActivityExecution` is called, the `CancelRequested` flag is set on the activity info:

[service/history/workflow/mutable_state_impl.go (`MutableStateImpl.ApplyActivityTaskCancelRequestedEvent`)](https://github.com/temporalio/temporal/blob/main/service/history/workflow/mutable_state_impl.go#L4199-L4227)
```go
func (ms *MutableStateImpl) ReplicateActivityTaskCancelRequestedEvent(
	event *historypb.HistoryEvent,
) error {
	// ...
	ai.CancelRequested = true
	ai.CancelRequestId = event.GetEventId()
	// ...
}
```

The heartbeat response returns this flag:

[service/history/api/recordactivitytaskheartbeat/api.go (`Invoke` - cancel_requested)](https://github.com/temporalio/temporal/blob/main/service/history/api/recordactivitytaskheartbeat/api.go#L98-L100)
```go
cancelRequested = ai.CancelRequested
// ...
return &historyservice.RecordActivityTaskHeartbeatResponse{CancelRequested: cancelRequested, ...}
```

### Error Definitions

[service/history/consts/const.go (`ErrActivityTaskNotFound`)](https://github.com/temporalio/temporal/blob/main/service/history/consts/const.go#L44-L46)
```go
// ErrActivityTaskNotFound is the error to indicate activity task could be duplicate
// and activity already completed
ErrActivityTaskNotFound = serviceerror.NewNotFound(
	"invalid activityID or activity already timed out or invoking workflow is completed")
```

**Test coverage:**

[tests/activity_test.go (`TestActivityHeartBeatWorkflow_Timeout`)](https://github.com/temporalio/temporal/blob/main/tests/activity_test.go#L782-L882)
```go
func (s *ActivityTestSuite) TestActivityHeartBeatWorkflow_Timeout() {
	// ... setup activity with 1s heartbeat timeout ...
	// Activity sleeps for 2s (longer than heartbeat timeout)
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		time.Sleep(2 * time.Second)  // Times out
		return payloads.EncodeString("Activity Result"), false, nil
	}
	// ...
	err = poller.PollAndProcessActivityTask(false)
	s.IsType(consts.ErrActivityTaskNotFound, err)
	s.Equal(consts.ErrActivityTaskNotFound.Error(), err.Error())
}
```

---

## Key Behavioral Consistency

Both implementations share these behaviors:

1. **STARTED/CANCEL_REQUESTED are the only valid states for heartbeating** — the activity must have been picked up by a worker and not yet reached a terminal state.

2. **Attempt validation** — the task token's attempt number must match the current attempt. Stale tokens from previous attempts are rejected.

3. **CancelRequested is delivered via heartbeat response** — this is how workers learn about pending cancellation requests without polling.

4. **Terminal states result in NotFound** — whether the activity completed successfully, failed, was canceled, or timed out, subsequent heartbeats fail.

The main difference is that standalone activities don't have a parent workflow lifecycle check, while workflow activities first verify the workflow is still running.

