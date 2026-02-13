# Standalone Activity Missing Test Coverage

## Investigation Summary

Compared the standalone activity integration tests (`tests/standalone_activity_test.go`) against
workflow integration tests and the implementation code. Found several categories of missing
coverage, ordered by importance.

Phase 1 is tests only. Write tests as they _should_ be -- if a test fails, that indicates a real
bug to fix in phase 2. Do not hack tests to pass.

---

## Category 1: Payload size limits on respond RPCs (HIGH)

Workflow tests (`tests/sizelimit_test.go`) comprehensively test what happens when payloads exceed
blob size limits. Standalone activity has **zero** tests for this. The frontend enforces these
limits in `service/frontend/workflow_handler.go`, but there are no integration tests verifying:

- **RespondActivityTaskCompleted with oversized result**: Frontend converts to a failure (line
  1569-1592). The standalone activity path goes through `HandleFailed`, which should cause the
  activity to fail with a server failure. This is a complex code path that needs testing.
- **RespondActivityTaskFailed with oversized failure**: Frontend truncates and wraps in a server
  failure (line 1676-1692).
- **RespondActivityTaskFailed with oversized LastHeartbeatDetails**: Frontend strips heartbeat
  details and adds a server failure (line 1656-1673).
- **RespondActivityTaskCanceled with oversized details**: Frontend rejects with an error (line
  1864-1876).
- **RecordActivityTaskHeartbeat with oversized details**: Frontend rejects with an error (line
  1258-1270).

These tests should override `BlobSizeLimitError` dynamic config (as workflow tests do in
`sizelimit_test.go`) and verify correct behavior.

---

## Category 2: Retry policy edge cases (HIGH)

Workflow activity tests (`tests/activity_test.go`) test retry exhaustion, non-retryable errors, etc.
Standalone activity is missing:

- **MaximumAttempts exhaustion**: `TestRetryWithoutScheduleToCloseTimeout` sets `MaximumAttempts: 2`
  but only verifies attempt 2 is scheduled -- it never fails attempt 2 and checks that the activity
  transitions to FAILED with the correct outcome. Need a test that: starts with MaximumAttempts=N,
  fails N times retryably, verifies the activity reaches FAILED state with
  `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`.
- **NonRetryable failure**: The `defaultFailure` uses `NonRetryable: true` in some tests but
  there's no dedicated test verifying that a `NonRetryable: true` failure prevents retry when retry
  policy allows retries (MaximumAttempts > 1).
- **NonRetryableErrorTypes**: No test for an activity whose `RetryPolicy.NonRetryableErrorTypes`
  matches the failure type, verifying that retry is skipped.

---

## Category 3: RequestCancel on already-completed activity (MEDIUM-HIGH)

`TestTerminate` has `AlreadyCompletedCannotTerminate`, but `TestRequestCancel` has no equivalent.
When `RequestCancelActivityExecution` is called on a completed/failed/terminated/canceled/timed-out
activity, the `TransitionCancelRequested` will fail because COMPLETED etc. are not valid source
states. There should be a test verifying this returns an appropriate error (likely
`FailedPrecondition` from the CHASM state machine).

---

## Category 4: RespondActivityTaskFailedById missing validation (MEDIUM)

The token-based `RespondActivityTaskFailed` (`service/frontend/workflow_handler.go:1643-1644`)
checks:

```go
if request.GetFailure() != nil && request.GetFailure().GetApplicationFailureInfo() == nil {
    return nil, errFailureMustHaveApplicationFailureInfo
}
```

The ById variant `RespondActivityTaskFailedById` (`service/frontend/workflow_handler.go:1709`) does
**not** have this check. This means a ById caller can send a failure without
`ApplicationFailureInfo`, which causes the activity to skip retry logic entirely (since
`HandleFailed` checks `appFailure != nil` to determine retryability). A test should verify that
`RespondActivityTaskFailedById` with a non-nil Failure lacking `ApplicationFailureInfo` returns
`InvalidArgument`. Phase 1 is test-only: if the test fails, that confirms the bug and the fix goes
in phase 2.

---

## Category 5: Feature disabled on respond RPCs (MEDIUM)

The frontend checks `!wh.IsStandaloneActivityEnabled(namespaceName)` when the token has a
`ComponentRef` (e.g., `workflow_handler.go:1639`). There are no integration tests verifying that
when standalone activity is disabled, Respond/Heartbeat RPCs with standalone activity tokens return
`ErrStandaloneActivityDisabled`. The Start/Describe/Poll/List/Count/Cancel/Terminate all check
`Enabled` in the activity frontend handler, but Respond RPCs check at the workflow handler level --
this deserves a test.

---

## Category 6: ScheduleToCloseTimeout without retry (LOW-MEDIUM)

There's a test for ScheduleToCloseTimeout with retry (`Test_ScheduleToCloseTimeout_WithRetry`), but
no test for a non-retryable activity that times out via ScheduleToClose. The workflow equivalent
exists in `tests/activity_test.go` (`TestActivityScheduleToClose_FiredDuringActivityRun`).

---

## Category 7: Force-complete a retrying activity via ById (MEDIUM)

Workflow's `TestActivityTaskCompleteForceCompletion` tests: activity fails retryably (enters retry
backoff), then `CompleteActivityByID` is called to force-complete it. There is no standalone
equivalent. For standalone activity, `TransitionCompleted` only allows source states STARTED and
CANCEL_REQUESTED -- so calling `RespondActivityTaskCompletedById` when the activity is in SCHEDULED
(retry) should fail. A test should verify the correct error is returned (or, if the intent is to
support force-completion, that it works).

---

## Category 8: Respond*ById with non-existent activity (LOW-MEDIUM)

The token-based NotFound path is covered (StaleToken tests). But
`RespondActivityTaskCompletedById`, `RespondActivityTaskFailedById`,
`RespondActivityTaskCanceledById`, and `RecordActivityTaskHeartbeatById` with a non-existent
activity ID are not tested. These go through a different code path (synthetic token construction in
the frontend, then CHASM lookup fails). Should verify `NotFound`.

---

## Implementation Plan

All new tests go in `tests/standalone_activity_test.go`, following the existing test patterns
(suite, helper methods, `testcore.RandomizeStr`), slotted as subtests under the appropriate
top-level RPC-grouped test.
