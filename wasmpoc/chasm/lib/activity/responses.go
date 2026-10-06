package activity

import (
	"fmt"

	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/wasmpoc/api/workflowservice/v1"
	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Projection of activity state onto the API response protos.

// InternalStatusToAPIStatus converts internal activity execution status to API status.
func InternalStatusToAPIStatus(status activitypb.ActivityExecutionStatus) enumspb.ActivityExecutionStatus {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED
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
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	default:
		panic(fmt.Sprintf("unknown activity execution status: %v", status)) //nolint:forbidigo
	}
}

func (a *Activity) runState() enumspb.PendingActivityState {
	status := a.GetStatus()
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		// The worker is still executing under its existing task token; the public PendingActivityState
		// enum does not have a RESET_REQUESTED variant. The reset is surfaced to the worker via
		// ActivityReset=true on its next heartbeat response.
		if a.isPaused() {
			return enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
		}
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return enumspb.PENDING_ACTIVITY_STATE_PAUSED
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	default:
		panic(fmt.Sprintf("unknown activity execution status: %v", status)) //nolint:forbidigo
	}
}

func (a *Activity) buildActivityExecutionInfo(
	ctx chasm.Context,
	request *workflowservice.DescribeActivityExecutionRequest,
) *apiactivitypb.ActivityExecutionInfo {
	status := InternalStatusToAPIStatus(a.GetStatus())
	runState := a.runState()

	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	heartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	key := ctx.ExecutionKey()
	executionInfo := ctx.ExecutionInfo()
	var closeTime *timestamppb.Timestamp
	var executionDuration *durationpb.Duration
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		executionDuration = durationpb.New(executionInfo.CloseTime.Sub(a.GetScheduleTime().AsTime()))
		closeTime = timestamppb.New(executionInfo.CloseTime)
	}

	var expirationTime *timestamppb.Timestamp
	if deadline := a.scheduleToCloseDeadline(); !deadline.IsZero() {
		expirationTime = timestamppb.New(deadline)
	}

	sa := &commonpb.SearchAttributes{
		IndexedFields: a.Visibility.Get(ctx).CustomSearchAttributes(ctx),
	}

	info := &apiactivitypb.ActivityExecutionInfo{
		ActivityId:              key.BusinessID,
		ActivityType:            a.GetActivityType(),
		Attempt:                 attempt.GetCount(),
		CanceledReason:          a.CancelState.GetReason(),
		CloseTime:               closeTime,
		CurrentRetryInterval:    a.currentRetryInterval(ctx, attempt),
		ExecutionDuration:       executionDuration,
		ExecutionTime:           timestamppb.New(a.firstDispatchTime()),
		ExpirationTime:          expirationTime,
		Header:                  requestData.GetHeader(),
		HeartbeatTimeout:        a.GetHeartbeatTimeout(),
		Links:                   ctx.Links(a),
		TotalHeartbeatCount:     heartbeat.GetTotalHeartbeatCount(),
		LastAttemptCompleteTime: attempt.GetCompleteTime(),
		LastHeartbeatTime:       heartbeat.GetRecordedTime(),
		LastStartedTime:         attempt.GetStartedTime(),
		LastWorkerIdentity:      attempt.GetLastWorkerIdentity(),
		LastDeploymentVersion:   attempt.GetLastDeploymentVersion(),
		SdkName:                 attempt.GetSdkName(),
		SdkVersion:              attempt.GetSdkVersion(),
		NextAttemptScheduleTime: a.nextAttemptDispatchTime(ctx, attempt),
		Priority:                a.GetPriority(),
		RetryPolicy:             a.GetRetryPolicy(),
		RunId:                   key.RunID,
		RunState:                runState,
		ScheduleTime:            a.GetScheduleTime(),
		ScheduleToCloseTimeout:  a.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout:  a.GetScheduleToStartTimeout(),
		StartDelay:              a.GetStartDelay(),
		StartToCloseTimeout:     a.GetStartToCloseTimeout(),
		StateSizeBytes:          int64(executionInfo.ApproximateStateSize),
		StateTransitionCount:    executionInfo.StateTransitionCount,
		SearchAttributes:        sa,
		Status:                  status,
		TaskQueue:               a.GetTaskQueue().GetName(),
		UserMetadata:            a.effectiveUserMetadata(ctx),
	}
	if request.GetIncludeHeartbeatDetails() {
		info.HeartbeatDetails = heartbeat.GetDetails()
	}
	if request.GetIncludeLastFailure() {
		info.LastFailure = attempt.GetLastFailureDetails().GetFailure()
	}

	return info
}

// TerminalFailure returns the failure for a closed activity. The failure may be stored in Outcome.Failed
// (terminated, canceled, timed out) or in LastAttempt.LastFailureDetails (failed after exhausting retries).
// Returns nil if no failure is found.
func (a *Activity) TerminalFailure(ctx chasm.Context) *failurepb.Failure {
	if f := a.Outcome.Get(ctx).GetFailed(); f != nil {
		return f.GetFailure()
	}
	if details := a.LastAttempt.Get(ctx).GetLastFailureDetails(); details != nil {
		return details.GetFailure()
	}
	return nil
}
