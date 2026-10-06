package activity

import (
	"fmt"
	"math/rand"
	"time"

	//nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/wasmpoc/common/metrics"
	"go.temporal.io/server/wasmpoc/common/payload"
	"go.temporal.io/server/wasmpoc/common/protoutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (a *Activity) handleCancellationRequested(ctx chasm.MutableContext, request *activitypb.RequestCancelActivityExecutionRequest) (
	*activitypb.RequestCancelActivityExecutionResponse, error,
) {
	req := request.GetFrontendRequest()
	newReqID := req.GetRequestId()
	existingReqID := a.GetCancelState().GetRequestId()

	// Deduplicate first because a retry may arrive after the activity transitions to Canceled.
	if newReqID != "" && existingReqID == newReqID {
		return &activitypb.RequestCancelActivityExecutionResponse{}, nil
	}

	if a.isTerminal() {
		return nil, a.errClosed()
	}

	// Reject a second cancellation request with a different request ID.
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return nil, serviceerror.NewFailedPrecondition(
			fmt.Sprintf("cancellation already requested with request ID %s", existingReqID))
	}

	hasAttemptInProgress := a.hasAttemptInProgress()
	originalStatus := a.GetStatus()

	// Always transition to CancelRequested
	// TODO: this is questionable, since CancelRequested is otherwise a state that implies an attempt
	// is in progress.
	if err := TransitionCancelRequested.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	// Transition to Canceled if no attempt in progress; otherwise wait for worker response.
	if !hasAttemptInProgress {
		metricsHandler := a.completionMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCanceledScope)
		err := TransitionCanceled.Apply(a, ctx, cancelEvent{
			metricsHandler: metricsHandler,
			fromStatus:     originalStatus,
			details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					payload.EncodeString(req.GetReason()),
				},
			},
		})
		if err != nil {
			return nil, err
		}
	}

	return &activitypb.RequestCancelActivityExecutionResponse{}, nil
}

// isPaused reports whether the activity is currently paused (waiting) or has a pending pause request
// (worker still running).
func (a *Activity) isPaused() bool {
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return true
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		return a.GetResetShouldPause()
	default:
		return false
	}
}

// unpauseDispatchTime computes when an unpaused attempt should be dispatched
func (a *Activity) unpauseDispatchTime(ctx chasm.MutableContext, event unpauseEvent) time.Time {
	unpauseTime := ctx.Now(a)
	if jitter := event.req.GetJitter().AsDuration(); jitter > 0 {
		unpauseTime = unpauseTime.Add(time.Duration(rand.Int63n(int64(jitter)))) //nolint:gosec
	}
	dispatchTime := a.dispatchTimeRespectingStartDelay(unpauseTime)
	retryDispatchTime := dispatchTimeForRetry(a.LastAttempt.Get(ctx))
	if retryDispatchTime != nil && retryDispatchTime.AsTime().After(dispatchTime) {
		return retryDispatchTime.AsTime()
	}
	return dispatchTime
}

func (a *Activity) recordPauseState(
	ctx chasm.MutableContext,
	event pauseEvent,
) {
	a.LastPauseState = &activitypb.ActivityPauseState{
		PauseTime: timestamppb.New(ctx.Now(a)),
		Identity:  event.req.GetIdentity(),
		Reason:    event.req.GetReason(),
		RequestId: event.req.GetRequestId(),
	}
	a.emitOnPausedMetrics(event.metricsHandler)
}

func (a *Activity) clearHeartbeatDetails(ctx chasm.MutableContext) {
	if hb, ok := a.LastHeartbeat.TryGet(ctx); ok {
		hb.Details = nil
		hb.RecordedTime = nil
	}
}

// applyDeferredOptionRestore applies a Reset(RestoreOriginalOptions) that was deferred because a
// worker was running an attempt at reset time (see handleReset).
func (a *Activity) applyDeferredOptionRestore(ctx chasm.MutableContext) {
	if !a.ResetRestoreOptions {
		return
	}
	a.ResetRestoreOptions = false
	a.restoreOriginalOptions(ctx)
}

// applyDeferredHeartbeatClear applies a Reset(ResetHeartbeat) that was deferred because a worker was
// running an attempt at reset time (see handleReset).
func (a *Activity) applyDeferredHeartbeatClear(ctx chasm.MutableContext) {
	if !a.ResetShouldClearHeartbeat {
		return
	}
	a.ResetShouldClearHeartbeat = false
	a.clearHeartbeatDetails(ctx)
}

// restoreOriginalOptions resets the activity's options to the values it was originally scheduled
// with and reissues the ScheduleToClose timer at the resulting deadline. start_delay is restored
// only if the activity has never started.
func (a *Activity) restoreOriginalOptions(ctx chasm.MutableContext) {
	og := a.GetOriginalOptions()
	a.TaskQueue = protoutil.CloneProto(og.GetTaskQueue())
	a.ScheduleToCloseTimeout = protoutil.CloneProto(og.GetScheduleToCloseTimeout())
	a.ScheduleToStartTimeout = protoutil.CloneProto(og.GetScheduleToStartTimeout())
	a.StartToCloseTimeout = protoutil.CloneProto(og.GetStartToCloseTimeout())
	a.HeartbeatTimeout = protoutil.CloneProto(og.GetHeartbeatTimeout())
	a.RetryPolicy = protoutil.CloneProto(og.GetRetryPolicy())
	a.Priority = protoutil.CloneProto(og.GetPriority())
	if a.GetFirstAttemptStartedTime() == nil {
		a.StartDelay = protoutil.CloneProto(og.GetStartDelay())
	}
	a.reissueScheduleToClose(ctx)
}
