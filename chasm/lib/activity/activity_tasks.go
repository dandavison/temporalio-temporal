package activity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

type activityDispatchTaskHandlerOptions struct {
	fx.In

	MatchingClient resource.MatchingClient
}

type activityDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.ActivityDispatchTask]
	opts activityDispatchTaskHandlerOptions
}

func newActivityDispatchTaskHandler(opts activityDispatchTaskHandlerOptions) *activityDispatchTaskHandler {
	return &activityDispatchTaskHandler{
		opts: opts,
	}
}

func (h *activityDispatchTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.ActivityDispatchTask,
) (bool, error) {
	return (TransitionStarted.Possible(activity) &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()), nil
}

func (h *activityDispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityDispatchTask,
) error {
	return h.pushToMatching(ctx, activityRef)
}

// Discard spills the task to matching instead of silently discarding it on standby clusters when the activity
// dispatch task has been pending past the discard delay.
func (h *activityDispatchTaskHandler) Discard(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityDispatchTask,
) error {
	return h.pushToMatching(ctx, activityRef)
}

func (h *activityDispatchTaskHandler) pushToMatching(
	ctx context.Context,
	activityRef chasm.ComponentRef,
) error {
	request, err := chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).createAddActivityTaskRequest,
		activityRef.NamespaceID,
	)
	if err != nil {
		return err
	}

	_, err = h.opts.MatchingClient.AddActivityTask(ctx, request)

	return err
}

type scheduleToStartTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func newScheduleToStartTimeoutTaskHandler() *scheduleToStartTimeoutTaskHandler {
	return &scheduleToStartTimeoutTaskHandler{}
}

func (h *scheduleToStartTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.ScheduleToStartTimeoutTask,
) (bool, error) {
	return (activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()), nil
}

func (h *scheduleToStartTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToStartTimeoutTask,
) error {
	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}

	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	}

	return TransitionTimedOut.Apply(activity, ctx, event)
}

type scheduleToCloseTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func newScheduleToCloseTimeoutTaskHandler() *scheduleToCloseTimeoutTaskHandler {
	return &scheduleToCloseTimeoutTaskHandler{}
}

func (h *scheduleToCloseTimeoutTaskHandler) Validate(
	_ chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	if !TransitionTimedOut.Possible(activity) {
		return false, nil
	}
	// If schedule-to-close was disabled via an options update, discard this task.
	if activity.GetScheduleToCloseTimeout().AsDuration() <= 0 {
		return false, nil
	}
	// Stamp check: discard tasks from before the most recent ScheduleToCloseTimeoutTask was
	// scheduled (e.g. after a schedule-to-close extension or a disable+re-enable cycle).
	if task.GetStamp() != activity.GetScheduleToCloseStamp() {
		return false, nil
	}
	return true, nil
}

func (h *scheduleToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) error {
	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}
	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	}

	return TransitionTimedOut.Apply(activity, ctx, event)
}

type startToCloseTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func newStartToCloseTimeoutTaskHandler() *startToCloseTimeoutTaskHandler {
	return &startToCloseTimeoutTaskHandler{}
}

func (h *startToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.StartToCloseTimeoutTask,
) (bool, error) {
	valid := activity.hasAttemptInProgress() &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()
	return valid, nil
}

// Execute executes a StartToCloseTimeoutTask. It fails the attempt, leading to retry or activity
// failure.
func (h *startToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.StartToCloseTimeoutTask,
) error {
	retryState, err := activity.tryReschedule(ctx, true, 0, createStartToCloseTimeoutFailure())
	if err != nil {
		return err
	}

	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		activity.emitOnAttemptTimedOutMetrics(ctx, metricsHandler, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)

		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		retryState:     retryState,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	})
}

// HeartbeatTimeoutTask is a pure task that enforces heartbeat timeouts.
type heartbeatTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func newHeartbeatTimeoutTaskHandler() *heartbeatTimeoutTaskHandler {
	return &heartbeatTimeoutTaskHandler{}
}

// Validate validates a HeartbeatTimeoutTask.
func (h *heartbeatTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.HeartbeatTimeoutTask,
) (bool, error) {
	// The task is registered as a singleton (SingletonTaskModeReplace), so each heartbeat replaces
	// the outstanding timeout task: at most one exists per attempt and it always reflects the latest
	// heartbeat. Staleness relative to a newer heartbeat is therefore structurally impossible, and
	// we only need to reject a task whose attempt is no longer in progress or has been superseded.
	if !activity.hasAttemptInProgress() {
		return false, nil
	}
	attempt := activity.LastAttempt.Get(ctx)
	if attempt.GetStamp() != task.Stamp {
		return false, nil
	}
	return true, nil
}

// Execute executes a HeartbeatTimeoutTask. It fails the attempt, leading to retry or activity
// failure.
func (h *heartbeatTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.HeartbeatTimeoutTask,
) error {
	retryState, err := activity.tryReschedule(ctx, true, 0, createHeartbeatTimeoutFailure())
	if err != nil {
		return err
	}

	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		activity.emitOnAttemptTimedOutMetrics(ctx, metricsHandler, enumspb.TIMEOUT_TYPE_HEARTBEAT)
		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		retryState:     retryState,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	})
}
