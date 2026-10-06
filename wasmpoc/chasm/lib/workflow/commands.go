package workflow

import (
	activitypb "go.temporal.io/api/activity/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/activity"
	"go.temporal.io/server/wasmpoc/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func closesWorkflow(commands []*commandpb.Command) bool {
	for _, command := range commands {
		switch command.GetCommandType() {
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
			return true
		default:
		}
	}
	return false
}

func (w *Workflow) handleCommand(ctx chasm.MutableContext, command *commandpb.Command, completedEventID int64) error {
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		return w.scheduleActivity(ctx, command.GetScheduleActivityTaskCommandAttributes(), completedEventID)
	case enumspb.COMMAND_TYPE_START_TIMER:
		w.startTimer(ctx, command.GetStartTimerCommandAttributes(), completedEventID)
		return nil
	case enumspb.COMMAND_TYPE_CANCEL_TIMER:
		return w.cancelTimer(ctx, command.GetCancelTimerCommandAttributes(), completedEventID)
	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		w.appendEvent(ctx, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				Result:                       command.GetCompleteWorkflowExecutionCommandAttributes().GetResult(),
				WorkflowTaskCompletedEventId: completedEventID,
			}},
		})
		w.Status = workflowpb.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
		return nil
	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		w.appendEvent(ctx, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure:                      command.GetFailWorkflowExecutionCommandAttributes().GetFailure(),
				RetryState:                   enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
				WorkflowTaskCompletedEventId: completedEventID,
			}},
		})
		w.Status = workflowpb.WorkflowStatus_WORKFLOW_STATUS_FAILED
		return nil
	default:
		return serviceerror.NewUnimplementedf("command %v is not supported", command.GetCommandType())
	}
}

func (w *Workflow) scheduleActivity(
	ctx chasm.MutableContext,
	attrs *commandpb.ScheduleActivityTaskCommandAttributes,
	completedEventID int64,
) error {
	if err := normalizeActivityAttributes(attrs, w.Events[1].Get(ctx).GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout(), w.TaskQueue); err != nil {
		return err
	}
	event := w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId:                   attrs.GetActivityId(),
			ActivityType:                 attrs.GetActivityType(),
			TaskQueue:                    attrs.GetTaskQueue(),
			Header:                       attrs.GetHeader(),
			Input:                        attrs.GetInput(),
			ScheduleToCloseTimeout:       attrs.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout:       attrs.GetScheduleToStartTimeout(),
			StartToCloseTimeout:          attrs.GetStartToCloseTimeout(),
			HeartbeatTimeout:             attrs.GetHeartbeatTimeout(),
			WorkflowTaskCompletedEventId: completedEventID,
			RetryPolicy:                  attrs.GetRetryPolicy(),
			UseWorkflowBuildId:           attrs.GetUseWorkflowBuildId(),
			Priority:                     attrs.GetPriority(),
		}},
	})
	a := activity.NewEmbeddedActivity(ctx, attrs)
	w.Activities[event.EventId] = chasm.NewComponentField(ctx, a)
	return activity.TransitionScheduled.Apply(a, ctx, nil)
}

// RecordCompleted implements activity.ActivityStore: after the activity records its outcome, the
// workflow adds the corresponding history events. As in the server, the ActivityTaskStarted event
// is written together with the outcome.
func (w *Workflow) RecordCompleted(
	ctx chasm.MutableContext,
	a *activity.Activity,
	applyFn func(ctx chasm.MutableContext) error,
) error {
	if err := applyFn(ctx); err != nil {
		return err
	}
	scheduledEventID, ok := w.activityScheduledEventID(ctx, a)
	if !ok {
		return serviceerror.NewInternal("activity not found in workflow")
	}
	attempt := a.LastAttempt.Get(ctx)
	var events []*historypb.HistoryEvent
	if attempt.GetStartedTime() != nil {
		events = append(events, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				Identity:         attempt.GetLastWorkerIdentity(),
				RequestId:        attempt.GetStartRequestId(),
				Attempt:          attempt.GetCount(),
				LastFailure:      attempt.GetLastFailureDetails().GetFailure(),
				WorkerVersion:    w.activityStartedStamp(ctx, scheduledEventID),
			}},
		})
	}
	// The activity's status changes after this hook returns, so the outcome is read from its data.
	outcome := a.Outcome.Get(ctx)
	failure := a.TerminalFailure(ctx)
	switch {
	case outcome.GetSuccessful() != nil:
		events = append(events, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				Result:           outcome.GetSuccessful().GetOutput(),
				ScheduledEventId: scheduledEventID,
			}},
		})
	case failure.GetCanceledFailureInfo() != nil:
		events = append(events, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				Details:          failure.GetCanceledFailureInfo().GetDetails(),
				ScheduledEventId: scheduledEventID,
			}},
		})
	case failure.GetTimeoutFailureInfo() != nil:
		events = append(events, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
			Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				Failure:          failure,
				ScheduledEventId: scheduledEventID,
				RetryState:       outcome.GetRetryState(),
			}},
		})
	default:
		events = append(events, &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
			Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				Failure:          failure,
				ScheduledEventId: scheduledEventID,
				RetryState:       outcome.GetRetryState(),
			}},
		})
	}
	w.addExternalEvents(ctx, events...)
	return nil
}

func (w *Workflow) activityScheduledEventID(ctx chasm.Context, a *activity.Activity) (int64, bool) {
	for id, field := range w.Activities {
		if field.Get(ctx) == a {
			return id, true
		}
	}
	return 0, false
}

// ActivityScheduledEvent returns the ActivityTaskScheduled event of an activity in this workflow.
func (w *Workflow) ActivityScheduledEvent(ctx chasm.Context, a *activity.Activity) (*historypb.HistoryEvent, error) {
	id, ok := w.activityScheduledEventID(ctx, a)
	if !ok {
		return nil, serviceerror.NewNotFound("activity not found in workflow")
	}
	return w.Events[id].Get(ctx), nil
}

// RecordActivityStarted records the worker version stamp of the worker that started an attempt of
// an activity.
func (w *Workflow) RecordActivityStarted(
	ctx chasm.MutableContext,
	a *activity.Activity,
	capabilities *commonpb.WorkerVersionCapabilities,
	options *deploymentpb.WorkerDeploymentOptions,
) error {
	scheduledEventID, ok := w.activityScheduledEventID(ctx, a)
	if !ok {
		return serviceerror.NewInternal("activity not found in workflow")
	}
	w.ActivityStartedStamps[scheduledEventID] = chasm.NewDataField(ctx, stampFromCapabilities(capabilities, options))
	return nil
}

func (w *Workflow) activityStartedStamp(ctx chasm.Context, scheduledEventID int64) *commonpb.WorkerVersionStamp {
	if field, ok := w.ActivityStartedStamps[scheduledEventID]; ok {
		return field.Get(ctx)
	}
	return nil
}

// normalizeActivityAttributes fills in defaults as the server does.
func normalizeActivityAttributes(
	attrs *commandpb.ScheduleActivityTaskCommandAttributes,
	runTimeout *durationpb.Duration,
	workflowTaskQueue string,
) error {
	if runTimeout == nil {
		runTimeout = durationpb.New(0)
	}
	if attrs.RetryPolicy == nil {
		attrs.RetryPolicy = &commonpb.RetryPolicy{}
	}
	options := &activitypb.ActivityOptions{
		TaskQueue:              attrs.TaskQueue,
		ScheduleToCloseTimeout: attrs.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout: attrs.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    attrs.GetStartToCloseTimeout(),
		HeartbeatTimeout:       attrs.GetHeartbeatTimeout(),
		RetryPolicy:            attrs.RetryPolicy,
	}
	if attrs.TaskQueue == nil {
		options.TaskQueue = &taskqueuepb.TaskQueue{}
	}
	if err := activity.ValidateAndNormalizeEmbeddedActivity(
		attrs.GetActivityId(),
		attrs.GetActivityType().GetName(),
		retrypolicy.DefaultDefaultRetrySettings,
		maxIDLengthLimit,
		options,
		attrs.GetPriority(),
		runTimeout,
		workflowTaskQueue,
	); err != nil {
		return err
	}
	attrs.TaskQueue = options.TaskQueue
	attrs.ScheduleToCloseTimeout = options.ScheduleToCloseTimeout
	attrs.ScheduleToStartTimeout = options.ScheduleToStartTimeout
	attrs.StartToCloseTimeout = options.StartToCloseTimeout
	attrs.HeartbeatTimeout = options.HeartbeatTimeout
	attrs.RetryPolicy = options.RetryPolicy
	return nil
}
