package workflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/wasmpoc/api/workflowservice/v1"
	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/workflow/gen/workflowpb/v1"
)

// scheduleWorkflowTask adds a WorkflowTaskScheduled event and a task to dispatch it, unless a
// workflow task is already scheduled.
func (w *Workflow) scheduleWorkflowTask(ctx chasm.MutableContext, attempt int32) {
	if w.WorkflowTaskScheduledEventId != 0 || !w.isRunning() {
		return
	}
	event := w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: w.TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			StartToCloseTimeout: w.WorkflowTaskTimeout,
			Attempt:             attempt,
		}},
	})
	w.WorkflowTaskScheduledEventId = event.EventId
	w.WorkflowTaskAttempt = attempt
	w.WorkflowTaskStamp++
	ctx.AddTask(w, chasm.TaskAttributes{}, &workflowpb.WorkflowTaskDispatchTask{Stamp: w.WorkflowTaskStamp})
}

// StartWorkflowTask records that a worker polled the scheduled workflow task and returns the poll
// response carrying the full history.
func (w *Workflow) StartWorkflowTask(
	ctx chasm.MutableContext,
	request *workflowservice.PollWorkflowTaskQueueRequest,
	stamp int32,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	if w.WorkflowTaskScheduledEventId == 0 || w.workflowTaskStarted() || stamp != w.WorkflowTaskStamp {
		return nil, serviceerror.NewNotFound("workflow task not found")
	}
	scheduledEvent := w.Events[w.WorkflowTaskScheduledEventId].Get(ctx)
	startedEvent := w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: w.WorkflowTaskScheduledEventId,
			Identity:         request.GetIdentity(),
			HistorySizeBytes: w.HistorySizeBytes,
		}},
	})
	w.WorkflowTaskStartedEventId = startedEvent.EventId

	ref, err := ctx.Ref(w)
	if err != nil {
		return nil, err
	}
	token, err := (&tokenspb.Task{
		NamespaceId:      ctx.ExecutionKey().NamespaceID,
		WorkflowId:       ctx.ExecutionKey().BusinessID,
		RunId:            ctx.ExecutionKey().RunID,
		ScheduledEventId: w.WorkflowTaskScheduledEventId,
		StartedEventId:   w.WorkflowTaskStartedEventId,
		Attempt:          w.WorkflowTaskAttempt,
		ComponentRef:     ref,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	return &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken: token,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: ctx.ExecutionKey().BusinessID,
			RunId:      ctx.ExecutionKey().RunID,
		},
		WorkflowType:               &commonpb.WorkflowType{Name: w.WorkflowType},
		PreviousStartedEventId:     w.LastCompletedWorkflowTaskStartedEventId,
		StartedEventId:             w.WorkflowTaskStartedEventId,
		Attempt:                    w.WorkflowTaskAttempt,
		History:                    &historypb.History{Events: w.History(ctx)},
		WorkflowExecutionTaskQueue: &taskqueuepb.TaskQueue{Name: w.TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ScheduledTime:              scheduledEvent.GetEventTime(),
		StartedTime:                startedEvent.GetEventTime(),
	}, nil
}

// CompleteWorkflowTask applies the worker's commands.
func (w *Workflow) CompleteWorkflowTask(
	ctx chasm.MutableContext,
	token *tokenspb.Task,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) error {
	if err := w.validateWorkflowTaskToken(token); err != nil {
		return err
	}
	if len(w.BufferedEvents.Get(ctx).GetEvents()) > 0 && closesWorkflow(request.GetCommands()) {
		// As in the server: the workflow must see the buffered events before it may close.
		return w.failWorkflowTask(ctx, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, request.GetIdentity())
	}
	completedEvent := w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: w.WorkflowTaskScheduledEventId,
			StartedEventId:   w.WorkflowTaskStartedEventId,
			Identity:         request.GetIdentity(),
		}},
	})
	w.LastCompletedWorkflowTaskStartedEventId = w.WorkflowTaskStartedEventId
	w.clearWorkflowTask()

	for _, command := range request.GetCommands() {
		if err := w.handleCommand(ctx, command, completedEvent.EventId); err != nil {
			return err
		}
	}
	if w.flushBufferedEvents(ctx) || request.GetForceCreateNewWorkflowTask() {
		w.scheduleWorkflowTask(ctx, 1)
	}
	return nil
}

// FailWorkflowTask records the worker's failure of the workflow task and schedules another attempt.
func (w *Workflow) FailWorkflowTask(
	ctx chasm.MutableContext,
	token *tokenspb.Task,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
) error {
	if err := w.validateWorkflowTaskToken(token); err != nil {
		return err
	}
	return w.failWorkflowTask(ctx, request.GetCause(), request.GetIdentity())
}

// failWorkflowTask adds a WorkflowTaskFailed event and schedules the next attempt. The server does
// not write events for attempts after the first (transient workflow tasks); this does.
func (w *Workflow) failWorkflowTask(ctx chasm.MutableContext, cause enumspb.WorkflowTaskFailedCause, identity string) error {
	w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: w.WorkflowTaskScheduledEventId,
			StartedEventId:   w.WorkflowTaskStartedEventId,
			Cause:            cause,
			Identity:         identity,
		}},
	})
	attempt := w.WorkflowTaskAttempt + 1
	w.clearWorkflowTask()
	w.flushBufferedEvents(ctx)
	w.scheduleWorkflowTask(ctx, attempt)
	return nil
}

func (w *Workflow) clearWorkflowTask() {
	w.WorkflowTaskScheduledEventId = 0
	w.WorkflowTaskStartedEventId = 0
}

func (w *Workflow) validateWorkflowTaskToken(token *tokenspb.Task) error {
	if !w.workflowTaskStarted() ||
		token.GetScheduledEventId() != w.WorkflowTaskScheduledEventId ||
		token.GetStartedEventId() != w.WorkflowTaskStartedEventId {
		return serviceerror.NewNotFound("workflow task not found")
	}
	return nil
}

// workflowTaskDispatchTaskHandler hands a scheduled workflow task to the task queue.
type workflowTaskDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*workflowpb.WorkflowTaskDispatchTask]
	matchingClient MatchingClient
}

func (h *workflowTaskDispatchTaskHandler) Validate(
	_ chasm.Context,
	w *Workflow,
	_ chasm.TaskInvocation,
	task *workflowpb.WorkflowTaskDispatchTask,
) (bool, error) {
	return w.WorkflowTaskScheduledEventId != 0 && !w.workflowTaskStarted() && task.GetStamp() == w.WorkflowTaskStamp, nil
}

func (h *workflowTaskDispatchTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *workflowpb.WorkflowTaskDispatchTask,
) error {
	taskQueue, err := chasm.ReadComponent(ctx, ref, func(w *Workflow, _ chasm.Context, _ struct{}) (string, error) {
		return w.TaskQueue, nil
	}, struct{}{})
	if err != nil {
		return err
	}
	return h.matchingClient.AddWorkflowTask(ctx, taskQueue, ref, task.GetStamp())
}
