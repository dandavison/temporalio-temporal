// Package workflow is a CHASM workflow archetype supporting activities and timers only. It keeps
// its history as CHASM data nodes, one per event, rather than in a mutable-state history branch.
package workflow

import (
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/wasmpoc/api/workflowservice/v1"
	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/activity"
	"go.temporal.io/server/wasmpoc/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultWorkflowTaskTimeout = 10 // seconds

type Workflow struct {
	chasm.UnimplementedComponent

	*workflowpb.WorkflowState

	// Events holds history events keyed by event ID.
	Events chasm.Map[int64, *historypb.HistoryEvent]
	// BufferedEvents holds events that arrived while a workflow task was started. They get event IDs
	// when the workflow task closes.
	BufferedEvents chasm.Field[*historypb.History]
	// Activities is keyed by ActivityTaskScheduled event ID.
	Activities chasm.Map[int64, *activity.Activity]
	// Timers is keyed by timer ID.
	Timers chasm.Map[string, *Timer]
}

var _ activity.ActivityStore = (*Workflow)(nil)

func NewWorkflow(ctx chasm.MutableContext, request *workflowservice.StartWorkflowExecutionRequest) (*Workflow, error) {
	taskTimeout := request.GetWorkflowTaskTimeout()
	if taskTimeout.AsDuration() == 0 {
		taskTimeout = durationpb.New(defaultWorkflowTaskTimeout * 1e9)
	}
	w := &Workflow{
		WorkflowState: &workflowpb.WorkflowState{
			Status:              workflowpb.WorkflowStatus_WORKFLOW_STATUS_RUNNING,
			WorkflowType:        request.GetWorkflowType().GetName(),
			TaskQueue:           request.GetTaskQueue().GetName(),
			WorkflowTaskTimeout: taskTimeout,
			NextEventId:         1,
		},
		Events:         chasm.Map[int64, *historypb.HistoryEvent]{},
		BufferedEvents: chasm.NewDataField(ctx, &historypb.History{}),
		Activities:     chasm.Map[int64, *activity.Activity]{},
		Timers:         chasm.Map[string, *Timer]{},
	}
	w.StartTime = timestamppb.New(ctx.Now(w))
	w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             request.GetWorkflowType(),
			TaskQueue:                request.GetTaskQueue(),
			Input:                    request.GetInput(),
			WorkflowExecutionTimeout: request.GetWorkflowExecutionTimeout(),
			WorkflowRunTimeout:       request.GetWorkflowRunTimeout(),
			WorkflowTaskTimeout:      taskTimeout,
			Identity:                 request.GetIdentity(),
			Header:                   request.GetHeader(),
			Attempt:                  1,
			OriginalExecutionRunId:   ctx.ExecutionKey().RunID,
			FirstExecutionRunId:      ctx.ExecutionKey().RunID,
			WorkflowId:               ctx.ExecutionKey().BusinessID,
		}},
	})
	w.scheduleWorkflowTask(ctx, 1)
	return w, nil
}

func (w *Workflow) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case workflowpb.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case workflowpb.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (w *Workflow) ContextMetadata(_ chasm.Context) map[string]string {
	return map[string]string{"workflow-type": w.WorkflowType}
}

func (w *Workflow) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("terminate is not supported")
}

// History returns the history events in event ID order.
func (w *Workflow) History(ctx chasm.Context) []*historypb.HistoryEvent {
	ids := make([]int64, 0, len(w.Events))
	for id := range w.Events {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	events := make([]*historypb.HistoryEvent, len(ids))
	for i, id := range ids {
		events[i] = w.Events[id].Get(ctx)
	}
	return events
}

func (w *Workflow) isRunning() bool {
	return w.Status == workflowpb.WorkflowStatus_WORKFLOW_STATUS_RUNNING
}

func (w *Workflow) workflowTaskStarted() bool {
	return w.WorkflowTaskStartedEventId != 0
}

// appendEvent adds an event to history, assigning its ID and time.
func (w *Workflow) appendEvent(ctx chasm.MutableContext, event *historypb.HistoryEvent) *historypb.HistoryEvent {
	event.EventTime = timestamppb.New(ctx.Now(w))
	w.appendEvents(ctx, []*historypb.HistoryEvent{event})
	return event
}

// appendEvents adds events to history, assigning event IDs. An activity outcome event refers to the
// ActivityTaskStarted event preceding it in the same batch, so its started event ID is assigned here.
func (w *Workflow) appendEvents(ctx chasm.MutableContext, events []*historypb.HistoryEvent) {
	startedEventIDs := map[int64]int64{} // scheduled event ID -> started event ID
	for _, event := range events {
		event.EventId = w.NextEventId
		w.NextEventId++
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			startedEventIDs[attrs.GetScheduledEventId()] = event.EventId
		}
		setActivityStartedEventID(event, startedEventIDs)
		w.Events[event.EventId] = chasm.NewDataField(ctx, event)
		w.HistorySizeBytes += int64(proto.Size(event))
	}
}

// addExternalEvents adds events not caused by a workflow task command. While a workflow task is
// started they are buffered; otherwise they are added to history and a workflow task is scheduled.
func (w *Workflow) addExternalEvents(ctx chasm.MutableContext, events ...*historypb.HistoryEvent) {
	if !w.isRunning() {
		return
	}
	for _, event := range events {
		event.EventTime = timestamppb.New(ctx.Now(w))
	}
	if w.workflowTaskStarted() {
		buffered := w.BufferedEvents.Get(ctx)
		buffered.Events = append(buffered.Events, events...)
		return
	}
	w.appendEvents(ctx, events)
	w.scheduleWorkflowTask(ctx, 1)
}

// flushBufferedEvents adds buffered events to history and reports whether there were any.
func (w *Workflow) flushBufferedEvents(ctx chasm.MutableContext) bool {
	buffered := w.BufferedEvents.Get(ctx)
	if len(buffered.Events) == 0 {
		return false
	}
	w.appendEvents(ctx, buffered.Events)
	w.BufferedEvents = chasm.NewDataField(ctx, &historypb.History{})
	return true
}

func setActivityStartedEventID(event *historypb.HistoryEvent, startedEventIDs map[int64]int64) {
	switch {
	case event.GetActivityTaskCompletedEventAttributes() != nil:
		attrs := event.GetActivityTaskCompletedEventAttributes()
		attrs.StartedEventId = startedEventIDs[attrs.GetScheduledEventId()]
	case event.GetActivityTaskFailedEventAttributes() != nil:
		attrs := event.GetActivityTaskFailedEventAttributes()
		attrs.StartedEventId = startedEventIDs[attrs.GetScheduledEventId()]
	case event.GetActivityTaskTimedOutEventAttributes() != nil:
		attrs := event.GetActivityTaskTimedOutEventAttributes()
		attrs.StartedEventId = startedEventIDs[attrs.GetScheduledEventId()]
	case event.GetActivityTaskCanceledEventAttributes() != nil:
		attrs := event.GetActivityTaskCanceledEventAttributes()
		attrs.StartedEventId = startedEventIDs[attrs.GetScheduledEventId()]
	}
}
