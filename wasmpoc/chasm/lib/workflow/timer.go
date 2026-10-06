package workflow

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/wasmpoc/chasm"
	"go.temporal.io/server/wasmpoc/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Timer is a durable timer started by a workflow command. It fires via a pure task.
type Timer struct {
	chasm.UnimplementedComponent

	*workflowpb.TimerState

	Workflow chasm.ParentPtr[*Workflow]
}

func (t *Timer) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if t.Status == workflowpb.TimerStatus_TIMER_STATUS_STARTED {
		return chasm.LifecycleStateRunning
	}
	return chasm.LifecycleStateCompleted
}

func (w *Workflow) startTimer(ctx chasm.MutableContext, attrs *commandpb.StartTimerCommandAttributes, completedEventID int64) {
	event := w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			TimerId:                      attrs.GetTimerId(),
			StartToFireTimeout:           attrs.GetStartToFireTimeout(),
			WorkflowTaskCompletedEventId: completedEventID,
		}},
	})
	fireTime := ctx.Now(w).Add(attrs.GetStartToFireTimeout().AsDuration())
	timer := &Timer{TimerState: &workflowpb.TimerState{
		Status:         workflowpb.TimerStatus_TIMER_STATUS_STARTED,
		TimerId:        attrs.GetTimerId(),
		StartedEventId: event.EventId,
		FireTime:       timestamppb.New(fireTime),
	}}
	w.Timers[attrs.GetTimerId()] = chasm.NewComponentField(ctx, timer)
	ctx.AddTask(timer, chasm.TaskAttributes{ScheduledTime: fireTime}, &workflowpb.TimerFireTask{})
}

func (w *Workflow) cancelTimer(ctx chasm.MutableContext, attrs *commandpb.CancelTimerCommandAttributes, completedEventID int64) error {
	field, ok := w.Timers[attrs.GetTimerId()]
	if !ok || field.Get(ctx).Status != workflowpb.TimerStatus_TIMER_STATUS_STARTED {
		return serviceerror.NewInvalidArgumentf("timer %q is not running", attrs.GetTimerId())
	}
	timer := field.Get(ctx)
	timer.Status = workflowpb.TimerStatus_TIMER_STATUS_CANCELED
	w.appendEvent(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_CANCELED,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
			TimerId:                      timer.TimerId,
			StartedEventId:               timer.StartedEventId,
			WorkflowTaskCompletedEventId: completedEventID,
		}},
	})
	return nil
}

type timerFireTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *timerFireTaskHandler) Validate(_ chasm.Context, t *Timer, _ chasm.TaskInvocation, _ *workflowpb.TimerFireTask) (bool, error) {
	return t.Status == workflowpb.TimerStatus_TIMER_STATUS_STARTED, nil
}

func (h *timerFireTaskHandler) Execute(ctx chasm.MutableContext, t *Timer, _ chasm.TaskAttributes, _ *workflowpb.TimerFireTask) error {
	t.Status = workflowpb.TimerStatus_TIMER_STATUS_FIRED
	t.Workflow.Get(ctx).addExternalEvents(ctx, &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
			TimerId:        t.TimerId,
			StartedEventId: t.StartedEventId,
		}},
	})
	return nil
}
