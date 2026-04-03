package workflow

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Workflow struct {
	chasm.UnimplementedComponent

	// For now, workflow state is managed by mutable_state_impl, not CHASM engine, leaving it empty as CHASM expects a
	// state object.
	*emptypb.Empty

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the workflow.
	Callbacks chasm.Map[string, *callback.Callback]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		MSPointer: msPointer,
	}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise, tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}

// ProcessCloseCallbacks triggers "WorkflowClosed" callbacks using the CHASM implementation.
// It iterates through all callbacks and schedules WorkflowClosed ones that are in STANDBY state.
func (w *Workflow) ProcessCloseCallbacks(ctx chasm.MutableContext) error {
	return callback.ScheduleStandbyCallbacks(ctx, w.Callbacks)
}

// AddCompletionCallbacks creates completion callbacks using the CHASM implementation.
// maxCallbacksPerWorkflow is the configured maximum number of callbacks allowed per workflow.
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacksPerWorkflow int,
) error {
	currentCount := len(w.Callbacks)
	if len(completionCallbacks)+currentCount > maxCallbacksPerWorkflow {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to an execution (%d callbacks already attached)",
			maxCallbacksPerWorkflow,
			currentCount,
		)
	}

	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	for idx, cb := range completionCallbacks {
		callbackObj, err := callback.NewCallbackFromAPICallback(requestID, eventTime, cb)
		if err != nil {
			return err
		}
		id := fmt.Sprintf("%s-%d", requestID, idx)
		w.Callbacks[id] = chasm.NewComponentField(ctx, callbackObj)
	}
	return nil
}

func (w *Workflow) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// Retrieve the completion data from the underlying mutable state via MSPointer
	return w.MSPointer.GetNexusCompletion(ctx, requestID)
}
