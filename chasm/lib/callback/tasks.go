package callback

import (
	"context"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
)

// InvocationTaskExecutor delivers a callback when its invocation task runs.
type InvocationTaskExecutor interface {
	Execute(
		ctx context.Context,
		ref chasm.ComponentRef,
		taskAttr chasm.TaskAttributes,
		task *callbackspb.InvocationTask,
	) error
}

type invocationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*callbackspb.InvocationTask]
	executor InvocationTaskExecutor
}

func (h *invocationTaskHandler) Validate(ctx chasm.Context, cb *Callback, attrs chasm.TaskInvocation, task *callbackspb.InvocationTask) (bool, error) {
	return cb.Attempt == task.Attempt && cb.Status == callbackspb.CALLBACK_STATUS_SCHEDULED, nil
}

func (h *invocationTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	taskAttr chasm.TaskAttributes,
	task *callbackspb.InvocationTask,
) error {
	return h.executor.Execute(ctx, ref, taskAttr, task)
}

type backoffTaskHandler struct {
	chasm.PureTaskHandlerBase
}

// Execute toggles the callback status from BACKING_OFF to SCHEDULED to trigger a new invocation attempt.
func (h *backoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	callback *Callback,
	taskAttrs chasm.TaskAttributes,
	task *callbackspb.BackoffTask,
) error {
	return TransitionRescheduled.Apply(callback, ctx, EventRescheduled{})
}

// Validate validates that the callback is in BACKING_OFF state and that the attempt number matches before allowing the
// backoff task to execute.
func (h *backoffTaskHandler) Validate(
	ctx chasm.Context,
	callback *Callback,
	taskAttr chasm.TaskInvocation,
	task *callbackspb.BackoffTask,
) (bool, error) {
	return callback.Status == callbackspb.CALLBACK_STATUS_BACKING_OFF && callback.Attempt == task.Attempt, nil
}
