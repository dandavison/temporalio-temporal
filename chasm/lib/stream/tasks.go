package stream

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type idleTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func (idleTimeoutTaskHandler) Execute(ctx chasm.MutableContext, s *Standalone, attrs chasm.TaskAttributes, _ *streampb.IdleTimeoutTask) error {
	return s.onIdleTimeoutTask(ctx, attrs.ScheduledTime)
}

func (idleTimeoutTaskHandler) Validate(ctx chasm.Context, s *Standalone, invocation chasm.TaskInvocation, _ *streampb.IdleTimeoutTask) (bool, error) {
	return s.idleCheckPending(ctx, invocation.ScheduledTime), nil
}
