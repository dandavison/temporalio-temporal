package stream

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

var _ chasm.RootComponent = (*Stream)(nil)

type Stream struct {
	chasm.UnimplementedComponent

	*streampb.StreamState
}

func (s *Stream) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (s *Stream) Terminate(
	chasm.MutableContext,
	chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("stream does not support termination yet")
}

func (s *Stream) ContextMetadata(chasm.Context) map[string]string {
	return nil
}
