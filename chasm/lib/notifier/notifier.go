package notifier

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/notifier/gen/notifierpb/v1"
)

var _ chasm.RootComponent = (*Notifier)(nil)

type Notifier struct {
	chasm.UnimplementedComponent

	*notifierpb.NotifierState
}

func (n *Notifier) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (n *Notifier) Terminate(
	chasm.MutableContext,
	chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("notifier does not support termination yet")
}

func (n *Notifier) ContextMetadata(chasm.Context) map[string]string {
	return nil
}
