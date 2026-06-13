package batch

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	batchpb "go.temporal.io/server/chasm/lib/batch/gen/batchpb/v1"
)

// Pack is the ephemeral dispatch unit: one task carrying many items to a worker. For
// record processing it embeds a single Activity covering all members, so the standard
// activity machinery (dispatch, started, timeout) operates at pack granularity and workers
// see today's activity protocol. A Pack has no identity across retries; failed members are
// repacked into fresh Packs.
type Pack struct {
	chasm.UnimplementedComponent

	*batchpb.PackState

	// Activity is the embedded processor for the pack's members.
	Activity chasm.Field[*activity.Activity]
}

func (p *Pack) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	if act, ok := p.Activity.TryGet(ctx); ok {
		return act.LifecycleState(ctx)
	}
	return chasm.LifecycleStateRunning
}
