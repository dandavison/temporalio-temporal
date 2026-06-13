package batch

import (
	"go.temporal.io/server/chasm"
	batchpb "go.temporal.io/server/chasm/lib/batch/gen/batchpb/v1"
)

// Item is a durable, addressable unit of accountability — addressable as (jobID, itemID),
// resolving to a row in a partition's tree. It is deliberately not an execution: its
// per-attempt state machine is delegated to the processor (a Pack's Activity, or a Nexus
// operation), and only accountability state is recorded here.
type Item struct {
	chasm.UnimplementedComponent

	*batchpb.ItemState

	// Outcome holds (a reference to) the successful result.
	Outcome chasm.Field[*batchpb.ItemOutcome]
}

func (i *Item) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch i.GetStatus() {
	case batchpb.ITEM_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case batchpb.ITEM_STATUS_FAILED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}
