package batch

import (
	"go.temporal.io/server/chasm"
	batchpb "go.temporal.io/server/chasm/lib/batch/gen/batchpb/v1"
)

// Partition is an internal execution owning a subset of a batch's items. It is the unit of
// placement, locking, and scaling, and is invisible in the API. A batch scales by adding
// partitions, not by growing them. Items are held as rows in its tree; admission advances
// its cursor as items drain.
type Partition struct {
	chasm.UnimplementedComponent

	*batchpb.PartitionState

	// Items are this partition's rows, keyed by item ID.
	Items chasm.Map[string, *Item]

	// Packs are the in-flight dispatch units, keyed by pack ID.
	Packs chasm.Map[string, *Pack]
}

func (p *Partition) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return executionLifecycleState(p.GetStatus())
}
