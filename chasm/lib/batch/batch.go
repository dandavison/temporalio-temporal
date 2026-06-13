// Package batch defines the CHASM data model for a batch execution: an operation applied
// across a large, enumerable population of items — records read from a Manifest, or
// existing executions selected by a query — with per-item accountability, retry, repair,
// and flow control.
//
// The tree is one Batch root execution plus N internal Partition executions. Each
// Partition owns a subset of the batch's items as Item rows in its tree; in-flight items
// are grouped into Packs (the dispatch unit), and world-touching steps are recorded as
// Effects on an Item. An Item is durable and addressable as (jobID, itemID) but is not
// itself an execution.
package batch

import (
	"go.temporal.io/server/chasm"
	batchpb "go.temporal.io/server/chasm/lib/batch/gen/batchpb/v1"
)

// Batch is the root component of a batch execution. It carries the policy and an
// eventually-consistent rollup of per-item counts; the items themselves live in the
// Partition executions it references.
type Batch struct {
	chasm.UnimplementedComponent

	*batchpb.BatchState

	// Manifest is the durable description of the input set: one record regardless of
	// cardinality. Pending items live in the source, not here.
	Manifest chasm.Field[*batchpb.Manifest]

	Visibility chasm.Field[*chasm.Visibility]
}

func (b *Batch) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return executionLifecycleState(b.GetStatus())
}

// executionLifecycleState maps the shared BatchExecutionStatus (used by both Batch and
// Partition) onto the framework lifecycle.
func executionLifecycleState(status batchpb.BatchExecutionStatus) chasm.LifecycleState {
	switch status {
	case batchpb.BATCH_EXECUTION_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case batchpb.BATCH_EXECUTION_STATUS_FAILED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}
