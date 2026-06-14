package collection

import (
	"go.temporal.io/server/chasm"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
)

// Collection is the root component of the Collection archetype: a tree of Partitions
// over a large population of Items, each carrying per-item accountability for an
// operation. This change registers the archetype as a no-op; the data model follows.
type Collection struct {
	chasm.UnimplementedComponent

	*collectionpb.CollectionState
}

func (c *Collection) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
