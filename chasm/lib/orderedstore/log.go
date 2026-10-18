// Package orderedstore is a stub of the ordered storage being built for CHASM components. It
// provides only what that design has settled: events are appended in batches, a batch is persisted
// as one row, and a component can hold any number of logs.
//
// Two things here are guesses, because they are undecided:
//   - Events are addressed by a global, contiguous event ID. The alternative is (batch ID, event
//     ID).
//   - Events are bytes.
//
// Trimming from the head is settled but not stubbed, because nothing uses it yet, so a log grows
// for as long as its component exists.
package orderedstore

import (
	"sort"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/orderedstore/gen/orderedstorepb/v1"
)

// Log is an append-only sequence of events. The stub stores each batch as a CHASM data node.
type Log struct {
	chasm.UnimplementedComponent

	*orderedstorepb.LogState

	Batches chasm.Map[int64, *orderedstorepb.Batch]
}

func New() *Log {
	return &Log{
		LogState: &orderedstorepb.LogState{},
		Batches:  make(chasm.Map[int64, *orderedstorepb.Batch]),
	}
}

func (l *Log) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Append persists events as one batch and returns the ID of the first. Appending no events
// persists nothing.
func (l *Log) Append(ctx chasm.MutableContext, events ...[]byte) int64 {
	first := l.NextEventId
	if len(events) == 0 {
		return first
	}
	l.Batches[l.NextBatchId] = chasm.NewDataField(ctx, &orderedstorepb.Batch{FirstEventId: first, Events: events})
	l.NextBatchId++
	l.NextEventId += int64(len(events))
	return first
}

// Read calls fn with each event from ID from onwards, in order, until fn returns false.
func (l *Log) Read(ctx chasm.Context, from int64, fn func(id int64, event []byte) bool) {
	batch := func(i int) *orderedstorepb.Batch { return l.Batches[int64(i)].Get(ctx) }
	start := sort.Search(int(l.NextBatchId), func(i int) bool {
		b := batch(i)
		return b.FirstEventId+int64(len(b.Events)) > from
	})
	for i := start; i < int(l.NextBatchId); i++ {
		b := batch(i)
		for j, event := range b.Events {
			id := b.FirstEventId + int64(j)
			if id >= from && !fn(id, event) {
				return
			}
		}
	}
}
