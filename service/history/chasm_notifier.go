package history

import (
	"sync"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/collection"
)

type subscriptionTracker struct {
	ch             chan struct{}
	numSubscribers int
}

// ChasmNotifier allows subscribers to receive notifications relating to a CHASM execution.
type ChasmNotifier struct {
	executions collection.ConcurrentTxMap
}

// NewChasmNotifier creates a new instance of ChasmNotifier.
func NewChasmNotifier() *ChasmNotifier {
	hashFn := func(key interface{}) uint32 {
		k := key.(chasm.ExecutionKey)
		var h uint32
		for _, c := range k.NamespaceID + k.BusinessID + k.RunID {
			h = h*31 + uint32(c)
		}
		return h
	}
	return &ChasmNotifier{
		executions: collection.NewShardedConcurrentTxMap(1024, hashFn),
	}
}

// Subscribe returns a channel that will be closed when there is a notification relating to the
// execution, along with an unsubscribe function. No data will be written to the channel: on
// notification, the caller should determine whether the execution state they are waiting for has
// been reached and resubscribe if necessary, while holding a lock on the execution. The caller must
// arrange for the unsubscribe function to be called when they have finished monitoring the channel
// for notifications. It is safe to call the unsubscribe function multiple times and concurrently.
func (n *ChasmNotifier) Subscribe(key chasm.ExecutionKey) (<-chan struct{}, func()) {
	newTracker := &subscriptionTracker{ch: make(chan struct{}), numSubscribers: 1}
	result, existed, _ := n.executions.PutOrDo(key, newTracker, func(_, v interface{}) error {
		v.(*subscriptionTracker).numSubscribers++
		return nil
	})
	s := result.(*subscriptionTracker)
	if existed {
		// newTracker wasn't used; close its channel to avoid leaking
		close(newTracker.ch)
	}
	return s.ch, sync.OnceFunc(func() {
		n.executions.RemoveIf(key, func(_, v interface{}) bool {
			tracker := v.(*subscriptionTracker)
			if tracker != s {
				return false
			}
			tracker.numSubscribers--
			return tracker.numSubscribers == 0
		})
	})
}

// Notify notifies all subscribers subscribed to key by closing the channel.
func (n *ChasmNotifier) Notify(key chasm.ExecutionKey) {
	n.executions.RemoveIf(key, func(_, v interface{}) bool {
		close(v.(*subscriptionTracker).ch)
		return true
	})
}
