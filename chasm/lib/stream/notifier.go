package stream

import (
	"math/rand/v2"
	"sync"

	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

const (
	NotifierStateOperation = "NotifierState"
	NotifyOperation        = "Notify"
)

// Notifier holds the pending Waits of readers of a stream in an external store, such as Redis. It
// is embedded in the stream's owner, and created by the first Wait. Producers call Notify after
// appending, which fires its subscriptions, and writes nothing when there are none. The server
// cannot see the external store, so whether a record is new is told by a counter of Notify calls
// (see NotifyCounters), not by the records.
type Notifier struct {
	chasm.UnimplementedComponent

	*streampb.NotifierState

	Subscriptions Subscriptions
}

func NewNotifier() *Notifier {
	return &Notifier{NotifierState: &streampb.NotifierState{}}
}

func (n *Notifier) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Subscribe stores a subscription for a Wait; see subscribe.
func (n *Notifier) Subscribe(ctx chasm.MutableContext, args WaitArgs) error {
	return subscribe(ctx, &n.Subscriptions, args.RequestID, args.Callback, -1, nil, false, args.Req.GetLifetime().AsDuration())
}

// CancelSubscription removes the subscription of a canceled Wait, if it has not fired.
func (n *Notifier) CancelSubscription(ctx chasm.MutableContext, requestID string) {
	cancelSubscription(ctx, n.Subscriptions, requestID)
}

// Fire completes every pending Wait with reason.
func (n *Notifier) Fire(ctx chasm.MutableContext, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	return fireAll(ctx, n.Subscriptions, reason, ownerEnd)
}

// HasPending reports whether a Wait is pending, so that a Notify needs a write.
func (n *Notifier) HasPending(ctx chasm.Context) bool {
	return unfired(ctx, n.Subscriptions) > 0
}

// NotifyCounters counts each owner's Notify calls in this History host's memory, so that a Notify
// with no subscription writes nothing. A Wait at the position its reader read before reading the
// external store completes at once if a Notify has moved the count since. The count is lost when the
// owner's shard moves to another host, or its entry is evicted; a new entry has a new random
// generation, so a Wait at a lost position completes at once, a spurious wake.
type NotifyCounters struct {
	mu      sync.Mutex
	entries map[string]*streamservicepb.NotifierPosition
}

// maxNotifyCounters bounds the entries kept. Beyond it an arbitrary entry is evicted.
const maxNotifyCounters = 100_000

var notifyCounters = &NotifyCounters{entries: make(map[string]*streamservicepb.NotifierPosition)}

// NotifierOwnerKey names a stream's owner in NotifyCounters: its namespace, kind, ID and run (a
// workflow's first run ID, or the activity's run ID).
func NotifierOwnerKey(ref *streamservicepb.StreamRef) string {
	if ref.GetActivityId() != "" {
		return ref.GetNamespace() + "/activity/" + ref.GetActivityId() + "/" + ref.GetActivityRunId()
	}
	return ref.GetNamespace() + "/workflow/" + ref.GetWorkflowId() + "/" + ref.GetFirstRunId()
}

// Counters is the History host's notify counters.
func Counters() *NotifyCounters {
	return notifyCounters
}

// Position returns the owner's current position, creating its entry if needed.
func (c *NotifyCounters) Position(owner string) *streamservicepb.NotifierPosition {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.entry(owner)
	return &streamservicepb.NotifierPosition{Generation: p.Generation, Counter: p.Counter}
}

// Increment counts a Notify for the owner.
func (c *NotifyCounters) Increment(owner string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entry(owner).Counter++
}

func (c *NotifyCounters) entry(owner string) *streamservicepb.NotifierPosition {
	p, ok := c.entries[owner]
	if ok {
		return p
	}
	if len(c.entries) >= maxNotifyCounters {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	p = &streamservicepb.NotifierPosition{Generation: rand.Int64()}
	c.entries[owner] = p
	return p
}
