package stream

import (
	"math/rand/v2"
	"sync"
	"time"

	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	WaitOperation          = "Wait"
	NotifierStateOperation = "NotifierState"
	NotifyOperation        = "Notify"
)

// Notifier holds the pending Waits on one stream whose records the server cannot see: a stream in
// an external store, such as Redis, whose producers call Notify after appending, or a stream of an
// owner that has not created it yet, whose creation fires them. An owner holds one per stream name,
// created by the first such Wait; a standalone stream in an external store has a notifier execution
// of its own (see StandaloneNotifier). Whether a Notify is new is told by a counter of Notify calls
// (see NotifyCounters), not by the records; a Notify with no subscription writes nothing.
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

// Subscribe stores a subscription for a Wait; see subscribe. wakeOnModeKnown makes the owner's
// first workflow task completion fire it, as for a Wait on a stream not created yet while the
// owner's mode is not known.
func (n *Notifier) Subscribe(ctx chasm.MutableContext, args WaitArgs, wakeOnModeKnown bool) error {
	return subscribe(ctx, &n.Subscriptions, args.RequestID, args.Callback, -1, nil, wakeOnModeKnown, args.Req.GetLifetime().AsDuration())
}

// WaitOnPosition completes a Wait at once if a Notify has moved the stream's position, at key,
// since the reader read it, and otherwise subscribes it.
func (n *Notifier) WaitOnPosition(ctx chasm.MutableContext, args WaitArgs, key string) (*streamservicepb.WaitResponse, error) {
	if !proto.Equal(Counters().Position(key), args.Req.GetNotifier()) {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_APPENDED}, nil
	}
	return nil, n.Subscribe(ctx, args, false)
}

// CancelSubscription removes the subscription of a canceled Wait, if it has not fired.
func (n *Notifier) CancelSubscription(ctx chasm.MutableContext, requestID string) {
	cancelSubscription(ctx, n.Subscriptions, requestID)
}

// Fire completes every pending Wait with reason.
func (n *Notifier) Fire(ctx chasm.MutableContext, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	return fireAll(ctx, n.Subscriptions, reason, ownerEnd)
}

// FireModeKnown completes the pending Waits made while the owner's mode was not known.
func (n *Notifier) FireModeKnown(ctx chasm.MutableContext) error {
	return fireWhere(ctx, n.Subscriptions, streamservicepb.WAKE_REASON_MODE_KNOWN, (*streampb.SubscriptionState).GetWakeOnMarkers)
}

// HasPending reports whether a Wait is pending, so that a Notify needs a write.
func (n *Notifier) HasPending(ctx chasm.Context) bool {
	return unfired(ctx, n.Subscriptions) > 0
}

// CanceledWait names the subscription of a canceled Wait: the stream's name among its owner's
// streams, and the Wait's request ID.
type CanceledWait struct {
	Name      string
	RequestID string
}

// Notifiers are an owner's notifiers, keyed by stream name.
type Notifiers = chasm.Map[string, *Notifier]

// NotifierOf returns the owner's notifier for the stream named name, creating it if needed.
func NotifierOf(ctx chasm.MutableContext, notifiers *Notifiers, name string) *Notifier {
	if field, ok := (*notifiers)[name]; ok {
		return field.Get(ctx)
	}
	n := NewNotifier()
	if *notifiers == nil {
		*notifiers = make(Notifiers)
	}
	(*notifiers)[name] = chasm.NewComponentField(ctx, n)
	return n
}

// FireNotifier completes with reason the pending Waits of the owner's notifier for the stream named
// name, if it has one.
func FireNotifier(ctx chasm.MutableContext, notifiers Notifiers, name string, reason streamservicepb.WakeReason) error {
	if field, ok := notifiers[name]; ok {
		return field.Get(ctx).Fire(ctx, reason, streamservicepb.CLOSE_STATE_UNSPECIFIED)
	}
	return nil
}

// FireAllNotifiers completes every pending Wait of the owner's notifiers with reason.
func FireAllNotifiers(ctx chasm.MutableContext, notifiers Notifiers, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	for _, field := range notifiers {
		if err := field.Get(ctx).Fire(ctx, reason, ownerEnd); err != nil {
			return err
		}
	}
	return nil
}

// StandaloneNotifier is the root of a standalone stream's notifier execution, whose business ID is
// the stream ID. It holds the Waits on a standalone stream in an external store, and is created by
// the first Wait. It is separate from a native standalone stream's execution, so that the same ID can
// name a stream on each backend. It closes at an idle check that finds it holding no subscriptions,
// a subscription's lifetime and an hour after its latest Wait, and is then removed after the
// namespace's retention; a later Wait starts a new one.
type StandaloneNotifier struct {
	chasm.UnimplementedComponent

	*streampb.StandaloneNotifierState

	Notifier chasm.Field[*Notifier]
}

// standaloneNotifierIdle is how long after its latest Wait a standalone notifier is checked for
// subscriptions: a subscription's lifetime, and time for the last callbacks to be delivered.
const standaloneNotifierIdle = MaxSubscriptionLifetime + time.Hour

func NewStandaloneNotifier(ctx chasm.MutableContext) *StandaloneNotifier {
	return &StandaloneNotifier{
		StandaloneNotifierState: &streampb.StandaloneNotifierState{},
		Notifier:                chasm.NewComponentField(ctx, NewNotifier()),
	}
}

func (n *StandaloneNotifier) LifecycleState(chasm.Context) chasm.LifecycleState {
	if n.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (n *StandaloneNotifier) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

// Terminate closes the notifier, completing its pending Waits as expired.
func (n *StandaloneNotifier) Terminate(ctx chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	n.Closed = true
	return chasm.TerminateComponentResponse{}, n.Notifier.Get(ctx).Fire(ctx, streamservicepb.WAKE_REASON_EXPIRED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
}

// Wait completes a Wait at once if a Notify has moved the stream's position since the reader read
// it, and otherwise subscribes it, and moves the idle deadline.
func (n *StandaloneNotifier) Wait(ctx chasm.MutableContext, args WaitArgs) (*streamservicepb.WaitResponse, error) {
	n.LastWaitTime = timestamppb.New(ctx.Now(n))
	if n.IdleCheckTime == nil {
		n.scheduleIdleCheck(ctx, ctx.Now(n).Add(standaloneNotifierIdle))
	}
	return n.Notifier.Get(ctx).WaitOnPosition(ctx, args, NotifierStreamKey(args.Req.GetStream()))
}

// HasPending reports whether a Wait is pending.
func (n *StandaloneNotifier) HasPending(ctx chasm.Context, _ *streamservicepb.StreamRef) (bool, error) {
	return n.Notifier.Get(ctx).HasPending(ctx), nil
}

// Fire completes the pending Waits, after a Notify.
func (n *StandaloneNotifier) Fire(ctx chasm.MutableContext, _ *streamservicepb.StreamRef) (chasm.NoValue, error) {
	return nil, n.Notifier.Get(ctx).Fire(ctx, streamservicepb.WAKE_REASON_APPENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
}

// CancelWait removes the subscription of a canceled Wait.
func (n *StandaloneNotifier) CancelWait(ctx chasm.MutableContext, requestID string) (chasm.NoValue, error) {
	n.Notifier.Get(ctx).CancelSubscription(ctx, requestID)
	return nil, nil
}

// onIdleCheck closes the notifier if, at the deadline the check was scheduled for, it holds no
// subscriptions, and otherwise checks again at the deadline that a later Wait set, or in an hour.
func (n *StandaloneNotifier) onIdleCheck(ctx chasm.MutableContext, scheduled time.Time) {
	notifier := n.Notifier.Get(ctx)
	removeFinished(ctx, notifier.Subscriptions)
	deadline := n.LastWaitTime.AsTime().Add(standaloneNotifierIdle)
	switch {
	case scheduled.Before(deadline):
		n.scheduleIdleCheck(ctx, deadline)
	case len(notifier.Subscriptions) > 0:
		n.scheduleIdleCheck(ctx, scheduled.Add(time.Hour))
	default:
		n.Closed = true
	}
}

func (n *StandaloneNotifier) scheduleIdleCheck(ctx chasm.MutableContext, at time.Time) {
	n.IdleCheckTime = timestamppb.New(at)
	ctx.AddTask(n, chasm.TaskAttributes{ScheduledTime: at}, &streampb.NotifierIdleTask{})
}

// idleCheckPending reports whether a task scheduled at scheduled is the pending idle check.
func (n *StandaloneNotifier) idleCheckPending(scheduled time.Time) bool {
	return !n.Closed && scheduled.Equal(n.IdleCheckTime.AsTime())
}

type notifierIdleTaskHandler struct{ chasm.PureTaskHandlerBase }

func (notifierIdleTaskHandler) Execute(ctx chasm.MutableContext, n *StandaloneNotifier, attrs chasm.TaskAttributes, _ *streampb.NotifierIdleTask) error {
	n.onIdleCheck(ctx, attrs.ScheduledTime)
	return nil
}

func (notifierIdleTaskHandler) Validate(_ chasm.Context, n *StandaloneNotifier, invocation chasm.TaskInvocation, _ *streampb.NotifierIdleTask) (bool, error) {
	return n.idleCheckPending(invocation.ScheduledTime), nil
}

// NotifyCounters counts each stream's Notify calls in this History host's memory, so that a Notify
// with no subscription writes nothing. A Wait at the position its reader read before reading the
// external store completes at once if a Notify has moved the count since. The count is lost when the
// shard of the execution that holds the stream's notifier moves to another host, or its entry is
// evicted; a new entry has a new random generation, so a Wait at a lost position completes at once,
// a spurious wake.
type NotifyCounters struct {
	mu      sync.Mutex
	entries map[string]*streamservicepb.NotifierPosition
}

// maxNotifyCounters bounds the entries kept. Beyond it an arbitrary entry is evicted.
const maxNotifyCounters = 100_000

var notifyCounters = &NotifyCounters{entries: make(map[string]*streamservicepb.NotifierPosition)}

// NotifierStreamKey names a stream in NotifyCounters: its namespace and, for a stream of an owner,
// the owner's kind, ID and run (a workflow's first run ID, or the activity's run ID) and the stream's
// name, or a standalone stream's ID.
func NotifierStreamKey(ref *streamservicepb.StreamRef) string {
	switch {
	case ref.GetStreamId() != "":
		return ref.GetNamespace() + "/stream/" + ref.GetStreamId()
	case ref.GetActivityId() != "":
		return ref.GetNamespace() + "/activity/" + ref.GetActivityId() + "/" + ref.GetActivityRunId() + "/" + ref.GetName()
	default:
		return ref.GetNamespace() + "/workflow/" + ref.GetWorkflowId() + "/" + ref.GetFirstRunId() + "/" + ref.GetName()
	}
}

// Counters is the History host's notify counters.
func Counters() *NotifyCounters {
	return notifyCounters
}

// Position returns the stream's current position, creating its entry if needed.
func (c *NotifyCounters) Position(stream string) *streamservicepb.NotifierPosition {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.entry(stream)
	return &streamservicepb.NotifierPosition{Generation: p.Generation, Counter: p.Counter}
}

// Increment counts a Notify of the stream.
func (c *NotifyCounters) Increment(stream string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entry(stream).Counter++
}

func (c *NotifyCounters) entry(stream string) *streamservicepb.NotifierPosition {
	p, ok := c.entries[stream]
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
	c.entries[stream] = p
	return p
}
