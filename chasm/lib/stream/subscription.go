package stream

import (
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	sdkconverter "go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// MaxSubscriptions is the most unfired subscriptions a stream holds; beyond it, Wait fails with
	// ResourceExhausted, and the reader polls instead.
	MaxSubscriptions = 2000
	// MaxSubscriptionLifetime is the longest a subscription lives before it expires. Expiry removes
	// the subscription of a reader that closed while the stream was quiet, which nothing else would.
	MaxSubscriptionLifetime = 7 * 24 * time.Hour
)

// Subscription is a pending Wait: a workflow reader's subscription to the stream. It fires once,
// which delivers the Wait's result through its callback, and is removed once the callback has
// finished, or when the Wait is canceled. It is detached, so that its callback is still delivered
// after the stream's owner has closed or continued as new.
type Subscription struct {
	chasm.UnimplementedComponent

	*streampb.SubscriptionState

	Callbacks chasm.Map[string, *callback.Callback]
}

func (s *Subscription) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (s *Subscription) fired() bool {
	return s.Reason != streamservicepb.WAKE_REASON_UNSPECIFIED
}

// fire completes the Wait with reason, once. The result is read when the callback is delivered, so
// a close or an owner's end after the subscription fired for an append, as in a workflow task
// completion that appends its commit record and then ends the run, replaces the reason if the
// result has not been delivered yet.
func (s *Subscription) fire(ctx chasm.MutableContext, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	if s.fired() {
		if s.Reason == streamservicepb.WAKE_REASON_APPENDED &&
			(reason == streamservicepb.WAKE_REASON_CLOSED || reason == streamservicepb.WAKE_REASON_OWNER_ENDED) {
			s.Reason, s.OwnerEnd = reason, ownerEnd
		}
		return nil
	}
	s.Reason = reason
	s.OwnerEnd = ownerEnd
	s.CloseTime = timestamppb.New(ctx.Now(s))
	return callback.ScheduleStandbyCallbacks(ctx, s.Callbacks)
}

// finished reports whether the subscription has fired and its callback has succeeded or failed.
func (s *Subscription) finished(ctx chasm.Context) bool {
	if !s.fired() {
		return false
	}
	for _, field := range s.Callbacks {
		switch field.Get(ctx).Status {
		case callbackspb.CALLBACK_STATUS_SUCCEEDED, callbackspb.CALLBACK_STATUS_FAILED:
		default:
			return false
		}
	}
	return true
}

// GetNexusCompletion implements callback.CompletionSource: the Wait's result.
func (s *Subscription) GetNexusCompletion(chasm.Context, string) (nexusrpc.CompleteOperationOptions, error) {
	result, err := sdkconverter.PreferProtoDataConverter.ToPayload(&streamservicepb.WaitResponse{Reason: s.Reason, OwnerEnd: s.OwnerEnd})
	if err != nil {
		return nexusrpc.CompleteOperationOptions{}, err
	}
	return nexusrpc.CompleteOperationOptions{
		Result:    result,
		StartTime: s.StartTime.AsTime(),
		CloseTime: s.CloseTime.AsTime(),
	}, nil
}

// Subscriptions are pending Waits, keyed by the Wait's request ID. A native Stream and an
// external store's Notifier each hold them.
type Subscriptions = chasm.Map[string, *Subscription]

// subscribe stores a subscription for a Wait with the caller's request ID and callback, which fires
// on the next record after offset after that wakes it (see wakes), on the next Notify, on the close,
// or when the owner ends. A retried start with the same request ID finds its subscription and adds
// nothing.
func subscribe(
	ctx chasm.MutableContext,
	subs *Subscriptions,
	requestID string,
	cb *commonpb.Callback,
	after int64,
	topics []string,
	wakeOnMarkers bool,
	lifetime time.Duration,
) error {
	removeFinished(ctx, *subs)
	if _, ok := (*subs)[requestID]; ok {
		return nil
	}
	if unfired(ctx, *subs) >= MaxSubscriptions {
		return serviceerror.NewResourceExhaustedf(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"the stream already has %d pending waits", MaxSubscriptions)
	}
	internal, err := callback.FromAPICallback(cb)
	if err != nil {
		return err
	}
	if lifetime <= 0 || lifetime > MaxSubscriptionLifetime {
		lifetime = MaxSubscriptionLifetime
	}
	now := ctx.Now(nil)
	sub := &Subscription{
		SubscriptionState: &streampb.SubscriptionState{
			After:         after,
			Topics:        topics,
			WakeOnMarkers: wakeOnMarkers,
			ExpireTime:    timestamppb.New(now.Add(lifetime)),
			StartTime:     timestamppb.New(now),
		},
		Callbacks: chasm.Map[string, *callback.Callback]{
			requestID: chasm.NewComponentField(ctx, callback.NewCallback(requestID, timestamppb.New(now), internal)),
		},
	}
	if *subs == nil {
		*subs = make(Subscriptions)
	}
	(*subs)[requestID] = chasm.NewComponentField(ctx, sub)
	ctx.AddTask(sub, chasm.TaskAttributes{ScheduledTime: now.Add(lifetime)}, &streampb.SubscriptionExpiryTask{})
	return nil
}

// cancelSubscription removes the subscription of a canceled Wait, if it has not fired.
func cancelSubscription(ctx chasm.MutableContext, subs Subscriptions, requestID string) {
	if field, ok := subs[requestID]; ok && !field.Get(ctx).fired() {
		delete(subs, requestID)
	}
}

// fireAll completes every pending Wait with reason.
func fireAll(ctx chasm.MutableContext, subs Subscriptions, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	for _, field := range subs {
		if err := field.Get(ctx).fire(ctx, reason, ownerEnd); err != nil {
			return err
		}
	}
	return nil
}

func removeFinished(ctx chasm.MutableContext, subs Subscriptions) {
	for key, field := range subs {
		if field.Get(ctx).finished(ctx) {
			delete(subs, key)
		}
	}
}

func unfired(ctx chasm.Context, subs Subscriptions) int {
	n := 0
	for _, field := range subs {
		if !field.Get(ctx).fired() {
			n++
		}
	}
	return n
}

// Subscribe stores a subscription for a Wait on the stream; see subscribe.
func (s *Stream) Subscribe(ctx chasm.MutableContext, requestID string, cb *commonpb.Callback, after int64, topics []string, wakeOnMarkers bool, lifetime time.Duration) error {
	return subscribe(ctx, &s.Subscriptions, requestID, cb, after, topics, wakeOnMarkers, lifetime)
}

// CancelSubscription removes the subscription of a canceled Wait, if it has not fired.
func (s *Stream) CancelSubscription(ctx chasm.MutableContext, requestID string) {
	cancelSubscription(ctx, s.Subscriptions, requestID)
}

// FireSubscriptions completes every pending Wait with reason.
func (s *Stream) FireSubscriptions(ctx chasm.MutableContext, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	return fireAll(ctx, s.Subscriptions, reason, ownerEnd)
}

// HasRecordAfter reports whether a record after offset after would wake a Wait for topics: a
// record of one of the topics, or the commit record of a workflow task that wrote one. A commit
// record older than the ones whose topics are kept counts, which may wake a Wait for nothing.
func (s *Stream) HasRecordAfter(ctx chasm.Context, after int64, topics []string) (bool, error) {
	found := false
	err := s.scan(ctx, after+1, func(offset int64, r *streamservicepb.Record) bool {
		switch {
		case r.WorkflowTaskCommit != nil:
			i := slices.IndexFunc(s.RecentCommits, func(c *streampb.CommitTopics) bool { return c.GetOffset() == offset })
			found = i < 0 || readsAny(topics, s.RecentCommits[i].GetTopics())
		default:
			found = wakes(&streampb.SubscriptionState{Topics: topics}, r)
		}
		return !found
	})
	return found, err
}

// fireSubscriptionsFor completes, as appended, the pending Waits for which wake is true.
func (s *Stream) fireSubscriptionsFor(ctx chasm.MutableContext, wake func(*streampb.SubscriptionState) bool) error {
	for _, field := range s.Subscriptions {
		sub := field.Get(ctx)
		if wake(sub.SubscriptionState) {
			if err := sub.fire(ctx, streamservicepb.WAKE_REASON_APPENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED); err != nil {
				return err
			}
		}
	}
	return nil
}

// wakes reports whether record r, other than a commit record, wakes the Wait of sub: a data or
// finish record of one of its topics does, and a marker does only for a Wait made while the
// stream's mode was not known. RecordCommit decides for commit records.
func wakes(sub *streampb.SubscriptionState, r *streamservicepb.Record) bool {
	if r.WorkflowTaskCommit != nil || r.StreamCreated != nil || r.WorkflowAppendMode != nil {
		return sub.GetWakeOnMarkers()
	}
	return len(sub.GetTopics()) == 0 || slices.Contains(sub.GetTopics(), r.GetTopic())
}

// readsAny reports whether a Wait for topics (empty for all) reads one of written.
func readsAny(topics, written []string) bool {
	if len(topics) == 0 {
		return len(written) > 0
	}
	return slices.ContainsFunc(written, func(t string) bool { return slices.Contains(topics, t) })
}

const (
	// maxRecentCommits is how many commit records' topics a stream keeps for HasRecordAfter.
	maxRecentCommits = 32
	// maxUncommittedTasks bounds the tasks whose topics a stream keeps until their commit record. A
	// workflow-owned stream keeps only its run's open task and late local activity records of
	// earlier tasks, cleared by each commit; an activity-owned stream has no commit records, so its
	// workflow producers' entries stay until this bound drops the oldest.
	maxUncommittedTasks = 64
)

// noteUncommittedTopics records the topics of records of workflow and local activity producers,
// which their task's commit record will decide.
func (s *Stream) noteUncommittedTopics(records []*streamservicepb.Record) {
	for _, r := range records {
		p := r.GetProducer()
		if p.GetWorkflowRunId() == "" {
			continue
		}
		i := slices.IndexFunc(s.UncommittedTasks, func(t *streampb.TaskTopics) bool {
			return t.GetWorkflowRunId() == p.GetWorkflowRunId() && t.GetWorkflowTask() == p.GetWorkflowTask()
		})
		if i < 0 {
			s.UncommittedTasks = append(s.UncommittedTasks, &streampb.TaskTopics{WorkflowRunId: p.GetWorkflowRunId(), WorkflowTask: p.GetWorkflowTask()})
			if n := len(s.UncommittedTasks); n > maxUncommittedTasks {
				s.UncommittedTasks = s.UncommittedTasks[n-maxUncommittedTasks:]
			}
			i = len(s.UncommittedTasks) - 1
		}
		if t := s.UncommittedTasks[i]; !slices.Contains(t.Topics, r.GetTopic()) {
			t.Topics = append(t.Topics, r.GetTopic())
		}
	}
}

// takeUncommittedTopics returns the topics that workflow task workflowTask of run runID wrote to,
// and forgets them and those of the run's earlier tasks, whose records already have commit records.
func (s *Stream) takeUncommittedTopics(runID string, workflowTask int64) []string {
	var topics []string
	s.UncommittedTasks = slices.DeleteFunc(s.UncommittedTasks, func(t *streampb.TaskTopics) bool {
		if t.GetWorkflowRunId() != runID || t.GetWorkflowTask() > workflowTask {
			return false
		}
		if t.GetWorkflowTask() == workflowTask {
			topics = t.GetTopics()
		}
		return true
	})
	return topics
}

// subscriptionExpiryTaskHandler fires a subscription that has not fired by its expire time.
type subscriptionExpiryTaskHandler struct{ chasm.PureTaskHandlerBase }

func (subscriptionExpiryTaskHandler) Validate(_ chasm.Context, sub *Subscription, _ chasm.TaskInvocation, _ *streampb.SubscriptionExpiryTask) (bool, error) {
	return !sub.fired(), nil
}

func (subscriptionExpiryTaskHandler) Execute(ctx chasm.MutableContext, sub *Subscription, _ chasm.TaskAttributes, _ *streampb.SubscriptionExpiryTask) error {
	return sub.fire(ctx, streamservicepb.WAKE_REASON_EXPIRED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
}
