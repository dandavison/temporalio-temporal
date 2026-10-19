package stream

import (
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

// Subscribe stores a subscription for a Wait with the caller's request ID and callback, which fires
// on the next record after offset after that matches topics, on the close, or when the owner ends.
// A retried start with the same request ID finds its subscription and adds nothing.
func (s *Stream) Subscribe(
	ctx chasm.MutableContext,
	requestID string,
	cb *commonpb.Callback,
	after int64,
	topics []string,
	lifetime time.Duration,
) error {
	s.removeFinishedSubscriptions(ctx)
	if _, ok := s.Subscriptions[requestID]; ok {
		return nil
	}
	if s.unfiredSubscriptions(ctx) >= MaxSubscriptions {
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
	now := ctx.Now(s)
	sub := &Subscription{
		SubscriptionState: &streampb.SubscriptionState{
			After:      after,
			Topics:     topics,
			ExpireTime: timestamppb.New(now.Add(lifetime)),
			StartTime:  timestamppb.New(now),
		},
		Callbacks: chasm.Map[string, *callback.Callback]{
			requestID: chasm.NewComponentField(ctx, callback.NewCallback(requestID, timestamppb.New(now), internal)),
		},
	}
	if s.Subscriptions == nil {
		s.Subscriptions = make(chasm.Map[string, *Subscription])
	}
	s.Subscriptions[requestID] = chasm.NewComponentField(ctx, sub)
	ctx.AddTask(sub, chasm.TaskAttributes{ScheduledTime: now.Add(lifetime)}, &streampb.SubscriptionExpiryTask{})
	return nil
}

// CancelSubscription removes the subscription of a canceled Wait, if it has not fired.
func (s *Stream) CancelSubscription(ctx chasm.MutableContext, requestID string) {
	if field, ok := s.Subscriptions[requestID]; ok && !field.Get(ctx).fired() {
		delete(s.Subscriptions, requestID)
	}
}

// FireSubscriptions completes every pending Wait with reason.
func (s *Stream) FireSubscriptions(ctx chasm.MutableContext, reason streamservicepb.WakeReason, ownerEnd streamservicepb.CloseState) error {
	for _, field := range s.Subscriptions {
		if err := field.Get(ctx).fire(ctx, reason, ownerEnd); err != nil {
			return err
		}
	}
	return nil
}

// HasRecordAfter reports whether a record after offset after matches topics.
func (s *Stream) HasRecordAfter(ctx chasm.Context, after int64, topics []string) (bool, error) {
	found := false
	err := s.scan(ctx, after+1, func(_ int64, r *streamservicepb.Record) bool {
		found = matches(r, topics)
		return !found
	})
	return found, err
}

// fireMatchingSubscriptions completes the pending Waits that one of records, just appended,
// matches.
func (s *Stream) fireMatchingSubscriptions(ctx chasm.MutableContext, records []*streamservicepb.Record) error {
	for _, field := range s.Subscriptions {
		sub := field.Get(ctx)
		for _, r := range records {
			if matches(r, sub.Topics) {
				if err := sub.fire(ctx, streamservicepb.WAKE_REASON_APPENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func (s *Stream) removeFinishedSubscriptions(ctx chasm.MutableContext) {
	for key, field := range s.Subscriptions {
		if field.Get(ctx).finished(ctx) {
			delete(s.Subscriptions, key)
		}
	}
}

func (s *Stream) unfiredSubscriptions(ctx chasm.Context) int {
	n := 0
	for _, field := range s.Subscriptions {
		if !field.Get(ctx).fired() {
			n++
		}
	}
	return n
}

// subscriptionExpiryTaskHandler fires a subscription that has not fired by its expire time.
type subscriptionExpiryTaskHandler struct{ chasm.PureTaskHandlerBase }

func (subscriptionExpiryTaskHandler) Validate(_ chasm.Context, sub *Subscription, _ chasm.TaskInvocation, _ *streampb.SubscriptionExpiryTask) (bool, error) {
	return !sub.fired(), nil
}

func (subscriptionExpiryTaskHandler) Execute(ctx chasm.MutableContext, sub *Subscription, _ chasm.TaskAttributes, _ *streampb.SubscriptionExpiryTask) error {
	return sub.fire(ctx, streamservicepb.WAKE_REASON_EXPIRED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
}
