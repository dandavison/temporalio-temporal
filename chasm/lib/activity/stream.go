package activity

// A Standalone Activity's streams: Create, Append, Read and Close of the stream service on the
// named streams embedded in the activity's execution. The activity's own attempts append with their
// task token, checked against the current attempt, so a superseded attempt is fenced. Other
// producers (workflows, other activities, clients) are not verified, and their dedupe and fencing
// state is never removed. Every stream closes with the activity's outcome, in the transaction that
// ends it; a retry does not close it. Waits on the activity's streams are held by the streams, and,
// for a stream in an external store or one not created yet, by the activity's notifier for the
// stream's name.

import (
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/protobuf/proto"
)

// CreateStream creates the activity's stream, or answers a create of the stream if it exists.
func (a *Activity) CreateStream(
	ctx chasm.MutableContext,
	req *streamservicepb.CreateRequest,
) (*streamservicepb.CreateResponse, error) {
	name := req.GetStream().GetName()
	if s, ok := a.stream(ctx, name); ok {
		created, err := s.Create(req.GetRequestId(), 0)
		return &streamservicepb.CreateResponse{Created: created}, err
	}
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		return nil, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_CLOSED, "the stream's owner has ended")
	}
	_, err := a.newStream(ctx, name, req.GetRequestId())
	return &streamservicepb.CreateResponse{Created: true}, err
}

// AppendToStream appends to the activity's stream, creating it with the first append.
func (a *Activity) AppendToStream(
	ctx chasm.MutableContext,
	req *streamservicepb.AppendRequest,
) (*streamservicepb.AppendResponse, error) {
	p, err := a.streamProducer(ctx, req.GetProducer())
	if err != nil {
		return nil, err
	}
	name := req.GetStream().GetName()
	s, err := a.mutableStream(ctx, name)
	if err != nil {
		return nil, err
	}
	offsets, err := s.Append(ctx, p, req.GetSeq(), req.GetRecords(), req.GetFinish(), nil)
	if err != nil {
		return nil, err
	}
	resp := &streamservicepb.AppendResponse{}
	for _, offset := range offsets {
		cursor, err := streamCursor(ctx, name)(offset)
		if err != nil {
			return nil, err
		}
		resp.Cursors = append(resp.Cursors, cursor)
	}
	return resp, nil
}

// CloseStream closes the activity's stream. Only the activity's current attempt may close it.
func (a *Activity) CloseStream(
	ctx chasm.MutableContext,
	req *streamservicepb.CloseRequest,
) (*streamservicepb.CloseResponse, error) {
	if req.GetClose().GetState() == streamservicepb.CLOSE_STATE_UNSPECIFIED {
		return nil, serviceerror.NewInvalidArgument("close state is required")
	}
	if len(req.GetActivityTaskToken()) == 0 {
		return nil, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_NOT_OWNER, "only the stream's owner may close it")
	}
	p, own, err := a.activityTokenProducer(ctx, req.GetActivityTaskToken())
	if err != nil {
		return nil, err
	}
	if !own {
		return nil, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_NOT_OWNER, "only the stream's owner may close it")
	}
	s, err := a.mutableStream(ctx, req.GetStream().GetName())
	if err != nil {
		return nil, err
	}
	_, err = s.Append(ctx, p, req.GetSeq(), nil, false, req.GetClose())
	return &streamservicepb.CloseResponse{}, err
}

// ReadStream reads the activity's stream.
func (a *Activity) ReadStream(ctx chasm.Context, args stream.ReadArgs) (*streamservicepb.ReadResponse, error) {
	after, err := streamOffset(ctx, args.Cursor)
	if err != nil {
		return nil, err
	}
	until, err := streamOffset(ctx, args.Until)
	if err != nil {
		return nil, err
	}
	name := args.Req.GetStream().GetName()
	s, _ := a.stream(ctx, name)
	resp, err := stream.ReadResponse(ctx, s, args.Req, after, until, streamCursor(ctx, name))
	if err != nil {
		return nil, err
	}
	// Only workflows declare commit mode.
	resp.Mode = streamservicepb.STREAM_MODE_SINK
	return resp, nil
}

// WaitOnStream completes a Wait on the activity's stream at once, or subscribes it. The stream's
// mode is always known: only workflows declare commit mode. A Wait on a stream not created yet is
// held by the activity's notifier for the stream's name until the stream is created, or the
// activity ends.
func (a *Activity) WaitOnStream(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	after, err := streamOffset(ctx, args.Cursor)
	if err != nil {
		return nil, err
	}
	name := args.Req.GetStream().GetName()
	if s, ok := a.stream(ctx, name); ok {
		return s.Wait(ctx, args, after, true)
	}
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_OWNER_ENDED, OwnerEnd: streamCloseState(a.Status)}, nil
	}
	return nil, stream.NotifierOf(ctx, &a.StreamNotifiers, name).Subscribe(ctx, args, false)
}

// WaitOnNotifier completes a Wait on the notifier of the activity's stream in an external store at
// once, if the activity has ended or a Notify has moved the position since the reader read it, and
// otherwise subscribes it.
func (a *Activity) WaitOnNotifier(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_OWNER_ENDED, OwnerEnd: streamCloseState(a.Status)}, nil
	}
	name := args.Req.GetStream().GetName()
	return stream.NotifierOf(ctx, &a.StreamNotifiers, name).WaitOnPosition(ctx, args, notifierKey(ctx, args.Req.GetStream()))
}

// CancelStreamWait removes the subscription of a canceled Wait on the stream named name.
func (a *Activity) CancelStreamWait(ctx chasm.MutableContext, wait stream.CanceledWait) (chasm.NoValue, error) {
	if s, ok := a.stream(ctx, wait.Name); ok {
		s.CancelSubscription(ctx, wait.RequestID)
	}
	if field, ok := a.StreamNotifiers[wait.Name]; ok {
		field.Get(ctx).CancelSubscription(ctx, wait.RequestID)
	}
	return nil, nil
}

// NotifierState returns the state the activity ended in, if it has, for NotifierState.
func (a *Activity) NotifierState(ctx chasm.Context, _ *streamservicepb.StreamRef) (*streamservicepb.NotifierStateResponse, error) {
	resp := &streamservicepb.NotifierStateResponse{}
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		resp.OwnerEnd = streamCloseState(a.Status)
	}
	return resp, nil
}

// NotifierPending reports whether the notifier of the stream has a pending Wait, so that a Notify
// must write.
func (a *Activity) NotifierPending(ctx chasm.Context, ref *streamservicepb.StreamRef) (bool, error) {
	field, ok := a.StreamNotifiers[ref.GetName()]
	return ok && field.Get(ctx).HasPending(ctx), nil
}

// FireNotifier completes the pending Waits of the stream's notifier, after a Notify.
func (a *Activity) FireNotifier(ctx chasm.MutableContext, ref *streamservicepb.StreamRef) (chasm.NoValue, error) {
	return nil, stream.FireNotifier(ctx, a.StreamNotifiers, ref.GetName(), streamservicepb.WAKE_REASON_APPENDED)
}

// notifierKey names this activity execution's stream in the notify counters.
func notifierKey(ctx chasm.Context, ref *streamservicepb.StreamRef) string {
	key := ctx.ExecutionKey()
	return stream.NotifierStreamKey(&streamservicepb.StreamRef{Namespace: ref.GetNamespace(), ActivityId: key.BusinessID, ActivityRunId: key.RunID, Name: ref.GetName()})
}

// streamOffset returns the offset of a cursor of this execution's stream, or -1 for none.
func streamOffset(ctx chasm.Context, c *streampb.Cursor) (int64, error) {
	if c == nil {
		return -1, nil
	}
	if c.GetActivityRunId() != ctx.ExecutionKey().RunID {
		return 0, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's activity execution no longer exists")
	}
	return c.GetOffset(), nil
}

// closeStreamOnEnd closes each of the activity's streams with state, unless it is already closed,
// and completes the Waits its notifiers hold, in the transaction in which the activity reaches a
// terminal state. A workflow activity has no streams. Each terminal transition passes its state,
// because the transition sets the activity's status only after its apply function returns.
func (a *Activity) closeStreamOnEnd(ctx chasm.MutableContext, state streamservicepb.CloseState) error {
	if err := stream.FireAllNotifiers(ctx, a.StreamNotifiers, streamservicepb.WAKE_REASON_OWNER_ENDED, state); err != nil {
		return err
	}
	for _, field := range a.Streams {
		if err := field.Get(ctx).CloseIfOpen(ctx, &streamservicepb.StreamClose{State: state}); err != nil {
			return err
		}
	}
	return nil
}

// stream returns the activity's stream named name, if it exists.
func (a *Activity) stream(ctx chasm.Context, name string) (*stream.Stream, bool) {
	field, ok := a.Streams[name]
	if !ok {
		return nil, false
	}
	return field.Get(ctx), true
}

// mutableStream returns the activity's stream named name, creating it if it does not exist. A
// stream created after the activity ended is created closed, so it takes no appends.
func (a *Activity) mutableStream(ctx chasm.MutableContext, name string) (*stream.Stream, error) {
	if s, ok := a.stream(ctx, name); ok {
		return s, nil
	}
	s, err := a.newStream(ctx, name, "")
	if err != nil {
		return nil, err
	}
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		return s, s.CloseIfOpen(ctx, &streamservicepb.StreamClose{State: streamCloseState(a.Status)})
	}
	return s, nil
}

// newStream creates the activity's stream named name, by the Create with requestID or by an append,
// and completes the Waits for it that the activity's notifier for the name holds.
func (a *Activity) newStream(ctx chasm.MutableContext, name, requestID string) (*stream.Stream, error) {
	s := stream.New(ctx, requestID, 0)
	if a.Streams == nil {
		a.Streams = make(chasm.Map[string, *stream.Stream])
	}
	a.Streams[name] = chasm.NewComponentField(ctx, s)
	return s, stream.FireNotifier(ctx, a.StreamNotifiers, name, streamservicepb.WAKE_REASON_APPENDED)
}

// streamProducer derives the producer and its epoch. The activity's own attempts are checked; other
// producers are taken as they say they are.
func (a *Activity) streamProducer(ctx chasm.Context, p *streamservicepb.Producer) (stream.Producer, error) {
	if v, ok := p.GetVariant().(*streamservicepb.Producer_ActivityTaskToken); ok {
		producer, _, err := a.activityTokenProducer(ctx, v.ActivityTaskToken)
		return producer, err
	}
	return stream.UnverifiedProducer(p)
}

// activityTokenProducer derives the producer of an activity task token. own is true when the token
// is for this activity, in which case it must be for the current attempt; another activity's token
// is not verified. A Standalone Activity's producer ID is its run ID; a workflow activity's is its
// workflow run and activity ID, as on a workflow-owned stream. A Standalone Activity token that the
// frontend made for a by-ID call names no attempt, so it is rejected.
func (a *Activity) activityTokenProducer(ctx chasm.Context, token []byte) (p stream.Producer, own bool, err error) {
	t, err := tasktoken.NewSerializer().Deserialize(token)
	if err != nil {
		return stream.Producer{}, false, serviceerror.NewInvalidArgument("invalid activity task token")
	}
	epoch := int64(t.GetActivityAttemptStamp())
	if len(t.GetComponentRef()) == 0 {
		return stream.Producer{ID: "activity/" + t.GetRunId() + "/" + t.GetActivityId(), Epoch: epoch}, false, nil
	}
	if t.GetAttempt() == ByIDTokenAttempt {
		return stream.Producer{}, false, serviceerror.NewInvalidArgument("a by-ID activity task token is not an activity attempt")
	}
	ref, err := chasm.DeserializeComponentRef(t.GetComponentRef())
	if err != nil {
		return stream.Producer{}, false, serviceerror.NewInvalidArgument("invalid activity task token")
	}
	key := ctx.ExecutionKey()
	p = stream.Producer{ID: "activity/" + ref.RunID, Epoch: epoch}
	if ref.BusinessID != key.BusinessID || ref.RunID != key.RunID {
		return p, false, nil
	}
	if err := a.validateActivityTaskToken(ctx, t, key.NamespaceID, false); err != nil {
		return stream.Producer{}, true, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "the activity attempt is not the current one")
	}
	return p, true, nil
}

func streamCursor(ctx chasm.Context, name string) func(offset int64) ([]byte, error) {
	key := ctx.ExecutionKey()
	return func(offset int64) ([]byte, error) {
		return proto.Marshal(&streampb.Cursor{ActivityId: key.BusinessID, ActivityRunId: key.RunID, Name: name, Offset: offset})
	}
}

func streamCloseState(status activitypb.ActivityExecutionStatus) streamservicepb.CloseState {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return streamservicepb.CLOSE_STATE_COMPLETED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return streamservicepb.CLOSE_STATE_FAILED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return streamservicepb.CLOSE_STATE_CANCELED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return streamservicepb.CLOSE_STATE_TERMINATED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return streamservicepb.CLOSE_STATE_TIMED_OUT
	default:
		return streamservicepb.CLOSE_STATE_UNSPECIFIED
	}
}
