package activity

// A Standalone Activity's stream: Append, Read and Close of the stream service on a stream embedded
// in the activity's execution. The activity's own attempts append with their task token, checked
// against the current attempt, so a superseded attempt is fenced. Other producers (workflows, other
// activities, clients) are not verified, and their dedupe and fencing state is never removed. The
// stream closes with the activity's outcome, in the transaction that ends it; a retry does not
// close it.

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

// AppendToStream appends to the activity's stream, creating it with the first append.
func (a *Activity) AppendToStream(
	ctx chasm.MutableContext,
	req *streamservicepb.AppendRequest,
) (*streamservicepb.AppendResponse, error) {
	p, err := a.streamProducer(ctx, req.GetProducer())
	if err != nil {
		return nil, err
	}
	s, err := a.mutableStream(ctx)
	if err != nil {
		return nil, err
	}
	offsets, err := s.Append(ctx, p, req.GetSeq(), req.GetRecords(), req.GetFinish(), nil)
	if err != nil {
		return nil, err
	}
	resp := &streamservicepb.AppendResponse{}
	for _, offset := range offsets {
		cursor, err := streamCursor(ctx)(offset)
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
	s, err := a.mutableStream(ctx)
	if err != nil {
		return nil, err
	}
	_, err = s.Append(ctx, p, req.GetSeq(), nil, false, req.GetClose())
	return &streamservicepb.CloseResponse{}, err
}

// WaitOnStream completes a Wait on the activity's stream at once, or subscribes it. The stream's
// mode is always known: only workflows declare commit mode.
func (a *Activity) WaitOnStream(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	after, err := streamOffset(ctx, args.Cursor)
	if err != nil {
		return nil, err
	}
	s, err := a.mutableStream(ctx)
	if err != nil {
		return nil, err
	}
	return s.Wait(ctx, args, after, true)
}

// CancelStreamWait removes the subscription of a canceled Wait.
func (a *Activity) CancelStreamWait(ctx chasm.MutableContext, requestID string) (chasm.NoValue, error) {
	if s, ok := a.Stream.TryGet(ctx); ok {
		s.CancelSubscription(ctx, requestID)
	}
	if n, ok := a.StreamNotifier.TryGet(ctx); ok {
		n.CancelSubscription(ctx, requestID)
	}
	return nil, nil
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
	s, _ := a.Stream.TryGet(ctx)
	resp, err := stream.ReadResponse(ctx, s, args.Req, after, until, streamCursor(ctx))
	if err != nil {
		return nil, err
	}
	// Only workflows declare commit mode.
	resp.Mode = streamservicepb.STREAM_MODE_SINK
	return resp, nil
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

// closeStreamOnEnd closes the activity's stream with state, unless it is already closed, in the
// transaction in which the activity reaches a terminal state. A workflow activity has no stream.
// Each terminal transition passes its state, because the transition sets the activity's status only
// after its apply function returns.
func (a *Activity) closeStreamOnEnd(ctx chasm.MutableContext, state streamservicepb.CloseState) error {
	if n, ok := a.StreamNotifier.TryGet(ctx); ok {
		if err := n.Fire(ctx, streamservicepb.WAKE_REASON_OWNER_ENDED, state); err != nil {
			return err
		}
	}
	if s, ok := a.Stream.TryGet(ctx); ok {
		return s.CloseIfOpen(ctx, &streamservicepb.StreamClose{State: state})
	}
	return nil
}

// WaitOnNotifier completes a Wait on the notifier of the activity's stream in an external store at
// once, if the activity has ended or a Notify has moved the position since the reader read it, and
// otherwise subscribes it.
func (a *Activity) WaitOnNotifier(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_OWNER_ENDED, OwnerEnd: streamCloseState(a.Status)}, nil
	}
	if !proto.Equal(stream.Counters().Position(notifierKey(ctx, args.Req.GetStream())), args.Req.GetNotifier()) {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_APPENDED}, nil
	}
	n, ok := a.StreamNotifier.TryGet(ctx)
	if !ok {
		n = stream.NewNotifier()
		a.StreamNotifier = chasm.NewComponentField(ctx, n)
	}
	return nil, n.Subscribe(ctx, args)
}

// NotifierState returns the state the activity ended in, if it has, for NotifierState.
func (a *Activity) NotifierState(ctx chasm.Context, _ *streamservicepb.StreamRef) (*streamservicepb.NotifierStateResponse, error) {
	resp := &streamservicepb.NotifierStateResponse{}
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		resp.OwnerEnd = streamCloseState(a.Status)
	}
	return resp, nil
}

// NotifierPending reports whether the notifier has a pending Wait, so that a Notify must write.
func (a *Activity) NotifierPending(ctx chasm.Context, _ *streamservicepb.StreamRef) (bool, error) {
	n, ok := a.StreamNotifier.TryGet(ctx)
	return ok && n.HasPending(ctx), nil
}

// FireNotifier completes the notifier's pending Waits, after a Notify.
func (a *Activity) FireNotifier(ctx chasm.MutableContext, _ *streamservicepb.StreamRef) (chasm.NoValue, error) {
	if n, ok := a.StreamNotifier.TryGet(ctx); ok {
		return nil, n.Fire(ctx, streamservicepb.WAKE_REASON_APPENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
	}
	return nil, nil
}

// notifierKey names this activity execution in the notify counters.
func notifierKey(ctx chasm.Context, ref *streamservicepb.StreamRef) string {
	key := ctx.ExecutionKey()
	return stream.NotifierOwnerKey(&streamservicepb.StreamRef{Namespace: ref.GetNamespace(), ActivityId: key.BusinessID, ActivityRunId: key.RunID})
}

// mutableStream returns the activity's stream, creating it if it does not exist. A stream created
// after the activity ended is created closed, so it takes no appends.
func (a *Activity) mutableStream(ctx chasm.MutableContext) (*stream.Stream, error) {
	if s, ok := a.Stream.TryGet(ctx); ok {
		return s, nil
	}
	s := stream.New(ctx)
	a.Stream = chasm.NewComponentField(ctx, s)
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		if err := a.closeStreamOnEnd(ctx, streamCloseState(a.Status)); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// streamProducer derives the producer and its epoch. The activity's own attempts are checked; other
// producers are taken as they say they are.
func (a *Activity) streamProducer(ctx chasm.Context, p *streamservicepb.Producer) (stream.Producer, error) {
	switch v := p.GetVariant().(type) {
	case *streamservicepb.Producer_Client:
		return stream.Producer{ID: "client/" + v.Client.GetId(), Epoch: v.Client.GetEpoch()}, nil
	case *streamservicepb.Producer_ActivityTaskToken:
		producer, _, err := a.activityTokenProducer(ctx, v.ActivityTaskToken)
		return producer, err
	case *streamservicepb.Producer_WorkflowTask:
		w := v.WorkflowTask
		return stream.Producer{
			ID:                  "workflow/" + w.GetRunId(),
			RunID:               w.GetRunId(),
			WorkflowTask:        w.GetPreviousStartedEventId(),
			WorkflowTaskAttempt: w.GetAttempt(),
			Epoch:               int64(w.GetAttempt()),
		}, nil
	case *streamservicepb.Producer_LocalActivity:
		la := v.LocalActivity
		return stream.Producer{
			ID:                  "local-activity/" + la.GetRunId() + "/" + la.GetActivityId(),
			RunID:               la.GetRunId(),
			WorkflowTask:        la.GetPreviousStartedEventId(),
			WorkflowTaskAttempt: la.GetWorkflowTaskAttempt(),
			Epoch:               int64(la.GetAttempt()),
		}, nil
	default:
		return stream.Producer{}, serviceerror.NewInvalidArgument("producer is required")
	}
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

func streamCursor(ctx chasm.Context) func(offset int64) ([]byte, error) {
	key := ctx.ExecutionKey()
	return func(offset int64) ([]byte, error) {
		return proto.Marshal(&streampb.Cursor{ActivityId: key.BusinessID, ActivityRunId: key.RunID, Offset: offset})
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
