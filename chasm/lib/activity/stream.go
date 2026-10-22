package activity

// A Standalone Activity's streams: Create, Append, Read and Close of the stream service on the
// named streams embedded in the activity's execution. The activity's own attempts append with their
// task token, checked against the current attempt, so a superseded attempt is fenced. Other
// producers (workflows, other activities, clients) are not verified, and their dedupe and fencing
// state is never removed. Every stream closes with the activity's outcome, in the transaction that
// ends it; a retry does not close it.

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
	a.newStream(ctx, name, req.GetRequestId())
	return &streamservicepb.CreateResponse{Created: true}, nil
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
	s := a.mutableStream(ctx, name)
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
	_, err = a.mutableStream(ctx, req.GetStream().GetName()).Append(ctx, p, req.GetSeq(), nil, false, req.GetClose())
	return &streamservicepb.CloseResponse{}, err
}

// ReadStream reads the activity's stream.
func (a *Activity) ReadStream(ctx chasm.Context, args stream.ReadArgs) (*streamservicepb.ReadResponse, error) {
	after := int64(-1)
	if args.Cursor != nil {
		if args.Cursor.GetActivityRunId() != ctx.ExecutionKey().RunID {
			return nil, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's activity execution no longer exists")
		}
		after = args.Cursor.GetOffset()
	}
	s, _ := a.stream(ctx, args.Req.GetStream().GetName())
	return stream.ReadResponse(ctx, s, args.Req, after, streamCursor(ctx, args.Req.GetStream().GetName()))
}

// closeStreamOnEnd closes each of the activity's streams with state, unless it is already closed,
// in the transaction in which the activity reaches a terminal state. A workflow activity has no
// streams. Each terminal transition passes its state, because the transition sets the activity's
// status only after its apply function returns.
func (a *Activity) closeStreamOnEnd(ctx chasm.MutableContext, state streamservicepb.CloseState) {
	for _, field := range a.Streams {
		field.Get(ctx).CloseIfOpen(&streamservicepb.StreamClose{State: state})
	}
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
func (a *Activity) mutableStream(ctx chasm.MutableContext, name string) *stream.Stream {
	if s, ok := a.stream(ctx, name); ok {
		return s
	}
	s := a.newStream(ctx, name, "")
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		s.CloseIfOpen(&streamservicepb.StreamClose{State: streamCloseState(a.Status)})
	}
	return s
}

// newStream creates the activity's stream named name, by the Create with requestID or by an append.
func (a *Activity) newStream(ctx chasm.MutableContext, name, requestID string) *stream.Stream {
	s := stream.New(ctx, requestID, 0)
	if a.Streams == nil {
		a.Streams = make(chasm.Map[string, *stream.Stream])
	}
	a.Streams[name] = chasm.NewComponentField(ctx, s)
	return s
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
