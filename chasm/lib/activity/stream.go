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

// ReadStream reads the activity's stream.
func (a *Activity) ReadStream(ctx chasm.Context, args stream.ReadArgs) (*streamservicepb.ReadResponse, error) {
	after := int64(-1)
	if args.Cursor != nil {
		if args.Cursor.GetActivityRunId() != ctx.ExecutionKey().RunID {
			return nil, stream.NewError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's activity execution no longer exists")
		}
		after = args.Cursor.GetOffset()
	}
	s, _ := a.Stream.TryGet(ctx)
	return stream.ReadResponse(ctx, s, args.Req, after, streamCursor(ctx))
}

// closeStreamOnEnd closes the activity's stream with state, unless it is already closed, in the
// transaction in which the activity reaches a terminal state. A workflow activity has no stream.
// Each terminal transition passes its state, because the transition sets the activity's status only
// after its apply function returns.
func (a *Activity) closeStreamOnEnd(ctx chasm.MutableContext, state streamservicepb.CloseState) {
	if s, ok := a.Stream.TryGet(ctx); ok {
		s.CloseIfOpen(&streamservicepb.StreamClose{State: state})
	}
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
		a.closeStreamOnEnd(ctx, streamCloseState(a.Status))
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
// workflow run and activity ID, as on a workflow-owned stream.
func (a *Activity) activityTokenProducer(ctx chasm.Context, token []byte) (p stream.Producer, own bool, err error) {
	t, err := tasktoken.NewSerializer().Deserialize(token)
	if err != nil {
		return stream.Producer{}, false, serviceerror.NewInvalidArgument("invalid activity task token")
	}
	epoch := int64(t.GetActivityAttemptStamp())
	if len(t.GetComponentRef()) == 0 {
		return stream.Producer{ID: "activity/" + t.GetRunId() + "/" + t.GetActivityId(), Epoch: epoch}, false, nil
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
