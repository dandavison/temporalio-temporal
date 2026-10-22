package stream

// A standalone stream: a stream with no owner execution, the root of an execution of its own whose
// business ID is the stream ID. It is created by Create or by its first append. Producers of every
// kind append to it unverified, since there is no owner to check them against, so a superseded
// producer is fenced only once its successor has appended, as on Redis. Any caller may close it:
// the creator's right to close is not enforced. It closes as timed out once its idle timeout passes
// with no create or append, and the execution is then removed after the namespace's retention.

import (
	"context"
	"errors"
	"time"

	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Standalone struct {
	chasm.UnimplementedComponent

	*streampb.StandaloneState

	Stream chasm.Field[*Stream]
}

// NewStandalone starts a standalone stream, created by the Create with requestID, or by an append if
// requestID is empty, and schedules its idle timeout.
func NewStandalone(ctx chasm.MutableContext, requestID string, idleTimeout time.Duration) *Standalone {
	s := &Standalone{
		StandaloneState: &streampb.StandaloneState{},
		Stream:          chasm.NewComponentField(ctx, New(ctx, requestID, idleTimeout)),
	}
	s.touch(ctx)
	s.scheduleIdleCheck(ctx, ctx.Now(s).Add(idleTimeout))
	return s
}

func (s *Standalone) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	switch c := s.Stream.Get(ctx).Close; {
	case c == nil:
		return chasm.LifecycleStateRunning
	case c.GetState() == streamservicepb.CLOSE_STATE_COMPLETED:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateFailed
	}
}

func (s *Standalone) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (s *Standalone) Terminate(ctx chasm.MutableContext, req chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	s.Stream.Get(ctx).CloseIfOpen(&streamservicepb.StreamClose{State: streamservicepb.CLOSE_STATE_TERMINATED, Reason: req.Reason})
	return chasm.TerminateComponentResponse{}, nil
}

// StandaloneAppend is an append and the idle timeout of a stream that it creates.
type StandaloneAppend struct {
	Req         *streamservicepb.AppendRequest
	IdleTimeout time.Duration
}

// Append appends with the producer as it says it is, and closes the stream first if its idle
// timeout has passed.
func (s *Standalone) Append(ctx chasm.MutableContext, in StandaloneAppend) (*streamservicepb.AppendResponse, error) {
	req := in.Req
	p, err := UnverifiedProducer(req.GetProducer())
	if err != nil {
		return nil, err
	}
	s.closeIfIdle(ctx, ctx.Now(s))
	offsets, err := s.Stream.Get(ctx).Append(ctx, p, req.GetSeq(), req.GetRecords(), req.GetFinish(), nil)
	if err != nil {
		return nil, err
	}
	s.touch(ctx)
	resp := &streamservicepb.AppendResponse{}
	for _, offset := range offsets {
		cursor, err := standaloneCursor(ctx)(offset)
		if err != nil {
			return nil, err
		}
		resp.Cursors = append(resp.Cursors, cursor)
	}
	return resp, nil
}

// StandaloneCreate is a create and the idle timeout it gives the stream.
type StandaloneCreate struct {
	Req         *streamservicepb.CreateRequest
	IdleTimeout time.Duration
}

// Create answers a create of the stream, which exists.
func (s *Standalone) Create(ctx chasm.MutableContext, in StandaloneCreate) (*streamservicepb.CreateResponse, error) {
	s.closeIfIdle(ctx, ctx.Now(s))
	created, err := s.Stream.Get(ctx).Create(in.Req.GetRequestId(), in.IdleTimeout)
	if err != nil {
		return nil, err
	}
	s.touch(ctx)
	return &streamservicepb.CreateResponse{Created: created}, nil
}

// Close closes the stream. A close of a closed stream succeeds if it is the same close.
func (s *Standalone) Close(ctx chasm.MutableContext, req *streamservicepb.CloseRequest) (*streamservicepb.CloseResponse, error) {
	if req.GetClose().GetState() == streamservicepb.CLOSE_STATE_UNSPECIFIED {
		return nil, serviceerror.NewInvalidArgument("close state is required")
	}
	s.closeIfIdle(ctx, ctx.Now(s))
	stream := s.Stream.Get(ctx)
	if stream.Close != nil && !proto.Equal(stream.Close, req.GetClose()) {
		return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_CLOSED, "stream is closed")
	}
	stream.CloseIfOpen(req.GetClose())
	return &streamservicepb.CloseResponse{}, nil
}

// Read reads the stream. A cursor of an earlier execution with the stream's ID fails as expired.
func (s *Standalone) Read(ctx chasm.Context, args ReadArgs) (*streamservicepb.ReadResponse, error) {
	after := int64(-1)
	if args.Cursor != nil {
		if args.Cursor.GetRunId() != ctx.ExecutionKey().RunID {
			return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's stream no longer exists")
		}
		after = args.Cursor.GetOffset()
	}
	return ReadResponse(ctx, s.Stream.Get(ctx), args.Req, after, standaloneCursor(ctx))
}

func (s *Standalone) touch(ctx chasm.MutableContext) {
	s.LastWriteTime = timestamppb.New(ctx.Now(s))
}

// closeIfIdle closes the stream if, at time now, its idle timeout has passed since the last create
// or append. It returns the idle deadline if the stream is open.
func (s *Standalone) closeIfIdle(ctx chasm.MutableContext, now time.Time) (time.Time, bool) {
	stream := s.Stream.Get(ctx)
	if stream.Close != nil {
		return time.Time{}, false
	}
	deadline := s.LastWriteTime.AsTime().Add(stream.IdleTimeout.AsDuration())
	if now.Before(deadline) {
		return deadline, true
	}
	stream.CloseIfOpen(&streamservicepb.StreamClose{State: streamservicepb.CLOSE_STATE_TIMED_OUT, Reason: IdleCloseReason})
	return time.Time{}, false
}

// onIdleTimeoutTask closes the stream if the task, scheduled at scheduled, was scheduled for the
// current idle deadline, and otherwise schedules a check at the deadline that a later create or
// append set. It compares the scheduled time, not the clock, because a timer task can run slightly
// before its time. So appends schedule no tasks, and one check is pending while the stream is open.
func (s *Standalone) onIdleTimeoutTask(ctx chasm.MutableContext, scheduled time.Time) {
	if deadline, open := s.closeIfIdle(ctx, scheduled); open {
		s.scheduleIdleCheck(ctx, deadline)
	}
}

func (s *Standalone) scheduleIdleCheck(ctx chasm.MutableContext, at time.Time) {
	s.IdleCheckTime = timestamppb.New(at)
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: at}, &streampb.IdleTimeoutTask{})
}

// idleCheckPending reports whether a task scheduled at scheduled is the pending idle check.
func (s *Standalone) idleCheckPending(ctx chasm.Context, scheduled time.Time) bool {
	return s.Stream.Get(ctx).Close == nil && scheduled.Equal(s.IdleCheckTime.AsTime())
}

func standaloneCursor(ctx chasm.Context) func(offset int64) ([]byte, error) {
	key := ctx.ExecutionKey()
	return func(offset int64) ([]byte, error) {
		return proto.Marshal(&streampb.Cursor{StreamId: key.BusinessID, RunId: key.RunID, Offset: offset})
	}
}

// UpdateOrStartStandalone applies update to the standalone stream with the key's business ID,
// starting the stream with start if it does not exist. A stream that closes between the two is
// reported as closed.
func UpdateOrStartStandalone[I, O any](
	ctx context.Context,
	key chasm.ExecutionKey,
	start func(chasm.MutableContext, I) (*Standalone, error),
	update func(*Standalone, chasm.MutableContext, I) (O, error),
	input I,
) (O, error) {
	out, _, err := chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Standalone](key), update, input)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
		return out, err
	}
	result, err := chasm.UpdateWithStartExecution(ctx, key, start, update, input,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting))
	if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
		return out, newError(streamservicepb.STREAM_FAILURE_TYPE_CLOSED, "stream is closed")
	}
	return result.UpdateOutput, err
}

// UnverifiedProducer derives a producer and its epoch from what the producer says it is, as a stream
// with no owner, or another owner's stream, must.
func UnverifiedProducer(p *streamservicepb.Producer) (Producer, error) {
	switch v := p.GetVariant().(type) {
	case *streamservicepb.Producer_Client:
		return Producer{ID: "client/" + v.Client.GetId(), Epoch: v.Client.GetEpoch()}, nil
	case *streamservicepb.Producer_WorkflowTask:
		w := v.WorkflowTask
		return Producer{
			ID:                  "workflow/" + w.GetRunId(),
			RunID:               w.GetRunId(),
			WorkflowTask:        w.GetPreviousStartedEventId(),
			WorkflowTaskAttempt: w.GetAttempt(),
			Epoch:               int64(w.GetAttempt()),
		}, nil
	case *streamservicepb.Producer_LocalActivity:
		la := v.LocalActivity
		return Producer{
			ID:                  "local-activity/" + la.GetRunId() + "/" + la.GetActivityId(),
			RunID:               la.GetRunId(),
			WorkflowTask:        la.GetPreviousStartedEventId(),
			WorkflowTaskAttempt: la.GetWorkflowTaskAttempt(),
			Epoch:               int64(la.GetAttempt()),
		}, nil
	case *streamservicepb.Producer_ActivityTaskToken:
		return activityTokenProducer(v.ActivityTaskToken)
	default:
		return Producer{}, serviceerror.NewInvalidArgument("producer is required")
	}
}

// activityTokenProducer is the producer of an activity task token, unverified: a workflow
// activity's is its workflow run and activity ID, and a Standalone Activity's its run ID.
func activityTokenProducer(token []byte) (Producer, error) {
	t, err := tasktoken.NewSerializer().Deserialize(token)
	if err != nil {
		return Producer{}, serviceerror.NewInvalidArgument("invalid activity task token")
	}
	epoch := int64(t.GetActivityAttemptStamp())
	if len(t.GetComponentRef()) == 0 {
		return Producer{ID: "activity/" + t.GetRunId() + "/" + t.GetActivityId(), Epoch: epoch}, nil
	}
	ref, err := chasm.DeserializeComponentRef(t.GetComponentRef())
	if err != nil {
		return Producer{}, serviceerror.NewInvalidArgument("invalid activity task token")
	}
	return Producer{ID: "activity/" + ref.RunID, Epoch: epoch}, nil
}

// ValidateRef checks that a reference names exactly one of a workflow, an activity and a standalone
// stream, with a name of 1 to MaxNameBytes bytes for an owner's stream and none for a standalone
// stream.
func ValidateRef(ref *streamservicepb.StreamRef) error {
	ids := 0
	for _, id := range []string{ref.GetWorkflowId(), ref.GetActivityId(), ref.GetStreamId()} {
		if id != "" {
			ids++
		}
	}
	switch {
	case ids != 1:
		return serviceerror.NewInvalidArgument("a stream is named by exactly one of a workflow ID, an activity ID and a stream ID")
	case ref.GetStreamId() != "" && ref.GetName() != "":
		return serviceerror.NewInvalidArgument("a standalone stream has no name")
	case ref.GetStreamId() == "" && (ref.GetName() == "" || len(ref.GetName()) > MaxNameBytes):
		return serviceerror.NewInvalidArgumentf("a stream of a workflow or activity needs a name of 1 to %d bytes", MaxNameBytes)
	}
	return nil
}
