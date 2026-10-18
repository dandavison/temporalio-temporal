package workflow

// The stream service on the System Nexus Endpoint: Append, Read and Close on the stream owned by a
// workflow run chain or a Standalone Activity execution. The stream is embedded in its owner's CHASM
// tree; the activity's side is in chasm/lib/activity. Moving a workflow's stream to the next run
// of its chain (continue-as-new, retry, cron) is not built, so a new run of the chain starts with
// no stream, and the earlier run's stream is no longer read or written.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/protobuf/proto"
)

// maxStreamReadWait is below the history client's 30 s per-attempt timeout.
const maxStreamReadWait = 20 * time.Second

// streamNotFoundRetryInterval is how often a read from the beginning retries a missing owner.
const streamNotFoundRetryInterval = 200 * time.Millisecond

type streamServiceHandler struct {
	namespaceRegistry namespace.Registry
}

func mustNewStreamServiceNexusHandler(h *streamServiceHandler) *nexus.Service {
	svc := nexus.NewService(stream.ServiceName)
	svc.MustRegister(
		nexus.NewSyncOperation(stream.AppendOperation, h.append),
		nexus.NewSyncOperation(stream.ReadOperation, h.read),
		nexus.NewSyncOperation(stream.CloseOperation, h.close),
	)
	return svc
}

func (h *streamServiceHandler) append(
	ctx context.Context,
	req *streamservicepb.AppendRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.AppendResponse, error) {
	ref, ownedByActivity, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var resp *streamservicepb.AppendResponse
	if ownedByActivity {
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).AppendToStream, req)
	} else {
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).appendToStream, req)
	}
	return resp, streamOperationError(ownerNotFound(err))
}

func (h *streamServiceHandler) close(
	ctx context.Context,
	req *streamservicepb.CloseRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.CloseResponse, error) {
	ref, ownedByActivity, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var resp *streamservicepb.CloseResponse
	if ownedByActivity {
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).CloseStream, req)
	} else {
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).closeStream, req)
	}
	return resp, streamOperationError(ownerNotFound(err))
}

// read returns the matching records after the request's cursor, waiting up to the request's wait
// for one. A read from the beginning of a stream whose owner does not exist yet waits as for an
// empty stream. A read after a cursor of a stream whose owner is gone fails as expired.
func (h *streamServiceHandler) read(
	ctx context.Context,
	req *streamservicepb.ReadRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.ReadResponse, error) {
	args, err := newStreamReadArgs(req)
	if err != nil {
		return nil, streamOperationError(err)
	}
	ref, ownedByActivity, err := h.ownerRef(req.GetStream(), args.Cursor)
	if err != nil {
		return nil, err
	}
	wait := min(req.GetWait().AsDuration(), maxStreamReadWait)
	if wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	for {
		var resp *streamservicepb.ReadResponse
		if ownedByActivity {
			resp, err = readStreamOnce(ctx, ref, (*activity.Activity).ReadStream, args, wait > 0)
		} else {
			resp, err = readStreamOnce(ctx, ref, (*Workflow).readStream, args, wait > 0)
		}
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			if args.Cursor != nil {
				return nil, streamOperationError(streamError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the stream's owner no longer exists"))
			}
			if wait == 0 {
				return &streamservicepb.ReadResponse{}, nil
			}
			select {
			case <-ctx.Done():
				return &streamservicepb.ReadResponse{}, nil
			case <-time.After(streamNotFoundRetryInterval):
				continue
			}
		}
		return resp, streamOperationError(err)
	}
}

// readStreamOnce reads the owner's stream, and if wait is set and there is nothing to return,
// long-polls until there is, or the context ends.
func readStreamOnce[C any](
	ctx context.Context,
	ref chasm.ComponentRef,
	read func(C, chasm.Context, stream.ReadArgs) (*streamservicepb.ReadResponse, error),
	args stream.ReadArgs,
	wait bool,
) (*streamservicepb.ReadResponse, error) {
	resp, err := chasm.ReadComponent(ctx, ref, read, args)
	if err != nil || !wait || len(resp.Records) > 0 || resp.Close != nil || args.Req.GetLast() > 0 {
		return resp, err
	}
	polled, _, err := chasm.PollComponent(ctx, ref, func(c C, ctx chasm.Context, args stream.ReadArgs) (*streamservicepb.ReadResponse, bool, error) {
		resp, err := read(c, ctx, args)
		return resp, err == nil && (len(resp.Records) > 0 || resp.Close != nil), err
	}, args)
	if ctx.Err() != nil || (err == nil && polled == nil) {
		return resp, nil
	}
	return polled, err
}

// ownerRef refers to the stream's owner: a workflow's current run, or a Standalone Activity
// execution, the pinned one or the current one. A read after a cursor of an activity's stream reads
// the execution the cursor belongs to.
func (h *streamServiceHandler) ownerRef(ref *streamservicepb.StreamRef, cursor *streampb.Cursor) (chasm.ComponentRef, bool, error) {
	nsID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(ref.GetNamespace()))
	if err != nil {
		return chasm.ComponentRef{}, false, err
	}
	if activityID := ref.GetActivityId(); activityID != "" {
		runID := ref.GetActivityRunId()
		if cursor != nil {
			runID = cursor.GetActivityRunId()
		}
		return chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
			NamespaceID: nsID.String(),
			BusinessID:  activityID,
			RunID:       runID,
		}), true, nil
	}
	return chasm.NewComponentRef[*Workflow](chasm.ExecutionKey{
		NamespaceID: nsID.String(),
		BusinessID:  ref.GetWorkflowId(),
	}), false, nil
}

// newStreamReadArgs decodes the request's cursor, which must belong to the requested stream.
func newStreamReadArgs(req *streamservicepb.ReadRequest) (stream.ReadArgs, error) {
	args := stream.ReadArgs{Req: req}
	if len(req.GetAfter()) == 0 {
		return args, nil
	}
	args.Cursor = &streampb.Cursor{}
	ref := req.GetStream()
	if err := proto.Unmarshal(req.GetAfter(), args.Cursor); err != nil ||
		args.Cursor.GetWorkflowId() != ref.GetWorkflowId() ||
		args.Cursor.GetActivityId() != ref.GetActivityId() ||
		(ref.GetActivityRunId() != "" && args.Cursor.GetActivityRunId() != ref.GetActivityRunId()) {
		return args, streamError(streamservicepb.STREAM_FAILURE_TYPE_CURSOR_MISMATCH, "the cursor belongs to another stream")
	}
	return args, nil
}

func (w *Workflow) appendToStream(
	ctx chasm.MutableContext,
	req *streamservicepb.AppendRequest,
) (*streamservicepb.AppendResponse, error) {
	if err := w.checkStreamChain(req.GetStream()); err != nil {
		return nil, err
	}
	p, err := w.streamProducer(req.GetStream().GetWorkflowId(), req.GetProducer())
	if err != nil {
		return nil, err
	}
	s, err := w.mutableStream(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetProducer().GetWorkflowTask() != nil {
		if err := s.FixWorkflowAppendMode(ctx, p.RunID, streamservicepb.WORKFLOW_APPEND_MODE_SINK); err != nil {
			return nil, err
		}
	}
	offsets, err := s.Append(ctx, p, req.GetSeq(), req.GetRecords(), req.GetFinish(), nil)
	if err != nil {
		return nil, err
	}
	resp := &streamservicepb.AppendResponse{}
	for _, offset := range offsets {
		cursor, err := w.streamCursor(offset)
		if err != nil {
			return nil, err
		}
		resp.Cursors = append(resp.Cursors, cursor)
	}
	return resp, nil
}

func (w *Workflow) closeStream(
	ctx chasm.MutableContext,
	req *streamservicepb.CloseRequest,
) (*streamservicepb.CloseResponse, error) {
	if err := w.checkStreamChain(req.GetStream()); err != nil {
		return nil, err
	}
	if req.GetClose().GetState() == streamservicepb.CLOSE_STATE_UNSPECIFIED {
		return nil, serviceerror.NewInvalidArgument("close state is required")
	}
	p, err := w.workflowTaskProducer(req.GetOwner())
	if err != nil {
		return nil, err
	}
	s, err := w.mutableStream(ctx)
	if err != nil {
		return nil, err
	}
	_, err = s.Append(ctx, p, req.GetSeq(), nil, false, req.GetClose())
	return &streamservicepb.CloseResponse{}, err
}

// AppendCommitModeRecords appends records that the run's current workflow task carries in its
// completion, in commit mode, with the task as their producer. It is called in the completion's
// transaction, before the task's completion is recorded, while
// LastCompletedWorkflowTaskStartedEventId is still the task's identity.
//
// The server assigns the seq: the worker sends none, and a completion is applied at most once, so
// there is no retry to dedupe.
func (w *Workflow) AppendCommitModeRecords(ctx chasm.MutableContext, records []*streamservicepb.AppendRecord) error {
	info := w.GetExecutionInfo()
	runID := w.GetExecutionState().GetRunId()
	p := stream.Producer{
		ID:                  workflowStreamProducerID(runID),
		RunID:               runID,
		WorkflowTask:        info.GetLastCompletedWorkflowTaskStartedEventId(),
		WorkflowTaskAttempt: info.GetWorkflowTaskAttempt(),
		Epoch:               int64(info.GetWorkflowTaskAttempt()),
	}
	s, err := w.mutableStream(ctx)
	if err != nil {
		return err
	}
	if err := s.FixWorkflowAppendMode(ctx, runID, streamservicepb.WORKFLOW_APPEND_MODE_COMMIT); err != nil {
		return err
	}
	_, err = s.Append(ctx, p, s.NextSeq(ctx, p), records, false, nil)
	return err
}

func (w *Workflow) readStream(ctx chasm.Context, args stream.ReadArgs) (*streamservicepb.ReadResponse, error) {
	if err := w.checkStreamChain(args.Req.GetStream()); err != nil {
		return nil, err
	}
	after := int64(-1)
	if args.Cursor != nil {
		if args.Cursor.GetFirstRunId() != w.firstRunID() {
			return nil, streamError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's run chain no longer exists")
		}
		// A cursor from an earlier run of the chain passes the check above, but its offset is in that
		// run's stream, so the read skips the current run's records up to that offset.
		after = args.Cursor.GetOffset()
	}
	s, _ := w.Stream.TryGet(ctx)
	return stream.ReadResponse(ctx, s, args.Req, after, w.streamCursor)
}

// RecordStreamWorkflowTaskCommit records, in the stream, that attempt completed the run's current
// workflow task. It is called in the transaction that completes the task, before
// LastCompletedWorkflowTaskStartedEventId moves past the task's identity.
func (w *Workflow) RecordStreamWorkflowTaskCommit(ctx chasm.MutableContext, attempt int32) error {
	s, ok := w.Stream.TryGet(ctx)
	if !ok {
		return nil
	}
	return s.RecordCommit(ctx, w.GetExecutionState().GetRunId(), w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId(), int64(attempt))
}

// CloseStreamOnOwnerEnd closes the stream, unless it is already closed, in the transaction in
// which its owner reaches a terminal state.
func (w *Workflow) CloseStreamOnOwnerEnd(ctx chasm.MutableContext, state streamservicepb.CloseState, failure *failurepb.Failure) {
	s, ok := w.Stream.TryGet(ctx)
	if !ok {
		return
	}
	s.CloseIfOpen(&streamservicepb.StreamClose{State: state, Failure: failure})
}

// RemoveStreamActivityProducer removes the state of the run's activity as a stream producer. It is
// called when the activity closes, after which the activity's task tokens cannot append.
func (w *Workflow) RemoveStreamActivityProducer(ctx chasm.MutableContext, activityID string) {
	if s, ok := w.Stream.TryGet(ctx); ok {
		s.RemoveProducer(activityStreamProducerID(w.GetExecutionState().GetRunId(), activityID))
	}
}

// mutableStream returns the workflow's stream, creating it, with its creation record, if it does
// not exist. The creation record holds the run's current task identity (see Stream.RecordCreated,
// including the case where readers' rule for it is wrong). A later run of the chain creates a new
// stream, with offsets from 0 again. A stream created after the run ended is created closed, so it
// takes no appends.
func (w *Workflow) mutableStream(ctx chasm.MutableContext) (*stream.Stream, error) {
	if s, ok := w.Stream.TryGet(ctx); ok {
		return s, nil
	}
	s := stream.New(ctx)
	w.Stream = chasm.NewComponentField(ctx, s)
	if status := w.GetExecutionState().GetStatus(); status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		s.CloseIfOpen(&streamservicepb.StreamClose{State: StreamCloseState(status)})
	}
	err := s.RecordCreated(ctx, w.GetExecutionState().GetRunId(), w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId())
	return s, err
}

func (w *Workflow) firstRunID() string {
	return w.GetExecutionState().GetFirstExecutionRunId()
}

// checkStreamChain checks a pinned run chain against the workflow's current chain.
func (w *Workflow) checkStreamChain(ref *streamservicepb.StreamRef) error {
	if pinned := ref.GetFirstRunId(); pinned != "" && pinned != w.firstRunID() {
		return streamError(streamservicepb.STREAM_FAILURE_TYPE_NOT_FOUND, "the run chain %s is not the workflow's current chain", pinned)
	}
	return nil
}

func (w *Workflow) streamCursor(offset int64) ([]byte, error) {
	return proto.Marshal(&streampb.Cursor{
		WorkflowId: w.GetExecutionInfo().GetWorkflowId(),
		FirstRunId: w.firstRunID(),
		Offset:     offset,
	})
}

// streamProducer derives the producer and its epoch. Activity and workflow producers are checked
// against the run's state, so they cannot be forged and a superseded attempt is fenced.
func (w *Workflow) streamProducer(workflowID string, p *streamservicepb.Producer) (stream.Producer, error) {
	switch v := p.GetVariant().(type) {
	case *streamservicepb.Producer_Client:
		return stream.Producer{ID: "client/" + v.Client.GetId(), Epoch: v.Client.GetEpoch()}, nil
	case *streamservicepb.Producer_WorkflowTask:
		return w.workflowTaskProducer(v.WorkflowTask)
	case *streamservicepb.Producer_ActivityTaskToken:
		return w.activityProducer(workflowID, v.ActivityTaskToken)
	case *streamservicepb.Producer_LocalActivity:
		return w.localActivityProducer(v.LocalActivity)
	default:
		return stream.Producer{}, serviceerror.NewInvalidArgument("producer is required")
	}
}

// workflowTaskProducer checks that the producer is the run's current workflow task attempt.
// Speculative tasks are rejected because a discarded one leaves no commit record and no History,
// and the next task can have the same identity and attempt.
func (w *Workflow) workflowTaskProducer(p *streamservicepb.WorkflowTaskProducer) (stream.Producer, error) {
	info := w.GetExecutionInfo()
	if p.GetRunId() != w.GetExecutionState().GetRunId() ||
		info.GetWorkflowTaskStartedEventId() == common.EmptyEventID ||
		info.GetLastCompletedWorkflowTaskStartedEventId() != p.GetPreviousStartedEventId() ||
		info.GetWorkflowTaskAttempt() != p.GetAttempt() {
		return stream.Producer{}, streamError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "the workflow task attempt is not the run's current one")
	}
	if info.GetWorkflowTaskType() == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return stream.Producer{}, streamError(streamservicepb.STREAM_FAILURE_TYPE_SPECULATIVE_WORKFLOW_TASK, "appends from speculative workflow tasks are not supported")
	}
	return stream.Producer{
		ID:                  workflowStreamProducerID(p.GetRunId()),
		RunID:               p.GetRunId(),
		WorkflowTask:        p.GetPreviousStartedEventId(),
		WorkflowTaskAttempt: p.GetAttempt(),
		Epoch:               int64(p.GetAttempt()),
	}, nil
}

// localActivityProducer accepts a local activity of the current run without further checks: the
// server cannot verify its Core-made task token. The workflow task it started in is part of its
// epoch, and that task's commit record tells readers whether to keep its records.
func (w *Workflow) localActivityProducer(p *streamservicepb.LocalActivityProducer) (stream.Producer, error) {
	if p.GetRunId() != w.GetExecutionState().GetRunId() {
		return stream.Producer{}, streamError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "the local activity's run is not the current one")
	}
	return stream.Producer{
		ID:                  "local-activity/" + p.GetRunId() + "/" + p.GetActivityId(),
		RunID:               p.GetRunId(),
		WorkflowTask:        p.GetPreviousStartedEventId(),
		WorkflowTaskAttempt: p.GetWorkflowTaskAttempt(),
		Epoch:               int64(p.GetAttempt()),
	}, nil
}

// activityProducer checks that the task token is for the started attempt of one of the run's
// activities: the token's stamp must be the one the attempt started with. An options update raises
// the activity's stamp during the attempt, but not the started stamp.
//
// What this does not guarantee:
//   - A retry raises the stamp only with system.enableActivityRetryStampIncrement, default off.
//     With it off, every attempt has the same stamp, so a superseded attempt is not fenced, and
//     its successor's appends fail as stale or divergent seqs in the same epoch, or are taken as a
//     retry of the earlier attempt's append and not stored.
//   - A token from before the stamp was added carries 0, which matches a first attempt.
//
// Activity-by-ID tokens, which have no scheduled event ID, are rejected. Standalone Activity tokens
// are rejected, since checking one needs the activity's state, which is in another execution.
func (w *Workflow) activityProducer(workflowID string, token []byte) (stream.Producer, error) {
	t, err := tasktoken.NewSerializer().Deserialize(token)
	if err != nil {
		return stream.Producer{}, serviceerror.NewInvalidArgument("invalid activity task token")
	}
	if len(t.GetComponentRef()) > 0 || t.GetScheduledEventId() == common.EmptyEventID || t.GetWorkflowId() != workflowID {
		return stream.Producer{}, serviceerror.NewInvalidArgument("the task token is not for an activity of this workflow")
	}
	runID := w.GetExecutionState().GetRunId()
	ai, ok := w.GetActivityInfo(t.GetScheduledEventId())
	if t.GetRunId() != runID || !ok || ai.GetStartedEventId() == common.EmptyEventID || ai.GetStartedStamp() != t.GetActivityAttemptStamp() {
		return stream.Producer{}, streamError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "the activity attempt is not the current one")
	}
	return stream.Producer{
		ID:    activityStreamProducerID(runID, t.GetActivityId()),
		Epoch: int64(t.GetActivityAttemptStamp()),
	}, nil
}

// ownerNotFound reports a missing owner as a missing stream: a stream exists only while its owner
// does.
func ownerNotFound(err error) error {
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return streamError(streamservicepb.STREAM_FAILURE_TYPE_NOT_FOUND, "the stream's owner does not exist: %v", err)
	}
	return err
}

func activityStreamProducerID(runID, activityID string) string {
	return "activity/" + runID + "/" + activityID
}

func workflowStreamProducerID(runID string) string {
	return "workflow/" + runID
}

func streamError(t streamservicepb.StreamFailureType, format string, args ...any) error {
	return &stream.Error{Type: t, Message: fmt.Sprintf(format, args...)}
}

// streamOperationError makes a stream error an operation failure whose ApplicationFailureInfo type
// is the StreamFailureType value's name.
func streamOperationError(err error) error {
	se, ok := errors.AsType[*stream.Error](err)
	if !ok {
		return err
	}
	nexusFailure, convErr := commonnexus.TemporalFailureToNexusFailure(&failurepb.Failure{
		Message: se.Message,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         streamservicepb.StreamFailureType_name[int32(se.Type)],
			NonRetryable: true,
		}},
	})
	if convErr != nil {
		return convErr
	}
	return &nexus.OperationError{State: nexus.OperationStateFailed, Cause: &nexus.FailureError{Failure: nexusFailure}}
}

// StreamCloseState maps a workflow's terminal status to the close state of its stream.
func StreamCloseState(status enumspb.WorkflowExecutionStatus) streamservicepb.CloseState {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return streamservicepb.CLOSE_STATE_COMPLETED
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return streamservicepb.CLOSE_STATE_FAILED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return streamservicepb.CLOSE_STATE_CANCELED
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return streamservicepb.CLOSE_STATE_TERMINATED
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return streamservicepb.CLOSE_STATE_TIMED_OUT
	default:
		return streamservicepb.CLOSE_STATE_UNSPECIFIED
	}
}

// streamOperationProcessor routes a stream operation to the shard of the stream's owner.
type streamOperationProcessor[I interface {
	GetStream() *streamservicepb.StreamRef
}] struct{}

func (streamOperationProcessor[I]) ProcessInput(ctx chasm.NexusOperationProcessorContext, req I) (*chasm.NexusOperationProcessorResult, error) {
	ref := req.GetStream()
	if (ref.GetWorkflowId() == "") == (ref.GetActivityId() == "") {
		return nil, serviceerror.NewInvalidArgument("a stream is named by exactly one of a workflow ID and an activity ID")
	}
	// The history-side handler is not given the namespace, so it travels in the request.
	ns := ctx.Namespace.Name().String()
	if ref.GetNamespace() == "" {
		ref.Namespace = ns
	} else if ref.GetNamespace() != ns {
		return nil, serviceerror.NewInvalidArgumentf("namespace in stream %q does not match namespace in context %q", ref.GetNamespace(), ns)
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  ref.GetWorkflowId() + ref.GetActivityId(),
		},
	}, nil
}

func NewStreamServiceNexusServiceProcessor() *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(stream.ServiceName)
	sp.MustRegisterOperation(stream.AppendOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.AppendRequest]{}))
	sp.MustRegisterOperation(stream.ReadOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.ReadRequest]{}))
	sp.MustRegisterOperation(stream.CloseOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.CloseRequest]{}))
	return sp
}
