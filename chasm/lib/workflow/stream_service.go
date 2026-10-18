package workflow

// The stream service on the System Nexus Endpoint: Append, Read and Close on the stream owned by a
// workflow run chain. The stream is embedded in the workflow's CHASM tree. Moving it to the new run
// at continue-as-new is not built, so a new run of the chain starts with no stream.

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
	ref, err := h.workflowRef(req.GetStream())
	if err != nil {
		return nil, err
	}
	resp, _, err := chasm.UpdateComponent(ctx, ref, (*Workflow).appendToStream, req)
	return resp, streamOperationError(ownerNotFound(err))
}

func (h *streamServiceHandler) close(
	ctx context.Context,
	req *streamservicepb.CloseRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.CloseResponse, error) {
	ref, err := h.workflowRef(req.GetStream())
	if err != nil {
		return nil, err
	}
	resp, _, err := chasm.UpdateComponent(ctx, ref, (*Workflow).closeStream, req)
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
	ref, err := h.workflowRef(req.GetStream())
	if err != nil {
		return nil, err
	}
	args, err := newStreamReadArgs(req)
	if err != nil {
		return nil, streamOperationError(err)
	}
	wait := min(req.GetWait().AsDuration(), maxStreamReadWait)
	if wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	for {
		resp, err := h.readOnce(ctx, ref, args, wait > 0)
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			if args.cursor != nil {
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

func (h *streamServiceHandler) readOnce(ctx context.Context, ref chasm.ComponentRef, args streamReadArgs, wait bool) (*streamservicepb.ReadResponse, error) {
	resp, err := chasm.ReadComponent(ctx, ref, (*Workflow).readStream, args)
	if err != nil || !wait || len(resp.Records) > 0 || resp.Close != nil || args.req.GetLast() > 0 {
		return resp, err
	}
	polled, _, err := chasm.PollComponent(ctx, ref, func(w *Workflow, ctx chasm.Context, args streamReadArgs) (*streamservicepb.ReadResponse, bool, error) {
		resp, err := w.readStream(ctx, args)
		return resp, err == nil && (len(resp.Records) > 0 || resp.Close != nil), err
	}, args)
	if ctx.Err() != nil || (err == nil && polled == nil) {
		return resp, nil
	}
	return polled, err
}

func (h *streamServiceHandler) workflowRef(ref *streamservicepb.StreamRef) (chasm.ComponentRef, error) {
	nsID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(ref.GetNamespace()))
	if err != nil {
		return chasm.ComponentRef{}, err
	}
	return chasm.NewComponentRef[*Workflow](chasm.ExecutionKey{
		NamespaceID: nsID.String(),
		BusinessID:  ref.GetWorkflowId(),
	}), nil
}

type streamReadArgs struct {
	req    *streamservicepb.ReadRequest
	cursor *streampb.Cursor
}

func newStreamReadArgs(req *streamservicepb.ReadRequest) (streamReadArgs, error) {
	args := streamReadArgs{req: req}
	if len(req.GetCursor()) == 0 {
		return args, nil
	}
	args.cursor = &streampb.Cursor{}
	if err := proto.Unmarshal(req.GetCursor(), args.cursor); err != nil || args.cursor.GetWorkflowId() != req.GetStream().GetWorkflowId() {
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
	offsets, err := w.mutableStream(ctx).Append(ctx, p, req.GetSeq(), req.GetRecords(), req.GetFinish(), nil)
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
	_, err = w.mutableStream(ctx).Append(ctx, p, req.GetSeq(), nil, false, req.GetClose())
	return &streamservicepb.CloseResponse{}, err
}

func (w *Workflow) readStream(ctx chasm.Context, args streamReadArgs) (*streamservicepb.ReadResponse, error) {
	if err := w.checkStreamChain(args.req.GetStream()); err != nil {
		return nil, err
	}
	resp := &streamservicepb.ReadResponse{Cursor: args.req.GetCursor()}
	after := int64(-1)
	if args.cursor != nil {
		if args.cursor.GetFirstRunId() != w.firstRunID() {
			return nil, streamError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's run chain no longer exists")
		}
		after = args.cursor.GetOffset()
	}
	s, ok := w.Stream.TryGet(ctx)
	if !ok {
		return resp, nil
	}
	var result stream.ReadResult
	if n := args.req.GetLast(); n > 0 {
		result = s.ReadLast(ctx, int(n), args.req.GetTopics())
	} else {
		result = s.Read(ctx, after, args.req.GetTopics(), int(args.req.GetMaxRecords()))
	}
	for _, r := range result.Records {
		cursor, err := w.streamCursor(r.Offset)
		if err != nil {
			return nil, err
		}
		record := proto.CloneOf(r.Record)
		record.Cursor = cursor
		resp.Records = append(resp.Records, record)
	}
	if result.Position >= 0 {
		cursor, err := w.streamCursor(result.Position)
		if err != nil {
			return nil, err
		}
		resp.Cursor = cursor
	}
	if result.Closed {
		resp.Close = s.Close
	}
	return resp, nil
}

// RecordStreamWorkflowTaskCommit records, in the stream, that attempt committed the run's current
// workflow task, if any attempt of that task appended. It is called in the transaction that
// completes the task.
func (w *Workflow) RecordStreamWorkflowTaskCommit(ctx chasm.MutableContext, attempt int32) {
	s, ok := w.Stream.TryGet(ctx)
	if !ok {
		return
	}
	s.RecordCommit(ctx, workflowStreamProducerID(w.GetExecutionState().GetRunId()), w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId(), int64(attempt))
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

func (w *Workflow) mutableStream(ctx chasm.MutableContext) *stream.Stream {
	if s, ok := w.Stream.TryGet(ctx); ok {
		return s
	}
	s := stream.New()
	w.Stream = chasm.NewComponentField(ctx, s)
	return s
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
	default:
		return stream.Producer{}, serviceerror.NewInvalidArgument("producer is required")
	}
}

// workflowTaskProducer checks that the producer is the run's current workflow task attempt.
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
		ID:           workflowStreamProducerID(p.GetRunId()),
		WorkflowTask: p.GetPreviousStartedEventId(),
		Epoch:        int64(p.GetAttempt()),
	}, nil
}

// activityProducer checks that the task token is for a started attempt of one of the run's
// activities, with the attempt's current stamp.
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
	if t.GetRunId() != runID || !ok || ai.GetStartedEventId() == common.EmptyEventID || ai.GetStamp() != t.GetActivityAttemptStamp() {
		return stream.Producer{}, streamError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "the activity attempt is not the current one")
	}
	return stream.Producer{
		ID:    "activity/" + runID + "/" + t.GetActivityId(),
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
	if ref.GetWorkflowId() == "" {
		return nil, serviceerror.NewInvalidArgument("stream workflow ID is required")
	}
	ns := ctx.Namespace.Name().String()
	if ref.GetNamespace() == "" {
		ref.Namespace = ns
	} else if ref.GetNamespace() != ns {
		return nil, serviceerror.NewInvalidArgumentf("namespace in stream %q does not match namespace in context %q", ref.GetNamespace(), ns)
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  ref.GetWorkflowId(),
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
