package workflow

// The stream service on the System Nexus Endpoint: Append, Read and Close on the stream owned by a
// workflow run chain or a Standalone Activity execution. The stream is embedded in its owner's CHASM
// tree; the activity's side is in chasm/lib/activity. Moving a workflow's stream to the next run
// of its chain (continue-as-new, retry, cron) is not built, so a new run of the chain starts with
// no stream, and the earlier run's stream is no longer read or written.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
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
	"google.golang.org/protobuf/types/known/durationpb"
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
		&waitOperation{h: h},
		nexus.NewSyncOperation(stream.NotifierStateOperation, h.notifierState),
		nexus.NewSyncOperation(stream.NotifyOperation, h.notify),
	)
	return svc
}

// waitOperation is the asynchronous Wait operation, which a workflow calls as a Nexus operation
// after a read returned nothing. It completes at once when what it waits for is already there;
// otherwise its subscription, held by the stream, completes it through the caller's callback.
type waitOperation struct {
	nexus.UnimplementedOperation[*streamservicepb.WaitRequest, *streamservicepb.WaitResponse]
	h *streamServiceHandler
}

func (o *waitOperation) Name() string {
	return stream.WaitOperation
}

func (o *waitOperation) Start(
	ctx context.Context,
	req *streamservicepb.WaitRequest,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*streamservicepb.WaitResponse], error) {
	cursor, err := decodeStreamCursor(req.GetStream(), req.GetAfter())
	if err != nil {
		return nil, streamOperationError(err)
	}
	if req.GetNotifier() != nil {
		if err := requireNotifierRun(req.GetStream()); err != nil {
			return nil, err
		}
	}
	ref, ownedByActivity, err := o.h.ownerRef(req.GetStream(), cursor)
	if err != nil {
		return nil, err
	}
	args := stream.WaitArgs{
		Req:       req,
		Cursor:    cursor,
		RequestID: options.RequestID,
		Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
			Url:    options.CallbackURL,
			Header: options.CallbackHeader,
		}}},
	}
	var resp *streamservicepb.WaitResponse
	switch {
	case req.GetNotifier() != nil && ownedByActivity:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).WaitOnNotifier, args)
	case req.GetNotifier() != nil:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).waitOnNotifier, args)
	case ownedByActivity:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).WaitOnStream, args)
	default:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).waitOnStream, args)
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return &nexus.HandlerStartOperationResultSync[*streamservicepb.WaitResponse]{
			Value: &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_OWNER_NOT_FOUND},
		}, nil
	}
	if err != nil {
		return nil, streamOperationError(err)
	}
	if resp != nil {
		return &nexus.HandlerStartOperationResultSync[*streamservicepb.WaitResponse]{Value: resp}, nil
	}
	token, err := proto.Marshal(&streampb.WaitToken{Stream: req.GetStream(), RequestId: options.RequestID})
	if err != nil {
		return nil, err
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: base64.URLEncoding.EncodeToString(token)}, nil
}

// Cancel removes the Wait's subscription, if it has not fired.
func (o *waitOperation) Cancel(ctx context.Context, token string, _ nexus.CancelOperationOptions) error {
	b, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return serviceerror.NewInvalidArgument("invalid Wait operation token")
	}
	t := &streampb.WaitToken{}
	if err := proto.Unmarshal(b, t); err != nil {
		return serviceerror.NewInvalidArgument("invalid Wait operation token")
	}
	ref, ownedByActivity, err := o.h.ownerRef(t.GetStream(), nil)
	if err != nil {
		return err
	}
	if ownedByActivity {
		_, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).CancelStreamWait, t.GetRequestId())
	} else {
		_, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).cancelStreamWait, t.GetRequestId())
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

// notifierState returns the notifier position of a stream in an external store, and the owner's
// terminal state if it has ended. The position is read before the owner, so that a reader that
// reads the store after this call misses no Notify.
func (h *streamServiceHandler) notifierState(
	ctx context.Context,
	req *streamservicepb.NotifierStateRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.NotifierStateResponse, error) {
	if err := requireNotifierRun(req.GetStream()); err != nil {
		return nil, err
	}
	pos := stream.Counters().Position(stream.NotifierOwnerKey(req.GetStream()))
	ref, ownedByActivity, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var resp *streamservicepb.NotifierStateResponse
	if ownedByActivity {
		resp, err = chasm.ReadComponent(ctx, ref, (*activity.Activity).NotifierState, req.GetStream())
	} else {
		resp, err = chasm.ReadComponent(ctx, ref, (*Workflow).notifierState, req.GetStream())
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		resp, err = &streamservicepb.NotifierStateResponse{}, nil
	}
	if err != nil {
		return nil, streamOperationError(err)
	}
	resp.Position = pos
	return resp, nil
}

// notify counts a Notify in memory, and fires the owner's pending Waits, if any. With none it writes
// nothing. A Notify to an owner that does not exist does nothing. The count moves before the owner
// is read, under the owner's lock, so a Wait that compared the count before this call either is
// subscribed already, and fires, or compared it after the increment, and completed at once.
func (h *streamServiceHandler) notify(
	ctx context.Context,
	req *streamservicepb.NotifyRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.NotifyResponse, error) {
	if err := requireNotifierRun(req.GetStream()); err != nil {
		return nil, err
	}
	stream.Counters().Increment(stream.NotifierOwnerKey(req.GetStream()))
	ref, ownedByActivity, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var pending bool
	if ownedByActivity {
		pending, err = chasm.ReadComponent(ctx, ref, (*activity.Activity).NotifierPending, req.GetStream())
	} else {
		pending, err = chasm.ReadComponent(ctx, ref, (*Workflow).notifierPending, req.GetStream())
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return &streamservicepb.NotifyResponse{}, nil
	}
	if _, ok := errors.AsType[*stream.Error](err); ok {
		return &streamservicepb.NotifyResponse{}, nil
	}
	if err != nil || !pending {
		return &streamservicepb.NotifyResponse{}, err
	}
	if ownedByActivity {
		_, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).FireNotifier, req.GetStream())
	} else {
		_, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).fireNotifier, req.GetStream())
	}
	return &streamservicepb.NotifyResponse{}, err
}

// requireNotifierRun rejects a notifier operation that does not name the owner's run: the notify
// count is kept per run, so a Notify and a Wait must name the same one.
func requireNotifierRun(ref *streamservicepb.StreamRef) error {
	if ref.GetFirstRunId() == "" && ref.GetActivityRunId() == "" {
		return serviceerror.NewInvalidArgument("notifier operations need the owner's run ID: a workflow's first run ID, or the activity's run ID")
	}
	return nil
}

// waitOnStream completes a Wait on the workflow's stream at once, or subscribes it. It creates the
// stream if it does not exist, as a subscription creates a missing notifier.
func (w *Workflow) waitOnStream(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	if err := w.checkStreamChain(args.Req.GetStream()); err != nil {
		return nil, err
	}
	after, err := w.streamOffset(args.Cursor)
	if err != nil {
		return nil, err
	}
	s, err := w.mutableStream(ctx)
	if err != nil {
		return nil, err
	}
	return s.Wait(ctx, args, after, w.streamMode(s) != streamservicepb.STREAM_MODE_UNSPECIFIED)
}

func (w *Workflow) cancelStreamWait(ctx chasm.MutableContext, requestID string) (chasm.NoValue, error) {
	if s, ok := w.Stream.TryGet(ctx); ok {
		s.CancelSubscription(ctx, requestID)
	}
	if n, ok := w.StreamNotifier.TryGet(ctx); ok {
		n.CancelSubscription(ctx, requestID)
	}
	return nil, nil
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
		if err == nil && resp.OwnerCloseTime != nil {
			if ns, nsErr := h.namespaceRegistry.GetNamespace(namespace.Name(req.GetStream().GetNamespace())); nsErr == nil {
				resp.Retention = durationpb.New(ns.Retention())
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

// newStreamReadArgs decodes the request's cursors, which must belong to the requested stream,
// including its pinned run chain or activity execution.
func newStreamReadArgs(req *streamservicepb.ReadRequest) (stream.ReadArgs, error) {
	args := stream.ReadArgs{Req: req}
	var err error
	if args.Cursor, err = decodeStreamCursor(req.GetStream(), req.GetAfter()); err != nil {
		return args, err
	}
	if args.Until, err = decodeStreamCursor(req.GetStream(), req.GetUntil()); err != nil {
		return args, err
	}
	if args.Cursor != nil && args.Until != nil && args.Cursor.GetActivityRunId() != args.Until.GetActivityRunId() {
		return args, streamError(streamservicepb.STREAM_FAILURE_TYPE_CURSOR_MISMATCH, "the cursors belong to different activity executions")
	}
	return args, nil
}

// decodeStreamCursor decodes a cursor of ref's stream, or returns nil for an empty one.
func decodeStreamCursor(ref *streamservicepb.StreamRef, b []byte) (*streampb.Cursor, error) {
	if len(b) == 0 {
		return nil, nil
	}
	c := &streampb.Cursor{}
	if err := proto.Unmarshal(b, c); err != nil ||
		c.GetWorkflowId() != ref.GetWorkflowId() ||
		c.GetActivityId() != ref.GetActivityId() ||
		(ref.GetFirstRunId() != "" && c.GetFirstRunId() != ref.GetFirstRunId()) ||
		(ref.GetActivityRunId() != "" && c.GetActivityRunId() != ref.GetActivityRunId()) {
		return nil, streamError(streamservicepb.STREAM_FAILURE_TYPE_CURSOR_MISMATCH, "the cursor belongs to another stream")
	}
	return c, nil
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
	after, err := w.streamOffset(args.Cursor)
	if err != nil {
		return nil, err
	}
	until, err := w.streamOffset(args.Until)
	if err != nil {
		return nil, err
	}
	s, _ := w.Stream.TryGet(ctx)
	resp, err := stream.ReadResponse(ctx, s, args.Req, after, until, w.streamCursor)
	if err != nil {
		return nil, err
	}
	resp.Mode = w.streamMode(s)
	return resp, nil
}

// streamOffset returns the offset of a cursor of the workflow's stream, or -1 for none. A cursor of
// another chain, or of an earlier run of this chain, fails as EXPIRED.
func (w *Workflow) streamOffset(c *streampb.Cursor) (int64, error) {
	if c == nil {
		return -1, nil
	}
	if c.GetFirstRunId() != w.firstRunID() {
		return 0, streamError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's run chain no longer exists")
	}
	if c.GetRunId() != w.GetExecutionState().GetRunId() {
		return 0, streamError(streamservicepb.STREAM_FAILURE_TYPE_EXPIRED, "the cursor's run is no longer the chain's current run, and its stream was not moved to the current run")
	}
	return c.GetOffset(), nil
}

// streamMode is the stream's mode for the current run: the mode the run fixed, by its first
// workflow append or its declaration; otherwise sink once the run's first workflow task has
// completed, or the run has ended, since only the first completion declares a mode; otherwise not
// yet known.
func (w *Workflow) streamMode(s *stream.Stream) streamservicepb.StreamMode {
	if s != nil {
		if mode, ok := s.WorkflowAppendMode(w.GetExecutionState().GetRunId()); ok {
			if mode == streamservicepb.WORKFLOW_APPEND_MODE_COMMIT {
				return streamservicepb.STREAM_MODE_COMMIT
			}
			return streamservicepb.STREAM_MODE_SINK
		}
	}
	if w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId() != common.EmptyEventID ||
		w.GetExecutionState().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return streamservicepb.STREAM_MODE_SINK
	}
	return streamservicepb.STREAM_MODE_UNSPECIFIED
}

// DeclareStreamMode applies a run's declaration of its stream's mode, from a protocol message in
// its workflow task completion. Only commit mode is declared; sink is the default. It creates the
// stream, and fixes the run's mode as its first workflow append would.
func (w *Workflow) DeclareStreamMode(ctx chasm.MutableContext, mode streamservicepb.StreamMode) error {
	if mode != streamservicepb.STREAM_MODE_COMMIT {
		return streamError(streamservicepb.STREAM_FAILURE_TYPE_WORKFLOW_APPEND_MODE_MISMATCH, "only commit mode is declared; a stream is sink-mode unless its owner declares commit")
	}
	s, err := w.mutableStream(ctx)
	if err != nil {
		return err
	}
	return s.FixWorkflowAppendMode(ctx, w.GetExecutionState().GetRunId(), streamservicepb.WORKFLOW_APPEND_MODE_COMMIT)
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
func (w *Workflow) CloseStreamOnOwnerEnd(ctx chasm.MutableContext, state streamservicepb.CloseState, failure *failurepb.Failure) error {
	if n, ok := w.StreamNotifier.TryGet(ctx); ok {
		if err := n.Fire(ctx, streamservicepb.WAKE_REASON_OWNER_ENDED, state); err != nil {
			return err
		}
	}
	s, ok := w.Stream.TryGet(ctx)
	if !ok {
		return nil
	}
	return s.CloseIfOpen(ctx, &streamservicepb.StreamClose{State: state, Failure: failure})
}

// EndStreamRun completes the stream's pending Waits when the run ends but its chain continues in a
// new run, which leaves the stream open: the readers read again, which addresses the new run.
func (w *Workflow) EndStreamRun(ctx chasm.MutableContext) error {
	if n, ok := w.StreamNotifier.TryGet(ctx); ok {
		if err := n.Fire(ctx, streamservicepb.WAKE_REASON_OWNER_ENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED); err != nil {
			return err
		}
	}
	s, ok := w.Stream.TryGet(ctx)
	if !ok {
		return nil
	}
	return s.FireSubscriptions(ctx, streamservicepb.WAKE_REASON_OWNER_ENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
}

// waitOnNotifier completes a Wait on the notifier of the workflow's stream in an external store at
// once, if the run has ended or a Notify has moved the position since the reader read it, and
// otherwise subscribes it. It creates the notifier if it does not exist.
func (w *Workflow) waitOnNotifier(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	if err := w.checkStreamChain(args.Req.GetStream()); err != nil {
		return nil, err
	}
	if end := w.streamOwnerEnd(); end != nil {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_OWNER_ENDED, OwnerEnd: *end}, nil
	}
	if !proto.Equal(stream.Counters().Position(w.notifierKey(args.Req.GetStream())), args.Req.GetNotifier()) {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_APPENDED}, nil
	}
	n, ok := w.StreamNotifier.TryGet(ctx)
	if !ok {
		n = stream.NewNotifier()
		w.StreamNotifier = chasm.NewComponentField(ctx, n)
	}
	return nil, n.Subscribe(ctx, args)
}

// notifierKey names the workflow's run chain in the notify counters.
func (w *Workflow) notifierKey(ref *streamservicepb.StreamRef) string {
	return stream.NotifierOwnerKey(&streamservicepb.StreamRef{Namespace: ref.GetNamespace(), WorkflowId: w.GetExecutionInfo().GetWorkflowId(), FirstRunId: w.firstRunID()})
}

// streamOwnerEnd returns the state the run ended in, or nil while it runs.
func (w *Workflow) streamOwnerEnd() *streamservicepb.CloseState {
	status := w.GetExecutionState().GetStatus()
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}
	end := StreamCloseState(status)
	return &end
}

// notifierState returns the state the run ended in, if it has, for NotifierState.
func (w *Workflow) notifierState(ctx chasm.Context, ref *streamservicepb.StreamRef) (*streamservicepb.NotifierStateResponse, error) {
	if err := w.checkStreamChain(ref); err != nil {
		return nil, err
	}
	resp := &streamservicepb.NotifierStateResponse{}
	if end := w.streamOwnerEnd(); end != nil {
		resp.OwnerEnd = *end
	}
	return resp, nil
}

// notifierPending reports whether the notifier has a pending Wait, so that a Notify must write.
func (w *Workflow) notifierPending(ctx chasm.Context, ref *streamservicepb.StreamRef) (bool, error) {
	if err := w.checkStreamChain(ref); err != nil {
		return false, err
	}
	n, ok := w.StreamNotifier.TryGet(ctx)
	return ok && n.HasPending(ctx), nil
}

func (w *Workflow) fireNotifier(ctx chasm.MutableContext, _ *streamservicepb.StreamRef) (chasm.NoValue, error) {
	if n, ok := w.StreamNotifier.TryGet(ctx); ok {
		return nil, n.Fire(ctx, streamservicepb.WAKE_REASON_APPENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
	}
	return nil, nil
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
		if err := s.CloseIfOpen(ctx, &streamservicepb.StreamClose{State: StreamCloseState(status)}); err != nil {
			return nil, err
		}
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
		RunId:      w.GetExecutionState().GetRunId(),
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
	sp.MustRegisterOperation(stream.WaitOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.WaitRequest]{}))
	sp.MustRegisterOperation(stream.NotifierStateOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.NotifierStateRequest]{}))
	sp.MustRegisterOperation(stream.NotifyOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.NotifyRequest]{}))
	return sp
}
