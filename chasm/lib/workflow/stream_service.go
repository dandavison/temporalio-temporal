package workflow

// The stream service on the System Nexus Endpoint: Create, Append, Read and Close on a named stream
// of a workflow run chain or a Standalone Activity execution, or on a standalone stream; Wait, which
// a workflow reader calls when a read returns nothing; and NotifierState and Notify, for streams in
// external stores. An owner's streams, and its notifiers, are embedded in its CHASM tree; the
// activity's side is in chasm/lib/activity, and a standalone stream, and a standalone stream's
// notifier, executions of their own, are in chasm/lib/stream. Moving a workflow's streams to the
// next run of its chain (continue-as-new, retry, cron) is not built, so a new run of the chain
// starts with no streams, and the earlier run's streams are no longer read or written.

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
	config            Config
}

func mustNewStreamServiceNexusHandler(h *streamServiceHandler) *nexus.Service {
	svc := nexus.NewService(stream.ServiceName)
	svc.MustRegister(
		nexus.NewSyncOperation(stream.CreateOperation, h.create),
		nexus.NewSyncOperation(stream.AppendOperation, h.append),
		nexus.NewSyncOperation(stream.ReadOperation, h.read),
		nexus.NewSyncOperation(stream.CloseOperation, h.close),
		&waitOperation{h: h},
		nexus.NewSyncOperation(stream.NotifierStateOperation, h.notifierState),
		nexus.NewSyncOperation(stream.NotifyOperation, h.notify),
	)
	return svc
}

// streamOwnerKind is the kind of execution that holds a stream: its owner, or, for a standalone
// stream, the stream's own execution.
type streamOwnerKind int

const (
	workflowStreamOwner streamOwnerKind = iota
	activityStreamOwner
	standaloneStream
)

// requireNativeStreams fails an operation on a stream in the server unless
// history.enableNativeStreams is on.
func (h *streamServiceHandler) requireNativeStreams(ref *streamservicepb.StreamRef) error {
	if !h.config.enableNativeStreams(ref.GetNamespace()) {
		return serviceerror.NewUnimplemented("native streams are disabled (history.enableNativeStreams)")
	}
	return nil
}

// requireWorkflowReads fails Wait, NotifierState and Notify unless history.enableStreamWorkflowReads
// is on.
func (h *streamServiceHandler) requireWorkflowReads(ref *streamservicepb.StreamRef) error {
	if !h.config.enableStreamWorkflowReads(ref.GetNamespace()) {
		return serviceerror.NewUnimplemented("workflow reads of streams are disabled (history.enableStreamWorkflowReads)")
	}
	return nil
}

func (h *streamServiceHandler) create(
	ctx context.Context,
	req *streamservicepb.CreateRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.CreateResponse, error) {
	if err := h.requireNativeStreams(req.GetStream()); err != nil {
		return nil, err
	}
	idle, err := stream.IdleTimeout(req.GetStream(), req.GetSettings())
	if err != nil {
		return nil, err
	}
	ref, kind, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var resp *streamservicepb.CreateResponse
	switch kind {
	case standaloneStream:
		resp, err = stream.UpdateOrStartStandalone(ctx, ref.ExecutionKey,
			func(ctx chasm.MutableContext, in stream.StandaloneCreate) (*stream.Standalone, error) {
				return stream.NewStandalone(ctx, in.Req.GetRequestId(), in.IdleTimeout), nil
			},
			(*stream.Standalone).Create, stream.StandaloneCreate{Req: req, IdleTimeout: idle})
	case activityStreamOwner:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).CreateStream, req)
	default:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).createStream, req)
	}
	return resp, streamOperationError(ownerNotFound(err))
}

func (h *streamServiceHandler) append(
	ctx context.Context,
	req *streamservicepb.AppendRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.AppendResponse, error) {
	if err := h.requireNativeStreams(req.GetStream()); err != nil {
		return nil, err
	}
	ref, kind, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var resp *streamservicepb.AppendResponse
	switch kind {
	case standaloneStream:
		resp, err = stream.UpdateOrStartStandalone(ctx, ref.ExecutionKey,
			func(ctx chasm.MutableContext, in stream.StandaloneAppend) (*stream.Standalone, error) {
				return stream.NewStandalone(ctx, "", in.IdleTimeout), nil
			},
			(*stream.Standalone).Append, stream.StandaloneAppend{Req: req, IdleTimeout: stream.DefaultIdleTimeout})
	case activityStreamOwner:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).AppendToStream, req)
	default:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).appendToStream, req)
	}
	return resp, streamOperationError(ownerNotFound(err))
}

func (h *streamServiceHandler) close(
	ctx context.Context,
	req *streamservicepb.CloseRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.CloseResponse, error) {
	if err := h.requireNativeStreams(req.GetStream()); err != nil {
		return nil, err
	}
	ref, kind, err := h.ownerRef(req.GetStream(), nil)
	if err != nil {
		return nil, err
	}
	var resp *streamservicepb.CloseResponse
	switch kind {
	case standaloneStream:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*stream.Standalone).Close, req)
	case activityStreamOwner:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).CloseStream, req)
	default:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).closeStream, req)
	}
	return resp, streamOperationError(ownerNotFound(err))
}

// read returns the matching records after the request's cursor, waiting up to the request's wait
// for one. A read from the beginning of a stream whose owner does not exist yet waits as for an
// empty stream. A read after a cursor of a stream whose owner is gone fails as expired. A read with
// until does not wait.
func (h *streamServiceHandler) read(
	ctx context.Context,
	req *streamservicepb.ReadRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.ReadResponse, error) {
	if err := h.requireNativeStreams(req.GetStream()); err != nil {
		return nil, err
	}
	args, err := newStreamReadArgs(req)
	if err != nil {
		return nil, streamOperationError(err)
	}
	ref, kind, err := h.ownerRef(req.GetStream(), args.Cursor)
	if err != nil {
		return nil, err
	}
	wait := min(req.GetWait().AsDuration(), maxStreamReadWait)
	if args.Until != nil {
		wait = 0
	}
	if wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	for {
		var resp *streamservicepb.ReadResponse
		switch kind {
		case standaloneStream:
			resp, err = readStreamOnce(ctx, ref, (*stream.Standalone).Read, args, wait > 0)
		case activityStreamOwner:
			resp, err = readStreamOnce(ctx, ref, (*activity.Activity).ReadStream, args, wait > 0)
		default:
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

// waitOperation is the asynchronous Wait operation, which a workflow calls as a Nexus operation
// after a read returned nothing. It completes at once when what it waits for is already there;
// otherwise a subscription, held by the stream or by a notifier, completes it through the caller's
// callback.
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
	if err := o.h.requireWorkflowReads(req.GetStream()); err != nil {
		return nil, err
	}
	if options.CallbackURL == "" {
		return nil, serviceerror.NewInvalidArgument("Wait needs a callback, which a direct call does not have")
	}
	cursor, err := decodeStreamCursor(req.GetStream(), req.GetAfter())
	if err != nil {
		return nil, streamOperationError(err)
	}
	notifier := req.GetNotifier() != nil
	if notifier {
		if err := requireNotifierRun(req.GetStream()); err != nil {
			return nil, err
		}
	}
	ref, kind, err := o.h.holderRef(req.GetStream(), cursor, notifier)
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
	case notifier && kind == standaloneStream:
		var result chasm.UpdateWithStartExecutionResult[*streamservicepb.WaitResponse]
		result, err = chasm.UpdateWithStartExecution(ctx, ref.ExecutionKey,
			func(ctx chasm.MutableContext, _ stream.WaitArgs) (*stream.StandaloneNotifier, error) {
				return stream.NewStandaloneNotifier(ctx), nil
			},
			(*stream.StandaloneNotifier).Wait, args,
			chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyUseExisting))
		resp = result.UpdateOutput
	case notifier && kind == activityStreamOwner:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).WaitOnNotifier, args)
	case notifier:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).waitOnNotifier, args)
	case kind == standaloneStream:
		resp, _, err = chasm.UpdateComponent(ctx, ref, (*stream.Standalone).Wait, args)
	case kind == activityStreamOwner:
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
	token, err := proto.Marshal(&streampb.WaitToken{Stream: req.GetStream(), RequestId: options.RequestID, Notifier: notifier})
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
	ref, kind, err := o.h.holderRef(t.GetStream(), nil, t.GetNotifier())
	if err != nil {
		return err
	}
	canceled := stream.CanceledWait{Name: t.GetStream().GetName(), RequestID: t.GetRequestId()}
	switch {
	case kind == standaloneStream && t.GetNotifier():
		_, _, err = chasm.UpdateComponent(ctx, ref, (*stream.StandaloneNotifier).CancelWait, t.GetRequestId())
	case kind == standaloneStream:
		_, _, err = chasm.UpdateComponent(ctx, ref, (*stream.Standalone).CancelWait, t.GetRequestId())
	case kind == activityStreamOwner:
		_, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).CancelStreamWait, canceled)
	default:
		_, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).cancelStreamWait, canceled)
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

// notifierState returns the notifier position of a stream in an external store, and the owner's
// terminal state if it has ended. The position is read before the owner, so that a reader that
// reads the store after this call misses no Notify. A standalone stream has no owner to end.
func (h *streamServiceHandler) notifierState(
	ctx context.Context,
	req *streamservicepb.NotifierStateRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.NotifierStateResponse, error) {
	if err := h.requireWorkflowReads(req.GetStream()); err != nil {
		return nil, err
	}
	if err := requireNotifierRun(req.GetStream()); err != nil {
		return nil, err
	}
	pos := stream.Counters().Position(stream.NotifierStreamKey(req.GetStream()))
	ref, kind, err := h.holderRef(req.GetStream(), nil, true)
	if err != nil {
		return nil, err
	}
	resp := &streamservicepb.NotifierStateResponse{}
	switch kind {
	case standaloneStream:
	case activityStreamOwner:
		resp, err = chasm.ReadComponent(ctx, ref, (*activity.Activity).NotifierState, req.GetStream())
	default:
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

// notify counts a Notify of the stream in memory, and fires the stream's pending Waits, if any.
// With none it writes nothing. A Notify for an owner, or a standalone notifier, that does not exist
// does nothing. The count moves before the holder is read, under its lock, so a Wait that compared
// the count before this call either is subscribed already, and fires, or compared it after the
// increment, and completed at once.
func (h *streamServiceHandler) notify(
	ctx context.Context,
	req *streamservicepb.NotifyRequest,
	_ nexus.StartOperationOptions,
) (*streamservicepb.NotifyResponse, error) {
	if err := h.requireWorkflowReads(req.GetStream()); err != nil {
		return nil, err
	}
	if err := requireNotifierRun(req.GetStream()); err != nil {
		return nil, err
	}
	stream.Counters().Increment(stream.NotifierStreamKey(req.GetStream()))
	ref, kind, err := h.holderRef(req.GetStream(), nil, true)
	if err != nil {
		return nil, err
	}
	var pending bool
	switch kind {
	case standaloneStream:
		pending, err = chasm.ReadComponent(ctx, ref, (*stream.StandaloneNotifier).HasPending, req.GetStream())
	case activityStreamOwner:
		pending, err = chasm.ReadComponent(ctx, ref, (*activity.Activity).NotifierPending, req.GetStream())
	default:
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
	switch kind {
	case standaloneStream:
		_, _, err = chasm.UpdateComponent(ctx, ref, (*stream.StandaloneNotifier).Fire, req.GetStream())
	case activityStreamOwner:
		_, _, err = chasm.UpdateComponent(ctx, ref, (*activity.Activity).FireNotifier, req.GetStream())
	default:
		_, _, err = chasm.UpdateComponent(ctx, ref, (*Workflow).fireNotifier, req.GetStream())
	}
	return &streamservicepb.NotifyResponse{}, err
}

// requireNotifierRun rejects a notifier operation on a stream of an owner that does not name the
// owner's run: the notify count is kept per run, so a Notify and a Wait must name the same one.
func requireNotifierRun(ref *streamservicepb.StreamRef) error {
	if ref.GetStreamId() == "" && ref.GetFirstRunId() == "" && ref.GetActivityRunId() == "" {
		return serviceerror.NewInvalidArgument("notifier operations on a stream of an owner need the owner's run ID: a workflow's first run ID, or the activity's run ID")
	}
	return nil
}

// ownerRef refers to the execution that holds the stream: a workflow's current run, a Standalone
// Activity execution, the pinned one or the current one, or a standalone stream's current
// execution. A read after a cursor of an activity's stream reads the execution the cursor belongs
// to.
func (h *streamServiceHandler) ownerRef(ref *streamservicepb.StreamRef, cursor *streampb.Cursor) (chasm.ComponentRef, streamOwnerKind, error) {
	return h.holderRef(ref, cursor, false)
}

// holderRef refers to the execution that holds the stream, as ownerRef does, or with notifier set,
// its notifier: the owner, or for a standalone stream, the standalone stream's notifier execution.
func (h *streamServiceHandler) holderRef(ref *streamservicepb.StreamRef, cursor *streampb.Cursor, notifier bool) (chasm.ComponentRef, streamOwnerKind, error) {
	nsID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(ref.GetNamespace()))
	if err != nil {
		return chasm.ComponentRef{}, 0, err
	}
	switch {
	case ref.GetActivityId() != "":
		runID := ref.GetActivityRunId()
		if cursor != nil {
			runID = cursor.GetActivityRunId()
		}
		return chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
			NamespaceID: nsID.String(),
			BusinessID:  ref.GetActivityId(),
			RunID:       runID,
		}), activityStreamOwner, nil
	case ref.GetStreamId() != "" && notifier:
		return chasm.NewComponentRef[*stream.StandaloneNotifier](chasm.ExecutionKey{
			NamespaceID: nsID.String(),
			BusinessID:  ref.GetStreamId(),
		}), standaloneStream, nil
	case ref.GetStreamId() != "":
		return chasm.NewComponentRef[*stream.Standalone](chasm.ExecutionKey{
			NamespaceID: nsID.String(),
			BusinessID:  ref.GetStreamId(),
		}), standaloneStream, nil
	}
	return chasm.NewComponentRef[*Workflow](chasm.ExecutionKey{
		NamespaceID: nsID.String(),
		BusinessID:  ref.GetWorkflowId(),
	}), workflowStreamOwner, nil
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
		c.GetStreamId() != ref.GetStreamId() ||
		c.GetName() != ref.GetName() ||
		(ref.GetFirstRunId() != "" && c.GetFirstRunId() != ref.GetFirstRunId()) ||
		(ref.GetActivityRunId() != "" && c.GetActivityRunId() != ref.GetActivityRunId()) {
		return nil, streamError(streamservicepb.STREAM_FAILURE_TYPE_CURSOR_MISMATCH, "the cursor belongs to another stream")
	}
	return c, nil
}

// createStream creates the workflow's stream, or answers a create of the stream if it exists.
func (w *Workflow) createStream(
	ctx chasm.MutableContext,
	req *streamservicepb.CreateRequest,
) (*streamservicepb.CreateResponse, error) {
	if err := w.checkStreamChain(req.GetStream()); err != nil {
		return nil, err
	}
	name := req.GetStream().GetName()
	if s, ok := w.stream(ctx, name); ok {
		created, err := s.Create(req.GetRequestId(), 0)
		return &streamservicepb.CreateResponse{Created: created}, err
	}
	if w.GetExecutionState().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil, streamError(streamservicepb.STREAM_FAILURE_TYPE_CLOSED, "the stream's owner has ended")
	}
	_, err := w.newStream(ctx, name, req.GetRequestId())
	return &streamservicepb.CreateResponse{Created: true}, err
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
	name := req.GetStream().GetName()
	s, err := w.mutableStream(ctx, name)
	if err != nil {
		return nil, err
	}
	if req.GetProducer().GetWorkflowTask() != nil {
		if err := w.fixWorkflowAppendMode(ctx, s, streamservicepb.WORKFLOW_APPEND_MODE_SINK); err != nil {
			return nil, err
		}
	}
	offsets, err := s.Append(ctx, p, req.GetSeq(), req.GetRecords(), req.GetFinish(), nil)
	if err != nil {
		return nil, err
	}
	resp := &streamservicepb.AppendResponse{}
	for _, offset := range offsets {
		cursor, err := w.streamCursor(name)(offset)
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
	s, err := w.mutableStream(ctx, req.GetStream().GetName())
	if err != nil {
		return nil, err
	}
	_, err = s.Append(ctx, p, req.GetSeq(), nil, false, req.GetClose())
	return &streamservicepb.CloseResponse{}, err
}

// AppendCommitModeRecords appends records that the run's current workflow task carries in its
// completion, in commit mode, to the workflow's stream named name, with the task as their producer.
// It is called in the completion's transaction, before the task's completion is recorded, while
// LastCompletedWorkflowTaskStartedEventId is still the task's identity.
//
// The server assigns the seq: the worker sends none, and a completion is applied at most once, so
// there is no retry to dedupe.
func (w *Workflow) AppendCommitModeRecords(ctx chasm.MutableContext, name string, records []*streamservicepb.AppendRecord) error {
	info := w.GetExecutionInfo()
	runID := w.GetExecutionState().GetRunId()
	p := stream.Producer{
		ID:                  workflowStreamProducerID(runID),
		RunID:               runID,
		WorkflowTask:        info.GetLastCompletedWorkflowTaskStartedEventId(),
		WorkflowTaskAttempt: info.GetWorkflowTaskAttempt(),
		Epoch:               int64(info.GetWorkflowTaskAttempt()),
	}
	s, err := w.mutableStream(ctx, name)
	if err != nil {
		return err
	}
	if err := w.fixWorkflowAppendMode(ctx, s, streamservicepb.WORKFLOW_APPEND_MODE_COMMIT); err != nil {
		return err
	}
	_, err = s.Append(ctx, p, s.NextSeq(ctx, p), records, false, nil)
	return err
}

// fixWorkflowAppendMode fixes the run's workflow append mode on the stream at its first workflow
// append to it, and rejects a workflow append in the other mode from the one the run fixed on the
// stream or declared for all of its streams.
func (w *Workflow) fixWorkflowAppendMode(ctx chasm.MutableContext, s *stream.Stream, mode streamservicepb.WorkflowAppendMode) error {
	runID := w.GetExecutionState().GetRunId()
	if declared, ok := w.StreamMode.TryGet(ctx); ok && declared.GetMode() == streamservicepb.STREAM_MODE_COMMIT && mode != streamservicepb.WORKFLOW_APPEND_MODE_COMMIT {
		return streamError(streamservicepb.STREAM_FAILURE_TYPE_WORKFLOW_APPEND_MODE_MISMATCH,
			"run %s declared commit mode for its streams, so a workflow append in %s is rejected; its workers disagree on the mode",
			runID, streamservicepb.WorkflowAppendMode_name[int32(mode)])
	}
	return s.FixWorkflowAppendMode(ctx, runID, mode)
}

// DeclareStreamMode applies a run's declaration of its streams' mode, from a protocol message in its
// workflow task completion. Only commit mode is declared; sink is the default. It covers every
// stream of the run, and fixes the run's mode on the streams that exist, as their first workflow
// append would.
func (w *Workflow) DeclareStreamMode(ctx chasm.MutableContext, mode streamservicepb.StreamMode) error {
	if mode != streamservicepb.STREAM_MODE_COMMIT {
		return streamError(streamservicepb.STREAM_FAILURE_TYPE_WORKFLOW_APPEND_MODE_MISMATCH, "only commit mode is declared; a stream is sink-mode unless its owner declares commit")
	}
	for _, field := range w.Streams {
		if err := field.Get(ctx).FixWorkflowAppendMode(ctx, w.GetExecutionState().GetRunId(), streamservicepb.WORKFLOW_APPEND_MODE_COMMIT); err != nil {
			return err
		}
	}
	w.StreamMode = chasm.NewDataField(ctx, &streampb.DeclaredStreamMode{Mode: mode})
	return nil
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
	name := args.Req.GetStream().GetName()
	s, _ := w.stream(ctx, name)
	resp, err := stream.ReadResponse(ctx, s, args.Req, after, until, w.streamCursor(name))
	if err != nil {
		return nil, err
	}
	resp.Mode = w.streamMode(ctx, s)
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

// streamMode is the mode of the workflow's stream s (nil if it does not exist yet) for the current
// run: the mode the run fixed on it by its first workflow append, or declared for all of its
// streams; otherwise sink once the run's first workflow task has completed, or the run has ended,
// since only the first completion declares a mode; otherwise not yet known.
func (w *Workflow) streamMode(ctx chasm.Context, s *stream.Stream) streamservicepb.StreamMode {
	if s != nil {
		if mode, ok := s.WorkflowAppendMode(w.GetExecutionState().GetRunId()); ok {
			if mode == streamservicepb.WORKFLOW_APPEND_MODE_COMMIT {
				return streamservicepb.STREAM_MODE_COMMIT
			}
			return streamservicepb.STREAM_MODE_SINK
		}
	}
	if declared, ok := w.StreamMode.TryGet(ctx); ok {
		return declared.GetMode()
	}
	if w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId() != common.EmptyEventID ||
		w.GetExecutionState().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return streamservicepb.STREAM_MODE_SINK
	}
	return streamservicepb.STREAM_MODE_UNSPECIFIED
}

// waitOnStream completes a Wait on the workflow's stream at once, or subscribes it. A Wait on a
// stream not created yet is held by the workflow's notifier for the stream's name until the stream
// is created, the run's mode becomes known, or the run ends.
func (w *Workflow) waitOnStream(ctx chasm.MutableContext, args stream.WaitArgs) (*streamservicepb.WaitResponse, error) {
	if err := w.checkStreamChain(args.Req.GetStream()); err != nil {
		return nil, err
	}
	after, err := w.streamOffset(args.Cursor)
	if err != nil {
		return nil, err
	}
	name := args.Req.GetStream().GetName()
	s, ok := w.stream(ctx, name)
	known := w.streamMode(ctx, s) != streamservicepb.STREAM_MODE_UNSPECIFIED
	if ok {
		return s.Wait(ctx, args, after, known)
	}
	if end := w.streamOwnerEnd(); end != nil {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_OWNER_ENDED, OwnerEnd: *end}, nil
	}
	return nil, stream.NotifierOf(ctx, &w.StreamNotifiers, name).Subscribe(ctx, args, !known)
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
	ref := args.Req.GetStream()
	return stream.NotifierOf(ctx, &w.StreamNotifiers, ref.GetName()).WaitOnPosition(ctx, args, w.notifierKey(ref))
}

func (w *Workflow) cancelStreamWait(ctx chasm.MutableContext, wait stream.CanceledWait) (chasm.NoValue, error) {
	if s, ok := w.stream(ctx, wait.Name); ok {
		s.CancelSubscription(ctx, wait.RequestID)
	}
	if field, ok := w.StreamNotifiers[wait.Name]; ok {
		field.Get(ctx).CancelSubscription(ctx, wait.RequestID)
	}
	return nil, nil
}

// notifierKey names the workflow's run chain's stream in the notify counters.
func (w *Workflow) notifierKey(ref *streamservicepb.StreamRef) string {
	return stream.NotifierStreamKey(&streamservicepb.StreamRef{
		Namespace:  ref.GetNamespace(),
		WorkflowId: w.GetExecutionInfo().GetWorkflowId(),
		FirstRunId: w.firstRunID(),
		Name:       ref.GetName(),
	})
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
func (w *Workflow) notifierState(_ chasm.Context, ref *streamservicepb.StreamRef) (*streamservicepb.NotifierStateResponse, error) {
	if err := w.checkStreamChain(ref); err != nil {
		return nil, err
	}
	resp := &streamservicepb.NotifierStateResponse{}
	if end := w.streamOwnerEnd(); end != nil {
		resp.OwnerEnd = *end
	}
	return resp, nil
}

// notifierPending reports whether the stream's notifier has a pending Wait, so that a Notify must
// write.
func (w *Workflow) notifierPending(ctx chasm.Context, ref *streamservicepb.StreamRef) (bool, error) {
	if err := w.checkStreamChain(ref); err != nil {
		return false, err
	}
	field, ok := w.StreamNotifiers[ref.GetName()]
	return ok && field.Get(ctx).HasPending(ctx), nil
}

func (w *Workflow) fireNotifier(ctx chasm.MutableContext, ref *streamservicepb.StreamRef) (chasm.NoValue, error) {
	return nil, stream.FireNotifier(ctx, w.StreamNotifiers, ref.GetName(), streamservicepb.WAKE_REASON_APPENDED)
}

// RecordStreamWorkflowTaskCommit records, in each of the workflow's streams, that attempt completed
// the run's current workflow task. It is called in the transaction that completes the task, before
// LastCompletedWorkflowTaskStartedEventId moves past the task's identity. The run's first completion
// makes its mode known, so it completes the Waits on its streams not created yet that were made
// while the mode was not known.
func (w *Workflow) RecordStreamWorkflowTaskCommit(ctx chasm.MutableContext, attempt int32) error {
	task := w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId()
	for _, field := range w.Streams {
		if err := field.Get(ctx).RecordCommit(ctx, w.GetExecutionState().GetRunId(), task, int64(attempt)); err != nil {
			return err
		}
	}
	if task != common.EmptyEventID {
		return nil
	}
	for _, field := range w.StreamNotifiers {
		if err := field.Get(ctx).FireModeKnown(ctx); err != nil {
			return err
		}
	}
	return nil
}

// CloseStreamOnOwnerEnd closes each of the workflow's streams, unless it is already closed, and
// completes the Waits its notifiers hold, in the transaction in which its owner reaches a terminal
// state.
func (w *Workflow) CloseStreamOnOwnerEnd(ctx chasm.MutableContext, state streamservicepb.CloseState, failure *failurepb.Failure) error {
	if err := stream.FireAllNotifiers(ctx, w.StreamNotifiers, streamservicepb.WAKE_REASON_OWNER_ENDED, state); err != nil {
		return err
	}
	for _, field := range w.Streams {
		if err := field.Get(ctx).CloseIfOpen(ctx, &streamservicepb.StreamClose{State: state, Failure: failure}); err != nil {
			return err
		}
	}
	return nil
}

// EndStreamRun completes the pending Waits on the workflow's streams when the run ends but its
// chain continues in a new run, which leaves the streams open: the readers read again, which
// addresses the new run.
func (w *Workflow) EndStreamRun(ctx chasm.MutableContext) error {
	if err := stream.FireAllNotifiers(ctx, w.StreamNotifiers, streamservicepb.WAKE_REASON_OWNER_ENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED); err != nil {
		return err
	}
	for _, field := range w.Streams {
		if err := field.Get(ctx).FireSubscriptions(ctx, streamservicepb.WAKE_REASON_OWNER_ENDED, streamservicepb.CLOSE_STATE_UNSPECIFIED); err != nil {
			return err
		}
	}
	return nil
}

// RemoveStreamActivityProducer removes the state of the run's activity as a producer to each of the
// workflow's streams. It is called when the activity closes, after which the activity's task tokens
// cannot append.
func (w *Workflow) RemoveStreamActivityProducer(ctx chasm.MutableContext, activityID string) {
	for _, field := range w.Streams {
		field.Get(ctx).RemoveProducer(activityStreamProducerID(w.GetExecutionState().GetRunId(), activityID))
	}
}

// stream returns the workflow's stream named name, if it exists.
func (w *Workflow) stream(ctx chasm.Context, name string) (*stream.Stream, bool) {
	field, ok := w.Streams[name]
	if !ok {
		return nil, false
	}
	return field.Get(ctx), true
}

// mutableStream returns the workflow's stream named name, creating it, with its creation record,
// if it does not exist. A stream created after the run ended is created closed, so it takes no
// appends.
func (w *Workflow) mutableStream(ctx chasm.MutableContext, name string) (*stream.Stream, error) {
	if s, ok := w.stream(ctx, name); ok {
		return s, nil
	}
	s, err := w.newStream(ctx, name, "")
	if err != nil {
		return nil, err
	}
	if status := w.GetExecutionState().GetStatus(); status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return s, s.CloseIfOpen(ctx, &streamservicepb.StreamClose{State: StreamCloseState(status)})
	}
	return s, nil
}

// newStream creates the workflow's stream named name, by the Create with requestID or by an append,
// with its creation record, which holds the run's current task identity (see Stream.RecordCreated,
// including the case where readers' rule for it is wrong), and completes the Waits for it that the
// workflow's notifier for the name holds. A later run of the chain creates new streams, with offsets
// from 0 again.
func (w *Workflow) newStream(ctx chasm.MutableContext, name, requestID string) (*stream.Stream, error) {
	s := stream.New(ctx, requestID, 0)
	if w.Streams == nil {
		w.Streams = make(chasm.Map[string, *stream.Stream])
	}
	w.Streams[name] = chasm.NewComponentField(ctx, s)
	if err := s.RecordCreated(ctx, w.GetExecutionState().GetRunId(), w.GetExecutionInfo().GetLastCompletedWorkflowTaskStartedEventId()); err != nil {
		return nil, err
	}
	return s, stream.FireNotifier(ctx, w.StreamNotifiers, name, streamservicepb.WAKE_REASON_APPENDED)
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

func (w *Workflow) streamCursor(name string) func(offset int64) ([]byte, error) {
	return func(offset int64) ([]byte, error) {
		return proto.Marshal(&streampb.Cursor{
			WorkflowId: w.GetExecutionInfo().GetWorkflowId(),
			FirstRunId: w.firstRunID(),
			RunId:      w.GetExecutionState().GetRunId(),
			Name:       name,
			Offset:     offset,
		})
	}
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
	if err := stream.ValidateRef(ref); err != nil {
		return nil, err
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
			BusinessID:  ref.GetWorkflowId() + ref.GetActivityId() + ref.GetStreamId(),
		},
	}, nil
}

func NewStreamServiceNexusServiceProcessor() *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(stream.ServiceName)
	sp.MustRegisterOperation(stream.CreateOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.CreateRequest]{}))
	sp.MustRegisterOperation(stream.AppendOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.AppendRequest]{}))
	sp.MustRegisterOperation(stream.ReadOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.ReadRequest]{}))
	sp.MustRegisterOperation(stream.CloseOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.CloseRequest]{}))
	sp.MustRegisterOperation(stream.WaitOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.WaitRequest]{}))
	sp.MustRegisterOperation(stream.NotifierStateOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.NotifierStateRequest]{}))
	sp.MustRegisterOperation(stream.NotifyOperation, chasm.NewRegisterableNexusOperationProcessor(streamOperationProcessor[*streamservicepb.NotifyRequest]{}))
	return sp
}
