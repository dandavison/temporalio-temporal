package tests

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// nativeStreamBackend calls the stream service's operations on the System Nexus Endpoint, as direct
// Nexus calls. Moving a workflow's stream to the next run of its chain (continue-as-new, retry,
// cron) is not built, so a new run starts with no stream, and the continue-as-new tests fail
// against this backend.
type nativeStreamBackend struct {
	env *testcore.TestEnv
}

const nativeStreamService = "temporal.api.nexusservices.streamservice.v1.StreamService"

func newNativeStreamBackend(env *testcore.TestEnv) streamBackendUnderTest {
	b := &nativeStreamBackend{env: env}
	return streamBackendUnderTest{
		name:          "Native",
		streamBackend: b,
		native:        true,
		deleteStream:  b.deleteStream,
		offset:        b.offset,
	}
}

func (b *nativeStreamBackend) Append(ctx context.Context, ref streamRef, req streamAppendRequest) (streamAppendResponse, error) {
	producer, err := nativeProducer(req.Producer)
	if err != nil {
		return streamAppendResponse{}, err
	}
	in := &streamservicepb.AppendRequest{Stream: nativeRef(ref), Producer: producer, Seq: req.Seq, Finish: req.Finish}
	for _, r := range req.Records {
		in.Records = append(in.Records, &streamservicepb.AppendRecord{Topic: r.Topic, Payload: r.Payload})
	}
	var out streamservicepb.AppendResponse
	if err := b.call(ctx, "Append", in, &out); err != nil {
		return streamAppendResponse{}, err
	}
	resp := streamAppendResponse{}
	for _, c := range out.Cursors {
		resp.Cursors = append(resp.Cursors, c)
	}
	return resp, nil
}

func (b *nativeStreamBackend) Close(ctx context.Context, ref streamRef, req streamCloseRequest) error {
	in := &streamservicepb.CloseRequest{
		Stream: nativeRef(ref),
		Seq:    req.Seq,
		Close: &streamservicepb.StreamClose{
			State:   streamservicepb.CloseState(req.Close.Status),
			Failure: req.Close.Failure,
			Reason:  req.Close.Reason,
		},
	}
	switch {
	case req.Owner.WorkflowTask != nil:
		in.Owner = nativeWorkflowTaskProducer(req.Owner.WorkflowTask)
	case req.Owner.ActivityTask != nil:
		in.ActivityTaskToken = req.Owner.ActivityTask.TaskToken
	default:
		return errStreamNotOwner
	}
	return b.call(ctx, "Close", in, &streamservicepb.CloseResponse{})
}

func (b *nativeStreamBackend) Read(ctx context.Context, ref streamRef, req streamReadRequest) (streamReadResponse, error) {
	in := &streamservicepb.ReadRequest{
		Stream:     nativeRef(ref),
		After:      req.From.after,
		Topics:     req.Topics,
		MaxRecords: int32(req.MaxRecords),
		Last:       int32(req.Last),
	}
	if req.Wait > 0 {
		in.Wait = durationpb.New(req.Wait)
	}
	var out streamservicepb.ReadResponse
	if err := b.call(ctx, "Read", in, &out); err != nil {
		return streamReadResponse{}, err
	}
	resp := streamReadResponse{Cursor: out.Cursor}
	for _, r := range out.Records {
		resp.Records = append(resp.Records, nativeRecord(r))
	}
	if c := out.Close; c != nil {
		resp.Close = &streamClose{Status: streamCloseStatus(c.State), Failure: c.Failure, Reason: c.Reason}
	}
	return resp, nil
}

// deleteStream deletes the stream's owner, as retention would.
func (b *nativeStreamBackend) deleteStream(ctx context.Context, ref streamRef) error {
	if ref.ActivityID != "" {
		return b.deleteActivity(ctx, ref)
	}
	execution := &commonpb.WorkflowExecution{WorkflowId: ref.WorkflowID}
	if _, err := b.env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         b.env.Namespace().String(),
		WorkflowExecution: execution,
	}); err != nil {
		return err
	}
	if _, err := b.env.FrontendClient().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         b.env.Namespace().String(),
		WorkflowExecution: execution,
	}); err != nil {
		return err
	}
	for {
		_, err := b.env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: b.env.Namespace().String(),
			Execution: execution,
		})
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("workflow not deleted: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *nativeStreamBackend) deleteActivity(ctx context.Context, ref streamRef) error {
	if _, err := b.env.FrontendClient().DeleteActivityExecution(ctx, &workflowservice.DeleteActivityExecutionRequest{
		Namespace:  b.env.Namespace().String(),
		ActivityId: ref.ActivityID,
		RunId:      ref.ActivityRunID,
	}); err != nil {
		return err
	}
	for {
		_, err := b.env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  b.env.Namespace().String(),
			ActivityId: ref.ActivityID,
			RunId:      ref.ActivityRunID,
		})
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("activity not deleted: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *nativeStreamBackend) offset(c streamCursor) (int64, error) {
	var cursor streampb.Cursor
	if err := proto.Unmarshal(c, &cursor); err != nil {
		return 0, err
	}
	return cursor.Offset, nil
}

// call makes a direct Nexus call to the stream service and decodes its result into out.
func (b *nativeStreamBackend) call(ctx context.Context, operation string, in, out proto.Message) error {
	input, err := sdkconverter.PreferProtoDataConverter.ToPayload(in)
	if err != nil {
		return err
	}
	resp, err := b.env.FrontendClient().StartNexusOperation(ctx, &workflowservice.StartNexusOperationRequest{
		Namespace: b.env.Namespace().String(),
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   nativeStreamService,
		Operation: operation,
		Input:     input,
	})
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return fmt.Errorf("%w: %w", errStreamInvalidArg, err)
	}
	if err != nil {
		return err
	}
	switch v := resp.Variant.(type) {
	case *workflowservice.StartNexusOperationResponse_SyncSuccess_:
		return sdkconverter.PreferProtoDataConverter.FromPayload(v.SyncSuccess.GetPayload(), out)
	case *workflowservice.StartNexusOperationResponse_OperationFailure_:
		return nativeError(v.OperationFailure.GetFailure())
	default:
		return fmt.Errorf("unexpected direct Nexus call response %T", v)
	}
}

func nativeRef(ref streamRef) *streamservicepb.StreamRef {
	return &streamservicepb.StreamRef{
		WorkflowId:    ref.WorkflowID,
		FirstRunId:    ref.FirstRunID,
		ActivityId:    ref.ActivityID,
		ActivityRunId: ref.ActivityRunID,
	}
}

func nativeProducer(p streamProducer) (*streamservicepb.Producer, error) {
	switch {
	case p.ClientID != "":
		return &streamservicepb.Producer{Variant: &streamservicepb.Producer_Client{
			Client: &streamservicepb.ClientProducer{Id: p.ClientID, Epoch: p.ClientEpoch},
		}}, nil
	case p.ActivityTask != nil:
		return &streamservicepb.Producer{Variant: &streamservicepb.Producer_ActivityTaskToken{
			ActivityTaskToken: p.ActivityTask.TaskToken,
		}}, nil
	case p.WorkflowTask != nil:
		return &streamservicepb.Producer{Variant: &streamservicepb.Producer_WorkflowTask{
			WorkflowTask: nativeWorkflowTaskProducer(p.WorkflowTask),
		}}, nil
	case p.LocalActivity != nil:
		la := p.LocalActivity
		return &streamservicepb.Producer{Variant: &streamservicepb.Producer_LocalActivity{
			LocalActivity: &streamservicepb.LocalActivityProducer{
				RunId:                  la.Task.WorkflowExecution.RunId,
				PreviousStartedEventId: la.Task.PreviousStartedEventId,
				WorkflowTaskAttempt:    la.Task.Attempt,
				ActivityId:             la.ActivityID,
				Attempt:                la.Attempt,
			},
		}}, nil
	default:
		return nil, errors.New("stream producer has no identity")
	}
}

func nativeWorkflowTaskProducer(t *workflowservice.PollWorkflowTaskQueueResponse) *streamservicepb.WorkflowTaskProducer {
	return &streamservicepb.WorkflowTaskProducer{
		RunId:                  t.WorkflowExecution.RunId,
		PreviousStartedEventId: t.PreviousStartedEventId,
		Attempt:                t.Attempt,
	}
}

func nativeRecord(r *streamservicepb.Record) streamRecord {
	rec := streamRecord{
		Cursor: r.Cursor,
		Producer: streamProducerIdentity{
			Producer:            r.Producer.GetProducer(),
			WorkflowRunID:       r.Producer.GetWorkflowRunId(),
			WorkflowTask:        r.Producer.GetWorkflowTask(),
			WorkflowTaskAttempt: r.Producer.GetWorkflowTaskAttempt(),
			Epoch:               r.Producer.GetEpoch(),
			Seq:                 r.Producer.GetSeq(),
		},
		Topic:   r.Topic,
		Payload: r.Payload,
		Finish:  r.Finish,
	}
	if c := r.WorkflowTaskCommit; c != nil {
		rec.WorkflowTaskCommit = &streamWorkflowTaskCommit{WorkflowRunID: c.WorkflowRunId, WorkflowTask: c.WorkflowTask, Attempt: c.Attempt}
	}
	if c := r.StreamCreated; c != nil {
		rec.StreamCreated = &streamCreated{WorkflowRunID: c.WorkflowRunId, NextWorkflowTask: c.NextWorkflowTask}
	}
	if m := r.WorkflowAppendMode; m != nil {
		rec.AppendModeFixed = &streamAppendModeFixed{WorkflowRunID: m.WorkflowRunId, Mode: nativeAppendModes[m.Mode]}
	}
	return rec
}

var nativeAppendModes = map[streamservicepb.WorkflowAppendMode]streamWorkflowAppendMode{
	streamservicepb.WORKFLOW_APPEND_MODE_SINK:   streamAppendModeSink,
	streamservicepb.WORKFLOW_APPEND_MODE_COMMIT: streamAppendModeCommit,
}

var nativeErrors = map[streamservicepb.StreamFailureType]error{
	streamservicepb.STREAM_FAILURE_TYPE_CLOSED:                        errStreamClosed,
	streamservicepb.STREAM_FAILURE_TYPE_NOT_FOUND:                     errStreamNotFound,
	streamservicepb.STREAM_FAILURE_TYPE_EXPIRED:                       errStreamExpired,
	streamservicepb.STREAM_FAILURE_TYPE_NOT_OWNER:                     errStreamNotOwner,
	streamservicepb.STREAM_FAILURE_TYPE_CURSOR_MISMATCH:               errStreamCursor,
	streamservicepb.STREAM_FAILURE_TYPE_SEQ_DIVERGENT:                 errStreamSeqDivergent,
	streamservicepb.STREAM_FAILURE_TYPE_SEQ_STALE:                     errStreamSeqStale,
	streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED:                  errStreamEpochFenced,
	streamservicepb.STREAM_FAILURE_TYPE_SPECULATIVE_WORKFLOW_TASK:     errStreamSpeculative,
	streamservicepb.STREAM_FAILURE_TYPE_SEQ_GAP:                       errStreamSeqGap,
	streamservicepb.STREAM_FAILURE_TYPE_TOO_LARGE:                     errStreamTooLarge,
	streamservicepb.STREAM_FAILURE_TYPE_WORKFLOW_APPEND_MODE_MISMATCH: errStreamAppendMode,
}

// nativeError maps an operation failure to the stream error given by the ApplicationFailureInfo
// type somewhere in its cause chain.
func nativeError(f *failurepb.Failure) error {
	for c := f; c != nil; c = c.GetCause() {
		if t, ok := streamservicepb.StreamFailureType_value[c.GetApplicationFailureInfo().GetType()]; ok {
			if err, ok := nativeErrors[streamservicepb.StreamFailureType(t)]; ok {
				return fmt.Errorf("%w: %s", err, c.GetMessage())
			}
		}
	}
	return fmt.Errorf("stream operation failed: %v", f)
}
