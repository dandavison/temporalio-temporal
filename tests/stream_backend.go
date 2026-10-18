package tests

// The stream backend interface that the shared stream tests (stream_test.go) run against.
//
// It is shaped like the SDK-side boundary where a stream store plugs in: a client or worker
// process, not the server. The native implementation calls the stream service's operations on the
// System Nexus Endpoint as direct Nexus calls. The Redis implementation talks to Redis as a worker or client would, and the server never
// talks to Redis. Code that the Redis implementation needs and the native one does not is the cost
// of an external store.
//
// Cursors are opaque: application code passes them back but never compares them or does
// arithmetic on them.

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
)

type streamBackend interface {
	// Append appends records as one atomic, contiguous batch. The first append creates the stream.
	Append(ctx context.Context, ref streamRef, req streamAppendRequest) (streamAppendResponse, error)
	// Close closes the stream. Only the stream's owner may close it.
	Close(ctx context.Context, ref streamRef, req streamCloseRequest) error
	Read(ctx context.Context, ref streamRef, req streamReadRequest) (streamReadResponse, error)
}

// streamRef identifies a stream owned by a workflow run chain. FirstRunID is optional; when empty
// the stream of the workflow ID's current chain is used.
type streamRef struct {
	WorkflowID string
	FirstRunID string
}

// streamCursor marks the boundary after a record. Its bytes belong to the backend that issued it.
type streamCursor []byte

// streamProducer says who is appending. The backend derives the stored producer identity from it:
// for an activity, from the activity task; for a workflow, from the workflow task; for a client,
// from the ID and epoch the client supplies.
type streamProducer struct {
	ClientID     string
	ClientEpoch  int64
	ActivityTask *workflowservice.PollActivityTaskQueueResponse
	WorkflowTask *workflowservice.PollWorkflowTaskQueueResponse
}

func clientProducer(id string, epoch int64) streamProducer {
	return streamProducer{ClientID: id, ClientEpoch: epoch}
}

func activityProducer(task *workflowservice.PollActivityTaskQueueResponse) streamProducer {
	return streamProducer{ActivityTask: task}
}

// workflowProducer is a workflow appending sink-style: the worker appends the records at the end of
// an activation of this workflow task, before responding to it. Appends from speculative workflow
// tasks are rejected.
func workflowProducer(task *workflowservice.PollWorkflowTaskQueueResponse) streamProducer {
	return streamProducer{WorkflowTask: task}
}

// streamProducerIdentity is the (producer, epoch, seq) stored with every record. Producer is a
// backend-chosen stable string: an activity, a workflow run, or a client's ID.
//
// The epoch numbers the producer's incarnations. For an activity it is the started stamp of the
// attempt, which increases on every retry, reset, pause, unpause and options update. For a client
// it is what the client supplies. For a workflow it is (WorkflowTask, Epoch): WorkflowTask is the
// started event ID of the run's last completed workflow task, and Epoch is the attempt of the
// current one. Records of a workflow are superseded only by a later attempt of the same task.
type streamProducerIdentity struct {
	Producer     string
	WorkflowTask int64
	Epoch        int64
	Seq          int64
}

// streamWorkflowTaskCommit records which attempt of a workflow task committed. Native only: the
// server writes it in the same transaction as the task's completion.
type streamWorkflowTaskCommit struct {
	Producer     string
	WorkflowTask int64
	Attempt      int64
}

type streamAppendRecord struct {
	Topic   string
	Payload *commonpb.Payload
}

type streamAppendRequest struct {
	Producer streamProducer
	// Seq increases within one (producer, epoch). A producer has at most one append in flight.
	Seq     int64
	Records []streamAppendRecord
	// Finish records that this producer will write no more. It does not close the stream.
	Finish bool
}

type streamAppendResponse struct {
	Cursors []streamCursor
}

type streamCloseStatus int

const (
	streamClosedCompleted streamCloseStatus = iota + 1
	streamClosedFailed
	streamClosedCanceled
	streamClosedTerminated
	streamClosedTimedOut
)

type streamClose struct {
	Status  streamCloseStatus
	Failure *failurepb.Failure
	Reason  string
}

type streamCloseRequest struct {
	Owner streamProducer
	Seq   int64
	Close streamClose
}

// streamPosition is where a read starts: at the beginning, or after a cursor.
type streamPosition struct {
	after streamCursor
}

func streamBeginning() streamPosition                { return streamPosition{} }
func streamAfter(cursor streamCursor) streamPosition { return streamPosition{after: cursor} }

type streamReadRequest struct {
	From streamPosition
	// Last, if positive, reads the last N matching records instead of reading From.
	Last int
	// Topics filters records by topic. Empty means all topics.
	Topics     []string
	MaxRecords int
	// Wait is how long the read may wait for a matching record. Zero returns at once.
	Wait time.Duration
}

type streamRecord struct {
	Cursor   streamCursor
	Producer streamProducerIdentity
	Topic    string
	Payload  *commonpb.Payload
	Finish   bool
	// WorkflowTaskCommit, when set, makes this record a commit marker rather than data.
	WorkflowTaskCommit *streamWorkflowTaskCommit
}

type streamReadResponse struct {
	Records []streamRecord
	// Cursor is where the next read resumes, past records a topic filter skipped.
	Cursor streamCursor
	// Close is set once the stream is closed and the reader has every record.
	Close *streamClose
}

var (
	errStreamClosed         = errors.New("stream is closed")
	errStreamNotFound       = errors.New("stream not found")
	errStreamExpired        = errors.New("stream expired")
	errStreamNotOwner       = errors.New("only the stream's owner may close it")
	errStreamCursor         = errors.New("cursor belongs to another stream")
	errStreamSeqDivergent   = errors.New("seq already stored with different content")
	errStreamSeqStale       = errors.New("seq lower than the producer's latest")
	errStreamEpochFenced    = errors.New("epoch lower than one already seen for this producer")
	errStreamSpeculative    = errors.New("appends from speculative workflow tasks are not supported")
	errStreamNotImplemented = errors.New("stream backend not implemented")
)

// streamBackendUnderTest is a backend plus what the tests need to know and do outside the
// interface.
type streamBackendUnderTest struct {
	name string
	streamBackend
	// native is true for the Temporal-native backend. Guarantees only it provides are tested
	// against it alone.
	native bool
	// deleteStream deletes a stream, as retention would.
	deleteStream func(ctx context.Context, ref streamRef) error
	// offset decodes a native cursor. Native only.
	offset func(cursor streamCursor) (int64, error)
}
