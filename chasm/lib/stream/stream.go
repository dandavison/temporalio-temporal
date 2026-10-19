package stream

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"fmt"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/orderedstore"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ServiceName is the stream service on the System Nexus Endpoint.
const ServiceName = "temporal.api.nexusservices.streamservice.v1.StreamService"

const (
	AppendOperation = "Append"
	ReadOperation   = "Read"
	CloseOperation  = "Close"
	WaitOperation   = "Wait"
)

const defaultMaxRecords = 1000

// Stream is a log of records embedded in its owner. A record's offset is its event ID in the
// ordered store, so offsets are contiguous from 0, and each append is one batch. It applies dedupe
// and fencing by (producer, epoch, seq); the owner checks that the producer is who it says it is.
//
// Producer state is kept in the component, not in the ordered store, because it is overwritten in
// place. An owning workflow removes the entry of one of its activities when the activity closes,
// and its run has one entry, overwritten by each workflow task. No other entry is removed (clients,
// local activities, and producers other than the owner's own activities and run), so they
// accumulate for the stream's life. Removing a client's entry at its finish would let a retried
// finish be stored twice.
type Stream struct {
	chasm.UnimplementedComponent

	*streampb.StreamState

	Records   chasm.Field[*orderedstore.Log]
	Producers chasm.Map[string, *streampb.ProducerState]
	// Subscriptions are the stream's pending Waits, keyed by the Wait's request ID.
	Subscriptions chasm.Map[string, *Subscription]
}

func New(ctx chasm.MutableContext) *Stream {
	return &Stream{
		StreamState: &streampb.StreamState{},
		Records:     chasm.NewComponentField(ctx, orderedstore.New()),
		Producers:   make(chasm.Map[string, *streampb.ProducerState]),
	}
}

func (s *Stream) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Error is a stream operation failure. Its type is returned to the caller as the
// ApplicationFailureInfo type.
type Error struct {
	Type    streamservicepb.StreamFailureType
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func newError(t streamservicepb.StreamFailureType, format string, args ...any) *Error {
	return &Error{Type: t, Message: fmt.Sprintf(format, args...)}
}

// NewError returns a stream operation failure of type t.
func NewError(t streamservicepb.StreamFailureType, format string, args ...any) *Error {
	return newError(t, format, args...)
}

// WaitArgs is a Wait request, its decoded cursor (nil for the beginning), and the caller's request
// ID and callback.
type WaitArgs struct {
	Req       *streamservicepb.WaitRequest
	Cursor    *streampb.Cursor
	RequestID string
	Callback  *commonpb.Callback
}

// Wait completes a Wait at once, returning its result, if what it waits for is already there, and
// otherwise subscribes it and returns nil. A Wait does not complete for records while the stream's
// mode is not yet known (known is false): the next record, which the owner's first workflow task
// completion appends as its commit record, completes it.
func (s *Stream) Wait(ctx chasm.MutableContext, args WaitArgs, after int64, known bool) (*streamservicepb.WaitResponse, error) {
	if s.Close != nil {
		return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_CLOSED}, nil
	}
	if known {
		found, err := s.HasRecordAfter(ctx, after, args.Req.GetTopics())
		if err != nil || found {
			return &streamservicepb.WaitResponse{Reason: streamservicepb.WAKE_REASON_APPENDED}, err
		}
	}
	return nil, s.Subscribe(ctx, args.RequestID, args.Callback, after, args.Req.GetTopics(), args.Req.GetLifetime().AsDuration())
}

// ReadArgs is a read request and its decoded cursors: Cursor is nil for a read from the beginning,
// and Until is nil for a read with no upper bound.
type ReadArgs struct {
	Req    *streamservicepb.ReadRequest
	Cursor *streampb.Cursor
	Until  *streampb.Cursor
}

// The size limits on an append.
const (
	MaxRecordsPerAppend = 1000
	MaxRecordBytes      = 1 << 20
	MaxAppendBytes      = 4 << 20
)

// Producer is a producer incarnation. Its epoch is (WorkflowTask, WorkflowTaskAttempt, Epoch),
// compared in that order. RunID, WorkflowTask and WorkflowTaskAttempt are set only for workflow and
// local activity producers.
type Producer struct {
	ID                  string
	RunID               string
	WorkflowTask        int64
	WorkflowTaskAttempt int32
	Epoch               int64
}

// compare orders the producer's epoch against the stored one: negative if earlier, 0 if the same,
// positive if later.
func (p Producer) compare(st *streampb.ProducerState) int {
	return cmp.Or(
		cmp.Compare(p.WorkflowTask, st.WorkflowTask),
		cmp.Compare(p.WorkflowTaskAttempt, st.WorkflowTaskAttempt),
		cmp.Compare(p.Epoch, st.Epoch),
	)
}

// MaxExactInt is the largest workflow task, epoch or seq that every stream backend accepts. Redis
// holds them as doubles, which hold integers exactly only up to 2^53-1, and the native stream applies
// the same range so that every backend accepts the same values.
const MaxExactInt = 1<<53 - 1

// Append stores records, and closes the stream if closing is set, as one append of (producer,
// seq). It returns the offsets of the data records. A retry of the producer's latest seq with the
// same content stores nothing and returns the original offsets; the dedupe check comes before the
// close check, so such a retry succeeds after close.
func (s *Stream) Append(
	ctx chasm.MutableContext,
	p Producer,
	seq int64,
	records []*streamservicepb.AppendRecord,
	finish bool,
	closing *streamservicepb.StreamClose,
) ([]int64, error) {
	for _, v := range []int64{p.WorkflowTask, int64(p.WorkflowTaskAttempt), p.Epoch, seq} {
		if v < 0 || v > MaxExactInt {
			return nil, serviceerror.NewInvalidArgument("workflow tasks, epochs and seqs must be integers from 0 to 2^53-1")
		}
	}
	if err := checkSize(records); err != nil {
		return nil, err
	}
	digest, err := appendDigest(records, finish, closing)
	if err != nil {
		return nil, err
	}
	next := int64(0)
	if field, ok := s.Producers[p.ID]; ok {
		st := field.Get(ctx)
		switch c := p.compare(st); {
		case c < 0:
			return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "producer %s has appended at a higher epoch", p.ID)
		case c == 0 && seq == st.Seq && bytes.Equal(digest, st.Digest):
			return offsetRange(st.FirstOffset, st.Count), nil
		case c == 0 && seq == st.Seq:
			return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_SEQ_DIVERGENT, "seq %d is stored with different content", seq)
		case c == 0 && seq < st.Seq:
			return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_SEQ_STALE, "seq %d is lower than the latest, %d", seq, st.Seq)
		case c == 0:
			next = st.Seq + 1
		default:
			// A later epoch starts again at seq 0.
		}
	}
	if seq != next {
		return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_SEQ_GAP, "seq %d is not the next seq, %d", seq, next)
	}
	if s.Close != nil {
		return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_CLOSED, "stream is closed")
	}
	identity := func() *streamservicepb.ProducerIdentity {
		return &streamservicepb.ProducerIdentity{
			Producer:            p.ID,
			WorkflowRunId:       p.RunID,
			WorkflowTask:        p.WorkflowTask,
			WorkflowTaskAttempt: p.WorkflowTaskAttempt,
			Epoch:               p.Epoch,
			Seq:                 seq,
		}
	}
	var batch []*streamservicepb.Record
	for _, r := range records {
		batch = append(batch, &streamservicepb.Record{Producer: identity(), Topic: r.GetTopic(), Payload: r.GetPayload()})
	}
	if finish {
		batch = append(batch, &streamservicepb.Record{Producer: identity(), Finish: true})
	}
	first, err := s.add(ctx, batch...)
	if err != nil {
		return nil, err
	}
	if closing != nil {
		s.Close = closing
		if err := s.FireSubscriptions(ctx, streamservicepb.WAKE_REASON_CLOSED, streamservicepb.CLOSE_STATE_UNSPECIFIED); err != nil {
			return nil, err
		}
	}
	s.Producers[p.ID] = chasm.NewDataField(ctx, &streampb.ProducerState{
		WorkflowTask:        p.WorkflowTask,
		WorkflowTaskAttempt: p.WorkflowTaskAttempt,
		Epoch:               p.Epoch,
		Seq:                 seq,
		FirstOffset:         first,
		Count:               int64(len(records)),
		Digest:              digest,
	})
	return offsetRange(first, int64(len(records))), nil
}

// RecordCommit records that attempt completed workflow task workflowTask of run runID. It is written
// for every task, so that readers can judge records that local activities append after their task
// completed.
func (s *Stream) RecordCommit(ctx chasm.MutableContext, runID string, workflowTask, attempt int64) error {
	_, err := s.add(ctx, &streamservicepb.Record{WorkflowTaskCommit: &streamservicepb.WorkflowTaskCommit{
		WorkflowRunId: runID,
		WorkflowTask:  workflowTask,
		Attempt:       attempt,
	}})
	return err
}

// RecordCreated records, as the stream's first record, the creating run's next workflow task: the
// run's tasks with a lower identity completed before the stream existed, so they have no commit
// record, and readers treat their records as committed.
//
// The rule is wrong in one case: a local activity of a failed attempt of a task that completed
// before the stream existed, still running after the rerun, appends records that readers treat as
// committed.
func (s *Stream) RecordCreated(ctx chasm.MutableContext, runID string, nextWorkflowTask int64) error {
	_, err := s.add(ctx, &streamservicepb.Record{StreamCreated: &streamservicepb.StreamCreated{
		WorkflowRunId:    runID,
		NextWorkflowTask: nextWorkflowTask,
	}})
	return err
}

// WorkflowAppendMode returns the append mode a run's first workflow append, or its declaration,
// fixed, if any.
func (s *Stream) WorkflowAppendMode(runID string) (streamservicepb.WorkflowAppendMode, bool) {
	mode, ok := s.WorkflowAppendModes[runID]
	return mode, ok
}

// FixWorkflowAppendMode fixes and records a run's workflow append mode at its first workflow
// append, or its declaration, and rejects a later workflow append in the other mode.
func (s *Stream) FixWorkflowAppendMode(ctx chasm.MutableContext, runID string, mode streamservicepb.WorkflowAppendMode) error {
	fixed, ok := s.WorkflowAppendModes[runID]
	if ok && fixed != mode {
		return newError(streamservicepb.STREAM_FAILURE_TYPE_WORKFLOW_APPEND_MODE_MISMATCH,
			"run %s appends to its stream in %s, so a workflow append in %s is rejected; its workers disagree on the append mode",
			runID, streamservicepb.WorkflowAppendMode_name[int32(fixed)], streamservicepb.WorkflowAppendMode_name[int32(mode)])
	}
	if ok {
		return nil
	}
	if s.WorkflowAppendModes == nil {
		s.WorkflowAppendModes = make(map[string]streamservicepb.WorkflowAppendMode)
	}
	s.WorkflowAppendModes[runID] = mode
	_, err := s.add(ctx, &streamservicepb.Record{WorkflowAppendMode: &streamservicepb.WorkflowAppendModeFixed{
		WorkflowRunId: runID,
		Mode:          mode,
	}})
	return err
}

// NextSeq returns the seq that the producer's next append in its epoch takes.
func (s *Stream) NextSeq(ctx chasm.Context, p Producer) int64 {
	if field, ok := s.Producers[p.ID]; ok {
		if st := field.Get(ctx); p.compare(st) == 0 {
			return st.Seq + 1
		}
	}
	return 0
}

// RemoveProducer removes a producer's dedupe and fencing state, once the producer can no longer
// append.
func (s *Stream) RemoveProducer(producer string) {
	delete(s.Producers, producer)
}

// CloseIfOpen closes the stream unless it is already closed, and completes its pending Waits.
func (s *Stream) CloseIfOpen(ctx chasm.MutableContext, c *streamservicepb.StreamClose) error {
	if s.Close != nil {
		return nil
	}
	s.Close = c
	return s.FireSubscriptions(ctx, streamservicepb.WAKE_REASON_CLOSED, streamservicepb.CLOSE_STATE_UNSPECIFIED)
}

// add appends records to the ordered store as one batch, completes the pending Waits that one of
// them matches, and returns the offset of the first.
func (s *Stream) add(ctx chasm.MutableContext, records ...*streamservicepb.Record) (int64, error) {
	events := make([][]byte, len(records))
	for i, r := range records {
		b, err := proto.Marshal(r)
		if err != nil {
			return 0, err
		}
		events[i] = b
	}
	first := s.Records.Get(ctx).Append(ctx, events...)
	return first, s.fireMatchingSubscriptions(ctx, records)
}

// OffsetRecord is a record and its offset.
type OffsetRecord struct {
	Offset int64
	Record *streamservicepb.Record
}

// ReadResult is the result of a read. Position is the offset of the last record the read passed,
// or -1 if it passed none.
type ReadResult struct {
	Records  []OffsetRecord
	Position int64
	// Closed is true when the stream is closed and the read passed every record.
	Closed bool
}

// ReadBounds bound a read: no record after offset Until (-1 for none), at most MaxRecords records
// (0 for the default), and at most MaxBytes payload bytes (0 for none). Markers have no payload.
type ReadBounds struct {
	Until      int64
	MaxRecords int
	MaxBytes   int64
}

// Read returns the records after offset after (-1 for the beginning) that match topics, within
// bounds. Markers (workflow task commits, creation, append modes) match every filter. A read stops
// before the first record that would exceed MaxBytes, and fails as TOO_LARGE if no record before it
// in the read has a payload.
func (s *Stream) Read(ctx chasm.Context, after int64, topics []string, bounds ReadBounds) (ReadResult, error) {
	maxRecords := bounds.MaxRecords
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	result := ReadResult{Position: after}
	var size int64
	tooLarge := false
	err := s.scan(ctx, after+1, func(offset int64, r *streamservicepb.Record) bool {
		if bounds.Until >= 0 && offset > bounds.Until {
			return false
		}
		if matches(r, topics) {
			n := int64(proto.Size(r.GetPayload()))
			if bounds.MaxBytes > 0 && size+n > bounds.MaxBytes {
				// Markers have no payload, so a record exceeds the limit alone if nothing before it
				// in this read had a payload.
				tooLarge = size == 0
				return false
			}
			size += n
			result.Records = append(result.Records, OffsetRecord{Offset: offset, Record: r})
		}
		result.Position = offset
		return len(result.Records) < maxRecords
	})
	if err == nil && tooLarge {
		return result, newError(streamservicepb.STREAM_FAILURE_TYPE_TOO_LARGE, "the next record exceeds the read's limit of %d bytes", bounds.MaxBytes)
	}
	result.Closed = s.Close != nil && result.Position == s.nextOffset(ctx)-1
	return result, err
}

// ReadLast returns the last n records that match topics. The ordered store reads forwards only, so
// it reads windows of growing size backwards from the end.
func (s *Stream) ReadLast(ctx chasm.Context, n int, topics []string) (ReadResult, error) {
	next := s.nextOffset(ctx)
	result := ReadResult{Position: next - 1, Closed: s.Close != nil}
	for end, size := next, int64(n); end > 0 && len(result.Records) < n; size *= 2 {
		start := max(0, end-size)
		var window []OffsetRecord
		err := s.scan(ctx, start, func(offset int64, r *streamservicepb.Record) bool {
			if offset >= end {
				return false
			}
			if matches(r, topics) {
				window = append(window, OffsetRecord{Offset: offset, Record: r})
			}
			return true
		})
		if err != nil {
			return result, err
		}
		result.Records = append(window, result.Records...)
		end = start
	}
	result.Records = result.Records[max(0, len(result.Records)-n):]
	return result, nil
}

func (s *Stream) nextOffset(ctx chasm.Context) int64 {
	return s.Records.Get(ctx).NextEventId
}

// scan calls fn with each record from offset from onwards, until fn returns false.
func (s *Stream) scan(ctx chasm.Context, from int64, fn func(offset int64, r *streamservicepb.Record) bool) error {
	var err error
	s.Records.Get(ctx).Read(ctx, from, func(id int64, event []byte) bool {
		r := &streamservicepb.Record{}
		if err = proto.Unmarshal(event, r); err != nil {
			return false
		}
		return fn(id, r)
	})
	return err
}

func matches(r *streamservicepb.Record, topics []string) bool {
	return len(topics) == 0 || r.WorkflowTaskCommit != nil || r.StreamCreated != nil || r.WorkflowAppendMode != nil ||
		slices.Contains(topics, r.Topic)
}

func checkSize(records []*streamservicepb.AppendRecord) error {
	if len(records) > MaxRecordsPerAppend {
		return newError(streamservicepb.STREAM_FAILURE_TYPE_TOO_LARGE, "%d records exceed the limit of %d per append", len(records), MaxRecordsPerAppend)
	}
	total := 0
	for _, r := range records {
		size := proto.Size(r.GetPayload())
		if size > MaxRecordBytes {
			return newError(streamservicepb.STREAM_FAILURE_TYPE_TOO_LARGE, "a record of %d bytes exceeds the limit of %d", size, MaxRecordBytes)
		}
		total += size
	}
	if total > MaxAppendBytes {
		return newError(streamservicepb.STREAM_FAILURE_TYPE_TOO_LARGE, "an append of %d bytes exceeds the limit of %d", total, MaxAppendBytes)
	}
	return nil
}

func offsetRange(first, count int64) []int64 {
	offsets := make([]int64, count)
	for i := range offsets {
		offsets[i] = first + int64(i)
	}
	return offsets
}

// appendDigest digests an append's content as encoded by the caller, so it reveals nothing about
// encrypted payloads.
func appendDigest(records []*streamservicepb.AppendRecord, finish bool, closing *streamservicepb.StreamClose) ([]byte, error) {
	content := &streamservicepb.AppendRequest{Records: records, Finish: finish}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(content)
	if err != nil {
		return nil, err
	}
	c, err := proto.MarshalOptions{Deterministic: true}.Marshal(closing)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(append(b, c...))
	return sum[:], nil
}

// ReadResponse answers a read of the records after offset after (-1 for the beginning) and up to
// offset until (-1 for no bound), with cursorFor encoding an offset as a cursor of the owner's
// stream. s is nil when the owner has no stream yet. It sets the owner's close time once the owner
// has closed; the caller sets the mode, and the retention.
func ReadResponse(
	ctx chasm.Context,
	s *Stream,
	req *streamservicepb.ReadRequest,
	after, until int64,
	cursorFor func(offset int64) ([]byte, error),
) (*streamservicepb.ReadResponse, error) {
	resp := &streamservicepb.ReadResponse{Cursor: req.GetAfter()}
	if closeTime := ctx.ExecutionInfo().CloseTime; !closeTime.IsZero() {
		resp.OwnerCloseTime = timestamppb.New(closeTime)
	}
	if s == nil {
		return resp, nil
	}
	var result ReadResult
	var err error
	if n := req.GetLast(); n > 0 {
		result, err = s.ReadLast(ctx, int(n), req.GetTopics())
	} else {
		result, err = s.Read(ctx, after, req.GetTopics(), ReadBounds{Until: until, MaxRecords: int(req.GetMaxRecords()), MaxBytes: req.GetMaxBytes()})
	}
	if err != nil {
		return nil, err
	}
	for _, r := range result.Records {
		cursor, err := cursorFor(r.Offset)
		if err != nil {
			return nil, err
		}
		record := proto.CloneOf(r.Record)
		record.Cursor = cursor
		resp.Records = append(resp.Records, record)
	}
	if result.Position >= 0 {
		if resp.Cursor, err = cursorFor(result.Position); err != nil {
			return nil, err
		}
	}
	if result.Closed {
		resp.Close = s.Close
	}
	return resp, nil
}
