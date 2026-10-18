package stream

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"fmt"
	"slices"

	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/orderedstore"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// ServiceName is the stream service on the System Nexus Endpoint.
const ServiceName = "temporal.api.nexusservices.streamservice.v1.StreamService"

const (
	AppendOperation = "Append"
	ReadOperation   = "Read"
	CloseOperation  = "Close"
)

const defaultMaxRecords = 1000

// Stream is a log of records embedded in its owner. A record's offset is its event ID in the
// ordered store, so offsets are contiguous from 0, and each append is one batch. It applies dedupe
// and fencing by (producer, epoch, seq); the owner checks that the producer is who it says it is.
//
// Producer state is kept in the component, not in the ordered store, because it is overwritten in
// place.
type Stream struct {
	chasm.UnimplementedComponent

	*streampb.StreamState

	Records   chasm.Field[*orderedstore.Log]
	Producers chasm.Map[string, *streampb.ProducerState]
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
func (s *Stream) RecordCreated(ctx chasm.MutableContext, runID string, nextWorkflowTask int64) error {
	_, err := s.add(ctx, &streamservicepb.Record{StreamCreated: &streamservicepb.StreamCreated{
		WorkflowRunId:    runID,
		NextWorkflowTask: nextWorkflowTask,
	}})
	return err
}

// FixWorkflowAppendMode fixes and records a run's workflow append mode at its first workflow
// append, and rejects a later workflow append in the other mode.
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

// CloseIfOpen closes the stream unless it is already closed.
func (s *Stream) CloseIfOpen(c *streamservicepb.StreamClose) {
	if s.Close == nil {
		s.Close = c
	}
}

// add appends records to the ordered store as one batch and returns the offset of the first.
func (s *Stream) add(ctx chasm.MutableContext, records ...*streamservicepb.Record) (int64, error) {
	events := make([][]byte, len(records))
	for i, r := range records {
		b, err := proto.Marshal(r)
		if err != nil {
			return 0, err
		}
		events[i] = b
	}
	return s.Records.Get(ctx).Append(ctx, events...), nil
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

// Read returns up to maxRecords records after offset after (-1 for the beginning) that match
// topics. Markers (workflow task commits, creation, append modes) match every filter.
func (s *Stream) Read(ctx chasm.Context, after int64, topics []string, maxRecords int) (ReadResult, error) {
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	result := ReadResult{Position: after}
	err := s.scan(ctx, after+1, func(offset int64, r *streamservicepb.Record) bool {
		result.Position = offset
		if matches(r, topics) {
			result.Records = append(result.Records, OffsetRecord{Offset: offset, Record: r})
		}
		return len(result.Records) < maxRecords
	})
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
