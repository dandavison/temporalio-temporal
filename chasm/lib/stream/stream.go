package stream

import (
	"bytes"
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

// Producer is a producer incarnation. A workflow producer's epoch is (WorkflowTask, Epoch).
type Producer struct {
	ID           string
	WorkflowTask int64
	Epoch        int64
}

func (p Producer) before(st *streampb.ProducerState) bool {
	return p.WorkflowTask < st.WorkflowTask || (p.WorkflowTask == st.WorkflowTask && p.Epoch < st.Epoch)
}

func (p Producer) sameEpoch(st *streampb.ProducerState) bool {
	return p.WorkflowTask == st.WorkflowTask && p.Epoch == st.Epoch
}

// Append stores records, and closes the stream if closing is set, as one append of (producer,
// seq). It returns the offsets of the data records. A retry of the producer's latest seq with the
// same content stores nothing and returns the original offsets; the dedupe check comes before the
// close check, so such a retry succeeds after close.
func (s *Stream) Append(
	ctx chasm.MutableContext,
	p Producer,
	seq int64,
	records []*streamservicepb.RecordInput,
	finish bool,
	closing *streamservicepb.StreamClose,
) ([]int64, error) {
	hash, err := contentHash(records, finish, closing)
	if err != nil {
		return nil, err
	}
	if field, ok := s.Producers[p.ID]; ok {
		st := field.Get(ctx)
		if p.before(st) {
			return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_EPOCH_FENCED, "producer %s has appended at a higher epoch", p.ID)
		}
		if p.sameEpoch(st) {
			switch {
			case seq == st.Seq && bytes.Equal(hash, st.ContentHash):
				return offsetRange(st.FirstOffset, st.Count), nil
			case seq == st.Seq:
				return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_SEQ_CONFLICT, "seq %d is stored with different content", seq)
			case seq < st.Seq:
				return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_SEQ_SUPERSEDED, "seq %d is lower than the latest, %d", seq, st.Seq)
			}
		}
	}
	if s.Close != nil {
		return nil, newError(streamservicepb.STREAM_FAILURE_TYPE_CLOSED, "stream is closed")
	}
	identity := func() *streamservicepb.ProducerIdentity {
		return &streamservicepb.ProducerIdentity{Producer: p.ID, WorkflowTask: p.WorkflowTask, Epoch: p.Epoch, Seq: seq}
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
		WorkflowTask: p.WorkflowTask,
		Epoch:        p.Epoch,
		Seq:          seq,
		FirstOffset:  first,
		Count:        int64(len(records)),
		ContentHash:  hash,
	})
	return offsetRange(first, int64(len(records))), nil
}

// RecordCommit records that attempt committed workflow task workflowTask of producer, if any
// attempt of that task appended.
func (s *Stream) RecordCommit(ctx chasm.MutableContext, producer string, workflowTask, attempt int64) error {
	field, ok := s.Producers[producer]
	if !ok || field.Get(ctx).WorkflowTask != workflowTask {
		return nil
	}
	_, err := s.add(ctx, &streamservicepb.Record{WorkflowTaskCommit: &streamservicepb.WorkflowTaskCommit{
		Producer:     producer,
		WorkflowTask: workflowTask,
		Attempt:      attempt,
	}})
	return err
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
// topics. Workflow task commit markers match every filter.
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
	return len(topics) == 0 || r.WorkflowTaskCommit != nil || slices.Contains(topics, r.Topic)
}

func offsetRange(first, count int64) []int64 {
	offsets := make([]int64, count)
	for i := range offsets {
		offsets[i] = first + int64(i)
	}
	return offsets
}

// contentHash hashes an append's content as encoded by the caller, so it reveals nothing about
// encrypted payloads.
func contentHash(records []*streamservicepb.RecordInput, finish bool, closing *streamservicepb.StreamClose) ([]byte, error) {
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
