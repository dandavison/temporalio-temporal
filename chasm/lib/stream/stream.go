package stream

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"

	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	"go.temporal.io/server/chasm"
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

// Stream is a log of records embedded in its owner. Offsets are contiguous from 0. It applies
// dedupe and fencing by (producer, epoch, seq); the owner checks that the producer is who it says
// it is.
type Stream struct {
	chasm.UnimplementedComponent

	*streampb.StreamState

	Records   chasm.Map[int64, *streamservicepb.Record]
	Producers chasm.Map[string, *streampb.ProducerState]
}

func New() *Stream {
	return &Stream{
		StreamState: &streampb.StreamState{},
		Records:     make(chasm.Map[int64, *streamservicepb.Record]),
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
	first := s.NextOffset
	for _, r := range records {
		s.add(ctx, &streamservicepb.Record{Producer: identity(), Topic: r.GetTopic(), Payload: r.GetPayload()})
	}
	if finish {
		s.add(ctx, &streamservicepb.Record{Producer: identity(), Finish: true})
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
func (s *Stream) RecordCommit(ctx chasm.MutableContext, producer string, workflowTask, attempt int64) {
	field, ok := s.Producers[producer]
	if !ok || field.Get(ctx).WorkflowTask != workflowTask {
		return
	}
	s.add(ctx, &streamservicepb.Record{WorkflowTaskCommit: &streamservicepb.WorkflowTaskCommit{
		Producer:     producer,
		WorkflowTask: workflowTask,
		Attempt:      attempt,
	}})
}

// CloseIfOpen closes the stream unless it is already closed.
func (s *Stream) CloseIfOpen(c *streamservicepb.StreamClose) {
	if s.Close == nil {
		s.Close = c
	}
}

func (s *Stream) add(ctx chasm.MutableContext, r *streamservicepb.Record) {
	s.Records[s.NextOffset] = chasm.NewDataField(ctx, r)
	s.NextOffset++
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
func (s *Stream) Read(ctx chasm.Context, after int64, topics []string, maxRecords int) ReadResult {
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	result := ReadResult{Position: after}
	for offset := after + 1; offset < s.NextOffset && len(result.Records) < maxRecords; offset++ {
		r := s.Records[offset].Get(ctx)
		result.Position = offset
		if matches(r, topics) {
			result.Records = append(result.Records, OffsetRecord{Offset: offset, Record: r})
		}
	}
	result.Closed = s.Close != nil && result.Position == s.NextOffset-1
	return result
}

// ReadLast returns the last n records that match topics.
func (s *Stream) ReadLast(ctx chasm.Context, n int, topics []string) ReadResult {
	result := ReadResult{Position: s.NextOffset - 1}
	for offset := s.NextOffset - 1; offset >= 0 && len(result.Records) < n; offset-- {
		if r := s.Records[offset].Get(ctx); matches(r, topics) {
			result.Records = append(result.Records, OffsetRecord{Offset: offset, Record: r})
		}
	}
	slices.Reverse(result.Records)
	result.Closed = s.Close != nil
	return result
}

// HasNews reports whether a read after offset after would return a record or the close.
func (s *Stream) HasNews(ctx chasm.Context, after int64, topics []string) bool {
	if s.Close != nil {
		return true
	}
	for offset := after + 1; offset < s.NextOffset; offset++ {
		if matches(s.Records[offset].Get(ctx), topics) {
			return true
		}
	}
	return false
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
