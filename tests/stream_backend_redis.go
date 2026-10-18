package tests

// redisStreamBackend stores streams in Redis, as a customer's worker or client would. The server
// never talks to Redis.
//
// A stream is three keys sharing a hash tag: a Redis stream holding the log, a hash holding each
// producer's dedupe and fencing state, and a metadata hash holding existence, close and deletion.
// Every append and close goes through appendScript, which checks and writes in one step. Producers
// that XADD to the log directly are not supported.
//
// Properties weaker than native, all because Redis cannot see the server's state:
//   - A stale producer is fenced only once its successor has appended.
//   - Nothing records which attempt of a workflow task committed, so a failed attempt's records are
//     not withdrawn if the attempt that succeeds appends nothing.
//   - Appends from speculative workflow tasks are not rejected.
//   - The stream is keyed by workflow ID, not by run chain, so a closed stream blocks a new chain
//     with the same workflow ID from appending.
//   - When the owner closes, its worker must write the close; after a worker crash it can be late
//     or missing.
//   - A topic filter is applied after reading: the reader receives every record.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
)

// appendScript appends entries for one (producer, epoch, seq), applying dedupe, fencing and close.
//
// KEYS: meta, log, producers.
// ARGV: producer, workflow task, epoch, seq, digest, closing ("1" or "0"), entry count, then
// for each entry a field count followed by that many field/value pairs.
const appendScript = `
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local producer, wt, epoch, seq, digest = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]
local closing = ARGV[6] == '1'
if redis.call('HGET', meta, 'deleted') == '1' then
  return redis.error_reply('EXPIRED')
end
local raw = redis.call('HGET', producers, producer)
if raw then
  local st = cjson.decode(raw)
  if wt < st.wt or (wt == st.wt and epoch < st.epoch) then
    return redis.error_reply('FENCED')
  end
  if wt == st.wt and epoch == st.epoch then
    if seq == st.seq then
      if digest == st.digest then
        return st.ids
      end
      return redis.error_reply('SEQ_DIVERGENT')
    end
    if seq < st.seq then
      return redis.error_reply('SEQ_STALE')
    end
  end
end
if redis.call('HGET', meta, 'closed') == '1' then
  return redis.error_reply('CLOSED')
end
local ids = {}
local i = 8
for _ = 1, tonumber(ARGV[7]) do
  local nfields = tonumber(ARGV[i])
  i = i + 1
  local args = {'XADD', log, '*', 'producer', producer, 'wt', wt, 'epoch', epoch, 'seq', seq}
  for j = 0, 2 * nfields - 1 do
    args[#args + 1] = ARGV[i + j]
    if closing and j % 2 == 0 then
      redis.call('HSET', meta, 'close_' .. ARGV[i + j], ARGV[i + j + 1])
    end
  end
  i = i + 2 * nfields
  ids[#ids + 1] = redis.call(unpack(args))
end
redis.call('HSET', producers, producer, cjson.encode({wt = wt, epoch = epoch, seq = seq, digest = digest, ids = ids}))
redis.call('HSET', meta, 'exists', '1')
if closing then
  redis.call('HSET', meta, 'closed', '1')
end
return ids
`

const redisDefaultMaxRecords = 1000

type redisStreamBackend struct {
	rdb       *redis.Client
	namespace string
	append    *redis.Script
}

func newRedisStreamBackend(t *testing.T, env *testcore.TestEnv) streamBackendUnderTest {
	b := &redisStreamBackend{rdb: startRedis(t), namespace: env.Namespace().String(), append: redis.NewScript(appendScript)}
	return streamBackendUnderTest{
		name:          "Redis",
		streamBackend: b,
		deleteStream:  b.deleteStream,
	}
}

// redisKeys are the keys of one stream.
type redisKeys struct {
	prefix, meta, log, producers string
}

func (b *redisStreamBackend) keys(ref streamRef) redisKeys {
	prefix := fmt.Sprintf("stream:{%s/%s}", b.namespace, ref.WorkflowID)
	return redisKeys{prefix: prefix, meta: prefix + ":meta", log: prefix + ":log", producers: prefix + ":producers"}
}

func (b *redisStreamBackend) Append(ctx context.Context, ref streamRef, req streamAppendRequest) (streamAppendResponse, error) {
	id, err := redisProducer(req.Producer)
	if err != nil {
		return streamAppendResponse{}, err
	}
	var entries [][]string
	for _, r := range req.Records {
		data, err := proto.Marshal(r.Payload)
		if err != nil {
			return streamAppendResponse{}, err
		}
		entries = append(entries, []string{"kind", "data", "topic", r.Topic, "payload", string(data)})
	}
	if req.Finish {
		entries = append(entries, []string{"kind", "finish"})
	}
	ids, err := b.runAppend(ctx, ref, id, req.Seq, entries, false)
	if err != nil {
		return streamAppendResponse{}, err
	}
	resp := streamAppendResponse{}
	for _, entryID := range ids[:len(req.Records)] {
		resp.Cursors = append(resp.Cursors, b.cursor(ref, entryID))
	}
	return resp, nil
}

func (b *redisStreamBackend) Close(ctx context.Context, ref streamRef, req streamCloseRequest) error {
	id, err := redisProducer(req.Owner)
	if err != nil {
		return err
	}
	if req.Owner.WorkflowTask.GetWorkflowExecution().GetWorkflowId() != ref.WorkflowID {
		return errStreamNotOwner
	}
	failure := ""
	if req.Close.Failure != nil {
		data, err := proto.Marshal(req.Close.Failure)
		if err != nil {
			return err
		}
		failure = string(data)
	}
	entry := []string{"kind", "close", "status", strconv.Itoa(int(req.Close.Status)), "failure", failure, "reason", req.Close.Reason}
	_, err = b.runAppend(ctx, ref, id, req.Seq, [][]string{entry}, true)
	return err
}

func (b *redisStreamBackend) runAppend(ctx context.Context, ref streamRef, id redisProducerIdentity, seq int64, entries [][]string, closing bool) ([]string, error) {
	k := b.keys(ref)
	args := []any{id.producer, id.workflowTask, id.epoch, seq, appendDigest(entries), boolArg(closing), len(entries)}
	for _, e := range entries {
		args = append(args, len(e)/2)
		for _, f := range e {
			args = append(args, f)
		}
	}
	res, err := b.append.Run(ctx, b.rdb, []string{k.meta, k.log, k.producers}, args...).StringSlice()
	if err != nil {
		return nil, redisError(err)
	}
	return res, nil
}

func (b *redisStreamBackend) Read(ctx context.Context, ref streamRef, req streamReadRequest) (streamReadResponse, error) {
	k := b.keys(ref)
	meta, err := b.rdb.HGetAll(ctx, k.meta).Result()
	if err != nil {
		return streamReadResponse{}, err
	}
	if meta["deleted"] == "1" {
		return streamReadResponse{}, errStreamExpired
	}
	lastID := "0-0"
	if req.From.after != nil {
		if lastID, err = b.entryID(ref, req.From.after); err != nil {
			return streamReadResponse{}, err
		}
		if meta["exists"] != "1" {
			return streamReadResponse{}, errStreamNotFound
		}
	}
	if req.Last > 0 {
		return b.readTail(ctx, ref, req)
	}
	maxRecords := cmp.Or(req.MaxRecords, redisDefaultMaxRecords)
	deadline := time.Now().Add(req.Wait)
	resp := streamReadResponse{Cursor: req.From.after}
	for {
		entries, err := b.rdb.XRangeN(ctx, k.log, "("+lastID, "+", int64(maxRecords)).Result()
		if err != nil {
			return streamReadResponse{}, err
		}
		if len(entries) == 0 && time.Until(deadline) > 0 {
			entries, err = b.block(ctx, k.log, lastID, time.Until(deadline))
			if err != nil {
				return streamReadResponse{}, err
			}
		}
		for _, e := range entries {
			lastID = e.ID
			resp.Cursor = b.cursor(ref, e.ID)
			if e.Values["kind"] == "close" {
				break
			}
			if r, ok := redisRecord(b.cursor(ref, e.ID), e.Values, req.Topics); ok {
				resp.Records = append(resp.Records, r)
			}
		}
		if len(resp.Records) > 0 || time.Until(deadline) <= 0 || b.closedAt(ctx, k, lastID) {
			break
		}
	}
	if b.closedAt(ctx, k, lastID) {
		if resp.Close, err = b.closeFromMeta(ctx, k); err != nil {
			return streamReadResponse{}, err
		}
	}
	return resp, nil
}

// block waits for entries after lastID, returning none if the wait ends first.
func (b *redisStreamBackend) block(ctx context.Context, log, lastID string, wait time.Duration) ([]redis.XMessage, error) {
	streams, err := b.rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{log, lastID}, Block: wait}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return streams[0].Messages, nil
}

// closedAt reports whether the stream is closed and the reader has read every entry up to lastID,
// which includes the close entry, the stream's last.
func (b *redisStreamBackend) closedAt(ctx context.Context, k redisKeys, lastID string) bool {
	if b.rdb.HGet(ctx, k.meta, "closed").Val() != "1" {
		return false
	}
	return len(b.rdb.XRangeN(ctx, k.log, "("+lastID, "+", 1).Val()) == 0
}

func (b *redisStreamBackend) closeFromMeta(ctx context.Context, k redisKeys) (*streamClose, error) {
	meta, err := b.rdb.HGetAll(ctx, k.meta).Result()
	if err != nil {
		return nil, err
	}
	status, err := strconv.Atoi(meta["close_status"])
	if err != nil {
		return nil, err
	}
	c := &streamClose{Status: streamCloseStatus(status), Reason: meta["close_reason"]}
	if f := meta["close_failure"]; f != "" {
		c.Failure = &failurepb.Failure{}
		if err := proto.Unmarshal([]byte(f), c.Failure); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// readTail returns the last req.Last matching records, scanning the whole log backwards.
func (b *redisStreamBackend) readTail(ctx context.Context, ref streamRef, req streamReadRequest) (streamReadResponse, error) {
	entries, err := b.rdb.XRevRange(ctx, b.keys(ref).log, "+", "-").Result()
	if err != nil {
		return streamReadResponse{}, err
	}
	resp := streamReadResponse{}
	if len(entries) > 0 {
		resp.Cursor = b.cursor(ref, entries[0].ID)
	}
	for _, e := range entries {
		if len(resp.Records) == req.Last {
			break
		}
		if r, ok := redisRecord(b.cursor(ref, e.ID), e.Values, req.Topics); ok {
			resp.Records = append([]streamRecord{r}, resp.Records...)
		}
	}
	return resp, nil
}

func (b *redisStreamBackend) deleteStream(ctx context.Context, ref streamRef) error {
	k := b.keys(ref)
	if err := b.rdb.Del(ctx, k.log, k.producers).Err(); err != nil {
		return err
	}
	return b.rdb.HSet(ctx, k.meta, "deleted", "1").Err()
}

// A cursor is the stream's key prefix and a Redis entry ID.
func (b *redisStreamBackend) cursor(ref streamRef, entryID string) streamCursor {
	return streamCursor(b.keys(ref).prefix + "#" + entryID)
}

func (b *redisStreamBackend) entryID(ref streamRef, c streamCursor) (string, error) {
	prefix, id, ok := strings.Cut(string(c), "#")
	if !ok || prefix != b.keys(ref).prefix {
		return "", errStreamCursor
	}
	return id, nil
}

type redisProducerIdentity struct {
	producer     string
	workflowTask int64
	epoch        int64
}

func redisProducer(p streamProducer) (redisProducerIdentity, error) {
	switch {
	case p.ClientID != "":
		return redisProducerIdentity{producer: "client/" + p.ClientID, epoch: p.ClientEpoch}, nil
	case p.ActivityTask != nil:
		t := p.ActivityTask
		producer := "activity/" + t.ActivityRunId
		if t.WorkflowExecution != nil {
			producer = "activity/" + t.WorkflowExecution.RunId + "/" + t.ActivityId
		}
		return redisProducerIdentity{producer: producer, epoch: int64(t.AttemptStamp)}, nil
	case p.WorkflowTask != nil:
		t := p.WorkflowTask
		return redisProducerIdentity{
			producer:     "workflow/" + t.WorkflowExecution.RunId,
			workflowTask: t.PreviousStartedEventId,
			epoch:        int64(t.Attempt),
		}, nil
	default:
		return redisProducerIdentity{}, errors.New("stream producer has no identity")
	}
}

func redisRecord(cursor streamCursor, v map[string]any, topics []string) (streamRecord, bool) {
	kind := v["kind"].(string)
	topic, _ := v["topic"].(string)
	if kind == "close" || (len(topics) > 0 && !slices.Contains(topics, topic)) {
		return streamRecord{}, false
	}
	r := streamRecord{
		Cursor: cursor,
		Producer: streamProducerIdentity{
			Producer:     v["producer"].(string),
			WorkflowTask: mustInt(v["wt"]),
			Epoch:        mustInt(v["epoch"]),
			Seq:          mustInt(v["seq"]),
		},
		Topic:  topic,
		Finish: kind == "finish",
	}
	if kind == "data" {
		r.Payload = &commonpb.Payload{}
		if err := proto.Unmarshal([]byte(v["payload"].(string)), r.Payload); err != nil {
			panic(err)
		}
	}
	return r, true
}

func redisError(err error) error {
	for code, sentinel := range map[string]error{
		"EXPIRED":       errStreamExpired,
		"FENCED":        errStreamEpochFenced,
		"SEQ_DIVERGENT": errStreamSeqDivergent,
		"SEQ_STALE":     errStreamSeqStale,
		"CLOSED":        errStreamClosed,
	} {
		if err.Error() == "ERR "+code {
			return sentinel
		}
	}
	return err
}

// appendDigest identifies an append's content, so that a retry with different content is detected.
func appendDigest(entries [][]string) string {
	var buf bytes.Buffer
	for _, e := range entries {
		for _, f := range e {
			_ = binary.Write(&buf, binary.BigEndian, int64(len(f)))
			buf.WriteString(f)
		}
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:])
}

func startRedis(t *testing.T) *redis.Client {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	require.NoError(t, l.Close())
	cmd := exec.Command("redis-server", "--port", port, "--save", "", "--appendonly", "no")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})
	t.Cleanup(func() { _ = rdb.Close() })
	await.Require(t.Context(), t, func(t *await.T) {
		require.NoError(t, rdb.Ping(t.Context()).Err())
	}, 10*time.Second, 50*time.Millisecond)
	return rdb
}

func boolArg(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func mustInt(v any) int64 {
	n, err := strconv.ParseInt(v.(string), 10, 64)
	if err != nil {
		panic(err)
	}
	return n
}
