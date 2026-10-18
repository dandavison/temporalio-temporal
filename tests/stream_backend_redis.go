package tests

// redisStreamBackend stores streams in Redis, as a customer's worker or client would. The server
// never talks to Redis. It mirrors the Redis backend of temporalio-streams (the sdk-streams
// submodule) and runs the same script, stream_append.lua, a copy of that crate's append.lua.
//
// A stream is keyed by its owner: a workflow run chain, by workflow ID and first run ID, or a
// Standalone Activity execution, by activity ID and run ID. It is four keys sharing a hash tag with
// the owner's kind and ID: a Redis stream holding the log, a hash holding each producer's dedupe and
// fencing state, a metadata hash holding existence and close, and a tombstone. A fifth key holds
// the owner ID's latest run, for readers that do not know the run ID. Every append and close goes
// through the script, which checks and writes in one step. Producers that XADD to the log directly
// are not supported.
//
// Properties weaker than native, all because Redis cannot see the server's state (the same on
// PostgreSQL, except the last):
//   - A stale producer is fenced only once its successor has appended.
//   - Nothing writes workflow task commit records, so a failed workflow task attempt's records,
//     and its local activities', are not withdrawn.
//   - Producers are not verified, and appends from speculative workflow tasks are not rejected.
//   - The owner's worker writes a workflow's close. The stream is left open when the workflow is
//     terminated or times out, when a failed run with a retry policy is not retried, or when the
//     worker stops before writing the close.
//   - Nothing closes an activity's stream when the activity ends; the activity must close it
//     itself. A worker cannot tell when its activity has ended: Core sends the Respond RPC and
//     discards its result, so the worker does not learn whether the server accepted the
//     completion, and RespondActivityTaskFailedResponse does not say whether a failed attempt
//     will be retried.
//   - A topic filter is applied after reading: the reader receives every record.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	_ "embed"
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

//go:embed stream_append.lua
var appendScript string

// redisStreamRetention is the time-to-live each write refreshes. The tests delete streams with
// deleteStream rather than waiting for it.
const redisStreamRetention = 7 * 24 * time.Hour

const redisChainPollInterval = 100 * time.Millisecond

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

// redisKeys are the keys of one owner run's stream, and the key holding the owner ID's latest run.
type redisKeys struct {
	prefix, meta, log, producers, tombstone, current, firstRun string
}

func (b *redisStreamBackend) ownerPrefix(owner string) string {
	return fmt.Sprintf("stream:{%s/%s}", b.namespace, owner)
}

func (b *redisStreamBackend) keys(owner, firstRun string) redisKeys {
	prefix := b.ownerPrefix(owner) + ":" + firstRun
	return redisKeys{
		prefix:    prefix,
		meta:      prefix + ":meta",
		log:       prefix + ":log",
		producers: prefix + ":producers",
		tombstone: prefix + ":tombstone",
		current:   b.ownerPrefix(owner) + ":current",
		firstRun:  firstRun,
	}
}

// writeKeys are the keys an append or close writes, which needs the owner's run ID.
func (b *redisStreamBackend) writeKeys(ref streamRef) (redisKeys, error) {
	if externalRun(ref) == "" {
		return redisKeys{}, errors.New("appends to a Redis stream need the run ID of its owner: a workflow's first run ID, or the activity's run ID")
	}
	return b.keys(externalOwner(ref), externalRun(ref)), nil
}

// externalOwner names a stream's owner on an external store, so that a workflow and an activity
// with the same ID have different streams.
func externalOwner(ref streamRef) string {
	if ref.ActivityID != "" {
		return "activity/" + ref.ActivityID
	}
	return "workflow/" + ref.WorkflowID
}

// externalRun is the run ID that pins a stream: a workflow's first run ID, or the activity's run ID.
func externalRun(ref streamRef) string {
	return ref.FirstRunID + ref.ActivityRunID
}

// isExternalOwner reports whether p is the stream's owner: a workflow task of the owning workflow,
// or an attempt of the owning Standalone Activity. External stores cannot verify it.
func isExternalOwner(ref streamRef, p streamProducer) bool {
	if ref.ActivityID != "" {
		return p.ActivityTask.GetActivityRunId() != "" && p.ActivityTask.GetActivityId() == ref.ActivityID
	}
	return p.WorkflowTask.GetWorkflowExecution().GetWorkflowId() == ref.WorkflowID
}

func (b *redisStreamBackend) Append(ctx context.Context, ref streamRef, req streamAppendRequest) (streamAppendResponse, error) {
	if err := checkStreamAppendSize(req.Records); err != nil {
		return streamAppendResponse{}, err
	}
	id, err := externalProducer(req.Producer)
	if err != nil {
		return streamAppendResponse{}, err
	}
	k, err := b.writeKeys(ref)
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
	ids, err := b.runAppend(ctx, k, id, req.Seq, entries, false)
	if err != nil {
		return streamAppendResponse{}, err
	}
	resp := streamAppendResponse{}
	for _, entryID := range ids[:len(req.Records)] {
		resp.Cursors = append(resp.Cursors, k.cursor(entryID))
	}
	return resp, nil
}

func (b *redisStreamBackend) Close(ctx context.Context, ref streamRef, req streamCloseRequest) error {
	id, err := externalProducer(req.Owner)
	if err != nil {
		return err
	}
	if !isExternalOwner(ref, req.Owner) {
		return errStreamNotOwner
	}
	k, err := b.writeKeys(ref)
	if err != nil {
		return err
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
	_, err = b.runAppend(ctx, k, id, req.Seq, [][]string{entry}, true)
	return err
}

func (b *redisStreamBackend) runAppend(ctx context.Context, k redisKeys, id externalProducerIdentity, seq int64, entries [][]string, closing bool) ([]string, error) {
	args := []any{
		k.firstRun, id.producer, id.runID, id.workflowTask, id.workflowTaskAttempt, id.epoch, seq,
		appendDigest(entries), boolArg(closing), redisStreamRetention.Milliseconds(), len(entries),
	}
	for _, e := range entries {
		args = append(args, len(e)/2)
		for _, f := range e {
			args = append(args, f)
		}
	}
	res, err := b.append.Run(ctx, b.rdb, []string{k.meta, k.log, k.producers, k.tombstone, k.current}, args...).StringSlice()
	if err != nil {
		return nil, redisError(err)
	}
	return res, nil
}

func (b *redisStreamBackend) Read(ctx context.Context, ref streamRef, req streamReadRequest) (streamReadResponse, error) {
	deadline := time.Now().Add(req.Wait)
	var k redisKeys
	lastID := "0-0"
	if req.From.after == nil {
		var ok bool
		var err error
		if k, ok, err = b.resolveChain(ctx, ref, deadline); err != nil || !ok {
			return streamReadResponse{}, err
		}
	} else {
		firstRun, entryID, err := b.parseCursor(externalOwner(ref), req.From.after)
		if err != nil {
			return streamReadResponse{}, err
		}
		if run := externalRun(ref); run != "" && run != firstRun {
			return streamReadResponse{}, errStreamCursor
		}
		k, lastID = b.keys(externalOwner(ref), firstRun), entryID
	}
	exists, err := b.exists(ctx, k.meta)
	if err != nil {
		return streamReadResponse{}, err
	}
	if !exists {
		if tombstone, err := b.exists(ctx, k.tombstone); err != nil || tombstone {
			return streamReadResponse{}, cmp.Or(err, errStreamExpired)
		}
		if req.From.after != nil {
			return streamReadResponse{}, errStreamNotFound
		}
		if ok, err := b.awaitChainRecords(ctx, k, deadline); err != nil || !ok {
			return streamReadResponse{}, err
		}
	}
	if req.Last > 0 {
		return b.readTail(ctx, k, req)
	}
	return b.readAfter(ctx, k, req, lastID, deadline)
}

// resolveChain returns the chain a read from the beginning reads: the pinned one, or the owner ID's
// latest, waiting until the deadline for one to exist.
func (b *redisStreamBackend) resolveChain(ctx context.Context, ref streamRef, deadline time.Time) (redisKeys, bool, error) {
	if run := externalRun(ref); run != "" {
		return b.keys(externalOwner(ref), run), true, nil
	}
	current := b.ownerPrefix(externalOwner(ref)) + ":current"
	for {
		firstRun, err := b.rdb.Get(ctx, current).Result()
		if err == nil {
			return b.keys(externalOwner(ref), firstRun), true, nil
		}
		if !errors.Is(err, redis.Nil) {
			return redisKeys{}, false, err
		}
		if time.Until(deadline) < redisChainPollInterval {
			return redisKeys{}, false, nil
		}
		if err := sleepCtx(ctx, redisChainPollInterval); err != nil {
			return redisKeys{}, false, err
		}
	}
}

// awaitChainRecords waits until the deadline for a chain with no records yet to get its first.
func (b *redisStreamBackend) awaitChainRecords(ctx context.Context, k redisKeys, deadline time.Time) (bool, error) {
	for {
		if exists, err := b.exists(ctx, k.meta); err != nil || exists {
			return exists, err
		}
		if time.Until(deadline) < redisChainPollInterval {
			return false, nil
		}
		if err := sleepCtx(ctx, redisChainPollInterval); err != nil {
			return false, err
		}
	}
}

func (b *redisStreamBackend) readAfter(ctx context.Context, k redisKeys, req streamReadRequest, lastID string, deadline time.Time) (streamReadResponse, error) {
	maxRecords := cmp.Or(req.MaxRecords, redisDefaultMaxRecords)
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
			resp.Cursor = k.cursor(e.ID)
			if r, ok := redisRecord(k.cursor(e.ID), e.Values, req.Topics); ok {
				resp.Records = append(resp.Records, r)
			}
		}
		if len(resp.Records) > 0 || time.Until(deadline) <= 0 || b.closedAt(ctx, k, lastID) {
			break
		}
	}
	if b.closedAt(ctx, k, lastID) {
		var err error
		if resp.Close, err = b.closeFromMeta(ctx, k); err != nil {
			return streamReadResponse{}, err
		}
	}
	return resp, nil
}

// sleepCtx waits for d, as the Redis backend of temporalio-streams does between polls.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *redisStreamBackend) exists(ctx context.Context, key string) (bool, error) {
	n, err := b.rdb.Exists(ctx, key).Result()
	return n == 1, err
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
func (b *redisStreamBackend) readTail(ctx context.Context, k redisKeys, req streamReadRequest) (streamReadResponse, error) {
	entries, err := b.rdb.XRevRange(ctx, k.log, "+", "-").Result()
	if err != nil {
		return streamReadResponse{}, err
	}
	resp := streamReadResponse{}
	if len(entries) > 0 {
		resp.Cursor = k.cursor(entries[0].ID)
	}
	for _, e := range entries {
		if len(resp.Records) == req.Last {
			break
		}
		if r, ok := redisRecord(k.cursor(e.ID), e.Values, req.Topics); ok {
			resp.Records = append([]streamRecord{r}, resp.Records...)
		}
	}
	return resp, nil
}

// deleteStream deletes a stream as its retention would: the tombstone outlives the data.
func (b *redisStreamBackend) deleteStream(ctx context.Context, ref streamRef) error {
	k, err := b.writeKeys(ref)
	if err != nil {
		return err
	}
	return b.rdb.Del(ctx, k.meta, k.log, k.producers).Err()
}

// A cursor is the chain's key prefix and a Redis entry ID.
func (k redisKeys) cursor(entryID string) streamCursor {
	return streamCursor(k.prefix + "#" + entryID)
}

// parseCursor splits a cursor into its chain's first run ID and its entry ID.
func (b *redisStreamBackend) parseCursor(owner string, c streamCursor) (firstRun, entryID string, err error) {
	chain, entryID, ok := strings.Cut(string(c), "#")
	if !ok {
		return "", "", errStreamCursor
	}
	firstRun, ok = strings.CutPrefix(chain, b.ownerPrefix(owner)+":")
	if !ok {
		return "", "", errStreamCursor
	}
	return firstRun, entryID, nil
}

// externalProducerIdentity is a producer's stored identity on an external store, the same on Redis
// and PostgreSQL.
type externalProducerIdentity struct {
	producer            string
	runID               string
	workflowTask        int64
	workflowTaskAttempt int32
	epoch               int64
}

func externalProducer(p streamProducer) (externalProducerIdentity, error) {
	switch {
	case p.ClientID != "":
		return externalProducerIdentity{producer: "client/" + p.ClientID, epoch: p.ClientEpoch}, nil
	case p.ActivityTask != nil:
		t := p.ActivityTask
		producer := "activity/" + t.ActivityRunId
		if t.WorkflowExecution != nil {
			producer = "activity/" + t.WorkflowExecution.RunId + "/" + t.ActivityId
		}
		return externalProducerIdentity{producer: producer, epoch: int64(t.AttemptStamp)}, nil
	case p.WorkflowTask != nil:
		t := p.WorkflowTask
		return externalProducerIdentity{
			producer:            "workflow/" + t.WorkflowExecution.RunId,
			runID:               t.WorkflowExecution.RunId,
			workflowTask:        t.PreviousStartedEventId,
			workflowTaskAttempt: t.Attempt,
			epoch:               int64(t.Attempt),
		}, nil
	case p.LocalActivity != nil:
		la := p.LocalActivity
		runID := la.Task.WorkflowExecution.RunId
		return externalProducerIdentity{
			producer:            "local-activity/" + runID + "/" + la.ActivityID,
			runID:               runID,
			workflowTask:        la.Task.PreviousStartedEventId,
			workflowTaskAttempt: la.Task.Attempt,
			epoch:               int64(la.Attempt),
		}, nil
	default:
		return externalProducerIdentity{}, errors.New("stream producer has no identity")
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
			Producer:            v["producer"].(string),
			WorkflowRunID:       v["run"].(string),
			WorkflowTask:        mustInt(v["wt"]),
			WorkflowTaskAttempt: int32(mustInt(v["wta"])),
			Epoch:               mustInt(v["epoch"]),
			Seq:                 mustInt(v["seq"]),
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

// redisError maps a failure the script reports, identified by its first word, to its sentinel.
func redisError(err error) error {
	code, _, _ := strings.Cut(err.Error(), " ")
	sentinel, ok := map[string]error{
		"INVALID_ARGUMENT": errStreamInvalidArg,
		"EXPIRED":          errStreamExpired,
		"EPOCH_FENCED":     errStreamEpochFenced,
		"SEQ_DIVERGENT":    errStreamSeqDivergent,
		"SEQ_STALE":        errStreamSeqStale,
		"SEQ_GAP":          errStreamSeqGap,
		"CLOSED":           errStreamClosed,
	}[code]
	if !ok {
		return err
	}
	return fmt.Errorf("%w: %s", sentinel, err)
}

// checkStreamAppendSize applies the size limits, as the shared SDK code does before a Redis append.
func checkStreamAppendSize(records []streamAppendRecord) error {
	if len(records) > streamMaxRecordsPerAppend {
		return errStreamTooLarge
	}
	total := 0
	for _, r := range records {
		n := proto.Size(r.Payload)
		if n > streamMaxRecordBytes {
			return errStreamTooLarge
		}
		total += n
	}
	if total > streamMaxAppendBytes {
		return errStreamTooLarge
	}
	return nil
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
