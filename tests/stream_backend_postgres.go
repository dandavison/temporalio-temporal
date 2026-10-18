package tests

// postgresStreamBackend stores streams in PostgreSQL, as a customer's worker or client would. The
// server never talks to PostgreSQL. It is a check that the stream interface is not shaped by Redis,
// not a product feature.
//
// A stream is keyed by its owner, as on Redis: a workflow run chain or a Standalone Activity
// execution. It is a row in streams, holding its next position and its close, with its records in
// records and each producer's dedupe and fencing state in producers. chains holds each owner ID's
// latest run. Positions are dense: each append takes the next ones under the stream row's lock. An
// append is one transaction, which notifies the channel streamChannel. PostgreSQL has no blocking
// read, so a waiting read listens on that channel, then reads, then waits for a notification or its
// deadline. Every stream notifies the same channel, so a waiting read also wakes, and reads again,
// on appends to other streams.
//
// Properties weaker than native, all because PostgreSQL cannot see the server's state:
//   - A stale producer is fenced only once its successor has appended.
//   - Nothing writes workflow task commit records, so a failed workflow task attempt's records,
//     and its local activities', are not withdrawn.
//   - Producers are not verified, and appends from speculative workflow tasks are not rejected.
//   - The owner's worker writes a workflow's close, so the close can be missing, as on Redis.
//   - Nothing closes an activity's stream when the activity ends, as on Redis.
//
// Unlike Redis, PostgreSQL filters topics in the query, so a reader receives only matching
// records, and a filtered wait returns only on a matching record.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
)

const streamChannel = "stream_appended"

const postgresStreamSchema = `
CREATE TABLE streams (
  owner_id text NOT NULL,
  owner_run_id text NOT NULL,
  next_pos bigint NOT NULL DEFAULT 1,
  closed boolean NOT NULL DEFAULT false,
  close_status int NOT NULL DEFAULT 0,
  close_failure bytea,
  close_reason text NOT NULL DEFAULT '',
  deleted boolean NOT NULL DEFAULT false,
  PRIMARY KEY (owner_id, owner_run_id)
);
CREATE TABLE chains (
  owner_id text PRIMARY KEY,
  owner_run_id text NOT NULL
);
CREATE TABLE records (
  owner_id text NOT NULL,
  owner_run_id text NOT NULL,
  pos bigint NOT NULL,
  kind text NOT NULL,
  topic text NOT NULL,
  payload bytea,
  producer text NOT NULL,
  run_id text NOT NULL,
  workflow_task bigint NOT NULL,
  workflow_task_attempt int NOT NULL,
  epoch bigint NOT NULL,
  seq bigint NOT NULL,
  PRIMARY KEY (owner_id, owner_run_id, pos)
);
CREATE TABLE producers (
  owner_id text NOT NULL,
  owner_run_id text NOT NULL,
  producer text NOT NULL,
  workflow_task bigint NOT NULL,
  workflow_task_attempt int NOT NULL,
  epoch bigint NOT NULL,
  seq bigint NOT NULL,
  digest text NOT NULL,
  first_pos bigint NOT NULL,
  data_count int NOT NULL,
  PRIMARY KEY (owner_id, owner_run_id, producer)
);
`

const postgresDefaultMaxRecords = 1000

type postgresStreamBackend struct {
	pool *pgxpool.Pool
}

func newPostgresStreamBackend(t *testing.T, _ *testcore.TestEnv) streamBackendUnderTest {
	b := &postgresStreamBackend{pool: startPostgres(t)}
	_, err := b.pool.Exec(t.Context(), postgresStreamSchema)
	require.NoError(t, err)
	return streamBackendUnderTest{
		name:          "PostgreSQL",
		streamBackend: b,
		deleteStream:  b.deleteStream,
	}
}

type postgresChain struct {
	owner, run string
}

func (b *postgresStreamBackend) Append(ctx context.Context, ref streamRef, req streamAppendRequest) (streamAppendResponse, error) {
	if err := checkStreamAppendSize(req.Records); err != nil {
		return streamAppendResponse{}, err
	}
	id, err := externalProducer(req.Producer)
	if err != nil {
		return streamAppendResponse{}, err
	}
	chain, err := postgresWriteChain(ref)
	if err != nil {
		return streamAppendResponse{}, err
	}
	var entries []postgresEntry
	for _, r := range req.Records {
		data, err := proto.Marshal(r.Payload)
		if err != nil {
			return streamAppendResponse{}, err
		}
		entries = append(entries, postgresEntry{kind: "data", topic: r.Topic, payload: data})
	}
	if req.Finish {
		entries = append(entries, postgresEntry{kind: "finish"})
	}
	first, err := b.runAppend(ctx, chain, id, req.Seq, entries, nil)
	if err != nil {
		return streamAppendResponse{}, err
	}
	resp := streamAppendResponse{}
	for i := range req.Records {
		resp.Cursors = append(resp.Cursors, chain.cursor(first+int64(i)))
	}
	return resp, nil
}

func (b *postgresStreamBackend) Close(ctx context.Context, ref streamRef, req streamCloseRequest) error {
	id, err := externalProducer(req.Owner)
	if err != nil {
		return err
	}
	if !isExternalOwner(ref, req.Owner) {
		return errStreamNotOwner
	}
	chain, err := postgresWriteChain(ref)
	if err != nil {
		return err
	}
	_, err = b.runAppend(ctx, chain, id, req.Seq, nil, &req.Close)
	return err
}

type postgresEntry struct {
	kind, topic string
	payload     []byte
}

// runAppend appends entries for one (producer, epoch, seq), or closes the stream, applying
// dedupe, fencing, seq order and close in one transaction. It returns the first entry's position.
func (b *postgresStreamBackend) runAppend(ctx context.Context, c postgresChain, id externalProducerIdentity, seq int64, entries []postgresEntry, closing *streamClose) (int64, error) {
	var first int64
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO streams (owner_id, owner_run_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, c.owner, c.run)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			if _, err := tx.Exec(ctx, `INSERT INTO chains VALUES ($1, $2) ON CONFLICT (owner_id) DO UPDATE SET owner_run_id = $2`, c.owner, c.run); err != nil {
				return err
			}
		}
		var nextPos int64
		var closed, deleted bool
		if err := tx.QueryRow(ctx, `SELECT next_pos, closed, deleted FROM streams WHERE owner_id = $1 AND owner_run_id = $2 FOR UPDATE`, c.owner, c.run).Scan(&nextPos, &closed, &deleted); err != nil {
			return err
		}
		if deleted {
			return errStreamExpired
		}
		digest := postgresDigest(entries, closing)
		stored, dedupedFirst, err := checkPostgresProducer(ctx, tx, c, id, seq, digest)
		if err != nil || stored {
			first = dedupedFirst
			return err
		}
		if closed {
			return errStreamClosed
		}
		first = nextPos
		for i, e := range entries {
			if _, err := tx.Exec(ctx, `INSERT INTO records VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				c.owner, c.run, nextPos+int64(i), e.kind, e.topic, e.payload,
				id.producer, id.runID, id.workflowTask, id.workflowTaskAttempt, id.epoch, seq); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO producers VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (owner_id, owner_run_id, producer) DO UPDATE SET
  workflow_task = $4, workflow_task_attempt = $5, epoch = $6, seq = $7, digest = $8, first_pos = $9, data_count = $10`,
			c.owner, c.run, id.producer, id.workflowTask, id.workflowTaskAttempt, id.epoch, seq, digest, nextPos, len(entries)); err != nil {
			return err
		}
		if closing != nil {
			var failure []byte
			if closing.Failure != nil {
				if failure, err = proto.Marshal(closing.Failure); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE streams SET closed = true, close_status = $3, close_failure = $4, close_reason = $5 WHERE owner_id = $1 AND owner_run_id = $2`,
				c.owner, c.run, int(closing.Status), failure, closing.Reason); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE streams SET next_pos = $3 WHERE owner_id = $1 AND owner_run_id = $2`, c.owner, c.run, nextPos+int64(len(entries))); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`, streamChannel, c.owner)
		return err
	})
	return first, err
}

// checkPostgresProducer applies dedupe, fencing and seq order. stored is true when the append is a
// retry of the producer's latest, which returns its first position.
func checkPostgresProducer(ctx context.Context, tx pgx.Tx, c postgresChain, id externalProducerIdentity, seq int64, digest string) (stored bool, first int64, err error) {
	var st externalProducerIdentity
	var stSeq, stFirst int64
	var stDigest string
	err = tx.QueryRow(ctx, `SELECT workflow_task, workflow_task_attempt, epoch, seq, digest, first_pos FROM producers WHERE owner_id = $1 AND owner_run_id = $2 AND producer = $3`,
		c.owner, c.run, id.producer).Scan(&st.workflowTask, &st.workflowTaskAttempt, &st.epoch, &stSeq, &stDigest, &stFirst)
	nextSeq := int64(0)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return false, 0, err
	default:
		switch cmpEpoch(id, st) {
		case -1:
			return false, 0, errStreamEpochFenced
		case 0:
			switch {
			case seq == stSeq && digest == stDigest:
				return true, stFirst, nil
			case seq == stSeq:
				return false, 0, errStreamSeqDivergent
			case seq < stSeq:
				return false, 0, errStreamSeqStale
			default:
				nextSeq = stSeq + 1
			}
		default:
			// A higher epoch starts at seq 0.
		}
	}
	if seq != nextSeq {
		return false, 0, errStreamSeqGap
	}
	return false, 0, nil
}

func cmpEpoch(a, b externalProducerIdentity) int {
	for _, d := range []int64{a.workflowTask - b.workflowTask, int64(a.workflowTaskAttempt - b.workflowTaskAttempt), a.epoch - b.epoch} {
		if d != 0 {
			if d < 0 {
				return -1
			}
			return 1
		}
	}
	return 0
}

func (b *postgresStreamBackend) Read(ctx context.Context, ref streamRef, req streamReadRequest) (streamReadResponse, error) {
	deadline := time.Now().Add(req.Wait)
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return streamReadResponse{}, err
	}
	defer conn.Release()
	// Listen before reading, so that an append committed after the read is not missed.
	if _, err := conn.Exec(ctx, "LISTEN "+streamChannel); err != nil {
		return streamReadResponse{}, err
	}
	defer func() { _, _ = conn.Exec(context.Background(), "UNLISTEN "+streamChannel) }()
	var chain postgresChain
	after := int64(0)
	if req.From.after == nil {
		var ok bool
		if chain, ok, err = b.resolveChain(ctx, conn, ref, deadline); err != nil || !ok {
			return streamReadResponse{}, err
		}
	} else {
		if chain, after, err = parsePostgresCursor(externalOwner(ref), req.From.after); err != nil {
			return streamReadResponse{}, err
		}
		if run := externalRun(ref); run != "" && run != chain.run {
			return streamReadResponse{}, errStreamCursor
		}
	}
	for {
		resp, found, err := b.readOnce(ctx, conn.Conn(), chain, req, after)
		if err != nil {
			return streamReadResponse{}, err
		}
		if !found && req.From.after != nil {
			return streamReadResponse{}, errStreamNotFound
		}
		if len(resp.Records) > 0 || resp.Close != nil || req.Last > 0 || time.Until(deadline) <= 0 {
			return resp, nil
		}
		if resp.Cursor != nil {
			_, after, _ = parsePostgresCursor(externalOwner(ref), resp.Cursor)
			req.From.after = resp.Cursor
		}
		if err := waitForNotification(ctx, conn.Conn(), deadline); err != nil {
			return streamReadResponse{}, err
		}
	}
}

// resolveChain returns the chain a read from the beginning reads: the pinned one, or the owner ID's
// latest, waiting until the deadline for one to exist.
func (b *postgresStreamBackend) resolveChain(ctx context.Context, conn *pgxpool.Conn, ref streamRef, deadline time.Time) (postgresChain, bool, error) {
	if run := externalRun(ref); run != "" {
		return postgresChain{externalOwner(ref), run}, true, nil
	}
	for {
		var firstRun string
		err := conn.QueryRow(ctx, `SELECT owner_run_id FROM chains WHERE owner_id = $1`, externalOwner(ref)).Scan(&firstRun)
		if err == nil {
			return postgresChain{externalOwner(ref), firstRun}, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return postgresChain{}, false, err
		}
		if time.Until(deadline) <= 0 {
			return postgresChain{}, false, nil
		}
		if err := waitForNotification(ctx, conn.Conn(), deadline); err != nil {
			return postgresChain{}, false, err
		}
	}
}

// readOnce reads matching records after a position in one snapshot. found is false if the stream
// does not exist.
func (b *postgresStreamBackend) readOnce(ctx context.Context, conn *pgx.Conn, c postgresChain, req streamReadRequest, after int64) (resp streamReadResponse, found bool, err error) {
	resp.Cursor = req.From.after
	err = pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var nextPos int64
		var closed, deleted bool
		var closeStatus int
		var closeFailure []byte
		var closeReason string
		err := tx.QueryRow(ctx, `SELECT next_pos, closed, deleted, close_status, close_failure, close_reason FROM streams WHERE owner_id = $1 AND owner_run_id = $2`,
			c.owner, c.run).Scan(&nextPos, &closed, &deleted, &closeStatus, &closeFailure, &closeReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if deleted {
			return errStreamExpired
		}
		found = true
		head := nextPos - 1
		limit := postgresDefaultMaxRecords
		if req.MaxRecords > 0 {
			limit = req.MaxRecords
		}
		query := `SELECT pos, kind, topic, payload, producer, run_id, workflow_task, workflow_task_attempt, epoch, seq FROM records
WHERE owner_id = $1 AND owner_run_id = $2 AND pos > $3 AND (coalesce(cardinality($4::text[]), 0) = 0 OR topic = ANY($4)) ORDER BY pos LIMIT $5`
		if req.Last > 0 {
			query = `SELECT pos, kind, topic, payload, producer, run_id, workflow_task, workflow_task_attempt, epoch, seq FROM records
WHERE owner_id = $1 AND owner_run_id = $2 AND pos > $3 AND (coalesce(cardinality($4::text[]), 0) = 0 OR topic = ANY($4)) ORDER BY pos DESC LIMIT $5`
			limit = req.Last
		}
		rows, err := tx.Query(ctx, query, c.owner, c.run, after, req.Topics, limit)
		if err != nil {
			return err
		}
		records, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (streamRecord, error) {
			return postgresRecord(c, row)
		})
		if err != nil {
			return err
		}
		if req.Last > 0 {
			slices.Reverse(records)
		}
		resp.Records = records
		readTo := head
		if req.Last == 0 && len(records) == limit {
			readTo = postgresPos(records[len(records)-1].Cursor)
		}
		if readTo > after {
			resp.Cursor = c.cursor(readTo)
		}
		if closed && readTo == head {
			resp.Close = &streamClose{Status: streamCloseStatus(closeStatus), Reason: closeReason}
			if len(closeFailure) > 0 {
				resp.Close.Failure = &failurepb.Failure{}
				if err := proto.Unmarshal(closeFailure, resp.Close.Failure); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return resp, found, err
}

func postgresRecord(c postgresChain, row pgx.CollectableRow) (streamRecord, error) {
	var r streamRecord
	var pos int64
	var kind string
	var payload []byte
	if err := row.Scan(&pos, &kind, &r.Topic, &payload, &r.Producer.Producer, &r.Producer.WorkflowRunID,
		&r.Producer.WorkflowTask, &r.Producer.WorkflowTaskAttempt, &r.Producer.Epoch, &r.Producer.Seq); err != nil {
		return r, err
	}
	r.Cursor = c.cursor(pos)
	r.Finish = kind == "finish"
	if kind == "data" {
		r.Payload = &commonpb.Payload{}
		if err := proto.Unmarshal(payload, r.Payload); err != nil {
			return r, err
		}
	}
	return r, nil
}

// waitForNotification waits for an append notification, returning nil also when the deadline
// passes.
func waitForNotification(ctx context.Context, conn *pgx.Conn, deadline time.Time) error {
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	_, err := conn.WaitForNotification(waitCtx)
	if err != nil && waitCtx.Err() != nil && ctx.Err() == nil {
		return nil
	}
	return err
}

// deleteStream deletes a stream as retention would, keeping a tombstone.
func (b *postgresStreamBackend) deleteStream(ctx context.Context, ref streamRef) error {
	chain, err := postgresWriteChain(ref)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		for _, table := range []string{"records", "producers"} {
			if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE owner_id = $1 AND owner_run_id = $2", chain.owner, chain.run); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `UPDATE streams SET deleted = true WHERE owner_id = $1 AND owner_run_id = $2`, chain.owner, chain.run)
		return err
	})
}

func postgresWriteChain(ref streamRef) (postgresChain, error) {
	if externalRun(ref) == "" {
		return postgresChain{}, errors.New("appends to a PostgreSQL stream need the run ID of its owner: a workflow's first run ID, or the activity's run ID")
	}
	return postgresChain{externalOwner(ref), externalRun(ref)}, nil
}

// A cursor is the owner, its run and a position: `<owner>:<run ID>#<position>`.
func (c postgresChain) cursor(pos int64) streamCursor {
	return streamCursor(fmt.Sprintf("%s:%s#%d", c.owner, c.run, pos))
}

func parsePostgresCursor(owner string, cur streamCursor) (postgresChain, int64, error) {
	chain, posStr, ok := strings.Cut(string(cur), "#")
	if !ok {
		return postgresChain{}, 0, errStreamCursor
	}
	firstRun, ok := strings.CutPrefix(chain, owner+":")
	if !ok {
		return postgresChain{}, 0, errStreamCursor
	}
	pos, err := strconv.ParseInt(posStr, 10, 64)
	if err != nil {
		return postgresChain{}, 0, errStreamCursor
	}
	return postgresChain{owner, firstRun}, pos, nil
}

func postgresPos(cur streamCursor) int64 {
	_, posStr, _ := strings.Cut(string(cur), "#")
	pos, _ := strconv.ParseInt(posStr, 10, 64)
	return pos
}

// postgresDigest identifies an append's content, so that a retry with different content is
// detected.
func postgresDigest(entries []postgresEntry, closing *streamClose) string {
	var fields [][]string
	for _, e := range entries {
		fields = append(fields, []string{e.kind, e.topic, string(e.payload)})
	}
	if closing != nil {
		failure, _ := proto.Marshal(closing.Failure)
		fields = append(fields, []string{"close", strconv.Itoa(int(closing.Status)), string(failure), closing.Reason})
	}
	return appendDigest(fields)
}

// startPostgres creates a database for one test, on a PostgreSQL server shared by the test
// process: macOS has too few SysV shared memory IDs for a server per test. initdb and postgres must
// be on PATH.
func startPostgres(t *testing.T) *pgxpool.Pool {
	postgresServerOnce.Do(func() { postgresServerURL, postgresServerErr = startPostgresServer() })
	require.NoError(t, postgresServerErr)
	db := fmt.Sprintf("stream_%d", postgresDatabases.Add(1))
	admin, err := pgx.Connect(t.Context(), postgresServerURL+"/postgres?sslmode=disable")
	require.NoError(t, err)
	defer func() { _ = admin.Close(context.Background()) }()
	_, err = admin.Exec(t.Context(), "CREATE DATABASE "+db)
	require.NoError(t, err)
	pool, err := pgxpool.New(t.Context(), postgresServerURL+"/"+db+"?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

var (
	postgresServerOnce sync.Once
	postgresServerURL  string
	postgresServerErr  error
	postgresDatabases  atomic.Int64
)

// startPostgresServer starts a server, under a shell that stops it and removes its data when the
// test process exits.
func startPostgresServer() (string, error) {
	dir, err := os.MkdirTemp("", "stream-postgres")
	if err != nil {
		return "", err
	}
	data := filepath.Join(dir, "data")
	if out, err := exec.Command("initdb", "-D", data, "-U", "postgres", "--auth=trust", "--no-sync").CombinedOutput(); err != nil {
		return "", fmt.Errorf("initdb: %w: %s", err, out)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	if err := l.Close(); err != nil {
		return "", err
	}
	script := `postgres -D "$1" -p "$2" -c unix_socket_directories= -c listen_addresses=127.0.0.1 -c fsync=off -c max_connections=500 >/dev/null 2>&1 &
server=$!
while kill -0 "$3" 2>/dev/null; do sleep 1; done
kill "$server"; wait "$server"; rm -rf "$4"`
	if err := exec.Command("sh", "-c", script, "sh", data, port, strconv.Itoa(os.Getpid()), dir).Start(); err != nil {
		return "", err
	}
	url := "postgres://postgres@127.0.0.1:" + port
	for range 200 {
		conn, err := pgx.Connect(context.Background(), url+"/postgres?sslmode=disable")
		if err == nil {
			return url, conn.Close(context.Background())
		}
		<-time.After(50 * time.Millisecond)
	}
	return "", errors.New("PostgreSQL did not start")
}
