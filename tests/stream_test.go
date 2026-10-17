package tests

// Shared stream tests. Each test runs against every backend in streamBackends, or against the
// native backend alone when the guarantee it checks is native-only. Each test's comment states
// the guarantee it checks.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type streamTestSuite struct {
	parallelsuite.Suite[*streamTestSuite]
}

func TestStreamTestSuite(t *testing.T) {
	parallelsuite.Run(t, &streamTestSuite{})
}

// streamFixture is one test's environment and the backend under test.
type streamFixture struct {
	t   *testing.T
	env *testcore.TestEnv
	b   streamBackendUnderTest
	tv  *testvars.TestVars
}

// streamBackends lists the backends. The native backend is first.
var streamBackends = []struct {
	name string
	new  func(t *testing.T, env *testcore.TestEnv) streamBackendUnderTest
}{
	{"Native", func(_ *testing.T, env *testcore.TestEnv) streamBackendUnderTest { return newNativeStreamBackend(env) }},
	{"Redis", func(t *testing.T, _ *testcore.TestEnv) streamBackendUnderTest { return newRedisStreamBackend(t) }},
}

func newStreamEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	return testcore.NewEnv(t, append(opts, testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true))...)
}

// forEachBackend runs test once per backend, each in its own environment.
func (s *streamTestSuite) forEachBackend(test func(f *streamFixture), opts ...testcore.TestOption) {
	for i := range streamBackends {
		s.runOnBackend(i, test, opts...)
	}
}

// nativeOnly runs test against the native backend alone.
func (s *streamTestSuite) nativeOnly(test func(f *streamFixture), opts ...testcore.TestOption) {
	s.runOnBackend(0, test, opts...)
}

func (s *streamTestSuite) runOnBackend(i int, test func(f *streamFixture), opts ...testcore.TestOption) {
	s.Run(streamBackends[i].name, func(s *streamTestSuite) {
		env := newStreamEnv(s.T(), opts...)
		test(&streamFixture{t: s.T(), env: env, b: streamBackends[i].new(s.T(), env), tv: env.Tv()})
	})
}

// Acknowledged appends are read in one total order, and a read from a given position returns the
// same records every time. Every backend.
func (s *streamTestSuite) TestRecordsAreReadInOneStableOrder() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		const appendsPerProducer = 10
		var g errgroup.Group
		for _, id := range []string{"a", "b"} {
			g.Go(func() error {
				for seq := range int64(appendsPerProducer) {
					if _, err := f.tryAppend(ref, clientProducer(id, 1), seq, "", fmt.Sprintf("%s%d", id, seq)); err != nil {
						return err
					}
				}
				return nil
			})
		}
		require.NoError(f.t, g.Wait())

		first := f.readAll(ref, streamBeginning())
		require.Len(f.t, first, 2*appendsPerProducer)
		byProducer := map[string][]string{}
		for _, r := range first {
			byProducer[r.Producer.Producer] = append(byProducer[r.Producer.Producer], f.data(r))
		}
		require.Len(f.t, byProducer, 2)
		for _, data := range byProducer {
			for seq, d := range data {
				require.Equal(f.t, fmt.Sprintf("%s%d", data[0][:1], seq), d)
			}
		}
		require.Equal(f.t, f.dataOf(first), f.dataOf(f.readAll(ref, streamBeginning())))
		mid := appendsPerProducer
		require.Equal(f.t, f.dataOf(first[mid:]), f.dataOf(f.readAll(ref, streamAfter(first[mid-1].Cursor))))
	})
}

// An append's records are stored contiguously, with no other producer's records between them.
// Every backend.
func (s *streamTestSuite) TestAppendIsContiguous() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		const batch = 5
		var g errgroup.Group
		for _, id := range []string{"a", "b", "c"} {
			g.Go(func() error {
				for seq := range int64(4) {
					data := make([]string, batch)
					for i := range data {
						data[i] = id
					}
					if _, err := f.tryAppend(ref, clientProducer(id, 1), seq, "", data...); err != nil {
						return err
					}
				}
				return nil
			})
		}
		require.NoError(f.t, g.Wait())

		records := f.readAll(ref, streamBeginning())
		require.Len(f.t, records, 3*4*batch)
		for i := 0; i < len(records); i += batch {
			for _, r := range records[i : i+batch] {
				require.Equal(f.t, records[i].Producer, r.Producer)
			}
		}
	})
}

// A retry of the latest append, with the same content, stores nothing new and returns the
// original cursors. Every backend.
func (s *streamTestSuite) TestRetriedAppendIsNotDuplicated() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		first := f.append(ref, p, 0, "", "x", "y")
		retry := f.append(ref, p, 0, "", "x", "y")
		require.Equal(f.t, first.Cursors, retry.Cursors)
		require.Equal(f.t, []string{"x", "y"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// A retry of the latest seq with different content is rejected. Every backend.
func (s *streamTestSuite) TestRetryWithDifferentContentIsRejected() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		f.append(ref, p, 0, "", "x")
		_, err := f.tryAppend(ref, p, 0, "", "z")
		require.ErrorIs(f.t, err, errStreamSeqConflict)
		require.Equal(f.t, []string{"x"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// An append whose seq is lower than the producer's latest is rejected as superseded. Every backend.
func (s *streamTestSuite) TestLowerSeqIsSuperseded() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		f.append(ref, p, 0, "", "x")
		f.append(ref, p, 1, "", "y")
		_, err := f.tryAppend(ref, p, 0, "", "x")
		require.ErrorIs(f.t, err, errStreamSeqSuperseded)
		require.Equal(f.t, []string{"x", "y"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// Once a producer has appended at an epoch, an append at a lower epoch is rejected. Every backend.
func (s *streamTestSuite) TestLowerEpochIsFenced() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "old")
		f.append(ref, clientProducer("a", 2), 0, "", "new")
		_, err := f.tryAppend(ref, clientProducer("a", 1), 1, "", "stale")
		require.ErrorIs(f.t, err, errStreamEpochFenced)
		require.Equal(f.t, []string{"old", "new"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// A timed-out activity attempt cannot append once its successor has appended. Every backend.
func (s *streamTestSuite) TestStaleActivityAttemptIsFencedAfterSuccessorAppends() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.scheduleRetriedActivity(f.tv)
		attempt1 := f.pollActivityTask(f.tv)
		attempt2 := f.pollActivityTask(f.tv) // dispatched once attempt 1 times out
		require.Equal(f.t, int32(2), attempt2.Attempt)

		f.append(ref, activityProducer(attempt2), 0, "", "attempt2")
		_, err := f.tryAppend(ref, activityProducer(attempt1), 0, "", "attempt1")
		require.ErrorIs(f.t, err, errStreamEpochFenced)
		require.Equal(f.t, []string{"attempt2"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// A timed-out activity attempt cannot append once its successor exists, even before the successor
// appends: the server derives the epoch from the task token. Native only; on Redis a stale attempt
// can write until its successor's first append.
func (s *streamTestSuite) TestStaleActivityAttemptIsFencedBeforeSuccessorAppends() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.scheduleRetriedActivity(f.tv)
		attempt1 := f.pollActivityTask(f.tv)
		f.pollActivityTask(f.tv)
		_, err := f.tryAppend(ref, activityProducer(attempt1), 0, "", "attempt1")
		require.ErrorIs(f.t, err, errStreamEpochFenced)
	})
}

// Every record carries its producer and epoch: an activity's epoch is its attempt, and records of a
// retried attempt carry a higher epoch than those of the attempt it replaced. Every backend.
func (s *streamTestSuite) TestRecordsCarryActivityProducerAndAttempt() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.scheduleRetriedActivity(f.tv)
		attempt1 := f.pollActivityTask(f.tv)
		f.append(ref, activityProducer(attempt1), 0, "", "attempt1")
		attempt2 := f.pollActivityTask(f.tv)
		f.append(ref, activityProducer(attempt2), 0, "", "attempt2")

		records := f.readAll(ref, streamBeginning())
		require.Len(f.t, records, 2)
		require.Equal(f.t, records[0].Producer.Producer, records[1].Producer.Producer)
		require.Equal(f.t, int64(1), records[0].Producer.Epoch)
		require.Equal(f.t, int64(2), records[1].Producer.Epoch)
	})
}

// A workflow appends sink-style, before responding to its workflow task. Records of a failed task
// attempt stay in the stream, and the rerun's records carry the same producer and a higher epoch.
// Every backend.
func (s *streamTestSuite) TestFailedWorkflowTaskRecordsCarryLowerEpoch() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		failed := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(failed), 0, "", "failed attempt")
		f.failWorkflowTask(failed)

		rerun := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(rerun), 0, "", "rerun")
		f.completeWorkflowTask(rerun)

		records := f.readAll(ref, streamBeginning())
		require.Equal(f.t, []string{"failed attempt", "rerun"}, f.dataOf(records))
		require.Equal(f.t, records[0].Producer.Producer, records[1].Producer.Producer)
		require.Greater(f.t, records[1].Producer.Epoch, records[0].Producer.Epoch)
	})
}

// A workflow's epoch increases across workflow tasks, not only across attempts of one task.
// Every backend.
func (s *streamTestSuite) TestWorkflowEpochIncreasesAcrossTasks() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task1 := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task1), 0, "", "task1")
		f.completeWorkflowTask(task1, f.forceNewWorkflowTask)
		task2 := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task2), 0, "", "task2")

		records := f.readAll(ref, streamBeginning())
		require.Equal(f.t, []string{"task1", "task2"}, f.dataOf(records))
		require.Greater(f.t, records[1].Producer.Epoch, records[0].Producer.Epoch)
	})
}

// A read waiting at the head returns when a record is appended, well before its wait ends.
// Every backend.
func (s *streamTestSuite) TestWaitingReadReturnsOnAppend() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "first").Cursors[0]
		const wait = 20 * time.Second
		result := f.readAsync(ref, streamReadRequest{From: streamAfter(head), Wait: wait})
		appended := time.Now()
		f.append(ref, clientProducer("a", 1), 1, "", "second")
		resp := f.awaitRead(result)
		require.Less(f.t, time.Since(appended), wait/4)
		require.Equal(f.t, []string{"second"}, f.dataOf(resp.Records))
	})
}

// No append is missed by a reader that keeps reading from its last cursor while records are
// appended concurrently. Every backend.
func (s *streamTestSuite) TestConcurrentReadsMissNoAppend() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		const n = 30
		var g errgroup.Group
		g.Go(func() error {
			for seq := range int64(n) {
				if _, err := f.tryAppend(ref, clientProducer("a", 1), seq, "", fmt.Sprint(seq)); err != nil {
					return err
				}
			}
			return nil
		})
		var got []string
		from := streamBeginning()
		for len(got) < n {
			resp := f.read(ref, streamReadRequest{From: from, Wait: 5 * time.Second})
			got = append(got, f.dataOf(resp.Records)...)
			if resp.Cursor != nil {
				from = streamAfter(resp.Cursor)
			}
		}
		require.NoError(f.t, g.Wait())
		want := make([]string, n)
		for i := range want {
			want[i] = fmt.Sprint(i)
		}
		require.Equal(f.t, want, got)
	})
}

// Appends succeed with no reader, and readers at different positions do not affect each other.
// Every backend.
func (s *streamTestSuite) TestReadersAreIndependent() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		cursors := f.append(ref, clientProducer("a", 1), 0, "", "x", "y", "z").Cursors
		require.Equal(f.t, []string{"x", "y", "z"}, f.dataOf(f.readAll(ref, streamBeginning())))
		require.Equal(f.t, []string{"z"}, f.dataOf(f.readAll(ref, streamAfter(cursors[1]))))
		require.Equal(f.t, []string{"y", "z"}, f.dataOf(f.readAll(ref, streamAfter(cursors[0]))))
	})
}

// A read with a topic filter returns only matching records, and its cursor resumes past the
// records it skipped. Every backend.
func (s *streamTestSuite) TestFilteredReadReturnsOnlyMatchingTopics() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		f.append(ref, p, 0, "progress", "p1")
		f.append(ref, p, 1, "delta", "d1", "d2")
		f.append(ref, p, 2, "progress", "p2")
		f.append(ref, p, 3, "delta", "d3")

		resp := f.read(ref, streamReadRequest{From: streamBeginning(), Topics: []string{"progress"}})
		require.Equal(f.t, []string{"p1", "p2"}, f.dataOf(resp.Records))
		for _, r := range resp.Records {
			require.Equal(f.t, "progress", r.Topic)
		}
		f.append(ref, p, 4, "progress", "p3")
		next := f.read(ref, streamReadRequest{From: streamAfter(resp.Cursor), Topics: []string{"progress"}})
		require.Equal(f.t, []string{"p3"}, f.dataOf(next.Records))
	})
}

// A filtered read waiting at the head is not woken by records of other topics. Native only: Redis
// readers receive every record and filter it themselves.
func (s *streamTestSuite) TestFilteredWaitIgnoresOtherTopics() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		head := f.append(ref, p, 0, "progress", "p1").Cursors[0]
		const wait = 3 * time.Second
		result := f.readAsync(ref, streamReadRequest{From: streamAfter(head), Topics: []string{"progress"}, Wait: wait})
		started := time.Now()
		f.append(ref, p, 1, "delta", "d1")
		resp := f.awaitRead(result)
		require.Empty(f.t, resp.Records)
		require.GreaterOrEqual(f.t, time.Since(started), wait/2)
	})
}

// A tail read returns the last N records, and its cursor resumes after them. Every backend.
func (s *streamTestSuite) TestTailRead() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		f.append(ref, p, 0, "", "1", "2", "3", "4", "5")
		resp := f.read(ref, streamReadRequest{Last: 2})
		require.Equal(f.t, []string{"4", "5"}, f.dataOf(resp.Records))
		f.append(ref, p, 1, "", "6")
		require.Equal(f.t, []string{"6"}, f.dataOf(f.readAll(ref, streamAfter(resp.Cursor))))
	})
}

// The owner's close is ordered after every earlier record, wakes a waiting reader, and carries
// its status. Every backend.
func (s *streamTestSuite) TestOwnerCloseIsSeenAfterAllRecords() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		result := f.readAsync(ref, streamReadRequest{From: streamAfter(head), Wait: 20 * time.Second})

		task := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "", "last")
		closeReq := streamCloseRequest{
			Owner: workflowProducer(task),
			Seq:   1,
			Close: streamClose{Status: streamClosedFailed, Failure: &failurepb.Failure{Message: "boom"}, Reason: "giving up"},
		}
		require.NoError(f.t, f.b.Close(f.ctx(), ref, closeReq))

		waited := f.awaitRead(result)
		require.Equal(f.t, []string{"last"}, f.dataOf(waited.Records))

		resp := f.read(ref, streamReadRequest{From: streamBeginning()})
		require.Equal(f.t, []string{"x", "last"}, f.dataOf(resp.Records))
		require.NotNil(f.t, resp.Close)
		require.Equal(f.t, streamClosedFailed, resp.Close.Status)
		require.Equal(f.t, "boom", resp.Close.Failure.GetMessage())
		require.Equal(f.t, "giving up", resp.Close.Reason)
	})
}

// After close, new appends are rejected, but a retry of an append that was already stored still
// succeeds with its original cursors. Every backend.
func (s *streamTestSuite) TestAppendAfterCloseIsRejectedButStoredRetrySucceeds() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		stored := f.append(ref, p, 0, "", "x")
		task := f.pollWorkflowTask(f.tv)
		require.NoError(f.t, f.b.Close(f.ctx(), ref, streamCloseRequest{
			Owner: workflowProducer(task), Close: streamClose{Status: streamClosedCompleted},
		}))

		_, err := f.tryAppend(ref, p, 1, "", "y")
		require.ErrorIs(f.t, err, errStreamClosed)
		retry := f.append(ref, p, 0, "", "x")
		require.Equal(f.t, stored.Cursors, retry.Cursors)
	})
}

// Only the owner may close the stream. Every backend.
func (s *streamTestSuite) TestNonOwnerCannotClose() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		err := f.b.Close(f.ctx(), ref, streamCloseRequest{
			Owner: clientProducer("a", 1), Seq: 1, Close: streamClose{Status: streamClosedCompleted},
		})
		require.ErrorIs(f.t, err, errStreamNotOwner)
		require.Nil(f.t, f.read(ref, streamReadRequest{From: streamBeginning()}).Close)
	})
}

// A producer's finish is a record readers see; it does not close the stream. Every backend.
func (s *streamTestSuite) TestProducerFinishDoesNotCloseStream() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		_, err := f.b.Append(f.ctx(), ref, streamAppendRequest{Producer: clientProducer("a", 1), Finish: true})
		require.NoError(f.t, err)
		f.append(ref, clientProducer("b", 1), 0, "", "y")

		resp := f.read(ref, streamReadRequest{From: streamBeginning()})
		require.Len(f.t, resp.Records, 2)
		require.True(f.t, resp.Records[0].Finish)
		require.Nil(f.t, resp.Close)
	})
}

// A stream embedded in its owner closes in the same transaction as the owner completing, with a
// status taken from the owner's terminal state. Native only: on Redis the owner's worker writes
// the close, so after a worker crash it can be late or missing.
func (s *streamTestSuite) TestStreamClosesWhenOwnerCompletes() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "", "x")
		f.completeWorkflowTask(task, f.completeWorkflow)

		resp := f.read(ref, streamReadRequest{From: streamBeginning()})
		require.Equal(f.t, []string{"x"}, f.dataOf(resp.Records))
		require.NotNil(f.t, resp.Close)
		require.Equal(f.t, streamClosedCompleted, resp.Close.Status)
	})
}

// A read from the beginning that starts before the stream's owner exists receives the first
// record. Every backend.
func (s *streamTestSuite) TestReadFromBeginningWaitsForFirstAppend() {
	s.forEachBackend(func(f *streamFixture) {
		ref := streamRef{WorkflowID: f.tv.WorkflowID()}
		result := make(chan streamReadResult, 1)
		go func() {
			for {
				resp, err := f.b.Read(f.ctx(), ref, streamReadRequest{From: streamBeginning(), Wait: 2 * time.Second})
				if err != nil || len(resp.Records) > 0 {
					result <- streamReadResult{resp, err}
					return
				}
			}
		}()
		f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "first")
		require.Equal(f.t, []string{"first"}, f.dataOf(f.awaitRead(result).Records))
	})
}

// A read after a cursor of a stream that has been deleted fails as expired, rather than waiting.
// Every backend.
func (s *streamTestSuite) TestReadAfterCursorOfDeletedStreamFails() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		cursor := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		require.NoError(f.t, f.b.deleteStream(f.ctx(), ref))
		_, err := f.b.Read(f.ctx(), ref, streamReadRequest{From: streamAfter(cursor), Wait: 5 * time.Second})
		require.ErrorIs(f.t, err, errStreamExpired)
	})
}

// A cursor from one stream is rejected by another. Every backend.
func (s *streamTestSuite) TestCursorFromAnotherStreamIsRejected() {
	s.forEachBackend(func(f *streamFixture) {
		ref1 := f.startOwner(f.tv.WithWorkflowIDNumber(1))
		ref2 := f.startOwner(f.tv.WithWorkflowIDNumber(2))
		cursor := f.append(ref1, clientProducer("a", 1), 0, "", "x").Cursors[0]
		f.append(ref2, clientProducer("a", 1), 0, "", "y")
		_, err := f.b.Read(f.ctx(), ref2, streamReadRequest{From: streamAfter(cursor)})
		require.ErrorIs(f.t, err, errStreamCursor)
	})
}

// A workflow-owned stream belongs to the run chain: records continue in order across
// continue-as-new, which does not close the stream, and the chain can be pinned by its first run
// ID. Every backend.
func (s *streamTestSuite) TestStreamContinuesAcrossContinueAsNew() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		firstRun := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(firstRun), 0, "", "run1")
		f.completeWorkflowTask(firstRun, f.continueAsNew)
		secondRun := f.pollWorkflowTask(f.tv)
		require.NotEqual(f.t, firstRun.WorkflowExecution.RunId, secondRun.WorkflowExecution.RunId)
		f.append(ref, workflowProducer(secondRun), 0, "", "run2")

		pinned := streamRef{WorkflowID: ref.WorkflowID, FirstRunID: firstRun.WorkflowExecution.RunId}
		for _, r := range []streamRef{ref, pinned} {
			resp := f.read(r, streamReadRequest{From: streamBeginning()})
			require.Equal(f.t, []string{"run1", "run2"}, f.dataOf(resp.Records))
			require.Nil(f.t, resp.Close)
		}
	})
}

// Native offsets are contiguous across continue-as-new. Native only.
func (s *streamTestSuite) TestNativeOffsetsAreContiguousAcrossContinueAsNew() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		firstRun := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(firstRun), 0, "", "run1")
		f.completeWorkflowTask(firstRun, f.continueAsNew)
		secondRun := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(secondRun), 0, "", "run2")

		records := f.readAll(ref, streamBeginning())
		require.Len(f.t, records, 2)
		first, err := f.b.offset(records[0].Cursor)
		require.NoError(f.t, err)
		second, err := f.b.offset(records[1].Cursor)
		require.NoError(f.t, err)
		require.Equal(f.t, first+1, second)
	})
}

// An acknowledged append survives the stream's History shard being reloaded. Native only: Redis
// durability depends on how Redis is configured.
func (s *streamTestSuite) TestAppendSurvivesShardReload() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		f.env.CloseShard(f.env.NamespaceID().String(), ref.WorkflowID)
		require.Equal(f.t, []string{"x"}, f.dataOf(f.readAll(ref, streamBeginning())))
	}, testcore.WithDedicatedCluster())
}

// Payloads, including their metadata, are stored and returned byte for byte. Every backend.
func (s *streamTestSuite) TestPayloadsRoundTripUnchanged() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("binary/encrypted"), "encryption-key-id": []byte("k1")},
			Data:     []byte{0, 1, 2, 255},
		}
		_, err := f.b.Append(f.ctx(), ref, streamAppendRequest{
			Producer: clientProducer("a", 1),
			Records:  []streamRecordInput{{Topic: "t", Payload: p}},
		})
		require.NoError(f.t, err)
		records := f.readAll(ref, streamBeginning())
		require.Len(f.t, records, 1)
		require.True(f.t, proto.Equal(p, records[0].Payload))
	})
}

func (f *streamFixture) ctx() context.Context {
	return testcontext.For(f.t)
}

// startOwner starts the workflow that owns the stream.
func (f *streamFixture) startOwner(tv *testvars.TestVars) streamRef {
	_, err := f.env.FrontendClient().StartWorkflowExecution(f.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    f.env.Namespace().String(),
		RequestId:    tv.Any().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Identity:     tv.WorkerIdentity(),
	})
	require.NoError(f.t, err)
	return streamRef{WorkflowID: tv.WorkflowID()}
}

func (f *streamFixture) tryAppend(ref streamRef, p streamProducer, seq int64, topic string, data ...string) (streamAppendResponse, error) {
	records := make([]streamRecordInput, len(data))
	for i, d := range data {
		records[i] = streamRecordInput{Topic: topic, Payload: payload.EncodeString(d)}
	}
	return f.b.Append(f.ctx(), ref, streamAppendRequest{Producer: p, Seq: seq, Records: records})
}

func (f *streamFixture) append(ref streamRef, p streamProducer, seq int64, topic string, data ...string) streamAppendResponse {
	resp, err := f.tryAppend(ref, p, seq, topic, data...)
	require.NoError(f.t, err)
	require.Len(f.t, resp.Cursors, len(data))
	return resp
}

func (f *streamFixture) read(ref streamRef, req streamReadRequest) streamReadResponse {
	resp, err := f.b.Read(f.ctx(), ref, req)
	require.NoError(f.t, err)
	return resp
}

// readAll reads, without waiting, every record after from.
func (f *streamFixture) readAll(ref streamRef, from streamPosition) []streamRecord {
	var records []streamRecord
	for {
		resp := f.read(ref, streamReadRequest{From: from})
		if len(resp.Records) == 0 {
			return records
		}
		records = append(records, resp.Records...)
		from = streamAfter(resp.Cursor)
	}
}

type streamReadResult struct {
	resp streamReadResponse
	err  error
}

func (f *streamFixture) readAsync(ref streamRef, req streamReadRequest) <-chan streamReadResult {
	result := make(chan streamReadResult, 1)
	go func() {
		resp, err := f.b.Read(f.ctx(), ref, req)
		result <- streamReadResult{resp, err}
	}()
	return result
}

func (f *streamFixture) awaitRead(result <-chan streamReadResult) streamReadResponse {
	select {
	case r := <-result:
		require.NoError(f.t, r.err)
		return r.resp
	case <-f.ctx().Done():
		f.t.Fatal("timed out waiting for read")
		panic("unreachable")
	}
}

func (f *streamFixture) data(r streamRecord) string {
	return payload.ToString(r.Payload)
}

func (f *streamFixture) dataOf(records []streamRecord) []string {
	data := make([]string, len(records))
	for i, r := range records {
		data[i] = f.data(r)
	}
	return data
}

func (f *streamFixture) pollWorkflowTask(tv *testvars.TestVars) *workflowservice.PollWorkflowTaskQueueResponse {
	task, err := f.env.FrontendClient().PollWorkflowTaskQueue(f.ctx(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: f.env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	require.NoError(f.t, err)
	require.NotEmpty(f.t, task.TaskToken)
	return task
}

func (f *streamFixture) completeWorkflowTask(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	edits ...func(*workflowservice.RespondWorkflowTaskCompletedRequest),
) {
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: f.env.Namespace().String(),
		TaskToken: task.TaskToken,
		Identity:  f.tv.WorkerIdentity(),
	}
	for _, edit := range edits {
		edit(req)
	}
	_, err := f.env.FrontendClient().RespondWorkflowTaskCompleted(f.ctx(), req)
	require.NoError(f.t, err)
}

func (f *streamFixture) failWorkflowTask(task *workflowservice.PollWorkflowTaskQueueResponse) {
	_, err := f.env.FrontendClient().RespondWorkflowTaskFailed(f.ctx(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: f.env.Namespace().String(),
		TaskToken: task.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Failure:   &failurepb.Failure{Message: "worker failed the task"},
		Identity:  f.tv.WorkerIdentity(),
	})
	require.NoError(f.t, err)
}

func (f *streamFixture) forceNewWorkflowTask(req *workflowservice.RespondWorkflowTaskCompletedRequest) {
	req.ForceCreateNewWorkflowTask = true
}

func (f *streamFixture) completeWorkflow(req *workflowservice.RespondWorkflowTaskCompletedRequest) {
	req.Commands = append(req.Commands, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	})
}

func (f *streamFixture) continueAsNew(req *workflowservice.RespondWorkflowTaskCompletedRequest) {
	req.Commands = append(req.Commands, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: f.tv.WorkflowType(),
				TaskQueue:    f.tv.TaskQueue(),
			},
		},
	})
}

// streamActivityStartToClose is short so that an unanswered attempt times out and is retried.
const streamActivityStartToClose = 2 * time.Second

// scheduleRetriedActivity completes the owner's first workflow task with an activity whose
// unanswered attempts time out and are retried at once.
func (f *streamFixture) scheduleRetriedActivity(tv *testvars.TestVars) {
	task := f.pollWorkflowTask(tv)
	f.completeWorkflowTask(task, func(req *workflowservice.RespondWorkflowTaskCompletedRequest) {
		req.Commands = append(req.Commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:          tv.ActivityID(),
					ActivityType:        tv.ActivityType(),
					TaskQueue:           tv.TaskQueue(),
					StartToCloseTimeout: durationpb.New(streamActivityStartToClose),
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval:    durationpb.New(time.Millisecond),
						BackoffCoefficient: 1,
						MaximumAttempts:    3,
					},
				},
			},
		})
	})
}

func (f *streamFixture) pollActivityTask(tv *testvars.TestVars) *workflowservice.PollActivityTaskQueueResponse {
	task, err := f.env.FrontendClient().PollActivityTaskQueue(f.ctx(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: f.env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	require.NoError(f.t, err)
	require.NotEmpty(f.t, task.TaskToken)
	return task
}
