package tests

// Shared stream tests. Each test runs against every backend in streamBackends, or against the
// native backend alone when the guarantee it checks is native-only. Each test's comment states
// the guarantee it checks.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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
	{"Redis", newRedisStreamBackend},
	{"PostgreSQL", newPostgresStreamBackend},
}

func newStreamEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	return testcore.NewEnv(t, append(opts, testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableDirectNexusCalls, true),
	)...)
}

// withRetryStamps makes activity retries increase the attempt stamp. The setting is global, so the
// test gets a dedicated cluster.
var withRetryStamps = testcore.WithDynamicConfig(dynamicconfig.EnableActivityRetryStampIncrement, true)

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
		require.ErrorIs(f.t, err, errStreamSeqDivergent)
		require.Equal(f.t, []string{"x"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// An append whose seq is lower than the producer's latest is rejected as stale. Every backend.
func (s *streamTestSuite) TestLowerSeqIsStale() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		p := clientProducer("a", 1)
		f.append(ref, p, 0, "", "x")
		f.append(ref, p, 1, "", "y")
		_, err := f.tryAppend(ref, p, 0, "", "x")
		require.ErrorIs(f.t, err, errStreamSeqStale)
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
	}, withRetryStamps)
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
	}, withRetryStamps)
}

// Every record carries its producer and epoch: records of a retried activity attempt carry the same
// producer and a higher epoch than those of the attempt it replaced. Every backend.
func (s *streamTestSuite) TestRetriedActivityAttemptHasHigherEpoch() {
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
		require.Greater(f.t, records[1].Producer.Epoch, records[0].Producer.Epoch)
	}, withRetryStamps)
}

// After a reset between attempts, the earlier attempt is fenced although the new attempt has the
// same attempt number: the epoch is the attempt stamp. Every backend.
//
// The reset happens between attempts because a reset during an attempt differs between the two
// activity surfaces. A standalone activity defers the reset until the running attempt ends: the
// attempt stays valid, and its successor gets a higher stamp. A workflow activity applies the reset
// at once by rewinding the attempt counter, which its task token uses as the credential, so the
// running attempt's result and heartbeats are refused although its stamp is unchanged. Workflow
// activities are planned to adopt the deferred reset, plus a forced reset that invalidates the
// running attempt by increasing the stamp. Under that design a stamp fences exactly the attempts
// that a reset supersedes, and this test can also cover a reset during an attempt.
func (s *streamTestSuite) TestAttemptBeforeResetIsFencedAfterSuccessorAppends() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.scheduleRetriedActivity(f.tv)
		attempt1 := f.pollActivityTask(f.tv)
		f.resetActivityAwaitingAttempt(f.tv, 2)
		afterReset := f.pollActivityTask(f.tv)
		require.Equal(f.t, attempt1.Attempt, afterReset.Attempt)

		f.append(ref, activityProducer(afterReset), 0, "", "after reset")
		_, err := f.tryAppend(ref, activityProducer(attempt1), 0, "", "before reset")
		require.ErrorIs(f.t, err, errStreamEpochFenced)
		require.Equal(f.t, []string{"after reset"}, f.dataOf(f.readAll(ref, streamBeginning())))
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
		require.Equal(f.t, records[0].Producer.WorkflowTask, records[1].Producer.WorkflowTask)
		require.Greater(f.t, records[1].Producer.Epoch, records[0].Producer.Epoch)
	})
}

// A workflow task's completion records, in the stream, which attempt committed, so readers can
// discard a failed attempt's records even when the attempt that succeeds appends nothing. Native
// only: on Redis those records are not withdrawn.
func (s *streamTestSuite) TestWorkflowTaskCompletionRecordsCommittedAttempt() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		failed := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(failed), 0, "", "failed attempt")
		f.failWorkflowTask(failed)
		rerun := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(rerun)

		records := f.readAllWithMarkers(ref, streamBeginning())
		require.Len(f.t, records, 2)
		commit := records[1].WorkflowTaskCommit
		require.NotNil(f.t, commit)
		require.Equal(f.t, records[0].Producer.WorkflowRunID, commit.WorkflowRunID)
		require.Equal(f.t, records[0].Producer.WorkflowTask, commit.WorkflowTask)
		require.Equal(f.t, int64(rerun.Attempt), commit.Attempt)
		require.Greater(f.t, commit.Attempt, records[0].Producer.Epoch)
	})
}

// A workflow task cannot append once a later task of the same run has appended. Every backend.
func (s *streamTestSuite) TestStaleWorkflowTaskIsFencedAfterSuccessorAppends() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task1 := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task1), 0, "", "task1")
		f.completeWorkflowTask(task1, f.forceNewWorkflowTask)
		task2 := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task2), 0, "", "task2")

		_, err := f.tryAppend(ref, workflowProducer(task1), 1, "", "stale")
		require.ErrorIs(f.t, err, errStreamEpochFenced)
		require.Equal(f.t, []string{"task1", "task2"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// A failed workflow task attempt cannot append once its rerun has started, even before the rerun
// appends: the server checks the task identity and attempt against the current workflow task.
// Native only.
func (s *streamTestSuite) TestFailedWorkflowTaskAttemptIsFencedBeforeRerunAppends() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		failed := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(failed), 0, "", "failed attempt")
		f.failWorkflowTask(failed)
		f.pollWorkflowTask(f.tv)

		_, err := f.tryAppend(ref, workflowProducer(failed), 1, "", "stale")
		require.ErrorIs(f.t, err, errStreamEpochFenced)
	})
}

// Appends from a speculative workflow task (one created for an Update) are rejected. Native only:
// a Redis backend cannot tell that a task is speculative.
func (s *streamTestSuite) TestSpeculativeWorkflowTaskAppendIsRejected() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.completeWorkflowTask(f.pollWorkflowTask(f.tv))
		go func() {
			// Fails when the test ends, since the update is never accepted.
			_, _ = f.env.FrontendClient().UpdateWorkflowExecution(f.ctx(), &workflowservice.UpdateWorkflowExecutionRequest{
				Namespace:         f.env.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: ref.WorkflowID},
				WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
				Request: &updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: f.tv.UpdateID()},
					Input: &updatepb.Input{Name: f.tv.HandlerName()},
				},
			})
		}()
		speculative := f.pollWorkflowTask(f.tv)
		require.NotEmpty(f.t, speculative.Messages)

		_, err := f.tryAppend(ref, workflowProducer(speculative), 0, "", "x")
		require.ErrorIs(f.t, err, errStreamSpeculative)
	})
}

// A workflow's task identity increases with each completed workflow task. Every backend.
func (s *streamTestSuite) TestWorkflowTaskIdentityIncreasesAcrossTasks() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task1 := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task1), 0, "", "task1")
		f.completeWorkflowTask(task1, f.forceNewWorkflowTask)
		task2 := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task2), 0, "", "task2")

		records := f.readAll(ref, streamBeginning())
		require.Equal(f.t, []string{"task1", "task2"}, f.dataOf(records))
		require.Equal(f.t, records[0].Producer.Producer, records[1].Producer.Producer)
		require.Greater(f.t, records[1].Producer.WorkflowTask, records[0].Producer.WorkflowTask)
	})
}

// A seq that skips one is rejected: a producer's first append, and the first append of each new
// epoch, has seq 0, and each later append has the next seq. Every backend.
func (s *streamTestSuite) TestSeqGapIsRejected() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		_, err := f.tryAppend(ref, clientProducer("a", 1), 1, "", "x")
		require.ErrorIs(f.t, err, errStreamSeqGap)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		_, err = f.tryAppend(ref, clientProducer("a", 1), 2, "", "z")
		require.ErrorIs(f.t, err, errStreamSeqGap)
		_, err = f.tryAppend(ref, clientProducer("a", 2), 1, "", "z")
		require.ErrorIs(f.t, err, errStreamSeqGap)
		require.Equal(f.t, []string{"x"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// Every completed workflow task of a run that owns a stream has a commit record, whether or not its
// producers appended before it completed. Native only.
func (s *streamTestSuite) TestEveryWorkflowTaskHasCommitRecord() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		task := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(task)

		commits := f.commits(ref)
		require.Len(f.t, commits, 1)
		require.Equal(f.t, task.WorkflowExecution.RunId, commits[0].WorkflowRunID)
		require.Equal(f.t, task.PreviousStartedEventId, commits[0].WorkflowTask)
		require.Equal(f.t, int64(task.Attempt), commits[0].Attempt)
	})
}

// A retry of a local activity within one workflow task has the same producer and a higher epoch.
// Every backend.
func (s *streamTestSuite) TestLocalActivityRetryHasHigherEpoch() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(ref, localActivityProducer(task, "1", 1), 0, "", "attempt 1")
		f.append(ref, localActivityProducer(task, "1", 2), 0, "", "attempt 2")

		records := f.readAll(ref, streamBeginning())
		require.Equal(f.t, []string{"attempt 1", "attempt 2"}, f.dataOf(records))
		require.Equal(f.t, records[0].Producer.Producer, records[1].Producer.Producer)
		require.Greater(f.t, records[1].Producer.Epoch, records[0].Producer.Epoch)
		require.Equal(f.t, task.WorkflowExecution.RunId, records[0].Producer.WorkflowRunID)
		require.Equal(f.t, task.PreviousStartedEventId, records[0].Producer.WorkflowTask)
		require.Equal(f.t, task.Attempt, records[0].Producer.WorkflowTaskAttempt)
	})
}

// A local activity that appends only after its workflow task has completed through a task heartbeat
// is accepted, and the task's commit record names the attempt it ran in. Native only.
func (s *streamTestSuite) TestLocalActivityAppendsAfterTaskHeartbeat() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "created")
		task := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(task, f.forceNewWorkflowTask)
		f.pollWorkflowTask(f.tv)
		f.append(ref, localActivityProducer(task, "1", 1), 0, "", "late")

		records := f.readAllWithMarkers(ref, streamBeginning())
		require.Equal(f.t, []string{"created", "late"}, f.dataOf(records))
		late := withoutMarkers(records)[1].Producer
		commit := f.commitOf(records, late.WorkflowRunID, late.WorkflowTask)
		require.NotNil(f.t, commit)
		require.Equal(f.t, int64(late.WorkflowTaskAttempt), commit.Attempt)
	})
}

// A local activity of a failed workflow task attempt does not collide with the same local activity
// rerun by the next attempt, and the commit record names the rerun's attempt, so readers withdraw
// the failed attempt's records. Native only, because Redis writes no commit records.
func (s *streamTestSuite) TestLocalActivityOfFailedTaskIsWithdrawn() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		failed := f.pollWorkflowTask(f.tv)
		f.append(ref, localActivityProducer(failed, "1", 1), 0, "", "failed attempt")
		f.failWorkflowTask(failed)
		rerun := f.pollWorkflowTask(f.tv)
		f.append(ref, localActivityProducer(rerun, "1", 1), 0, "", "rerun")
		f.completeWorkflowTask(rerun)

		records := f.readAllWithMarkers(ref, streamBeginning())
		data := withoutMarkers(records)
		require.Equal(f.t, []string{"failed attempt", "rerun"}, f.dataOf(data))
		commit := f.commitOf(records, rerun.WorkflowExecution.RunId, rerun.PreviousStartedEventId)
		require.NotNil(f.t, commit)
		require.NotEqual(f.t, int64(data[0].Producer.WorkflowTaskAttempt), commit.Attempt)
		require.Equal(f.t, int64(data[1].Producer.WorkflowTaskAttempt), commit.Attempt)
	})
}

// An activity options update during a running attempt does not fence the attempt: its epoch is the
// stamp it started with. Every backend.
func (s *streamTestSuite) TestOptionsUpdateDoesNotFenceRunningAttempt() {
	s.forEachBackend(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.scheduleRetriedActivity(f.tv)
		task := f.pollActivityTask(f.tv)
		f.append(ref, activityProducer(task), 0, "", "before")
		_, err := f.env.FrontendClient().UpdateActivityOptions(f.ctx(), &workflowservice.UpdateActivityOptionsRequest{
			Namespace:       f.env.Namespace().String(),
			Execution:       &commonpb.WorkflowExecution{WorkflowId: ref.WorkflowID},
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: f.tv.ActivityID()},
			ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Minute)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(f.t, err)
		f.append(ref, activityProducer(task), 1, "", "after")
		require.Equal(f.t, []string{"before", "after"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// An append whose record exceeds the record size limit, or that has too many records, is rejected.
// Native only; the shared SDK code enforces the same limits for Redis.
func (s *streamTestSuite) TestOversizedAppendIsRejected() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		big := &commonpb.Payload{Data: make([]byte, streamMaxRecordBytes+1)}
		_, err := f.b.Append(f.ctx(), ref, streamAppendRequest{
			Producer: clientProducer("a", 1),
			Records:  []streamAppendRecord{{Payload: big}},
		})
		require.ErrorIs(f.t, err, errStreamTooLarge)
		many := make([]string, streamMaxRecordsPerAppend+1)
		_, err = f.tryAppend(ref, clientProducer("a", 1), 0, "", many...)
		require.ErrorIs(f.t, err, errStreamTooLarge)
	})
}

// A later run chain that reuses a workflow ID has its own stream: the earlier chain's records and
// close do not affect it. Every backend.
func (s *streamTestSuite) TestNewChainWithSameWorkflowIDHasNewStream() {
	s.forEachBackend(func(f *streamFixture) {
		first := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(first, workflowProducer(task), 0, "", "first chain")
		require.NoError(f.t, f.b.Close(f.ctx(), first, streamCloseRequest{
			Owner: workflowProducer(task), Seq: 1, Close: streamClose{Status: streamClosedCompleted},
		}))
		f.completeWorkflowTask(task, f.completeWorkflow)

		second := f.startOwner(f.tv)
		require.NotEqual(f.t, first.FirstRunID, second.FirstRunID)
		f.append(second, clientProducer("a", 1), 0, "", "second chain")
		resp := f.read(second, streamReadRequest{From: streamBeginning()})
		require.Equal(f.t, []string{"second chain"}, f.dataOf(resp.Records))
		require.Nil(f.t, resp.Close)
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
		owner := f.startOwner(f.tv)
		f.append(owner, clientProducer("a", 1), 0, "", "first")
		require.Equal(f.t, []string{"first"}, f.dataOf(f.awaitRead(result).Records))
	})
}

// An append to a stream whose owner does not exist fails as not found. Native only: a native stream
// is embedded in its owner, while on Redis the first append creates the stream.
func (s *streamTestSuite) TestAppendWithoutOwnerFailsAsNotFound() {
	s.nativeOnly(func(f *streamFixture) {
		_, err := f.tryAppend(streamRef{WorkflowID: f.tv.WorkflowID()}, clientProducer("a", 1), 0, "", "x")
		require.ErrorIs(f.t, err, errStreamNotFound)
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

		records := f.readAllWithMarkers(ref, streamBeginning())
		require.Equal(f.t, []string{"run1", "run2"}, f.dataOf(records))
		first, err := f.b.offset(records[0].Cursor)
		require.NoError(f.t, err)
		for i, r := range records {
			offset, err := f.b.offset(r.Cursor)
			require.NoError(f.t, err)
			require.Equal(f.t, first+int64(i), offset)
		}
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

// An activity's producer state is removed when the activity closes, so the stream keeps no state
// for producers that can no longer append. Native only.
func (s *streamTestSuite) TestClosedActivityProducerStateIsRemoved() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.scheduleRetriedActivity(f.tv)
		task := f.pollActivityTask(f.tv)
		f.append(ref, activityProducer(task), 0, "", "x")
		require.Len(f.t, f.activityProducerNodes(ref), 1)

		_, err := f.env.FrontendClient().RespondActivityTaskCompleted(f.ctx(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: f.env.Namespace().String(),
			TaskToken: task.TaskToken,
			Identity:  f.tv.WorkerIdentity(),
		})
		require.NoError(f.t, err)
		require.Empty(f.t, f.activityProducerNodes(ref))
		require.Equal(f.t, []string{"x"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// An append that creates a stream records the run's next workflow task, so that readers can treat
// records of tasks that completed before the stream existed, which have no commit record, as
// committed. Native only.
func (s *streamTestSuite) TestStreamCreationRecordsCompletedTasks() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		first := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(first, f.forceNewWorkflowTask)
		second := f.pollWorkflowTask(f.tv)
		f.append(ref, localActivityProducer(first, "1", 1), 0, "", "late")

		records := f.readAllWithMarkers(ref, streamBeginning())
		created := records[0].StreamCreated
		require.NotNil(f.t, created)
		require.Equal(f.t, second.WorkflowExecution.RunId, created.WorkflowRunID)
		require.Equal(f.t, second.PreviousStartedEventId, created.NextWorkflowTask)
		late := withoutMarkers(records)[0].Producer
		require.Less(f.t, late.WorkflowTask, created.NextWorkflowTask)
		require.Nil(f.t, f.commitOf(records, late.WorkflowRunID, late.WorkflowTask))
	})
}

// A workflow's commit-mode records, carried in its workflow task completion, are appended in the
// completion's transaction, with the workflow task as their producer, and the run's append mode is
// recorded. Native only.
func (s *streamTestSuite) TestCommitModeAppendIsVisibleAtTaskCommit() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(task, f.commitModeAppend("x", "y"))

		records := f.readAllWithMarkers(ref, streamBeginning())
		data := withoutMarkers(records)
		require.Equal(f.t, []string{"x", "y"}, f.dataOf(data))
		p := data[0].Producer
		require.Equal(f.t, task.WorkflowExecution.RunId, p.WorkflowRunID)
		require.Equal(f.t, task.PreviousStartedEventId, p.WorkflowTask)
		require.Equal(f.t, task.Attempt, p.WorkflowTaskAttempt)
		require.Equal(f.t, int64(task.Attempt), f.commitOf(records, p.WorkflowRunID, p.WorkflowTask).Attempt)
		require.Equal(f.t, []streamAppendModeFixed{{task.WorkflowExecution.RunId, streamAppendModeCommit}}, f.appendModes(records))
	})
}

// Commit-mode records in the run's terminal workflow task completion are appended before the stream
// closes. Native only.
func (s *streamTestSuite) TestCommitModeAppendLandsBeforeClose() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(task, f.commitModeAppend("last"), f.completeWorkflow)

		resp := f.read(ref, streamReadRequest{From: streamBeginning()})
		require.Equal(f.t, []string{"last"}, f.dataOf(resp.Records))
		require.NotNil(f.t, resp.Close)
	})
}

// A run's first workflow append fixes its mode, which is recorded once. Native only.
func (s *streamTestSuite) TestSinkModeIsRecordedOncePerRun() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		first := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(first), 0, "", "a")
		f.completeWorkflowTask(first, f.forceNewWorkflowTask)
		second := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(second), 0, "", "b")

		records := f.readAllWithMarkers(ref, streamBeginning())
		require.Equal(f.t, []string{"a", "b"}, f.dataOf(records))
		require.Equal(f.t, []streamAppendModeFixed{{first.WorkflowExecution.RunId, streamAppendModeSink}}, f.appendModes(records))
	})
}

// Two workers of the same workflow disagree on the append mode, as in a rolling deploy: the run's
// first workflow append, sink-style, fixes its mode, and a commit-mode append from the second worker
// fails its workflow task, naming both modes. Native only.
func (s *streamTestSuite) TestCommitModeAppendAfterSinkModeFailsTask() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		first := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(first), 0, "", "sink")
		f.completeWorkflowTask(first, f.forceNewWorkflowTask)
		second := f.pollWorkflowTask(f.tv)
		err := f.tryCompleteWorkflowTask(second, f.commitModeAppend("commit"))
		require.ErrorContains(f.t, err, "SINK")
		require.ErrorContains(f.t, err, "COMMIT")
		require.Equal(f.t, []string{"sink"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
}

// As TestCommitModeAppendAfterSinkModeFailsTask, with the modes the other way round: a sink-style
// append fails, naming both modes. Native only.
func (s *streamTestSuite) TestSinkModeAppendAfterCommitModeFails() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		first := f.pollWorkflowTask(f.tv)
		f.completeWorkflowTask(first, f.commitModeAppend("commit"), f.forceNewWorkflowTask)
		second := f.pollWorkflowTask(f.tv)
		_, err := f.tryAppend(ref, workflowProducer(second), 0, "", "sink")
		require.ErrorIs(f.t, err, errStreamAppendMode)
		require.ErrorContains(f.t, err, "SINK")
		require.ErrorContains(f.t, err, "COMMIT")
		require.Equal(f.t, []string{"commit"}, f.dataOf(f.readAll(ref, streamBeginning())))
	})
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
			Records:  []streamAppendRecord{{Topic: "t", Payload: p}},
		})
		require.NoError(f.t, err)
		records := f.readAll(ref, streamBeginning())
		require.Len(f.t, records, 1)
		require.True(f.t, proto.Equal(p, records[0].Payload))
	})
}

// activityProducerNodes returns the CHASM nodes holding activity producers' state.
func (f *streamFixture) activityProducerNodes(ref streamRef) []string {
	desc, err := f.env.AdminClient().DescribeMutableState(f.ctx(), &adminservice.DescribeMutableStateRequest{
		Namespace: f.env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: ref.WorkflowID},
		Archetype: chasm.WorkflowArchetype,
	})
	require.NoError(f.t, err)
	var nodes []string
	for path := range desc.GetDatabaseMutableState().GetChasmNodes() {
		if strings.Contains(path, "Producers") && strings.Contains(path, "activity") {
			nodes = append(nodes, path)
		}
	}
	return nodes
}

func (f *streamFixture) ctx() context.Context {
	return testcontext.For(f.t)
}

// startOwner starts the workflow that owns the stream.
func (f *streamFixture) startOwner(tv *testvars.TestVars) streamRef {
	resp, err := f.env.FrontendClient().StartWorkflowExecution(f.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    f.env.Namespace().String(),
		RequestId:    tv.Any().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Identity:     tv.WorkerIdentity(),
	})
	require.NoError(f.t, err)
	return streamRef{WorkflowID: tv.WorkflowID(), FirstRunID: resp.RunId}
}

func (f *streamFixture) tryAppend(ref streamRef, p streamProducer, seq int64, topic string, data ...string) (streamAppendResponse, error) {
	records := make([]streamAppendRecord, len(data))
	for i, d := range data {
		records[i] = streamAppendRecord{Topic: topic, Payload: payload.EncodeString(d)}
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

// readAll reads, without waiting, every data record after from. It omits markers (workflow task
// commits, stream creation, append modes), which only native writes.
func (f *streamFixture) readAll(ref streamRef, from streamPosition) []streamRecord {
	return withoutMarkers(f.readAllWithMarkers(ref, from))
}

func (f *streamFixture) readAllWithMarkers(ref streamRef, from streamPosition) []streamRecord {
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

// commits reads the stream's workflow task commit records.
func (f *streamFixture) commits(ref streamRef) []*streamWorkflowTaskCommit {
	var commits []*streamWorkflowTaskCommit
	for _, r := range f.readAllWithMarkers(ref, streamBeginning()) {
		if r.WorkflowTaskCommit != nil {
			commits = append(commits, r.WorkflowTaskCommit)
		}
	}
	return commits
}

// commitOf returns the commit record of a run's workflow task, or nil.
func (f *streamFixture) commitOf(records []streamRecord, runID string, workflowTask int64) *streamWorkflowTaskCommit {
	for _, r := range records {
		if c := r.WorkflowTaskCommit; c != nil && c.WorkflowRunID == runID && c.WorkflowTask == workflowTask {
			return c
		}
	}
	return nil
}

func withoutMarkers(records []streamRecord) []streamRecord {
	var data []streamRecord
	for _, r := range records {
		if !r.isMarker() {
			data = append(data, r)
		}
	}
	return data
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
	var d string
	require.NoError(f.t, payload.Decode(r.Payload, &d))
	return d
}

// dataOf decodes the data records, omitting markers.
func (f *streamFixture) dataOf(records []streamRecord) []string {
	data := []string{}
	for _, r := range withoutMarkers(records) {
		data = append(data, f.data(r))
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
	require.NoError(f.t, f.tryCompleteWorkflowTask(task, edits...))
}

func (f *streamFixture) tryCompleteWorkflowTask(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	edits ...func(*workflowservice.RespondWorkflowTaskCompletedRequest),
) error {
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: f.env.Namespace().String(),
		TaskToken: task.TaskToken,
		Identity:  f.tv.WorkerIdentity(),
	}
	for _, edit := range edits {
		edit(req)
	}
	_, err := f.env.FrontendClient().RespondWorkflowTaskCompleted(f.ctx(), req)
	return err
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

// commitModeAppend carries records in the workflow task completion, as a worker in commit mode does.
func (f *streamFixture) commitModeAppend(data ...string) func(*workflowservice.RespondWorkflowTaskCompletedRequest) {
	return func(req *workflowservice.RespondWorkflowTaskCompletedRequest) {
		records := make([]*streamservicepb.AppendRecord, len(data))
		for i, d := range data {
			records[i] = &streamservicepb.AppendRecord{Payload: payload.EncodeString(d)}
		}
		body, err := anypb.New(&streamservicepb.CommitModeAppend{Records: records})
		require.NoError(f.t, err)
		req.Messages = append(req.Messages, &protocolpb.Message{
			Id:                 f.tv.Any().String(),
			ProtocolInstanceId: "stream",
			Body:               body,
		})
	}
}

// appendModes returns the run append modes recorded in records.
func (f *streamFixture) appendModes(records []streamRecord) []streamAppendModeFixed {
	var modes []streamAppendModeFixed
	for _, r := range records {
		if r.AppendModeFixed != nil {
			modes = append(modes, *r.AppendModeFixed)
		}
	}
	return modes
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

// resetActivityAwaitingAttempt waits until the activity's unanswered attempt has timed out and the
// given attempt is scheduled but not started, then resets the activity.
func (f *streamFixture) resetActivityAwaitingAttempt(tv *testvars.TestVars, attempt int32) {
	await.Require(f.ctx(), f.t, func(t *await.T) {
		resp, err := f.env.FrontendClient().DescribeWorkflowExecution(f.ctx(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: f.env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
		})
		require.NoError(t, err)
		require.Len(t, resp.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, resp.PendingActivities[0].State)
		require.Equal(t, attempt, resp.PendingActivities[0].Attempt)
	}, 2*streamActivityStartToClose+5*time.Second, 100*time.Millisecond)
	_, err := f.env.FrontendClient().ResetActivity(f.ctx(), &workflowservice.ResetActivityRequest{
		Namespace: f.env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
		Activity:  &workflowservice.ResetActivityRequest_Id{Id: tv.ActivityID()},
	})
	require.NoError(f.t, err)
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
