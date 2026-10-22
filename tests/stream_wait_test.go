package tests

// Shared stream tests of what workflow readers need: read bounds, the stream's mode, and waiting
// with the stream service's Wait operation, on a stream in the server or, through its notifier, on
// a stream in an external store. Each test's comment states the guarantee it checks and the
// backends it holds on.

import (
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streamservicepb "go.temporal.io/api/nexusservices/streamservice/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payload"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A read with Until returns no record after the one at that cursor, so a workflow's replay can
// re-read exactly a recorded range. Every backend.
func (s *streamTestSuite) TestReadStopsAtUntil() {
	s.forEachBackendAndOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		cursors := f.append(ref, clientProducer("a", 1), 0, "", "x", "y", "z").Cursors
		resp := f.read(ref, streamReadRequest{From: streamBeginning(), Until: cursors[1]})
		require.Equal(f.t, []string{"x", "y"}, f.dataOf(resp.Records))
		require.Equal(f.t, []string{"z"}, f.dataOf(f.read(ref, streamReadRequest{From: streamAfter(resp.Cursor), Until: cursors[2]}).Records))
	})
}

// A read with MaxBytes returns records in order until the next would exceed it, and its cursor
// resumes at that record; a record that alone exceeds it fails the read as too large. Every
// backend.
func (s *streamTestSuite) TestReadMaxBytesResumesAtFirstRecordLeftOut() {
	s.forEachBackendAndOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "xxxx", "yyyy", "zzzz")
		size := proto.Size(payload.EncodeString("xxxx"))
		resp := f.read(ref, streamReadRequest{From: streamBeginning(), MaxBytes: 2*size + size/2})
		require.Equal(f.t, []string{"xxxx", "yyyy"}, f.dataOf(resp.Records))
		require.Equal(f.t, []string{"zzzz"}, f.dataOf(f.read(ref, streamReadRequest{From: streamAfter(resp.Cursor), MaxBytes: size}).Records))
		_, err := f.b.Read(f.ctx(), ref, streamReadRequest{From: streamBeginning(), MaxBytes: size - 1})
		require.ErrorIs(f.t, err, errStreamTooLarge)
	})
}

// A read response states the stream's mode: unknown while a workflow owner's first workflow task
// has not completed, then sink unless that run declared commit mode; always sink for a Standalone
// Activity's stream and a standalone stream. External stores are always sink-mode, so native only.
func (s *streamTestSuite) TestReadResponseStatesStreamMode() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		if f.owner != workflowOwner {
			require.Equal(f.t, streamModeSink, f.read(ref, streamReadRequest{From: streamBeginning()}).Mode)
			return
		}
		require.Equal(f.t, streamModeUnknown, f.read(ref, streamReadRequest{From: streamBeginning()}).Mode)
		f.completeWorkflowTask(f.pollWorkflowTask(f.tv))
		require.Equal(f.t, streamModeSink, f.read(ref, streamReadRequest{From: streamBeginning()}).Mode)

		tv := f.tv.WithWorkflowIDNumber(2).WithTaskQueueNumber(2)
		declared := f.startOwner(tv)
		f.completeWorkflowTask(f.pollWorkflowTask(tv), f.declareStreamMode(streamservicepb.STREAM_MODE_COMMIT))
		require.Equal(f.t, streamModeCommit, f.read(declared, streamReadRequest{From: streamBeginning()}).Mode)
	})
}

// A run's declaration of commit mode covers every one of its streams, including ones it has not
// created yet: their reads state commit mode, and a sink-style append to one of them fails. Native
// only.
func (s *streamTestSuite) TestModeDeclarationCoversEveryStreamOfRun() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.completeWorkflowTask(f.pollWorkflowTask(f.tv), f.declareStreamMode(streamservicepb.STREAM_MODE_COMMIT), f.forceNewWorkflowTask)
		other := named(ref, "other")
		require.Equal(f.t, streamModeCommit, f.read(other, streamReadRequest{From: streamBeginning()}).Mode)
		_, err := f.tryAppend(other, workflowProducer(f.pollWorkflowTask(f.tv)), 0, "", "x")
		require.ErrorIs(f.t, err, errStreamAppendMode)
	})
}

// A read response states when the execution that holds the stream closed, and the namespace's
// retention, after which the stream is removed, so that a reader knows how long its range can be
// replayed. Native only.
func (s *streamTestSuite) TestReadResponseReportsOwnerCloseAndRetention() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		require.Nil(f.t, f.read(ref, streamReadRequest{From: streamBeginning()}).OwnerCloseTime)
		f.endHolder(ref)
		var resp streamReadResponse
		await.Require(f.ctx(), f.t, func(t *await.T) {
			resp = f.read(ref, streamReadRequest{From: streamBeginning()})
			require.NotNil(t, resp.OwnerCloseTime)
		}, 10*time.Second, 100*time.Millisecond)
		require.WithinDuration(f.t, time.Now(), *resp.OwnerCloseTime, time.Minute)
		ns, err := f.env.FrontendClient().DescribeNamespace(f.ctx(), &workflowservice.DescribeNamespaceRequest{Namespace: f.env.Namespace().String()})
		require.NoError(f.t, err)
		require.Equal(f.t, ns.Config.WorkflowExecutionRetentionTtl.AsDuration(), resp.Retention)
	})
}

// A Wait after the stream's head completes on the next matching append, and not on an append of
// another topic. Native only: a Wait on a stream in an external store waits for Notify instead.
func (s *streamTestSuite) TestWaitCompletesOnNextMatchingAppend() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "progress", "p1").Cursors[0]
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head, Topics: []string{"progress"}})
		r.awaitStarted()
		f.append(ref, clientProducer("a", 1), 1, "delta", "d1")
		r.requireNotCompleted(2 * time.Second)
		f.append(ref, clientProducer("a", 1), 2, "progress", "p2")
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A Wait on one of an owner's streams is not completed by an append to another of its streams.
// Native only.
func (s *streamTestSuite) TestWaitIgnoresOtherStreamsOfOwner() {
	s.nativeOnlyEachExecutionOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head})
		r.awaitStarted()
		f.append(named(ref, "other"), clientProducer("a", 1), 0, "", "y")
		r.requireNotCompleted(2 * time.Second)
		f.append(ref, clientProducer("a", 1), 1, "", "z")
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A Wait on a stream that its owner has not created yet completes when the stream is created. The
// Wait does not create the stream. Native only.
func (s *streamTestSuite) TestWaitOnStreamNotCreatedYetCompletesOnCreation() {
	s.nativeOnlyEachExecutionOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		if f.owner == workflowOwner {
			f.completeWorkflowTask(f.pollWorkflowTask(f.tv), f.forceNewWorkflowTask)
		}
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{})
		r.awaitStarted()
		require.False(f.t, f.read(ref, streamReadRequest{From: streamBeginning()}).Exists)
		_, err := f.b.Create(f.ctx(), ref, streamCreateRequest{})
		require.NoError(f.t, err)
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A pending Wait completes when the owner's workflow task that wrote a record the reader read
// completes: the task's commit record decides that record, which the reader may be holding as
// provisional. Native only.
func (s *streamTestSuite) TestWaitCompletesOnCommitOfTaskThatWroteMatchingRecord() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "progress", "p1")
		head := f.read(ref, streamReadRequest{From: streamBeginning()}).Cursor
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head, Topics: []string{"progress"}})
		r.awaitStarted()
		f.completeWorkflowTask(task)
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A pending Wait does not complete when the owner completes a workflow task that wrote no record
// matching the Wait's topics: that task's commit record decides nothing the reader holds. Without
// this, every workflow task of a busy owner would wake every idle reader. Native only.
func (s *streamTestSuite) TestWaitIgnoresCommitOfTaskThatWroteNoMatchingRecord() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "progress", "p1")
		f.completeWorkflowTask(task, f.forceNewWorkflowTask)
		head := f.read(ref, streamReadRequest{From: streamBeginning()}).Cursor
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head, Topics: []string{"progress"}})
		r.awaitStarted()
		f.completeWorkflowTask(f.pollWorkflowTask(f.tv), f.forceNewWorkflowTask)
		task = f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "delta", "d1")
		f.completeWorkflowTask(task, f.forceNewWorkflowTask)
		r.requireNotCompleted(2 * time.Second)
		f.append(ref, clientProducer("a", 1), 0, "progress", "p2")
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A Wait whose cursor is before the commit record of a task that wrote a matching record completes
// at once: the record it decides was read before the Wait. Native only.
func (s *streamTestSuite) TestWaitCompletesAtOnceForCommitOfTaskThatWroteMatchingRecord() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "progress", "p1")
		head := f.read(ref, streamReadRequest{From: streamBeginning()}).Cursor
		f.completeWorkflowTask(task)
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head, Topics: []string{"progress"}})
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
		r.requireNotStarted()
	})
}

// A Wait whose cursor is before only commit records of tasks that wrote no matching record
// subscribes. Native only.
func (s *streamTestSuite) TestWaitSubscribesPastCommitOfTaskThatWroteNoMatchingRecord() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "progress", "p1")
		f.completeWorkflowTask(task, f.forceNewWorkflowTask)
		head := f.read(ref, streamReadRequest{From: streamBeginning()}).Cursor
		task = f.pollWorkflowTask(f.tv)
		f.append(ref, workflowProducer(task), 0, "delta", "d1")
		f.completeWorkflowTask(task)
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head, Topics: []string{"progress"}})
		r.awaitStarted()
		r.requireNotCompleted(2 * time.Second)
	})
}

// A Wait completes at once, without a subscription, when a matching record after its cursor is
// already there. Native only.
func (s *streamTestSuite) TestWaitCompletesAtOnceWhenRecordExists() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		if f.owner == workflowOwner {
			f.completeWorkflowTask(f.pollWorkflowTask(f.tv))
		}
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{})
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
		r.requireNotStarted()
	})
}

// A pending Wait completes when the stream closes: when its owner ends, or, for a standalone
// stream, when a caller closes it. Native only.
func (s *streamTestSuite) TestWaitCompletesOnClose() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head})
		r.awaitStarted()
		f.endHolder(ref)
		require.Equal(f.t, streamservicepb.WAKE_REASON_CLOSED, r.awaitCompleted().Reason)
	})
}

// A Wait on an owner, or a standalone stream, that does not exist completes at once, as not found.
// Native only.
func (s *streamTestSuite) TestWaitOnMissingOwnerCompletesAsNotFound() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		r := f.startReader()
		r.wait(f.unstartedRef(f.tv), &streamservicepb.WaitRequest{})
		require.Equal(f.t, streamservicepb.WAKE_REASON_OWNER_NOT_FOUND, r.awaitCompleted().Reason)
	})
}

// A Wait on a workflow's stream whose mode is not yet known completes when the owner's first
// workflow task completes, which fixes the mode, whether or not the stream exists yet. Native only.
func (s *streamTestSuite) TestWaitWhileModeUnknownCompletesWhenOwnerFirstTaskCompletes() {
	s.nativeOnly(func(f *streamFixture) {
		for i, exists := range []bool{false, true} {
			tv := f.tv.WithWorkflowIDNumber(10 + i).WithTaskQueueNumber(10 + i)
			ref := f.startOwner(tv)
			if exists {
				f.append(ref, clientProducer("a", 1), 0, "", "x")
			}
			r := f.startReaderNumbered(100 + i)
			r.wait(ref, &streamservicepb.WaitRequest{})
			r.awaitStarted()
			f.completeWorkflowTask(f.pollWorkflowTask(tv))
			r.awaitCompleted()
			require.Equal(f.t, streamModeSink, f.read(ref, streamReadRequest{From: streamBeginning()}).Mode)
		}
	})
}

// A pending Wait completes when its workflow owner continues as new, so that the reader reads the
// new run. Native only.
func (s *streamTestSuite) TestWaitCompletesWhenOwnerContinuesAsNew() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		task := f.pollWorkflowTask(f.tv)
		head := f.append(ref, workflowProducer(task), 0, "", "x").Cursors[0]
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head})
		r.awaitStarted()
		f.completeWorkflowTask(task, f.continueAsNew)
		require.Equal(f.t, streamservicepb.WAKE_REASON_OWNER_ENDED, r.awaitCompleted().Reason)
	})
}

// A canceled Wait removes its subscription. Native only.
func (s *streamTestSuite) TestCanceledWaitRemovesSubscription() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		r := f.startReader()
		scheduled := r.wait(ref, &streamservicepb.WaitRequest{After: head})
		r.awaitStarted()
		require.Len(f.t, f.subscriptionNodes(ref, false), 1)
		r.cancel(scheduled)
		r.awaitEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED)
		require.Empty(f.t, f.subscriptionNodes(ref, false))
	})
}

// A Wait completes as expired when its subscription's lifetime passes. Native only.
func (s *streamTestSuite) TestWaitExpires() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head, Lifetime: durationpb.New(2 * time.Second)})
		r.awaitStarted()
		require.Equal(f.t, streamservicepb.WAKE_REASON_EXPIRED, r.awaitCompleted().Reason)
	})
}

// A subscription whose reader has closed is removed after it fires: its delivery fails as not
// found, and the stream's next append removes it. Native only.
func (s *streamTestSuite) TestSubscriptionOfClosedReaderIsRemoved() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{After: head})
		r.awaitStarted()
		_, err := f.env.FrontendClient().TerminateWorkflowExecution(f.ctx(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         f.env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: r.tv.WorkflowID()},
		})
		require.NoError(f.t, err)
		f.append(ref, clientProducer("a", 1), 1, "", "y")
		seq := int64(2)
		await.Require(f.ctx(), f.t, func(t *await.T) {
			f.append(ref, clientProducer("a", 1), seq, "", "z")
			seq++
			require.Empty(t, f.subscriptionNodes(ref, false))
		}, 30*time.Second, 500*time.Millisecond)
	})
}

// A Wait made with no callback, as a direct call is, fails before storing anything, so it leaves
// no subscription that nothing can deliver. Native only.
func (s *streamTestSuite) TestWaitWithoutCallbackStoresNothing() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		head := f.append(ref, clientProducer("a", 1), 0, "", "x").Cursors[0]
		err := f.native().call(f.ctx(), "Wait", &streamservicepb.WaitRequest{Stream: nativeRef(ref), After: head}, &streamservicepb.WaitResponse{})
		require.ErrorIs(f.t, err, errStreamInvalidArg)
		require.Empty(f.t, f.subscriptionNodes(ref, false))
	})
}

// For a stream in an external store, a Wait at the notifier position that NotifierState returned
// completes on the next Notify of the stream. The notifier is held by the owner, or by a notifier
// execution with a standalone stream's ID; these tests call it directly, as the Redis backend does.
func (s *streamTestSuite) TestNotifyCompletesWaitAtCurrentPosition() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		pos := f.notifierState(ref).Position
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{Notifier: pos})
		r.awaitStarted()
		require.Len(f.t, f.subscriptionNodes(ref, true), 1)
		f.notify(ref)
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A Notify of one stream does not complete a Wait on another stream of the same owner.
func (s *streamTestSuite) TestNotifyWakesOnlyItsStream() {
	s.nativeOnlyEachExecutionOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		pos := f.notifierState(ref).Position
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{Notifier: pos})
		r.awaitStarted()
		f.notify(named(ref, "other"))
		r.requireNotCompleted(2 * time.Second)
		f.notify(ref)
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
	})
}

// A Wait at a notifier position that a Notify has since moved past completes at once, so a Notify
// between a reader's read and its Wait is not lost.
func (s *streamTestSuite) TestWaitAtStaleNotifierPositionCompletesAtOnce() {
	s.nativeOnlyEachOwner(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		pos := f.notifierState(ref).Position
		f.notify(ref)
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{Notifier: pos})
		require.Equal(f.t, streamservicepb.WAKE_REASON_APPENDED, r.awaitCompleted().Reason)
		r.requireNotStarted()
	})
}

// A Notify to an owner that does not exist succeeds and does nothing, so producers whose owner has
// not started are unaffected; and a Notify with no subscriptions writes nothing.
func (s *streamTestSuite) TestNotifyWithoutSubscriptionsWritesNothing() {
	s.nativeOnlyEachExecutionOwner(func(f *streamFixture) {
		missing := f.unstartedRef(f.tv)
		if f.owner == activityOwner {
			missing.ActivityRunID = uuid.NewString()
		} else {
			missing.FirstRunID = uuid.NewString()
		}
		f.notify(missing)
		ref := f.startOwner(f.tv)
		before := f.stateTransitionCount(ref)
		f.notify(ref)
		f.notify(ref)
		require.Equal(f.t, before, f.stateTransitionCount(ref))
	})
}

// A Notify of a standalone stream with no notifier execution succeeds and creates none.
func (s *streamTestSuite) TestNotifyOfStandaloneStreamCreatesNoNotifier() {
	s.runOnBackendWithOwner(0, standaloneOwner, func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.notify(ref)
		_, err := f.env.AdminClient().DescribeMutableState(f.ctx(), f.describeHolder(ref, true))
		var notFound *serviceerror.NotFound
		require.ErrorAs(f.t, err, &notFound)
	})
}

// A Wait on the notifier completes when the owner ends, with the state it ended in, so a reader of
// an external store can tell an owner that ended without closing the stream from a closed stream.
func (s *streamTestSuite) TestNotifierWaitCompletesWithOwnerEnd() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		pos := f.notifierState(ref).Position
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{Notifier: pos})
		r.awaitStarted()
		_, err := f.env.FrontendClient().TerminateWorkflowExecution(f.ctx(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         f.env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: ref.WorkflowID},
		})
		require.NoError(f.t, err)
		resp := r.awaitCompleted()
		require.Equal(f.t, streamservicepb.WAKE_REASON_OWNER_ENDED, resp.Reason)
		require.Equal(f.t, streamservicepb.CLOSE_STATE_TERMINATED, resp.OwnerEnd)
		require.Equal(f.t, streamservicepb.CLOSE_STATE_TERMINATED, f.notifierState(ref).OwnerEnd)
	})
}

// With history.enableNativeStreams off, the server stores no streams: Create, Append, Read and
// Close fail as unimplemented, and so do commit-mode appends in a workflow task completion. Native
// only.
func (s *streamTestSuite) TestNativeStreamsSwitchedOff() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		var unimplemented *serviceerror.Unimplemented
		_, err := f.b.Create(f.ctx(), ref, streamCreateRequest{})
		require.ErrorAs(f.t, err, &unimplemented)
		_, err = f.tryAppend(ref, clientProducer("a", 1), 0, "", "x")
		require.ErrorAs(f.t, err, &unimplemented)
		_, err = f.b.Read(f.ctx(), ref, streamReadRequest{From: streamBeginning()})
		require.ErrorAs(f.t, err, &unimplemented)
		require.Error(f.t, f.tryCompleteWorkflowTask(f.pollWorkflowTask(f.tv), f.commitModeAppend("x")))
		require.Equal(f.t, "BAD_STREAM_APPEND", f.lastWorkflowTaskFailure(ref).GetFailure().GetApplicationFailureInfo().GetType())
	}, testcore.WithDynamicConfig(dynamicconfig.EnableNativeStreams, false))
}

// With history.enableStreamWorkflowReads off, Wait, NotifierState and Notify fail as
// unimplemented, and native streams work as before. Native only.
func (s *streamTestSuite) TestStreamWorkflowReadsSwitchedOff() {
	s.nativeOnly(func(f *streamFixture) {
		ref := f.startOwner(f.tv)
		f.append(ref, clientProducer("a", 1), 0, "", "x")
		require.Equal(f.t, []string{"x"}, f.dataOf(f.readAll(ref, streamBeginning())))
		var unimplemented *serviceerror.Unimplemented
		err := f.native().call(f.ctx(), "NotifierState", &streamservicepb.NotifierStateRequest{Stream: nativeRef(ref)}, &streamservicepb.NotifierStateResponse{})
		require.ErrorAs(f.t, err, &unimplemented)
		err = f.native().call(f.ctx(), "Notify", &streamservicepb.NotifyRequest{Stream: nativeRef(ref)}, &streamservicepb.NotifyResponse{})
		require.ErrorAs(f.t, err, &unimplemented)
		r := f.startReader()
		r.wait(ref, &streamservicepb.WaitRequest{})
		require.NotNil(f.t, r.awaitEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED))
	}, testcore.WithDynamicConfig(dynamicconfig.EnableStreamWorkflowReads, false))
}

// streamReader is a workflow that calls the stream service's Wait operation, as a workflow reader
// of a stream does when a read returns nothing.
type streamReader struct {
	f  *streamFixture
	tv *testvars.TestVars
}

func (f *streamFixture) startReader() *streamReader {
	return f.startReaderNumbered(100)
}

func (f *streamFixture) startReaderNumbered(n int) *streamReader {
	tv := f.tv.WithWorkflowIDNumber(n).WithTaskQueueNumber(n)
	_, err := f.env.FrontendClient().StartWorkflowExecution(f.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    f.env.Namespace().String(),
		RequestId:    tv.Any().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Identity:     tv.WorkerIdentity(),
	})
	require.NoError(f.t, err)
	return &streamReader{f: f, tv: tv}
}

// wait completes the reader's next workflow task with a Wait on ref, and returns the operation's
// scheduled event ID.
func (r *streamReader) wait(ref streamRef, req *streamservicepb.WaitRequest) int64 {
	req.Stream = nativeRef(ref)
	input, err := sdkconverter.PreferProtoDataConverter.ToPayload(req)
	require.NoError(r.f.t, err)
	r.f.completeWorkflowTask(r.f.pollWorkflowTask(r.tv), func(c *workflowservice.RespondWorkflowTaskCompletedRequest) {
		c.Commands = append(c.Commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               commonnexus.SystemEndpoint,
					Service:                nativeStreamService,
					Operation:              "Wait",
					Input:                  input,
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
				},
			},
		})
	})
	return r.awaitEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED).EventId
}

// cancel completes the reader's next workflow task with a request to cancel the operation.
func (r *streamReader) cancel(scheduledEventID int64) {
	r.f.completeWorkflowTask(r.f.pollWorkflowTask(r.tv), func(c *workflowservice.RespondWorkflowTaskCompletedRequest) {
		c.Commands = append(c.Commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{ScheduledEventId: scheduledEventID},
			},
		})
	})
}

func (r *streamReader) events() []*historypb.HistoryEvent {
	resp, err := r.f.env.FrontendClient().GetWorkflowExecutionHistory(r.f.ctx(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: r.f.env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: r.tv.WorkflowID()},
	})
	require.NoError(r.f.t, err)
	return resp.History.Events
}

func (r *streamReader) find(t enumspb.EventType) *historypb.HistoryEvent {
	for _, e := range r.events() {
		if e.EventType == t {
			return e
		}
	}
	return nil
}

func (r *streamReader) awaitEvent(t enumspb.EventType) *historypb.HistoryEvent {
	var event *historypb.HistoryEvent
	await.Require(r.f.ctx(), r.f.t, func(at *await.T) {
		event = r.find(t)
		require.NotNil(at, event, "no %v event", t)
	}, 30*time.Second, 100*time.Millisecond)
	return event
}

func (r *streamReader) awaitStarted() {
	r.awaitEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
}

func (r *streamReader) requireNotStarted() {
	require.Nil(r.f.t, r.find(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED))
}

// awaitCompleted returns the Wait operation's result.
func (r *streamReader) awaitCompleted() *streamservicepb.WaitResponse {
	event := r.awaitEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	resp := &streamservicepb.WaitResponse{}
	require.NoError(r.f.t, sdkconverter.PreferProtoDataConverter.FromPayload(event.GetNexusOperationCompletedEventAttributes().GetResult(), resp))
	return resp
}

func (r *streamReader) requireNotCompleted(d time.Duration) {
	time.Sleep(d) //nolint:forbidigo // the test checks that nothing happens for a while
	require.Nil(r.f.t, r.find(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED))
}

func (f *streamFixture) native() *nativeStreamBackend {
	return f.b.streamBackend.(*nativeStreamBackend)
}

// endHolder ends the execution that holds the stream: its owner, or, for a standalone stream, the
// stream's own execution, by closing the stream.
func (f *streamFixture) endHolder(ref streamRef) {
	if f.owner == standaloneOwner {
		require.NoError(f.t, f.b.Close(f.ctx(), ref, streamCloseRequest{Close: streamClose{Status: streamClosedCompleted}}))
		return
	}
	f.endOwner(f.tv)
}

// describeHolder is the request that describes the execution that holds the stream's
// subscriptions: its owner, or, for a standalone stream, its own execution, or with notifier set,
// the notifier execution that holds Waits on the stream in an external store.
func (f *streamFixture) describeHolder(ref streamRef, notifier bool) *adminservice.DescribeMutableStateRequest {
	req := &adminservice.DescribeMutableStateRequest{
		Namespace: f.env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: ref.WorkflowID},
		Archetype: chasm.WorkflowArchetype,
	}
	switch {
	case ref.ActivityID != "":
		req.Execution = &commonpb.WorkflowExecution{WorkflowId: ref.ActivityID, RunId: ref.ActivityRunID}
		req.Archetype = activity.Archetype
	case ref.StreamID != "" && notifier:
		req.Execution = &commonpb.WorkflowExecution{WorkflowId: ref.StreamID}
		req.Archetype = chasm.FullyQualifiedName("stream", "notifier")
	case ref.StreamID != "":
		req.Execution = &commonpb.WorkflowExecution{WorkflowId: ref.StreamID}
		req.Archetype = chasm.FullyQualifiedName("stream", "standalone")
	default:
	}
	return req
}

// subscriptionNodes returns the CHASM nodes of the stream's subscriptions: of the native stream,
// or with notifier set, of its notifier.
func (f *streamFixture) subscriptionNodes(ref streamRef, notifier bool) []string {
	desc, err := f.env.AdminClient().DescribeMutableState(f.ctx(), f.describeHolder(ref, notifier))
	require.NoError(f.t, err)
	var nodes []string
	for path := range desc.GetDatabaseMutableState().GetChasmNodes() {
		segments := strings.Split(path, "$")
		if strings.HasPrefix(segments[len(segments)-1], "Subscriptions#") {
			nodes = append(nodes, path)
		}
	}
	return nodes
}

func (f *streamFixture) notifierState(ref streamRef) *streamservicepb.NotifierStateResponse {
	out := &streamservicepb.NotifierStateResponse{}
	require.NoError(f.t, f.native().call(f.ctx(), "NotifierState", &streamservicepb.NotifierStateRequest{Stream: nativeRef(ref)}, out))
	return out
}

func (f *streamFixture) notify(ref streamRef) {
	require.NoError(f.t, f.native().call(f.ctx(), "Notify", &streamservicepb.NotifyRequest{Stream: nativeRef(ref)}, &streamservicepb.NotifyResponse{}))
}

// stateTransitionCount returns the number of transactions on the stream's owner.
func (f *streamFixture) stateTransitionCount(ref streamRef) int64 {
	desc, err := f.env.AdminClient().DescribeMutableState(f.ctx(), f.describeHolder(ref, false))
	require.NoError(f.t, err)
	return desc.GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount()
}

// declareStreamMode adds a declaration of the run's streams' mode to a workflow task completion.
func (f *streamFixture) declareStreamMode(mode streamservicepb.StreamMode) func(*workflowservice.RespondWorkflowTaskCompletedRequest) {
	return func(req *workflowservice.RespondWorkflowTaskCompletedRequest) {
		body, err := anypb.New(&streamservicepb.StreamModeDeclaration{Mode: mode})
		require.NoError(f.t, err)
		req.Messages = append(req.Messages, &protocolpb.Message{
			Id:                 f.tv.Any().String(),
			ProtocolInstanceId: "stream",
			Body:               body,
		})
	}
}
