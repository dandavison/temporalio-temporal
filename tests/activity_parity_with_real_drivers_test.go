package tests

// SAA↔WFA parity repros. For each behavior at the intersection of the standalone activity (SAA) and the
// workflow activity (WFA), a test drives the same trace through both surfaces as "WorkflowActivity" and
// "StandaloneActivity" subtests, and asserts the same public activity info.
//
// There is no oracle. Each `want` encodes how the product should behave, not what either implementation
// currently does, and both subtests are checked against it. A failure on either — or both — is useful
// information. Never adjust a `want` to match observed behavior; if the intended behavior is unclear,
// stop and resolve that instead.
//
// The drivers are activity_standalone_driver.go and activity_workflow_driver.go, over the event
// vocabulary in chasm/lib/activity/model. main carries the same repros driven by small ad-hoc
// per-surface drivers, in tests/activity_parity_test.go.
//
// These live on standaloneActivityTestSuite because its env enables the standalone activity. WFA needs
// nothing special, so one SAA-enabled env drives both.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/testcontext"
)

// TestWFASAAStartToCloseTimeout ports a slice of Test_ActivityTimeouts: a started attempt exceeds its
// StartToClose timeout and, with no retries left, the activity ends TIMED_OUT with the StartToClose
// TimeoutType.
//
// The failure message is deliberately not asserted: SAA carries a proto message and WFA's SDK
// TimeoutError formats its own, so the strings differ by construction. TimeoutType is the stable
// cross-surface discriminant.
func (s *standaloneActivityTestSuite) TestWFASAAStartToCloseTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, model.StartToCloseElapses}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 1)
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 1})
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAScheduleToCloseTimeout ports the schedule-to-close slice of Test_ActivityTimeouts: the
// activity is started, then its ScheduleToClose deadline elapses while it runs, so it ends TIMED_OUT
// with the ScheduleToClose TimeoutType. The trace polls first because a never-started activity that
// hits the deadline times out as ScheduleToStart instead, on both surfaces.
func (s *standaloneActivityTestSuite) TestWFASAAScheduleToCloseTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, {Kind: model.ScheduleToCloseElapsesKind}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 1)
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 1, HasScheduleToClose: true})
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAATimeoutPreservesUnderlyingFailureCause ports TestTimeoutPreservesUnderlyingFailureCause:
// when a timeout closes an activity whose retries were driven by an application failure, the terminal
// TimedOut failure must chain that application failure as its Cause, so an SDK can surface the real
// failure. See mutable_state_impl.go AddActivityTaskTimedOutEvent and temporalio/temporal#3667.
func (s *standaloneActivityTestSuite) TestWFASAATimeoutPreservesUnderlyingFailureCause() {
	env := s.newTestEnv()

	// The application failure driven on attempt 1; see saaFailure. The terminal timeout must chain it
	// verbatim, both Type and Message.
	wantCause := failureCause{Type: "drive", Message: "drive"}

	// assertCausePreserved drives the trace on both surfaces and asserts each ends TIMED_OUT with the given
	// timeout type, chaining wantCause.
	assertCausePreserved := func(t *testing.T, maxAttempts int32, trace []model.Event, timeoutType enumspb.TimeoutType) {
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: timeoutType.String()}
		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriverDeclarative(t, env, maxAttempts)
			d.shortTimeout = saaTimeoutIn(trace)
			a := d.driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), "the terminal timeout must chain the underlying application failure as its Cause")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			cfg := model.Config{MaxAttempts: maxAttempts}
			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				cfg.HasScheduleToClose = true
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				cfg.HasHeartbeat = true
			}
			d := newSAADriverDeclarative(t, env, cfg)
			d.shortTimeout = saaTimeoutIn(trace)
			a := d.driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), "the terminal timeout must chain the underlying application failure as its Cause")
		})
	}

	// Retries exhausted by a StartToClose timeout on the final attempt (attempt 1 failed retryably).
	s.T().Run("StartToClose", func(t *testing.T) {
		assertCausePreserved(t, 2, []model.Event{model.Poll, model.FailRetryably, model.Poll, model.StartToCloseElapses}, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	// Retries exhausted by a Heartbeat timeout on the final attempt: the attempt starts but never
	// heartbeats. A distinct code path that must chain the same cause.
	s.T().Run("Heartbeat", func(t *testing.T) {
		assertCausePreserved(t, 2, []model.Event{model.Poll, model.FailRetryably, model.Poll, {Kind: model.HeartbeatElapsesKind}}, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry. A third code path.
	s.T().Run("ScheduleToClose", func(t *testing.T) {
		assertCausePreserved(t, 0, []model.Event{model.Poll, model.FailRetryably, {Kind: model.ScheduleToCloseElapsesKind}}, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	})
}

// TestWFASAATimeoutTypeOnRetryDeadline ports the HeartbeatWithScheduleToClose slice of
// Test_ActivityTimeouts: a heartbeat timeout fires on a started attempt, but the retry interval cannot
// fit before the schedule-to-close deadline, so retries are given up and the terminal timeout is
// reported as ScheduleToClose rather than Heartbeat.
func (s *standaloneActivityTestSuite) TestWFASAATimeoutTypeOnRetryDeadline() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, {Kind: model.HeartbeatElapsesKind}}
	// Heartbeat fires at ~2s; the 30s retry cannot fit before the 10s schedule-to-close deadline.
	const retryInterval, scheduleToClose = 30 * time.Second, 10 * time.Second
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 2)
		d.retryInterval = retryInterval
		d.scheduleToClose = scheduleToClose
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 2, HasHeartbeat: true, HasScheduleToClose: true})
		d.retryInterval = retryInterval
		d.scheduleToClose = scheduleToClose
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAQueuedRetryInterval covers the queued-but-not-started window: SCHEDULED on attempt 2, after
// the backoff elapsed and the retry was dispatched to Matching. Nothing is backing off there, so no
// current retry interval should be reported. Uses a non-constant backoff, so it is a distinct config
// from the constant-interval tests.

func (s *standaloneActivityTestSuite) TestWFASAAQueuedRetryInterval() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses}
	const initialInterval, maxInterval = 5 * time.Second, 30 * time.Second
	want := activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = initialInterval
		d.backoffCoefficient = 2.0
		d.maxRetryInterval = maxInterval
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = initialInterval
		d.backoffCoefficient = 2.0
		d.maxRetryInterval = maxInterval
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAAHeartBeat ports the core of TestActivityHeartBeatWorkflow_Success: a worker polls the
// activity and heartbeats a checkpoint payload, the checkpoint is readable while it runs, then the
// worker completes it. The original also asserts the workflow history-event shape, which is not part of
// the cross-surface contract.
var heartbeatWant = []byte(`"hb"`) // == saaHeartbeatDetails

func (s *standaloneActivityTestSuite) TestWFASAAHeartBeat() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, {Kind: model.HeartbeatKind}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = 2 * time.Second
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, want, a.terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = 2 * time.Second
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, want, a.terminal(t))
	})
}

// TestWFASAARetry: an attempt fails retryably, the backoff elapses, the next attempt fails
// non-retryably, and the activity ends FAILED with the application failure type. The original also
// covers a schedule-to-start timeout on a no-worker queue (a separate scenario here) and asserts the
// workflow history-event shape.
func (s *standaloneActivityTestSuite) TestWFASAARetry() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.FailNonRetryably}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: "drive"}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = 2 * time.Second
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = 2 * time.Second
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAHeartbeatTimeout ports the core of TestActivityHeartBeatWorkflow_Timeout: a started attempt
// heartbeats nothing within its HeartbeatTimeout and, with no retries left, ends TIMED_OUT with the
// Heartbeat TimeoutType.
func (s *standaloneActivityTestSuite) TestWFASAAHeartbeatTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, {Kind: model.HeartbeatElapsesKind}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 1)
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 1, HasHeartbeat: true})
		d.shortTimeout = saaTimeoutIn(trace)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAACancel ports the core of TestTryActivityCancellationFromWorkflow: a running activity is
// cancel-requested, the worker acknowledges with RespondActivityTaskCanceled, and the activity ends
// CANCELED. The RequestCancel event realizes differently per surface — SAA's direct
// RequestCancelActivityExecution RPC vs WFA's signal-then-RequestCancelActivity — which is what the
// drivers hide. The original also asserts that the workflow observed the cancellation.
func (s *standaloneActivityTestSuite) TestWFASAACancel() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, model.RequestCancel, {Kind: model.RespondCanceledKind}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 1)
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 1})
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAARetryAfterFail: attempt 1 fails retryably, the backoff elapses, attempt 2 starts. The
// activity is then running, with no pending retry, so there is no current retry interval and no
// next-attempt schedule time.

func (s *standaloneActivityTestSuite) TestWFASAARetryAfterFail() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                2,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = 2 * time.Second
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = 2 * time.Second
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAABackingOff: attempt 1 fails retryably and is observed during the backoff window, so the next
// dispatch is still in the future. The retry is pending, so both the current retry interval and the
// next-attempt schedule time are populated. The long interval keeps the window open across the describe.

func (s *standaloneActivityTestSuite) TestWFASAABackingOff() {
	env := s.newTestEnv()
	backingOffInterval := 30 * time.Second
	trace := []model.Event{model.Poll, model.FailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   backingOffInterval,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = backingOffInterval
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = backingOffInterval
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAANextRetryDelayOverride: the worker fails with a next_retry_delay that overrides the policy
// backoff, observed during the override-length window. The reported current retry interval must be the
// override, not the policy's interval.
func (s *standaloneActivityTestSuite) TestWFASAANextRetryDelayOverride() {
	env := s.newTestEnv()
	nextRetryDelayOverride := 30 * time.Second
	trace := []model.Event{model.Poll, model.FailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   nextRetryDelayOverride,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = 5 * time.Second
		d.nextRetryDelay = nextRetryDelayOverride
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = 5 * time.Second
		d.nextRetryDelay = nextRetryDelayOverride
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAAFirstAttemptStarted: a worker polls the first attempt, which is now running. No attempt has
// failed, so there is no current retry interval and no next-attempt schedule time.
func (s *standaloneActivityTestSuite) TestWFASAAFirstAttemptStarted() {
	env := s.newTestEnv()
	trace := []model.Event{model.Poll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                1,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriverDeclarative(t, env, 3)
		d.retryInterval = 2 * time.Second
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
		d.retryInterval = 2 * time.Second
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval sweeps NextAttemptScheduleTime and
// CurrentRetryInterval across the activity lifecycle. A running attempt is not a pending retry, so the
// running-attempt scenarios report neither. StartDelayPending is SAA-only.
func (s *standaloneActivityTestSuite) TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval() {
	env := s.newTestEnv()
	t := s.T()

	// both drives a trace through both surfaces, asserting each reports want.
	both := func(t *testing.T, maxAttempts int32, retryInterval time.Duration, trace []model.Event, want activityInfoProjection) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriverDeclarative(t, env, maxAttempts)
			d.retryInterval = retryInterval
			require.Equal(t, want, d.driveTrace(t, trace).projection(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: maxAttempts})
			d.retryInterval = retryInterval
			require.Equal(t, want, d.driveTrace(t, trace).projection(t))
		})
	}

	// First attempt within its start delay: the dispatch is pending in the future, and is not a retry.
	t.Run("StartDelayPending", func(t *testing.T) {
		info := s.driveTrace(t, env, saaTrace{trace: []model.Event{}, startDelayed: true}).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no preceding backoff, so no retry interval.
	t.Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{model.Poll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1})
	})

	// Backing off before the retry dispatches: the retry is pending, so both the interval and the
	// next-attempt schedule time are populated.
	t.Run("BackingOffBeforeRetry", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{model.Poll, model.FailRetryably},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2, CurrentRetryInterval: saaDelayWindow, NextAttemptScheduleSet: true})
	})

	// Retry dispatched to Matching but not yet polled: schedulable now, not backing off, so no current
	// retry interval and no future dispatch time.
	t.Run("RetryQueuedNotStarted", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2})
	})

	// Retry attempt running with a further retry permitted: nothing pending.
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Final attempt running with no retry remaining: nothing pending.
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, 2, saaDelayWindow, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Completed after a retry: terminal, nothing pending.
	t.Run("Completed", func(t *testing.T) {
		trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Complete}
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriverDeclarative(t, env, 3)
			d.retryInterval = saaDelayWindow
			require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3})
			d.retryInterval = saaDelayWindow
			require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
		})
	})

	// Paused while backing off, before the retry dispatches. No dispatch will occur while paused, so there
	// is neither a next attempt scheduled nor a current retry interval to report.
	t.Run("PausedBeforeDispatch", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{model.Poll, model.FailRetryably, model.Pause},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2})
	})

	// Paused after the backoff elapsed and the retry was dispatched to Matching. As above, no
	// next-attempt schedule time; and as in RetryQueuedNotStarted, nothing is backing off.
	//
	// This `want` is necessarily identical to PausedBeforeDispatch's: diffing the whole of
	// ActivityExecutionInfo and PendingActivityInfo between the two states, over repeated runs to
	// separate signal from run-to-run noise, turns up no field that distinguishes them on either
	// surface. So the two subtests differ only in the state they reach, which the driver verifies (it
	// requires BackoffElapses to clear the pending dispatch time), not in what they assert. The
	// behavioral difference — whether an unpause dispatches at once or waits out the rest of the
	// backoff — is observable, and is covered by the backoff/pause-{before,after}-dispatch-then-unpause
	// traces.
	t.Run("PausedAfterDispatch", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2})
	})
}

// The rest of this file is one-sided SAA coverage: behavior with no WFA counterpart (worker-side
// validation, per-activity start delay, the SAA-only operator commands), or config injected through
// customizeStart, which exists only on the SAA driver.

// saaTraceBudget is the context budget for a group of declarative traces. They pay real wall-clock
// waits, so a group can run a few minutes past the default per-test timeout.
func saaTraceBudget() time.Duration {
	const floor = 8 * time.Minute
	if d := testcontext.DefaultTimeout(); d > floor {
		return d
	}
	return floor
}

// driveTrace drives one declared trace on its own driver and returns a handle at the reached state. It
// checks conformance to the model at every step, unless customizeStart is set.
func (s *standaloneActivityTestSuite) driveTrace(t *testing.T, env *standaloneActivityEnv, tr saaTrace) *saaHandle {
	d := newSAADriverDeclarative(t, env, tr.config())
	d.startDelay = tr.startDelay()
	d.retryInterval = tr.retryInterval
	d.nextRetryDelay = tr.nextRetryDelay
	d.customizeStart = tr.customizeStart
	d.shortTimeout = saaTimeoutIn(tr.trace)
	// Bound the positive poll below the delay window, so that "Dispatchable" means "dispatches promptly".
	// That is what distinguishes a reset which discards a backoff from one which is still delayed.
	d.positivePollTimeout = saaPollTimeout
	// customizeStart injects config the model cannot see, so drive model-free and leave the assertions to
	// the caller.
	if tr.customizeStart != nil {
		return d.driveTrace(t, tr.trace)
	}
	return d.driveTraceWithModelConformanceChecking(t, tr.trace)
}

// TestSAAWorkerMustSendApplicationFailure: a worker failing an attempt must send an
// ApplicationFailureInfo failure, and a server failure is rejected. SAA-only worker-side RPC validation.
func (s *standaloneActivityTestSuite) TestSAAWorkerMustSendApplicationFailure() {
	env := s.newTestEnv()
	a := s.driveTrace(s.T(), env, saaTrace{trace: []model.Event{model.Poll}, maxAttempts: 3})
	_, err := env.FrontendClient().RespondActivityTaskFailed(testcontext.For(s.T()), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: a.token,
		Identity:  "worker",
		Failure: &failurepb.Failure{
			Message:     "server failure",
			FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: false}},
		},
	})
	require.ErrorContains(s.T(), err, "Failure must have ApplicationFailureInfo")
}

// TestWFASAANonRetryableTimeout ports TestParityNonRetryableTimeout: a StartToClose or Heartbeat timeout
// whose type is named in RetryPolicy.NonRetryableErrorTypes must fail the activity terminally when it
// fires, rather than retrying. The type is set via the WFA driver's nonRetryableErrorTypes and, on SAA,
// via customizeStart.
func (s *standaloneActivityTestSuite) TestWFASAANonRetryableTimeout() {
	env := s.newTestEnv()
	t := s.T()

	both := func(t *testing.T, elapse model.EventKind, timeoutType enumspb.TimeoutType) {
		trace := []model.Event{model.Poll, {Kind: elapse}}
		nonRetryableType := retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()
		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriverDeclarative(t, env, 3)
			d.shortTimeout = saaTimeoutIn(trace)
			d.nonRetryableErrorTypes = []string{nonRetryableType}
			require.Equalf(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, d.driveTrace(t, trace).terminal(t).Status,
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriverDeclarative(t, env, model.Config{MaxAttempts: 3, HasHeartbeat: elapse == model.HeartbeatElapsesKind})
			d.shortTimeout = saaTimeoutIn(trace)
			d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
				req.RetryPolicy.NonRetryableErrorTypes = []string{nonRetryableType}
			}
			require.Equalf(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, d.driveTrace(t, trace).terminal(t).Status,
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType)
		})
	}

	t.Run("StartToClose", func(t *testing.T) {
		both(t, model.StartToCloseElapsesKind, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	t.Run("Heartbeat", func(t *testing.T) {
		both(t, model.HeartbeatElapsesKind, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
}

// TestStartDelay_Declarative drives the start-delay scenarios, each a named subtest with its trace
// declared inline. Each step is model-checked by driveTrace; there are no further assertions. SAA-only:
// WFA has no per-activity start delay.
func (s *standaloneActivityTestSuite) TestStartDelay_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := s.newTestEnv()
	t := s.T()

	t.Run("start-delay/first-dispatch", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/pause-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Pause, {Kind: model.UnpauseKind}, model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/reset", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.ResetKind}, model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Pause, {Kind: model.UpdateOptionsKind, SetsStartDelay: true}},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-then-restore-original", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{
				{Kind: model.UpdateOptionsKind, SetsStartDelay: true},
				{Kind: model.UpdateOptionsKind, RestoreOriginal: true},
				model.Poll, model.StartDelayElapses, model.Poll,
			},
			startDelayed: true,
		})
	})
}

// TestBackoff_Declarative drives the retry-backoff scenarios, including the operator commands during a
// backoff. Model-checked by driveTrace.
func (s *standaloneActivityTestSuite) TestBackoff_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := s.newTestEnv()
	t := s.T()

	t.Run("backoff/retry-dispatch", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:         []model.Event{model.Poll, model.FailRetryably, model.Poll, model.BackoffElapses, model.Poll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	t.Run("backoff/next-retry-delay-override", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:          []model.Event{model.Poll, model.FailRetryably, model.Poll, model.BackoffElapses, model.Poll},
			maxAttempts:    3,
			nextRetryDelay: saaDelayWindow,
		})
	})
	// Paused mid-backoff: the unpause resumes waiting, so the next poll must find nothing until the
	// remaining window elapses.
	t.Run("backoff/pause-before-dispatch-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:         []model.Event{model.Poll, model.FailRetryably, model.Pause, {Kind: model.UnpauseKind}, model.Poll, model.BackoffElapses, model.Poll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	// The counterpart: paused after the backoff already elapsed, so the unpause must dispatch at once
	// rather than impose a fresh window. This is the only observable difference between the two paused
	// states — see PausedAfterDispatch.
	t.Run("backoff/pause-after-dispatch-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:         []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause, {Kind: model.UnpauseKind}, model.Poll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	t.Run("backoff/pause-unpause-then-update", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:         []model.Event{model.Poll, model.FailRetryably, model.Pause, {Kind: model.UnpauseKind}, {Kind: model.UpdateOptionsKind}, model.Poll, model.BackoffElapses, model.Poll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	t.Run("backoff/next-retry-delay-override-then-update", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:          []model.Event{model.Poll, model.FailRetryably, {Kind: model.UpdateOptionsKind}, model.Poll, model.BackoffElapses, model.Poll},
			maxAttempts:    3,
			nextRetryDelay: saaDelayWindow,
		})
	})
	t.Run("backoff/reset", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:         []model.Event{model.Poll, model.FailRetryably, {Kind: model.ResetKind}, model.Poll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
}

// TestTimeout_Declarative drives the activity-timeout scenarios, including the start-delay and paused
// variants that have no WFA counterpart. Model-checked by driveTrace.
func (s *standaloneActivityTestSuite) TestTimeout_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := s.newTestEnv()
	t := s.T()

	t.Run("schedule-to-close/elapses-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Pause, {Kind: model.ScheduleToCloseElapsesKind}},
		})
	})
	t.Run("schedule-to-start/elapses-while-scheduled", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{{Kind: model.ScheduleToStartElapsesKind}},
		})
	})
	t.Run("schedule-to-start/elapses-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Pause, {Kind: model.ScheduleToStartElapsesKind}},
		})
	})
	t.Run("start-to-close/elapses-while-started/retries-remain", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
		})
	})
	t.Run("start-to-close/elapses-while-started/last-attempt", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:       []model.Event{model.Poll, model.StartToCloseElapses},
			maxAttempts: 1,
		})
	})
	t.Run("start-to-close/elapses-while-cancel-requested", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.RequestCancel, model.StartToCloseElapses},
		})
	})
	t.Run("heartbeat/elapses-while-started/retries-remain", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, {Kind: model.HeartbeatElapsesKind}},
		})
	})
	t.Run("heartbeat/elapses-while-started/last-attempt", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:       []model.Event{model.Poll, {Kind: model.HeartbeatElapsesKind}},
			maxAttempts: 1,
		})
	})
	t.Run("schedule-to-start/elapses-within-start-delay", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.ScheduleToStartElapsesKind}},
			startDelayed: true,
		})
	})
	t.Run("schedule-to-close/elapses-within-start-delay", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.ScheduleToCloseElapsesKind}},
			startDelayed: true,
		})
	})
}
