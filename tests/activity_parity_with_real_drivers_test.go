// SAA <-> WFA parity tests.
package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
)

type activityParityTestSuite struct {
	parallelsuite.Suite[*activityParityTestSuite]
}

func TestActivityParityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &activityParityTestSuite{})
}

func newActivityParityEnv(t *testing.T) *testcore.TestEnv {
	env := testcore.NewEnv(t)
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.StartDelayEnabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableStandaloneActivityOperatorCommands, nsValues(true))
	return env
}

// TestWFASAAStartToCloseTimeout ports a slice of Test_ActivityTimeouts: a started attempt exceeds its
// StartToClose timeout and, with no retries left, the activity ends TIMED_OUT with the StartToClose
// TimeoutType.
//
// The failure message differs by construction — SAA carries a proto message, WFA's SDK TimeoutError
// formats its own — so TimeoutType is the cross-surface discriminant.
func (s *activityParityTestSuite) TestWFASAAStartToCloseTimeout() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.StartToCloseElapses}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}

	cfg := activityConfig{MaxAttempts: 1, StartToClose: activityShortTimeout}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAScheduleToCloseTimeout ports the schedule-to-close slice of Test_ActivityTimeouts: the
// activity is started, then its ScheduleToClose deadline elapses while it runs, so it ends TIMED_OUT
// with the ScheduleToClose TimeoutType. The trace polls first because a never-started activity that
// hits the deadline times out as ScheduleToStart instead, on both surfaces.
func (s *activityParityTestSuite) TestWFASAAScheduleToCloseTimeout() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.ScheduleToCloseElapsesType}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	cfg := activityConfig{MaxAttempts: 1, ScheduleToClose: activityShortTimeout}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAATimeoutPreservesUnderlyingFailureCause ports TestTimeoutPreservesUnderlyingFailureCause:
// when a timeout closes an activity whose retries were driven by an application failure, the terminal
// TimedOut failure must chain that application failure as its Cause, so an SDK can surface the real
// failure. See mutable_state_impl.go AddActivityTaskTimedOutEvent and temporalio/temporal#3667.
func (s *activityParityTestSuite) TestWFASAATimeoutPreservesUnderlyingFailureCause() {
	env := newActivityParityEnv(s.T())

	// The application failure driven on attempt 1; see activityFailure. The terminal timeout must chain it
	// verbatim, both Type and Message.
	wantCause := failureCause{Type: "drive", Message: "drive"}

	// assertCausePreserved drives the trace on both surfaces and asserts each ends TIMED_OUT with the given
	// timeout type, chaining wantCause.
	assertCausePreserved := func(t *testing.T, cfg activityConfig, trace []model.Event, timeoutType enumspb.TimeoutType) {
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: timeoutType.String()}
		const chained = "the terminal timeout must chain the underlying application failure as its Cause"
		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), chained)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), chained)
		})
	}

	// Retries exhausted by a StartToClose timeout on the final attempt (attempt 1 failed retryably).
	s.T().Run("StartToClose", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{MaxAttempts: 2, StartToClose: activityShortTimeout},
			[]model.Event{model.Poll, model.FailRetryably, model.Poll, model.StartToCloseElapses}, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	// Retries exhausted by a Heartbeat timeout on the final attempt: the attempt starts but never
	// heartbeats. A distinct code path that must chain the same cause.
	s.T().Run("Heartbeat", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{MaxAttempts: 2, Heartbeat: activityShortTimeout},
			[]model.Event{model.Poll, model.FailRetryably, model.Poll, model.HeartbeatElapses}, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry. A third code path.
	s.T().Run("ScheduleToClose", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{ScheduleToClose: activityShortTimeout},
			[]model.Event{model.Poll, model.FailRetryably, model.ScheduleToCloseElapses}, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	})
}

// TestWFASAATimeoutTypeOnRetryDeadline ports the HeartbeatWithScheduleToClose slice of
// Test_ActivityTimeouts: a heartbeat timeout fires on a started attempt, but the retry interval cannot
// fit before the schedule-to-close deadline, so retries are given up and the terminal timeout is
// reported as ScheduleToClose rather than Heartbeat.
func (s *activityParityTestSuite) TestWFASAATimeoutTypeOnRetryDeadline() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}}
	// Heartbeat fires at ~2s; the 30s retry cannot fit before the 10s schedule-to-close deadline.
	const retryInterval, scheduleToClose = 30 * time.Second, 10 * time.Second
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	cfg := activityConfig{
		MaxAttempts: 2, RetryInterval: retryInterval, ScheduleToClose: scheduleToClose, Heartbeat: activityShortTimeout,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAQueuedRetryInterval covers the queued-but-not-started window: SCHEDULED on attempt 2, after
// the backoff elapsed and the retry was dispatched to Matching. Nothing is backing off there, so no
// current retry interval should be reported. Uses a non-constant backoff, so it is a distinct config
// from the constant-interval tests.

func (s *activityParityTestSuite) TestWFASAAQueuedRetryInterval() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses}
	const initialInterval, maxInterval = 5 * time.Second, 30 * time.Second
	want := activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2}

	cfg := activityConfig{
		MaxAttempts: 3, RetryInterval: initialInterval, BackoffCoefficient: 2.0, MaxRetryInterval: maxInterval,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).projection(t))
	})
}

// TestWFASAAHeartBeat ports the core of TestActivityHeartBeatWorkflow_Success: a worker polls the
// activity and heartbeats a checkpoint payload, the checkpoint is readable while it runs, then the
// worker completes it.
var heartbeatWant = []byte(`"hb"`) // == activityHeartbeatDetails

func (s *activityParityTestSuite) TestWFASAAHeartBeat() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.HeartbeatType}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, want, a.terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, want, a.terminal(t))
	})
}

// TestWFASAARetry: an attempt fails retryably, the backoff elapses, the next attempt fails
// non-retryably, and the activity ends FAILED with the application failure type.
func (s *activityParityTestSuite) TestWFASAARetry() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.FailNonRetryably}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: "drive"}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, want, d.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAHeartbeatTimeout ports the core of TestActivityHeartBeatWorkflow_Timeout: a started attempt
// heartbeats nothing within its HeartbeatTimeout and, with no retries left, ends TIMED_OUT with the
// Heartbeat TimeoutType.
func (s *activityParityTestSuite) TestWFASAAHeartbeatTimeout() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String()}

	cfg := activityConfig{MaxAttempts: 1, Heartbeat: activityShortTimeout}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAACancel ports the core of TestTryActivityCancellationFromWorkflow: a running activity is
// cancel-requested, the worker acknowledges with RespondActivityTaskCanceled, and the activity ends
// CANCELED. The RequestCancel event realizes differently per surface — SAA's direct
// RequestCancelActivityExecution RPC vs WFA's signal-then-RequestCancelActivity — which the drivers
// hide.
func (s *activityParityTestSuite) TestWFASAACancel() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.RequestCancel, {Type: model.RespondCanceledType}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	cfg := activityConfig{MaxAttempts: 1}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAARetryAfterFail: attempt 1 fails retryably, the backoff elapses, attempt 2 starts. The
// activity is then running, with no pending retry, so there is no current retry interval and no
// next-attempt schedule time.

func (s *activityParityTestSuite) TestWFASAARetryAfterFail() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                2,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAABackingOff: attempt 1 fails retryably and is observed during the backoff window, so the next
// dispatch is still in the future. The retry is pending, so both the current retry interval and the
// next-attempt schedule time are populated. The long interval keeps the window open across the describe.

func (s *activityParityTestSuite) TestWFASAABackingOff() {
	env := newActivityParityEnv(s.T())
	backingOffInterval := 30 * time.Second
	trace := []model.Event{model.Poll, model.FailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   backingOffInterval,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAANextRetryDelayOverride: the worker fails with a next_retry_delay that overrides the policy
// backoff, observed during the override-length window. The reported current retry interval must be the
// override, not the policy's interval.
func (s *activityParityTestSuite) TestWFASAANextRetryDelayOverride() {
	env := newActivityParityEnv(s.T())
	nextRetryDelayOverride := 30 * time.Second
	trace := []model.Event{model.Poll, model.FailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   nextRetryDelayOverride,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 5 * time.Second, NextRetryDelay: nextRetryDelayOverride})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 5 * time.Second, NextRetryDelay: nextRetryDelayOverride})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAAFirstAttemptStarted: a worker polls the first attempt, which is now running. No attempt has
// failed, so there is no current retry interval and no next-attempt schedule time.
func (s *activityParityTestSuite) TestWFASAAFirstAttemptStarted() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                1,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, want, d.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval sweeps NextAttemptScheduleTime and
// CurrentRetryInterval across the activity lifecycle. A running attempt is not a pending retry, so the
// running-attempt scenarios report neither. StartDelayPending is SAA-only.
func (s *activityParityTestSuite) TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval() {
	env := newActivityParityEnv(s.T())
	t := s.T()

	// both drives a trace through both surfaces, asserting each reports want.
	both := func(t *testing.T, cfg activityConfig, trace []model.Event, want activityInfoProjection) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).projection(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).projection(t))
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
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}, []model.Event{model.Poll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1})
	})

	// Backing off before the retry dispatches: the retry is pending, so both the interval and the
	// next-attempt schedule time are populated.
	t.Run("BackingOffBeforeRetry", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}, []model.Event{model.Poll, model.FailRetryably},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2, CurrentRetryInterval: activityDelayWindow, NextAttemptScheduleSet: true})
	})

	// Retry dispatched to Matching but not yet polled: schedulable now, not backing off, so no current
	// retry interval and no future dispatch time.
	t.Run("RetryQueuedNotStarted", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2})
	})

	// Retry attempt running with a further retry permitted: nothing pending.
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Final attempt running with no retry remaining: nothing pending.
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 2, RetryInterval: activityDelayWindow}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Completed after a retry: terminal, nothing pending.
	t.Run("Completed", func(t *testing.T) {
		trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Complete}
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
		})
	})

	// Paused while backing off, before the retry dispatches. No dispatch will occur while paused, so there
	// is neither a next attempt scheduled nor a current retry interval to report.
	t.Run("PausedBeforeDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}, []model.Event{model.Poll, model.FailRetryably, model.Pause},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2})
	})

	// Paused after the backoff elapsed and the retry was dispatched to Matching. No field of
	// ActivityExecutionInfo or PendingActivityInfo distinguishes this from PausedBeforeDispatch on either
	// surface, so the two subtests differ in the state they reach, not in what they assert. The
	// behavioral difference — whether an unpause dispatches at once — is covered by the
	// backoff/pause-{before,after}-dispatch-then-unpause traces.
	t.Run("PausedAfterDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause},
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
func (s *activityParityTestSuite) driveTrace(t *testing.T, env *testcore.TestEnv, tr saaTrace) *saaHandle {
	d := newSAADriver(t, env, tr.config())
	d.customizeStart = tr.customizeStart
	// Bound the positive poll below the delay window, so that "Dispatchable" means "dispatches promptly".
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
func (s *activityParityTestSuite) TestSAAWorkerMustSendApplicationFailure() {
	env := newActivityParityEnv(s.T())
	a := s.driveTrace(s.T(), env, saaTrace{trace: []model.Event{model.Poll}, cfg: activityConfig{MaxAttempts: 3}})
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
func (s *activityParityTestSuite) TestWFASAANonRetryableTimeout() {
	env := newActivityParityEnv(s.T())
	t := s.T()

	both := func(t *testing.T, cfg activityConfig, elapses model.Event, timeoutType enumspb.TimeoutType) {
		trace := []model.Event{model.Poll, elapses}
		cfg.MaxAttempts = 3
		cfg.NonRetryableErrorTypes = []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()}
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equalf(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t).Status,
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equalf(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t).Status,
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType)
		})
	}

	t.Run("StartToClose", func(t *testing.T) {
		both(t, activityConfig{StartToClose: activityShortTimeout}, model.StartToCloseElapses, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	t.Run("Heartbeat", func(t *testing.T) {
		both(t, activityConfig{Heartbeat: activityShortTimeout}, model.HeartbeatElapses, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
}

// TestStartDelay_Declarative drives the start-delay scenarios, each a named subtest with its trace
// declared inline. Each step is model-checked by driveTrace; there are no further assertions. SAA-only:
// WFA has no per-activity start delay.
func (s *activityParityTestSuite) TestStartDelay_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := newActivityParityEnv(s.T())
	t := s.T()

	t.Run("start-delay/first-dispatch", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/pause-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Pause, {Type: model.UnpauseType}, model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/reset", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Type: model.ResetType}, model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Pause, {Type: model.UpdateOptionsType, SetsStartDelay: true}},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-then-restore-original", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{
				{Type: model.UpdateOptionsType, SetsStartDelay: true},
				{Type: model.UpdateOptionsType, RestoreOriginal: true},
				model.Poll, model.StartDelayElapses, model.Poll,
			},
			startDelayed: true,
		})
	})
}

// TestBackoff_Declarative drives the retry-backoff scenarios, including the operator commands during a
// backoff. Model-checked by driveTrace.
func (s *activityParityTestSuite) TestBackoff_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := newActivityParityEnv(s.T())
	t := s.T()

	t.Run("backoff/retry-dispatch", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	t.Run("backoff/next-retry-delay-override", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, NextRetryDelay: activityDelayWindow},
		})
	})
	// Paused mid-backoff: the unpause resumes waiting, so the next poll must find nothing until the
	// remaining window elapses.
	t.Run("backoff/pause-before-dispatch-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Pause, {Type: model.UnpauseType}, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	// The counterpart: paused after the backoff already elapsed, so the unpause must dispatch at once
	// rather than impose a fresh window.
	t.Run("backoff/pause-after-dispatch-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause, {Type: model.UnpauseType}, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	t.Run("backoff/pause-unpause-then-update", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Pause, {Type: model.UnpauseType}, {Type: model.UpdateOptionsType}, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	t.Run("backoff/next-retry-delay-override-then-update", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, {Type: model.UpdateOptionsType}, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, NextRetryDelay: activityDelayWindow},
		})
	})
	t.Run("backoff/reset", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, {Type: model.ResetType}, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
}

// TestTimeout_Declarative drives the activity-timeout scenarios, including the start-delay and paused
// variants that have no WFA counterpart. Model-checked by driveTrace.
func (s *activityParityTestSuite) TestTimeout_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := newActivityParityEnv(s.T())
	t := s.T()

	t.Run("schedule-to-close/elapses-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Pause, {Type: model.ScheduleToCloseElapsesType}},
		})
	})
	t.Run("schedule-to-start/elapses-while-scheduled", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{{Type: model.ScheduleToStartElapsesType}},
		})
	})
	t.Run("schedule-to-start/elapses-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Pause, {Type: model.ScheduleToStartElapsesType}},
		})
	})
	t.Run("start-to-close/elapses-while-started/retries-remain", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
		})
	})
	t.Run("start-to-close/elapses-while-started/last-attempt", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
			cfg:   activityConfig{MaxAttempts: 1},
		})
	})
	t.Run("start-to-close/elapses-while-cancel-requested", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.RequestCancel, model.StartToCloseElapses},
		})
	})
	t.Run("heartbeat/elapses-while-started/retries-remain", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}},
		})
	})
	t.Run("heartbeat/elapses-while-started/last-attempt", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}},
			cfg:   activityConfig{MaxAttempts: 1},
		})
	})
	t.Run("schedule-to-start/elapses-within-start-delay", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Type: model.ScheduleToStartElapsesType}},
			startDelayed: true,
		})
	})
	t.Run("schedule-to-close/elapses-within-start-delay", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Type: model.ScheduleToCloseElapsesType}},
			startDelayed: true,
		})
	})
}
