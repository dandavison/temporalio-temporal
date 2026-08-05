package tests

// WFA <-> SAA parity tests, mapped one to one onto the parity survey in .task/wfa-saa-parity.md.
//
// The survey is the index. Each of its entries has a label — A for core activity, R for reset, P for
// pause/unpause, U for update options — and there is one top-level test per section. Each subtest is
// named for the entry it checks, beginning with that entry's label, and checks that entry alone, so a
// reader can go from an entry to the test that establishes it and back.
//
// Parity claims that hold on both surfaces have no survey entry, since the survey records differences;
// they live in the other activity_parity files rather than here, as does the harness these tests drive
// activities with.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestActivityCore covers the survey's core-activity (A) entries.
func (s *activityParityTestSuite) TestActivityCore() {
	env := newActivityParityEnv(s.T())
	ns := env.Namespace().String()

	// A01: a worker that reports a schedule-to-start or schedule-to-close timeout itself must close the
	// activity the same way the server's own timer does. Both surfaces already agree on TIMED_OUT with
	// retry state Timeout when the timer fires server-side (TestScheduleToStartTimeout,
	// TestParityScheduleToCloseTimeout), and which side noticed the deadline is not something a user
	// should be able to see. TestSyntheticFailuresHaveRetryParity pins the current divergence, one
	// expectation per surface; this states the outcome both ought to give.
	s.T().Run("A01_WorkerReportedScheduleTimeout", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
		for _, tc := range []struct {
			name    string
			event   model.Event
			timeout enumspb.TimeoutType
		}{
			{"ScheduleToStart", model.FailByIDWithScheduleToStartTimeoutFailure, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START},
			{"ScheduleToClose", model.FailByIDWithScheduleToCloseTimeoutFailure, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE},
		} {
			t.Run(tc.name, func(t *testing.T) {
				expected := activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
					FailureType: tc.timeout.String(),
					RetryState:  enumspb.RETRY_STATE_TIMEOUT,
				}
				parityDrive(t, env, cfg, []model.Event{model.Poll, tc.event},
					func(t *testing.T, a parityActivity) {
						require.Equalf(t, expected, a.terminal(t),
							"a worker-reported %s timeout must close the activity as the server's own timer does",
							tc.timeout)
					})
			})
		}
	})

	// A02: an operator command naming an activity that has already closed must be refused the same way
	// on both surfaces. What fails is the precondition that the activity is still running, not the
	// lookup of the activity, which is also what the behavior model requires of every operator command
	// from a terminal status (model.terminalOutcome). Pause and reset are already asserted for both
	// surfaces by TestPauseActivityExecution/PauseTerminalState and
	// TestResetActivityExecution/TerminalStateReturnsFailedPrecondition; this covers all four commands.
	s.T().Run("A02_OperatorCommandAfterClose", func(t *testing.T) {
		parityDriveOutlivingActivity(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Complete},
			func(t *testing.T, a parityActivity) {
				for _, e := range []model.Event{model.Pause, model.Unpause, model.Reset, model.UpdateOptions} {
					var failedPrecondition *serviceerror.FailedPrecondition
					require.ErrorAsf(t, a.rpc(t, e), &failedPrecondition,
						"%s naming a closed activity must be refused as a failed precondition", e)
				}
			})
	})

	// A03 is about which describe API exists rather than about one behavior: a standalone activity is an
	// execution that is retained after it closes and keeps reporting its terminal status, close time and
	// duration, while a workflow activity is only ever visible as an entry in its workflow's pending
	// set, which it leaves when it closes. Per-surface arms.
	s.T().Run("A03_DescribeAfterClose", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll, model.Complete}

		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriver(t, env, cfg)
			d.holdOpen = true // the workflow must outlive the activity, or there is nothing left to describe
			a := d.driveTrace(t, trace)
			await.Require(a.d.ctx, t, func(t *await.T) {
				t.Require().Nil(a.pendingActivityInfo(t),
					"a closed workflow activity leaves the pending set, so nothing is reported about it")
			}, activityDriverTimeout, activityDriverPollInterval)
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, a.terminal(t).Status)
			info := a.describe(t).GetInfo()
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.GetStatus(),
				"a closed standalone activity must still report its terminal status")
			require.NotNil(t, info.GetCloseTime(), "...and when it closed")
			require.NotNil(t, info.GetExecutionDuration(), "...and how long it ran")
		})
	})

	// A04: the activity is closed by its schedule-to-close deadline — the retry the attempt's timeout
	// asked for cannot be scheduled before it — so that deadline, not the per-attempt timeout, is the
	// timeout a user must be told about, with retry state Timeout saying that retries were given up for
	// lack of time. The trace stops at the poll and lets the heartbeat timeout fire on its own, because
	// the driver's timer wait insists on seeing the per-attempt timeout reported, which is the very
	// thing in question here. TestParityTimeoutTypeOnInsufficientTimeForRetry asserts this for the
	// standalone surface only, skipping the workflow one.
	s.T().Run("A04_TimeoutTypeWhenRetryCannotFitBeforeScheduleToClose", func(t *testing.T) {
		cfg := activityConfig{
			MaxAttempts:      2,
			RetryInterval:    30 * time.Second,
			ScheduleToClose:  10 * time.Second,
			HeartbeatTimeout: activityShortTimeout,
		}
		expected := activityTerminalProjection{
			Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
			RetryState:  enumspb.RETRY_STATE_TIMEOUT,
		}
		parityDrive(t, env, cfg, []model.Event{model.Poll}, func(t *testing.T, a parityActivity) {
			require.Equal(t, expected, a.terminal(t),
				"an activity whose retry cannot fit before its schedule-to-close deadline must report that deadline as the timeout that closed it")
		})

		// The second half of the claim: whether the timeout that actually ended the attempt survives as the
		// terminal failure's cause. The projection above cannot see it, and each surface exposes a cause
		// through a different shape, so each arm reads its own.
		const chained = "the timeout that ended the attempt must survive as the cause of the deadline that closed the activity"
		t.Run("Cause", func(t *testing.T) {
			t.Run("WorkflowActivity", func(t *testing.T) {
				a := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
				timeoutErr, ok := errors.AsType[*temporal.TimeoutError](a.run.Get(a.d.ctx, nil))
				require.True(t, ok)
				require.NotNil(t, timeoutErr.Unwrap(), chained)
			})
			t.Run("StandaloneActivity", func(t *testing.T) {
				a := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
				// awaitTerminal first: the outcome does not exist until the deadline closes the activity, so
				// describing it before then would report a missing cause for the wrong reason.
				require.NotNil(t, a.awaitTerminal(t).GetOutcome().GetFailure().GetCause(), chained)
			})
		})
	})

	// A05 (API shape): a standalone activity is addressable on its own, so it has a per-activity result
	// API; a workflow activity is not, and its result reaches the user through the workflow that
	// scheduled it. Per-surface arms.
	s.T().Run("A05_WaitForOneActivityResult", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll, model.Complete}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
				a.terminal(t), "the workflow result is the only place a workflow activity's outcome appears")
			_, err := env.FrontendClient().PollActivityExecution(a.d.ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace: ns, ActivityId: a.activityID,
			})
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound,
				"a workflow activity's id names no activity execution, so it cannot be polled for a result")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			resp, err := env.FrontendClient().PollActivityExecution(a.d.ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace: ns, ActivityId: a.activityID, RunId: a.runID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, resp.GetRunId(), "PollActivityExecution must resolve once the activity is terminal")
			require.NotNil(t, resp.GetOutcome().GetResult(), "...carrying the result the worker returned")
			require.NotNil(t, a.describe(t).GetOutcome().GetResult(),
				"DescribeActivityExecution must return the outcome when asked for it")
		})
	})

	// A06: an operator command carrying a request id the server has already applied must not be applied
	// again — a client retrying a call whose response it never saw would otherwise silently undo a later
	// change. Update-options is the command used here because its effect is a value that can be read
	// back. Neither driver sends a request id for update-options, so each arm issues the calls itself,
	// and reads the installed timeout from where its surface reports it.
	s.T().Run("A06_RepeatedRequestIDIsDeduplicated", func(t *testing.T) {
		const firstTimeout, laterTimeout = time.Hour, 2 * time.Hour
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		check := func(
			t *testing.T,
			update func(requestID string, heartbeatTimeout time.Duration) error,
			installed func() time.Duration,
		) {
			replayed := uuid.NewString()
			require.NoError(t, update(replayed, firstTimeout))
			require.NoError(t, update(uuid.NewString(), laterTimeout))
			require.NoError(t, update(replayed, firstTimeout))
			require.Equal(t, laterTimeout, installed(),
				"replaying an update whose request id was already applied must not undo the later update")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			check(t, func(requestID string, heartbeatTimeout time.Duration) error {
				_, err := env.FrontendClient().UpdateActivityExecutionOptions(a.d.ctx,
					&workflowservice.UpdateActivityExecutionOptionsRequest{
						Namespace: ns, WorkflowId: a.workflowID, RunId: a.runID, ActivityId: a.activityID,
						Identity: env.Tv().ClientIdentity(), RequestId: requestID,
						ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(heartbeatTimeout)},
						UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
					})
				return err
			}, func() time.Duration {
				return a.pendingActivityInfo(t).GetActivityOptions().GetHeartbeatTimeout().AsDuration()
			})
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			check(t, func(requestID string, heartbeatTimeout time.Duration) error {
				_, err := env.FrontendClient().UpdateActivityExecutionOptions(a.d.ctx,
					&workflowservice.UpdateActivityExecutionOptionsRequest{
						Namespace: ns, RunId: a.runID, ActivityId: a.activityID,
						Identity: env.Tv().ClientIdentity(), RequestId: requestID,
						ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(heartbeatTimeout)},
						UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
					})
				return err
			}, func() time.Duration {
				return a.describe(t).GetInfo().GetHeartbeatTimeout().AsDuration()
			})
		})
	})

	// A07 (API shape): the workflow-activity operator RPCs take a workflow execution plus a target that
	// may be an activity id, an activity type, or every activity of the run; the standalone RPCs address
	// exactly one activity, by id. Per-surface arms.
	s.T().Run("A07_TargetingManyActivitiesInOneCall", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			execution := &commonpb.WorkflowExecution{WorkflowId: a.workflowID, RunId: a.runID}
			_, err := env.FrontendClient().PauseActivity(a.d.ctx, &workflowservice.PauseActivityRequest{
				Namespace: ns, Execution: execution, Identity: env.Tv().ClientIdentity(), Reason: "A07",
				Activity: &workflowservice.PauseActivityRequest_Type{Type: coreParityWFAActivityType},
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, a.activityInfo(t).RunState,
				"naming an activity type, with no activity id, must pause the run's activities of that type")

			_, err = env.FrontendClient().UnpauseActivity(a.d.ctx, &workflowservice.UnpauseActivityRequest{
				Namespace: ns, Execution: execution, Identity: env.Tv().ClientIdentity(),
				Activity: &workflowservice.UnpauseActivityRequest_UnpauseAll{UnpauseAll: true},
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, a.activityInfo(t).RunState,
				"unpause_all must unpause every activity of the run")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			_, err := env.FrontendClient().PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
				Namespace: ns, RunId: a.runID, Identity: env.Tv().ClientIdentity(), Reason: "A07",
				RequestId: uuid.NewString(),
			})
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument,
				"a standalone pause must name the one activity it targets: there is no by-type or all form")
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, a.activityInfo(t).RunState,
				"the refused call must change nothing")
		})
	})

	// A08 (API shape): each surface reports the worker's version through its own fields, and what the
	// entry is about is which fields exist. Reading them is the compile-time half of the assertion; the
	// harness's worker is unversioned, so the runtime half is that each reports nothing. The
	// assigned_build_id oneof's versioning-1 form is not read here: only its build-id member has a value
	// an unversioned dispatch is guaranteed to leave empty.
	s.T().Run("A08_WorkerVersionReporting", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll}

		t.Run("WorkflowActivity", func(t *testing.T) {
			pa := newWFADriver(t, env, cfg).driveTrace(t, trace).pendingActivityInfo(t)
			require.Empty(t, pa.GetLastIndependentlyAssignedBuildId(), "an unversioned worker is assigned no build id")
			require.Nil(t, pa.GetLastWorkerVersionStamp(), "...and stamps no version")
			require.Nil(t, pa.GetLastDeployment(), "...and belongs to no deployment")
			require.Empty(t, pa.GetLastWorkerDeploymentVersion(), "...and to no deployment version")
			require.Nil(t, pa.GetLastDeploymentVersion(), "...by either name")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			info := newSAADriver(t, env, cfg).driveTrace(t, trace).describe(t).GetInfo()
			require.Nil(t, info.GetLastDeploymentVersion(),
				"last_deployment_version is the one version field a standalone activity reports, and an unversioned worker leaves it unset")
		})
	})

	// A09 (API shape): per-activity metadata exists on the standalone surface only. For a workflow
	// activity this information belongs to the enclosing workflow and PendingActivityInfo has no field
	// for any of it, so there is nothing to read on that side and no workflow arm here. sdk_name and
	// sdk_version are not asserted: the harness's worker is a bare poll RPC that sends no SDK headers.
	s.T().Run("A09_PerActivityMetadata", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 1})
		d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
			req.SearchAttributes = coreParitySearchAttributes
			req.Header = coreParityHeader
			req.UserMetadata = coreParityUserMetadata
			req.Links = coreParityLinks(ns)
		}
		a := d.driveTrace(t, []model.Event{model.Poll, model.Heartbeat})
		info := a.describe(t).GetInfo()

		protorequire.ProtoEqual(t, coreParitySearchAttributes, info.GetSearchAttributes())
		protorequire.ProtoEqual(t, coreParityHeader, info.GetHeader())
		protorequire.ProtoEqual(t, coreParityUserMetadata, info.GetUserMetadata())
		require.Len(t, info.GetLinks(), 1, "the links the activity was started with must be reported")
		require.EqualValues(t, 1, info.GetTotalHeartbeatCount(), "the worker's one heartbeat must be counted")
		require.Equal(t, a.taskQueue, info.GetTaskQueue(), "the task queue the activity dispatches on must be reported")
		require.Positive(t, info.GetStateTransitionCount(), "a started activity has undergone state transitions")
		require.Positive(t, info.GetStateSizeBytes(), "...and occupies state")
		require.NotNil(t, info.GetExecutionTime(), "...and reports when it became dispatchable")
	})

	// A10 (API shape): the timeouts and retry policy the user configured are read back from different
	// places — nested in activity_options for a workflow activity, with maximum_attempts also lifted out
	// on its own, and flat on ActivityExecutionInfo for a standalone one. Per-surface arms, so that
	// either shape changing fails the test.
	s.T().Run("A10_ConfiguredTimeoutsAndRetryPolicy", func(t *testing.T) {
		cfg := activityConfig{
			MaxAttempts:      3,
			RetryInterval:    activityLongDuration,
			StartToClose:     time.Hour,
			ScheduleToStart:  2 * time.Hour,
			ScheduleToClose:  3 * time.Hour,
			HeartbeatTimeout: 30 * time.Minute,
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			pa := newWFADriver(t, env, cfg).driveTrace(t, nil).pendingActivityInfo(t)
			options := pa.GetActivityOptions()
			require.Equal(t, cfg.StartToClose, options.GetStartToCloseTimeout().AsDuration(),
				"the configured timeouts are reported nested in activity_options")
			require.Equal(t, cfg.ScheduleToStart, options.GetScheduleToStartTimeout().AsDuration())
			require.Equal(t, cfg.ScheduleToClose, options.GetScheduleToCloseTimeout().AsDuration())
			require.Equal(t, cfg.HeartbeatTimeout, options.GetHeartbeatTimeout().AsDuration())
			require.Equal(t, cfg.MaxAttempts, options.GetRetryPolicy().GetMaximumAttempts(),
				"the retry policy is reported nested in activity_options too")
			require.Equal(t, cfg.MaxAttempts, pa.GetMaximumAttempts(),
				"maximum_attempts is also exposed on its own")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
			require.Equal(t, cfg.StartToClose, info.GetStartToCloseTimeout().AsDuration(),
				"the configured timeouts are flat fields on ActivityExecutionInfo")
			require.Equal(t, cfg.ScheduleToStart, info.GetScheduleToStartTimeout().AsDuration())
			require.Equal(t, cfg.ScheduleToClose, info.GetScheduleToCloseTimeout().AsDuration())
			require.Equal(t, cfg.HeartbeatTimeout, info.GetHeartbeatTimeout().AsDuration())
			require.Equal(t, cfg.MaxAttempts, info.GetRetryPolicy().GetMaximumAttempts(),
				"...as is the retry policy")
		})
	})

	// A11 (API shape): the same value — when the activity was scheduled — is named scheduled_time on
	// PendingActivityInfo and schedule_time on ActivityExecutionInfo. Each arm reads its surface's name,
	// so renaming either fails the test.
	s.T().Run("A11_ScheduleTimeFieldName", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			require.NotNil(t, newWFADriver(t, env, cfg).driveTrace(t, nil).pendingActivityInfo(t).GetScheduledTime(),
				"a workflow activity reports its schedule time as scheduled_time")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			require.NotNil(t, newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo().GetScheduleTime(),
				"a standalone activity reports its schedule time as schedule_time")
		})
	})

	// A12 (intended divergence): only a standalone activity has a start delay of its own. A workflow
	// decides when to schedule its activity — it can sleep on a durable timer first — so the activity it
	// schedules is dispatchable at once.
	s.T().Run("A12_StartDelay", func(t *testing.T) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}).
				driveTrace(t, nil)
			require.Nil(t, a.pendingActivityInfo(t).GetActivityOptions().GetStartDelay(),
				"a workflow activity has no start delay to report")
			require.NotNil(t, a.pollForTask(t, activityDriverTimeout),
				"its first attempt is dispatchable as soon as the workflow schedules it")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, StartDelay: activityLongDuration}
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			info := a.describe(t).GetInfo()
			require.Equal(t, cfg.StartDelay, info.GetStartDelay().AsDuration(),
				"start_delay must be reported as configured")
			require.Equal(t, info.GetScheduleTime().AsTime().Add(cfg.StartDelay), info.GetExecutionTime().AsTime(),
				"the first dispatch is held back until schedule_time + start_delay")
			require.Nil(t, a.pollForTask(t, activityDriverTimeout),
				"no task may be dispatched during the start delay")
		})
	})

	// A13 (intended divergence): the two surfaces take different options at creation. The workflow-side
	// options the survey names — eager execution and use_workflow_build_id — are fields of the
	// workflow's ScheduleActivityTask command that the harness's wrapper workflow cannot set through the
	// SDK's ActivityOptions, so there is no workflow arm; the standalone arm asserts a start request
	// carrying the standalone-only options is accepted and that the ones ActivityExecutionInfo echoes
	// come back. Completion callbacks and on-conflict options are exercised by A22.
	s.T().Run("A13_StartOptions", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration})
		d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
			req.IdReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
			req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
			req.SearchAttributes = coreParitySearchAttributes
			req.UserMetadata = coreParityUserMetadata
			req.Links = coreParityLinks(ns)
			req.StartDelay = durationpb.New(activityLongDuration)
		}
		info := d.driveTrace(t, nil).describe(t).GetInfo()
		protorequire.ProtoEqual(t, coreParitySearchAttributes, info.GetSearchAttributes())
		protorequire.ProtoEqual(t, coreParityUserMetadata, info.GetUserMetadata())
		require.Len(t, info.GetLinks(), 1)
		require.Equal(t, activityLongDuration, info.GetStartDelay().AsDuration(),
			"a start request carrying the standalone-only options must be accepted and reported")
	})

	// A14 (intended divergence): how the terminal timeout's message reads. A workflow activity's user
	// gets an SDK error that renders the message client-side, appending the timeout type to the server's
	// text; a standalone caller gets the server's message verbatim. TimeoutType is the discriminant both
	// share, so it is the message rendering that each arm asserts.
	s.T().Run("A14_TimeoutFailureMessage", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll, model.StartToCloseElapses}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			var timeoutErr *temporal.TimeoutError
			require.ErrorAs(t, a.run.Get(a.d.ctx, nil), &timeoutErr)
			require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())
			require.Equal(t,
				fmt.Sprintf("%s (type: %s)", timeoutErr.Message(), enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
				timeoutErr.Error(),
				"the SDK renders the failure message client-side, from the timeout type and the server's text")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			failure := a.describe(t).GetOutcome().GetFailure()
			require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, failure.GetTimeoutFailureInfo().GetTimeoutType())
			require.NotEmpty(t, failure.GetMessage(), "the server's own message is what a standalone caller reads")
			require.NotContains(t, failure.GetMessage(), "(type:",
				"...returned verbatim, without the SDK's rendering")
		})
	})

	// A15: an activity id means different things. A workflow activity's id is scoped to its workflow
	// run, so two runs can each schedule one called "act" and the two are unrelated activities. A
	// standalone activity id is namespace-scoped and names at most one running activity, so a second
	// start under a running id is refused rather than creating a second activity.
	s.T().Run("A15_ActivityIDScope", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriver(t, env, cfg)
			first := d.driveTrace(t, []model.Event{model.Poll})
			second := d.driveTrace(t, nil)
			require.Equal(t, first.activityID, second.activityID, "both runs schedule the same activity id")
			require.NotEqual(t, first.workflowID, second.workflowID)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, first.activityInfo(t).RunState)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, second.activityInfo(t).RunState,
				"an activity id names one activity per workflow run, so the two do not collide")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			_, err := env.FrontendClient().StartActivityExecution(a.d.ctx,
				a.d.startRequest(a.d.cfg, a.activityID, a.taskQueue))
			var alreadyStarted *serviceerror.ActivityExecutionAlreadyStarted
			require.ErrorAs(t, err, &alreadyStarted,
				"an activity id is namespace-scoped, so a second run may not overlap the first")
		})
	})

	// A16: a workflow activity cannot outlive the workflow run that owns it — terminating the run takes
	// the activity with it, and the worker holding its task can no longer report on it. A standalone
	// activity has no owner that could end it early: not even a workflow of the same id, which does not
	// exist, so the activity runs to its own terminal state.
	s.T().Run("A16_OwnerClosesWhileActivityPending", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			require.NoError(t, env.SdkClient().TerminateWorkflow(a.d.ctx, a.workflowID, a.runID, "A16"))
			// The entry's checkable claim is that the activity does not carry on: the worker that still
			// holds its task can no longer report on it. Whether the closed run keeps listing the activity
			// in its pending set is not asserted — the entry reads either way, and it does keep listing it.
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, a.rpc(t, model.Complete), &notFound,
				"a completion for an activity whose workflow has closed must be rejected")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, env.SdkClient().TerminateWorkflow(a.d.ctx, a.activityID, "", "A16"), &notFound,
				"a standalone activity has no owning workflow that could close it")
			a.driveEvent(t, model.Complete)
			require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
				a.terminal(t), "it reaches its own terminal state instead")
		})
	})

	// A17: a standalone activity is an execution a user can search for — it has its own visibility
	// record and its own list and count APIs — while a workflow activity has none, so a user searches
	// for the enclosing workflow instead.
	s.T().Run("A17_Visibility", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			query := fmt.Sprintf("ActivityId = '%s'", a.activityID)
			coreParityAwaitVisible(t, env, query, 1)
			resp, err := env.FrontendClient().CountActivityExecutions(a.d.ctx,
				&workflowservice.CountActivityExecutionsRequest{Namespace: ns, Query: query})
			require.NoError(t, err)
			require.EqualValues(t, 1, resp.GetCount(), "a standalone activity must be countable by query")
		})

		t.Run("WorkflowActivity", func(t *testing.T) {
			wfa := newWFADriver(t, env, cfg).driveTrace(t, nil)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, wfa.activityInfo(t).RunState)
			// A standalone activity started alongside is the control: once its record is indexed, the
			// workflow activity's absence is not simply indexing lag.
			control := newSAADriver(t, env, cfg).driveTrace(t, nil)
			coreParityAwaitVisible(t, env, fmt.Sprintf("ActivityId = '%s'", control.activityID), 1)

			resp, err := env.FrontendClient().ListActivityExecutions(wfa.d.ctx,
				&workflowservice.ListActivityExecutionsRequest{
					Namespace: ns, PageSize: 10,
					Query: fmt.Sprintf("ActivityType = '%s'", coreParityWFAActivityType),
				})
			require.NoError(t, err)
			require.Empty(t, resp.GetExecutions(), "a workflow activity has no visibility record of its own")
		})
	})

	// A18 has no subtest. Its claim is about routing: a workflow activity can be routed by Workflow
	// Build ID inheritance or a Task Queue versioning directive, and a standalone activity cannot.
	// Establishing that needs a versioned worker deployment — a registered worker deployment version and
	// task-queue versioning rules — which the harness cannot express: its worker is a bare
	// PollActivityTaskQueue call that carries no deployment options, and the standalone half of the
	// claim is the absence of a request field, which has no runtime observation.

	// A19: an activity that names no priority of its own must be dispatched with the priority of
	// whatever created it. A workflow activity has a workflow to inherit from; a standalone activity has
	// nothing above it, so the namespace default applies. The dispatched task is where a user sees the
	// effective priority, so that is what each arm reads.
	s.T().Run("A19_PriorityInheritance", func(t *testing.T) {
		const priorityKey = 1
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			taskQueue, _ := coreParityActivityUnderWorkflow(t, env, cfg,
				sdkclient.StartWorkflowOptions{Priority: temporal.Priority{PriorityKey: priorityKey}})
			resp := activityPollForTask(testcontext.For(t), t, "coreParity", env, taskQueue, activityDriverTimeout)
			require.NotNil(t, resp, "the activity must be dispatched")
			require.EqualValues(t, priorityKey, resp.GetPriority().GetPriorityKey(),
				"an activity with no priority of its own must be dispatched with its workflow's")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			resp := newSAADriver(t, env, cfg).driveTrace(t, nil).pollForTask(t, activityDriverTimeout)
			require.NotNil(t, resp, "the activity must be dispatched")
			require.Zero(t, resp.GetPriority().GetPriorityKey(),
				"a standalone activity has no parent priority to inherit, so nothing overrides the default")
		})
	})

	// A20: what bounds an activity whose user supplied only a start-to-close timeout. A workflow
	// activity is bounded by the run that owns it, so its missing schedule-to-close deadline is defaulted
	// from the workflow run timeout; a standalone activity has no such parent, so the deadline stays
	// unset and it has no expiration time.
	s.T().Run("A20_MissingScheduleTimeouts", func(t *testing.T) {
		const runTimeout = time.Hour
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, StartToClose: time.Minute}

		t.Run("WorkflowActivity", func(t *testing.T) {
			_, pending := coreParityActivityUnderWorkflow(t, env, cfg,
				sdkclient.StartWorkflowOptions{WorkflowRunTimeout: runTimeout})
			pa := pending(t)
			scheduleToClose := pa.GetActivityOptions().GetScheduleToCloseTimeout().AsDuration()
			require.Positive(t, scheduleToClose,
				"a workflow activity's missing schedule-to-close deadline is defaulted from the workflow run timeout")
			require.LessOrEqual(t, scheduleToClose, runTimeout, "...and bounded by it")
			require.NotNil(t, pa.GetExpirationTime(), "...so the activity has an absolute deadline")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
			require.Zero(t, info.GetScheduleToCloseTimeout().AsDuration(),
				"there is no parent run timeout to default a standalone activity's schedule-to-close deadline from")
			require.Nil(t, info.GetExpirationTime(),
				"...so it has no expiration time either")
		})
	})

	// A21: terminate and delete act on a standalone activity execution and have no workflow-activity
	// form — their requests have no workflow_id field at all — because a workflow activity is not an
	// independently retained execution. The closest a user can come is naming the activity id, which
	// finds nothing and leaves the workflow's activity alone.
	s.T().Run("A21_TerminateAndDelete", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			var notFound *serviceerror.NotFound
			_, err := env.FrontendClient().TerminateActivityExecution(a.d.ctx,
				&workflowservice.TerminateActivityExecutionRequest{
					Namespace: ns, ActivityId: a.activityID, Identity: env.Tv().ClientIdentity(),
					Reason: "A21", RequestId: uuid.NewString(),
				})
			require.ErrorAs(t, err, &notFound,
				"a workflow activity is not an activity execution these APIs can terminate")
			_, err = env.FrontendClient().DeleteActivityExecution(a.d.ctx,
				&workflowservice.DeleteActivityExecutionRequest{Namespace: ns, ActivityId: a.activityID})
			require.ErrorAs(t, err, &notFound, "...nor one they can delete")
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, a.activityInfo(t).RunState,
				"the workflow's activity is untouched")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			a.driveEvent(t, model.Terminate)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, a.terminal(t).Status,
				"terminate must close the standalone execution")
			_, err := env.FrontendClient().DeleteActivityExecution(a.d.ctx,
				&workflowservice.DeleteActivityExecutionRequest{Namespace: ns, ActivityId: a.activityID, RunId: a.runID})
			require.NoError(t, err)
			await.Require(a.d.ctx, t, func(t *await.T) {
				_, err := env.FrontendClient().DescribeActivityExecution(t.Context(),
					&workflowservice.DescribeActivityExecutionRequest{
						Namespace: ns, ActivityId: a.activityID, RunId: a.runID,
					})
				var notFound *serviceerror.NotFound
				t.Require().ErrorAs(err, &notFound, "a deleted activity execution must no longer be described")
			}, activityDriverTimeout, activityDriverPollInterval)
		})
	})

	// A22: a completion callback can be attached to a standalone activity, including to one that already
	// exists, via id conflict policy USE_EXISTING with attach_completion_callbacks. A workflow's
	// ScheduleActivityTask command has no callback facility, and a start request naming a workflow
	// activity's id does not reach that activity — it creates a new standalone one — so there is no way
	// to attach a callback to a workflow activity.
	s.T().Run("A22_AttachCompletionCallbackToExistingActivity", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}
		defer env.OverrideDynamicConfig(activity.EnableCallbacks, true)()
		defer env.OverrideDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}})()

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			d := newSAADriver(t, env, cfg)
			req := d.startRequest(cfg, a.activityID, a.taskQueue)
			req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
			// attach_completion_callbacks is only accepted alongside attach_request_id.
			req.OnConflictOptions = &commonpb.OnConflictOptions{AttachRequestId: true, AttachCompletionCallbacks: true}
			req.CompletionCallbacks = coreParityCallbacks("http://localhost/a22-wfa")
			resp, err := env.FrontendClient().StartActivityExecution(a.d.ctx, req)
			require.NoError(t, err)
			require.True(t, resp.GetStarted(),
				"a start naming a workflow activity's id creates a new standalone activity rather than attaching to it")
			require.NotEqual(t, a.runID, resp.GetRunId())
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriver(t, env, cfg)
			d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
				req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
				req.CompletionCallbacks = coreParityCallbacks("http://localhost/a22-first")
			}
			a := d.driveTrace(t, nil)

			req := d.startRequest(d.cfg, a.activityID, a.taskQueue)
			// attach_completion_callbacks is only accepted alongside attach_request_id.
			req.OnConflictOptions = &commonpb.OnConflictOptions{AttachRequestId: true, AttachCompletionCallbacks: true}
			req.CompletionCallbacks = coreParityCallbacks("http://localhost/a22-second")
			resp, err := env.FrontendClient().StartActivityExecution(a.d.ctx, req)
			require.NoError(t, err)
			require.False(t, resp.GetStarted(), "the second start must attach to the running activity")
			require.Equal(t, a.runID, resp.GetRunId())
			require.Len(t, a.describe(t).GetCallbacks(), 2,
				"the callback the second start carried must be attached to the existing activity")
		})
	})

	// A23: the standalone operator-command feature gate. With it off, the four operator commands must be
	// refused for a standalone activity while remaining available for a workflow activity, which the same
	// four RPCs also serve. The standalone arm pauses one activity before turning the gate off, so that
	// the unpause it then attempts would otherwise have succeeded.
	s.T().Run("A23_OperatorCommandFeatureGate", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			defer env.OverrideDynamicConfig(activity.EnableStandaloneActivityOperatorCommands, false)()
			// The driver holds each call to the model, which permits all four from these states.
			for _, e := range []model.Event{model.Pause, model.Unpause, model.Reset, model.UpdateOptions} {
				a.driveEvent(t, e)
			}
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriver(t, env, cfg)
			scheduled := d.driveTrace(t, nil)
			paused := d.driveTrace(t, []model.Event{model.Pause})
			defer env.OverrideDynamicConfig(activity.EnableStandaloneActivityOperatorCommands, false)()

			for _, e := range []model.Event{model.Pause, model.Reset, model.UpdateOptions} {
				require.Errorf(t, scheduled.rpc(t, e),
					"%s must be refused while the standalone operator-command gate is off", e)
			}
			require.Error(t, paused.rpc(t, model.Unpause),
				"unpause must be refused while the standalone operator-command gate is off")
		})
	})
}

// TestReset covers the survey's reset (R) entries. Every entry here concerns a reset requested while a
// worker owns the attempt, so that the reset cannot be applied at once.
//
// The reset request's jitter option has no entry and no subtest: the trace vocabulary has no way to send
// it, and it speaks only to an activity waiting out a backoff, which one with an attempt in progress is
// not.
func (s *activityParityTestSuite) TestReset() {
	env := newActivityParityEnv(s.T())

	// A long retry interval makes the reset's effect on dispatch observable: an ordinary retry would
	// have to wait it out, whereas the reset attempt must be dispatchable at once.
	deferredResetCfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
	// spentBudgetCfg leaves the activity no retry, so an attempt that ends would close it if the reset
	// were not there to rewind the attempt counter.
	spentBudgetCfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

	failRetryablyByID := model.Event{Type: model.RespondFailedByIDType, Failure: &model.Failure{Retryable: true}}

	// assertResetApplied is what both surfaces must report once a pending reset has been applied: a fresh
	// attempt 1, with no backoff to serve before it dispatches.
	resetAttempt := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1}
	assertResetApplied := func(t *testing.T, a parityActivity) {
		require.Equal(t, resetAttempt, a.activityInfo(t),
			"the pending reset must be applied when the attempt ends, starting the activity over")
		a.driveEvent(t, model.Poll)
		require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1}, a.activityInfo(t),
			"the reset attempt must dispatch without waiting out a retry backoff")
	}

	// R01: the reset is held until the attempt ends, and what it then starts is a fresh attempt 1 with no
	// backoff to serve. Driven by each shape of failure a worker can report the attempt ending with, and
	// by a keep_paused reset with no pause to keep, which is a default reset.
	s.T().Run("R01_ResetWhileAttemptInProgress", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			trace []model.Event
		}{
			{name: "RetryableFailure", trace: []model.Event{model.Poll, model.Reset, model.FailRetryably}},
			{name: "OmittedFailure", trace: []model.Event{model.Poll, model.Reset, model.FailWithoutFailure}},
			{name: "FailureByID", trace: []model.Event{model.Poll, model.Reset, failRetryablyByID}},
			{
				name:  "KeepPausedWithNoPauseToKeep",
				trace: []model.Event{model.Poll, model.ResetKeepPaused, model.FailRetryably},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, deferredResetCfg, tc.trace, assertResetApplied)
			})
		}
	})

	// R02: a non-retryable failure is the attempt's answer, not the activity's, while a reset is pending:
	// the reset asked for the activity to start over, so it must be applied rather than the failure
	// closing the activity. The retry budget makes no difference, since the reset rewinds it either way.
	s.T().Run("R02_NonRetryableFailureWhileResetPending", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			cfg  activityConfig
		}{
			{name: "WithRetriesLeft", cfg: deferredResetCfg},
			{name: "WithRetryBudgetSpent", cfg: spentBudgetCfg},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, []model.Event{model.Poll, model.Reset, model.FailNonRetryably},
					assertResetApplied)
			})
		}
	})

	// R03: a reset that does not ask to keep the pause withdraws it. keep_paused exists precisely so that a
	// user can ask for the pause to survive the reset, so a reset without it must not leave one pending:
	// the activity goes back to being an ordinary running attempt, and the reset it defers lands SCHEDULED
	// rather than PAUSED.
	s.T().Run("R03_ResetWhilePauseRequested", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll, model.Pause, model.Reset},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1}, a.activityInfo(t),
					"a reset that does not ask to keep the pause must withdraw it")
				a.driveEvent(t, model.FailRetryably)
				assertResetApplied(t, a)
			})
	})

	// R04: keep_paused asks for the pause to survive the reset, so when the worker goes silent and the
	// attempt's own timeout ends it, both must land: the attempt counter is rewound and the activity is
	// PAUSED at attempt 1. Attempt 1 is the discriminant — a retry that ignored the reset would be at
	// attempt 2. Driven by each per-attempt timeout, since either is how a silent worker's attempt ends.
	// The timeout is not in the trace, so its window is configured explicitly, and it is absorbed by the
	// reset rather than advancing the attempt counter, so the driver's timer wait cannot express it.
	s.T().Run("R04_KeepPausedResetWhilePauseRequestedAndWorkerGoesSilent", func(t *testing.T) {
		pausedResetAttempt := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 1}
		for _, tc := range []struct {
			name string
			cfg  activityConfig
		}{
			{
				name: "StartToCloseTimeout",
				cfg:  activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration, StartToClose: activityShortTimeout},
			},
			{
				name: "HeartbeatTimeout",
				cfg:  activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration, HeartbeatTimeout: activityShortTimeout},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, []model.Event{model.Poll, model.Pause, model.ResetKeepPaused},
					func(t *testing.T, a parityActivity) {
						require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, Attempt: 1},
							a.activityInfo(t),
							"the pause and the keep_paused reset must both be pending on the attempt that is about to time out")
						awaitAbsorbedAttemptTimeout(t, a, pausedResetAttempt, activityShortTimeout)
					})
			})
		}
	})

	// R05: a cancellation is the end of the activity, so there is nothing left for a reset to start over
	// and the call is refused rather than recording an intent that can never be applied. Cancel outranks
	// reset (see the model's order of precedence), which is what makes refusal the answer here.
	s.T().Run("R05_ResetWhileCancellationIsPending", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Reset))
			})
	})

	// R06: a second reset has nothing to add while the first is still pending, so it is refused. Whether
	// it should instead be an idempotent success is an open question; what this holds the two surfaces to
	// is answering it the same way.
	s.T().Run("R06_ResetWhileAnEarlierResetIsPending", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Reset))
			})
	})

	// R07: reset carries the heartbeat checkpoint into the attempt it starts, and reset_heartbeat discards
	// it. The checkpoint here is reported after the reset was requested, so what is at stake is the flag
	// still being honoured when the deferred reset is applied. Only the checkpoint is asserted; the attempt
	// and backoff the same trace reports are R01's subject.
	s.T().Run("R07_HeartbeatProgressAcrossAReset", func(t *testing.T) {
		for _, tc := range []struct {
			name                 string
			resetEvent           model.Event
			lastHeartbeatDetails []byte
		}{
			{
				name:                 "Keep",
				resetEvent:           model.Reset,
				lastHeartbeatDetails: activityMarshalPayloads(activityRecordedHeartbeatDetails),
			},
			{name: "Clear", resetEvent: model.ResetClearingHeartbeat},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, deferredResetCfg,
					[]model.Event{model.Poll, tc.resetEvent, model.Heartbeat, model.FailRetryably},
					func(t *testing.T, a parityActivity) {
						require.Equal(t, tc.lastHeartbeatDetails, a.activityInfo(t).LastHeartbeatDetails)
					})
			})
		}
	})

	// R08: an attempt ending with the retry budget spent would close the activity, but the pending reset
	// rewinds the attempt counter, so the budget is no longer spent and the activity must continue. This
	// is what reset exists to rescue. Driven by a retryable failure, and by each per-attempt timeout,
	// which is how the attempt ends when the worker simply goes silent.
	s.T().Run("R08_AttemptEndsWithRetryBudgetSpentWhileResetPending", func(t *testing.T) {
		t.Run("RetryableFailure", func(t *testing.T) {
			parityDrive(t, env, spentBudgetCfg, []model.Event{model.Poll, model.Reset, model.FailRetryably},
				assertResetApplied)
		})
		for _, tc := range []struct {
			name string
			cfg  activityConfig
		}{
			{
				name: "StartToCloseTimeout",
				cfg:  activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, StartToClose: activityShortTimeout},
			},
			{
				name: "HeartbeatTimeout",
				cfg:  activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, HeartbeatTimeout: activityShortTimeout},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, []model.Event{model.Poll, model.Reset},
					func(t *testing.T, a parityActivity) {
						awaitAbsorbedAttemptTimeout(t, a, resetAttempt, activityShortTimeout)
					})
			})
		}
	})

	// R09: restore_original_options is part of the reset, so on a deferred reset it lands with the reset
	// rather than ahead of it: the attempt in progress keeps running under the options it was dispatched
	// with. The heartbeat timeout stands in for the options as a whole, being the only one each surface
	// reports; each arm reads its own, as there is no shared projection of an activity's current options.
	s.T().Run("R09_RestoreOriginalOptionsWhileAttemptInProgress", func(t *testing.T) {
		const originalHeartbeatTimeout = 2 * time.Hour
		// The heartbeat timeout model.UpdateOptions installs; see the drivers' updateOptions.
		const updatedHeartbeatTimeout = time.Hour
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration, HeartbeatTimeout: originalHeartbeatTimeout}
		// The update is read back before the reset is requested, so that a surface reporting the original
		// timeout later is a restore rather than an update that was never visible.
		trace := []model.Event{model.Poll, model.UpdateOptions}
		const updated = "the update must be visible on the activity it was applied to"
		const inForce = "the superseded attempt keeps the options it was dispatched with"
		const restored = "applying the reset must restore the options the activity was started with"

		restoreOriginal := model.Event{Type: model.ResetType, RestoreOriginal: true}
		assertRestoreDeferred := func(t *testing.T, a parityActivity, heartbeatTimeout func() time.Duration) {
			require.Equal(t, updatedHeartbeatTimeout, heartbeatTimeout(), updated)
			a.driveEvent(t, restoreOriginal)
			require.Equal(t, updatedHeartbeatTimeout, heartbeatTimeout(), inForce)
			a.driveEvent(t, model.FailRetryably)
			require.Equal(t, originalHeartbeatTimeout, heartbeatTimeout(), restored)
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			assertRestoreDeferred(t, a, func() time.Duration {
				return a.pendingActivityInfo(t).GetActivityOptions().GetHeartbeatTimeout().AsDuration()
			})
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			assertRestoreDeferred(t, a, func() time.Duration {
				return a.describe(t).GetInfo().GetHeartbeatTimeout().AsDuration()
			})
		})
	})

	// R10: once a cancellation has superseded the pending reset, the reset will never be applied, so the
	// worker must not be told of it.
	s.T().Run("R10_HeartbeatAfterCancellationSupersedesAPendingReset", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg,
			[]model.Event{model.Poll, model.Reset, model.RequestCancel, model.Heartbeat},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, model.HeartbeatFlags{CancelRequested: true}, a.heartbeatFlags(),
					"the worker is told to cancel, and no longer told of a reset that will not happen")
			})
	})

	// R11: a worker that finishes its work is not superseded by a reset that was waiting for it to yield:
	// the result closes the activity, whether reported by token or by id. The after-a-retry variant is
	// what discriminates for the survey entry's claim about token staleness: a reset applied at once
	// rewinds the attempt counter under the token the worker is still holding, which only shows on a
	// later attempt.
	s.T().Run("R11_AttemptCompletesWhileResetPending", func(t *testing.T) {
		assertCompleted := func(t *testing.T, a parityActivity) {
			require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
				a.terminal(t))
		}
		for _, tc := range []struct {
			name  string
			cfg   activityConfig
			trace []model.Event
		}{
			{name: "ByToken", cfg: deferredResetCfg, trace: []model.Event{model.Poll, model.Reset, model.Complete}},
			{name: "ByID", cfg: deferredResetCfg, trace: []model.Event{model.Poll, model.Reset, model.CompleteByID}},
			{
				name:  "ByTokenAfterARetry",
				cfg:   activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay},
				trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Reset, model.Complete},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, tc.trace, assertCompleted)
			})
		}
	})

	// R12: priority is one of the activity's options, so restore_original_options must put it back like any
	// other. Each arm issues the priority update itself, since the trace vocabulary cannot carry one, and
	// reads the priority its own surface reports. No attempt is in progress, so the reset lands at once:
	// whether the restore is deferred while one is running is R09's subject, not this one.
	s.T().Run("R12_RestoreOriginalOptionsAfterUpdatingPriority", func(t *testing.T) {
		const updatedPriorityKey = 4
		restoreOriginal := model.Event{Type: model.ResetType, RestoreOriginal: true}
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}

		// The original priority is read before the update, so the assertion is that the reset put back
		// whatever the activity was started with, rather than any particular value.
		assertPriorityRestored := func(t *testing.T, a parityActivity, ref resetParityRef, priorityKey func() int32) {
			original := priorityKey()
			require.NotEqual(t, int32(updatedPriorityKey), original,
				"the update must change the priority whose restoration is asserted")
			resetParityUpdatePriority(t, env, ref, updatedPriorityKey)
			require.EqualValues(t, updatedPriorityKey, priorityKey(),
				"the priority update must be visible on the activity it was applied to")
			a.driveEvent(t, restoreOriginal)
			require.Equal(t, original, priorityKey(),
				"restore_original_options must restore the priority the activity was started with")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			assertPriorityRestored(t, a, resetParityRef{ctx: a.d.ctx, workflowID: a.workflowID, activityID: a.activityID, runID: a.runID},
				func() int32 { return a.pendingActivityInfo(t).GetPriority().GetPriorityKey() })
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			assertPriorityRestored(t, a, resetParityRef{ctx: a.d.ctx, activityID: a.activityID, runID: a.runID},
				func() int32 { return a.describe(t).GetInfo().GetPriority().GetPriorityKey() })
		})
	})

	// R13: a reset rewinds the attempt counter; it does not erase the record of what the activity has
	// already done. The failure that drove the retry is the diagnosis the user reset in response to, and
	// the last-started time says when the activity last ran, so both must survive the reset — as the
	// heartbeat checkpoint does (R07). Each is read before the reset as well, so a surface that never
	// reports one fails on the first read rather than passing the second. Neither is in the shared
	// projection, so each arm reads its own.
	s.T().Run("R13_ResetWhileWaitingInRetryBackoff", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
		trace := []model.Event{model.Poll, model.FailRetryably}
		const recorded = "the attempt that just failed must be recorded while the retry backs off"
		const retained = "a reset must not erase the record of the attempt the user reset in response to"

		// Each field is asserted on its own, so that a surface which never reports one of them while
		// backing off does not hide what the reset then does to the other.
		assertRetainedAcrossReset := func(t *testing.T, a parityActivity, read func() any) {
			require.NotNil(t, read(), recorded)
			a.driveEvent(t, model.Reset)
			require.Equal(t, int32(1), a.activityInfo(t).Attempt, "the reset must have been applied")
			require.NotNil(t, read(), retained)
		}

		for _, tc := range []struct {
			name string
			wfa  func(*wfaHandle, require.TestingT) any
			saa  func(*saaHandle, require.TestingT) any
		}{
			{
				name: "LastFailure",
				wfa:  func(a *wfaHandle, t require.TestingT) any { return a.pendingActivityInfo(t).GetLastFailure() },
				saa:  func(a *saaHandle, t require.TestingT) any { return a.describe(t).GetInfo().GetLastFailure() },
			},
			{
				name: "LastStartedTime",
				wfa:  func(a *wfaHandle, t require.TestingT) any { return a.pendingActivityInfo(t).GetLastStartedTime() },
				saa:  func(a *saaHandle, t require.TestingT) any { return a.describe(t).GetInfo().GetLastStartedTime() },
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Run("WorkflowActivity", func(t *testing.T) {
					a := newWFADriver(t, env, cfg).driveTrace(t, trace)
					assertRetainedAcrossReset(t, a, func() any { return tc.wfa(a, t) })
				})
				t.Run("StandaloneActivity", func(t *testing.T) {
					a := newSAADriver(t, env, cfg).driveTrace(t, trace)
					assertRetainedAcrossReset(t, a, func() any { return tc.saa(a, t) })
				})
			})
		}
	})

	// R14: a keep_paused reset subsumes the pause it keeps. What the worker must act on is that its attempt
	// has been superseded, which is the reset; the pause the reset will land in is not a second instruction
	// to it. This is also what model.ExpectedHeartbeatFlags requires of the pending-reset state, which the
	// conformance suite holds both surfaces to.
	s.T().Run("R14_HeartbeatWhilePauseAndKeepPausedResetArePending", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg,
			[]model.Event{model.Poll, model.Pause, model.ResetKeepPaused, model.Heartbeat},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, model.HeartbeatFlags{ActivityReset: true}, a.heartbeatFlags(),
					"the worker is told its attempt was superseded, not separately told of the pause the reset will apply")
			})
	})

	// R15: a token from an attempt a reset superseded must be refused however the attempt counter has
	// moved. Here it lands back where it started — attempt 1 failed, the retry made it attempt 2, and the
	// reset rewound it to 1 — so a token checked against the attempt number alone would match. Accepting it
	// would close the activity with the result of an abandoned attempt and discard the one now running.
	s.T().Run("R15_StaleTokenFromBeforeAResetIsRejected", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll},
			func(t *testing.T, a parityActivity) {
				superseded := a.driverState().token
				for _, e := range []model.Event{model.FailRetryably, model.Reset, model.Poll} {
					a.driveEvent(t, e)
				}
				a.driverState().token = superseded
				resetParityRequireNotFound(t, a.rpc(t, model.Complete),
					"a token from an attempt the reset superseded must not be accepted")
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1}, a.activityInfo(t),
					"the attempt the reset started must still be running")
			})
	})
}

// TestPauseUnpause covers the survey's pause/unpause (P) entries.
func (s *activityParityTestSuite) TestPauseUnpause() {
	env := newActivityParityEnv(s.T())

	// pausedRetryCfg leaves the activity retries to spare and a retry interval long enough that a retry
	// which lands paused stays where the pause put it, rather than being raced by a dispatch.
	pausedRetryCfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
	// pausedRetry is where an attempt that ends under a pending pause must leave the activity: a second
	// attempt, held back by the pause, so with no dispatch deadline or retry interval to report.
	pausedRetry := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2}

	timedOutOnScheduleToClose := activityTerminalProjection{
		Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
		RetryState:  enumspb.RETRY_STATE_TIMEOUT,
	}

	// P01: a pending pause must not cost the running attempt the server-side timeout that ends it when
	// the worker goes silent. The start-to-close timeout fires, and the retry it schedules is where the
	// pause takes effect — otherwise a paused activity with an unresponsive worker never pauses at all.
	// Also covered, for both surfaces, by TestPauseActivityExecution/StartToCloseTimeoutWhilePauseRequested.
	s.T().Run("P01_StartToCloseTimeoutWhilePausePending", func(t *testing.T) {
		cfg := pausedRetryCfg
		cfg.StartToClose = activityShortTimeout
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Pause, model.StartToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, pausedRetry, a.activityInfo(t),
					"the start-to-close timeout must fire under a pending pause, landing the retry paused")
			})
	})

	// P02: as P01 for the heartbeat timeout, which is the other clock a worker that stops responding
	// runs out. Also covered by TestPauseActivityExecution/HeartbeatTimeoutWhilePauseRequested.
	s.T().Run("P02_HeartbeatTimeoutWhilePausePending", func(t *testing.T) {
		cfg := pausedRetryCfg
		cfg.HeartbeatTimeout = activityShortTimeout
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Pause, model.HeartbeatElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, pausedRetry, a.activityInfo(t),
					"the heartbeat timeout must fire under a pending pause, landing the retry paused")
			})
	})

	// P03: schedule-to-close is the user's absolute bound on the activity. Pausing suspends dispatch,
	// not that deadline, so the deadline still closes a paused activity — a pause that suspended it
	// would leave the activity alive with no bound at all. Also covered by
	// TestPauseActivityExecution/ScheduleToCloseTimeoutWhilePaused.
	s.T().Run("P03_ScheduleToCloseDeadlinePassesWhilePaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{RetryInterval: activityLongDuration},
			[]model.Event{model.Pause, model.ScheduleToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, timedOutOnScheduleToClose, a.terminal(t),
					"a paused activity is still subject to its schedule-to-close deadline")
			})
	})

	// P04: a pause is identified by its request id, so a redelivery of one the user has since unpaused
	// must be recognised as already applied. Re-pausing would silently undo the unpause. Also covered by
	// TestPauseActivityExecution/PauseReplayAfterUnpause_IsDeduplicated.
	s.T().Run("P04_PauseReplayAfterUnpause", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration},
			[]model.Event{model.Pause},
			func(t *testing.T, a parityActivity) {
				establishRequestID(a, model.PauseType)
				a.driveEvent(t, model.Unpause)
				a.driveEvent(t, model.Event{Type: model.PauseType, SameRequestID: true})
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t),
					"a replayed pause whose request id was already consumed must not re-pause the activity")
				// The dispatch the unpause released must still reach a poller, which the run state alone
				// does not establish.
				a.driveEvent(t, model.Poll)
			})
	})

	// P05: there is no pause to withdraw, so unpause is refused. Reporting success would tell the user
	// they undid something they never did. Also covered by TestUnpauseWithoutPause, whose WFA arm is
	// skipped.
	s.T().Run("P05_UnpauseWithoutPause", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}, nil,
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})

	// P06: cancellation outranks pausing — the activity is on its way out, so there is nothing to
	// suspend it for, and accepting the pause would suggest the cancellation had been held back. Also
	// covered by TestPauseActivityExecution/PauseWhileCancelRequested.
	s.T().Run("P06_PauseWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// P07: a pending cancellation is not a pause, so unpause has nothing to undo. Also covered by
	// TestUnpauseActivityExecution/UnpauseWhileCancelRequestedFails.
	s.T().Run("P07_UnpauseWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})

	// P08: a pending reset is likewise not a state to pause from: the reset is waiting for the attempt
	// to end, and a pause accepted now would silently change what the reset lands in. Also covered by
	// TestPauseActivityExecution/PauseWhileResetRequested.
	s.T().Run("P08_PauseWhileResetRequested", func(t *testing.T) {
		parityDrive(t, env, pausedRetryCfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// P09: reset_attempts and reset_heartbeat rewind the attempt counter and discard the checkpoint as
	// part of the unpause. Only the workflow-activity UnpauseActivity request carries those fields —
	// UnpauseActivityExecution, the request both surfaces share, has neither — so what is at stake is
	// the shape of each surface's API rather than a behavior they could both be held to. Hence
	// per-surface arms. TestResetSubstitutesForUnpauseFlags asserts, for both surfaces, that Reset
	// covers what the flags cover.
	s.T().Run("P09_UnpauseResetAttemptsAndResetHeartbeat", func(t *testing.T) {
		// Paused while backing off on attempt 2, with a checkpoint from attempt 1 to discard, so both
		// flags have something to do.
		trace := []model.Event{model.Poll, model.Heartbeat, model.FailRetryably, model.Pause}
		recorded := activityMarshalPayloads(activityRecordedHeartbeatDetails)

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			require.Equal(t, recorded, a.activityInfo(t).LastHeartbeatDetails,
				"the paused retry carries the checkpoint the flag is to discard")
			_, err := env.FrontendClient().UnpauseActivity(a.d.ctx, &workflowservice.UnpauseActivityRequest{
				Namespace:      env.Namespace().String(),
				Execution:      &commonpb.WorkflowExecution{WorkflowId: a.workflowID, RunId: a.runID},
				Activity:       &workflowservice.UnpauseActivityRequest_Id{Id: a.activityID},
				Identity:       env.Tv().ClientIdentity(),
				ResetAttempts:  true,
				ResetHeartbeat: true,
			})
			require.NoError(t, err)
			info := a.activityInfo(t)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.RunState,
				"the unpause must release the retry")
			require.Equal(t, int32(1), info.Attempt, "reset_attempts must rewind the attempt counter")
			require.Nil(t, info.LastHeartbeatDetails, "reset_heartbeat must discard the checkpoint")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			a.driveEvent(t, model.Unpause)
			info := a.activityInfo(t)
			require.Equal(t, int32(2), info.Attempt,
				"the standalone unpause has no reset_attempts flag, so the attempt counter stands")
			require.Equal(t, recorded, info.LastHeartbeatDetails,
				"the standalone unpause has no reset_heartbeat flag, so the checkpoint stands")
			a.driveEvent(t, model.ResetClearingHeartbeat)
			info = a.activityInfo(t)
			require.Equal(t, int32(1), info.Attempt, "a separate Reset is what rewinds the attempt counter")
			require.Nil(t, info.LastHeartbeatDetails,
				"a separate Reset carrying reset_heartbeat is what discards the checkpoint")
		})
	})

	// P10: who paused the activity, and when. This is about which fields each surface reports rather
	// than about behavior — PendingActivityInfo has pause_info, ActivityExecutionInfo has no counterpart
	// — so the arms are per surface. The standalone arm searches the whole Describe response for the
	// identity and reason the pause carried, since a field that does not exist cannot be read: it fails
	// if the surface reports them anywhere, which is what the survey claims it does not.
	s.T().Run("P10_WhoPausedTheActivityAndWhen", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			require.NoError(t, pauseParityAttributedPauseWFA(a))
			pauseInfo := a.pendingActivityInfo(t).GetPauseInfo()
			require.NotNil(t, pauseInfo.GetPauseTime(), "pause_info must report when the activity was paused")
			require.Equal(t, pauseParityPauseIdentity, pauseInfo.GetManual().GetIdentity(),
				"pause_info must report who paused the activity")
			require.Equal(t, pauseParityPauseReason, pauseInfo.GetManual().GetReason(),
				"pause_info must report why the activity was paused")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			require.NoError(t, pauseParityAttributedPauseSAA(a))
			response := a.describe(t)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, response.GetInfo().GetRunState())
			require.NotContains(t, response.String(), pauseParityPauseIdentity,
				"a standalone activity reports nothing about who paused it")
			require.NotContains(t, response.String(), pauseParityPauseReason,
				"a standalone activity reports nothing about why it was paused")
		})
	})

	// P11: with the retry budget spent there is no retry for the pending pause to take effect on, so a
	// retryable failure closes the activity exactly as it would have without the pause. A pause the user
	// asked for cannot keep an activity that has run out of attempts in progress.
	s.T().Run("P11_LastPermittedAttemptFailsRetryablyWhilePausePending", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Pause, model.FailRetryably},
			func(t *testing.T, a parityActivity) {
				// Closure is asserted first, and bounded: a.terminal waits on the workflow result for WFA,
				// which is unbounded, so an activity that wrongly stays in progress would otherwise stall
				// until the whole test context expires instead of failing here.
				pauseParityRequireClosed(t, a)
				require.Equal(t, activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
					FailureType: "TestFailure",
					RetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
				}, a.terminal(t), "a pending pause must not keep an activity with no retries left in progress")
			})
	})

	// P12: an unpause is identified by its request id, as a pause is (P04), so a redelivery of one the
	// user has since re-paused past must be recognised as already applied: the later pause stands. Both
	// pauses and the replay are sent as raw RPCs, because the trace vocabulary sends no request id on
	// unpause, and driving them raw leaves the model cursor behind — nothing may be driven afterwards.
	s.T().Run("P12_UnpauseReplayAfterPause", func(t *testing.T) {
		assertReplayIgnored := func(t *testing.T, a parityActivity, unpause pauseParityUnpause) {
			spent := uuid.NewString()
			require.NoError(t, a.rpc(t, model.Pause))
			require.NoError(t, unpause(spent, 0))
			require.NoError(t, a.rpc(t, model.Pause))
			// Whether a recognised replay is reported as success or refused is not this claim; what the
			// activity is left in is.
			_ = unpause(spent, 0)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, a.activityInfo(t).RunState,
				"a replayed unpause whose request id was already consumed must not undo the later pause")
		}
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			assertReplayIgnored(t, a, pauseParityUnpauseWFA(a))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			assertReplayIgnored(t, a, pauseParityUnpauseSAA(a))
		})
	})

	// P13: unpausing an activity that was paused while waiting out a retry backoff must not shorten that
	// backoff — otherwise pause-then-unpause is a way around the retry policy the user set — and jitter
	// can only push the dispatch later, never earlier. The retry interval here is far longer than the
	// jitter, so the deadline reported after the unpause discriminates: keeping the backoff means at or
	// after the original deadline, discarding it means dispatching from the unpause time instead. Only
	// the lower bound is asserted, since how far into the jitter window a surface places the dispatch is
	// its own business. Per-surface arms, because the trace vocabulary cannot send jitter and the shared
	// activityInfo projection reduces the deadline to a bool.
	s.T().Run("P13_UnpauseAnActivityPausedInRetryBackoff", func(t *testing.T) {
		trace := []model.Event{model.Poll, model.FailRetryably}
		assertBackoffKept := func(
			t *testing.T,
			a parityActivity,
			nextAttemptScheduleTime func() *timestamppb.Timestamp,
			unpause pauseParityUnpause,
		) {
			deadline := nextAttemptScheduleTime()
			require.NotNil(t, deadline, "an activity backing off must report the deadline its retry dispatches at")
			a.driveEvent(t, model.Pause)
			require.NoError(t, unpause(uuid.NewString(), pauseParityUnpauseJitter))
			released := nextAttemptScheduleTime()
			require.NotNil(t, released, "the released retry must still report the deadline it dispatches at")
			require.False(t, released.AsTime().Before(deadline.AsTime()),
				"unpausing must not dispatch the retry earlier than the backoff deadline it was already serving")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			assertBackoffKept(t, a, func() *timestamppb.Timestamp {
				return a.pendingActivityInfo(t).GetNextAttemptScheduleTime()
			}, pauseParityUnpauseWFA(a))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			assertBackoffKept(t, a, func() *timestamppb.Timestamp {
				return a.describe(t).GetInfo().GetNextAttemptScheduleTime()
			}, pauseParityUnpauseSAA(a))
		})
	})

	// P14: as P08 for unpause. A pending reset is not a pause, so there is nothing for unpause to undo,
	// and accepting it would suggest the reset had been withdrawn.
	s.T().Run("P14_UnpauseWhileResetRequested", func(t *testing.T) {
		parityDrive(t, env, pausedRetryCfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})
}

// TestUpdateOptions covers the survey's update-options (U) entries.
func (s *activityParityTestSuite) TestUpdateOptions() {
	env := newActivityParityEnv(s.T())

	// U01: a pending cancellation is a decision to stop the activity, so options that may never be used
	// again cannot be updated: the call is refused, and the refusal must leave the cancellation in place.
	// TestPauseActivityExecution's UpdateWhileCancelRequestedFails (activity_parity_ported_test.go) already
	// covers this; this is the entry's own subtest.
	s.T().Run("U01_UpdateWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1}, []model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.UpdateOptions))
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, a.activityInfo(t).RunState,
					"a refused update must leave the pending cancellation in place")
			})
	})

	// U02: a pending reset will restart the activity, and may restore the options it was started with, so
	// an update landing between the request and its application has no well-defined effect and is refused.
	// The refusal must leave the reset pending: it is still applied when the attempt ends. Sibling of
	// TestPauseActivityExecution's PauseWhileResetRequested (activity_parity_ported_test.go), which refuses
	// a pause in the same state.
	s.T().Run("U02_UpdateWhileResetRequested", func(t *testing.T) {
		// A long retry interval makes the applied reset legible: an ordinary retry would be attempt 2
		// waiting the interval out, whereas the reset is a dispatchable attempt 1.
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.UpdateOptions))
				a.driveEvent(t, model.FailRetryably)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t), "a refused update must leave the pending reset to be applied")
			})
	})

	// U03: an accepted update to the non-retryable error types must be the list that decides the next
	// retry, exactly as if it had been given at start time — an update the server accepts but neither
	// reports nor obeys is worse than a refusal. The whole retry_policy is the field-mask path that
	// carries the list (see common/activityoptions.MergeActivityOptions), so every other policy field is
	// re-sent unchanged and the list is the only change.
	s.T().Run("U03_UpdateNonRetryableErrorTypes", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
		nonRetryable := []string{updateParityFailureType}
		updated := &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(cfg.retryInterval()),
			BackoffCoefficient:     1.0,
			MaximumInterval:        durationpb.New(cfg.retryInterval()),
			MaximumAttempts:        cfg.MaxAttempts,
			NonRetryableErrorTypes: nonRetryable,
		}}
		const decided = "a failure whose type is listed non-retryable must close the activity, not retry it"
		// The failure closes the activity in the same transaction that reports it, so closure is readable
		// at once. Reading it before the terminal outcome is what makes a surface that retried instead fail
		// here rather than block waiting for a close that is a retry interval away.
		assertClosedNonRetryably := func(t *testing.T, v updateParityView) {
			v.driveEvent(t, model.FailRetryably)
			require.True(t, v.closed(t), decided)
			require.Equal(t, updateParityNonRetryableTerminal, v.terminal(t), decided)
		}

		// The behavior the updated list has to reproduce, with the list given at start time instead.
		t.Run("SetAtStartTime", func(t *testing.T) {
			startCfg := cfg
			startCfg.NonRetryableErrorTypes = nonRetryable
			updateParityDrive(t, env, startCfg, []model.Event{model.Poll}, assertClosedNonRetryably)
		})

		t.Run("SetByUpdate", func(t *testing.T) {
			updateParityDrive(t, env, cfg, []model.Event{model.Poll}, func(t *testing.T, v updateParityView) {
				require.NoError(t, updateParitySend(v.driverState().ctx, env, v.ids, updated, "retry_policy"))
				require.Equal(t, nonRetryable, v.policy(t).GetNonRetryableErrorTypes(),
					"an accepted update to the non-retryable error types must be reported back")
				assertClosedNonRetryably(t, v)
			})
		})
	})

	// U04: a worker-supplied next_retry_delay is that attempt's own instruction about when to retry, not a
	// value derived from the retry policy, so an options update must leave the pending dispatch where the
	// worker put it — including an update to the retry policy, whose new interval governs later attempts
	// rather than the one already scheduled.
	s.T().Run("U04_UpdateWhileServingWorkerRetryDelay", func(t *testing.T) {
		cfg := activityConfig{
			MaxAttempts:    10,
			RetryInterval:  activityLongDuration,
			NextRetryDelay: updateParityWorkerRetryDelay,
		}
		trace := []model.Event{model.Poll, model.FailRetryably}
		updatedPolicy := &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(updateParityUpdatedRetryInterval),
		}}

		assertOverrideSurvives := func(t *testing.T, v updateParityView, update func(*testing.T, updateParityView)) {
			interval, dispatchTime := v.pending(t)
			require.Equal(t, updateParityWorkerRetryDelay, interval,
				"the worker's next_retry_delay must be the pending retry's interval")
			require.NotNil(t, dispatchTime, "a backing-off retry must report when it will be dispatched")
			update(t, v)
			updatedInterval, updatedDispatchTime := v.pending(t)
			require.Equal(t, updateParityWorkerRetryDelay, updatedInterval,
				"an options update must not replace the worker's next_retry_delay with a policy interval")
			require.NotNil(t, updatedDispatchTime, "the retry must still be pending after the update")
			require.Equal(t, dispatchTime.AsTime(), updatedDispatchTime.AsTime(),
				"the pending retry's dispatch time must survive an options update")
		}

		for _, tc := range []struct {
			name   string
			update func(*testing.T, updateParityView)
		}{
			{
				name:   "UnrelatedOption",
				update: func(t *testing.T, v updateParityView) { v.driveEvent(t, model.UpdateOptions) },
			},
			{
				name: "RetryPolicyInterval",
				update: func(t *testing.T, v updateParityView) {
					require.NoError(t, updateParitySend(v.driverState().ctx, env, v.ids, updatedPolicy,
						"retry_policy.initial_interval"))
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				updateParityDrive(t, env, cfg, trace, func(t *testing.T, v updateParityView) {
					assertOverrideSurvives(t, v, tc.update)
				})
			})
		}
	})
}

// --- helpers ---------------------------------------------------------------------------------
//
// Everything below is machinery for the four tests above: each is prefixed with the section that
// uses it.

// coreParityWFAActivityType is the activity type wfaSingleActivityWorkflow schedules. A07 and A17 name
// it in a request, which is the only way to reach an activity by type.
const coreParityWFAActivityType = "testWFA"

// Metadata a standalone activity can be started with; A09 and A13 read it back. CustomKeywordField is
// one of the search attributes every test namespace registers.
var (
	coreParitySearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		"CustomKeywordField": sadefs.MustEncodeValue("core-parity", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
	}}
	coreParityHeader = &commonpb.Header{Fields: map[string]*commonpb.Payload{
		"core-parity": payload.EncodeString("header value"),
	}}
	coreParityUserMetadata = &sdkpb.UserMetadata{
		Summary: payload.EncodeString("core-parity summary"),
		Details: payload.EncodeString("core-parity details"),
	}
)

// coreParityLinks is a single link to an event of another execution, as a caller that started the
// activity on someone's behalf would attach.
func coreParityLinks(ns string) []*commonpb.Link {
	return []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{
		Namespace:  ns,
		WorkflowId: "core-parity-caller",
		RunId:      "core-parity-caller-run",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}}}}
}

// coreParityCallbacks is one Nexus completion callback, for A22.
func coreParityCallbacks(url string) []*commonpb.Callback {
	return []*commonpb.Callback{
		{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}}},
	}
}

// coreParityAwaitVisible waits for query to return want activity executions, allowing for the delay
// before a new record is indexed.
func coreParityAwaitVisible(t *testing.T, env *testcore.TestEnv, query string, want int) {
	await.Require(testcontext.For(t), t, func(t *await.T) {
		resp, err := env.FrontendClient().ListActivityExecutions(t.Context(),
			&workflowservice.ListActivityExecutionsRequest{
				Namespace: env.Namespace().String(), PageSize: 10, Query: query,
			})
		t.Require().NoError(err)
		t.Require().Len(resp.GetExecutions(), want, "expected %d result(s) for query: %s", want, query)
	}, testcore.WaitForESToSettle, activityDriverPollInterval)
}

// coreParityActivityUnderWorkflow starts a wrapper workflow with the given start options, waits for it
// to schedule its activity, and returns that activity's task queue and a reader for its
// PendingActivityInfo. wfaDriver always starts its workflows with default options, whereas A19 and A20
// turn on an option of the enclosing workflow rather than of the activity.
func coreParityActivityUnderWorkflow(
	t *testing.T,
	env *testcore.TestEnv,
	cfg activityConfig,
	opts sdkclient.StartWorkflowOptions,
) (activityTaskQueue string, pending func(require.TestingT) *workflowpb.PendingActivityInfo) {
	const activityID = "act"
	d := newWFADriver(t, env, cfg)
	opts.ID = testcore.RandomizeStr("core-parity-wf")
	opts.TaskQueue = d.wfTQ
	activityTaskQueue = testcore.RandomizeStr("core-parity-act")

	run, err := env.SdkClient().ExecuteWorkflow(d.ctx, opts, wfaSingleActivityWorkflow,
		wfaActivityParams{Cfg: cfg, ActivityTQ: activityTaskQueue, ActivityID: activityID})
	require.NoError(t, err)

	pending = func(t require.TestingT) *workflowpb.PendingActivityInfo {
		resp, err := env.SdkClient().DescribeWorkflowExecution(d.ctx, opts.ID, run.GetRunID())
		require.NoError(t, err)
		for _, pa := range resp.GetPendingActivities() {
			if pa.GetActivityId() == activityID {
				return pa
			}
		}
		return nil
	}
	await.Require(d.ctx, t, func(t *await.T) {
		t.Require().NotNil(pending(t), "the wrapper workflow must schedule its activity")
	}, activityDriverTimeout, activityDriverPollInterval)
	return activityTaskQueue, pending
}

// awaitAbsorbedAttemptTimeout waits for a per-attempt timeout that a pending reset absorbs to land the
// activity in expected. The driver's timer-event wait cannot express this edge: it watches for the
// attempt counter advancing or the activity closing, and an absorbed timeout does neither, because the
// reset rewinds the counter to 1. Driving the timeout outside driveEvent leaves the driver's model
// cursor behind the activity, so nothing may be driven on it afterwards.
func awaitAbsorbedAttemptTimeout(t *testing.T, a parityActivity, expected activityInfo, window time.Duration) {
	await.Require(a.driverState().ctx, t, func(t *await.T) {
		t.Require().Equal(expected, a.activityInfo(t))
	}, window+activityDriverTimerMargin, activityDriverPollInterval)
}

// resetParityRef identifies an activity to an RPC the drivers do not issue. workflowID is empty for a
// standalone activity.
type resetParityRef struct {
	ctx        context.Context
	workflowID string
	activityID string
	runID      string
}

// resetParityUpdatePriority sets the activity's priority, which the trace vocabulary's UpdateOptions
// event cannot carry.
func resetParityUpdatePriority(t require.TestingT, env *testcore.TestEnv, ref resetParityRef, priorityKey int32) {
	_, err := env.FrontendClient().UpdateActivityExecutionOptions(ref.ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace:       env.Namespace().String(),
		WorkflowId:      ref.workflowID,
		ActivityId:      ref.activityID,
		RunId:           ref.runID,
		Identity:        env.Tv().ClientIdentity(),
		ActivityOptions: &activitypb.ActivityOptions{Priority: &commonpb.Priority{PriorityKey: priorityKey}},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	})
	require.NoError(t, err)
}

func resetParityRequireNotFound(t require.TestingT, err error, msg string) {
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound, msg)
}

// pauseParityPauseIdentity and pauseParityPauseReason are what P10 pauses with: values distinctive
// enough that their absence from a Describe response means the surface reports no pause attribution,
// rather than that the test looked in the wrong place.
const (
	pauseParityPauseIdentity = "pause-parity-identity"
	pauseParityPauseReason   = "pause-parity-reason"
)

// pauseParityUnpauseJitter is the unpause jitter P13 asks for: shorter than activityLongDuration, so a
// dispatch scheduled from the unpause time falls well before the backoff deadline being kept.
const pauseParityUnpauseJitter = time.Hour

// pauseParityUnpause is an UnpauseActivityExecution carrying a request id and a jitter, neither of
// which the trace vocabulary can send. It bypasses the driver's model check, so nothing may be driven
// on the handle after it.
type pauseParityUnpause func(requestID string, jitter time.Duration) error

func pauseParityUnpauseWFA(a *wfaHandle) pauseParityUnpause {
	return func(requestID string, jitter time.Duration) error {
		_, err := a.d.env.FrontendClient().UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: a.d.env.Namespace().String(), WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID,
			Identity: a.d.env.Tv().ClientIdentity(), RequestId: requestID, Jitter: pauseParityDuration(jitter),
		})
		return err
	}
}

func pauseParityUnpauseSAA(a *saaHandle) pauseParityUnpause {
	return func(requestID string, jitter time.Duration) error {
		_, err := a.d.env.FrontendClient().UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
			Identity: a.d.env.Tv().ClientIdentity(), RequestId: requestID, Jitter: pauseParityDuration(jitter),
		})
		return err
	}
}

// pauseParityAttributedPauseWFA and pauseParityAttributedPauseSAA pause with the identity and reason
// P10 looks for; the drivers' Pause event chooses its own.
func pauseParityAttributedPauseWFA(a *wfaHandle) error {
	_, err := a.d.env.FrontendClient().PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
		Namespace: a.d.env.Namespace().String(), WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID,
		Identity: pauseParityPauseIdentity, Reason: pauseParityPauseReason, RequestId: uuid.NewString(),
	})
	return err
}

func pauseParityAttributedPauseSAA(a *saaHandle) error {
	_, err := a.d.env.FrontendClient().PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
		Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
		Identity: pauseParityPauseIdentity, Reason: pauseParityPauseReason, RequestId: uuid.NewString(),
	})
	return err
}

// pauseParityRequireClosed waits, bounded, for the activity to stop being in progress. parityActivity
// offers no bounded closure read: activityInfo fails outright once a workflow activity has left the
// pending set, and terminal waits on the workflow result with no deadline of its own, so an activity
// that wrongly stays in progress stalls until the whole test context expires.
func pauseParityRequireClosed(t *testing.T, a parityActivity) {
	const mustClose = "the activity must close rather than stay in progress"
	switch h := a.(type) {
	case *wfaHandle:
		// A workflow activity's closure shows up as its wrapper workflow closing.
		ctx, cancel := context.WithTimeout(h.d.ctx, activityDriverTimeout)
		defer cancel()
		require.NotErrorIs(t, h.run.Get(ctx, nil), context.DeadlineExceeded, mustClose)
	case *saaHandle:
		await.Require(h.d.ctx, t, func(t *await.T) {
			t.Require().NotEqual(enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, h.describe(t).GetInfo().GetStatus(), mustClose)
		}, activityDriverTimeout, activityDriverPollInterval)
	default:
		require.FailNow(t, "pauseParityRequireClosed: unknown activity handle")
	}
}

func pauseParityDuration(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

// updateParityFailureType is the application-failure type the drivers report; see respondFailedFailure.
const updateParityFailureType = "TestFailure"

// updateParityNonRetryableTerminal is what a failure whose type is listed in non_retryable_error_types
// must produce, whatever retry budget is left: the activity closes rather than retrying.
var updateParityNonRetryableTerminal = activityTerminalProjection{
	Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
	FailureType: updateParityFailureType,
	RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
}

// updateParityWorkerRetryDelay is the next_retry_delay a worker reports in U04, and
// updateParityUpdatedRetryInterval the interval U04's retry-policy update installs. Both differ from the
// configured retry interval and from each other, so a preserved dispatch time cannot be mistaken for a
// recomputed one.
const (
	updateParityWorkerRetryDelay     = 12 * time.Hour
	updateParityUpdatedRetryInterval = 6 * time.Hour
)

// updateParityView is a driven activity plus the per-surface reads these tests need beyond the shared
// activityInfo projection: the ids UpdateActivityExecutionOptions is addressed by, the retry policy the
// activity reports, whether it has stopped running, and its pending retry as an interval and an exact
// dispatch time.
type updateParityView struct {
	parityActivity
	ids     updateParityIDs
	policy  func(require.TestingT) *commonpb.RetryPolicy
	closed  func(require.TestingT) bool
	pending func(require.TestingT) (time.Duration, *timestamppb.Timestamp)
}

// updateParityDrive is parityDrive for a test that also needs the per-surface reads in updateParityView.
func updateParityDrive(
	t *testing.T,
	env *testcore.TestEnv,
	cfg activityConfig,
	trace []model.Event,
	check func(*testing.T, updateParityView),
) {
	t.Run("WorkflowActivity", func(t *testing.T) {
		a := newWFADriver(t, env, cfg).driveTrace(t, trace)
		check(t, updateParityView{
			parityActivity: a,
			ids:            updateParityIDs{WorkflowID: a.workflowID, ActivityID: a.activityID, RunID: a.runID},
			policy: func(t require.TestingT) *commonpb.RetryPolicy {
				return a.pendingActivityInfo(t).GetActivityOptions().GetRetryPolicy()
			},
			closed: func(t require.TestingT) bool { return a.pendingActivityInfo(t) == nil },
			pending: func(t require.TestingT) (time.Duration, *timestamppb.Timestamp) {
				pendingActivity := a.pendingActivityInfo(t)
				require.NotNil(t, pendingActivity, "the activity is no longer pending")
				return pendingActivity.GetCurrentRetryInterval().AsDuration().Round(time.Second),
					pendingActivity.GetNextAttemptScheduleTime()
			},
		})
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		a := newSAADriver(t, env, cfg).driveTrace(t, trace)
		check(t, updateParityView{
			parityActivity: a,
			ids:            updateParityIDs{ActivityID: a.activityID, RunID: a.runID},
			policy: func(t require.TestingT) *commonpb.RetryPolicy {
				return a.describe(t).GetInfo().GetRetryPolicy()
			},
			closed: func(t require.TestingT) bool {
				return a.describe(t).GetInfo().GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
			},
			pending: func(t require.TestingT) (time.Duration, *timestamppb.Timestamp) {
				info := a.describe(t).GetInfo()
				return info.GetCurrentRetryInterval().AsDuration().Round(time.Second),
					info.GetNextAttemptScheduleTime()
			},
		})
	})
}

// updateParityIDs identifies an activity to UpdateActivityExecutionOptions on either surface;
// WorkflowID is empty for a standalone activity.
type updateParityIDs struct {
	WorkflowID string
	ActivityID string
	RunID      string
}

// updateParitySend issues an update the drivers' UpdateOptions event cannot express, and returns the
// server's answer.
func updateParitySend(
	ctx context.Context,
	env *testcore.TestEnv,
	ids updateParityIDs,
	options *activitypb.ActivityOptions,
	paths ...string,
) error {
	_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace:       env.Namespace().String(),
		WorkflowId:      ids.WorkflowID,
		ActivityId:      ids.ActivityID,
		RunId:           ids.RunID,
		Identity:        env.Tv().ClientIdentity(),
		ActivityOptions: options,
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: paths},
	})
	return err
}
