package tests

// Driver for workflow-activity (WFA) tests: it drives an activity scheduled by a workflow through a
// sequence of events (a 'trace'), and observes it via DescribeWorkflowExecution.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- the activity info both surfaces expose --------------------------------------------------

func wfaActivityInfo(p *workflowpb.PendingActivityInfo) activityInfo {
	return activityInfo{
		RunState:                   p.GetState(),
		Attempt:                    p.GetAttempt(),
		CurrentRetryInterval:       p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: p.GetNextAttemptScheduleTime() != nil,
	}
}

// --- driver --------------------------------------------------------------------------------

type wfaDriver struct {
	env *testcore.TestEnv
	ctx context.Context
	cfg activityConfig

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => activityDriverTimeout
}

// newWFADriver builds a driver with the test-scoped context. cfg.StartDelay is ignored: a
// workflow activity has no per-activity start delay.
func newWFADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *wfaDriver {
	return &wfaDriver{env: env, ctx: testcontext.For(t), cfg: cfg}
}

// wfaHandle is a handle to one workflow-scheduled activity: the ids that address it and the workflow
// that owns it, plus the token last dispatched to it.
type wfaHandle struct {
	cursor     *activityModelCursor // the model state reached, so driveEvent can check each event
	cfg        activityConfig       // d.cfg with the windows this trace needs; see activityConfig.forTrace
	d          *wfaDriver
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	activityTQ string
	token      []byte
}

type wfaActivityParams struct {
	ActivityTQ             string
	ActivityID             string
	StartToClose           time.Duration
	ScheduleToClose        time.Duration // 0 = unset
	ScheduleToStart        time.Duration // 0 = unset
	Heartbeat              time.Duration // 0 = unset
	RetryInterval          time.Duration
	BackoffCoefficient     float64       // 0 = 1.0 (constant interval)
	MaxInterval            time.Duration // 0 = RetryInterval (no growth)
	MaxAttempts            int32
	NonRetryableErrorTypes []string
}

// activityDriverCancelRequestedTimeout bounds the wait for a signalled cancel to reach the activity.
const activityDriverCancelRequestedTimeout = 10 * time.Second

// wfaCancelSignal makes the helper workflow cancel the activity, which is how a workflow activity is
// cancelled rather than by a direct RPC.
const wfaCancelSignal = "cancel"

// wfaOneActivityWorkflow schedules a single activity with the given options on its own task queue and
// waits for it to finish. No worker executes the activity — the test drives it with raw worker RPCs.
// WaitForCancellation makes the workflow wait for RespondActivityTaskCanceled, so a cancelled activity
// reaches CANCELED before the workflow closes.
func wfaOneActivityWorkflow(ctx workflow.Context, p wfaActivityParams) error {
	coefficient := p.BackoffCoefficient
	if coefficient == 0 {
		coefficient = 1.0
	}
	maxInterval := p.MaxInterval
	if maxInterval == 0 {
		maxInterval = p.RetryInterval
	}
	actCtx, cancelActivity := workflow.WithCancel(ctx)
	actCtx = workflow.WithActivityOptions(actCtx, workflow.ActivityOptions{
		TaskQueue:              p.ActivityTQ,
		ActivityID:             p.ActivityID,
		DisableEagerExecution:  true, // force the task through matching so the test can poll it
		StartToCloseTimeout:    p.StartToClose,
		ScheduleToCloseTimeout: p.ScheduleToClose,
		ScheduleToStartTimeout: p.ScheduleToStart,
		HeartbeatTimeout:       p.Heartbeat,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        p.RetryInterval,
			BackoffCoefficient:     coefficient,
			MaximumInterval:        maxInterval,
			MaximumAttempts:        p.MaxAttempts,
			NonRetryableErrorTypes: p.NonRetryableErrorTypes,
		},
	})
	fut := workflow.ExecuteActivity(actCtx, "wfaNoop")
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, wfaCancelSignal).Receive(gctx, nil)
		cancelActivity()
	})
	return fut.Get(ctx, nil)
}

// driveTrace runs a trace on a fresh workflow-scheduled activity and returns a handle at the reached
// state. Model-free.
func (d *wfaDriver) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	cfg := d.cfg.forTrace(trace)
	a := d.start(t, cfg)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *wfaHandle) driveEvent(t require.TestingT, e model.Event) {
	a.cursor.check(t, e)
	d := a.d
	switch {
	case e.Type == model.PollType:
		// A poll captures the dispatched task token. Every Poll a trace drives is a positive poll — the
		// activity is meant to be dispatchable — so finding no task is a failure, not a step to skip.
		timeout := cmp.Or(d.positivePollTimeout, activityDriverTimeout)
		resp := a.pollForTask(t, timeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, timeout)
		a.token = resp.GetTaskToken()
	case isTimerEvent(e.Type):
		// A wall-clock event is realized by waiting out its configured window.
		a.awaitTimerEvent(t, e)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// awaitTimerEvent blocks until a wall-clock event's effect shows up in the workflow's view of the
// activity, and fails if it does not within (window + settle).
func (a *wfaHandle) awaitTimerEvent(t require.TestingT, e model.Event) {
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchTimePassed(t, e)
		return
	}
	a.awaitTimeout(t, e, time.Now().Add(a.cfg.timerDuration(e)+activityDriverTimerMargin))
}

// awaitTimeout blocks until the activity reports the timeout the event names, and fails if it does
// not within (window + settle). See saaHandle.awaitTimeout.
func (a *wfaHandle) awaitTimeout(t require.TestingT, e model.Event, deadline time.Time) {
	want := timeoutType(e)
	before := a.timeoutMark(t)
	var got activityTimeoutMark
	fired := func() bool {
		got = a.timeoutMark(t)
		return got.reports(want) && (got.closed || got != before)
	}
	if activityDriverPollUntil(deadline, fired) {
		return
	}
	t.Errorf("%s: the activity did not report a %s timeout within %s of driving the event; it reports %+v. "+
		"Check that the config makes this the timeout that fires.",
		e, want, a.cfg.timerDuration(e)+activityDriverTimerMargin, got)
}

// timeoutMark reads the pending activity while there is one. A closed activity has left the pending
// set, and the workflow result reports only the timeout it closed with: an attempt ended by one
// timeout and closed by another is no longer distinguishable here, unlike on the SAA surface.
func (a *wfaHandle) timeoutMark(t require.TestingT) activityTimeoutMark {
	if pa := a.pendingActivity(t); pa != nil {
		return activityTimeoutMark{attemptFailure: timeoutTypeOf(pa.GetLastFailure()), attempt: pa.GetAttempt()}
	}
	m := activityTimeoutMark{closed: true}
	var outcome *temporal.TimeoutError
	if errors.As(a.run.Get(a.d.ctx, nil), &outcome) {
		m.outcome = outcome.TimeoutType()
		var cause *temporal.TimeoutError
		if errors.As(outcome.Unwrap(), &cause) {
			m.cause = cause.TimeoutType()
		}
	}
	return m
}

// awaitDispatchTimePassed polls the activity until the delayed dispatch is no longer pending, and
// fails if it is still pending, or if the activity ended first and so never dispatched at all.
// See saaHandle.awaitDispatchTimePassed.
func (a *wfaHandle) awaitDispatchTimePassed(t require.TestingT, e model.Event) {
	pa := a.pendingActivity(t)
	deadline := time.Now().Add(activityDriverTimerMargin)
	if next := pa.GetNextAttemptScheduleTime(); next != nil {
		deadline = next.AsTime().Add(activityDriverTimerMargin)
	}
	for {
		switch {
		case pa == nil:
			t.Errorf("%s: the activity is no longer pending, so its delayed dispatch never happened", e)
			return
		case pa.GetNextAttemptScheduleTime() == nil:
			return
		case !time.Now().Before(deadline):
			t.Errorf("%s: a dispatch is still pending %s after the time the server scheduled it for. "+
				"Last observed: %+v", e, activityDriverTimerMargin, wfaActivityInfo(pa))
			return
		}
		time.Sleep(activityDriverPollInterval)
		pa = a.pendingActivity(t)
	}
}

// pendingActivity is the activity's entry in the workflow's pending set, nil once it is no longer
// pending. A Describe error is reported rather than treated as absence.
func (a *wfaHandle) pendingActivity(t require.TestingT) *workflowpb.PendingActivityInfo {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return pa
		}
	}
	return nil
}

// pendingSnapshot is the activity's info, and whether it is currently pending.
func (a *wfaHandle) pendingSnapshot(t require.TestingT) (activityInfo, bool) {
	pa := a.pendingActivity(t)
	if pa == nil {
		return activityInfo{}, false
	}
	return wfaActivityInfo(pa), true
}

func (d *wfaDriver) start(t *testing.T, cfg activityConfig) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// A dedicated workflow worker runs the helper workflow. Nothing polls the activity task queue, so the
	// test is the only consumer of the activity's tasks.
	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaOneActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	c := cfg
	wfID := testcore.RandomizeStr("wfa-run")
	run, err := d.env.SdkClient().ExecuteWorkflow(d.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaOneActivityWorkflow, wfaActivityParams{
			ActivityTQ:             actTQ,
			ActivityID:             actID,
			StartToClose:           c.startToClose(),
			ScheduleToClose:        c.ScheduleToClose,
			ScheduleToStart:        c.ScheduleToStart,
			Heartbeat:              c.HeartbeatTimeout,
			RetryInterval:          c.retryInterval(),
			BackoffCoefficient:     c.BackoffCoefficient,
			MaxInterval:            c.MaxRetryInterval,
			MaxAttempts:            c.MaxAttempts,
			NonRetryableErrorTypes: c.NonRetryableErrorTypes,
		})
	require.NoError(t, err)
	return &wfaHandle{d: d, cfg: cfg, cursor: newActivityModelCursor(cfg), run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, activityTQ: actTQ}
}

// terminal waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal outcome is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminal(t require.TestingT) activityTerminalProjection {
	err := a.run.Get(a.d.ctx, nil)
	if err == nil {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
	}
	// A canceled activity surfaces as a bare CanceledError, not wrapped in an ActivityError.
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	switch cause := actErr.Unwrap().(type) {
	case *temporal.ApplicationError:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: cause.Type()}
	case *temporal.TimeoutError:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: cause.TimeoutType().String()}
	default:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED}
	}
}

// terminalStatus is the terminal status alone, for a test that asserts nothing about the failure.
func (a *wfaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	return a.terminal(t).Status
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none. The
// SDK surfaces it via TimeoutError.Unwrap().
func (a *wfaHandle) terminalCause(_ require.TestingT) failureCause {
	var toErr *temporal.TimeoutError
	if errors.As(a.run.Get(a.d.ctx, nil), &toErr) {
		var appErr *temporal.ApplicationError
		if errors.As(toErr.Unwrap(), &appErr) {
			return failureCause{Type: appErr.Type(), Message: appErr.Message()}
		}
	}
	return failureCause{}
}

func (a *wfaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.d.ctx, timeout)
	defer cancel()
	resp, err := a.d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.activityTQ},
		Identity:  a.d.env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error. The operator
// commands are the same *Execution APIs with WorkflowId set; cancel is the exception, see below.
func (a *wfaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.HeartbeatType:
		_, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityHeartbeatDetails,
		})
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.d.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RespondFailedType:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: activityFailure(e.Retryable, a.cfg.NextRetryDelay),
		})
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.d.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RequestCancelType:
		// WFA cancel comes from the workflow, so signal it, then wait for CANCEL_REQUESTED, which SAA's
		// RequestCancelActivityExecution reaches synchronously.
		if err := a.d.env.SdkClient().SignalWorkflow(a.d.ctx, a.workflowID, a.runID, wfaCancelSignal, nil); err != nil {
			return err
		}
		return a.waitForCancelRequested()
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsType:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("wfaDriver: unhandled event type %v", e.Type)
	}
}

func (a *wfaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.d.env.Namespace().String(), WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
	}
	switch {
	case e.RestoreOriginal:
		req.RestoreOriginal = true
	case e.SetsStartDelay:
		req.ActivityOptions = &apiactivitypb.ActivityOptions{StartDelay: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}}
	default:
		// A minimal, always-valid update: re-set the heartbeat timeout.
		req.ActivityOptions = &apiactivitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.d.env.FrontendClient().UpdateActivityExecutionOptions(a.d.ctx, req)
	return err
}

// waitForCancelRequested blocks until the activity reports CANCEL_REQUESTED.
func (a *wfaHandle) waitForCancelRequested() error {
	var describeErr error
	cancelRequested := func() bool {
		resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
		if err != nil {
			describeErr = err
			return true
		}
		for _, pa := range resp.GetPendingActivities() {
			if pa.GetActivityId() == a.activityID && pa.GetState() == enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED {
				return true
			}
		}
		return false
	}
	if activityDriverPollUntil(time.Now().Add(activityDriverCancelRequestedTimeout), cancelRequested) {
		return describeErr
	}
	return fmt.Errorf("wfaDriver: activity %q did not reach CANCEL_REQUESTED after signal", a.activityID)
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes. Readable only
// while the activity is still pending.
func (a *wfaHandle) heartbeatDetails(t require.TestingT) []byte {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return firstPayloadData(pa.GetHeartbeatDetails())
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending", a.activityID)
	return nil
}

// activityInfo is the activity's PendingActivityInfo, projected.
func (a *wfaHandle) activityInfo(t require.TestingT) activityInfo {
	p, pending := a.pendingSnapshot(t)
	require.Truef(t, pending, "activity %q not pending; workflow may have closed", a.activityID)
	return p
}
