package tests

// Driver for workflow-activity (WFA) tests, parallel to the standalone-activity driver in
// activity_standalone_driver.go: it drives an activity scheduled by a workflow through the same
// scripted event sequence, and observes it via DescribeWorkflowExecution. Driving the same trace
// through both drivers and comparing the public activity info is how SAA↔WFA equivalence is checked.
// Neither surface is an oracle; both are asserted against a shared want.
//
// The worker-facing RPCs (poll / respond) are the same frontend APIs the SAA driver uses. The
// WFA-specific parts are that the activity is scheduled by a workflow and observed through it.

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

// --- shared observable projections ---------------------------------------------------------
//
// activityInfoProjection, the retry-scheduling contract both surfaces expose, is defined in
// activity_parity_test.go.

// activityTerminalProjection is the terminal status plus the failure discriminant a user sees: the
// application failure Type for FAILED, the TimeoutType string for TIMED_OUT, empty otherwise. See the
// two terminal() methods.
type activityTerminalProjection struct {
	Status      enumspb.ActivityExecutionStatus
	FailureType string
}

// failureCause is the Type and Message of the failure a terminal outcome chains as its Cause. See the
// two terminalCause() methods.
type failureCause struct {
	Type    string
	Message string
}

func projectWFA(p *workflowpb.PendingActivityInfo) activityInfoProjection {
	return activityInfoProjection{
		State:                  p.GetState(),
		Attempt:                p.GetAttempt(),
		CurrentRetryInterval:   p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleSet: p.GetNextAttemptScheduleTime() != nil,
	}
}

// --- driver --------------------------------------------------------------------------------

type wfaDriverDeclarative struct {
	env                *standaloneActivityEnv
	ctx                context.Context
	maxAttempts        int32         // RetryPolicy MaximumAttempts (0 = unlimited)
	retryInterval      time.Duration // RetryPolicy InitialInterval; 0 => saaDefaultRetryInterval
	backoffCoefficient float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	maxRetryInterval   time.Duration // RetryPolicy MaximumInterval; 0 => retryInterval
	nextRetryDelay     time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed

	shortTimeout        model.EventKind // this timeout is configured short at schedule time; mirrors saaDriverDeclarative.shortTimeout
	scheduleToClose     time.Duration   // ScheduleToClose deadline; mirrors saaDriverDeclarative.scheduleToClose
	positivePollTimeout time.Duration   // bounds a "must dispatch" poll; 0 => saaPositivePollTimeout

	// nonRetryableErrorTypes are the RetryPolicy's NonRetryableErrorTypes. A timeout type is named via
	// retrypolicy.TimeoutFailureTypePrefix. The WFA analog of saaDriverDeclarative.customizeStart.
	nonRetryableErrorTypes []string
}

// newWFADriverDeclarative builds a driver with the test-scoped context. The caller sets whichever timing knobs it
// needs on the result.
func newWFADriverDeclarative(t *testing.T, env *standaloneActivityEnv, maxAttempts int32) *wfaDriverDeclarative {
	return &wfaDriverDeclarative{env: env, ctx: testcontext.For(t), maxAttempts: maxAttempts}
}

// effectiveRetryInterval is the RetryPolicy InitialInterval the driver schedules activities with.
func (d *wfaDriverDeclarative) effectiveRetryInterval() time.Duration {
	return cmp.Or(d.retryInterval, saaDefaultRetryInterval)
}

// wfaHandle is a handle to one workflow-scheduled activity: the ids that address it and the workflow
// that owns it, plus the token last dispatched to it.
type wfaHandle struct {
	d          *wfaDriverDeclarative
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

// wfaCancelSignal makes the helper workflow cancel the activity. A workflow activity is cancelled by
// its workflow rather than by a direct RPC; see wfaHandle.rpc's RequestCancel case.
const wfaCancelSignal = "cancel"

// wfaOneActivityWorkflow schedules a single activity with the given options on its own task queue and
// waits for it to finish. No worker executes the activity — the test drives it with raw worker RPCs —
// so the workflow stays running while the test polls and responds. WaitForCancellation makes the
// workflow wait for the worker's RespondActivityTaskCanceled, so a cancelled activity reaches CANCELED
// before the workflow closes.
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
// state. Model-free, parallel to saaDriverDeclarative.driveTrace.
func (d *wfaDriverDeclarative) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	a := d.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event. Parallel to saaHandle.driveEvent.
func (a *wfaHandle) driveEvent(t require.TestingT, e model.Event) {
	d := a.d
	switch {
	case e.Kind == model.PollKind:
		// A poll captures the dispatched task token.
		if resp := a.pollForTask(t, cmp.Or(d.positivePollTimeout, saaPositivePollTimeout)); resp != nil {
			a.token = resp.GetTaskToken()
		}
	case saaIsWallClock(e.Kind):
		// A wall-clock event is realized by waiting out its configured window.
		a.awaitWallClock(t, e)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// awaitWallClock blocks until a wall-clock event's effect shows up in the workflow's view of the
// activity, and reports a failure if it does not within (window + settle). The effect is a change in the
// pending-activity projection, or the activity leaving the pending set. WFA has no long-poll Describe, so
// unlike saaHandle.awaitWallClock this polls.
func (a *wfaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	before, beforePending := a.pendingSnapshot(t)
	deadline := time.Now().Add(a.d.eventClock(e) + saaWallClockSettle)
	for {
		if now, nowPending := a.pendingSnapshot(t); nowPending != beforePending || (nowPending && now != before) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Errorf("%s: the activity did not change within %s of driving the event, so the event did not "+
				"take effect. Last observed: %+v", model.EventLabel(e), a.d.eventClock(e)+saaWallClockSettle, before)
			return
		}
		time.Sleep(saaPollInterval)
	}
}

// pendingSnapshot is the activity's pending-activity projection, and whether it is currently pending. A
// Describe error is reported rather than reported as absence, which awaitWallClock would read as the
// activity having closed.
func (a *wfaHandle) pendingSnapshot(t require.TestingT) (activityInfoProjection, bool) {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return projectWFA(pa), true
		}
	}
	return activityInfoProjection{}, false
}

// eventClock is how long the clock behind a wall-clock event takes to elapse. WFA has no per-activity
// start delay, so the only dispatch delay is a retry backoff.
func (d *wfaDriverDeclarative) eventClock(e model.Event) time.Duration {
	if e.Kind == model.BackoffElapsesKind {
		return cmp.Or(d.nextRetryDelay, d.effectiveRetryInterval())
	}
	return saaShortTimeout // the four timeouts
}

func (d *wfaDriverDeclarative) start(t *testing.T) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// A dedicated workflow worker runs the helper workflow. Nothing polls the activity task queue, so the
	// test is the only consumer of the activity's tasks.
	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaOneActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	// dur is short for the one timeout under test and long otherwise, so no other timeout fires
	// mid-scenario. Mirrors saaDriverDeclarative.startRequest.
	dur := func(k model.EventKind) time.Duration {
		if d.shortTimeout == k {
			return saaShortTimeout
		}
		return time.Hour
	}
	params := wfaActivityParams{
		ActivityTQ: actTQ, ActivityID: actID,
		StartToClose:  dur(model.StartToCloseElapsesKind),
		RetryInterval: d.effectiveRetryInterval(), BackoffCoefficient: d.backoffCoefficient, MaxInterval: d.maxRetryInterval,
		MaxAttempts:            d.maxAttempts,
		NonRetryableErrorTypes: d.nonRetryableErrorTypes,
	}
	if d.shortTimeout == model.ScheduleToCloseElapsesKind {
		params.ScheduleToClose = saaShortTimeout
	}
	if d.shortTimeout == model.ScheduleToStartElapsesKind {
		params.ScheduleToStart = saaShortTimeout
	}
	if d.shortTimeout == model.HeartbeatElapsesKind {
		params.Heartbeat = saaShortTimeout
	}
	if d.scheduleToClose > 0 {
		params.ScheduleToClose = d.scheduleToClose
	}
	wfID := testcore.RandomizeStr("wfa-run")
	run, err := d.env.SdkClient().ExecuteWorkflow(d.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaOneActivityWorkflow, params)
	require.NoError(t, err)
	return &wfaHandle{d: d, run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, activityTQ: actTQ}
}

// terminal waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal outcome is not in PendingActivities, so it is read from the workflow-result error's cause.
// Parallel to saaHandle.terminal.
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

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none. The
// SDK surfaces it via TimeoutError.Unwrap(). Parallel to saaHandle.terminalCause.
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
		Identity:  "worker",
	})
	require.NoError(t, err)
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error. Parallel to
// saaHandle.rpc: the operator commands are the same *Execution APIs with WorkflowId set. Cancel is the
// exception; see RequestCancel below.
func (a *wfaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Kind {
	case model.HeartbeatKind:
		_, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: saaHeartbeatDetails,
		})
		return err
	case model.RespondCompletedKind:
		_, err := fc.RespondActivityTaskCompleted(a.d.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RespondFailedKind:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: saaFailure(e.Retryable, a.d.nextRetryDelay),
		})
		return err
	case model.RespondCanceledKind:
		_, err := fc.RespondActivityTaskCanceled(a.d.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RequestCancelKind:
		// WFA cancel comes from the workflow, so signal it, then wait for CANCEL_REQUESTED. SAA's direct
		// RequestCancelActivityExecution RPC is synchronous; waiting here makes the two comparable.
		if err := a.d.env.SdkClient().SignalWorkflow(a.d.ctx, a.workflowID, a.runID, wfaCancelSignal, nil); err != nil {
			return err
		}
		return a.waitForCancelRequested()
	case model.PauseKind:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.UnpauseKind:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.ResetKind:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsKind:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("wfaDriverDeclarative: unhandled event kind %v", e.Kind)
	}
}

func (a *wfaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.d.env.Namespace().String(), WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
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
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
		if err != nil {
			return err
		}
		for _, pa := range resp.GetPendingActivities() {
			if pa.GetActivityId() == a.activityID && pa.GetState() == enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("wfaDriverDeclarative: activity %q did not reach CANCEL_REQUESTED after signal", a.activityID)
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes. Readable only
// while the activity is still pending. Parallel to saaHandle.heartbeatDetails.
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

// projection is the activity's pending-activity info as an activityInfoProjection. Parallel to
// saaHandle.projection.
func (a *wfaHandle) projection(t require.TestingT) activityInfoProjection {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return projectWFA(pa)
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending; workflow may have closed", a.activityID)
	return activityInfoProjection{}
}
