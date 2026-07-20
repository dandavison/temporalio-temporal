package tests

// Driver for workflow-activity (WFA) tests, parallel to the standalone-activity driver in
// activity_standalone_utils.go. It drives an activity scheduled by a workflow through a scripted
// sequence of events (the same event DSL), realizing each event as the corresponding worker RPC /
// poll / wall-clock wait, and observes it via DescribeWorkflowExecution. Its purpose is to prove the
// standalone (CHASM) activity behaves like the workflow activity at their intersection: drive the same
// trace through both and compare the public activity info (activityInfoProjection), with WFA as the
// oracle. The worker-facing RPCs (poll / respond) are the same frontend APIs the SAA driver uses; the
// only WFA-specific parts are that the activity is scheduled by a workflow and observed through it.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/tests/testcore"
)

// --- shared observable projection ----------------------------------------------------------
//
// activityInfoProjection is the retry-scheduling contract both surfaces expose — SAA's
// ActivityExecutionInfo (see projectSAA in activity_standalone_utils.go) and WFA's PendingActivityInfo
// (see projectWFA) — and that users depend on. It is the part of the contract SAA GA locks and must
// match WFA. CurrentRetryInterval is rounded to the second: WFA derives it by subtracting two stored
// timestamps (a few µs of noise) while SAA stores it exactly, so an unrounded compare would flag a
// non-divergence. NextAttemptScheduleTime is compared by set-ness, not value (its absolute wall-clock
// value differs across two independent runs).
//
// The last-* timestamps (last started / last completed / last worker) are intentionally left out:
// their cross-surface semantics differ in ways that are separate open questions — e.g. during a
// backoff WFA reports LastStartedTime nil (reset on reschedule) where SAA keeps the prior attempt's —
// not part of this clean first-cut equivalence check.
type activityInfoProjection struct {
	State                  enumspb.PendingActivityState
	Attempt                int32
	CurrentRetryInterval   time.Duration // rounded to the second (see above)
	NextAttemptScheduleSet bool          // NextAttemptScheduleTime != nil
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

type wfaHarness struct {
	env            *standaloneActivityEnv
	ctx            context.Context
	maxAttempts    int32         // RetryPolicy MaximumAttempts (0 = unlimited)
	retryInterval  time.Duration // RetryPolicy interval; how long the driver waits for BackoffElapses
	nextRetryDelay time.Duration // ApplicationFailureInfo.NextRetryDelay injected into RespondFailed
	// positivePollTimeout bounds a "must dispatch" poll; 0 => 10s.
	positivePollTimeout time.Duration
}

// wfaHandle is a handle to one workflow-scheduled activity: the token last dispatched to it plus the
// ids needed to address it and the workflow that owns it.
type wfaHandle struct {
	h          *wfaHarness
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	activityTQ string
	token      []byte
}

type wfaActivityParams struct {
	ActivityTQ    string
	ActivityID    string
	StartToClose  time.Duration
	RetryInterval time.Duration
	MaxAttempts   int32
}

// wfaOneActivityWorkflow schedules a single activity with the given options on its own task queue and
// waits for it to finish. The activity is never executed by a worker — the test drives it with raw
// worker RPCs — so the workflow simply stays running while the test polls and responds.
func wfaOneActivityWorkflow(ctx workflow.Context, p wfaActivityParams) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:             p.ActivityTQ,
		ActivityID:            p.ActivityID,
		DisableEagerExecution: true, // force the task through matching so the test can poll it
		StartToCloseTimeout:   p.StartToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    p.RetryInterval,
			BackoffCoefficient: 1.0,
			MaximumInterval:    p.RetryInterval,
			MaximumAttempts:    p.MaxAttempts,
		},
	})
	return workflow.ExecuteActivity(ctx, "wfaNoop").Get(ctx, nil)
}

// driveTrace runs a trace on a fresh workflow-scheduled activity, realizing each event against the
// server, and returns a handle to it at the reached state. Model-free, parallel to
// saaHarness.driveTrace.
func (h *wfaHarness) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	a := h.start(t)
	for _, e := range trace {
		a.applyEvent(t, e)
	}
	return a
}

// applyEvent advances the activity by one event: a poll captures the dispatched token, a wall-clock
// event is waited out, any other event is its worker RPC (which must succeed). Parallel to
// saaHandle.applyEvent.
func (a *wfaHandle) applyEvent(t require.TestingT, e model.Event) {
	h := a.h
	switch {
	case e.Kind == model.Poll:
		timeout := 10 * time.Second
		if h.positivePollTimeout > 0 {
			timeout = h.positivePollTimeout
		}
		if resp := a.pollForTask(t, timeout); resp != nil {
			a.token = resp.GetTaskToken()
		}
	case saaIsWallClock(e.Kind):
		time.Sleep(h.eventClock(e) + saaWallClockSettle)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// eventClock is how long the clock behind a wall-clock event takes to elapse: a backoff lasts the
// retry interval, a timeout under test is configured short. (WFA has no per-activity start delay.)
func (h *wfaHarness) eventClock(e model.Event) time.Duration {
	if e.Kind == model.BackoffElapses {
		return h.retryInterval
	}
	return saaShortTimeout // the four timeouts
}

func (h *wfaHarness) start(t *testing.T) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// A dedicated workflow worker runs the helper workflow; nothing polls the activity task queue, so
	// the test is the only consumer of the activity's tasks.
	w := sdkworker.New(h.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaOneActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	wfID := testcore.RandomizeStr("wfa-run")
	run, err := h.env.SdkClient().ExecuteWorkflow(h.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaOneActivityWorkflow,
		wfaActivityParams{
			ActivityTQ: actTQ, ActivityID: actID,
			StartToClose: time.Hour, RetryInterval: h.retryInterval, MaxAttempts: h.maxAttempts,
		})
	require.NoError(t, err)
	return &wfaHandle{h: h, run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, activityTQ: actTQ}
}

// terminalStatus waits for the activity to reach a terminal state and reports it, mapped onto the
// ActivityExecutionStatus enum SAA reports directly. A workflow-activity's terminal outcome is not in
// PendingActivities; it is the outcome the workflow's ExecuteActivity().Get returns, so we read it
// from the workflow result. Parallel to saaHandle.terminalStatus.
func (a *wfaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	err := a.run.Get(a.h.ctx, nil)
	if err == nil {
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	switch actErr.Unwrap().(type) {
	case *temporal.TimeoutError:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case *temporal.CanceledError:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	default:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	}
}

func (a *wfaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.h.ctx, timeout)
	defer cancel()
	resp, err := a.h.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.h.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.activityTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

// rpc performs the worker RPC for a non-Poll, non-wall-clock event and returns its error. Parallel to
// saaHandle.rpc, minus the operator commands (which are out of scope for the equivalence work).
func (a *wfaHandle) rpc(e model.Event) error {
	fc := a.h.env.FrontendClient()
	ns := a.h.env.Namespace().String()
	switch e.Kind {
	case model.Heartbeat:
		_, err := fc.RecordActivityTaskHeartbeat(a.h.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token,
		})
		return err
	case model.RespondCompleted:
		_, err := fc.RespondActivityTaskCompleted(a.h.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RespondFailed:
		_, err := fc.RespondActivityTaskFailed(a.h.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: saaFailure(e.Retryable, a.h.nextRetryDelay),
		})
		return err
	case model.RespondCanceled:
		_, err := fc.RespondActivityTaskCanceled(a.h.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	default:
		return fmt.Errorf("wfaHarness: unhandled event kind %v", e.Kind)
	}
}

// projection reads the activity's public info back via DescribeWorkflowExecution, as the shared
// activityInfoProjection. Parallel to saaHandle.projection.
func (a *wfaHandle) projection(t require.TestingT) activityInfoProjection {
	resp, err := a.h.env.SdkClient().DescribeWorkflowExecution(a.h.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return projectWFA(pa)
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending; workflow may have closed", a.activityID)
	return activityInfoProjection{}
}
