package tests

// Driver for standalone-activity (SAA) tests. A test starts an activity on a real onebox server and
// advances it a step at a time — a worker poll, an operator/worker RPC, or waiting out a wall-clock
// window (a timeout or dispatch delay) — realizing each as the corresponding frontend call, and reads
// internal state back via ReadComponent. The event alphabet and the observable-state projection come
// from the archetype model package (chasm/lib/activity/model); this file is the SAA adapter that
// realizes them, plus the assertion helpers (see the bottom) that let a functional test read as a
// declarative script.

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- driver --------------------------------------------------------------------------------

type saaHarness struct {
	env      *standaloneActivityEnv
	ctx      context.Context
	chasmCtx context.Context
	nsID     string
	cfg      model.Config
	cfgIdx   int
	counter  int
	// shortTimeout, when set to one of the four timeout *Elapses kinds, makes that timeout short at
	// Start so a trace can trigger it. Left at its zero value (Poll), all timeouts are long.
	shortTimeout model.EventKind
	// Timing knobs a trace configures.
	startDelay     time.Duration // StartActivityExecutionRequest.StartDelay
	retryInterval  time.Duration // RetryPolicy InitialInterval; 0 => the default short backoff
	nextRetryDelay time.Duration // ApplicationFailureInfo.NextRetryDelay injected into RespondFailed
}

const saaShortTimeout = 2 * time.Second

// saaWallClockSettle is slack added to a wall-clock event's clock when waiting for it to fire, so the
// wait comfortably outlasts the firing instant (a timeout deadline, or a dispatch-delay instant like
// schedule_time + start_delay or complete_time + backoff).
const saaWallClockSettle = 2 * time.Second

// saaNegativePollTimeout is the deadline for a "must not dispatch" poll. It must exceed
// common.MinLongPollTimeout, or the frontend rejects the poll before consulting matching (making the
// check vacuous). A genuine empty long poll blocks for roughly this long.
const saaNegativePollTimeout = common.MinLongPollTimeout + time.Second

// saaActor is one activity instance.
type saaActor struct {
	h          *saaHarness
	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

func (h *saaHarness) start(t require.TestingT) *saaActor {
	// cfg.HasStartDelay tells the model to predict StartDelayPending; the server only enters that state
	// if a real start_delay is configured. Guard against the decoupling so a misconfigured harness fails
	// loudly rather than as a confusing first-state mismatch.
	if h.cfg.HasStartDelay && h.startDelay <= 0 {
		require.Fail(t, "saaHarness misconfigured: cfg.HasStartDelay requires startDelay > 0")
	}
	h.counter++
	id := fmt.Sprintf("saaexp-%d-%d", h.cfgIdx, h.counter)
	resp, err := h.env.FrontendClient().StartActivityExecution(h.ctx, h.startRequest(id, id))
	require.NoError(t, err)
	return &saaActor{h: h, activityID: id, taskQueue: id, runID: resp.RunId}
}

func (h *saaHarness) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	long := durationpb.New(time.Hour)
	// dur returns the short timeout for the one timeout under test, long otherwise.
	dur := func(k model.EventKind) *durationpb.Duration {
		if h.shortTimeout == k {
			return durationpb.New(saaShortTimeout)
		}
		return long
	}
	// Retries dispatch after this interval. The default is short so retry loops run quickly; the backoff
	// traces lengthen it to observe the backoff.
	interval := 200 * time.Millisecond
	if h.retryInterval > 0 {
		interval = h.retryInterval
	}
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           h.env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        h.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: dur(model.StartToCloseElapses),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(interval),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(interval),
			MaximumAttempts:    h.cfg.MaxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if h.startDelay > 0 {
		req.StartDelay = durationpb.New(h.startDelay)
	}
	if h.cfg.HasScheduleToClose {
		req.ScheduleToCloseTimeout = dur(model.ScheduleToCloseElapses)
	}
	if h.cfg.HasScheduleToStart {
		req.ScheduleToStartTimeout = dur(model.ScheduleToStartElapses)
	}
	if h.cfg.HasHeartbeat {
		req.HeartbeatTimeout = dur(model.HeartbeatElapses)
	}
	return req
}

// observed reads the activity's internal state back via ReadComponent, as the model's AbstractState.
func (a *saaActor) observed() (model.AbstractState, error) {
	o, err := saaReadObserved(a.h.chasmCtx, a.h.nsID, a.activityID, a.runID)
	if err != nil {
		return model.AbstractState{}, err
	}
	return model.Abstract(o), nil
}

// rpc performs the RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaActor) rpc(e model.Event) error {
	fc := a.h.env.FrontendClient()
	ns := a.h.env.Namespace().String()
	switch e.Kind {
	case model.Heartbeat:
		_, err := fc.RecordActivityTaskHeartbeat(a.h.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns,
			TaskToken: a.token,
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
	case model.RequestCancel:
		_, err := fc.RequestCancelActivityExecution(a.h.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.Terminate:
		_, err := fc.TerminateActivityExecution(a.h.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.Pause:
		_, err := fc.PauseActivityExecution(a.h.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.Unpause:
		_, err := fc.UnpauseActivityExecution(a.h.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.Reset:
		_, err := fc.ResetActivityExecution(a.h.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptions:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaHarness: unhandled event kind %v", e.Kind)
	}
}

func (a *saaActor) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.h.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: "op",
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
	_, err := a.h.env.FrontendClient().UpdateActivityExecutionOptions(a.h.ctx, req)
	return err
}

func (a *saaActor) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.h.ctx, timeout)
	defer cancel()
	resp, err := a.h.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.h.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	if err != nil {
		// Matching signals "waited, found nothing" with an empty response and a nil error, so a genuine
		// no-task result never surfaces as an error. Any error here means the poll did not complete
		// cleanly (e.g. deadline below MinLongPollTimeout, or our deadline fired first); treating that as
		// "no task" would be vacuous, so fail loudly. Teardown (parent context done) is the benign case.
		if a.h.ctx.Err() != nil {
			return nil
		}
		if deadline, ok := a.h.ctx.Deadline(); ok && time.Until(deadline) < common.MinLongPollTimeout {
			// The test context is nearly spent, so the poll's derived deadline fell below the server's
			// long-poll floor. This is a budget problem, not a harness bug: the run outgrew its window.
			t.Errorf("saaHarness: test context budget exhausted before the poll could run (%.1fs left, need >= %s). "+
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`.\n  %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
			return nil
		}
		t.Errorf("saaHarness harness bug: PollActivityTaskQueue did not complete cleanly (server rejected the poll, "+
			"or the deadline fired before matching answered): %v\n"+
			"  the poll timeout must be >= MinLongPollTimeout (2s); only an empty response with a nil error means \"no task\"", err)
		return nil
	}
	// Empty response with a nil error is matching's authoritative "waited, no task available".
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

// eventClock is how long the clock behind a wall-clock event takes to elapse: a timeout under test is
// configured short (saaShortTimeout), and a dispatch delay lasts dispatchDelay.
func (h *saaHarness) eventClock(e model.Event) time.Duration {
	switch e.Kind {
	case model.StartDelayElapses:
		return h.dispatchDelay(model.StartDelayPending)
	case model.BackoffElapses:
		return h.dispatchDelay(model.BackoffPending)
	default: // the four timeouts
		return saaShortTimeout
	}
}

// dispatchDelay is how long the harness configured the pending delay to last. For a backoff, a
// worker-supplied next_retry_delay overrides the policy interval.
func (h *saaHarness) dispatchDelay(d model.Dispatchability) time.Duration {
	switch d {
	case model.StartDelayPending:
		return h.startDelay
	case model.BackoffPending:
		if h.nextRetryDelay > 0 {
			return h.nextRetryDelay
		}
		return h.retryInterval
	default:
		return 0
	}
}

func saaReadObserved(chasmCtx context.Context, nsID, activityID, runID string) (model.Observed, error) {
	ref := chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
		NamespaceID: nsID, BusinessID: activityID, RunID: runID,
	})
	return chasm.ReadComponent(chasmCtx, ref, func(act *activity.Activity, cctx chasm.Context, _ struct{}) (model.Observed, error) {
		attempt := act.LastAttempt.Get(cctx)
		return model.Observed{
			Status:               act.GetStatus(),
			Count:                attempt.GetCount(),
			Stamp:                attempt.GetStamp(),
			ScheduleToCloseStamp: act.GetScheduleToCloseStamp(),
			ResetKeepPaused:      act.GetResetKeepPaused(),
			ResetRestoreOptions:  act.GetResetRestoreOptions(),
			FirstAttemptStarted:  act.GetFirstAttemptStartedTime() != nil,
			DispatchTimeSet:      attempt.GetDispatchTime() != nil,
		}, nil
	}, struct{}{})
}

func saaFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}

// --- declarative helpers -------------------------------------------------------------------
//
// These let a functional (sub)test script an activity's lifecycle against the driver — start it,
// advance it a step at a time, and assert observed state — so the test reads declaratively while
// exercising the real server.

// newSaaHarness builds a driver harness bound to env and cfg, resolving the ReadComponent context.
func newSaaHarness(t require.TestingT, env *standaloneActivityEnv, ctx context.Context, cfg model.Config) *saaHarness {
	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)
	return &saaHarness{env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(), cfg: cfg}
}

// requireObserved asserts the activity's observable internal state (read via ReadComponent) matches want.
func (a *saaActor) requireObserved(t require.TestingT, want model.AbstractState) {
	got, err := a.observed()
	require.NoError(t, err)
	require.Truef(t, want.SameObserved(got), "observed state:\n  want %+v\n  got  %+v", want, got)
}

// requireNoDispatch asserts a poll finds no task, i.e. the activity is not currently dispatchable.
func (a *saaActor) requireNoDispatch(t require.TestingT) {
	require.Nil(t, a.pollForTask(t, saaNegativePollTimeout), "expected no task to be dispatched")
}

// requireDispatch polls until a task is dispatched (blocking through any pending delay), captures its
// token, and returns the poll response.
func (a *saaActor) requireDispatch(t require.TestingT) *workflowservice.PollActivityTaskQueueResponse {
	resp := a.pollForTask(t, 10*time.Second)
	require.NotNil(t, resp, "expected a task to be dispatched")
	a.token = resp.GetTaskToken()
	return resp
}

// elapse waits out the wall-clock window behind e (a timeout or a dispatch-delay clock).
func (a *saaActor) elapse(e model.EventKind) {
	time.Sleep(a.h.eventClock(model.Event{Kind: e}) + saaWallClockSettle)
}
