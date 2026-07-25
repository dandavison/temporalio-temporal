package tests

// Driver for standalone-activity (SAA) tests: starts an activity and drives it through a scripted
// sequence of events (a trace), realizing each event as a frontend RPC, a poll, or a wall-clock wait.
// It makes no assertions. The event vocabulary is chasm/lib/activity/model.

import (
	"cmp"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- driver --------------------------------------------------------------------------------

type saaDriverDeclarative struct {
	env        *standaloneActivityEnv
	ctx        context.Context
	chasmCtx   context.Context // memoized by chasmContext
	cfg        model.Config
	cfgIdx     int
	numStarted int
	idBase     string // activity-id prefix, unique per driver

	shortTimeout        model.EventKind // this timeout is configured short at Start; zero leaves all timeouts long
	startDelay          time.Duration   // StartActivityExecutionRequest.StartDelay
	retryInterval       time.Duration   // RetryPolicy InitialInterval; 0 => saaDefaultRetryInterval
	backoffCoefficient  float64         // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	maxRetryInterval    time.Duration   // RetryPolicy MaximumInterval; 0 => the InitialInterval
	nextRetryDelay      time.Duration   // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	scheduleToClose     time.Duration   // ScheduleToCloseTimeout, overriding the long default
	positivePollTimeout time.Duration   // bounds a "must dispatch" poll; 0 => saaPositivePollTimeout

	// customizeStart mutates the StartActivityExecutionRequest before it is sent.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// newSAADriverDeclarative builds a driver with the test-scoped context and its own activity-id prefix. The
// caller sets whichever timing knobs it needs on the result.
func newSAADriverDeclarative(t *testing.T, env *standaloneActivityEnv, cfg model.Config) *saaDriverDeclarative {
	return &saaDriverDeclarative{
		env:    env,
		ctx:    testcontext.For(t),
		cfg:    cfg,
		idBase: testcore.RandomizeStr(t.Name()),
	}
}

const saaShortTimeout = 2 * time.Second

// saaDefaultRetryInterval is the RetryPolicy InitialInterval when a driver sets none.
const saaDefaultRetryInterval = 200 * time.Millisecond

// saaPositivePollTimeout bounds a poll that must find a task.
const saaPositivePollTimeout = 10 * time.Second

// saaWallClockSettle is slack added to a wall-clock event's window when waiting for its effect.
const saaWallClockSettle = 2 * time.Second

// saaPollInterval is the gap between reads when polling for a wall-clock event's effect.
const saaPollInterval = 100 * time.Millisecond

// saaPollTimeout is a poll timeout above common.MinLongPollTimeout, the floor below which the frontend
// rejects the poll rather than reaching matching.
const saaPollTimeout = common.MinLongPollTimeout + time.Second

// saaHandle is a handle to one activity instance: the ids that address it, plus the token last
// dispatched to it.
type saaHandle struct {
	d             *saaDriverDeclarative
	activityID    string
	taskQueue     string
	runID         string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	// establishedReqID[kind] is the request id that established the current state for an operator
	// command; a SameRequestID event reuses it. lastReqID is the most recent operator RPC's id, promoted
	// into establishedReqID by apply when that RPC changes state.
	establishedReqID map[model.EventKind]string
	lastReqID        string
	path             []model.Event // events driven to reach the edge under test, for failure reports

	// Raw stamps, shifted cur->prev by each observed() read; see checkTaskInvalidation.
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
}

// driveTrace runs a trace on a fresh activity and returns a handle at the reached state. Model-free:
// each RPC must succeed.
func (d *saaDriverDeclarative) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	a := d.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
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

// awaitWallClock blocks until a wall-clock event's effect is visible on the frontend surface, and reports
// a failure if it is not visible within (window + settle). A timeout advances the execution's
// transition-history version, so it is waited for with a long poll. A dispatch-delay elapse advances no
// version — the dispatch time simply passes — so it is detected by the read-time
// NextAttemptScheduleTime flip instead.
func (a *saaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	deadline := time.Now().Add(a.d.eventClock(e) + saaWallClockSettle)
	if saaIsDispatchDelay(e.Kind) {
		a.awaitDispatchTimePassed(t, e, deadline)
		return
	}
	a.awaitStateTransition(t, e, deadline)
}

// awaitStateTransition long-polls DescribeActivityExecution until the execution's transition-history
// version advances past the token's, and fails if none does by the deadline. An empty response means the
// server's long-poll window expired, so resubmit. Each long poll is bounded by the deadline so that a
// server window longer than the deadline cannot overrun it.
func (a *saaHandle) awaitStateTransition(t require.TestingT, e model.Event, deadline time.Time) {
	token := a.describe(t).GetLongPollToken()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(a.d.ctx, deadline)
		resp, err := a.d.env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     a.d.env.Namespace().String(),
			ActivityId:    a.activityID,
			RunId:         a.runID,
			LongPollToken: token,
		})
		cancel()
		if err != nil {
			if time.Now().Before(deadline) {
				require.NoError(t, err)
			}
			break // the deadline cancelled the long poll
		}
		if resp.GetInfo() != nil {
			return // non-empty: the state advanced
		}
	}
	t.Errorf("%s: the activity did not transition within %s of driving the event, so the event did not take "+
		"effect. Last observed: %+v", model.EventLabel(e), a.d.eventClock(e)+saaWallClockSettle, a.projection(t))
}

// awaitDispatchTimePassed polls the public projection until the pending dispatch time has passed, which
// is how a start-delay or retry-backoff window elapsing is observable, and fails if it has not by the
// deadline.
func (a *saaHandle) awaitDispatchTimePassed(t require.TestingT, e model.Event, deadline time.Time) {
	for {
		p := a.projection(t)
		if !p.NextAttemptScheduleSet {
			return
		}
		if !time.Now().Before(deadline) {
			t.Errorf("%s: a dispatch is still pending in the future %s after driving the event, so the "+
				"window did not elapse. Last observed: %+v",
				model.EventLabel(e), a.d.eventClock(e)+saaWallClockSettle, p)
			return
		}
		time.Sleep(saaPollInterval)
	}
}

// driveTraceWithModelConformanceChecking drives a trace like driveTrace, additionally checking each
// step against model.Transition (see apply). The state after Start must equal model.Initial(cfg).
// Requires a config the model can see in full, so no customizeStart.
func (d *saaDriverDeclarative) driveTraceWithModelConformanceChecking(t *testing.T, trace []model.Event) *saaHandle {
	a := d.start(t)
	a.path = trace
	cur := model.Initial(d.cfg)
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("after Start, state disagrees with Initial(cfg).\n%s", saaStateDiff(obs, cur))
	}
	for _, e := range trace {
		out := model.Transition(d.cfg, cur, e)
		a.apply(t, e, cur, out, true)
		cur = out.Next
	}
	return a
}

func (d *saaDriverDeclarative) start(t require.TestingT) *saaHandle {
	d.requireConsistentConfig(t)
	d.numStarted++
	// cfgIdx keeps ids distinct across the per-config drivers an explorer sweeps.
	id := fmt.Sprintf("%s-%d-%d", d.idBase, d.cfgIdx, d.numStarted)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.ctx, d.startRequest(id, id))
	require.NoError(t, err)
	return &saaHandle{d: d, activityID: id, taskQueue: id, runID: resp.RunId, establishedReqID: map[model.EventKind]string{}}
}

func (d *saaDriverDeclarative) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	long := durationpb.New(time.Hour)
	dur := func(k model.EventKind) *durationpb.Duration {
		if d.shortTimeout == k {
			return durationpb.New(saaShortTimeout)
		}
		return long
	}
	retryInterval := d.effectiveRetryInterval()
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           d.env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        d.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: dur(model.StartToCloseElapsesKind),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(retryInterval),
			BackoffCoefficient: cmp.Or(d.backoffCoefficient, 1.0),
			MaximumInterval:    durationpb.New(cmp.Or(d.maxRetryInterval, retryInterval)),
			MaximumAttempts:    d.cfg.MaxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if d.startDelay > 0 {
		req.StartDelay = durationpb.New(d.startDelay)
	}
	if d.cfg.HasScheduleToClose {
		if d.scheduleToClose > 0 {
			req.ScheduleToCloseTimeout = durationpb.New(d.scheduleToClose)
		} else {
			req.ScheduleToCloseTimeout = dur(model.ScheduleToCloseElapsesKind)
		}
	}
	if d.cfg.HasScheduleToStart {
		req.ScheduleToStartTimeout = dur(model.ScheduleToStartElapsesKind)
	}
	if d.cfg.HasHeartbeat {
		req.HeartbeatTimeout = dur(model.HeartbeatElapsesKind)
	}
	if d.customizeStart != nil {
		d.customizeStart(req)
	}
	return req
}

// describe returns the DescribeActivityExecution response, including the outcome, the last failure, and
// the heartbeat details.
func (a *saaHandle) describe(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := a.d.env.FrontendClient().DescribeActivityExecution(a.d.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:               a.d.env.Namespace().String(),
		ActivityId:              a.activityID,
		RunId:                   a.runID,
		IncludeOutcome:          true,
		IncludeLastFailure:      true,
		IncludeHeartbeatDetails: true,
	})
	require.NoError(t, err)
	return resp
}

// projection is the activity's public info as an activityInfoProjection. Parallel to
// wfaHandle.projection.
func (a *saaHandle) projection(t require.TestingT) activityInfoProjection {
	return projectSAA(a.describe(t).GetInfo())
}

// terminal is the terminal status from Info plus the failure discriminant from the Outcome. Parallel to
// wfaHandle.terminal.
func (a *saaHandle) terminal(t require.TestingT) activityTerminalProjection {
	resp := a.describe(t)
	return activityTerminalProjection{
		Status:      resp.GetInfo().GetStatus(),
		FailureType: saaFailureType(resp.GetOutcome().GetFailure()),
	}
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none.
// Parallel to wfaHandle.terminalCause.
func (a *saaHandle) terminalCause(t require.TestingT) failureCause {
	cause := a.describe(t).GetOutcome().GetFailure().GetCause()
	return failureCause{Type: saaFailureType(cause), Message: cause.GetMessage()}
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes. Parallel to
// wfaHandle.heartbeatDetails.
func (a *saaHandle) heartbeatDetails(t require.TestingT) []byte {
	return firstPayloadData(a.describe(t).GetInfo().GetHeartbeatDetails())
}

// saaHeartbeatDetails is the checkpoint payload both drivers send with a Heartbeat event.
var saaHeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{
	Metadata: map[string][]byte{"encoding": []byte("json/plain")},
	Data:     []byte(`"hb"`),
}}}

func firstPayloadData(p *commonpb.Payloads) []byte {
	if ps := p.GetPayloads(); len(ps) > 0 {
		return ps[0].GetData()
	}
	return nil
}

// saaFailureType is the application failure Type, the TimeoutType string, or "" for neither.
func saaFailureType(f *failurepb.Failure) string {
	if app := f.GetApplicationFailureInfo(); app != nil {
		return app.GetType()
	}
	if to := f.GetTimeoutFailureInfo(); to != nil {
		return to.GetTimeoutType().String()
	}
	return ""
}

func projectSAA(i *apiactivitypb.ActivityExecutionInfo) activityInfoProjection {
	return activityInfoProjection{
		State:                  i.GetRunState(),
		Attempt:                i.GetAttempt(),
		CurrentRetryInterval:   i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleSet: i.GetNextAttemptScheduleTime() != nil,
	}
}

// observed is the activity's internal state as the model's AbstractState. It shifts the raw stamps
// cur->prev, so a caller can compare the stamp change across the last edge.
func (a *saaHandle) observed() (model.AbstractState, error) {
	o, err := a.readObserved()
	if err != nil {
		return model.AbstractState{}, err
	}
	a.prevStamp, a.curStamp = a.curStamp, o.Stamp
	a.prevSTCStamp, a.curSTCStamp = a.curSTCStamp, o.ScheduleToCloseStamp
	return model.Abstract(o), nil
}

// observedRaw is observed without the stamp shift, for use in a polling loop.
func (a *saaHandle) observedRaw() (model.AbstractState, error) {
	o, err := a.readObserved()
	if err != nil {
		return model.AbstractState{}, err
	}
	return model.Abstract(o), nil
}

// awaitObservedMatch polls the internal state until it matches want, or the deadline passes.
func (a *saaHandle) awaitObservedMatch(want model.AbstractState, deadline time.Time) {
	for {
		if obs, err := a.observedRaw(); err == nil && want.SameObserved(obs) {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(saaPollInterval)
	}
}

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Kind {
	case model.HeartbeatKind:
		resp, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: saaHeartbeatDetails,
		})
		a.lastHeartbeat = resp
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
		_, err := fc.RequestCancelActivityExecution(a.d.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.TerminateKind:
		_, err := fc.TerminateActivityExecution(a.d.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.PauseKind:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.UnpauseKind:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.ResetKind:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsKind:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaDriverDeclarative: unhandled event kind %v", e.Kind)
	}
}

func (a *saaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: "op",
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

// reqID is the request id for an operator command: the id that established the current state for that
// command kind if the event is a SameRequestID replay, else a fresh one. It is recorded as lastReqID.
func (a *saaHandle) reqID(e model.Event) string {
	id := uuid.NewString()
	if e.SameRequestID {
		if est, ok := a.establishedReqID[e.Kind]; ok {
			id = est
		}
	}
	a.lastReqID = id
	return id
}

func (a *saaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.d.ctx, timeout)
	defer cancel()
	resp, err := a.d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue},
		Identity:  "worker",
	})
	// Matching signals "waited, found nothing" with an empty response and a nil error, so any error
	// means the poll did not complete cleanly.
	if err != nil {
		if a.d.ctx.Err() != nil {
			return nil // teardown
		}
		if deadline, ok := a.d.ctx.Deadline(); ok && time.Until(deadline) < common.MinLongPollTimeout {
			t.Errorf("saaDriverDeclarative: test context budget exhausted before the poll could run (%.1fs left, need >= %s). "+
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`.\n  %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
			return nil
		}
		t.Errorf("saaDriverDeclarative bug: PollActivityTaskQueue did not complete cleanly (poll timeout must be >= "+
			"MinLongPollTimeout; only an empty response with a nil error means \"no task\"): %v", err)
		return nil
	}
	if resp.GetActivityId() == "" {
		return nil // no task available
	}
	return resp
}

// eventClock is how long the clock behind a wall-clock event takes to elapse.
func (d *saaDriverDeclarative) eventClock(e model.Event) time.Duration {
	switch e.Kind {
	case model.StartDelayElapsesKind:
		return d.dispatchDelay(model.StartDelayPending)
	case model.BackoffElapsesKind:
		return d.dispatchDelay(model.BackoffPending)
	default: // the four timeouts
		return saaShortTimeout
	}
}

// dispatchDelay is how long the driver configured the pending delay to last.
func (d *saaDriverDeclarative) dispatchDelay(disp model.Dispatchability) time.Duration {
	switch disp {
	case model.StartDelayPending:
		return d.startDelay
	case model.BackoffPending:
		return cmp.Or(d.nextRetryDelay, d.effectiveRetryInterval())
	default:
		return 0
	}
}

// effectiveRetryInterval is the RetryPolicy InitialInterval the driver starts activities with.
func (d *saaDriverDeclarative) effectiveRetryInterval() time.Duration {
	return cmp.Or(d.retryInterval, saaDefaultRetryInterval)
}

// requireConsistentConfig fails unless cfg and the timing knobs describe the same activity. cfg is what
// the model reasons about and the knobs are what startRequest sends, so a disagreement means the test is
// asserting against an activity nobody configured.
func (d *saaDriverDeclarative) requireConsistentConfig(t require.TestingT) {
	requireBoth := func(flag bool, knobSet bool, flagName, knobName string) {
		if flag && !knobSet {
			require.Fail(t, fmt.Sprintf("saaDriverDeclarative misconfigured: cfg.%s requires %s", flagName, knobName))
		}
		if knobSet && !flag {
			require.Fail(t, fmt.Sprintf("saaDriverDeclarative misconfigured: %s requires cfg.%s, or the model cannot "+
				"see it", knobName, flagName))
		}
	}
	requireBoth(d.cfg.HasStartDelay, d.startDelay > 0, "HasStartDelay", "startDelay")
	// A timeout knob only reaches the request when its cfg flag is set, so the flag is what makes the knob
	// meaningful. The reverse does not hold: a set flag with no knob configures that timeout long, which is
	// how a trace leaves a timeout alive without firing it.
	if d.scheduleToClose > 0 && !d.cfg.HasScheduleToClose {
		require.Fail(t, "saaDriverDeclarative misconfigured: scheduleToClose requires cfg.HasScheduleToClose, or "+
			"startRequest drops it")
	}
	// Shortening a timeout so a trace can fire it requires that timeout to be configured at all.
	for _, c := range []struct {
		kind     model.EventKind
		flag     bool
		flagName string
	}{
		{model.ScheduleToCloseElapsesKind, d.cfg.HasScheduleToClose, "HasScheduleToClose"},
		{model.ScheduleToStartElapsesKind, d.cfg.HasScheduleToStart, "HasScheduleToStart"},
		{model.HeartbeatElapsesKind, d.cfg.HasHeartbeat, "HasHeartbeat"},
	} {
		if d.shortTimeout == c.kind && !c.flag {
			require.Fail(t, fmt.Sprintf("saaDriverDeclarative misconfigured: shortTimeout=%s requires cfg.%s, or that "+
				"timeout is never configured and the event cannot fire", model.KindName(c.kind), c.flagName))
		}
	}
}

// chasmContext is the context ReadComponent needs to read internal component state, memoized.
func (d *saaDriverDeclarative) chasmContext() (context.Context, error) {
	if d.chasmCtx == nil {
		ctx, err := d.env.GetTestCluster().Host().ChasmContext(d.ctx)
		if err != nil {
			return nil, err
		}
		d.chasmCtx = ctx
	}
	return d.chasmCtx, nil
}

// readObserved reads the activity's internal component state.
func (a *saaHandle) readObserved() (model.Observed, error) {
	chasmCtx, err := a.d.chasmContext()
	if err != nil {
		return model.Observed{}, err
	}
	ref := chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
		NamespaceID: a.d.env.NamespaceID().String(), BusinessID: a.activityID, RunID: a.runID,
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

// saaIsWallClock reports whether an event fires on wall-clock time rather than synchronously.
func saaIsWallClock(k model.EventKind) bool {
	switch k {
	case model.ScheduleToStartElapsesKind, model.ScheduleToCloseElapsesKind, model.StartToCloseElapsesKind,
		model.HeartbeatElapsesKind, model.StartDelayElapsesKind, model.BackoffElapsesKind:
		return true
	default:
		return false
	}
}

// saaIsDispatchDelay reports whether an event is a dispatch-delay window elapsing, as opposed to a
// timeout. A dispatch delay advances no transition-history version; its effect is the pending dispatch
// time passing.
func saaIsDispatchDelay(k model.EventKind) bool {
	return k == model.StartDelayElapsesKind || k == model.BackoffElapsesKind
}

// saaTimeoutIn is the timeout whose *Elapses event a trace fires, zero if none. A trace fires at most
// one, as its final event.
func saaTimeoutIn(trace []model.Event) model.EventKind {
	for _, e := range trace {
		switch e.Kind {
		case model.ScheduleToStartElapsesKind, model.ScheduleToCloseElapsesKind,
			model.StartToCloseElapsesKind, model.HeartbeatElapsesKind:
			return e.Kind
		}
	}
	return 0 // none; zero value (Poll) means no timeout is shortened
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

// --- traces --------------------------------------------------------------------------------
//
// A trace is an event sequence run once on one fresh activity. Writing a timeout's *Elapses event into
// the sequence is what makes the driver configure that timeout short, so that it fires.

type saaTrace struct {
	trace          []model.Event
	maxAttempts    int32         // RetryPolicy MaximumAttempts (0 = unlimited)
	startDelayed   bool          // activity created with a start_delay; see startDelay for the window length
	retryInterval  time.Duration // RetryPolicy InitialInterval
	nextRetryDelay time.Duration // worker-supplied next_retry_delay
	// customizeStart mutates the StartActivityExecutionRequest before it is sent.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// config is the model Config the trace implies: MaxAttempts, plus a timeout flag per timeout the trace
// fires.
func (tr saaTrace) config() model.Config {
	cfg := model.Config{MaxAttempts: tr.maxAttempts, HasStartDelay: tr.startDelayed}
	for _, e := range tr.trace {
		switch e.Kind {
		case model.ScheduleToStartElapsesKind:
			cfg.HasScheduleToStart = true
		case model.ScheduleToCloseElapsesKind:
			cfg.HasScheduleToClose = true
		case model.HeartbeatElapsesKind:
			cfg.HasHeartbeat = true
		}
	}
	return cfg
}

// startDelay is the activity's start_delay: short when the trace fires StartDelayElapses, otherwise
// long enough to stay open for the whole trace. Zero when not start-delayed.
func (tr saaTrace) startDelay() time.Duration {
	if !tr.startDelayed {
		return 0
	}
	for _, e := range tr.trace {
		if e.Kind == model.StartDelayElapsesKind {
			return saaDelayWindow
		}
	}
	return saaLongStartDelay
}

// saaDelayWindow is a dispatch-delay window long enough to outlast a valid negative long poll, so that
// "not dispatchable yet" is observable within it.
const saaDelayWindow = 5 * time.Second

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace.
const saaLongStartDelay = time.Hour
