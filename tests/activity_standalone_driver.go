package tests

// Driver for standalone-activity (SAA) tests: starts an activity and drives it through a scripted
// sequence of events (a trace), realizing each event as a frontend RPC, a poll, or a wall-clock wait.
// The event vocabulary is chasm/lib/activity/model.

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
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- the activity under test -----------------------------------------------------------------

// activityConfig is the activity a driver starts. One value configures either surface, so a parity
// test describes a single activity rather than two that might differ.
//
// Every field is the value it names. A zero duration leaves that option unset, which for a timeout
// means it never fires; the exceptions are noted. A trace that fires a timeout must therefore
// configure it: writing model.HeartbeatElapses into a trace requires a Heartbeat here, and the
// duration set is the window the driver waits out.
type activityConfig struct {
	MaxAttempts            int32         // RetryPolicy MaximumAttempts; 0 = unlimited
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityDefaultRetryInterval
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes

	StartToClose    time.Duration // 0 => activityLongTimeout, so it does not fire
	ScheduleToClose time.Duration
	ScheduleToStart time.Duration
	Heartbeat       time.Duration
	StartDelay      time.Duration // SAA only: WFA has no per-activity start delay
}

// activityParityDefaultInput is the payload the drivers start activities with. Its content is never asserted on.
var activityParityDefaultInput = payloads.EncodeString("Input")

// activityLongTimeout is a timeout long enough not to fire during a test.
const activityLongTimeout = time.Hour

// activityShortTimeout is a timeout short enough for a trace to wait out.
const activityShortTimeout = 2 * time.Second

func (c activityConfig) retryInterval() time.Duration {
	return cmp.Or(c.RetryInterval, activityDefaultRetryInterval)
}
func (c activityConfig) startToClose() time.Duration {
	return cmp.Or(c.StartToClose, activityLongTimeout)
}

// window is how long the clock behind a wall-clock event takes to elapse, from the option that event
// fires on. Zero for an event whose option is not configured, which no trace should drive.
func (c activityConfig) window(e model.Event) time.Duration {
	switch e.Type {
	case model.StartDelayElapsesType:
		return c.StartDelay
	case model.BackoffElapsesType:
		return cmp.Or(c.NextRetryDelay, c.retryInterval())
	case model.StartToCloseElapsesType:
		return c.startToClose()
	case model.ScheduleToCloseElapsesType:
		return c.ScheduleToClose
	case model.ScheduleToStartElapsesType:
		return c.ScheduleToStart
	case model.HeartbeatElapsesType:
		return c.Heartbeat
	default:
		return 0
	}
}

// modelConfig is the model's view of the activity: which options are configured at all. Deriving it
// means the two cannot disagree.
func (c activityConfig) modelConfig() model.Config {
	return model.Config{
		MaxAttempts:        c.MaxAttempts,
		HasStartDelay:      c.StartDelay > 0,
		HasScheduleToClose: c.ScheduleToClose > 0,
		HasScheduleToStart: c.ScheduleToStart > 0,
		HasHeartbeat:       c.Heartbeat > 0,
	}
}

// --- driver --------------------------------------------------------------------------------

type saaDriver struct {
	env        *testcore.TestEnv
	ctx        context.Context
	chasmCtx   context.Context // memoized by chasmContext
	cfg        activityConfig
	cfgIdx     int
	numStarted int
	idBase     string // activity-id prefix, unique per driver

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => driverPositivePollTimeout

	// customizeStart mutates the StartActivityExecutionRequest before it is sent.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// newSAADriver builds a driver with the test-scoped context and its own activity-id prefix.
func newSAADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *saaDriver {
	return &saaDriver{
		env:    env,
		ctx:    testcontext.For(t),
		cfg:    cfg,
		idBase: testcore.RandomizeStr(t.Name()),
	}
}

// activityDefaultRetryInterval is the RetryPolicy InitialInterval when a driver sets none.
const activityDefaultRetryInterval = 200 * time.Millisecond

// driverPositivePollTimeout bounds a poll that must find a task.
const driverPositivePollTimeout = 10 * time.Second

// driverWallClockSettle is slack added to a wall-clock event's window when waiting for its effect.
const driverWallClockSettle = 2 * time.Second

// driverPollInterval is the gap between reads when polling for a wall-clock event's effect.
const driverPollInterval = 100 * time.Millisecond

// saaPollTimeout is a poll timeout above common.MinLongPollTimeout, the floor below which the frontend
// rejects the poll rather than reaching matching.
const saaPollTimeout = common.MinLongPollTimeout + time.Second

// saaHandle is a handle to one activity instance: the ids that address it, plus the token last
// dispatched to it.
type saaHandle struct {
	d             *saaDriver
	activityID    string
	taskQueue     string
	runID         string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	// establishedReqID[eventType] is the request id that established the current state for an operator
	// command; a SameRequestID event reuses it. lastReqID is the most recent operator RPC's id, promoted
	// into establishedReqID by apply when that RPC changes state.
	establishedReqID map[model.EventType]string
	lastReqID        string
	path             []model.Event // events driven to reach the edge under test, for failure reports

	// Raw stamps, shifted cur->prev by each observed() read; see checkTaskInvalidation.
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
}

// driveTrace runs a trace on a fresh activity and returns a handle at the reached state. Model-free:
// each RPC must succeed.
func (d *saaDriver) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
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
	case e.Type == model.PollType:
		// A poll captures the dispatched task token.
		if resp := a.pollForTask(t, cmp.Or(d.positivePollTimeout, driverPositivePollTimeout)); resp != nil {
			a.token = resp.GetTaskToken()
		}
	case isWallClockEvent(e.Type):
		// A wall-clock event is realized by waiting out its configured window.
		a.awaitWallClock(t, e)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// awaitWallClock blocks until a wall-clock event's effect is visible, and fails if it is not within
// (window + settle). A timeout advances the transition-history version, so it is waited for with a long
// poll; a dispatch-delay elapse advances no version, so it is detected by NextAttemptScheduleTime
// clearing.
func (a *saaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	deadline := time.Now().Add(a.d.cfg.window(e) + driverWallClockSettle)
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchTimePassed(t, e, deadline)
		return
	}
	a.awaitStateTransition(t, e, deadline)
}

// awaitStateTransition long-polls DescribeActivityExecution until the transition-history version
// advances past the token's, and fails if none does by the deadline. An empty response means the
// server's long-poll window expired, so resubmit. Each poll is bounded by the deadline.
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
		"effect. Last observed: %+v", model.EventLabel(e), a.d.cfg.window(e)+driverWallClockSettle, a.projection(t))
}

// awaitDispatchTimePassed polls the public projection until the pending dispatch time has passed, and
// fails if it has not by the deadline.
func (a *saaHandle) awaitDispatchTimePassed(t require.TestingT, e model.Event, deadline time.Time) {
	for {
		p := a.projection(t)
		if !p.NextAttemptScheduleSet {
			return
		}
		if !time.Now().Before(deadline) {
			t.Errorf("%s: a dispatch is still pending in the future %s after driving the event, so the "+
				"window did not elapse. Last observed: %+v",
				model.EventLabel(e), a.d.cfg.window(e)+driverWallClockSettle, p)
			return
		}
		time.Sleep(driverPollInterval)
	}
}

func (d *saaDriver) start(t require.TestingT) *saaHandle {
	d.numStarted++
	// cfgIdx keeps ids distinct across the per-config drivers an explorer sweeps.
	id := fmt.Sprintf("%s-%d-%d", d.idBase, d.cfgIdx, d.numStarted)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.ctx, d.startRequest(id, id))
	require.NoError(t, err)
	return &saaHandle{d: d, activityID: id, taskQueue: id, runID: resp.RunId, establishedReqID: map[model.EventType]string{}}
}

func (d *saaDriver) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	c := d.cfg
	opt := func(v time.Duration) *durationpb.Duration {
		if v == 0 {
			return nil
		}
		return durationpb.New(v)
	}
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:              d.env.Namespace().String(),
		ActivityId:             activityID,
		ActivityType:           d.env.Tv().ActivityType(),
		Identity:               "worker",
		Input:                  activityParityDefaultInput,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout:    durationpb.New(c.startToClose()),
		ScheduleToCloseTimeout: opt(c.ScheduleToClose),
		ScheduleToStartTimeout: opt(c.ScheduleToStart),
		HeartbeatTimeout:       opt(c.Heartbeat),
		StartDelay:             opt(c.StartDelay),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(c.retryInterval()),
			BackoffCoefficient:     cmp.Or(c.BackoffCoefficient, 1.0),
			MaximumInterval:        durationpb.New(cmp.Or(c.MaxRetryInterval, c.retryInterval())),
			MaximumAttempts:        c.MaxAttempts,
			NonRetryableErrorTypes: c.NonRetryableErrorTypes,
		},
		RequestId: uuid.NewString(),
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

// projection is the activity's public info as an activityInfoProjection.
func (a *saaHandle) projection(t require.TestingT) activityInfoProjection {
	return projectSAA(a.describe(t).GetInfo())
}

// terminal is the terminal status from Info plus the failure discriminant from the Outcome.
func (a *saaHandle) terminal(t require.TestingT) activityTerminalProjection {
	resp := a.describe(t)
	return activityTerminalProjection{
		Status:      resp.GetInfo().GetStatus(),
		FailureType: saaFailureType(resp.GetOutcome().GetFailure()),
	}
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none.
func (a *saaHandle) terminalCause(t require.TestingT) failureCause {
	cause := a.describe(t).GetOutcome().GetFailure().GetCause()
	return failureCause{Type: saaFailureType(cause), Message: cause.GetMessage()}
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes.
func (a *saaHandle) heartbeatDetails(t require.TestingT) []byte {
	return firstPayloadData(a.describe(t).GetInfo().GetHeartbeatDetails())
}

// activityHeartbeatDetails is the checkpoint payload both drivers send with a Heartbeat event.
var activityHeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{
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

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.HeartbeatType:
		resp, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityHeartbeatDetails,
		})
		a.lastHeartbeat = resp
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.d.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RespondFailedType:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: activityFailure(e.Retryable, a.d.cfg.NextRetryDelay),
		})
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.d.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RequestCancelType:
		_, err := fc.RequestCancelActivityExecution(a.d.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.TerminateType:
		_, err := fc.TerminateActivityExecution(a.d.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsType:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaDriver: unhandled event type %v", e.Type)
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
// command type if the event is a SameRequestID replay, else a fresh one. It is recorded as lastReqID.
func (a *saaHandle) reqID(e model.Event) string {
	id := uuid.NewString()
	if e.SameRequestID {
		if est, ok := a.establishedReqID[e.Type]; ok {
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
			t.Errorf("saaDriver: test context budget exhausted before the poll could run (%.1fs left, need >= %s). "+
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`.\n  %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
			return nil
		}
		t.Errorf("saaDriver bug: PollActivityTaskQueue did not complete cleanly (poll timeout must be >= "+
			"MinLongPollTimeout; only an empty response with a nil error means \"no task\"): %v", err)
		return nil
	}
	if resp.GetActivityId() == "" {
		return nil // no task available
	}
	return resp
}

// isWallClockEvent reports whether an event fires on wall-clock time rather than synchronously.
func isWallClockEvent(k model.EventType) bool {
	switch k {
	case model.ScheduleToStartElapsesType, model.ScheduleToCloseElapsesType, model.StartToCloseElapsesType,
		model.HeartbeatElapsesType, model.StartDelayElapsesType, model.BackoffElapsesType:
		return true
	default:
		return false
	}
}

// isDispatchDelayEvent reports whether an event is a dispatch-delay window elapsing rather than a timeout.
// A dispatch delay advances no transition-history version; its effect is the pending dispatch time
// passing.
func isDispatchDelayEvent(k model.EventType) bool {
	return k == model.StartDelayElapsesType || k == model.BackoffElapsesType
}

func activityFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}
