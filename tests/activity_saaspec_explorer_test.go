package tests

// Explorer for the standalone-activity behavior spec (see chasm/lib/activity/saaspec and
// .task/saa-verification-plan.md).
//
// It walks the transition graph that saaspec.Model() describes and verifies every edge against
// a real onebox server. From each reachable state it tries every event; for the events Model()
// has decided, it replays the path from a fresh activity, drives the event as a real RPC, reads
// the internal state back with ReadComponent, and asserts:
//   - the resulting internal state equals Model().Next exactly (all fields, both stamps);
//   - the RPC's accept/reject outcome matches Model().Reject;
//   - for a heartbeat, the response flags equal ExpectedHeartbeatFlags.
// Undecided cells (Model panics with TODO, or returns an empty Outcome) are skipped, so coverage
// grows as the spec is filled in.
//
// Timers are configured long (hours) so no timeout fires mid-scenario; retry backoff is short so
// retries can be traversed. Timeout timing is checked by separate tests, not here.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const saaExplorerMaxDepth = 4

func (s *standaloneActivityTestSuite) TestSpecExplorer() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	for i, cfg := range saaExplorerConfigs {
		ex := &saaExplorer{
			env:      env,
			ctx:      ctx,
			chasmCtx: chasmCtx,
			nsID:     env.NamespaceID().String(),
			cfg:      cfg,
			cfgIdx:   i,
		}
		ex.explore(t)
	}
}

var saaExplorerConfigs = []saaspec.Config{
	{}, // no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
}

type saaExplorer struct {
	env      *standaloneActivityEnv
	ctx      context.Context
	chasmCtx context.Context
	nsID     string
	cfg      saaspec.Config
	cfgIdx   int
	counter  int
	// shortTimer, when set to one of the *Fires event kinds, makes that timeout short at Start so
	// the timer test can trigger it. The explorer leaves it at its zero value (Poll), so all
	// timeouts are long and no timer fires during RPC exploration.
	shortTimer saaspec.EventKind
}

// explore does a breadth-first walk of the model's reachable states, verifying every decided
// edge against the server.
func (ex *saaExplorer) explore(t *testing.T) {
	type node struct {
		path  []saaspec.Event
		state saaspec.AbstractState
	}
	start := saaspec.Initial(ex.cfg)
	visited := map[string]bool{saaFingerprint(start): true}
	frontier := []node{{nil, start}}

	verifiedCells := map[saaCell]bool{}
	skippedCells := map[saaCell]bool{}

	ex.verifyPath(t, nil) // the freshly started activity matches Initial(cfg)

	edges, states := 0, 1
	for depth := 0; depth < saaExplorerMaxDepth && len(frontier) > 0; depth++ {
		var next []node
		for _, nd := range frontier {
			for _, e := range saaCandidateEvents() {
				out, decided := saaEvalModel(ex.cfg, nd.state, e)
				if !decided {
					continue
				}
				edges++
				path := append(append([]saaspec.Event{}, nd.path...), e)
				res, reached := ex.verifyPath(t, path)
				if reached {
					c := saaCell{nd.state.Status, e.Kind}
					if res == saaSkippedNoToken {
						skippedCells[c] = true
					} else {
						verifiedCells[c] = true
					}
				}
				if out.Reject != saaspec.NoError {
					continue // rejected/no-op: no new state to extend from
				}
				fp := saaFingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					states++
					next = append(next, node{path, out.Next})
				}
			}
		}
		frontier = next
	}

	// Coverage ledger. The only decided edges the explorer does not verify are worker RPCs reached
	// on a path that never polled (no task token to send); surface those so the gap stays visible.
	var unexercised []string
	for c := range skippedCells {
		if !verifiedCells[c] {
			unexercised = append(unexercised, fmt.Sprintf("%s/%s", c.status, saaKindName(c.kind)))
		}
	}
	sort.Strings(unexercised)
	t.Logf("cfg %d: verified %d decided edges (%d distinct cells) across %d reachable states (depth<=%d)",
		ex.cfgIdx, edges, len(verifiedCells), states, saaExplorerMaxDepth)
	if len(unexercised) > 0 {
		t.Logf("cfg %d: decided cells NOT exercised (worker RPC, no token on a never-polled path): %v",
			ex.cfgIdx, unexercised)
	}
}

// verifyPath starts a fresh activity, replays the path (asserting only the final edge), and
// aborts silently if a prefix edge diverges — that edge is reported when it is itself a final
// edge of its own shorter path.
func (ex *saaExplorer) verifyPath(t require.TestingT, path []saaspec.Event) (saaApply, bool) {
	a := ex.start(t)
	cur := saaspec.Initial(ex.cfg)

	// The freshly started activity should match Initial(cfg).
	obs, err := a.observed()
	require.NoError(t, err)
	if obs != cur {
		t.Errorf("after Start (cfg %d): state got %+v want %+v", ex.cfgIdx, obs, cur)
		return saaMismatch, false
	}

	for i, e := range path {
		out := saaspec.Model(ex.cfg, cur, e) // decided: guaranteed by explore()
		final := i == len(path)-1
		res := a.apply(t, e, cur, out, final)
		if final {
			return res, true // res concerns the final edge, which the ledger records
		}
		if res != saaVerified {
			return res, false // prefix diverged or was skipped; that edge is checked as its own path
		}
		cur = out.Next
	}
	return saaVerified, false // empty path: only the Initial check ran
}

// --- driving one event ---------------------------------------------------------------------

// saaApply is the outcome of driving one event, for the coverage ledger.
type saaApply int

const (
	saaVerified       saaApply = iota // the RPC was driven and the result checked
	saaMismatch                       // driven, but the result did not match the model
	saaSkippedNoToken                 // a worker RPC with no task token held; not drivable on this path
)

// saaCell identifies a (source status, event kind) pair for the coverage ledger.
type saaCell struct {
	status saaspec.Status
	kind   saaspec.EventKind
}

func (a *saaActor) apply(t require.TestingT, e saaspec.Event, cur saaspec.AbstractState, out saaspec.Outcome, final bool) saaApply {
	if e.Kind == saaspec.Poll {
		return a.applyPoll(cur, out, final, t)
	}
	// Worker RPCs authenticate with a task token, which the actor only holds after a poll. On a path
	// that never polled (e.g. a fresh SCHEDULED or PAUSED activity) there is no token; sending an
	// empty token yields a different error than the token-validation NotFound the spec means, so this
	// edge is not drivable here. The coverage ledger records the skip.
	if saaNeedsToken(e.Kind) && a.token == nil {
		return saaSkippedNoToken
	}
	err := a.rpc(e)
	ok := a.verify(t, e, out, err, final)
	if e.Kind == saaspec.Heartbeat && out.Reject == saaspec.NoError {
		got := saaspec.HeartbeatFlags{
			CancelRequested: a.lastHeartbeat.GetCancelRequested(),
			ActivityPaused:  a.lastHeartbeat.GetActivityPaused(),
			ActivityReset:   a.lastHeartbeat.GetActivityReset(),
		}
		want := saaspec.ExpectedHeartbeatFlags(cur)
		if got != want {
			ok = false
			if final {
				t.Errorf("Heartbeat flags from %s: got %+v want %+v", cur.Status, got, want)
			}
		}
	}
	if ok {
		return saaVerified
	}
	return saaMismatch
}

// verify checks the current state against the model's predicted Outcome and, on the final edge, the
// public Describe projection.
func (a *saaActor) verify(t require.TestingT, e saaspec.Event, out saaspec.Outcome, rpcErr error, final bool) bool {
	gotKind := saaRejectKind(rpcErr)
	obs, err := a.observed()
	require.NoError(t, err)
	ok := gotKind == out.Reject && obs == out.Next
	if final {
		if gotKind != out.Reject {
			t.Errorf("%s from %s: reject kind got %v want %v (err=%v)", saaKindName(e.Kind), out.Next.Status, gotKind, out.Reject, rpcErr)
		}
		if obs != out.Next {
			t.Errorf("%s: resulting state got %+v want %+v", saaKindName(e.Kind), obs, out.Next)
		}
		a.checkDescribe(t, out.Next)
	}
	return ok
}

func (a *saaActor) applyPoll(cur saaspec.AbstractState, out saaspec.Outcome, final bool, t require.TestingT) saaApply {
	expectTask := cur.Status == saaspec.Scheduled && out.Next.Status == saaspec.Started
	if expectTask {
		resp := a.pollForTask(10 * time.Second)
		if resp == nil {
			if final {
				t.Errorf("Poll from %s: spec predicts STARTED but no task was dispatched", cur.Status)
			}
			return saaMismatch
		}
		a.token = resp.GetTaskToken()
		if final && resp.GetAttempt() != out.Next.Count {
			t.Errorf("Poll from %s: task Attempt got %d want %d", cur.Status, resp.GetAttempt(), out.Next.Count)
		}
	} else if resp := a.pollForTask(300 * time.Millisecond); resp != nil {
		if final {
			t.Errorf("Poll from %s: spec predicts no advance but a task was dispatched", cur.Status)
		}
		return saaMismatch
	}
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if obs != out.Next {
			t.Errorf("Poll: resulting state got %+v want %+v", obs, out.Next)
		}
		a.checkDescribe(t, out.Next)
	}
	if obs == out.Next {
		return saaVerified
	}
	return saaMismatch
}

// checkDescribe asserts the public status and run state DescribeActivityExecution reports match
// ExpectedDescribe. It is a no-op when ExpectedDescribe has not decided this state.
func (a *saaActor) checkDescribe(t require.TestingT, expected saaspec.AbstractState) {
	st, rs, ok := saaExpectedDescribe(expected)
	if !ok {
		return
	}
	resp, err := a.ex.env.FrontendClient().DescribeActivityExecution(a.ex.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace: a.ex.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
	})
	require.NoError(t, err)
	gotSt, gotRs := resp.GetInfo().GetStatus(), resp.GetInfo().GetRunState()
	if gotSt != st || gotRs != rs {
		t.Errorf("Describe from %s: got (status=%v run=%v) want (status=%v run=%v)", expected.Status, gotSt, gotRs, st, rs)
	}
}

// rpc performs the RPC for a non-Poll event and returns its error.
func (a *saaActor) rpc(e saaspec.Event) error {
	fc := a.ex.env.FrontendClient()
	ns := a.ex.env.Namespace().String()
	switch e.Kind {
	case saaspec.Heartbeat:
		resp, err := fc.RecordActivityTaskHeartbeat(a.ex.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns,
			TaskToken: a.token,
		})
		a.lastHeartbeat = resp
		return err
	case saaspec.RespondCompleted:
		_, err := fc.RespondActivityTaskCompleted(a.ex.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case saaspec.RespondFailed:
		_, err := fc.RespondActivityTaskFailed(a.ex.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: saaFailure(e.Retryable),
		})
		return err
	case saaspec.RespondCanceled:
		_, err := fc.RespondActivityTaskCanceled(a.ex.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case saaspec.RequestCancel:
		_, err := fc.RequestCancelActivityExecution(a.ex.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "explore", RequestId: a.reqID(e),
		})
		return err
	case saaspec.Terminate:
		_, err := fc.TerminateActivityExecution(a.ex.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "explore", RequestId: a.reqID(e),
		})
		return err
	case saaspec.Pause:
		_, err := fc.PauseActivityExecution(a.ex.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "explore", RequestId: a.reqID(e),
		})
		return err
	case saaspec.Unpause:
		_, err := fc.UnpauseActivityExecution(a.ex.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case saaspec.Reset:
		_, err := fc.ResetActivityExecution(a.ex.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case saaspec.UpdateOptions:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaExplorer: unhandled event kind %v", e.Kind)
	}
}

func (a *saaActor) updateOptions(e saaspec.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.ex.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: "op",
	}
	if e.RestoreOriginal {
		req.RestoreOriginal = true
	} else {
		// A minimal, always-valid update: re-set the heartbeat timeout. (RespondFailed-style
		// retryability and richer option merges are refined when the model decides these cells.)
		req.ActivityOptions = &apiactivitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.ex.env.FrontendClient().UpdateActivityExecutionOptions(a.ex.ctx, req)
	return err
}

// --- actor: one activity instance ----------------------------------------------------------

type saaActor struct {
	ex            *saaExplorer
	activityID    string
	taskQueue     string
	runID         string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	reqIDs        map[saaspec.EventKind]string
}

func (ex *saaExplorer) start(t require.TestingT) *saaActor {
	ex.counter++
	id := fmt.Sprintf("saaexp-%d-%d", ex.cfgIdx, ex.counter)
	resp, err := ex.env.FrontendClient().StartActivityExecution(ex.ctx, ex.startRequest(id, id))
	require.NoError(t, err)
	return &saaActor{ex: ex, activityID: id, taskQueue: id, runID: resp.RunId, reqIDs: map[saaspec.EventKind]string{}}
}

const saaShortTimer = 2 * time.Second

func (ex *saaExplorer) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	long := durationpb.New(time.Hour)
	// dur returns the short timeout for the one timer under test, long otherwise.
	dur := func(k saaspec.EventKind) *durationpb.Duration {
		if ex.shortTimer == k {
			return durationpb.New(saaShortTimer)
		}
		return long
	}
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           ex.env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        ex.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: dur(saaspec.StartToCloseFires),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(200 * time.Millisecond),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(200 * time.Millisecond),
			MaximumAttempts:    ex.cfg.MaxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if ex.cfg.HasScheduleToClose {
		req.ScheduleToCloseTimeout = dur(saaspec.ScheduleToCloseFires)
	}
	if ex.cfg.HasScheduleToStart {
		req.ScheduleToStartTimeout = dur(saaspec.ScheduleToStartFires)
	}
	if ex.cfg.HasHeartbeat {
		req.HeartbeatTimeout = dur(saaspec.HeartbeatFires)
	}
	return req
}

func (a *saaActor) observed() (saaspec.AbstractState, error) {
	o, err := saaReadObserved(a.ex.chasmCtx, a.ex.nsID, a.activityID, a.runID)
	if err != nil {
		return saaspec.AbstractState{}, err
	}
	return saaspec.Abstract(o), nil
}

func (a *saaActor) pollForTask(timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.ex.ctx, timeout)
	defer cancel()
	resp, err := a.ex.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.ex.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	// Any error (deadline, transient) means we did not get a task; the caller decides whether that
	// is expected. The empty response at a server-side long-poll timeout is likewise "no task".
	if err != nil || resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

func (a *saaActor) reqID(e saaspec.Event) string {
	if e.SameRequestID {
		if id, ok := a.reqIDs[e.Kind]; ok {
			return id
		}
	}
	id := uuid.NewString()
	a.reqIDs[e.Kind] = id
	return id
}

// --- helpers -------------------------------------------------------------------------------

func saaReadObserved(chasmCtx context.Context, nsID, activityID, runID string) (saaspec.Observed, error) {
	ref := chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
		NamespaceID: nsID, BusinessID: activityID, RunID: runID,
	})
	return chasm.ReadComponent(chasmCtx, ref, func(act *activity.Activity, cctx chasm.Context, _ struct{}) (saaspec.Observed, error) {
		attempt := act.LastAttempt.Get(cctx)
		return saaspec.Observed{
			Status:               act.GetStatus(),
			Count:                attempt.GetCount(),
			Stamp:                attempt.GetStamp(),
			ScheduleToCloseStamp: act.GetScheduleToCloseStamp(),
			ResetKeepPaused:      act.GetResetKeepPaused(),
			ResetHeartbeats:      act.GetResetHeartbeats(),
			ResetRestoreOptions:  act.GetResetRestoreOptions(),
			FirstAttemptStarted:  act.GetFirstAttemptStartedTime() != nil,
			DispatchTimeSet:      attempt.GetDispatchTime() != nil,
		}, nil
	}, struct{}{})
}

func saaNeedsToken(k saaspec.EventKind) bool {
	switch k {
	case saaspec.Heartbeat, saaspec.RespondCompleted, saaspec.RespondFailed, saaspec.RespondCanceled:
		return true
	default:
		return false
	}
}

// saaExpectedDescribe wraps saaspec.ExpectedDescribe, returning ok=false when the spec has not yet
// decided the projection for this state (the stub panics).
func saaExpectedDescribe(s saaspec.AbstractState) (st enumspb.ActivityExecutionStatus, rs enumspb.PendingActivityState, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	st, rs = saaspec.ExpectedDescribe(s)
	ok = true
	return
}

func saaRejectKind(err error) saaspec.ErrorKind {
	if err == nil {
		return saaspec.NoError
	}
	// The FrontendClient returns Temporal serviceerror types, so classify by type rather than by
	// gRPC status code.
	var nf *serviceerror.NotFound
	var fp *serviceerror.FailedPrecondition
	var ia *serviceerror.InvalidArgument
	switch {
	case errors.As(err, &nf):
		return saaspec.NotFound
	case errors.As(err, &fp):
		return saaspec.FailedPrecondition
	case errors.As(err, &ia):
		return saaspec.InvalidArgument
	default:
		return saaspec.ErrorKind(-1) // unrecognized: will not match any predicted kind
	}
}

func saaFailure(retryable bool) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "explore",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "explore",
				NonRetryable: !retryable,
			},
		},
	}
}

// saaEvalModel calls Model, treating TODO panics and empty Outcomes as "undecided" so the
// explorer skips them.
func saaEvalModel(cfg saaspec.Config, s saaspec.AbstractState, e saaspec.Event) (out saaspec.Outcome, decided bool) {
	defer func() {
		if r := recover(); r != nil {
			decided = false
		}
	}()
	out = saaspec.Model(cfg, s, e)
	if out.Reject == saaspec.NoError && out.Next.Status == saaspec.Unspecified && s.Status != saaspec.Unspecified {
		return out, false // empty Outcome placeholder
	}
	return out, true
}

func saaFingerprint(s saaspec.AbstractState) string {
	count := min(s.Count, 3)
	return fmt.Sprintf("%v|%d|%v|%v|%v|%v|%v|%v",
		s.Status, count, s.STCStamp > 0, s.ResetKeepPaused, s.ResetHeartbeats,
		s.ResetRestoreOptions, s.FirstAttemptStarted, s.DispatchTimeSet)
}

func saaCandidateEvents() []saaspec.Event {
	var out []saaspec.Event
	simple := []saaspec.EventKind{
		saaspec.Poll, saaspec.Heartbeat, saaspec.RespondCompleted, saaspec.RespondCanceled, saaspec.UpdateOptions,
	}
	for _, k := range simple {
		out = append(out, saaspec.Event{Kind: k})
	}
	for _, r := range []bool{false, true} {
		out = append(out, saaspec.Event{Kind: saaspec.RespondFailed, Retryable: r})
	}
	for _, sr := range []bool{false, true} {
		out = append(out,
			saaspec.Event{Kind: saaspec.Pause, SameRequestID: sr},
			saaspec.Event{Kind: saaspec.Terminate, SameRequestID: sr},
			saaspec.Event{Kind: saaspec.RequestCancel, SameRequestID: sr},
		)
	}
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, saaspec.Event{Kind: saaspec.Reset, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	for _, ra := range []bool{false, true} {
		out = append(out, saaspec.Event{Kind: saaspec.Unpause, ResetAttempts: ra})
	}
	return out
}

func saaKindName(k saaspec.EventKind) string {
	switch k {
	case saaspec.Poll:
		return "Poll"
	case saaspec.Heartbeat:
		return "Heartbeat"
	case saaspec.RespondCompleted:
		return "RespondCompleted"
	case saaspec.RespondFailed:
		return "RespondFailed"
	case saaspec.RespondCanceled:
		return "RespondCanceled"
	case saaspec.RequestCancel:
		return "RequestCancel"
	case saaspec.Terminate:
		return "Terminate"
	case saaspec.Pause:
		return "Pause"
	case saaspec.Unpause:
		return "Unpause"
	case saaspec.Reset:
		return "Reset"
	case saaspec.UpdateOptions:
		return "UpdateOptions"
	default:
		return fmt.Sprintf("EventKind(%d)", k)
	}
}
