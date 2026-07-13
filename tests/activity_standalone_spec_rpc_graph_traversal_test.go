package tests

// RPC graph traversal for the standalone-activity behavior spec (see chasm/lib/activity/saaspec and
// .task/saa-verification-plan.md).
//
// It walks the transition graph that saaspec.Model() describes and verifies every edge against
// a real onebox server. From each reachable state it tries every event, replays the path from a
// fresh activity, drives the event as a real RPC, reads the internal state back with
// ReadComponent, and asserts:
//   - the resulting internal state equals Model().Next exactly (all fields, both stamps);
//   - the RPC's accept/reject outcome matches Model().Reject;
//   - for a heartbeat, the response flags equal ExpectedHeartbeatFlags.
// Model() is total over the RPC event alphabet; a cell it does not handle panics and fails the run.
//
// Timeouts are configured long (hours) so none fires mid-scenario; retry backoff is short so
// retries can be traversed. Timeout timing is checked by separate tests, not here.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// saaMaxDepth is the BFS depth cap. The default keeps CI fast; SAASPEC_MAX_DEPTH raises it for
// deeper local verification. Cost grows with depth — mostly the per-Paused negative poll, which
// SAASPEC_NO_NEGATIVE_POLL can disable (see applyPoll).
func saaMaxDepth() int {
	if v := os.Getenv("SAASPEC_MAX_DEPTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 4
}

// saaSkipNegativePoll disables the ~3s "a Paused activity must not dispatch" long-poll — the
// dominant cost of deep walks. Set SAASPEC_NO_NEGATIVE_POLL for fast deep runs; the per-edge state
// check still verifies the Paused transition, only the matching-level assertion is dropped.
func saaSkipNegativePoll() bool { return os.Getenv("SAASPEC_NO_NEGATIVE_POLL") != "" }

func (s *standaloneActivityTestSuite) TestSpecRPCGraphTraversal() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	// SAASPEC_EVENT=Reset,Pause focuses reporting on those event kinds. The full graph is still
	// traversed (reaching a state needs the other events); only the listed events are asserted.
	focus := saaParseFocus(os.Getenv("SAASPEC_EVENT"))
	if len(focus) > 0 {
		t.Logf("SAASPEC_EVENT focus: reporting only on %s", os.Getenv("SAASPEC_EVENT"))
	}

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env:      env,
			ctx:      ctx,
			chasmCtx: chasmCtx,
			nsID:     env.NamespaceID().String(),
			cfg:      cfg,
			cfgIdx:   i,
			focus:    focus,
		}
		h.traverse(t)
	}
}

var saaTraversalConfigs = []saaspec.Config{
	{}, // no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
	// Retries exhaust after the first attempt, so the RespondFailed exhaustion boundary
	// (retryable failure with no retries left -> Failed) is reached at depth 2 rather than
	// past the depth bound. See the completeness check.
	{MaxAttempts: 1},
}

type saaHarness struct {
	env      *standaloneActivityEnv
	ctx      context.Context
	chasmCtx context.Context
	nsID     string
	cfg      saaspec.Config
	cfgIdx   int
	counter  int
	// focus, when non-empty, limits which final-edge events are reported (see SAASPEC_EVENT). The
	// full graph is still traversed.
	focus map[saaspec.EventKind]bool
	// shortTimeout, when set to one of the four timeout *Fires kinds, makes that timeout short at
	// Start so the timeout traces can trigger it. The traversal leaves it at its zero value (Poll),
	// so all timeouts are long and none fires during RPC traversal.
	shortTimeout saaspec.EventKind
	// The dispatch-delay paths set these; the RPC graph traversal leaves them zero.
	startDelay     time.Duration // StartActivityExecutionRequest.StartDelay
	retryInterval  time.Duration // RetryPolicy InitialInterval; 0 => the default short backoff
	nextRetryDelay time.Duration // ApplicationFailureInfo.NextRetryDelay injected into RespondFailed
	// positivePollTimeout bounds the "must dispatch" poll; 0 => 10s (the RPC traversal's generous
	// default). The dispatch-delay traces set it just above the long-poll minimum so a Dispatchable
	// state must dispatch promptly, not merely eventually.
	positivePollTimeout time.Duration
}

// traverse does a breadth-first walk of the model's reachable states, verifying every decided
// edge against the server.
func (h *saaHarness) traverse(t *testing.T) {
	type node struct {
		path  []saaspec.Event
		state saaspec.AbstractState
	}
	start := saaspec.Initial(h.cfg)
	visited := map[string]bool{saaFingerprint(start): true}
	frontier := []node{{nil, start}}

	verifiedCells := map[saaCell]bool{}
	skippedCells := map[saaCell]bool{}
	// Fingerprint-granularity ledger (includes attempt-count bucket etc.) for the completeness check.
	verifiedFine := map[string]bool{}
	skippedFine := map[string]bool{}

	h.verifyPath(t, nil) // the freshly started activity matches Initial(cfg)

	edges, states := 0, 1
	maxDepth := saaMaxDepth()
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []node
		for _, nd := range frontier {
			for _, e := range saaCandidateEvents() {
				out := saaspec.Model(h.cfg, nd.state, e)
				edges++
				path := append(append([]saaspec.Event{}, nd.path...), e)
				res, reached := h.verifyPath(t, path)
				if reached {
					c := saaCell{nd.state.Status, e.Kind}
					key := saaCellKey(nd.state, e.Kind)
					if res == saaSkippedNoToken {
						skippedCells[c] = true
						skippedFine[key] = true
					} else {
						verifiedCells[c] = true
						verifiedFine[key] = true
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

	// Coverage ledger. The only decided edges the traversal does not verify are worker RPCs reached
	// on a path that never polled (no task token to send); surface those so the gap stays visible.
	t.Logf("cfg %d: verified %d decided edges (%d distinct cells) across %d reachable states (depth<=%d)",
		h.cfgIdx, edges, len(verifiedCells), states, maxDepth)

	// Coverage detail (the no-token-skip ledger) prints only under SAASPEC_COMPLETENESS, so the
	// default output is just the spec violations.
	if os.Getenv("SAASPEC_COMPLETENESS") != "" {
		var unexercised []string
		for c := range skippedCells {
			if !verifiedCells[c] {
				unexercised = append(unexercised, fmt.Sprintf("%s/%s", c.status, saaKindName(c.kind)))
			}
		}
		sort.Strings(unexercised)
		if len(unexercised) > 0 {
			t.Logf("cfg %d: decided cells NOT exercised (worker RPC, no token on a never-polled path): %v",
				h.cfgIdx, unexercised)
		}
	}

	h.checkCompleteness(t, verifiedFine, skippedFine)
}

// checkCompleteness is an informational (never-failing) coverage report: it compares what this run
// verified/skipped against the model's OWN reachable set, computed to fixpoint with no depth bound
// (server-free — it walks Model() alone). Cells the model can reach but this run did not are the
// ones the depth cap left out. It does NOT fail — under a depth cap that is expected — it just
// prints them, so raising SAASPEC_MAX_DEPTH and watching the list shrink is the way to see how much
// deeper the walk still has to go.
func (h *saaHarness) checkCompleteness(t *testing.T, verifiedFine, skippedFine map[string]bool) {
	// Off by default so the traversal's failures are just spec violations. Set SAASPEC_COMPLETENESS=1
	// to enable the type-(A) reachable-but-unexercised report.
	if os.Getenv("SAASPEC_COMPLETENESS") == "" {
		return
	}
	var gaps []string
	for key, kind := range saaModelReachable(h.cfg) {
		if verifiedFine[key] || skippedFine[key] {
			continue
		}
		if len(h.focus) > 0 && !h.focus[kind] {
			continue // focused run: only report gaps for the focused events
		}
		gaps = append(gaps, key)
	}
	if len(gaps) == 0 {
		return
	}
	sort.Strings(gaps)
	shown := gaps
	suffix := ""
	if len(shown) > 30 {
		shown, suffix = shown[:30], fmt.Sprintf("\n  … and %d more", len(gaps)-30)
	}
	t.Logf("cfg %d: %d model-reachable cell(s) not exercised at depth<=%d (raise SAASPEC_MAX_DEPTH to reach deeper).\n"+
		"  fingerprint = Status|count|scheduleToClose>0|resetKeepPaused|resetHeartbeats|resetRestoreOpts|firstStarted|dispatchSet|dispatch\n  %s%s",
		h.cfgIdx, len(gaps), saaMaxDepth(), strings.Join(shown, "\n  "), suffix)
}

// verifyPath starts a fresh activity, replays the path (asserting only the final edge), and
// aborts silently if a prefix edge diverges — that edge is reported when it is itself a final
// edge of its own shorter path.
func (h *saaHarness) verifyPath(t require.TestingT, path []saaspec.Event) (saaApply, bool) {
	a := h.start(t)
	a.path = path
	cur := saaspec.Initial(h.cfg)

	// The freshly started activity should match Initial(cfg).
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Errorf("cfg %d: state immediately after StartActivityExecution disagrees with Initial(cfg).\n%s",
			h.cfgIdx, saaStateDiff(obs, cur))
		return saaMismatch, false
	}

	for i, e := range path {
		out := saaspec.Model(h.cfg, cur, e)
		final := i == len(path)-1
		// On a focused run, drive+check the final edge but swallow its report unless it is a
		// focused event. Prefix edges always use the real t so reachability failures surface.
		et := t
		if final && len(h.focus) > 0 && !h.focus[e.Kind] {
			et = saaDiscardT{}
		}
		res := a.apply(et, e, cur, out, final)
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
	if saaIsWallClock(e.Kind) {
		return a.applyWallClock(t, e, cur, out, final)
	}
	// Worker RPCs authenticate with a task token, which the actor only holds after a poll. On a path
	// that never polled (e.g. a fresh SCHEDULED or PAUSED activity) there is no token; sending an
	// empty token yields a different error than the token-validation NotFound the spec means, so this
	// edge is not drivable here. The coverage ledger records the skip.
	if saaNeedsToken(e.Kind) && a.token == nil {
		return saaSkippedNoToken
	}
	err := a.rpc(e)
	ok := a.verify(t, e, cur, out, err, final)
	if e.Kind == saaspec.Heartbeat && out.Reject == saaspec.NoError {
		observed := saaspec.HeartbeatFlags{
			CancelRequested: a.lastHeartbeat.GetCancelRequested(),
			ActivityPaused:  a.lastHeartbeat.GetActivityPaused(),
			ActivityReset:   a.lastHeartbeat.GetActivityReset(),
		}
		expected := saaspec.ExpectedHeartbeatFlags(cur)
		if observed != expected {
			ok = false
			if final {
				t.Errorf("%s", a.flagsFailure(e, cur.Status, observed, expected))
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
func (a *saaActor) verify(t require.TestingT, e saaspec.Event, cur saaspec.AbstractState, out saaspec.Outcome, rpcErr error, final bool) bool {
	gotKind := saaRejectKind(rpcErr)
	obs, err := a.observed()
	require.NoError(t, err)
	ok := gotKind == out.Reject && out.Next.SameObserved(obs)
	if final {
		if gotKind != out.Reject {
			t.Errorf("%s", a.rejectFailure(e, cur.Status, gotKind, out.Reject, rpcErr))
		}
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(e, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
	}
	return ok
}

func (a *saaActor) applyPoll(cur saaspec.AbstractState, out saaspec.Outcome, final bool, t require.TestingT) saaApply {
	poll := saaspec.Event{Kind: saaspec.Poll}
	switch {
	case cur.Status == saaspec.Scheduled && out.Next.Status == saaspec.Started:
		// Positive: a SCHEDULED+Dispatchable activity must be dispatched to a worker. The dispatch-delay
		// traces shorten this deadline so "Dispatchable" means "dispatches promptly" — which is how it
		// distinguishes an immediate dispatch (e.g. reset discarding a backoff) from one still waiting
		// out a delay.
		timeout := 10 * time.Second
		if a.h.positivePollTimeout > 0 {
			timeout = a.h.positivePollTimeout
		}
		resp := a.pollForTask(t, timeout)
		if resp == nil {
			if final {
				t.Errorf("%s: model expected STARTED but no task was dispatched within %s (scheduled, never dispatched)\n%s",
					a.edge(poll, cur.Status), timeout, a.pathLine())
			}
			return saaMismatch
		}
		a.token = resp.GetTaskToken()
		if final && resp.GetAttempt() != out.Next.Count {
			t.Errorf("%s: dispatched task attempt number disagrees — server saw %d, model expected %d\n%s",
				a.edge(poll, cur.Status), resp.GetAttempt(), out.Next.Count, a.pathLine())
		}
	case cur.Status == saaspec.Scheduled && cur.Dispatchability != saaspec.Dispatchable:
		// Delayed dispatch: a start_delay or retry backoff is still pending, so the model says the
		// poll finds no task (it stays SCHEDULED). Verify with a negative poll — but only when the
		// pending delay is long enough to outlast a valid long poll; under the fast-backoff configs
		// it is not, so there we rely on the state comparison below alone.
		if dur := a.h.dispatchDelay(cur.Dispatchability); dur > saaNegativePollTimeout {
			if resp := a.pollForTask(t, saaNegativePollTimeout); resp != nil {
				if final {
					t.Errorf("%s: model expected no dispatch (%s pending) but a task WAS dispatched (attempt %d)\n%s",
						a.edge(poll, cur.Status), cur.Dispatchability, resp.GetAttempt(), a.pathLine())
				}
				return saaMismatch
			}
		}
	case cur.Status == saaspec.Paused && !saaSkipNegativePoll():
		// Negative safety check: a PAUSED activity must not be dispatchable. Pause bumped the attempt
		// stamp, invalidating the pending dispatch task, so matching must have nothing. This is the
		// only status where a spurious dispatch is possible (worker-token and terminal statuses cannot
		// dispatch), so it is the only place we pay the full long-poll wait. The timeout must exceed
		// MinLongPollTimeout or the frontend rejects the poll without ever consulting matching. It is
		// also the dominant cost of deep walks, so SAASPEC_NO_NEGATIVE_POLL disables it (the
		// ReadComponent state check below still confirms Paused/stamp/dispatch; only the matching-level
		// "no task" assertion is dropped).
		if resp := a.pollForTask(t, saaNegativePollTimeout); resp != nil {
			if final {
				t.Errorf("%s: model expected no advance but a task WAS dispatched\n%s",
					a.edge(poll, cur.Status), a.pathLine())
			}
			return saaMismatch
		}
	}
	// For all other statuses a dispatch is structurally impossible, so we skip the poll entirely and
	// rely on the ReadComponent state comparison below (which also catches the pause stamp bump).
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(poll, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
	}
	if out.Next.SameObserved(obs) {
		return saaVerified
	}
	return saaMismatch
}

// applyWallClock drives a wall-clock event — one of the four timeouts, or a dispatch-delay clock
// (StartDelayElapses / BackoffElapses) — by waiting long enough for that clock to elapse on the
// server, then asserting the observed state equals Model.Next. The single check covers all cases:
// a timeout that fires changes the status (TimedOut, or a retry); a stale timeout or a stale elapse
// changes nothing; and a live elapse changes only the latent Dispatchability (excluded from SameObserved),
// so the status is unchanged and it is a subsequent Poll that confirms the activity became
// dispatchable.
func (a *saaActor) applyWallClock(t require.TestingT, e saaspec.Event, cur saaspec.AbstractState, out saaspec.Outcome, final bool) saaApply {
	time.Sleep(a.h.eventClock(e, cur) + saaWallClockSettle)
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(e, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
	}
	if out.Next.SameObserved(obs) {
		return saaVerified
	}
	return saaMismatch
}

// eventClock is how long the clock behind a wall-clock event takes to elapse: a timeout under test is
// configured short (saaShortTimeout), and a dispatch delay lasts dispatchDelay.
func (h *saaHarness) eventClock(e saaspec.Event, cur saaspec.AbstractState) time.Duration {
	switch e.Kind {
	case saaspec.StartDelayElapses, saaspec.BackoffElapses:
		return h.dispatchDelay(cur.Dispatchability)
	default: // the four timeouts
		return saaShortTimeout
	}
}

// dispatchDelay is how long the harness configured the pending delay to last, so it knows how
// long to wait for the clock to fire and whether a negative poll can validly sit inside the window.
// For a backoff, a worker-supplied next_retry_delay overrides the policy interval, so it wins here
// too — that is what lets a trace prove the override is honored.
func (h *saaHarness) dispatchDelay(d saaspec.Dispatchability) time.Duration {
	switch d {
	case saaspec.StartDelayPending:
		return h.startDelay
	case saaspec.BackoffPending:
		if h.nextRetryDelay > 0 {
			return h.nextRetryDelay
		}
		return h.retryInterval
	default:
		return 0
	}
}

// driveTrace runs one trace on a single fresh activity, asserting the observed state and pollability
// against Model() at every step. Both the timeout traces and the dispatch-delay traces use it: every
// event — RPC, poll, timeout firing, or dispatch-delay clock — is driven through apply and checked
// against Model. (The traversal, by contrast, replays each path from a fresh activity so it can walk
// the graph exhaustively; a trace pays each real wall-clock wait once.)
func (h *saaHarness) driveTrace(t *testing.T, trace []saaspec.Event) {
	a := h.start(t)
	a.path = trace
	cur := saaspec.Initial(h.cfg)

	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("after Start, state disagrees with Initial(cfg).\n%s", saaStateDiff(obs, cur))
	}

	for _, e := range trace {
		out := saaspec.Model(h.cfg, cur, e)
		a.apply(t, e, cur, out, true)
		cur = out.Next
	}
}

// checkDescribe asserts the public status and run state DescribeActivityExecution reports match
// ExpectedDescribe.
func (a *saaActor) checkDescribe(t require.TestingT, expected saaspec.AbstractState) {
	st, rs := saaspec.ExpectedDescribe(expected)
	resp, err := a.h.env.FrontendClient().DescribeActivityExecution(a.h.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace: a.h.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
	})
	require.NoError(t, err)
	gotSt, gotRs := resp.GetInfo().GetStatus(), resp.GetInfo().GetRunState()
	if gotSt != st || gotRs != rs {
		t.Errorf("Describe while in internal status %s does not match model expectation\n%s\n"+
			"  server saw:     status=%v run=%v\n"+
			"  model expected: status=%v run=%v",
			expected.Status, a.pathLine(), gotSt, gotRs, st, rs)
	}
}

// rpc performs the RPC for a non-Poll event and returns its error.
func (a *saaActor) rpc(e saaspec.Event) error {
	fc := a.h.env.FrontendClient()
	ns := a.h.env.Namespace().String()
	switch e.Kind {
	case saaspec.Heartbeat:
		resp, err := fc.RecordActivityTaskHeartbeat(a.h.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns,
			TaskToken: a.token,
		})
		a.lastHeartbeat = resp
		return err
	case saaspec.RespondCompleted:
		_, err := fc.RespondActivityTaskCompleted(a.h.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case saaspec.RespondFailed:
		_, err := fc.RespondActivityTaskFailed(a.h.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: saaFailure(e.Retryable, a.h.nextRetryDelay),
		})
		return err
	case saaspec.RespondCanceled:
		_, err := fc.RespondActivityTaskCanceled(a.h.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case saaspec.RequestCancel:
		_, err := fc.RequestCancelActivityExecution(a.h.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "traverse", RequestId: a.reqID(e),
		})
		return err
	case saaspec.Terminate:
		_, err := fc.TerminateActivityExecution(a.h.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "traverse", RequestId: a.reqID(e),
		})
		return err
	case saaspec.Pause:
		_, err := fc.PauseActivityExecution(a.h.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "traverse", RequestId: a.reqID(e),
		})
		return err
	case saaspec.Unpause:
		_, err := fc.UnpauseActivityExecution(a.h.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case saaspec.Reset:
		_, err := fc.ResetActivityExecution(a.h.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case saaspec.UpdateOptions:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaHarness: unhandled event kind %v", e.Kind)
	}
}

func (a *saaActor) updateOptions(e saaspec.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.h.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: "op",
	}
	if e.RestoreOriginal {
		req.RestoreOriginal = true
	} else {
		// A minimal, always-valid update: re-set the heartbeat timeout. (RespondFailed-style
		// retryability and richer option merges are refined when the model decides these cells.)
		req.ActivityOptions = &apiactivitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.h.env.FrontendClient().UpdateActivityExecutionOptions(a.h.ctx, req)
	return err
}

// --- actor: one activity instance ----------------------------------------------------------

type saaActor struct {
	h             *saaHarness
	activityID    string
	taskQueue     string
	runID         string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	reqIDs        map[saaspec.EventKind]string
	path          []saaspec.Event // events replayed to reach the edge under test, for failure reports
}

func (h *saaHarness) start(t require.TestingT) *saaActor {
	h.counter++
	id := fmt.Sprintf("saaexp-%d-%d", h.cfgIdx, h.counter)
	resp, err := h.env.FrontendClient().StartActivityExecution(h.ctx, h.startRequest(id, id))
	require.NoError(t, err)
	return &saaActor{h: h, activityID: id, taskQueue: id, runID: resp.RunId, reqIDs: map[saaspec.EventKind]string{}}
}

const saaShortTimeout = 2 * time.Second

// saaWallClockSettle is slack added to a wall-clock event's clock when waiting for it to fire, so
// the wait comfortably outlasts the firing instant (a timeout deadline, or a dispatch-delay instant
// like schedule_time + start_delay or complete_time + backoff).
const saaWallClockSettle = 2 * time.Second

// saaIsWallClock reports whether an event fires on wall-clock time — the four timeouts and the two
// dispatch-delay clocks — rather than synchronously like an RPC. apply drives these by waiting.
func saaIsWallClock(k saaspec.EventKind) bool {
	switch k {
	case saaspec.ScheduleToStartFires, saaspec.ScheduleToCloseFires, saaspec.StartToCloseFires,
		saaspec.HeartbeatFires, saaspec.StartDelayElapses, saaspec.BackoffElapses:
		return true
	default:
		return false
	}
}

func (h *saaHarness) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	long := durationpb.New(time.Hour)
	// dur returns the short timeout for the one timeout under test, long otherwise.
	dur := func(k saaspec.EventKind) *durationpb.Duration {
		if h.shortTimeout == k {
			return durationpb.New(saaShortTimeout)
		}
		return long
	}
	// Retries dispatch after this interval. The default is short so the graph traversal can drive retry
	// loops quickly; the dispatch-delay traces lengthen it to observe the backoff.
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
		StartToCloseTimeout: dur(saaspec.StartToCloseFires),
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
		req.ScheduleToCloseTimeout = dur(saaspec.ScheduleToCloseFires)
	}
	if h.cfg.HasScheduleToStart {
		req.ScheduleToStartTimeout = dur(saaspec.ScheduleToStartFires)
	}
	if h.cfg.HasHeartbeat {
		req.HeartbeatTimeout = dur(saaspec.HeartbeatFires)
	}
	return req
}

func (a *saaActor) observed() (saaspec.AbstractState, error) {
	o, err := saaReadObserved(a.h.chasmCtx, a.h.nsID, a.activityID, a.runID)
	if err != nil {
		return saaspec.AbstractState{}, err
	}
	return saaspec.Abstract(o), nil
}

// saaNegativePollTimeout is the deadline for the "must not dispatch" poll. It must exceed
// common.MinLongPollTimeout, or the frontend rejects the poll before consulting matching (making the
// check vacuous). A genuine empty long poll blocks for roughly this long, so we only pay it where a
// spurious dispatch is actually possible (see applyPoll).
const saaNegativePollTimeout = common.MinLongPollTimeout + time.Second

func (a *saaActor) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.h.ctx, timeout)
	defer cancel()
	resp, err := a.h.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.h.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	if err != nil {
		// Matching signals "waited, found nothing" with an empty response and a nil error (see
		// matching_engine.go: errNoTasks -> emptyPoll...Response, returned just before the deadline via
		// returnEmptyTaskTimeBudget). So a genuine no-task result never surfaces as an error. Any error
		// here means the poll did not complete cleanly — the server rejected it (e.g.
		// ErrContextTimeoutTooShort when the deadline is below MinLongPollTimeout=2s) or our deadline
		// fired before matching returned its verdict. Treating that as "no task" is the vacuity trap
		// (a paused activity could dispatch and we would never notice), so fail loudly. The one benign
		// case is teardown: if the traversal's parent context is done, the poll errors through no fault
		// of the server.
		if a.h.ctx.Err() != nil {
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

func saaFingerprint(s saaspec.AbstractState) string {
	count := min(s.Count, 3)
	return fmt.Sprintf("%v|%d|%v|%v|%v|%v|%v|%v|%v",
		s.Status, count, s.ScheduleToCloseStamp > 0, s.ResetKeepPaused, s.ResetHeartbeats,
		s.ResetRestoreOptions, s.FirstAttemptStarted, s.DispatchTimeSet, s.Dispatchability)
}

// saaCellKey identifies a (state, event kind) cell at fingerprint granularity — the unit the
// completeness check and the coverage ledger reason about.
func saaCellKey(s saaspec.AbstractState, kind saaspec.EventKind) string {
	return saaFingerprint(s) + " / " + saaKindName(kind)
}

// saaModelReachable computes, purely from Model() (no server), every (state, event) cell the model
// can reach from Initial(cfg) by following non-reject edges to fixpoint. States are deduplicated by
// fingerprint, so the walk is finite and terminates. This is the reference set the traversal's
// verified/skipped cells are checked against, independent of any depth bound.
func saaModelReachable(cfg saaspec.Config) map[string]saaspec.EventKind {
	cells := map[string]saaspec.EventKind{}
	start := saaspec.Initial(cfg)
	visited := map[string]bool{saaFingerprint(start): true}
	frontier := []saaspec.AbstractState{start}
	for len(frontier) > 0 {
		var next []saaspec.AbstractState
		for _, s := range frontier {
			for _, e := range saaCandidateEvents() {
				out := saaspec.Model(cfg, s, e)
				cells[saaCellKey(s, e.Kind)] = e.Kind
				if out.Reject != saaspec.NoError {
					continue // no state change; nothing new to reach
				}
				fp := saaFingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					next = append(next, out.Next)
				}
			}
		}
		frontier = next
	}
	return cells
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
	// ResetHeartbeat is orthogonal to KeepPaused/RestoreOriginal (it only governs whether the
	// deferred reset clears heartbeat state), so exercise it as its own axis rather than crossing
	// it with the others and tripling the graph.
	out = append(out, saaspec.Event{Kind: saaspec.Reset, ResetHeartbeat: true})
	for _, ra := range []bool{false, true} {
		out = append(out, saaspec.Event{Kind: saaspec.Unpause, ResetAttempts: ra})
	}
	return out
}
