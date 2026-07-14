package tests

// Harness engine and helpers for the standalone-activity spec. The engine (saaHarness / saaActor and
// their methods) replays or drives one event against a real onebox server and checks the result
// against saaspec.Model(); the graph traversal, the random-walk explorer, and the trace driver live
// here, along with the tuning knobs. Below the "helpers" divider are the pure utilities: state
// reading, fingerprints, event enumeration, error classification, and failure formatting. The
// editable configs, traces, and test entry points are in activity_standalone_spec_test.go.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- tuning knobs --------------------------------------------------------------------------

// saaMaxDepth is the BFS depth cap. The default keeps CI fast; TEMPORAL_SAASPEC_MAX_DEPTH raises it
// for deeper local verification. Cost grows with depth — mostly the per-Paused negative poll, which
// TEMPORAL_SAASPEC_NO_NEGATIVE_POLL can disable (see applyPoll).
func saaMaxDepth() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_MAX_DEPTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 4
}

// saaSkipNegativePoll disables the ~3s "a Paused activity must not dispatch" long-poll — the
// dominant cost of deep walks. Set TEMPORAL_SAASPEC_NO_NEGATIVE_POLL for fast deep runs; the
// per-edge state check still verifies the Paused transition, only the matching-level assertion is
// dropped.
func saaSkipNegativePoll() bool { return os.Getenv("TEMPORAL_SAASPEC_NO_NEGATIVE_POLL") != "" }

func saaWalkSteps() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_STEPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 200 // a bare `go test` (90s per-test context) smoke; raise it with TEMPORAL_TEST_TIMEOUT for real exploration
}

func saaWalkSeed() int64 {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 1 // deterministic default; override for a different walk
}

func saaVerbose() bool { return os.Getenv("TEMPORAL_SAASPEC_VERBOSE") != "" }

// --- harness / engine ----------------------------------------------------------------------

type saaHarness struct {
	env      *standaloneActivityEnv
	ctx      context.Context
	chasmCtx context.Context
	nsID     string
	cfg      saaspec.Config
	cfgIdx   int
	counter  int
	// shortTimeout, when set to one of the four timeout *Elapses kinds, makes that timeout short at
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

const saaShortTimeout = 2 * time.Second

// saaWallClockSettle is slack added to a wall-clock event's clock when waiting for it to fire, so
// the wait comfortably outlasts the firing instant (a timeout deadline, or a dispatch-delay instant
// like schedule_time + start_delay or complete_time + backoff).
const saaWallClockSettle = 2 * time.Second

// saaNegativePollTimeout is the deadline for the "must not dispatch" poll. It must exceed
// common.MinLongPollTimeout, or the frontend rejects the poll before consulting matching (making the
// check vacuous). A genuine empty long poll blocks for roughly this long, so we only pay it where a
// spurious dispatch is actually possible (see applyPoll).
const saaNegativePollTimeout = common.MinLongPollTimeout + time.Second

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

	// Coverage detail (the no-token-skip ledger) prints only under TEMPORAL_SAASPEC_COMPLETENESS, so
	// the default output is just the spec violations.
	if os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") != "" {
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
// verified/skipped against the model's own reachable set (computed server-free to fixpoint, no depth
// bound). Cells the model can reach but this run did not are what the depth cap left out.
func (h *saaHarness) checkCompleteness(t *testing.T, verifiedFine, skippedFine map[string]bool) {
	if os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") == "" {
		return
	}
	var gaps []string
	for key := range saaModelReachable(h.cfg) {
		if verifiedFine[key] || skippedFine[key] {
			continue
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
	t.Logf("cfg %d: %d model-reachable cell(s) not exercised at depth<=%d (raise TEMPORAL_SAASPEC_MAX_DEPTH to reach deeper).\n"+
		"  fingerprint = Status|count|resetKeepPaused|resetHeartbeats|resetRestoreOpts|firstStarted|dispatchSet|dispatch\n  %s%s",
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
	// Worker RPCs need a task token, held only after a poll. On a never-polled path there is no token,
	// and an empty token yields a different error than the spec's NotFound, so the edge is not drivable
	// here; the ledger records the skip.
	if saaNeedsToken(e.Kind) && a.token == nil {
		return saaSkippedNoToken
	}
	err := a.rpc(e)
	if saaCarriesReqID(e.Kind) && out.Reject == saaspec.NoError && out.Next.Status != cur.Status {
		// This request established a new state, so its id is the one a later SameRequestID must reuse
		// for the server's request-id idempotency; an intervening rejected/no-op request must not
		// overwrite it.
		a.establishedReqID[e.Kind] = a.lastReqID
	}
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
		a.checkTaskInvalidation(t, e, cur, out)
	}
	return ok
}

// checkTaskInvalidation compares whether each raw stamp changed across the edge under test against the
// model's per-transition invalidation bools. observed() has already refreshed cur/prev for this edge.
func (a *saaActor) checkTaskInvalidation(t require.TestingT, e saaspec.Event, cur saaspec.AbstractState, out saaspec.Outcome) {
	gotAttempt := a.curStamp != a.prevStamp
	gotSTC := a.curSTCStamp != a.prevSTCStamp
	if gotAttempt != out.AttemptTasksInvalidated {
		t.Errorf("%s: attempt-task invalidation disagrees — server %v, model %v\n%s",
			a.edge(e, cur.Status), gotAttempt, out.AttemptTasksInvalidated, a.pathLine())
	}
	if gotSTC != out.ScheduleToCloseTaskInvalidated {
		t.Errorf("%s: schedule-to-close-task invalidation disagrees — server %v, model %v\n%s",
			a.edge(e, cur.Status), gotSTC, out.ScheduleToCloseTaskInvalidated, a.pathLine())
	}
}

func (a *saaActor) applyPoll(cur saaspec.AbstractState, out saaspec.Outcome, final bool, t require.TestingT) saaApply {
	poll := saaspec.Event{Kind: saaspec.Poll}
	switch {
	case cur.Status == saaspec.Scheduled && out.Next.Status == saaspec.Started:
		// Positive: a SCHEDULED+Dispatchable activity must be dispatched. The dispatch-delay traces
		// shorten this deadline so "Dispatchable" means "dispatches promptly".
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
		// Delayed dispatch: a start_delay or backoff is still pending, so the poll finds no task. Verify
		// with a negative poll, but only when the delay outlasts a valid long poll (not under the
		// fast-backoff configs, where the state comparison below suffices).
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
		// A PAUSED activity must not be dispatchable (Pause invalidated the pending dispatch task).
		// The only status where a spurious dispatch is possible, so the only place we pay the
		// full long-poll wait; the timeout must exceed MinLongPollTimeout. TEMPORAL_SAASPEC_NO_NEGATIVE_POLL
		// disables it — the state check below still confirms Paused/dispatch.
		if resp := a.pollForTask(t, saaNegativePollTimeout); resp != nil {
			if final {
				t.Errorf("%s: model expected no advance but a task WAS dispatched\n%s",
					a.edge(poll, cur.Status), a.pathLine())
			}
			return saaMismatch
		}
	}
	// Other statuses cannot dispatch, so skip the poll and rely on the state comparison below.
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(poll, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
		a.checkTaskInvalidation(t, poll, cur, out)
	}
	if out.Next.SameObserved(obs) {
		return saaVerified
	}
	return saaMismatch
}

// applyWallClock drives a wall-clock event (a timeout or a dispatch-delay clock) by waiting for it to
// elapse on the server, then asserting the observed state equals Model.Next. A firing timeout changes
// status; a stale timeout/elapse changes nothing; a live elapse changes only the latent Dispatchability
// (excluded from SameObserved), which a later Poll confirms.
func (a *saaActor) applyWallClock(t require.TestingT, e saaspec.Event, cur saaspec.AbstractState, out saaspec.Outcome, final bool) saaApply {
	time.Sleep(a.h.eventClock(e, cur) + saaWallClockSettle)
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(e, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
		a.checkTaskInvalidation(t, e, cur, out)
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

// dispatchDelay is how long the harness configured the pending delay to last. For a backoff, a
// worker-supplied next_retry_delay overrides the policy interval — that is what lets a trace prove
// the override is honored.
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
	if gotAttempt := resp.GetInfo().GetAttempt(); gotAttempt != expected.Count {
		t.Errorf("Describe attempt while in internal status %s does not match model expectation\n%s\n"+
			"  server saw:     attempt=%d\n"+
			"  model expected: attempt=%d",
			expected.Status, a.pathLine(), gotAttempt, expected.Count)
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
	switch {
	case e.RestoreOriginal:
		req.RestoreOriginal = true
	case e.SetsStartDelay:
		req.ActivityOptions = &apiactivitypb.ActivityOptions{StartDelay: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}}
	default:
		// A minimal, always-valid update: re-set the heartbeat timeout. (RespondFailed-style
		// retryability and richer option merges are refined when the model decides these cells.)
		req.ActivityOptions = &apiactivitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.h.env.FrontendClient().UpdateActivityExecutionOptions(a.h.ctx, req)
	return err
}

// saaActor is one activity instance.
type saaActor struct {
	h             *saaHarness
	activityID    string
	taskQueue     string
	runID         string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	// establishedReqID[kind] is the request id that established the current idempotent state for an
	// operator command (RequestCancel/Terminate/Pause); a SameRequestID event reuses it so the server's
	// request-id idempotency check sees the establishing id, not merely the last id used for that
	// operation. lastReqID is the id used by the most recent operator RPC, promoted into
	// establishedReqID when that RPC changes state.
	establishedReqID map[saaspec.EventKind]string
	lastReqID        string
	path             []saaspec.Event // events replayed to reach the edge under test, for failure reports

	// Raw stamps read across the edge under test. observed() shifts cur->prev on each read, so after
	// driving edge N, cur is the post-N value and prev the post-(N-1) value: their inequality is the
	// stamp bump across edge N, compared to the model's Outcome bools (see checkTaskInvalidation).
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
}

func (h *saaHarness) start(t require.TestingT) *saaActor {
	// cfg.HasStartDelay tells the model to predict StartDelayPending; the server only enters that
	// state if a real start_delay is configured. Guard against the decoupling so a misconfigured
	// config fails loudly rather than as a confusing first-state mismatch.
	if h.cfg.HasStartDelay && h.startDelay <= 0 {
		require.Fail(t, "saaHarness misconfigured: cfg.HasStartDelay requires startDelay > 0")
	}
	h.counter++
	id := fmt.Sprintf("saaexp-%d-%d", h.cfgIdx, h.counter)
	resp, err := h.env.FrontendClient().StartActivityExecution(h.ctx, h.startRequest(id, id))
	require.NoError(t, err)
	return &saaActor{h: h, activityID: id, taskQueue: id, runID: resp.RunId, establishedReqID: map[saaspec.EventKind]string{}}
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
		StartToCloseTimeout: dur(saaspec.StartToCloseElapses),
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
		req.ScheduleToCloseTimeout = dur(saaspec.ScheduleToCloseElapses)
	}
	if h.cfg.HasScheduleToStart {
		req.ScheduleToStartTimeout = dur(saaspec.ScheduleToStartElapses)
	}
	if h.cfg.HasHeartbeat {
		req.HeartbeatTimeout = dur(saaspec.HeartbeatElapses)
	}
	return req
}

func (a *saaActor) observed() (saaspec.AbstractState, error) {
	o, err := saaReadObserved(a.h.chasmCtx, a.h.nsID, a.activityID, a.runID)
	if err != nil {
		return saaspec.AbstractState{}, err
	}
	a.prevStamp, a.curStamp = a.curStamp, o.Stamp
	a.prevSTCStamp, a.curSTCStamp = a.curSTCStamp, o.ScheduleToCloseStamp
	return saaspec.Abstract(o), nil
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
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`, or lower TEMPORAL_SAASPEC_MAX_DEPTH / TEMPORAL_SAASPEC_WALK_STEPS.\n  %v",
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

func (a *saaActor) reqID(e saaspec.Event) string {
	id := uuid.NewString()
	if e.SameRequestID {
		if est, ok := a.establishedReqID[e.Kind]; ok {
			id = est // reuse the id that established the current state, not merely the last one used
		}
	}
	a.lastReqID = id
	return id
}

// randomWalk drives one activity, picking a random applicable event each step and checking it against
// Model(); on reaching a terminal state (or after a divergence) it starts a fresh activity and keeps
// going until the step budget is spent. It reports the distinct states (by fingerprint) it covered.
func (h *saaHarness) randomWalk(t *testing.T, rng *rand.Rand, maxSteps int) {
	verbose := saaVerbose()
	seen := map[string]bool{}
	walks := 0

	a, cur := h.walkStart(t)
	var trace []saaspec.Event // events driven since the last (re)start, so a divergence prints its path
	seen[saaFingerprint(cur)] = true
	walks++
	if verbose {
		t.Logf("cfg %d walk %d: start %s", h.cfgIdx, walks, cur.Status)
	}

	for step := 0; step < maxSteps; step++ {
		if cur.Status.Terminal() {
			a, cur = h.walkStart(t)
			trace = nil
			seen[saaFingerprint(cur)] = true
			walks++
			if verbose {
				t.Logf("cfg %d walk %d: start %s (restart after terminal)", h.cfgIdx, walks, cur.Status)
			}
			continue
		}
		e := h.pickWalkEvent(rng, a, cur)
		trace = append(trace, e)
		a.path = trace
		out := saaspec.Model(h.cfg, cur, e)
		res := a.apply(t, e, cur, out, true)
		if verbose {
			t.Logf("cfg %d walk %d step %d: %s", h.cfgIdx, walks, step, saaStepDesc(cur, e, out, res))
		}
		switch res {
		case saaVerified:
			cur = out.Next
			seen[saaFingerprint(cur)] = true
		case saaSkippedNoToken:
			trace = trace[:len(trace)-1] // event not driven; drop it from the segment trace
		case saaMismatch:
			// apply() already reported the divergence (with a.path = this trace); restart from a known
			// state so the walk keeps exploring rather than compounding from a suspect one.
			a, cur = h.walkStart(t)
			trace = nil
			walks++
		}
	}
	t.Logf("cfg %d: random walk done — %d steps, %d walks, %d distinct states covered",
		h.cfgIdx, maxSteps, walks, len(seen))
}

// walkStart begins a fresh activity and asserts it matches Initial(cfg).
func (h *saaHarness) walkStart(t *testing.T) (*saaActor, saaspec.AbstractState) {
	a := h.start(t)
	cur := saaspec.Initial(h.cfg)
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("cfg %d: fresh activity disagrees with Initial(cfg)\n%s", h.cfgIdx, saaStateDiff(obs, cur))
	}
	return a, cur
}

// pickWalkEvent chooses the next event. It strongly prefers non-terminal progress so the walk
// wanders deep instead of restarting every few steps (terminal-reaching events like Terminate /
// RespondCompleted end a walk immediately); it still occasionally takes any changing edge (maybe
// terminal) or a reject/no-op so those are exercised in deep contexts too. Terminal edges are
// covered exhaustively by the BFS; here depth is the goal. Events needing a task token we do not
// hold are skipped (they would be un-drivable no-ops).
func (h *saaHarness) pickWalkEvent(rng *rand.Rand, a *saaActor, cur saaspec.AbstractState) saaspec.Event {
	var applicable, changing, deep []saaspec.Event
	for _, e := range saaCandidateEvents() {
		if saaNeedsToken(e.Kind) && a.token == nil {
			continue
		}
		applicable = append(applicable, e)
		if out := saaspec.Model(h.cfg, cur, e); out.Reject == saaspec.NoError && !out.Next.SameObserved(cur) {
			changing = append(changing, e)
			if !out.Next.Status.Terminal() {
				deep = append(deep, e)
			}
		}
	}
	switch {
	case len(deep) > 0 && rng.Float64() < 0.85:
		return deep[rng.Intn(len(deep))]
	case len(changing) > 0 && rng.Float64() < 0.5:
		return changing[rng.Intn(len(changing))]
	case len(applicable) > 0:
		return applicable[rng.Intn(len(applicable))]
	default:
		return saaspec.Event{Kind: saaspec.Poll} // always applicable (needs no token)
	}
}

// --- helpers -------------------------------------------------------------------------------

// saaStepDesc renders one walk step as "FromStatus --Event--> ToStatus", annotated with the reject
// kind or a no-token skip.
func saaStepDesc(cur saaspec.AbstractState, e saaspec.Event, out saaspec.Outcome, res saaApply) string {
	desc := fmt.Sprintf("%s --%s--> %s", cur.Status, saaEventLabel(e), out.Next.Status)
	switch {
	case res == saaSkippedNoToken:
		desc += "  [skipped: no token]"
	case out.Reject != saaspec.NoError:
		desc += "  [" + saaRejectKindName(out.Reject) + "]"
	}
	return desc
}

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

// saaIsWallClock reports whether an event fires on wall-clock time — the four timeouts and the two
// dispatch-delay clocks — rather than synchronously like an RPC. apply drives these by waiting.
func saaIsWallClock(k saaspec.EventKind) bool {
	switch k {
	case saaspec.ScheduleToStartElapses, saaspec.ScheduleToCloseElapses, saaspec.StartToCloseElapses,
		saaspec.HeartbeatElapses, saaspec.StartDelayElapses, saaspec.BackoffElapses:
		return true
	default:
		return false
	}
}

// saaCarriesReqID reports whether an operator command carries a request id whose server-side
// idempotency is keyed on it (RequestCancel/Terminate/Pause).
func saaCarriesReqID(k saaspec.EventKind) bool {
	switch k {
	case saaspec.RequestCancel, saaspec.Terminate, saaspec.Pause:
		return true
	default:
		return false
	}
}

func saaFingerprint(s saaspec.AbstractState) string {
	count := min(s.Count, 3)
	return fmt.Sprintf("%v|%d|%v|%v|%v|%v|%v|%v",
		s.Status, count, s.ResetKeepPaused, s.ResetHeartbeats,
		s.ResetRestoreOptions, s.FirstAttemptStarted, s.DispatchTimeSet, s.Dispatchability)
}

// saaCellKey identifies a (state, event kind) cell at fingerprint granularity — the unit the
// completeness check and the coverage ledger reason about.
func saaCellKey(s saaspec.AbstractState, kind saaspec.EventKind) string {
	return saaFingerprint(s) + " / " + saaKindName(kind)
}

// saaModelReachable computes, purely from Model() (no server), every (state, event) cell reachable
// from Initial(cfg) by following non-reject edges to fixpoint (states deduped by fingerprint). This
// is the reference set the traversal's coverage is checked against, independent of any depth bound.
func saaModelReachable(cfg saaspec.Config) map[string]bool {
	cells := map[string]bool{}
	start := saaspec.Initial(cfg)
	visited := map[string]bool{saaFingerprint(start): true}
	frontier := []saaspec.AbstractState{start}
	for len(frontier) > 0 {
		var next []saaspec.AbstractState
		for _, s := range frontier {
			for _, e := range saaCandidateEvents() {
				out := saaspec.Model(cfg, s, e)
				cells[saaCellKey(s, e.Kind)] = true
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
	// An update that changes start_delay: the model rejects it outside the StartDelayPending window
	// (the only window it is mutable in), which is every state the RPC-only traversal reaches.
	out = append(out, saaspec.Event{Kind: saaspec.UpdateOptions, SetsStartDelay: true})
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

// --- error / outcome classification --------------------------------------------------------

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

func saaFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "traverse", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "traverse",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}

// --- failure reporting ---------------------------------------------------------------------
//
// A failure means the real server ("observed") disagreed with saaspec.Model ("expected") after one
// event. Each report opens with a one-line summary of what diverged, then the path to the edge, then
// a field-aligned diff.

// edge names the event and the status it was driven from, e.g. "RespondFailed[retryable=true] from
// Started".
func (a *saaActor) edge(e saaspec.Event, src saaspec.Status) string {
	return fmt.Sprintf("%s from %s", saaEventLabel(e), src)
}

func (a *saaActor) pathLine() string {
	return "  path: " + saaPathString(a.path)
}

// stateFailure reports that the persisted state after an event disagreed with the model.
func (a *saaActor) stateFailure(e saaspec.Event, src saaspec.Status, observed, expected saaspec.AbstractState) string {
	var summary string
	if observed.Status != expected.Status {
		summary = fmt.Sprintf("model expected %s, server saw %s", expected.Status, observed.Status)
	} else {
		summary = fmt.Sprintf("status %s agrees but persisted state differs", observed.Status)
	}
	return fmt.Sprintf("%s: %s\n%s\n%s", a.edge(e, src), summary, a.pathLine(), saaStateDiff(observed, expected))
}

// rejectFailure reports that the RPC's accept/reject outcome disagreed with the model.
func (a *saaActor) rejectFailure(e saaspec.Event, src saaspec.Status, got, want saaspec.ErrorKind, err error) string {
	msg := fmt.Sprintf("%s: server %s, model expected %s\n%s",
		a.edge(e, src), saaOutcomeDesc(got), saaOutcomeDesc(want), a.pathLine())
	if err != nil {
		msg += fmt.Sprintf("\n  server error: %v", err)
	}
	return msg
}

// flagsFailure reports that the worker-facing heartbeat response flags disagreed with the model.
func (a *saaActor) flagsFailure(e saaspec.Event, src saaspec.Status, observed, expected saaspec.HeartbeatFlags) string {
	rows, agree := saaFlagRows(observed, expected)
	return fmt.Sprintf("%s: heartbeat response flags disagree\n%s\n%s",
		a.edge(e, src), a.pathLine(), saaDiffBlock(rows, agree))
}

// saaPathString renders the event sequence that reached an edge, e.g.
// "Schedule → Poll → RespondFailed[retryable=false]". The origin is labeled Schedule (the status the
// StartActivityExecution RPC lands in), to avoid confusion with Started.
func saaPathString(path []saaspec.Event) string {
	parts := make([]string, 0, len(path)+1)
	parts = append(parts, "Schedule")
	for _, e := range path {
		parts = append(parts, saaEventLabel(e))
	}
	return strings.Join(parts, " → ")
}

// saaEventLabel names an event and appends the flags that affect its outcome.
func saaEventLabel(e saaspec.Event) string {
	var flags []string
	add := func(cond bool, name string) {
		if cond {
			flags = append(flags, name)
		}
	}
	switch e.Kind {
	case saaspec.RespondFailed:
		flags = append(flags, fmt.Sprintf("retryable=%v", e.Retryable))
	case saaspec.Reset:
		add(e.KeepPaused, "keepPaused")
		add(e.RestoreOriginal, "restoreOriginal")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case saaspec.Unpause:
		add(e.ResetAttempts, "resetAttempts")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case saaspec.Pause, saaspec.Terminate, saaspec.RequestCancel:
		add(e.SameRequestID, "sameRequestID")
	case saaspec.UpdateOptions:
		add(e.SetsStartDelay, "setsStartDelay")
		add(e.RestoreOriginal, "restoreOriginal")
	}
	if len(flags) == 0 {
		return saaKindName(e.Kind)
	}
	return fmt.Sprintf("%s[%s]", saaKindName(e.Kind), strings.Join(flags, ","))
}

// saaStateDiff renders the AbstractState fields that differ in aligned columns, with the agreeing
// fields listed as field=value beneath.
func saaStateDiff(observed, expected saaspec.AbstractState) string {
	b2s := func(b bool) string { return fmt.Sprint(b) }
	fields := [][3]string{
		{"Status", observed.Status.String(), expected.Status.String()},
		{"Count", fmt.Sprint(observed.Count), fmt.Sprint(expected.Count)},
		{"ResetKeepPaused", b2s(observed.ResetKeepPaused), b2s(expected.ResetKeepPaused)},
		{"ResetHeartbeats", b2s(observed.ResetHeartbeats), b2s(expected.ResetHeartbeats)},
		{"ResetRestoreOptions", b2s(observed.ResetRestoreOptions), b2s(expected.ResetRestoreOptions)},
		{"FirstAttemptStarted", b2s(observed.FirstAttemptStarted), b2s(expected.FirstAttemptStarted)},
		{"DispatchTimeSet", b2s(observed.DispatchTimeSet), b2s(expected.DispatchTimeSet)},
	}
	return saaDiffBlock(saaSplit(fields))
}

func saaFlagRows(observed, expected saaspec.HeartbeatFlags) (rows [][3]string, agree []string) {
	fields := [][3]string{
		{"CancelRequested", fmt.Sprint(observed.CancelRequested), fmt.Sprint(expected.CancelRequested)},
		{"ActivityPaused", fmt.Sprint(observed.ActivityPaused), fmt.Sprint(expected.ActivityPaused)},
		{"ActivityReset", fmt.Sprint(observed.ActivityReset), fmt.Sprint(expected.ActivityReset)},
	}
	return saaSplit(fields)
}

// saaSplit partitions (field, observed, expected) triples into the ones that differ (rows) and the
// ones that agree, the latter rendered as "field=value" so the report shows every field's value.
func saaSplit(fields [][3]string) (rows [][3]string, agree []string) {
	for _, f := range fields {
		if f[1] != f[2] {
			rows = append(rows, f)
		} else {
			agree = append(agree, f[0]+"="+f[1])
		}
	}
	return rows, agree
}

// saaDiffBlock formats differing (field, observed, expected) rows as an aligned three-column table
// and lists the agreeing "field=value" pairs beneath it.
func saaDiffBlock(rows [][3]string, agree []string) string {
	const hName, hObs, hExp = "field", "observed (server)", "expected (model)"
	nameW, obsW := len(hName), len(hObs)
	for _, r := range rows {
		nameW = max(nameW, len(r[0]))
		obsW = max(obsW, len(r[1]))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", nameW, hName, obsW, hObs, hExp)
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", nameW, r[0], obsW, r[1], r[2])
	}
	if len(agree) > 0 {
		fmt.Fprintf(&b, "  agree: %s", strings.Join(agree, " "))
	}
	return b.String()
}

func saaOutcomeDesc(k saaspec.ErrorKind) string {
	if k == saaspec.NoError {
		return "accepted"
	}
	return "rejected with " + saaRejectKindName(k)
}

func saaRejectKindName(k saaspec.ErrorKind) string {
	switch k {
	case saaspec.NoError:
		return "NoError"
	case saaspec.FailedPrecondition:
		return "FailedPrecondition"
	case saaspec.NotFound:
		return "NotFound"
	case saaspec.InvalidArgument:
		return "InvalidArgument"
	default:
		return fmt.Sprintf("unrecognized(%d)", int(k))
	}
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
	case saaspec.ScheduleToStartElapses:
		return "ScheduleToStartElapses"
	case saaspec.ScheduleToCloseElapses:
		return "ScheduleToCloseElapses"
	case saaspec.StartToCloseElapses:
		return "StartToCloseElapses"
	case saaspec.HeartbeatElapses:
		return "HeartbeatElapses"
	case saaspec.StartDelayElapses:
		return "StartDelayElapses"
	case saaspec.BackoffElapses:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventKind(%d)", k)
	}
}
