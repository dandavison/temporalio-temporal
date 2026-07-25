package tests

// Model-conformance engine for the standalone-activity surface: it drives each event with the driver in
// activity_standalone_driver.go and checks the result against model.Transition(). Holds the graph
// traversal, the random-walk explorer, the per-step checker, and the tuning knobs; below the "helpers"
// divider, event enumeration, error classification, and failure formatting. The configs and test entry
// points are in activity_standalone_conformance_test.go.

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
)

// --- tuning knobs --------------------------------------------------------------------------

// saaMaxDepth is the BFS depth cap, raised by TEMPORAL_SAASPEC_MAX_DEPTH.
func saaMaxDepth() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_MAX_DEPTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 4
}

// saaSkipNegativePoll drops the ~3s "a Paused activity must not dispatch" long poll, the dominant cost
// of deep walks. The per-edge state check still runs.
func saaSkipNegativePoll() bool { return os.Getenv("TEMPORAL_SAASPEC_NO_NEGATIVE_POLL") != "" }

func saaWalkSteps() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_STEPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 200 // fits the default per-test context; raise it along with TEMPORAL_TEST_TIMEOUT
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

// --- driver / engine ----------------------------------------------------------------------

// traverse does a breadth-first walk of the model's reachable states, verifying every decided
// edge against the server.
func (d *saaDriverDeclarative) traverse(t *testing.T) {
	type node struct {
		path  []model.Event
		state model.AbstractState
	}
	start := model.Initial(d.cfg)
	visited := map[string]bool{model.Fingerprint(start): true}
	frontier := []node{{nil, start}}

	verifiedCells := map[saaCell]bool{}
	skippedCells := map[saaCell]bool{}
	// Fingerprint-granularity ledger, for the completeness check.
	verifiedFine := map[string]bool{}
	skippedFine := map[string]bool{}

	d.verifyPath(t, nil) // the freshly started activity matches Initial(cfg)

	edges, states := 0, 1
	maxDepth := saaMaxDepth()
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []node
		for _, nd := range frontier {
			for _, e := range saaCandidateEvents() {
				out := model.Transition(d.cfg, nd.state, e)
				edges++
				path := append(append([]model.Event{}, nd.path...), e)
				res, reached := d.verifyPath(t, path)
				if reached {
					c := saaCell{nd.state.Status, e.Kind}
					key := model.CellKey(nd.state, e.Kind)
					if res == saaSkippedNoToken {
						skippedCells[c] = true
						skippedFine[key] = true
					} else {
						verifiedCells[c] = true
						verifiedFine[key] = true
					}
				}
				if out.Reject != model.NoError {
					continue // rejected/no-op: no new state to extend from
				}
				fp := model.Fingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					states++
					next = append(next, node{path, out.Next})
				}
			}
		}
		frontier = next
	}

	t.Logf("cfg %d: verified %d decided edges (%d distinct cells) across %d reachable states (depth<=%d)",
		d.cfgIdx, edges, len(verifiedCells), states, maxDepth)

	// The only decided edges the traversal cannot verify are worker RPCs on a path that never polled, so
	// holds no task token.
	if os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") != "" {
		var unexercised []string
		for c := range skippedCells {
			if !verifiedCells[c] {
				unexercised = append(unexercised, fmt.Sprintf("%s/%s", c.status, model.KindName(c.kind)))
			}
		}
		sort.Strings(unexercised)
		if len(unexercised) > 0 {
			t.Logf("cfg %d: decided cells NOT exercised (worker RPC, no token on a never-polled path): %v",
				d.cfgIdx, unexercised)
		}
	}

	d.checkCompleteness(t, verifiedFine, skippedFine)
}

// checkCompleteness logs the cells the model can reach but this run did not — what the depth cap left
// out. Informational; it never fails.
func (d *saaDriverDeclarative) checkCompleteness(t *testing.T, verifiedFine, skippedFine map[string]bool) {
	if os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") == "" {
		return
	}
	var gaps []string
	for key := range model.Reachable(d.cfg, saaCandidateEvents()) {
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
		d.cfgIdx, len(gaps), saaMaxDepth(), strings.Join(shown, "\n  "), suffix)
}

// verifyPath starts a fresh activity, replays the path, and asserts only its final edge. A prefix edge
// that diverges aborts the replay silently; that edge is reported when it is the final edge of its own
// shorter path.
func (d *saaDriverDeclarative) verifyPath(t require.TestingT, path []model.Event) (saaApply, bool) {
	a := d.start(t)
	a.path = path
	cur := model.Initial(d.cfg)

	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Errorf("cfg %d: state immediately after StartActivityExecution disagrees with Initial(cfg).\n%s",
			d.cfgIdx, saaStateDiff(obs, cur))
		return saaMismatch, false
	}

	for i, e := range path {
		out := model.Transition(d.cfg, cur, e)
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

// outranDispatchWindow marks a report as a driver failure rather than a product one: the dispatch
// window the negative poll meant to check had already closed by the time it ran.
const outranDispatchWindow = "the driver outran the dispatch window"

// negativePollResult is what a negative poll established about a pending dispatch window.
type negativePollResult int

const (
	dispatchedNothing negativePollResult = iota // the window held
	dispatchedEarly                             // a task arrived while the window was still open
	windowOutrun                                // the window closed too soon to establish either
)

// negativePoll checks that a pending dispatch window dispatches nothing, bounding the poll by what the
// server says is left of the window rather than by the delay the driver configured, which assumes no
// time has passed since it began.
//
// A task it does find is adjudicated by comparing two observed times — whether the dispatch time had
// arrived by the time the poll returned — so no margin is needed to keep the poll inside the window. A
// poll that straddles the boundary establishes nothing rather than blaming the product.
func (a *saaHandle) negativePoll(t require.TestingT) (negativePollResult, *workflowservice.PollActivityTaskQueueResponse) {
	next := a.describe(t).GetInfo().GetNextAttemptScheduleTime()
	if next == nil {
		return windowOutrun, nil // the dispatch time passed before the check began
	}
	dispatchTime := next.AsTime()
	timeout := min(saaPollTimeout, time.Until(dispatchTime))
	if timeout < common.MinLongPollTimeout {
		return windowOutrun, nil // too little left for a poll that reaches matching
	}
	resp := a.pollForTask(t, timeout)
	if resp == nil {
		return dispatchedNothing, nil
	}
	return adjudicateDispatch(time.Now(), dispatchTime), resp
}

// adjudicateDispatch says whether a task a negative poll found arrived early, or only as the window it
// was checking closed underneath it.
func adjudicateDispatch(polledUntil, dispatchTime time.Time) negativePollResult {
	if polledUntil.Before(dispatchTime) {
		return dispatchedEarly
	}
	return windowOutrun
}

// saaCell identifies a (source status, event kind) pair for the coverage ledger.
type saaCell struct {
	status model.Status
	kind   model.EventKind
}

func (a *saaHandle) apply(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome, final bool) saaApply {
	if e.Kind == model.PollKind {
		return a.applyPoll(cur, out, final, t)
	}
	if saaIsWallClock(e.Kind) {
		return a.applyWallClock(t, e, cur, out, final)
	}
	// A worker RPC needs a task token, held only after a poll. An empty token yields a different error
	// than the model's NotFound, so the edge is not drivable on a never-polled path.
	if model.NeedsToken(e.Kind) && a.token == nil {
		return saaSkippedNoToken
	}
	err := a.rpc(e)
	if model.CarriesReqID(e.Kind) && out.Reject == model.NoError && out.Next.Status != cur.Status {
		// This request established a new state, so its id is the one a later SameRequestID reuses. An
		// intervening rejected or no-op request must not overwrite it.
		a.establishedReqID[e.Kind] = a.lastReqID
	}
	// Drop an established id once the model no longer treats that op's SameRequestID replay as an
	// idempotent no-op, which is what probing it here determines. Beyond that region the server would
	// dedupe the already-consumed id, while the model, which tracks no id history, expects a fresh op.
	for k := range a.establishedReqID {
		probe := model.Transition(a.d.cfg, out.Next, model.Event{Kind: k, SameRequestID: true})
		if probe.Reject != model.NoError || !probe.Next.SameObserved(out.Next) {
			delete(a.establishedReqID, k)
		}
	}
	ok := a.verify(t, e, cur, out, err, final)
	if e.Kind == model.HeartbeatKind && out.Reject == model.NoError {
		observed := model.HeartbeatFlags{
			CancelRequested: a.lastHeartbeat.GetCancelRequested(),
			ActivityPaused:  a.lastHeartbeat.GetActivityPaused(),
			ActivityReset:   a.lastHeartbeat.GetActivityReset(),
		}
		expected := model.ExpectedHeartbeatFlags(cur)
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
func (a *saaHandle) verify(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome, rpcErr error, final bool) bool {
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

// checkTaskInvalidation compares each raw stamp's change across the edge under test against the model's
// per-transition invalidation bools. observed() has already refreshed cur/prev for this edge.
func (a *saaHandle) checkTaskInvalidation(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome) {
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

// applyPoll drives a Poll and checks the dispatch against the model: a dispatchable activity must
// dispatch a task, and a delayed or paused one must not.
func (a *saaHandle) applyPoll(cur model.AbstractState, out model.Outcome, final bool, t require.TestingT) saaApply {
	poll := model.Poll
	switch {
	case cur.Status == model.Scheduled && out.Next.Status == model.Started:
		// A dispatchable activity must dispatch. The traces bound the deadline, so "Dispatchable" means
		// "dispatches promptly".
		timeout := cmp.Or(a.d.positivePollTimeout, saaPositivePollTimeout)
		resp := a.pollForTask(t, timeout)
		if resp == nil {
			if final {
				t.Errorf("%s: model expected STARTED but no task was dispatched within %s (scheduled, never dispatched)\n%s",
					a.edge(poll, cur.Status), timeout, a.pathLine())
			}
			return saaMismatch
		}
		a.token = resp.GetTaskToken()
		if final && resp.GetAttempt() != out.Next.AttemptCount {
			t.Errorf("%s: dispatched task attempt number disagrees — server saw %d, model expected %d\n%s",
				a.edge(poll, cur.Status), resp.GetAttempt(), out.Next.AttemptCount, a.pathLine())
		}
	case cur.Status == model.Scheduled && cur.Dispatchability != model.Dispatchable:
		// A start_delay or backoff is still pending, so the poll must find no task. Only worth a negative
		// poll when the configured delay outlasts a valid long poll; otherwise the state comparison below
		// suffices.
		if a.d.dispatchDelay(cur.Dispatchability) > saaPollTimeout {
			switch result, resp := a.negativePoll(t); result {
			case dispatchedEarly:
				if final {
					t.Errorf("%s: model expected no dispatch (%s pending) but a task WAS dispatched (attempt %d)\n%s",
						a.edge(poll, cur.Status), cur.Dispatchability, resp.GetAttempt(), a.pathLine())
				}
				return saaMismatch
			case windowOutrun:
				if final {
					t.Errorf("%s: %s — %s was configured, but its dispatch time arrived before the check could "+
						"establish anything, so this edge went unchecked. Lengthen the window, or shorten the "+
						"trace ahead of it.\n%s",
						a.edge(poll, cur.Status), outranDispatchWindow, cur.Dispatchability, a.pathLine())
				}
				return saaMismatch
			}
		}
	case cur.Status == model.Paused && !saaSkipNegativePoll():
		// A PAUSED activity must not dispatch: Pause invalidated the pending dispatch task. The only status
		// where a spurious dispatch is possible, so the only place worth the full long-poll wait.
		if resp := a.pollForTask(t, saaPollTimeout); resp != nil {
			if final {
				t.Errorf("%s: model expected no advance but a task WAS dispatched\n%s",
					a.edge(poll, cur.Status), a.pathLine())
			}
			return saaMismatch
		}
	}
	// No other status can dispatch, so the state comparison below suffices.
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

// applyWallClock waits for a wall-clock event to take effect, then asserts the observed state equals
// out.Next. Where the model predicts an observable change it polls for that state; where it predicts
// none, the only way to confirm is to wait the window out and see nothing move.
func (a *saaHandle) applyWallClock(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome, final bool) saaApply {
	deadline := time.Now().Add(a.d.eventClock(e) + saaWallClockSettle)
	switch {
	case saaIsDispatchDelay(e.Kind) && out.Next.Dispatchability == model.Dispatchable &&
		cur.Dispatchability != model.Dispatchable:
		// The delay elapsing is not visible in the component state the oracle compares — Dispatchability is
		// masked out of SameObserved — so assert the public dispatch time passing instead. Otherwise this
		// edge would be a sleep that verifies nothing.
		a.awaitDispatchTimePassed(t, e, deadline)
	case out.Next.SameObserved(cur):
		time.Sleep(time.Until(deadline))
	default:
		a.awaitObservedMatch(out.Next, deadline)
	}
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

// checkDescribe asserts that the status, run state, and attempt DescribeActivityExecution reports match
// model.ExpectedDescribe.
func (a *saaHandle) checkDescribe(t require.TestingT, expected model.AbstractState) {
	st, rs := model.ExpectedDescribe(expected)
	resp, err := a.d.env.FrontendClient().DescribeActivityExecution(a.d.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
	})
	require.NoError(t, err)
	gotSt, gotRs := resp.GetInfo().GetStatus(), resp.GetInfo().GetRunState()
	if gotSt != st || gotRs != rs {
		t.Errorf("Describe while in internal status %s does not match model expectation\n%s\n"+
			"  server saw:     status=%v run=%v\n"+
			"  model expected: status=%v run=%v",
			expected.Status, a.pathLine(), gotSt, gotRs, st, rs)
	}
	if gotAttempt := resp.GetInfo().GetAttempt(); gotAttempt != expected.AttemptCount {
		t.Errorf("Describe attempt while in internal status %s does not match model expectation\n%s\n"+
			"  server saw:     attempt=%d\n"+
			"  model expected: attempt=%d",
			expected.Status, a.pathLine(), gotAttempt, expected.AttemptCount)
	}
}

// randomWalk drives one activity, picking a random applicable event each step and checking it against
// model.Transition. On reaching a terminal state, or diverging, it restarts on a fresh activity, until
// the step budget is spent.
func (d *saaDriverDeclarative) randomWalk(t *testing.T, rng *rand.Rand, maxSteps int) {
	verbose := saaVerbose()
	seen := map[string]bool{}
	walks := 0

	a, cur := d.walkStart(t)
	var trace []model.Event // events driven since the last (re)start, for the divergence report
	seen[model.Fingerprint(cur)] = true
	walks++
	if verbose {
		t.Logf("cfg %d walk %d: start %s", d.cfgIdx, walks, cur.Status)
	}

	for step := range maxSteps {
		if cur.Status.Terminal() {
			a, cur = d.walkStart(t)
			trace = nil
			seen[model.Fingerprint(cur)] = true
			walks++
			if verbose {
				t.Logf("cfg %d walk %d: start %s (restart after terminal)", d.cfgIdx, walks, cur.Status)
			}
			continue
		}
		e := d.pickWalkEvent(rng, a, cur)
		trace = append(trace, e)
		a.path = trace
		out := model.Transition(d.cfg, cur, e)
		res := a.apply(t, e, cur, out, true)
		if verbose {
			t.Logf("cfg %d walk %d step %d: %s", d.cfgIdx, walks, step, saaStepDesc(cur, e, out, res))
		}
		switch res {
		case saaVerified:
			cur = out.Next
			seen[model.Fingerprint(cur)] = true
		case saaSkippedNoToken:
			trace = trace[:len(trace)-1] // not driven, so not part of the trace
		case saaMismatch:
			// apply() has already reported the divergence. Restart from a known state rather than compound
			// from a suspect one.
			a, cur = d.walkStart(t)
			trace = nil
			walks++
		}
	}
	t.Logf("cfg %d: random walk done — %d steps, %d walks, %d distinct states covered",
		d.cfgIdx, maxSteps, walks, len(seen))
}

// walkStart begins a fresh activity and asserts it matches Initial(cfg).
func (d *saaDriverDeclarative) walkStart(t *testing.T) (*saaHandle, model.AbstractState) {
	a := d.start(t)
	cur := model.Initial(d.cfg)
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("cfg %d: fresh activity disagrees with Initial(cfg)\n%s", d.cfgIdx, saaStateDiff(obs, cur))
	}
	return a, cur
}

// pickWalkEvent chooses the next event, strongly preferring one that makes non-terminal progress so the
// walk goes deep rather than restarting every few steps. It still sometimes takes a terminal or a
// reject/no-op edge, so those are exercised deep too. The BFS covers terminal edges exhaustively.
// Events needing a task token the handle does not hold are skipped.
func (d *saaDriverDeclarative) pickWalkEvent(rng *rand.Rand, a *saaHandle, cur model.AbstractState) model.Event {
	var applicable, changing, deep []model.Event
	for _, e := range saaCandidateEvents() {
		if model.NeedsToken(e.Kind) && a.token == nil {
			continue
		}
		applicable = append(applicable, e)
		if out := model.Transition(d.cfg, cur, e); out.Reject == model.NoError && !out.Next.SameObserved(cur) {
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
		return model.Poll // needs no token, so always applicable
	}
}

// --- helpers -------------------------------------------------------------------------------

// saaStepDesc renders one walk step as "FromStatus --Event--> ToStatus", annotated with the reject
// kind or a no-token skip.
func saaStepDesc(cur model.AbstractState, e model.Event, out model.Outcome, res saaApply) string {
	desc := fmt.Sprintf("%s --%s--> %s", cur.Status, model.EventLabel(e), out.Next.Status)
	switch {
	case res == saaSkippedNoToken:
		desc += "  [skipped: no token]"
	case out.Reject != model.NoError:
		desc += "  [" + saaRejectKindName(out.Reject) + "]"
	}
	return desc
}

// saaCandidateEvents is the tier-3 event alphabet: the worker RPCs and the operator commands, with a
// variant per outcome-affecting flag.
func saaCandidateEvents() []model.Event {
	var out []model.Event
	simple := []model.EventKind{
		model.PollKind, model.HeartbeatKind, model.RespondCompletedKind, model.RespondCanceledKind, model.UpdateOptionsKind,
	}
	for _, k := range simple {
		out = append(out, model.Event{Kind: k})
	}
	// start_delay is mutable only within the StartDelayPending window, so the model rejects this in every
	// state the RPC-only traversal reaches.
	out = append(out, model.Event{Kind: model.UpdateOptionsKind, SetsStartDelay: true})
	for _, r := range []bool{false, true} {
		out = append(out, model.Event{Kind: model.RespondFailedKind, Retryable: r})
	}
	for _, sr := range []bool{false, true} {
		out = append(out,
			model.Event{Kind: model.PauseKind, SameRequestID: sr},
			model.Event{Kind: model.TerminateKind, SameRequestID: sr},
			model.Event{Kind: model.RequestCancelKind, SameRequestID: sr},
		)
	}
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, model.Event{Kind: model.ResetKind, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	for _, ra := range []bool{false, true} {
		out = append(out, model.Event{Kind: model.UnpauseKind, ResetAttempts: ra})
	}
	return out
}

// --- error / outcome classification --------------------------------------------------------

// saaRejectKind classifies an RPC error as the model's ErrorKind. The FrontendClient returns
// serviceerror types, so this matches on type rather than on gRPC status code.
func saaRejectKind(err error) model.ErrorKind {
	if err == nil {
		return model.NoError
	}
	var nf *serviceerror.NotFound
	var fp *serviceerror.FailedPrecondition
	var ia *serviceerror.InvalidArgument
	switch {
	case errors.As(err, &nf):
		return model.NotFound
	case errors.As(err, &fp):
		return model.FailedPrecondition
	case errors.As(err, &ia):
		return model.InvalidArgument
	default:
		return model.ErrorKind(-1) // unrecognized, so it matches no predicted kind
	}
}

// --- failure reporting ---------------------------------------------------------------------
//
// A failure means the server ("observed") disagreed with model.Transition ("expected") after one event.
// Each report is a one-line summary, the path to the edge, then a field-aligned diff.

// edge names the event and the status it was driven from, e.g. "RespondFailed[retryable=true] from
// Started".
func (a *saaHandle) edge(e model.Event, src model.Status) string {
	return fmt.Sprintf("%s from %s", model.EventLabel(e), src)
}

func (a *saaHandle) pathLine() string {
	return "  path: " + saaPathString(a.path)
}

// stateFailure reports that the persisted state after an event disagreed with the model.
func (a *saaHandle) stateFailure(e model.Event, src model.Status, observed, expected model.AbstractState) string {
	var summary string
	if observed.Status != expected.Status {
		summary = fmt.Sprintf("model expected %s, server saw %s", expected.Status, observed.Status)
	} else {
		summary = fmt.Sprintf("status %s agrees but persisted state differs", observed.Status)
	}
	return fmt.Sprintf("%s: %s\n%s\n%s", a.edge(e, src), summary, a.pathLine(), saaStateDiff(observed, expected))
}

// rejectFailure reports that the RPC's accept/reject outcome disagreed with the model.
func (a *saaHandle) rejectFailure(e model.Event, src model.Status, got, want model.ErrorKind, err error) string {
	msg := fmt.Sprintf("%s: server %s, model expected %s\n%s",
		a.edge(e, src), saaOutcomeDesc(got), saaOutcomeDesc(want), a.pathLine())
	if err != nil {
		msg += fmt.Sprintf("\n  server error: %v", err)
	}
	return msg
}

// flagsFailure reports that the worker-facing heartbeat response flags disagreed with the model.
func (a *saaHandle) flagsFailure(e model.Event, src model.Status, observed, expected model.HeartbeatFlags) string {
	rows, agree := saaFlagRows(observed, expected)
	return fmt.Sprintf("%s: heartbeat response flags disagree\n%s\n%s",
		a.edge(e, src), a.pathLine(), saaDiffBlock(rows, agree))
}

// saaPathString renders the event sequence that reached an edge, e.g.
// "Schedule → Poll → RespondFailed[retryable=false]". The origin is labeled Schedule, the status
// StartActivityExecution lands in, rather than Started.
func saaPathString(path []model.Event) string {
	parts := make([]string, 0, len(path)+1)
	parts = append(parts, "Schedule")
	for _, e := range path {
		parts = append(parts, model.EventLabel(e))
	}
	return strings.Join(parts, " → ")
}

// saaStateDiff renders the AbstractState fields that differ in aligned columns, with the agreeing
// fields listed as field=value beneath.
func saaStateDiff(observed, expected model.AbstractState) string {
	b2s := func(b bool) string { return fmt.Sprint(b) }
	fields := [][3]string{
		{"Status", observed.Status.String(), expected.Status.String()},
		{"Count", fmt.Sprint(observed.AttemptCount), fmt.Sprint(expected.AttemptCount)},
		{"ResetKeepPaused", b2s(observed.ResetKeepPaused), b2s(expected.ResetKeepPaused)},
		{"ResetHeartbeats", b2s(observed.ResetHeartbeats), b2s(expected.ResetHeartbeats)},
		{"ResetRestoreOptions", b2s(observed.ResetRestoreOptions), b2s(expected.ResetRestoreOptions)},
		{"FirstAttemptStarted", b2s(observed.FirstAttemptStarted), b2s(expected.FirstAttemptStarted)},
		{"DispatchTimeSet", b2s(observed.DispatchTimeSet), b2s(expected.DispatchTimeSet)},
	}
	return saaDiffBlock(saaSplit(fields))
}

func saaFlagRows(observed, expected model.HeartbeatFlags) (rows [][3]string, agree []string) {
	fields := [][3]string{
		{"CancelRequested", fmt.Sprint(observed.CancelRequested), fmt.Sprint(expected.CancelRequested)},
		{"ActivityPaused", fmt.Sprint(observed.ActivityPaused), fmt.Sprint(expected.ActivityPaused)},
		{"ActivityReset", fmt.Sprint(observed.ActivityReset), fmt.Sprint(expected.ActivityReset)},
	}
	return saaSplit(fields)
}

// saaSplit partitions (field, observed, expected) triples into those that differ and those that agree,
// the latter rendered as "field=value".
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

func saaOutcomeDesc(k model.ErrorKind) string {
	if k == model.NoError {
		return "accepted"
	}
	return "rejected with " + saaRejectKindName(k)
}

func saaRejectKindName(k model.ErrorKind) string {
	switch k {
	case model.NoError:
		return "NoError"
	case model.FailedPrecondition:
		return "FailedPrecondition"
	case model.NotFound:
		return "NotFound"
	case model.InvalidArgument:
		return "InvalidArgument"
	default:
		return fmt.Sprintf("unrecognized(%d)", int(k))
	}
}
