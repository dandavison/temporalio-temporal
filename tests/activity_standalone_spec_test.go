package tests

// Executable behavior spec for the standalone-activity (SAA) product surface. saaspec.Model() is the
// spec — a total function Model(cfg, state, event) -> Outcome — and these tests drive a real onebox
// server through the same event alphabet, asserting the server agrees with Model() at every step.
//
// This file holds the parts intended to be edited: the traversal configs and the dispatch-delay /
// timeout traces (top), followed by the four test entry points, all subtests of
// TestStandaloneActivityTestSuite/TestSpec. The harness that drives and checks each event lives in
// activity_standalone_spec_utils_test.go.

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

// ---------------------------------------------------------------------------------------------
// Configs and traces (edit these)
// ---------------------------------------------------------------------------------------------

// saaTraversalConfigs are the activity configurations the graph traversal and random walk explore.
var saaTraversalConfigs = []saaspec.Config{
	{}, // no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
	// Retries exhaust after the first attempt, so the RespondFailed exhaustion boundary
	// (retryable failure with no retries left -> Failed) is reached at depth 2 rather than
	// past the depth bound. See the completeness check.
	{MaxAttempts: 1},
	// Start-delay window: the activity stays StartDelayPending for the whole traversal (no RPC event
	// leaves that state), so this crosses the operator commands (pause/unpause/reset/update/cancel/
	// terminate) with the start-delay window and verifies via the per-Poll negative poll that none of
	// them dispatches early. The second adds schedule-to-close so its window-invalidation is exercised.
	{HasStartDelay: true},
	{HasStartDelay: true, HasScheduleToClose: true},
}

// --- dispatch-delay traces -------------------------------------------------------------------
//
// Each trace checks the impl against the Dispatchability behavior Model() specifies (start_delay /
// retry backoff) and the *Elapses events. A trace is an event sequence run once on one activity; at
// every step the harness asserts the observed state and pollability against Model(), and a *Elapses
// event is realized by waiting for the real timer. Traces rather than the breadth-first traversal
// because each pending-delay step costs a real multi-second wait, and the traversal replays every
// path from scratch (re-incurring every prefix wait). A trace pays each wait once.

// The delay windows are long enough to outlast a valid negative long poll (> the long-poll
// minimum), so "not dispatchable yet" is observable.
const saaDispatchWindow = 5 * time.Second

type saaDispatchTrace struct {
	name           string
	cfg            saaspec.Config
	startDelay     time.Duration
	retryInterval  time.Duration
	nextRetryDelay time.Duration // worker-supplied override of the policy backoff
	trace          []saaspec.Event
}

var (
	saaPoll      = saaspec.Event{Kind: saaspec.Poll}
	saaFailRetry = saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}
	saaSDElapse  = saaspec.Event{Kind: saaspec.StartDelayElapses}
	saaBOElapse  = saaspec.Event{Kind: saaspec.BackoffElapses}
)

var saaDispatchTraces = []saaDispatchTrace{
	// start_delay delays the first dispatch: a poll finds no task until the delay elapses.
	{
		name: "start-delay/first-dispatch", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaSDElapse, saaPoll},
	},
	// pause during the start delay, then unpause: still delayed (poll finds nothing) until it elapses.
	{
		name: "start-delay/pause-then-unpause", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{{Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, saaPoll, saaSDElapse, saaPoll},
	},
	// reset during the start delay: still delayed (behaves like unpause).
	{
		name: "start-delay/reset", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{{Kind: saaspec.Reset}, saaPoll, saaSDElapse, saaPoll},
	},
	// update start_delay to a long value during the delay window, then UpdateOptions(RestoreOriginal):
	// the dispatch window must return to the original start_delay. If restore-original did not restore
	// start_delay, the window would stay long and the final poll (after the original delay elapses)
	// would find no task. This gives UpdateOptions(RestoreOriginal) genuine state-level coverage.
	{
		name: "start-delay/update-then-restore-original", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{
			{Kind: saaspec.UpdateOptions, SetsStartDelay: true},
			{Kind: saaspec.UpdateOptions, RestoreOriginal: true},
			saaPoll, saaSDElapse, saaPoll,
		},
	},
	// a retry is delayed by the policy backoff: a poll finds no task until the backoff elapses.
	{
		name: "backoff/retry-dispatch", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, saaPoll, saaBOElapse, saaPoll},
	},
	// a worker-supplied next_retry_delay overrides the (short, default) policy interval: the retry is
	// delayed by the override. The policy backoff is ~200ms, so if the override were ignored the
	// retry would dispatch during the negative poll and the trace would catch it.
	{
		name: "backoff/next-retry-delay-override", cfg: saaspec.Config{MaxAttempts: 3}, nextRetryDelay: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, saaPoll, saaBOElapse, saaPoll},
	},
	// pause during the backoff, then unpause: still delayed until the backoff elapses.
	{
		name: "backoff/pause-then-unpause", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, saaPoll, saaBOElapse, saaPoll},
	},
	// pause/unpause during the backoff, then an unrelated options update: the pending backoff must
	// survive the update and not re-dispatch early (regression guard for the unpause path clearing
	// CurrentRetryInterval so a later re-dispatch loses the retry deadline).
	{
		name: "backoff/pause-unpause-then-update", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, {Kind: saaspec.UpdateOptions}, saaPoll, saaBOElapse, saaPoll},
	},
	// a worker next_retry_delay override followed by an unrelated options update: the override must be
	// preserved (not recalculated to the short policy interval), so the retry stays delayed. The policy
	// backoff is ~200ms, so if the update dropped the override the retry would dispatch during the
	// negative poll and the trace would catch it.
	{
		name: "backoff/next-retry-delay-override-then-update", cfg: saaspec.Config{MaxAttempts: 3}, nextRetryDelay: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.UpdateOptions}, saaPoll, saaBOElapse, saaPoll},
	},
	// reset during the backoff discards it: the reset attempt dispatches immediately.
	{
		name: "backoff/reset", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Reset}, saaPoll},
	},
}

// --- timeout traces --------------------------------------------------------------------------
//
// A timeout is modeled as an event (saaspec.ScheduleToCloseElapses, etc.), triggered by configuring
// the matching timeout short and waiting; driveTrace checks the resulting state against Model().
// Model must handle every timeout event, else the trace panics.

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace, so a short
// timeout under test fires while the activity is still SCHEDULED and pending dispatch.
const saaLongStartDelay = time.Hour

// An saaTimeoutTrace specifies a sequence of events (involving timeouts and/or delay elapses) to be
// tested.
type saaTimeoutTrace struct {
	name string
	// The timeout under test. It's set to a short value, while all the others are long, and placed
	// as the last event in the trace.
	timeout    saaspec.EventKind
	cfg        saaspec.Config
	path       []saaspec.Event
	startDelay time.Duration
}

// To read these: `timeout` is the timeout that fires first; `path` is the events leading up to it.
var saaTimeoutTraces = []saaTimeoutTrace{
	{name: "schedule-to-close/elapses-while-paused", timeout: saaspec.ScheduleToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Pause}}, cfg: saaspec.Config{HasScheduleToClose: true}},
	{name: "schedule-to-start/elapses-while-scheduled", timeout: saaspec.ScheduleToStartElapses, cfg: saaspec.Config{HasScheduleToStart: true}},
	{name: "schedule-to-start/elapses-while-paused", timeout: saaspec.ScheduleToStartElapses, path: []saaspec.Event{{Kind: saaspec.Pause}}, cfg: saaspec.Config{HasScheduleToStart: true}},
	{name: "start-to-close/elapses-while-started/retries-remain", timeout: saaspec.StartToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{}},
	{name: "start-to-close/elapses-while-started/last-attempt", timeout: saaspec.StartToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{MaxAttempts: 1}},
	// A worker that ignores a cancellation request must still time out: even with retries remaining,
	// a per-attempt timeout in CANCEL_REQUESTED ends the activity as TimedOut (not a retry).
	{name: "start-to-close/elapses-while-cancel-requested", timeout: saaspec.StartToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Poll}, {Kind: saaspec.RequestCancel}}, cfg: saaspec.Config{}},
	{name: "heartbeat/elapses-while-started/retries-remain", timeout: saaspec.HeartbeatElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{HasHeartbeat: true}},
	{name: "heartbeat/elapses-while-started/last-attempt", timeout: saaspec.HeartbeatElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{HasHeartbeat: true, MaxAttempts: 1}},
	// Dispatch-delay interactions
	{name: "schedule-to-start/elapses-within-start-delay", timeout: saaspec.ScheduleToStartElapses, startDelay: saaLongStartDelay, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToStart: true}},
	{name: "schedule-to-close/elapses-within-start-delay", timeout: saaspec.ScheduleToCloseElapses, startDelay: saaLongStartDelay, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToClose: true}},
}

// ---------------------------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------------------------

func (s *standaloneActivityTestSuite) TestSpec() {
	s.T().Run("RPCGraphTraversal", s.specRPCGraphTraversal)
	s.T().Run("RandomWalk", s.specRandomWalk)
	s.T().Run("DispatchDelayPaths", s.specDispatchDelayPaths)
	s.T().Run("TimeoutPaths", s.specTimeoutPaths)
}

// specRPCGraphTraversal walks the transition graph that saaspec.Model() describes and verifies every
// edge against a real onebox server. From each reachable state it tries every event, replays the path
// from a fresh activity, drives the event as a real RPC, reads the internal state back with
// ReadComponent, and asserts:
//   - the resulting internal state equals Model().Next exactly (all fields);
//   - each stamp's change across the edge matches Model()'s AttemptTasksInvalidated / ScheduleToCloseTaskInvalidated;
//   - the RPC's accept/reject outcome matches Model().Reject;
//   - for a heartbeat, the response flags equal ExpectedHeartbeatFlags.
//
// Model() is total over the RPC event alphabet; a cell it does not handle panics and fails the run.
// Timeouts are configured long (hours) so none fires mid-scenario; retry backoff is short so retries
// can be traversed. Timeout timing is checked by the timeout traces, not here.
func (s *standaloneActivityTestSuite) specRPCGraphTraversal(t *testing.T) {
	env := s.newTestEnv()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env:      env,
			ctx:      ctx,
			chasmCtx: chasmCtx,
			nsID:     env.NamespaceID().String(),
			cfg:      cfg,
			cfgIdx:   i,
		}
		if cfg.HasStartDelay {
			// Keep the first-dispatch window open for the whole traversal so the activity stays
			// StartDelayPending (the model never leaves that state via an RPC event), letting the BFS
			// cross the operator commands with the start-delay window.
			h.startDelay = time.Hour
		}
		h.traverse(t)
	}
}

// specRandomWalk drives one activity forward through randomly chosen events — no replay, no
// backtracking, no state dedup. Where the graph traversal is exhaustive but depth-bounded, this
// reaches deep, long interaction sequences the bounded BFS structurally never visits, at ~one RPC per
// step. Every step is checked against Model() (via the same apply()), so a divergence is caught the
// same way; the walk is deterministic in its seed (logged), so any failure reproduces exactly with
// TEMPORAL_SAASPEC_WALK_SEED. Deep runs need a raised budget: TEMPORAL_TEST_TIMEOUT and the go test
// -timeout. Set TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1 to skip the ~3s Paused negative poll.
func (s *standaloneActivityTestSuite) specRandomWalk(t *testing.T) {
	env := s.newTestEnv()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	seed, steps := saaWalkSeed(), saaWalkSteps()
	t.Logf("random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
			cfg: cfg, cfgIdx: i,
		}
		if cfg.HasStartDelay {
			// Keep the first-dispatch window open for the whole walk so the activity stays
			// StartDelayPending (no RPC event leaves it); the walk explores operator commands in the
			// window and, unlike the BFS, re-polls post-operation states (catching early re-dispatch).
			h.startDelay = time.Hour
		}
		// Independent, reproducible RNG stream per config.
		h.randomWalk(t, rand.New(rand.NewSource(seed+int64(i))), steps)
	}
}

// specDispatchDelayPaths runs the delayed-dispatch traces (start_delay / retry backoff), each once on
// one activity, paying each real multi-second wait a single time.
func (s *standaloneActivityTestSuite) specDispatchDelayPaths(t *testing.T) {
	env := s.newTestEnv()

	for i, tr := range saaDispatchTraces {
		t.Run(tr.name, func(t *testing.T) {
			// Fresh context per trace: each carries the default 90s deadline, so the multi-second
			// waits do not accumulate against a single shared budget.
			ctx := s.Context()
			chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
			require.NoError(t, err)
			h := &saaHarness{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: tr.cfg, cfgIdx: i,
				startDelay: tr.startDelay, retryInterval: tr.retryInterval, nextRetryDelay: tr.nextRetryDelay,
				// "Dispatchable" must mean "dispatches promptly", so bound the positive poll below the
				// delay window — that is how reset-discards (immediate) is told from still-delayed.
				positivePollTimeout: saaNegativePollTimeout,
			}
			h.driveTrace(t, tr.trace)
		})
	}
}

// specTimeoutPaths runs the timeout traces: each reaches a source state via the RPC path, then fires
// the timeout under test (configured short) and checks the resulting state against Model().
func (s *standaloneActivityTestSuite) specTimeoutPaths(t *testing.T) {
	env := s.newTestEnv()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	for i, p := range saaTimeoutTraces {
		t.Run(p.name, func(t *testing.T) {
			h := &saaHarness{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: p.cfg, cfgIdx: i, shortTimeout: p.timeout, startDelay: p.startDelay,
			}
			h.driveTrace(t, append(append([]saaspec.Event{}, p.path...), saaspec.Event{Kind: p.timeout}))
		})
	}
}
